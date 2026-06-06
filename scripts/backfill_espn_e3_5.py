#!/usr/bin/env python3
"""
ESPN Bronze backfill for E3.5 — APL seasons 2122/2223/2324 + redo 2425.

Backfill scope:
 - bronze.espn_schedule for 2122, 2223, 2324 (currently only 2425/2526 exist).
 - bronze.espn_lineup for 2122, 2223, 2324 (currently absent for these seasons).
 - bronze.espn_lineup REDO for 2425 (currently 1200 rows / 30 games — incomplete; redo to ~13K rows).
 - bronze.espn_matchsheet via soccerdata ``read_matchsheet`` (match-level team
   stats + venue). Previously populated by legacy ad-hoc ingestion with no
   surviving write-path (#298); this branch formalises it.

Idempotency: every save uses ``replace_partitions=['league','season']``, so re-runs
delete the (league, season) partition before inserting fresh rows. Per-season writes
are issued one-shot per table (no batched accumulation needed because each season is
its own partition; ESPN scraper class is per-season-instantiated which already maps
1:1 to a single partition write).

Why not the existing DAG/runner: ``dags/scripts/run_espn_scraper.py`` only writes
``espn_schedule``. ``espn_lineup`` and ``espn_matchsheet`` were
previously populated by ad-hoc ingestion (single batch_id per season). This script
formalises both through the same SoccerdataScraper.save_to_iceberg() path while
preserving the ``replace_partitions`` invariant required by full-state Bronze writes.
Mirror of espn_lineup: matchsheet stays backfill-only, NOT wired into the daily DAG.

Usage:
    python scripts/backfill_espn_e3_5.py                    # all 4 seasons, all tables
    python scripts/backfill_espn_e3_5.py --season 2122      # one season
    python scripts/backfill_espn_e3_5.py --skip-schedule    # only lineups + matchsheets
    python scripts/backfill_espn_e3_5.py --skip-lineup --skip-schedule   # only matchsheets
    python scripts/backfill_espn_e3_5.py --dry-run          # plan + soccerdata fetch only

Run inside Airflow container:
    docker compose exec -T airflow-scheduler python /opt/airflow/scripts/backfill_espn_e3_5.py
"""

import argparse
import logging
import sys
import traceback
from typing import List, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger('backfill_espn_e3_5')

LEAGUE = 'ENG-Premier League'
DEFAULT_SEASONS = ['2122', '2223', '2324', '2425']
PARTITION_COLS = ['league', 'season']


def _scrape_one_season(
    season: str,
    *,
    do_schedule: bool,
    do_lineup: bool,
    do_matchsheet: bool,
    dry_run: bool,
) -> dict:
    """Scrape a single ESPN season.

    Each call is its own ESPNScraper context to keep memory bounded and to ensure
    soccerdata's reader cache is per-season (avoids cross-season state leak).
    """
    from scrapers.espn import ESPNScraper

    out = {
        'season': season,
        'schedule_rows': 0,
        'lineup_rows': 0,
        'matchsheet_rows': 0,
        'schedule_error': None,
        'lineup_error': None,
        'matchsheet_error': None,
    }

    with ESPNScraper(leagues=[LEAGUE], seasons=[season]) as scraper:
        # ---- schedule ----
        if do_schedule:
            try:
                logger.info(f"[{season}] Fetching schedule")
                df = scraper.read_schedule()
                if df is None or df.empty:
                    raise RuntimeError("ESPN reader returned empty schedule")
                df = scraper._standardize_schedule(df)
                logger.info(f"[{season}] schedule rows={len(df)}, dry_run={dry_run}")
                if not dry_run:
                    scraper.save_to_iceberg(
                        df=df,
                        table_name='espn_schedule',
                        partition_cols=PARTITION_COLS,
                        replace_partitions=PARTITION_COLS,
                    )
                out['schedule_rows'] = len(df)
            except Exception as e:
                out['schedule_error'] = str(e)
                logger.error(f"[{season}] schedule failed: {e}")
                logger.debug(traceback.format_exc())

        # ---- lineup ----
        if do_lineup:
            try:
                logger.info(f"[{season}] Fetching lineup (calls per-match endpoints)")
                # soccerdata ESPN reader exposes read_lineup(match_id=None)
                # which iterates over the season's schedule under the hood and
                # caches each match JSON to ~/soccerdata/data/ESPN.
                reader = scraper._get_reader()
                df = scraper._execute_with_resilience(reader.read_lineup)
                if df is None or df.empty:
                    raise RuntimeError("ESPN reader returned empty lineup")
                # Reset multi-index (league, season, game) into columns and add
                # standard scraper metadata (_source / _entity_type / _ingested_at /
                # _batch_id) — same shape produced by SoccerdataScraper helpers.
                df = df.reset_index()
                df = scraper._add_metadata(df, 'lineup')
                # Existing bronze.espn_lineup schema declares the following
                # columns as ``varchar`` but soccerdata can return them as
                # int/float (when a value exists) or NaN. PyArrow infers a
                # numeric Arrow type from the column and Iceberg write rejects
                # the schema mismatch with:
                #   "Conversion failed for column sub_in with type object"
                # Force these columns to nullable string to match the existing
                # bronze schema.
                for col in (
                    'league', 'season', 'game', 'team', 'player',
                    'position', 'formation_place', 'sub_in', 'sub_out',
                ):
                    if col in df.columns:
                        # Map NaN -> None so pyarrow infers string with nulls;
                        # use Pandas StringDtype to keep nulls as <NA> rather
                        # than the literal "nan".
                        df[col] = df[col].astype('string').where(df[col].notna(), None)
                logger.info(
                    f"[{season}] lineup rows={len(df)} games={df['game'].nunique()}, "
                    f"dry_run={dry_run}"
                )
                if not dry_run:
                    scraper.save_to_iceberg(
                        df=df,
                        table_name='espn_lineup',
                        partition_cols=PARTITION_COLS,
                        replace_partitions=PARTITION_COLS,
                    )
                out['lineup_rows'] = len(df)
            except Exception as e:
                out['lineup_error'] = str(e)
                logger.error(f"[{season}] lineup failed: {e}")
                logger.debug(traceback.format_exc())

        # ---- matchsheet ----
        if do_matchsheet:
            try:
                logger.info(f"[{season}] Fetching matchsheet (match-level team stats + venue)")
                # soccerdata ESPN read_matchsheet returns one row per (game, team)
                # with venue + ~35 team stat columns. Same per-match-endpoint
                # iteration as read_lineup; results cache under ~/soccerdata/ESPN.
                reader = scraper._get_reader()
                df = scraper._execute_with_resilience(reader.read_matchsheet)
                if df is None or df.empty:
                    raise RuntimeError("ESPN reader returned empty matchsheet")
                df = df.reset_index()
                df = scraper._add_metadata(df, 'matchsheet')
                # Existing bronze.espn_matchsheet declares every stat column as
                # ``varchar``, but soccerdata returns numeric/NaN when a value
                # exists — PyArrow would infer a numeric Arrow type and the
                # Iceberg write rejects the schema mismatch (same failure mode the
                # lineup branch guards above). Force every object column to
                # nullable string (NaN -> None); leave is_home (boolean),
                # attendance (bigint) and _ingested_at (timestamp) at their types.
                for col in df.columns:
                    if col in ('is_home', 'attendance', '_ingested_at'):
                        continue
                    df[col] = df[col].astype('string').where(df[col].notna(), None)
                logger.info(
                    f"[{season}] matchsheet rows={len(df)} games={df['game'].nunique()}, "
                    f"dry_run={dry_run}"
                )
                if not dry_run:
                    scraper.save_to_iceberg(
                        df=df,
                        table_name='espn_matchsheet',
                        partition_cols=PARTITION_COLS,
                        replace_partitions=PARTITION_COLS,
                    )
                out['matchsheet_rows'] = len(df)
            except Exception as e:
                out['matchsheet_error'] = str(e)
                logger.error(f"[{season}] matchsheet failed: {e}")
                logger.debug(traceback.format_exc())

    return out


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description='ESPN E3.5 backfill')
    parser.add_argument(
        '--season',
        action='append',
        choices=DEFAULT_SEASONS + ['2526'],
        help='Season(s) to scrape (repeat). Default = 2122 2223 2324 2425.',
    )
    parser.add_argument('--skip-schedule', action='store_true')
    parser.add_argument('--skip-lineup', action='store_true')
    parser.add_argument('--skip-matchsheet', action='store_true')
    parser.add_argument('--dry-run', action='store_true', help='Fetch but do not write to Iceberg')
    args = parser.parse_args(argv)

    seasons = args.season or DEFAULT_SEASONS
    do_schedule = not args.skip_schedule
    do_lineup = not args.skip_lineup
    do_matchsheet = not args.skip_matchsheet

    logger.info(
        f"Backfill plan: seasons={seasons} schedule={do_schedule} "
        f"lineup={do_lineup} matchsheet={do_matchsheet} dry_run={args.dry_run}"
    )

    results = []
    overall_ok = True
    for season in seasons:
        r = _scrape_one_season(
            season,
            do_schedule=do_schedule,
            do_lineup=do_lineup,
            do_matchsheet=do_matchsheet,
            dry_run=args.dry_run,
        )
        results.append(r)
        if r['schedule_error'] or r['lineup_error'] or r['matchsheet_error']:
            overall_ok = False

    print('\n=== Backfill summary ===')
    for r in results:
        print(
            f"  season={r['season']:4s}  "
            f"schedule_rows={r['schedule_rows']:>6}  "
            f"lineup_rows={r['lineup_rows']:>6}  "
            f"matchsheet_rows={r['matchsheet_rows']:>6}  "
            f"errors={[e for e in (r['schedule_error'], r['lineup_error'], r['matchsheet_error']) if e]}"
        )

    return 0 if overall_ok else 2


if __name__ == '__main__':
    sys.exit(main())
