#!/usr/bin/env python3
"""One-off backfill: flip historical `bronze.sofascore_event_player_stats`
anchor columns NULL -> /lineups values (#337, followup on #301).

Why this exists
---------------
#301 added forward-enrichment: NEW event_player_stats rows get
``is_home/captain/substitute/position_specific`` overlaid from
``/event/{id}/lineups`` (the statistics endpoint returns ``extra: null``
and no ``statistics.position``). But the event_player_stats runner is
append-only + skip-existing, so the ~15189 historical rows already in
bronze stay 100% NULL on those 4 columns. This script back-fills them.

How
---
1. Resolve distinct match_ids from the bronze table.
2. Fetch ``/lineups`` once per match (~380 calls, ~19 min at 20 req/min;
   NO 15k statistics calls needed — the 4 fields live only in lineups).
3. Build the same per-(match, player) overlay the scraper uses
   (``SofaScoreScraper._build_lineup_overlay_lookup``).
4. Stage the overlay in a VARCHAR temp table, then ``MERGE`` it into the
   bronze table, updating only rows whose anchors are still NULL.

Formatting consistency: the bronze columns are stored as VARCHAR (they
were created all-NULL, so no type was inferred). The forward path writes
Python bools through ``TrinoTableManager._format_sql_value`` which, for a
VARCHAR target, emits ``str(True)`` -> ``'True'`` / ``'False'``. We reuse
the SAME insert path (bool values into a VARCHAR staging table), so the
back-filled rows are byte-identical to future forward-filled rows.

Usage
-----
    python scripts/backfill_sofascore_eps_lineups.py --dry-run
    python scripts/backfill_sofascore_eps_lineups.py            # execute
    python scripts/backfill_sofascore_eps_lineups.py --limit 5  # smoke

Run inside the airflow container (needs proxys.txt + Trino creds).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import pandas as pd

sys.path.insert(0, '/opt/airflow')
sys.path.insert(0, '/opt/airflow/scrapers')

from scrapers.base.trino_manager import TrinoTableManager  # noqa: E402
from scrapers.sofascore.scraper import SofaScoreScraper  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger('backfill_eps_lineups')

TABLE = 'sofascore_event_player_stats'
SCHEMA = 'bronze'
STAGING = 'tmp_eps_lineup_overlay_337'
ANCHORS = ['is_home', 'captain', 'substitute', 'position_specific']


def _resolve_match_ids(mgr: TrinoTableManager) -> list[str]:
    rows = mgr._execute(
        f"SELECT DISTINCT match_id FROM {mgr.catalog}.{SCHEMA}.{TABLE} "
        f"ORDER BY match_id",
        fetch=True,
    )
    return [str(r[0]) for r in rows if r[0] is not None]


def _collect_overlay(match_ids: list[str], proxy_file: str | None) -> pd.DataFrame:
    """Fetch /lineups per match and flatten into overlay rows.

    Columns: match_id, player_id, is_home, captain, substitute,
    position_specific. is_home/captain/substitute are Python bools (the
    VARCHAR staging insert serialises them to 'True'/'False', matching the
    forward path).
    """
    overlay_rows: list[dict] = []
    misses = 0
    with SofaScoreScraper(
        leagues=['ENG-Premier League'], seasons=[2025], proxy_file=proxy_file,
    ) as scraper:
        for idx, mid in enumerate(match_ids, start=1):
            payload = scraper._fetch_lineup_payload(str(mid))
            if payload is None:
                misses += 1
                logger.warning("lineup miss for match_id=%s", mid)
                continue
            lookup = scraper._build_lineup_overlay_lookup(payload)
            for pid, fields in lookup.items():
                overlay_rows.append({
                    'match_id': str(mid),
                    'player_id': pid,
                    'is_home': fields['is_home'],
                    'captain': fields['captain'],
                    'substitute': fields['substitute'],
                    'position_specific': fields['position_specific'],
                })
            if idx % 25 == 0:
                logger.info("lineups progress: %d/%d matches", idx, len(match_ids))

    logger.info(
        "Collected %d overlay rows across %d matches (%d lineup misses)",
        len(overlay_rows), len(match_ids), misses,
    )
    return pd.DataFrame(
        overlay_rows,
        columns=['match_id', 'player_id', *ANCHORS],
    )


def _stage_and_merge(mgr: TrinoTableManager, df: pd.DataFrame) -> None:
    fq = f"{mgr.catalog}.{SCHEMA}.{TABLE}"
    stg = f"{mgr.catalog}.{SCHEMA}.{STAGING}"

    # Fresh staging table, all VARCHAR so bool -> 'True'/'False' on insert.
    mgr._execute(f"DROP TABLE IF EXISTS {stg}")
    mgr.create_iceberg_table(
        schema=SCHEMA,
        table=STAGING,
        columns={
            'match_id': 'varchar',
            'player_id': 'varchar',
            'is_home': 'varchar',
            'captain': 'varchar',
            'substitute': 'varchar',
            'position_specific': 'varchar',
        },
        if_not_exists=False,
    )
    inserted = mgr.insert_dataframe(SCHEMA, STAGING, df)
    logger.info("Staged %d overlay rows into %s", inserted, stg)

    # MERGE: only fill rows whose anchors are still NULL (idempotent re-run).
    merge_sql = f"""
MERGE INTO {fq} t
USING {stg} s
ON t.match_id = s.match_id AND t.player_id = s.player_id
WHEN MATCHED AND t.is_home IS NULL THEN UPDATE SET
    is_home = s.is_home,
    captain = s.captain,
    substitute = s.substitute,
    position_specific = s.position_specific
""".strip()
    logger.info("Running MERGE into %s ...", fq)
    mgr._execute(merge_sql)

    mgr._execute(f"DROP TABLE IF EXISTS {stg}")
    logger.info("Dropped staging table %s", stg)


def _verify(mgr: TrinoTableManager) -> None:
    fq = f"{mgr.catalog}.{SCHEMA}.{TABLE}"
    row = mgr._execute(
        f"SELECT "
        f"count(*) FILTER (WHERE is_home IS NOT NULL), "
        f"count(*) FILTER (WHERE captain IS NOT NULL), "
        f"count(*) FILTER (WHERE substitute IS NOT NULL), "
        f"count(*) FILTER (WHERE position_specific IS NOT NULL), "
        f"count(*) "
        f"FROM {fq}",
        fetch=True,
    )[0]
    logger.info(
        "POST-MERGE non-NULL: is_home=%d captain=%d substitute=%d "
        "position_specific=%d / total=%d",
        *row,
    )
    distinct = mgr._execute(
        f"SELECT DISTINCT is_home FROM {fq} WHERE is_home IS NOT NULL",
        fetch=True,
    )
    logger.info("Distinct is_home values: %s", sorted(str(r[0]) for r in distinct))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--dry-run', action='store_true',
                    help='Collect overlay and report; do NOT write to bronze.')
    ap.add_argument('--limit', type=int, default=None,
                    help='Cap number of matches (smoke test).')
    ap.add_argument('--match-ids', type=str, default=None,
                    help='Comma-separated match_ids to backfill instead of the '
                         'full table. Use to retry matches whose /lineups fetch '
                         'transiently failed during a full run (the MERGE only '
                         'fills rows still NULL, so this is safe to re-run).')
    args = ap.parse_args()

    proxy_file = os.environ.get('PROXY_FILE', '/opt/airflow/proxys.txt')
    if not os.path.exists(proxy_file):
        logger.warning("Proxy file %s not found — SofaScore may 403.", proxy_file)
        proxy_file = None

    mgr = TrinoTableManager()
    if args.match_ids:
        match_ids = [m.strip() for m in args.match_ids.split(',') if m.strip()]
    else:
        match_ids = _resolve_match_ids(mgr)
        if args.limit:
            match_ids = match_ids[: args.limit]
    logger.info("Backfilling overlay for %d matches", len(match_ids))

    df = _collect_overlay(match_ids, proxy_file)
    if df.empty:
        logger.error("No overlay rows collected — aborting (no DB writes).")
        return 2

    # Report what the overlay looks like.
    logger.info(
        "Overlay summary: is_home True=%d False=%d | captain True=%d | "
        "substitute True=%d | position_specific non-null=%d",
        int((df['is_home'] == True).sum()),  # noqa: E712
        int((df['is_home'] == False).sum()),  # noqa: E712
        int((df['captain'] == True).sum()),  # noqa: E712
        int((df['substitute'] == True).sum()),  # noqa: E712
        int(df['position_specific'].notna().sum()),
    )

    if args.dry_run:
        logger.info("--dry-run: skipping staging/MERGE. %d overlay rows ready.",
                    len(df))
        return 0

    _stage_and_merge(mgr, df)
    _verify(mgr)
    logger.info("Backfill complete.")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
