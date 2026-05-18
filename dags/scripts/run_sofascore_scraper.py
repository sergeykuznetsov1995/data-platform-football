#!/usr/bin/env python3
"""
SofaScore Scraper Runner Script
===============================

Standalone script to run SofaScore scraper.
Called from Airflow via BashOperator to avoid memory issues with PythonOperator.

Supported entities:
- ``schedule``        : per-round schedule + final scores (default)
- ``league_table``    : standings snapshot
- ``player_ratings``  : per-match player ratings (Opta 0.0-10.0) via
                       the public ``/api/v1/event/{id}/lineups`` endpoint.
                       Daily DAG passes the full set of finished matches;
                       writer uses ``replace_partitions=True`` so each run
                       refreshes ``(league, season)`` partition wholly.

Exit codes:
    0 — scrape completed successfully (>= 1 row written)
    1 — hard failure (exception raised, runner crashed)
    2 — graceful R0.2B_FALLBACK: lineups endpoint unavailable
        (HTTP 403 / proxy quota empty / repeated timeouts).
        DataFrame is empty, nothing written to bronze. The Gold-layer
        E4.4 schema-stub path keys off this exit code so the medallion
        pipeline keeps building without ratings.
"""

import argparse
import json
import logging
import os
import sys
import warnings
from typing import List, Optional

warnings.filterwarnings('ignore', category=DeprecationWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


# Entities runnable from this script. Kept as constants so we can wire
# the same names into Airflow without round-tripping through magic strings.
ENTITY_SCHEDULE = 'schedule'
ENTITY_LEAGUE_TABLE = 'league_table'
ENTITY_PLAYER_RATINGS = 'player_ratings'

VALID_ENTITIES = {ENTITY_SCHEDULE, ENTITY_LEAGUE_TABLE, ENTITY_PLAYER_RATINGS}


def _resolve_match_ids_from_bronze(
    league: str,
    season: str,
    limit: Optional[int],
) -> List[str]:
    """Pull finished match ids straight from ``bronze.sofascore_schedule``.

    Avoids re-hitting SofaScore for the schedule when we already have a
    fresh copy in the lakehouse. Returns ``[]`` when the table is missing
    or empty — the caller will then emit ``R0.2B_FALLBACK``.
    """
    try:
        import trino
        import trino.auth as trino_auth
    except ImportError as e:
        logger.error("trino client unavailable: %s", e)
        return []

    user = os.environ.get('TRINO_USER', 'airflow')
    password = os.environ.get('TRINO_PASSWORD')

    try:
        if password:
            conn = trino.dbapi.connect(
                host=os.environ.get('TRINO_HOST', 'trino'),
                port=int(os.environ.get('TRINO_PORT', 8443)),
                user=user,
                catalog='iceberg',
                http_scheme='https',
                auth=trino_auth.BasicAuthentication(user, password),
                verify=False,
            )
        else:
            conn = trino.dbapi.connect(
                host=os.environ.get('TRINO_HOST', 'trino'),
                port=int(os.environ.get('TRINO_PORT', 8080)),
                user=user,
                catalog='iceberg',
            )

        cur = conn.cursor()
        sql = (
            "SELECT CAST(game_id AS varchar) AS gid "
            "FROM iceberg.bronze.sofascore_schedule "
            "WHERE league = ? AND CAST(season AS varchar) = ? "
            "  AND home_score IS NOT NULL "
            "ORDER BY date DESC"
        )
        if limit:
            # Trino dialect: LIMIT goes in SQL; bind params don't bind it.
            sql = sql + f" LIMIT {int(limit)}"
        cur.execute(sql, (league, season))
        rows = cur.fetchall()
        return [r[0] for r in rows if r and r[0]]
    except Exception as e:
        logger.warning(
            "Could not resolve match_ids from bronze (%s) — "
            "falling back to soccerdata schedule fetch.", e,
        )
        return []


def _run_player_ratings(
    leagues: List[str],
    season: int,
    limit: Optional[int],
    output_path: str,
) -> int:
    """R0.2b player-ratings entrypoint. Returns process exit code."""
    from scrapers.sofascore import SofaScoreScraper
    from scrapers.sofascore.scraper import R0_2B_FALLBACK_MARKER

    league = leagues[0]  # ratings scrape is single-league per invocation
    # Schedule writer stores season as the soccerdata short form (e.g. "2526")
    season_str = str(season)
    if len(season_str) == 4 and season_str.isdigit():
        season_short = f"{season_str[2:4]}{int(season_str[2:4]) + 1:02d}"
    else:
        season_short = season_str

    logger.info(
        "R0.2b player_ratings: league=%s season=%s (short=%s) limit=%s",
        league, season, season_short, limit,
    )

    # 1) Pre-resolve match_ids from bronze.sofascore_schedule — avoids a
    #    fresh schedule scrape on every run.
    match_ids = _resolve_match_ids_from_bronze(league, season_short, limit)
    if not match_ids:
        # try with int-form season too — just in case the writer used int
        match_ids = _resolve_match_ids_from_bronze(league, season_str, limit)

    if not match_ids:
        logger.error(
            "%s: no match_ids available for league=%s season=%s. "
            "Run sofascore schedule scrape first or pass --match-ids.",
            R0_2B_FALLBACK_MARKER, league, season_short,
        )
        _write_results(output_path, {
            'entity': ENTITY_PLAYER_RATINGS,
            'tables': [],
            'rows': 0,
            'matches_attempted': 0,
            'fallback': True,
            'fallback_reason': 'no_match_ids_in_bronze',
            'errors': [f'{R0_2B_FALLBACK_MARKER}: no_match_ids'],
        })
        return 2

    logger.info("Resolved %d match_ids from bronze.sofascore_schedule",
                len(match_ids))

    proxy_file = os.environ.get('PROXY_FILE', '/opt/airflow/proxys.txt')
    if not os.path.exists(proxy_file):
        logger.warning(
            "Proxy file %s not found — SofaScore is likely to 403 "
            "without residential proxy.", proxy_file,
        )
        proxy_file = None

    results = {
        'entity': ENTITY_PLAYER_RATINGS,
        'tables': [],
        'rows': 0,
        'matches_attempted': len(match_ids),
        'matches_with_ratings': 0,
        'fallback': False,
        'fallback_reason': None,
        'errors': [],
    }

    try:
        with SofaScoreScraper(
            leagues=[league],
            seasons=[season],
            proxy_file=proxy_file,
        ) as scraper:
            df = scraper.read_player_ratings(
                league=league,
                season=int(season),
                match_ids=match_ids,
                limit=limit,
            )

            if df is None or df.empty:
                # Look at scraper's last fetch error to classify the
                # fallback reason.
                last_err = getattr(scraper, '_last_lineup_error', None)
                reason = 'empty_payload'
                if last_err:
                    status = last_err.get('status')
                    if status == 403:
                        reason = 'http_403'
                    elif status == 429:
                        reason = 'http_429'
                    elif status is None:
                        reason = 'transport_error'
                    else:
                        reason = f'http_{status}'

                logger.error(
                    "%s: SofaScore ratings unavailable — reason=%s detail=%s",
                    R0_2B_FALLBACK_MARKER, reason, last_err,
                )
                results['fallback'] = True
                results['fallback_reason'] = reason
                results['errors'].append(
                    f'{R0_2B_FALLBACK_MARKER}: {reason}'
                )
                _write_results(output_path, results)
                return 2

            table_path = scraper.save_to_iceberg(
                df=df,
                table_name='sofascore_player_ratings',
                partition_cols=['league', 'season'],
                replace_partitions=True,
            )
            results['tables'].append(table_path)
            results['rows'] = int(len(df))
            results['matches_with_ratings'] = int(df['match_id'].nunique())
            logger.info(
                "Saved %d rating rows for %d matches -> %s",
                results['rows'], results['matches_with_ratings'], table_path,
            )

    except Exception as e:
        logger.error("player_ratings scrape failed hard: %s", e, exc_info=True)
        results['errors'].append(str(e))
        _write_results(output_path, results)
        return 1

    _write_results(output_path, results)
    return 0


def _write_results(path: str, payload: dict) -> None:
    """Persist runner results to disk for Airflow XCom pickup."""
    try:
        with open(path, 'w') as f:
            json.dump(payload, f, default=str)
    except Exception as e:
        logger.warning("Could not write results to %s: %s", path, e)
    # Also dump to stdout so BashOperator XCom can capture it.
    try:
        print(json.dumps(payload, default=str))
    except Exception:
        pass


def _run_legacy(
    leagues: List[str],
    season: int,
    output_path: str,
) -> int:
    """Original behaviour: scrape schedule + league_table."""
    results = {
        'entity': 'all',
        'tables': [],
        'schedule_rows': 0,
        'league_table_rows': 0,
        'errors': [],
    }

    try:
        from scrapers.sofascore import SofaScoreScraper

        with SofaScoreScraper(leagues=leagues, seasons=[season]) as scraper:
            try:
                df = scraper.read_schedule()
                if df is not None and not df.empty:
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='sofascore_schedule',
                        partition_cols=['league', 'season'],
                    )
                    results['tables'].append(table_path)
                    results['schedule_rows'] = len(df)
                    logger.info(f"Saved {len(df)} schedule rows")
            except Exception as e:
                error_msg = f"Schedule scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

            try:
                df = scraper.read_league_table()
                if df is not None and not df.empty:
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='sofascore_league_table',
                        partition_cols=['league', 'season'],
                    )
                    results['tables'].append(table_path)
                    results['league_table_rows'] = len(df)
                    logger.info(f"Saved {len(df)} league table rows")
            except Exception as e:
                error_msg = f"League table scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
    except Exception as e:
        logger.error(f"Scraper failed: {e}", exc_info=True)
        results['errors'].append(str(e))
        _write_results(output_path, results)
        return 1

    _write_results(output_path, results)
    return 0


def main():
    parser = argparse.ArgumentParser(description='Run SofaScore scraper')
    parser.add_argument(
        '--entity',
        type=str,
        default='all',
        help=(
            'Which entity to scrape. One of: schedule, league_table, '
            'player_ratings, all (default: all -> schedule + league_table). '
            'player_ratings is R0.2b extension.'
        ),
    )
    parser.add_argument(
        '--leagues',
        type=str,
        default='ENG-Premier League',
        help='Comma-separated list of leagues',
    )
    # Keep legacy --league alias (singular) for player_ratings clarity.
    parser.add_argument(
        '--league',
        type=str,
        default=None,
        help='Single league override (used for player_ratings)',
    )
    parser.add_argument(
        '--season',
        type=int,
        default=2024,
        help='Season year (e.g. 2024 for 24-25, 2526 for 25-26 short)',
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Smoke-test cap: max number of matches to fetch ratings for',
    )
    parser.add_argument(
        '--output',
        type=str,
        default='/tmp/sofascore_result.json',
        help='Output file for results',
    )
    args = parser.parse_args()

    if args.league:
        leagues = [args.league]
    else:
        leagues = [l.strip() for l in args.leagues.split(',')]

    entity = args.entity.lower()
    if entity not in VALID_ENTITIES and entity != 'all':
        logger.error(
            "Invalid --entity %s. Must be one of %s or 'all'.",
            entity, sorted(VALID_ENTITIES),
        )
        return 1

    logger.info(
        "Starting SofaScore scraper: entity=%s leagues=%s season=%s limit=%s",
        entity, leagues, args.season, args.limit,
    )

    if entity == ENTITY_PLAYER_RATINGS:
        return _run_player_ratings(
            leagues=leagues,
            season=args.season,
            limit=args.limit,
            output_path=args.output,
        )

    # Default: legacy schedule+league_table flow.
    return _run_legacy(
        leagues=leagues,
        season=args.season,
        output_path=args.output,
    )


if __name__ == '__main__':
    sys.exit(main())
