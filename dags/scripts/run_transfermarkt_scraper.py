#!/usr/bin/env python3
"""
Transfermarkt Scraper Runner Script
====================================

Standalone runner invoked from Airflow via BashOperator (isolated subprocess
to keep the LocalExecutor lean).

Supported entities:
- ``players``               : per-player snapshot for (league, season).
                              Anchor entity — runs first; transfers and
                              market_value_history resolve their player_id
                              roster from ``bronze.transfermarkt_players``.
- ``market_value_history``  : per-(player_id, mv_date) timeline via the
                              ``/ceapi/marketValueDevelopment/graph/{id}``
                              JSON endpoint.
- ``transfers``             : per-transfer rows via the
                              ``/ceapi/transferHistory/list/{id}`` JSON
                              endpoint.

Exit codes:
    0 — scrape completed successfully (>= 1 row written, or ``--dry-run``)
    1 — hard failure (exception raised, runner crashed; or a CLI parse error
        — unknown/typo'd flag, invalid value — #512, kept off exit 2 so the
        DAG wrapper does not mistake it for a TM_FALLBACK soft-success)
    2 — graceful ``TM_FALLBACK``: upstream endpoint unavailable (HTTP 403,
        proxy quota empty, repeated timeouts), or the bronze players table
        is missing/empty when a dependent entity ran. DataFrame is empty,
        nothing written to bronze. The DAG wraps exit 2 → exit 0 so
        validate_data can still summarise the run.
    3 — ``TM_REPLACE_GUARD``: completeness guard refused the
        replace-partitions save because the scraped frame holds fewer
        distinct players than 90% of the existing bronze partition
        (#484/#486). Nothing written. Bypass with ``--force-replace``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import warnings
from datetime import date
from typing import List, Optional

warnings.filterwarnings('ignore', category=DeprecationWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


class _ArgparseError(Exception):
    """Raised by _StrictArgumentParser.error so main() returns exit 1."""


class _StrictArgumentParser(argparse.ArgumentParser):
    """argparse exits 2 on a CLI parse error (bad/unknown flag, wrong type).
    The DAG bash wrapper maps exit 2 to TM_FALLBACK soft-success, so a flag
    typo would silently no-op the task (#512). Funnel every parse error
    through a catchable exception → main() returns hard-failure exit 1.
    """

    def error(self, message):
        self.print_usage(sys.stderr)
        raise _ArgparseError(message)


ENTITY_PLAYERS = 'players'
ENTITY_MV_HISTORY = 'market_value_history'
ENTITY_TRANSFERS = 'transfers'
ENTITY_COACHES = 'coaches'

VALID_ENTITIES = {ENTITY_PLAYERS, ENTITY_MV_HISTORY, ENTITY_TRANSFERS, ENTITY_COACHES}

# Replace-partitions completeness guard (#484/#486, generalised into
# BaseScraper.save_to_iceberg in #513): passed as min_replace_ratio +
# replace_guard_key='player_id'. Refuse the save when the scraped frame holds
# fewer distinct players than this share of the existing bronze partition.
# Distinct players (not raw rows) because per-player timeline lengths vary
# wildly for mv_history/transfers. ReplaceGuardError → exit 3.
_MIN_REPLACE_RATIO = 0.9
REPLACE_GUARD_MARKER = 'TM_REPLACE_GUARD'

# Rotating roster window (#620): transfers / mv_history are rate-capped to ~100
# players per run, so each run scrapes the NEXT window of the numerically sorted
# roster. The window index must advance by exactly +1 per DAG run; the DAG is
# weekly (0 4 * * 1), so we derive it from the logical date as ordinal // 7
# (consecutive weekly runs differ by exactly 1). Using the logical date keeps
# reruns reproducible — idempotent with the per-player upsert.
_WINDOW_STRIDE_DAYS = 7


def _window_offset(as_of_date: Optional[str]) -> int:
    """Roster-window index for this run.

    ``as_of_date`` is the Airflow logical date 'YYYY-MM-DD' (``{{ ds }}``);
    ``None`` falls back to today (standalone CLI use).
    """
    d = date.fromisoformat(as_of_date) if as_of_date else date.today()
    return d.toordinal() // _WINDOW_STRIDE_DAYS


def _write_results(path: str, payload: dict) -> None:
    """Persist runner JSON for Airflow XCom pickup. Mirrors the SofaScore
    runner's helper exactly so DAG validate_data code stays consistent.
    """
    try:
        with open(path, 'w') as f:
            json.dump(payload, f, default=str)
    except Exception as e:
        logger.warning("Could not write results to %s: %s", path, e)
    try:
        print(json.dumps(payload, default=str))
    except Exception:
        pass
    # Residential-proxy traffic per-run log (#789). Emits one grep-friendly
    # "PROXY_TRAFFIC source=transfermarkt total=… MB" line when the entity
    # captured a traffic block. Passive — never fails the run.
    traffic = payload.get('traffic')
    if traffic:
        try:
            from utils.proxy_traffic import (
                log_traffic_summary,
                summarize_result_traffic,
            )
            log_traffic_summary(
                summarize_result_traffic('transfermarkt', traffic)
            )
        except Exception as e:  # noqa: BLE001 — logging must not fail the run
            logger.warning("proxy-traffic log failed: %s", e)


def _classify_fallback(scraper) -> str:
    """Map ``_last_endpoint_error.status`` → short fallback reason tag."""
    last_err = getattr(scraper, '_last_endpoint_error', None)
    if not last_err:
        return 'empty_payload'
    status = last_err.get('status')
    if status == 403:
        return 'http_403'
    if status == 429:
        return 'http_429'
    if status is None:
        return 'transport_error'
    return f'http_{status}'


def _fallback_exit_code(reason: str) -> int:
    """Pick the runner exit code for a soft-fallback.

    An active block — the source refused us (http_403/429/5xx) or a transport
    error — is a real failure → exit 1, which the DAG bash wrapper lets turn the
    task red (mirrors the ESPN/SoFIFA runners, #466). A genuinely empty result
    (an empty page with NO http error → ``empty_payload``) stays exit 2, which
    the wrapper maps to a soft green — Transfermarkt always has data for an APL
    season, but we still reserve the soft path for the no-error case. (#790)
    """
    if reason and (reason.startswith('http_') or reason == 'transport_error'):
        return 1
    return 2


def _run_players(
    leagues: List[str],
    season: int,
    limit: Optional[int],
    output_path: str,
    dry_run: bool = False,
    force_replace: bool = False,
) -> int:
    """Anchor entity: league listing → squad pages → per-player profiles.

    Writes ``bronze.transfermarkt_players`` with replace-partitions on
    ``(league, season)``.
    """
    from scrapers.base.base_scraper import ReplaceGuardError
    from scrapers.transfermarkt import TransfermarktScraper
    from scrapers.transfermarkt.scraper import R0_2B_FALLBACK_MARKER

    league = leagues[0]
    results = {
        'entity': ENTITY_PLAYERS,
        'tables': [],
        'rows': 0,
        'players_with_rows': 0,
        'fallback': False,
        'fallback_reason': None,
        'errors': [],
    }

    proxy_file = os.environ.get('PROXY_FILE', '/opt/airflow/proxys.txt')
    if not os.path.exists(proxy_file):
        proxy_file = None

    try:
        with TransfermarktScraper(
            leagues=[league],
            seasons=[season],
            proxy_file=proxy_file,
        ) as scraper:
            df = scraper.read_players(
                league=league, season=int(season), limit=limit,
            )
            results['traffic'] = scraper.get_traffic_stats()  # #789
            if df is None or df.empty:
                reason = _classify_fallback(scraper)
                logger.error(
                    "%s: players unavailable — reason=%s",
                    R0_2B_FALLBACK_MARKER, reason,
                )
                results['fallback'] = True
                results['fallback_reason'] = reason
                results['errors'].append(f'{R0_2B_FALLBACK_MARKER}: {reason}')
                _write_results(output_path, results)
                return _fallback_exit_code(results['fallback_reason'])

            results['rows'] = int(len(df))
            results['players_with_rows'] = int(df['player_id'].nunique())

            if dry_run:
                results['dry_run'] = True
                logger.info(
                    "Dry-run: scraped %d player rows (%d unique) — skipping save.",
                    results['rows'], results['players_with_rows'],
                )
                _write_results(output_path, results)
                return 0

            table_path = scraper.save_to_iceberg(
                df=df,
                table_name='transfermarkt_players',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
                min_replace_ratio=(None if force_replace else _MIN_REPLACE_RATIO),
                replace_guard_key='player_id',
            )
            results['tables'].append(table_path)
            logger.info(
                "Saved %d player rows (%d unique) → %s",
                results['rows'], results['players_with_rows'], table_path,
            )
    except ReplaceGuardError as e:
        msg = f"{REPLACE_GUARD_MARKER}: {e}"
        logger.error(msg)
        results['errors'].append(msg)
        _write_results(output_path, results)
        return 3
    except Exception as e:
        logger.error("players scrape failed hard: %s", e, exc_info=True)
        results['errors'].append(str(e))
        _write_results(output_path, results)
        return 1

    _write_results(output_path, results)
    return 0


def _run_mv_history(
    leagues: List[str],
    season: int,
    limit: Optional[int],
    output_path: str,
    dry_run: bool = False,
    force_replace: bool = False,
    window_offset: int = 0,
) -> int:
    """Per-player MV timeline via the ceapi JSON endpoint.

    Depends on a fresh ``bronze.transfermarkt_players``; if that table is
    empty, the resolver returns ``[]`` and we emit TM_FALLBACK exit 2.
    ``window_offset`` rotates which ``limit``-sized roster slice is scraped
    (#620).
    """
    from scrapers.base.base_scraper import ReplaceGuardError
    from scrapers.transfermarkt import TransfermarktScraper
    from scrapers.transfermarkt.scraper import R0_2B_FALLBACK_MARKER

    league = leagues[0]
    results = {
        'entity': ENTITY_MV_HISTORY,
        'tables': [],
        'rows': 0,
        'players_with_rows': 0,
        'fallback': False,
        'fallback_reason': None,
        'errors': [],
    }

    proxy_file = os.environ.get('PROXY_FILE', '/opt/airflow/proxys.txt')
    if not os.path.exists(proxy_file):
        proxy_file = None

    try:
        with TransfermarktScraper(
            leagues=[league],
            seasons=[season],
            proxy_file=proxy_file,
        ) as scraper:
            df = scraper.read_market_value_history(
                league=league, season=int(season), limit=limit,
                window_offset=window_offset,
            )
            results['traffic'] = scraper.get_traffic_stats()  # #789
            if df is None or df.empty:
                reason = _classify_fallback(scraper)
                logger.error(
                    "%s: market_value_history unavailable — reason=%s",
                    R0_2B_FALLBACK_MARKER, reason,
                )
                results['fallback'] = True
                results['fallback_reason'] = reason
                results['errors'].append(f'{R0_2B_FALLBACK_MARKER}: {reason}')
                _write_results(output_path, results)
                return _fallback_exit_code(results['fallback_reason'])

            results['rows'] = int(len(df))
            results['players_with_rows'] = int(df['player_id'].nunique())

            if dry_run:
                results['dry_run'] = True
                logger.info(
                    "Dry-run: scraped %d MV history rows for %d players — "
                    "skipping save.",
                    results['rows'], results['players_with_rows'],
                )
                _write_results(output_path, results)
                return 0

            table_path = scraper.save_to_iceberg(
                df=df,
                table_name='transfermarkt_market_value_history',
                partition_cols=['league', 'season'],
                # Upsert by player (#620): delete+reinsert ONLY the scraped
                # window's players so prior windows accumulate. The guard stays
                # armed but, scoped per-window, is now structurally satisfied
                # (new >= existing) — it cannot shrink other windows, so
                # rotation never trips it. Partial-scrape protection lives
                # upstream in read_* (ConsecutiveFailureError/PartialScrapeError).
                replace_partitions=['league', 'season', 'player_id'],
                min_replace_ratio=(None if force_replace else _MIN_REPLACE_RATIO),
                replace_guard_key='player_id',
            )
            results['tables'].append(table_path)
            logger.info(
                "Saved %d MV history rows for %d players → %s",
                results['rows'], results['players_with_rows'], table_path,
            )
    except ReplaceGuardError as e:
        msg = f"{REPLACE_GUARD_MARKER}: {e}"
        logger.error(msg)
        results['errors'].append(msg)
        _write_results(output_path, results)
        return 3
    except Exception as e:
        logger.error("mv_history scrape failed hard: %s", e, exc_info=True)
        results['errors'].append(str(e))
        _write_results(output_path, results)
        return 1

    _write_results(output_path, results)
    return 0


def _run_transfers(
    leagues: List[str],
    season: int,
    limit: Optional[int],
    output_path: str,
    dry_run: bool = False,
    force_replace: bool = False,
    window_offset: int = 0,
) -> int:
    """Per-player transfers via the ceapi JSON endpoint. Same dependency
    contract as ``_run_mv_history`` (rotating window via ``window_offset``, #620).
    """
    from scrapers.base.base_scraper import ReplaceGuardError
    from scrapers.transfermarkt import TransfermarktScraper
    from scrapers.transfermarkt.scraper import R0_2B_FALLBACK_MARKER

    league = leagues[0]
    results = {
        'entity': ENTITY_TRANSFERS,
        'tables': [],
        'rows': 0,
        'players_with_rows': 0,
        'fallback': False,
        'fallback_reason': None,
        'errors': [],
    }

    proxy_file = os.environ.get('PROXY_FILE', '/opt/airflow/proxys.txt')
    if not os.path.exists(proxy_file):
        proxy_file = None

    try:
        with TransfermarktScraper(
            leagues=[league],
            seasons=[season],
            proxy_file=proxy_file,
        ) as scraper:
            df = scraper.read_transfers(
                league=league, season=int(season), limit=limit,
                window_offset=window_offset,
            )
            results['traffic'] = scraper.get_traffic_stats()  # #789
            if df is None or df.empty:
                reason = _classify_fallback(scraper)
                logger.error(
                    "%s: transfers unavailable — reason=%s",
                    R0_2B_FALLBACK_MARKER, reason,
                )
                results['fallback'] = True
                results['fallback_reason'] = reason
                results['errors'].append(f'{R0_2B_FALLBACK_MARKER}: {reason}')
                _write_results(output_path, results)
                return _fallback_exit_code(results['fallback_reason'])

            results['rows'] = int(len(df))
            results['players_with_rows'] = int(df['player_id'].nunique())

            if dry_run:
                results['dry_run'] = True
                logger.info(
                    "Dry-run: scraped %d transfer rows for %d players — "
                    "skipping save.",
                    results['rows'], results['players_with_rows'],
                )
                _write_results(output_path, results)
                return 0

            table_path = scraper.save_to_iceberg(
                df=df,
                table_name='transfermarkt_transfers',
                partition_cols=['league', 'season'],
                # Upsert by player (#620): see _run_mv_history for the rationale —
                # delete+reinsert only the scraped window so prior windows
                # accumulate; the per-window guard cannot trip on rotation.
                replace_partitions=['league', 'season', 'player_id'],
                min_replace_ratio=(None if force_replace else _MIN_REPLACE_RATIO),
                replace_guard_key='player_id',
            )
            results['tables'].append(table_path)
            logger.info(
                "Saved %d transfer rows for %d players → %s",
                results['rows'], results['players_with_rows'], table_path,
            )
    except ReplaceGuardError as e:
        msg = f"{REPLACE_GUARD_MARKER}: {e}"
        logger.error(msg)
        results['errors'].append(msg)
        _write_results(output_path, results)
        return 3
    except Exception as e:
        logger.error("transfers scrape failed hard: %s", e, exc_info=True)
        results['errors'].append(str(e))
        _write_results(output_path, results)
        return 1

    _write_results(output_path, results)
    return 0


def _run_coaches(
    leagues: List[str],
    season: int,
    limit: Optional[int],
    output_path: str,
    dry_run: bool = False,
    force_replace: bool = False,
) -> int:
    """Head-coach entity (issue #434): listing → staff pages → coach profiles.

    Writes ``bronze.transfermarkt_coaches`` with replace-partitions on
    ``(league, season)``. Feeds gold.dim_manager nationality/dob enrichment.
    """
    from scrapers.base.base_scraper import ReplaceGuardError
    from scrapers.transfermarkt import TransfermarktScraper
    from scrapers.transfermarkt.scraper import R0_2B_FALLBACK_MARKER

    league = leagues[0]
    results = {
        'entity': ENTITY_COACHES,
        'tables': [],
        'rows': 0,
        'coaches_with_rows': 0,
        'fallback': False,
        'fallback_reason': None,
        'errors': [],
    }

    proxy_file = os.environ.get('PROXY_FILE', '/opt/airflow/proxys.txt')
    if not os.path.exists(proxy_file):
        proxy_file = None

    try:
        with TransfermarktScraper(
            leagues=[league],
            seasons=[season],
            proxy_file=proxy_file,
        ) as scraper:
            df = scraper.read_coaches(
                league=league, season=int(season), limit=limit,
            )
            results['traffic'] = scraper.get_traffic_stats()  # #789
            if df is None or df.empty:
                reason = _classify_fallback(scraper)
                logger.error(
                    "%s: coaches unavailable — reason=%s",
                    R0_2B_FALLBACK_MARKER, reason,
                )
                results['fallback'] = True
                results['fallback_reason'] = reason
                results['errors'].append(f'{R0_2B_FALLBACK_MARKER}: {reason}')
                _write_results(output_path, results)
                return _fallback_exit_code(results['fallback_reason'])

            results['rows'] = int(len(df))
            results['coaches_with_rows'] = int(df['coach_id'].nunique())

            if dry_run:
                results['dry_run'] = True
                logger.info(
                    "Dry-run: scraped %d coach rows (%d unique) — skipping save.",
                    results['rows'], results['coaches_with_rows'],
                )
                _write_results(output_path, results)
                return 0

            table_path = scraper.save_to_iceberg(
                df=df,
                table_name='transfermarkt_coaches',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
                min_replace_ratio=(None if force_replace else _MIN_REPLACE_RATIO),
                replace_guard_key='coach_id',
            )
            results['tables'].append(table_path)
            logger.info(
                "Saved %d coach rows (%d unique) → %s",
                results['rows'], results['coaches_with_rows'], table_path,
            )
    except ReplaceGuardError as e:
        msg = f"{REPLACE_GUARD_MARKER}: {e}"
        logger.error(msg)
        results['errors'].append(msg)
        _write_results(output_path, results)
        return 3
    except Exception as e:
        logger.error("coaches scrape failed hard: %s", e, exc_info=True)
        results['errors'].append(str(e))
        _write_results(output_path, results)
        return 1

    _write_results(output_path, results)
    return 0


def main() -> int:
    parser = _StrictArgumentParser(description='Run Transfermarkt Bronze scraper')
    parser.add_argument(
        '--entity',
        type=str,
        default=ENTITY_PLAYERS,
        help=f"Which entity to scrape. One of: {sorted(VALID_ENTITIES)}",
    )
    parser.add_argument(
        '--league',
        type=str,
        default='ENG-Premier League',
        help='League key (default: ENG-Premier League)',
    )
    parser.add_argument(
        '--season',
        type=int,
        default=2025,
        help='Season year (e.g. 2025 for 25-26)',
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Smoke-test cap on number of players',
    )
    parser.add_argument(
        '--output',
        type=str,
        default='/tmp/transfermarkt_result.json',
        help='Output JSON file path (also printed to stdout)',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Scrape and report rows without saving to Iceberg (smoke runs)',
    )
    parser.add_argument(
        '--force-replace',
        action='store_true',
        help='Bypass the completeness guard and replace the partition '
             'unconditionally (operator recovery, never used by the DAG)',
    )
    parser.add_argument(
        '--as-of-date',
        type=str,
        default=None,
        help="Logical run date 'YYYY-MM-DD' (Airflow {{ ds }}) used to rotate "
             'the transfers/mv_history roster window (#620). None → today.',
    )
    try:
        args = parser.parse_args()
    except _ArgparseError as exc:
        logger.error("Invalid CLI arguments: %s — failing hard (not TM_FALLBACK)", exc)
        return 1

    entity = args.entity.lower()
    if entity not in VALID_ENTITIES:
        logger.error(
            "Invalid --entity %s. Must be one of %s.", entity, sorted(VALID_ENTITIES),
        )
        return 1

    leagues = [args.league]
    window_offset = _window_offset(args.as_of_date)
    logger.info(
        "Starting Transfermarkt scraper: entity=%s league=%s season=%s limit=%s "
        "window_offset=%s",
        entity, leagues, args.season, args.limit, window_offset,
    )

    if entity == ENTITY_PLAYERS:
        return _run_players(
            leagues, args.season, args.limit, args.output,
            args.dry_run, args.force_replace,
        )
    if entity == ENTITY_MV_HISTORY:
        return _run_mv_history(
            leagues, args.season, args.limit, args.output,
            args.dry_run, args.force_replace, window_offset,
        )
    if entity == ENTITY_TRANSFERS:
        return _run_transfers(
            leagues, args.season, args.limit, args.output,
            args.dry_run, args.force_replace, window_offset,
        )
    if entity == ENTITY_COACHES:
        return _run_coaches(
            leagues, args.season, args.limit, args.output,
            args.dry_run, args.force_replace,
        )
    return 1


if __name__ == '__main__':
    sys.exit(main())
