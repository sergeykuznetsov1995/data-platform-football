#!/usr/bin/env python3
"""
Capology Scraper Runner Script
==============================

Standalone runner invoked from Airflow via BashOperator. One entity:

- ``player_salaries`` : per-(player, club) salary snapshot, one HTTP call
                       per (league, season, currency) tuple.

MVP currency scope: GBP only (issue #43). EUR/USD lift is a separate issue.

Exit codes:
    0 — scrape completed successfully (>= 1 row written)
    1 — hard failure (exception raised; or a CLI parse error — unknown/typo'd
        flag, invalid value — #512, kept off exit 2 so the DAG wrapper does
        not mistake it for a CAPOLOGY_FALLBACK soft-success)
    2 — graceful CAPOLOGY_FALLBACK: HTML fetch failed (CF block / 5xx) or
        the inline ``var data`` array couldn't be sliced. DataFrame empty,
        nothing written; DAG wraps exit 2 → exit 0 for validate_data.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import warnings
from typing import Optional

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
    The DAG bash wrapper maps exit 2 to CAPOLOGY_FALLBACK soft-success, so a
    flag typo would silently no-op the task (#512). Funnel every parse error
    through a catchable exception → main() returns hard-failure exit 1.
    """

    def error(self, message):
        self.print_usage(sys.stderr)
        raise _ArgparseError(message)


ENTITY_PLAYER_SALARIES = 'player_salaries'
ENTITY_TEAM_PAYROLLS = 'team_payrolls'
ENTITY_CONTRACT_EXTENSIONS = 'contract_extensions'
ENTITY_TRANSFER_WINDOW = 'transfer_window'

# Club/contract products share one fetch→parse→save shape (partition by
# league+season, all 3 currencies inline). entity → (read method, bronze
# table, the column counted into `units` for the result JSON).
PRODUCTS = {
    ENTITY_TEAM_PAYROLLS: (
        'read_team_payrolls', 'capology_team_payrolls', 'club_slug',
    ),
    ENTITY_CONTRACT_EXTENSIONS: (
        'read_contract_extensions', 'capology_contract_extensions', 'player_slug',
    ),
    ENTITY_TRANSFER_WINDOW: (
        'read_transfer_window', 'capology_transfer_window', 'club_slug',
    ),
}

VALID_ENTITIES = {ENTITY_PLAYER_SALARIES, *PRODUCTS}

# Replace-partitions completeness guard (#513 → #583): refuse a save that would
# shrink a bronze.capology_* partition below this share of its existing rows, so
# a partial/failed scrape can't wipe a good partition. COUNT(*) (no
# replace_guard_key) — each partition tuple is scraped full-state.
# ReplaceGuardError → exit 3; bypass with --force-replace.
_MIN_REPLACE_RATIO = 0.9
REPLACE_GUARD_MARKER = 'CAPOLOGY_REPLACE_GUARD'


def _write_results(path: str, payload: dict) -> None:
    try:
        with open(path, 'w') as f:
            json.dump(payload, f, default=str)
    except Exception as e:
        logger.warning("Could not write results to %s: %s", path, e)
    try:
        print(json.dumps(payload, default=str))
    except Exception:
        pass


def _classify_fallback(scraper) -> str:
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
    (empty page, NO http error → ``empty_payload``) stays exit 2, mapped to a
    soft green by the wrapper. (#790)
    """
    if reason and (reason.startswith('http_') or reason == 'transport_error'):
        return 1
    return 2


def _run_player_salaries(
    league: str,
    season: int,
    currency: str,
    limit: Optional[int],
    output_path: str,
    force_replace: bool = False,
) -> int:
    from scrapers.base.base_scraper import ReplaceGuardError
    from scrapers.capology import CapologyScraper
    from scrapers.capology.scraper import R0_2B_FALLBACK_MARKER

    results = {
        'entity': ENTITY_PLAYER_SALARIES,
        'tables': [],
        'rows': 0,
        'players_with_rows': 0,
        'currency': currency.upper(),
        'fallback': False,
        'fallback_reason': None,
        'errors': [],
    }

    # Capology cold-path tls_requests works without a proxy (probe 0.2);
    # an optional proxy_file is still honoured if the operator wants it.
    proxy_file = os.environ.get('CAPOLOGY_PROXY_FILE')
    if proxy_file and not os.path.exists(proxy_file):
        proxy_file = None

    try:
        with CapologyScraper(
            leagues=[league],
            seasons=[season],
            currency=currency,
            proxy_file=proxy_file,
        ) as scraper:
            df = scraper.read_player_salaries(
                league=league, season=int(season), currency=currency, limit=limit,
            )
            if df is None or df.empty:
                reason = _classify_fallback(scraper)
                logger.error(
                    "%s: player_salaries unavailable — reason=%s",
                    R0_2B_FALLBACK_MARKER, reason,
                )
                results['fallback'] = True
                results['fallback_reason'] = reason
                results['errors'].append(f'{R0_2B_FALLBACK_MARKER}: {reason}')
                _write_results(output_path, results)
                return _fallback_exit_code(results['fallback_reason'])

            table_path = scraper.save_to_iceberg(
                df=df,
                table_name='capology_player_salaries',
                partition_cols=['league', 'season', 'currency'],
                replace_partitions=['league', 'season', 'currency'],
                min_replace_ratio=(
                    None if force_replace else _MIN_REPLACE_RATIO
                ),
            )
            results['tables'].append(table_path)
            results['rows'] = int(len(df))
            results['players_with_rows'] = int(df['player_slug'].nunique())
            logger.info(
                "Saved %d salary rows (%d unique players) → %s",
                results['rows'], results['players_with_rows'], table_path,
            )
    except ReplaceGuardError as e:
        # Guard refused the save (partial scrape would shrink the partition) —
        # nothing written. Distinct exit 3 so an operator can tell a refused
        # guard from a hard failure (1) or a fallback (2) (#583).
        msg = f"{REPLACE_GUARD_MARKER}: {e}"
        logger.error(msg)
        results['errors'].append(msg)
        _write_results(output_path, results)
        return 3
    except Exception as e:
        logger.error(
            "player_salaries scrape failed hard: %s", e, exc_info=True,
        )
        results['errors'].append(str(e))
        _write_results(output_path, results)
        return 1

    _write_results(output_path, results)
    return 0


def _run_product(
    entity: str,
    league: str,
    season: int,
    limit: Optional[int],
    output_path: str,
    force_replace: bool = False,
) -> int:
    """Generic runner for the club/contract products (payrolls / contracts /
    transfer-window). Same exit-code contract as _run_player_salaries."""
    from scrapers.base.base_scraper import ReplaceGuardError
    from scrapers.capology import CapologyScraper
    from scrapers.capology.scraper import R0_2B_FALLBACK_MARKER

    method, table_name, unit_col = PRODUCTS[entity]
    results = {
        'entity': entity,
        'tables': [],
        'rows': 0,
        'units': 0,
        'fallback': False,
        'fallback_reason': None,
        'errors': [],
    }

    proxy_file = os.environ.get('CAPOLOGY_PROXY_FILE')
    if proxy_file and not os.path.exists(proxy_file):
        proxy_file = None

    try:
        with CapologyScraper(
            leagues=[league], seasons=[season], proxy_file=proxy_file,
        ) as scraper:
            df = getattr(scraper, method)(
                league=league, season=int(season), limit=limit,
            )
            if df is None or df.empty:
                reason = _classify_fallback(scraper)
                logger.error(
                    "%s: %s unavailable — reason=%s",
                    R0_2B_FALLBACK_MARKER, entity, reason,
                )
                results['fallback'] = True
                results['fallback_reason'] = reason
                results['errors'].append(f'{R0_2B_FALLBACK_MARKER}: {reason}')
                _write_results(output_path, results)
                return _fallback_exit_code(results['fallback_reason'])

            table_path = scraper.save_to_iceberg(
                df=df,
                table_name=table_name,
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
                min_replace_ratio=(
                    None if force_replace else _MIN_REPLACE_RATIO
                ),
            )
            results['tables'].append(table_path)
            results['rows'] = int(len(df))
            if unit_col in df.columns:
                results['units'] = int(df[unit_col].nunique())
            logger.info(
                "Saved %d %s rows (%d unique %s) → %s",
                results['rows'], entity, results['units'], unit_col, table_path,
            )
    except ReplaceGuardError as e:
        # Guard refused the save (partial scrape would shrink the partition) —
        # nothing written. Distinct exit 3 (#583).
        msg = f"{REPLACE_GUARD_MARKER}: {e}"
        logger.error(msg)
        results['errors'].append(msg)
        _write_results(output_path, results)
        return 3
    except Exception as e:
        logger.error("%s scrape failed hard: %s", entity, e, exc_info=True)
        results['errors'].append(str(e))
        _write_results(output_path, results)
        return 1

    _write_results(output_path, results)
    return 0


def main() -> int:
    parser = _StrictArgumentParser(description='Run Capology Bronze scraper')
    parser.add_argument(
        '--entity', type=str, default=ENTITY_PLAYER_SALARIES,
        help=f"Entity to scrape. One of: {sorted(VALID_ENTITIES)}",
    )
    parser.add_argument(
        '--league', type=str, default='ENG-Premier League',
        help='League key (default: ENG-Premier League)',
    )
    parser.add_argument(
        '--season', type=int, default=2024,
        help='Season year (e.g. 2024 for 24-25, Capology URL uses 2024-2025)',
    )
    parser.add_argument(
        '--currency', type=str, default='GBP',
        help='Currency to materialise (MVP: GBP only)',
    )
    parser.add_argument(
        '--limit', type=int, default=None,
        help='Smoke-test cap on number of rows',
    )
    parser.add_argument(
        '--output', type=str,
        default='/tmp/capology_result.json',
        help='Output JSON file path',
    )
    parser.add_argument(
        '--force-replace', action='store_true',
        help='Bypass the completeness guard — write even if the scraped frame '
             'shrinks the existing partition. Use for a deliberate first '
             'backfill or a known legitimate shrink.',
    )
    try:
        args = parser.parse_args()
    except _ArgparseError as exc:
        logger.error("Invalid CLI arguments: %s — failing hard (not CAPOLOGY_FALLBACK)", exc)
        return 1

    entity = args.entity.lower()
    if entity not in VALID_ENTITIES:
        logger.error(
            "Invalid --entity %s. Must be one of %s.", entity, sorted(VALID_ENTITIES),
        )
        return 1

    logger.info(
        "Starting Capology scraper: entity=%s league=%s season=%s currency=%s limit=%s",
        entity, args.league, args.season, args.currency, args.limit,
    )

    if entity == ENTITY_PLAYER_SALARIES:
        return _run_player_salaries(
            league=args.league,
            season=args.season,
            currency=args.currency,
            limit=args.limit,
            output_path=args.output,
            force_replace=args.force_replace,
        )
    if entity in PRODUCTS:
        return _run_product(
            entity=entity,
            league=args.league,
            season=args.season,
            limit=args.limit,
            output_path=args.output,
            force_replace=args.force_replace,
        )
    return 1


if __name__ == '__main__':
    sys.exit(main())
