#!/usr/bin/env python3
"""
ClubElo Scraper Runner Script
==============================

Standalone runner of the ClubElo HTML collection (the API is dead, #1459).
Called from Airflow via BashOperator to avoid memory issues with PythonOperator.

- ``--mode daily`` (#1463): /Ranking + /Results → ``bronze.clubelo_rank_snapshot``
  (replace by rating_date) and ``bronze.clubelo_result`` (MERGE), plus up to
  10 new club pages into the history tables;
- ``--mode history`` (#1462): club pages /{slug} into the four append-only
  history tables, resumable batches.

Daily/history exit non-zero on ANY incompleteness. ``--mode watch`` (#1466)
reports optional weekly checks without making daily collection red.
"""

import argparse
import json
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def run_daily_mode(args) -> int:
    """--mode daily (#1463): red on a failed check, guard, date mismatch or new-slug page."""
    try:
        from scrapers.clubelo.daily import exit_code, run_default

        results = run_default()
        code = exit_code(results)
    except Exception as e:
        logger.error(f"ClubElo daily failed: {e}", exc_info=True)
        results = {'error': f'{type(e).__name__}: {e}'}
        code = 1

    with open(args.output, 'w') as f:
        json.dump(results, f)
    print(json.dumps(results))  # Also print for Airflow logs
    logger.info(f"ClubElo daily complete, exit {code}")
    return code


def run_history_mode(args) -> int:
    """--mode history (#1462): red (non-zero) on ANY incompleteness.

    Failed pages, redirects, a block by the site, pending clubs left or an
    error all give exit 1 — a partial history must never look green (R-01).
    """
    try:
        from scrapers.clubelo.history import exit_code, run_default

        results = run_default(batch_size=args.batch_size)
        code = exit_code(results)
    except Exception as e:
        logger.error(f"ClubElo history failed: {e}", exc_info=True)
        results = {'error': f'{type(e).__name__}: {e}'}
        code = 1

    with open(args.output, 'w') as f:
        json.dump(results, f)
    print(json.dumps(results))  # Also print for Airflow logs
    logger.info(f"ClubElo history complete, exit {code}")
    return code


def run_watch_mode(args) -> int:
    """The optional check reports errors; its exit must not fail daily."""
    try:
        from scrapers.clubelo.watch import run_default

        results = run_default(state_file=args.state_file, source=args.source)
    except Exception as exc:
        logger.warning("ClubElo watch failed", exc_info=True)
        results = {'status': 'error', 'errors': [f'{type(exc).__name__}: {exc}']}
    print(json.dumps(results))
    try:
        with open(args.output, 'w') as stream:
            json.dump(results, stream)
    except Exception:
        logger.warning("ClubElo watch result file unavailable", exc_info=True)
    return 0


def main():
    parser = argparse.ArgumentParser(description='Run ClubElo scraper')
    parser.add_argument(
        '--output',
        type=str,
        required=True,
        help='Output file for the result JSON (one per DAG run)'
    )
    parser.add_argument(
        '--mode',
        choices=['daily', 'history', 'watch'],
        default='daily',
        help="daily = /Ranking + /Results snapshot of clubelo.com (#1463); "
             "history = club pages /{slug} of clubelo.com into the four "
             "append-only history tables (#1462), resumable batches; "
             "watch = weekly registration and Fixtures observations (#1466)"
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=200,
        help='--mode history: clubs per committed batch (default 200)'
    )
    parser.add_argument('--state-file', default='/opt/airflow/logs/clubelo_registration_watch.json',
                        help='--mode watch: persistent dedup state on the logs volume')
    parser.add_argument('--source', choices=['html', 'api'], default='html',
                        help='--mode watch: same source switch as the collection; api remains unimplemented')
    args = parser.parse_args()

    if args.mode == 'watch':
        return run_watch_mode(args)
    if args.mode == 'history':
        return run_history_mode(args)
    return run_daily_mode(args)


if __name__ == '__main__':
    sys.exit(main())
