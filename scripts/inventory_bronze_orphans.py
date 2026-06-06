#!/usr/bin/env python3
"""Inventory + remove orphan tables in ``iceberg.bronze.*``.

An *orphan* is a live ``iceberg.bronze.*`` table that no active producer
(scraper / DAG / backfill) writes anymore — leftover from a rename, a dead
experiment, or a retired stat category (e.g. the FBref ``passing`` / ``gca`` /
``defense`` stat_types removed in ``dags/dag_ingest_fbref.py``; see the static
precedent ``scripts/drop_empty_fbref_tables.sql``).

KEEP-set is *derived*, not re-hardcoded: it reuses the existing parser contract
``scripts/audit_bronze_columns.py::EXPECTED_TABLES`` (source -> table -> columns)
plus a small documented set of producer-only tables that live outside the
contract. Anything live but not in KEEP is an orphan candidate.

Usage (inside the airflow-webserver container):

    # dry-run: print KEEP / ORPHAN report with row counts, drop nothing
    python /opt/airflow/scripts/inventory_bronze_orphans.py

    # actually DROP confirmed orphans (only after the report has been reviewed)
    python /opt/airflow/scripts/inventory_bronze_orphans.py --drop

Trino DDL footgun: every DROP is followed by ``fetchall()`` (via
``utils.silver_tasks._execute``), otherwise ``cursor.close()`` sends DELETE ->
``USER_CANCELED``. Uses ``utils.silver_tasks`` (lightweight: trino only) to skip
the heavy ``scrapers/__init__.py``.
"""
from __future__ import annotations

import argparse
import logging
import sys

sys.path.insert(0, '/opt/airflow/dags')
from utils.silver_tasks import _execute, _get_trino_connection  # noqa: E402

# ``audit_bronze_columns`` sits in the same scripts/ dir; it is on sys.path[0]
# when this file runs as a script. Import the contract — don't copy 63 names.
from audit_bronze_columns import EXPECTED_TABLES  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('inventory_bronze_orphans')

# Producer-only tables that have an ACTIVE writer but are NOT in the parser
# contract (EXPECTED_TABLES). Dropping these would delete live data.
#   - espn_standings: written by scrapers/espn/scraper.py; the ESPN contract
#     only covers espn_schedule / espn_lineup / espn_matchsheet.
EXTRA_PRODUCED: set[str] = {
    'espn_standings',
}

# Producer-orphans (no active writer) that MUST NOT be dropped because live
# Silver SQL still READS them. Dropping would break Silver with TABLE_NOT_FOUND.
# Each entry is blocked behind the issue that migrates the reader off it.
#   - (empty) matchhistory_games was migrated off by #307 — all four Silver
#     consumers (xref_match / xref_team / xref_referee / matchhistory_match_odds)
#     now read matchhistory_results, so games is a plain droppable orphan.
BLOCKED_ORPHANS: dict[str, str] = {}


def build_keep_set() -> set[str]:
    """KEEP = every table in the parser contract + documented producer-only tables."""
    keep: set[str] = set(EXTRA_PRODUCED)
    for _source, tables in EXPECTED_TABLES.items():
        keep.update(tables.keys())
    return keep


def find_orphans(live: list[str], keep: set[str]) -> list[str]:
    """Live bronze tables with no known producer = orphan candidates."""
    return sorted(t for t in live if t not in keep)


def classify_orphans(orphans: list[str]) -> tuple[list[str], list[str]]:
    """Split orphans into (droppable, blocked). Blocked have live Silver readers."""
    droppable = [t for t in orphans if t not in BLOCKED_ORPHANS]
    blocked = [t for t in orphans if t in BLOCKED_ORPHANS]
    return droppable, blocked


def list_live_tables(conn) -> list[str]:
    rows = _execute(
        conn,
        "SELECT table_name FROM iceberg.information_schema.tables "
        "WHERE table_schema = 'bronze' ORDER BY table_name",
        fetch=True,
    )
    return [r[0] for r in (rows or [])]


def row_count(conn, table: str) -> int:
    rows = _execute(conn, f"SELECT COUNT(*) FROM iceberg.bronze.{table}", fetch=True)
    return int(rows[0][0]) if rows else 0


def drop_table(conn, table: str) -> None:
    # fetchall() inside _execute closes the USER_CANCELED footgun.
    _execute(conn, f"DROP TABLE IF EXISTS iceberg.bronze.{table}")
    logger.info("  dropped iceberg.bronze.%s", table)


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Inventory and (optionally) drop orphan iceberg.bronze tables.'
    )
    parser.add_argument(
        '--drop', action='store_true',
        help='Actually DROP confirmed orphans (default: dry-run report only).',
    )
    args = parser.parse_args()

    keep = build_keep_set()
    conn = _get_trino_connection()
    try:
        live = list_live_tables(conn)
        orphans = find_orphans(live, keep)
        droppable, blocked = classify_orphans(orphans)
        missing = sorted(keep - set(live))  # contract tables not materialised — FYI only

        logger.info("Live bronze tables : %d", len(live))
        logger.info("KEEP-set size      : %d", len(keep))
        logger.info("Droppable orphans  : %d", len(droppable))
        logger.info("Blocked orphans    : %d", len(blocked))

        if droppable:
            logger.info("---- DROPPABLE ORPHANS (live, no producer, no reader) ----")
            for t in droppable:
                logger.info("  %-44s rows=%d", t, row_count(conn, t))
        if blocked:
            logger.info("---- BLOCKED ORPHANS (live reader — do NOT drop) ----")
            for t in blocked:
                logger.info("  %-44s rows=%-7d (%s)", t, row_count(conn, t), BLOCKED_ORPHANS[t])
        if missing:
            logger.info("---- contract tables NOT live (FYI, no action) ----")
            for t in missing:
                logger.info("  %s", t)

        if not args.drop:
            logger.info("DRY-RUN: nothing dropped. Re-run with --drop after the list is reviewed.")
            return 0

        if not droppable:
            logger.info("No droppable orphans.")
            return 0

        logger.info("==== DROPPING %d orphan(s) ====", len(droppable))
        for t in droppable:
            drop_table(conn, t)

        # Re-inventory: confirm zero droppable orphans remain (Definition of Done).
        droppable_after, _ = classify_orphans(find_orphans(list_live_tables(conn), keep))
        logger.info("Post-drop droppable orphans: %d", len(droppable_after))
        if droppable_after:
            logger.error("Still orphaned after drop: %s", droppable_after)
            return 1
        logger.info("OK — zero droppable orphans remain.")
        return 0
    finally:
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())
