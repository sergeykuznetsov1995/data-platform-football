"""
Iceberg Maintenance Tasks
=========================

Periodic maintenance for Iceberg tables: expire stale snapshots and remove
orphan files. Without this, delete-then-insert DAGs (e.g. dag_ingest_whoscored)
accumulate thousands of metadata snapshots — `whoscored_events` reached 12K+
files / 26 GB metadata for 49 MB of data before the first sweep.

Trino DOES NOT expose Iceberg table properties like
`write.metadata.delete-after-commit.enabled` or
`write.metadata.previous-versions-max` via SET PROPERTIES /
extra_properties (Iceberg connector blocks `write.metadata.*` keys), so the
only way to keep the warehouse healthy is periodic sweeps from this module.

Uses `_get_trino_connection()` from `silver_tasks` (lightweight `import trino`,
avoids heavy `scrapers/__init__.py`).
"""

from __future__ import annotations

import logging
from typing import Iterable, List, Optional, Tuple

import trino as trino_lib

from utils.silver_tasks import _get_trino_connection

logger = logging.getLogger(__name__)

DEFAULT_SCHEMAS: Tuple[str, ...] = ("bronze", "silver", "gold")
DEFAULT_RETENTION = "7d"

# High-churn tables — daily DAGs do delete-then-insert, so even a 7-day
# retention leaves >14 stale snapshots between weekly sweeps. Run a separate
# daily DAG with retention='3d' against this allowlist.
HIGH_CHURN_BRONZE: Tuple[str, ...] = (
    "clubelo_team_history",
    "whoscored_events",
    "whoscored_missing_players",
    "whoscored_schedule",
    "fbref_match_events",
    "fbref_match_player_stats",
    "fbref_match_team_stats",
    "fbref_lineups",
    "understat_shots",
    "understat_player_match_stats",
    "matchhistory_games",
)


def _row_to_stats(cursor) -> dict:
    """Convert one-row procedure output to {col_name: value}.

    Trino's `EXECUTE remove_orphan_files` returns either:
      - a single row with named columns (scanned_files_count, deleted_files_count, ...)
      - or, depending on procedure, a list of (name, value) pairs.
    Read via `cursor.description` to handle both shapes safely.
    """
    rows = cursor.fetchall()
    if not rows:
        return {}
    cols = [d[0] for d in (cursor.description or [])]
    # Shape A: single row, multi-column
    if len(rows) == 1 and len(cols) == len(rows[0]) and len(cols) > 1:
        return {cols[i]: rows[0][i] for i in range(len(cols))}
    # Shape B: list of (name, value) pairs (legacy / different procedures)
    if all(len(r) == 2 for r in rows):
        return {r[0]: r[1] for r in rows}
    # Fallback — return as-is dict by column zero
    return {f"row_{i}": r for i, r in enumerate(rows)}


def _list_tables(conn, schema: str) -> List[str]:
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW TABLES FROM iceberg.{schema}")
        rows = cur.fetchall()
    finally:
        cur.close()
    return [r[0] for r in rows]


def _exec_alter(conn, sql: str) -> dict:
    """Execute ALTER TABLE ... EXECUTE ... and return parsed stats."""
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return _row_to_stats(cur)
    finally:
        cur.close()


def _maintain_one(conn, fq: str, retention_threshold: str) -> dict:
    """Run expire_snapshots + remove_orphan_files on a single table.

    Returns parsed stats from remove_orphan_files (deleted_files_count etc.).
    """
    _exec_alter(
        conn,
        f"ALTER TABLE {fq} EXECUTE expire_snapshots(retention_threshold => '{retention_threshold}')",
    )
    return _exec_alter(
        conn,
        f"ALTER TABLE {fq} EXECUTE remove_orphan_files(retention_threshold => '{retention_threshold}')",
    )


def maintain_iceberg_tables(
    schemas: Tuple[str, ...] = DEFAULT_SCHEMAS,
    retention_threshold: str = DEFAULT_RETENTION,
    table_filter: Optional[Iterable[str]] = None,
) -> dict:
    """Run expire_snapshots + remove_orphan_files on every table in `schemas`.

    Args:
        schemas: which Iceberg schemas to walk (default bronze/silver/gold).
        retention_threshold: '7d' for weekly, '3d' for daily high-churn.
            Requires `iceberg.{expire-snapshots,remove-orphan-files}.min-retention`
            in `configs/trino/catalog/iceberg.properties` to allow it.
        table_filter: if set, only tables whose short name is in this set
            are processed (used by the daily high-churn DAG).
    """
    conn = _get_trino_connection()
    total_tables = 0
    total_deleted = 0
    total_scanned = 0
    failures: List[Tuple[str, str]] = []
    filter_set = set(table_filter) if table_filter else None

    for schema in schemas:
        try:
            tables = _list_tables(conn, schema)
        except Exception as e:
            logger.error("Failed to list tables in iceberg.%s: %s", schema, e)
            failures.append((f"iceberg.{schema}", str(e)[:300]))
            # Trino may have dropped the connection — re-open for next schema.
            try:
                conn.close()
            except Exception:
                pass
            conn = _get_trino_connection()
            continue

        for tn in tables:
            if filter_set is not None and tn not in filter_set:
                continue
            fq = f"iceberg.{schema}.{tn}"
            total_tables += 1
            try:
                stats = _maintain_one(conn, fq, retention_threshold)
                deleted = int(stats.get("deleted_files_count", 0) or 0)
                scanned = int(stats.get("scanned_files_count", 0) or 0)
                total_deleted += deleted
                total_scanned += scanned
                if deleted > 0:
                    logger.info("%s: scanned=%d deleted=%d", fq, scanned, deleted)
            except trino_lib.exceptions.TrinoConnectionError as e:
                logger.warning("Connection lost on %s, reconnecting: %s", fq, e)
                failures.append((fq, f"connection: {e}"[:300]))
                try:
                    conn.close()
                except Exception:
                    pass
                conn = _get_trino_connection()
            except Exception as e:
                logger.error("Maintenance failed on %s: %s", fq, e)
                failures.append((fq, str(e)[:300]))

    logger.info(
        "Iceberg maintenance done: tables=%d scanned=%d deleted=%d failures=%d",
        total_tables, total_scanned, total_deleted, len(failures),
    )
    for fq, err in failures:
        logger.warning("  FAIL %s: %s", fq, err)

    try:
        conn.close()
    except Exception:
        pass

    return {
        "tables_processed": total_tables,
        "files_scanned": total_scanned,
        "files_deleted": total_deleted,
        "failures": failures,
    }
