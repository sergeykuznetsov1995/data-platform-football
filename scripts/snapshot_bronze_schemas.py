"""Snapshot Bronze column schemas referenced by Silver SQL.

Walks every Silver SQL (`dags/sql/silver/*.sql` + `*.sql.j2`), collects every
`iceberg.bronze.<table>` reference, then SHOW COLUMNS via Trino for each and
writes the result to ``tests/fixtures/bronze_schemas.json`` with a stable
(sorted) key order so re-runs produce no diff when Bronze hasn't changed.

Run from inside the Airflow webserver container (where Trino env vars and
network access are set up correctly):

    docker compose exec airflow-webserver \\
        python /opt/airflow/scripts/snapshot_bronze_schemas.py

Closes #71 followup: this fixture is consumed by
``tests/unit/sql/test_silver_bronze_column_alignment.py``.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import trino as trino_lib

# Make the test helper importable without installing it as a package.
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "tests" / "unit" / "sql"))

from _bronze_alignment import collect_all_bronze_tables  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("snapshot_bronze_schemas")

_FIXTURE_PATH = _REPO_ROOT / "tests" / "fixtures" / "bronze_schemas.json"


def _connect() -> trino_lib.dbapi.Connection:
    """Same connection idiom as dags/utils/silver_tasks.py:_get_trino_connection."""
    host = os.environ.get("TRINO_HOST", "trino")
    user = os.environ.get("TRINO_USER", "airflow")
    password = os.environ.get("TRINO_PASSWORD")
    if password:
        port = int(os.environ.get("TRINO_PORT", 8443))
        return trino_lib.dbapi.connect(
            host=host,
            port=port,
            user=user,
            catalog="iceberg",
            http_scheme="https",
            auth=trino_lib.auth.BasicAuthentication(user, password),
            verify=False,
        )
    port = int(os.environ.get("TRINO_PORT", 8080))
    logger.info("TRINO_PASSWORD not set, connecting via HTTP (no auth)")
    return trino_lib.dbapi.connect(host=host, port=port, user=user, catalog="iceberg")


def _show_columns(conn, table: str) -> dict[str, str]:
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW COLUMNS FROM iceberg.bronze.{table}")
        rows = cursor.fetchall()
    finally:
        cursor.close()
    # SHOW COLUMNS returns (column, type, extra, comment)
    return {row[0]: row[1] for row in rows}


def main() -> int:
    tables = sorted(collect_all_bronze_tables())
    logger.info("Bronze tables referenced by Silver SQL: %d", len(tables))

    conn = _connect()
    missing: list[str] = []
    try:
        snapshot: dict[str, dict] = {}
        for table in tables:
            try:
                cols = _show_columns(conn, table)
            except trino_lib.exceptions.TrinoUserError as e:
                # Some Silver SQL references Bronze that was deprecated/never
                # materialised (e.g. fbref_shot_events — FBref Feb 2026
                # restriction). Record as ``missing_tables`` so the alignment
                # test can xfail-skip those references with a clear message.
                if e.error_name == "TABLE_NOT_FOUND":
                    logger.warning("Bronze table missing in Trino: %s", table)
                    missing.append(table)
                    continue
                raise
            logger.info("  %s: %d columns", table, len(cols))
            # Stable column order — sort by name so JSON diffs stay clean.
            snapshot[f"bronze.{table}"] = {
                "columns": {k: cols[k] for k in sorted(cols)},
            }
    finally:
        conn.close()

    _FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "snapshot_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "missing_tables": sorted(missing),
        "tables": {k: snapshot[k] for k in sorted(snapshot)},
    }

    # Idempotency: if the existing snapshot differs only in ``snapshot_at``,
    # keep the old timestamp so a no-op re-run yields no git diff.
    if _FIXTURE_PATH.exists():
        try:
            old = json.loads(_FIXTURE_PATH.read_text())
            if old.get("tables") == payload["tables"]:
                payload["snapshot_at"] = old.get("snapshot_at", payload["snapshot_at"])
                logger.info("No schema changes — preserving existing snapshot_at")
        except json.JSONDecodeError:
            pass

    _FIXTURE_PATH.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n")
    logger.info("Wrote %s", _FIXTURE_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
