"""ESPN request journal in ``iceberg.ops`` (#1500).

One row per ``RequestLedgerEntry``: address, status, attempts, bytes,
encoding, latency, pace step and lane, so a blocked origin or a slow step is
visible in Trino.  The DAG side (``ensure_journal_table`` once, then
``flush_journal`` per batch) is wired in #1504.  Pattern:
``dags/utils/proxy_traffic.py::ensure_ops_table``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

from .transport_contracts import RequestLedgerEntry


JOURNAL_SCHEMA = "iceberg.ops"
JOURNAL_TABLE = "iceberg.ops.espn_request_journal_v1"
JOURNAL_COLUMNS = (
    ("request_date", "date"),
    ("requested_at", "timestamp(6)"),
    ("run_id", "varchar"),
    ("task_id", "varchar"),
    ("url_fingerprint", "varchar"),
    ("endpoint", "varchar"),
    ("host", "varchar"),
    ("transport_origin", "varchar"),
    ("lane", "varchar"),
    ("step", "integer"),
    ("attempts", "integer"),
    ("status", "integer"),
    ("origin_attempts", "varchar"),
    ("direct_bytes", "bigint"),
    ("content_encoding", "varchar"),
    ("latency_ms", "double"),
    ("disposition", "varchar"),
    ("error", "varchar"),
    ("raw_uri", "varchar"),
    ("content_hash", "varchar"),
)
_BATCH_ROWS = 500


def _execute(conn, sql: str) -> None:
    # fetchall() lets the statement finish; a bare close() cancels it in Trino.
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        cursor.fetchall()
    finally:
        cursor.close()


def ensure_journal_table(conn) -> None:
    _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {JOURNAL_SCHEMA}")
    columns = ", ".join(f"{name} {sql_type}" for name, sql_type in JOURNAL_COLUMNS)
    _execute(
        conn,
        f"CREATE TABLE IF NOT EXISTS {JOURNAL_TABLE} ({columns}) "
        "WITH (partitioning = ARRAY['request_date'])",
    )


def journal_rows(
    entries: Iterable[RequestLedgerEntry], *, run_id: str, task_id: str
) -> list[dict[str, Any]]:
    rows = []
    for entry in entries:
        requested_at = (
            datetime.fromisoformat(entry.requested_at) if entry.requested_at else None
        )
        rows.append(
            {
                "request_date": requested_at.date() if requested_at else None,
                "requested_at": requested_at,
                "run_id": run_id,
                "task_id": task_id,
                "url_fingerprint": entry.url_fingerprint,
                "endpoint": entry.endpoint.value,
                "host": entry.host,
                "transport_origin": entry.transport_origin,
                "lane": entry.lane,
                "step": entry.step,
                "attempts": entry.attempts,
                "status": entry.status,
                "origin_attempts": json.dumps(
                    [list(pair) for pair in entry.origin_attempts]
                ),
                "direct_bytes": entry.direct_bytes,
                "content_encoding": entry.content_encoding,
                "latency_ms": entry.latency_ms,
                "disposition": entry.disposition,
                "error": entry.error,
                "raw_uri": entry.raw_uri,
                "content_hash": entry.content_hash,
            }
        )
    return rows


def _literal(value: Any, sql_type: str) -> str:
    if value is None:
        return "NULL"
    if sql_type == "date":
        return f"DATE '{value.isoformat()}'"
    if sql_type.startswith("timestamp"):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return f"TIMESTAMP '{value.isoformat(sep=' ', timespec='microseconds')}'"
    if sql_type in ("integer", "bigint"):
        return str(int(value))
    if sql_type == "double":
        return repr(float(value))
    return "'" + str(value).replace("'", "''") + "'"


def flush_journal(conn, rows: Sequence[dict[str, Any]]) -> int:
    """Insert rows in multi-row batches; returns the number of rows written."""

    names = ", ".join(name for name, _ in JOURNAL_COLUMNS)
    for start in range(0, len(rows), _BATCH_ROWS):
        values = ", ".join(
            "("
            + ", ".join(
                _literal(row.get(name), sql_type) for name, sql_type in JOURNAL_COLUMNS
            )
            + ")"
            for row in rows[start : start + _BATCH_ROWS]
        )
        _execute(conn, f"INSERT INTO {JOURNAL_TABLE} ({names}) VALUES {values}")
    return len(rows)


__all__ = [
    "JOURNAL_COLUMNS",
    "JOURNAL_TABLE",
    "ensure_journal_table",
    "flush_journal",
    "journal_rows",
]
