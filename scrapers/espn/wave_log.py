"""Wave log of the current-data waves in ``iceberg.ops`` (#1505).

One row per tournament per wave (``run_id, wave_started_at, wave_finished_at,
slug, season_year, state, matches, first_error``) plus one row for the wave
itself (``slug = WAVE_ROW``: state and reason of the wave, all its matches).
``wave_summary`` writes it before it decides that the wave is red, so a red
wave leaves its trace too.  Read by the red-tournament alert of
``deploy/espn/espn_stall_watch.py`` and by the p95 wave duration of the
morning report (``criterion.WAVE_DURATION_SQL``).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Sequence

from .journal import _execute, _literal

WAVE_LOG_SCHEMA = "iceberg.ops"
WAVE_LOG_TABLE = "iceberg.ops.espn_wave_tournament_v1"
WAVE_LOG_COLUMNS = (
    ("run_id", "varchar"),
    ("wave_started_at", "timestamp(6)"),
    ("wave_finished_at", "timestamp(6)"),
    ("slug", "varchar"),
    ("season_year", "integer"),
    ("state", "varchar"),
    ("matches", "integer"),
    ("first_error", "varchar"),
)
# The row of the wave itself; tournament rows carry their slug.
WAVE_ROW = "(wave)"


def ensure_wave_log_table(conn) -> None:
    _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {WAVE_LOG_SCHEMA}")
    columns = ", ".join(f"{name} {sql_type}" for name, sql_type in WAVE_LOG_COLUMNS)
    _execute(
        conn,
        f"CREATE TABLE IF NOT EXISTS {WAVE_LOG_TABLE} ({columns}) "
        "WITH (partitioning = ARRAY['day(wave_started_at)'])",
    )


def wave_log_rows(
    outcomes: Sequence[Mapping[str, Any]],
    failed: Sequence[str],
    summary,
    *,
    run_id: str,
    started_at: datetime | None,
    finished_at: datetime,
) -> list[dict[str, Any]]:
    """Rows of one wave: the wave row, then a row per tournament.

    ``failed`` names mapped tasks that ended without an outcome; they are red
    rows under their task name (no slug is known for them).  ``summary`` is
    the ``wave.WaveSummary`` of the same outcomes.
    """

    base = {
        "run_id": run_id,
        "wave_started_at": started_at or finished_at,
        "wave_finished_at": finished_at,
    }
    rows = [
        {
            **base,
            "slug": WAVE_ROW,
            "season_year": None,
            "state": "red" if summary.red else "green",
            "matches": sum(int(item.get("matches") or 0) for item in outcomes),
            "first_error": summary.reason,
        }
    ]
    rows.extend(
        {
            **base,
            "slug": item["slug"],
            "season_year": item.get("season_year"),
            "state": item["state"],
            "matches": int(item.get("matches") or 0),
            "first_error": item.get("first_error"),
        }
        for item in outcomes
    )
    rows.extend(
        {
            **base,
            "slug": name,
            "season_year": None,
            "state": "red",
            "matches": 0,
            "first_error": "task ended without an outcome",
        }
        for name in failed
    )
    return rows


def write_wave_log(conn, rows: Sequence[Mapping[str, Any]]) -> int:
    if not rows:
        return 0
    names = ", ".join(name for name, _ in WAVE_LOG_COLUMNS)
    values = ", ".join(
        "("
        + ", ".join(_literal(row.get(name), sql_type) for name, sql_type in WAVE_LOG_COLUMNS)
        + ")"
        for row in rows
    )
    _execute(conn, f"INSERT INTO {WAVE_LOG_TABLE} ({names}) VALUES {values}")
    return len(rows)


__all__ = [
    "WAVE_LOG_COLUMNS",
    "WAVE_LOG_TABLE",
    "WAVE_ROW",
    "ensure_wave_log_table",
    "wave_log_rows",
    "write_wave_log",
]
