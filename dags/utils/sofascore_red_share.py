"""One red-share threshold for the SofaScore scope lanes (#1356).

The owner's criterion (grill 22.09): fewer than 20 % red scope attempts per
UTC day, counted by ``task_instance`` — not by ``dag_run`` colour and not by
a streak. The host morning report and the stall watch import this file
straight from the release tree (no Airflow on the host), so it depends on the
standard library only.

A red scope yields up to three red task instances (``run_*_scope``,
``validate_*_scope``, the propagate leaf); only the mapped ``run_*_scope``
task instances are counted. A ``task_instance`` row keeps its latest try
only: a retry moves the row to the day the retry started, and
``up_for_retry``/``running`` are not terminal and not counted. With the 4 h
scope timeout and the 2 min retry delay a UTC day's number is stable from
~04:05 UTC the next day; the morning report (05:00 UTC) and the stall watch
R4 (from 05:00 UTC) read it after that.
"""

from __future__ import annotations

import re
from typing import NamedTuple

RED_SHARE_THRESHOLD_PCT = 20

HISTORY_DAG_ID = "dag_backfill_sofascore_all_mens"
REFRESH_DAG_ID = "dag_refresh_sofascore_all_mens"
SCOPE_DAG_IDS = (HISTORY_DAG_ID, REFRESH_DAG_ID)

# LIKE mask for run_historical_scope / run_refresh_scope; the daily
# run_sofascore_dq is not a scope and must not match.
SCOPE_TASK_PATTERN = "run\\_%\\_scope"

_UTC_LITERAL = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def red_share_sql(t0_utc: str, t1_utc: str) -> str:
    """Per-DAG ``dag_id|failed|total`` of terminal scope TIs started in
    ``[t0_utc, t1_utc)``; both bounds are ``YYYY-MM-DDTHH:MM:SSZ``."""

    for bound in (t0_utc, t1_utc):
        if not _UTC_LITERAL.match(bound):
            raise ValueError(f"not a UTC timestamp literal: {bound!r}")
    dag_ids = ", ".join(f"'{dag_id}'" for dag_id in SCOPE_DAG_IDS)
    return (
        "SELECT dag_id, count(*) FILTER (WHERE state = 'failed'), count(*) "
        "FROM task_instance "
        f"WHERE dag_id IN ({dag_ids}) "
        f"AND task_id LIKE '{SCOPE_TASK_PATTERN}' ESCAPE '\\' "
        "AND map_index >= 0 "
        "AND state IN ('success', 'failed') "
        f"AND start_date >= '{t0_utc}'::timestamptz "
        f"AND start_date < '{t1_utc}'::timestamptz "
        "GROUP BY 1 ORDER BY 1"
    )


class RedShare(NamedTuple):
    # NamedTuple, not a dataclass: the host loads this file by path without
    # registering it in sys.modules, which dataclasses require.
    failed: int
    total: int
    pct: float
    verdict: str  # "ok" | "red" | "no_data"

    @classmethod
    def of(cls, failed: int, total: int) -> "RedShare":
        if total == 0:
            return cls(failed, total, 0.0, "no_data")
        pct = 100.0 * failed / total
        verdict = "red" if pct >= RED_SHARE_THRESHOLD_PCT else "ok"
        return cls(failed, total, pct, verdict)


def parse_rows(psql_out: str) -> dict[str, tuple[int, int]]:
    """``psql -tA`` output of :func:`red_share_sql` → ``{dag_id: (failed, total)}``."""

    per_dag: dict[str, tuple[int, int]] = {}
    for line in psql_out.splitlines():
        if not line.strip():
            continue
        dag_id, failed, total = line.strip().split("|")
        per_dag[dag_id] = (int(failed), int(total))
    return per_dag


def total_share(per_dag: dict[str, tuple[int, int]]) -> RedShare:
    failed = sum(f for f, _ in per_dag.values())
    total = sum(t for _, t in per_dag.values())
    return RedShare.of(failed, total)


def format_line(day: str, per_dag: dict[str, tuple[int, int]]) -> str:
    """The one line the morning report and the stall watch both print;
    ``day`` is the UTC day ``YYYY-MM-DD`` the window covers."""

    share = total_share(per_dag)
    hf, ht = per_dag.get(HISTORY_DAG_ID, (0, 0))
    rf, rt = per_dag.get(REFRESH_DAG_ID, (0, 0))
    ddmm = f"{day[8:10]}.{day[5:7]}"
    lanes = (
        f"(порог < {RED_SHARE_THRESHOLD_PCT} %; история {hf}/{ht}, актуалка {rf}/{rt})"
    )
    if share.verdict == "no_data":
        return f"красных run_*_scope TI за {ddmm} (UTC): ⚠️ нет терминальных TI {lanes}"
    mark = "✅" if share.verdict == "ok" else "⛔"
    pct = f"{share.pct:.1f}".replace(".", ",")
    return (
        f"красных run_*_scope TI за {ddmm} (UTC): "
        f"{share.failed}/{share.total} = {pct} % {mark} {lanes}"
    )
