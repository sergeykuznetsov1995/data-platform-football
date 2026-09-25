"""How long the SofaScore refresh lane waits for a pool slot (#1360).

Owner's criterion: the refresh lane has its own pool and gateway, so its runs
do not wait for the daily ingest, and the history lane keeps working while
the daily ingest runs. The host morning report imports this file straight
from the release tree (no Airflow on the host), so it depends on the standard
library only — same contract as ``sofascore_red_share.py``.

Why not ``queued_dttm -> start_date``: in Airflow 2.11 a task waiting for a
pool slot stays ``scheduled``; ``queued_dttm`` is stamped when the slot is
granted, so that difference is always ~0. The honest measure is the *gap*:
a task instance's ``start_date`` minus the later of its DagRun's start and
the latest ``end_date`` of the run's task instances that started before it.
Scheduler hand-overs take up to ``WAIT_THRESHOLD_S``; only gaps above it are
waiting. Only first tries are measured (a retry's gap is its retry delay);
every earlier task instance of the run counts as a predecessor.
"""

from __future__ import annotations

import re
from typing import NamedTuple

WAIT_THRESHOLD_S = 60

REFRESH_DAG_ID = "dag_refresh_sofascore_all_mens"
HISTORY_DAG_ID = "dag_backfill_sofascore_all_mens"
DAILY_DAG_ID = "dag_ingest_sofascore"
# The refresh lane's pool: the former players lane (#1244), absorbed by #1360
# with its name kept (deploy/sofascore/airflow.compose.yaml).
REFRESH_POOL = "sofascore_players_pool"
HISTORY_SCOPE_TASK_ID = "run_historical_scope"

_UTC_LITERAL = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def _check(t0_utc: str, t1_utc: str) -> None:
    for bound in (t0_utc, t1_utc):
        if not _UTC_LITERAL.match(bound):
            raise ValueError(f"not a UTC timestamp literal: {bound!r}")


def pool_wait_sql(t0_utc: str, t1_utc: str) -> str:
    """One row ``runs|waiting_runs|wait_sum_s|wait_max_s`` over the refresh
    DAG's first-try terminal task instances started in ``[t0_utc, t1_utc)``.

    ``wait_sum_s`` sums gaps above the threshold; ``wait_max_s`` is the
    largest gap of any measured task instance, threshold or not."""

    _check(t0_utc, t1_utc)
    measured = (
        f"t.dag_id = '{REFRESH_DAG_ID}' "
        "AND t.try_number = 1 "
        "AND t.state IN ('success', 'failed') "
        f"AND t.start_date >= '{t0_utc}'::timestamptz "
        f"AND t.start_date < '{t1_utc}'::timestamptz"
    )
    return (
        "WITH runs AS ("
        f"SELECT DISTINCT t.run_id FROM task_instance t WHERE {measured}"
        "), ti AS ("
        "SELECT t.dag_id, t.run_id, t.try_number, t.state, t.start_date, "
        "GREATEST(dr.start_date, max(t.end_date) OVER ("
        "PARTITION BY t.run_id ORDER BY t.start_date "
        "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS ready_at "
        "FROM task_instance t "
        "JOIN dag_run dr ON dr.dag_id = t.dag_id AND dr.run_id = t.run_id "
        f"WHERE t.dag_id = '{REFRESH_DAG_ID}' AND t.start_date IS NOT NULL "
        "AND t.run_id IN (SELECT run_id FROM runs)"
        "), gaps AS ("
        "SELECT t.run_id, "
        "GREATEST(extract(epoch FROM t.start_date - t.ready_at), 0) AS gap_s "
        f"FROM ti t WHERE {measured}"
        "), per_run AS ("
        "SELECT run_id, "
        f"coalesce(sum(gap_s) FILTER (WHERE gap_s > {WAIT_THRESHOLD_S}), 0) AS wait_s, "
        "max(gap_s) AS max_gap_s FROM gaps GROUP BY run_id"
        ") "
        "SELECT count(*), count(*) FILTER (WHERE wait_s > 0), "
        "round(coalesce(sum(wait_s), 0)), round(coalesce(max(max_gap_s), 0)::numeric, 1) "
        "FROM per_run"
    )


def lane_overlap_sql(t0_utc: str, t1_utc: str) -> str:
    """One row ``history_during_daily|daily_in_refresh_pool`` for task
    instances started in ``[t0_utc, t1_utc)``: history scope attempts that
    started inside a daily DagRun, and daily task instances that took the
    refresh lane's pool (expected 0)."""

    _check(t0_utc, t1_utc)
    window = (
        f"t.start_date >= '{t0_utc}'::timestamptz "
        f"AND t.start_date < '{t1_utc}'::timestamptz"
    )
    return (
        "SELECT ("
        "SELECT count(*) FROM task_instance t "
        f"WHERE t.dag_id = '{HISTORY_DAG_ID}' "
        f"AND t.task_id = '{HISTORY_SCOPE_TASK_ID}' AND {window} "
        "AND EXISTS (SELECT 1 FROM dag_run d "
        f"WHERE d.dag_id = '{DAILY_DAG_ID}' AND d.start_date IS NOT NULL "
        "AND t.start_date >= d.start_date "
        "AND t.start_date < coalesce(d.end_date, now()))"
        "), ("
        "SELECT count(*) FROM task_instance t "
        f"WHERE t.dag_id = '{DAILY_DAG_ID}' AND t.pool = '{REFRESH_POOL}' "
        f"AND {window}"
        ")"
    )


class PoolWait(NamedTuple):
    # NamedTuple, not a dataclass: the host loads this file by path without
    # registering it in sys.modules, which dataclasses require.
    runs: int
    waiting_runs: int
    wait_sum_s: float
    wait_max_s: float
    history_during_daily: int
    daily_in_refresh_pool: int

    @property
    def verdict(self) -> str:  # "ok" | "wait" | "no_data"
        if self.runs == 0:
            return "no_data"
        if self.waiting_runs or self.daily_in_refresh_pool:
            return "wait"
        return "ok"


def _fields(psql_out: str, width: int) -> list[str]:
    lines = [line.strip() for line in psql_out.splitlines() if line.strip()]
    if len(lines) != 1 or len(lines[0].split("|")) != width:
        raise ValueError(f"expected one row of {width} fields: {psql_out!r}")
    return lines[0].split("|")


def parse_rows(wait_out: str, overlap_out: str) -> PoolWait:
    """``psql -tA`` outputs of :func:`pool_wait_sql` and :func:`lane_overlap_sql`."""

    runs, waiting, wait_sum, wait_max = _fields(wait_out, 4)
    during, in_pool = _fields(overlap_out, 2)
    return PoolWait(
        int(runs), int(waiting), float(wait_sum), float(wait_max),
        int(during), int(in_pool),
    )


def _minutes(seconds: float) -> str:
    text = f"{seconds / 60:.1f}".replace(".", ",")
    return text[:-2] if text.endswith(",0") else text


def format_line(wait: PoolWait) -> str:
    """The morning-report line; ✅ only when no refresh run waited and no
    daily task instance took the refresh pool."""

    tail = (
        f"история во время дейли: {wait.history_during_daily} скоупов · "
        f"дейли в пуле актуалки: {wait.daily_in_refresh_pool}"
    )
    if wait.verdict == "no_data":
        return f"ожидание пула актуалкой: ⚠️ нет прогонов · {tail}"
    mark = "✅" if wait.verdict == "ok" else "⛔"
    return (
        f"ожидание пула актуалкой: {_minutes(wait.wait_sum_s)} мин "
        f"({wait.waiting_runs}/{wait.runs} прогонов ждали, "
        f"макс {_minutes(wait.wait_max_s)} мин) {mark} · {tail}"
    )
