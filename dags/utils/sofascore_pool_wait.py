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
waiting. A retry is measured from its own previous try's end plus the
lane's retry delay (``RETRY_DELAY_S``), so the delay itself is not waiting
but a pool wait after it is; every earlier attempt of the run, retries
included, counts as a predecessor.
"""

from __future__ import annotations

import re
from typing import NamedTuple

WAIT_THRESHOLD_S = 60
# retry_delay of the only retrying refresh task (run_refresh_scope, 2 min in
# dags/dag_refresh_sofascore_all_mens.py); every other task has retries=0.
RETRY_DELAY_S = 120

REFRESH_DAG_ID = "dag_refresh_sofascore_all_mens"
HISTORY_DAG_ID = "dag_backfill_sofascore_all_mens"
DAILY_DAG_ID = "dag_ingest_sofascore"
# The refresh lane's pool: the former players lane (#1244), absorbed by #1360
# with its name kept (deploy/sofascore/airflow.compose.yaml).
REFRESH_POOL = "sofascore_players_pool"
HISTORY_POOL = "sofascore_history_pool"
HISTORY_SCOPE_TASK_ID = "run_historical_scope"
# Task instance rows keep the latest try only; earlier tries of a retried task
# live in task_instance_history (Airflow >= 2.10). Both are read as attempts.
_ATTEMPT_COLUMNS = (
    "dag_id, run_id, task_id, map_index, try_number, state, pool, start_date, end_date"
)

_UTC_LITERAL = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def _check(t0_utc: str, t1_utc: str) -> None:
    for bound in (t0_utc, t1_utc):
        if not _UTC_LITERAL.match(bound):
            raise ValueError(f"not a UTC timestamp literal: {bound!r}")


def _attempts(where: str) -> str:
    return (
        f"SELECT {_ATTEMPT_COLUMNS} FROM task_instance WHERE {where} "
        f"UNION ALL SELECT {_ATTEMPT_COLUMNS} FROM task_instance_history WHERE {where}"
    )


def pool_wait_sql(t0_utc: str, t1_utc: str) -> str:
    """One row ``runs|waiting_runs|wait_sum_s|wait_max_s`` over the refresh
    DagRuns started in ``[t0_utc, t1_utc)``.

    Every attempt that started is measured, finished or not; earlier tries
    are read from ``task_instance_history``. A retry's gap starts after its
    previous try's end plus ``RETRY_DELAY_S``. A task still
    ``scheduled``/``queued`` while nothing of its run runs is waiting right
    now and is measured up to ``now()``. ``wait_sum_s`` sums gaps above the
    threshold; ``wait_max_s`` is the largest gap, threshold or not."""

    _check(t0_utc, t1_utc)
    pending = (
        "EXISTS (SELECT 1 FROM task_instance p "
        f"WHERE p.dag_id = '{REFRESH_DAG_ID}' AND p.run_id = r.run_id "
        "AND p.start_date IS NULL AND p.state IN ('scheduled', 'queued')) "
        "AND NOT EXISTS (SELECT 1 FROM attempts b WHERE b.run_id = r.run_id "
        "AND b.start_date IS NOT NULL AND b.end_date IS NULL)"
    )
    return (
        "WITH runs AS ("
        "SELECT run_id, start_date AS run_start FROM dag_run "
        f"WHERE dag_id = '{REFRESH_DAG_ID}' "
        f"AND start_date >= '{t0_utc}'::timestamptz "
        f"AND start_date < '{t1_utc}'::timestamptz"
        "), attempts AS ("
        + _attempts(
            f"dag_id = '{REFRESH_DAG_ID}' AND run_id IN (SELECT run_id FROM runs)"
        )
        + "), started AS ("
        "SELECT a.run_id, a.try_number, a.state, a.start_date, "
        "GREATEST(r.run_start, max(a.end_date) OVER ("
        "PARTITION BY a.run_id ORDER BY a.start_date "
        "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS ready_at, "
        "lag(a.end_date) OVER ("
        "PARTITION BY a.run_id, a.task_id, a.map_index ORDER BY a.try_number"
        ") AS previous_try_end "
        "FROM attempts a JOIN runs r ON r.run_id = a.run_id "
        "WHERE a.start_date IS NOT NULL"
        "), gaps AS ("
        "SELECT run_id, "
        "GREATEST(extract(epoch FROM start_date - GREATEST(ready_at, "
        f"previous_try_end + INTERVAL '{RETRY_DELAY_S} seconds')), 0) AS gap_s "
        "FROM started "
        "WHERE state NOT IN ('skipped', 'upstream_failed', 'removed') "
        "UNION ALL "
        "SELECT r.run_id, GREATEST(extract(epoch FROM now() - GREATEST(r.run_start, "
        "(SELECT max(e.end_date) FROM attempts e WHERE e.run_id = r.run_id))), 0) "
        f"FROM runs r WHERE {pending}"
        "), per_run AS ("
        "SELECT r.run_id, "
        f"coalesce(sum(g.gap_s) FILTER (WHERE g.gap_s > {WAIT_THRESHOLD_S}), 0) AS wait_s, "
        "coalesce(max(g.gap_s), 0) AS max_gap_s "
        "FROM runs r LEFT JOIN gaps g ON g.run_id = r.run_id GROUP BY r.run_id"
        ") "
        "SELECT count(*), count(*) FILTER (WHERE wait_s > 0), "
        "round(coalesce(sum(wait_s), 0)), round(coalesce(max(max_gap_s), 0)::numeric, 1) "
        "FROM per_run"
    )


def lane_overlap_sql(t0_utc: str, t1_utc: str) -> str:
    """One row ``history_during_daily|daily_in_refresh_pool|daily_in_history_pool``:
    history scope attempts started in ``[t0_utc, t1_utc)`` inside a daily
    DagRun, and daily attempts that held the refresh or the history pool at
    any moment of the window (started before it included; expected 0)."""

    _check(t0_utc, t1_utc)
    started_in_window = (
        f"a.start_date >= '{t0_utc}'::timestamptz "
        f"AND a.start_date < '{t1_utc}'::timestamptz"
    )
    held_in_window = (
        f"a.start_date < '{t1_utc}'::timestamptz "
        f"AND coalesce(a.end_date, now()) > '{t0_utc}'::timestamptz"
    )
    daily = _attempts(f"dag_id = '{DAILY_DAG_ID}' AND start_date IS NOT NULL")
    return (
        "SELECT ("
        "SELECT count(*) FROM ("
        + _attempts(f"dag_id = '{HISTORY_DAG_ID}' AND task_id = '{HISTORY_SCOPE_TASK_ID}'")
        + f") a WHERE {started_in_window} "
        "AND EXISTS (SELECT 1 FROM dag_run d "
        f"WHERE d.dag_id = '{DAILY_DAG_ID}' AND d.start_date IS NOT NULL "
        "AND a.start_date >= d.start_date "
        "AND a.start_date < coalesce(d.end_date, now()))"
        "), ("
        f"SELECT count(*) FROM ({daily}) a WHERE a.pool = '{REFRESH_POOL}' AND {held_in_window}"
        "), ("
        f"SELECT count(*) FROM ({daily}) a WHERE a.pool = '{HISTORY_POOL}' AND {held_in_window}"
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
    daily_in_history_pool: int

    @property
    def verdict(self) -> str:  # "ok" | "wait" | "no_data"
        if self.runs == 0:
            return "no_data"
        if self.waiting_runs or self.daily_in_refresh_pool or self.daily_in_history_pool:
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
    during, in_refresh, in_history = _fields(overlap_out, 3)
    return PoolWait(
        int(runs), int(waiting), float(wait_sum), float(wait_max),
        int(during), int(in_refresh), int(in_history),
    )


def _minutes(seconds: float) -> str:
    text = f"{seconds / 60:.1f}".replace(".", ",")
    return text[:-2] if text.endswith(",0") else text


def format_line(wait: PoolWait) -> str:
    """The morning-report line; ✅ only when no refresh run waited and no
    daily task instance took the refresh or the history pool."""

    tail = (
        f"история во время дейли: {wait.history_during_daily} скоупов · "
        "дейли в пулах актуалки/истории: "
        f"{wait.daily_in_refresh_pool}/{wait.daily_in_history_pool}"
    )
    if wait.verdict == "no_data":
        return f"ожидание пула актуалкой: ⚠️ нет прогонов · {tail}"
    mark = "✅" if wait.verdict == "ok" else "⛔"
    return (
        f"ожидание пула актуалкой: {_minutes(wait.wait_sum_s)} мин "
        f"({wait.waiting_runs}/{wait.runs} прогонов ждали, "
        f"макс {_minutes(wait.wait_max_s)} мин) {mark} · {tail}"
    )
