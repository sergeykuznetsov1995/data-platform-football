"""Read-only classification of Airflow work before a shared-writer window.

A terminal DAG run alone is not proof that its tasks stopped executing. Only
old waiting tasks with absent or conclusively finished jobs are historical.
A cleared (NULL-state) task with a job must also prove that job finished.
Running job metadata older than 24 hours is abandoned only if start and heartbeat
are old, end is absent, and every reference is terminal with an old update
and terminal parent. Orphans are counted separately; all fresh/unknown evidence
blocks. These rules never mutate task, DAG-run or job state.
"""
from __future__ import annotations

import math
import re
from typing import Any

HISTORICAL_AGE = 24 * 60 * 60
HEARTBEAT_AGE = 5 * 60
# Airflow Job.__init__ seeds latest_heartbeat, then prepare_for_execution resets
# start_date without resetting heartbeat. Allow at most one second of that
# initialization skew (observed maximum 0.795361s); larger reversals fail closed.
# This tolerance never applies to start/end or heartbeat/end ordering.
INITIAL_HEARTBEAT_SKEW = 1
TERMINAL_RUNS = {"success", "failed"}
TERMINAL_JOBS = {"success", "failed"}
WAITING_STATES = {"scheduled", "up_for_retry", "up_for_reschedule"}
EXECUTING_STATES = {"running", "queued", "restarting", "deferred"}
MAX_BLOCKERS = 50

# An expression for embedding in the host's single repeatable-read snapshot.
# NULL is Airflow's unstarted state: no job and a finished parent cannot execute.
# Retain other NULL states, unknown states and orphan tasks as blockers.
WORK_SQL = f"""
json_build_object(
  'now', extract(epoch FROM CURRENT_TIMESTAMP),
  'active_runs', (SELECT count(*) FROM dag_run
    WHERE state IS NULL OR state NOT IN ('success', 'failed')),
  'unstarted_tasks', (SELECT count(*) FROM task_instance ti
    JOIN dag_run dr ON dr.dag_id = ti.dag_id AND dr.run_id = ti.run_id
    WHERE ti.state IS NULL AND ti.job_id IS NULL AND dr.state IN ('success', 'failed')),
  'tasks', COALESCE((SELECT json_agg(json_build_object(
    'dag_id', ti.dag_id, 'task_id', ti.task_id, 'run_id', ti.run_id,
    'map_index', ti.map_index, 'state', ti.state, 'parent_state', dr.state,
    'updated', extract(epoch FROM ti.updated_at), 'job_id', ti.job_id,
    'job_found', j.id IS NOT NULL, 'job_type', j.job_type, 'job_state', j.state,
    'job_start', extract(epoch FROM j.start_date),
    'job_end', extract(epoch FROM j.end_date),
    'job_heartbeat', extract(epoch FROM j.latest_heartbeat))
    ORDER BY ti.dag_id, ti.run_id, ti.task_id, ti.map_index)
    FROM task_instance ti
    LEFT JOIN dag_run dr ON dr.dag_id = ti.dag_id AND dr.run_id = ti.run_id
    LEFT JOIN job j ON j.id = ti.job_id
    WHERE ti.state IN ('running', 'queued', 'restarting', 'deferred',
                       'scheduled', 'up_for_retry', 'up_for_reschedule')
      OR (ti.state IS NOT NULL AND ti.state NOT IN
          ('success', 'failed', 'skipped', 'upstream_failed', 'removed'))
      OR (ti.state IS NULL AND (ti.job_id IS NOT NULL
          OR dr.state IS NULL OR dr.state NOT IN ('success', 'failed')))
  ), '[]'::json),
  'local_jobs', COALESCE((SELECT json_agg(json_build_object(
    'job_id', j.id, 'state', j.state, 'start', extract(epoch FROM j.start_date),
    'end', extract(epoch FROM j.end_date),
    'heartbeat', extract(epoch FROM j.latest_heartbeat),
    'references', refs.total, 'unsafe_references', refs.unsafe)
    ORDER BY j.id)
    FROM job j CROSS JOIN LATERAL (
      SELECT count(*) AS total, count(*) FILTER (WHERE
        ti.state IS NULL OR ti.state NOT IN ('success', 'failed', 'skipped', 'upstream_failed', 'removed')
        OR dr.state IS NULL OR dr.state NOT IN ('success', 'failed')
        OR ti.updated_at IS NULL OR ti.updated_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'
      ) AS unsafe
      FROM task_instance ti
      LEFT JOIN dag_run dr ON dr.dag_id = ti.dag_id AND dr.run_id = ti.run_id
      WHERE ti.job_id = j.id
    ) refs
    WHERE j.job_type = 'LocalTaskJob' AND (
      j.state IS NULL OR j.state NOT IN ('success', 'failed') OR j.end_date IS NULL
      OR j.latest_heartbeat IS NULL OR j.start_date IS NULL
      OR j.start_date > CURRENT_TIMESTAMP
      OR j.start_date > j.end_date
      OR j.start_date > j.latest_heartbeat + INTERVAL '{INITIAL_HEARTBEAT_SKEW} seconds'
      OR j.end_date >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'
      OR j.latest_heartbeat >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'
      OR j.latest_heartbeat > j.end_date
  )), '[]'::json)
)
"""


def _epoch(value: Any, *, nullable: bool = True) -> None:
    if value is None and nullable:
        return
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError("invalid work timestamp")


def _text(value: Any, *, nullable: bool = False) -> None:
    if value is None and nullable:
        return
    if not isinstance(value, str) or not value or len(value) > 1024:
        raise ValueError("invalid work identifier or state")


def _safe(value: str | None) -> str | None:
    """Bound metadata in evidence; never include commands, hosts or tracebacks."""
    return re.sub(r"[^A-Za-z0-9_.:+@-]", "?", value)[:200] if value else None


def classify_work(raw: Any) -> dict[str, Any]:
    """Validate SQL evidence and count blockers; malformed evidence fails closed.

    ``active`` includes runs, tasks and jobs; it is a blocker count, not a count
    of distinct executions. Detailed blocker rows are capped for journal size.
    """
    if not isinstance(raw, dict) or set(raw) != {"now", "active_runs", "unstarted_tasks", "tasks", "local_jobs"}:
        raise ValueError("invalid work snapshot")
    _epoch(raw["now"], nullable=False)
    now = raw["now"]
    if any(type(raw[k]) is not int or raw[k] < 0 for k in ("active_runs", "unstarted_tasks")):
        raise ValueError("invalid active run count")
    if not isinstance(raw["tasks"], list) or not isinstance(raw["local_jobs"], list):
        raise ValueError("invalid work rows")
    blockers = []
    historical = 0
    historical_reset_tasks = 0
    blocking_tasks = 0
    task_keys = {"dag_id", "task_id", "run_id", "map_index", "state", "parent_state", "updated",
                 "job_id", "job_found", "job_type", "job_state", "job_start", "job_end", "job_heartbeat"}
    seen = set()
    for row in raw["tasks"]:
        if not isinstance(row, dict) or set(row) != task_keys:
            raise ValueError("invalid task evidence")
        for field in ("dag_id", "task_id", "run_id"):
            _text(row[field])
        for field in ("state", "parent_state", "job_type", "job_state"):
            _text(row[field], nullable=True)
        for field in ("updated", "job_start", "job_end", "job_heartbeat"):
            _epoch(row[field])
        if type(row["map_index"]) is not int or row["map_index"] < -1:
            raise ValueError("invalid task map index")
        key = tuple(row[field] for field in ("dag_id", "task_id", "run_id", "map_index"))
        if key in seen:
            raise ValueError("duplicate task evidence")
        seen.add(key)
        if type(row["job_found"]) is not bool or (row["job_id"] is not None and
                (type(row["job_id"]) is not int or row["job_id"] <= 0)):
            raise ValueError("invalid task job identity")
        if row["state"] in EXECUTING_STATES:
            reason = "executing_task"
        elif row["state"] not in WAITING_STATES and not (row["state"] is None and row["job_id"] is not None):
            reason = "unknown_or_unstarted_task"
        elif row["parent_state"] not in TERMINAL_RUNS:
            reason = "unfinished_or_missing_parent"
        elif row["updated"] is None or now - row["updated"] < HISTORICAL_AGE:
            reason = "recent_or_undated_task"
        elif row["job_id"] is None:
            reason = ("inconsistent_job" if row["job_found"] or any(row[k] is not None
                      for k in ("job_type", "job_state", "job_start", "job_end", "job_heartbeat")) else None)
        elif not row["job_found"]:
            reason = "missing_job"
        elif row["job_type"] != "LocalTaskJob" or row["job_state"] not in TERMINAL_JOBS:
            reason = "unfinished_or_unknown_job"
        elif (row["job_start"] is None or row["job_end"] is None or row["job_heartbeat"] is None
              or now - row["job_start"] < HISTORICAL_AGE
              or row["job_start"] > row["job_heartbeat"] + INITIAL_HEARTBEAT_SKEW
              or row["job_start"] > row["job_end"]
              or now - row["job_end"] <= HEARTBEAT_AGE
              or now - row["job_heartbeat"] <= HEARTBEAT_AGE
              or row["job_heartbeat"] > row["job_end"]):
            reason = "recent_or_inconsistent_job"
        else:
            reason = None
        if reason is None:
            historical += int(row["state"] is not None)
            historical_reset_tasks += int(row["state"] is None)
        else:
            blocking_tasks += 1
            if len(blockers) < MAX_BLOCKERS:
                blockers.append({"kind": "task", "reason": reason,
                                 **{k: _safe(row[k]) for k in ("dag_id", "task_id", "run_id", "state", "parent_state")},
                                 "map_index": row["map_index"], "job_id": row["job_id"]})
    job_ids = set()
    historical_jobs = 0
    abandoned_orphan_jobs = 0
    local_jobs = 0
    for row in raw["local_jobs"]:
        if not isinstance(row, dict) or set(row) != {"job_id", "state", "start", "end", "heartbeat", "references", "unsafe_references"}:
            raise ValueError("invalid local job evidence")
        if type(row["job_id"]) is not int or row["job_id"] <= 0 or row["job_id"] in job_ids:
            raise ValueError("invalid or duplicate local job identity")
        job_ids.add(row["job_id"])
        _text(row["state"], nullable=True)
        for field in ("start", "end", "heartbeat"):
            _epoch(row[field])
        if any(type(row[k]) is not int or row[k] < 0 for k in ("references", "unsafe_references")) or row["unsafe_references"] > row["references"]:
            raise ValueError("invalid local job references")
        # Stale running records are abandoned only with no runnable references.
        # Keep orphan residuals visible separately; no metabase state is changed.
        old_abandoned = (
            row["state"] == "running" and row["unsafe_references"] == 0
            and row["start"] is not None and now - row["start"] >= HISTORICAL_AGE
            and row["heartbeat"] is not None and now - row["heartbeat"] >= HISTORICAL_AGE
            and row["start"] <= row["heartbeat"] + INITIAL_HEARTBEAT_SKEW
            and row["end"] is None
        )
        if old_abandoned:
            historical_jobs += 1
            abandoned_orphan_jobs += int(row["references"] == 0)
            continue
        local_jobs += 1
        if len(blockers) < MAX_BLOCKERS:
            blockers.append({"kind": "local_job", "reason": "unfinished_or_recent_job",
                             "job_id": row["job_id"], "state": _safe(row["state"])})
    return {"active": raw["active_runs"] + blocking_tasks + local_jobs,
            "active_runs": raw["active_runs"], "blocking_tasks": blocking_tasks,
            "historical_tasks": historical, "historical_reset_tasks": historical_reset_tasks,
            "unstarted_tasks": raw["unstarted_tasks"],
            "historical_jobs": historical_jobs, "abandoned_orphan_jobs": abandoned_orphan_jobs,
            "local_jobs": local_jobs,
            "blockers": blockers, "blockers_truncated": max(0, blocking_tasks + local_jobs - len(blockers))}
