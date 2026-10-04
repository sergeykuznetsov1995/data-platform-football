from copy import deepcopy

import pytest

from deploy.shared_writer.work import classify_work, HEARTBEAT_AGE, HISTORICAL_AGE, MAX_BLOCKERS

NOW = 10_000_000
OLD = NOW - HISTORICAL_AGE - 1


def task(**changes):
    return {"dag_id": "dag_ingest_clubelo", "task_id": "ingest", "run_id": "scheduled__old",
            "map_index": -1, "state": "scheduled", "parent_state": "failed", "updated": OLD,
            "job_id": None, "job_found": False, "job_type": None, "job_state": None,
            "job_start": None, "job_end": None, "job_heartbeat": None, **changes}


def ended_task(**changes):
    return task(job_id=1, job_found=True, job_type="LocalTaskJob", job_state="success",
                job_start=OLD - 1000, job_end=OLD, job_heartbeat=OLD - 1, **changes)


def job(**changes):
    return {"job_id": 1, "state": "running", "start": OLD - 1000, "end": None,
            "heartbeat": OLD, "references": 1, "unsafe_references": 0, **changes}


def snapshot(tasks=(), jobs=(), **changes):
    return {"now": NOW, "active_runs": 0, "unstarted_tasks": 0,
            "tasks": list(tasks), "local_jobs": list(jobs), **changes}


def test_333_captured_equivalent_old_tasks_do_not_block():
    # Captured 03 Oct: 324 never-started scheduled tasks and 9 retries whose
    # LocalTaskJobs ended successfully in July, all in failed parent DAG runs.
    rows = [task(task_id=f"scheduled_{i}") for i in range(324)]
    rows += [ended_task(state="up_for_retry", task_id=f"retry_{i}") for i in range(9)]
    result = classify_work(snapshot(rows, unstarted_tasks=237))
    assert result["active"] == 0
    assert result["historical_tasks"] == 333
    assert result["unstarted_tasks"] == 237
    assert result["blockers"] == []


@pytest.mark.parametrize("state", ["queued", "running", "restarting", "deferred", "future_state", None])
@pytest.mark.parametrize("parent", ["failed", "success", "running", None])
def test_executing_and_unknown_tasks_always_block(state, parent):
    result = classify_work(snapshot([task(state=state, parent_state=parent)]))
    assert result["active"] == 1
    assert result["historical_tasks"] == 0


@pytest.mark.parametrize("state", ["scheduled", "up_for_retry", "up_for_reschedule"])
@pytest.mark.parametrize("parent", ["queued", "running", "unknown", None])
def test_waiting_task_requires_known_terminal_parent(state, parent):
    result = classify_work(snapshot([task(state=state, parent_state=parent)]))
    assert result["blockers"][0]["reason"] == "unfinished_or_missing_parent"


@pytest.mark.parametrize("updated,blocked", [(None, True), (NOW + 1, True),
    (NOW - HISTORICAL_AGE + 1, True), (NOW - HISTORICAL_AGE, False)])
def test_historical_age_boundary(updated, blocked):
    assert bool(classify_work(snapshot([task(updated=updated)]))["active"]) is blocked


@pytest.mark.parametrize("changes", [
    {"job_found": False}, {"job_type": None}, {"job_type": "SchedulerJob"},
    {"job_state": "running"}, {"job_state": "unknown"}, {"job_state": None},
    {"job_end": None}, {"job_heartbeat": None}, {"job_end": NOW + 1},
    {"job_heartbeat": NOW + 1}, {"job_end": NOW - HEARTBEAT_AGE},
    {"job_heartbeat": NOW - HEARTBEAT_AGE}, {"job_heartbeat": OLD + 1},
])
def test_job_evidence_must_prove_finished_stale_and_consistent(changes):
    row = ended_task()
    row.update(changes)
    assert classify_work(snapshot([row]))["active"] == 1


def test_absent_job_id_with_job_evidence_is_blocker():
    assert classify_work(snapshot([task(job_state="success")]))["active"] == 1


@pytest.mark.parametrize("parent", ["success", "failed"])
@pytest.mark.parametrize("job_state", ["success", "failed"])
def test_ended_jobs_with_old_heartbeat_allow_historical_task(parent, job_state):
    row = ended_task(parent_state=parent)
    row["job_state"] = job_state
    assert classify_work(snapshot([row]))["historical_tasks"] == 1


def test_stale_running_job_residuals_are_visible_separately():
    result = classify_work(snapshot(jobs=[job(), job(job_id=2, references=0)]))
    assert result["active"] == 0
    assert result["historical_jobs"] == 2
    assert result["abandoned_orphan_jobs"] == 1


@pytest.mark.parametrize("changes", [
    {"heartbeat": NOW - HEARTBEAT_AGE}, {"heartbeat": NOW}, {"heartbeat": NOW + 1},
    {"heartbeat": None}, {"heartbeat": NOW - HISTORICAL_AGE + 1},
    {"start": None}, {"start": NOW - 1}, {"start": NOW + 1},
    {"end": NOW - 1}, {"state": None}, {"state": "unknown"},
    {"state": "success"}, {"unsafe_references": 1},
])
@pytest.mark.parametrize("references", [0, 1])
def test_local_job_fresh_unknown_or_runnable_evidence_blocks(changes, references):
    if references == 0 and changes.get("unsafe_references"):
        return
    row = job(references=references)
    row.update(changes)
    assert classify_work(snapshot(jobs=[row]))["active"] == 1


def test_orphan_resumed_heartbeat_or_new_runnable_reference_revokes_abandoned_status():
    raw = snapshot(jobs=[job(references=0)])
    assert classify_work(raw)["active"] == 0
    raw["local_jobs"][0]["heartbeat"] = NOW
    assert classify_work(raw)["active"] == 1
    raw["local_jobs"][0].update(heartbeat=OLD, references=1, unsafe_references=1)
    assert classify_work(raw)["active"] == 1


def test_abandoned_job_age_boundary_and_independent_run_gate():
    raw = snapshot(jobs=[job(start=NOW - HISTORICAL_AGE, heartbeat=NOW - HISTORICAL_AGE)], active_runs=1)
    result = classify_work(raw)
    assert result["historical_jobs"] == 1
    assert result["active"] == 1


def test_details_are_bounded_but_count_is_complete_and_sanitized():
    rows = [task(task_id=f"unsafe\n{i}", state="running") for i in range(MAX_BLOCKERS + 7)]
    result = classify_work(snapshot(rows))
    assert result["active"] == MAX_BLOCKERS + 7
    assert len(result["blockers"]) == MAX_BLOCKERS
    assert result["blockers_truncated"] == 7
    assert "\n" not in result["blockers"][0]["task_id"]


@pytest.mark.parametrize("field,value", [("now", float("nan")), ("now", False), ("now", None),
    ("active_runs", True), ("active_runs", -1), ("unstarted_tasks", -1),
    ("tasks", {}), ("local_jobs", None)])
def test_invalid_snapshot_fails_closed(field, value):
    raw = snapshot()
    raw[field] = value
    with pytest.raises(ValueError):
        classify_work(raw)


@pytest.mark.parametrize("field,value", [("map_index", True), ("map_index", -2),
    ("job_id", True), ("job_found", 0), ("updated", float("inf")), ("dag_id", "")])
def test_invalid_task_evidence_fails_closed(field, value):
    row = task()
    row[field] = value
    with pytest.raises(ValueError):
        classify_work(snapshot([row]))


def test_duplicate_or_missing_records_fail_closed():
    row = task()
    with pytest.raises(ValueError):
        classify_work(snapshot([row, deepcopy(row)]))
    del row["job_found"]
    with pytest.raises(ValueError):
        classify_work(snapshot([row]))
    with pytest.raises(ValueError):
        classify_work(snapshot(jobs=[job(), job()]))


def test_cleared_task_with_conclusively_ended_old_job_is_separate_history():
    result = classify_work(snapshot([ended_task(state=None)]))
    assert result["active"] == 0
    assert result["historical_tasks"] == 0
    assert result["historical_reset_tasks"] == 1


@pytest.mark.parametrize("changes", [
    {"job_found": False}, {"job_state": "running"}, {"job_end": None},
    {"job_heartbeat": NOW}, {"parent_state": None}, {"updated": NOW},
])
def test_cleared_task_does_not_hide_missing_or_live_job(changes):
    row = ended_task(state=None)
    row.update(changes)
    assert classify_work(snapshot([row]))["active"] == 1


@pytest.mark.parametrize("changes", [{"start": OLD + 2}, {"end": OLD + 1}])
def test_old_running_job_with_inconsistent_timeline_is_not_abandoned(changes):
    assert classify_work(snapshot(jobs=[job(**changes)]))["local_jobs"] == 1


@pytest.mark.parametrize("state", ["up_for_retry", None])
@pytest.mark.parametrize("start", [None, NOW + 1, NOW, NOW - HISTORICAL_AGE + 1, OLD + 1])
def test_historical_task_requires_old_consistent_job_start(state, start):
    row = ended_task(state=state)
    row["job_start"] = start
    assert classify_work(snapshot([row]))["blocking_tasks"] == 1


def test_historical_job_start_age_boundary_is_allowed_with_consistent_timeline():
    row = ended_task()
    row.update(job_start=NOW - HISTORICAL_AGE,
               job_heartbeat=NOW - HISTORICAL_AGE + 1, job_end=NOW - HISTORICAL_AGE + 2)
    assert classify_work(snapshot([row]))["historical_tasks"] == 1


@pytest.mark.parametrize("start", [None, NOW + 1, NOW, OLD + 1])
def test_terminal_job_with_missing_or_inverted_start_is_blocker(start):
    row = job(state="success", start=start, end=OLD, heartbeat=OLD)
    assert classify_work(snapshot(jobs=[row]))["local_jobs"] == 1


def test_unlinked_task_does_not_ignore_stray_job_start():
    assert classify_work(snapshot([task(job_start=OLD)]))["blocking_tasks"] == 1


@pytest.mark.parametrize("skew,blocked", [(0.05, False), (0.795361, False), (1, False), (1.001, True)])
def test_initial_heartbeat_skew_is_bounded_for_terminal_and_abandoned_jobs(skew, blocked):
    row = ended_task()
    row.update(job_start=OLD - 100, job_heartbeat=OLD - 100 - skew)
    assert bool(classify_work(snapshot([row]))["blocking_tasks"]) is blocked
    assert bool(classify_work(snapshot(jobs=[job(start=OLD - 100, heartbeat=OLD - 100 - skew)]))["local_jobs"]) is blocked


def test_initial_heartbeat_allowance_never_permits_start_after_end():
    row = ended_task()
    row.update(job_start=OLD - 0.01, job_heartbeat=OLD - 0.05, job_end=OLD - 0.02)
    assert classify_work(snapshot([row]))["blocking_tasks"] == 1
