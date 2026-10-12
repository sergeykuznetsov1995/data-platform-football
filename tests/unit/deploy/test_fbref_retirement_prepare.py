"""Preparation must never import source code or emit a production commit."""
from copy import deepcopy
import json

import pytest

from deploy.fbref_retirement import prepare as p


@pytest.fixture
def snapshot():
    name = "fbref_silver__dag_ingest_fbref__scheduled__2026-10-12T00:00:00+00:00"
    return {"checked_at": "2026-10-12T12:00:00Z",
            "runs": [{"id": 1, "dag_id": p.SILVER, "run_id": name, "state": "queued", "start_date": None, "end_date": None, "clear_number": 0, "creating_job_id": None}],
            "tasks": [{"dag_id": p.SILVER, "run_id": name, "task_id": "a", "map_index": -1,
                       **{k: None for k in p.TASK_UNSET}, "try_number": 0}],
            "parents": [{"id": 2, "dag_id": "dag_ingest_fbref", "run_id": "scheduled__2026-10-12T00:00:00+00:00", "state": "success"}]}


def test_queue_accepts_only_proven_unstarted_work(snapshot):
    assert p.validate_queue(snapshot) == snapshot
    sql = p.cancellation_sql(snapshot, "a" * 64)
    assert sql.rstrip().endswith("ROLLBACK;")
    assert "\nCOMMIT;" not in sql


@pytest.mark.parametrize("section,key,value", [
    ("runs", "state", "running"), ("runs", "dag_id", "other"),
    ("runs", "start_date", "2026-10-12T12:00:00Z"), ("runs", "end_date", "2026-10-12T12:00:00Z"),
    ("runs", "id", True), ("runs", "run_id", "fbref_silver__alien__run"),
    ("runs", "clear_number", 1), ("runs", "creating_job_id", 9),
    ("tasks", "try_number", 4), ("tasks", "queued_by_job_id", 9),
    ("tasks", "queued_dttm", "2026-10-12T12:00:00Z"),
    ("tasks", "external_executor_id", "executor-run"), ("tasks", "trigger_id", 7),
    ("tasks", "next_method", "execute_complete"), ("tasks", "pid", 123),
    ("tasks", "state", "skipped"), ("tasks", "job_id", 7),
    ("tasks", "start_date", "2026-10-12T12:00:00Z"), ("tasks", "end_date", "2026-10-12T12:00:00Z"),
    ("tasks", "dag_id", "other"), ("tasks", "map_index", True),
    ("parents", "state", "running"), ("parents", "run_id", "alien"),
])
def test_rejects_ambiguous_or_foreign_rows(snapshot, section, key, value):
    snapshot[section][0][key] = value
    with pytest.raises(ValueError):
        p.validate_queue(snapshot)


@pytest.mark.parametrize("section", ["runs", "tasks", "parents"])
def test_rejects_duplicate_evidence(snapshot, section):
    snapshot[section].append(deepcopy(snapshot[section][0]))
    with pytest.raises(ValueError):
        p.validate_queue(snapshot)


@pytest.mark.parametrize("section", ["runs", "tasks", "parents"])
def test_rejects_missing_evidence(snapshot, section):
    snapshot[section] = []
    with pytest.raises(ValueError):
        p.validate_queue(snapshot)


@pytest.mark.parametrize("section,key", [("runs", "start_date"), ("runs", "clear_number"), ("tasks", "try_number"), ("tasks", "queued_by_job_id"), ("tasks", "job_id"), ("tasks", "state")])
def test_missing_fields_are_not_null_proof(snapshot, section, key):
    del snapshot[section][0][key]
    with pytest.raises(ValueError):
        p.validate_queue(snapshot)


def test_sql_escapes_row_text_as_data(snapshot):
    snapshot["tasks"][0]["task_id"] = "evil'; RAISE EXCEPTION 'oops"
    sql = p.cancellation_sql(snapshot, "b" * 64)
    assert "evil''; RAISE EXCEPTION ''oops" in sql


def test_queue_bundle_preserves_before_bytes_and_authorizes_nothing(snapshot, tmp_path, monkeypatch):
    source = tmp_path / "snapshot.json"
    data = json.dumps(snapshot).encode(); source.write_bytes(data)
    output = tmp_path / "output"
    monkeypatch.setattr(p, "output_directory", lambda path: (output.mkdir(), output)[1])
    manifest = p.prepare_queue(source, output)
    assert (output / "snapshot.json").read_bytes() == data
    assert manifest["production_execution_authorized"] is False
    assert manifest["execution_qualified"] is False
    assert manifest["run_count"] == manifest["task_count"] == 1
    assert p.digest((output / "manifest.json").read_bytes()) in (output / "rehearsal.sql").read_text()


def test_source_drift_fails_before_any_output(tmp_path):
    for name in p.BEFORE | p.SUPPORT:
        target = tmp_path / "source" / name; target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(b"unexpected")
    with pytest.raises(ValueError, match="source drift"):
        p.prepare_payload(tmp_path / "source", tmp_path / "output")
    assert not (tmp_path / "output").exists()


def test_output_cannot_target_runtime_or_arbitrary_directory(tmp_path):
    with pytest.raises(ValueError, match="Football"):
        p.output_directory(tmp_path / "runtime")


def test_read_rejects_symlink_and_hardlink(tmp_path):
    import os
    original = tmp_path / "file"; original.write_bytes(b"ok")
    link = tmp_path / "link"; link.symlink_to(original)
    with pytest.raises(ValueError):
        p.read_regular(link)
    os.link(original, tmp_path / "hardlink")
    with pytest.raises(ValueError):
        p.read_regular(original)
