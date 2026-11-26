"""Scheduled publication waits for its source files. Manual runs replay local packages."""

from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


ENTRY = Path(__file__).resolve().parents[3] / "dags" / "dag_ingest_transfermarkt.py"


def _load_entry(monkeypatch, engine):
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    # Stubs collect instances across parses; keep this verify-only import private to this test.
    monkeypatch.setattr(PythonOperator, "_instances", [])
    monkeypatch.setattr(BashOperator, "_instances", [])
    if engine is None:
        monkeypatch.delenv("TM_INGEST_ENGINE", raising=False)
    else:
        monkeypatch.setenv("TM_INGEST_ENGINE", engine)
    spec = importlib.util.spec_from_file_location("_tm_ingest_engine_test", ENTRY)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag, PythonOperator._instances, BashOperator._instances


def test_core_engine_waits_for_the_planned_cycle_before_its_bronze_task(monkeypatch):
    from airflow.sensors.python import PythonSensor

    dag, tasks, commands = _load_entry(monkeypatch, "core")
    assert dag.dag_id == "dag_ingest_transfermarkt"
    assert dag.schedule == "0 4 * * *"
    assert dag._dag_kwargs["catchup"] is False
    assert dag._dag_kwargs["is_paused_upon_creation"] is True
    assert dag._dag_kwargs["max_active_runs"] == 1
    assert commands == []
    sensor, task = tasks
    assert isinstance(sensor, PythonSensor)
    assert (sensor.task_id, task.task_id) == ("wait_for_cycle_inputs", "run_bronze_cycle")
    assert sensor.downstream_task_ids == {task.task_id}
    assert task.upstream_task_ids == {sensor.task_id}
    assert sensor._init_kwargs["mode"] == "reschedule"
    assert sensor._init_kwargs["poke_interval"] == 60
    assert sensor._init_kwargs["timeout"] == 3600
    assert sensor._init_kwargs["pool"] == "transfermarkt_control"
    assert sensor._init_kwargs["retries"] == 0
    assert task._init_kwargs["pool"] == "transfermarkt_control"
    assert task._init_kwargs["retries"] == 0


@pytest.mark.parametrize("engine", [None, "native_v2"])
def test_legacy_deployments_keep_the_existing_graph(monkeypatch, engine):
    dag, tasks, commands = _load_entry(monkeypatch, engine)
    assert dag.dag_id == "dag_ingest_transfermarkt"
    assert {task.task_id: task.upstream_task_ids for task in tasks + commands} == {
        "daily_bronze_dq": set(),
        "preflight_reader_route_for_paid_cycle": set(),
        "plan_exact_scopes": {"preflight_reader_route_for_paid_cycle"},
        "run_exact_child_cycle": {"plan_exact_scopes"},
        "validate_scope_set": {"run_exact_child_cycle"},
    }


def test_unknown_engine_fails_to_parse(monkeypatch):
    with pytest.raises(ValueError, match="Unknown Transfermarkt ingest engine"):
        _load_entry(monkeypatch, "typo")


@pytest.fixture
def routes(monkeypatch):
    from dags.scripts import run_transfermarkt_bronze_cycle
    from dags.utils import transfermarkt_core_rehearsal
    from utils import transfermarkt_core_dag

    rehearsal = Mock(return_value={"rows": 25})
    publish = Mock(return_value={"rows": 48})
    monkeypatch.setattr(transfermarkt_core_rehearsal, "run_rehearsal", rehearsal)
    monkeypatch.setattr(run_transfermarkt_bronze_cycle, "publish_cycle", publish)
    return transfermarkt_core_dag, rehearsal, publish


def test_manual_run_only_calls_rehearsal(routes):
    module, rehearsal, publish = routes
    run = SimpleNamespace(
        run_type=SimpleNamespace(value="manual"), conf={"write": True}
    )
    args = dict(package="one", league="GB1", season="2627")
    assert module.wait_for_cycle_inputs(dag_run=run) is True
    assert module.run_bronze_cycle(dag_run=run, params=args) == rehearsal.return_value
    rehearsal.assert_called_once_with(
        module.STATE_ROOT / "replays", "one", "GB1", "2627"
    )
    publish.assert_not_called()


def test_scheduled_run_uses_scheduler_identity_and_fixed_policy(routes):
    module, rehearsal, publish = routes
    run = SimpleNamespace(
        run_type="scheduled",
        conf={},
        run_id="scheduled__2026-11-12",
        start_date=datetime(2026, 11, 12, 4, tzinfo=timezone.utc),
    )
    assert (
        module.run_bronze_cycle(dag_run=run, params={"package": "manual"})
        == publish.return_value
    )
    publish.assert_called_once_with(
        module.STATE_ROOT,
        module.POLICY_FILE,
        cycle_id=run.run_id,
        started_at=run.start_date,
    )
    rehearsal.assert_not_called()


def test_scheduled_sensor_publishes_its_pinned_plan_and_waits_for_its_captures(routes, monkeypatch, tmp_path):
    from dags.scripts.run_transfermarkt_bronze_cycle import cycle_directory

    module, rehearsal, publish = routes
    monkeypatch.setattr(module, "STATE_ROOT", tmp_path)
    monkeypatch.setenv("TM_STANDING_POLICY_ENABLED", "true")
    started = datetime.now(timezone.utc)
    run = SimpleNamespace(run_type="scheduled", conf={}, run_id="scheduled__this-run", start_date=started)
    scope = {"scope_id": "coaches", "league": "GB1", "season": "2627", "edition_id": "2026",
             "coaches": ["42:example-coach"]}
    registry = json.dumps({"valid_from": (started - timedelta(days=1)).isoformat(),
                           "valid_until": (started + timedelta(days=1)).isoformat(), "scopes": [scope]}).encode()
    digest = hashlib.sha256(registry).hexdigest()
    (tmp_path / "registry").mkdir()
    (tmp_path / "registry" / f"{digest}.json").write_bytes(registry)
    monkeypatch.setenv("TM_CORE_INVENTORY_SHA256", digest)
    directory = cycle_directory(tmp_path, run.run_id)
    other = cycle_directory(tmp_path, "scheduled__previous-run")
    other.mkdir(parents=True)
    for name in ("captures.json", "plan.json"):
        (other / name).write_text("{}")
    assert not directory.exists()
    assert module.wait_for_cycle_inputs(dag_run=run) is False
    plan_bytes = (directory / "plan.json").read_bytes()
    assert json.loads(plan_bytes) == {"cycle_id": run.run_id, "registry_snapshot_id": digest, "scopes": [scope]}
    pending = directory / "captures.json.tmp"
    pending.write_text("{}")
    assert module.wait_for_cycle_inputs(dag_run=run) is False
    pending.replace(directory / "captures.json")
    assert module.wait_for_cycle_inputs(dag_run=run) is True
    assert (directory / "plan.json").read_bytes() == plan_bytes
    rehearsal.assert_not_called()
    publish.assert_not_called()


def test_finished_capture_file_cannot_bypass_a_changed_registry(routes, monkeypatch, tmp_path):
    from dags.scripts.run_transfermarkt_bronze_cycle import cycle_directory

    module, rehearsal, publish = routes
    monkeypatch.setattr(module, "STATE_ROOT", tmp_path)
    monkeypatch.setenv("TM_STANDING_POLICY_ENABLED", "true")
    digest = hashlib.sha256(b"original registry").hexdigest()
    monkeypatch.setenv("TM_CORE_INVENTORY_SHA256", digest)
    run = SimpleNamespace(run_type="scheduled", conf={}, run_id="scheduled__changed",
                          start_date=datetime.now(timezone.utc))
    (tmp_path / "registry").mkdir()
    (tmp_path / "registry" / f"{digest}.json").write_bytes(b"changed registry")
    directory = cycle_directory(tmp_path, run.run_id)
    directory.mkdir(parents=True)
    (directory / "captures.json").write_text("{}")
    with pytest.raises(ValueError, match="digest mismatch"):
        module.wait_for_cycle_inputs(dag_run=run)
    assert not (directory / "plan.json").exists()
    rehearsal.assert_not_called()
    publish.assert_not_called()


def test_disabled_schedule_fails_before_waiting(routes, monkeypatch):
    module, rehearsal, publish = routes
    monkeypatch.delenv("TM_STANDING_POLICY_ENABLED", raising=False)
    run = SimpleNamespace(run_type="scheduled", conf={}, run_id="scheduled__closed")
    with pytest.raises(ValueError, match="writes are disabled"):
        module.wait_for_cycle_inputs(dag_run=run)
    rehearsal.assert_not_called()
    publish.assert_not_called()


@pytest.mark.parametrize(
    ("kind", "conf"),
    [
        ("scheduled", {"package": "manual"}),
        ("backfill", {}),
        ("dataset_triggered", {}),
        ("unknown", {}),
    ],
)
def test_other_run_types_and_schedule_overrides_cannot_publish(routes, kind, conf):
    module, rehearsal, publish = routes
    run = SimpleNamespace(run_type=kind, conf=conf)
    with pytest.raises(ValueError, match="scheduled run without overrides"):
        module.wait_for_cycle_inputs(dag_run=run)
    with pytest.raises(ValueError, match="scheduled run without overrides"):
        module.run_bronze_cycle(dag_run=run)
    rehearsal.assert_not_called()
    publish.assert_not_called()
