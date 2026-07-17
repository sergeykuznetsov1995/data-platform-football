"""Topology and hard-profile tests for the FBref Bronze acceptance DAGs."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from dags.utils import fbref_bronze_acceptance_tasks as acceptance


def _load(module_name: str):
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop(module_name, None)
    sys.modules.pop(f"dags.{module_name}", None)
    module = importlib.import_module(module_name)
    tasks = {task.task_id: task for task in PythonOperator._instances}
    return module, tasks


@pytest.fixture()
def acceptance_dag():
    return _load("dag_accept_fbref_bronze")


@pytest.fixture()
def replay_dag():
    return _load("dag_replay_fbref_bronze")


@pytest.mark.unit
def test_acceptance_dag_is_manual_nonpublishing_and_fixed_profile(
    acceptance_dag,
):
    module, tasks = acceptance_dag
    assert module.dag.dag_id == "dag_accept_fbref_bronze"
    assert module.dag.schedule is None
    assert set(tasks) == {
        "validate_production_readiness",
        "initialize_run",
        "acquire_publication_lock",
        "select_acceptance_cohort",
        "capture_raw_baseline",
        "run_live_wave",
        "audit_raw_integrity",
        "validate_acceptance_run",
        "release_publication_lock",
    }
    assert not any("silver" in task_id or "gold" in task_id for task_id in tasks)
    assert not any("trigger" in task_id for task_id in tasks)
    scope = module.dag._dag_kwargs["params"]["scope"]
    assert scope.default == "current"
    assert scope._kw["enum"] == ["current", "history"]
    assert acceptance.ACCEPTANCE_REQUEST_LIMIT == 100
    assert acceptance.ACCEPTANCE_BYTE_LIMIT_MB == 50
    assert acceptance.ACCEPTANCE_SHARD_SIZE == 25


@pytest.mark.unit
def test_acceptance_dag_is_one_strictly_sequential_live_batch(acceptance_dag):
    _, tasks = acceptance_dag
    ordered = [
        "validate_production_readiness",
        "initialize_run",
        "acquire_publication_lock",
        "select_acceptance_cohort",
        "capture_raw_baseline",
        "run_live_wave",
        "audit_raw_integrity",
        "validate_acceptance_run",
        "release_publication_lock",
    ]
    for current, following in zip(ordered, ordered[1:]):
        assert tasks[current].downstream_task_ids == {following}
    assert tasks["run_live_wave"].python_callable.__name__ == (
        "run_fbref_acceptance_live_wave"
    )
    assert tasks["acquire_publication_lock"].python_callable.__name__ == (
        "acquire_fbref_acceptance_publication_lock"
    )
    assert tasks["release_publication_lock"]._init_kwargs[
        "trigger_rule"
    ] == "all_done"
    assert all(
        task._init_kwargs["trigger_rule"] == "all_success"
        for task_id, task in tasks.items()
        if task_id != "release_publication_lock"
    )


@pytest.mark.unit
def test_replay_dag_has_required_source_and_no_network_tasks(replay_dag):
    module, tasks = replay_dag
    assert module.dag.dag_id == "dag_replay_fbref_bronze"
    assert module.dag.schedule is None
    assert set(tasks) == {
        "validate_production_readiness",
        "initialize_run",
        "acquire_publication_lock",
        "capture_raw_baseline",
        "parse_source_cohort",
        "audit_raw_integrity",
        "validate_acceptance_run",
        "release_publication_lock",
    }
    assert not any(
        task_id.startswith(("fetch", "seed")) for task_id in tasks
    )
    assert not any("silver" in task_id or "gold" in task_id for task_id in tasks)
    source = module.dag._dag_kwargs["params"]["source_control_run_id"]
    assert source.default is None
    assert source._kw["type"] == ["null", "string"]
    assert source._kw["minLength"] == source._kw["maxLength"] == 36


@pytest.mark.unit
def test_replay_budget_is_physically_zero_and_topology_is_sequential(replay_dag):
    _, tasks = replay_dag
    readiness = tasks["validate_production_readiness"]
    assert readiness.python_callable.__name__ == (
        "validate_fbref_acceptance_replay_readiness"
    )
    assert readiness.op_kwargs == {}
    assert tasks["initialize_run"].python_callable.__name__ == (
        "initialize_fbref_acceptance_replay_run"
    )
    ordered = [
        "validate_production_readiness",
        "initialize_run",
        "acquire_publication_lock",
        "capture_raw_baseline",
        "parse_source_cohort",
        "audit_raw_integrity",
        "validate_acceptance_run",
        "release_publication_lock",
    ]
    for current, following in zip(ordered, ordered[1:]):
        assert tasks[current].downstream_task_ids == {following}
    assert tasks["parse_source_cohort"].python_callable.__name__ == (
        "parse_fbref_acceptance_replay"
    )
    assert tasks["acquire_publication_lock"].python_callable.__name__ == (
        "acquire_fbref_acceptance_publication_lock"
    )
    assert tasks["validate_acceptance_run"].op_kwargs["replay"] is True


@pytest.mark.unit
def test_real_airflow_dagbag_accepts_both_files_if_available():
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("Airflow not installed")
    if not hasattr(DagBag, "process_file"):
        pytest.skip("Stubbed Airflow detected")

    dags_dir = Path(__file__).resolve().parents[3] / "dags"
    bag = DagBag(dag_folder=str(dags_dir), include_examples=False)
    relevant = {
        path: error
        for path, error in bag.import_errors.items()
        if "fbref_bronze" in path
    }
    assert relevant == {}
    assert "dag_accept_fbref_bronze" in bag.dags
    assert "dag_replay_fbref_bronze" in bag.dags
