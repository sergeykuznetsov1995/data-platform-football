"""Topology contracts for the standalone SofaScore daily orchestrator (#951)."""

from __future__ import annotations

import importlib
import sys

import pytest


def _reload_pipeline():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    for name in (
        "dag_sofascore_pipeline",
        "dags.dag_sofascore_pipeline",
        "dag_master_pipeline",
        "dags.dag_master_pipeline",
    ):
        sys.modules.pop(name, None)
    return importlib.import_module("dag_sofascore_pipeline")


def _task(task_id: str):
    # Importing dag_sofascore_pipeline pulls in dag_master_pipeline, whose
    # stub tasks share ids (trigger_xref_transforms, ...). The pipeline's own
    # instances are created after that import — take the LAST match.
    from airflow.operators.python import PythonOperator

    matches = [
        task for task in PythonOperator._instances if task.task_id == task_id
    ]
    assert matches, f"no stub task {task_id!r} was created"
    return matches[-1]


@pytest.mark.unit
def test_chain_is_ingest_and_fbref_scope_then_xref_then_e3_then_e4():
    _reload_pipeline()

    ingest = _task("trigger_sofascore_ingest")
    sensor = _task("wait_for_scheduled_fbref")
    scope = _task("resolve_fbref_publication_scope")
    xref = _task("trigger_xref_transforms")
    e3 = _task("trigger_e3_transforms")
    e4 = _task("trigger_e4_transforms")

    assert sensor.task_id in scope.upstream_task_ids
    assert ingest.upstream_task_ids == set()
    assert scope.task_id in xref.upstream_task_ids
    assert ingest.task_id in xref.upstream_task_ids
    assert xref.task_id in e3.upstream_task_ids
    assert e3.task_id in e4.upstream_task_ids


@pytest.mark.unit
def test_every_child_trigger_is_fail_closed_and_waits():
    _reload_pipeline()

    for task_id in (
        "trigger_sofascore_ingest",
        "trigger_xref_transforms",
        "trigger_e3_transforms",
        "trigger_e4_transforms",
    ):
        kwargs = _task(task_id)._init_kwargs
        assert kwargs["wait_for_completion"] is True
        assert kwargs["allowed_states"] == ["success"]
        assert kwargs["failed_states"] == ["failed"]
        assert kwargs["retries"] == 0


@pytest.mark.unit
def test_ingest_conf_carries_the_saturday_player_gate_interval():
    _reload_pipeline()

    kwargs = _task("trigger_sofascore_ingest")._init_kwargs
    assert kwargs["conf"] == {
        "master_data_interval_end": "{{ data_interval_end }}"
    }


@pytest.mark.unit
def test_xref_conf_pins_the_scheduled_fbref_generation():
    _reload_pipeline()

    kwargs = _task("trigger_xref_transforms")._init_kwargs
    conf = kwargs["conf"]
    assert conf["publication_owner"] == "dag_sofascore_pipeline"
    assert conf["fbref_source_dag_id"] == "dag_ingest_fbref"
    assert "resolve_fbref_publication_scope" in conf["fbref_control_run_id"]
    # A distinct run-id namespace: the master uses master_xref__...; colliding
    # ids would make the two orchestrators steal each other's child runs.
    assert kwargs["trigger_run_id"].startswith("sofascore_xref__")


@pytest.mark.unit
def test_pipeline_runs_in_the_masters_daily_slot():
    # resolve_scheduled_fbref_control_run subtracts 8h from logical_date to
    # find the 06:00 FBref run: the slot must stay in lockstep with it.
    from utils.config import SCHEDULES

    assert SCHEDULES["dag_sofascore_pipeline"] == "0 14 * * *"

    _reload_pipeline()
    sensor_kwargs = _task("wait_for_scheduled_fbref")._init_kwargs
    assert sensor_kwargs["external_dag_id"] == "dag_ingest_fbref"
    assert sensor_kwargs["execution_delta"].total_seconds() == 8 * 3600


@pytest.mark.unit
def test_master_no_longer_triggers_sofascore():
    # Mutual-exclusion contract (#951): exactly one orchestrator owns the
    # SofaScore daily slot.
    _reload_pipeline()
    master = importlib.import_module("dag_master_pipeline")

    assert "dag_ingest_sofascore" not in master.TRIGGERED_INGESTION_DAGS


@pytest.mark.unit
def test_dag_has_an_explicit_start_date():
    # Regression: DEFAULT_ARGS carries no start_date, and Airflow raises
    # "DAG is missing the start_date parameter" at construction. Stub-based
    # topology tests don't run that validation, so assert it directly — a live
    # DagBag parse caught this after deploy.
    module = _reload_pipeline()

    assert module.dag._dag_kwargs.get("start_date") is not None


@pytest.mark.unit
def test_module_does_not_import_master_dag_at_top_level():
    # Regression: `from dag_master_pipeline import ...` at module top level
    # re-registers the master DAG under this file during DagBag parsing
    # (AirflowDagDuplicatedIdException). The helper must be resolved lazily
    # inside the callable, so the master symbol must NOT leak into this
    # module's globals.
    module = _reload_pipeline()

    assert not hasattr(module, "resolve_scheduled_fbref_control_run")
    # The wrapper callable that does the deferred import must exist instead.
    assert callable(module.resolve_fbref_publication_scope)
