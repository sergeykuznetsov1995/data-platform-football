"""Fail-closed contracts for required sources through the master DAG."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
import sys

import pytest


def _reload_master():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_master_pipeline", None)
    sys.modules.pop("dags.dag_master_pipeline", None)
    return importlib.import_module("dag_master_pipeline")


def _task(task_id: str):
    from airflow.operators.python import PythonOperator

    return next(task for task in PythonOperator._instances if task.task_id == task_id)


def _dag_run(**states: str):
    return SimpleNamespace(
        get_task_instances=lambda: [
            SimpleNamespace(task_id=task_id, state=state)
            for task_id, state in states.items()
        ]
    )


def test_whoscored_child_failure_is_not_an_allowed_success_state():
    _reload_master()
    whoscored = _task("ingestion_triggers.trigger_whoscored")

    assert whoscored._init_kwargs["allowed_states"] == ["success"]
    assert whoscored._init_kwargs["failed_states"] == ["failed"]


def test_fotmob_child_failure_is_not_an_allowed_success_state():
    _reload_master()
    fotmob = _task("ingestion_triggers.trigger_fotmob")

    assert fotmob._init_kwargs["allowed_states"] == ["success"]
    assert fotmob._init_kwargs["failed_states"] == ["failed"]


@pytest.mark.parametrize(
    "task_id",
    [
        "wait_for_scheduled_fbref",
        "trigger_e3_transforms",
        "trigger_fbref_gold",
    ],
)
def test_required_publication_child_failure_is_not_an_allowed_state(task_id):
    _reload_master()
    trigger = _task(task_id)

    assert trigger._init_kwargs["allowed_states"] == ["success"]
    assert trigger._init_kwargs["failed_states"] == ["failed"]


def test_required_source_gate_waits_for_all_ingestion_and_blocks_transforms():
    _reload_master()
    gate = _task("validate_required_sources")
    fbref_sensor = _task("wait_for_scheduled_fbref")
    e3 = _task("trigger_e3_transforms")

    # #933: FBref is externally scheduled at 06:00 and only sensed here — it
    # must not appear among the master-triggered ingestion tasks.
    expected_ingestion = {
        "ingestion_triggers.trigger_fotmob",
        "ingestion_triggers.trigger_matchhistory",
        "ingestion_triggers.trigger_understat",
        "ingestion_triggers.trigger_whoscored",
        "ingestion_triggers.trigger_sofascore",
        "ingestion_triggers.trigger_espn",
        "ingestion_triggers.trigger_clubelo",
    }
    assert expected_ingestion <= gate.upstream_task_ids
    assert "ingestion_triggers.trigger_fbref" not in gate.upstream_task_ids
    assert gate._init_kwargs["trigger_rule"] == "all_done"
    assert gate.task_id in fbref_sensor.upstream_task_ids
    assert fbref_sensor.task_id in e3.upstream_task_ids
    assert fbref_sensor._init_kwargs["external_dag_id"] == "dag_ingest_fbref"
    assert e3._init_kwargs["trigger_rule"] == "all_success"


@pytest.mark.parametrize("state", ["failed", "upstream_failed", "skipped", "none"])
def test_required_source_gate_rejects_every_non_success_state(state):
    module = _reload_master()
    from airflow.exceptions import AirflowException

    run = _dag_run(
        **{
            "ingestion_triggers.trigger_fotmob": "success",
            "ingestion_triggers.trigger_whoscored": state,
        }
    )
    with pytest.raises(AirflowException, match="downstream transforms are blocked"):
        module.enforce_required_source_success(dag_run=run)


def test_required_source_gate_rejects_missing_trigger_evidence():
    module = _reload_master()
    from airflow.exceptions import AirflowException

    with pytest.raises(AirflowException, match="dag_ingest_whoscored=missing"):
        module.enforce_required_source_success(dag_run=_dag_run())


def test_required_source_gate_accepts_exact_current_master_success():
    module = _reload_master()
    run = _dag_run(
        **{
            "ingestion_triggers.trigger_fotmob": "success",
            "ingestion_triggers.trigger_whoscored": "success",
        }
    )

    assert module.enforce_required_source_success(dag_run=run) == {
        "dag_ingest_fotmob": "success",
        "dag_ingest_whoscored": "success",
    }


@pytest.mark.parametrize("state", ["failed", "upstream_failed", "skipped", "none"])
def test_required_publication_gate_rejects_every_non_success_state(state):
    module = _reload_master()
    from airflow.exceptions import AirflowException

    run = _dag_run(
        wait_for_scheduled_fbref="success",
        trigger_e3_transforms=state,
        trigger_fbref_gold="success",
    )
    with pytest.raises(
        AirflowException,
        match="Required publication transform did not complete successfully",
    ):
        module.enforce_required_publication_success(dag_run=run)


@pytest.mark.parametrize("state", ["failed", "upstream_failed", "skipped", "none"])
def test_fbref_silver_publication_rejects_every_non_success_state(state):
    module = _reload_master()
    from airflow.exceptions import AirflowException

    # #933: the sensed scheduled FBref run is the Silver/xref evidence — the
    # external DAG cannot succeed until its blocking Silver -> xref chain does.
    run = _dag_run(
        wait_for_scheduled_fbref=state,
        trigger_e3_transforms="success",
        trigger_fbref_gold="success",
    )
    with pytest.raises(
        AirflowException,
        match=f"dag_ingest_fbref={state}",
    ):
        module.enforce_required_publication_success(dag_run=run)


def test_fbref_silver_publication_rejects_missing_current_run_evidence():
    module = _reload_master()
    from airflow.exceptions import AirflowException

    run = _dag_run(
        trigger_e3_transforms="success",
        trigger_fbref_gold="success",
    )
    with pytest.raises(
        AirflowException,
        match="dag_ingest_fbref=missing",
    ):
        module.enforce_required_publication_success(dag_run=run)


def test_required_publication_gate_accepts_exact_current_master_success():
    module = _reload_master()
    run = _dag_run(
        wait_for_scheduled_fbref="success",
        trigger_e3_transforms="success",
        trigger_fbref_gold="success",
    )

    assert module.enforce_required_publication_success(dag_run=run) == {
        "dag_ingest_fbref": "success",
        "dag_transform_e3": "success",
        "dag_transform_fbref_gold": "success",
    }


def test_no_downstream_publication_task_uses_all_done():
    _reload_master()
    for task_id in (
        "trigger_e3_transforms",
        "trigger_e4_transforms",
        "trigger_silver_transfermarkt",
        "trigger_silver_capology",
        "trigger_silver_sofifa",
    ):
        assert _task(task_id)._init_kwargs["trigger_rule"] == "all_success"


def test_terminal_check_reuses_required_source_gate():
    module = _reload_master()
    from airflow.exceptions import AirflowException

    run = _dag_run(
        **{
            "ingestion_triggers.trigger_fotmob": "success",
            "ingestion_triggers.trigger_whoscored": "failed",
        }
    )
    with pytest.raises(AirflowException, match="downstream transforms are blocked"):
        module.check_pipeline_success(dag_run=run)


def test_terminal_check_rejects_failed_downstream_publication():
    module = _reload_master()
    from airflow.exceptions import AirflowException

    run = _dag_run(
        **{
            "ingestion_triggers.trigger_fotmob": "success",
            "ingestion_triggers.trigger_whoscored": "success",
            "wait_for_scheduled_fbref": "success",
            "trigger_e3_transforms": "failed",
            "trigger_fbref_gold": "upstream_failed",
        }
    )
    with pytest.raises(AirflowException, match="dag_transform_e3=failed"):
        module.check_pipeline_success(dag_run=run)
