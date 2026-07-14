"""Fail-closed source/Silver and separate master xref -> Gold topology."""

from __future__ import annotations

import ast
import importlib
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]


def _operator_calls(filename: str, operator_name: str) -> list[dict[str, ast.AST]]:
    tree = ast.parse((ROOT / "dags" / filename).read_text(encoding="utf-8"))
    calls = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = node.func.id if isinstance(node.func, ast.Name) else None
        if name == operator_name:
            calls.append({kw.arg: kw.value for kw in node.keywords if kw.arg})
    return calls


def _trigger_calls(filename: str) -> list[dict[str, ast.AST]]:
    return _operator_calls(filename, "TriggerDagRunOperator")


def _literal(node: ast.AST):
    return ast.literal_eval(node)


def _literal_trigger(filename: str, child_dag_id: str) -> dict[str, ast.AST]:
    matches = []
    for call in _trigger_calls(filename):
        child = call.get("trigger_dag_id")
        if isinstance(child, ast.Constant) and child.value == child_dag_id:
            matches.append(call)
    assert len(matches) == 1, (filename, child_dag_id, len(matches))
    return matches[0]


def _timedelta_hours(node: ast.AST) -> int:
    assert isinstance(node, ast.Call)
    assert isinstance(node.func, ast.Name) and node.func.id == "timedelta"
    kwargs = {kw.arg: _literal(kw.value) for kw in node.keywords}
    assert set(kwargs) == {"hours"}
    return kwargs["hours"]


@pytest.mark.unit
def test_silver_is_fbref_only_and_never_launches_xref_or_gold():
    publication_targets = [
        call["trigger_dag_id"].value
        for call in _trigger_calls("dag_transform_fbref_silver.py")
        if isinstance(call.get("trigger_dag_id"), ast.Constant)
    ]
    assert "dag_transform_xref" not in publication_targets
    assert "dag_transform_fbref_gold" not in publication_targets


@pytest.mark.unit
@pytest.mark.parametrize(
    ("filename", "child_dag_id", "timeout_hours"),
    [
        ("dag_ingest_fbref.py", "dag_transform_fbref_silver", 12),
        ("dag_backfill_fbref.py", "dag_transform_fbref_silver", 12),
        ("dag_replay_fbref.py", "dag_transform_fbref_silver", 12),
        ("dag_master_pipeline.py", "dag_transform_xref", 5),
        ("dag_master_pipeline.py", "dag_transform_fbref_gold", 12),
    ],
)
def test_blocking_fbref_handoffs_do_not_retry_reset_child_dags(
    filename,
    child_dag_id,
    timeout_hours,
):
    trigger = _literal_trigger(filename, child_dag_id)

    assert _literal(trigger["wait_for_completion"]) is True
    expected_reset = child_dag_id == "dag_transform_fbref_gold"
    assert _literal(trigger["reset_dag_run"]) is expected_reset
    assert _literal(trigger["retries"]) == 0
    assert _timedelta_hours(trigger["execution_timeout"]) == timeout_hours


@pytest.mark.unit
def test_fbref_child_runs_have_parent_unique_identity_and_never_share_ds():
    parents = {
        "dag_ingest_fbref.py": "dag_ingest_fbref",
        "dag_backfill_fbref.py": "dag_backfill_fbref",
        "dag_replay_fbref.py": "dag_replay_fbref",
    }
    rendered_ids = set()
    for filename, parent_dag_id in parents.items():
        trigger = _literal_trigger(filename, "dag_transform_fbref_silver")
        template = _literal(trigger["trigger_run_id"])
        assert "{{ dag.dag_id }}" in template
        assert "{{ run_id }}" in template
        assert _literal(trigger["logical_date"]) == "{{ ti.start_date }}"
        assert "execution_date" not in trigger
        rendered_ids.add(
            template.replace("{{ dag.dag_id }}", parent_dag_id).replace(
                "{{ run_id }}", "same-day-run"
            )
        )

    assert len(rendered_ids) == len(parents)

    xref = _literal_trigger("dag_master_pipeline.py", "dag_transform_xref")
    assert "{{ dag.dag_id }}" in _literal(xref["trigger_run_id"])
    assert "{{ run_id }}" in _literal(xref["trigger_run_id"])
    assert _literal(xref["logical_date"]) == "{{ ti.start_date }}"
    assert "execution_date" not in xref


@pytest.mark.unit
def test_silver_run_has_a_bounded_fbref_only_timeout():
    dag_calls = _operator_calls("dag_transform_fbref_silver.py", "DAG")
    assert len(dag_calls) == 1
    dag_timeout = _timedelta_hours(dag_calls[0]["dagrun_timeout"])
    assert dag_timeout >= 8


@pytest.mark.unit
def test_master_owns_one_fail_closed_xref_and_one_final_gold_launch():
    calls = _trigger_calls("dag_master_pipeline.py")
    literal_targets = [
        call["trigger_dag_id"].value
        for call in calls
        if isinstance(call.get("trigger_dag_id"), ast.Constant)
    ]
    assert literal_targets.count("dag_transform_xref") == 1
    assert literal_targets.count("dag_transform_fbref_gold") == 1

    xref = _literal_trigger("dag_master_pipeline.py", "dag_transform_xref")
    assert _literal(xref["wait_for_completion"]) is True
    assert _literal(xref["allowed_states"]) == ["success"]
    assert _literal(xref["failed_states"]) == ["failed"]
    assert _literal(xref["trigger_rule"]) == "all_success"

    gold = _literal_trigger("dag_master_pipeline.py", "dag_transform_fbref_gold")
    assert _literal(gold["wait_for_completion"]) is True
    assert _literal(gold["allowed_states"]) == ["success"]
    assert _literal(gold["failed_states"]) == ["failed"]
    assert _literal(gold["trigger_rule"]) == "all_success"


@pytest.mark.unit
def test_master_waits_for_exact_scheduled_fbref_run_fail_closed():
    calls = _operator_calls("dag_master_pipeline.py", "ExternalTaskSensor")
    assert len(calls) == 1
    sensor = calls[0]

    assert _literal(sensor["task_id"]) == "wait_for_scheduled_fbref"
    assert _literal(sensor["external_dag_id"]) == "dag_ingest_fbref"
    assert _literal(sensor["external_task_id"]) is None
    assert _literal(sensor["allowed_states"]) == ["success"]
    assert _literal(sensor["failed_states"]) == ["failed"]
    assert _literal(sensor["mode"]) == "reschedule"
    assert _literal(sensor["check_existence"]) is True

    delta = sensor["execution_delta"]
    assert isinstance(delta, ast.Call)
    assert isinstance(delta.func, ast.Name) and delta.func.id == "timedelta"
    assert {
        kw.arg: _literal(kw.value) for kw in delta.keywords
    } == {"hours": 8}


@pytest.mark.unit
def test_master_senses_scheduled_fbref_then_runs_direct_gold_prerequisites():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_master_pipeline", None)
    sys.modules.pop("dags.dag_master_pipeline", None)
    module = importlib.import_module("dag_master_pipeline")

    assert "dag_ingest_fbref" not in module.TRIGGERED_INGESTION_DAGS
    assert module.SCHEDULED_INGESTION_DAGS == ["dag_ingest_fbref"]
    assert "dag_ingest_fbref" in module.INGESTION_DAGS
    tasks = {task.task_id: task for task in PythonOperator._instances}

    assert "ingestion_triggers.trigger_fbref" not in tasks
    fbref_sensor = tasks["wait_for_scheduled_fbref"]
    scope = tasks["resolve_fbref_publication_scope"]
    xref = tasks["trigger_xref_transforms"]
    e3 = tasks["trigger_e3_transforms"]
    e4 = tasks["trigger_e4_transforms"]
    gold = tasks["trigger_fbref_gold"]
    check = tasks["check_pipeline_success"]

    assert scope.task_id in fbref_sensor.downstream_task_ids
    assert xref.task_id in scope.downstream_task_ids
    assert e3.task_id in xref.downstream_task_ids
    assert e4.task_id in e3.downstream_task_ids
    assert {
        "trigger_silver_transfermarkt",
        "trigger_silver_capology",
        "trigger_silver_sofifa",
    } <= gold.upstream_task_ids
    assert check.task_id in gold.downstream_task_ids
