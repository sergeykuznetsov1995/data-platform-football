"""Fail-closed validation wiring for the FotMob Silver DAG."""

import ast
import importlib
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[3]
pytestmark = pytest.mark.unit


def test_silver_validation_cannot_mask_failed_transform_with_all_done():
    """A failed transform must prevent both validation tasks from succeeding."""

    path = ROOT / "dags" / "dag_transform_fotmob_silver.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    trigger_rules = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        kwargs = {keyword.arg: keyword.value for keyword in node.keywords if keyword.arg}
        task = kwargs.get("task_id")
        if not isinstance(task, ast.Constant):
            continue
        if task.value not in {"validate_silver", "validate_silver_quality"}:
            continue
        rule = kwargs.get("trigger_rule")
        trigger_rules[task.value] = (
            rule.value if isinstance(rule, ast.Constant) else None
        )

    assert set(trigger_rules) == {"validate_silver", "validate_silver_quality"}
    assert all(rule != "all_done" for rule in trigger_rules.values())


def test_every_transform_and_dq_is_writer_fenced_before_candidate():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_transform_fotmob_silver", None)
    sys.modules.pop("dags.dag_transform_fotmob_silver", None)
    module = importlib.import_module("dag_transform_fotmob_silver")
    tasks = {task.task_id: task for task in PythonOperator._instances}

    candidate = tasks["record_fotmob_silver_candidate"]
    quality = tasks["validate_silver_quality"]
    row_gate = tasks["validate_silver"]
    transform_ids = {
        f"silver_transforms.{task_id}"
        for task_id, _sql_file, _table_name in module.SILVER_TRANSFORMS
    }
    assert candidate.upstream_task_ids == {quality.task_id}
    assert quality.upstream_task_ids == {row_gate.task_id}
    assert transform_ids <= row_gate.upstream_task_ids
    assert set(candidate.op_kwargs["transform_task_ids"]) == transform_ids

    tree = ast.parse(
        (ROOT / "dags" / "dag_transform_fotmob_silver.py").read_text(
            encoding="utf-8"
        )
    )
    guarded_functions = set()
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if any(
            isinstance(child, ast.With)
            and any(
                isinstance(item.context_expr, ast.Call)
                and isinstance(item.context_expr.func, ast.Name)
                and item.context_expr.func.id == "fotmob_publication_writer"
                for item in child.items
            )
            for child in ast.walk(node)
        ):
            guarded_functions.add(node.name)
    assert {
        "_run_transform",
        "_validate_silver",
        "_validate_silver_quality",
    } <= guarded_functions
