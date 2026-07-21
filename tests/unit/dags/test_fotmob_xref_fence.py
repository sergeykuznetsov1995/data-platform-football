"""Fail-closed FotMob consumer admission for the shared xref writer DAG."""

from __future__ import annotations

import importlib
import sys

import pytest


pytestmark = pytest.mark.unit


def _reload_xref():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_transform_xref", None)
    sys.modules.pop("dags.dag_transform_xref", None)
    return importlib.import_module("dag_transform_xref")


def _reload_child(module_name: str):
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop(module_name, None)
    sys.modules.pop(f"dags.{module_name}", None)
    return importlib.import_module(module_name)


def _tasks():
    from airflow.operators.python import PythonOperator

    return {task.task_id: task for task in PythonOperator._instances}


def test_consumer_preflight_is_before_every_xref_writer():
    module = _reload_xref()
    tasks = _tasks()
    start = tasks["start_marker"]
    fence = tasks["validate_fotmob_publication_consumer"]
    team = tasks["xref_transforms.xref_team"]
    player = tasks["xref_player"]

    assert fence.python_callable is module.validate_fotmob_consumer_fence
    assert fence.upstream_task_ids == {start.task_id}
    assert fence.task_id in team.upstream_task_ids
    assert team.task_id in tasks["xref_transforms.xref_referee"].upstream_task_ids
    assert "xref_transforms.xref_manager" in player.upstream_task_ids
    assert fence._init_kwargs["retries"] == 0


def test_xref_docs_forbid_unfenced_direct_trigger():
    module = _reload_xref()

    normalized_doc = " ".join(module.__doc__.split())
    assert "Direct/manual runs" in normalized_doc
    assert "fail before" in normalized_doc


@pytest.mark.parametrize(
    ("module_name", "first_writer"),
    [
        ("dag_transform_e3", "silver_e3.whoscored_events_spadl"),
        ("dag_transform_e4", "silver_e4.matchhistory_match_odds"),
    ],
)
def test_e3_e4_consumer_preflight_is_before_first_writer(
    module_name, first_writer
):
    module = _reload_child(module_name)
    tasks = _tasks()
    start = tasks["start_marker"]
    fence = tasks["validate_fotmob_publication_consumer"]
    writer = tasks[first_writer]

    assert fence.python_callable is module.validate_fotmob_consumer_fence
    assert fence.upstream_task_ids == {start.task_id}
    assert fence.task_id in writer.upstream_task_ids
    assert fence._init_kwargs["retries"] == 0
    normalized_doc = " ".join(module.__doc__.split())
    assert "Direct/manual runs" in normalized_doc
    assert "fail before" in normalized_doc


def test_fbref_gold_consumer_preflight_is_before_first_writer():
    module = _reload_child("dag_transform_fbref_gold")
    tasks = _tasks()
    fence = tasks["validate_fotmob_publication_consumer"]
    transfermarkt_preflight = tasks["transfermarkt_reader_precondition"]
    first_writer = tasks["s2a_config_dims.dim_competition"]

    assert fence.python_callable is module.validate_fotmob_consumer_fence
    assert fence.upstream_task_ids == set()
    assert fence.task_id in transfermarkt_preflight.upstream_task_ids
    assert transfermarkt_preflight.task_id in first_writer.upstream_task_ids
    assert fence._init_kwargs["retries"] == 0
    assert "Direct/manual runs fail" in module.__doc__
