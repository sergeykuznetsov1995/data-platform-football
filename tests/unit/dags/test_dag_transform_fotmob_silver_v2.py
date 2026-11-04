"""Topology and callable contracts for the trigger-only FotMob Silver v2 DAG (#2020)."""

from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager
from datetime import timedelta

import pytest

pytestmark = pytest.mark.unit


def _reload():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_transform_fotmob_silver_v2", None)
    sys.modules.pop("dags.dag_transform_fotmob_silver_v2", None)
    return importlib.import_module("dag_transform_fotmob_silver_v2")


def _tasks():
    from airflow.operators.python import PythonOperator

    return {task.task_id: task for task in PythonOperator._instances}


class _TI:
    def __init__(self, xcoms):
        self.xcoms = xcoms

    def xcom_pull(self, task_ids):
        if isinstance(task_ids, list):
            return [self.xcoms.get(task_id) for task_id in task_ids]
        return self.xcoms.get(task_ids)


def _context(mode="build", rebuilt=(0, 0, 0)):
    run = {"active_slot": "a", "revision": 3, "build_slot": "b"}
    xcoms = {"bootstrap_reader": run, **dict(zip(
        ["build_match_tables", "build_season_tables", "build_global_tables"], rebuilt))}
    return {"params": {"mode": mode}, "ti": _TI(xcoms), "run_id": "manual__1"}


def test_trigger_only_dag_runs_five_tasks_in_order():
    module = _reload()
    tasks = _tasks()
    kwargs = module.dag._dag_kwargs

    assert module.dag.schedule is None
    assert kwargs["catchup"] is False
    assert kwargs["max_active_runs"] == 1
    assert kwargs["max_active_tasks"] == 1
    assert set(kwargs["params"]) == {
        "competitions", "tables", "max_partitions_per_batch", "entity_buckets",
        "full_rebuild", "mode",
    }
    assert kwargs["params"]["mode"].default == "build"
    order = ["bootstrap_reader", "build_match_tables", "build_season_tables",
             "build_global_tables", "switch_reader"]
    assert set(tasks) == set(order)
    for upstream, downstream in zip(order, order[1:]):
        assert tasks[upstream].downstream_task_ids == {downstream}
        assert tasks[downstream].upstream_task_ids == {upstream}
    for task_id, family in module.BUILD_TASKS.items():
        assert tasks[task_id].op_kwargs == {"family": family}
        assert tasks[task_id]._init_kwargs["execution_timeout"] == timedelta(hours=6)


def test_config_from_params_defaults_to_all_tables():
    module = _reload()
    cfg = module._config({"competitions": [47, 112], "tables": [], "entity_buckets": {},
                          "full_rebuild": True, "mode": "rollback"})
    assert cfg.competitions == (47, 112)
    assert cfg.tables == module.build.ALL_TABLES
    assert cfg.entity_buckets is None and cfg.full_rebuild is False
    assert cfg.sql_dir.is_dir()


def _patch_cursor(monkeypatch, module, calls):
    @contextmanager
    def cursor():
        yield "cur"

    monkeypatch.setattr(module, "_cursor", cursor)
    monkeypatch.setattr(module.build, "advance", lambda cur, spec, **kw: calls.append(("advance", kw)) or kw)
    monkeypatch.setattr(module.build, "rollback", lambda cur, spec, **kw: calls.append(("rollback", kw)) or kw)


def test_switch_advances_on_empty_input(monkeypatch):
    module = _reload()
    calls = []
    _patch_cursor(monkeypatch, module, calls)
    module._switch_reader(**_context(rebuilt=(0, None, 0)))
    assert calls == [("advance", {"expected_revision": 3, "actor": "manual__1"})]


def test_switch_advances_with_run_id_actor(monkeypatch):
    module = _reload()
    calls = []
    _patch_cursor(monkeypatch, module, calls)
    module._switch_reader(**_context(rebuilt=(2, 0, 1)))
    assert calls == [("advance", {"expected_revision": 3, "actor": "manual__1"})]


def test_rollback_mode_builds_nothing_and_rolls_back(monkeypatch):
    module = _reload()
    calls = []
    _patch_cursor(monkeypatch, module, calls)
    assert module._build("match", **_context(mode="rollback")) == 0
    module._switch_reader(**_context(mode="rollback"))
    assert calls == [("rollback", {"expected_revision": 3, "actor": "manual__1"})]
