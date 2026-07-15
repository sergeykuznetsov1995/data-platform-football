"""Topology and zero-network tests for the FBref raw replay DAG."""

from __future__ import annotations

import ast
import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def loaded_dag(request):
    from airflow.operators.python import PythonOperator

    original_init = PythonOperator.__init__

    def capturing_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self._captured_kwargs = dict(kwargs)

    PythonOperator.__init__ = capturing_init
    request.addfinalizer(
        lambda: setattr(PythonOperator, "__init__", original_init)
    )
    PythonOperator._instances.clear()
    sys.modules.pop("dag_replay_fbref", None)
    sys.modules.pop("dags.dag_replay_fbref", None)
    module = importlib.import_module("dag_replay_fbref")
    tasks = {task.task_id: task for task in PythonOperator._instances}
    return module, tasks


@pytest.mark.unit
class TestFBrefReplayTopology:
    def test_is_manual_with_required_source_run_id(self, loaded_dag):
        module, _ = loaded_dag
        assert module.dag.dag_id == "dag_replay_fbref"
        assert module.dag.schedule is None
        assert (
            module.dag._dag_kwargs["on_failure_callback"].__name__
            == "fbref_dag_failure_callback"
        )
        assert module.dag._dag_kwargs["dagrun_timeout"].total_seconds() == (
            18 * 60 * 60
        )
        assert set(module.dag._dag_kwargs["params"]) == {
            "source_control_run_id"
        }
        param = module.dag._dag_kwargs["params"]["source_control_run_id"]
        assert param.default is None
        assert param._kw["type"] == ["null", "string"]
        assert param._kw["minLength"] == 36
        assert param._kw["maxLength"] == 36

        # Airflow 2.7 needs an import-safe default, while the task callable
        # rejects None before looking up any raw candidates.
        tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
        source_param_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "Param"
            and any(
                keyword.arg == "description"
                and isinstance(keyword.value, ast.Constant)
                and "Required crawl_run UUID" in str(keyword.value.value)
                for keyword in node.keywords
            )
        ]
        assert len(source_param_calls) == 1
        default = next(
            keyword.value
            for keyword in source_param_calls[0].keywords
            if keyword.arg == "default"
        )
        assert isinstance(default, ast.Constant) and default.value is None

    def test_graph_contains_no_fetch_or_seed_task(self, loaded_dag):
        module, tasks = loaded_dag
        assert len(tasks) == module.REPLAY_WAVE_COUNT + 9
        assert not any(task_id.startswith("fetch") for task_id in tasks)
        assert not any(task_id.startswith("seed") for task_id in tasks)
        assert "recover_raw_before_fetch" not in tasks
        callable_names = {
            task.python_callable.__name__
            for task_id, task in tasks.items()
            if task_id != "trigger_silver_transform"
        }
        assert "run_fbref_live_waves" not in callable_names
        assert callable_names == {
            "initialize_fbref_run",
            "acquire_fbref_publication_lock",
            "audit_fbref_raw_integrity",
            "capture_fbref_raw_baseline",
            "export_fbref_publication_scope",
            "parse_fbref_wave",
            "finalize_fbref_publication_lock",
            "validate_fbref_production_readiness",
            "validate_fbref_run",
        }

    def test_replay_run_has_zero_network_budget(self, loaded_dag):
        module, tasks = loaded_dag
        initialize = tasks["initialize_run"]
        assert initialize.op_kwargs["run_type"] == "replay"
        assert initialize.op_kwargs["request_limit"] == 0
        assert initialize.op_kwargs["byte_limit_mb"] == 0
        assert initialize.op_kwargs["reservation_mb"] == 3
        for task_id, task in tasks.items():
            if task_id.startswith("parse_wave_"):
                assert task.op_kwargs["run_type"] == "replay"
                assert task.op_kwargs["request_limit"] == 0
                assert task.op_kwargs["byte_limit_mb"] == 0
                assert task.op_kwargs["reservation_mb"] == 3
                assert task.op_kwargs["source_control_run_id"] == (
                    module.SOURCE_CONTROL_RUN_ID
                )

    def test_offline_shards_are_strictly_sequential(self, loaded_dag):
        module, tasks = loaded_dag
        assert tasks["validate_production_readiness"].downstream_task_ids == {
            "initialize_run"
        }
        assert tasks["initialize_run"].downstream_task_ids == {
            "acquire_publication_lock"
        }
        assert tasks["acquire_publication_lock"].downstream_task_ids == {
            "capture_raw_baseline"
        }
        assert tasks["capture_raw_baseline"].downstream_task_ids == {
            "parse_wave_01"
        }
        for number in range(1, module.REPLAY_WAVE_COUNT + 1):
            task = tasks[f"parse_wave_{number:02d}"]
            expected_next = (
                f"parse_wave_{number + 1:02d}"
                if number < module.REPLAY_WAVE_COUNT
                else "audit_raw_integrity"
            )
            assert task.downstream_task_ids == {expected_next}
        assert tasks["audit_raw_integrity"].downstream_task_ids == {
            "validate_run"
        }

    def test_validation_precedes_waiting_silver_trigger(self, loaded_dag):
        module, tasks = loaded_dag
        assert all(
            task._captured_kwargs.get("trigger_rule") == "all_success"
            for task_id, task in tasks.items()
            if task_id != "release_publication_lock"
        )
        assert tasks["release_publication_lock"]._captured_kwargs[
            "trigger_rule"
        ] == "all_done"
        assert tasks["validate_run"].upstream_task_ids == {
            "audit_raw_integrity"
        }
        assert tasks["validate_run"].downstream_task_ids == {
            "export_publication_scope"
        }
        trigger = tasks["trigger_silver_transform"]
        assert trigger.upstream_task_ids == {"export_publication_scope"}
        assert trigger._captured_kwargs["wait_for_completion"] is True
        assert trigger._captured_kwargs["failed_states"] == ["failed"]
        assert (
            trigger._captured_kwargs["execution_timeout"].total_seconds()
            == 12 * 60 * 60
        )
        assert trigger._captured_kwargs["retries"] == 0
        assert trigger._captured_kwargs["reset_dag_run"] is False
        assert trigger._captured_kwargs["logical_date"] == "{{ ti.start_date }}"
        assert trigger._captured_kwargs["conf"]["trigger_xref"] is False
        assert trigger.downstream_task_ids == {"release_publication_lock"}
