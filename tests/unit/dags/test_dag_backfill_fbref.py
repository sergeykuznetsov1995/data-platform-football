"""Topology tests for the separate bounded FBref backfill DAG."""

from __future__ import annotations

import importlib
import sys

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
    sys.modules.pop("dag_backfill_fbref", None)
    sys.modules.pop("dags.dag_backfill_fbref", None)
    module = importlib.import_module("dag_backfill_fbref")
    tasks = {task.task_id: task for task in PythonOperator._instances}
    return module, tasks


@pytest.mark.unit
class TestFBrefBackfillTopology:
    def test_is_manual_separate_and_hard_bounded(self, loaded_dag):
        module, tasks = loaded_dag
        assert module.dag.dag_id == "dag_backfill_fbref"
        assert module.dag.schedule is None
        assert (
            module.dag._dag_kwargs["on_failure_callback"].__name__
            == "fbref_dag_failure_callback"
        )
        params = module.dag._dag_kwargs["params"]
        assert params["request_limit"].default == 25
        assert params["request_limit"]._kw["minimum"] == 22
        assert params["request_limit"]._kw["maximum"] == 25
        assert params["byte_limit_mb"]._kw["maximum"] == 100
        assert params["byte_limit_mb"]._kw["minimum"] == 7
        assert params["shard_size"].default == 2
        assert params["shard_size"]._kw["maximum"] == 2
        assert tasks["initialize_run"].op_kwargs["run_type"] == "backfill"

    def test_auto_resume_seeds_historical_scope_without_operator_cursor(
        self, loaded_dag
    ):
        module, tasks = loaded_dag
        seed = tasks["seed_historical_seasons"]
        assert seed.python_callable.__name__ == "seed_fbref_historical_seasons"
        assert "after_competition_id" not in seed.op_kwargs
        assert "after_season_id" not in seed.op_kwargs
        assert "after_competition_id" not in module.dag._dag_kwargs["params"]
        assert "after_season_id" not in module.dag._dag_kwargs["params"]
        assert seed.op_kwargs["request_limit"] == "{{ params.request_limit }}"
        assert seed.op_kwargs["byte_limit_mb"] == "{{ params.byte_limit_mb }}"
        assert seed.op_kwargs["reservation_mb"] == 7
        assert "competition_index" not in module.BACKFILL_PAGE_KINDS
        assert "competition" not in module.BACKFILL_PAGE_KINDS

    def test_fixed_fetch_parse_batches_are_sequential(self, loaded_dag):
        module, tasks = loaded_dag
        assert module.BACKFILL_WAVE_COUNT == module.BACKFILL_REQUEST_LIMIT
        assert len(tasks) == 54
        assert tasks["initialize_run"].downstream_task_ids == {
            "seed_historical_seasons"
        }
        assert tasks["seed_historical_seasons"].downstream_task_ids == {
            "fetch_wave_01"
        }
        for number in range(1, module.BACKFILL_WAVE_COUNT + 1):
            fetch_id = f"fetch_wave_{number:02d}"
            parse_id = f"parse_wave_{number:02d}"
            fetch = tasks[fetch_id]
            parse = tasks[parse_id]
            assert fetch.python_callable.__name__ == "fetch_fbref_wave"
            assert parse.python_callable.__name__ == "parse_fbref_wave"
            assert fetch.op_kwargs["run_type"] == "backfill"
            assert parse.op_kwargs["run_type"] == "backfill"
            assert fetch.downstream_task_ids == {parse_id}
            expected_next = (
                f"fetch_wave_{number + 1:02d}"
                if number < module.BACKFILL_WAVE_COUNT
                else "validate_run"
            )
            assert parse.downstream_task_ids == {expected_next}

    def test_validation_is_fail_closed_before_silver(self, loaded_dag):
        module, tasks = loaded_dag
        assert all(
            task._captured_kwargs.get("trigger_rule") == "all_success"
            for task in tasks.values()
        )
        assert tasks["validate_run"].upstream_task_ids == {
            f"parse_wave_{module.BACKFILL_WAVE_COUNT:02d}"
        }
        trigger = tasks["trigger_silver_transform"]
        assert trigger.upstream_task_ids == {"validate_run"}
        assert trigger._captured_kwargs["wait_for_completion"] is True
        assert trigger._captured_kwargs["failed_states"] == ["failed"]
        assert (
            trigger._captured_kwargs["execution_timeout"].total_seconds()
            == 12 * 60 * 60
        )
        assert trigger._captured_kwargs["retries"] == 0
        assert trigger._captured_kwargs["reset_dag_run"] is False
        assert trigger._captured_kwargs["logical_date"] == "{{ ti.start_date }}"

    def test_no_manual_league_allowlist(self, loaded_dag):
        module, _ = loaded_dag
        assert "leagues" not in module.dag._dag_kwargs["params"]
