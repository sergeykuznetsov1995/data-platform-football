"""Topology and zero-network tests for the FBref version re-parse DAG."""

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
    sys.modules.pop("dag_reparse_fbref", None)
    module = importlib.import_module("dag_reparse_fbref")
    tasks = {task.task_id: task for task in PythonOperator._instances}
    return module, tasks


@pytest.mark.unit
class TestFBrefReparseTopology:
    def test_is_paused_hourly_and_low_priority_on_the_fbref_pool(
        self, loaded_dag
    ):
        module, _ = loaded_dag
        kwargs = module.dag._dag_kwargs
        assert module.dag.dag_id == "dag_reparse_fbref"
        assert kwargs["is_paused_upon_creation"] is True
        assert kwargs["schedule"] == "40 * * * *"
        assert kwargs["catchup"] is False
        assert kwargs["max_active_runs"] == 1
        assert kwargs["on_failure_callback"].__name__ == (
            "fbref_dag_failure_callback"
        )
        assert kwargs["default_args"]["pool"] == "fbref_scraper_pool"
        assert kwargs["default_args"]["priority_weight"] == 1

    def test_graph_is_sequential_without_fetch_or_publication(
        self, loaded_dag
    ):
        _module, tasks = loaded_dag
        order = [
            "validate_production_readiness",
            "initialize_run",
            "acquire_publication_lock",
            "drain_reparse",
            "validate_run",
            "release_publication_lock",
        ]
        assert set(tasks) == set(order)
        for upstream, downstream in zip(order, order[1:]):
            assert tasks[upstream].downstream_task_ids == {downstream}
        callables = {
            task.python_callable.__name__ for task in tasks.values()
        }
        assert "run_fbref_live_waves" not in callables
        assert "run_recovery_wave" not in callables
        assert "export_fbref_publication_scope" not in callables
        assert tasks["release_publication_lock"]._captured_kwargs[
            "trigger_rule"
        ] == "all_done"
        assert tasks["release_publication_lock"].python_callable.__name__ == (
            "finalize_fbref_reparse_lock"
        )

    def test_run_is_a_zero_budget_version_reparse_without_players(
        self, loaded_dag
    ):
        module, tasks = loaded_dag
        initialize = tasks["initialize_run"].op_kwargs
        assert initialize["run_type"] == "replay"
        assert initialize["request_limit"] == 0
        assert initialize["byte_limit_mb"] == 0
        assert initialize["version_reparse"] is True
        assert "player" not in module.REPARSE_PAGE_KINDS
        assert tasks["drain_reparse"].op_kwargs["page_kinds"] == (
            module.REPARSE_PAGE_KINDS
        )

    def test_lock_wait_soft_fails_within_the_hour(self, loaded_dag):
        module, tasks = loaded_dag
        sensor = tasks["acquire_publication_lock"]._captured_kwargs
        assert sensor["soft_fail"] is True
        assert sensor["mode"] == "reschedule"
        assert sensor["timeout"] < 60 * 60
        assert sensor["op_kwargs"]["ttl_seconds"] == (
            module.REPARSE_LOCK_TTL_SECONDS
        )
