"""Topology and fail-closed tests for the production FBref refresh DAG."""

from __future__ import annotations

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
    sys.modules.pop("dag_ingest_fbref", None)
    sys.modules.pop("dags.dag_ingest_fbref", None)
    module = importlib.import_module("dag_ingest_fbref")
    tasks = {task.task_id: task for task in PythonOperator._instances}
    return module, tasks


@pytest.mark.unit
class TestFBrefCurrentTopology:
    def test_daily_source_discovered_scope(self, loaded_dag):
        module, _ = loaded_dag
        assert module.dag.dag_id == "dag_ingest_fbref"
        assert module.dag.schedule == "0 6 * * *"
        assert module.dag._dag_kwargs["max_active_runs"] == 1
        assert module.dag._dag_kwargs["max_active_tasks"] == 1
        assert module.dag._dag_kwargs["dagrun_timeout"].total_seconds() == (
            18 * 60 * 60
        )
        assert (
            module.dag._dag_kwargs["on_failure_callback"].__name__
            == "fbref_dag_failure_callback"
        )
        assert set(module.dag._dag_kwargs["params"]) == {
            "request_limit",
            "byte_limit_mb",
            "shard_size",
            "bootstrap_only",
        }
        source = Path(module.__file__).read_text(encoding="utf-8")
        assert "LEAGUES" not in source
        assert "params.leagues" not in source

    def test_global_budget_and_shard_bounds(self, loaded_dag):
        module, tasks = loaded_dag
        params = module.dag._dag_kwargs["params"]
        assert params["request_limit"].default == 200
        assert params["request_limit"]._kw["enum"] == [100, 200]
        assert params["byte_limit_mb"].default == 100
        assert params["byte_limit_mb"]._kw["enum"] == [50, 100]
        assert params["shard_size"].default == 25
        assert params["shard_size"]._kw["minimum"] == 1
        assert params["shard_size"]._kw["maximum"] == 25
        assert params["bootstrap_only"].default is False
        assert params["bootstrap_only"]._kw["type"] == "boolean"

        initialize = tasks["initialize_run"]
        assert initialize.python_callable.__name__ == "initialize_fbref_run"
        assert initialize.op_kwargs["run_type"] == "current"
        assert initialize.op_kwargs["request_limit"] == (
            "{{ dag_run.conf.get('request_limit', params.request_limit) }}"
        )
        readiness = tasks["validate_production_readiness"]
        assert readiness.python_callable.__name__ == (
            "validate_fbref_production_readiness"
        )
        assert readiness.downstream_task_ids == {"initialize_run"}

    def test_one_warm_live_runner_replaces_cold_wave_tasks(self, loaded_dag):
        module, tasks = loaded_dag
        assert module.CURRENT_MAX_BATCHES == 16
        assert len(tasks) == 18
        assert tasks["validate_production_readiness"].downstream_task_ids == {
            "initialize_run"
        }
        assert tasks["initialize_run"].downstream_task_ids == {
            "acquire_publication_lock"
        }
        assert tasks["acquire_publication_lock"].downstream_task_ids == {
            "seed_competition_index"
        }
        assert tasks["seed_competition_index"].downstream_task_ids == {
            "capture_raw_baseline"
        }
        assert tasks["capture_raw_baseline"].downstream_task_ids == {
            "recover_raw_before_fetch"
        }
        recovery = tasks["recover_raw_before_fetch"]
        assert recovery.python_callable.__name__ == "run_recovery_wave"
        assert recovery.downstream_task_ids == {
            "run_live_waves"
        }
        live = tasks["run_live_waves"]
        assert live.python_callable.__name__ == "run_fbref_live_waves"
        assert live._captured_kwargs["retries"] == 0
        assert live._captured_kwargs["execution_timeout"].total_seconds() == (
            120 * 60
        )
        assert live.op_kwargs["page_kinds"] == module.PAGE_KINDS
        assert live.op_kwargs["max_batches"] == 16
        assert live.op_kwargs["reservation_mb"] == 3
        assert live.downstream_task_ids == {"audit_raw_integrity"}
        raw_audit = tasks["audit_raw_integrity"]
        assert raw_audit.python_callable.__name__ == (
            "audit_fbref_raw_integrity"
        )
        assert raw_audit.downstream_task_ids == {"choose_publication_path"}
        assert not any(
            task_id.startswith(("fetch_wave_", "parse_wave_"))
            for task_id in tasks
        )

    def test_failure_edges_cannot_be_masked(self, loaded_dag):
        _, tasks = loaded_dag
        assert all(
            task._captured_kwargs.get("trigger_rule") == "all_success"
            for task_id, task in tasks.items()
            if task_id != "release_publication_lock"
        )
        release = tasks["release_publication_lock"]
        assert release._captured_kwargs["trigger_rule"] == "all_done"
        assert release._captured_kwargs["retries"] == 0
        assert release.python_callable.__name__ == (
            "finalize_fbref_publication_lock"
        )
        freshness = tasks["validate_current_scope_freshness"]
        assert freshness.python_callable.__name__ == (
            "validate_fbref_current_scope_freshness"
        )
        assert freshness.op_kwargs["fail_fast"] is True
        assert freshness.upstream_task_ids == {"choose_publication_path"}
        assert tasks["choose_publication_path"].downstream_task_ids == {
            "validate_canary_run",
            "validate_bootstrap_run",
            "validate_current_scope_freshness",
        }
        assert tasks["validate_canary_run"].upstream_task_ids == {
            "choose_publication_path"
        }
        assert tasks["validate_canary_run"]._captured_kwargs["retries"] == 0
        assert tasks["validate_canary_run"].downstream_task_ids == {
            "release_canary_publication_lock"
        }
        assert tasks["release_canary_publication_lock"].upstream_task_ids == {
            "validate_canary_run"
        }
        assert tasks["validate_bootstrap_run"].upstream_task_ids == {
            "choose_publication_path"
        }
        assert tasks["validate_bootstrap_run"].python_callable.__name__ == (
            "validate_fbref_bootstrap_run"
        )
        assert tasks["validate_bootstrap_run"]._captured_kwargs["retries"] == 0
        assert tasks["validate_bootstrap_run"].downstream_task_ids == {
            "release_bootstrap_publication_lock"
        }
        assert tasks[
            "release_bootstrap_publication_lock"
        ].upstream_task_ids == {"validate_bootstrap_run"}
        assert tasks["validate_run"].upstream_task_ids == {
            "validate_current_scope_freshness"
        }
        assert tasks["validate_run"].downstream_task_ids == {
            "export_publication_scope"
        }
        assert tasks["export_publication_scope"].downstream_task_ids == {
            "trigger_silver_transform"
        }
        assert tasks["trigger_silver_transform"].upstream_task_ids == {
            "export_publication_scope"
        }
        assert tasks["trigger_silver_transform"].downstream_task_ids == {
            "release_publication_lock"
        }
        assert tasks["release_canary_publication_lock"].downstream_task_ids == {
            "release_publication_lock"
        }
        assert tasks[
            "release_bootstrap_publication_lock"
        ].downstream_task_ids == {"release_publication_lock"}
        assert release.upstream_task_ids == {
            "trigger_silver_transform",
            "release_canary_publication_lock",
            "release_bootstrap_publication_lock",
        }

    def test_silver_waits_and_propagates_child_failure(self, loaded_dag):
        _, tasks = loaded_dag
        kwargs = tasks["trigger_silver_transform"]._captured_kwargs
        assert kwargs["trigger_dag_id"] == "dag_transform_fbref_silver"
        assert kwargs["wait_for_completion"] is True
        assert kwargs["allowed_states"] == ["success"]
        assert kwargs["failed_states"] == ["failed"]
        assert kwargs["execution_timeout"].total_seconds() == 12 * 60 * 60
        assert kwargs["retries"] == 0
        assert kwargs["reset_dag_run"] is False
        assert kwargs["trigger_run_id"] == (
            "fbref_silver__{{ dag.dag_id }}__{{ run_id }}"
        )
        assert kwargs["execution_date"] == "{{ ti.start_date }}"
        assert kwargs["conf"]["publication_scope"] == "fbref_silver_only"
        assert kwargs["conf"]["trigger_xref"] is False

    def test_legacy_transport_tasks_are_absent(self, loaded_dag):
        _, tasks = loaded_dag
        legacy = {
            "season_stats_all",
            "match_schedule",
            "match_all_data",
            "traffic_guard_season_stats",
            "report_proxy_traffic",
        }
        assert legacy.isdisjoint(tasks)
        assert all(
            task.python_callable is not None
            for task_id, task in tasks.items()
            if task_id != "trigger_silver_transform"
        )
