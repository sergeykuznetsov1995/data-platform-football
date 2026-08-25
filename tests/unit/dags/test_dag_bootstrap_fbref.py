"""Topology contracts for the manual non-publishing FBref bootstrap DAG."""

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
    sys.modules.pop("dag_bootstrap_fbref", None)
    sys.modules.pop("dags.dag_bootstrap_fbref", None)
    module = importlib.import_module("dag_bootstrap_fbref")
    tasks = {task.task_id: task for task in PythonOperator._instances}
    return module, tasks


EXPECTED_TASK_IDS = {
    "validate_production_readiness",
    "initialize_run",
    "acquire_publication_lock",
    "seed_competition_index",
    "capture_raw_baseline",
    "recover_raw_before_fetch",
    "run_live_waves",
    "audit_raw_integrity",
    "validate_bootstrap_run",
    "release_bootstrap_publication_lock",
    "release_publication_lock",
}


@pytest.mark.unit
class TestFBrefBootstrapTopology:
    def test_manual_unpaused_bounded_dag(self, loaded_dag):
        module, tasks = loaded_dag

        assert module.dag.dag_id == "dag_bootstrap_fbref"
        assert module.dag.schedule is None
        assert module.dag._dag_kwargs["is_paused_upon_creation"] is False
        assert module.dag._dag_kwargs["max_active_runs"] == 1
        assert module.dag._dag_kwargs["max_active_tasks"] == 1
        assert module.dag._dag_kwargs["dagrun_timeout"].total_seconds() == (
            8 * 60 * 60
        )
        assert module.dag._dag_kwargs.get("params", {}) == {}
        assert set(tasks) == EXPECTED_TASK_IDS

        live = tasks["run_live_waves"]
        assert live._captured_kwargs["pool"] == "fbref_scraper_pool"
        assert live._captured_kwargs["execution_timeout"].total_seconds() == (
            6 * 60 * 60 + 5 * 60
        )
        # Bootstrap is a current-lane runner with the same 25-page waves and
        # six-hour-five-minute task timeout, so it shares the production-safe
        # 16-wave cap rather than reviving the stale 80-wave contract.
        assert live.op_kwargs["max_batches"] == 16

    def test_dedicated_pool_is_created_idempotently(self):
        compose = (
            Path(__file__).resolve().parents[3] / "compose.yaml"
        ).read_text(encoding="utf-8")
        command = compose.split("airflow-init:", 1)[1].split(
            "airflow-webserver:", 1
        )[0]

        assert command.count(
            "airflow pools set 'fbref_scraper_pool' 1"
        ) == 1
        assert command.count(
            "airflow pools set 'ingest_scraper_pool' 1"
        ) == 1

    def test_exact_profile_is_literal_and_not_conf_overridable(self, loaded_dag):
        _, tasks = loaded_dag

        for task_id in (
            "validate_production_readiness",
            "initialize_run",
            "recover_raw_before_fetch",
            "run_live_waves",
            "validate_bootstrap_run",
        ):
            kwargs = tasks[task_id].op_kwargs
            assert kwargs["request_limit"] == 4096
            assert kwargs["byte_limit_mb"] == 2048
            assert kwargs["shard_size"] == 25
        for task_id in (
            "validate_production_readiness",
            "initialize_run",
            "validate_bootstrap_run",
        ):
            kwargs = tasks[task_id].op_kwargs
            assert kwargs["bootstrap_only"] is True
            assert kwargs["dag_run_type"] == "{{ dag_run.run_type }}"
        assert tasks["initialize_run"].op_kwargs["dag_id"] == (
            "{{ dag.dag_id }}"
        )

    def test_raw_first_prefix_has_one_nonpublishing_exit(self, loaded_dag):
        _, tasks = loaded_dag

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
        assert tasks["recover_raw_before_fetch"].downstream_task_ids == {
            "run_live_waves"
        }
        assert tasks["run_live_waves"].downstream_task_ids == {
            "audit_raw_integrity"
        }
        assert tasks["audit_raw_integrity"].downstream_task_ids == {
            "validate_bootstrap_run"
        }
        assert tasks["validate_bootstrap_run"].downstream_task_ids == {
            "release_bootstrap_publication_lock"
        }
        assert tasks[
            "release_bootstrap_publication_lock"
        ].downstream_task_ids == {"release_publication_lock"}

    def test_publication_tasks_do_not_exist(self, loaded_dag):
        _, tasks = loaded_dag

        forbidden = {
            "choose_publication_path",
            "validate_canary_run",
            "release_canary_publication_lock",
            "validate_current_scope_freshness",
            "validate_run",
            "export_publication_scope",
            "trigger_silver_transform",
        }
        assert forbidden.isdisjoint(tasks)

    def test_finalizer_is_explicit_and_cannot_mask_red_prefix(self, loaded_dag):
        _, tasks = loaded_dag

        assert all(
            task._captured_kwargs.get("trigger_rule") == "all_success"
            for task_id, task in tasks.items()
            if task_id != "release_publication_lock"
        )
        finalizer = tasks["release_publication_lock"]
        assert finalizer._captured_kwargs["trigger_rule"] == "all_done"
        assert finalizer._captured_kwargs["retries"] == 0
        assert finalizer.op_kwargs == {
            "airflow_run_id": "{{ run_id }}",
            "dag_id": "{{ dag.dag_id }}",
            "bootstrap_only": True,
        }
        assert finalizer.python_callable.__name__ == (
            "finalize_fbref_publication_lock"
        )
