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
        assert module.dag._dag_kwargs["dagrun_timeout"].total_seconds() == (
            18 * 60 * 60
        )
        params = module.dag._dag_kwargs["params"]
        assert params["dry_run"].default is False
        assert params["request_limit"].default == 4096
        assert params["request_limit"]._kw["enum"] == [100, 4096]
        assert params["byte_limit_mb"].default == 256
        assert params["byte_limit_mb"]._kw["enum"] == [50, 256]
        assert params["shard_size"].default == 25
        assert params["shard_size"]._kw["maximum"] == 25
        assert params["publish"].default is True
        assert params["max_batches"].default == 80
        assert params["max_batches"]._kw["minimum"] == 1
        assert params["max_batches"]._kw["maximum"] == 80
        assert tasks["initialize_run"].op_kwargs["run_type"] == "backfill"
        assert tasks["choose_backfill_mode"].downstream_task_ids == {
            "plan_backfill",
            "validate_production_readiness",
        }
        assert tasks["plan_backfill"].downstream_task_ids == set()
        assert tasks["validate_production_readiness"].downstream_task_ids == {
            "assert_history_window"
        }
        assert tasks["assert_history_window"].downstream_task_ids == {
            "initialize_run"
        }

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
        assert seed.op_kwargs["request_limit"] == module.REQUEST_LIMIT
        assert seed.op_kwargs["byte_limit_mb"] == module.BYTE_LIMIT_MB
        assert seed.op_kwargs["reservation_mb"] == 3
        assert "competition_index" not in module.BACKFILL_PAGE_KINDS
        assert "competition" not in module.BACKFILL_PAGE_KINDS

    def test_one_warm_live_runner_is_bounded(self, loaded_dag):
        module, tasks = loaded_dag
        assert module.BACKFILL_MAX_BATCHES == 80
        assert module.BACKFILL_REQUEST_LIMIT == 4096
        assert len(tasks) == 18
        assert tasks["initialize_run"].downstream_task_ids == {
            "validate_current_scope_freshness_preflight"
        }
        preflight = tasks["validate_current_scope_freshness_preflight"]
        assert preflight.python_callable.__name__ == (
            "validate_fbref_current_scope_freshness"
        )
        assert preflight.op_kwargs["run_type"] == "backfill"
        assert preflight.op_kwargs["fail_fast"] is False
        assert preflight.downstream_task_ids == {
            "acquire_publication_lock"
        }
        assert tasks["acquire_publication_lock"].downstream_task_ids == {
            "capture_raw_baseline"
        }
        assert tasks["capture_raw_baseline"].downstream_task_ids == {
            "recover_raw_before_fetch"
        }
        recovery = tasks["recover_raw_before_fetch"]
        assert recovery.python_callable.__name__ == "run_recovery_wave"
        assert recovery.downstream_task_ids == {
            "seed_historical_seasons"
        }
        assert tasks["seed_historical_seasons"].downstream_task_ids == {
            "run_live_waves"
        }
        live = tasks["run_live_waves"]
        assert live.python_callable.__name__ == "run_fbref_live_waves"
        assert live.op_kwargs["run_type"] == "backfill"
        assert live.op_kwargs["max_batches"] == module.MAX_BATCHES
        assert live.op_kwargs["reservation_mb"] == 3
        assert live._captured_kwargs["execution_timeout"].total_seconds() == (
            6 * 60 * 60 + 5 * 60
        )
        assert live._captured_kwargs["pool"] == "fbref_scraper_pool"
        assert live._captured_kwargs["retries"] == 0
        assert live.downstream_task_ids == {
            "audit_raw_integrity"
        }
        assert tasks["audit_raw_integrity"].downstream_task_ids == {
            "validate_current_scope_freshness"
        }
        assert not any(
            task_id.startswith(("fetch_wave_", "parse_wave_"))
            for task_id in tasks
        )

    def test_validation_is_fail_closed_before_silver(self, loaded_dag):
        module, tasks = loaded_dag
        assert all(
            task._captured_kwargs.get("trigger_rule") == "all_success"
            for task_id, task in tasks.items()
            if task_id != "release_publication_lock"
        )
        assert tasks["release_publication_lock"]._captured_kwargs[
            "trigger_rule"
        ] == "all_done"
        freshness = tasks["validate_current_scope_freshness"]
        assert freshness.op_kwargs["fail_fast"] is True
        assert freshness.upstream_task_ids == {
            "audit_raw_integrity"
        }
        assert tasks["validate_run"].upstream_task_ids == {
            "validate_current_scope_freshness"
        }
        assert tasks["validate_run"].downstream_task_ids == {
            "choose_publication_path"
        }
        assert tasks["choose_publication_path"].downstream_task_ids == {
            "export_publication_scope",
            "release_publication_lock",
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
        assert trigger._captured_kwargs["execution_date"] == "{{ ti.start_date }}"
        assert trigger._captured_kwargs["conf"]["trigger_xref"] is False
        assert trigger.downstream_task_ids == {"release_publication_lock"}

    def test_run_byte_ceiling_leaves_room_in_the_daily_allowance(
        self, loaded_dag
    ):
        """A run cap equal to the daily budget makes every extension fail.

        The meter grants `min(daily_remaining, run_remaining, url_remaining)`
        as the new lease cap, while the warm-HTTP phase boundary asks for the
        full run cap.  With both ceilings equal, `daily_remaining` is the
        smaller term the moment anything was spent today, the meter answers
        409, and the runner turns that into hard_transport_policy — the wave
        dies with its paid bootstrap burnt.  Keeping the run ceiling a fraction
        of the daily one makes `run_remaining` binding, so the extension asks
        for exactly what the meter can grant.
        """

        from utils.fbref_pipeline_tasks import FBREF_PRODUCTION_BYTE_LIMIT_MB

        module, tasks = loaded_dag
        assert module.BACKFILL_BYTE_LIMIT_MB == 256
        assert module.BACKFILL_BYTE_LIMIT_MB * 4 <= FBREF_PRODUCTION_BYTE_LIMIT_MB
        assert (
            module.dag._dag_kwargs["params"]["byte_limit_mb"].default
            == module.BACKFILL_BYTE_LIMIT_MB
        )

    def test_raw_recovery_drains_before_the_historical_seed(self, loaded_dag):
        """A seeded season must never be judged on raw the seed itself unhid.

        The seed reopens a scope-quarantined season and opens a 'pending'
        run_target for it; the same step makes that target's days-old raw
        visible to the recovery drain, which may retire the target on those
        stale bytes.  A contract quarantine touches only page_frontier, so the
        run_target stays open, the wave gate counts an unclaimable target as
        unfinished, and the run raises before its first request -- the zero-work
        runs that stopped the history campaign on 2026-08-17.  Draining before
        the seed keeps every verdict on bytes this run fetched itself.
        """

        _, tasks = loaded_dag
        assert tasks["capture_raw_baseline"].upstream_task_ids == {
            "acquire_publication_lock"
        }
        assert tasks["recover_raw_before_fetch"].upstream_task_ids == {
            "capture_raw_baseline"
        }
        assert tasks["seed_historical_seasons"].upstream_task_ids == {
            "recover_raw_before_fetch"
        }
        assert tasks["run_live_waves"].upstream_task_ids == {
            "seed_historical_seasons"
        }

    def test_no_manual_league_allowlist(self, loaded_dag):
        module, _ = loaded_dag
        assert "leagues" not in module.dag._dag_kwargs["params"]
