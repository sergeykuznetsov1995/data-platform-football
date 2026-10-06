"""Topology and fail-closed tests for the production FBref refresh DAG."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from scrapers.fbref.settings import DEFAULT_REQUEST_RESERVATION_BYTES, MIB


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
    def test_four_windows_source_discovered_scope(self, loaded_dag):
        module, _ = loaded_dag
        assert module.dag.dag_id == "dag_ingest_fbref"
        assert module.dag.schedule == "0 0,6,12,18 * * *"
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
            "wave_deadline_seconds",
            "max_batches",
        }
        source = Path(module.__file__).read_text(encoding="utf-8")
        assert "LEAGUES" not in source
        assert "params.leagues" not in source

    def test_global_budget_and_shard_bounds(self, loaded_dag):
        module, tasks = loaded_dag
        params = module.dag._dag_kwargs["params"]
        assert params["request_limit"].default == 4096
        assert params["request_limit"]._kw["enum"] == [100, 4096]
        assert params["byte_limit_mb"].default == 2048
        assert params["byte_limit_mb"]._kw["enum"] == [50, 2048]
        assert params["shard_size"].default == 25
        assert params["shard_size"]._kw["minimum"] == 1
        assert params["shard_size"]._kw["maximum"] == 25
        # Nullable defaults leave automatic UTC selection to the template.
        assert params["wave_deadline_seconds"].default is None
        assert params["wave_deadline_seconds"]._kw["minimum"] == 0
        assert params["wave_deadline_seconds"]._kw["maximum"] == 6 * 60 * 60
        assert params["max_batches"].default is None
        assert params["max_batches"]._kw["minimum"] == 1
        assert params["max_batches"]._kw["maximum"] == 80

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
        factory = sys.modules["utils.fbref_current_dag_factory"]
        assert (
            factory.CURRENT_MAX_BATCHES_POLICY
            == "fbref-current-max-batches-20-v1"
        )
        assert (
            factory.CURRENT_PAGE_KINDS_POLICY
            == "fbref-current-page-kinds-no-players-v1"
        )
        assert factory.CURRENT_MAX_BATCHES == 20
        assert module.CURRENT_MAX_BATCHES == 20
        assert len(tasks) == 15
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
            6 * 60 * 60 + 5 * 60
        )
        assert live._captured_kwargs["pool"] == "fbref_scraper_pool"
        assert live.op_kwargs["page_kinds"] == module.PAGE_KINDS
        assert "player" not in live.op_kwargs["page_kinds"]
        assert "matchlog" not in live.op_kwargs["page_kinds"]
        assert module.PAGE_KINDS == (
            "competition_index",
            "competition",
            "season",
            "season_stats",
            "schedule",
            "standings",
            "squad",
            "match",
        )
        assert live.op_kwargs["max_batches"] == factory.MAX_BATCHES
        assert factory.CURRENT_WAVE_DEADLINE_SECONDS == 16200
        assert live.op_kwargs["deadline_seconds"] == factory.WAVE_DEADLINE_SECONDS
        expected_reservation_mb = DEFAULT_REQUEST_RESERVATION_BYTES // MIB
        assert expected_reservation_mb == 9
        assert tasks["initialize_run"].op_kwargs["reservation_mb"] == (
            expected_reservation_mb
        )
        assert recovery.op_kwargs["reservation_mb"] == expected_reservation_mb
        assert live.op_kwargs["reservation_mb"] == expected_reservation_mb
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
        assert type(release) is not type(tasks["choose_publication_path"])
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
        assert tasks["validate_run"].upstream_task_ids == {
            "validate_current_scope_freshness"
        }
        assert tasks["validate_run"].downstream_task_ids == {
            "export_publication_scope"
        }
        assert tasks["export_publication_scope"].downstream_task_ids == {
            "release_publication_lock",
        }
        assert tasks["release_canary_publication_lock"].downstream_task_ids == {
            "release_publication_lock"
        }
        assert release.upstream_task_ids == {
            "export_publication_scope",
            "release_canary_publication_lock",
        }
        assert release.downstream_task_ids == set()

    def test_legacy_silver_trigger_is_absent(self, loaded_dag):
        _, tasks = loaded_dag
        assert "trigger_silver_transform" not in tasks

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
        assert all(task.python_callable is not None for task in tasks.values())


def _render_live_profile(loaded_dag, *, interval_end=None, conf=None, params=None,
                         omit_interval=False):
    from datetime import datetime, timezone
    from types import SimpleNamespace
    from jinja2 import StrictUndefined
    from jinja2.nativetypes import NativeEnvironment

    module, tasks = loaded_dag
    dag_kwargs = module.dag._dag_kwargs
    env = NativeEnvironment(undefined=StrictUndefined)
    env.globals.update(dag_kwargs["user_defined_macros"])
    defaults = {name: param.default for name, param in dag_kwargs["params"].items()}
    defaults.update(params or {})
    context = {
        "dag_run": SimpleNamespace(conf=conf or {}),
        "params": defaults,
        # Deliberately unrelated to the interval end: late starts cannot
        # select a different profile, nor may the logical/interval start date.
        "logical_date": datetime(2026, 10, 1, 0, tzinfo=timezone.utc),
        "data_interval_start": datetime(2026, 10, 1, 0, tzinfo=timezone.utc),
        "ti": SimpleNamespace(start_date=datetime(2026, 10, 2, 18, tzinfo=timezone.utc)),
    }
    if not omit_interval:
        context["data_interval_end"] = interval_end
    live = tasks["run_live_waves"]
    return {
        name: env.from_string(live.op_kwargs[name]).render(context)
        for name in ("max_batches", "deadline_seconds", "request_limit", "byte_limit_mb")
    }


@pytest.mark.unit
@pytest.mark.parametrize("hour,batches,budget", [(0, 9, 10800), (6, 20, 16200),
                                                (12, 9, 10800), (18, 9, 10800)])
def test_native_render_selects_interval_end_profile(loaded_dag, hour, batches, budget):
    from datetime import datetime, timezone

    result = _render_live_profile(
        loaded_dag, interval_end=datetime(2026, 10, 1, hour, tzinfo=timezone.utc)
    )
    assert result == {"max_batches": batches, "deadline_seconds": budget,
                      "request_limit": 4096, "byte_limit_mb": 2048}
    assert all(type(value) is int for value in result.values())


@pytest.mark.unit
@pytest.mark.parametrize("end,batches,budget", [
    ("2026-10-01T08:00:00+02:00", 20, 16200),
    ("2026-10-01T06:00:00+02:00", 9, 10800),
    ("2026-09-30T20:00:00-04:00", 9, 10800),
    ("2026-10-01T06:00:00", 20, 16200),
])
def test_native_render_normalizes_interval_to_utc(loaded_dag, end, batches, budget):
    from datetime import datetime

    result = _render_live_profile(loaded_dag, interval_end=datetime.fromisoformat(end))
    assert (result["max_batches"], result["deadline_seconds"]) == (batches, budget)


@pytest.mark.unit
@pytest.mark.parametrize("omit_interval", [False, True])
def test_manual_missing_interval_uses_small_profile(loaded_dag, omit_interval):
    result = _render_live_profile(loaded_dag, omit_interval=omit_interval)
    assert (result["max_batches"], result["deadline_seconds"]) == (9, 10800)


@pytest.mark.unit
@pytest.mark.parametrize("budget", [0, 60, 19800, 21600])
@pytest.mark.parametrize("batches", [1, 80])
def test_native_conf_overrides_preserve_zero_and_canary(loaded_dag, budget, batches):
    from datetime import datetime, timezone

    result = _render_live_profile(
        loaded_dag, interval_end=datetime(2026, 10, 1, 6, tzinfo=timezone.utc),
        conf={"wave_deadline_seconds": budget, "max_batches": batches,
              "request_limit": 100, "byte_limit_mb": 50},
        params={"wave_deadline_seconds": 300, "max_batches": 5},
    )
    assert result == {"max_batches": batches, "deadline_seconds": budget,
                      "request_limit": 100, "byte_limit_mb": 50}


@pytest.mark.unit
def test_native_params_override_and_null_auto(loaded_dag):
    result = _render_live_profile(
        loaded_dag, params={"wave_deadline_seconds": 0, "max_batches": 7}
    )
    assert (result["max_batches"], result["deadline_seconds"]) == (7, 0)
    result = _render_live_profile(
        loaded_dag, conf={"wave_deadline_seconds": None, "max_batches": None}
    )
    assert (result["max_batches"], result["deadline_seconds"]) == (9, 10800)


@pytest.mark.unit
@pytest.mark.parametrize("value", [0, 81, -1, 1.5, True, "20"])
def test_batch_param_schema_rejects_invalid_override(loaded_dag, value):
    import jsonschema

    module, _ = loaded_dag
    schema = module.dag._dag_kwargs["params"]["max_batches"]._kw
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(value, schema)
