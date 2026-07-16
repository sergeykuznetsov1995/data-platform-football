from __future__ import annotations

import inspect
import sys
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dags.utils import fbref_pipeline_tasks


@pytest.fixture(autouse=True)
def _select_test_browser_interpreter(monkeypatch):
    monkeypatch.setenv(
        fbref_pipeline_tasks.LEGACY_SCRAPER_PYTHON_ENV,
        sys.executable,
    )


def _freshness_summary(*, stale_kind: str | None = None) -> dict:
    by_kind = {}
    for kind in fbref_pipeline_tasks.FBREF_REQUIRED_CURRENT_PAGE_KINDS:
        stale = int(kind == stale_kind)
        by_kind[kind] = {
            "sla_seconds": (
                fbref_pipeline_tasks.FBREF_CURRENT_SCOPE_FRESHNESS_HOURS[kind]
                * 60
                * 60
            ),
            "total_targets": 1,
            "fresh_targets": 1 - stale,
            "stale_targets": stale,
            "never_fetched_targets": 0,
            "oldest_last_fetched_at": "2026-07-14T00:00:00+00:00",
        }
    return {
        "freshness_by_page_kind": by_kind,
        "current_scope_freshness": {
            "total_targets": len(by_kind),
            "fresh_targets": len(by_kind) - int(stale_kind is not None),
            "stale_targets": int(stale_kind is not None),
            "never_fetched_targets": 0,
            "all_within_sla": stale_kind is None,
        },
    }


@pytest.mark.unit
def test_runtime_limits_allow_only_hard_production_canary_and_replay_profiles():
    production = fbref_pipeline_tasks.validate_fbref_runtime_limits(
        run_type="current", request_limit=200, byte_limit_mb=100, shard_size=25
    )
    canary = fbref_pipeline_tasks.validate_fbref_runtime_limits(
        run_type="backfill", request_limit=100, byte_limit_mb=50, shard_size=1
    )
    replay = fbref_pipeline_tasks.validate_fbref_runtime_limits(
        run_type="replay", request_limit=0, byte_limit_mb=0, shard_size=25
    )

    assert production["profile"] == "production"
    assert canary["profile"] == "canary"
    assert replay["profile"] == "replay"

    with pytest.raises(ValueError, match="Unsupported FBref live budget"):
        fbref_pipeline_tasks.validate_fbref_runtime_limits(
            run_type="current",
            request_limit=200,
            byte_limit_mb=50,
            shard_size=25,
        )
    with pytest.raises(ValueError, match="between 1 and 25"):
        fbref_pipeline_tasks.validate_fbref_runtime_limits(
            run_type="current",
            request_limit=200,
            byte_limit_mb=100,
            shard_size=26,
        )
    with pytest.raises(ValueError, match="Unknown FBref run_type"):
        fbref_pipeline_tasks.validate_fbref_runtime_limits(
            run_type="adhoc",
            request_limit=200,
            byte_limit_mb=100,
            shard_size=25,
        )


@pytest.mark.unit
def test_production_readiness_combines_alert_env_and_runtime_limits(monkeypatch):
    monkeypatch.setenv("ALERT_ENV", "prod")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")

    result = fbref_pipeline_tasks.validate_fbref_production_readiness(
        run_type="current",
        request_limit=200,
        byte_limit_mb=100,
        shard_size=25,
    )

    assert result["status"] == "ready"
    assert result["alert_env"] == "prod"
    assert result["alert_delivery"] == "telegram"
    assert result["profile"] == "production"


@pytest.mark.unit
def test_publication_lock_task_uses_exact_control_generation(monkeypatch):
    control = MagicMock()
    control.acquire_publication_lock.return_value = {
        "acquired": True,
        "idempotent": False,
    }
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_control_store", MagicMock(return_value=control)
    )

    result = fbref_pipeline_tasks.acquire_fbref_publication_lock(
        airflow_run_id="scheduled__2026-07-14T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
    )

    owner = control.acquire_publication_lock.call_args.args[0]
    assert str(uuid.UUID(owner)) == owner
    assert control.acquire_publication_lock.call_args.kwargs == {
        "dag_id": "dag_ingest_fbref",
        "ttl_seconds": (
            fbref_pipeline_tasks.FBREF_PUBLICATION_LOCK_TTL_SECONDS
        ),
    }
    assert result["acquired"] is True

    control.release_publication_lock.return_value = {
        "released": True,
        "idempotent": False,
    }
    released = fbref_pipeline_tasks.release_fbref_publication_lock(
        control_run_id=owner
    )
    control.release_publication_lock.assert_called_once_with(owner)
    assert released["released"] is True


@pytest.mark.unit
def test_publication_lock_finalizer_propagates_failure_and_allows_dry_run(
    monkeypatch,
):
    release = MagicMock(return_value={"released": True})
    monkeypatch.setattr(
        fbref_pipeline_tasks, "release_fbref_publication_lock", release
    )
    success_run = SimpleNamespace(
        get_task_instances=lambda: [
            SimpleNamespace(
                task_id="acquire_publication_lock", state="success"
            ),
            SimpleNamespace(
                task_id="trigger_silver_transform", state="success"
            ),
        ]
    )
    result = fbref_pipeline_tasks.finalize_fbref_publication_lock(
        airflow_run_id="manual__backfill",
        dag_id="dag_backfill_fbref",
        dag_run=success_run,
    )
    assert result == {"released": True}
    release.assert_called_once()

    release.reset_mock()
    failed_run = SimpleNamespace(
        get_task_instances=lambda: [
            SimpleNamespace(
                task_id="acquire_publication_lock", state="success"
            ),
            SimpleNamespace(
                task_id="trigger_silver_transform", state="failed"
            ),
        ]
    )
    from airflow.exceptions import AirflowException

    with pytest.raises(AirflowException, match="lock retained"):
        fbref_pipeline_tasks.finalize_fbref_publication_lock(
            airflow_run_id="manual__backfill",
            dag_id="dag_backfill_fbref",
            dag_run=failed_run,
        )
    release.assert_not_called()

    never_started = SimpleNamespace(
        get_task_instances=lambda: [
            SimpleNamespace(
                task_id="acquire_publication_lock", state="success"
            ),
            SimpleNamespace(
                task_id="trigger_silver_transform",
                state="upstream_failed",
            ),
        ]
    )
    with pytest.raises(AirflowException, match="released because"):
        fbref_pipeline_tasks.finalize_fbref_publication_lock(
            airflow_run_id="manual__backfill",
            dag_id="dag_backfill_fbref",
            dag_run=never_started,
        )
    release.assert_called_once()

    release.reset_mock()
    dry_run = SimpleNamespace(
        get_task_instances=lambda: [
            SimpleNamespace(
                task_id="acquire_publication_lock", state="skipped"
            ),
            SimpleNamespace(task_id="plan_backfill", state="success"),
            SimpleNamespace(
                task_id="trigger_silver_transform", state="skipped"
            ),
        ]
    )
    assert fbref_pipeline_tasks.finalize_fbref_publication_lock(
        airflow_run_id="manual__dry-run",
        dag_id="dag_backfill_fbref",
        dag_run=dry_run,
    ) == {
        "released": False,
        "dry_run": True,
        "status": "not_acquired",
    }


@pytest.mark.unit
def test_publication_scope_preflight_requires_succeeded_hashed_male_rows(
    monkeypatch,
):
    control_run_id = "11111111-1111-4111-8111-111111111111"
    control = MagicMock()
    control.get_run.return_value = {"status": "succeeded"}
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_control_store", MagicMock(return_value=control)
    )
    manager = MagicMock()
    manager.execute_query.return_value = [(20, 15, 1, "abc123", 0)]
    import scrapers.base.trino_manager as trino_manager

    monkeypatch.setattr(
        trino_manager, "TrinoTableManager", MagicMock(return_value=manager)
    )

    result = fbref_pipeline_tasks.validate_fbref_publication_scope(
        control_run_id=control_run_id
    )
    assert result == {
        "control_run_id": control_run_id,
        "rows": 20,
        "eligible_male_rows": 15,
        "scope_hash": "abc123",
        "status": "ready",
    }

    manager.execute_query.return_value = [(20, 15, 0, None, 20)]
    with pytest.raises(RuntimeError, match="not immutable"):
        fbref_pipeline_tasks.validate_fbref_publication_scope(
            control_run_id=control_run_id
        )


@pytest.mark.unit
def test_backfill_dry_run_is_read_only_and_returns_exact_next_cohort(
    monkeypatch,
):
    control = MagicMock()
    control.list_backfill_seasons.return_value = [
        {
            "competition_id": "9",
            "season_id": "2024-2025",
            "canonical_url": "https://fbref.com/en/comps/9/2024-2025/source",
            "competition_name": "Premier League",
            "metadata": {"not": "published"},
        }
    ]
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_control_store", MagicMock(return_value=control)
    )
    pipeline_factory = MagicMock(side_effect=AssertionError("no pipeline"))
    monkeypatch.setattr(fbref_pipeline_tasks, "_pipeline", pipeline_factory)

    result = fbref_pipeline_tasks.plan_fbref_backfill(
        request_limit=100,
        byte_limit_mb=50,
        shard_size=25,
    )

    assert result["dry_run"] is True
    assert result["profile"] == "canary"
    assert result["network_requests"] == 0
    assert result["state_mutations"] == 0
    assert result["effective_cohort_limit"] == 7
    assert result["next_cohort_count"] == 1
    assert result["next_cohort"][0]["season_id"] == "2024-2025"
    control.list_backfill_seasons.assert_called_once_with(limit=7)
    pipeline_factory.assert_not_called()


@pytest.mark.unit
def test_publication_scope_export_is_atomic_and_keeps_quarantine_evidence(
    monkeypatch,
):
    control = MagicMock()
    control.list_publication_scope.return_value = [
        {
            "source_competition_id": "9",
            "source_season_id": "2025-2026",
            "canonical_season_id": "2025-2026",
            "scope_kind": "canonical",
            "competition_name": "Premier League",
            "gender": "male",
            "competition_crawl_state": "active",
            "competition_lifecycle_state": "present",
            "competition_present": True,
            "season_label": "2025-2026",
            "season_is_current": True,
            "season_lifecycle_state": "present",
            "season_present": True,
            "direct_match_only": False,
            "eligible_male": True,
        },
        {
            "source_competition_id": "189",
            "source_season_id": "2025-2026",
            "canonical_season_id": "2025-2026",
            "scope_kind": "canonical",
            "competition_name": "Women's Super League",
            "gender": "female",
            "competition_crawl_state": "skipped",
            "competition_lifecycle_state": "present",
            "competition_present": True,
            "season_label": "2025-2026",
            "season_is_current": True,
            "season_lifecycle_state": "present",
            "season_present": True,
            "direct_match_only": False,
            "eligible_male": False,
        },
    ]
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_control_store", MagicMock(return_value=control)
    )
    manager = MagicMock()
    manager.insert_dataframe_atomic.return_value = 2
    manager.get_table_columns.return_value = {}
    manager.execute_query.return_value = [(0, 0, None)]
    import scrapers.base.trino_manager as trino_manager

    monkeypatch.setattr(
        trino_manager, "TrinoTableManager", MagicMock(return_value=manager)
    )

    result = fbref_pipeline_tasks.export_fbref_publication_scope(
        airflow_run_id="scheduled__2026-07-14T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
    )

    assert result["eligible_male_rows"] == 1
    assert result["quarantined_rows"] == 1
    control.list_publication_scope.assert_called_once_with(source="fbref")
    write = manager.insert_dataframe_atomic.call_args
    assert write.args[:2] == ("bronze", "fbref_target_scope")
    control_run_id = result["control_run_id"]
    assert write.kwargs["delete_filter"] == (
        "source = 'fbref' AND control_run_id = "
        f"'{control_run_id}'"
    )
    assert write.kwargs["staging_id"] == (
        "scope_" + control_run_id.replace("-", "")
    )
    assert write.kwargs["single_statement_replace"] is True
    frame = write.args[2]
    assert set(frame["gender"]) == {"male", "female"}
    male = frame.loc[frame["gender"] == "male"].iloc[0]
    female = frame.loc[frame["gender"] == "female"].iloc[0]
    assert male["legacy_league"] == "ENG-Premier League"
    assert male["legacy_season"] == 2025
    assert female["legacy_league"] is None
    assert female["legacy_season"] is None
    assert set(frame["scope_hash"]) == {result["scope_hash"]}
    assert result["idempotent"] is False

    manager.insert_dataframe_atomic.reset_mock()
    manager.execute_query.return_value = [
        (2, 1, result["scope_hash"])
    ]
    replayed = fbref_pipeline_tasks.export_fbref_publication_scope(
        airflow_run_id="scheduled__2026-07-14T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
    )
    assert replayed["idempotent"] is True
    manager.insert_dataframe_atomic.assert_not_called()

    manager.execute_query.return_value = [(2, 1, "different-scope-hash")]
    with pytest.raises(RuntimeError, match="generation is immutable"):
        fbref_pipeline_tasks.export_fbref_publication_scope(
            airflow_run_id="scheduled__2026-07-14T06:00:00+00:00",
            dag_id="dag_ingest_fbref",
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("value", "expected"),
    [(True, "plan_backfill"), ("true", "plan_backfill"),
     (False, "validate_production_readiness"),
     ("false", "validate_production_readiness")],
)
def test_backfill_mode_branch_parses_native_and_string_booleans(value, expected):
    assert fbref_pipeline_tasks.choose_fbref_backfill_mode(
        dry_run=value
    ) == expected


@pytest.mark.unit
def test_recovery_wave_reuses_raw_before_live_fetch(monkeypatch):
    first = MagicMock()
    first.as_dict.return_value = {
        "cohort_size": 2,
        "claimed": 2,
        "parsed": 2,
    }
    drained = MagicMock()
    drained.as_dict.return_value = {"cohort_size": 0, "parsed": 0}
    pipeline = MagicMock()
    pipeline.recover_unprocessed_wave.side_effect = [first, drained]
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_pipeline", MagicMock(return_value=pipeline)
    )

    recovered = fbref_pipeline_tasks.run_recovery_wave(
        airflow_run_id="manual__raw-recovery",
        dag_id="dag_backfill_fbref",
        page_kinds=["schedule", "match"],
        run_type="backfill",
        request_limit=200,
        byte_limit_mb=100,
        shard_size=25,
    )

    assert recovered["batches"] == 1
    assert recovered["claimed"] == 2
    assert recovered["parsed"] == 2
    assert pipeline.recover_unprocessed_wave.call_count == 2
    call = pipeline.recover_unprocessed_wave.call_args_list[0]
    assert call.kwargs["page_kinds"] == ["schedule", "match"]
    assert call.kwargs["settings"].request_limit == 200
    pipeline.fetch_wave.assert_not_called()


@pytest.mark.unit
def test_recovery_drain_fails_before_transport_when_processing_is_stalled(
    monkeypatch,
):
    stalled = MagicMock()
    stalled.as_dict.return_value = {
        "cohort_size": 1,
        "claimed": 0,
        "parsed": 0,
    }
    pipeline = MagicMock()
    pipeline.recover_unprocessed_wave.return_value = stalled
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_pipeline", MagicMock(return_value=pipeline)
    )

    with pytest.raises(RuntimeError, match="made no progress"):
        fbref_pipeline_tasks.run_recovery_wave(
            airflow_run_id="manual__stalled-recovery",
            dag_id="dag_backfill_fbref",
            page_kinds=["match"],
            run_type="backfill",
            request_limit=200,
            byte_limit_mb=100,
            shard_size=25,
        )

    pipeline.fetch_wave.assert_not_called()


@pytest.mark.unit
def test_current_scope_freshness_accepts_complete_per_kind_evidence(monkeypatch):
    control = MagicMock()
    control.get_run_summary.return_value = _freshness_summary()
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_control_store", MagicMock(return_value=control)
    )

    result = fbref_pipeline_tasks.validate_fbref_current_scope_freshness(
        airflow_run_id="scheduled__2026-07-14T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
        run_type="current",
    )

    assert result["status"] == "passed"
    assert set(result["freshness_by_page_kind"]) == (
        fbref_pipeline_tasks.FBREF_REQUIRED_CURRENT_PAGE_KINDS
    )


@pytest.mark.unit
def test_current_scope_freshness_allows_new_final_match_inside_24h_sla(
    monkeypatch,
):
    summary = _freshness_summary()
    summary["freshness_by_page_kind"]["match"]["never_fetched_targets"] = 1
    summary["current_scope_freshness"]["never_fetched_targets"] = 1
    control = MagicMock()
    control.get_run_summary.return_value = summary
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_control_store", MagicMock(return_value=control)
    )

    result = fbref_pipeline_tasks.validate_fbref_current_scope_freshness(
        airflow_run_id="scheduled__2026-07-14T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
        run_type="current",
    )

    assert result["status"] == "passed"
    assert result["freshness_by_page_kind"]["match"][
        "never_fetched_targets"
    ] == 1


@pytest.mark.unit
def test_current_scope_freshness_fails_closed_for_stale_or_missing_evidence(
    monkeypatch,
):
    from airflow.exceptions import AirflowException

    control = MagicMock()
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_control_store", MagicMock(return_value=control)
    )
    control.get_run_summary.return_value = _freshness_summary(
        stale_kind="schedule"
    )
    with pytest.raises(AirflowException, match="schedule:stale=1"):
        fbref_pipeline_tasks.validate_fbref_current_scope_freshness(
            airflow_run_id="manual__stale",
            dag_id="dag_backfill_fbref",
            run_type="backfill",
        )

    control.get_run_summary.return_value = {"target_counts": {}}
    with pytest.raises(RuntimeError, match="no current-scope freshness"):
        fbref_pipeline_tasks.validate_fbref_current_scope_freshness(
            airflow_run_id="manual__missing",
            dag_id="dag_backfill_fbref",
            run_type="backfill",
        )


@pytest.mark.unit
def test_replay_freshness_is_not_applicable_without_control_query(monkeypatch):
    control_factory = MagicMock(side_effect=AssertionError("must not query"))
    monkeypatch.setattr(fbref_pipeline_tasks, "_control_store", control_factory)

    result = fbref_pipeline_tasks.validate_fbref_current_scope_freshness(
        airflow_run_id="manual__replay",
        dag_id="dag_replay_fbref",
        run_type="replay",
    )

    assert result == {"status": "not_applicable", "run_type": "replay"}
    control_factory.assert_not_called()


@pytest.mark.unit
def test_validate_records_control_budget_in_proxy_traffic_rollup(monkeypatch):
    summary = {
        "requests_used": 7,
        "bytes_used": 3 * 1024 * 1024,
        "target_counts": {"succeeded": 4},
        "dataset_validation_counts": {"succeeded": 12},
    }
    pipeline = MagicMock()
    pipeline.validate_and_finish.return_value = summary
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_pipeline", MagicMock(return_value=pipeline)
    )
    telemetry = SimpleNamespace(
        log_traffic_summary=MagicMock(),
        record_traffic_run=MagicMock(return_value=True),
    )
    monkeypatch.setitem(sys.modules, "utils.proxy_traffic", telemetry)

    result = fbref_pipeline_tasks.validate_fbref_run(
        airflow_run_id="scheduled__2026-07-11T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
    )

    assert result is summary
    traffic = telemetry.record_traffic_run.call_args.args[0]
    assert traffic["source"] == "fbref"
    assert traffic["total_mb"] == pytest.approx(3.0)
    assert telemetry.record_traffic_run.call_args.kwargs["dag_run_id"] == (
        "scheduled__2026-07-11T06:00:00+00:00"
    )
    assert telemetry.record_traffic_run.call_args.kwargs["replace_existing"] is True


@pytest.mark.unit
def test_control_traffic_bridge_is_non_fatal(monkeypatch):
    telemetry = SimpleNamespace(
        log_traffic_summary=MagicMock(side_effect=RuntimeError("trino unavailable")),
        record_traffic_run=MagicMock(),
    )
    monkeypatch.setitem(sys.modules, "utils.proxy_traffic", telemetry)

    fbref_pipeline_tasks._record_control_traffic(
        {"bytes_used": 1024}, airflow_run_id="manual__canary"
    )

    telemetry.record_traffic_run.assert_not_called()


@pytest.mark.unit
def test_replay_parse_rejects_import_safe_none_source_param(monkeypatch):
    pipeline = MagicMock(side_effect=AssertionError("must fail before pipeline"))
    monkeypatch.setattr(fbref_pipeline_tasks, "_pipeline", pipeline)

    with pytest.raises(ValueError, match="requires source_control_run_id"):
        fbref_pipeline_tasks.parse_fbref_wave(
            airflow_run_id="manual__missing-source",
            dag_id="dag_replay_fbref",
            page_kinds=["match"],
            run_type="replay",
            source_control_run_id=None,
            request_limit=0,
            byte_limit_mb=0,
            shard_size=1,
        )

    pipeline.assert_not_called()


@pytest.mark.unit
def test_replay_parse_propagates_source_run_preflight_failure(monkeypatch):
    pipeline = MagicMock()
    pipeline.parse_wave.side_effect = RuntimeError(
        "replay_source_run_not_found"
    )
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_pipeline", MagicMock(return_value=pipeline)
    )
    source_run_id = "00000000-0000-4000-8000-000000000099"

    with pytest.raises(RuntimeError, match="replay_source_run_not_found"):
        fbref_pipeline_tasks.parse_fbref_wave(
            airflow_run_id="manual__missing-source",
            dag_id="dag_replay_fbref",
            page_kinds=["match"],
            run_type="replay",
            source_control_run_id=source_run_id,
            request_limit=0,
            byte_limit_mb=0,
            shard_size=1,
        )

    assert pipeline.parse_wave.call_args.kwargs["source_run_id"] == source_run_id


@pytest.mark.unit
def test_replay_validation_propagates_source_run_preflight_failure(monkeypatch):
    pipeline = MagicMock()
    pipeline.validate_and_finish.side_effect = RuntimeError(
        "replay_source_run_not_terminal=running"
    )
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_pipeline", MagicMock(return_value=pipeline)
    )
    source_run_id = "00000000-0000-4000-8000-000000000099"

    with pytest.raises(RuntimeError, match="replay_source_run_not_terminal"):
        fbref_pipeline_tasks.validate_fbref_run(
            airflow_run_id="manual__live-source",
            dag_id="dag_replay_fbref",
            source_control_run_id=source_run_id,
        )

    assert (
        pipeline.validate_and_finish.call_args.kwargs["replay_source_run_id"]
        == source_run_id
    )


@pytest.mark.unit
def test_abort_callable_uses_control_store_without_constructing_pipeline(
    monkeypatch,
):
    control = MagicMock()
    control.abort_run.return_value = {
        "status": "failed",
        "targets_released": 2,
        "reservations_settled": 1,
    }
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "_control_store",
        MagicMock(return_value=control),
    )
    pipeline_factory = MagicMock(side_effect=AssertionError("must not construct"))
    monkeypatch.setattr(fbref_pipeline_tasks, "_pipeline", pipeline_factory)

    result = fbref_pipeline_tasks.abort_fbref_run(
        airflow_run_id="scheduled__2026-07-11T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
    )

    assert result["status"] == "failed"
    control.abort_run.assert_called_once_with(
        fbref_pipeline_tasks._control_run_id(
            airflow_run_id="scheduled__2026-07-11T06:00:00+00:00",
            dag_id="dag_ingest_fbref",
        ),
        error_class="AirflowDagFailure",
        error_message="Airflow DAG reached a terminal failure",
    )
    pipeline_factory.assert_not_called()


@pytest.mark.unit
def test_dag_failure_callback_aborts_without_telegram(monkeypatch):
    abort = MagicMock()
    monkeypatch.setattr(fbref_pipeline_tasks, "abort_fbref_run", abort)
    context = {
        "dag_run": SimpleNamespace(
            run_id="manual__failed", dag_id="dag_backfill_fbref"
        ),
        "task_instance": SimpleNamespace(task_id="fetch_wave_02"),
    }

    fbref_pipeline_tasks.fbref_dag_failure_callback(context)

    abort.assert_called_once_with(
        airflow_run_id="manual__failed",
        dag_id="dag_backfill_fbref",
        error_class="AirflowDagFailure",
        error_message="Airflow DAG failed after task fetch_wave_02",
    )
    callback_source = inspect.getsource(
        fbref_pipeline_tasks.fbref_dag_failure_callback
    ).casefold()
    assert "telegram" not in callback_source


@pytest.mark.unit
def test_dag_failure_callback_is_best_effort(monkeypatch):
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "abort_fbref_run",
        MagicMock(side_effect=RuntimeError("postgres unavailable")),
    )

    fbref_pipeline_tasks.fbref_dag_failure_callback({
        "dag_run": SimpleNamespace(
            run_id="manual__failed", dag_id="dag_replay_fbref"
        )
    })


@pytest.mark.unit
def test_fetch_wave_runs_in_an_unforked_subprocess(monkeypatch):
    """Playwright's sync API deadlocks in a process forked from the scheduler,
    so the browser wave must never be driven inside the Airflow task itself."""
    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return SimpleNamespace(
            returncode=0,
            stdout=(
                "camoufox noise\n"
                'FBREF_FETCH_WAVE_RESULT:{"claimed": 2, "fetched": 2}\n'
            ),
            stderr="",
        )

    monkeypatch.setattr(fbref_pipeline_tasks.subprocess, "run", fake_run)
    pipeline = MagicMock()
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_pipeline", MagicMock(return_value=pipeline)
    )

    result = fbref_pipeline_tasks.fetch_fbref_wave(
        airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
        worker_id="current-wave-01",
        page_kinds=["competition", "season"],
        run_type="current",
        request_limit=200,
        byte_limit_mb=100,
        shard_size=25,
    )

    assert result == {"claimed": 2, "fetched": 2}
    pipeline.fetch_wave.assert_not_called()
    command = captured["command"]
    assert command[0] == sys.executable
    assert command[1] == fbref_pipeline_tasks.FETCH_WAVE_RUNNER
    assert "--page-kinds" in command
    assert command[command.index("--page-kinds") + 1] == "competition,season"
    assert command[command.index("--request-limit") + 1] == "200"
    assert command[command.index("--proxy-file") + 1] == (
        fbref_pipeline_tasks.DEFAULT_PROXY_FILE
    )


@pytest.mark.unit
def test_fetch_wave_fails_closed_when_the_subprocess_fails(monkeypatch):
    abort = MagicMock()
    monkeypatch.setattr(fbref_pipeline_tasks, "abort_fbref_run", abort)
    monkeypatch.setattr(
        fbref_pipeline_tasks.subprocess,
        "run",
        lambda command, **kwargs: SimpleNamespace(
            returncode=1, stdout="", stderr="clearance failed"
        ),
    )

    with pytest.raises(RuntimeError, match="exit code 1"):
        fbref_pipeline_tasks.fetch_fbref_wave(
            airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
            dag_id="dag_ingest_fbref",
            worker_id="current-wave-01",
            page_kinds=["competition"],
            run_type="current",
        )
    abort.assert_called_once_with(
        airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
        error_class="FetchWaveSubprocessFailure",
        error_message="FBref fetch wave subprocess failed with exit code 1",
    )


@pytest.mark.unit
def test_fetch_wave_fails_closed_without_a_result_document(monkeypatch):
    monkeypatch.setattr(
        fbref_pipeline_tasks.subprocess,
        "run",
        lambda command, **kwargs: SimpleNamespace(
            returncode=0, stdout="browser noise only\n", stderr=""
        ),
    )

    with pytest.raises(RuntimeError, match="no result document"):
        fbref_pipeline_tasks.fetch_fbref_wave(
            airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
            dag_id="dag_ingest_fbref",
            worker_id="current-wave-01",
            page_kinds=["competition"],
            run_type="current",
        )


@pytest.mark.unit
def test_fetch_wave_fails_closed_when_the_subprocess_hangs(monkeypatch):
    """A hung clearance must not hold this wave's leases until they expire."""

    def fake_run(command, **kwargs):
        assert kwargs["timeout"] == fbref_pipeline_tasks.FETCH_WAVE_TIMEOUT_SECONDS
        raise fbref_pipeline_tasks.subprocess.TimeoutExpired(
            cmd=command, timeout=kwargs["timeout"]
        )

    monkeypatch.setattr(fbref_pipeline_tasks.subprocess, "run", fake_run)
    abort = MagicMock()
    monkeypatch.setattr(fbref_pipeline_tasks, "abort_fbref_run", abort)

    with pytest.raises(RuntimeError, match="exceeded 1800s"):
        fbref_pipeline_tasks.fetch_fbref_wave(
            airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
            dag_id="dag_ingest_fbref",
            worker_id="current-wave-01",
            page_kinds=["competition"],
            run_type="current",
        )
    abort.assert_called_once_with(
        airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
        error_class="FetchWaveSubprocessTimeout",
        error_message="FBref fetch wave subprocess exceeded 1800s",
    )
