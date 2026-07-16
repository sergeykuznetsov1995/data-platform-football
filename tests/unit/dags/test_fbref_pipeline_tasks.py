from __future__ import annotations

import dis
import inspect
import json
import sys
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dags.utils import fbref_pipeline_tasks


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
def test_canary_publication_path_is_non_publishing():
    assert (
        fbref_pipeline_tasks.choose_fbref_publication_path(
            request_limit=100, byte_limit_mb=50
        )
        == "validate_canary_run"
    )
    assert (
        fbref_pipeline_tasks.choose_fbref_publication_path(
            request_limit=200, byte_limit_mb=100
        )
        == "validate_current_scope_freshness"
    )

@pytest.mark.unit
def test_production_readiness_combines_alert_env_and_runtime_limits(monkeypatch):
    monkeypatch.setenv("ALERT_ENV", "prod")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
    monkeypatch.setenv("FBREF_RAW_STORE_URI", "s3://football/raw/fbref")
    monkeypatch.setenv(
        "FBREF_PROXY_CONTROL_URL", "http://fbref_proxy_filter:8899"
    )
    monkeypatch.setenv("FBREF_PROXY_CONTROL_TOKEN", "t" * 32)
    proxy_check = MagicMock(return_value={
        "configured": 1000,
        "minimum_configured": 4,
        "probe": "authenticated_control_only_zero_paid_bytes",
    })
    browser_check = MagicMock(return_value={
        "status": "passed",
        "camoufox_browser": "152.0.4-beta.26",
    })
    monkeypatch.setattr(
        "scrapers.fbref.readiness.validate_camoufox_runtime", browser_check
    )
    monkeypatch.setattr(
        "scrapers.fbref.readiness.validate_fbref_proxy_meter", proxy_check
    )
    control = MagicMock()
    control.validate_migrations.return_value = {
        "status": "passed",
        "versions": [1],
        "checksum_verified": True,
        "read_only": True,
    }
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_control_store", MagicMock(return_value=control)
    )
    raw_store = object()
    raw_from_uri = MagicMock(return_value=raw_store)
    raw_health = MagicMock(return_value={"status": "passed"})
    trino = object()
    trino_factory = MagicMock(return_value=trino)
    trino_health = MagicMock(return_value={"status": "passed"})
    monkeypatch.setattr(
        "scrapers.fbref.raw_store.RawPageStore.from_uri", raw_from_uri
    )
    monkeypatch.setattr(
        "scrapers.fbref.readiness.check_raw_store_roundtrip", raw_health
    )
    monkeypatch.setattr(
        "scrapers.base.trino_manager.TrinoTableManager", trino_factory
    )
    monkeypatch.setattr(
        "scrapers.fbref.readiness.check_trino_roundtrip", trino_health
    )

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
    assert result["configured"] == 1000
    assert result["dependencies"]["control_migrations"]["read_only"] is True
    assert result["dependencies"]["raw_store"] == {"status": "passed"}
    assert result["dependencies"]["trino"] == {"status": "passed"}
    assert result["dependencies"]["camoufox"] == {
        "status": "passed",
        "camoufox_browser": "152.0.4-beta.26",
    }
    control.validate_migrations.assert_called_once_with()
    raw_from_uri.assert_called_once_with("s3://football/raw/fbref")
    raw_health.assert_called_once_with(raw_store)
    trino_health.assert_called_once_with(trino)
    browser_check.assert_called_once_with()
    proxy_check.assert_called_once_with(
        "http://fbref_proxy_filter:8899",
        control_token="t" * 32,
        required_bytes=100 * fbref_pipeline_tasks.MIB,
        minimum_configured_exits=4,
    )

    replay = fbref_pipeline_tasks.validate_fbref_production_readiness(
        run_type="replay",
        request_limit=0,
        byte_limit_mb=0,
        shard_size=25,
    )

    assert replay["dependencies"]["camoufox"] == {
        "status": "not_required"
    }
    assert replay["dependencies"]["proxy_meter"] == {
        "status": "not_required"
    }
    browser_check.assert_called_once_with()
    proxy_check.assert_called_once()

    browser_check.side_effect = RuntimeError("broken browser")
    with pytest.raises(RuntimeError, match="broken browser"):
        fbref_pipeline_tasks.validate_fbref_production_readiness(
            run_type="current",
            request_limit=200,
            byte_limit_mb=100,
            shard_size=25,
        )
    # A broken local browser must stop the run before the paid proxy is probed.
    proxy_check.assert_called_once()


@pytest.mark.unit
def test_raw_baseline_is_persistent_idempotent_and_never_overwritten(
    monkeypatch, tmp_path
):
    monkeypatch.setenv(
        fbref_pipeline_tasks.FBREF_ACCEPTANCE_OUTPUT_ROOT_ENV,
        str(tmp_path / "acceptance"),
    )
    from scrapers.fbref.raw_store import RawPageStore

    raw_store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    raw_store._write_bytes("immutable/one.bin", b"one")
    monkeypatch.setattr(
        "scrapers.fbref.raw_store.RawPageStore.from_env",
        MagicMock(return_value=raw_store),
    )
    control = MagicMock()
    control.record_raw_baseline.side_effect = [
        {"idempotent": False},
        {"idempotent": True},
        {"idempotent": True},
    ]
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_control_store", MagicMock(return_value=control)
    )

    kwargs = {
        "airflow_run_id": "scheduled__2026-07-15T06:00:00+00:00",
        "dag_id": "dag_ingest_fbref",
    }
    first = fbref_pipeline_tasks.capture_fbref_raw_baseline(**kwargs)
    second = fbref_pipeline_tasks.capture_fbref_raw_baseline(**kwargs)

    assert first["idempotent"] is False
    assert second["idempotent"] is True
    assert first["baseline_sha256"] == second["baseline_sha256"]
    baseline_path = (
        tmp_path
        / "acceptance"
        / first["control_run_id"]
        / fbref_pipeline_tasks.FBREF_RAW_BASELINE_FILENAME
    )
    assert json.loads(baseline_path.read_text())["object_count"] == 1
    assert baseline_path.with_name(
        f"{baseline_path.name}.sqlite3"
    ).is_file()

    # A retry after source progress returns the original anchored evidence;
    # it must not recapture the now-mutated raw store.
    raw_store._write_bytes("immutable/two.bin", b"two")
    retried = fbref_pipeline_tasks.capture_fbref_raw_baseline(**kwargs)
    assert retried["fingerprint_sha256"] == first["fingerprint_sha256"]
    assert retried["object_count"] == 1
    assert retried["idempotent"] is True
    assert control.record_raw_baseline.call_count == 3


@pytest.mark.unit
@pytest.mark.parametrize("run_type", ["current", "replay"])
def test_airflow_raw_audit_is_a_persisted_publication_gate(
    monkeypatch, tmp_path, run_type
):
    monkeypatch.setenv(
        fbref_pipeline_tasks.FBREF_ACCEPTANCE_OUTPUT_ROOT_ENV,
        str(tmp_path / "acceptance"),
    )
    airflow_run_id = "manual__raw-audit"
    dag_id = (
        "dag_replay_fbref" if run_type == "replay" else "dag_ingest_fbref"
    )
    processing_run_id = fbref_pipeline_tasks._control_run_id(
        airflow_run_id=airflow_run_id, dag_id=dag_id
    )
    baseline_path = fbref_pipeline_tasks._fbref_raw_baseline_path(
        processing_run_id
    )
    baseline_path.parent.mkdir(parents=True)
    from scrapers.fbref.raw_audit import (
        capture_and_write_raw_inventory,
        raw_baseline_anchor,
        successful_attempt_snapshot,
    )
    from scrapers.fbref.raw_store import RawPageStore

    baseline_store = RawPageStore.from_uri(
        (tmp_path / "baseline-raw").as_uri()
    )
    _, baseline, _ = capture_and_write_raw_inventory(
        baseline_store, baseline_path
    )
    source_run_id = "11111111-1111-4111-8111-111111111111"
    audited_run_id = source_run_id if run_type == "replay" else processing_run_id
    attempts = [
        {
            "attempt_id": str(uuid.uuid4()),
            "logical_refresh_id": str(uuid.uuid4()),
        }
    ]
    attempt_snapshot = successful_attempt_snapshot(attempts)
    load = MagicMock(return_value=attempts)
    monkeypatch.setattr(
        "scrapers.fbref.raw_audit.load_successful_run_attempts", load
    )
    audit = MagicMock(
        return_value={
            "control_run_id": audited_run_id,
            "status": "passed",
            "successful_attempt_count": 1,
            "audited_attempt_count": 1,
            "failures": [],
            "metadata": {
                "processing_control_run_id": processing_run_id,
                "raw_attempt_snapshot_sha256": attempt_snapshot[
                    "successful_attempt_ids_sha256"
                ],
            },
        }
    )
    monkeypatch.setattr("scrapers.fbref.raw_audit.audit_raw_fetches", audit)
    raw_store = object()
    monkeypatch.setattr(
        "scrapers.fbref.raw_store.RawPageStore.from_env",
        MagicMock(return_value=raw_store),
    )
    control = MagicMock()
    control.get_raw_baseline.return_value = raw_baseline_anchor(
        baseline.summary, baseline.baseline_sha256
    )
    installed_raw_audit = None

    def get_raw_audit(_run_id):
        return installed_raw_audit

    def record_raw_audit(_run_id, evidence):
        nonlocal installed_raw_audit
        installed_raw_audit = dict(evidence)
        return {**installed_raw_audit, "idempotent": False}

    control.get_raw_audit.side_effect = get_raw_audit
    control.seal_raw_fetch_attempts.return_value = attempt_snapshot
    control.record_raw_audit.side_effect = record_raw_audit
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_control_store", MagicMock(return_value=control)
    )

    summary = fbref_pipeline_tasks.audit_fbref_raw_integrity(
        airflow_run_id=airflow_run_id,
        dag_id=dag_id,
        run_type=run_type,
        source_control_run_id=(
            source_run_id if run_type == "replay" else None
        ),
    )

    load.assert_called_once_with(control, audited_run_id)
    assert audit.call_args.args == (raw_store, attempts)
    assert audit.call_args.kwargs["baseline_inventory"].summary == (
        baseline.summary
    )
    assert audit.call_args.kwargs["require_baseline"] is True
    assert audit.call_args.kwargs["require_nonempty"] is True
    assert audit.call_args.kwargs["require_zero_delta"] is (
        run_type == "replay"
    )
    assert summary["status"] == "passed"
    assert summary["zero_delta_required"] is (run_type == "replay")
    assert summary["processing_control_run_id"] == processing_run_id
    assert summary["artifact_sha256"]
    assert summary["artifact"].startswith(str(tmp_path / "acceptance"))
    assert summary["attempt_snapshot_sha256"] == (
        attempt_snapshot["successful_attempt_ids_sha256"]
    )
    assert summary["control_anchored"] is True
    assert summary["idempotent"] is False
    assert control.seal_raw_fetch_attempts.call_count == 2
    assert control.record_raw_audit.call_count == 1
    anchored_run_id, anchored = control.record_raw_audit.call_args.args
    assert anchored_run_id == processing_run_id
    assert anchored["audited_control_run_id"] == audited_run_id
    assert anchored["artifact_sha256"] == summary["artifact_sha256"]

    # Simulate a worker crash after the database anchor committed but before
    # Airflow recorded task success. The retry verifies and reuses that exact
    # artifact instead of generating a timestamp-dependent conflicting one.
    retried = fbref_pipeline_tasks.audit_fbref_raw_integrity(
        airflow_run_id=airflow_run_id,
        dag_id=dag_id,
        run_type=run_type,
        source_control_run_id=(
            source_run_id if run_type == "replay" else None
        ),
    )

    assert retried["idempotent"] is True
    assert retried["artifact_sha256"] == summary["artifact_sha256"]
    assert audit.call_count == 1
    assert load.call_count == 1
    assert control.record_raw_audit.call_count == 1
    assert control.seal_raw_fetch_attempts.call_count == 3


@pytest.mark.unit
def test_airflow_raw_audit_rejects_a_replaced_baseline_file(
    monkeypatch, tmp_path
):
    monkeypatch.setenv(
        fbref_pipeline_tasks.FBREF_ACCEPTANCE_OUTPUT_ROOT_ENV,
        str(tmp_path / "acceptance"),
    )
    airflow_run_id = "manual__replaced-baseline"
    dag_id = "dag_ingest_fbref"
    run_id = fbref_pipeline_tasks._control_run_id(
        airflow_run_id=airflow_run_id, dag_id=dag_id
    )
    path = fbref_pipeline_tasks._fbref_raw_baseline_path(run_id)
    path.parent.mkdir(parents=True)
    from scrapers.fbref.raw_audit import (
        DiskBackedRawInventory,
        capture_and_write_raw_inventory,
        raw_baseline_anchor,
    )
    from scrapers.fbref.raw_store import RawPageStore

    baseline_store = RawPageStore.from_uri(
        (tmp_path / "baseline-raw").as_uri()
    )
    _, installed, _ = capture_and_write_raw_inventory(baseline_store, path)
    anchor = raw_baseline_anchor(
        installed.summary, installed.baseline_sha256
    )
    replaced = DiskBackedRawInventory(
        summary={**installed.summary, "fingerprint_sha256": "c" * 64},
        baseline_sha256="d" * 64,
        index_path=installed.index_path,
    )
    control = MagicMock()
    control.get_raw_baseline.return_value = anchor
    control.get_raw_audit.return_value = None
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_control_store", MagicMock(return_value=control)
    )
    monkeypatch.setattr(
        "scrapers.fbref.raw_audit.open_disk_backed_inventory",
        MagicMock(return_value=replaced),
    )

    with pytest.raises(RuntimeError, match="control-plane anchor"):
        fbref_pipeline_tasks.audit_fbref_raw_integrity(
            airflow_run_id=airflow_run_id,
            dag_id=dag_id,
            run_type="current",
        )

    control.seal_raw_fetch_attempts.assert_not_called()


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

    release.reset_mock()
    canary_run = SimpleNamespace(
        get_task_instances=lambda: [
            SimpleNamespace(
                task_id="acquire_publication_lock", state="success"
            ),
            SimpleNamespace(
                task_id="release_canary_publication_lock", state="success"
            ),
            SimpleNamespace(
                task_id="trigger_silver_transform", state="upstream_failed"
            ),
        ]
    )
    assert fbref_pipeline_tasks.finalize_fbref_publication_lock(
        airflow_run_id="manual__canary",
        dag_id="dag_ingest_fbref",
        dag_run=canary_run,
    ) == {
        "released": True,
        "canary": True,
        "status": "released_by_canary_path",
    }
    release.assert_not_called()


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
    control.guard_publication_lock.assert_called_once_with(
        result["control_run_id"], source="fbref"
    )
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
    assert control.guard_publication_lock.call_count == 2
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
def test_backfill_freshness_preflight_blocks_pending_current_matches(monkeypatch):
    from airflow.exceptions import AirflowFailException

    control = MagicMock()
    summary = _freshness_summary()
    summary["promotion_pending_match_count"] = 4
    control.get_run_summary.return_value = summary
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_control_store", MagicMock(return_value=control)
    )

    with pytest.raises(
        AirflowFailException, match="promotion_pending_match_count=4"
    ):
        fbref_pipeline_tasks.validate_fbref_current_scope_freshness(
            airflow_run_id="manual__backfill",
            dag_id="dag_backfill_fbref",
            run_type="backfill",
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
    from airflow.exceptions import AirflowFailException

    control = MagicMock()
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_control_store", MagicMock(return_value=control)
    )
    control.get_run_summary.return_value = _freshness_summary(
        stale_kind="schedule"
    )
    with pytest.raises(AirflowFailException, match="schedule:stale=1"):
        fbref_pipeline_tasks.validate_fbref_current_scope_freshness(
            airflow_run_id="manual__stale",
            dag_id="dag_backfill_fbref",
            run_type="backfill",
        )

    from airflow.exceptions import AirflowException

    with pytest.raises(AirflowException) as retryable:
        fbref_pipeline_tasks.validate_fbref_current_scope_freshness(
            airflow_run_id="manual__stale",
            dag_id="dag_backfill_fbref",
            run_type="backfill",
            fail_fast=False,
        )
    assert not isinstance(retryable.value, AirflowFailException)

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
def test_live_waves_use_one_process_group_for_all_batches(monkeypatch):
    captured = {}

    class Process:
        pid = 1234
        returncode = 0

        def communicate(self, *, timeout=None):
            assert timeout == fbref_pipeline_tasks.LIVE_WAVES_TIMEOUT_SECONDS
            return (
                'FBREF_LIVE_WAVES_RESULT:{"batches": 3, '
                '"frontier_closed": true}\n',
                "",
            )

    def popen(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return Process()

    monkeypatch.setattr(fbref_pipeline_tasks.subprocess, "Popen", popen)

    result = fbref_pipeline_tasks.run_fbref_live_waves(
        airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
        worker_id="current-live",
        page_kinds=["competition", "season"],
        run_type="current",
        request_limit=200,
        byte_limit_mb=100,
        shard_size=25,
    )

    assert result == {"batches": 3, "frontier_closed": True}
    assert captured["kwargs"]["start_new_session"] is True
    command = captured["command"]
    assert command[1] == fbref_pipeline_tasks.LIVE_WAVES_RUNNER
    assert command[command.index("--parent-pid") + 1] == str(
        fbref_pipeline_tasks.os.getpid()
    )
    assert command[command.index("--max-batches") + 1] == "16"
    assert command[command.index("--reservation-mb") + 1] == "3"


@pytest.mark.unit
def test_live_waves_reject_success_with_a_surviving_descendant(monkeypatch):
    calls = 0

    class Process:
        pid = 3210
        returncode = 0

        def communicate(self, *, timeout=None):
            nonlocal calls
            calls += 1
            if calls == 1:
                return 'FBREF_LIVE_WAVES_RESULT:{"batches": 1}\n', ""
            return "cleanup stdout", "cleanup stderr"

    monkeypatch.setattr(
        fbref_pipeline_tasks.subprocess,
        "Popen",
        lambda *args, **kwargs: Process(),
    )
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "_process_group_exists",
        lambda process_group_id: True,
    )
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "_wait_for_process_group_exit",
        lambda *args, **kwargs: True,
    )
    killed = []
    monkeypatch.setattr(
        fbref_pipeline_tasks.os,
        "killpg",
        lambda pid, sig: killed.append((pid, sig)),
    )

    with pytest.raises(RuntimeError, match="descendants remained"):
        fbref_pipeline_tasks.run_fbref_live_waves(
            airflow_run_id="manual__surviving-descendant",
            dag_id="dag_ingest_fbref",
            worker_id="current-live",
            page_kinds=["competition"],
            run_type="current",
            request_limit=200,
            byte_limit_mb=100,
            shard_size=25,
        )

    assert calls == 2
    assert killed == [(3210, fbref_pipeline_tasks.signal.SIGTERM)]


@pytest.mark.unit
def test_live_waves_timeout_terminates_the_complete_process_group(monkeypatch):
    calls = 0

    class Process:
        pid = 4321
        returncode = None

        def communicate(self, *, timeout=None):
            nonlocal calls
            calls += 1
            if calls == 1:
                assert timeout == fbref_pipeline_tasks.LIVE_WAVES_TIMEOUT_SECONDS
                raise fbref_pipeline_tasks.subprocess.TimeoutExpired(
                    cmd=["runner"], timeout=timeout
                )
            assert timeout == (
                fbref_pipeline_tasks.LIVE_WAVES_TERMINATION_GRACE_SECONDS
            )
            return "partial stdout", "partial stderr"

    monkeypatch.setattr(
        fbref_pipeline_tasks.subprocess,
        "Popen",
        lambda *args, **kwargs: Process(),
    )
    killed = []
    monkeypatch.setattr(
        fbref_pipeline_tasks.os,
        "killpg",
        lambda pid, sig: killed.append((pid, sig)),
    )
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "_wait_for_process_group_exit",
        lambda *args, **kwargs: True,
    )
    abort = MagicMock()
    monkeypatch.setattr(fbref_pipeline_tasks, "abort_fbref_run", abort)

    with pytest.raises(RuntimeError, match="process group was killed"):
        fbref_pipeline_tasks.run_fbref_live_waves(
            airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
            dag_id="dag_ingest_fbref",
            worker_id="current-live",
            page_kinds=["competition"],
            run_type="current",
            request_limit=200,
            byte_limit_mb=100,
            shard_size=25,
        )

    assert killed == [(4321, fbref_pipeline_tasks.signal.SIGTERM)]
    abort.assert_called_once_with(
        airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
        error_class="LiveWavesSubprocessTimeout",
        error_message="FBref live runner exceeded 6600s",
    )


@pytest.mark.unit
def test_live_waves_external_interruption_terminates_process_group(monkeypatch):
    calls = 0

    class Process:
        pid = 5432
        returncode = None

        def communicate(self, *, timeout=None):
            nonlocal calls
            calls += 1
            if calls == 1:
                assert timeout == fbref_pipeline_tasks.LIVE_WAVES_TIMEOUT_SECONDS
                raise KeyboardInterrupt
            assert timeout == (
                fbref_pipeline_tasks.LIVE_WAVES_TERMINATION_GRACE_SECONDS
            )
            return "interrupted stdout", "interrupted stderr"

    monkeypatch.setattr(
        fbref_pipeline_tasks.subprocess,
        "Popen",
        lambda *args, **kwargs: Process(),
    )
    killed = []
    monkeypatch.setattr(
        fbref_pipeline_tasks.os,
        "killpg",
        lambda pid, sig: killed.append((pid, sig)),
    )
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "_wait_for_process_group_exit",
        lambda *args, **kwargs: True,
    )

    with pytest.raises(KeyboardInterrupt):
        fbref_pipeline_tasks.run_fbref_live_waves(
            airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
            dag_id="dag_ingest_fbref",
            worker_id="current-live",
            page_kinds=["competition"],
            run_type="current",
            request_limit=200,
            byte_limit_mb=100,
            shard_size=25,
        )

    assert calls == 2
    assert killed == [(5432, fbref_pipeline_tasks.signal.SIGTERM)]


@pytest.mark.unit
def test_live_waves_sigterm_reaps_child_and_restores_signal_handlers(
    monkeypatch,
):
    calls = 0
    previous_sigterm = fbref_pipeline_tasks.signal.getsignal(
        fbref_pipeline_tasks.signal.SIGTERM
    )
    previous_sigalrm = fbref_pipeline_tasks.signal.getsignal(
        fbref_pipeline_tasks.signal.SIGALRM
    )

    class Process:
        pid = 7654
        returncode = None

        def communicate(self, *, timeout=None):
            nonlocal calls
            calls += 1
            if calls == 1:
                assert timeout == fbref_pipeline_tasks.LIVE_WAVES_TIMEOUT_SECONDS
                fbref_pipeline_tasks.os.kill(
                    fbref_pipeline_tasks.os.getpid(),
                    fbref_pipeline_tasks.signal.SIGTERM,
                )
                raise AssertionError("SIGTERM handler did not interrupt wait")
            assert timeout == (
                fbref_pipeline_tasks.LIVE_WAVES_TERMINATION_GRACE_SECONDS
            )
            return "terminated stdout", "terminated stderr"

    monkeypatch.setattr(
        fbref_pipeline_tasks.subprocess,
        "Popen",
        lambda *args, **kwargs: Process(),
    )
    killed = []
    monkeypatch.setattr(
        fbref_pipeline_tasks.os,
        "killpg",
        lambda pid, sig: killed.append((pid, sig)),
    )
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "_wait_for_process_group_exit",
        lambda *args, **kwargs: True,
    )

    with pytest.raises(
        fbref_pipeline_tasks._LiveRunnerTermination
    ) as terminated:
        fbref_pipeline_tasks.run_fbref_live_waves(
            airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
            dag_id="dag_ingest_fbref",
            worker_id="current-live",
            page_kinds=["competition"],
            run_type="current",
            request_limit=200,
            byte_limit_mb=100,
            shard_size=25,
        )

    assert terminated.value.code == 128 + fbref_pipeline_tasks.signal.SIGTERM
    assert calls == 2
    assert killed == [(7654, fbref_pipeline_tasks.signal.SIGTERM)]
    assert (
        fbref_pipeline_tasks.signal.getsignal(
            fbref_pipeline_tasks.signal.SIGTERM
        )
        is previous_sigterm
    )
    assert (
        fbref_pipeline_tasks.signal.getsignal(
            fbref_pipeline_tasks.signal.SIGALRM
        )
        is previous_sigalrm
    )


@pytest.mark.unit
def test_live_waves_repeated_sigterm_cannot_interrupt_group_cleanup(
    monkeypatch,
):
    calls = 0
    previous_sigterm = fbref_pipeline_tasks.signal.getsignal(
        fbref_pipeline_tasks.signal.SIGTERM
    )

    class Process:
        pid = 7755
        returncode = None

        def communicate(self, *, timeout=None):
            nonlocal calls
            calls += 1
            if calls == 1:
                fbref_pipeline_tasks.os.kill(
                    fbref_pipeline_tasks.os.getpid(),
                    fbref_pipeline_tasks.signal.SIGTERM,
                )
                raise AssertionError("first SIGTERM did not interrupt wait")
            return "terminated stdout", "terminated stderr"

    monkeypatch.setattr(
        fbref_pipeline_tasks.subprocess,
        "Popen",
        lambda *args, **kwargs: Process(),
    )
    killed = []

    def killpg(pid, sig):
        killed.append((pid, sig))
        if sig == fbref_pipeline_tasks.signal.SIGTERM:
            fbref_pipeline_tasks.os.kill(
                fbref_pipeline_tasks.os.getpid(),
                fbref_pipeline_tasks.signal.SIGTERM,
            )

    monkeypatch.setattr(fbref_pipeline_tasks.os, "killpg", killpg)
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "_wait_for_process_group_exit",
        lambda *args, **kwargs: True,
    )

    with pytest.raises(
        fbref_pipeline_tasks._LiveRunnerTermination
    ) as terminated:
        fbref_pipeline_tasks.run_fbref_live_waves(
            airflow_run_id="scheduled__repeated-sigterm",
            dag_id="dag_ingest_fbref",
            worker_id="current-live",
            page_kinds=["competition"],
            run_type="current",
            request_limit=200,
            byte_limit_mb=100,
            shard_size=25,
        )

    assert terminated.value.code == 128 + fbref_pipeline_tasks.signal.SIGTERM
    assert calls == 2
    assert killed == [(7755, fbref_pipeline_tasks.signal.SIGTERM)]
    assert (
        fbref_pipeline_tasks.signal.getsignal(
            fbref_pipeline_tasks.signal.SIGTERM
        )
        is previous_sigterm
    )


@pytest.mark.unit
def test_live_waves_sigterm_on_timeout_handler_boundary_still_reaps_group(
    monkeypatch,
):
    calls = 0

    class Process:
        pid = 7766
        returncode = None

        def communicate(self, *, timeout=None):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise fbref_pipeline_tasks.subprocess.TimeoutExpired(
                    cmd=["runner"], timeout=timeout
                )
            return "terminated stdout", "terminated stderr"

    monkeypatch.setattr(
        fbref_pipeline_tasks.subprocess,
        "Popen",
        lambda *args, **kwargs: Process(),
    )
    killed = []
    monkeypatch.setattr(
        fbref_pipeline_tasks.os,
        "killpg",
        lambda pid, sig: killed.append((pid, sig)),
    )
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "_wait_for_process_group_exit",
        lambda *args, **kwargs: True,
    )

    function = fbref_pipeline_tasks.run_fbref_live_waves
    source, first_line = inspect.getsourcelines(function)
    timeout_index = next(
        index
        for index, line in enumerate(source)
        if "except subprocess.TimeoutExpired as exc:" in line
    )
    boundary_line = first_line + timeout_index + 1
    fired = False

    def trace_timeout_boundary(frame, event, _arg):
        nonlocal fired
        if (
            frame.f_code is function.__code__
            and event == "line"
            and frame.f_lineno == boundary_line
            and not fired
        ):
            fired = True
            fbref_pipeline_tasks.os.kill(
                fbref_pipeline_tasks.os.getpid(),
                fbref_pipeline_tasks.signal.SIGTERM,
            )
        return trace_timeout_boundary

    previous_trace = sys.gettrace()
    sys.settrace(trace_timeout_boundary)
    try:
        with pytest.raises(
            fbref_pipeline_tasks._LiveRunnerTermination
        ) as terminated:
            function(
                airflow_run_id="scheduled__timeout-boundary",
                dag_id="dag_ingest_fbref",
                worker_id="current-live",
                page_kinds=["competition"],
                run_type="current",
                request_limit=200,
                byte_limit_mb=100,
                shard_size=25,
            )
    finally:
        sys.settrace(previous_trace)

    assert fired is True
    assert terminated.value.code == 128 + fbref_pipeline_tasks.signal.SIGTERM
    assert calls == 2
    assert killed == [(7766, fbref_pipeline_tasks.signal.SIGTERM)]


@pytest.mark.unit
def test_live_waves_sigterm_during_handler_restore_is_not_lost(monkeypatch):
    previous_sigterm = fbref_pipeline_tasks.signal.getsignal(
        fbref_pipeline_tasks.signal.SIGTERM
    )

    class Process:
        pid = 7777
        returncode = 0

        def communicate(self, *, timeout=None):
            return 'FBREF_LIVE_WAVES_RESULT:{"batches": 1}\n', ""

    monkeypatch.setattr(
        fbref_pipeline_tasks.subprocess,
        "Popen",
        lambda *args, **kwargs: Process(),
    )
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "_process_group_exists",
        lambda _process_group_id: False,
    )
    real_signal = fbref_pipeline_tasks.signal.signal
    restore_hook_fired = False

    def signal_with_restore_hook(signum, handler):
        nonlocal restore_hook_fired
        if (
            signum == fbref_pipeline_tasks.signal.SIGTERM
            and handler is previous_sigterm
            and not restore_hook_fired
        ):
            restore_hook_fired = True
            installed = fbref_pipeline_tasks.signal.getsignal(signum)
            installed(signum, None)
        return real_signal(signum, handler)

    monkeypatch.setattr(
        fbref_pipeline_tasks.signal, "signal", signal_with_restore_hook
    )

    with pytest.raises(
        fbref_pipeline_tasks._LiveRunnerTermination
    ) as terminated:
        fbref_pipeline_tasks.run_fbref_live_waves(
            airflow_run_id="scheduled__restore-boundary",
            dag_id="dag_ingest_fbref",
            worker_id="current-live",
            page_kinds=["competition"],
            run_type="current",
            request_limit=200,
            byte_limit_mb=100,
            shard_size=25,
        )

    assert restore_hook_fired is True
    assert terminated.value.code == 128 + fbref_pipeline_tasks.signal.SIGTERM
    assert (
        fbref_pipeline_tasks.signal.getsignal(
            fbref_pipeline_tasks.signal.SIGTERM
        )
        is previous_sigterm
    )


@pytest.mark.unit
def test_live_waves_sigterm_during_spawn_reaps_the_returned_group(monkeypatch):
    previous_sigterm = fbref_pipeline_tasks.signal.getsignal(
        fbref_pipeline_tasks.signal.SIGTERM
    )

    class Process:
        pid = 8765
        returncode = None

        def communicate(self, *, timeout=None):
            assert timeout == (
                fbref_pipeline_tasks.LIVE_WAVES_TERMINATION_GRACE_SECONDS
            )
            return "spawn stdout", "spawn stderr"

    def popen(*args, **kwargs):
        fbref_pipeline_tasks.os.kill(
            fbref_pipeline_tasks.os.getpid(),
            fbref_pipeline_tasks.signal.SIGTERM,
        )
        return Process()

    monkeypatch.setattr(fbref_pipeline_tasks.subprocess, "Popen", popen)
    killed = []
    monkeypatch.setattr(
        fbref_pipeline_tasks.os,
        "killpg",
        lambda pid, sig: killed.append((pid, sig)),
    )
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "_wait_for_process_group_exit",
        lambda *args, **kwargs: True,
    )

    with pytest.raises(
        fbref_pipeline_tasks._LiveRunnerTermination
    ) as terminated:
        fbref_pipeline_tasks.run_fbref_live_waves(
            airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
            dag_id="dag_ingest_fbref",
            worker_id="current-live",
            page_kinds=["competition"],
            run_type="current",
            request_limit=200,
            byte_limit_mb=100,
            shard_size=25,
        )

    assert terminated.value.code == 128 + fbref_pipeline_tasks.signal.SIGTERM
    assert killed == [(8765, fbref_pipeline_tasks.signal.SIGTERM)]
    assert (
        fbref_pipeline_tasks.signal.getsignal(
            fbref_pipeline_tasks.signal.SIGTERM
        )
        is previous_sigterm
    )


@pytest.mark.unit
def test_live_waves_sigterm_on_spawn_return_bytecode_reaps_group(monkeypatch):
    previous_sigterm = fbref_pipeline_tasks.signal.getsignal(
        fbref_pipeline_tasks.signal.SIGTERM
    )

    class Process:
        pid = 8790
        returncode = None

        def communicate(self, *, timeout=None):
            assert timeout == (
                fbref_pipeline_tasks.LIVE_WAVES_TERMINATION_GRACE_SECONDS
            )
            return "boundary stdout", "boundary stderr"

    monkeypatch.setattr(
        fbref_pipeline_tasks.subprocess,
        "Popen",
        lambda *args, **kwargs: Process(),
    )
    killed = []
    monkeypatch.setattr(
        fbref_pipeline_tasks.os,
        "killpg",
        lambda pid, sig: killed.append((pid, sig)),
    )
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "_wait_for_process_group_exit",
        lambda *args, **kwargs: True,
    )

    function = fbref_pipeline_tasks.run_fbref_live_waves
    instructions = list(dis.get_instructions(function))
    spawn_load_index = next(
        index
        for index, instruction in enumerate(instructions)
        if instruction.argval == "_spawn_live_runner"
    )
    boundary_instruction = next(
        instruction
        for instruction in instructions[spawn_load_index + 1 :]
        if instruction.starts_line is not None
    )
    boundary_offset = boundary_instruction.offset
    boundary_line = boundary_instruction.starts_line
    assert any(
        entry.start <= boundary_offset < entry.end
        for entry in dis.Bytecode(function).exception_entries
    )

    fired = False

    def trace_boundary(frame, event, _arg):
        nonlocal fired
        if frame.f_code is function.__code__:
            if (
                event == "line"
                and frame.f_lineno == boundary_line
                and not fired
            ):
                fired = True
                fbref_pipeline_tasks.os.kill(
                    fbref_pipeline_tasks.os.getpid(),
                    fbref_pipeline_tasks.signal.SIGTERM,
                )
            return trace_boundary
        return None

    previous_trace = sys.gettrace()
    sys.settrace(trace_boundary)
    try:
        with pytest.raises(
            fbref_pipeline_tasks._LiveRunnerTermination
        ) as terminated:
            function(
                airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
                dag_id="dag_ingest_fbref",
                worker_id="current-live",
                page_kinds=["competition"],
                run_type="current",
                request_limit=200,
                byte_limit_mb=100,
                shard_size=25,
            )
    finally:
        sys.settrace(previous_trace)

    assert fired is True
    assert terminated.value.code == 128 + fbref_pipeline_tasks.signal.SIGTERM
    assert killed == [(8790, fbref_pipeline_tasks.signal.SIGTERM)]
    assert (
        fbref_pipeline_tasks.signal.getsignal(
            fbref_pipeline_tasks.signal.SIGTERM
        )
        is previous_sigterm
    )


@pytest.mark.unit
def test_process_group_cleanup_kills_descendants_after_leader_exit(monkeypatch):
    class Process:
        pid = 8876
        returncode = 0

        def poll(self):
            return 0

        def communicate(self, *, timeout=None):
            return "leader exited", ""

    killed = []
    monkeypatch.setattr(
        fbref_pipeline_tasks.os,
        "killpg",
        lambda pid, sig: killed.append((pid, sig)),
    )
    group_waits = iter((False, True))
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "_wait_for_process_group_exit",
        lambda *args, **kwargs: next(group_waits),
    )

    output = fbref_pipeline_tasks._terminate_process_group(Process(), 8876)

    assert output == ("leader exited", "")
    assert killed == [
        (8876, fbref_pipeline_tasks.signal.SIGTERM),
        (8876, fbref_pipeline_tasks.signal.SIGKILL),
    ]


@pytest.mark.unit
def test_process_group_cleanup_fails_if_group_survives_sigkill(monkeypatch):
    class Process:
        pid = 8987
        returncode = 0

        def communicate(self, *, timeout=None):
            return "", ""

    monkeypatch.setattr(fbref_pipeline_tasks.os, "killpg", lambda *args: None)
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "_wait_for_process_group_exit",
        lambda *args, **kwargs: False,
    )

    with pytest.raises(RuntimeError, match="survived SIGKILL grace"):
        fbref_pipeline_tasks._terminate_process_group(Process())


@pytest.mark.unit
def test_process_group_cleanup_does_not_hide_signal_permission_failure(
    monkeypatch,
):
    process = MagicMock(pid=9098)
    monkeypatch.setattr(
        fbref_pipeline_tasks.os,
        "killpg",
        MagicMock(side_effect=PermissionError("denied")),
    )

    with pytest.raises(RuntimeError, match="could not signal"):
        fbref_pipeline_tasks._terminate_process_group(process)

    process.communicate.assert_not_called()


@pytest.mark.unit
def test_process_group_cleanup_bounds_term_and_kill_waits(monkeypatch):
    timeouts = []

    class Process:
        pid = 6543
        returncode = None

        def communicate(self, *, timeout=None):
            timeouts.append(timeout)
            raise fbref_pipeline_tasks.subprocess.TimeoutExpired(
                cmd=["runner"],
                timeout=timeout,
                output=f"stdout-{timeout}",
                stderr=f"stderr-{timeout}",
            )

    killed = []
    monkeypatch.setattr(
        fbref_pipeline_tasks.os,
        "killpg",
        lambda pid, sig: killed.append((pid, sig)),
    )
    group_waits = iter((False, True))
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "_wait_for_process_group_exit",
        lambda *args, **kwargs: next(group_waits),
    )

    stdout, stderr = fbref_pipeline_tasks._terminate_process_group(Process())

    assert timeouts == [
        fbref_pipeline_tasks.LIVE_WAVES_TERMINATION_GRACE_SECONDS,
        fbref_pipeline_tasks.LIVE_WAVES_KILL_GRACE_SECONDS,
    ]
    assert killed == [
        (6543, fbref_pipeline_tasks.signal.SIGTERM),
        (6543, fbref_pipeline_tasks.signal.SIGKILL),
    ]
    assert stdout == "stdout-10"
    assert stderr == "stderr-10"
