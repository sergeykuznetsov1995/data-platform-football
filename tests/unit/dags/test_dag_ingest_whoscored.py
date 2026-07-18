"""Production contracts for the dynamic WhoScored daily DAG."""

from __future__ import annotations

import importlib
import hashlib
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from scrapers.whoscored.repository import (
    ProfileCandidateCapacityExceeded,
    ProfileCandidateSnapshot,
    catalog_payload_sha256,
    entity_id_payload_sha256,
)


@pytest.fixture(autouse=True)
def _clean_operator_registries(monkeypatch):
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    from scrapers.whoscored import runtime_contract

    monkeypatch.setattr(runtime_contract, "_airflow_pool_slots", lambda _pool: 2)
    monkeypatch.setenv("WHOSCORED_DIRECT_POOL", "whoscored_direct_pool")
    monkeypatch.setenv("WHOSCORED_BACKFILL_POOL", "whoscored_direct_pool")
    monkeypatch.setenv("WHOSCORED_SOURCE_POOL_SLOTS", "2")
    yield


def _load_dag_module():
    sys.modules.pop("dag_ingest_whoscored", None)
    sys.modules.pop("dags.dag_ingest_whoscored", None)
    return importlib.import_module("dag_ingest_whoscored")


def _context(run_id="scheduled__2026-07-11T10:00:00+00:00"):
    return {
        "dag": SimpleNamespace(dag_id="dag_ingest_whoscored"),
        "run_id": run_id,
        "logical_date": datetime(2026, 7, 11, 10, tzinfo=timezone.utc),
        "params": {"require_zero_paid": True},
    }


def _catalog_identity(*, batch_id="wsc2-test-generation", payload_sha256="a" * 64):
    return {
        "schema_version": 1,
        "catalog_batch_id": batch_id,
        "catalog_payload_sha256": payload_sha256,
        "catalog_raw_provenance_sha256": "b" * 64,
        "technical_exclusion_audit_sha256": "c" * 64,
        "catalog_as_of_date": "2026-07-11",
        "parent_catalog_batch_id": None,
        "parent_catalog_payload_sha256": None,
        "parent_catalog_raw_provenance_sha256": None,
    }


def _stub_traffic_reconciliation_context(monkeypatch, mod):
    monkeypatch.setattr(
        mod,
        "_traffic_reconciliation_context",
        lambda transport: {
            "contract": mod.TRAFFIC_RECONCILIATION_CONTRACT,
            "runtime_manifest_sha256": "a" * 64,
            "runtime_code_tree_sha256": "b" * 64,
            "transport_policy": (
                "direct_then_paid" if transport.is_paid else "direct_only"
            ),
            "campaign_id": None,
            "approval_id": None,
            "approval_sha256": None,
        },
    )


def _scope_plan(scopes=None):
    values = sorted(scopes or ["WS-252-2=2526"])
    return {
        "schema_version": 2,
        **{
            key: value
            for key, value in _catalog_identity().items()
            if key != "schema_version"
        },
        "active_scopes": values,
        "active_scope_count": len(values),
        "active_scopes_sha256": hashlib.sha256(
            ("\n".join(values) + "\n").encode()
        ).hexdigest(),
    }


def _profile_capacity(count=500, payload_sha256="b" * 64, maximum_limit=3_000):
    return {
        "schema_version": 1,
        "status": "success",
        "catalog_batch_id": "wsc2-test-generation",
        "refresh_days": 90,
        "candidate_count": count,
        "candidate_payload_sha256": payload_sha256,
        "selected_limit": count,
        "maximum_limit": maximum_limit,
        "theoretical_roster_capacity": maximum_limit * 90,
    }


def _producer_commits(
    *,
    scope=("wss2-" + "1" * 64,),
    match=(),
    match_not_available=(),
    preview=(),
    preview_not_available=(),
    profile=(),
    profile_not_available=(),
):
    return {
        "schema_version": 1,
        "scope": list(scope),
        "match": list(match),
        "match_not_available": list(match_not_available),
        "preview": list(preview),
        "preview_not_available": list(preview_not_available),
        "profile": list(profile),
        "profile_not_available": list(profile_not_available),
    }


def _producer_attempts(
    *,
    scope="WS-252-2=2526",
    match=(),
    preview=(),
    profile=None,
):
    def records(values):
        if values is None:
            return []
        return [
            {
                "schema_version": 1,
                "scope": scope,
                "count": len(values),
                "payload_sha256": entity_id_payload_sha256(values),
            }
        ]

    return {
        "schema_version": 1,
        "match": records(match),
        "preview": records(preview),
        "profile": records(profile),
    }


def _exact_scope_integrity(commits, *, match_ids=(), preview_ids=()):
    return {
        "expected_scope_batches": len(commits["scope"]),
        "exact_scope_manifests": len(commits["scope"]),
        "exact_scope_dataset_mismatches": 0,
        "expected_match_batches": len(commits["match"]),
        "exact_match_manifests": len(commits["match"]),
        "exact_match_dataset_mismatches": 0,
        "expected_match_not_available_batches": len(commits["match_not_available"]),
        "exact_match_not_available_manifests": len(commits["match_not_available"]),
        "exact_match_not_available_physical_rows": 0,
        "exact_match_outcome_count": len(match_ids),
        "exact_match_outcome_duplicates": 0,
        "exact_match_outcome_payload_sha256": entity_id_payload_sha256(match_ids),
        "expected_preview_batches": len(commits["preview"]),
        "exact_preview_manifests": len(commits["preview"]),
        "exact_preview_dataset_mismatches": 0,
        "expected_preview_not_available_batches": len(commits["preview_not_available"]),
        "exact_preview_not_available_manifests": len(commits["preview_not_available"]),
        "exact_preview_not_available_physical_rows": 0,
        "exact_preview_outcome_count": len(preview_ids),
        "exact_preview_outcome_duplicates": 0,
        "exact_preview_outcome_payload_sha256": entity_id_payload_sha256(preview_ids),
    }


def _exact_profile_integrity(commits, *, player_ids=()):
    return {
        "expected_profile_batches": len(commits["profile"]),
        "exact_profile_manifests": len(commits["profile"]),
        "exact_profile_row_mismatches": 0,
        "exact_profile_participation_mismatches": 0,
        "expected_profile_not_available_batches": len(commits["profile_not_available"]),
        "exact_profile_not_available_manifests": len(commits["profile_not_available"]),
        "exact_profile_not_available_physical_rows": 0,
        "exact_profile_outcome_count": len(player_ids),
        "exact_profile_outcome_duplicates": 0,
        "exact_profile_outcome_payload_sha256": entity_id_payload_sha256(player_ids),
    }


@pytest.mark.unit
def test_dag_import_does_not_read_runtime_catalog(monkeypatch):
    from dags.scripts import run_whoscored_scraper as runner

    monkeypatch.setattr(
        runner,
        "resolve_daily_scope_specs",
        lambda: (_ for _ in ()).throw(RuntimeError("Trino unavailable")),
    )

    mod = _load_dag_module()

    assert mod.dag.dag_id == "dag_ingest_whoscored"
    assert mod.dag._dag_kwargs["max_active_runs"] == 1
    assert "WHOSCORED_LEAGUES" not in vars(mod)


@pytest.mark.unit
def test_runtime_builder_maps_every_persisted_active_scope(monkeypatch):
    mod = _load_dag_module()
    scopes = ["WS-252-2=2526", "WS-247-12=2026"]
    monkeypatch.setattr(mod, "_active_scope_specs", lambda: scopes)

    commands = mod.build_daily_commands(**_context())

    assert len(commands) == 2
    for scope, command in zip(scopes, commands):
        assert "run_whoscored_scraper.py daily" in command
        assert f"--scope {scope}" in command
        assert "--skip-profiles" in command
        assert "--leagues" not in command
        assert "--seasons" not in command
        assert "--proxy-file" not in command
        assert "/scope_" in command


@pytest.mark.unit
def test_runtime_builder_pins_every_worker_to_one_catalog_generation():
    mod = _load_dag_module()
    plan = _scope_plan(["WS-247-12=2026", "WS-252-2=2526"])

    commands = mod.build_daily_commands(scope_plan=plan, **_context())
    validation = mod.build_scope_validation_kwargs(
        scope_plan=plan,
        **_context(),
    )
    profile = mod.build_daily_profile_command(
        scope_plan=plan,
        capacity=_profile_capacity(),
        **_context(),
    )

    assert all("--catalog-batch-id wsc2-test-generation" in item for item in commands)
    assert {item["expected_catalog_batch_id"] for item in validation} == {
        "wsc2-test-generation"
    }
    assert "daily --profiles-only" in profile
    assert "--catalog-batch-id wsc2-test-generation" in profile
    assert "--expected-profile-candidate-count 500" in profile
    assert f"--expected-profile-candidate-sha256 {'b' * 64}" in profile
    assert profile.count("--scope ") == 2


@pytest.mark.unit
def test_daily_scope_plan_binds_exact_catalog_snapshot(monkeypatch):
    mod = _load_dag_module()
    from dags.scripts import run_whoscored_scraper as runner

    rows = {"competitions": [], "seasons": [], "stages": []}
    payload_sha256 = catalog_payload_sha256(rows)
    catalog = SimpleNamespace(to_rows=lambda: rows)
    identity = _catalog_identity(
        batch_id="wsc2-generation",
        payload_sha256=payload_sha256,
    )
    repository = SimpleNamespace(
        load_catalog_generation_snapshot=lambda *, batch_id: (
            (
                {
                    key: value
                    for key, value in identity.items()
                    if key != "schema_version"
                },
                catalog,
            )
            if batch_id == "wsc2-generation"
            else pytest.fail("daily scope plan loaded the wrong catalog batch")
        ),
    )
    monkeypatch.setattr(runner, "_new_repository", lambda: repository)
    monkeypatch.setattr(
        runner,
        "_select_catalog_snapshot_scopes",
        lambda value, requested, *, active_only: (
            [
                (SimpleNamespace(spec="WS-2=2026"), object()),
                (SimpleNamespace(spec="WS-1=2026"), object()),
            ]
            if value is catalog and not requested and active_only
            else pytest.fail("daily scope plan did not use the exact snapshot")
        ),
    )

    plan = mod.freeze_daily_scope_plan(
        validated_catalog={
            "status": "success",
            "catalog_identity": identity,
        }
    )

    assert plan["catalog_batch_id"] == "wsc2-generation"
    assert plan["active_scopes"] == ["WS-1=2026", "WS-2=2026"]
    assert mod._daily_scope_plan_specs(plan) == (
        "wsc2-generation",
        ["WS-1=2026", "WS-2=2026"],
    )


@pytest.mark.unit
def test_daily_scope_plan_ignores_a_concurrently_published_newer_catalog(monkeypatch):
    mod = _load_dag_module()
    from dags.scripts import run_whoscored_scraper as runner

    old_rows = {"competitions": [], "seasons": [], "stages": []}
    old_payload = catalog_payload_sha256(old_rows)
    old_catalog = SimpleNamespace(to_rows=lambda: old_rows)
    loaded = []
    mutable_latest_reads = []

    identity = _catalog_identity(
        batch_id="wsc2-dagrun",
        payload_sha256=old_payload,
    )

    def load_catalog_generation_snapshot(*, batch_id):
        loaded.append(batch_id)
        if batch_id != "wsc2-dagrun":
            pytest.fail("concurrent latest catalog replaced the DagRun catalog")
        return (
            {key: value for key, value in identity.items() if key != "schema_version"},
            old_catalog,
        )

    repository = SimpleNamespace(
        load_catalog_generation_snapshot=load_catalog_generation_snapshot,
        # A newer catalog is now globally latest, after this DagRun discovered
        # and validated wsc2-dagrun. The freeze task must never observe it.
        latest_catalog_generation=lambda: (
            mutable_latest_reads.append(True)
            or {"catalog_batch_id": "wsc2-concurrent-newer"}
        ),
    )
    monkeypatch.setattr(runner, "_new_repository", lambda: repository)
    monkeypatch.setattr(
        runner,
        "_select_catalog_snapshot_scopes",
        lambda value, requested, *, active_only: (
            [(SimpleNamespace(spec="WS-1=2026"), object())]
            if value is old_catalog and not requested and active_only
            else pytest.fail("scope plan did not keep the DagRun catalog")
        ),
    )

    plan = mod.freeze_daily_scope_plan(
        validated_catalog={
            "status": "success",
            "catalog_identity": identity,
        }
    )

    assert loaded == ["wsc2-dagrun"]
    assert mutable_latest_reads == []
    assert plan["catalog_batch_id"] == "wsc2-dagrun"
    assert plan["active_scopes"] == ["WS-1=2026"]


@pytest.mark.unit
def test_daily_scope_plan_rejects_exact_manifest_identity_drift(monkeypatch):
    mod = _load_dag_module()
    from dags.scripts import run_whoscored_scraper as runner

    identity = _catalog_identity()
    generation = {
        key: value for key, value in identity.items() if key != "schema_version"
    }
    generation["catalog_raw_provenance_sha256"] = "f" * 64
    repository = SimpleNamespace(
        load_catalog_generation_snapshot=lambda *, batch_id: (
            generation,
            SimpleNamespace(to_rows=lambda: {}),
        )
    )
    monkeypatch.setattr(runner, "_new_repository", lambda: repository)

    with pytest.raises(mod.AirflowException, match="manifest differs"):
        mod.freeze_daily_scope_plan(
            validated_catalog={
                "status": "success",
                "catalog_identity": identity,
            }
        )


@pytest.mark.unit
def test_runtime_builder_can_make_a_structural_direct_only_canary(monkeypatch):
    mod = _load_dag_module()
    monkeypatch.setattr(mod, "_active_scope_specs", lambda: ["WS-252-2=2526"])
    context = _context()
    context["params"]["direct_only"] = True

    assert "--transport-policy direct_only" in mod.build_daily_commands(**context)[0]


@pytest.mark.unit
def test_discovery_command_uses_dagrun_logical_date(monkeypatch):
    mod = _load_dag_module()
    transport = SimpleNamespace(
        cli_args=lambda **_kwargs: "--transport-policy direct_only"
    )
    monkeypatch.setattr(mod, "_transport_runtime", lambda *_args, **_kwargs: transport)
    monkeypatch.setattr(
        mod,
        "paid_alert_source_guard_command",
        lambda *_args, **_kwargs: "",
    )
    context = _context()
    context["logical_date"] = datetime(2020, 2, 3, 23, 59, tzinfo=timezone.utc)

    command = mod.build_daily_discovery_command(**context)

    assert "--as-of-date 2020-02-03" in command


@pytest.mark.unit
def test_paid_alert_mutation_blocks_every_daily_source_builder(monkeypatch):
    mod = _load_dag_module()
    paid = SimpleNamespace(
        is_paid=True,
        cli_args=lambda **_kwargs: "--transport-policy direct_then_paid",
    )
    monkeypatch.setattr(mod, "_transport_runtime", lambda *_args, **_kwargs: paid)
    monkeypatch.setattr(
        mod,
        "_bind_transport_allocation",
        lambda *_args, **_kwargs: paid,
    )
    monkeypatch.setattr(mod, "_active_scope_specs", lambda: ["WS-252-2=2526"])
    calls = []

    def reject_mutation(runtime, metadata, context):
        calls.append((runtime, metadata, context))
        assert metadata == {"status": "delivered", "receipt_sha256": "stale"}
        raise mod.WhoScoredProxyRuntimeError("paid source alert receipt is invalid")

    monkeypatch.setattr(mod, "paid_alert_source_guard_command", reject_mutation)
    metadata = {"status": "delivered", "receipt_sha256": "stale"}
    context = _context()
    plan = _scope_plan(["WS-252-2=2526"])

    builders = (
        lambda: mod.build_daily_discovery_command(alert_metadata=metadata, **context),
        lambda: mod.build_daily_commands(alert_metadata=metadata, **context),
        lambda: mod.build_daily_profile_command(
            scope_plan=plan,
            capacity=_profile_capacity(),
            alert_metadata=metadata,
            **context,
        ),
    )
    for builder in builders:
        with pytest.raises(
            mod.WhoScoredProxyRuntimeError,
            match="alert receipt is invalid",
        ):
            builder()
    assert len(calls) == 3


@pytest.mark.unit
def test_runtime_preflight_requires_local_executor_and_forbids_paid_override(
    monkeypatch,
):
    mod = _load_dag_module()
    monkeypatch.setenv("AIRFLOW__CORE__EXECUTOR", "CeleryExecutor")
    with pytest.raises(mod.AirflowException, match="requires.*LocalExecutor"):
        mod.validate_whoscored_runtime(params={})

    monkeypatch.setenv("AIRFLOW__CORE__EXECUTOR", "LocalExecutor")
    with pytest.raises(mod.AirflowException, match="legacy booleans cannot authorize"):
        mod.validate_whoscored_runtime(params={"direct_only": False})
    result = mod.validate_whoscored_runtime(params={})
    assert result["direct_only"] is True
    assert result["runtime_contract"]["parser_version"] == "whoscored-parser-v8"
    assert result["source_pool_contract"] == {
        "pool": "whoscored_direct_pool",
        "expected_slots": 2,
        "actual_slots": 2,
    }

    monkeypatch.setenv("WHOSCORED_BACKFILL_POOL", "different_pool")
    with pytest.raises(mod.AirflowException, match="must share one Airflow source"):
        mod.validate_whoscored_runtime(params={})


@pytest.mark.unit
def test_daily_alerts_are_deduplicated_at_dagrun_level():
    mod = _load_dag_module()

    assert "on_failure_callback" not in mod.WHOSCORED_ARGS
    assert callable(mod.dag._dag_kwargs["on_failure_callback"])


@pytest.mark.unit
def test_feed_state_contract_has_exactly_68_unique_keys_per_stage():
    mod = _load_dag_module()

    first = mod._expected_feed_state_keys([23752])
    both = mod._expected_feed_state_keys([23752, 23753])

    assert len(first) == mod.EXPECTED_FEEDS_PER_STAGE == 68
    assert len(both) == 136
    assert first < both
    assert all(key.startswith("23752:") for key in first)


@pytest.mark.unit
def test_scope_parity_contract_covers_all_scope_datasets():
    mod = _load_dag_module()

    assert mod.SCOPE_PARITY_TABLES == (
        "whoscored_schedule",
        "whoscored_match_incidents",
        "whoscored_match_bets",
        "whoscored_stage_standings",
        "whoscored_stage_forms",
        "whoscored_stage_streaks",
        "whoscored_stage_performance",
        "whoscored_team_stage_stats",
        "whoscored_player_stage_stats",
        "whoscored_referee_stage_stats",
    )


@pytest.mark.unit
def test_feed_state_summary_checks_exact_keys_and_surfaces_unavailable():
    mod = _load_dag_module()
    keys = sorted(mod._expected_feed_state_keys([23752]))
    feeds = {key: "available" for key in keys}
    feeds[keys[0]] = "empty"
    feeds[keys[1]] = "not_available"

    summary = mod._feed_state_integrity_summary(
        json.dumps({"whoscored_schedule": "available", "__feeds__": feeds}),
        [23752],
    )

    assert summary == {
        "feed_state_stage_count": 1,
        "expected_feed_state_count": 68,
        "actual_feed_state_count": 68,
        "missing_feed_state_count": 0,
        "extra_feed_state_count": 0,
        "malformed_feed_state_count": 0,
        "unavailable_feed_count": 1,
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    "mutate, expected",
    [
        ("missing", (67, 1, 0, 0)),
        ("extra", (69, 0, 1, 0)),
        ("invalid_status", (68, 0, 0, 1)),
    ],
)
def test_feed_state_summary_detects_missing_extra_and_malformed(mutate, expected):
    mod = _load_dag_module()
    keys = sorted(mod._expected_feed_state_keys([23752]))
    feeds = {key: "available" for key in keys}
    if mutate == "missing":
        feeds.pop(keys[0])
    elif mutate == "extra":
        feeds["23752:team:invented:feed"] = "available"
    else:
        feeds[keys[0]] = "unknown"

    summary = mod._feed_state_integrity_summary(
        json.dumps({"__feeds__": feeds}), [23752]
    )

    assert (
        summary["actual_feed_state_count"],
        summary["missing_feed_state_count"],
        summary["extra_feed_state_count"],
        summary["malformed_feed_state_count"],
    ) == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    "payload",
    [
        None,
        "not json",
        "[]",
        "{}",
        '{"__feeds__": {}, "__feeds__": {}}',
    ],
)
def test_feed_state_summary_fails_closed_on_malformed_manifest(payload):
    mod = _load_dag_module()

    summary = mod._feed_state_integrity_summary(payload, [23752])

    assert summary["malformed_feed_state_count"] >= 1
    assert summary["missing_feed_state_count"] == 68


@pytest.mark.unit
def test_scope_integrity_summary_counts_schedule_bets_without_offer_rows(monkeypatch):
    mod = _load_dag_module()
    feeds = {key: "available" for key in mod._expected_feed_state_keys([23752])}
    match_row = [0] * 28
    match_row[0] = 1
    match_row[1] = 1
    match_row[23] = 2
    scope_row = [
        json.dumps({table: 0 for table in mod.SCOPE_PARITY_TABLES}),
        json.dumps({"__feeds__": feeds}),
        [23752],
        *([0] * len(mod.SCOPE_PARITY_TABLES)),
    ]
    responses = iter(
        [
            [[*match_row[:11], *match_row[21:]]],
            [match_row[11:21]],
            [scope_row],
            [[0] * 10],
        ]
    )
    queries = []

    class _Cursor:
        def execute(self, query):
            queries.append(query)

        def fetchall(self):
            return next(responses)

        def close(self):
            return None

    class _Connection:
        def cursor(self):
            return _Cursor()

        def close(self):
            return None

    from scrapers.base import trino_manager

    monkeypatch.setattr(trino_manager, "get_trino_connection", lambda: _Connection())

    summary = mod._scope_integrity_summary("WS-252-2=2526")

    assert summary["uncovered_bet_matches"] == 2
    assert "whoscored_match_bets_current" in queries[0]
    assert "json_size(json_parse(s.bets), '$')" in queries[0]
    assert "<> 7" in queries[0]
    assert "availability_version =" in queries[0]
    assert "failure_code IS NOT NULL" in queries[0]
    assert "http_status IN (404, 410)" in queries[0]
    assert "valid_preview AS" in queries[3]
    assert "LEFT JOIN valid_preview p" in queries[3]
    assert "state = 'not_available'" in queries[3]
    assert "availability_version =" in queries[3]
    assert "payload_sha256 IS NOT NULL" in queries[3]
    assert "<> 3" in queries[3]


@pytest.mark.unit
def test_failed_run_directories_are_removed_after_retention(monkeypatch, tmp_path):
    mod = _load_dag_module()
    root = tmp_path / "runs"
    monkeypatch.setattr(mod, "RUN_ROOT", str(root))
    monkeypatch.setenv("WHOSCORED_RUN_RETENTION_DAYS", "90")
    old = root / "dag" / "old-run"
    current = root / "dag" / "current-run"
    recent = root / "dag" / "recent-run"
    for path in (old, current, recent):
        path.mkdir(parents=True)
        (path / "requests.jsonl").write_bytes(b"1234")
    old_timestamp = datetime(2025, 1, 1).timestamp()
    recent_timestamp = datetime(2026, 6, 1).timestamp()
    os.utime(old, (old_timestamp, old_timestamp))
    os.utime(current, (old_timestamp, old_timestamp))
    os.utime(recent, (recent_timestamp, recent_timestamp))

    result = mod.cleanup_stale_run_directories(
        current_run_dir=current,
        now=datetime(2026, 7, 14),
    )

    assert result == {"removed_directories": 1, "removed_bytes": 4}
    assert not old.exists()
    assert current.exists()
    assert recent.exists()


@pytest.mark.unit
@pytest.mark.parametrize("link_level", ["dag", "run"])
def test_run_retention_rejects_symlink_escape(monkeypatch, tmp_path, link_level):
    mod = _load_dag_module()
    root = tmp_path / "runs"
    outside = tmp_path / "outside-evidence"
    outside.mkdir()
    evidence = outside / "must-survive.jsonl"
    evidence.write_text("forensic evidence\n")
    root.mkdir()
    if link_level == "dag":
        (root / "evil-dag").symlink_to(outside, target_is_directory=True)
    else:
        dag = root / "dag"
        dag.mkdir()
        (dag / "evil-run").symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr(mod, "RUN_ROOT", str(root))
    monkeypatch.setenv("WHOSCORED_RUN_RETENTION_DAYS", "1")

    with pytest.raises(mod.AirflowException, match="symlinked"):
        mod.cleanup_stale_run_directories(now=datetime(2026, 7, 14))

    assert evidence.read_text() == "forensic evidence\n"


@pytest.mark.unit
def test_run_retention_swap_to_symlink_never_follows_target(monkeypatch, tmp_path):
    mod = _load_dag_module()
    root = tmp_path / "runs"
    stale = root / "dag" / "old-run"
    stale.mkdir(parents=True)
    old_timestamp = datetime(2025, 1, 1).timestamp()
    os.utime(stale, (old_timestamp, old_timestamp))
    outside = tmp_path / "outside-evidence"
    outside.mkdir()
    evidence = outside / "must-survive.jsonl"
    evidence.write_text("forensic evidence\n")
    monkeypatch.setattr(mod, "RUN_ROOT", str(root))
    monkeypatch.setenv("WHOSCORED_RUN_RETENTION_DAYS", "1")
    real_rename = mod.os.rename

    def swap_then_rename(source, target, **kwargs):
        if source == "old-run":
            stale.rmdir()
            stale.symlink_to(outside, target_is_directory=True)
        return real_rename(source, target, **kwargs)

    monkeypatch.setattr(mod.os, "rename", swap_then_rename)

    with pytest.raises(mod.AirflowException, match="cannot remove stale"):
        mod.cleanup_stale_run_directories(now=datetime(2026, 7, 14))

    assert evidence.read_text() == "forensic evidence\n"


@pytest.mark.unit
def test_long_run_ids_have_collision_resistant_local_and_s3_tokens():
    mod = _load_dag_module()
    prefix = "manual__" + "q" * 180
    first = mod._run_dir_from_context(
        {"dag_id": "dag_ingest_whoscored", "run_id": prefix + "__one"}
    )
    second = mod._run_dir_from_context(
        {"dag_id": "dag_ingest_whoscored", "run_id": prefix + "__two"}
    )

    assert first != second
    assert mod._safe_token(prefix + "__one") != mod._safe_token(prefix + "__two")
    assert len(first.name) <= 120


@pytest.mark.unit
def test_jinja_writers_and_python_readers_share_the_exact_run_directory():
    from jinja2 import Environment

    mod = _load_dag_module()
    context = _context("manual__" + "scope:" * 40 + "+00:00")
    environment = Environment()
    environment.filters.update(mod.dag._dag_kwargs["user_defined_filters"])
    rendered = environment.from_string(mod._RUN_DIR_TEMPLATE).render(
        dag=context["dag"],
        run_id=context["run_id"],
    )

    assert Path(rendered) == mod._run_dir_from_context(context)
    assert mod._RUN_DIR_TEMPLATE in mod._TASK_ENV["WHOSCORED_REQUEST_LEDGER_PATH"]
    assert "stable_safe_token" in mod._RUN_DIR_TEMPLATE


@pytest.mark.unit
def test_daily_slo_warms_up_then_enforces_rolling_p95(monkeypatch):
    mod = _load_dag_module()
    monkeypatch.setenv("WHOSCORED_DAILY_SLO_MIN_SAMPLES", "20")
    monkeypatch.setenv("WHOSCORED_DAILY_P95_LIMIT_HOURS", "4")

    warmup = mod.validate_whoscored_daily_slo(durations_hours=[5.5])
    passing = mod.validate_whoscored_daily_slo(durations_hours=[1.0] * 19 + [5.5])

    assert warmup["status"] == "warming_up"
    assert warmup["cold_hard_limit_hours"] == 6
    assert passing["status"] == "success"
    assert passing["p95_hours"] == 1.0
    with pytest.raises(mod.AirflowException, match="rolling p95 SLO failed"):
        mod.validate_whoscored_daily_slo(durations_hours=[1.0] * 18 + [4.01, 5.5])


@pytest.mark.unit
def test_profile_capacity_covers_exact_backlog_and_fails_above_hard_cap(monkeypatch):
    mod = _load_dag_module()
    plan = _scope_plan()
    from dags.scripts import run_whoscored_scraper as runner

    repository = SimpleNamespace(
        load_discovered_catalog=lambda *, batch_id: object(),
        profile_candidate_snapshot=lambda **_kwargs: ProfileCandidateSnapshot(
            player_ids=tuple(range(1, 501)),
            count=500,
            payload_sha256="1" * 64,
        ),
    )
    monkeypatch.setattr(runner, "_new_repository", lambda: repository)
    monkeypatch.setattr(
        runner,
        "_select_catalog_snapshot_scopes",
        lambda _catalog, requested, active_only: [
            (requested[0], SimpleNamespace(scope=object()))
        ],
    )

    normal = mod.plan_daily_profile_capacity(scope_plan=plan)
    repository.profile_candidate_snapshot = lambda **_kwargs: ProfileCandidateSnapshot(
        player_ids=tuple(range(1, 2_501)),
        count=2_500,
        payload_sha256="2" * 64,
    )
    repair = mod.plan_daily_profile_capacity(scope_plan=plan)

    assert normal["selected_limit"] == normal["candidate_count"] == 500
    assert repair["selected_limit"] == repair["candidate_count"] == 2_500
    assert repair["candidate_payload_sha256"] == "2" * 64
    monkeypatch.setenv("WHOSCORED_DAILY_PROFILE_MAX_LIMIT", "1000")
    repository.profile_candidate_snapshot = lambda **_kwargs: (_ for _ in ()).throw(
        ProfileCandidateCapacityExceeded(count=2_500, hard_cap=1_000)
    )
    with pytest.raises(mod.AirflowException, match="count=2500, hard_cap=1000"):
        mod.plan_daily_profile_capacity(scope_plan=plan)


@pytest.mark.unit
def test_daily_slo_limit_cannot_be_relaxed_above_four_hours(monkeypatch):
    mod = _load_dag_module()
    monkeypatch.setenv("WHOSCORED_DAILY_P95_LIMIT_HOURS", "4.1")

    with pytest.raises(mod.AirflowException, match="must be <=4h"):
        mod.validate_whoscored_daily_slo(durations_hours=[])


@pytest.mark.unit
def test_daily_slo_history_includes_failed_elapsed_and_excludes_bootstrap(
    monkeypatch,
):
    mod = _load_dag_module()

    class Column:
        def __eq__(self, _other):
            return self

        def like(self, _value):
            return self

        def isnot(self, _value):
            return self

        def in_(self, _value):
            return self

        def asc(self):
            return self

        def desc(self):
            return self

    class DagRun:
        dag_id = run_id = state = start_date = end_date = Column()

    bootstrap = datetime(2026, 7, 1, 10)
    failed_start = datetime(2026, 7, 2, 10)

    class Query:
        def __init__(self, result):
            self.result = result

        def filter(self, *_args):
            return self

        def order_by(self, *_args):
            return self

        def limit(self, _value):
            return self

        def all(self):
            return self.result

    class Session:
        calls = 0

        def query(self, *_args):
            self.calls += 1
            if self.calls == 1:
                return Query([("scheduled__bootstrap",)])
            return Query(
                [
                    (
                        "scheduled__failed",
                        "failed",
                        failed_start,
                        failed_start + timedelta(minutes=12),
                    ),
                    (
                        "scheduled__bootstrap",
                        "success",
                        bootstrap,
                        bootstrap + timedelta(hours=5, minutes=30),
                    ),
                ]
            )

    class SessionContext:
        def __enter__(self):
            return Session()

        def __exit__(self, *_args):
            return False

    dagrun_module = types.ModuleType("airflow.models.dagrun")
    dagrun_module.DagRun = DagRun
    session_module = types.ModuleType("airflow.utils.session")
    session_module.create_session = SessionContext
    monkeypatch.setitem(sys.modules, "airflow.models.dagrun", dagrun_module)
    monkeypatch.setitem(sys.modules, "airflow.utils.session", session_module)

    assert mod._scheduled_daily_durations_hours() == pytest.approx([0.2])


@pytest.mark.unit
def test_scope_validation_accepts_atomic_manifest_parity(monkeypatch):
    mod = _load_dag_module()
    match_ids = (2,)
    preview_ids = (3,)
    commits = _producer_commits(
        match=("ws2-v3-" + "2" * 64,),
        preview=("wsp2-v3-" + "3" * 64,),
    )
    monkeypatch.setattr(
        mod,
        "_load_result",
        lambda _path: {
            "schema_version": 3,
            "status": "success",
            "paid_proxy_bytes": 0,
            "scopes": [{"scope": "WS-252-2=2526"}],
            "producer_commits": commits,
            "producer_attempts": _producer_attempts(
                match=match_ids, preview=preview_ids
            ),
        },
    )
    integrity = {
        "schedule_rows": 20,
        "schedule_games": 20,
        "successful_matches": 4,
        "manifest_event_rows": 500,
        "current_event_rows": 500,
        "manifest_lineup_rows": 80,
        "current_lineup_rows": 80,
        "completed_games": 4,
        "uncovered_completed_games": 0,
        "event_game_mismatches": 0,
        "lineup_game_mismatches": 0,
        "manifest_match_rows": 4,
        "current_match_rows": 4,
        "manifest_substitution_rows": 10,
        "current_substitution_rows": 10,
        "manifest_formation_rows": 8,
        "current_formation_rows": 8,
        "manifest_team_stat_rows": 8,
        "current_team_stat_rows": 8,
        "manifest_player_stat_rows": 80,
        "current_player_stat_rows": 80,
        "incomplete_final_opta_games": 0,
        "uncovered_incident_summaries": 0,
        "uncovered_bet_matches": 0,
        "incomplete_match_snapshots": 0,
        "invalid_event_identity_rows": 0,
        "duplicate_source_event_ids": 0,
        "duplicate_team_event_ids": 0,
        "scope_manifest_mismatches": 0,
        "feed_state_stage_count": 1,
        "expected_feed_state_count": 68,
        "actual_feed_state_count": 68,
        "missing_feed_state_count": 0,
        "extra_feed_state_count": 0,
        "malformed_feed_state_count": 0,
        "unavailable_feed_count": 2,
        "required_previews": 3,
        "successful_previews": 3,
        "uncovered_previews": 0,
        "manifest_missing_player_rows": 2,
        "current_missing_player_rows": 2,
        "manifest_preview_lineup_rows": 30,
        "current_preview_lineup_rows": 30,
        "manifest_preview_section_rows": 6,
        "current_preview_section_rows": 6,
        "incomplete_preview_snapshots": 0,
    }
    exact = _exact_scope_integrity(
        commits, match_ids=match_ids, preview_ids=preview_ids
    )
    monkeypatch.setattr(
        mod,
        "_scope_producer_integrity_summary",
        lambda _scope, _reported: exact,
    )
    monkeypatch.setattr(mod, "_scope_integrity_summary", lambda _scope: integrity)

    result = mod.validate_scope_result(scope_spec="WS-252-2=2526", result_path="unused")

    assert result == {
        "scope": "WS-252-2=2526",
        "paid_proxy_bytes": 0,
        **exact,
        **integrity,
    }


@pytest.mark.unit
def test_scope_validation_cannot_use_a_newer_latest_batch_for_an_old_report(
    monkeypatch,
):
    mod = _load_dag_module()
    old_match = "ws2-v3-" + "a" * 64
    match_ids = (10,)
    commits = _producer_commits(match=(old_match,))
    monkeypatch.setattr(
        mod,
        "_load_result",
        lambda _path: {
            "status": "success",
            "paid_proxy_bytes": 0,
            "scopes": [{"scope": "WS-252-2=2526"}],
            "producer_commits": commits,
            "producer_attempts": _producer_attempts(match=match_ids),
        },
    )

    def exact(_scope, reported):
        assert reported["match"] == (old_match,)
        return {
            **_exact_scope_integrity(commits, match_ids=match_ids),
            "exact_match_manifests": 0,
        }

    monkeypatch.setattr(mod, "_scope_producer_integrity_summary", exact)
    monkeypatch.setattr(
        mod,
        "_scope_integrity_summary",
        lambda _scope: pytest.fail("mutable latest coverage must not hide the race"),
    )

    with pytest.raises(mod.AirflowException, match="exact match commit parity"):
        mod.validate_scope_result(scope_spec="WS-252-2=2526", result_path="unused")


@pytest.mark.unit
def test_scope_exact_producer_summary_queries_every_reported_batch(monkeypatch):
    mod = _load_dag_module()
    from scrapers.whoscored.repository import (
        MATCH_DATASET_TABLES,
        PREVIEW_DATASET_TABLES,
    )

    old_scope = "wss2-" + "1" * 64
    old_match = "ws2-v3-" + "2" * 64
    old_preview = "wsp2-v3-" + "3" * 64
    newer = "f" * 64
    scope_counts = {
        table: int(table == "whoscored_schedule") for table in mod.SCOPE_PARITY_TABLES
    }
    match_counts = {name: int(name == "matches") for name in MATCH_DATASET_TABLES}
    preview_counts = {
        name: int(name == "preview_sections") for name in PREVIEW_DATASET_TABLES
    }
    queries = []

    class Cursor:
        rows = []

        def execute(self, query):
            queries.append(query)
            assert newer not in query
            assert "_current" not in query
            assert "_latest" not in query
            if "whoscored_scope_ingest_manifest" in query:
                assert old_scope in query
                self.rows = [(old_scope, json.dumps(scope_counts))]
            elif "whoscored_match_ingest_manifest" in query:
                assert old_match in query
                self.rows = [(old_match, json.dumps(match_counts), 2)]
            elif "whoscored_preview_ingest_manifest" in query:
                assert old_preview in query
                self.rows = [(old_preview, json.dumps(preview_counts), 3)]
            elif "_scope_batch_id" in query:
                assert old_scope in query
                self.rows = [("whoscored_schedule", old_scope, 1)]
            elif "_game_batch_id" in query:
                assert old_match in query
                self.rows = [("matches", old_match, 1)]
            elif "_preview_batch_id" in query:
                assert old_preview in query
                self.rows = [("preview_sections", old_preview, 1)]
            else:
                pytest.fail(f"unexpected exact-batch query: {query}")

        def fetchall(self):
            return self.rows

        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    from scrapers.base import trino_manager

    monkeypatch.setattr(trino_manager, "get_trino_connection", Connection)
    commits = _producer_commits(
        scope=(old_scope,), match=(old_match,), preview=(old_preview,)
    )

    result = mod._scope_producer_integrity_summary("WS-252-2=2526", commits)

    assert len(queries) == 6
    assert result == _exact_scope_integrity(commits, match_ids=(2,), preview_ids=(3,))


@pytest.mark.unit
def test_scope_exact_producer_summary_validates_all_not_available_outcomes(
    monkeypatch,
):
    mod = _load_dag_module()
    scope_batch = "wss2-" + "a" * 64
    match_na = "wsna2-v3-" + "b" * 64
    preview_na = "wspna2-v3-" + "c" * 64
    scope_counts = {
        table: int(table == "whoscored_schedule") for table in mod.SCOPE_PARITY_TABLES
    }
    queries = []

    class Cursor:
        rows = []

        def execute(self, query):
            queries.append(query)
            assert "_current" not in query and "_latest" not in query
            if "whoscored_scope_ingest_manifest" in query:
                assert scope_batch in query
                self.rows = [(scope_batch, json.dumps(scope_counts))]
            elif "whoscored_match_ingest_manifest" in query:
                assert match_na in query and "state = 'not_available'" in query
                self.rows = [(match_na, 404)]
            elif "whoscored_preview_ingest_manifest" in query:
                assert preview_na in query and "state = 'not_available'" in query
                self.rows = [(preview_na, 405)]
            elif "_scope_batch_id" in query:
                self.rows = [("whoscored_schedule", scope_batch, 1)]
            elif "_game_batch_id" in query:
                assert match_na in query
                self.rows = []
            elif "_preview_batch_id" in query:
                assert preview_na in query
                self.rows = []
            else:
                pytest.fail(f"unexpected exact outcome query: {query}")

        def fetchall(self):
            return self.rows

        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    from scrapers.base import trino_manager

    monkeypatch.setattr(trino_manager, "get_trino_connection", Connection)
    commits = _producer_commits(
        scope=(scope_batch,),
        match_not_available=(match_na,),
        preview_not_available=(preview_na,),
    )

    result = mod._scope_producer_integrity_summary("WS-252-2=2526", commits)

    assert len(queries) == 6
    assert result == _exact_scope_integrity(
        commits, match_ids=(404,), preview_ids=(405,)
    )


@pytest.mark.unit
def test_scope_nonzero_attempts_cannot_be_greened_with_no_exact_outcomes(
    monkeypatch,
):
    mod = _load_dag_module()
    commits = _producer_commits()
    monkeypatch.setattr(
        mod,
        "_load_result",
        lambda _path: {
            "status": "success",
            "paid_proxy_bytes": 0,
            "scopes": [{"scope": "WS-252-2=2526"}],
            "producer_commits": commits,
            "producer_attempts": _producer_attempts(match=(404,)),
        },
    )
    monkeypatch.setattr(
        mod,
        "_scope_producer_integrity_summary",
        lambda _scope, _commits: _exact_scope_integrity(commits),
    )
    monkeypatch.setattr(
        mod,
        "_scope_integrity_summary",
        lambda _scope: pytest.fail("newer mutable latest must not be consulted"),
    )

    with pytest.raises(mod.AirflowException, match="exact match outcome identity"):
        mod.validate_scope_result(scope_spec="WS-252-2=2526", result_path="unused")


@pytest.mark.unit
def test_scope_validation_rejects_missing_exact_physical_rows_even_if_latest_is_green(
    monkeypatch,
):
    mod = _load_dag_module()
    old_scope = "wss2-" + "4" * 64
    counts = {
        table: int(table == "whoscored_schedule") for table in mod.SCOPE_PARITY_TABLES
    }
    queries = []

    class Cursor:
        rows = []

        def execute(self, query):
            queries.append(query)
            assert old_scope in query
            assert "_current" not in query
            assert "_latest" not in query
            self.rows = (
                [(old_scope, json.dumps(counts))]
                if "whoscored_scope_ingest_manifest" in query
                else []
            )

        def fetchall(self):
            return self.rows

        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    from scrapers.base import trino_manager

    monkeypatch.setattr(trino_manager, "get_trino_connection", Connection)
    monkeypatch.setattr(
        mod,
        "_load_result",
        lambda _path: {
            "status": "success",
            "paid_proxy_bytes": 0,
            "scopes": [{"scope": "WS-252-2=2526"}],
            "producer_commits": _producer_commits(scope=(old_scope,)),
            "producer_attempts": _producer_attempts(),
        },
    )
    monkeypatch.setattr(
        mod,
        "_scope_integrity_summary",
        lambda _scope: pytest.fail("newer mutable latest must not be consulted"),
    )

    with pytest.raises(mod.AirflowException, match="exact scope commit parity"):
        mod.validate_scope_result(scope_spec="WS-252-2=2526", result_path="unused")
    assert len(queries) == 2


@pytest.mark.unit
@pytest.mark.parametrize(
    "change, message",
    [
        ({"schedule_games": 19}, "schedule duplicates"),
        ({"current_event_rows": 499}, "event manifest parity"),
        ({"current_lineup_rows": 79}, "lineup manifest parity"),
        ({"incomplete_final_opta_games": 1}, "final Opta event completeness"),
        ({"uncovered_incident_summaries": 1}, "incident coverage"),
        ({"uncovered_bet_matches": 1}, "bet coverage"),
        ({"incomplete_match_snapshots": 1}, "dataset-state contract"),
        ({"invalid_event_identity_rows": 1}, "event identity contract"),
        ({"duplicate_source_event_ids": 1}, "event identity contract"),
        ({"duplicate_team_event_ids": 1}, "event identity contract"),
        ({"missing_feed_state_count": 1}, "source feed manifest completeness"),
        ({"extra_feed_state_count": 1}, "source feed manifest completeness"),
        ({"malformed_feed_state_count": 1}, "source feed manifest completeness"),
        ({"uncovered_previews": 1}, "preview coverage"),
        ({"current_preview_lineup_rows": 29}, "preview manifest parity"),
    ],
)
def test_scope_validation_fails_closed_on_partial_publication(
    monkeypatch, change, message
):
    mod = _load_dag_module()
    commits = _producer_commits()
    monkeypatch.setattr(
        mod,
        "_load_result",
        lambda _path: {
            "status": "success",
            "paid_proxy_bytes": 0,
            "scopes": [{"scope": "WS-252-2=2526"}],
            "producer_commits": commits,
            "producer_attempts": _producer_attempts(),
        },
    )
    integrity = {
        "schedule_rows": 20,
        "schedule_games": 20,
        "successful_matches": 4,
        "manifest_event_rows": 500,
        "current_event_rows": 500,
        "manifest_lineup_rows": 80,
        "current_lineup_rows": 80,
        "completed_games": 4,
        "uncovered_completed_games": 0,
        "event_game_mismatches": 0,
        "lineup_game_mismatches": 0,
        "manifest_match_rows": 4,
        "current_match_rows": 4,
        "manifest_substitution_rows": 10,
        "current_substitution_rows": 10,
        "manifest_formation_rows": 8,
        "current_formation_rows": 8,
        "manifest_team_stat_rows": 8,
        "current_team_stat_rows": 8,
        "manifest_player_stat_rows": 80,
        "current_player_stat_rows": 80,
        "incomplete_final_opta_games": 0,
        "uncovered_incident_summaries": 0,
        "uncovered_bet_matches": 0,
        "incomplete_match_snapshots": 0,
        "invalid_event_identity_rows": 0,
        "duplicate_source_event_ids": 0,
        "duplicate_team_event_ids": 0,
        "scope_manifest_mismatches": 0,
        "feed_state_stage_count": 1,
        "expected_feed_state_count": 68,
        "actual_feed_state_count": 68,
        "missing_feed_state_count": 0,
        "extra_feed_state_count": 0,
        "malformed_feed_state_count": 0,
        "unavailable_feed_count": 2,
        "required_previews": 3,
        "successful_previews": 3,
        "uncovered_previews": 0,
        "manifest_missing_player_rows": 2,
        "current_missing_player_rows": 2,
        "manifest_preview_lineup_rows": 30,
        "current_preview_lineup_rows": 30,
        "manifest_preview_section_rows": 6,
        "current_preview_section_rows": 6,
        "incomplete_preview_snapshots": 0,
    }
    integrity.update(change)
    monkeypatch.setattr(
        mod,
        "_scope_producer_integrity_summary",
        lambda _scope, _commits: _exact_scope_integrity(commits),
    )
    monkeypatch.setattr(mod, "_scope_integrity_summary", lambda _scope: integrity)

    with pytest.raises(mod.AirflowException, match=message):
        mod.validate_scope_result(scope_spec="WS-252-2=2526", result_path="unused")


@pytest.mark.unit
def test_normal_daily_rejects_any_paid_proxy_bytes(monkeypatch):
    mod = _load_dag_module()
    monkeypatch.setattr(
        mod,
        "_load_result",
        lambda _path: {
            "status": "success",
            "paid_proxy_bytes": 1,
            "scopes": [{"scope": "WS-252-2=2526"}],
        },
    )

    with pytest.raises(mod.AirflowException, match="used paid proxy"):
        mod.validate_scope_result(scope_spec="WS-252-2=2526", result_path="unused")


@pytest.mark.unit
def test_profile_validation_requires_complete_manifest_backed_roster(monkeypatch):
    mod = _load_dag_module()
    commits = _producer_commits(scope=())
    monkeypatch.setattr(
        mod,
        "_load_result",
        lambda _path: {
            "status": "success",
            "paid_proxy_bytes": 0,
            "producer_commits": commits,
            "producer_attempts": _producer_attempts(
                match=None, preview=None, profile=()
            ),
        },
    )
    monkeypatch.setattr(mod, "_active_scope_specs", lambda: ["WS-252-2=2526"])
    complete = {
        "roster_players": 10,
        "current_profile_manifests": 10,
        "current_profile_rows": 10,
        "uncovered_profiles": 0,
        "stale_profiles": 0,
        "manifest_participation_rows": 30,
        "current_participation_rows": 30,
    }
    monkeypatch.setattr(
        mod,
        "_profile_producer_integrity_summary",
        lambda _commits: _exact_profile_integrity(commits),
    )
    monkeypatch.setattr(mod, "_profile_integrity_summary", lambda _scopes: complete)

    assert mod.validate_profile_result()["roster_players"] == 10

    monkeypatch.setattr(
        mod,
        "_profile_integrity_summary",
        lambda _scopes: {**complete, "uncovered_profiles": 1},
    )
    with pytest.raises(mod.AirflowException, match="coverage is incomplete"):
        mod.validate_profile_result()


@pytest.mark.unit
def test_profile_validation_cannot_use_a_newer_latest_batch_for_an_old_report(
    monkeypatch,
):
    mod = _load_dag_module()
    old_profile = "wspr2-v3-" + "b" * 64
    player_ids = (11,)
    commits = _producer_commits(scope=(), profile=(old_profile,))
    monkeypatch.setattr(
        mod,
        "_load_result",
        lambda _path: {
            "status": "success",
            "paid_proxy_bytes": 0,
            "producer_commits": commits,
            "producer_attempts": _producer_attempts(
                match=None, preview=None, profile=player_ids
            ),
        },
    )
    monkeypatch.setattr(mod, "_active_scope_specs", lambda: ["WS-252-2=2526"])

    def exact(reported):
        assert reported["profile"] == (old_profile,)
        return {
            **_exact_profile_integrity(commits, player_ids=player_ids),
            "exact_profile_manifests": 0,
        }

    monkeypatch.setattr(mod, "_profile_producer_integrity_summary", exact)
    monkeypatch.setattr(
        mod,
        "_profile_integrity_summary",
        lambda _scopes: pytest.fail("mutable latest coverage must not hide the race"),
    )

    with pytest.raises(mod.AirflowException, match="exact profile commit parity"):
        mod.validate_profile_result()


@pytest.mark.unit
def test_profile_exact_producer_summary_checks_manifest_and_physical_counts(
    monkeypatch,
):
    mod = _load_dag_module()
    old_profile = "wspr2-v3-" + "6" * 64
    newer_profile = "wspr2-v3-" + "7" * 64
    queries = []

    class Cursor:
        rows = []

        def execute(self, query):
            queries.append(query)
            assert old_profile in query
            assert newer_profile not in query
            assert "_current" not in query
            assert "_latest" not in query
            if "whoscored_profile_ingest_manifest" in query:
                self.rows = [(old_profile, 0, 6)]
            else:
                self.rows = [("profile", old_profile, 1)]

        def fetchall(self):
            return self.rows

        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    from scrapers.base import trino_manager

    monkeypatch.setattr(trino_manager, "get_trino_connection", Connection)
    commits = _producer_commits(scope=(), profile=(old_profile,))

    assert mod._profile_producer_integrity_summary(
        {key: tuple(value) for key, value in commits.items() if key != "schema_version"}
    ) == _exact_profile_integrity(commits, player_ids=(6,))
    assert len(queries) == 2


@pytest.mark.unit
def test_profile_exact_producer_summary_validates_all_not_available_outcomes(
    monkeypatch,
):
    mod = _load_dag_module()
    not_available = "wsprna2-" + "d" * 64
    queries = []

    class Cursor:
        rows = []

        def execute(self, query):
            queries.append(query)
            assert not_available in query
            assert "_current" not in query and "_latest" not in query
            self.rows = (
                [(not_available, 404)]
                if "whoscored_profile_ingest_manifest" in query
                else []
            )

        def fetchall(self):
            return self.rows

        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    from scrapers.base import trino_manager

    monkeypatch.setattr(trino_manager, "get_trino_connection", Connection)
    commits = _producer_commits(scope=(), profile_not_available=(not_available,))
    exact_commits = {
        key: tuple(value) for key, value in commits.items() if key != "schema_version"
    }

    assert mod._profile_producer_integrity_summary(
        exact_commits
    ) == _exact_profile_integrity(commits, player_ids=(404,))
    assert len(queries) == 2


@pytest.mark.unit
def test_planned_profiles_cannot_be_greened_with_no_exact_outcomes(monkeypatch):
    mod = _load_dag_module()
    player_ids = (404,)
    payload_sha256 = entity_id_payload_sha256(player_ids)
    report = {
        "status": "success",
        "paid_proxy_bytes": 0,
        "catalog_batch_id": "wsc2-test-generation",
        "profile_candidates": {
            "schema_version": 1,
            "count": 1,
            "payload_sha256": payload_sha256,
            "attempted": 1,
        },
        "producer_commits": _producer_commits(scope=()),
        "producer_attempts": _producer_attempts(
            match=None, preview=None, profile=player_ids
        ),
    }
    monkeypatch.setattr(mod, "_load_result", lambda _path: report)
    monkeypatch.setattr(
        mod,
        "_profile_integrity_summary",
        lambda _scopes: pytest.fail("newer mutable latest must not be consulted"),
    )

    with pytest.raises(mod.AirflowException, match="exact profile commit parity"):
        mod.validate_profile_result(
            scope_plan=_scope_plan(),
            capacity=_profile_capacity(count=1, payload_sha256=payload_sha256),
        )


@pytest.mark.unit
def test_zero_profile_attempts_reject_a_foreign_exact_outcome(monkeypatch):
    mod = _load_dag_module()
    foreign = "wspr2-v3-" + "e" * 64
    commits = _producer_commits(scope=(), profile=(foreign,))
    monkeypatch.setattr(
        mod,
        "_load_result",
        lambda _path: {
            "status": "success",
            "paid_proxy_bytes": 0,
            "producer_commits": commits,
            "producer_attempts": _producer_attempts(
                match=None, preview=None, profile=()
            ),
        },
    )
    monkeypatch.setattr(mod, "_active_scope_specs", lambda: ["WS-252-2=2526"])
    monkeypatch.setattr(
        mod,
        "_profile_producer_integrity_summary",
        lambda _commits: _exact_profile_integrity(commits, player_ids=(9,)),
    )
    monkeypatch.setattr(
        mod,
        "_profile_integrity_summary",
        lambda _scopes: pytest.fail("newer mutable latest must not be consulted"),
    )

    with pytest.raises(mod.AirflowException, match="exact profile commit parity"):
        mod.validate_profile_result()


@pytest.mark.unit
def test_missing_exact_profile_absence_cannot_be_hidden_by_latest(monkeypatch):
    mod = _load_dag_module()
    missing = "wsprna2-" + "f" * 64
    commits = _producer_commits(scope=(), profile_not_available=(missing,))
    monkeypatch.setattr(
        mod,
        "_load_result",
        lambda _path: {
            "status": "success",
            "paid_proxy_bytes": 0,
            "producer_commits": commits,
            "producer_attempts": _producer_attempts(
                match=None, preview=None, profile=(404,)
            ),
        },
    )
    monkeypatch.setattr(mod, "_active_scope_specs", lambda: ["WS-252-2=2526"])
    monkeypatch.setattr(
        mod,
        "_profile_producer_integrity_summary",
        lambda _commits: {
            **_exact_profile_integrity(commits),
            "exact_profile_not_available_manifests": 0,
        },
    )
    monkeypatch.setattr(
        mod,
        "_profile_integrity_summary",
        lambda _scopes: pytest.fail("newer mutable latest must not be consulted"),
    )

    with pytest.raises(mod.AirflowException, match="exact profile commit parity"):
        mod.validate_profile_result()


@pytest.mark.unit
def test_profile_validation_rejects_exact_row_count_mismatch_before_latest(
    monkeypatch,
):
    mod = _load_dag_module()
    old_profile = "wspr2-v3-" + "8" * 64

    class Cursor:
        rows = []

        def execute(self, query):
            assert old_profile in query
            assert "_current" not in query
            assert "_latest" not in query
            if "whoscored_profile_ingest_manifest" in query:
                self.rows = [(old_profile, 2, 8)]
            else:
                self.rows = [
                    ("profile", old_profile, 2),
                    ("participation", old_profile, 2),
                ]

        def fetchall(self):
            return self.rows

        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    from scrapers.base import trino_manager

    monkeypatch.setattr(trino_manager, "get_trino_connection", Connection)
    monkeypatch.setattr(
        mod,
        "_load_result",
        lambda _path: {
            "status": "success",
            "paid_proxy_bytes": 0,
            "producer_commits": _producer_commits(scope=(), profile=(old_profile,)),
            "producer_attempts": _producer_attempts(
                match=None, preview=None, profile=(8,)
            ),
        },
    )
    monkeypatch.setattr(mod, "_active_scope_specs", lambda: ["WS-252-2=2526"])
    monkeypatch.setattr(
        mod,
        "_profile_integrity_summary",
        lambda _scopes: pytest.fail("newer mutable latest must not be consulted"),
    )

    with pytest.raises(mod.AirflowException, match="exact profile commit parity"):
        mod.validate_profile_result()


@pytest.mark.unit
def test_profile_validation_requires_the_planned_candidate_identity(monkeypatch):
    mod = _load_dag_module()
    plan = _scope_plan()
    player_ids = (4, 5)
    candidate_sha256 = entity_id_payload_sha256(player_ids)
    capacity = _profile_capacity(count=2, payload_sha256=candidate_sha256)
    report = {
        "status": "success",
        "paid_proxy_bytes": 0,
        "catalog_batch_id": "wsc2-test-generation",
        "profile_candidates": {
            "schema_version": 1,
            "count": 2,
            "payload_sha256": candidate_sha256,
            "attempted": 2,
        },
        "producer_commits": _producer_commits(
            scope=(),
            profile=("wspr2-v3-" + "4" * 64, "wspr2-v3-" + "5" * 64),
        ),
        "producer_attempts": _producer_attempts(
            match=None, preview=None, profile=player_ids
        ),
    }
    monkeypatch.setattr(mod, "_load_result", lambda _path: report)
    monkeypatch.setattr(
        mod,
        "_profile_producer_integrity_summary",
        lambda commits: _exact_profile_integrity(commits, player_ids=player_ids),
    )
    monkeypatch.setattr(
        mod,
        "_profile_integrity_summary",
        lambda _scopes: {
            "roster_players": 2,
            "current_profile_manifests": 2,
            "current_profile_rows": 2,
            "uncovered_profiles": 0,
            "stale_profiles": 0,
            "manifest_participation_rows": 4,
            "current_participation_rows": 4,
        },
    )

    assert (
        mod.validate_profile_result(
            scope_plan=plan,
            capacity=capacity,
        )["status"]
        == "success"
    )

    report["profile_candidates"] = {
        **report["profile_candidates"],
        "attempted": 2.0,
    }
    with pytest.raises(mod.AirflowException, match="candidate identity mismatch"):
        mod.validate_profile_result(scope_plan=plan, capacity=capacity)


@pytest.mark.unit
def test_profile_integrity_is_one_scoped_aggregate_query(monkeypatch):
    mod = _load_dag_module()
    queries = []

    class Cursor:
        def execute(self, query):
            queries.append(query)

        def fetchall(self):
            return [[10, 9, 9, 0, 0, 1, 0, 0, 0, 30, 30]]

        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    from scrapers.base import trino_manager

    monkeypatch.setattr(trino_manager, "get_trino_connection", Connection)
    result = mod._profile_integrity_summary(["WS-1=2026"])

    assert len(queries) == 1
    assert "WITH roster AS" in queries[0]
    assert "JOIN roster r" in queries[0]
    assert "availability_version=" in queries[0]
    assert result["not_available_profiles"] == 1
    assert result["uncovered_profiles"] == 0


@pytest.mark.unit
def test_catalog_validation_requires_atomic_full_history_snapshot(monkeypatch):
    mod = _load_dag_module()
    monkeypatch.setattr(
        mod,
        "_load_result",
        lambda _path: {
            "status": "success",
            "errors": [],
            **{
                key: value
                for key, value in _catalog_identity().items()
                if key != "schema_version"
            },
        },
    )
    complete = {
        "manifest_competitions": 433,
        "physical_competitions": 433,
        "distinct_competitions": 433,
        "manifest_seasons": 3000,
        "physical_seasons": 3000,
        "distinct_seasons": 3000,
        "manifest_stages": 3200,
        "physical_stages": 3200,
        "distinct_stages": 3200,
        "quarantined": 0,
        "eligible_seasons_without_stages": 0,
        "active_scopes": 5,
        "manifest_identity_valid": 1,
    }
    monkeypatch.setattr(
        mod,
        "_catalog_integrity_summary",
        lambda identity: (
            complete
            if identity == _catalog_identity()
            else pytest.fail("catalog DQ received the wrong report identity")
        ),
    )

    validated = mod.validate_catalog_result()
    assert validated["manifest_competitions"] == 433
    assert validated["catalog_identity"] == _catalog_identity()

    monkeypatch.setattr(
        mod,
        "_catalog_integrity_summary",
        lambda _identity: {**complete, "eligible_seasons_without_stages": 1},
    )
    with pytest.raises(mod.AirflowException, match="catalog is incomplete"):
        mod.validate_catalog_result()


@pytest.mark.unit
def test_catalog_dq_queries_only_the_discovery_report_batch(monkeypatch):
    mod = _load_dag_module()
    identity = _catalog_identity(batch_id="wsc2-dagrun")
    queries = []

    class Cursor:
        query = ""

        def execute(self, query):
            self.query = query
            queries.append(query)

        def fetchall(self):
            if "WITH selected_manifest AS" in self.query:
                return [[1, 433, 433, 433, 3000, 3000, 3000, 3200, 3200, 3200, 0, 0]]
            if "SELECT batch_id, payload_sha256" in self.query:
                descriptors = json.dumps(
                    [
                        {
                            "target_id": "whoscored:catalog-provenance:wsc2-dagrun",
                            "payload_sha256": "b" * 64,
                            "raw_uri": "s3://raw/whoscored/catalog/wsc2-dagrun.json",
                            "input_count": 433,
                            "as_of_date": "2026-07-11",
                        },
                        {
                            "audit_type": "whoscored_technical_exclusion_audit",
                            "payload_sha256": "c" * 64,
                            "as_of_date": "2026-07-11",
                        },
                    ]
                )
                return [
                    [
                        "wsc2-dagrun",
                        "a" * 64,
                        "whoscored-v2",
                        "a" * 64,
                        "b" * 64,
                        descriptors,
                        "2026-07-11",
                        None,
                        None,
                        None,
                    ]
                ]
            if "SELECT payload_json" in self.query:
                return []
            pytest.fail(f"unexpected catalog DQ query: {self.query}")

        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    fake_catalog = SimpleNamespace(
        to_rows=lambda: {},
        eligible_scopes=lambda **_kwargs: tuple(range(5)),
    )
    from scrapers.base import trino_manager
    from scrapers.whoscored import catalog as catalog_module
    from scrapers.whoscored import parsers, repository

    monkeypatch.setattr(trino_manager, "get_trino_connection", Connection)
    monkeypatch.setattr(
        catalog_module.WhoScoredCatalog,
        "from_rows",
        classmethod(lambda _cls, _rows: fake_catalog),
    )
    monkeypatch.setattr(repository, "catalog_payload_sha256", lambda _rows: "a" * 64)
    monkeypatch.setattr(parsers, "PARSER_VERSION", "whoscored-v2")

    summary = mod._catalog_integrity_summary(identity)

    assert summary["manifest_identity_valid"] == 1
    assert summary["active_scopes"] == 5
    assert all("latest" not in query.lower() for query in queries)
    assert all(
        "wsc2-dagrun" in query
        for query in queries
        if "whoscored_catalog_manifest" in query
        or "whoscored_competitions" in query
        or "whoscored_seasons" in query
        or "whoscored_stages" in query
    )


@pytest.mark.unit
def test_initial_catalog_discovery_timeout_covers_cold_full_history():
    _load_dag_module()
    from airflow.operators.bash import BashOperator

    task = next(
        task
        for task in BashOperator._instances
        if task.task_id == "discover_whoscored_catalog"
    )

    assert task._init_kwargs["execution_timeout"] == timedelta(hours=4)


@pytest.mark.unit
def test_traffic_aggregation_rejects_legacy_boolean_paid_override(
    monkeypatch, tmp_path
):
    mod = _load_dag_module()
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path))
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    run_dir = mod._run_dir_from_context(_context())
    run_dir.mkdir(parents=True)
    (run_dir / "one.json").write_text(
        json.dumps({"schema_version": 3, "paid_proxy_bytes": 4_000_001})
    )
    (run_dir / "two.json").write_text(
        json.dumps({"schema_version": 3, "paid_proxy_bytes": 4_000_000})
    )

    with pytest.raises(mod.AirflowException, match="used paid proxy"):
        mod.aggregate_traffic_reports(
            **{**_context(), "params": {"require_zero_paid": False}}
        )


@pytest.mark.unit
def test_traffic_aggregation_streams_retry_ledgers_with_exact_url_and_task(
    monkeypatch, tmp_path
):
    mod = _load_dag_module()
    _stub_traffic_reconciliation_context(monkeypatch, mod)
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    paid_ledger = tmp_path / "paid.jsonl"
    monkeypatch.setattr(mod, "PAID_LEDGER_PATH", str(paid_ledger))
    context = {**_context(), "params": {"require_zero_paid": False}}
    monkeypatch.setattr(
        mod,
        "_transport_runtime",
        lambda *_args, **_kwargs: SimpleNamespace(
            is_paid=True,
            dagrun_limit_bytes=1_000_000,
        ),
    )
    monkeypatch.setattr(
        mod,
        "_campaign_ledger_paid_bytes",
        lambda _transport, _events, *_args, **_identity: 500,
    )
    monkeypatch.setattr(mod, "_validate_paid_report_identity", lambda *_a, **_k: None)
    run_dir = mod._run_dir_from_context(context)
    run_dir.mkdir(parents=True)
    canonical = "https://www.whoscored.com/Matches/1/Live?a=1&z=2"
    from dags.scripts import run_whoscored_scraper as runner

    output = run_dir / "scope.json"
    for try_number, paid_bytes, status in (
        (1, 125, "retryable"),
        (2, 375, "success"),
    ):
        runner._write_report(
            str(output),
            {
                "schema_version": 3,
                "status": status,
                "paid_proxy_bytes": paid_bytes,
                "traffic": {"paid_proxy_bytes_by_url": {canonical: paid_bytes}},
                "airflow": {
                    "dag_id": "dag_ingest_whoscored",
                    "dag_run_id": context["run_id"],
                    "task_id": "ingest_active_scope",
                    "map_index": 4,
                    "try_number": try_number,
                },
            },
        )
    request_events = [
        {
            "route": "direct_http",
            "status": "error",
            "url": canonical,
            "request_bytes": 20,
            "response_bytes": 80,
            "task_id": "ingest_active_scope",
            "map_index": 4,
            "try_number": 1,
        },
        {
            "route": "paid_http",
            "status": "accounted",
            "url": "HTTPS://WWW.WHOSCORED.COM/Matches/1/Live?z=2&a=1#fragment",
            "paid_proxy_bytes": 125,
            "request_bytes": 25,
            "response_bytes": 100,
            "task_id": "ingest_active_scope",
            "map_index": 4,
            "try_number": 1,
        },
        {
            "route": "paid_http",
            "status": "accounted",
            "url": canonical,
            "paid_proxy_bytes": 375,
            "request_bytes": 75,
            "response_bytes": 300,
            "task_id": "ingest_active_scope",
            "map_index": 4,
            "try_number": 2,
        },
    ]
    (run_dir / "requests_ingest_4_try1.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in request_events)
    )
    paid_events = [
        {
            "event_type": "bytes",
            "dag_id": "dag_ingest_whoscored",
            "run_id": context["run_id"],
            "task_id": "ingest_active_scope",
            "map_index": 4,
            "try_number": try_number,
            "canonical_url": url,
            "bytes": count,
        }
        for try_number, url, count in (
            (1, "https://www.whoscored.com/Matches/1/Live?z=2&a=1", 125),
            (2, canonical, 375),
        )
    ]
    paid_ledger.write_text("".join(json.dumps(item) + "\n" for item in paid_events))

    def _forbid_unbounded_read_text(*_args, **_kwargs):
        raise AssertionError("JSONL must be streamed, not loaded with read_text")

    monkeypatch.setattr(Path, "read_text", _forbid_unbounded_read_text)
    result = mod.aggregate_traffic_reports(**context)

    assert result["paid_proxy_bytes"] == 500
    assert result["request_ledger_paid_proxy_bytes"] == 500
    assert result["durable_paid_proxy_bytes"] == 500
    assert result["paid_urls"] == 1
    assert result["request_count"] == 1
    assert len(json.dumps(result).encode("utf-8")) < mod.MAX_TRAFFIC_XCOM_BYTES
    with Path(result["artifact_uri"]).open("r", encoding="utf-8") as handle:
        detail = json.load(handle)
    assert detail["paid_proxy_bytes_by_url"] == {canonical: 500}
    assert detail["paid_proxy_bytes_by_task"] == {"ingest_active_scope[4]": 500}
    assert detail["paid_proxy_bytes_by_task_try"] == {
        "ingest_active_scope[4]/try1": 125,
        "ingest_active_scope[4]/try2": 375,
    }
    # The paid accounting summaries are not counted as extra HTTP requests.
    assert detail["route_requests"] == {"direct_http": 1}
    assert detail["route_bytes"] == {"direct_http": 100}
    assert result["cleaned_local_files"] == 4
    assert not (run_dir / "scope.json").exists()
    assert not (run_dir / "requests_ingest_4_try1.jsonl").exists()
    # A retry after cleanup resolves the immutable completion receipt and does
    # not require the deleted 31MB-class local staging files.
    assert mod.aggregate_traffic_reports(**context) == result


def _campaign_gateway_stub(snapshot):
    def call(_approval, operation, **_kwargs):
        if operation in {"snapshot", "sealed_snapshot"}:
            return snapshot
        if operation == "seal_for_reconciliation":
            return {
                **snapshot,
                "status": "sealed",
                "reconciliation_seal": {"schema_version": 1},
            }
        raise AssertionError(operation)

    return call


@pytest.mark.unit
def test_campaign_ledger_is_reconciled_by_exact_airflow_attempt(monkeypatch):
    mod = _load_dag_module()
    from scrapers.whoscored import proxy_campaign

    allocation = SimpleNamespace(
        allocation_id="scope-1",
        task_id="ingest_active_scope",
        work_item_id="scope-work-1",
        budget_bytes=1_000,
    )
    approval = SimpleNamespace(
        campaign_id="campaign-1",
        approval_id="approval-1",
        approval_sha256="a" * 64,
        run_id="manual__paid-1",
        allowed_dag_ids=("dag_ingest_whoscored",),
        allocations=(allocation,),
    )
    transport = SimpleNamespace(is_paid=True, approval=approval)
    event = {
        "dag_id": "dag_ingest_whoscored",
        "run_id": "manual__paid-1",
        "task_id": "ingest_active_scope",
        "map_index": 4,
        "try_number": 2,
        "allocation_id": "scope-1",
        "proxy_campaign_id": "campaign-1",
        "proxy_approval_id": "approval-1",
        "proxy_approval_sha256": "a" * 64,
        "provider_meter": proxy_campaign.PROXY_CAMPAIGN_METER,
        "proxy_work_item_id": allocation.work_item_id,
    }
    attempt_id = proxy_campaign.deterministic_proxy_attempt_id(
        dag_id=event["dag_id"],
        run_id=event["run_id"],
        task_id=event["task_id"],
        map_index=event["map_index"],
        try_number=event["try_number"],
    )
    attempt_hash = hashlib.sha256(attempt_id.encode()).hexdigest()
    lease_id = "lease-1"
    lease_hash = hashlib.sha256(lease_id.encode()).hexdigest()
    canonical_url = "https://www.whoscored.com/Matches/1/Live"
    canonical_url_hash = hashlib.sha256(canonical_url.encode()).hexdigest()
    snapshot = {
        "campaign_id": approval.campaign_id,
        "approval_id": approval.approval_id,
        "approval_sha256": approval.approval_sha256,
        "run_id_hash": hashlib.sha256(event["run_id"].encode()).hexdigest(),
        "dag_id_hash": hashlib.sha256(event["dag_id"].encode()).hexdigest(),
        "status": "active",
        "spent_provider_bytes": 125,
        "active_claims": {},
        "allocations": {
            "scope-1": {
                "allocation_id": "scope-1",
                "task_id": "ingest_active_scope",
                "spent_provider_bytes": 125,
                "attempts": [
                    {
                        "attempt_id_hash": attempt_hash,
                        "lease_id_hash": lease_hash,
                        "canonical_url_sha256": canonical_url_hash,
                        "provider_billed_bytes": 125,
                    }
                ],
            }
        },
    }
    monkeypatch.setattr(
        mod, "paid_campaign_gateway_call", _campaign_gateway_stub(snapshot)
    )

    common_event = {
        **event,
        "event_version": "paid-proxy-v2",
        "lease_id": lease_id,
        "proxy_attempt_id": attempt_id,
        "canonical_url": canonical_url,
    }
    events = [
        {
            **common_event,
            "event_id": "1" * 24,
            "event_type": "lease_created",
            "max_bytes": 1_000,
        },
        {
            **common_event,
            "event_id": "2" * 24,
            "event_type": "bytes",
            "bytes": 125,
            "direction": "down",
        },
        {
            **common_event,
            "event_id": "3" * 24,
            "event_type": "lease_closed",
            "total_bytes": 125,
        },
    ]
    request_event = {
        "event_version": "whoscored-request-v1",
        "event_id": "7" * 32,
        "status": "accounted",
        "dag_id": event["dag_id"],
        "run_id": event["run_id"],
        "task_id": event["task_id"],
        "map_index": event["map_index"],
        "try_number": event["try_number"],
        "proxy_campaign_id": approval.campaign_id,
        "proxy_approval_id": approval.approval_id,
        "proxy_approval_sha256": approval.approval_sha256,
        "proxy_allocation_id": allocation.allocation_id,
        "proxy_work_item_id": allocation.work_item_id,
        "proxy_attempt_id": attempt_id,
        "lease_id": lease_id,
        "url": canonical_url,
        "paid_proxy_bytes": 125,
    }

    assert (
        mod._campaign_ledger_paid_bytes(
            transport,
            events,
            [request_event],
            dag_id=event["dag_id"],
            run_id=event["run_id"],
        )
        == 125
    )


@pytest.mark.unit
def test_campaign_ledger_rejects_zero_byte_active_claim_for_current_dagrun(
    monkeypatch,
):
    mod = _load_dag_module()

    dag_id = "dag_ingest_whoscored"
    run_id = "manual__zero-byte-active"
    allocation = SimpleNamespace(
        allocation_id="scope-1",
        task_id="ingest_active_scope",
        work_item_id="scope-work-1",
        budget_bytes=1_000,
    )
    approval = SimpleNamespace(
        campaign_id="campaign-1",
        approval_id="approval-1",
        approval_sha256="a" * 64,
        run_id=run_id,
        allowed_dag_ids=(dag_id,),
        allocations=(allocation,),
    )
    transport = SimpleNamespace(is_paid=True, approval=approval)
    snapshot = {
        "campaign_id": approval.campaign_id,
        "approval_id": approval.approval_id,
        "approval_sha256": approval.approval_sha256,
        "run_id_hash": hashlib.sha256(run_id.encode()).hexdigest(),
        "dag_id_hash": hashlib.sha256(dag_id.encode()).hexdigest(),
        "status": "active",
        "spent_provider_bytes": 0,
        "active_claims": {
            "lease-1": {
                "dag_id": dag_id,
                "run_id_hash": hashlib.sha256(run_id.encode()).hexdigest(),
                "attempt_id_hash": "b" * 64,
            }
        },
        "allocations": {
            "scope-1": {
                "allocation_id": "scope-1",
                "task_id": "ingest_active_scope",
                "spent_provider_bytes": 0,
                "attempts": [],
            }
        },
    }
    monkeypatch.setattr(
        mod, "paid_campaign_gateway_call", _campaign_gateway_stub(snapshot)
    )

    with pytest.raises(mod.AirflowException, match="active claims for this DagRun"):
        mod._campaign_ledger_paid_bytes(
            transport,
            [],
            dag_id=dag_id,
            run_id=run_id,
        )


def _finished_campaign_reconciliation_case(provider_billed_bytes=125):
    from scrapers.whoscored import proxy_campaign

    dag_id = "dag_ingest_whoscored"
    run_id = "manual__paid-reconciliation"
    allocation = SimpleNamespace(
        allocation_id="scope-1",
        task_id="ingest_active_scope",
        work_item_id="scope-work-1",
        budget_bytes=1_000,
    )
    approval = SimpleNamespace(
        campaign_id="campaign-1",
        approval_id="approval-1",
        approval_sha256="a" * 64,
        run_id=run_id,
        allowed_dag_ids=(dag_id,),
        allocations=(allocation,),
    )
    attempt_id = proxy_campaign.deterministic_proxy_attempt_id(
        dag_id=dag_id,
        run_id=run_id,
        task_id=allocation.task_id,
        map_index=4,
        try_number=2,
    )
    lease_id = "lease-reconciliation"
    canonical_url = "https://www.whoscored.com/Matches/2/Live"
    common_event = {
        "event_version": "paid-proxy-v2",
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": allocation.task_id,
        "map_index": 4,
        "try_number": 2,
        "allocation_id": allocation.allocation_id,
        "proxy_campaign_id": approval.campaign_id,
        "proxy_approval_id": approval.approval_id,
        "proxy_approval_sha256": approval.approval_sha256,
        "provider_meter": proxy_campaign.PROXY_CAMPAIGN_METER,
        "proxy_work_item_id": allocation.work_item_id,
        "proxy_attempt_id": attempt_id,
        "lease_id": lease_id,
        "canonical_url": canonical_url,
    }
    events = [
        {
            **common_event,
            "event_id": "4" * 24,
            "event_type": "lease_created",
            "max_bytes": 1_000,
        },
        {
            **common_event,
            "event_id": "5" * 24,
            "event_type": "lease_closed",
            "total_bytes": provider_billed_bytes,
        },
    ]
    if provider_billed_bytes:
        events.insert(
            1,
            {
                **common_event,
                "event_id": "6" * 24,
                "event_type": "bytes",
                "bytes": provider_billed_bytes,
                "direction": "down",
            },
        )
    attempt = {
        "attempt_id_hash": hashlib.sha256(attempt_id.encode()).hexdigest(),
        "lease_id_hash": hashlib.sha256(lease_id.encode()).hexdigest(),
        "canonical_url_sha256": hashlib.sha256(canonical_url.encode()).hexdigest(),
        "provider_billed_bytes": provider_billed_bytes,
    }
    snapshot = {
        "campaign_id": approval.campaign_id,
        "approval_id": approval.approval_id,
        "approval_sha256": approval.approval_sha256,
        "run_id_hash": hashlib.sha256(run_id.encode()).hexdigest(),
        "dag_id_hash": hashlib.sha256(dag_id.encode()).hexdigest(),
        "status": "active",
        "spent_provider_bytes": provider_billed_bytes,
        "active_claims": {},
        "allocations": {
            allocation.allocation_id: {
                "allocation_id": allocation.allocation_id,
                "task_id": allocation.task_id,
                "spent_provider_bytes": provider_billed_bytes,
                "attempts": [attempt],
            }
        },
    }
    request_event = {
        "event_version": "whoscored-request-v1",
        "event_id": "8" * 32,
        "status": "accounted",
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": allocation.task_id,
        "map_index": 4,
        "try_number": 2,
        "proxy_campaign_id": approval.campaign_id,
        "proxy_approval_id": approval.approval_id,
        "proxy_approval_sha256": approval.approval_sha256,
        "proxy_allocation_id": allocation.allocation_id,
        "proxy_work_item_id": allocation.work_item_id,
        "proxy_attempt_id": attempt_id,
        "lease_id": lease_id,
        "url": canonical_url,
        "paid_proxy_bytes": provider_billed_bytes,
    }
    return approval, events, [request_event], snapshot, dag_id, run_id


@pytest.mark.unit
def test_campaign_reconciliation_resumes_seal_after_approval_expiry(monkeypatch):
    mod = _load_dag_module()
    from scrapers.whoscored import proxy_campaign

    approval, events, request_events, snapshot, dag_id, run_id = (
        _finished_campaign_reconciliation_case()
    )
    allocation_id = approval.allocations[0].allocation_id
    accounting = [
        {
            "allocation_id": allocation_id,
            "attempt_id_hash": attempt["attempt_id_hash"],
            "lease_id_hash": attempt["lease_id_hash"],
            "canonical_url_sha256": attempt["canonical_url_sha256"],
            "provider_billed_bytes": attempt["provider_billed_bytes"],
        }
        for attempt in snapshot["allocations"][allocation_id]["attempts"]
    ]
    accounting.sort(
        key=lambda item: (
            item["allocation_id"],
            item["attempt_id_hash"],
            item["lease_id_hash"],
        )
    )
    accounting_sha256 = hashlib.sha256(
        proxy_campaign.canonical_json_bytes(accounting)
    ).hexdigest()
    terminal_snapshot = {
        **snapshot,
        "status": "sealed",
        "reconciliation_seal": {
            "schema_version": 1,
            "approval_sha256": approval.approval_sha256,
            "dag_id_hash": hashlib.sha256(dag_id.encode()).hexdigest(),
            "run_id_hash": hashlib.sha256(run_id.encode()).hexdigest(),
            "provider_billed_bytes": 125,
            "attempt_accounting_sha256": accounting_sha256,
        },
    }
    live_snapshot_calls = 0

    def expired_live_snapshot():
        nonlocal live_snapshot_calls
        live_snapshot_calls += 1
        raise mod.WhoScoredProxyRuntimeError("approval expired after seal")

    def campaign_gateway(_approval, operation, **values):
        if operation == "snapshot":
            return expired_live_snapshot()
        if operation == "sealed_snapshot":
            return terminal_snapshot
        assert operation == "seal_for_reconciliation"
        assert values["provider_billed_bytes"] == 125
        assert values["attempt_accounting_sha256"] == accounting_sha256
        return terminal_snapshot

    monkeypatch.setattr(mod, "paid_campaign_gateway_call", campaign_gateway)

    assert (
        mod._campaign_ledger_paid_bytes(
            SimpleNamespace(is_paid=True, approval=approval),
            events,
            request_events,
            dag_id=dag_id,
            run_id=run_id,
        )
        == 125
    )
    assert live_snapshot_calls == 1


@pytest.mark.unit
def test_campaign_ledger_rejects_campaign_only_attempt(monkeypatch):
    mod = _load_dag_module()

    approval, _events, _request_events, snapshot, dag_id, run_id = (
        _finished_campaign_reconciliation_case()
    )
    monkeypatch.setattr(
        mod, "paid_campaign_gateway_call", _campaign_gateway_stub(snapshot)
    )

    with pytest.raises(mod.AirflowException, match="attempt and lease sets differ"):
        mod._campaign_ledger_paid_bytes(
            SimpleNamespace(is_paid=True, approval=approval),
            [],
            dag_id=dag_id,
            run_id=run_id,
        )


@pytest.mark.unit
def test_campaign_ledger_rejects_proxy_only_attempt(monkeypatch):
    mod = _load_dag_module()

    approval, events, request_events, snapshot, dag_id, run_id = (
        _finished_campaign_reconciliation_case()
    )
    snapshot["spent_provider_bytes"] = 0
    allocation = snapshot["allocations"]["scope-1"]
    allocation["spent_provider_bytes"] = 0
    allocation["attempts"] = []
    monkeypatch.setattr(
        mod, "paid_campaign_gateway_call", _campaign_gateway_stub(snapshot)
    )

    with pytest.raises(mod.AirflowException, match="attempt and lease sets differ"):
        mod._campaign_ledger_paid_bytes(
            SimpleNamespace(is_paid=True, approval=approval),
            events,
            request_events,
            dag_id=dag_id,
            run_id=run_id,
        )


@pytest.mark.unit
def test_campaign_ledger_rejects_snapshot_total_drift(monkeypatch):
    mod = _load_dag_module()

    approval, events, request_events, snapshot, dag_id, run_id = (
        _finished_campaign_reconciliation_case()
    )
    snapshot["spent_provider_bytes"] += 1
    monkeypatch.setattr(
        mod, "paid_campaign_gateway_call", _campaign_gateway_stub(snapshot)
    )

    with pytest.raises(mod.AirflowException, match="total byte counters differ"):
        mod._campaign_ledger_paid_bytes(
            SimpleNamespace(is_paid=True, approval=approval),
            events,
            request_events,
            dag_id=dag_id,
            run_id=run_id,
        )


@pytest.mark.unit
def test_campaign_ledger_rejects_fake_request_lease_with_same_total(monkeypatch):
    mod = _load_dag_module()

    approval, events, request_events, snapshot, dag_id, run_id = (
        _finished_campaign_reconciliation_case()
    )
    request_events[0] = {**request_events[0], "lease_id": "fake-lease"}
    monkeypatch.setattr(
        mod, "paid_campaign_gateway_call", _campaign_gateway_stub(snapshot)
    )

    with pytest.raises(mod.AirflowException, match="request/proxy.*sets differ"):
        mod._campaign_ledger_paid_bytes(
            SimpleNamespace(is_paid=True, approval=approval),
            events,
            request_events,
            dag_id=dag_id,
            run_id=run_id,
        )


@pytest.mark.unit
def test_campaign_ledger_reconciles_zero_byte_finished_lease(monkeypatch):
    mod = _load_dag_module()

    approval, events, request_events, snapshot, dag_id, run_id = (
        _finished_campaign_reconciliation_case(provider_billed_bytes=0)
    )
    monkeypatch.setattr(
        mod, "paid_campaign_gateway_call", _campaign_gateway_stub(snapshot)
    )

    assert (
        mod._campaign_ledger_paid_bytes(
            SimpleNamespace(is_paid=True, approval=approval),
            events,
            request_events,
            dag_id=dag_id,
            run_id=run_id,
        )
        == 0
    )


@pytest.mark.unit
def test_traffic_aggregation_rejects_same_total_different_attribution_maps(
    monkeypatch, tmp_path
):
    mod = _load_dag_module()
    _stub_traffic_reconciliation_context(monkeypatch, mod)
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    paid_ledger = tmp_path / "paid.jsonl"
    monkeypatch.setattr(mod, "PAID_LEDGER_PATH", str(paid_ledger))
    context = {**_context(), "params": {"require_zero_paid": False}}
    monkeypatch.setattr(
        mod,
        "_transport_runtime",
        lambda *_args, **_kwargs: SimpleNamespace(
            is_paid=True,
            dagrun_limit_bytes=1_000_000,
        ),
    )
    monkeypatch.setattr(
        mod,
        "_campaign_ledger_paid_bytes",
        lambda _transport, _events, *_args, **_identity: 100,
    )
    monkeypatch.setattr(mod, "_validate_paid_report_identity", lambda *_a, **_k: None)
    run_dir = mod._run_dir_from_context(context)
    run_dir.mkdir(parents=True)
    (run_dir / "scope.json").write_text(
        json.dumps(
            {
                "schema_version": 3,
                "paid_proxy_bytes": 100,
                "traffic": {
                    "paid_proxy_bytes_by_url": {"https://www.whoscored.com/report": 100}
                },
                "airflow": {
                    "task_id": "ingest_active_scope",
                    "map_index": 4,
                    "try_number": 2,
                },
            }
        )
    )
    (run_dir / "requests_ingest_4_try2.jsonl").write_text(
        json.dumps(
            {
                "status": "accounted",
                "url": "https://www.whoscored.com/request-ledger",
                "paid_proxy_bytes": 100,
                "request_bytes": 0,
                "response_bytes": 0,
                "task_id": "ingest_active_scope",
                "map_index": 4,
                "try_number": 2,
            }
        )
        + "\n"
    )
    paid_ledger.write_text(
        json.dumps(
            {
                "event_type": "bytes",
                "dag_id": "dag_ingest_whoscored",
                "run_id": context["run_id"],
                "task_id": "ingest_active_scope",
                "map_index": 4,
                "try_number": 2,
                "canonical_url": "https://www.whoscored.com/proxy-ledger",
                "bytes": 100,
            }
        )
        + "\n"
    )

    with pytest.raises(mod.AirflowException, match="attribution maps differ"):
        mod.aggregate_traffic_reports(**context)


@pytest.mark.unit
def test_traffic_aggregation_rejects_proxy_ledger_drift(monkeypatch, tmp_path):
    mod = _load_dag_module()
    monkeypatch.setattr(mod, "_validate_paid_report_identity", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    paid_ledger = tmp_path / "paid.jsonl"
    monkeypatch.setattr(mod, "PAID_LEDGER_PATH", str(paid_ledger))
    context = {**_context(), "params": {"require_zero_paid": False}}
    monkeypatch.setattr(
        mod,
        "_transport_runtime",
        lambda *_args, **_kwargs: SimpleNamespace(
            is_paid=True,
            dagrun_limit_bytes=10_000_000,
        ),
    )
    run_dir = mod._run_dir_from_context(context)
    run_dir.mkdir(parents=True)
    (run_dir / "scope.json").write_text(
        json.dumps({"schema_version": 3, "paid_proxy_bytes": 0})
    )
    paid_ledger.write_text(
        json.dumps(
            {
                "event_type": "bytes",
                "dag_id": "dag_ingest_whoscored",
                "run_id": context["run_id"],
                "task_id": "matches",
                "canonical_url": "https://www.whoscored.com/x?stage=1",
                "bytes": 2_000_001,
            }
        )
        + "\n"
    )

    with pytest.raises(mod.AirflowException, match="paid accounting mismatch"):
        mod.aggregate_traffic_reports(**context)


@pytest.mark.unit
@pytest.mark.parametrize("target", ["detail", "completion"])
def test_traffic_resume_rejects_tampered_content_addressed_artifacts(
    monkeypatch, tmp_path, target
):
    mod = _load_dag_module()
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    monkeypatch.setattr(mod, "PAID_LEDGER_PATH", str(tmp_path / "missing.jsonl"))
    context = _context()
    run_dir = mod._run_dir_from_context(context)
    run_dir.mkdir(parents=True)
    (run_dir / "scope.json").write_text(
        json.dumps({"schema_version": 3, "paid_proxy_bytes": 0})
    )
    result = mod.aggregate_traffic_reports(**context)
    if target == "detail":
        path = Path(result["artifact_uri"])
    else:
        matches = list((tmp_path / "traffic").rglob("completion/*.json"))
        assert len(matches) == 1
        path = matches[0]
    value = json.loads(path.read_text(encoding="utf-8"))
    value["schema_version"] = 999
    path.write_text(
        json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="content-addressed"):
        mod.aggregate_traffic_reports(**context)


@pytest.mark.unit
def test_traffic_resume_rejects_extra_or_replaced_staging_inputs(monkeypatch, tmp_path):
    mod = _load_dag_module()
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    monkeypatch.setattr(mod, "PAID_LEDGER_PATH", str(tmp_path / "missing.jsonl"))
    monkeypatch.setattr(
        mod,
        "_traffic_reconciliation_context",
        lambda _transport: {
            "contract": mod.TRAFFIC_RECONCILIATION_CONTRACT,
            "runtime_manifest_sha256": "a" * 64,
            "runtime_code_tree_sha256": "b" * 64,
            "transport_policy": "direct_only",
            "campaign_id": None,
            "approval_id": None,
            "approval_sha256": None,
        },
    )
    context = _context()
    run_dir = mod._run_dir_from_context(context)
    run_dir.mkdir(parents=True)
    original = run_dir / "scope.json"
    original.write_text(json.dumps({"schema_version": 3, "paid_proxy_bytes": 0}))
    summary = mod.aggregate_traffic_reports(**context)

    extra = run_dir / "extra.json"
    extra.write_text(json.dumps({"schema_version": 3, "paid_proxy_bytes": 0}))
    with pytest.raises(mod.AirflowException, match="staging input changed"):
        mod.aggregate_traffic_reports(**context)
    extra.unlink()

    extra_request = run_dir / "requests_cleared_task_try2.jsonl"
    extra_request.write_text("{}\n", encoding="utf-8")
    with pytest.raises(mod.AirflowException, match="staging input changed"):
        mod.aggregate_traffic_reports(**context)
    extra_request.unlink()

    original.write_text(
        json.dumps({"schema_version": 3, "paid_proxy_bytes": 0, "changed": True})
    )
    with pytest.raises(mod.AirflowException, match="staging input changed"):
        mod.aggregate_traffic_reports(**context)
    original.unlink()
    assert mod.aggregate_traffic_reports(**context) == summary


def _prepare_paid_traffic_completion(monkeypatch, tmp_path):
    mod = _load_dag_module()
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    paid_ledger = tmp_path / "paid.jsonl"
    monkeypatch.setattr(mod, "PAID_LEDGER_PATH", str(paid_ledger))
    context = {**_context(), "params": {"require_zero_paid": False}}
    allocation = SimpleNamespace(
        allocation_id="scope-1",
        task_id="ingest_active_scope",
        work_item_id="scope-work-1",
        budget_bytes=1_000,
    )
    approval = SimpleNamespace(
        campaign_id="campaign-paid-replay",
        approval_id="approval-paid-replay",
        approval_sha256="a" * 64,
        allocations=(allocation,),
    )
    transport = SimpleNamespace(
        is_paid=True,
        dagrun_limit_bytes=1_000,
        policy="direct_then_paid",
        approval=approval,
    )
    monkeypatch.setattr(mod, "_transport_runtime", lambda *_a, **_k: transport)
    reconciliation_context = {
        "contract": mod.TRAFFIC_RECONCILIATION_CONTRACT,
        "runtime_manifest_sha256": "b" * 64,
        "runtime_code_tree_sha256": "c" * 64,
        "transport_policy": "direct_then_paid",
        "campaign_id": approval.campaign_id,
        "approval_id": approval.approval_id,
        "approval_sha256": approval.approval_sha256,
    }
    monkeypatch.setattr(
        mod,
        "_traffic_reconciliation_context",
        lambda _transport: dict(reconciliation_context),
    )
    sealed_snapshot = {
        "campaign_id": approval.campaign_id,
        "approval_id": approval.approval_id,
        "approval_sha256": approval.approval_sha256,
        "status": "sealed",
        "spent_provider_bytes": 100,
        "active_claims": {},
        "reconciliation_seal": {
            "schema_version": 1,
            "attempt_accounting_sha256": "d" * 64,
        },
    }
    monkeypatch.setattr(
        mod,
        "paid_campaign_gateway_call",
        lambda _approval, operation, **_kwargs: (
            dict(sealed_snapshot)
            if operation == "sealed_snapshot"
            else (_ for _ in ()).throw(AssertionError(operation))
        ),
    )

    def campaign_paid(
        _transport,
        _events,
        *_args,
        reconciliation_evidence=None,
        **_identity,
    ):
        assert reconciliation_evidence is not None
        reconciliation_evidence["campaign_snapshot_sha256"] = (
            mod._current_campaign_snapshot_sha256(transport)
        )
        return 100

    monkeypatch.setattr(mod, "_campaign_ledger_paid_bytes", campaign_paid)
    monkeypatch.setattr(mod, "_validate_paid_report_identity", lambda *_a, **_k: None)
    run_dir = mod._run_dir_from_context(context)
    run_dir.mkdir(parents=True)
    canonical_url = "https://www.whoscored.com/Matches/1/Live"
    (run_dir / "scope.json").write_text(
        json.dumps(
            {
                "schema_version": 3,
                "paid_proxy_bytes": 100,
                "traffic": {"paid_proxy_bytes_by_url": {canonical_url: 100}},
                "airflow": {
                    "dag_id": "dag_ingest_whoscored",
                    "dag_run_id": context["run_id"],
                    "task_id": allocation.task_id,
                    "map_index": 4,
                    "try_number": 2,
                },
            }
        ),
        encoding="utf-8",
    )
    request_path = run_dir / "requests_ingest_4_try2.jsonl"
    request_path.write_text(
        json.dumps(
            {
                "status": "accounted",
                "url": canonical_url,
                "paid_proxy_bytes": 100,
                "request_bytes": 0,
                "response_bytes": 0,
                "task_id": allocation.task_id,
                "map_index": 4,
                "try_number": 2,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    paid_ledger.write_text(
        json.dumps(
            {
                "event_type": "bytes",
                "dag_id": "dag_ingest_whoscored",
                "run_id": context["run_id"],
                "task_id": allocation.task_id,
                "map_index": 4,
                "try_number": 2,
                "canonical_url": canonical_url,
                "bytes": 100,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    summary = mod.aggregate_traffic_reports(**context)
    assert summary["paid_proxy_bytes"] == 100
    assert mod.aggregate_traffic_reports(**context) == summary
    return (
        mod,
        context,
        paid_ledger,
        sealed_snapshot,
        reconciliation_context,
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("changed_input", "error"),
    [
        ("sealed_snapshot", "completion/detail identity mismatch"),
        ("proxy_ledger", "proxy evidence changed"),
        ("durable_request_set", "durable.*request.*evidence"),
        ("runtime_contract", "invalid durable.*completion"),
    ],
)
def test_paid_traffic_resume_rejects_changed_reconciliation_authority(
    monkeypatch, tmp_path, changed_input, error
):
    (
        mod,
        context,
        paid_ledger,
        sealed_snapshot,
        reconciliation_context,
    ) = _prepare_paid_traffic_completion(monkeypatch, tmp_path)

    if changed_input == "sealed_snapshot":
        sealed_snapshot["reconciliation_seal"] = {
            "schema_version": 1,
            "attempt_accounting_sha256": "e" * 64,
        }
    elif changed_input == "proxy_ledger":
        with paid_ledger.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "event_type": "lease_closed",
                        "dag_id": "dag_ingest_whoscored",
                        "run_id": context["run_id"],
                        "event_id": "f" * 24,
                    }
                )
                + "\n"
            )
    elif changed_input == "durable_request_set":
        from dags.scripts.whoscored_ops_store import WhoScoredOpsStore

        store = WhoScoredOpsStore.from_env(optional=False)
        assert store is not None
        prefix = (
            f"traffic/{mod._safe_token('dag_ingest_whoscored')}/"
            f"{mod._safe_token(context['run_id'])}/request-ledgers"
        )
        store.put_content_addressed_json(
            prefix,
            {
                "schema_version": 1,
                "source_name": "late-request.jsonl",
                "source_sha256": "f" * 64,
                "source_bytes": 1,
            },
        )
    else:
        reconciliation_context["runtime_manifest_sha256"] = "f" * 64

    with pytest.raises(mod.AirflowException, match=error):
        mod.aggregate_traffic_reports(**context)


@pytest.mark.unit
def test_completion_prewrite_rejects_changed_evidence_with_same_source_triple(
    monkeypatch, tmp_path
):
    mod = _load_dag_module()
    _stub_traffic_reconciliation_context(monkeypatch, mod)
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    monkeypatch.setattr(mod, "PAID_LEDGER_PATH", str(tmp_path / "missing.jsonl"))
    monkeypatch.setattr(
        mod,
        "_transport_runtime",
        lambda *_a, **_k: SimpleNamespace(
            is_paid=False,
            dagrun_limit_bytes=0,
            policy="direct_only",
            approval=None,
        ),
    )
    context = _context()
    run_dir = mod._run_dir_from_context(context)
    run_dir.mkdir(parents=True)
    (run_dir / "scope.json").write_text(
        json.dumps({"schema_version": 3, "paid_proxy_bytes": 0}),
        encoding="utf-8",
    )
    identity = {
        "dag_id": "dag_ingest_whoscored",
        "run_id": context["run_id"],
        "task_id": "prepare_backfill_plan",
        "map_index": -1,
        "try_number": 1,
    }
    event = {
        "event_version": "whoscored-request-v1",
        "event_id": "1" * 32,
        **identity,
        "route": "direct_http",
        "status": "success",
        "url": "https://www.whoscored.com/discovery",
        "request_bytes": 1,
        "response_bytes": 2,
        "paid_proxy_bytes": 0,
    }
    source_payload = (
        json.dumps(
            event,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    evidence = {
        "schema_version": 1,
        "evidence_type": "whoscored_request_ledger",
        **identity,
        "source_name": "requests_prepare_backfill_plan_-1_try1.jsonl",
        "source_sha256": hashlib.sha256(source_payload).hexdigest(),
        "source_bytes": len(source_payload),
        "event_count": 1,
        "request_count": 1,
        "wire_bytes": 3,
        "paid_proxy_bytes": 0,
        "events": [event],
    }
    forged_evidence = {**evidence, "task_id": "forged_task"}
    from dags.scripts.whoscored_ops_store import WhoScoredOpsStore

    store = WhoScoredOpsStore.from_env(optional=False)
    assert store is not None
    traffic_prefix = (
        f"traffic/{mod._safe_token('dag_ingest_whoscored')}/"
        f"{mod._safe_token(context['run_id'])}"
    )
    request_prefix = f"{traffic_prefix}/request-ledgers"
    store.put_content_addressed_json(request_prefix, evidence)
    store.put_content_addressed_json(request_prefix, forged_evidence)
    real_iter = WhoScoredOpsStore.iter_content_addressed_json
    request_reads = 0

    def mutate_after_initial_parse(self, prefix):
        nonlocal request_reads
        records = list(real_iter(self, prefix))
        if prefix.strip("/") == request_prefix:
            request_reads += 1
            if request_reads == 1:
                records = [
                    item
                    for item in records
                    if item[1].get("task_id") == identity["task_id"]
                ]
        return iter(records)

    monkeypatch.setattr(
        WhoScoredOpsStore,
        "iter_content_addressed_json",
        mutate_after_initial_parse,
    )

    with pytest.raises(mod.AirflowException, match="invalid event in durable"):
        mod.aggregate_traffic_reports(**context)

    assert request_reads == 2
    assert list(real_iter(store, f"{traffic_prefix}/completion")) == []


@pytest.mark.unit
def test_traffic_resume_rejects_legacy_completion_receipt(monkeypatch, tmp_path):
    mod = _load_dag_module()
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    monkeypatch.setattr(mod, "PAID_LEDGER_PATH", str(tmp_path / "missing.jsonl"))
    monkeypatch.setattr(
        mod,
        "_traffic_reconciliation_context",
        lambda _transport: {
            "contract": mod.TRAFFIC_RECONCILIATION_CONTRACT,
            "runtime_manifest_sha256": "a" * 64,
            "runtime_code_tree_sha256": "b" * 64,
            "transport_policy": "direct_only",
            "campaign_id": None,
            "approval_id": None,
            "approval_sha256": None,
        },
    )
    context = _context()
    run_dir = mod._run_dir_from_context(context)
    run_dir.mkdir(parents=True)
    (run_dir / "scope.json").write_text(
        json.dumps({"schema_version": 3, "paid_proxy_bytes": 0})
    )
    mod.aggregate_traffic_reports(**context)
    from dags.scripts.whoscored_ops_store import WhoScoredOpsStore

    store = WhoScoredOpsStore.from_env(optional=False)
    assert store is not None
    prefix = (
        f"traffic/{mod._safe_token('dag_ingest_whoscored')}/"
        f"{mod._safe_token(context['run_id'])}/completion"
    )
    store.put_content_addressed_json(prefix, {"schema_version": 1})

    with pytest.raises(mod.AirflowException, match="invalid durable.*completion"):
        mod.aggregate_traffic_reports(**context)


@pytest.mark.unit
def test_traffic_aggregation_rejects_oversized_jsonl_event(monkeypatch, tmp_path):
    mod = _load_dag_module()
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    monkeypatch.setattr(mod, "PAID_LEDGER_PATH", str(tmp_path / "missing.jsonl"))
    monkeypatch.setattr(mod, "MAX_LEDGER_EVENT_BYTES", 32)
    context = {**_context(), "params": {"require_zero_paid": False}}
    run_dir = mod._run_dir_from_context(context)
    run_dir.mkdir(parents=True)
    (run_dir / "scope.json").write_text(
        json.dumps({"schema_version": 3, "paid_proxy_bytes": 0})
    )
    (run_dir / "requests_task.jsonl").write_bytes(b'{"value":"' + b"x" * 80)

    with pytest.raises(mod.AirflowException, match="oversized request ledger"):
        mod.aggregate_traffic_reports(**context)


@pytest.mark.unit
def test_terminal_gate_rejects_false_green_after_all_done_dq():
    mod = _load_dag_module()
    instances = [
        SimpleNamespace(task_id="ingest_active_scope", map_index=2, state="failed"),
        SimpleNamespace(task_id="validate_active_scope", map_index=2, state="success"),
        SimpleNamespace(task_id="final_success_gate", map_index=-1, state="running"),
    ]
    dag_run = SimpleNamespace(get_task_instances=lambda: instances)
    ti = SimpleNamespace(task_id="final_success_gate")

    with pytest.raises(mod.AirflowException, match=r"ingest_active_scope\[2\]=failed"):
        mod.enforce_terminal_gate(dag_run=dag_run, ti=ti)


@pytest.mark.unit
def test_dag_uses_workflow_commands_durable_reports_and_bounded_tasks():
    mod = _load_dag_module()
    from airflow.operators.bash import BashOperator

    by_id = {task.task_id: task for task in BashOperator._instances}
    assert mod.dag._dag_kwargs["dagrun_timeout"].total_seconds() == 6 * 3600
    assert mod.dag._dag_kwargs["is_paused_upon_creation"] is True
    from airflow.operators.python import PythonOperator

    slo = next(
        task
        for task in PythonOperator._instances
        if task.task_id == "validate_whoscored_daily_slo"
    )
    assert slo._init_kwargs["pool"] == mod.DQ_POOL
    profile_capacity = next(
        task
        for task in PythonOperator._instances
        if task.task_id == "plan_daily_profile_capacity"
    )
    profile_dq = next(
        task
        for task in PythonOperator._instances
        if task.task_id == "validate_profile_refresh"
    )
    assert profile_capacity._init_kwargs["pool"] == mod.DQ_POOL
    assert profile_dq._init_kwargs["pool"] == mod.DQ_POOL
    alert_preflight = next(
        task
        for task in PythonOperator._instances
        if task.task_id == "validate_whoscored_paid_alert_delivery"
    )
    assert alert_preflight._init_kwargs["retries"] == 0
    for builder_task_id in (
        "build_whoscored_discovery_command",
        "build_active_scope_commands",
        "build_daily_profile_command",
    ):
        builder = next(
            task
            for task in PythonOperator._instances
            if task.task_id == builder_task_id
        )
        alert_argument = builder._init_kwargs["op_kwargs"]["alert_metadata"]
        assert alert_argument.operator.task_id == alert_preflight.task_id
    assert set(by_id) == {
        "discover_whoscored_catalog",
        "ingest_active_scope",
        "refresh_whoscored_profiles",
    }
    assert (
        "build_whoscored_discovery_command"
        in by_id["discover_whoscored_catalog"].bash_command
    )
    mapped = by_id["ingest_active_scope"]
    assert mapped.is_mapped is True
    assert (
        mapped._expand_kwargs["bash_command"].operator.task_id
        == "build_active_scope_commands"
    )
    assert (
        "build_daily_profile_command"
        in by_id["refresh_whoscored_profiles"].bash_command
    )
    for task in by_id.values():
        assert task._init_kwargs["append_env"] is True
        assert task._init_kwargs["pool"] == mod.DIRECT_POOL
        assert task._init_kwargs["do_xcom_push"] is False
        assert "WHOSCORED_PAID_PROXY_URL" not in task.env
        assert "WHOSCORED_REQUEST_LEDGER_PATH" in task.env
        assert task.env["AIRFLOW_CTX_MAP_INDEX"] == "{{ ti.map_index }}"
    # Transport handles bounded request retries and manifests own entity
    # retry_after. Airflow retries before those deadlines could return an
    # empty success and hide the original failure.
    assert mapped._init_kwargs["retries"] == 0
    assert "retry_delay" not in mapped._init_kwargs
    assert by_id["refresh_whoscored_profiles"]._init_kwargs["retries"] == 0
    assert "retry_delay" not in by_id["refresh_whoscored_profiles"]._init_kwargs
