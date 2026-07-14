"""Production contracts for the dynamic WhoScored daily DAG."""

from __future__ import annotations

import importlib
import hashlib
import json
import os
import sys
import types
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from scrapers.whoscored.repository import (
    ProfileCandidateCapacityExceeded,
    ProfileCandidateSnapshot,
)


@pytest.fixture(autouse=True)
def _clean_operator_registries():
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    yield


def _load_dag_module():
    sys.modules.pop("dag_ingest_whoscored", None)
    sys.modules.pop("dags.dag_ingest_whoscored", None)
    return importlib.import_module("dag_ingest_whoscored")


def _context(run_id="scheduled__2026-07-11T10:00:00+00:00"):
    return {
        "dag": SimpleNamespace(dag_id="dag_ingest_whoscored"),
        "run_id": run_id,
        "params": {"require_zero_paid": True},
    }


def _scope_plan(scopes=None):
    values = sorted(scopes or ["WS-252-2=2526"])
    return {
        "schema_version": 1,
        "catalog_batch_id": "wsc2-test-generation",
        "catalog_payload_sha256": "a" * 64,
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

    catalog = object()
    repository = SimpleNamespace(
        load_catalog_generation_snapshot=lambda: (
            {
                "catalog_batch_id": "wsc2-generation",
                "catalog_payload_sha256": "b" * 64,
            },
            catalog,
        )
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

    plan = mod.freeze_daily_scope_plan()

    assert plan["catalog_batch_id"] == "wsc2-generation"
    assert plan["active_scopes"] == ["WS-1=2026", "WS-2=2026"]
    assert mod._daily_scope_plan_specs(plan) == (
        "wsc2-generation",
        ["WS-1=2026", "WS-2=2026"],
    )


@pytest.mark.unit
def test_runtime_builder_can_make_a_structural_direct_only_canary(monkeypatch):
    mod = _load_dag_module()
    monkeypatch.setattr(mod, "_active_scope_specs", lambda: ["WS-252-2=2526"])
    context = _context()
    context["params"]["direct_only"] = True

    assert "--direct-only" in mod.build_daily_commands(**context)[0]


@pytest.mark.unit
def test_runtime_preflight_requires_local_executor_and_forbids_paid_override(
    monkeypatch,
):
    mod = _load_dag_module()
    monkeypatch.setenv("AIRFLOW__CORE__EXECUTOR", "CeleryExecutor")
    with pytest.raises(mod.AirflowException, match="requires.*LocalExecutor"):
        mod.validate_whoscored_runtime(params={})

    monkeypatch.setenv("AIRFLOW__CORE__EXECUTOR", "LocalExecutor")
    with pytest.raises(mod.AirflowException, match="permanently direct-only"):
        mod.validate_whoscored_runtime(params={"direct_only": False})
    assert mod.validate_whoscored_runtime(params={})["direct_only"] is True


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

    import utils.silver_tasks as silver_tasks

    monkeypatch.setattr(silver_tasks, "_get_trino_connection", lambda: _Connection())

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
    monkeypatch.setattr(
        mod,
        "_load_result",
        lambda _path: {
            "schema_version": 3,
            "status": "success",
            "paid_proxy_bytes": 0,
            "scopes": [{"scope": "WS-252-2=2526"}],
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
    monkeypatch.setattr(mod, "_scope_integrity_summary", lambda _scope: integrity)

    result = mod.validate_scope_result(scope_spec="WS-252-2=2526", result_path="unused")

    assert result == {
        "scope": "WS-252-2=2526",
        "paid_proxy_bytes": 0,
        **integrity,
    }


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
    monkeypatch.setattr(
        mod,
        "_load_result",
        lambda _path: {
            "status": "success",
            "paid_proxy_bytes": 0,
            "scopes": [{"scope": "WS-252-2=2526"}],
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
    monkeypatch.setattr(
        mod,
        "_load_result",
        lambda _path: {"status": "success", "paid_proxy_bytes": 0},
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
def test_profile_validation_requires_the_planned_candidate_identity(monkeypatch):
    mod = _load_dag_module()
    plan = _scope_plan()
    capacity = _profile_capacity(count=2, payload_sha256="c" * 64)
    report = {
        "status": "success",
        "paid_proxy_bytes": 0,
        "catalog_batch_id": "wsc2-test-generation",
        "profile_candidates": {
            "schema_version": 1,
            "count": 2,
            "payload_sha256": "c" * 64,
            "attempted": 2,
        },
    }
    monkeypatch.setattr(mod, "_load_result", lambda _path: report)
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

    assert mod.validate_profile_result(
        scope_plan=plan,
        capacity=capacity,
    )["status"] == "success"

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

    import utils.silver_tasks as silver_tasks

    monkeypatch.setattr(silver_tasks, "_get_trino_connection", Connection)
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
        lambda _path: {"status": "success", "errors": []},
    )
    monkeypatch.setattr(mod, "_active_scope_specs", lambda: ["WS-252-2=2526"])
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
        "manifest_identity_valid": 1,
    }
    monkeypatch.setattr(mod, "_catalog_integrity_summary", lambda: complete)

    assert mod.validate_catalog_result()["manifest_competitions"] == 433

    monkeypatch.setattr(
        mod,
        "_catalog_integrity_summary",
        lambda: {**complete, "eligible_seasons_without_stages": 1},
    )
    with pytest.raises(mod.AirflowException, match="catalog is incomplete"):
        mod.validate_catalog_result()


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
def test_traffic_aggregation_enforces_one_dagrun_budget(monkeypatch, tmp_path):
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

    with pytest.raises(mod.AirflowException, match="budget exceeded"):
        mod.aggregate_traffic_reports(
            **{**_context(), "params": {"require_zero_paid": False}}
        )


@pytest.mark.unit
def test_traffic_aggregation_streams_retry_ledgers_with_exact_url_and_task(
    monkeypatch, tmp_path
):
    mod = _load_dag_module()
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    paid_ledger = tmp_path / "paid.jsonl"
    monkeypatch.setattr(mod, "PAID_LEDGER_PATH", str(paid_ledger))
    context = {**_context(), "params": {"require_zero_paid": False}}
    run_dir = mod._run_dir_from_context(context)
    run_dir.mkdir(parents=True)
    (run_dir / "scope.json").write_text(
        json.dumps(
            {
                "schema_version": 3,
                "paid_proxy_bytes": 0,
                "traffic": {},
                "airflow": {
                    "task_id": "ingest_active_scope",
                    "map_index": 4,
                    "try_number": 2,
                },
            }
        )
    )
    canonical = "https://www.whoscored.com/Matches/1/Live?a=1&z=2"
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
    assert result["cleaned_local_files"] == 2
    assert not (run_dir / "scope.json").exists()
    assert not (run_dir / "requests_ingest_4_try1.jsonl").exists()
    # A retry after cleanup resolves the immutable completion receipt and does
    # not require the deleted 31MB-class local staging files.
    assert mod.aggregate_traffic_reports(**context) == result


@pytest.mark.unit
def test_traffic_aggregation_enforces_full_url_paid_limit(monkeypatch, tmp_path):
    mod = _load_dag_module()
    monkeypatch.setattr(mod, "RUN_ROOT", str(tmp_path / "runs"))
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    paid_ledger = tmp_path / "paid.jsonl"
    monkeypatch.setattr(mod, "PAID_LEDGER_PATH", str(paid_ledger))
    context = {**_context(), "params": {"require_zero_paid": False}}
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
                "bytes": mod.PAID_URL_LIMIT_BYTES + 1,
            }
        )
        + "\n"
    )

    with pytest.raises(mod.AirflowException, match="URL budget exceeded"):
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
    assert set(by_id) == {
        "discover_whoscored_catalog",
        "ingest_active_scope",
        "refresh_whoscored_profiles",
    }
    assert (
        "run_whoscored_scraper.py discover"
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
        if (
            task.bash_command is not None
            and task.task_id != "refresh_whoscored_profiles"
        ):
            assert "/opt/airflow/logs/whoscored_runs/" in task.bash_command
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
