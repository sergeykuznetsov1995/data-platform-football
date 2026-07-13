"""Contracts for the canonical WhoScored service runner."""

from __future__ import annotations

import json
import os
from types import SimpleNamespace

import pytest

from dags.scripts import run_whoscored_scraper as runner


def _result(
    entity: str,
    *,
    counts=None,
    tables=None,
    errors=None,
    retryable=None,
):
    return SimpleNamespace(
        entity=entity,
        counts=dict(counts or {}),
        tables=list(tables or []),
        errors=list(errors or []),
        retryable=list(retryable or []),
    )


DEFAULT_RESULTS = {
    "schedule": _result(
        "schedule",
        counts={"schedule": 1},
        tables=["iceberg.bronze.whoscored_schedule"],
    ),
    "previews": _result(
        "previews",
        counts={"missing_players": 0},
        tables=[
            "iceberg.bronze.whoscored_missing_players",
            "iceberg.bronze.whoscored_preview_ingest_manifest",
        ],
    ),
    "matches": _result(
        "matches",
        counts={"events": 2, "lineups": 1},
        tables=[
            "iceberg.bronze.whoscored_events",
            "iceberg.bronze.whoscored_lineups",
            "iceberg.bronze.whoscored_match_ingest_manifest",
        ],
    ),
    "profiles": _result(
        "profiles",
        counts={"player_profile": 1},
        tables=[
            "iceberg.bronze.whoscored_player_profile_versions",
            "iceberg.bronze.whoscored_profile_ingest_manifest",
        ],
    ),
}


class _Catalog:
    def __init__(self):
        self._scopes = {
            ("ENG-Premier League", "2526"): SimpleNamespace(
                scope=SimpleNamespace(
                    competition_id="ENG-Premier League",
                    season_id="2526",
                    spec="ENG-Premier League=2526",
                )
            ),
            ("INT-World Cup", "2026"): SimpleNamespace(
                scope=SimpleNamespace(
                    competition_id="INT-World Cup",
                    season_id="2026",
                    spec="INT-World Cup=2026",
                )
            ),
        }

    def resolve_scope(self, competition_id, season_id):
        try:
            return self._scopes[(competition_id, str(season_id))]
        except KeyError as exc:
            raise ValueError(f"unknown scope {competition_id}={season_id}") from exc

    def competition(self, competition_id):
        if not any(key[0] == competition_id for key in self._scopes):
            raise ValueError(f"unknown competition {competition_id}")
        return SimpleNamespace(whoscored_enabled=True)


class _Repository:
    def __init__(self, catalog, candidate_ids=None):
        self.catalog = catalog
        self.candidate_ids = list(candidate_ids or [])
        self.catalog_calls = []
        self.include_failed_calls = []
        self.ensure_schema_calls = 0

    def ensure_schema(self):
        self.ensure_schema_calls += 1

    def list_catalog_scopes(self, *, active_only, include_quarantined=False):
        self.catalog_calls.append((active_only, include_quarantined))
        return list(self.catalog._scopes.values())

    def list_match_candidates(
        self,
        _competition_id,
        _season_id,
        *,
        match_ids,
        limit,
        include_failed,
    ):
        self.include_failed_calls.append(bool(include_failed))
        ids = list(self.candidate_ids)
        if match_ids is not None:
            selected = {int(value) for value in match_ids}
            ids = [value for value in ids if value in selected]
        if limit is not None:
            ids = ids[:limit]
        return [
            SimpleNamespace(
                game_id=value,
                kickoff=__import__("datetime").datetime(2025, 8, 1),
            )
            for value in ids
        ]


def _runtime(behaviors=None, *, candidate_ids=None):
    catalog = _Catalog()
    repository = _Repository(catalog, candidate_ids=candidate_ids)
    configured = dict(behaviors or {})

    class Service:
        instances = []
        discovery_calls = []
        discovery_paid_urls = []

        def __init__(self, scope, *, repository):
            self.scope = scope
            self.repository = repository
            self.calls = []
            self.match_force_replays = []
            self.preview_force_replays = []
            type(self).instances.append(self)

        @classmethod
        def discover_catalog(cls, *, repository, full_history):
            cls.discovery_calls.append((repository, full_history))
            cls.discovery_paid_urls.append(os.environ.get("WHOSCORED_PAID_PROXY_URL"))
            return _result(
                "catalog",
                counts={"competitions": 433, "seasons": 1000},
                tables=[
                    "iceberg.bronze.whoscored_competitions",
                    "iceberg.bronze.whoscored_seasons",
                ],
            )

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def _value(self, operation):
            value = configured.get(operation, DEFAULT_RESULTS[operation])
            if isinstance(value, BaseException):
                raise value
            return value() if callable(value) else value

        def sync_schedule(self):
            self.calls.append(("schedule", None))
            return self._value("schedule")

        def sync_previews(self, *, match_ids, force_replay):
            self.preview_force_replays.append(bool(force_replay))
            self.calls.append(("previews", None))
            return self._value("previews")

        def sync_matches(self, *, match_ids, limit, force_replay, kickoff_from=None):
            self.match_force_replays.append(bool(force_replay))
            if kickoff_from is not None:
                assert kickoff_from.tzinfo is not None
            call = ("matches", limit)
            if match_ids is not None:
                call = (*call, tuple(match_ids))
            self.calls.append(call)
            return self._value("matches")

        def sync_profiles(self, *, limit, candidate_scopes):
            self.calls.append(("profiles", limit, tuple(candidate_scopes)))
            return self._value("profiles")

        def traffic_stats(self):
            return {
                "paid_proxy_bytes": 0,
                "route_requests": {"direct_http": 1},
            }

    return Service, catalog, repository


def _run(monkeypatch, tmp_path, args, *, behaviors=None, candidate_ids=None):
    monkeypatch.delenv("WHOSCORED_SCHEMA_READY", raising=False)
    runtime, catalog, repository = _runtime(behaviors, candidate_ids=candidate_ids)
    monkeypatch.setattr(runner, "_load_runtime", lambda: runtime)
    monkeypatch.setattr(runner, "_new_repository", lambda: repository)
    output = tmp_path / "result.json"
    rc = runner.main([*args, "--output", str(output)])
    return rc, json.loads(output.read_text(encoding="utf-8")), runtime, catalog


@pytest.mark.unit
def test_report_is_group_readable_after_atomic_publish(tmp_path):
    output = tmp_path / "result.json"

    runner._write_report(str(output), {"status": "success"})

    assert output.stat().st_mode & 0o777 == 0o640
    assert json.loads(output.read_text(encoding="utf-8")) == {"status": "success"}


@pytest.mark.unit
def test_replay_uses_direct_service_and_emits_stable_v2_report(monkeypatch, tmp_path):
    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "replay",
            "--scope",
            "ENG-Premier League=2526",
            "--game-id",
            "123",
        ],
    )

    assert rc == 0
    assert len(service_cls.instances) == 1
    assert service_cls.instances[0].calls == [("matches", None, (123,))]
    assert report["schema_version"] == 3
    assert report["status"] == "success"
    assert report["command"] == "replay"
    assert report["rows"] == 3
    assert report["row_counts_complete"] is True
    assert report["entities"]["events"] == {
        "table": "iceberg.bronze.whoscored_events",
        "rows_written": 2,
        "counts_complete": True,
    }
    assert report["tables_by_entity"]["lineups"].endswith(".whoscored_lineups")
    assert "iceberg.bronze.whoscored_match_ingest_manifest" in report["tables"]
    assert report["traffic"]["paid_proxy_bytes"] == 0


@pytest.mark.unit
def test_repeated_scopes_create_independent_services_not_a_cross_product(
    monkeypatch, tmp_path
):
    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "replay",
            "--scope",
            "ENG-Premier League=2526",
            "--scope",
            "INT-World Cup=2026",
            "--game-id",
            "123",
        ],
    )

    assert rc == 0
    assert [item.scope.scope.spec for item in service_cls.instances] == [
        "ENG-Premier League=2526",
        "INT-World Cup=2026",
    ]
    assert [item.calls for item in service_cls.instances] == [
        [("matches", None, (123,))],
        [("matches", None, (123,))],
    ]
    assert [item["status"] for item in report["scopes"]] == [
        "success",
        "success",
    ]


@pytest.mark.unit
def test_daily_runs_each_v2_entity_once_in_order(monkeypatch, tmp_path):
    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "daily",
            "--scope",
            "ENG-Premier League=2526",
            "--skip-profiles",
        ],
    )

    assert rc == 0
    assert service_cls.instances[0].calls == [
        ("schedule", None),
        ("previews", None),
        ("matches", 100),
    ]
    assert report["rows"] == 4
    assert report["scopes"][0]["entities"]["missing_players"]["rows_written"] == 0


@pytest.mark.unit
def test_daily_scope_stops_after_schedule_failure(monkeypatch, tmp_path):
    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "daily",
            "--scope",
            "ENG-Premier League=2526",
            "--skip-profiles",
        ],
        behaviors={"schedule": RuntimeError("schedule parse failed")},
    )

    assert rc == 1
    assert report["status"] == "failed"
    assert service_cls.instances[0].calls == [("schedule", None)]


@pytest.mark.unit
def test_retryable_service_result_has_distinct_exit_code(monkeypatch, tmp_path):
    retryable = _result("matches", retryable=["123"])
    rc, report, _, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "replay",
            "--scope",
            "ENG-Premier League=2526",
            "--game-id",
            "123",
        ],
        behaviors={"matches": retryable},
    )

    assert rc == 2
    assert report["status"] == "retryable"
    assert report["scopes"][0]["status"] == "retryable"
    assert report["error_details"] == [
        {
            "scope": "ENG-Premier League=2526",
            "entity": "matches",
            "type": "RetryableWork",
            "message": "matches retryable ids: 123",
            "retryable": True,
        }
    ]


@pytest.mark.unit
def test_partial_service_error_is_fatal_but_keeps_committed_counts(
    monkeypatch, tmp_path
):
    partial = _result(
        "matches",
        counts={"events": 2},
        tables=["iceberg.bronze.whoscored_events"],
        errors=["lineup manifest failed"],
    )
    rc, report, _, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "replay",
            "--scope",
            "ENG-Premier League=2526",
            "--game-id",
            "123",
        ],
        behaviors={"matches": partial},
    )

    assert rc == 1
    assert report["status"] == "failed"
    assert report["rows"] == 2
    assert report["entities"]["events"]["rows_written"] == 2
    assert report["error_details"][0]["retryable"] is False


@pytest.mark.unit
def test_timeout_exception_is_retryable(monkeypatch, tmp_path):
    rc, report, _, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "replay",
            "--scope",
            "ENG-Premier League=2526",
            "--game-id",
            "123",
        ],
        behaviors={"matches": TimeoutError("upstream timeout")},
    )

    assert rc == 2
    assert report["error_details"][0]["type"] == "TimeoutError"
    assert report["error_details"][0]["entity"] == "matches"


@pytest.mark.unit
def test_unknown_catalog_scope_fails_closed_without_constructing_service(
    monkeypatch, tmp_path
):
    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "replay",
            "--scope",
            "UNKNOWN-League=2526",
            "--game-id",
            "123",
        ],
    )

    assert rc == 1
    assert service_cls.instances == []
    assert report["scopes"][0]["status"] == "failed"
    assert report["error_details"][0]["entity"] == "scope"


@pytest.mark.unit
def test_runtime_import_failure_still_publishes_failure_report(monkeypatch, tmp_path):
    def fail_runtime():
        raise ImportError("pyarrow missing")

    monkeypatch.setattr(runner, "_load_runtime", fail_runtime)
    output = tmp_path / "result.json"
    rc = runner.main(
        [
            "replay",
            "--scope",
            "ENG-Premier League=2526",
            "--game-id",
            "123",
            "--output",
            str(output),
        ]
    )

    report = json.loads(output.read_text(encoding="utf-8"))
    assert rc == 1
    assert report["status"] == "failed"
    assert report["error_details"][0]["entity"] == "runtime"


@pytest.mark.unit
def test_removed_entity_cli_commands_are_rejected():
    parser = runner._build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["matches", "--scope", "ENG-Premier League=2526"])
    with pytest.raises(SystemExit):
        parser.parse_args(["profiles", "--scope", "ENG-Premier League=2526"])


@pytest.mark.unit
def test_duplicate_or_malformed_scopes_are_rejected():
    parser = runner._build_parser()
    with pytest.raises(SystemExit):
        runner._resolve_scopes(
            parser,
            ["ENG-Premier League=2526", "ENG-Premier League=2526"],
        )
    with pytest.raises(SystemExit):
        runner._resolve_scopes(parser, ["ENG-Premier League=2025-26"])
    with pytest.raises(SystemExit):
        runner._resolve_scopes(parser, ["WS-11-605=2021-single-ws0"])

    resolved = runner._resolve_scopes(parser, ["WS-11-605=2021-single-ws8534"])
    assert resolved[0].spec == "WS-11-605=2021-single-ws8534"


@pytest.mark.unit
def test_discover_uses_scope_free_service_boundary(monkeypatch, tmp_path):
    rc, report, service_cls, _ = _run(monkeypatch, tmp_path, ["discover"])

    assert rc == 0
    assert report["status"] == "success"
    assert report["command"] == "discover"
    assert report["scopes"] == []
    assert report["entities"]["competitions"]["rows_written"] == 433
    assert len(service_cls.discovery_calls) == 1
    assert service_cls.discovery_calls[0][0].ensure_schema_calls == 1
    assert service_cls.instances == []


@pytest.mark.unit
def test_direct_only_removes_paid_endpoint_before_transport_construction(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("WHOSCORED_PAID_PROXY_URL", "http://paid-secret")

    rc, report, service_cls, _ = _run(
        monkeypatch, tmp_path, ["discover", "--direct-only"]
    )

    assert rc == 0
    assert report["direct_only"] is True
    assert service_cls.discovery_paid_urls == [""]


@pytest.mark.unit
def test_explicit_discovery_can_expand_the_historical_stage_catalog(
    monkeypatch, tmp_path
):
    rc, _, service_cls, _ = _run(monkeypatch, tmp_path, ["discover", "--full-history"])

    assert rc == 0
    assert service_cls.discovery_calls[0][1] is True


@pytest.mark.unit
def test_daily_without_scope_reads_all_active_persisted_scopes(monkeypatch, tmp_path):
    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        ["daily", "--skip-profiles"],
    )

    assert rc == 0
    assert [item["scope"] for item in report["scopes"]] == [
        "ENG-Premier League=2526",
        "INT-World Cup=2026",
    ]
    assert len(service_cls.instances) == 2
    assert all(
        service.calls
        == [
            ("schedule", None),
            ("previews", None),
            ("matches", 100),
        ]
        for service in service_cls.instances
    )


@pytest.mark.unit
def test_daily_profiles_only_uses_one_global_active_scope_union(monkeypatch, tmp_path):
    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        ["daily", "--profiles-only", "--profiles-limit", "17"],
    )

    assert rc == 0
    assert len(service_cls.instances) == 1
    call = service_cls.instances[0].calls[0]
    assert call[:2] == ("profiles", 17)
    assert [scope.scope.spec for scope in call[2]] == [
        "ENG-Premier League=2526",
        "INT-World Cup=2026",
    ]
    assert report["scopes"][1]["delegated_to"] == "ENG-Premier League=2526"


@pytest.mark.unit
def test_replay_passes_a_frozen_explicit_game_set(monkeypatch, tmp_path):
    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "replay",
            "--scope",
            "ENG-Premier League=2526",
            "--game-id",
            "20",
            "--game-id",
            "10",
        ],
    )

    assert rc == 0
    assert report["status"] == "success"
    assert service_cls.instances[0].calls == [("matches", None, (10, 20))]


@pytest.mark.unit
def test_backfill_freezes_and_checkpoints_25_match_chunks(monkeypatch, tmp_path):
    game_ids = list(range(1, 53))
    state_dir = tmp_path / "state"
    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "backfill",
            "--scope",
            "ENG-Premier League=2526",
            "--state-dir",
            str(state_dir),
            "--queue-id",
            "unit-queue",
        ],
        candidate_ids=game_ids,
    )

    assert rc == 0
    assert report["queue"] == {
        "queue_id": "unit-queue",
        "path": str(state_dir / "unit-queue.json"),
        "status": "complete",
        "chunk_size": 25,
        "pending_matches": 0,
        "completed_matches": 52,
        "completed_profiles": 1,
        "pending_profile_scopes": 0,
    }
    match_calls = [
        call
        for service in service_cls.instances
        for call in service.calls
        if call[0] == "matches"
    ]
    assert [len(call[2]) for call in match_calls] == [25, 25, 2]
    assert all(
        service.match_force_replays == [True]
        for service in service_cls.instances
        if any(call[0] == "matches" for call in service.calls)
    )
    assert all(
        service.preview_force_replays == [True]
        for service in service_cls.instances
        if any(call[0] == "matches" for call in service.calls)
    )
    planning_repository = next(
        service.repository
        for service in service_cls.instances
        if any(call[0] == "schedule" for call in service.calls)
    )
    assert planning_repository.include_failed_calls == [True]
    assert planning_repository.ensure_schema_calls == 1
    queue = json.loads((state_dir / "unit-queue.json").read_text(encoding="utf-8"))
    assert queue["status"] == "complete"
    assert queue["scopes"][0]["pending_game_ids"] == []


@pytest.mark.unit
def test_backfill_retry_keeps_the_same_pending_chunk(monkeypatch, tmp_path):
    retryable = _result("matches", retryable=["1"])
    state_dir = tmp_path / "state"
    rc, report, _, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "backfill",
            "--scope",
            "ENG-Premier League=2526",
            "--state-dir",
            str(state_dir),
            "--queue-id",
            "retry-queue",
        ],
        behaviors={"matches": retryable},
        candidate_ids=[1, 2, 3],
    )

    assert rc == 2
    assert report["status"] == "retryable"
    assert report["queue"]["pending_matches"] == 3
    queue = json.loads((state_dir / "retry-queue.json").read_text(encoding="utf-8"))
    assert queue["scopes"][0]["pending_game_ids"] == [1, 2, 3]
    assert queue["scopes"][0]["blocked_until"] is not None


@pytest.mark.unit
def test_workflow_command_selector_contracts_are_fail_closed():
    parser = runner._build_parser()
    with pytest.raises(SystemExit):
        args = parser.parse_args(["backfill"])
        runner._validate_args(parser, args)
    with pytest.raises(SystemExit):
        args = parser.parse_args(["replay", "--scope", "ENG-Premier League=2526"])
        runner._validate_args(parser, args)
    with pytest.raises(SystemExit):
        args = parser.parse_args(["discover", "--game-id", "1"])
        runner._validate_args(parser, args)
    args = parser.parse_args(["backfill", "--all-catalog"])
    assert runner._validate_args(parser, args) == []
    with pytest.raises(SystemExit):
        args = parser.parse_args(
            [
                "backfill",
                "--all-catalog",
                "--scope",
                "ENG-Premier League=2526",
            ]
        )
        runner._validate_args(parser, args)
