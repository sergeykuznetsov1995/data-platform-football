"""Contracts for the canonical WhoScored service runner."""

from __future__ import annotations

import hashlib
import json
import os
from types import SimpleNamespace

import pytest

from dags.scripts import run_whoscored_scraper as runner
from scrapers.whoscored.repository import profile_candidate_payload_sha256


DISCOVERY_IDENTITY = {
    "catalog_batch_id": "wsc2-test-discovery",
    "catalog_payload_sha256": "1" * 64,
    "catalog_raw_provenance_sha256": "2" * 64,
    "technical_exclusion_audit_sha256": "3" * 64,
    "catalog_as_of_date": "2026-07-11",
    "parent_catalog_batch_id": None,
    "parent_catalog_payload_sha256": None,
    "parent_catalog_raw_provenance_sha256": None,
}


def _result(
    entity: str,
    *,
    scope="test",
    counts=None,
    tables=None,
    errors=None,
    retryable=None,
    metadata=None,
    committed_batches=None,
    attempted_snapshots=None,
):
    return SimpleNamespace(
        entity=entity,
        scope=scope,
        counts=dict(counts or {}),
        tables=list(tables or []),
        errors=list(errors or []),
        retryable=list(retryable or []),
        metadata=dict(metadata or {}),
        committed_batches={
            str(kind): list(batch_ids)
            for kind, batch_ids in (committed_batches or {}).items()
        },
        attempted_snapshots={
            str(kind): dict(snapshot)
            for kind, snapshot in (attempted_snapshots or {}).items()
        },
    )


DEFAULT_RESULTS = {
    "schedule": _result(
        "schedule",
        counts={"schedule": 1},
        tables=["iceberg.bronze.whoscored_schedule"],
        metadata={"source_stage_ids": [23752], "source_stage_count": 1},
        committed_batches={"scope": ["wss2-" + "1" * 64]},
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
                stage_ids=(23752,),
                scope=SimpleNamespace(
                    competition_id="ENG-Premier League",
                    season_id="2526",
                    spec="ENG-Premier League=2526",
                ),
            ),
            ("INT-World Cup", "2026"): SimpleNamespace(
                stage_ids=tuple(range(23752, 23765)),
                scope=SimpleNamespace(
                    competition_id="INT-World Cup",
                    season_id="2026",
                    spec="INT-World Cup=2026",
                ),
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

    def eligible_scopes(self, *, active_only):
        del active_only
        return list(self._scopes.values())


class _Repository:
    def __init__(self, catalog, candidate_ids=None, profile_candidate_ids=None):
        self.catalog = catalog
        self.candidate_ids = list(candidate_ids or [])
        self.profile_candidate_ids = sorted(set(profile_candidate_ids or []))
        self.catalog_calls = []
        self.all_completed_calls = []
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

    def list_completed_match_candidates(
        self,
        competition_id,
        season_id,
        *,
        match_ids=None,
    ):
        self.all_completed_calls.append((competition_id, season_id, match_ids))
        return self.list_match_candidates(
            competition_id,
            season_id,
            match_ids=match_ids,
            limit=None,
            include_failed=True,
        )

    def list_roster_player_ids(self, *, scopes):
        assert len(scopes) == 1
        return [1001]

    def profile_candidate_snapshot(self, *, scopes, hard_cap):
        assert scopes
        assert hard_cap == 3_000
        return SimpleNamespace(
            player_ids=tuple(self.profile_candidate_ids),
            count=len(self.profile_candidate_ids),
            payload_sha256=profile_candidate_payload_sha256(self.profile_candidate_ids),
        )

    def list_preview_candidates(
        self,
        _competition_id,
        _season_id,
        *,
        match_ids,
        force_replay,
    ):
        assert force_replay is True
        return [{"game_id": int(value)} for value in (match_ids or [])]

    def latest_catalog_generation(self):
        return {
            "catalog_batch_id": "wsc2-test-generation",
            "catalog_payload_sha256": "a" * 64,
            "catalog_discovery_mode": "incremental",
        }

    def load_discovered_catalog(self, *, batch_id=None):
        if batch_id is None:
            raise AssertionError("mutable latest catalog must not be reloaded")
        assert batch_id == "wsc2-test-generation"
        return self.catalog

    def load_catalog_generation_snapshot(self, *, batch_id=None):
        if batch_id is not None:
            assert batch_id == "wsc2-test-generation"
        return self.latest_catalog_generation(), self.catalog


def _runtime(behaviors=None, *, candidate_ids=None, profile_candidate_ids=None):
    catalog = _Catalog()
    repository = _Repository(
        catalog,
        candidate_ids=candidate_ids,
        profile_candidate_ids=profile_candidate_ids,
    )
    configured = dict(behaviors or {})

    class Service:
        instances = []
        discovery_calls = []
        discovery_transport_environments = []

        def __init__(self, scope, *, catalog=None, repository):
            if catalog is None:
                catalog = repository.load_discovered_catalog()
            self.scope = scope
            self.catalog = catalog
            self.repository = repository
            self.calls = []
            self.match_force_replays = []
            self.preview_force_replays = []
            type(self).instances.append(self)

        @classmethod
        def discover_catalog(cls, *, repository, full_history, as_of_date):
            cls.discovery_calls.append((repository, full_history, as_of_date))
            cls.discovery_transport_environments.append(
                {
                    name: os.environ.get(name)
                    for name in (
                        "WHOSCORED_PAID_GATEWAY_URL",
                        "WHOSCORED_PAID_GATEWAY_TOKEN",
                        "WHOSCORED_PAID_PROXY_URL",
                        "WHOSCORED_PROXY_CONTROL_URL",
                        "WHOSCORED_PROXY_CONTROL_TOKEN",
                        "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET",
                        "WHOSCORED_PAID_ALERT_HMAC_SECRET",
                    )
                }
            )
            return _result(
                "catalog",
                counts={"competitions": 433, "seasons": 1000},
                tables=[
                    "iceberg.bronze.whoscored_competitions",
                    "iceberg.bronze.whoscored_seasons",
                ],
                metadata=DISCOVERY_IDENTITY,
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

        def _scope_spec(self):
            return (
                self.scope.scope.spec
                if hasattr(self.scope, "scope")
                else self.scope.spec
            )

        def sync_schedule(self):
            self.calls.append(("schedule", None))
            result = SimpleNamespace(**vars(self._value("schedule")))
            if "schedule" not in configured:
                digest = hashlib.sha256(
                    f"scope\0{self._scope_spec()}".encode("utf-8")
                ).hexdigest()
                result.committed_batches = {"scope": ["wss2-" + digest]}
            return result

        def sync_previews(self, *, match_ids, force_replay):
            self.preview_force_replays.append(bool(force_replay))
            self.calls.append(("previews", None))
            result = SimpleNamespace(**vars(self._value("previews")))
            result.attempted = len(match_ids or [])
            result.succeeded = len(match_ids or []) if not result.retryable else 0
            result.terminal = list(getattr(result, "terminal", []))
            result.scope = self._scope_spec()
            result.attempted_snapshots = {
                "preview": {
                    "schema_version": 1,
                    "count": len(match_ids or []),
                    "payload_sha256": profile_candidate_payload_sha256(
                        [int(value) for value in (match_ids or [])]
                    ),
                }
            }
            if not result.committed_batches:
                result.committed_batches = {
                    "preview": [
                        "wsp2-v3-"
                        + hashlib.sha256(
                            f"{self._scope_spec()}\0{int(game_id)}".encode("utf-8")
                        ).hexdigest()
                        for game_id in (match_ids or [])
                    ]
                }
            return result

        def sync_matches(
            self,
            *,
            match_ids,
            limit,
            force_replay,
            historical_replay=False,
            kickoff_from=None,
        ):
            self.match_force_replays.append(bool(force_replay))
            if historical_replay:
                assert force_replay is True
            if kickoff_from is not None:
                assert kickoff_from.tzinfo is not None
            call = ("matches", limit)
            if match_ids is not None:
                call = (*call, tuple(match_ids))
            self.calls.append(call)
            result = SimpleNamespace(**vars(self._value("matches")))
            result.attempted = len(match_ids or [])
            result.succeeded = len(match_ids or []) if not result.retryable else 0
            result.terminal = list(getattr(result, "terminal", []))
            result.scope = self._scope_spec()
            result.attempted_snapshots = {
                "match": {
                    "schema_version": 1,
                    "count": len(match_ids or []),
                    "payload_sha256": profile_candidate_payload_sha256(
                        [int(value) for value in (match_ids or [])]
                    ),
                }
            }
            if not result.committed_batches:
                result.committed_batches = {
                    "match": [
                        "ws2-v3-"
                        + hashlib.sha256(
                            f"{self._scope_spec()}\0{int(game_id)}".encode("utf-8")
                        ).hexdigest()
                        for game_id in (match_ids or [])
                    ]
                }
            return result

        def sync_profiles(self, *, limit, candidate_scopes, player_ids=None):
            self.calls.append(
                ("profiles", limit, tuple(candidate_scopes), tuple(player_ids or ()))
            )
            result = SimpleNamespace(**vars(self._value("profiles")))
            result.attempted = len(player_ids or [])
            result.succeeded = len(player_ids or [])
            result.terminal = list(getattr(result, "terminal", []))
            result.scope = self._scope_spec()
            result.attempted_snapshots = {
                "profile": {
                    "schema_version": 1,
                    "count": len(player_ids or []),
                    "payload_sha256": profile_candidate_payload_sha256(
                        [int(value) for value in (player_ids or [])]
                    ),
                }
            }
            if not result.committed_batches:
                result.committed_batches = {
                    "profile": [
                        "wspr2-v3-" + f"{int(player_id):064x}"
                        for player_id in (player_ids or [])
                    ]
                }
            return result

        def traffic_stats(self):
            return {
                "paid_proxy_bytes": 0,
                "route_requests": {"direct_http": 1},
            }

    return Service, catalog, repository


def _run(
    monkeypatch,
    tmp_path,
    args,
    *,
    behaviors=None,
    candidate_ids=None,
    profile_candidate_ids=None,
):
    monkeypatch.delenv("WHOSCORED_SCHEMA_READY", raising=False)
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    runtime, catalog, repository = _runtime(
        behaviors,
        candidate_ids=candidate_ids,
        profile_candidate_ids=profile_candidate_ids,
    )
    monkeypatch.setattr(runner, "_load_runtime", lambda: runtime)
    monkeypatch.setattr(runner, "_new_repository", lambda: repository)
    monkeypatch.setattr(
        runner._WHOSCORED_RUNTIME_CONTRACT,
        "validate_runtime_contract",
        lambda **_kwargs: {},
    )
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
def test_unscoped_discovery_merge_copies_the_strict_catalog_identity():
    report = runner._new_report("discover", ())
    result = _result(
        "catalog",
        counts={"competitions": 1},
        tables=["iceberg.bronze.whoscored_competitions"],
        metadata=DISCOVERY_IDENTITY,
    )

    runner._merge_unscoped_result(report, result)

    for key, value in DISCOVERY_IDENTITY.items():
        assert report[key] == value

    malformed = _result(
        "catalog",
        metadata={**DISCOVERY_IDENTITY, "catalog_raw_provenance_sha256": "bad"},
    )
    with pytest.raises(ValueError, match="catalog_raw_provenance_sha256"):
        runner._merge_unscoped_result(runner._new_report("discover", ()), malformed)


@pytest.mark.unit
def test_report_keeps_the_exact_service_commit_identities():
    scope = runner.RunnerScope.parse("ENG-Premier League=2526")
    report = runner._new_report("daily", (scope,))
    result = _result(
        "matches",
        committed_batches={"match": ["ws2-v3-" + "a" * 64]},
    )

    runner._merge_result(report, result, scope_record=report["scopes"][0])

    assert report["producer_commits"] == {
        "schema_version": 1,
        "scope": [],
        "match": ["ws2-v3-" + "a" * 64],
        "match_not_available": [],
        "preview": [],
        "preview_not_available": [],
        "profile": [],
        "profile_not_available": [],
    }


@pytest.mark.unit
def test_report_surfaces_the_match_backlog_from_service_metadata():
    scope = runner.RunnerScope.parse("ENG-Premier League=2526")
    report = runner._new_report("daily", (scope,))
    assert report["match_candidates"] is None
    result = _result(
        "matches",
        metadata={
            "match_candidates": {
                "schema_version": 1,
                "count": 12,
                "attempted": 5,
                "remaining": 7,
            }
        },
    )

    runner._merge_result(report, result, scope_record=report["scopes"][0])

    assert report["match_candidates"] == {
        "schema_version": 1,
        "count": 12,
        "attempted": 5,
        "remaining": 7,
    }


@pytest.mark.unit
def test_report_accumulates_match_backlog_across_scopes():
    scope = runner.RunnerScope.parse("ENG-Premier League=2526")
    report = runner._new_report("daily", (scope,))
    first = _result(
        "matches",
        metadata={
            "match_candidates": {
                "schema_version": 1,
                "count": 20,
                "attempted": 5,
                "remaining": 15,
            }
        },
    )
    second = _result(
        "matches",
        metadata={
            "match_candidates": {
                "schema_version": 1,
                "count": 8,
                "attempted": 8,
                "remaining": 0,
            }
        },
    )

    runner._merge_result(report, first, scope_record=report["scopes"][0])
    runner._merge_result(report, second, scope_record=report["scopes"][0])

    # Whole-run backlog, not just the last scope.
    assert report["match_candidates"] == {
        "schema_version": 1,
        "count": 28,
        "attempted": 13,
        "remaining": 15,
    }


@pytest.mark.unit
def test_report_publishes_content_bound_attempt_sidecar_in_new_directory(tmp_path):
    output = tmp_path / "nested" / "result.json"
    report = {
        "schema_version": 3,
        "status": "retryable",
        "paid_proxy_bytes": 17,
        "airflow": {
            "dag_id": "dag_ingest_whoscored",
            "dag_run_id": "manual__retry-history",
            "task_id": "ingest_active_scope",
            "map_index": "4",
            "try_number": "1",
        },
    }

    runner._write_report(str(output), report)

    sidecars = list(output.parent.glob("result.json.attempt-*.json"))
    assert len(sidecars) == 1
    sidecar = sidecars[0]
    payload = sidecar.read_bytes()
    assert sidecar.name.endswith(f"-{hashlib.sha256(payload).hexdigest()}.json")
    assert json.loads(payload) == report
    assert output.read_bytes() == payload
    assert sidecar.stat().st_mode & 0o777 == 0o640


@pytest.mark.unit
def test_report_rejects_conflicting_or_tampered_attempt_sidecar(tmp_path):
    output = tmp_path / "result.json"
    report = {
        "schema_version": 3,
        "status": "retryable",
        "paid_proxy_bytes": 17,
        "airflow": {
            "dag_id": "dag_ingest_whoscored",
            "dag_run_id": "manual__retry-history",
            "task_id": "ingest_active_scope",
            "map_index": "4",
            "try_number": "1",
        },
    }
    runner._write_report(str(output), report)

    with pytest.raises(RuntimeError, match="conflicting immutable"):
        runner._write_report(str(output), {**report, "paid_proxy_bytes": 18})

    sidecar = next(output.parent.glob("result.json.attempt-*.json"))
    sidecar.write_text("{}", encoding="utf-8")
    with pytest.raises(RuntimeError, match="conflicting immutable"):
        runner._write_report(str(output), report)


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
def test_every_cli_process_rechecks_the_runtime_contract(monkeypatch, tmp_path):
    from scrapers.whoscored import runtime_contract

    def fail_contract(**_kwargs):
        raise runtime_contract.RuntimeContractError("worker tree changed")

    monkeypatch.setattr(runtime_contract, "validate_runtime_contract", fail_contract)
    monkeypatch.setattr(
        runner,
        "_load_runtime",
        lambda: pytest.fail("source runtime loaded before contract validation"),
    )
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
    assert "worker tree changed" in report["errors"][0]


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
    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        ["discover", "--as-of-date", "2026-07-11"],
    )

    assert rc == 0
    assert report["status"] == "success"
    assert report["command"] == "discover"
    assert report["scopes"] == []
    assert report["entities"]["competitions"]["rows_written"] == 433
    assert len(service_cls.discovery_calls) == 1
    assert service_cls.discovery_calls[0][0].ensure_schema_calls == 1
    assert service_cls.discovery_calls[0][2].isoformat() == "2026-07-11"
    for key, value in DISCOVERY_IDENTITY.items():
        assert report[key] == value
    assert service_cls.instances == []


@pytest.mark.unit
def test_direct_only_removes_all_paid_authority_before_transport_construction(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("WHOSCORED_PAID_GATEWAY_URL", "http://paid-gateway")
    monkeypatch.setenv("WHOSCORED_PAID_GATEWAY_TOKEN", "g" * 32)
    monkeypatch.setenv("WHOSCORED_PAID_PROXY_URL", "http://paid-secret")
    monkeypatch.setenv("WHOSCORED_PROXY_CONTROL_URL", "http://control-secret")
    monkeypatch.setenv("WHOSCORED_PROXY_CONTROL_TOKEN", "c" * 32)
    monkeypatch.setenv("WHOSCORED_PROXY_APPROVAL_HMAC_SECRET", "a" * 32)
    monkeypatch.setenv("WHOSCORED_PAID_ALERT_HMAC_SECRET", "h" * 32)

    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        ["discover", "--as-of-date", "2026-07-11", "--direct-only"],
    )

    assert rc == 0
    assert report["direct_only"] is True
    assert service_cls.discovery_transport_environments == [
        {
            "WHOSCORED_PAID_GATEWAY_URL": None,
            "WHOSCORED_PAID_GATEWAY_TOKEN": None,
            "WHOSCORED_PAID_PROXY_URL": None,
            "WHOSCORED_PROXY_CONTROL_URL": None,
            "WHOSCORED_PROXY_CONTROL_TOKEN": None,
            "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET": None,
            "WHOSCORED_PAID_ALERT_HMAC_SECRET": None,
        }
    ]


@pytest.mark.unit
def test_explicit_discovery_can_expand_the_historical_stage_catalog(
    monkeypatch, tmp_path
):
    rc, _, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        ["discover", "--full-history", "--as-of-date", "2026-07-11"],
    )

    assert rc == 0
    assert service_cls.discovery_calls[0][1] is True


@pytest.mark.unit
def test_daily_without_scope_reads_all_active_persisted_scopes(monkeypatch, tmp_path):
    monkeypatch.setattr(
        runner._WHOSCORED_RUNTIME_CONTRACT,
        "validate_runtime_contract",
        lambda **_kwargs: {},
    )
    rc, report, service_cls, catalog = _run(
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
    assert all(service.catalog is catalog for service in service_cls.instances)
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
    monkeypatch.setattr(
        runner._WHOSCORED_RUNTIME_CONTRACT,
        "validate_runtime_contract",
        lambda **_kwargs: {},
    )
    candidate_ids = list(range(1, 18))
    candidate_sha256 = profile_candidate_payload_sha256(candidate_ids)
    rc, report, service_cls, catalog = _run(
        monkeypatch,
        tmp_path,
        [
            "daily",
            "--profiles-only",
            "--profiles-limit",
            "17",
            "--expected-profile-candidate-count",
            "17",
            "--expected-profile-candidate-sha256",
            candidate_sha256,
        ],
        profile_candidate_ids=candidate_ids,
    )

    assert rc == 0
    assert len(service_cls.instances) == 1
    assert service_cls.instances[0].catalog is catalog
    call = service_cls.instances[0].calls[0]
    assert call[:2] == ("profiles", 17)
    assert [scope.scope.spec for scope in call[2]] == [
        "ENG-Premier League=2526",
        "INT-World Cup=2026",
    ]
    assert call[3] == tuple(candidate_ids)
    assert report["profile_candidates"] == {
        "schema_version": 1,
        "count": 17,
        "payload_sha256": candidate_sha256,
        "attempted": 17,
    }
    assert report["scopes"][1]["delegated_to"] == "ENG-Premier League=2526"


@pytest.mark.unit
def test_daily_zero_profile_snapshot_stays_explicit_and_reports_zero(
    monkeypatch, tmp_path
):
    candidate_sha256 = profile_candidate_payload_sha256([])

    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "daily",
            "--profiles-only",
            "--profiles-limit",
            "0",
            "--expected-profile-candidate-count",
            "0",
            "--expected-profile-candidate-sha256",
            candidate_sha256,
        ],
        profile_candidate_ids=[],
    )

    assert rc == 0
    assert service_cls.instances[0].calls[0][3] == ()
    assert report["profile_candidates"] == {
        "schema_version": 1,
        "count": 0,
        "payload_sha256": candidate_sha256,
        "attempted": 0,
    }


@pytest.mark.unit
def test_daily_profile_snapshot_drift_fails_before_source_transport(
    monkeypatch, tmp_path
):
    expected_sha256 = profile_candidate_payload_sha256([1])

    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "daily",
            "--profiles-only",
            "--profiles-limit",
            "1",
            "--expected-profile-candidate-count",
            "1",
            "--expected-profile-candidate-sha256",
            expected_sha256,
        ],
        profile_candidate_ids=[2],
    )

    assert rc == 1
    assert service_cls.instances == []
    assert report["paid_proxy_bytes"] == 0
    assert "changed before source work" in report["errors"][0]


@pytest.mark.unit
def test_daily_worker_loads_the_exact_catalog_generation(monkeypatch, tmp_path):
    monkeypatch.setattr(
        runner._WHOSCORED_RUNTIME_CONTRACT,
        "validate_runtime_contract",
        lambda **_kwargs: {},
    )
    rc, report, service_cls, catalog = _run(
        monkeypatch,
        tmp_path,
        [
            "daily",
            "--scope",
            "ENG-Premier League=2526",
            "--skip-profiles",
            "--catalog-batch-id",
            "wsc2-test-generation",
        ],
    )

    assert rc == 0
    assert report["catalog_batch_id"] == "wsc2-test-generation"
    assert len(service_cls.instances) == 1
    assert service_cls.instances[0].catalog is catalog
    assert report["scopes"][0]["scope"] == "ENG-Premier League=2526"


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
def test_backfill_freezes_s3_plan_and_receipts_for_25_match_chunks(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(
        runner._WHOSCORED_RUNTIME_CONTRACT,
        "validate_runtime_contract",
        lambda **_kwargs: {},
    )
    game_ids = list(range(1, 53))
    rc, report, service_cls, catalog = _run(
        monkeypatch,
        tmp_path,
        [
            "backfill",
            "--scope",
            "ENG-Premier League=2526",
            "--queue-id",
            "unit-queue",
        ],
        candidate_ids=game_ids,
    )

    assert rc == 0
    assert report["queue"]["queue_id"] == "unit-queue"
    assert report["queue"]["status"] == "complete"
    assert report["queue"]["completed_schedules"] == 1
    assert report["queue"]["completed_match_chunks"] == 3
    assert report["queue"]["completed_roster_freezes"] == 1
    assert report["queue"]["completed_profile_chunks"] == 1
    assert report["queue"]["successful_receipts"] == 6
    assert report["queue"]["processed_work_items"] == 6
    assert len(report["producer_commits"]["scope"]) == 1
    assert len(report["producer_commits"]["match"]) == 52
    assert len(report["producer_commits"]["preview"]) == 52
    assert len(report["producer_commits"]["profile"]) == 1
    assert service_cls.instances
    assert all(service.catalog is catalog for service in service_cls.instances)
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
    assert len(planning_repository.all_completed_calls) == 1
    assert planning_repository.ensure_schema_calls == 1
    root = tmp_path / "backfill" / "unit-queue"
    plan_paths = list((root / "plans").rglob("*.json"))
    assert len(plan_paths) == 1
    frozen_plan = json.loads(plan_paths[0].read_text(encoding="utf-8"))
    assert frozen_plan["schedule_stage_ids"] == {"ENG-Premier League=2526": [23752]}
    assert len(list((root / "receipts").rglob("*.json"))) == 6
    assert len(list((root / "checkpoints").rglob("*.json"))) == 5
    assert len(list((root / "batches").rglob("*.json"))) == 4


@pytest.mark.unit
def test_backfill_retry_keeps_the_same_pending_chunk(monkeypatch, tmp_path):
    retryable = _result("matches", retryable=["1"])
    rc, report, _, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "backfill",
            "--scope",
            "ENG-Premier League=2526",
            "--queue-id",
            "retry-queue",
        ],
        behaviors={"matches": retryable},
        candidate_ids=[1, 2, 3],
    )

    assert rc == 2
    assert report["status"] == "retryable"
    assert report["queue"]["status"] == "running"
    assert report["queue"]["completed_schedules"] == 1
    assert report["queue"]["completed_match_chunks"] == 0
    assert report["queue"]["next_work_items"] == 1
    assert report["queue"]["successful_receipts"] == 1

    resumed_rc, resumed, _, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "backfill",
            "--queue-id",
            "retry-queue",
            "--plan-id",
            report["queue"]["plan_id"],
        ],
        candidate_ids=[1, 2, 3],
    )
    assert resumed_rc == 0
    assert resumed["queue"]["plan_id"] == report["queue"]["plan_id"]
    assert resumed["queue"]["status"] == "complete"


@pytest.mark.unit
def test_workflow_command_selector_contracts_are_fail_closed():
    parser = runner._build_parser()
    with pytest.raises(SystemExit):
        args = parser.parse_args(["backfill"])
        runner._validate_args(parser, args)
    with pytest.raises(SystemExit):
        args = parser.parse_args(
            [
                "backfill",
                "--queue-id",
                "q",
                "--plan-id",
                "a" * 64,
                "--all-catalog",
            ]
        )
        runner._validate_args(parser, args)
    empty_sha256 = profile_candidate_payload_sha256([])
    args = parser.parse_args(
        [
            "daily",
            "--profiles-only",
            "--profiles-limit",
            "0",
            "--expected-profile-candidate-count",
            "0",
            "--expected-profile-candidate-sha256",
            empty_sha256,
        ]
    )
    assert runner._validate_args(parser, args) == []
    args = parser.parse_args(
        ["backfill", "--all-catalog", "--as-of-date", "2026-07-11"]
    )
    assert runner._validate_args(parser, args) == []
    with pytest.raises(SystemExit):
        args = parser.parse_args(
            ["backfill", "--all-catalog", "--state-dir", "/tmp/legacy"]
        )
        runner._validate_args(parser, args)
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
    with pytest.raises(SystemExit):
        args = parser.parse_args(["replay", "--scope", "ENG-Premier League=2526"])
        runner._validate_args(parser, args)
    with pytest.raises(SystemExit):
        args = parser.parse_args(["discover", "--game-id", "1"])
        runner._validate_args(parser, args)
    with pytest.raises(SystemExit):
        args = parser.parse_args(["discover"])
        runner._validate_args(parser, args)
    with pytest.raises(SystemExit):
        args = parser.parse_args(["discover", "--as-of-date", "20260711"])
        runner._validate_args(parser, args)
    with pytest.raises(SystemExit):
        args = parser.parse_args(["daily", "--profiles-only"])
        runner._validate_args(parser, args)
    with pytest.raises(SystemExit):
        args = parser.parse_args(
            [
                "daily",
                "--profiles-only",
                "--profiles-limit",
                "1",
                "--expected-profile-candidate-count",
                "2",
                "--expected-profile-candidate-sha256",
                "a" * 64,
            ]
        )
        runner._validate_args(parser, args)


@pytest.mark.unit
def test_daily_profile_cli_enforces_the_deployed_lower_hard_cap(monkeypatch):
    monkeypatch.setenv("WHOSCORED_DAILY_PROFILE_MAX_LIMIT", "500")
    parser = runner._build_parser()
    args = parser.parse_args(
        [
            "daily",
            "--profiles-only",
            "--profiles-limit",
            "501",
            "--expected-profile-candidate-count",
            "501",
            "--expected-profile-candidate-sha256",
            "a" * 64,
        ]
    )

    with pytest.raises(SystemExit):
        runner._validate_args(parser, args)


@pytest.mark.unit
def test_paid_runner_derives_work_item_and_rejects_allocation_selector(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "ingest_active_scope")
    scope = "ENG-Premier League=2526"
    expected_work_item = (
        "scope-" + __import__("hashlib").sha256(scope.encode("utf-8")).hexdigest()
    )
    base = [
        "daily",
        "--scope",
        scope,
        "--skip-profiles",
        "--catalog-batch-id",
        "catalog-1",
        "--transport-policy",
        "direct_then_paid",
        "--proxy-approval-path",
        "/run/approval.json",
        "--proxy-approval-id",
        "approval-1",
        "--proxy-approval-sha256",
        "a" * 64,
        "--proxy-work-item-id",
        expected_work_item,
    ]

    parser = runner._build_parser()
    args = parser.parse_args(base)
    scopes = runner._validate_args(parser, args)
    assert [item.spec for item in scopes] == [scope]
    assert args.expected_proxy_work_item_id == expected_work_item

    parser = runner._build_parser()
    with pytest.raises(SystemExit):
        args = parser.parse_args(base + ["--proxy-allocation-id", "allocation-1"])
        runner._validate_args(parser, args)

    parser = runner._build_parser()
    wrong = list(base)
    wrong[-1] = "scope-attacker-selected"
    with pytest.raises(SystemExit):
        args = parser.parse_args(wrong)
        runner._validate_args(parser, args)


@pytest.mark.unit
def test_standalone_paid_runner_enforces_code_owned_runtime_gates(monkeypatch):
    import scrapers.whoscored.proxy_campaign as proxy_campaign_module
    from scrapers.whoscored.transport import TransportContext

    # The runner projects these values directly into os.environ.  Seed them via
    # monkeypatch so its teardown knows to restore/delete every value written by
    # the failed startup path instead of leaking paid authority to later tests.
    for name in (
        "WHOSCORED_TRANSPORT_POLICY",
        *runner.PROXY_CAMPAIGN_CLI_ENV.values(),
    ):
        monkeypatch.setenv(name, "")

    identity = {
        "dag_id": "dag_backfill_whoscored",
        "run_id": "run-1",
        "task_id": "ingest_active_scope",
    }
    for name, value in {
        "AIRFLOW_CTX_DAG_ID": identity["dag_id"],
        "AIRFLOW_CTX_DAG_RUN_ID": identity["run_id"],
        "AIRFLOW_CTX_TASK_ID": identity["task_id"],
        "AIRFLOW_CTX_MAP_INDEX": "-1",
        "AIRFLOW_CTX_TRY_NUMBER": "1",
        "WHOSCORED_PAID_GATEWAY_URL": "http://whoscored-paid-gateway:8898",
        "WHOSCORED_PAID_GATEWAY_TOKEN": "g" * 32,
        "WHOSCORED_PROXY_CONTROL_TOKEN": "forbidden-control-secret",
        "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET": "forbidden-approval-secret",
        "WHOSCORED_PAID_ALERT_HMAC_SECRET": "forbidden-alert-secret",
    }.items():
        monkeypatch.setenv(name, value)
    attempt_id = proxy_campaign_module.deterministic_proxy_attempt_id(
        **identity,
        map_index=-1,
        try_number=1,
    )
    allocation = {
        "allocation_id": "allocation-1",
        "task_id": identity["task_id"],
    }
    metadata = {
        "transport_policy": "direct_then_paid",
        "proxy_campaign_approval": {
            "runtime_sha256": "a" * 64,
            "classifier_sha256": "b" * 64,
        },
        "proxy_campaign_id": "campaign-1",
        "proxy_allocation": allocation,
        "proxy_allocation_id": "allocation-1",
        "proxy_attempt_id": attempt_id,
    }

    def load_context(*_args, **_kwargs):
        for name in runner._RUNNER_FORBIDDEN_AUTHORITY_ENV_NAMES:
            assert name not in os.environ
        assert os.environ[runner.PAID_GATEWAY_URL_ENV] == (
            "http://whoscored-paid-gateway:8898"
        )
        assert os.environ[runner.PAID_GATEWAY_TOKEN_ENV] == "g" * 32
        return metadata

    monkeypatch.setattr(
        proxy_campaign_module,
        "load_proxy_campaign_context",
        load_context,
    )

    def assert_runtime(context):
        assert context["dag_id"] == identity["dag_id"]
        assert context["run_id"] == identity["run_id"]
        assert context["task_id"] == identity["task_id"]
        assert context["proxy_campaign_id"] == "campaign-1"
        raise proxy_campaign_module.ProxyCampaignValidationError(
            "WhoScored paid traffic has no provider-side invoice hard cap"
        )

    monkeypatch.setattr(
        proxy_campaign_module,
        "assert_paid_runtime_available",
        assert_runtime,
    )
    monkeypatch.setattr(
        TransportContext,
        "from_env",
        classmethod(
            lambda cls: cls(
                **identity,
                map_index=-1,
                try_number=1,
                transport_policy="direct_then_paid",
                proxy_campaign=metadata,
            )
        ),
    )
    args = SimpleNamespace(
        command="daily",
        direct_only=False,
        transport_policy="direct_then_paid",
        proxy_attempt_id="",
        proxy_work_item_id="scope-work",
        expected_proxy_work_item_id="scope-work",
        proxy_approval_path="/run/approval.json",
        proxy_approval_id="approval-1",
        proxy_approval_sha256="c" * 64,
    )

    with pytest.raises(
        proxy_campaign_module.ProxyCampaignValidationError,
        match="provider-side invoice hard cap",
    ):
        runner._configure_transport_environment(args)


@pytest.mark.unit
def test_report_rebuild_preserves_exact_paid_transport_identity():
    args = SimpleNamespace(
        direct_only=False,
        transport_policy="direct_then_paid",
        proxy_approval_id="approval-1",
        proxy_approval_sha256="a" * 64,
        proxy_allocation_id="allocation-1",
        proxy_work_item_id="work-1",
        proxy_attempt_id="attempt-1",
        catalog_batch_id="catalog-1",
    )
    report = runner._new_report("backfill", ())

    runner._bind_report_transport_identity(report, args)

    assert {
        key: report[key]
        for key in (
            "transport_policy",
            "proxy_approval_id",
            "proxy_approval_sha256",
            "proxy_allocation_id",
            "proxy_work_item_id",
            "proxy_attempt_id",
        )
    } == {
        "transport_policy": "direct_then_paid",
        "proxy_approval_id": "approval-1",
        "proxy_approval_sha256": "a" * 64,
        "proxy_allocation_id": "allocation-1",
        "proxy_work_item_id": "work-1",
        "proxy_attempt_id": "attempt-1",
    }
