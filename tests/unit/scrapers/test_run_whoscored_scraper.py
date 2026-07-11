"""Contracts for the canonical WhoScored service runner."""

from __future__ import annotations

import json
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
        counts={"schedule": 1, "season_stages": 1},
        tables=[
            "iceberg.bronze.whoscored_schedule",
            "iceberg.bronze.whoscored_season_stages",
        ],
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


def _runtime(behaviors=None):
    catalog = _Catalog()
    configured = dict(behaviors or {})

    class CatalogClass:
        @classmethod
        def from_file(cls):
            return catalog

    class Service:
        instances = []

        def __init__(self, scope, *, catalog):
            self.scope = scope
            self.catalog = catalog
            self.calls = []
            type(self).instances.append(self)

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

        def sync_previews(self):
            self.calls.append(("previews", None))
            return self._value("previews")

        def sync_matches(self, *, limit):
            self.calls.append(("matches", limit))
            return self._value("matches")

        def sync_profiles(self, *, limit, candidate_scopes):
            self.calls.append(("profiles", limit, tuple(candidate_scopes)))
            return self._value("profiles")

        def traffic_stats(self):
            return {
                "paid_proxy_bytes": 0,
                "route_requests": {"direct_http": 1},
            }

    return (CatalogClass, Service), catalog


def _run(monkeypatch, tmp_path, args, *, behaviors=None):
    runtime, catalog = _runtime(behaviors)
    monkeypatch.setattr(runner, "_load_runtime", lambda: runtime)
    output = tmp_path / "result.json"
    rc = runner.main([*args, "--output", str(output)])
    return rc, json.loads(output.read_text(encoding="utf-8")), runtime[1], catalog


@pytest.mark.unit
def test_report_is_group_readable_after_atomic_publish(tmp_path):
    output = tmp_path / "result.json"

    runner._write_report(str(output), {"status": "success"})

    assert output.stat().st_mode & 0o777 == 0o640
    assert json.loads(output.read_text(encoding="utf-8")) == {
        "status": "success"
    }


@pytest.mark.unit
def test_matches_use_direct_service_and_emit_stable_v2_report(monkeypatch, tmp_path):
    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        ["matches", "--scope", "ENG-Premier League=2526"],
    )

    assert rc == 0
    assert len(service_cls.instances) == 1
    assert service_cls.instances[0].calls == [("matches", None)]
    assert report["schema_version"] == 2
    assert report["status"] == "success"
    assert report["command"] == "matches"
    assert report["rows"] == 3
    assert report["row_counts_complete"] is True
    assert report["entities"]["events"] == {
        "table": "iceberg.bronze.whoscored_events",
        "rows_written": 2,
        "counts_complete": True,
    }
    assert report["tables_by_entity"]["lineups"].endswith(
        ".whoscored_lineups"
    )
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
            "matches",
            "--scope",
            "ENG-Premier League=2526",
            "--scope",
            "INT-World Cup=2026",
            "--max-matches",
            "3",
        ],
    )

    assert rc == 0
    assert [item.scope.scope.spec for item in service_cls.instances] == [
        "ENG-Premier League=2526",
        "INT-World Cup=2026",
    ]
    assert [item.calls for item in service_cls.instances] == [
        [("matches", 3)],
        [("matches", 3)],
    ]
    assert [item["status"] for item in report["scopes"]] == [
        "success",
        "success",
    ]


@pytest.mark.unit
@pytest.mark.parametrize("limit", [0, 200])
def test_profiles_union_all_scopes_under_one_global_limit(
    monkeypatch, tmp_path, limit
):
    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        [
            "profiles",
            "--scope",
            "ENG-Premier League=2526",
            "--scope",
            "INT-World Cup=2026",
            "--limit",
            str(limit),
        ],
    )

    assert rc == 0
    assert len(service_cls.instances) == 1
    call = service_cls.instances[0].calls[0]
    assert call[:2] == ("profiles", limit)
    assert [scope.scope.spec for scope in call[2]] == [
        "ENG-Premier League=2526",
        "INT-World Cup=2026",
    ]
    assert [item["status"] for item in report["scopes"]] == [
        "success",
        "success",
    ]
    assert report["scopes"][1]["delegated_to"] == (
        "ENG-Premier League=2526"
    )


@pytest.mark.unit
def test_profiles_default_limit_is_200(monkeypatch, tmp_path):
    rc, _, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        ["profiles", "--scope", "ENG-Premier League=2526"],
    )

    assert rc == 0
    assert service_cls.instances[0].calls[0][1] == 200


@pytest.mark.unit
def test_all_runs_each_v2_entity_once_in_order(monkeypatch, tmp_path):
    rc, report, service_cls, _ = _run(
        monkeypatch,
        tmp_path,
        ["all", "--scope", "ENG-Premier League=2526"],
    )

    assert rc == 0
    assert service_cls.instances[0].calls == [
        ("schedule", None),
        ("previews", None),
        ("matches", None),
    ]
    assert report["rows"] == 5
    assert report["scopes"][0]["entities"]["missing_players"][
        "rows_written"
    ] == 0


@pytest.mark.unit
def test_retryable_service_result_has_distinct_exit_code(monkeypatch, tmp_path):
    retryable = _result("matches", retryable=["123"])
    rc, report, _, _ = _run(
        monkeypatch,
        tmp_path,
        ["matches", "--scope", "ENG-Premier League=2526"],
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
        ["matches", "--scope", "ENG-Premier League=2526"],
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
        ["matches", "--scope", "ENG-Premier League=2526"],
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
        ["matches", "--scope", "UNKNOWN-League=2526"],
    )

    assert rc == 1
    assert service_cls.instances == []
    assert report["scopes"][0]["status"] == "failed"
    assert report["error_details"][0]["entity"] == "scope"


@pytest.mark.unit
def test_runtime_import_failure_still_publishes_failure_report(
    monkeypatch, tmp_path
):
    def fail_runtime():
        raise ImportError("pyarrow missing")

    monkeypatch.setattr(runner, "_load_runtime", fail_runtime)
    output = tmp_path / "result.json"
    rc = runner.main(
        [
            "matches",
            "--scope",
            "ENG-Premier League=2526",
            "--output",
            str(output),
        ]
    )

    report = json.loads(output.read_text(encoding="utf-8"))
    assert rc == 1
    assert report["status"] == "failed"
    assert report["error_details"][0]["entity"] == "runtime"


@pytest.mark.unit
def test_legacy_cli_and_missing_scope_are_rejected():
    parser = runner._build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["matches", "--leagues", "ENG-Premier League"])
    with pytest.raises(SystemExit):
        parser.parse_args(["matches"])


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
