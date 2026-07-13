"""Unit tests for the non-publishing WhoScored workflow benchmark."""

from argparse import Namespace
from collections import Counter
from datetime import datetime
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from typing import Any

import pytest

from scrapers.whoscored.catalog import CatalogSeason
from scrapers.whoscored.domain import SeasonFormat, WhoScoredScope
from scrapers.whoscored.repository import MatchCommit, PreviewCommit


SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "scripts"
    / "research"
    / "bench_whoscored_workflow.py"
)
SPEC = importlib.util.spec_from_file_location("bench_whoscored_workflow", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
bench = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bench
SPEC.loader.exec_module(bench)


def test_json_fingerprint_accepts_lone_source_surrogates():
    first = bench._json_fingerprint({"preview": "broken-\ud83d-text"})
    second = bench._json_fingerprint({"preview": "broken-\ud83d-text"})

    assert first == second
    assert len(first) == 64


class FakeResult:
    def __init__(
        self,
        entity: str,
        attempted: int,
        succeeded: int,
        counts: dict[str, int],
        *,
        errors: list[str] | None = None,
    ) -> None:
        self.entity = entity
        self.scope = bench.DEFAULT_SCOPE
        self.attempted = attempted
        self.succeeded = succeeded
        self.skipped = 0
        self.retryable: list[str] = []
        self.terminal: list[str] = []
        self.tables: list[str] = []
        self.counts = counts
        self.errors = list(errors or ())

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity": self.entity,
            "scope": self.scope,
            "attempted": self.attempted,
            "succeeded": self.succeeded,
            "retryable": self.retryable,
            "terminal": self.terminal,
            "counts": self.counts,
            "errors": self.errors,
        }


class FakeCatalog:
    def __init__(self) -> None:
        self.season = CatalogSeason(
            WhoScoredScope("INT-World Cup", "2026", SeasonFormat.SINGLE_YEAR),
            source_season_id=99,
        )

    def parse_scope_spec(self, spec: str) -> CatalogSeason:
        if spec != self.season.scope.spec:
            raise ValueError("unknown scope")
        return self.season

    @staticmethod
    def competition(competition_id: str) -> Any:
        assert competition_id == "INT-World Cup"
        return SimpleNamespace(
            whoscored_enabled=True,
            region_id=247,
            tournament_id=36,
        )


class FakeRawStore:
    def __init__(self) -> None:
        self.invalidated: Any = None

    def has(self, target: Any) -> bool:
        return (
            self.invalidated is None or self.invalidated.target_id != target.target_id
        )

    def quarantine(self, target: Any, *, reason: str) -> str:
        assert "incremental" in reason
        self.invalidated = target
        return f"quarantine/{target.target_id}"


class FakeRepository:
    DATASETS = {
        "schedule": {"whoscored_schedule": 5},
        "matches": {
            "whoscored_matches": 3,
            "whoscored_events": 60,
            "whoscored_lineups": 66,
        },
        "previews": {
            "whoscored_missing_players": 2,
            "whoscored_preview_lineups": 6,
            "whoscored_preview_sections": 3,
        },
        "profiles": {
            "whoscored_player_profile_versions": 3,
            "whoscored_player_stage_participations": 9,
        },
    }

    def __init__(self, scope: CatalogSeason) -> None:
        self.scope = scope
        self.phase = -1
        self.accepted: Counter[str] = Counter()
        self.new: Counter[str] = Counter()
        self.idempotent: Counter[str] = Counter()
        self.current: Counter[str] = Counter()
        self.calls: Counter[str] = Counter()

    def begin_phase(self) -> None:
        self.phase += 1

    def record(self, entity: str) -> None:
        counts = self.DATASETS[entity]
        for name, value in counts.items():
            self.accepted[name] += value
            if self.phase == 0:
                self.new[name] += value
                self.current[name] = value
            else:
                self.idempotent[name] += value
        self.calls[entity] += 1

    @staticmethod
    def benchmark_match_ids(limit: int) -> list[int]:
        return [101 + offset for offset in range(limit)]

    @staticmethod
    def benchmark_profile_ids(limit: int) -> list[int]:
        return [901 + offset for offset in range(limit)]

    def metrics_snapshot(self) -> dict[str, Any]:
        return {
            "accepted_rows": dict(self.accepted),
            "new_batch_rows": dict(self.new),
            "idempotent_rows": dict(self.idempotent),
            "logical_current_rows": dict(self.current),
            "commit_calls": dict(self.calls),
            "failure_records": 0,
        }


class FakeTransport:
    def __init__(
        self,
        ledger: bench.MemoryRequestLedger,
        raw_store: FakeRawStore,
        *,
        paid_phase: int | None = None,
    ) -> None:
        self.ledger = ledger
        self.raw_store = raw_store
        self.paid_phase = paid_phase
        self.phase = -1
        self.closed = False
        self.stats: dict[str, Any] = {
            "route_requests": {},
            "route_wire_bytes": {},
            "failures": {},
            "cache_hits": 0,
            "cache_invalid": 0,
            "browser_sessions": 0,
            "paid_urls": 0,
            "paid_proxy_up_bytes": 0,
            "paid_proxy_down_bytes": 0,
            "paid_proxy_bytes": 0,
            "paid_proxy_bytes_by_url": {},
        }

    def _add_route(self, route: str, requests: int, wire_bytes: int) -> None:
        self.stats["route_requests"][route] = (
            self.stats["route_requests"].get(route, 0) + requests
        )
        self.stats["route_wire_bytes"][route] = (
            self.stats["route_wire_bytes"].get(route, 0) + wire_bytes
        )

    def begin_phase(self) -> None:
        self.phase += 1
        if self.phase == 0:
            self._add_route("direct_http", 4, 400)
            for offset in range(4):
                key = f"cold:{offset}"
                self.ledger.append(
                    {"route": "raw_cache", "status": "miss", "cache_key": key}
                )
                self.ledger.append(
                    {
                        "route": "direct_http",
                        "status": "success",
                        "cache_key": key,
                        "url": f"https://www.whoscored.com/{offset}",
                    }
                )
        elif self.phase == 1:
            self._add_route("raw_cache", 4, 0)
            self.stats["cache_hits"] += 4
            for offset in range(4):
                self.ledger.append(
                    {
                        "route": "raw_cache",
                        "status": "success",
                        "cache_key": f"warm:{offset}",
                    }
                )
        else:
            assert self.raw_store.invalidated is not None
            target = self.raw_store.invalidated.target_id
            self._add_route("raw_cache", 3, 0)
            self._add_route("direct_http", 1, 125)
            self.stats["cache_hits"] += 3
            self.ledger.append(
                {"route": "raw_cache", "status": "miss", "cache_key": target}
            )
            self.ledger.append(
                {
                    "route": "direct_http",
                    "status": "success",
                    "cache_key": target,
                    "url": "https://www.whoscored.com/Matches/101/Live",
                }
            )

        if self.paid_phase == self.phase:
            self._add_route("paid_http", 1, 10)
            self.stats["paid_urls"] += 1
            self.stats["paid_proxy_up_bytes"] += 4
            self.stats["paid_proxy_down_bytes"] += 6
            self.stats["paid_proxy_bytes"] += 10
            self.stats["paid_proxy_bytes_by_url"]["https://paid.invalid"] = 10

    def get_traffic_stats(self) -> dict[str, Any]:
        return json.loads(json.dumps(self.stats))

    def close(self) -> None:
        self.closed = True


class FakeService:
    def __init__(self, **kwargs: Any) -> None:
        self.scope = kwargs["scope"].scope
        self.repository = kwargs["repository"]
        self.transport = kwargs["transport"]
        self.raw_store = kwargs["raw_store"]

    def sync_schedule(self) -> FakeResult:
        self.repository.begin_phase()
        self.transport.begin_phase()
        self.repository.record("schedule")
        return FakeResult("schedule", 1, 1, {"schedule": 5})

    def sync_matches(self, **kwargs: Any) -> FakeResult:
        count = len(kwargs["match_ids"])
        self.repository.record("matches")
        return FakeResult(
            "matches",
            count,
            count,
            {"matches": count, "events": 60, "lineups": 66},
        )

    def sync_previews(self, **kwargs: Any) -> FakeResult:
        count = len(kwargs["match_ids"])
        self.repository.record("previews")
        return FakeResult(
            "previews",
            count,
            count,
            {"missing_players": 2, "preview_lineups": 6},
        )

    def sync_profiles(self, *, limit: int, **kwargs: Any) -> FakeResult:
        self.repository.record("profiles")
        return FakeResult(
            "profiles",
            limit,
            limit,
            {
                "player_profile_versions": limit,
                "player_stage_participations": 9,
            },
        )


def _args(**overrides: Any) -> Namespace:
    values = {
        "scope": bench.DEFAULT_SCOPE,
        "match_limit": 3,
        "profile_limit": 3,
        "catalog": "/does/not/matter.yaml",
        "flaresolverr_url": "http://flaresolverr.invalid:8191",
    }
    values.update(overrides)
    return Namespace(**values)


def _factories(*, paid_phase: int | None = None):
    raw_store = FakeRawStore()
    objects: dict[str, Any] = {"raw_store": raw_store}

    def create_transport(
        scope: str, ledger: bench.MemoryRequestLedger, args: Namespace
    ) -> FakeTransport:
        assert scope == bench.DEFAULT_SCOPE
        assert args.flaresolverr_url
        transport = FakeTransport(ledger, raw_store, paid_phase=paid_phase)
        objects["transport"] = transport
        return transport

    factories = bench.BenchmarkFactories(
        load_catalog=lambda path: FakeCatalog(),
        create_raw_store=lambda uri: raw_store,
        create_transport=create_transport,
        create_repository=FakeRepository,
        create_service=lambda **kwargs: FakeService(**kwargs),
    )
    return factories, objects


def test_three_phase_workflow_is_json_safe_and_non_publishing():
    factories, objects = _factories()

    code, report = bench.run(_args(), factories=factories)

    assert code == 0
    assert report["status"] == "success"
    assert report["publishes"] is False
    assert report["writes_bronze"] is False
    assert report["executes_ddl"] is False
    assert report["stage_statistics_contract"]["team_query_defaults"] == {
        "page": "",
        "numberOfTeamsToPick": "",
        "incPens": "",
        "against": "",
    }
    assert report["structured_transport"] == {
        "max_urls_per_browser_batch": 8,
        "fixed_browser_concurrency": 4,
        "default_requests_per_minute_per_task": 60,
        "hard_max_requests_per_minute_per_task": 60,
        "rate_token_grain": "one_per_source_url",
    }
    assert report["stage_statistics_contract"]["team_zero_paging_sentinel"] == (
        "nonempty_rows_zero_totals_and_sizes_current_page_zero_or_one"
    )
    assert report["stage_statistics_contract"]["team_xg_query_filters"] == {
        "sortAscending": "false",
        "incPens": "true",
        "against": "false",
    }
    assert report["stage_statistics_contract"]["team_tabs"] == [
        {
            "category": "summaryteam",
            "subcategory": "all",
            "sort_by": "Rating",
            "sort_ascending": "",
            "inc_pens": "",
            "against": "",
        },
        {
            "category": "summaryteam",
            "subcategory": "offensive",
            "sort_by": "shotsPerGame",
            "sort_ascending": "",
            "inc_pens": "",
            "against": "",
        },
        {
            "category": "summaryteam",
            "subcategory": "defensive",
            "sort_by": "tacklePerGame",
            "sort_ascending": "",
            "inc_pens": "",
            "against": "",
        },
        {
            "category": "xg-teamstats",
            "subcategory": "summary",
            "sort_by": "xG",
            "sort_ascending": "false",
            "inc_pens": "true",
            "against": "false",
        },
    ]
    assert report["stage_statistics_contract"]["player_tabs"] == [
        {"category": "summary", "subcategory": "all", "inc_pens": False},
        {"category": "summary", "subcategory": "defensive", "inc_pens": False},
        {"category": "summary", "subcategory": "offensive", "inc_pens": False},
        {"category": "summary", "subcategory": "passing", "inc_pens": False},
        {"category": "xg-stats", "subcategory": "summary", "inc_pens": True},
    ]
    assert [phase["name"] for phase in report["phases"]] == [
        "cold",
        "warm",
        "incremental",
    ]
    cold, warm, incremental = report["phases"]
    assert cold["traffic"]["source_request_attempts"] == 4
    assert warm["traffic"]["source_request_attempts"] == 0
    assert warm["traffic"]["cache_hits"] == 4
    assert warm["committed_rows"]["new_batch_total"] == 0
    assert incremental["traffic"]["raw_cache_misses"] == 1
    assert incremental["traffic"]["source_targets"] == ["whoscored:match:101"]
    assert report["incremental_invalidation"]["target_id"] == ("whoscored:match:101")
    assert report["paid_proxy_mb"] == 0.0
    assert json.loads(json.dumps(report)) == report
    assert objects["transport"].closed is True


def test_any_paid_route_fails_closed():
    factories, _ = _factories(paid_phase=0)

    code, report = bench.run(_args(), factories=factories)

    assert code == 1
    assert report["status"] == "failed"
    assert "paid proxy bytes" in report["error"]
    assert len(report["phases"]) == 1


def test_invalid_limits_fail_before_factories_are_used():
    factories = bench.BenchmarkFactories(
        load_catalog=lambda path: (_ for _ in ()).throw(AssertionError("called")),
        create_raw_store=lambda uri: (_ for _ in ()).throw(AssertionError("called")),
        create_transport=lambda *args: (_ for _ in ()).throw(AssertionError("called")),
        create_repository=lambda scope: (_ for _ in ()).throw(AssertionError("called")),
        create_service=lambda **kwargs: (_ for _ in ()).throw(AssertionError("called")),
    )

    code, report = bench.run(_args(match_limit=0), factories=factories)

    assert code == 2
    assert report["status"] == "configuration_error"
    assert "1..10" in report["error"]


def test_default_transport_never_receives_paid_configuration(monkeypatch):
    captured: dict[str, Any] = {}

    class CapturingTransport:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(bench, "WhoScoredTransport", CapturingTransport)
    factory = bench._default_factories()

    factory.create_transport(
        bench.DEFAULT_SCOPE,
        bench.MemoryRequestLedger(),
        _args(),
    )

    assert captured["paid_proxy_url"] is None
    assert captured["proxy_control_url"] is None
    assert captured["request_ledger"] is not None


def test_benchmark_feed_contract_is_exact_and_fail_closed():
    expected = bench._expected_stage_feed_keys([23752])
    assert len(expected) == 68
    assert len(bench._expected_stage_feed_keys([23752, 23753])) == 136

    season = CatalogSeason(
        WhoScoredScope("INT-World Cup", "2026", SeasonFormat.SINGLE_YEAR)
    )
    repository = bench.InMemoryBenchmarkRepository(season)
    datasets = {
        # Production scope commits carry stage_id on schedule rows; they do
        # not include the catalog-level whoscored_stages dataset.
        "whoscored_schedule": [{"game_id": 1, "stage_id": 23752}],
    }
    kwargs = {
        "league": "INT-World Cup",
        "season": "2026",
        "entity_group": "season",
        "datasets": datasets,
        "distinct_keys": {"whoscored_schedule": "game_id"},
        "payload_sha256": "a" * 64,
        "raw_uris": ["/tmp/raw"],
        "feed_states": {key: "available" for key in expected},
    }
    repository.commit_scope_bundle(**kwargs)

    incomplete = dict(kwargs["feed_states"])
    incomplete.pop(next(iter(incomplete)))
    with pytest.raises(ValueError, match="feed-state contract mismatch"):
        repository.commit_scope_bundle(**{**kwargs, "feed_states": incomplete})

    wrong_stage = {key: "available" for key in bench._expected_stage_feed_keys([23753])}
    with pytest.raises(ValueError, match="does not cover schedule stages"):
        repository.commit_scope_bundle(**{**kwargs, "feed_states": wrong_stage})


def test_in_memory_repository_deduplicates_warm_batches():
    season = CatalogSeason(
        WhoScoredScope("INT-World Cup", "2026", SeasonFormat.SINGLE_YEAR)
    )
    repository = bench.InMemoryBenchmarkRepository(season)
    schedule = [
        {
            "game_id": game_id,
            "date": datetime(2026, 6, 11 + game_id),
            "status": 6,
            "home_score": 1,
            "away_score": 0,
            "match_is_opta": True,
            "has_preview": True,
            "game": f"A{game_id} - B{game_id}",
        }
        for game_id in (1, 2, 3)
    ]
    kwargs = {
        "league": "INT-World Cup",
        "season": "2026",
        "entity_group": "season",
        "datasets": {"whoscored_schedule": schedule},
        "distinct_keys": {"whoscored_schedule": "game_id"},
        "payload_sha256": "a" * 64,
        "raw_uris": ["/tmp/raw"],
    }

    repository.commit_scope_bundle(**kwargs)
    repository.commit_scope_bundle(**kwargs)

    metrics = repository.metrics_snapshot()
    assert metrics["new_batch_rows"]["whoscored_schedule"] == 3
    assert metrics["idempotent_rows"]["whoscored_schedule"] == 3
    assert metrics["logical_current_rows"]["whoscored_schedule"] == 3
    assert repository.benchmark_match_ids(3) == [1, 2, 3]

    commit = MatchCommit(
        game_id=1,
        league="INT-World Cup",
        season="2026",
        game="A - B",
        payload_sha256="b" * 64,
        raw_uri="/tmp/match",
        events=(
            {
                "game_id": 1,
                "source_event_id": 9_000_001,
                "team_event_id": 1,
                "team_id": 1,
                "player_id": 9,
            },
        ),
        lineups=({"game_id": 1, "player_id": 9},),
        lineups_available=True,
        transport_mode="raw_cache",
        datasets={"matches": ({"game_id": 1},)},
        dataset_statuses={
            "matches": "available",
            "events": "available",
            "lineups": "available",
        },
        is_opta=True,
        schedule_status=6,
    )
    repository.commit_matches((commit,))
    repository.commit_matches((commit,))

    metrics = repository.metrics_snapshot()
    assert metrics["new_batch_rows"]["whoscored_events"] == 1
    assert metrics["idempotent_rows"]["whoscored_events"] == 1
    assert metrics["logical_current_rows"]["whoscored_events"] == 1
    assert repository.benchmark_profile_ids(3) == [9]


@pytest.mark.parametrize(
    "statuses, message",
    [
        ({}, "lacks dataset statuses"),
        (
            {
                "missing_players": "not_available",
                "preview_lineups": "empty",
                "preview_sections": "empty",
            },
            "structure is not available",
        ),
    ],
)
def test_preview_commit_without_available_structure_fails_closed(statuses, message):
    season = CatalogSeason(
        WhoScoredScope("INT-World Cup", "2026", SeasonFormat.SINGLE_YEAR)
    )
    repository = bench.InMemoryBenchmarkRepository(season)
    commit = PreviewCommit(
        game_id=1,
        league="INT-World Cup",
        season="2026",
        game="A - B",
        payload_sha256="c" * 64,
        raw_uri="/tmp/preview",
        missing_players=(),
        transport_mode="raw_cache",
        datasets={"preview_lineups": (), "preview_sections": ()},
        dataset_statuses=statuses,
    )

    with pytest.raises(ValueError, match=message):
        repository.validate_preview_commit(commit)


def test_main_prints_a_single_json_document(monkeypatch, capsys):
    expected = {"status": "failed", "publishes": False}
    monkeypatch.setattr(
        bench,
        "_parser",
        lambda: SimpleNamespace(parse_args=lambda: _args()),
    )
    monkeypatch.setattr(bench, "run", lambda args: (1, expected))

    assert bench.main() == 1

    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected
    assert captured.out.count("\n") == 1
