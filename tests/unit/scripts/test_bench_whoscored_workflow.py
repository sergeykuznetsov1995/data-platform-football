"""Unit tests for the non-publishing WhoScored workflow benchmark."""

from argparse import Namespace
from collections import Counter
from datetime import datetime
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import sys
from types import SimpleNamespace
from typing import Any

import pytest

from scrapers.whoscored.catalog import CatalogSeason
from scrapers.whoscored.domain import SeasonFormat, WhoScoredScope
from scrapers.whoscored.repository import (
    MatchCommit,
    PreviewCommit,
    WhoScoredScopeRowSpool,
)
from scrapers.whoscored.transport import FailureKind, WhoScoredTransportError


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


def test_production_bootstrap_attests_before_application_import(monkeypatch):
    source = SCRIPT.read_text(encoding="utf-8")
    prefix, separator, _remainder = source.partition("\nimport argparse\n")
    assert separator
    calls: list[str] = []
    contract = object()
    monkeypatch.delenv("WHOSCORED_CAPACITY_BUNDLE_PATH", raising=False)
    monkeypatch.delenv("WHOSCORED_CAPACITY_SITE_PACKAGES", raising=False)
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(sys, "_whoscored_runtime_startup_schema", 2, raising=False)
    monkeypatch.setattr(
        sys,
        "_load_whoscored_runtime_contract",
        lambda root: calls.append(root) or contract,
        raising=False,
    )

    namespace = {
        "__file__": "/opt/airflow/scripts/research/bench_whoscored_workflow.py",
        "__name__": "whoscored_capacity_bootstrap_test",
    }
    exec(compile(prefix, str(SCRIPT), "exec"), namespace)

    assert calls == ["/opt/airflow"]
    assert namespace["_WHOSCORED_RUNTIME_CONTRACT"] is contract


@pytest.mark.parametrize("missing", ["schema", "loader"])
def test_production_bootstrap_requires_image_anchor(monkeypatch, missing):
    source = SCRIPT.read_text(encoding="utf-8")
    prefix, separator, _remainder = source.partition("\nimport argparse\n")
    assert separator
    monkeypatch.setattr(sys, "path", list(sys.path))
    if missing == "schema":
        monkeypatch.delattr(sys, "_whoscored_runtime_startup_schema", raising=False)
    else:
        monkeypatch.setattr(
            sys, "_whoscored_runtime_startup_schema", 2, raising=False
        )
        monkeypatch.delattr(sys, "_load_whoscored_runtime_contract", raising=False)

    with pytest.raises(RuntimeError, match="startup anchor|runtime loader"):
        exec(
            compile(prefix, str(SCRIPT), "exec"),
            {
                "__file__": (
                    "/opt/airflow/scripts/research/bench_whoscored_workflow.py"
                ),
                "__name__": "whoscored_capacity_bootstrap_failure_test",
            },
        )


def test_run_rechecks_production_runtime_class(monkeypatch):
    operations: list[str] = []
    monkeypatch.setattr(
        bench,
        "_WHOSCORED_RUNTIME_CONTRACT",
        SimpleNamespace(
            require_production_runtime_class=lambda *, operation: operations.append(
                operation
            )
        ),
    )
    monkeypatch.setattr(bench, "_validate_args", lambda _args: "stop after check")

    code, report = bench.run(Namespace())

    assert code == 2
    assert report["status"] == "configuration_error"
    assert operations == ["WhoScored capacity workflow"]


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGHUP])
def test_cli_termination_handler_is_one_shot_system_exit(signum):
    previous = bench._install_cli_termination_handlers()
    try:
        handler = signal.getsignal(signum)
        with pytest.raises(SystemExit) as raised:
            handler(signum, None)
        assert raised.value.code == 128 + signum
        # A second host signal must not interrupt transport cleanup.
        assert handler(signum, None) is None
    finally:
        bench._restore_cli_termination_handlers(previous)

    assert all(signal.getsignal(item) is prior for item, prior in previous.items())


def test_json_fingerprint_accepts_lone_source_surrogates():
    first = bench._json_fingerprint({"preview": "broken-\ud83d-text"})
    second = bench._json_fingerprint({"preview": "broken-\ud83d-text"})

    assert first == second
    assert len(first) == 64


def test_successful_page_units_deduplicate_retries_and_route_fallbacks():
    traffic = bench._traffic_delta(
        {},
        {
            "route_requests": {"direct_http": 2, "direct_flaresolverr": 1},
            "route_wire_bytes": {"direct_http": 20, "direct_flaresolverr": 30},
        },
        [
            {
                "route": "direct_http",
                "status": "failure",
                "cache_key": "target-a",
            },
            {
                "route": "direct_flaresolverr",
                "status": "success",
                "cache_key": "target-a",
            },
            {
                "route": "direct_http",
                "status": "success",
                "cache_key": "target-b",
            },
        ],
    )

    assert traffic["source_request_attempts"] == 3
    assert traffic["successful_source_targets"] == ["target-a", "target-b"]
    assert traffic["successful_page_units"] == 2


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
        self.preview_targets: set[str] = set()

    def has(self, target: Any) -> bool:
        return (
            self.invalidated is None or self.invalidated.target_id != target.target_id
        )

    def load_bytes(self, target: Any) -> tuple[bytes, Any]:
        assert self.has(target)
        return b"raw", SimpleNamespace(target_id=target.target_id)

    def quarantine(self, target: Any, *, reason: str, record: Any = None) -> str:
        assert "incremental" in reason
        assert record is not None and record.target_id == target.target_id
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

    @staticmethod
    def list_preview_candidates(
        league: str,
        season: str,
        *,
        match_ids: Any,
        limit: int,
        force_replay: bool,
    ) -> list[dict[str, Any]]:
        assert league == "INT-World Cup"
        assert season == "2026"
        assert force_replay is True
        return [
            {
                "game_id": int(game_id),
                "game": f"A{game_id} - B{game_id}",
                "home_team": f"A{game_id}",
                "away_team": f"B{game_id}",
            }
            for game_id in list(match_ids)[:limit]
        ]

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

    def record_preview_probe(self, target_id: str) -> None:
        if target_id in self.raw_store.preview_targets:
            self._add_route("raw_cache", 1, 0)
            self.stats["cache_hits"] += 1
            self.ledger.append(
                {
                    "route": "raw_cache",
                    "status": "success",
                    "cache_key": target_id,
                }
            )
            return
        self.ledger.append(
            {"route": "raw_cache", "status": "miss", "cache_key": target_id}
        )
        self._add_route("direct_http", 1, 100)
        self.ledger.append(
            {
                "route": "direct_http",
                "status": "success",
                "cache_key": target_id,
                "url": f"https://www.whoscored.com/{target_id}",
            }
        )
        self.raw_store.preview_targets.add(target_id)

    def close(self) -> None:
        self.closed = True


class FakeService:
    def __init__(self, **kwargs: Any) -> None:
        self.scope = kwargs["scope"].scope
        self.repository = kwargs["repository"]
        self.transport = kwargs["transport"]
        self.raw_store = kwargs["raw_store"]
        self.fetch_error_id = kwargs.get("fetch_error_id")

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

    def _fetch(self, target: Any, *, validator: Any, allow_cache: bool) -> Any:
        assert allow_cache is True
        game_id = int(target.target_id.rsplit(":", 1)[-1])
        if game_id == self.fetch_error_id:
            raise WhoScoredTransportError(
                "probe transport failed",
                kind=FailureKind.TIMEOUT,
                url=target.canonical_url,
                retryable=True,
            )
        self.transport.record_preview_probe(target.target_id)
        response = SimpleNamespace(text=f"preview:{target.target_id}")
        validator(response)
        return response, f"raw://{target.target_id}"

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
        "browser_session_owner": None,
    }
    values.update(overrides)
    return Namespace(**values)


def _cache_args(**overrides: Any) -> Namespace:
    values = {
        "mode": bench.CACHE_CAPACITY_MODE,
        "catalog": str(bench.DEFAULT_COMPETITIONS_PATH),
        "flaresolverr_url": None,
        "cache_seed_file": None,
    }
    values.update(overrides)
    return _args(**values)


def _control_pipe(payload: bytes) -> int:
    read_fd, write_fd = os.pipe()
    try:
        os.write(write_fd, payload)
    finally:
        os.close(write_fd)
    return read_fd


def _valid_control_payload(**overrides: Any) -> bytes:
    document = {
        "schema_version": bench._CAPACITY_CONTROL_SCHEMA_VERSION,
        "owner": "a" * 24,
        "flaresolverr_endpoint": bench._CAPACITY_FLARESOLVERR_ENDPOINT,
    }
    document.update(overrides)
    return json.dumps(document, separators=(",", ":")).encode()


def test_capacity_control_reads_exact_pipe_once_and_closes_fd():
    control_fd = _control_pipe(_valid_control_payload())
    args = _args(
        flaresolverr_url=None,
        browser_session_owner=None,
        capacity_control_fd=control_fd,
    )

    resolved = bench._apply_capacity_control(args)

    assert resolved.browser_session_owner == "a" * 24
    assert resolved.flaresolverr_url == bench._CAPACITY_FLARESOLVERR_ENDPOINT
    assert resolved.capacity_control_fd is None
    with pytest.raises(OSError):
        os.fstat(control_fd)


@pytest.mark.parametrize(
    "payload",
    [
        b"not-json",
        _valid_control_payload(schema_version=2),
        _valid_control_payload(owner="bad"),
        _valid_control_payload(flaresolverr_endpoint="http://127.0.0.1:8192"),
        json.dumps(
            {
                "schema_version": 1,
                "owner": "a" * 24,
                "flaresolverr_endpoint": bench._CAPACITY_FLARESOLVERR_ENDPOINT,
                "extra": 1,
            }
        ).encode(),
        b"x" * bench._CAPACITY_CONTROL_READ_LIMIT,
    ],
)
def test_capacity_control_rejects_invalid_payload_and_closes_fd(payload):
    control_fd = _control_pipe(payload)
    args = _args(
        flaresolverr_url=None,
        browser_session_owner=None,
        capacity_control_fd=control_fd,
    )

    with pytest.raises(ValueError, match="capacity control"):
        bench._apply_capacity_control(args)

    with pytest.raises(OSError):
        os.fstat(control_fd)


def test_capacity_control_rejects_regular_file_and_cli_conflict(tmp_path):
    regular = tmp_path / "control.json"
    regular.write_bytes(_valid_control_payload())
    regular_fd = os.open(regular, os.O_RDONLY)
    with pytest.raises(ValueError, match="capacity control"):
        bench._apply_capacity_control(
            _args(
                flaresolverr_url=None,
                browser_session_owner=None,
                capacity_control_fd=regular_fd,
            )
        )
    with pytest.raises(OSError):
        os.fstat(regular_fd)

    pipe_fd = _control_pipe(_valid_control_payload())
    with pytest.raises(ValueError, match="conflict"):
        bench._apply_capacity_control(
            _args(
                flaresolverr_url="http://explicit.invalid:8191",
                browser_session_owner=None,
                capacity_control_fd=pipe_fd,
            )
        )
    with pytest.raises(OSError):
        os.fstat(pipe_fd)


def test_invalid_capacity_control_main_output_never_leaks_payload(
    monkeypatch, capsys
):
    sentinel = "secret-owner-sentinel"
    control_fd = _control_pipe(
        json.dumps({"invalid": sentinel}, separators=(",", ":")).encode()
    )
    args = _args(
        flaresolverr_url=None,
        browser_session_owner=None,
        capacity_control_fd=control_fd,
    )
    monkeypatch.setattr(
        bench, "_parser", lambda: SimpleNamespace(parse_args=lambda: args)
    )

    assert bench.main() == 2

    output = capsys.readouterr()
    assert sentinel not in output.out
    assert sentinel not in output.err
    assert json.loads(output.out)["status"] == "configuration_error"


def _factories(
    *,
    paid_phase: int | None = None,
    preview_statuses: dict[int, dict[str, Any]] | None = None,
    preview_parser_error_id: int | None = None,
    preview_fetch_error_id: int | None = None,
):
    raw_store = FakeRawStore()
    parsed_game_ids: list[int] = []
    objects: dict[str, Any] = {
        "raw_store": raw_store,
        "parsed_game_ids": parsed_game_ids,
    }

    def create_transport(
        scope: str, ledger: bench.MemoryRequestLedger, args: Namespace
    ) -> FakeTransport:
        assert scope == bench.DEFAULT_SCOPE
        assert args.flaresolverr_url
        transport = FakeTransport(ledger, raw_store, paid_phase=paid_phase)
        objects["transport"] = transport
        return transport

    def parse_preview(*args: Any, **kwargs: Any) -> Any:
        del args
        game_id = int(kwargs["game_id"])
        parsed_game_ids.append(game_id)
        if game_id == preview_parser_error_id:
            raise ValueError("probe parser failed")
        statuses = (preview_statuses or {}).get(
            game_id,
            {
                "missing_players": bench.DatasetStatus.AVAILABLE,
                "preview_lineups": bench.DatasetStatus.EMPTY,
                "preview_sections": bench.DatasetStatus.AVAILABLE,
            },
        )
        return SimpleNamespace(
            datasets={
                name: SimpleNamespace(status=status)
                for name, status in statuses.items()
            }
        )

    factories = bench.BenchmarkFactories(
        load_catalog=lambda path: FakeCatalog(),
        create_raw_store=lambda uri: raw_store,
        create_transport=create_transport,
        create_repository=FakeRepository,
        create_service=lambda **kwargs: FakeService(
            **kwargs,
            fetch_error_id=preview_fetch_error_id,
        ),
        parse_preview=parse_preview,
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
    assert cold["traffic"]["source_request_attempts"] == 7
    assert cold["traffic"]["successful_page_units"] == 7
    assert warm["traffic"]["source_request_attempts"] == 0
    assert warm["traffic"]["cache_hits"] == 7
    assert warm["committed_rows"]["new_batch_total"] == 0
    assert incremental["traffic"]["raw_cache_misses"] == 1
    assert incremental["traffic"]["source_targets"] == ["whoscored:match:101"]
    assert cold["preview_probe"] == warm["preview_probe"]
    assert cold["preview_probe"] == incremental["preview_probe"]
    assert cold["preview_probe"] == {
        "candidate_pool_limit": 9,
        "candidate_count": 9,
        "probed_match_ids": [101, 102, 103],
        "rejected_not_available": 0,
        "selected_match_ids": [101, 102, 103],
    }
    assert report["incremental_invalidation"]["target_id"] == ("whoscored:match:101")
    assert report["paid_proxy_mb"] == 0.0
    assert json.loads(json.dumps(report)) == report
    assert objects["transport"].closed is True


def test_cache_capacity_replays_exact_seed_without_network_and_cleans_up():
    code, report = bench.run(_cache_args())

    assert code == 0
    assert report["schema_version"] == 1
    assert report["mode"] == "cache-capacity-v1"
    assert report["seed_sha256"] == bench.EXPECTED_CACHE_SEED_SHA256
    assert report["network_requests"] == 0
    assert report["paid_proxy_bytes"] == 0
    assert report["cleanup"] == {
        "status": "success",
        "temporary_workspace_removed": True,
    }
    phase = report["phases"][0]
    assert phase["traffic"] == {
        "cache_work_units_attempted": 5,
        "successful_page_units": 5,
        "source_request_attempts": 0,
        "network_requests": 0,
        "paid_proxy_bytes": 0,
        "paid_route_requests": 0,
    }
    assert {result["entity"] for result in phase["results"]} == {
        "matches",
        "previews",
        "profiles",
        "multistage",
    }
    multistage = next(
        result for result in phase["results"] if result["entity"] == "multistage"
    )
    assert multistage["metadata"]["source_stage_count"] == 2
    assert phase["committed_rows"]["idempotent_total"] > 0


@pytest.mark.parametrize("kind", ["missing", "tampered"])
def test_cache_capacity_rejects_missing_or_tampered_seed_before_parse(
    monkeypatch, tmp_path, kind
):
    seed_path = tmp_path / "seed.json"
    if kind == "tampered":
        seed_path.write_bytes(bench._EMBEDDED_CACHE_SEED_BYTES + b" ")
    monkeypatch.setattr(
        bench,
        "parse_matchcentre_data",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("parsed")),
    )

    code, report = bench.run(_cache_args(cache_seed_file=seed_path))

    assert code == 2
    assert report["status"] == "configuration_error"
    assert "seed" in report["error"]
    assert report["network_requests"] == 0
    assert report["phases"] == []


def test_cache_capacity_fails_closed_and_counts_network_escape(monkeypatch):
    def network_escape(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        return socket.create_connection(("127.0.0.1", 9), timeout=0.01)

    monkeypatch.setattr(bench, "parse_matchcentre_data", network_escape)

    code, report = bench.run(_cache_args())

    assert code == 1
    assert report["status"] == "failed"
    assert report["network_requests"] == 1
    assert "attempted network access" in report["error"]
    assert report["cleanup"] == {
        "status": "success",
        "temporary_workspace_removed": True,
    }


def test_cli_sigterm_closes_transport_before_temporary_raw_cleanup(
    monkeypatch, tmp_path
):
    factories, objects = _factories()
    observed_at_raw_cleanup: list[bool] = []

    class TrackingTemporaryDirectory:
        def __init__(self, **kwargs: Any) -> None:
            assert kwargs["prefix"] == "whoscored-workflow-bench-"

        def __enter__(self) -> str:
            return str(tmp_path)

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
            observed_at_raw_cleanup.append(objects["transport"].closed)
            return False

    original_create_service = factories.create_service
    previous = bench._install_cli_termination_handlers()
    handler = signal.getsignal(signal.SIGTERM)

    def create_terminating_service(**kwargs: Any) -> FakeService:
        service = original_create_service(**kwargs)

        def terminate_during_schedule() -> FakeResult:
            handler(signal.SIGTERM, None)
            raise AssertionError("SIGTERM handler returned on its first signal")

        service.sync_schedule = terminate_during_schedule  # type: ignore[method-assign]
        return service

    factories.create_service = create_terminating_service
    monkeypatch.setattr(bench, "TemporaryDirectory", TrackingTemporaryDirectory)
    try:
        with pytest.raises(SystemExit) as raised:
            bench.run(_args(), factories=factories)
    finally:
        bench._restore_cli_termination_handlers(previous)

    assert raised.value.code == 128 + signal.SIGTERM
    assert objects["transport"].closed is True
    assert observed_at_raw_cleanup == [True]


def test_system_exit_during_first_transport_close_retries_before_raw_cleanup(
    monkeypatch, tmp_path
):
    factories, objects = _factories()
    events: list[str] = []

    class TrackingTemporaryDirectory:
        def __init__(self, **kwargs: Any) -> None:
            assert kwargs["prefix"] == "whoscored-workflow-bench-"

        def __enter__(self) -> str:
            return str(tmp_path)

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
            events.append("raw-cleanup")
            return False

    previous = bench._install_cli_termination_handlers()
    handler = signal.getsignal(signal.SIGTERM)
    original_create_transport = factories.create_transport

    def create_terminating_transport(*args: Any, **kwargs: Any) -> FakeTransport:
        transport = original_create_transport(*args, **kwargs)
        original_close = transport.close

        def close() -> None:
            events.append("transport-close")
            handler(signal.SIGTERM, None)
            original_close()

        transport.close = close  # type: ignore[method-assign]
        return transport

    factories.create_transport = create_terminating_transport
    monkeypatch.setattr(bench, "TemporaryDirectory", TrackingTemporaryDirectory)
    try:
        with pytest.raises(SystemExit) as raised:
            bench.run(_args(), factories=factories)
    finally:
        bench._restore_cli_termination_handlers(previous)

    assert raised.value.code == 128 + signal.SIGTERM
    assert events == ["transport-close", "transport-close", "raw-cleanup"]
    assert objects["transport"].closed is True


def test_failed_close_retry_keeps_transport_for_outer_fallback(monkeypatch, tmp_path):
    factories, objects = _factories()
    events: list[str] = []

    class TrackingTemporaryDirectory:
        def __init__(self, **kwargs: Any) -> None:
            assert kwargs["prefix"] == "whoscored-workflow-bench-"

        def __enter__(self) -> str:
            return str(tmp_path)

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
            events.append("raw-cleanup")
            return False

    previous = bench._install_cli_termination_handlers()
    handler = signal.getsignal(signal.SIGTERM)
    original_create_transport = factories.create_transport
    close_calls = 0

    def create_retrying_transport(*args: Any, **kwargs: Any) -> FakeTransport:
        transport = original_create_transport(*args, **kwargs)
        original_close = transport.close

        def close() -> None:
            nonlocal close_calls
            close_calls += 1
            events.append(f"transport-close-{close_calls}")
            if close_calls == 1:
                handler(signal.SIGTERM, None)
            elif close_calls == 2:
                raise RuntimeError("retry close failed")
            else:
                original_close()

        transport.close = close  # type: ignore[method-assign]
        return transport

    factories.create_transport = create_retrying_transport
    monkeypatch.setattr(bench, "TemporaryDirectory", TrackingTemporaryDirectory)
    try:
        with pytest.raises(SystemExit) as raised:
            bench.run(_args(), factories=factories)
    finally:
        bench._restore_cli_termination_handlers(previous)

    assert raised.value.code == 128 + signal.SIGTERM
    assert events == [
        "transport-close-1",
        "transport-close-2",
        "raw-cleanup",
        "transport-close-3",
    ]
    assert objects["transport"].closed is True


@pytest.mark.parametrize("preview_parser_error_id", [None, 101])
def test_transport_close_error_still_fails_report_once(preview_parser_error_id):
    factories, objects = _factories(
        preview_parser_error_id=preview_parser_error_id
    )
    original_create_transport = factories.create_transport
    close_calls = 0

    def create_failing_transport(*args: Any, **kwargs: Any) -> FakeTransport:
        nonlocal close_calls
        transport = original_create_transport(*args, **kwargs)
        original_close = transport.close

        def fail_close() -> None:
            nonlocal close_calls
            close_calls += 1
            original_close()
            raise RuntimeError("close boom")

        transport.close = fail_close  # type: ignore[method-assign]
        return transport

    factories.create_transport = create_failing_transport

    code, report = bench.run(_args(), factories=factories)

    assert code == 1
    assert report["status"] == "failed"
    assert report["error"] == "transport close failed: RuntimeError: close boom"
    assert objects["transport"].closed is True
    assert close_calls == 1


def test_preview_probe_skips_only_explicit_unavailability_and_replays_cache():
    partial = {
        "missing_players": bench.DatasetStatus.NOT_AVAILABLE,
        "preview_lineups": bench.DatasetStatus.AVAILABLE,
        "preview_sections": bench.DatasetStatus.AVAILABLE,
    }
    factories, objects = _factories(preview_statuses={101: partial})

    code, report = bench.run(_args(), factories=factories)

    assert code == 0
    cold, warm, incremental = report["phases"]
    expected_probe = {
        "candidate_pool_limit": 9,
        "candidate_count": 9,
        "probed_match_ids": [101, 102, 103, 104],
        "rejected_not_available": 1,
        "selected_match_ids": [102, 103, 104],
    }
    assert cold["preview_probe"] == expected_probe
    assert warm["preview_probe"] == expected_probe
    assert incremental["preview_probe"] == expected_probe
    assert cold["selected_match_ids"] == [102, 103, 104]
    assert warm["traffic"]["source_request_attempts"] == 0
    assert incremental["traffic"]["source_targets"] == ["whoscored:match:102"]
    assert objects["parsed_game_ids"] == [
        101,
        102,
        103,
        104,
        101,
        102,
        103,
        104,
        101,
        102,
        103,
        104,
    ]


@pytest.mark.parametrize(
    "factory_kwargs, expected_error",
    [
        ({"preview_parser_error_id": 101}, "probe parser failed"),
        ({"preview_fetch_error_id": 101}, "probe transport failed"),
    ],
)
def test_preview_probe_does_not_skip_transport_or_parser_errors(
    factory_kwargs, expected_error
):
    factories, _ = _factories(**factory_kwargs)

    code, report = bench.run(_args(), factories=factories)

    assert code == 1
    assert report["phases"][0]["status"] == "failed"
    assert expected_error in report["phases"][0]["error"]
    assert [result["entity"] for result in report["phases"][0]["results"]] == [
        "schedule"
    ]


def test_preview_probe_rejects_string_status_contract_drift():
    factories, _ = _factories(
        preview_statuses={
            101: {
                "missing_players": "not_available",
                "preview_lineups": bench.DatasetStatus.AVAILABLE,
                "preview_sections": bench.DatasetStatus.AVAILABLE,
            }
        }
    )

    code, report = bench.run(_args(), factories=factories)

    assert code == 1
    assert "preview probe returned an invalid dataset status" in report["error"]
    assert report["phases"][0]["selected_match_ids"] == []


def test_preview_probe_is_bounded_and_fails_when_sample_is_incomplete():
    unavailable = {
        name: bench.DatasetStatus.NOT_AVAILABLE
        for name in ("missing_players", "preview_lineups", "preview_sections")
    }
    factories, objects = _factories(
        preview_statuses={game_id: unavailable for game_id in range(101, 110)}
    )

    code, report = bench.run(_args(), factories=factories)

    assert code == 1
    assert "first 9 bounded candidates; 3 required" in report["error"]
    assert objects["parsed_game_ids"] == list(range(101, 110))
    assert report["phases"][0]["selected_match_ids"] == []
    assert [result["entity"] for result in report["phases"][0]["results"]] == [
        "schedule"
    ]


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
    assert captured["browser_session_owner"] is None


def test_capacity_owner_is_passed_only_to_transport():
    owner = "a1b2c3d4e5f60718"
    captured: dict[str, Any] = {}

    class CapturingTransport:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    original = bench.WhoScoredTransport
    bench.WhoScoredTransport = CapturingTransport
    try:
        factory = bench._default_factories()
        factory.create_transport(
            bench.DEFAULT_SCOPE,
            bench.MemoryRequestLedger(),
            _args(browser_session_owner=owner),
        )
    finally:
        bench.WhoScoredTransport = original

    assert captured["browser_session_owner"] == owner
    assert owner not in captured["context"].as_dict().values()


@pytest.mark.parametrize(
    "owner",
    ["a" * 15, "a" * 33, "A" * 16, "a" * 15 + "-", 1234567890123456],
)
def test_invalid_capacity_owner_fails_closed_without_echo(owner):
    factories = bench.BenchmarkFactories(
        load_catalog=lambda path: (_ for _ in ()).throw(AssertionError("called")),
        create_raw_store=lambda uri: (_ for _ in ()).throw(AssertionError("called")),
        create_transport=lambda *args: (_ for _ in ()).throw(AssertionError("called")),
        create_repository=lambda scope: (_ for _ in ()).throw(AssertionError("called")),
        create_service=lambda **kwargs: (_ for _ in ()).throw(AssertionError("called")),
    )

    code, report = bench.run(
        _args(browser_session_owner=owner),
        factories=factories,
    )

    assert code == 2
    assert report["status"] == "configuration_error"
    assert report["error"] == "browser session owner is invalid"
    assert str(owner) not in json.dumps(report)


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


def test_in_memory_repository_fingerprints_spooled_stats_without_retaining_rows(
    tmp_path,
):
    season = CatalogSeason(
        WhoScoredScope("INT-World Cup", "2026", SeasonFormat.SINGLE_YEAR)
    )
    repository = bench.InMemoryBenchmarkRepository(season)
    spool = WhoScoredScopeRowSpool(
        table="whoscored_team_stage_stats",
        league="INT-World Cup",
        season="2026",
        directory=str(tmp_path),
    )
    try:
        spool.append_entity_rows(
            {
                "league": "INT-World Cup",
                "season": "2026",
                "stage_id": 23752,
                "row_index": index,
                "source_raw_json": "x" * 1024,
            }
            for index in range(128)
        )
        datasets = {
            "whoscored_schedule": [
                {
                    "game_id": 1,
                    "stage_id": None,
                    "status": 6,
                    "has_preview": True,
                }
            ],
            "whoscored_team_stage_stats": spool,
        }
        kwargs = {
            "league": "INT-World Cup",
            "season": "2026",
            "entity_group": "season",
            "datasets": datasets,
            "distinct_keys": {
                "whoscored_schedule": "game_id",
                "whoscored_team_stage_stats": "entity_key",
            },
            "payload_sha256": "a" * 64,
            "raw_uris": ["/tmp/raw"],
        }

        repository.commit_scope_bundle(**kwargs)
        repository.commit_scope_bundle(**kwargs)

        assert set(repository._scope_datasets) == {"whoscored_schedule"}
        assert repository._scope_counts["whoscored_team_stage_stats"] == 128
        metrics = repository.metrics_snapshot()
        assert metrics["new_batch_rows"]["whoscored_team_stage_stats"] == 128
        assert metrics["idempotent_rows"]["whoscored_team_stage_stats"] == 128
    finally:
        spool.close()


def test_in_memory_repository_accepts_explicitly_empty_scope_spool(tmp_path):
    season = CatalogSeason(
        WhoScoredScope("ENG-Premier League", "2526", SeasonFormat.SPLIT_YEAR)
    )
    repository = bench.InMemoryBenchmarkRepository(season)
    with WhoScoredScopeRowSpool(
        table="whoscored_referee_stage_stats",
        league="ENG-Premier League",
        season="2526",
        directory=str(tmp_path),
    ) as spool:
        repository.commit_scope_bundle(
            league="ENG-Premier League",
            season="2526",
            entity_group="season",
            datasets={
                "whoscored_schedule": [{"game_id": 1, "stage_id": None}],
                "whoscored_referee_stage_stats": spool,
            },
            distinct_keys={
                "whoscored_schedule": "game_id",
                "whoscored_referee_stage_stats": "entity_key",
            },
            payload_sha256="a" * 64,
            raw_uris=["/tmp/raw"],
            source_empty={"whoscored_referee_stage_stats"},
        )

    assert repository._scope_counts["whoscored_referee_stage_stats"] == 0


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
