from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest
from pyarrow import fs

from scrapers.whoscored.catalog import (
    apply_schedule_classification,
    CatalogError,
    WhoScoredCatalog,
)
from scrapers.whoscored.detailed_feeds import DETAILED_FEED_OPTIONS
from scrapers.whoscored.parsers import (
    CalendarMonth,
    DatasetStatus,
    ParsedDataset,
    PlayerStageStatisticsPage,
)
from scrapers.whoscored.raw_store import (
    RawTarget,
    WhoScoredRawStore,
    match_page_target,
    preview_page_target,
    profile_page_target,
    schedule_month_target,
)
from scrapers.whoscored.repository import MatchCandidate
from scrapers.whoscored.service import (
    ACTIVE_SCHEDULE_CACHE_TTL,
    CATALOG_REQUEST_BURST_SIZE,
    DEFAULT_CATALOG_REQUESTS_PER_MINUTE,
    DEFAULT_STRUCTURED_REQUESTS_PER_MINUTE,
    STRUCTURED_PARSE_BATCH_SIZE,
    STRUCTURED_REQUEST_BURST_SIZE,
    WhoScoredIngestService,
    _ParsedFetchSpec,
    _is_source_stage_statistics_unavailable,
    catalog_requests_per_minute_from_env,
    structured_requests_per_minute_from_env,
)
from scrapers.whoscored.transport import (
    CachedPayload,
    FailureKind,
    TransportBudgets,
    TransportResponse,
    TransportRoute,
    WhoScoredTransportError,
)


def _catalog():
    return WhoScoredCatalog.from_mapping(
        {
            "competitions": [
                {
                    "id": "INT-World Cup",
                    "in_scope": True,
                    "seasons": [
                        {
                            "id": 2026,
                            "season_format": "single_year",
                            "start": "2026-06-11",
                            "end": "2026-07-19",
                        }
                    ],
                    "sources": {
                        "primary": ["whoscored"],
                        "fallback": [],
                        "whoscored": {"region_id": 247, "tournament_id": 36},
                    },
                },
                {
                    "id": "ENG-Premier League",
                    "seasons": [
                        {
                            "id": 2526,
                            "season_format": "split_year",
                        }
                    ],
                    "sources": {
                        "primary": ["whoscored"],
                        "fallback": [],
                        "whoscored": {"region_id": 252, "tournament_id": 2},
                    },
                },
            ]
        }
    )


def test_catalog_rejects_duplicate_canonical_scope_before_publication():
    rows = {
        "competitions": [
            {
                "competition_id": "WS-11-605",
                "region_id": 11,
                "tournament_id": 605,
                "eligibility": "included",
            }
        ],
        "seasons": [
            {
                "competition_id": "WS-11-605",
                "region_id": 11,
                "tournament_id": 605,
                "season_id": "2021",
                "source_season_id": 8534,
                "season_format": "single_year",
                "eligibility": "included",
            },
            {
                "competition_id": "WS-11-605",
                "region_id": 11,
                "tournament_id": 605,
                "season_id": "2021",
                "source_season_id": 8426,
                "season_format": "split_year",
                "eligibility": "included",
            },
        ],
        "stages": [],
    }

    with pytest.raises(CatalogError, match="Duplicate canonical scope"):
        WhoScoredCatalog.from_rows(rows)


def test_schedule_classification_preserves_independent_season_quarantine():
    competition = {
        "competition_id": "WS-11-605",
        "region_id": 11,
        "tournament_id": 605,
        "tournament_name": "Argentina 4",
        "eligibility": "quarantined",
        "classification_reason": "source_sex_not_yet_observed",
    }
    season = {
        "competition_id": "WS-11-605",
        "region_id": 11,
        "tournament_id": 605,
        "season_id": "2021",
        "source_season_id": 8534,
        "season_format": "single_year",
        "eligibility": "quarantined",
        "classification_reason": "duplicate_canonical_season_identity",
    }
    stage = {
        "competition_id": "WS-11-605",
        "region_id": 11,
        "tournament_id": 605,
        "source_season_id": 8534,
        "stage_id": 19000,
        "eligibility": "included",
    }

    resolved = apply_schedule_classification(
        {
            "competitions": [competition],
            "seasons": [season],
            "stages": [stage],
        },
        [{"region_id": 11, "tournament_id": 605, "source_sex": 1}],
    )

    assert resolved["competitions"][0]["eligibility"] == "included"
    assert resolved["seasons"][0]["eligibility"] == "quarantined"
    assert resolved["seasons"][0]["classification_reason"] == (
        "duplicate_canonical_season_identity"
    )
    assert resolved["stages"][0]["eligibility"] == "quarantined"
    assert resolved["stages"][0]["classification_reason"].startswith("season:")


def _match_html() -> str:
    payload = {
        "expandedMaxMinute": 90,
        "playerIdNameDictionary": {"10": "Player One", "20": "Player Two"},
        "home": {
            "teamId": 1,
            "name": "Home",
            "players": [
                {
                    "playerId": 10,
                    "name": "Player One",
                    "isFirstEleven": True,
                    "shirtNo": 9,
                    "stats": {"ratings": {"90": 7.1}},
                }
            ],
        },
        "away": {
            "teamId": 2,
            "name": "Away",
            "players": [
                {
                    "playerId": 20,
                    "name": "Player Two",
                    "isFirstEleven": True,
                    "shirtNo": 1,
                    "stats": {"ratings": {"90": 6.8}},
                }
            ],
        },
        "events": [
            {
                "id": 9_000_101.0,
                "eventId": 101,
                "period": {"displayName": "FirstHalf"},
                "minute": 1,
                "second": 2,
                "type": {"displayName": "Pass"},
                "outcomeType": {"displayName": "Successful"},
                "teamId": 1,
                "playerId": 10,
                "qualifiers": [],
            }
        ],
    }
    return (
        "<script>require.config.params['args'] = {matchCentreData: "
        + json.dumps(payload)
        + "};</script>"
    )


def _preview_html() -> str:
    return """
    <div id="predicted-lineups"></div>
    <div id="missing-players"><table><tbody><tr>
      <td class="pn"><a href="/Players/10/Show/A">Player A</a></td>
      <td class="reason"><span title="Hamstring"></span></td>
      <td class="confirmed">Out</td>
    </tr></tbody></table></div>
    """


class _Transport:
    def __init__(self, html: str):
        self.html = html
        self.raw_cache = None
        self.budgets = TransportBudgets()
        self.cache_load_allowed = []

    def fetch(
        self,
        url,
        *,
        cache_key,
        validator,
        before_network=None,
        scope=None,
        entity=None,
        browser_bootstrap_url=None,
    ):
        del scope, entity, browser_bootstrap_url
        self.cache_load_allowed.append(bool(self.raw_cache.allow_load))
        if before_network is not None:
            before_network()
        content = self.html.encode()
        response = TransportResponse(
            url=url,
            content=content,
            status_code=200,
            headers={},
            route=TransportRoute.DIRECT_HTTP,
            wire_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )
        assert validator(response) is True
        self.raw_cache.store(cache_key, CachedPayload(content), response.sha256)
        return response

    def get_traffic_stats(self):
        return {"paid_proxy_bytes": 0, "route_requests": {"direct_http": 1}}

    def close(self):
        return None


class _RawCacheAwareTransport:
    """Tiny transport double that honors the service's raw-cache hook."""

    def __init__(self, content: bytes):
        self.content = content
        self.raw_cache = None
        self.budgets = TransportBudgets()
        self.network_calls = 0

    def fetch(
        self,
        url,
        *,
        cache_key,
        validator,
        before_network=None,
        scope=None,
        entity=None,
        browser_bootstrap_url=None,
    ):
        del scope, entity, browser_bootstrap_url
        cached = self.raw_cache.load(cache_key)
        if cached is not None:
            content = cached.content
            route = TransportRoute.RAW_CACHE
            wire_bytes = 0
        else:
            if before_network is not None:
                before_network()
            self.network_calls += 1
            content = self.content
            route = TransportRoute.DIRECT_HTTP
            wire_bytes = len(content)
        digest = hashlib.sha256(content).hexdigest()
        response = TransportResponse(
            url=url,
            content=content,
            status_code=200,
            headers={},
            route=route,
            wire_bytes=wire_bytes,
            sha256=digest,
        )
        assert validator(response) is True
        if route is not TransportRoute.RAW_CACHE:
            self.raw_cache.store(cache_key, CachedPayload(content), digest)
        return response

    def get_traffic_stats(self):
        return {"paid_proxy_bytes": 0}

    def close(self):
        return None


class _Repository:
    def __init__(self):
        self.commits = []
        self.failures = []
        self.profile_candidates = []
        self.profile_candidate_scope_requests = []
        self.profile_commits = []
        self.profile_failures = []
        self.preview_candidates = []
        self.preview_commits = []
        self.preview_commit_batches = []
        self.preview_failures = []
        self.scope_snapshots = []

    def ensure_schema(self):
        return None

    def list_match_candidates(self, *args, **kwargs):
        return [
            MatchCandidate(
                game_id=123,
                league="INT-World Cup",
                season="2026",
                game="Home-Away",
                kickoff=datetime(2026, 6, 12),
                status=6,
                match_is_opta=True,
            )
        ]

    def commit_matches(self, commits):
        self.commits.extend(commits)
        return tuple(commit.batch_id for commit in commits)

    def validate_match_commit(self, _commit):
        return None

    def validate_preview_commit(self, commit):
        if "not_available" in commit.dataset_statuses.values():
            raise ValueError("preview structure is incomplete")

    def record_failure(self, failure):
        self.failures.append(failure)

    def list_profile_candidates(self, *, scopes, limit):
        self.profile_candidate_scope_requests.append(tuple(scopes))
        return self.profile_candidates[:limit]

    def commit_profiles(self, commits):
        self.profile_commits.extend(commits)
        return tuple(commit.batch_id for commit in commits)

    def record_profile_failure(self, **failure):
        self.profile_failures.append(failure)

    def list_preview_candidates(self, *args, **kwargs):
        return self.preview_candidates[: kwargs.get("limit")]

    def commit_previews(self, commits):
        self.preview_commit_batches.append(tuple(commits))
        self.preview_commits.extend(commits)
        return tuple(commit.batch_id for commit in commits)

    def record_preview_failure(self, failure):
        self.preview_failures.append(failure)

    def commit_scope_bundle(self, **snapshot):
        self.scope_snapshots.append(snapshot)
        return "wss2-test"

    def latest_source_season_id(self, *_args, **_kwargs):
        return None


class _CatalogRepository:
    def __init__(self, previous=None):
        self.previous = previous
        self.persisted = []
        self.load_options = []

    def load_discovered_catalog(
        self, *, allow_legacy_parser_for_full_history=False
    ):
        self.load_options.append(bool(allow_legacy_parser_for_full_history))
        return self.previous

    def persist_discovered_catalog(self, catalog, **metadata):
        self.persisted.append((catalog, metadata))


class _CatalogTransport:
    def get_traffic_stats(self):
        return {"paid_proxy_bytes": 0, "route_requests": {"direct_http": 0}}

    def close(self):
        return None


def test_full_history_service_requests_the_narrow_legacy_catalog_migration_path():
    repository = _CatalogRepository(previous=None)

    # A source failure after bootstrap is sufficient here: the regression is
    # that the explicit full-history boundary requested the legacy-safe read.
    result = WhoScoredIngestService.discover_catalog(
        repository=repository,
        transport=_CatalogTransport(),
        raw_store=object(),
        full_history=True,
    )

    assert repository.load_options == [True]
    assert result.errors


def _discovery_competition_row(
    competition_id="INT-World Cup", *, region_id=247, tournament_id=36
):
    return {
        "competition_id": competition_id,
        "region_id": region_id,
        "region_name": "International",
        "region_code": "INT",
        "tournament_id": tournament_id,
        "tournament_name": "World Cup",
        "tournament_url": (
            f"/Regions/{region_id}/Tournaments/{tournament_id}/Show/World-Cup"
        ),
        "source_sex": None,
        "eligibility": "quarantined",
        "classification_reason": "source_sex_not_yet_observed",
        "classifier_version": "test-v1",
    }


def _discovery_season_row(
    *,
    competition_id="INT-World Cup",
    region_id=247,
    tournament_id=36,
    source_season_id=9001,
    season_id="2026",
):
    return {
        "competition_id": competition_id,
        "region_id": region_id,
        "tournament_id": tournament_id,
        "source_season_id": source_season_id,
        "season_id": season_id,
        "season_format": "single_year",
        "source_label": season_id,
        "source_url": (
            f"/Regions/{region_id}/Tournaments/{tournament_id}"
            f"/Seasons/{source_season_id}"
        ),
        "source_selected": True,
        "is_active": None,
        "eligibility": "quarantined",
        "classification_reason": "parent:source_sex_not_yet_observed",
    }


def _discovery_stage_row(
    *,
    competition_id="INT-World Cup",
    region_id=247,
    tournament_id=36,
    source_season_id=9001,
    stage_id=700,
):
    return {
        "competition_id": competition_id,
        "season": "2026",
        "season_id": "2026",
        "season_format": "single_year",
        "region_id": region_id,
        "tournament_id": tournament_id,
        "source_season_id": source_season_id,
        "stage_id": stage_id,
        "stage": "Finals",
        "stage_name": "Finals",
        "source_url": (
            f"/Regions/{region_id}/Tournaments/{tournament_id}"
            f"/Seasons/{source_season_id}/Stages/{stage_id}"
        ),
        "eligibility": "quarantined",
        "classification_reason": "parent:source_sex_not_yet_observed",
    }


def _patch_catalog_discovery(
    monkeypatch,
    *,
    competition_rows,
    season_rows=(),
    stage_rows=(),
    schedule_rows=(),
    minimum=1,
):
    """Install deterministic discovery responses without touching the network."""

    monkeypatch.setenv("WHOSCORED_CATALOG_MIN_TOURNAMENTS", str(minimum))
    monkeypatch.setattr(
        "scrapers.whoscored.service.parse_all_regions",
        lambda *_args, **_kwargs: ParsedDataset(
            "competitions", DatasetStatus.AVAILABLE, tuple(competition_rows)
        ),
    )
    monkeypatch.setattr(
        "scrapers.whoscored.service.parse_tournament_seasons",
        lambda *_args, **_kwargs: ParsedDataset(
            "seasons", DatasetStatus.AVAILABLE, tuple(season_rows)
        ),
    )
    monkeypatch.setattr(
        "scrapers.whoscored.service.parse_season_page",
        lambda *_args, **_kwargs: SimpleNamespace(
            stages=(
                ParsedDataset("stages", DatasetStatus.AVAILABLE, tuple(stage_rows))
                if stage_rows
                else ParsedDataset("stages", DatasetStatus.EMPTY)
            )
        ),
    )
    monkeypatch.setattr(
        "scrapers.whoscored.service.parse_calendar_months",
        lambda *_args, **_kwargs: (CalendarMonth(2026, 7),),
    )
    monkeypatch.setattr(
        "scrapers.whoscored.service.parse_schedule_json",
        lambda *_args, **_kwargs: (
            ParsedDataset("schedule", DatasetStatus.AVAILABLE, tuple(schedule_rows))
            if schedule_rows
            else ParsedDataset("schedule", DatasetStatus.EMPTY)
        ),
    )

    def fake_fetch(self, target, **kwargs):
        content = b"{}"
        response = TransportResponse(
            url=target.canonical_url,
            content=content,
            status_code=200,
            headers={},
            route=TransportRoute.DIRECT_HTTP,
            wire_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )
        assert kwargs["validator"](response) is True
        return response, f"s3://raw/{target.target_id}"

    monkeypatch.setattr(WhoScoredIngestService, "_fetch", fake_fetch)


class _FailingTransport:
    def __init__(self, error):
        self.error = error
        self.raw_cache = None
        self.budgets = TransportBudgets()

    def fetch(self, *args, **kwargs):
        raise self.error

    def get_traffic_stats(self):
        return {"paid_proxy_bytes": 0}

    def close(self):
        return None


class _RawPersistingContentFailureTransport:
    """Mirror transport's raw-first behavior for a validator/parser failure."""

    def __init__(self, content: bytes):
        self.content = content
        self.raw_cache = None
        self.budgets = TransportBudgets()

    def fetch(
        self,
        url,
        *,
        cache_key,
        validator,
        before_network=None,
        scope=None,
        entity=None,
        browser_bootstrap_url=None,
    ):
        del scope, entity, browser_bootstrap_url
        if before_network is not None:
            before_network()
        digest = hashlib.sha256(self.content).hexdigest()
        response = TransportResponse(
            url=url,
            content=self.content,
            status_code=200,
            headers={},
            route=TransportRoute.DIRECT_HTTP,
            wire_bytes=len(self.content),
            sha256=digest,
        )
        try:
            validator(response)
        except Exception as exc:
            self.raw_cache.store(cache_key, CachedPayload(self.content), digest)
            raise WhoScoredTransportError(
                f"content validator failed: {exc}",
                kind=FailureKind.CONTENT,
                url=url,
                route=TransportRoute.DIRECT_HTTP,
                status_code=200,
            ) from exc
        raise AssertionError("fixture content unexpectedly passed validation")

    def get_traffic_stats(self):
        return {"paid_proxy_bytes": 0}

    def close(self):
        return None


def _service(tmp_path, *, html=None):
    catalog = _catalog()
    repository = _Repository()
    raw_store = WhoScoredRawStore(fs.LocalFileSystem(), str(tmp_path / "raw"))
    service = WhoScoredIngestService(
        catalog.resolve_scope("INT-World Cup", "2026"),
        catalog=catalog,
        repository=repository,
        transport=_Transport(html or _match_html()),
        raw_store=raw_store,
    )
    return service, repository, raw_store


def test_structured_rate_limit_defaults_to_hard_max_with_four_request_burst(
    tmp_path, monkeypatch
):
    monkeypatch.delenv("WHOSCORED_STRUCTURED_REQUESTS_PER_MINUTE", raising=False)

    service, _, _ = _service(tmp_path)

    assert DEFAULT_STRUCTURED_REQUESTS_PER_MINUTE == 60
    assert service._structured_rate_limiter.config.max_requests == 60
    assert (
        service._structured_rate_limiter.config.burst_size
        == STRUCTURED_REQUEST_BURST_SIZE
        == 4
    )
    assert service._rate_limiter.config.max_requests == 30


def test_catalog_rate_limit_defaults_to_hard_max_with_four_request_burst():
    assert DEFAULT_CATALOG_REQUESTS_PER_MINUTE == 60
    assert catalog_requests_per_minute_from_env({}) == 60
    assert CATALOG_REQUEST_BURST_SIZE == 4


def test_only_exact_browser_source_header_absence_marks_stage_stats_unavailable():
    unavailable = WhoScoredTransportError(
        "FlareSolverr HTTP 502: WhoScored page request header is unavailable.",
        kind=FailureKind.BROWSER,
        url="https://www.whoscored.com/statisticsfeed/1/getteamstatistics",
    )
    ordinary_502 = WhoScoredTransportError(
        "FlareSolverr HTTP 502: Bad gateway",
        kind=FailureKind.BROWSER,
        url="https://www.whoscored.com/statisticsfeed/1/getteamstatistics",
    )
    wrong_route_kind = WhoScoredTransportError(
        "WhoScored page request header is unavailable.",
        kind=FailureKind.HTTP_STATUS,
        url="https://www.whoscored.com/statisticsfeed/1/getteamstatistics",
    )

    assert _is_source_stage_statistics_unavailable(unavailable)
    assert not _is_source_stage_statistics_unavailable(ordinary_502)
    assert not _is_source_stage_statistics_unavailable(wrong_route_kind)


@pytest.mark.parametrize("value", ["", "0", "61", "1.5", "sixty", "+60", "060"])
def test_structured_rate_limit_env_is_fail_closed(value):
    with pytest.raises(ValueError, match="WHOSCORED_STRUCTURED_REQUESTS_PER_MINUTE"):
        structured_requests_per_minute_from_env(
            {"WHOSCORED_STRUCTURED_REQUESTS_PER_MINUTE": value}
        )


@pytest.mark.parametrize("value", ["1", "4", "59", "60"])
def test_structured_rate_limit_env_accepts_only_canonical_in_range_integer(value):
    assert structured_requests_per_minute_from_env(
        {"WHOSCORED_STRUCTURED_REQUESTS_PER_MINUTE": value}
    ) == int(value)


@pytest.mark.parametrize("value", ["", "0", "61", "1.5", "sixty", "+60", "060"])
def test_catalog_rate_limit_env_is_fail_closed(value):
    with pytest.raises(ValueError, match="WHOSCORED_CATALOG_REQUESTS_PER_MINUTE"):
        catalog_requests_per_minute_from_env(
            {"WHOSCORED_CATALOG_REQUESTS_PER_MINUTE": value}
        )


@pytest.mark.parametrize("value", ["1", "4", "59", "60"])
def test_catalog_rate_limit_env_accepts_only_canonical_in_range_integer(value):
    assert catalog_requests_per_minute_from_env(
        {"WHOSCORED_CATALOG_REQUESTS_PER_MINUTE": value}
    ) == int(value)


def test_service_refuses_invalid_structured_rate_limit_before_network(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("WHOSCORED_STRUCTURED_REQUESTS_PER_MINUTE", "61")

    with pytest.raises(ValueError, match="WHOSCORED_STRUCTURED_REQUESTS_PER_MINUTE"):
        _service(tmp_path)


def test_schedule_refuses_invalid_write_chunk_before_source_resolution(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("WHOSCORED_SCOPE_WRITE_CHUNK_ROWS", "0")
    service, _, _ = _service(tmp_path)
    service._source_season_id = lambda: pytest.fail(
        "source resolution must not run for invalid memory configuration"
    )

    result = service.sync_schedule()

    assert result.succeeded == 0
    assert len(result.errors) == 1
    assert "WHOSCORED_SCOPE_WRITE_CHUNK_ROWS" in result.errors[0]


def test_structured_rate_limit_is_wired_into_example_and_airflow_environment():
    root = Path(__file__).resolve().parents[3]
    env_example = (root / ".env.example").read_text(encoding="utf-8")
    compose = (root / "compose.yaml").read_text(encoding="utf-8")

    assert "WHOSCORED_STRUCTURED_REQUESTS_PER_MINUTE=60" in env_example
    assert "WHOSCORED_SCOPE_WRITE_CHUNK_ROWS=20000" in env_example
    assert "WHOSCORED_CATALOG_REQUESTS_PER_MINUTE=60" in env_example
    assert (
        "WHOSCORED_STRUCTURED_REQUESTS_PER_MINUTE: "
        "${WHOSCORED_STRUCTURED_REQUESTS_PER_MINUTE:-60}"
    ) in compose
    assert (
        "WHOSCORED_SCOPE_WRITE_CHUNK_ROWS: ${WHOSCORED_SCOPE_WRITE_CHUNK_ROWS:-20000}"
    ) in compose
    assert (
        "WHOSCORED_CATALOG_REQUESTS_PER_MINUTE: "
        "${WHOSCORED_CATALOG_REQUESTS_PER_MINUTE:-60}"
    ) in compose


def test_discover_catalog_rejects_implausibly_small_initial_all_regions(
    tmp_path, monkeypatch
):
    repository = _CatalogRepository()
    _patch_catalog_discovery(
        monkeypatch,
        competition_rows=(_discovery_competition_row(),),
        minimum=2,
    )

    result = WhoScoredIngestService.discover_catalog(
        repository=repository,
        transport=_CatalogTransport(),
        raw_store=object(),
        full_history=True,
    )

    assert result.status == "failed"
    assert any("implausibly small" in error for error in result.errors)
    assert repository.persisted == []


def test_discover_catalog_automatically_bootstraps_full_history_before_initial_publication(
    tmp_path, monkeypatch
):
    repository = _CatalogRepository()
    competition = _discovery_competition_row()
    season = _discovery_season_row()
    stage = _discovery_stage_row()
    schedule = {
        "date": datetime.combine(date.today(), datetime.min.time()),
        "region_id": 247,
        "tournament_id": 36,
        "source_sex": 1,
    }
    _patch_catalog_discovery(
        monkeypatch,
        competition_rows=(competition,),
        season_rows=(season,),
        stage_rows=(stage,),
        schedule_rows=(schedule,),
        minimum=1,
    )
    raw_store = WhoScoredRawStore(fs.LocalFileSystem(), str(tmp_path / "raw"))

    result = WhoScoredIngestService.discover_catalog(
        repository=repository,
        transport=_CatalogTransport(),
        raw_store=raw_store,
    )

    assert result.status == "success"
    assert result.counts["full_history"] == 1
    assert len(repository.persisted) == 1
    metadata = repository.persisted[0][1]
    assert metadata["discovery_mode"] == "full_history"
    assert len(metadata["raw_inputs"]) == 1
    descriptor = metadata["raw_inputs"][0]
    assert descriptor["input_count"] > 1
    target = RawTarget(
        source="whoscored",
        page_kind="catalog_provenance",
        target_id=descriptor["target_id"],
        canonical_url=(
            "https://www.whoscored.com/catalog/provenance/"
            f"{metadata['discovery_batch_id']}"
        ),
        source_ids={"discovery_batch_id": metadata["discovery_batch_id"]},
    )
    payload, record = raw_store.load_bytes(target)
    assert len(json.loads(payload)) == descriptor["input_count"]
    assert record.content_hash == descriptor["payload_sha256"]
    assert metadata["raw_uri"] == raw_store.object_uri(record.blob_key)


def test_discovery_batch_identity_includes_normalized_raw_provenance(
    tmp_path, monkeypatch
):
    competition = _discovery_competition_row()
    season = _discovery_season_row()
    stage = _discovery_stage_row()
    schedule = {
        "date": datetime.combine(date.today(), datetime.min.time()),
        "region_id": 247,
        "tournament_id": 36,
        "source_sex": 1,
    }
    _patch_catalog_discovery(
        monkeypatch,
        competition_rows=(competition,),
        season_rows=(season,),
        stage_rows=(stage,),
        schedule_rows=(schedule,),
    )
    source_revision = [b"source-revision-one"]

    def fake_fetch(self, target, **kwargs):
        content = source_revision[0]
        response = TransportResponse(
            url=target.canonical_url,
            content=content,
            status_code=200,
            headers={},
            route=TransportRoute.DIRECT_HTTP,
            wire_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )
        assert kwargs["validator"](response) is True
        return response, f"s3://raw/{target.target_id}"

    monkeypatch.setattr(WhoScoredIngestService, "_fetch", fake_fetch)
    first_repository = _CatalogRepository()
    first = WhoScoredIngestService.discover_catalog(
        repository=first_repository,
        transport=_CatalogTransport(),
        raw_store=object(),
        full_history=True,
    )
    source_revision[0] = b"source-revision-two"
    second_repository = _CatalogRepository()
    second = WhoScoredIngestService.discover_catalog(
        repository=second_repository,
        transport=_CatalogTransport(),
        raw_store=object(),
        full_history=True,
    )

    assert first.status == second.status == "success"
    first_metadata = first_repository.persisted[0][1]
    second_metadata = second_repository.persisted[0][1]
    assert first_metadata["payload_sha256"] == second_metadata["payload_sha256"]
    assert first_metadata["discovery_batch_id"] != second_metadata["discovery_batch_id"]
    assert first_metadata["raw_inputs"] == sorted(
        first_metadata["raw_inputs"],
        key=lambda item: json.dumps(
            item,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


def test_discover_catalog_rejects_loss_of_previously_published_tournament(
    tmp_path, monkeypatch
):
    repository = _CatalogRepository(previous=_catalog())
    _patch_catalog_discovery(
        monkeypatch,
        competition_rows=(_discovery_competition_row(),),
    )

    result = WhoScoredIngestService.discover_catalog(
        repository=repository,
        transport=_CatalogTransport(),
        raw_store=object(),
        full_history=True,
    )

    assert result.status == "failed"
    assert any(
        "lost previously published tournaments" in error and "252/2" in error
        for error in result.errors
    )
    assert repository.persisted == []


def test_discover_catalog_rejects_loss_of_previously_published_season(
    tmp_path, monkeypatch
):
    previous = WhoScoredCatalog.from_mapping(
        {
            "competitions": [
                {
                    "id": "INT-World Cup",
                    "seasons": [
                        {
                            "id": 2022,
                            "season_format": "single_year",
                            "source_season_id": 8000,
                        },
                        {
                            "id": 2026,
                            "season_format": "single_year",
                            "source_season_id": 9001,
                        },
                    ],
                    "sources": {
                        "primary": ["whoscored"],
                        "whoscored": {"region_id": 247, "tournament_id": 36},
                    },
                }
            ]
        }
    )
    repository = _CatalogRepository(previous=previous)
    schedule = {
        "date": datetime(2026, 7, 11, 19),
        "region_id": 247,
        "tournament_id": 36,
        "source_sex": 1,
    }
    _patch_catalog_discovery(
        monkeypatch,
        competition_rows=(_discovery_competition_row(),),
        season_rows=(_discovery_season_row(),),
        stage_rows=(_discovery_stage_row(),),
        schedule_rows=(schedule,),
    )

    result = WhoScoredIngestService.discover_catalog(
        repository=repository,
        transport=_CatalogTransport(),
        raw_store=object(),
    )

    assert result.status == "failed"
    assert any(
        "season menus lost previously published seasons" in error
        and "INT-World Cup/8000" in error
        for error in result.errors
    )
    assert repository.persisted == []


def test_full_history_discovery_rejects_loss_of_previously_published_stage(
    tmp_path, monkeypatch
):
    competition = _discovery_competition_row()
    competition.update(
        {
            "source_sex": 1,
            "eligibility": "included",
            "classification_reason": "source_sex_male_no_youth_marker",
        }
    )
    season = _discovery_season_row()
    season.update(
        {
            "eligibility": "included",
            "classification_reason": "parent:source_sex_male_no_youth_marker",
            "is_active": True,
        }
    )
    stages = (
        _discovery_stage_row(stage_id=700),
        _discovery_stage_row(stage_id=701),
    )
    previous = WhoScoredCatalog.from_rows(
        {
            "competitions": (competition,),
            "seasons": (season,),
            "stages": stages,
        }
    )
    repository = _CatalogRepository(previous=previous)
    schedule = {
        "date": datetime(2026, 7, 11, 19),
        "region_id": 247,
        "tournament_id": 36,
        "source_sex": 1,
    }
    _patch_catalog_discovery(
        monkeypatch,
        competition_rows=(_discovery_competition_row(),),
        season_rows=(_discovery_season_row(),),
        stage_rows=(stages[0],),
        schedule_rows=(schedule,),
    )

    result = WhoScoredIngestService.discover_catalog(
        repository=repository,
        transport=_CatalogTransport(),
        raw_store=object(),
        full_history=True,
    )

    assert result.status == "failed"
    assert any(
        "lost previously published stages" in error and "/701" in error
        for error in result.errors
    )
    assert repository.persisted == []


def test_discover_catalog_quarantines_active_candidate_without_fixture_date(
    tmp_path, monkeypatch
):
    repository = _CatalogRepository()
    schedule_without_date = {
        "date": None,
        "region_id": 247,
        "tournament_id": 36,
        "source_sex": 1,
    }
    _patch_catalog_discovery(
        monkeypatch,
        competition_rows=(_discovery_competition_row(),),
        season_rows=(_discovery_season_row(),),
        stage_rows=(_discovery_stage_row(),),
        schedule_rows=(schedule_without_date,),
    )
    captured = {}
    real_from_rows = WhoScoredCatalog.from_rows

    def capture_rows(cls, rows):
        captured.update(
            {name: tuple(dict(row) for row in values) for name, values in rows.items()}
        )
        return real_from_rows(rows)

    monkeypatch.setattr(WhoScoredCatalog, "from_rows", classmethod(capture_rows))

    result = WhoScoredIngestService.discover_catalog(
        repository=repository,
        transport=_CatalogTransport(),
        raw_store=object(),
        full_history=True,
    )

    assert result.status == "failed"
    assert any("catalog completeness" in error for error in result.errors)
    assert result.counts["quarantined_competitions"] == 0
    assert result.counts["quarantined_seasons"] == 1
    assert result.counts["quarantined_stages"] == 1
    assert result.counts["quarantined_unique_competitions"] == 1
    assert any(
        "active_candidate_has_no_fixture_date_evidence=1" in error
        and "season:active_candidate_has_no_fixture_date_evidence=1" in error
        and "INT-World Cup" in error
        for error in result.errors
    )
    assert repository.persisted == []
    assert captured["seasons"][0]["is_active"] is None
    assert captured["seasons"][0]["eligibility"] == "quarantined"
    assert (
        captured["seasons"][0]["classification_reason"]
        == "active_candidate_has_no_fixture_date_evidence"
    )
    assert captured["stages"][0]["eligibility"] == "quarantined"
    assert captured["stages"][0]["classification_reason"].startswith("season:")


def test_discover_catalog_marks_structurally_empty_calendars_source_unavailable(
    tmp_path, monkeypatch
):
    competition = _discovery_competition_row()
    competition.update(
        {
            "source_sex": None,
            "eligibility": "included",
            "classification_reason": (
                "explicit_override:audited senior men: empty source calendar"
            ),
            "override_version": "review-1",
        }
    )
    season = _discovery_season_row()
    season.update(
        {
            "eligibility": "included",
            "classification_reason": "parent:explicit_override",
        }
    )
    stage = _discovery_stage_row()
    stage.update(
        {
            "eligibility": "included",
            "classification_reason": "parent:explicit_override",
        }
    )
    repository = _CatalogRepository()
    _patch_catalog_discovery(
        monkeypatch,
        competition_rows=(competition,),
        season_rows=(season,),
        stage_rows=(stage,),
    )
    monkeypatch.setattr(
        "scrapers.whoscored.service.parse_calendar_months",
        lambda *_args, **_kwargs: (),
    )

    result = WhoScoredIngestService.discover_catalog(
        repository=repository,
        transport=_CatalogTransport(),
        raw_store=object(),
        full_history=True,
    )

    assert result.status == "success", result.as_dict()
    persisted = repository.persisted[0][0]
    rows = persisted.to_rows()
    assert rows["seasons"][0]["eligibility"] == "source_unavailable"
    assert rows["seasons"][0]["classification_reason"] == "season_stage_calendars_empty"
    assert rows["stages"][0]["eligibility"] == "source_unavailable"
    assert rows["stages"][0]["classification_reason"].startswith("season:")
    assert persisted.quarantined == ()
    assert persisted.enabled_scopes() == ()


def test_discover_catalog_falls_back_from_unavailable_future_season(
    tmp_path, monkeypatch
):
    repository = _CatalogRepository()
    older = _discovery_season_row(source_season_id=9001, season_id="2026")
    older["source_selected"] = False
    future = _discovery_season_row(source_season_id=9002, season_id="2027")
    ancient = _discovery_season_row(source_season_id=100, season_id="9900")
    ancient.update(
        {
            "season_format": "split_year",
            "source_label": "1999/2000",
            "source_selected": False,
        }
    )
    schedule = {
        "date": datetime(2026, 7, 11, 19),
        "region_id": 247,
        "tournament_id": 36,
        "source_sex": 1,
    }
    _patch_catalog_discovery(
        monkeypatch,
        competition_rows=(_discovery_competition_row(),),
        # The source dropdown is newest-first. Lexical canonical-season sorting
        # would incorrectly put 9900 between the unavailable future and 2026.
        season_rows=(future, older, ancient),
        schedule_rows=(schedule,),
    )
    monkeypatch.setattr(
        "scrapers.whoscored.service.parse_season_page",
        lambda *_args, source_season_id, **_kwargs: SimpleNamespace(
            stages=ParsedDataset(
                "stages",
                DatasetStatus.AVAILABLE,
                (
                    _discovery_stage_row(
                        source_season_id=source_season_id,
                        stage_id=700,
                    ),
                ),
            )
        ),
    )

    visited_source_seasons = []

    def fake_fetch(self, target, **kwargs):
        if target.page_kind == "season_stages":
            visited_source_seasons.append(int(target.source_ids["source_season_id"]))
        if (
            target.page_kind == "season_stages"
            and target.source_ids.get("source_season_id") == "9002"
        ):
            raise WhoScoredTransportError(
                "HTTP 403",
                kind=FailureKind.HTTP_STATUS,
                url=target.canonical_url,
                route=TransportRoute.DIRECT_HTTP,
                status_code=403,
            )
        content = b"{}"
        response = TransportResponse(
            url=target.canonical_url,
            content=content,
            status_code=200,
            headers={},
            route=TransportRoute.DIRECT_HTTP,
            wire_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )
        assert kwargs["validator"](response) is True
        return response, f"s3://raw/{target.target_id}"

    monkeypatch.setattr(WhoScoredIngestService, "_fetch", fake_fetch)

    result = WhoScoredIngestService.discover_catalog(
        repository=repository,
        transport=_CatalogTransport(),
        raw_store=object(),
        full_history=True,
    )

    assert result.status == "success", result.as_dict()
    persisted = repository.persisted[0][0]
    by_source = {
        int(row["source_season_id"]): row for row in persisted.to_rows()["seasons"]
    }
    assert by_source[9002]["eligibility"] == "source_unavailable"
    assert by_source[9002]["is_active"] is False
    assert by_source[9001]["eligibility"] == "included"
    assert by_source[9001]["is_active"] is True
    assert by_source[100]["is_active"] is False
    assert visited_source_seasons[:2] == [9002, 9001]


def test_discover_catalog_uses_fixture_interval_for_long_qualification(
    tmp_path, monkeypatch
):
    repository = _CatalogRepository()
    season = _discovery_season_row(source_season_id=9100, season_id="2527")
    season.update(
        {
            "season_format": "multi_year",
            "source_label": "2025/2027",
        }
    )
    stage = _discovery_stage_row(source_season_id=9100, stage_id=710)
    _patch_catalog_discovery(
        monkeypatch,
        competition_rows=(_discovery_competition_row(),),
        season_rows=(season,),
        stage_rows=(stage,),
    )
    monkeypatch.setattr(
        "scrapers.whoscored.service.parse_calendar_months",
        lambda *_args, **_kwargs: (
            CalendarMonth(2025, 9),
            CalendarMonth(2027, 3),
        ),
    )

    def parse_schedule(payload, **_kwargs):
        url = payload.decode("utf-8")
        kickoff = datetime(2025, 9, 1) if "d=202509" in url else datetime(2027, 3, 1)
        return ParsedDataset(
            "schedule",
            DatasetStatus.AVAILABLE,
            (
                {
                    "date": kickoff,
                    "region_id": 247,
                    "tournament_id": 36,
                    "source_sex": 1,
                },
            ),
        )

    monkeypatch.setattr(
        "scrapers.whoscored.service.parse_schedule_json", parse_schedule
    )
    schedule_fetches = []

    def fake_fetch(self, target, **kwargs):
        if target.page_kind == "schedule_month":
            schedule_fetches.append(target.canonical_url)
        content = target.canonical_url.encode("utf-8")
        response = TransportResponse(
            url=target.canonical_url,
            content=content,
            status_code=200,
            headers={},
            route=TransportRoute.DIRECT_HTTP,
            wire_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )
        assert kwargs["validator"](response) is True
        return response, f"s3://raw/{target.target_id}"

    monkeypatch.setattr(WhoScoredIngestService, "_fetch", fake_fetch)

    result = WhoScoredIngestService.discover_catalog(
        repository=repository,
        transport=_CatalogTransport(),
        raw_store=object(),
        full_history=True,
    )

    assert result.status == "success", result.as_dict()
    persisted = repository.persisted[0][0]
    discovered = persisted.resolve_scope("INT-World Cup", "2527")
    assert discovered.is_active is True
    assert discovered.start == date(2025, 9, 1)
    assert discovered.end == date(2027, 3, 1)
    assert len(schedule_fetches) == 2


def test_discover_catalog_skips_distant_future_fixtures_for_current_edition(
    tmp_path, monkeypatch
):
    repository = _CatalogRepository()
    future = _discovery_season_row(source_season_id=9002, season_id="2027")
    current = _discovery_season_row(source_season_id=9001, season_id="2026")
    current["source_selected"] = False
    _patch_catalog_discovery(
        monkeypatch,
        competition_rows=(_discovery_competition_row(),),
        season_rows=(future, current),
    )
    monkeypatch.setattr(
        "scrapers.whoscored.service.parse_season_page",
        lambda *_args, source_season_id, **_kwargs: SimpleNamespace(
            stages=ParsedDataset(
                "stages",
                DatasetStatus.AVAILABLE,
                (
                    _discovery_stage_row(
                        source_season_id=source_season_id,
                        stage_id=701 if source_season_id == 9002 else 700,
                    ),
                ),
            )
        ),
    )
    monkeypatch.setattr(
        "scrapers.whoscored.service.parse_calendar_months",
        lambda html: (
            (CalendarMonth(2027, 12),)
            if "/Stages/701" in html
            else (CalendarMonth(2026, 7),)
        ),
    )

    def parse_schedule(payload, **_kwargs):
        url = payload.decode("utf-8")
        kickoff = datetime(2027, 12, 1) if "d=202712" in url else datetime(2026, 7, 11)
        return ParsedDataset(
            "schedule",
            DatasetStatus.AVAILABLE,
            (
                {
                    "date": kickoff,
                    "region_id": 247,
                    "tournament_id": 36,
                    "source_sex": 1,
                },
            ),
        )

    monkeypatch.setattr(
        "scrapers.whoscored.service.parse_schedule_json", parse_schedule
    )

    def fake_fetch(self, target, **kwargs):
        content = target.canonical_url.encode("utf-8")
        response = TransportResponse(
            url=target.canonical_url,
            content=content,
            status_code=200,
            headers={},
            route=TransportRoute.DIRECT_HTTP,
            wire_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )
        assert kwargs["validator"](response) is True
        return response, f"s3://raw/{target.target_id}"

    monkeypatch.setattr(WhoScoredIngestService, "_fetch", fake_fetch)

    result = WhoScoredIngestService.discover_catalog(
        repository=repository,
        transport=_CatalogTransport(),
        raw_store=object(),
        full_history=True,
    )

    assert result.status == "success", result.as_dict()
    persisted = repository.persisted[0][0]
    assert persisted.resolve_scope("INT-World Cup", "2027").is_active is False
    assert persisted.resolve_scope("INT-World Cup", "2026").is_active is True


def test_daily_discovery_preserves_unvisited_source_unavailable_season(
    tmp_path, monkeypatch
):
    competition = _discovery_competition_row()
    competition.update(
        {
            "source_sex": 1,
            "eligibility": "included",
            "classification_reason": "source_sex_male_no_youth_marker",
        }
    )
    current = _discovery_season_row(source_season_id=9001, season_id="2026")
    current.update(
        {
            "eligibility": "included",
            "classification_reason": "parent:source_sex_male_no_youth_marker",
            "is_active": True,
        }
    )
    historical = _discovery_season_row(source_season_id=8000, season_id="2022")
    historical.update(
        {
            "source_selected": False,
            "start": "2022-11-20",
            "end": "2022-12-18",
            "is_active": False,
            "eligibility": "source_unavailable",
            "classification_reason": "season_stage_page_http_404",
        }
    )
    stage = _discovery_stage_row(source_season_id=9001, stage_id=700)
    stage.update(
        {
            "eligibility": "included",
            "classification_reason": "parent:source_sex_male_no_youth_marker",
        }
    )
    previous = WhoScoredCatalog.from_rows(
        {
            "competitions": (competition,),
            "seasons": (current, historical),
            "stages": (stage,),
        }
    )
    repository = _CatalogRepository(previous=previous)
    schedule = {
        "date": datetime(2026, 7, 11, 19),
        "region_id": 247,
        "tournament_id": 36,
        "source_sex": 1,
    }
    fresh_current = dict(current)
    fresh_current["is_active"] = None
    fresh_historical = dict(historical)
    fresh_historical.update(
        {
            "start": None,
            "end": None,
            "is_active": None,
            "eligibility": "quarantined",
            "classification_reason": "parent:source_sex_not_yet_observed",
        }
    )
    _patch_catalog_discovery(
        monkeypatch,
        competition_rows=(_discovery_competition_row(),),
        season_rows=(fresh_current, fresh_historical),
        stage_rows=(stage,),
        schedule_rows=(schedule,),
    )

    result = WhoScoredIngestService.discover_catalog(
        repository=repository,
        transport=_CatalogTransport(),
        raw_store=object(),
    )

    assert result.status == "success", result.as_dict()
    persisted = repository.persisted[0][0]
    retained = persisted.resolve_scope("INT-World Cup", "2022")
    assert retained.eligibility.value == "source_unavailable"
    assert retained.start == date(2022, 11, 20)
    assert retained.end == date(2022, 12, 18)


def test_ttl_cache_replays_same_day_then_refreshes_once_after_expiry(tmp_path):
    service, _, raw_store = _service(tmp_path)
    transport = _RawCacheAwareTransport(b'{"schedule":"fresh"}')
    service.transport = transport
    rate_limit_calls = []
    service._rate_limiter = SimpleNamespace(
        acquire=lambda: rate_limit_calls.append("acquired")
    )
    target = schedule_month_target(700, 2026, 7)

    first, _ = service._fetch(
        target,
        validator=lambda _response: True,
        content_type="application/json",
        cache_ttl=ACTIVE_SCHEDULE_CACHE_TTL,
    )
    second, _ = service._fetch(
        target,
        validator=lambda _response: True,
        content_type="application/json",
        cache_ttl=ACTIVE_SCHEDULE_CACHE_TTL,
    )

    assert first.route is TransportRoute.DIRECT_HTTP
    assert second.route is TransportRoute.RAW_CACHE
    assert transport.network_calls == 1
    assert rate_limit_calls == ["acquired"]

    # Append-only raw history intentionally refuses to let an older delayed
    # writer replace the current receipt. Explicitly invalidate the current
    # test fixture before installing an expired version.
    _, current_record = raw_store.load_bytes(target)
    raw_store.quarantine(
        target,
        reason="expire TTL cache test fixture",
        record=current_record,
    )
    raw_store.store_bytes(
        target,
        transport.content,
        content_type="application/json",
        fetched_at=(datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(),
    )
    expired, _ = service._fetch(
        target,
        validator=lambda _response: True,
        content_type="application/json",
        cache_ttl=ACTIVE_SCHEDULE_CACHE_TTL,
    )

    assert expired.route is TransportRoute.DIRECT_HTTP
    assert transport.network_calls == 2
    assert rate_limit_calls == ["acquired", "acquired"]


def test_parsed_batch_keeps_independent_raw_objects_validators_and_warm_replay(
    tmp_path,
):
    class LockAwareRawStore:
        def __init__(self, backend, expected_locks):
            self.backend = backend
            self.expected_locks = expected_locks
            self.active_locks = 0
            self.freshness_checks = 0

        @contextmanager
        def target_lock(self, target):
            with self.backend.target_lock(target):
                self.active_locks += 1
                try:
                    yield
                finally:
                    self.active_locks -= 1

        def is_fresh(self, target, *, max_age):
            assert self.active_locks == self.expected_locks
            self.freshness_checks += 1
            return self.backend.is_fresh(target, max_age=max_age)

        def __getattr__(self, name):
            return getattr(self.backend, name)

    class BatchTransport:
        def __init__(self, bodies):
            self.raw_cache = None
            self.bodies = bodies
            self.network_urls = []

        def fetch_many(self, requests):
            responses = []
            for request in requests:
                cached = self.raw_cache.load(request.cache_key)
                if cached is None:
                    request.before_network()
                    self.network_urls.append(request.url)
                    content = self.bodies[request.url]
                    route = TransportRoute.DIRECT_FLARESOLVERR
                    wire_bytes = len(content)
                else:
                    content = cached.content
                    route = TransportRoute.RAW_CACHE
                    wire_bytes = 0
                digest = hashlib.sha256(content).hexdigest()
                response = TransportResponse(
                    url=request.url,
                    content=content,
                    status_code=200,
                    headers={"content-type": "application/json"},
                    route=route,
                    wire_bytes=wire_bytes,
                    sha256=digest,
                )
                assert request.validator(response) is True
                if cached is None:
                    self.raw_cache.store(
                        request.cache_key, CachedPayload(content), digest
                    )
                responses.append(response)
            return responses

    service, _, raw_store = _service(tmp_path)
    lock_aware_store = LockAwareRawStore(raw_store, expected_locks=2)
    service.raw_store = lock_aware_store
    bootstrap = (
        "https://www.whoscored.com/Regions/247/Tournaments/36/Seasons/9001/"
        "Stages/700/TeamStatistics"
    )
    targets = [
        RawTarget(
            source="whoscored",
            page_kind="team_stage_statistics",
            target_id=f"whoscored:batch:{index}",
            canonical_url=(
                "https://www.whoscored.com/statisticsfeed/1/getteamstatistics"
                f"?stageId=700&category={index}"
            ),
            source_ids={"stage_id": "700", "category": str(index)},
        )
        for index in range(2)
    ]
    bodies = {
        targets[0].canonical_url: b'{"value":1}',
        targets[1].canonical_url: b'{"value":2}',
    }
    specs = [
        _ParsedFetchSpec(
            target=target,
            parser=lambda response: json.loads(response.content)["value"],
            content_type="application/json",
            cache_ttl=timedelta(hours=1),
            browser_bootstrap_url=bootstrap,
        )
        for target in targets
    ]
    cold_transport = BatchTransport(bodies)
    service.transport = cold_transport
    service._rate_limiter = SimpleNamespace(acquire=lambda: True)

    cold = service._fetch_parsed_many(specs)

    assert [parsed for _, _, parsed in cold] == [1, 2]
    assert cold_transport.network_urls == [target.canonical_url for target in targets]
    assert len({uri for _, uri, _ in cold}) == 2
    assert lock_aware_store.freshness_checks == 2
    for target, expected in zip(targets, (b'{"value":1}', b'{"value":2}')):
        content, _ = raw_store.load_bytes(target)
        assert content == expected

    warm_transport = BatchTransport(bodies)
    service.transport = warm_transport
    warm = service._fetch_parsed_many(specs)

    assert [response.route for response, _, _ in warm] == [
        TransportRoute.RAW_CACHE,
        TransportRoute.RAW_CACHE,
    ]
    assert warm_transport.network_urls == []
    assert lock_aware_store.freshness_checks == 4


def test_schedule_cache_policy_ttls_only_mutable_active_targets(tmp_path, monkeypatch):
    class _FixedDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 7, 11)

    monkeypatch.setattr("scrapers.whoscored.service.date", _FixedDate)
    service, repository, _ = _service(tmp_path)
    service.catalog_season = replace(service.catalog_season, end=None)
    service._source_season_id = lambda: 9001
    calls = []
    calendar_urls = []
    team_statistics_urls = []
    player_statistics_urls = []
    team_stage_feed_urls = []
    stage_html = """
    <select id="stages"><option
      value="/Regions/247/Tournaments/36/Seasons/9001/Stages/700/Fixtures/world-cup-2026">
      Group Stage</option></select>
    """
    calendar_html = """
    <script>var wsCalendar = {mask:{2026:{0:{1:1},6:{1:1}}}};</script>
    """

    def fake_fetch(target, **kwargs):
        calls.append((target.page_kind, dict(target.source_ids), kwargs))
        if target.page_kind == "stage_calendar":
            calendar_urls.append(target.canonical_url)
        elif target.page_kind == "team_stage_statistics":
            team_statistics_urls.append(target.canonical_url)
        elif target.page_kind == "player_stage_statistics":
            player_statistics_urls.append(target.canonical_url)
        elif target.page_kind == "team_stage_feed":
            team_stage_feed_urls.append(target.canonical_url)
        if target.page_kind == "season_stages":
            content = stage_html.encode()
        elif target.page_kind == "stage_calendar":
            content = calendar_html.encode()
        elif target.page_kind == "team_stage_statistics":
            content = b'{"teamTableStats": []}'
        elif target.page_kind == "player_stage_statistics":
            content = b'{"playerTableStats": []}'
        elif target.page_kind == "team_stage_feed":
            content = b"[[]]"
        elif target.page_kind == "referee_stage_statistics":
            content = b"<html><body><h1>Referee Statistics</h1></body></html>"
        else:
            token = target.source_ids["month"]
            month = int(token[-2:])
            payload = {
                "tournaments": [
                    {
                        "matches": [
                            {
                                "id": 1000 + month,
                                "startTimeUtc": f"2026-{month:02d}-11T19:00:00Z",
                                "homeTeamName": "Home",
                                "awayTeamName": "Away",
                                "homeTeamId": 1,
                                "awayTeamId": 2,
                                "status": 6,
                                "matchIsOpta": True,
                                "hasPreview": True,
                                "bets": {
                                    "home": {
                                        "betName": "Home",
                                        "betId": f"bet-{month}",
                                        "offers": [
                                            {
                                                "providerId": 23,
                                                "bettingProvider": "B3",
                                                "oddsDecimal": "2.25",
                                                "oddsFractional": "5/4",
                                                "oddsUS": "+125",
                                                "clickOutUrl": "https://book.test/bet",
                                            }
                                        ],
                                    }
                                },
                            }
                        ]
                    }
                ]
            }
            content = json.dumps(payload).encode()
        response = TransportResponse(
            url=target.canonical_url,
            content=content,
            status_code=200,
            headers={},
            route=TransportRoute.DIRECT_HTTP,
            wire_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )
        assert kwargs["validator"](response) is True
        return response, f"s3://raw/{target.target_id}"

    service._fetch = fake_fetch
    structured_batch_sizes = []

    def fetch_parsed_many(specs):
        specs = tuple(specs)
        structured_batch_sizes.append(len(specs))
        return [
            service._fetch_parsed(
                spec.target,
                parser=spec.parser,
                content_type=spec.content_type,
                allow_cache=spec.allow_cache,
                cache_ttl=spec.cache_ttl,
                browser_bootstrap_url=spec.browser_bootstrap_url,
            )
            for spec in specs
        ]

    service._fetch_parsed_many = fetch_parsed_many

    result = service.sync_schedule()

    assert result.status == "success", result.as_dict()
    assert result.metadata == {
        "source_stage_ids": [700],
        "source_stage_count": 1,
    }
    assert sum(structured_batch_sizes) == 17 + 2 * len(DETAILED_FEED_OPTIONS)
    assert max(structured_batch_sizes) == STRUCTURED_PARSE_BATCH_SIZE
    assert result.counts == {
        "schedule": 2,
        "match_incidents": 0,
        "match_bets": 2,
        "stage_standings": 0,
        "stage_forms": 0,
        "stage_streaks": 0,
        "stage_performance": 0,
        "team_stage_stats": 0,
        "player_stage_stats": 0,
        "referee_stage_stats": 0,
    }
    assert service.transport.budgets.max_paid_urls == 3
    policy = {
        (kind, ids.get("month")): kwargs.get("cache_ttl") for kind, ids, kwargs in calls
    }
    assert policy[("season_stages", None)] == ACTIVE_SCHEDULE_CACHE_TTL
    assert policy[("stage_calendar", None)] == ACTIVE_SCHEDULE_CACHE_TTL
    assert calendar_urls == [
        "https://www.whoscored.com/Regions/247/Tournaments/36/Seasons/9001/"
        "Stages/700/Fixtures/world-cup-2026"
    ]
    assert policy[("schedule_month", "202607")] == ACTIVE_SCHEDULE_CACHE_TTL
    assert policy[("schedule_month", "202601")] is None
    assert all(kwargs["allow_cache"] is True for _, _, kwargs in calls)

    team_params = [
        parse_qs(urlparse(url).query, keep_blank_values=True)
        for url in team_statistics_urls
    ]
    assert {urlparse(url).path for url in team_statistics_urls} == {
        "/statisticsfeed/1/getteamstatistics"
    }
    team_summary = team_params[:4]
    team_detailed = team_params[4:]
    assert [
        (params["category"][0], params["subcategory"][0]) for params in team_summary
    ] == [
        ("summaryteam", "all"),
        ("summaryteam", "offensive"),
        ("summaryteam", "defensive"),
        ("xg-teamstats", "summary"),
    ]
    assert [
        (params["category"][0], params["subcategory"][0]) for params in team_detailed
    ] == [(option.category, option.subcategory) for option in DETAILED_FEED_OPTIONS]
    assert all(params["page"] == [""] for params in team_params)
    assert all(params["numberOfTeamsToPick"] == [""] for params in team_params)
    assert all(params["incPens"] == [""] for params in team_summary[:3])
    assert all(params["against"] == [""] for params in team_summary[:3])
    assert team_summary[-1]["sortAscending"] == ["false"]
    assert team_summary[-1]["incPens"] == ["true"]
    assert team_summary[-1]["against"] == ["false"]
    assert all(params["sortBy"] == [""] for params in team_detailed)
    assert all(params["incPens"] == [""] for params in team_detailed)
    assert all(params["against"] == [""] for params in team_detailed)
    statistics_bootstrap = (
        "https://www.whoscored.com/Regions/247/Tournaments/36/Seasons/9001/"
        "Stages/700/TeamStatistics"
    )
    assert {
        kwargs["browser_bootstrap_url"]
        for kind, _, kwargs in calls
        if kind in {"team_stage_statistics", "player_stage_statistics"}
    } == {statistics_bootstrap}

    player_params = [
        parse_qs(urlparse(url).query, keep_blank_values=True)
        for url in player_statistics_urls
    ]
    assert {urlparse(url).path for url in player_statistics_urls} == {
        "/statisticsfeed/1/getplayerstatistics"
    }
    player_summary = player_params[:5]
    player_detailed = player_params[5:]
    assert [
        (params["category"][0], params["subcategory"][0]) for params in player_summary
    ] == [
        ("summary", "all"),
        ("summary", "defensive"),
        ("summary", "offensive"),
        ("summary", "passing"),
        ("xg-stats", "summary"),
    ]
    assert [
        (params["category"][0], params["subcategory"][0]) for params in player_detailed
    ] == [(option.category, option.subcategory) for option in DETAILED_FEED_OPTIONS]
    assert all(params["isMinApp"] == ["true"] for params in player_params)
    assert all(params["numberOfPlayersToPick"] == ["5000"] for params in player_params)
    assert all("incPens" not in params for params in player_summary[:-1])
    assert player_summary[-1]["incPens"] == ["true"]
    assert all("incPens" not in params for params in player_detailed)
    assert all(params["sortBy"] == [""] for params in player_detailed)

    stage_feed_params = [
        parse_qs(urlparse(url).query, keep_blank_values=True)
        for url in team_stage_feed_urls
    ]
    assert [int(params["type"][0]) for params in stage_feed_params] == [
        2,
        3,
        6,
        7,
        8,
        11,
        18,
        25,
    ]
    assert all(params["teamId"] == ["-1"] for params in stage_feed_params)
    assert all(params["field"] == ["2"] for params in stage_feed_params[:-1])
    assert all(params["against"] == ["0"] for params in stage_feed_params[:-1])
    assert stage_feed_params[-1]["field"] == ["-1"]
    assert stage_feed_params[-1]["against"] == ["-1"]

    assert len(repository.scope_snapshots) == 1
    snapshot = repository.scope_snapshots[0]
    assert set(snapshot["datasets"]) == {
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
    }
    assert snapshot["entity_group"] == "season"
    assert len(snapshot["datasets"]["whoscored_match_bets"]) == 2
    assert len(snapshot["feed_states"]) == 68
    assert set(snapshot["feed_states"].values()) == {"empty"}

    calls.clear()
    service.catalog_season = replace(service.catalog_season, end=date(2020, 1, 1))
    historical = service.sync_schedule()
    assert historical.status == "success", historical.as_dict()
    assert all(kwargs.get("cache_ttl") is None for _, _, kwargs in calls)


def test_player_stage_statistics_fetches_every_declared_page(tmp_path):
    service, _, _ = _service(tmp_path)
    spec = service._player_stage_statistics_spec(
        stage_id=700,
        source_season_id=9001,
        active=True,
        category="summary",
        subcategory="all",
        cache_ttl=timedelta(hours=1),
        browser_bootstrap_url=(
            "https://www.whoscored.com/Regions/247/Tournaments/36/Seasons/9001/"
            "Stages/700/TeamStatistics"
        ),
    )

    def response_for(current_page, player_id, url):
        content = json.dumps(
            {
                "playerTableStats": [
                    {"playerId": player_id, "teamId": 26, "apps": current_page}
                ],
                "paging": {
                    "currentPage": current_page,
                    "pageIndex": current_page - 1,
                    "totalPages": 2,
                    "totalResults": 2,
                    "resultsPerPage": 1,
                    "firstRecordIndex": current_page - 1,
                    "lastRecordIndex": current_page - 1,
                },
            }
        ).encode()
        return TransportResponse(
            url=url,
            content=content,
            status_code=200,
            headers={},
            route=TransportRoute.DIRECT_HTTP,
            wire_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )

    first_response = response_for(1, 11, spec.target.canonical_url)
    first_result = (first_response, "s3://raw/page-1", spec.parser(first_response))
    requested = []

    def fetch_next(specs):
        requested.extend(specs)
        next_spec = specs[0]
        next_response = response_for(2, 12, next_spec.target.canonical_url)
        return [(next_response, "s3://raw/page-2", next_spec.parser(next_response))]

    service._fetch_parsed_many = fetch_next
    completed, additional_raw = service._complete_player_statistics_pages(
        [spec], [first_result]
    )

    assert len(requested) == 1
    params = parse_qs(urlparse(requested[0].target.canonical_url).query)
    assert params["page"] == ["2"]
    assert requested[0].target.target_id.endswith(":page:2")
    parsed = completed[0][2]
    assert isinstance(parsed, ParsedDataset)
    assert {row["player_id"] for row in parsed.rows} == {11, 12}
    assert len(additional_raw) == 1
    assert additional_raw[0][1] == "s3://raw/page-2"


def test_player_stage_pagination_fetches_in_bounded_batches(tmp_path):
    service, _, _ = _service(tmp_path)
    spec = service._player_stage_statistics_spec(
        stage_id=700,
        source_season_id=9001,
        active=True,
        category="summary",
        subcategory="all",
        cache_ttl=None,
        browser_bootstrap_url=(
            "https://www.whoscored.com/Regions/247/Tournaments/36/Seasons/9001/"
            "Stages/700/TeamStatistics"
        ),
    )

    def page(number):
        return PlayerStageStatisticsPage(
            page_number=number,
            page_index=None,
            total_pages=10,
            total_results=10,
            results_per_page=1,
            first_record_index=number - 1,
            last_record_index=number - 1,
            index_base=0,
            records=({"playerId": number, "teamId": 26, "apps": 1},),
        )

    first_response = TransportResponse(
        url=spec.target.canonical_url,
        content=b"page-1",
        status_code=200,
        headers={},
        route=TransportRoute.DIRECT_HTTP,
        wire_bytes=6,
        sha256=hashlib.sha256(b"page-1").hexdigest(),
    )
    batch_sizes = []

    def fetch_next(specs):
        batch_sizes.append(len(specs))
        results = []
        for next_spec in specs:
            number = int(next_spec.target.source_ids["page"])
            content = f"page-{number}".encode()
            response = TransportResponse(
                url=next_spec.target.canonical_url,
                content=content,
                status_code=200,
                headers={},
                route=TransportRoute.DIRECT_HTTP,
                wire_bytes=len(content),
                sha256=hashlib.sha256(content).hexdigest(),
            )
            results.append((response, f"s3://raw/page-{number}", page(number)))
        return results

    service._fetch_parsed_many = fetch_next
    completed, additional_raw = service._complete_player_statistics_pages(
        [spec], [(first_response, "s3://raw/page-1", page(1))]
    )

    assert batch_sizes == [8, 1]
    assert len(additional_raw) == 9
    assert {row["player_id"] for row in completed[0][2].rows} == set(range(1, 11))


def test_source_season_discovery_uses_ttl_only_for_active_scope(tmp_path, monkeypatch):
    class _FixedDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 7, 11)

    monkeypatch.setattr("scrapers.whoscored.service.date", _FixedDate)
    service, _, _ = _service(tmp_path)
    calls = []
    html = """
    <select id="seasons"><option
      value="/Regions/247/Tournaments/36/Seasons/9001">2026</option></select>
    """

    def fake_fetch(target, **kwargs):
        calls.append(kwargs)
        content = html.encode()
        response = TransportResponse(
            url=target.canonical_url,
            content=content,
            status_code=200,
            headers={},
            route=TransportRoute.DIRECT_HTTP,
            wire_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )
        assert kwargs["validator"](response) is True
        return response, "s3://raw/tournament"

    service._fetch = fake_fetch
    assert service._source_season_id() == 9001
    assert calls[-1]["cache_ttl"] == ACTIVE_SCHEDULE_CACHE_TTL

    service.catalog_season = replace(service.catalog_season, end=date(2020, 1, 1))
    assert service._source_season_id() == 9001
    assert calls[-1]["cache_ttl"] is None


def test_match_sync_commits_source_id_and_raw_before_manifest(tmp_path):
    service, repository, raw_store = _service(tmp_path)

    result = service.sync_matches()

    assert result.status == "success", result.as_dict()
    assert result.counts == {
        "matches": 1,
        "events": 1,
        "lineups": 2,
        "substitutions": 0,
        "formations": 0,
        "team_match_stats": 0,
        "player_match_stats": 2,
    }
    assert not result.errors
    commit = repository.commits[0]
    assert commit.events[0]["source_event_id"] == 9_000_101
    assert commit.events[0]["team_event_id"] == 101
    assert commit.lineups_available is True
    assert commit.paid_bytes == 0
    assert raw_store.has(match_page_target(123))


def test_historical_match_replay_uses_stale_raw_without_network(tmp_path, monkeypatch):
    service, repository, raw_store = _service(tmp_path)
    raw_store.store_bytes(
        match_page_target(123),
        _match_html().encode(),
        content_type="text/html",
    )
    monkeypatch.setattr(raw_store, "is_fresh", lambda *_args, **_kwargs: False)
    transport = _RawCacheAwareTransport(b"must not reach the network")
    service.transport = transport

    result = service.sync_matches(force_replay=True, historical_replay=True)

    assert result.status == "success", result.as_dict()
    assert transport.network_calls == 0
    assert repository.commits[0].transport_mode == "raw_cache"


def test_parser_drift_is_fatal_and_manifested_without_commit(tmp_path):
    service, repository, raw_store = _service(tmp_path)
    service.transport = _RawPersistingContentFailureTransport(
        b"<html><body>layout changed</body></html>"
    )

    result = service.sync_matches()

    assert result.status == "failed"
    assert repository.commits == []
    assert result.terminal == []
    assert result.errors
    failure = repository.failures[0]
    assert failure.state == "parse_failed"
    assert (
        failure.payload_sha256
        == hashlib.sha256(b"<html><body>layout changed</body></html>").hexdigest()
    )
    assert failure.raw_uri
    assert raw_store.has(match_page_target(123))


def test_non_opta_match_without_matchcentre_is_explicitly_unavailable(tmp_path):
    service, repository, raw_store = _service(tmp_path)
    repository.list_match_candidates = lambda *_args, **_kwargs: [
        MatchCandidate(
            game_id=123,
            league="INT-World Cup",
            season="2026",
            game="Home-Away",
            kickoff=datetime(2026, 6, 12),
            status=6,
            match_is_opta=False,
        )
    ]
    service.transport = _RawPersistingContentFailureTransport(
        b"""
        <html><head><title>Home 1-0 Away - League 2026 Live</title></head>
        <body>
        <script>require.config.params['matchheader'] = {
          input: [1, 2, 'Home', 'Away'], matchId: 123
        };</script>
        <script>require.config.params[\"args\"] = {
          matchId: 123, initialMatchDataForScrappers: [[[1, 2]]]
        };</script>
        </body></html>
        """
    )

    result = service.sync_matches()

    assert result.status == "success", result.as_dict()
    assert repository.commits == []
    assert result.errors == []
    assert result.retryable == []
    assert result.terminal == []
    assert len(repository.failures) == 1
    assert repository.failures[0].state == "not_available"
    assert repository.failures[0].failure_code == "source_not_available"
    assert raw_store.has(match_page_target(123))


def test_present_but_unsupported_matchcentre_is_parse_failed(tmp_path):
    service, repository, raw_store = _service(tmp_path)
    repository.list_match_candidates = lambda *_args, **_kwargs: [
        MatchCandidate(
            game_id=123,
            league="INT-World Cup",
            season="2026",
            game="Home-Away",
            kickoff=datetime(2026, 6, 12),
            status=6,
            match_is_opta=False,
        )
    ]
    service.transport = _RawPersistingContentFailureTransport(
        b"""
        <script>require.config.params['matchheader'] = {
          input: [1, 2, 'Home', 'Away'], matchId: 123
        };</script>
        <script>require.config.params["args"] = {
          matchId: 123,
          matchCentreData: JSON.parse('{"events":[]}'),
          initialMatchDataForScrappers: [[[1, 2]]]
        };</script>
        """
    )

    result = service.sync_matches()

    assert result.status == "failed"
    assert repository.commits == []
    assert len(repository.failures) == 1
    assert repository.failures[0].state == "parse_failed"
    assert repository.failures[0].failure_code == "content"
    assert raw_store.has(match_page_target(123))


def test_semantically_truncated_match_is_manifested_as_parse_failed(tmp_path):
    service, repository, raw_store = _service(tmp_path)
    candidate = replace(repository.list_match_candidates()[0], attempt_no=3)
    repository.list_match_candidates = lambda *_args, **_kwargs: [candidate]

    def reject(_commit):
        raise ValueError("game 123 has incomplete final Opta events")

    repository.validate_match_commit = reject

    result = service.sync_matches()

    assert result.status == "failed"
    assert repository.commits == []
    assert len(repository.failures) == 1
    failure = repository.failures[0]
    assert failure.state == "parse_failed"
    assert failure.attempt_no == 3
    assert failure.retry_after is None
    assert "incomplete final Opta events" in failure.error
    assert failure.raw_uri
    assert raw_store.has(match_page_target(123))


def test_match_task_budget_is_backed_off_not_permanently_blacklisted(tmp_path):
    service, repository, _ = _service(tmp_path)
    service.transport = _FailingTransport(
        WhoScoredTransportError(
            "paid URL limit reached",
            kind=FailureKind.BUDGET,
            url="https://www.whoscored.com/Matches/123/Live",
            route=TransportRoute.PAID_HTTP,
            retryable=False,
        )
    )

    result = service.sync_matches()

    assert result.status == "retryable"
    assert result.retryable == ["123"]
    assert result.terminal == []
    assert repository.failures[0].state == "retryable"
    assert repository.failures[0].failure_code == "budget"


@pytest.mark.parametrize(
    ("attempt_no", "delay_hours"),
    [(1, 6), (2, 12), (3, 24), (4, 48), (7, 48)],
)
def test_match_retry_backoff_increases_and_caps(tmp_path, attempt_no, delay_hours):
    service, repository, _ = _service(tmp_path)
    candidate = replace(repository.list_match_candidates()[0], attempt_no=attempt_no)
    repository.list_match_candidates = lambda *_args, **_kwargs: [candidate]
    service.transport = _FailingTransport(
        WhoScoredTransportError(
            "origin timed out",
            kind=FailureKind.TIMEOUT,
            url="https://www.whoscored.com/Matches/123/Live",
            route=TransportRoute.DIRECT_HTTP,
            retryable=True,
        )
    )

    before = datetime.utcnow()
    result = service.sync_matches()

    assert result.status == "retryable"
    failure = repository.failures[0]
    assert failure.attempt_no == attempt_no
    assert before + timedelta(hours=delay_hours) <= failure.retry_after
    assert failure.retry_after <= before + timedelta(hours=delay_hours, seconds=2)


def test_match_success_after_retry_carries_attempt_into_manifest_commit(tmp_path):
    service, repository, _ = _service(tmp_path)
    candidate = replace(repository.list_match_candidates()[0], attempt_no=3)
    repository.list_match_candidates = lambda *_args, **_kwargs: [candidate]

    result = service.sync_matches()

    assert result.status == "success", result.as_dict()
    assert repository.commits[0].attempt_no == 3


def test_match_terminal_failure_keeps_attempt_and_has_no_retry_deadline(tmp_path):
    service, repository, _ = _service(tmp_path)
    candidate = replace(repository.list_match_candidates()[0], attempt_no=4)
    repository.list_match_candidates = lambda *_args, **_kwargs: [candidate]
    service.transport = _FailingTransport(
        WhoScoredTransportError(
            "origin rejected request",
            kind=FailureKind.HTTP_STATUS,
            url="https://www.whoscored.com/Matches/123/Live",
            route=TransportRoute.DIRECT_HTTP,
            status_code=403,
            retryable=False,
        )
    )

    result = service.sync_matches()

    assert result.status == "success"
    assert result.terminal == ["123"]
    failure = repository.failures[0]
    assert failure.state == "terminal"
    assert failure.attempt_no == 4
    assert failure.retry_after is None


def test_match_failure_manifest_error_is_visible_without_aborting_result(tmp_path):
    service, repository, _ = _service(tmp_path)
    service.transport = _FailingTransport(
        WhoScoredTransportError(
            "cache unavailable",
            kind=FailureKind.CACHE,
            url="https://www.whoscored.com/Matches/123/Live",
            route=TransportRoute.RAW_CACHE,
            retryable=False,
        )
    )

    def fail_manifest(_failure):
        raise RuntimeError("manifest sink unavailable")

    repository.record_failure = fail_manifest

    result = service.sync_matches()

    assert result.status == "failed"
    assert result.retryable == []
    assert any("failure manifest" in error for error in result.errors)
    assert any("manifest sink unavailable" in error for error in result.errors)


def test_preview_sync_commits_raw_first_batch_and_bounds_paid_urls(tmp_path):
    service, repository, raw_store = _service(tmp_path, html=_preview_html())
    repository.preview_candidates = [
        {
            "game_id": 456,
            "game": "Home-Away",
            "date": datetime(2026, 7, 11, 12),
            "home_team": "Home",
            "away_team": "Away",
            "attempt_no": 1,
            "force_refresh": False,
        }
    ]

    result = service.sync_previews(limit=10)

    assert result.status == "success", result.as_dict()
    assert result.counts == {
        "missing_players": 1,
        "preview_lineups": 0,
        "preview_sections": 2,
    }
    assert service.transport.budgets.max_paid_urls == 3
    assert service.transport.cache_load_allowed == [True]
    commit = repository.preview_commits[0]
    assert commit.missing_players[0]["player_id"] == 10
    assert commit.attempt_no == 1
    assert commit.paid_bytes == 0
    assert raw_store.has(preview_page_target(456))


def test_preview_sync_publishes_candidates_as_one_repository_chunk(tmp_path):
    service, repository, _ = _service(tmp_path, html=_preview_html())
    repository.preview_candidates = [
        {
            "game_id": game_id,
            "game": "Home-Away",
            "date": datetime(2026, 7, 11, 12),
            "home_team": "Home",
            "away_team": "Away",
            "attempt_no": 1,
            "force_refresh": False,
        }
        for game_id in (456, 457)
    ]

    result = service.sync_previews(limit=10)

    assert result.status == "success", result.as_dict()
    assert result.succeeded == 2
    assert len(repository.preview_commit_batches) == 1
    assert [commit.game_id for commit in repository.preview_commit_batches[0]] == [
        456,
        457,
    ]


def test_preview_without_source_structures_fails_without_publishing(tmp_path):
    service, repository, _ = _service(
        tmp_path, html="<html><body><h1>Preview</h1></body></html>"
    )
    repository.preview_candidates = [
        {
            "game_id": 457,
            "game": "Home-Away",
            "date": datetime(2026, 7, 11, 12),
            "home_team": "Home",
            "away_team": "Away",
            "attempt_no": 1,
            "force_refresh": True,
        }
    ]

    result = service.sync_previews()

    assert result.status == "failed"
    assert result.succeeded == 0
    assert repository.preview_commits == []
    assert repository.preview_failures[0].state == "parse_failed"
    assert service.transport.cache_load_allowed == [False]


def test_preview_transport_failure_is_backed_off_and_manifested(tmp_path):
    service, repository, _ = _service(tmp_path)
    repository.preview_candidates = [
        {
            "game_id": 458,
            "game": "Home-Away",
            "date": datetime(2026, 7, 11, 12),
            "home_team": "Home",
            "away_team": "Away",
            "attempt_no": 2,
            "force_refresh": False,
        }
    ]
    service.transport = _FailingTransport(
        WhoScoredTransportError(
            "origin timed out",
            kind=FailureKind.TIMEOUT,
            url="https://www.whoscored.com/Matches/458/Preview",
            route=TransportRoute.DIRECT_HTTP,
            retryable=True,
        )
    )

    before = datetime.utcnow()
    result = service.sync_previews()

    assert result.status == "retryable"
    assert result.retryable == ["458"]
    failure = repository.preview_failures[0]
    assert failure.state == "retryable"
    assert failure.attempt_no == 2
    assert before + timedelta(hours=11, minutes=59) < failure.retry_after
    assert failure.retry_after <= before + timedelta(hours=12, seconds=2)
    assert service.transport.budgets.max_paid_urls == 3


def test_preview_404_is_proven_not_available(tmp_path):
    service, repository, _ = _service(tmp_path)
    repository.preview_candidates = [
        {
            "game_id": 404,
            "game": "Home-Away",
            "date": datetime(2026, 7, 11, 12),
            "home_team": "Home",
            "away_team": "Away",
            "attempt_no": 1,
            "force_refresh": False,
        }
    ]
    service.transport = _FailingTransport(
        WhoScoredTransportError(
            "HTTP 404",
            kind=FailureKind.HTTP_STATUS,
            url="https://www.whoscored.com/Matches/404/Preview",
            route=TransportRoute.DIRECT_HTTP,
            status_code=404,
            retryable=False,
        )
    )

    result = service.sync_previews()

    assert result.status == "success"
    assert result.succeeded == 1
    assert result.counts["not_available"] == 1
    assert result.retryable == []
    assert result.terminal == []
    assert repository.preview_failures[0].state == "not_available"


def test_preview_retry_replays_persisted_raw_without_a_second_network_call(
    tmp_path,
):
    service, repository, raw_store = _service(tmp_path)
    content = _preview_html().encode()
    raw_store.store_bytes(preview_page_target(459), content, content_type="text/html")
    transport = _RawCacheAwareTransport(b"must not reach the network")
    service.transport = transport
    repository.preview_candidates = [
        {
            "game_id": 459,
            "game": "Home-Away",
            "date": datetime(2026, 7, 11, 12),
            "home_team": "Home",
            "away_team": "Away",
            "attempt_no": 3,
            "force_refresh": False,
        }
    ]

    result = service.sync_previews()

    assert result.status == "success", result.as_dict()
    assert transport.network_calls == 0
    commit = repository.preview_commits[0]
    assert commit.transport_mode == "raw_cache"
    assert commit.direct_bytes == 0
    assert commit.attempt_no == 3


def test_profile_retryable_transport_failure_is_manifested_with_backoff(tmp_path):
    service, repository, _ = _service(tmp_path)
    repository.profile_candidates = [42]
    service.transport = _FailingTransport(
        WhoScoredTransportError(
            "origin timed out",
            kind=FailureKind.TIMEOUT,
            url="https://www.whoscored.com/Players/42/Show",
            route=TransportRoute.DIRECT_HTTP,
            retryable=True,
        )
    )

    before = datetime.utcnow()
    result = service.sync_profiles(limit=1)

    assert result.status == "retryable"
    assert result.retryable == ["42"]
    assert result.terminal == []
    assert result.errors == []
    assert repository.profile_commits == []
    failure = repository.profile_failures[0]
    assert failure["state"] == "retryable"
    assert failure["failure_code"] == "timeout"
    assert failure["transport_mode"] == "direct_http"
    assert failure["proxy_mode"] == "none"
    assert before < failure["retry_after"] <= before + timedelta(hours=24, seconds=2)


def test_profile_parser_drift_is_visible_and_raw_backed_for_next_parser(tmp_path):
    service, repository, raw_store = _service(tmp_path)
    repository.profile_candidates = [42]
    content = b"<html><body>profile layout changed</body></html>"
    service.transport = _RawPersistingContentFailureTransport(content)

    result = service.sync_profiles(limit=1)

    assert result.status == "failed"
    assert result.errors
    assert result.retryable == []
    assert result.terminal == []
    assert repository.profile_commits == []
    failure = repository.profile_failures[0]
    assert failure["state"] == "parse_failed"
    assert failure["payload_sha256"] == hashlib.sha256(content).hexdigest()
    assert failure["raw_uri"]
    assert raw_store.has(profile_page_target(42))


def test_profile_missing_participation_structure_cannot_publish_partial_success(
    tmp_path,
):
    service, repository, raw_store = _service(tmp_path)
    repository.profile_candidates = [42]
    content = b"""
        <html><body>
          <div><span class="info-label">Name:</span> Player Forty Two</div>
          <div><span class="info-label">Age:</span> 25 (01-01-2001)</div>
          <div><span class="info-label">Nationality:</span> Exampleland</div>
        </body></html>
    """
    service.transport = _RawPersistingContentFailureTransport(content)

    result = service.sync_profiles(limit=1)

    assert result.status == "failed"
    assert repository.profile_commits == []
    failure = repository.profile_failures[0]
    assert failure["state"] == "parse_failed"
    assert "participation/playerStatistics structure is absent" in failure["error"]
    assert raw_store.has(profile_page_target(42))


def test_sparse_but_identifiable_profile_is_published(tmp_path):
    service, repository, _ = _service(tmp_path)
    repository.profile_candidates = [621946]
    service.transport = _Transport(
        """
        <div><span class="info-label">Name: </span>Luiz Felipe</div>
        <div><span class="info-label">Current Team: </span>
          <a href="/Teams/1219/Show/Brazil-Internacional">Internacional</a>
        </div>
        <div><span class="info-label">Shirt Number: </span>53</div>
        <div><span class="info-label">Positions: </span>Defender</div>
        <script>var currentParticipations = [{
          tournamentId:1,seasonId:2,stageId:3,teamId:1219,
          teamName:'Internacional',position:{displayName:'DC'}
        }];</script>
        """
    )

    result = service.sync_profiles(limit=1)

    assert result.status == "success"
    assert result.succeeded == 1
    assert repository.profile_failures == []
    assert repository.profile_commits[0].profile["player_id"] == 621946


def test_profile_candidates_use_one_deduplicated_union_scope_query(tmp_path):
    service, repository, _ = _service(tmp_path)

    result = service.sync_profiles(
        limit=200,
        candidate_scopes=[
            "INT-World Cup=2026",
            "ENG-Premier League=2526",
            "INT-World Cup=2026",
        ],
    )

    assert result.status == "success"
    assert result.attempted == 0
    assert result.scope == ("INT-World Cup=2026,ENG-Premier League=2526")
    requested = repository.profile_candidate_scope_requests[-1]
    assert [scope.spec for scope in requested] == [
        "INT-World Cup=2026",
        "ENG-Premier League=2526",
    ]


def test_explicit_profile_ids_bypass_mutable_candidate_selection(tmp_path):
    service, repository, _ = _service(tmp_path)
    repository.profile_candidates = [999999]
    service.transport = _Transport(
        """
        <div><span class="info-label">Name: </span>Frozen Player</div>
        <div><span class="info-label">Current Team: </span>
          <a href="/Teams/1219/Show/Test">Test</a>
        </div>
        <div><span class="info-label">Positions: </span>Defender</div>
        <script>var currentParticipations = [{
          tournamentId:1,seasonId:2,stageId:3,teamId:1219,
          teamName:'Test',position:{displayName:'DC'}
        }];</script>
        """
    )
    candidate_queries = len(repository.profile_candidate_scope_requests)

    result = service.sync_profiles(limit=200, player_ids=[621946])

    assert result.status == "success"
    assert result.attempted == result.succeeded == 1
    assert repository.profile_commits[0].player_id == 621946
    assert len(repository.profile_candidate_scope_requests) == candidate_queries


def test_profile_404_is_proven_not_available_and_not_retried(tmp_path):
    service, repository, _ = _service(tmp_path)
    repository.profile_candidates = [404]
    service.transport = _FailingTransport(
        WhoScoredTransportError(
            "HTTP 404",
            kind=FailureKind.HTTP_STATUS,
            url="https://www.whoscored.com/Players/404/Show",
            route=TransportRoute.DIRECT_HTTP,
            status_code=404,
            retryable=False,
        )
    )

    result = service.sync_profiles(limit=1)

    assert result.status == "success"
    assert result.retryable == []
    assert result.terminal == []
    assert result.succeeded == 1
    assert result.counts["not_available"] == 1
    assert result.errors == []
    failure = repository.profile_failures[0]
    assert failure["state"] == "not_available"
    assert failure["retry_after"] is None
    assert failure["http_status"] == 404
    assert failure["failure_code"] == "http_status"


def test_profile_task_budget_is_backed_off_not_permanently_blacklisted(tmp_path):
    service, repository, _ = _service(tmp_path)
    repository.profile_candidates = [43]
    service.transport = _FailingTransport(
        WhoScoredTransportError(
            "paid URL limit reached",
            kind=FailureKind.BUDGET,
            url="https://www.whoscored.com/Players/43/Show",
            route=TransportRoute.PAID_HTTP,
            retryable=False,
        )
    )

    result = service.sync_profiles(limit=1)

    assert result.status == "retryable"
    assert result.retryable == ["43"]
    assert result.terminal == []
    assert repository.profile_failures[0]["state"] == "retryable"
    assert repository.profile_failures[0]["failure_code"] == "budget"
