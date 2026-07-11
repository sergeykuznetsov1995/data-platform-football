from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

from pyarrow import fs

from scrapers.whoscored.catalog import WhoScoredCatalog
from scrapers.whoscored.raw_store import (
    WhoScoredRawStore,
    match_page_target,
    preview_page_target,
    profile_page_target,
    schedule_month_target,
)
from scrapers.whoscored.repository import MatchCandidate
from scrapers.whoscored.service import (
    ACTIVE_SCHEDULE_CACHE_TTL,
    WhoScoredIngestService,
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
    return "<script>require.config.params['args'] = {matchCentreData: " + json.dumps(payload) + "};</script>"


def _preview_html() -> str:
    return """
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

    def fetch(self, url, *, cache_key, validator):
        self.cache_load_allowed.append(bool(self.raw_cache.allow_load))
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

    def fetch(self, url, *, cache_key, validator):
        cached = self.raw_cache.load(cache_key)
        if cached is not None:
            content = cached.content
            route = TransportRoute.RAW_CACHE
            wire_bytes = 0
        else:
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

    def commit_match(self, commit):
        self.commits.append(commit)
        return commit.batch_id

    def record_failure(self, failure):
        self.failures.append(failure)

    def list_profile_candidates(self, *, scopes, limit):
        self.profile_candidate_scope_requests.append(tuple(scopes))
        return self.profile_candidates[:limit]

    def commit_profile(self, **commit):
        self.profile_commits.append(commit)

    def record_profile_failure(self, **failure):
        self.profile_failures.append(failure)

    def list_preview_candidates(self, *args, **kwargs):
        return self.preview_candidates[: kwargs.get("limit")]

    def commit_preview(self, commit):
        self.preview_commits.append(commit)
        return commit.batch_id

    def record_preview_failure(self, failure):
        self.preview_failures.append(failure)

    def write_scope_snapshot(self, **snapshot):
        self.scope_snapshots.append(snapshot)
        return f"iceberg.bronze.{snapshot['table']}"

    def latest_source_season_id(self, *_args, **_kwargs):
        return None


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

    def fetch(self, url, *, cache_key, validator):
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


def test_ttl_cache_replays_same_day_then_refreshes_once_after_expiry(tmp_path):
    service, _, raw_store = _service(tmp_path)
    transport = _RawCacheAwareTransport(b'{"schedule":"fresh"}')
    service.transport = transport
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


def test_schedule_cache_policy_ttls_only_mutable_active_targets(
    tmp_path, monkeypatch
):
    class _FixedDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 7, 11)

    monkeypatch.setattr("scrapers.whoscored.service.date", _FixedDate)
    service, repository, _ = _service(tmp_path)
    service.catalog_season = replace(service.catalog_season, end=None)
    service._source_season_id = lambda: 9001
    calls = []
    stage_html = """
    <select id="stages"><option
      value="/Regions/247/Tournaments/36/Seasons/9001/Stages/700">
      Group Stage</option></select>
    """
    calendar_html = """
    <script>var wsCalendar = {mask:{2026:{0:{1:1},6:{1:1}}}};</script>
    """

    def fake_fetch(target, **kwargs):
        calls.append((target.page_kind, dict(target.source_ids), kwargs))
        if target.page_kind == "season_stages":
            content = stage_html.encode()
        elif target.page_kind == "stage_calendar":
            content = calendar_html.encode()
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

    result = service.sync_schedule()

    assert result.status == "success", result.as_dict()
    assert result.counts == {"schedule": 2, "season_stages": 1}
    assert service.transport.budgets.max_paid_urls == 3
    policy = {
        (kind, ids.get("month")): kwargs.get("cache_ttl")
        for kind, ids, kwargs in calls
    }
    assert policy[("season_stages", None)] == ACTIVE_SCHEDULE_CACHE_TTL
    assert policy[("stage_calendar", None)] == ACTIVE_SCHEDULE_CACHE_TTL
    assert policy[("schedule_month", "202607")] == ACTIVE_SCHEDULE_CACHE_TTL
    assert policy[("schedule_month", "202601")] is None
    assert all(kwargs["allow_cache"] is True for _, _, kwargs in calls)
    assert {snapshot["table"] for snapshot in repository.scope_snapshots} == {
        "whoscored_schedule",
        "whoscored_season_stages",
    }

    calls.clear()
    service.catalog_season = replace(
        service.catalog_season, end=date(2020, 1, 1)
    )
    historical = service.sync_schedule()
    assert historical.status == "success", historical.as_dict()
    assert all(kwargs.get("cache_ttl") is None for _, _, kwargs in calls)


def test_source_season_discovery_uses_ttl_only_for_active_scope(
    tmp_path, monkeypatch
):
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

    service.catalog_season = replace(
        service.catalog_season, end=date(2020, 1, 1)
    )
    assert service._source_season_id() == 9001
    assert calls[-1]["cache_ttl"] is None


def test_match_sync_commits_source_id_and_raw_before_manifest(tmp_path):
    service, repository, raw_store = _service(tmp_path)

    result = service.sync_matches()

    assert result.status == "success", result.as_dict()
    assert result.counts == {"events": 1, "lineups": 2}
    assert not result.errors
    commit = repository.commits[0]
    assert commit.events[0]["source_event_id"] == 101
    assert commit.lineups_available is True
    assert commit.paid_bytes == 0
    assert raw_store.has(match_page_target(123))


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
    assert failure.payload_sha256 == hashlib.sha256(
        b"<html><body>layout changed</body></html>"
    ).hexdigest()
    assert failure.raw_uri
    assert raw_store.has(match_page_target(123))


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
    assert result.counts == {"missing_players": 1}
    assert service.transport.budgets.max_paid_urls == 3
    assert service.transport.cache_load_allowed == [True]
    commit = repository.preview_commits[0]
    assert commit.missing_players[0]["player_id"] == 10
    assert commit.attempt_no == 1
    assert commit.paid_bytes == 0
    assert raw_store.has(preview_page_target(456))


def test_preview_zero_rows_is_a_successful_logical_snapshot(tmp_path):
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

    assert result.status == "success"
    assert result.counts == {"missing_players": 0}
    assert repository.preview_commits[0].missing_players == ()
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


def test_preview_retry_replays_persisted_raw_without_a_second_network_call(
    tmp_path,
):
    service, repository, raw_store = _service(tmp_path)
    content = _preview_html().encode()
    raw_store.store_bytes(
        preview_page_target(459), content, content_type="text/html"
    )
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
    assert before < failure["retry_after"] <= before + timedelta(
        hours=24, seconds=2
    )


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
    assert result.scope == (
        "INT-World Cup=2026,ENG-Premier League=2526"
    )
    requested = repository.profile_candidate_scope_requests[-1]
    assert [scope.spec for scope in requested] == [
        "INT-World Cup=2026",
        "ENG-Premier League=2526",
    ]


def test_profile_terminal_404_is_manifested_and_not_reported_retryable(tmp_path):
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
    assert result.terminal == ["404"]
    assert result.errors == []
    failure = repository.profile_failures[0]
    assert failure["state"] == "terminal"
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
