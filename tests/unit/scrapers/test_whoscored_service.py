from __future__ import annotations

import json
from datetime import datetime, timedelta

from pyarrow import fs

from scrapers.whoscored.catalog import WhoScoredCatalog
from scrapers.whoscored.raw_store import WhoScoredRawStore, match_page_target
from scrapers.whoscored.repository import MatchCandidate
from scrapers.whoscored.service import WhoScoredIngestService
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
                }
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


class _Transport:
    def __init__(self, html: str):
        self.html = html
        self.raw_cache = None
        self.budgets = TransportBudgets()

    def fetch(self, url, *, cache_key, validator):
        content = self.html.encode()
        response = TransportResponse(
            url=url,
            content=content,
            status_code=200,
            headers={},
            route=TransportRoute.DIRECT_HTTP,
            wire_bytes=len(content),
            sha256=__import__("hashlib").sha256(content).hexdigest(),
        )
        assert validator(response) is True
        self.raw_cache.store(cache_key, CachedPayload(content), response.sha256)
        return response

    def get_traffic_stats(self):
        return {"paid_proxy_bytes": 0, "route_requests": {"direct_http": 1}}

    def close(self):
        return None


class _Repository:
    def __init__(self):
        self.commits = []
        self.failures = []
        self.profile_candidates = []
        self.profile_commits = []
        self.profile_failures = []

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

    def list_profile_candidates(self, *, limit):
        return self.profile_candidates[:limit]

    def commit_profile(self, **commit):
        self.profile_commits.append(commit)

    def record_profile_failure(self, **failure):
        self.profile_failures.append(failure)


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
    service, repository, _ = _service(
        tmp_path, html="<html><body>layout changed</body></html>"
    )

    result = service.sync_matches()

    assert result.status == "failed"
    assert repository.commits == []
    # The transport classifies validator drift as a typed content failure;
    # the game remains visible to operators and is never sent to paid proxy.
    assert result.terminal == ["123"] or result.errors


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
