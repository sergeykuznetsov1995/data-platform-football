import hashlib
import json
from datetime import datetime, timezone

import pytest

from scrapers.fotmob.domain import ScopeRef
from scrapers.fotmob.parsers import parse_season_bundle
from scrapers.fotmob.planner import RunMode, TransportBudget
from scrapers.fotmob.repository import (
    ManifestStatus,
    MemoryFotMobRepository,
    TargetCommit,
)
from scrapers.fotmob.service import FotMobIngestService, OperationResult
from scrapers.fotmob.transport import (
    FetchOutcome,
    FetchResult,
    TransportStats,
    canonicalize_target,
)


def _league_payload(selected="2025/2026"):
    return {
        "details": {
            "id": 47,
            "name": "Premier League",
            "selectedSeason": selected,
            "latestSeason": "2025/2026",
        },
        "allAvailableSeasons": ["2025/2026", "2024/2025"],
        "tabs": ["fixtures", "table", "stats"],
        "fixtures": {
            "fixtureInfo": {
                "teams": [
                    {"id": 1, "name": "Alpha"},
                    {"id": 2, "name": "Beta"},
                ]
            },
            "allMatches": [
                {
                    "id": 100,
                    "pageUrl": "/matches/alpha-vs-beta/x#100",
                    "home": {"id": 1, "name": "Alpha"},
                    "away": {"id": 2, "name": "Beta"},
                    "status": {
                        "finished": True,
                        "scoreStr": "0 - 0",
                        "utcTime": "2026-01-01T12:00:00.000Z",
                    },
                }
            ],
        },
        "table": [
            {
                "data": {
                    "tables": [
                        {
                            "leagueName": "Group A",
                            "table": {
                                "all": [
                                    {
                                        "id": 1,
                                        "name": "Alpha",
                                        "idx": 1,
                                        "pts": 0,
                                    }
                                ]
                            },
                        },
                        {
                            "leagueName": "Best third",
                            "table": {
                                "all": [
                                    {
                                        "id": 1,
                                        "name": "Alpha",
                                        "idx": 1,
                                        "pts": 0,
                                    }
                                ]
                            },
                        },
                    ]
                }
            }
        ],
        "stats": {
            "players": [
                {
                    "name": "goals",
                    "header": "Goals",
                    "fetchAllUrl": "https://data.fotmob.com/stats/47/season/goals.json",
                }
            ],
            "teams": [],
        },
    }


class StubTransport:
    max_attempts = 1

    def __init__(self, responses):
        self.responses = responses
        self.calls = []
        self.aliases = []
        self._results = []

    def _get(self, endpoint, params, replay=False):
        target = canonicalize_target(endpoint, params)
        self.calls.append((target.canonical_url, replay))
        payload = self.responses[target.canonical_url]
        if isinstance(payload, list):
            payload = payload.pop(0)
        body = json.dumps(payload).encode()
        result = FetchResult(
            outcome=FetchOutcome.SUCCESS,
            target_key=target.target_key,
            url=target.canonical_url,
            http_status=None if replay else 200,
            json_data=payload,
            body=body,
            attempts=0 if replay else 1,
            retries=0,
            cache_hit=replay,
            stale=False,
            terminal=False,
            etag='"etag"',
            last_modified=None,
            raw_uri=f"memory://{target.target_key}.json.gz",
            content_hash=hashlib.sha256(body).hexdigest(),
            fetched_at="2026-07-11T10:00:00+00:00",
            encoded_bytes=0 if replay else len(body),
            decoded_bytes=len(body),
            direct_bytes=0 if replay else len(body),
            proxy_bytes=0,
        )
        self._results.append(result)
        return result

    def fetch_json(self, endpoint, params=None):
        return self._get(endpoint, params, False)

    def replay_json(self, endpoint, params=None):
        return self._get(endpoint, params, True)

    def alias_cached_json(self, source, target):
        self.aliases.append((source, target))
        return None

    def snapshot_stats(self):
        return TransportStats(
            logical_targets=len(self._results),
            attempts=sum(item.attempts for item in self._results),
            encoded_bytes=sum(item.encoded_bytes for item in self._results),
            decoded_bytes=sum(item.decoded_bytes for item in self._results),
            direct_bytes=sum(item.direct_bytes for item in self._results),
            proxy_bytes=0,
        )


def _service(responses, mode=RunMode.DAILY):
    transport = StubTransport(responses)
    repository = MemoryFotMobRepository()
    service = FotMobIngestService(
        transport=transport,
        repository=repository,
        mode=mode,
        budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
        run_id="test-run",
        max_workers=2,
    )
    return service, transport, repository


def test_catalog_discovers_numeric_ids_dedupes_popular_and_records_exclusion():
    all_leagues = {
        "countries": [
            {
                "ccode": "ENG",
                "name": "England",
                "leagues": [{"id": 47, "name": "Premier League"}],
            },
            {
                "ccode": "INT",
                "name": "International",
                "leagues": [{"id": 999, "name": "Women Friendly Cup"}],
            },
        ],
        "popularLeagues": [{"id": 47, "name": "Premier League"}],
    }
    target = canonicalize_target("allLeagues").canonical_url
    service, _, repository = _service({target: all_leagues})

    catalog = service.discover_catalog()

    assert catalog.operation.ok
    assert catalog.operation.counts["competitions"] == 2
    assert catalog.operation.counts["included"] == 1
    assert catalog.operation.counts["excluded"] == 1
    rows = repository.tables["fotmob_competitions"]
    assert [row["competition_id"] for row in rows] == ["47", "999"]
    assert rows[1]["scope_decision"] == "excluded"


def test_catalog_tombstones_only_after_two_complete_absences():
    payload = {
        "countries": [
            {"ccode": "ENG", "leagues": [{"id": 47, "name": "Premier League"}]}
        ]
    }
    target = canonicalize_target("allLeagues").canonical_url
    service, _, repository = _service({target: payload})
    body = json.dumps(payload).encode()
    repository.record(
        TargetCommit(
            run_id="prior-identical-raw",
            target_type="all_leagues",
            target_key=canonicalize_target("allLeagues").target_key,
            status=ManifestStatus.SUCCESS,
            content_hash=hashlib.sha256(body).hexdigest(),
        )
    )
    repository.tables["fotmob_competitions"] = [
        {
            "competition_id": "47",
            "discovery_run_id": "older",
            "is_tombstoned": False,
        },
        {
            "competition_id": "99",
            "discovery_run_id": "older",
            "is_tombstoned": False,
        },
        {
            "competition_id": "47",
            "discovery_run_id": "previous",
            "is_tombstoned": False,
        },
    ]

    result = service.discover_catalog()

    assert result.operation.ok
    rows = repository.tables["fotmob_competitions"]
    tombstones = [row for row in rows if row.get("is_tombstoned")]
    assert [row["competition_id"] for row in tombstones] == ["99"]
    assert result.operation.counts["tombstones"] == 1
    assert rows[-1]["discovery_run_id"] == "test-run"
    catalog_commits = [
        commit for commit in repository.commits if commit.target_type == "all_leagues"
    ]
    assert catalog_commits[-1].observation_id == "test-run"
    assert catalog_commits[-1].batch_id != catalog_commits[0].batch_id


def test_catalog_stale_on_error_fails_closed_without_snapshot_or_tombstone():
    payload = {
        "countries": [
            {"ccode": "ENG", "leagues": [{"id": 47, "name": "Premier League"}]}
        ]
    }
    target = canonicalize_target("allLeagues")
    service, transport, repository = _service({target.canonical_url: payload})
    body = json.dumps(payload).encode()
    transport.fetch_json = lambda endpoint, params=None: FetchResult(
        outcome=FetchOutcome.STALE_REPLAY,
        target_key=target.target_key,
        url=target.canonical_url,
        http_status=503,
        json_data=payload,
        body=body,
        attempts=3,
        retries=2,
        cache_hit=True,
        stale=True,
        terminal=False,
        etag='"old"',
        last_modified=None,
        raw_uri="memory://stale.json.gz",
        content_hash=hashlib.sha256(body).hexdigest(),
        fetched_at="2026-07-10T10:00:00+00:00",
        encoded_bytes=0,
        decoded_bytes=len(body),
        direct_bytes=0,
        proxy_bytes=0,
        error="FotMob returned retryable HTTP 503",
    )

    result = service.discover_catalog()

    assert not result.operation.ok
    assert result.discovery is None
    assert result.operation.retryable == [target.canonical_url]
    assert result.operation.metadata["stale_replay_rejected"] is True
    assert "fotmob_competitions" not in repository.tables
    commit = repository.commits[-1]
    assert commit.status == ManifestStatus.RETRYABLE_FAILURE
    assert commit.error_code == "stale_catalog_replay"


def test_offline_catalog_replay_reparses_without_tombstones():
    payload = {
        "countries": [
            {"ccode": "ENG", "leagues": [{"id": 47, "name": "Premier League"}]}
        ]
    }
    target = canonicalize_target("allLeagues").canonical_url
    service, _, repository = _service({target: payload}, mode=RunMode.REPLAY)
    # Even two complete historical absences cannot turn a cache-only reparse
    # into a third source observation.
    repository.previous_catalog_snapshots = lambda limit=2: [{47, 99}, {47}]

    result = service.discover_catalog()

    assert result.operation.ok
    assert result.operation.counts["tombstones"] == 0
    assert result.operation.metadata["authoritative_source_observation"] is False
    assert not any(
        row.get("is_tombstoned") for row in repository.tables["fotmob_competitions"]
    )
    commit = repository.commits[-1]
    assert commit.fetch_outcome == FetchOutcome.SUCCESS.value
    assert commit.attempts == 0 and commit.cache_hit


def test_source_validated_catalog_304_remains_authoritative():
    payload = {
        "countries": [
            {"ccode": "ENG", "leagues": [{"id": 47, "name": "Premier League"}]}
        ]
    }
    target = canonicalize_target("allLeagues")
    service, transport, repository = _service({target.canonical_url: payload})
    body = json.dumps(payload).encode()
    transport.fetch_json = lambda endpoint, params=None: FetchResult(
        outcome=FetchOutcome.NOT_MODIFIED,
        target_key=target.target_key,
        url=target.canonical_url,
        http_status=304,
        json_data=payload,
        body=body,
        attempts=1,
        retries=0,
        cache_hit=True,
        stale=False,
        terminal=False,
        etag='"same"',
        last_modified=None,
        raw_uri="memory://cached.json.gz",
        content_hash=hashlib.sha256(body).hexdigest(),
        fetched_at="2026-07-11T10:00:00+00:00",
        encoded_bytes=0,
        decoded_bytes=len(body),
        direct_bytes=0,
        proxy_bytes=0,
    )

    result = service.discover_catalog()

    assert result.operation.ok
    assert result.operation.metadata["authoritative_source_observation"] is True
    assert repository.commits[-1].status == ManifestStatus.NOT_MODIFIED


def test_season_sync_preserves_context_duplicates_and_zero_points():
    url = canonicalize_target(
        "leagues", {"id": 47, "season": "2025/2026"}
    ).canonical_url
    service, _, repository = _service({url: _league_payload()})

    result, bundle = service.sync_season(47, "2025/2026")

    assert result.ok and bundle is not None
    assert result.counts["matches"] == 1
    assert result.counts["standings"] == 2
    standings = repository.tables["fotmob_standings"]
    assert [row["table_name"] for row in standings] == ["Group A", "Best third"]
    assert [row["points"] for row in standings] == [0, 0]
    assert all(row["source_season_key"] == "2025/2026" for row in standings)
    categories = repository.tables["fotmob_leaderboard_categories"]
    assert [(row["participant_type"], row["name"]) for row in categories] == [
        ("player", "goals")
    ]
    assert "fotmob_competition_seasons" not in repository.tables


def test_selected_discovery_prefetch_is_committed_under_exact_season_target():
    root_url = canonicalize_target("leagues", {"id": 47}).canonical_url
    service, transport, repository = _service({root_url: _league_payload()})
    prefetched = transport.fetch_json("leagues", {"id": 47})

    result, bundle = service.sync_season(
        47,
        "2025/2026",
        prefetched=prefetched,
    )

    assert result.ok and bundle is not None
    exact = canonicalize_target("leagues", {"id": 47, "season": "2025/2026"})
    commit = repository.commits[-1]
    assert commit.target_key == exact.target_key
    assert commit.attempts == 0
    assert commit.direct_bytes == 0
    assert transport.aliases == [(root_url, exact.canonical_url)]


def test_byte_identical_success_reuses_committed_physical_batch():
    url = canonicalize_target(
        "leagues", {"id": 47, "season": "2025/2026"}
    ).canonical_url
    service, _, repository = _service({url: _league_payload()})

    first, _ = service.sync_season(47, "2025/2026")
    row_counts = {name: len(rows) for name, rows in repository.tables.items()}
    second, _ = service.sync_season(47, "2025/2026")

    assert first.ok and second.ok
    assert {name: len(rows) for name, rows in repository.tables.items()} == row_counts
    assert len(repository.commits) == 2


def test_backfill_resumes_successful_season_from_raw_without_network_attempt():
    url = canonicalize_target(
        "leagues", {"id": 47, "season": "2025/2026"}
    ).canonical_url
    service, transport, _ = _service({url: _league_payload()})
    first, _ = service.sync_season(47, "2025/2026")
    requests_after_first = service.ledger.requests
    service.mode = RunMode.BACKFILL

    resumed, bundle = service.sync_season(47, "2025/2026")

    assert first.ok and resumed.ok and bundle is not None
    assert transport.calls == [(url, False), (url, True)]
    assert service.ledger.requests == requests_after_first == 1


def test_backfill_falls_back_to_network_when_committed_raw_replay_is_missing():
    url = canonicalize_target(
        "leagues", {"id": 47, "season": "2025/2026"}
    ).canonical_url
    service, transport, _ = _service({url: _league_payload()})
    assert service.sync_season(47, "2025/2026")[0].ok

    def missing_replay(endpoint, params=None):
        target = canonicalize_target(endpoint, params)
        transport.calls.append((target.canonical_url, True))
        return FetchResult(
            outcome=FetchOutcome.TERMINAL_FAILURE,
            target_key=target.target_key,
            url=target.canonical_url,
            http_status=None,
            json_data=None,
            body=None,
            attempts=0,
            retries=0,
            cache_hit=False,
            stale=False,
            terminal=True,
            etag=None,
            last_modified=None,
            raw_uri=None,
            content_hash=None,
            fetched_at=None,
            encoded_bytes=0,
            decoded_bytes=0,
            direct_bytes=0,
            proxy_bytes=0,
            error="raw target not found",
        )

    transport.replay_json = missing_replay
    service.mode = RunMode.BACKFILL

    resumed, bundle = service.sync_season(47, "2025/2026")

    assert resumed.ok and bundle is not None
    assert resumed.metadata["raw_replay_fallback"]["outcome"] == "terminal_failure"
    assert transport.calls == [(url, False), (url, True), (url, False)]


def test_selected_season_mismatch_is_schema_drift_and_publishes_no_rows():
    url = canonicalize_target(
        "leagues", {"id": 47, "season": "2024/2025"}
    ).canonical_url
    service, _, repository = _service({url: _league_payload("2025/2026")})

    result, bundle = service.sync_season(47, "2024/2025")

    assert bundle is None
    assert not result.ok
    assert "fotmob_matches" not in repository.tables
    assert repository.commits[-1].status == ManifestStatus.SCHEMA_DRIFT


def test_leaderboard_parses_all_top_lists_and_team_name_fallback():
    bundle = parse_season_bundle(_league_payload(), ScopeRef(47, "2025/2026"))
    url = "https://data.fotmob.com/stats/47/season/goals.json"
    payload = {
        "TopLists": [
            {
                "Title": "Goals",
                "StatName": "goals",
                "StatList": [
                    {
                        "ParticiantId": 10,
                        "ParticipantName": "Player",
                        "TeamId": 1,
                        "Rank": 1,
                    }
                ],
            },
            {
                "Title": "Per 90",
                "StatName": "goals_per_90",
                "StatList": [
                    {
                        "ParticiantId": 10,
                        "ParticipantName": "Player",
                        "TeamId": 1,
                        "Rank": 1,
                    }
                ],
            },
        ]
    }
    service, transport, repository = _service({url: payload})

    result = service.sync_leaderboards(bundle)

    assert result.ok and result.counts["rows"] == 2
    assert len(transport.calls) == 1
    assert {row["stat_name"] for row in repository.tables["fotmob_leaderboards"]} == {
        "goals",
        "goals_per_90",
    }


def test_backfill_skips_current_parser_successful_exact_leaderboard_target():
    bundle = parse_season_bundle(_league_payload(), ScopeRef(47, "2025/2026"))
    url = "https://data.fotmob.com/stats/47/season/goals.json"
    payload = {"TopLists": []}
    service, transport, _ = _service({url: payload}, mode=RunMode.BACKFILL)

    first = service.sync_leaderboards(bundle)
    second = service.sync_leaderboards(bundle)

    assert first.ok and first.succeeded == 1
    assert second.ok and second.skipped == 1 and second.succeeded == 0
    assert len(transport.calls) == 1


def test_advertised_leaderboard_without_url_is_explicit_policy_unavailable():
    payload = _league_payload()
    payload["stats"]["players"][0].pop("fetchAllUrl")
    bundle = parse_season_bundle(payload, ScopeRef(47, "2025/2026"))
    service, transport, repository = _service({})

    result = service.sync_leaderboards(bundle)

    assert result.ok
    assert result.not_available == 1
    assert result.metadata["intentional_not_available"] == 1
    assert transport.calls == []
    assert repository.commits[-1].status == ManifestStatus.NOT_AVAILABLE
    assert repository.commits[-1].error_code == "missing_fetch_all_url"


def test_missing_leaderboard_url_tombstones_the_prior_logical_category():
    url = "https://data.fotmob.com/stats/47/season/goals.json"
    service, _, repository = _service({url: {"TopLists": []}})
    present_bundle = parse_season_bundle(_league_payload(), ScopeRef(47, "2025/2026"))
    service.sync_leaderboards(present_bundle)

    missing_payload = _league_payload()
    missing_payload["stats"]["players"][0].pop("fetchAllUrl")
    missing_bundle = parse_season_bundle(missing_payload, ScopeRef(47, "2025/2026"))
    service.sync_leaderboards(missing_bundle)

    success, tombstone = [
        commit for commit in repository.commits if commit.target_type == "leaderboard"
    ]
    assert success.target_key != tombstone.target_key
    assert (
        success.target_type,
        success.competition_id,
        success.source_season_key,
        success.entity_id,
    ) == (
        tombstone.target_type,
        tombstone.competition_id,
        tombstone.source_season_key,
        tombstone.entity_id,
    )
    assert tombstone.status == ManifestStatus.NOT_AVAILABLE


@pytest.mark.parametrize("invalid_shape", ["missing", "duplicate"])
def test_ambiguous_leaderboard_category_identity_fails_before_requests(
    invalid_shape,
):
    payload = _league_payload()
    if invalid_shape == "missing":
        payload["stats"]["players"][0].pop("name")
    else:
        payload["stats"]["teams"] = [
            {
                "name": "goals",
                "fetchAllUrl": (
                    "https://data.fotmob.com/stats/47/season/team-goals.json"
                ),
            }
        ]
    bundle = parse_season_bundle(payload, ScopeRef(47, "2025/2026"))
    service, transport, repository = _service({})

    result = service.sync_leaderboards(bundle)

    assert not result.ok
    assert transport.calls == []
    assert repository.commits[-1].status == ManifestStatus.SCHEMA_DRIFT
    assert repository.commits[-1].error_code == "ambiguous_leaderboard_identity"


def test_transfer_pagination_uses_league_ids_and_stops_at_unique_hits():
    page1 = canonicalize_target(
        "transfers", {"leagueIds": "47", "page": 1}
    ).canonical_url
    page2 = canonicalize_target(
        "transfers", {"leagueIds": "47", "page": 2}
    ).canonical_url
    first = {
        "hits": 2,
        "page": 1,
        "transfers": [
            {
                "playerId": 1,
                "name": "One",
                "transferDate": "2026-07-01",
                "fromClubId": 10,
                "toClubId": 20,
                "feeText": "€1m",
                "localizedFeeText": "€1m",
                "value": 1_000_000,
            }
        ],
    }
    second = {
        "hits": 2,
        "page": 2,
        "transfers": [
            {
                "playerId": 2,
                "name": "Two",
                "transferDate": "2026-07-02",
                "fromClubId": 30,
                "toClubId": 40,
                "feeText": "Free",
            }
        ],
    }
    service, transport, repository = _service({page1: first, page2: second})

    result = service.sync_transfers(47)

    assert result.ok and result.counts["events"] == 2
    assert len(transport.calls) == 2
    rows = repository.tables["fotmob_transfer_events"]
    assert rows[0]["fee_text"] == "€1m"
    assert rows[0]["fee_value"] == 1_000_000
    assert all(row["competition_id"] == "47" for row in rows)
    assert all("source_season_key" not in row for row in rows)


def test_transfer_backfill_replays_checkpoint_pages_and_separates_windows():
    pages = {
        page: canonicalize_target(
            "transfers", {"leagueIds": "47", "page": page}
        ).canonical_url
        for page in (1, 2, 3)
    }
    responses = {
        url: {
            "hits": 3,
            "page": page,
            "transfers": [
                {
                    "playerId": page,
                    "name": f"Player {page}",
                    "transferDate": f"2026-07-0{page}",
                    "fromClubId": page * 10,
                    "toClubId": page * 10 + 1,
                }
            ],
        }
        for page, url in pages.items()
    }
    recent_url = canonicalize_target(
        "transfers",
        {"leagueIds": "47", "page": 1, "last": "1year"},
    ).canonical_url
    responses[recent_url] = {"hits": 0, "page": 1, "transfers": []}
    service, transport, repository = _service(
        responses,
        mode=RunMode.BACKFILL,
    )

    first = service.sync_transfers(47, max_pages=2)
    second = service.sync_transfers(47, max_pages=3)
    recent = service.sync_transfers(47, max_pages=1, recent_only=True)

    assert not first.ok
    assert second.ok and second.counts["events"] == 3
    assert second.metadata["resumed_raw_pages"] == 1
    second_run_calls = transport.calls[2:5]
    assert second_run_calls == [
        (pages[1], False),
        (pages[2], True),
        (pages[3], False),
    ]
    assert recent.ok and recent.counts["events"] == 0
    transfer_commits = [
        commit
        for commit in repository.commits
        if commit.target_type == "transfers_page"
    ]
    assert {commit.entity_id for commit in transfer_commits} >= {
        "all:1",
        "all:2",
        "all:3",
        "1year:1",
    }


def test_one_page_transfer_bound_advances_to_first_missing_page():
    pages = {
        page: canonicalize_target(
            "transfers", {"leagueIds": "47", "page": page}
        ).canonical_url
        for page in (1, 2, 3)
    }
    responses = {
        url: {
            "hits": 3,
            "page": page,
            "transfers": [
                {
                    "playerId": page,
                    "name": f"Player {page}",
                    "transferDate": f"2026-07-0{page}",
                    "fromClubId": page * 10,
                    "toClubId": page * 10 + 1,
                }
            ],
        }
        for page, url in pages.items()
    }
    service, transport, _ = _service(responses, mode=RunMode.BACKFILL)

    assert not service.sync_transfers(47, max_pages=2).ok
    resumed = service.sync_transfers(47, max_pages=1)

    assert resumed.ok and resumed.counts["events"] == 3
    assert transport.calls[2:] == [
        (pages[1], True),
        (pages[2], True),
        (pages[3], False),
    ]
    assert resumed.metadata["resumed_raw_pages"] == 2
    assert resumed.metadata["network_pages"] == 1


def test_match_payload_uses_one_request_and_second_call_skips_success():
    bundle = parse_season_bundle(_league_payload(), ScopeRef(47, "2025/2026"))
    match_url = canonicalize_target("matchDetails", {"matchId": "100"}).canonical_url
    payload = {"content": {"matchFacts": {"events": []}, "stats": {"x": 1}}}
    service, transport, repository = _service({match_url: payload})

    first = service.sync_match_payloads(bundle)
    second = service.sync_match_payloads(bundle)

    assert first.ok and first.succeeded == 1
    assert second.ok and second.skipped == 1
    assert len(transport.calls) == 1
    assert "/matchDetails?" in transport.calls[0][0]
    assert "/match?" not in transport.calls[0][0]
    assert repository.tables["fotmob_match_payloads"][0]["page_url"].startswith(
        "/matches/"
    )


def test_match_payload_data_not_found_body_is_intentional_not_available():
    bundle = parse_season_bundle(_league_payload(), ScopeRef(47, "2025/2026"))
    match_url = canonicalize_target("matchDetails", {"matchId": "100"}).canonical_url
    payload = {"error": True, "message": "Data not found", "matchId": "100"}
    service, transport, repository = _service({match_url: payload})

    result = service.sync_match_payloads(bundle)

    assert result.ok
    assert result.not_available == 1
    assert result.metadata["intentional_not_available"] == 1
    assert not result.errors
    assert "fotmob_match_payloads" not in repository.tables
    commit = next(c for c in repository.commits if c.target_type == "match")
    assert commit.status == ManifestStatus.NOT_AVAILABLE
    assert commit.error_code == "source_data_not_found"


def test_match_data_not_found_must_echo_the_exact_requested_match_id():
    bundle = parse_season_bundle(_league_payload(), ScopeRef(47, "2025/2026"))
    match_url = canonicalize_target("matchDetails", {"matchId": "100"}).canonical_url
    service, _, repository = _service(
        {
            match_url: {
                "error": True,
                "message": "Data not found",
                "matchId": "999",
            }
        }
    )

    result = service.sync_match_payloads(bundle)

    assert result.not_available == 0
    assert result.errors
    commit = next(c for c in repository.commits if c.target_type == "match")
    assert commit.status == ManifestStatus.SCHEMA_DRIFT


def test_match_payload_unfamiliar_error_body_stays_schema_drift():
    bundle = parse_season_bundle(_league_payload(), ScopeRef(47, "2025/2026"))
    match_url = canonicalize_target("matchDetails", {"matchId": "100"}).canonical_url
    payload = {"error": True, "message": "Internal error", "matchId": "100"}
    service, transport, repository = _service({match_url: payload})

    result = service.sync_match_payloads(bundle)

    assert result.not_available == 0
    assert "intentional_not_available" not in result.metadata
    assert any("incomplete" in error for error in result.errors)
    commit = next(c for c in repository.commits if c.target_type == "match")
    assert commit.status == ManifestStatus.SCHEMA_DRIFT


def test_team_snapshots_are_global_observations_not_historical_season_rows():
    bundle = parse_season_bundle(_league_payload(), ScopeRef(47, "2025/2026"))
    team1 = canonicalize_target("teams", {"id": "1"}).canonical_url
    team2 = canonicalize_target("teams", {"id": "2"}).canonical_url
    payload = {
        "details": {"name": "Alpha"},
        "overview": {},
        "squad": {
            "squad": [
                {
                    "title": "Players",
                    "members": [{"id": 10, "name": "Player"}],
                }
            ]
        },
    }
    service, _, repository = _service({team1: payload, team2: payload})

    result, player_ids = service.sync_team_snapshots(bundle)

    assert result.ok and player_ids == {10}
    team_rows = repository.tables["fotmob_team_snapshots"]
    squad_rows = repository.tables["fotmob_squad_snapshots"]
    assert all("source_season_key" not in row for row in team_rows + squad_rows)
    assert all("snapshot_date" in row and "observed_at" in row for row in team_rows)

    second, cached_player_ids = service.sync_team_snapshots(bundle)
    assert second.ok and second.skipped == 2
    assert cached_player_ids == {10}


def test_player_next_snapshot_is_global_and_fresh_entity_is_skipped():
    player_url = "https://www.fotmob.com/_next/data/build-1/players/10.json"
    payload = {
        "pageProps": {
            "data": {
                "id": 10,
                "name": "Player",
                "primaryTeam": {"teamId": 1, "teamName": "Alpha"},
                "marketValues": [{"value": 100}],
                "careerHistory": {"careerItems": []},
            }
        }
    }
    service, transport, repository = _service({player_url: payload})

    first = service.sync_player_snapshots([10], build_id="build-1")
    second = service.sync_player_snapshots([10], build_id="build-1")

    assert first.ok and first.succeeded == 1
    assert second.ok and second.skipped == 1
    assert len(transport.calls) == 1
    row = repository.tables["fotmob_player_snapshots"][0]
    assert row["player_id"] == "10"
    assert row["primary_team_id"] == 1
    assert "source_season_key" not in row
    assert row["snapshot_date"] == "2026-07-11"


def test_backfill_reprocesses_prior_generation_children_for_current_lineage():
    bundle = parse_season_bundle(_league_payload(), ScopeRef(47, "2025/2026"))
    leaderboard_url = "https://data.fotmob.com/stats/47/season/goals.json"
    match_url = canonicalize_target("matchDetails", {"matchId": "100"}).canonical_url
    team_urls = {
        team_id: canonicalize_target("teams", {"id": str(team_id)}).canonical_url
        for team_id in (1, 2)
    }
    player_url = "https://www.fotmob.com/_next/data/build-1/players/10.json"
    team_payload = {
        "details": {"name": "Alpha"},
        "overview": {},
        "squad": {
            "squad": [
                {
                    "title": "Players",
                    "members": [{"id": 10, "name": "Player"}],
                }
            ]
        },
    }
    responses = {
        leaderboard_url: {"TopLists": []},
        match_url: {"content": {"matchFacts": {"events": []}, "stats": {}}},
        team_urls[1]: team_payload,
        team_urls[2]: team_payload,
        player_url: {"pageProps": {"data": {"id": 10, "name": "Player"}}},
    }
    service, transport, repository = _service(responses, mode=RunMode.BACKFILL)
    prior_targets = (
        ("leaderboard", canonicalize_target(leaderboard_url), "goals"),
        ("match", canonicalize_target(match_url), "100"),
        ("team", canonicalize_target(team_urls[1]), "1"),
        ("team", canonicalize_target(team_urls[2]), "2"),
        ("player", canonicalize_target(player_url), "10"),
    )
    for target_type, target, entity_id in prior_targets:
        repository.record(
            TargetCommit(
                run_id="prior-publication-generation",
                target_type=target_type,
                target_key=target.target_key,
                status=ManifestStatus.SUCCESS,
                entity_id=entity_id,
                content_hash="a" * 64,
                raw_uri=f"memory://{target.target_key}.json.gz",
                completed_at=datetime.now(timezone.utc),
            )
        )

    leaderboard = service.sync_leaderboards(bundle)
    matches = service.sync_match_payloads(bundle)
    teams, player_ids = service.sync_team_snapshots(bundle)
    players = service.sync_player_snapshots(player_ids, build_id="build-1")

    assert all(result.ok for result in (leaderboard, matches, teams, players))
    assert (
        leaderboard.skipped == matches.skipped == teams.skipped == players.skipped == 0
    )
    assert len(transport.calls) == 5
    current_commits = [
        commit for commit in repository.commits if commit.run_id == service.run_id
    ]
    assert {commit.target_type for commit in current_commits} == {
        "leaderboard",
        "match",
        "team",
        "player",
    }
    assert {
        commit.entity_id for commit in current_commits if commit.target_type == "team"
    } == {
        "1",
        "2",
    }


def _absent_team_fetch(outcome, team_id="2222"):
    target = canonicalize_target("teams", {"id": str(team_id)})
    return FetchResult(
        outcome=outcome,
        target_key=target.target_key,
        url=target.canonical_url,
        http_status=200,
        json_data=None,
        body=b"null",
        attempts=1,
        retries=0,
        cache_hit=False,
        stale=False,
        terminal=False,
        etag=None,
        last_modified=None,
        raw_uri=None,
        content_hash=None,
        fetched_at=None,
        encoded_bytes=4,
        decoded_bytes=4,
        direct_bytes=4,
        proxy_bytes=0,
    )


def test_record_failure_does_not_scope_complete_generic_transport_absence():
    # A generic 204/404/null response does not prove the advertised entity is
    # absent. Only an entity-aware parser may opt into a tombstone.
    result = OperationResult("team_snapshots")
    FotMobIngestService._record_failure(
        result, "2222", _absent_team_fetch(FetchOutcome.NOT_AVAILABLE)
    )
    assert result.not_available == 0
    assert "intentional_not_available" not in result.metadata
    assert result.terminal
    assert not result.ok


def test_generic_transport_absence_cannot_publish_entity_tombstone():
    service, _, repository = _service({})
    service._commit_for_fetch(
        _absent_team_fetch(FetchOutcome.NOT_AVAILABLE),
        target_type="team",
        entity_id="2222",
    )

    assert repository.commits[-1].status == ManifestStatus.TERMINAL_FAILURE


def test_historical_advertised_team_absence_resolves_without_tombstone():
    bundle = parse_season_bundle(
        _league_payload(selected="2010/2011"),
        ScopeRef(47, "2010/2011"),
    )
    service, _, repository = _service({}, mode=RunMode.BACKFILL)
    prior_target = canonicalize_target("teams", {"id": "1"})
    repository.record(
        TargetCommit(
            run_id="prior-v2",
            target_type="team",
            target_key=prior_target.target_key,
            status=ManifestStatus.SUCCESS,
            entity_id="1",
            content_hash="a" * 64,
            completed_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        )
    )
    service._fetch_many = lambda requests: {
        key: _absent_team_fetch(FetchOutcome.NOT_AVAILABLE, key)
        for key, _, _ in requests
    }

    result, player_ids = service.sync_team_snapshots(
        bundle,
        allow_advertised_absence=True,
    )

    assert result.ok
    assert result.not_available == 2
    assert result.metadata["intentional_not_available"] == 2
    assert player_ids == set()
    absences = [
        commit
        for commit in repository.commits
        if commit.error_code == "source_historical_team_unavailable"
    ]
    assert len(absences) == 2
    assert all(commit.status == ManifestStatus.EXCLUDED for commit in absences)
    # EXCLUDED is a plan disposition, not an entity tombstone: the prior
    # global observation remains the latest serving success.
    assert repository.latest_entity_success("team", "1")["status"] == "success"


def test_record_failure_leaves_retryable_failure_open_and_not_intentional():
    result = OperationResult("team_snapshots")
    FotMobIngestService._record_failure(
        result, "3333", _absent_team_fetch(FetchOutcome.RETRYABLE_FAILURE)
    )
    assert result.not_available == 0
    assert "intentional_not_available" not in result.metadata
    assert result.retryable == ["3333"]


def test_player_null_pageprops_data_is_intentional_not_available():
    player_url = "https://www.fotmob.com/_next/data/build-1/players/2090857.json"
    payload = {
        "pageProps": {"data": None, "fallback": {}, "translations": {}},
        "__N_SSP": True,
    }
    service, _, repository = _service({player_url: payload})

    result = service.sync_player_snapshots([2090857], build_id="build-1")

    assert result.ok
    assert result.not_available == 1
    assert result.metadata["intentional_not_available"] == 1
    assert not result.errors
    assert "fotmob_player_snapshots" not in repository.tables
    commit = next(c for c in repository.commits if c.target_type == "player")
    assert commit.status == ManifestStatus.NOT_AVAILABLE
    assert commit.error_code == "source_player_no_data"


def test_player_payload_without_pageprops_container_stays_parse_failure():
    player_url = "https://www.fotmob.com/_next/data/build-1/players/10.json"
    payload = {"unexpected": True}
    service, _, repository = _service({player_url: payload})

    result = service.sync_player_snapshots([10], build_id="build-1")

    assert result.not_available == 0
    assert "intentional_not_available" not in result.metadata
    assert any("parse" in error for error in result.errors)
    commit = next(c for c in repository.commits if c.target_type == "player")
    assert commit.status != ManifestStatus.NOT_AVAILABLE


@pytest.mark.parametrize(
    "data",
    [
        {"id": 20, "name": "Wrong player"},
        {"name": "Missing source id"},
        {"id": "10", "name": "Wrong id type"},
    ],
)
def test_player_payload_id_mismatch_is_schema_drift_without_row_or_freshness(data):
    player_url = "https://www.fotmob.com/_next/data/build-1/players/10.json"
    payload = {"pageProps": {"data": data}}
    service, _, repository = _service({player_url: payload})

    result = service.sync_player_snapshots([10], build_id="build-1")

    assert not result.ok and result.succeeded == 0
    assert any("id mismatch" in error for error in result.errors)
    assert "fotmob_player_snapshots" not in repository.tables
    commit = next(c for c in repository.commits if c.target_type == "player")
    assert commit.entity_id == "10"
    assert commit.status == ManifestStatus.SCHEMA_DRIFT
    assert repository.latest_entity_success("player", 10) is None


def test_player_limit_applies_after_freshness_filter_without_prefix_starvation():
    url10 = "https://www.fotmob.com/_next/data/build-1/players/10.json"
    url20 = "https://www.fotmob.com/_next/data/build-1/players/20.json"
    payload10 = {"pageProps": {"data": {"id": 10, "name": "Ten"}}}
    payload20 = {"pageProps": {"data": {"id": 20, "name": "Twenty"}}}
    service, transport, _ = _service({url10: payload10, url20: payload20})
    assert service.sync_player_snapshots([10], build_id="build-1").ok

    result = service.sync_player_snapshots([10, 20], build_id="build-1", limit=1)

    assert result.ok and result.attempted == 2
    assert result.skipped == 1 and result.succeeded == 1
    assert result.metadata["due_before_limit"] == 1
    assert result.metadata["deferred_by_limit"] == 0
    assert [call[0] for call in transport.calls] == [url10, url20]


def test_completion_markers_round_trip_exact_plan_and_scope_identity():
    service, _, repository = _service({})

    service.record_scope_completion(
        289,
        "2017/2019",
        plan_signature="fmplan1-scope",
        coverage={"entities": ["season", "leaderboards"]},
        counts={"matches": 52},
    )
    service.record_competition_completion(
        289,
        plan_signature="fmplan1-transfers",
        coverage={"source_hits": 10, "unique": 10},
        counts={"transfer_events": 10},
    )

    assert repository.completed_scope_keys("fmplan1-scope") == {(289, "2017/2019")}
    assert repository.completed_scope_keys("fmplan1-other") == set()
    assert repository.completed_competition_ids("fmplan1-transfers") == {289}
    scope = repository.commits[-2]
    assert scope.target_type == "scope_completion"
    assert scope.entity_id == "fmplan1-scope"
    assert scope.source_season_key == "2017/2019"
    assert scope.expected_counts == {"matches": 52}


def test_network_fetch_is_not_started_without_full_retry_reservation():
    transport = StubTransport({})
    transport.max_attempts = 4
    service = FotMobIngestService(
        transport=transport,
        repository=MemoryFotMobRepository(),
        mode=RunMode.DAILY,
        budget=TransportBudget(max_requests=3, max_direct_bytes=1_000),
        run_id="budget-test",
    )

    result = service.discover_catalog()

    assert not result.operation.ok
    assert "retry-bounded" in result.operation.errors[0]
    assert transport.calls == []


def test_next_build_fetch_is_not_started_without_full_retry_reservation():
    transport = StubTransport({})
    transport.max_attempts = 4
    document_calls = []
    transport.fetch_document = lambda url: document_calls.append(url)
    service = FotMobIngestService(
        transport=transport,
        repository=MemoryFotMobRepository(),
        mode=RunMode.DAILY,
        budget=TransportBudget(max_requests=3, max_direct_bytes=1_000),
        run_id="build-budget-test",
    )

    result = service.sync_player_snapshots([10])

    assert not result.ok
    assert "Next build discovery" in result.errors[0]
    assert "cannot cover Next build" in result.errors[0]
    assert document_calls == []


def test_offline_player_replay_uses_each_manifest_target_without_build_id(tmp_path):
    from scrapers.fotmob.raw_store import FotMobRawStore
    from scrapers.fotmob.repository import LEGACY_PARSER_VERSION
    from scrapers.fotmob.transport import FotMobTransport

    raw_store = FotMobRawStore.from_uri(tmp_path.as_uri())
    historical = canonicalize_target(
        "https://www.fotmob.com/_next/data/historical-build/players/10.json"
    )
    body = b'{"pageProps":{"data":{"id":10,"name":"Ten"}}}'
    raw = raw_store.store(
        historical,
        body,
        fetched_at="2026-07-20T10:00:00+00:00",
    )
    repository = MemoryFotMobRepository()
    repository.record(
        TargetCommit(
            run_id="production-v1",
            target_type="player",
            target_key=historical.target_key,
            status=ManifestStatus.SUCCESS,
            entity_id="10",
            content_hash=raw.content_hash,
            raw_uri=raw.raw_uri,
            parser_version=LEGACY_PARSER_VERSION,
            fetched_at=datetime(2026, 7, 20, 10, 0),
            completed_at=datetime(2026, 7, 20, 10, 0),
        )
    )
    service = FotMobIngestService(
        transport=FotMobTransport(raw_store),
        repository=repository,
        mode=RunMode.REPLAY,
        budget=TransportBudget(max_requests=1, max_direct_bytes=1),
        run_id="issue930-v2-replay",
        max_workers=2,
    )

    result = service.sync_player_snapshots([10])

    assert result.ok and result.succeeded == 1
    assert result.errors == []
    assert repository.tables["fotmob_player_snapshots"][0]["player_id"] == "10"
    replay = repository.commits[-1]
    assert replay.run_id == "issue930-v2-replay"
    assert replay.target_key == historical.target_key
    assert replay.parser_version != LEGACY_PARSER_VERSION
    assert replay.attempts == replay.direct_bytes == replay.proxy_bytes == 0
    assert replay.cache_hit is True


def test_infrastructure_faults_are_retryable_not_schema_drift():
    # A Trino restart used to be committed as schema_drift with an empty
    # unknown-path list (observed 2026-07-14). Drift is a claim about the
    # source payload; it must not fire when our own catalog blinks, or the
    # backfill driver stops and the canary's drift signal becomes meaningless.
    import requests

    from scrapers.base.trino_manager import TrinoError
    from scrapers.fotmob.repository import ManifestStatus
    from scrapers.fotmob.service import _failure_status

    class WrappedTrinoError(TrinoError):
        """Subclasses of the platform error must still classify as infra."""

    retryable = [
        TrinoError("SQL execution failed"),
        WrappedTrinoError("SQL execution failed inside a batch"),
        requests.exceptions.ConnectionError("connection refused"),
        ConnectionResetError("peer reset"),
        TimeoutError("catalog timeout"),
    ]
    for exc in retryable:
        assert _failure_status(exc) == ManifestStatus.RETRYABLE_FAILURE, exc

    drift = [KeyError("matchFacts"), ValueError("unexpected shape"), TypeError("int")]
    for exc in drift:
        assert _failure_status(exc) == ManifestStatus.SCHEMA_DRIFT, exc
