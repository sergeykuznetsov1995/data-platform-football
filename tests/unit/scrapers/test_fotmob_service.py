import hashlib
import json

from scrapers.fotmob.domain import ScopeRef
from scrapers.fotmob.parsers import parse_season_bundle
from scrapers.fotmob.planner import RunMode, TransportBudget
from scrapers.fotmob.repository import (
    ManifestStatus,
    MemoryFotMobRepository,
    TargetCommit,
)
from scrapers.fotmob.service import FotMobIngestService
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


def test_infrastructure_faults_are_retryable_not_schema_drift():
    # A Trino restart used to be committed as schema_drift with an empty
    # unknown-path list (observed 2026-07-14). Drift is a claim about the
    # source payload; it must not fire when our own catalog blinks, or the
    # backfill driver stops and the canary's drift signal becomes meaningless.
    import requests

    from scrapers.fotmob.repository import ManifestStatus
    from scrapers.fotmob.service import _failure_status

    class TrinoError(Exception):
        pass

    retryable = [
        TrinoError("SQL execution failed"),
        requests.exceptions.ConnectionError("connection refused"),
        ConnectionResetError("peer reset"),
        TimeoutError("catalog timeout"),
    ]
    for exc in retryable:
        assert _failure_status(exc) == ManifestStatus.RETRYABLE_FAILURE, exc

    drift = [KeyError("matchFacts"), ValueError("unexpected shape"), TypeError("int")]
    for exc in drift:
        assert _failure_status(exc) == ManifestStatus.SCHEMA_DRIFT, exc
