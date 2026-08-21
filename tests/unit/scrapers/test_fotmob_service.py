import copy
import hashlib
import json
from datetime import datetime, timedelta, timezone

import pytest

from scrapers.fotmob.domain import ProbeStatus, ScopeDecision, ScopeRef
from scrapers.fotmob.parsers import parse_season_bundle
from scrapers.fotmob.planner import RunMode, TransportBudget
from scrapers.fotmob.repository import (
    ManifestStatus,
    MemoryFotMobRepository,
    TargetCommit,
)
from scrapers.fotmob.service import (
    LEADERBOARD_REFRESH_AFTER,
    PLAYER_REFRESH_AFTER,
    TEAM_REFRESH_AFTER,
    FotMobIngestService,
    OperationResult,
    profile_probe_delay,
)
from scrapers.fotmob.transport import (
    FetchOutcome,
    FetchResult,
    FotMobTransport,
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
            "gender": "male",
            "type": "league",
            "ageGroup": "adult",
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
        if isinstance(payload, FetchResult):
            self._results.append(payload)
            return payload
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


def _competition_payload(
    competition_id,
    name,
    *,
    gender="male",
    competition_type="league",
    age_group="adult",
):
    payload = copy.deepcopy(_league_payload())
    payload["details"].update(
        {
            "id": competition_id,
            "name": name,
            "gender": gender,
            "type": competition_type,
            "ageGroup": age_group,
        }
    )
    return payload


def _failed_profile(competition_id, outcome, status):
    target = canonicalize_target("leagues", {"id": competition_id})
    return FetchResult(
        outcome=outcome,
        target_key=target.target_key,
        url=target.canonical_url,
        http_status=status,
        json_data=None,
        body=None,
        attempts=1,
        retries=0,
        cache_hit=False,
        stale=False,
        terminal=outcome != FetchOutcome.RETRYABLE_FAILURE,
        etag=None,
        last_modified=None,
        raw_uri=None,
        content_hash=None,
        fetched_at="2026-08-08T10:00:00+00:00",
        encoded_bytes=0,
        decoded_bytes=0,
        direct_bytes=0,
        proxy_bytes=0,
        error=f"HTTP {status}",
    )


def test_profile_probe_delay_uses_bounded_ladder_jitter_and_retry_after():
    bases = [timedelta(minutes=15), timedelta(hours=1), timedelta(hours=6), timedelta(hours=24)]
    delays = [profile_probe_delay(index, 47) for index in range(1, 6)]
    for actual, base in zip(delays, [*bases, bases[-1]]):
        assert base <= actual < base + timedelta(minutes=1)
    assert delays == [profile_probe_delay(index, 47) for index in range(1, 6)]
    assert profile_probe_delay(3, 48) != delays[2]
    assert profile_probe_delay(4, 47, timedelta(hours=2)) == timedelta(hours=2)


def test_first_seen_male_profile_is_included_and_reused_for_season_discovery():
    all_leagues = {"countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]}
    profile = _competition_payload(47, "Premier League")
    profile_url = canonicalize_target("leagues", {"id": 47}).canonical_url
    service, transport, repository = _service(
        {
            canonicalize_target("allLeagues").canonical_url: all_leagues,
            profile_url: profile,
        }
    )

    catalog = service.discover_catalog()
    discovered = service.discover_competitions(
        catalog.classifications,
        profile_payloads=catalog.profile_payloads,
    )

    assert catalog.classifications[0].decision is ScopeDecision.INCLUDED
    assert catalog.profile_payloads[47] is profile
    assert discovered[0].seasons
    assert sum(url == profile_url for url, _ in transport.calls) == 1
    assert repository.commits[0].target_type == "competition_profile"
    assert repository.commits[-1].target_type == "competition_seasons"


def test_fresh_profile_revalidates_cached_male_before_season_fanout():
    competition_id = 47
    all_leagues = {
        "countries": [
            {"leagues": [{"id": competition_id, "name": "Premier League"}]}
        ]
    }
    catalog_url = canonicalize_target("allLeagues").canonical_url
    profile_url = canonicalize_target(
        "leagues", {"id": competition_id}
    ).canonical_url
    first_service, _, repository = _service(
        {
            catalog_url: all_leagues,
            profile_url: _competition_payload(
                competition_id, "Premier League", gender="male"
            ),
        }
    )
    first_service.discover_catalog()

    second_transport = StubTransport(
        {
            catalog_url: all_leagues,
            profile_url: _competition_payload(
                competition_id, "Premier League", gender="female"
            ),
        }
    )
    second_service = FotMobIngestService(
        transport=second_transport,
        repository=repository,
        budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
        run_id="fresh-female-profile",
    )

    cached = second_service.discover_catalog()
    discovered = second_service.discover_competitions(cached.classifications)
    evidence = repository.latest_scope_evidence([competition_id])[competition_id]

    assert cached.classifications[0].decision is ScopeDecision.INCLUDED
    assert discovered[0].classification.decision is ScopeDecision.EXCLUDED
    assert discovered[0].classification.policy_rule == "exclude_female"
    assert discovered[0].seasons == ()
    assert evidence.decision is ScopeDecision.EXCLUDED
    assert evidence.source_gender == "female"
    assert not any(
        commit.run_id == "fresh-female-profile"
        and commit.target_type == "competition_seasons"
        for commit in repository.commits
    )


def test_included_profile_revalidation_leaves_evidence_to_the_commit_buffer():
    # Revalidation used to flush after every included league, which is one
    # Iceberg commit per one-row observation: a production wave spent 2.3 h on
    # 450 of them, median 16 s apart (#1163).  Evidence must still be persisted
    # before the fan-out it authorizes -- that is the buffer's ordering, not the
    # flush's -- so the commit is required and the flush is not.
    competition_ids = (47, 53, 87)
    all_leagues = {
        "countries": [
            {
                "leagues": [
                    {"id": competition_id, "name": f"League {competition_id}"}
                    for competition_id in competition_ids
                ]
            }
        ]
    }
    catalog_url = canonicalize_target("allLeagues").canonical_url
    responses = {catalog_url: all_leagues}
    for competition_id in competition_ids:
        responses[
            canonicalize_target(
                "leagues", {"id": competition_id}
            ).canonical_url
        ] = _competition_payload(
            competition_id, f"League {competition_id}", gender="male"
        )
    first_service, _, repository = _service(responses)
    first_service.discover_catalog()

    female_responses = {catalog_url: all_leagues}
    for competition_id in competition_ids:
        female_responses[
            canonicalize_target(
                "leagues", {"id": competition_id}
            ).canonical_url
        ] = _competition_payload(
            competition_id, f"League {competition_id}", gender="female"
        )
    second_service = FotMobIngestService(
        transport=StubTransport(female_responses),
        repository=repository,
        budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
        run_id="batched-revalidation",
    )
    cached = second_service.discover_catalog()

    flushes = []
    buffered_flush = repository.flush

    def counting_flush():
        flushes.append(len(repository.commits))
        return buffered_flush()

    repository.flush = counting_flush
    discovered = second_service.discover_competitions(cached.classifications)

    assert [item.classification.decision for item in discovered] == [
        ScopeDecision.EXCLUDED
    ] * len(competition_ids)
    revalidated = [
        commit
        for commit in repository.commits
        if commit.run_id == "batched-revalidation"
        and commit.target_type == "competition_profile"
    ]
    assert len(revalidated) == len(competition_ids)
    assert flushes == []


def test_failed_fresh_revalidation_uses_profile_backoff_before_retry():
    competition_id = 47
    all_leagues = {
        "countries": [
            {"leagues": [{"id": competition_id, "name": "Premier League"}]}
        ]
    }
    catalog_url = canonicalize_target("allLeagues").canonical_url
    profile_url = canonicalize_target(
        "leagues", {"id": competition_id}
    ).canonical_url
    first_service, _, repository = _service(
        {
            catalog_url: all_leagues,
            profile_url: _competition_payload(competition_id, "Premier League"),
        }
    )
    first_service.discover_catalog()
    failed_transport = StubTransport(
        {
            catalog_url: all_leagues,
            profile_url: _failed_profile(
                competition_id, FetchOutcome.RETRYABLE_FAILURE, 503
            ),
        }
    )
    failed_service = FotMobIngestService(
        transport=failed_transport,
        repository=repository,
        budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
        run_id="fresh-profile-503",
    )

    cached = failed_service.discover_catalog()
    discovered = failed_service.discover_competitions(cached.classifications)
    pending = repository.latest_scope_evidence([competition_id])[competition_id]

    assert discovered[0].classification.decision is ScopeDecision.PENDING_PROBE
    assert pending.probe_status is ProbeStatus.PENDING
    assert pending.probe_attempt_count == 1
    assert timedelta(minutes=15) <= (
        pending.next_probe_at - pending.observed_at
    ) < timedelta(minutes=16)

    before_due_transport = StubTransport({catalog_url: all_leagues})
    before_due_service = FotMobIngestService(
        transport=before_due_transport,
        repository=repository,
        budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
        run_id="before-fresh-profile-retry",
    )
    before_due = before_due_service.discover_catalog()

    assert before_due.classifications[0].decision is ScopeDecision.PENDING_PROBE
    assert [url for url, _ in before_due_transport.calls] == [catalog_url]


def test_first_seen_female_persists_evidence_but_never_creates_seasons_or_matches():
    competition_id = 10557
    all_leagues = {"countries": [{"leagues": [{"id": competition_id, "name": "Premier League"}]}]}
    profile = _competition_payload(competition_id, "Premier League", gender="female")
    service, _, repository = _service(
        {
            canonicalize_target("allLeagues").canonical_url: all_leagues,
            canonicalize_target("leagues", {"id": competition_id}).canonical_url: profile,
        }
    )

    catalog = service.discover_catalog()
    discovered = service.discover_competitions(
        catalog.classifications,
        profile_payloads=catalog.profile_payloads,
    )

    assert catalog.classifications[0].decision is ScopeDecision.EXCLUDED
    assert catalog.profile_payloads == {}
    assert discovered[0].operation.skipped == 1
    assert "fotmob_competition_scope_observations" in repository.tables
    assert "fotmob_competition_seasons" not in repository.tables
    assert "fotmob_matches" not in repository.tables
    assert {
        commit.target_type for commit in repository.commits
    } == {"competition_profile", "all_leagues"}


def test_first_seen_unknown_profile_is_review_only_without_season_fanout():
    competition_id = 70003
    all_leagues = {"countries": [{"leagues": [{"id": competition_id, "name": "Mystery Cup"}]}]}
    profile = _competition_payload(
        competition_id,
        "Mystery Cup",
        competition_type="unknown",
    )
    service, transport, repository = _service(
        {
            canonicalize_target("allLeagues").canonical_url: all_leagues,
            canonicalize_target("leagues", {"id": competition_id}).canonical_url: profile,
        }
    )

    catalog = service.discover_catalog()
    service.discover_competitions(
        catalog.classifications,
        profile_payloads=catalog.profile_payloads,
    )

    assert catalog.classifications[0].decision is ScopeDecision.REVIEW_REQUIRED
    assert len(transport.calls) == 2
    assert "fotmob_competition_seasons" not in repository.tables
    assert "fotmob_matches" not in repository.tables


def test_timeout_is_pending_with_first_backoff_and_never_dead():
    competition_id = 70001
    all_leagues = {"countries": [{"leagues": [{"id": competition_id, "name": "Senior Cup"}]}]}
    failed = _failed_profile(competition_id, FetchOutcome.RETRYABLE_FAILURE, 503)
    service, _, repository = _service(
        {
            canonicalize_target("allLeagues").canonical_url: all_leagues,
            canonicalize_target("leagues", {"id": competition_id}).canonical_url: failed,
        }
    )

    catalog = service.discover_catalog()
    evidence = repository.latest_scope_evidence([competition_id])[competition_id]

    assert catalog.classifications[0].decision is ScopeDecision.PENDING_PROBE
    assert evidence.probe_status is ProbeStatus.PENDING
    assert evidence.authoritative_miss_count == 0
    assert evidence.probe_attempt_count == 1
    delay = evidence.next_probe_at - evidence.observed_at
    assert timedelta(minutes=15) <= delay < timedelta(minutes=16)


def test_final_429_retry_after_controls_persisted_profile_schedule():
    from tests.unit.scrapers.test_fotmob_transport import FakeResponse, FakeSession

    competition_id = 70006
    all_leagues = {
        "countries": [{"leagues": [{"id": competition_id, "name": "Senior Cup"}]}]
    }
    retry_after = timedelta(hours=2)
    profile_fetch = FotMobTransport(
        session=FakeSession(
            [FakeResponse(429, b"rate", {"Retry-After": "7200"})]
        ),
        max_attempts=1,
        sleep_fn=lambda _delay: None,
        jitter_fn=lambda _low, _high: 0.0,
    ).fetch_json("leagues", {"id": competition_id})
    service, _, repository = _service(
        {
            canonicalize_target("allLeagues").canonical_url: all_leagues,
            canonicalize_target("leagues", {"id": competition_id}).canonical_url: profile_fetch,
        }
    )

    service.discover_catalog()
    evidence = repository.latest_scope_evidence([competition_id])[competition_id]

    assert evidence.probe_attempt_count == 1
    assert evidence.next_probe_at - evidence.observed_at == retry_after


def test_successive_5xx_profiles_back_off_without_ever_becoming_dead(monkeypatch):
    competition_id = 70005
    all_leagues = {"countries": [{"leagues": [{"id": competition_id, "name": "Senior Cup"}]}]}
    catalog_url = canonicalize_target("allLeagues").canonical_url
    profile_url = canonicalize_target("leagues", {"id": competition_id}).canonical_url
    service, _, repository = _service(
        {catalog_url: all_leagues, profile_url: _failed_profile(competition_id, FetchOutcome.RETRYABLE_FAILURE, 503)}
    )
    service.discover_catalog()
    prior = repository.latest_scope_evidence([competition_id])[competition_id]
    assert prior.probe_attempt_count == 1
    monkeypatch.setattr("scrapers.fotmob.service.utc_now", lambda: prior.next_probe_at + timedelta(seconds=1))
    second_service = FotMobIngestService(
        transport=StubTransport(
            {catalog_url: all_leagues, profile_url: _failed_profile(competition_id, FetchOutcome.RETRYABLE_FAILURE, 503)}
        ),
        repository=repository,
        budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
        run_id="second-5xx",
    )

    second_service.discover_catalog()
    evidence = repository.latest_scope_evidence([competition_id])[competition_id]

    assert evidence.probe_status is ProbeStatus.PENDING
    assert evidence.authoritative_miss_count == 0
    assert evidence.probe_attempt_count == 2
    delay = evidence.next_probe_at - evidence.observed_at
    assert timedelta(hours=1) <= delay < timedelta(hours=1, minutes=1)


@pytest.mark.parametrize("probe_status", [ProbeStatus.PENDING, ProbeStatus.NOT_FOUND])
@pytest.mark.parametrize(
    ("legacy_delay", "expected_attempt_count", "expected_delay"),
    [
        (timedelta(minutes=15, seconds=30), 2, timedelta(hours=1)),
        (timedelta(hours=1, seconds=30), 3, timedelta(hours=6)),
        (timedelta(hours=6, seconds=30), 4, timedelta(hours=24)),
        (timedelta(hours=24, seconds=30), 4, timedelta(hours=24)),
    ],
)
def test_legacy_null_probe_attempt_continues_recognizable_backoff_rung(
    monkeypatch,
    probe_status,
    legacy_delay,
    expected_attempt_count,
    expected_delay,
):
    competition_id = 70009
    all_leagues = {
        "countries": [{"leagues": [{"id": competition_id, "name": "Senior Cup"}]}]
    }
    catalog_url = canonicalize_target("allLeagues").canonical_url
    profile_url = canonicalize_target("leagues", {"id": competition_id}).canonical_url
    service, _, repository = _service(
        {
            catalog_url: all_leagues,
            profile_url: _failed_profile(
                competition_id,
                FetchOutcome.RETRYABLE_FAILURE,
                503,
            ),
        }
    )
    service.discover_catalog()
    prior = repository.latest_scope_evidence([competition_id])[competition_id]
    legacy_row = repository.tables["fotmob_competition_scope_observations"][-1]
    legacy_row["probe_attempt_count"] = None
    legacy_row["probe_status"] = probe_status.value
    legacy_row["authoritative_miss_count"] = (
        1 if probe_status is ProbeStatus.NOT_FOUND else 0
    )
    legacy_row["next_probe_at"] = prior.observed_at + legacy_delay
    monkeypatch.setattr(
        "scrapers.fotmob.service.utc_now",
        lambda: prior.observed_at + legacy_delay + timedelta(seconds=1),
    )
    second_service = FotMobIngestService(
        transport=StubTransport(
            {
                catalog_url: all_leagues,
                profile_url: _failed_profile(
                    competition_id,
                    FetchOutcome.RETRYABLE_FAILURE,
                    503,
                ),
            }
        ),
        repository=repository,
        budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
        run_id=(
            f"legacy-{probe_status.value}-{expected_attempt_count}"
        ),
    )

    second_service.discover_catalog()
    evidence = repository.latest_scope_evidence([competition_id])[competition_id]

    assert evidence.probe_attempt_count == expected_attempt_count
    delay = evidence.next_probe_at - evidence.observed_at
    assert expected_delay <= delay < expected_delay + timedelta(minutes=1)


def test_two_authoritative_not_found_observations_become_dead_but_5xx_does_not(monkeypatch):
    competition_id = 70002
    all_leagues = {"countries": [{"leagues": [{"id": competition_id, "name": "Senior Cup"}]}]}
    catalog_url = canonicalize_target("allLeagues").canonical_url
    profile_url = canonicalize_target("leagues", {"id": competition_id}).canonical_url
    service, _, repository = _service(
        {catalog_url: all_leagues, profile_url: _failed_profile(competition_id, FetchOutcome.NOT_AVAILABLE, 404)}
    )
    first = service.discover_catalog()
    assert first.classifications[0].decision is ScopeDecision.PENDING_PROBE
    prior = repository.latest_scope_evidence([competition_id])[competition_id]
    monkeypatch.setattr("scrapers.fotmob.service.utc_now", lambda: prior.next_probe_at + timedelta(seconds=1))
    second_service = FotMobIngestService(
        transport=StubTransport({catalog_url: all_leagues, profile_url: _failed_profile(competition_id, FetchOutcome.NOT_AVAILABLE, 404)}),
        repository=repository,
        budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
        run_id="second-run",
    )

    second = second_service.discover_catalog()
    evidence = repository.latest_scope_evidence([competition_id])[competition_id]

    assert second.classifications[0].policy_rule == "exclude_dead_profile"
    assert evidence.probe_status is ProbeStatus.DEAD
    assert evidence.authoritative_miss_count == 2


def test_stale_profile_replay_does_not_advance_authoritative_miss_count(monkeypatch):
    competition_id = 70004
    all_leagues = {"countries": [{"leagues": [{"id": competition_id, "name": "Senior Cup"}]}]}
    catalog_url = canonicalize_target("allLeagues").canonical_url
    profile_url = canonicalize_target("leagues", {"id": competition_id}).canonical_url
    service, _, repository = _service(
        {catalog_url: all_leagues, profile_url: _failed_profile(competition_id, FetchOutcome.NOT_AVAILABLE, 404)}
    )
    service.discover_catalog()
    prior = repository.latest_scope_evidence([competition_id])[competition_id]
    monkeypatch.setattr("scrapers.fotmob.service.utc_now", lambda: prior.next_probe_at + timedelta(seconds=1))
    stale = _failed_profile(competition_id, FetchOutcome.STALE_REPLAY, 503)
    stale = FetchResult(
        **{
            **stale.__dict__,
            "json_data": _competition_payload(competition_id, "Senior Cup"),
            "stale": True,
            "cache_hit": True,
            "terminal": False,
        }
    )
    second_service = FotMobIngestService(
        transport=StubTransport({catalog_url: all_leagues, profile_url: stale}),
        repository=repository,
        budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
        run_id="stale-run",
    )

    second_service.discover_catalog()
    evidence = repository.latest_scope_evidence([competition_id])[competition_id]

    assert evidence.probe_status is ProbeStatus.PENDING
    assert evidence.authoritative_miss_count == 1
    assert evidence.probe_attempt_count == 2


def test_transport_shaped_stale_null_does_not_advance_authoritative_miss(monkeypatch):
    competition_id = 70007
    all_leagues = {
        "countries": [{"leagues": [{"id": competition_id, "name": "Senior Cup"}]}]
    }
    catalog_url = canonicalize_target("allLeagues").canonical_url
    profile_target = canonicalize_target("leagues", {"id": competition_id})
    service, _, repository = _service(
        {
            catalog_url: all_leagues,
            profile_target.canonical_url: _failed_profile(
                competition_id,
                FetchOutcome.NOT_AVAILABLE,
                200,
            ),
        }
    )
    service.discover_catalog()
    prior = repository.latest_scope_evidence([competition_id])[competition_id]
    assert prior.probe_status is ProbeStatus.NOT_FOUND
    assert prior.authoritative_miss_count == 1
    monkeypatch.setattr(
        "scrapers.fotmob.service.utc_now",
        lambda: prior.next_probe_at + timedelta(seconds=1),
    )
    stale_null = FotMobTransport()._result(
        outcome=FetchOutcome.STALE_REPLAY,
        target=profile_target,
        http_status=503,
        body=b"null",
        json_data=None,
        attempts=2,
        network_bytes=8,
        cache_hit=True,
        stale=True,
        error="FotMob returned retryable HTTP 503",
    )
    second_service = FotMobIngestService(
        transport=StubTransport(
            {catalog_url: all_leagues, profile_target.canonical_url: stale_null}
        ),
        repository=repository,
        budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
        run_id="stale-null-run",
    )

    second = second_service.discover_catalog()
    evidence = repository.latest_scope_evidence([competition_id])[competition_id]

    assert second.classifications[0].decision is ScopeDecision.PENDING_PROBE
    assert evidence.probe_status is ProbeStatus.PENDING
    assert evidence.authoritative_miss_count == 1


@pytest.mark.parametrize(
    ("profile_kwargs", "expected_decision"),
    [
        ({"gender": "female"}, ScopeDecision.EXCLUDED),
        ({"competition_type": "unknown"}, ScopeDecision.REVIEW_REQUIRED),
    ],
)
def test_transient_after_30_day_profile_review_restarts_at_first_backoff(
    monkeypatch,
    profile_kwargs,
    expected_decision,
):
    competition_id = 70008
    all_leagues = {
        "countries": [{"leagues": [{"id": competition_id, "name": "Senior Cup"}]}]
    }
    catalog_url = canonicalize_target("allLeagues").canonical_url
    profile_url = canonicalize_target("leagues", {"id": competition_id}).canonical_url
    service, _, repository = _service(
        {
            catalog_url: all_leagues,
            profile_url: _competition_payload(
                competition_id,
                "Senior Cup",
                **profile_kwargs,
            ),
        }
    )
    first = service.discover_catalog()
    assert first.classifications[0].decision is expected_decision
    prior = repository.latest_scope_evidence([competition_id])[competition_id]
    monkeypatch.setattr(
        "scrapers.fotmob.service.utc_now",
        lambda: prior.next_probe_at + timedelta(seconds=1),
    )
    second_service = FotMobIngestService(
        transport=StubTransport(
            {
                catalog_url: all_leagues,
                profile_url: _failed_profile(
                    competition_id,
                    FetchOutcome.RETRYABLE_FAILURE,
                    503,
                ),
            }
        ),
        repository=repository,
        budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
        run_id=f"review-retry-{expected_decision.value}",
    )

    second_service.discover_catalog()
    evidence = repository.latest_scope_evidence([competition_id])[competition_id]

    assert evidence.probe_status is ProbeStatus.PENDING
    assert evidence.probe_attempt_count == 1
    delay = evidence.next_probe_at - evidence.observed_at
    assert timedelta(minutes=15) <= delay < timedelta(minutes=16)


def test_unchanged_excluded_evidence_is_cached_but_catalog_change_forces_probe():
    competition_id = 10558
    base_catalog = {"countries": [{"leagues": [{"id": competition_id, "name": "Premier League"}]}]}
    profile = _competition_payload(competition_id, "Premier League", gender="female")
    catalog_url = canonicalize_target("allLeagues").canonical_url
    profile_url = canonicalize_target("leagues", {"id": competition_id}).canonical_url
    service, _, repository = _service({catalog_url: base_catalog, profile_url: profile})
    service.discover_catalog()

    cached_transport = StubTransport({catalog_url: base_catalog})
    cached_service = FotMobIngestService(
        transport=cached_transport,
        repository=repository,
        budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
        run_id="cached-run",
    )
    cached = cached_service.discover_catalog()
    assert cached.classifications[0].decision is ScopeDecision.EXCLUDED
    assert [url for url, _ in cached_transport.calls] == [catalog_url]

    changed_catalog = {"countries": [{"leagues": [{"id": competition_id, "name": "Renamed League"}]}]}
    changed_profile = _competition_payload(competition_id, "Renamed League", gender="female")
    changed_transport = StubTransport({catalog_url: changed_catalog, profile_url: changed_profile})
    changed_service = FotMobIngestService(
        transport=changed_transport,
        repository=repository,
        budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
        run_id="changed-run",
    )
    changed_service.discover_catalog()
    assert [url for url, _ in changed_transport.calls] == [catalog_url, profile_url]


def test_conflict_variant_only_metadata_change_forces_profile_reprobe():
    competition_id = 10559
    catalog_url = canonicalize_target("allLeagues").canonical_url
    profile_url = canonicalize_target("leagues", {"id": competition_id}).canonical_url

    def catalog(alias_name):
        return {
            "countries": [
                {
                    "name": "Canonical Country",
                    "leagues": [
                        {"id": competition_id, "name": "Canonical Senior Cup"}
                    ]
                },
                {
                    "name": "Alias Country",
                    "leagues": [{"id": competition_id, "name": alias_name}],
                },
            ]
        }

    profile = _competition_payload(competition_id, "Canonical Senior Cup")
    service, _, repository = _service(
        {catalog_url: catalog("Alias A"), profile_url: profile}
    )
    first = service.discover_catalog()
    assert first.classifications[0].decision is ScopeDecision.REVIEW_REQUIRED

    changed_transport = StubTransport(
        {catalog_url: catalog("Alias B"), profile_url: profile}
    )
    changed_service = FotMobIngestService(
        transport=changed_transport,
        repository=repository,
        budget=TransportBudget(max_requests=100, max_direct_bytes=10_000_000),
        run_id="variant-only-change",
    )

    changed_service.discover_catalog()

    assert [url for url, _ in changed_transport.calls] == [catalog_url, profile_url]


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
    service, _, repository = _service(
        {
            target: all_leagues,
            canonicalize_target("leagues", {"id": 47}).canonical_url: (
                _competition_payload(47, "Premier League")
            ),
            canonicalize_target("leagues", {"id": 999}).canonical_url: (
                _competition_payload(999, "Women Friendly Cup", gender="female")
            ),
        }
    )

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


def test_forced_player_refresh_reobserves_partial_current_run_commits():
    url10 = "https://www.fotmob.com/_next/data/build-1/players/10.json"
    url20 = "https://www.fotmob.com/_next/data/build-1/players/20.json"
    service, transport, _ = _service(
        {
            url10: [
                {"pageProps": {"data": {"id": 10, "name": "Ten"}}},
                {"pageProps": {"data": {"id": 10, "name": "Ten"}}},
            ],
            url20: {"pageProps": {"data": {"id": 20, "name": "Twenty"}}},
        },
        mode=RunMode.BACKFILL,
    )
    assert service.sync_player_snapshots([10], build_id="build-1").ok

    retried = service.sync_player_snapshots(
        [10, 20],
        build_id="build-1",
        force_refresh=True,
        capture_terminal_outcomes=True,
    )

    assert retried.ok and retried.succeeded == 2 and retried.skipped == 0
    assert retried.metadata["terminal_outcomes"] == [
        {"player_id": 10, "status": "success"},
        {"player_id": 20, "status": "success"},
    ]
    assert transport.calls[0][0] == url10
    assert sorted(call[0] for call in transport.calls[1:]) == sorted([url10, url20])


def test_backfill_skips_fresh_prior_generation_children():
    """#1146 отменяет контракт #995: свежие цели прошлого рана не перекачиваются.

    Прежнее поведение (backfill переобрабатывает всё, что собрал предыдущий ран)
    стоило 35 тыс. лишних запросов из 76 тыс. за трое суток — при том, что витрины
    `*_current` берут свежайший batch по натуральному ключу и к поколению не
    привязаны, то есть данные прошлого рана видны silver без перекачки.
    """

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
        commit = TargetCommit(
            run_id="prior-publication-generation",
            target_type=target_type,
            target_key=target.target_key,
            status=ManifestStatus.SUCCESS,
            entity_id=entity_id,
            content_hash="a" * 64,
            raw_uri=f"memory://{target.target_key}.json.gz",
            completed_at=datetime.now(timezone.utc),
        )
        repository.record(commit)
        if target_type == "team":
            # Состав прошлого рана обязан лежать в снимке: иначе fan-out на
            # игроков пуст сам по себе и ассерт по игрокам ничего не проверяет.
            repository.tables.setdefault("fotmob_squad_snapshots", []).append(
                {
                    "team_id": entity_id,
                    "member_type": "player",
                    "member_id": 10,
                    "_target_batch_id": commit.batch_id,
                }
            )

    leaderboard = service.sync_leaderboards(bundle)
    matches = service.sync_match_payloads(bundle)
    teams, player_ids = service.sync_team_snapshots(bundle)
    players = service.sync_player_snapshots(player_ids, build_id="build-1")

    assert all(result.ok for result in (leaderboard, matches, teams, players))
    # Всё, что прошлый ран собрал только что, пропускается целиком.
    assert leaderboard.skipped == 1
    assert matches.skipped == 1
    assert teams.skipped == 2
    # Игрок из состава свежей команды в план попал и пропущен по свежести —
    # а не «не планировался вовсе».
    assert player_ids == {10}
    assert players.attempted == 1
    assert players.skipped == 1
    # Главное следствие: ни одного сетевого обращения.
    assert transport.calls == []
    current_commits = [
        commit for commit in repository.commits if commit.run_id == service.run_id
    ]
    assert current_commits == []


def test_backfill_refetches_stale_prior_generation_children():
    """Обратная сторона #1146: протухшую цель прошлого рана всё равно берём."""

    bundle = parse_season_bundle(_league_payload(), ScopeRef(47, "2025/2026"))
    leaderboard_url = "https://data.fotmob.com/stats/47/season/goals.json"
    team_url = canonicalize_target("teams", {"id": "1"}).canonical_url
    # Bundle планирует обе команды; без ответа на вторую результат был бы
    # «зелёным, но пустым» — команды берутся, но с ошибкой.
    other_team_url = canonicalize_target("teams", {"id": "2"}).canonical_url
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
        team_url: team_payload,
        other_team_url: team_payload,
        player_url: {"pageProps": {"data": {"id": 10, "name": "Player"}}},
    }
    service, transport, repository = _service(responses, mode=RunMode.BACKFILL)
    now = datetime.now(timezone.utc)
    stale = (
        # лидерборд: старше LEADERBOARD_REFRESH_AFTER. Возраст берём от самой
        # константы, а не числом: тест проверяет «прошлое поколение перекачивается»,
        # и при пересмотре порога он не должен молча начать проверять другое.
        (
            "leaderboard",
            canonicalize_target(leaderboard_url),
            "goals",
            LEADERBOARD_REFRESH_AFTER + timedelta(hours=1),
        ),
        # команда: старше окна обновления состава. Возраст берём от константы,
        # а не числом: под прежними 20 часами литерал 21 ч означал «просрочено»,
        # а после подъёма порога молча стал бы означать «ещё свежее».
        ("team", canonicalize_target(team_url), "1", TEAM_REFRESH_AFTER + timedelta(hours=1)),
        # игрок: старше окна карточки игрока (PLAYER_REFRESH_AFTER, 14 суток)
        ("player", canonicalize_target(player_url), "10", timedelta(days=15)),
    )
    for target_type, target, entity_id, age in stale:
        repository.record(
            TargetCommit(
                run_id="prior-publication-generation",
                target_type=target_type,
                target_key=target.target_key,
                status=ManifestStatus.SUCCESS,
                entity_id=entity_id,
                content_hash="a" * 64,
                raw_uri=f"memory://{target.target_key}.json.gz",
                completed_at=now - age,
            )
        )

    leaderboard = service.sync_leaderboards(bundle)
    teams, player_ids = service.sync_team_snapshots(bundle)
    players = service.sync_player_snapshots(player_ids, build_id="build-1")

    assert all(result.ok for result in (leaderboard, teams, players))
    assert leaderboard.skipped == 0
    assert players.skipped == 0
    # Команда 1 протухла и берётся заново; команда 2 в манифесте не значится вовсе.
    assert teams.attempted == 2
    assert teams.skipped == 0
    fetched = {call[0] for call in transport.calls}
    assert leaderboard_url in fetched
    assert team_url in fetched
    assert other_team_url in fetched
    assert player_url in fetched


def test_stale_raw_replay_does_not_count_as_freshness_validation():
    """Реплей сырья при 5xx коммитится как success — но валидацией не является.

    Иначе недоступность источника морозит цель на весь TTL: лидерборд — на
    сутки, а матч (TTL бесконечен) — навсегда.
    """

    bundle = parse_season_bundle(_league_payload(), ScopeRef(47, "2025/2026"))
    leaderboard_url = "https://data.fotmob.com/stats/47/season/goals.json"
    match_url = canonicalize_target("matchDetails", {"matchId": "100"}).canonical_url
    responses = {
        leaderboard_url: {"TopLists": []},
        match_url: {"content": {"matchFacts": {"events": []}, "stats": {}}},
    }
    service, transport, repository = _service(responses, mode=RunMode.BACKFILL)
    for target_type, url, entity_id in (
        ("leaderboard", leaderboard_url, "goals"),
        ("match", match_url, "100"),
    ):
        target = canonicalize_target(url)
        repository.record(
            TargetCommit(
                run_id="prior-publication-generation",
                target_type=target_type,
                target_key=target.target_key,
                status=ManifestStatus.SUCCESS,
                entity_id=entity_id,
                content_hash="a" * 64,
                raw_uri=f"memory://{target.target_key}.json.gz",
                stale=True,
                completed_at=datetime.now(timezone.utc),
            )
        )

    leaderboard = service.sync_leaderboards(bundle)
    matches = service.sync_match_payloads(bundle)

    assert leaderboard.skipped == 0
    assert matches.skipped == 0
    fetched = {call[0] for call in transport.calls}
    assert leaderboard_url in fetched
    assert match_url in fetched


def test_player_freshness_survives_build_id_rotation():
    """Критерий A1: смена build id меняет URL игрока, но не его свежесть.

    Порог по ``entity_id`` (а не по URL) — единственное, что удерживает
    ротацию Next.js от полной перезакачки всех карточек игроков.
    """

    old_player_url = "https://www.fotmob.com/_next/data/build-1/players/10.json"
    new_player_url = "https://www.fotmob.com/_next/data/build-2/players/10.json"
    service, transport, repository = _service(
        {new_player_url: {"pageProps": {"data": {"id": 10, "name": "Player"}}}},
        mode=RunMode.BACKFILL,
    )
    old_target = canonicalize_target(old_player_url)
    repository.record(
        TargetCommit(
            run_id="prior-publication-generation",
            target_type="player",
            target_key=old_target.target_key,
            status=ManifestStatus.SUCCESS,
            entity_id="10",
            content_hash="a" * 64,
            raw_uri=f"memory://{old_target.target_key}.json.gz",
            completed_at=datetime.now(timezone.utc),
        )
    )

    players = service.sync_player_snapshots({10}, build_id="build-2")

    assert canonicalize_target(new_player_url).target_key != old_target.target_key
    assert players.attempted == 1
    assert players.skipped == 1
    assert transport.calls == []


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
    # Не tombstone, но и не терминальный отказ: цель уходит в повтор, а не
    # хоронит скоуп и цвет рана.
    assert result.retryable
    assert not result.terminal
    assert result.status == "retryable"
    assert not result.ok


def test_generic_transport_absence_cannot_publish_entity_tombstone():
    service, _, repository = _service({})
    service._commit_for_fetch(
        _absent_team_fetch(FetchOutcome.NOT_AVAILABLE),
        target_type="team",
        entity_id="2222",
    )

    assert repository.commits[-1].status == ManifestStatus.TERMINAL_FAILURE


def test_advertised_team_absence_resolves_without_tombstone():
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
        if commit.error_code == "source_team_unavailable"
    ]
    assert len(absences) == 2
    assert all(commit.status == ManifestStatus.EXCLUDED for commit in absences)
    # EXCLUDED is a plan disposition, not an entity tombstone: the prior
    # global observation remains the latest serving success.
    assert repository.latest_entity_success("team", "1")["status"] == "success"


def _stub_team_fetch(team_id, payload):
    target = canonicalize_target("teams", {"id": str(team_id)})
    body = json.dumps(payload).encode()
    return FetchResult(
        outcome=FetchOutcome.SUCCESS,
        target_key=target.target_key,
        url=target.canonical_url,
        http_status=200,
        json_data=payload,
        body=body,
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
        encoded_bytes=len(body),
        decoded_bytes=len(body),
        direct_bytes=len(body),
        proxy_bytes=0,
    )


def test_not_found_stub_for_an_advertised_team_is_an_absence_not_drift():
    # An unfilled future bracket advertises team_id 0; /teams answers 200 with
    # a routing stub for it, which used to raise CatalogShapeError every run.
    bundle = parse_season_bundle(
        _league_payload(selected="2010/2011"),
        ScopeRef(47, "2010/2011"),
    )
    service, _, repository = _service({}, mode=RunMode.BACKFILL)
    service._fetch_many = lambda requests: {
        key: _stub_team_fetch(key, {"action": {"notFound": {"statusCode": 404}}})
        for key, _, _ in requests
    }

    result, player_ids = service.sync_team_snapshots(
        bundle,
        allow_advertised_absence=True,
    )

    assert result.ok
    assert not result.errors
    assert result.not_available == 2
    assert result.metadata["intentional_not_available"] == 2
    assert player_ids == set()
    absences = [
        commit
        for commit in repository.commits
        if commit.error_code == "source_team_unavailable"
    ]
    assert len(absences) == 2
    assert all(commit.status == ManifestStatus.EXCLUDED for commit in absences)


def test_a_team_payload_carrying_the_stub_key_still_fails_closed():
    # Only the bare stub counts as an absence: a payload that also carries team
    # JSON is a real shape change and must stay a hard failure.
    bundle = parse_season_bundle(
        _league_payload(selected="2010/2011"),
        ScopeRef(47, "2010/2011"),
    )
    service, _, _ = _service({}, mode=RunMode.BACKFILL)
    service._fetch_many = lambda requests: {
        key: _stub_team_fetch(
            key,
            {"action": {"notFound": {}}, "details": {"id": int(key)}},
        )
        for key, _, _ in requests
    }

    result, _ = service.sync_team_snapshots(
        bundle,
        allow_advertised_absence=True,
    )

    assert result.not_available == 0
    assert result.errors
    assert not result.ok


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


def test_rotated_player_build_has_exact_64_request_retry_ceiling():
    class RotatingBuildTransport:
        max_attempts = 4

        def __init__(self):
            self.document_calls = 0

        @staticmethod
        def _result(url, outcome, *, data=None, body=None, status=200):
            target = canonicalize_target(url)
            raw_body = body if body is not None else json.dumps(data).encode()
            return FetchResult(
                outcome=outcome,
                target_key=target.target_key,
                url=target.canonical_url,
                http_status=status,
                json_data=data,
                body=raw_body,
                attempts=4,
                retries=3,
                cache_hit=False,
                stale=False,
                terminal=outcome == FetchOutcome.NOT_AVAILABLE,
                etag=None,
                last_modified=None,
                raw_uri=f"memory://{target.target_key}.json.gz",
                content_hash=hashlib.sha256(raw_body).hexdigest(),
                fetched_at="2026-07-21T10:00:00+00:00",
                encoded_bytes=len(raw_body),
                decoded_bytes=len(raw_body),
                direct_bytes=len(raw_body),
                proxy_bytes=0,
            )

        def fetch_document(self, url):
            self.document_calls += 1
            build = "stale-build" if self.document_calls == 1 else "fresh-build"
            body = f'{{"buildId":"{build}"}}'.encode()
            return self._result(
                url,
                FetchOutcome.SUCCESS,
                body=body,
                data=None,
            )

        def fetch_json(self, endpoint, params=None):
            player_id = int(str(endpoint).removesuffix(".json").rsplit("/", 1)[-1])
            if "/stale-build/" in str(endpoint):
                return self._result(
                    endpoint,
                    FetchOutcome.NOT_AVAILABLE,
                    data=None,
                    body=b"not found",
                    status=404,
                )
            return self._result(
                endpoint,
                FetchOutcome.SUCCESS,
                data={
                    "pageProps": {
                        "data": {"id": player_id, "name": f"Player {player_id}"}
                    }
                },
            )

    transport = RotatingBuildTransport()
    service = FotMobIngestService(
        transport=transport,
        repository=MemoryFotMobRepository(),
        mode=RunMode.BACKFILL,
        budget=TransportBudget(max_requests=64, max_direct_bytes=1_000_000),
        run_id="source-refresh-rotated-build",
        max_workers=1,
    )

    result = service.sync_player_snapshots(range(1, 8), capture_terminal_outcomes=True)

    assert result.ok and result.succeeded == 7 and result.skipped == 0
    assert transport.document_calls == 2
    assert service.ledger.requests == 64
    assert result.metadata["terminal_outcomes"] == [
        {"player_id": player_id, "status": "success"} for player_id in range(1, 8)
    ]


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


def test_offline_player_replay_migrates_raw_bearing_v1_not_available(tmp_path):
    from scrapers.fotmob.raw_store import FotMobRawStore
    from scrapers.fotmob.repository import LEGACY_PARSER_VERSION, PARSER_VERSION
    from scrapers.fotmob.transport import FotMobTransport

    raw_store = FotMobRawStore.from_uri(tmp_path.as_uri())
    historical = canonicalize_target(
        "https://www.fotmob.com/_next/data/historical-build/players/2090857.json"
    )
    body = b'{"pageProps":{"data":null,"fallback":{},"translations":{}}}'
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
            status=ManifestStatus.NOT_AVAILABLE,
            entity_id="2090857",
            content_hash=raw.content_hash,
            raw_uri=raw.raw_uri,
            parser_version=LEGACY_PARSER_VERSION,
            fetched_at=datetime(2026, 7, 20, 10, 0),
            completed_at=datetime(2026, 7, 20, 10, 0),
            error_code="source_player_no_data",
        )
    )
    service = FotMobIngestService(
        transport=FotMobTransport(raw_store),
        repository=repository,
        mode=RunMode.REPLAY,
        budget=TransportBudget(max_requests=1, max_direct_bytes=1),
        run_id="issue930-v2-replay",
        max_workers=1,
    )

    result = service.sync_player_snapshots([2090857], capture_terminal_outcomes=True)

    assert result.ok and result.not_available == 1 and result.succeeded == 0
    assert result.metadata["intentional_not_available"] == 1
    assert result.metadata["terminal_outcomes"] == [
        {"player_id": 2090857, "status": "not_available"}
    ]
    assert "fotmob_player_snapshots" not in repository.tables
    replay = repository.commits[-1]
    assert replay.status == ManifestStatus.NOT_AVAILABLE
    assert replay.parser_version == PARSER_VERSION
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


def _null_body_fetch(endpoint, params):
    target = canonicalize_target(endpoint, params)
    return FetchResult(
        outcome=FetchOutcome.NOT_AVAILABLE,
        target_key=target.target_key,
        url=target.canonical_url,
        http_status=304,
        json_data=None,
        body=b"null",
        attempts=1,
        retries=0,
        cache_hit=True,
        stale=False,
        terminal=True,
        etag='"etag"',
        last_modified=None,
        raw_uri=None,
        content_hash=None,
        fetched_at=None,
        encoded_bytes=4,
        decoded_bytes=4,
        direct_bytes=0,
        proxy_bytes=0,
    )


def test_dead_catalog_entry_resolves_as_intentional_absence():
    # #1070: allLeagues advertises the id, /leagues answers a null body. That
    # is a fact about the source catalog, not a collection failure.
    from scrapers.fotmob.catalog import classify_competition
    from scrapers.fotmob.domain import CompetitionRef, ScopeDecision

    catalog = CompetitionRef(285, "Landesliga")
    profile = CompetitionRef(
        285,
        "Landesliga",
        gender="male",
        competition_type="league",
        age_group="adult",
    )
    classification = classify_competition(catalog, profile)
    assert classification.decision == ScopeDecision.INCLUDED
    service, _, repository = _service({})

    outcome = service.discover_competition(
        classification,
        prefetched=_null_body_fetch("leagues", {"id": 285}),
    )

    assert outcome.operation.ok
    assert outcome.operation.not_available == 1
    assert outcome.operation.metadata["intentional_not_available"] == 1
    commit = repository.commits[-1]
    assert commit.status == ManifestStatus.EXCLUDED
    assert commit.error_code == "source_dead_catalog_entry"


def test_transfer_stream_tolerates_source_hits_self_disagreement():
    # #1074: every page fetched, stream ran dry, unique events short of the
    # source's own ``hits`` by <= 2 — complete, deficit recorded as metadata.
    page1 = canonicalize_target(
        "transfers", {"leagueIds": "47", "page": 1}
    ).canonical_url
    page2 = canonicalize_target(
        "transfers", {"leagueIds": "47", "page": 2}
    ).canonical_url
    first = {
        "hits": 3,
        "page": 1,
        "transfers": [
            {
                "playerId": 1,
                "name": "One",
                "transferDate": "2026-07-01",
                "fromClubId": 10,
                "toClubId": 20,
                "feeText": "€1m",
            },
            {
                "playerId": 2,
                "name": "Two",
                "transferDate": "2026-07-02",
                "fromClubId": 30,
                "toClubId": 40,
                "feeText": "Free",
            },
        ],
    }
    second = {"hits": 3, "page": 2, "transfers": []}
    service, _, _ = _service({page1: first, page2: second})

    result = service.sync_transfers(47)

    assert result.ok
    assert result.counts["events"] == 2
    assert result.metadata["source_hits_deficit"] == 1
    assert "next_missing_page" not in result.metadata


def test_transfer_stream_rejects_malformed_nonempty_terminal_page():
    page1 = canonicalize_target(
        "transfers", {"leagueIds": "47", "page": 1}
    ).canonical_url
    page2 = canonicalize_target(
        "transfers", {"leagueIds": "47", "page": 2}
    ).canonical_url
    first = {
        "hits": 3,
        "page": 1,
        "transfers": [
            {
                "playerId": 1,
                "name": "One",
                "transferDate": "2026-07-01",
                "fromClubId": 10,
                "toClubId": 20,
                "feeText": "€1m",
            },
            {
                "playerId": 2,
                "name": "Two",
                "transferDate": "2026-07-02",
                "fromClubId": 30,
                "toClubId": 40,
                "feeText": "Free",
            },
        ],
    }
    second = {"hits": 3, "page": 2, "transfers": [None]}
    service, _, _ = _service({page1: first, page2: second})

    result = service.sync_transfers(47)

    assert not result.ok
    assert any("transfers page 2 parse" in item for item in result.errors)
    assert "source_hits_deficit" not in result.metadata


def test_transfer_stream_tolerance_scales_with_stream_size():
    """Большой поток: расхождение счётчика источника растёт вместе с ним.

    13.08 у турнира 86 источник отдал 677 строк без единого дубля и заявил 681 —
    собирать было нечего, но абсолютный допуск в два события красил ран целиком.
    """

    page1 = canonicalize_target(
        "transfers", {"leagueIds": "47", "page": 1}
    ).canonical_url
    page2 = canonicalize_target(
        "transfers", {"leagueIds": "47", "page": 2}
    ).canonical_url
    first = {
        "hits": 300,
        "page": 1,
        "transfers": [
            {
                "playerId": index,
                "name": f"Player {index}",
                "transferDate": "2026-07-01",
                "fromClubId": 10,
                "toClubId": 20,
                "feeText": "Free",
            }
            for index in range(1, 298)
        ],
    }
    second = {"hits": 300, "page": 2, "transfers": []}
    service, _, _ = _service({page1: first, page2: second})

    result = service.sync_transfers(47)

    assert result.ok
    assert result.counts["events"] == 297
    assert result.metadata["source_hits_deficit"] == 3


def test_transfer_stream_large_deficit_beyond_ratio_tolerance_stays_incomplete():
    page1 = canonicalize_target(
        "transfers", {"leagueIds": "47", "page": 1}
    ).canonical_url
    page2 = canonicalize_target(
        "transfers", {"leagueIds": "47", "page": 2}
    ).canonical_url
    first = {
        "hits": 649,
        "page": 1,
        "transfers": [
            {
                "playerId": index,
                "name": f"Player {index}",
                "transferDate": "2026-07-01",
                "fromClubId": 10,
                "toClubId": 20,
                "feeText": "Free",
            }
            for index in range(1, 51)
        ],
    }
    second = {"hits": 649, "page": 2, "transfers": []}
    service, _, _ = _service({page1: first, page2: second})

    result = service.sync_transfers(47)

    assert not result.ok
    assert "source_hits_deficit" not in result.metadata
    assert any("transfer pagination incomplete" in item for item in result.errors)


def test_transfer_stream_deficit_beyond_tolerance_stays_incomplete():
    page1 = canonicalize_target(
        "transfers", {"leagueIds": "47", "page": 1}
    ).canonical_url
    page2 = canonicalize_target(
        "transfers", {"leagueIds": "47", "page": 2}
    ).canonical_url
    first = {
        "hits": 9,
        "page": 1,
        "transfers": [
            {
                "playerId": 1,
                "name": "One",
                "transferDate": "2026-07-01",
                "fromClubId": 10,
                "toClubId": 20,
                "feeText": "€1m",
            }
        ],
    }
    second = {"hits": 9, "page": 2, "transfers": []}
    service, _, _ = _service({page1: first, page2: second})

    result = service.sync_transfers(47)

    assert not result.ok
    assert any("transfer pagination incomplete" in item for item in result.errors)
    assert "source_hits_deficit" not in result.metadata


def test_leaderboard_freshness_is_keyed_by_url_not_category_name():
    """Анти-мина #1146: порог свежести лидерборда — по target_key, не по имени.

    Имя категории ("goals") повторяется у сотен турниров: живьём 72 значения
    entity_id на 9146 разных URL. Если бы порог считался по entity_id, свежий
    лидерборд одного турнира закрывал бы цели всех остальных.
    """

    own_url = "https://data.fotmob.com/stats/47/season/goals.json"
    other_url = "https://data.fotmob.com/stats/48/season/goals.json"
    other_payload = copy.deepcopy(_league_payload())
    other_payload["details"]["id"] = 48
    other_payload["stats"]["players"][0]["fetchAllUrl"] = other_url
    bundle = parse_season_bundle(other_payload, ScopeRef(48, "2025/2026"))

    service, transport, repository = _service({other_url: {"TopLists": []}})
    # Свежий лидерборд ЧУЖОГО турнира с тем же именем категории.
    repository.record(
        TargetCommit(
            run_id="prior-run",
            target_type="leaderboard",
            target_key=canonicalize_target(own_url).target_key,
            status=ManifestStatus.SUCCESS,
            entity_id="goals",
            content_hash="a" * 64,
            raw_uri="memory://own.json.gz",
            completed_at=datetime.now(timezone.utc),
        )
    )

    result = service.sync_leaderboards(bundle)

    assert result.skipped == 0
    assert [call[0] for call in transport.calls] == [other_url]


def test_leaderboard_threshold_outlives_the_lane_period():
    """A6/#1198: порог таблиц лидеров обязан быть ДЛИННЕЕ периода полосы.

    Урок 74: период полосы контура — 24 часа, поэтому порог, равный периоду или
    короче, не отсекает практически ничего. Прежние 24 часа были ровно таким
    декоративным порогом: замер 20.08 за 7 суток дал у leaderboard 25 368 сетевых
    обращений (28,2 % физического бюджета) при медиане реального цикла повтора
    57,7 часа — то есть цель и так ходила вдвое реже порога.

    Тест держит две вещи разом: конкретное принятое значение и правило, по
    которому оно выбрано. Возврат к 24 часам (или к любому значению ≤ периода
    полосы) обязан ронять этот тест, а не проходить молча.
    """

    lane_period = timedelta(hours=24)

    assert LEADERBOARD_REFRESH_AFTER == timedelta(hours=72)
    assert LEADERBOARD_REFRESH_AFTER > lane_period
    # 57,7 ч — измеренная медиана; порог обязан её перекрывать, иначе он не
    # отсекает даже те повторы, которые цель делает сама по себе.
    assert LEADERBOARD_REFRESH_AFTER > timedelta(hours=57, minutes=42)


def test_team_threshold_outlives_the_lane_period():
    """A6/#1198: порог карточки команды обязан быть ДЛИННЕЕ периода полосы.

    Прежние 20 часов были короче периода полосы (24 ч) и по уроку 74 почти ничего
    не отсекали, при том что команда — 26,7 % физических сетевых обращений
    платформы, вторая статья бюджета.

    Цена подъёма измерена (замер 20.08, 14 суток): паспорт команды на интервале
    24–48 ч меняется у 9 повторов из 6661 (0,14 %), состав — у 936 из 4808
    (19,5 %). То есть порог платит не свежестью витрины команд, а задержкой
    обнаружения смены состава на срок до двух суток.

    Тест держит и значение, и правило выбора: возврат к любому значению
    ≤ периода полосы обязан его уронить.
    """

    lane_period = timedelta(hours=24)

    assert TEAM_REFRESH_AFTER == timedelta(hours=48)
    assert TEAM_REFRESH_AFTER > lane_period


def test_stale_leaderboard_past_the_threshold_is_still_refetched():
    """Обратная сторона порога: перешагнувший его лидерборд обязан перекачаться.

    Возраст записи задан ОТНОСИТЕЛЬНО константы, поэтому тест остаётся зелёным
    при любом её значении — от бесконечного порога он не защищает, это делает
    `test_leaderboard_threshold_outlives_the_lane_period`. Здесь проверяется
    другое: что предикат свежести не вывернут и пропуск не безусловен, то есть
    что старая запись действительно приводит к сетевому вызову.
    """

    url = "https://data.fotmob.com/stats/47/season/goals.json"
    payload = copy.deepcopy(_league_payload())
    payload["stats"]["players"][0]["fetchAllUrl"] = url
    bundle = parse_season_bundle(payload, ScopeRef(47, "2025/2026"))

    service, transport, repository = _service({url: {"TopLists": []}})
    repository.record(
        TargetCommit(
            run_id="prior-run",
            target_type="leaderboard",
            target_key=canonicalize_target(url).target_key,
            status=ManifestStatus.SUCCESS,
            entity_id="goals",
            content_hash="a" * 64,
            raw_uri="memory://own.json.gz",
            completed_at=datetime.now(timezone.utc)
            - (LEADERBOARD_REFRESH_AFTER + timedelta(hours=1)),
        )
    )

    result = service.sync_leaderboards(bundle)

    assert result.skipped == 0
    assert [call[0] for call in transport.calls] == [url]


def test_player_refresh_is_rarer_but_first_collection_is_never_delayed():
    """A2 (решение владельца 18.08): реже обновляем, но первичный сбор не трогаем.

    Замер 15–18.08: игроки — 63,5 % бюджета волны, и 94,6 % этих запросов
    приходится на ПОВТОРНОЕ обновление уже собранной карточки. Порог режет
    именно их; игрок, которого в манифесте нет вовсе, обязан собираться сразу,
    иначе словарь игроков перестанет пополняться.
    """

    player_url = "https://www.fotmob.com/_next/data/build-1/players/10.json"
    fresh_player_url = "https://www.fotmob.com/_next/data/build-1/players/11.json"
    service, transport, repository = _service(
        {
            player_url: {"pageProps": {"data": {"id": 10, "name": "Known"}}},
            fresh_player_url: {"pageProps": {"data": {"id": 11, "name": "New"}}},
        },
        mode=RunMode.BACKFILL,
    )
    target = canonicalize_target(player_url)
    repository.record(
        TargetCommit(
            run_id="prior",
            target_type="player",
            target_key=target.target_key,
            status=ManifestStatus.SUCCESS,
            entity_id="10",
            content_hash="a" * 64,
            raw_uri=f"memory://{target.target_key}.json.gz",
            # Прежний порог (7 суток) отправил бы карточку на перекачку;
            # принятый (14 суток) — нет. Возраст фиксирован намеренно: если
            # порог однажды опустят обратно, тест обязан упасть.
            completed_at=datetime.now(timezone.utc) - timedelta(days=13),
        )
    )

    players = service.sync_player_snapshots({10, 11}, build_id="build-1")

    assert players.ok, players.errors
    assert PLAYER_REFRESH_AFTER == timedelta(days=14)
    assert players.attempted == 2
    # Известная карточка десятидневной давности больше не перекачивается...
    assert players.skipped == 1
    # ...а игрок, которого в манифесте нет, собран немедленно.
    assert players.succeeded == 1
    assert [call[0] for call in transport.calls] == [fresh_player_url]


def test_player_card_older_than_the_threshold_is_refetched():
    """Обратная сторона A2: за порогом карточка всё-таки обновляется."""

    player_url = "https://www.fotmob.com/_next/data/build-1/players/10.json"
    service, transport, repository = _service(
        {player_url: {"pageProps": {"data": {"id": 10, "name": "Known"}}}},
        mode=RunMode.BACKFILL,
    )
    target = canonicalize_target(player_url)
    repository.record(
        TargetCommit(
            run_id="prior",
            target_type="player",
            target_key=target.target_key,
            status=ManifestStatus.SUCCESS,
            entity_id="10",
            content_hash="a" * 64,
            raw_uri=f"memory://{target.target_key}.json.gz",
            # Ровно за порогом, фиксированным числом: TTL длиннее 15 суток
            # этот тест обязан ронять.
            completed_at=datetime.now(timezone.utc) - timedelta(days=15),
        )
    )

    players = service.sync_player_snapshots({10}, build_id="build-1")

    assert players.ok, players.errors
    assert players.skipped == 0
    assert players.succeeded == 1
    assert [call[0] for call in transport.calls] == [player_url]
