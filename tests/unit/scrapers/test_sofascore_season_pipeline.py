from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pyarrow import fs

from scrapers.sofascore.capture_engine import (
    EndpointSpec,
    ParsedDataset,
    RetryPolicy,
    SofaScoreCaptureEngine,
)
from scrapers.sofascore.manifest import (
    EndpointManifest,
    InMemoryManifestStore,
    ManifestStatus,
)
from scrapers.sofascore.pipeline import (
    CaptureRuntime,
    DeferredCaptureSink,
    finalize_materialized_results,
)
from scrapers.sofascore.raw_store import PayloadTarget, RawPayloadStore
from scrapers.sofascore.season_pipeline import (
    SeasonMaterializationError,
    SeasonPlanningError,
    build_cup_trees_spec,
    build_participants_spec,
    build_referee_profile_spec,
    build_rounds_spec,
    build_schedule_page_spec,
    build_season_specs,
    build_squad_spec,
    build_standings_total_spec,
    materialize_season_partition,
    plan_season_partition,
    replay_season_partition,
    replay_season_specs,
    squad_player_ids,
)


FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
TOURNAMENT_ID = 17
SEASON_ID = 76986
FRESHNESS = "season-final-v1"

FIXTURE_PATHS = {
    "schedule_last": FIXTURES / "sofascore_season_76986_schedule_last_0.json",
    "schedule_next": FIXTURES / "sofascore_season_76986_schedule_next_0.json",
    "standings_total": FIXTURES / "sofascore_season_76986_standings_total.json",
    "rounds": FIXTURES / "sofascore_season_76986_rounds.json",
    "cup_trees": FIXTURES / "sofascore_season_76986_cup_trees.json",
    "participants": FIXTURES / "sofascore_season_76986_participants.json",
    "squads": FIXTURES / "sofascore_team_42_season_76986_squad.json",
    "referee_profile": FIXTURES / "sofascore_referee_900_profile.json",
}
PLAYER_EVIDENCE_CASES = FIXTURES / "sofascore_season_76986_player_evidence_cases.json"
CUP_BRACKET_NEXT_PAGE = FIXTURES / "sofascore_season_76986_cup_schedule_next_0.json"


class UnlimitedLimiter:
    def acquire(self):
        return True


class NoNetworkTransport:
    def __init__(self):
        self.calls = 0

    def request(self, url, *, provider_budget):
        self.calls += 1
        raise AssertionError("offline season replay attempted source access")


class RecordingSink:
    def __init__(self):
        self.calls = []
        self._lock = threading.Lock()

    def write(self, key, datasets, raw):
        with self._lock:
            self.calls.append((key, datasets, raw))


def _raw_store(tmp_path) -> RawPayloadStore:
    return RawPayloadStore(fs.LocalFileSystem(), str(tmp_path / "raw"))


def _engine(tmp_path, *, raw_store=None, manifest_store=None, sink=None):
    transport = NoNetworkTransport()
    engine = SofaScoreCaptureEngine(
        raw_store=raw_store or _raw_store(tmp_path),
        manifest_store=manifest_store or InMemoryManifestStore(),
        transport=transport,
        run_id="season-fixture-run",
        task_id="season-replay",
        sink=sink or RecordingSink(),
        rate_limiter=UnlimitedLimiter(),
        retry_policy=RetryPolicy(max_attempts=1),
        max_workers=2,
    )
    return engine, transport


def _common() -> dict:
    return {
        "source_tournament_id": TOURNAMENT_ID,
        "source_season_id": SEASON_ID,
        "freshness_key": FRESHNESS,
        "paid_proxy": False,
    }


def _specs() -> list[EndpointSpec]:
    return [
        build_schedule_page_spec(direction="last", page=0, **_common()),
        build_schedule_page_spec(direction="next", page=0, **_common()),
        build_standings_total_spec(**_common()),
        build_rounds_spec(**_common()),
        build_cup_trees_spec(**_common()),
        build_participants_spec(**_common()),
        build_squad_spec(team_id=42, **_common()),
        build_referee_profile_spec(referee_id=900, **_common()),
    ]


def _payload(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def _seed_raw(store: RawPayloadStore, spec: EndpointSpec, body: bytes) -> None:
    store.store_bytes(
        spec.raw_target,
        body,
        request_url=spec.url,
        http_status=200,
        response_headers={"content-type": "application/json"},
    )


def _seed_json(store: RawPayloadStore, spec: EndpointSpec, payload: object) -> None:
    _seed_raw(
        store,
        spec,
        json.dumps(payload, separators=(",", ":")).encode("utf-8"),
    )


def _schedule_event(event_id: int, *, referee_id: int | None = None) -> dict:
    event = json.loads(
        json.dumps(_payload(FIXTURE_PATHS["schedule_last"])["events"][0])
    )
    event["id"] = event_id
    if referee_id is not None:
        event["referee"] = {"id": referee_id, "name": f"Referee {referee_id}"}
    return event


def _schedule_payload(event_ids, *, has_next: bool) -> dict:
    return {
        "events": [_schedule_event(event_id) for event_id in event_ids],
        "hasNextPage": has_next,
    }


def _placeholder_team(team_id: int) -> dict:
    """Unresolved knockout-bracket slot in the exact shape SofaScore sends (#946)."""

    return {
        "id": team_id,
        "name": f"Winner of match {team_id}",
        "slug": f"winner-of-match-{team_id}",
        "gender": "M",
        "userCount": 0,
        "type": 0,
        "country": {},
        "disabled": True,
    }


def _seed_full_event_referee(
    store: RawPayloadStore,
    *,
    event_id: int,
    referee_id: int,
    freshness_key: str = FRESHNESS,
) -> None:
    target = PayloadTarget(
        source_tournament_id=str(TOURNAMENT_ID),
        source_season_id=str(SEASON_ID),
        target_type="event",
        target_id=str(event_id),
        endpoint="event",
        freshness_key=freshness_key,
    )
    payload = {
        "event": {
            "id": event_id,
            "season": {"id": SEASON_ID},
            "referee": {"id": referee_id, "name": f"Referee {referee_id}"},
        }
    }
    store.store_bytes(
        target,
        json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        request_url=f"https://www.sofascore.com/api/v1/event/{event_id}",
        http_status=200,
        response_headers={"content-type": "application/json"},
    )


def _seed_complete_partition_roots(
    store: RawPayloadStore,
    *,
    schedule_next_payload=None,
) -> None:
    evidence = _payload(PLAYER_EVIDENCE_CASES)
    if schedule_next_payload is None:
        schedule_next_payload = _payload(FIXTURE_PATHS["schedule_next"])
    roots = [
        (
            build_schedule_page_spec(direction="last", page=0, **_common()),
            _payload(FIXTURE_PATHS["schedule_last"]),
        ),
        (
            build_schedule_page_spec(direction="next", page=0, **_common()),
            schedule_next_payload,
        ),
        (
            build_standings_total_spec(**_common()),
            _payload(FIXTURE_PATHS["standings_total"]),
        ),
        (build_rounds_spec(**_common()), _payload(FIXTURE_PATHS["rounds"])),
        (
            build_cup_trees_spec(**_common()),
            _payload(FIXTURE_PATHS["cup_trees"]),
        ),
        (
            build_participants_spec(**_common()),
            evidence["full_participants"],
        ),
    ]
    for spec, payload in roots:
        _seed_json(store, spec, payload)
    _seed_full_event_referee(store, event_id=14000001, referee_id=900)


def _complete_plan_with_expansion_raw(store, manifest):
    _seed_complete_partition_roots(store)
    plan = plan_season_partition(
        store,
        manifest,
        **_common(),
    )
    for spec in plan.specs:
        if spec.key.endpoint == "squads":
            _seed_raw(store, spec, FIXTURE_PATHS["squads"].read_bytes())
        elif spec.key.endpoint == "referee_profile":
            _seed_raw(
                store,
                spec,
                FIXTURE_PATHS["referee_profile"].read_bytes(),
            )
    return plan


@pytest.mark.unit
def test_normalized_fixture_specs_use_existing_schedule_and_standings_shapes():
    last, next_page, standings = _specs()[:3]
    last_payload = _payload(FIXTURE_PATHS["schedule_last"])
    next_payload = _payload(FIXTURE_PATHS["schedule_next"])
    standings_payload = _payload(FIXTURE_PATHS["standings_total"])

    assert last.schema_validator(last_payload) is True
    schedule = last.parsers["schedule"](last_payload)
    assert [row["game_id"] for row in schedule] == [14000001, 14000002]
    assert all(row["source_season_id"] == str(SEASON_ID) for row in schedule)
    assert all(row["source_page_direction"] == "last" for row in schedule)
    assert next_page.schema_validator(next_payload) is True
    assert next_page.empty_predicate(next_payload) is True

    assert standings.schema_validator(standings_payload) is True
    table = standings.parsers["league_table"](standings_payload)
    assert len(table) == 4
    assert {row["group"] for row in table} == {"Group A", "Group B"}
    assert {row["team_id"] for row in table} == {"17", "33", "42", "44"}


@pytest.mark.unit
def test_manifest_keys_distinguish_pages_teams_and_referees():
    pages = [
        build_schedule_page_spec(direction="last", page=0, **_common()),
        build_schedule_page_spec(direction="last", page=1, **_common()),
        build_schedule_page_spec(direction="next", page=0, **_common()),
    ]
    squads = [
        build_squad_spec(team_id=42, **_common()),
        build_squad_spec(team_id=44, **_common()),
    ]
    referees = [
        build_referee_profile_spec(referee_id=900, **_common()),
        build_referee_profile_spec(referee_id=901, **_common()),
    ]

    assert [spec.key.target_id for spec in pages] == [
        "last:0",
        "last:1",
        "next:0",
    ]
    assert [spec.key.target_id for spec in squads] == ["42", "44"]
    assert [spec.key.target_id for spec in referees] == ["900", "901"]
    assert len({spec.key for spec in [*pages, *squads, *referees]}) == 7
    assert all(spec.paid_proxy is False for spec in [*pages, *squads, *referees])


@pytest.mark.unit
def test_season_plan_has_unique_normalized_and_raw_only_specs():
    specs = build_season_specs(
        last_pages=(0, 1),
        next_pages=(0,),
        **_common(),
    )
    assert len({spec.key for spec in specs}) == len(specs)
    assert {spec.key.endpoint for spec in specs} == {
        "schedule_last",
        "schedule_next",
        "standings_total",
        "rounds",
        "cup_trees",
        "participants",
    }
    assert {spec.key.endpoint for spec in specs if spec.raw_only} == {
        "rounds",
        "cup_trees",
        "participants",
    }


@pytest.mark.unit
def test_raw_only_endpoint_spec_is_parserless_but_standard_specs_are_not():
    raw = build_rounds_spec(**_common())
    assert raw.raw_only is True
    assert raw.parsers == {}
    with pytest.raises(ValueError, match="at least one parser"):
        EndpointSpec(
            key=raw.key,
            url=raw.url,
            schema_validator=raw.schema_validator,
            empty_predicate=raw.empty_predicate,
            parsers={},
            paid_proxy=False,
        )
    with pytest.raises(ValueError, match="must not declare parsers"):
        EndpointSpec(
            key=raw.key,
            url=raw.url,
            schema_validator=raw.schema_validator,
            empty_predicate=raw.empty_predicate,
            parsers={"fake": lambda payload: [{}]},
            paid_proxy=False,
            raw_only=True,
        )


@pytest.mark.unit
def test_fixture_offline_replay_retains_exact_raw_without_fake_tables(tmp_path):
    raw_store = _raw_store(tmp_path)
    sink = RecordingSink()
    specs = _specs()
    for spec in specs:
        body = FIXTURE_PATHS[spec.key.endpoint].read_bytes()
        _seed_raw(raw_store, spec, body)

    engine, transport = _engine(tmp_path, raw_store=raw_store, sink=sink)
    results = replay_season_specs(engine, specs)
    by_endpoint = {result.manifest.key.endpoint: result for result in results}

    assert transport.calls == 0
    assert by_endpoint["schedule_last"].manifest.row_count == 2
    assert by_endpoint["schedule_next"].manifest.status == (
        ManifestStatus.LEGITIMATE_EMPTY
    )
    assert by_endpoint["standings_total"].manifest.row_count == 4
    assert {key.endpoint for key, _, _ in sink.calls} == {
        "schedule_last",
        "standings_total",
    }

    for endpoint in (
        "rounds",
        "cup_trees",
        "participants",
        "squads",
        "referee_profile",
    ):
        result = by_endpoint[endpoint]
        body = FIXTURE_PATHS[endpoint].read_bytes()
        assert result.manifest.status == ManifestStatus.SUCCESS
        assert result.manifest.row_count == 1  # one retained raw payload unit
        assert result.datasets == {}
        assert result.replay_hit is True
        assert result.raw.content_hash == hashlib.sha256(body).hexdigest()
        stored, record = raw_store.load_bytes(result.raw.target)
        assert stored == body
        assert record.content_hash == result.raw.content_hash


@pytest.mark.unit
@pytest.mark.parametrize(
    ("builder", "payload"),
    [
        (
            lambda: build_schedule_page_spec(direction="last", page=0, **_common()),
            {"events": [{"id": 1, "season": {"id": SEASON_ID}}]},
        ),
        (
            lambda: build_standings_total_spec(**_common()),
            {"standings": [{"type": "total", "rows": {}}]},
        ),
        (lambda: build_rounds_spec(**_common()), {"rounds": {}}),
        (
            lambda: build_referee_profile_spec(referee_id=900, **_common()),
            {"referee": {"id": 901, "name": "Wrong referee"}},
        ),
    ],
)
def test_schema_drift_replays_to_schema_error_with_raw_lineage(
    tmp_path,
    builder,
    payload,
):
    spec = builder()
    raw_store = _raw_store(tmp_path)
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    _seed_raw(raw_store, spec, body)
    engine, transport = _engine(tmp_path, raw_store=raw_store)

    result = replay_season_specs(engine, [spec])[0]

    assert transport.calls == 0
    assert result.manifest.status == ManifestStatus.SCHEMA_ERROR
    assert result.manifest.raw_content_hash == hashlib.sha256(body).hexdigest()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("builder", "payload"),
    [
        (
            lambda: build_schedule_page_spec(direction="next", page=0, **_common()),
            {"events": [], "hasNextPage": False},
        ),
        (lambda: build_rounds_spec(**_common()), {"rounds": []}),
        (
            lambda: build_referee_profile_spec(referee_id=900, **_common()),
            {"referee": None},
        ),
    ],
)
def test_structurally_valid_empty_payloads_are_terminal_and_raw_backed(
    tmp_path,
    builder,
    payload,
):
    spec = builder()
    raw_store = _raw_store(tmp_path)
    body = json.dumps(payload).encode("utf-8")
    _seed_raw(raw_store, spec, body)
    sink = RecordingSink()
    engine, transport = _engine(tmp_path, raw_store=raw_store, sink=sink)

    result = replay_season_specs(engine, [spec])[0]

    assert transport.calls == 0
    assert result.manifest.status == ManifestStatus.LEGITIMATE_EMPTY
    assert result.manifest.row_count == 0
    assert result.manifest.raw_content_hash == hashlib.sha256(body).hexdigest()
    assert sink.calls == []


@pytest.mark.unit
def test_network_free_planner_follows_every_stored_schedule_page(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    pages = [
        (
            build_schedule_page_spec(direction="last", page=0, **_common()),
            _schedule_payload([14000001, 14000002], has_next=True),
        ),
        (
            build_schedule_page_spec(direction="last", page=1, **_common()),
            _schedule_payload([14000003], has_next=False),
        ),
        (
            build_schedule_page_spec(direction="next", page=0, **_common()),
            _schedule_payload([], has_next=False),
        ),
    ]
    for spec, payload in pages:
        _seed_json(raw_store, spec, payload)

    plan = plan_season_partition(raw_store, manifest, **_common())

    schedule_targets = [
        spec.key.target_id
        for spec in plan.specs
        if spec.key.endpoint.startswith("schedule_")
    ]
    assert schedule_targets == ["last:0", "last:1", "next:0"]
    assert plan.schedule_event_ids == ("14000001", "14000002", "14000003")
    assert not {page[0].key for page in pages}.intersection(plan.missing_raw_keys)


@pytest.mark.unit
def test_seed_schedule_404_is_legitimate_empty_but_promised_page_is_not():
    seed = build_schedule_page_spec(direction="next", page=0, **_common())
    promised = build_schedule_page_spec(direction="next", page=1, **_common())

    assert 404 in seed.legitimate_empty_http_statuses
    assert 404 not in promised.legitimate_empty_http_statuses
    assert seed.not_supported_http_statuses == ()
    assert promised.not_supported_http_statuses == ()


@pytest.mark.unit
def test_seed_schedule_terminal_404_closes_direction_without_json(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    seed = build_schedule_page_spec(direction="next", page=0, **_common())
    raw = raw_store.store_bytes(
        seed.raw_target,
        b'{"error":"not found"}',
        request_url=seed.url,
        http_status=404,
        response_headers={"content-type": "application/json"},
    )
    manifest.upsert(
        EndpointManifest(
            key=seed.key,
            status=ManifestStatus.LEGITIMATE_EMPTY,
            run_id="season-empty-direction",
            task_id="season",
            attempts=1,
            row_count=0,
            http_status=404,
            raw_content_hash=raw.content_hash,
            raw_blob_key=raw.blob_key,
            request_url=seed.url,
        )
    )

    plan = plan_season_partition(raw_store, manifest, **_common())

    assert seed.key not in plan.pending_keys
    assert seed.key not in plan.missing_raw_keys
    assert all(
        not (
            spec.key.endpoint == "schedule_next"
            and spec.key.target_id != "next:0"
        )
        for spec in plan.specs
    )


@pytest.mark.unit
def test_missing_promised_schedule_page_stays_planned_and_nonterminal(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    last_zero = build_schedule_page_spec(direction="last", page=0, **_common())
    next_zero = build_schedule_page_spec(direction="next", page=0, **_common())
    _seed_json(
        raw_store,
        last_zero,
        _schedule_payload([14000001], has_next=True),
    )
    _seed_json(raw_store, next_zero, _schedule_payload([], has_next=False))

    plan = plan_season_partition(raw_store, manifest, **_common())
    promised = next(
        spec.key
        for spec in plan.specs
        if spec.key.endpoint == "schedule_last" and spec.key.target_id == "last:1"
    )

    assert promised in plan.pending_keys
    assert promised in plan.missing_raw_keys
    assert plan.complete is False
    assert all(spec.key.target_id != "last:2" for spec in plan.specs)

    with pytest.raises(SeasonPlanningError, match="exceeded max_pages=1"):
        plan_season_partition(
            raw_store,
            manifest,
            max_pages=1,
            **_common(),
        )


@pytest.mark.unit
def test_planner_expands_squads_and_referees_from_stored_evidence(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    _seed_json(
        raw_store,
        build_schedule_page_spec(direction="last", page=0, **_common()),
        _schedule_payload([14000001], has_next=False),
    )
    _seed_json(
        raw_store,
        build_schedule_page_spec(direction="next", page=0, **_common()),
        _schedule_payload([], has_next=False),
    )
    _seed_json(
        raw_store,
        build_participants_spec(**_common()),
        {
            "teams": [
                {"id": 44, "name": "Liverpool"},
                {"id": 42, "name": "Arsenal"},
                {"id": 42, "name": "Arsenal"},
            ]
        },
    )
    _seed_full_event_referee(
        raw_store,
        event_id=14000001,
        referee_id=900,
        freshness_key="event-final-v2",
    )

    plan = plan_season_partition(
        raw_store,
        manifest,
        event_freshness_key="event-final-v2",
        **_common(),
    )

    assert plan.team_ids == ("42", "44")
    assert plan.referee_ids == ("900",)
    assert [
        spec.key.target_id for spec in plan.specs if spec.key.endpoint == "squads"
    ] == ["42", "44"]
    assert [
        spec.key.target_id
        for spec in plan.specs
        if spec.key.endpoint == "referee_profile"
    ] == ["900"]


@pytest.mark.unit
def test_empty_participants_cannot_prove_player_universe(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    evidence = _payload(PLAYER_EVIDENCE_CASES)
    _seed_json(
        raw_store,
        build_schedule_page_spec(direction="last", page=0, **_common()),
        _schedule_payload([14000001], has_next=False),
    )
    _seed_json(
        raw_store,
        build_schedule_page_spec(direction="next", page=0, **_common()),
        _schedule_payload([], has_next=False),
    )
    participants = build_participants_spec(**_common())
    _seed_json(raw_store, participants, evidence["empty_participants"])

    plan = plan_season_partition(raw_store, manifest, **_common())

    assert plan.team_ids == ("42", "44")
    assert participants.key in plan.pending_keys
    assert {
        spec.key.target_id for spec in plan.specs if spec.key.endpoint == "squads"
    } == {
        "42",
        "44",
    }
    assert plan.player_universe_evidence_gaps == (
        "participants returned no teams; squad universe is unproven",
    )
    assert plan.complete is False
    with pytest.raises(SeasonPlanningError, match="evidence is incomplete"):
        squad_player_ids(raw_store, plan)


@pytest.mark.unit
def test_partial_participants_and_missing_squad_stay_incomplete(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    evidence = _payload(PLAYER_EVIDENCE_CASES)
    _seed_json(
        raw_store,
        build_schedule_page_spec(direction="last", page=0, **_common()),
        _schedule_payload([14000001], has_next=False),
    )
    _seed_json(
        raw_store,
        build_schedule_page_spec(direction="next", page=0, **_common()),
        _schedule_payload([], has_next=False),
    )
    participants = build_participants_spec(**_common())
    _seed_json(raw_store, participants, evidence["partial_participants"])
    _seed_json(
        raw_store,
        build_squad_spec(team_id=42, **_common()),
        evidence["nonempty_squad"],
    )

    plan = plan_season_partition(raw_store, manifest, **_common())
    missing_squad = build_squad_spec(team_id=44, **_common()).key

    assert plan.team_ids == ("42", "44")
    assert participants.key in plan.pending_keys
    assert missing_squad in plan.pending_keys
    assert missing_squad in plan.missing_raw_keys
    assert plan.player_universe_evidence_gaps == (
        "participants omitted scheduled team ids: 44",
    )
    assert plan.complete is False


@pytest.mark.unit
def test_disabled_bracket_placeholders_stay_out_of_team_universe(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    evidence = _payload(PLAYER_EVIDENCE_CASES)
    _seed_json(
        raw_store,
        build_schedule_page_spec(direction="last", page=0, **_common()),
        _payload(FIXTURE_PATHS["schedule_last"]),
    )
    _seed_json(
        raw_store,
        build_schedule_page_spec(direction="next", page=0, **_common()),
        _payload(CUP_BRACKET_NEXT_PAGE),
    )
    _seed_json(
        raw_store,
        build_participants_spec(**_common()),
        evidence["full_participants"],
    )

    plan = plan_season_partition(raw_store, manifest, **_common())

    assert plan.team_ids == ("17", "33", "42", "44")
    assert {
        spec.key.target_id for spec in plan.specs if spec.key.endpoint == "squads"
    } == {"17", "33", "42", "44"}
    assert plan.placeholder_team_ids == ("999901", "999902")
    assert plan.player_universe_evidence_gaps == ()
    assert "14000101" in plan.schedule_event_ids


@pytest.mark.unit
def test_unfinished_cup_bracket_plan_completes_without_placeholder_squads(
    tmp_path,
):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    evidence = _payload(PLAYER_EVIDENCE_CASES)
    _seed_complete_partition_roots(
        raw_store,
        schedule_next_payload=_payload(CUP_BRACKET_NEXT_PAGE),
    )
    initial = plan_season_partition(raw_store, manifest, **_common())
    for spec in initial.specs:
        if spec.key.endpoint == "squads":
            _seed_json(raw_store, spec, evidence["nonempty_squad"])
        elif spec.key.endpoint == "referee_profile":
            _seed_raw(raw_store, spec, FIXTURE_PATHS["referee_profile"].read_bytes())
    engine, transport = _engine(
        tmp_path,
        raw_store=raw_store,
        manifest_store=manifest,
    )
    replay_season_specs(engine, initial.specs)

    committed = plan_season_partition(raw_store, manifest, **_common())

    assert committed.complete is True
    assert committed.team_ids == ("17", "33", "42", "44")
    assert committed.placeholder_team_ids == ("999901", "999902")
    assert squad_player_ids(raw_store, committed) == ("1001", "1002")

    materialized = replay_season_partition(
        engine,
        committed,
        canonical_league="ENG-Premier League",
        canonical_season="2025/26",
    )
    assert transport.calls == 0
    # Two played rows from schedule_last plus the retained placeholder final.
    assert len(materialized.schedule_rows) == 3
    assert {row["game_id"] for row in materialized.schedule_rows} == {
        14000001,
        14000002,
        14000101,
    }


@pytest.mark.unit
def test_missing_real_participant_still_blocks_plan_with_placeholders_filtered(
    tmp_path,
):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    evidence = _payload(PLAYER_EVIDENCE_CASES)
    _seed_json(
        raw_store,
        build_schedule_page_spec(direction="last", page=0, **_common()),
        _schedule_payload([14000001], has_next=False),
    )
    _seed_json(
        raw_store,
        build_schedule_page_spec(direction="next", page=0, **_common()),
        _payload(CUP_BRACKET_NEXT_PAGE),
    )
    participants = build_participants_spec(**_common())
    _seed_json(raw_store, participants, evidence["partial_participants"])

    plan = plan_season_partition(raw_store, manifest, **_common())

    assert plan.player_universe_evidence_gaps == (
        "participants omitted scheduled team ids: 44",
    )
    assert participants.key in plan.pending_keys
    assert plan.complete is False
    assert plan.team_ids == ("42", "44")


@pytest.mark.unit
def test_mixed_event_keeps_real_disabled_false_team_and_drops_placeholder(
    tmp_path,
):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    mixed_event = {
        "id": 14000201,
        "season": {"id": SEASON_ID, "name": "Premier League 25/26", "year": "25/26"},
        "status": {"type": "notstarted"},
        "startTimestamp": 1782000000,
        "homeTeam": {"id": 42, "name": "Arsenal", "gender": "M", "disabled": False},
        "awayTeam": _placeholder_team(999901),
        "referee": {"id": 900, "name": "Referee 900"},
        "roundInfo": {"round": 30},
    }
    _seed_json(
        raw_store,
        build_schedule_page_spec(direction="last", page=0, **_common()),
        {"events": [mixed_event], "hasNextPage": False},
    )
    _seed_json(
        raw_store,
        build_schedule_page_spec(direction="next", page=0, **_common()),
        _schedule_payload([], has_next=False),
    )
    _seed_json(
        raw_store,
        build_participants_spec(**_common()),
        {"teams": [{"id": 42, "name": "Arsenal", "gender": "M"}]},
    )

    plan = plan_season_partition(raw_store, manifest, **_common())

    assert plan.team_ids == ("42",)
    assert plan.placeholder_team_ids == ("999901",)
    assert plan.player_universe_evidence_gaps == ()
    # The placeholder-side skip must stay per-side: the event's embedded
    # referee is still planned even when one bracket slot is unresolved.
    assert plan.referee_ids == ("900",)


@pytest.mark.unit
def test_empty_squad_is_not_terminal_player_universe_success(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    evidence = _payload(PLAYER_EVIDENCE_CASES)
    _seed_json(
        raw_store,
        build_schedule_page_spec(direction="last", page=0, **_common()),
        _schedule_payload([14000001], has_next=False),
    )
    _seed_json(
        raw_store,
        build_schedule_page_spec(direction="next", page=0, **_common()),
        _schedule_payload([], has_next=False),
    )
    _seed_json(
        raw_store,
        build_participants_spec(**_common()),
        {
            "teams": [
                {"id": 42, "name": "Arsenal"},
                {"id": 44, "name": "Liverpool"},
            ]
        },
    )
    _seed_json(
        raw_store,
        build_squad_spec(team_id=42, **_common()),
        evidence["nonempty_squad"],
    )
    empty_squad = build_squad_spec(team_id=44, **_common())
    _seed_json(raw_store, empty_squad, evidence["empty_squad"])

    plan = plan_season_partition(raw_store, manifest, **_common())

    assert empty_squad.key in plan.pending_keys
    assert empty_squad.key not in plan.missing_raw_keys
    assert plan.player_universe_evidence_gaps == (
        "scheduled/participating team 44 has an empty squad",
    )
    with pytest.raises(SeasonMaterializationError, match="empty squad"):
        materialize_season_partition(
            plan,
            [],
            canonical_league="ENG-Premier League",
            canonical_season="2025/26",
        )


@pytest.mark.unit
def test_source_unsupported_season_squads_do_not_fake_historical_rosters(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    _seed_complete_partition_roots(raw_store)
    preliminary = plan_season_partition(raw_store, manifest, **_common())
    squad_specs = [
        spec for spec in preliminary.specs if spec.key.endpoint == "squads"
    ]
    assert squad_specs

    for spec in squad_specs:
        raw = raw_store.store_bytes(
            spec.raw_target,
            b'{"error":"not found"}',
            request_url=spec.url,
            http_status=404,
            response_headers={"content-type": "application/json"},
        )
        manifest.upsert(
            EndpointManifest(
                key=spec.key,
                status=ManifestStatus.NOT_SUPPORTED,
                run_id="season-squad-unsupported",
                task_id="season",
                attempts=1,
                row_count=0,
                http_status=404,
                raw_content_hash=raw.content_hash,
                raw_blob_key=raw.blob_key,
                request_url=spec.url,
            )
        )

    plan = plan_season_partition(raw_store, manifest, **_common())

    assert plan.player_universe_evidence_gaps == ()
    assert squad_player_ids(raw_store, plan) == ()


@pytest.mark.unit
def test_full_team_and_squad_evidence_builds_nonempty_universe(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    evidence = _payload(PLAYER_EVIDENCE_CASES)
    _seed_complete_partition_roots(raw_store)
    initial = plan_season_partition(raw_store, manifest, **_common())
    for spec in initial.specs:
        if spec.key.endpoint == "squads":
            _seed_json(raw_store, spec, evidence["nonempty_squad"])
        elif spec.key.endpoint == "referee_profile":
            _seed_raw(raw_store, spec, FIXTURE_PATHS["referee_profile"].read_bytes())
    engine, _ = _engine(
        tmp_path,
        raw_store=raw_store,
        manifest_store=manifest,
    )
    replay_season_specs(engine, initial.specs)

    committed = plan_season_partition(raw_store, manifest, **_common())

    assert committed.complete is True
    assert committed.player_universe_evidence_gaps == ()
    assert committed.team_ids == ("17", "33", "42", "44")
    assert squad_player_ids(raw_store, committed) == ("1001", "1002")


@pytest.mark.unit
def test_raw_squads_expand_complete_profile_universe(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    plan = _complete_plan_with_expansion_raw(raw_store, manifest)

    assert squad_player_ids(raw_store, plan) == ("1001", "1002")


@pytest.mark.unit
def test_whole_partition_offline_replay_materializes_with_zero_network(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    plan = _complete_plan_with_expansion_raw(raw_store, manifest)
    engine, transport = _engine(
        tmp_path,
        raw_store=raw_store,
        manifest_store=manifest,
    )

    materialized = replay_season_partition(
        engine,
        plan,
        canonical_league="ENG-Premier League",
        canonical_season="2025/26",
    )

    assert transport.calls == 0
    assert len(materialized.schedule_rows) == 2
    assert len(materialized.standings_rows) == 4
    assert materialized.endpoint_completeness == 1.0
    assert all(
        row["league"] == "ENG-Premier League"
        and row["season"] == "2025/26"
        and row["raw_content_hash"]
        and row["raw_blob_key"]
        for row in (
            *materialized.schedule_rows,
            *materialized.standings_rows,
        )
    )
    assert {key.endpoint for key in materialized.raw_lineage} == {
        "schedule_last",
        "schedule_next",
        "standings_total",
        "rounds",
        "cup_trees",
        "participants",
        "squads",
        "referee_profile",
    }

    resumed = plan_season_partition(raw_store, manifest, **_common())
    assert resumed.complete is True
    assert resumed.pending_keys == ()
    assert resumed.missing_raw_keys == ()


@pytest.mark.unit
def test_optional_standings_not_supported_does_not_break_partition_replay(
    tmp_path,
):
    raw_store = _raw_store(tmp_path)
    common = _common()
    standings = build_standings_total_spec(**common)
    manifest = InMemoryManifestStore(
        [
            EndpointManifest(
                key=standings.key,
                status=ManifestStatus.NOT_SUPPORTED,
                run_id="cup-capture",
                task_id="season",
                attempts=1,
                row_count=0,
                http_status=404,
            )
        ]
    )
    seeded = [
        (
            build_schedule_page_spec(direction="last", page=0, **common),
            _payload(FIXTURE_PATHS["schedule_last"]),
        ),
        (
            build_schedule_page_spec(direction="next", page=0, **common),
            _payload(FIXTURE_PATHS["schedule_next"]),
        ),
        (build_rounds_spec(**common), {"rounds": []}),
        (build_cup_trees_spec(**common), {"cupTrees": []}),
        (
            build_participants_spec(**common),
            _payload(PLAYER_EVIDENCE_CASES)["full_participants"],
        ),
    ]
    for spec, payload in seeded:
        _seed_json(raw_store, spec, payload)

    plan = plan_season_partition(raw_store, manifest, **common)
    for spec in plan.specs:
        if spec.key.endpoint == "squads":
            _seed_json(
                raw_store,
                spec,
                _payload(PLAYER_EVIDENCE_CASES)["nonempty_squad"],
            )
    assert standings.key not in plan.pending_keys
    assert standings.key not in plan.missing_raw_keys
    engine, transport = _engine(
        tmp_path,
        raw_store=raw_store,
        manifest_store=manifest,
    )

    materialized = replay_season_partition(
        engine,
        plan,
        canonical_league="INT-Cup",
        canonical_season="2025",
    )

    assert transport.calls == 0
    assert len(materialized.schedule_rows) == 2
    assert materialized.standings_rows == ()
    assert materialized.endpoint_statuses[standings.key] == (
        ManifestStatus.NOT_SUPPORTED
    )
    assert materialized.endpoint_completeness == 1.0


@pytest.mark.unit
def test_deferred_normalized_results_materialize_before_atomic_merge_finalize(
    tmp_path,
):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    plan = _complete_plan_with_expansion_raw(raw_store, manifest)
    sink = DeferredCaptureSink()
    engine, transport = _engine(
        tmp_path,
        raw_store=raw_store,
        manifest_store=manifest,
        sink=sink,
    )

    results = replay_season_specs(engine, plan.specs)
    by_endpoint = {result.manifest.key.endpoint: result for result in results}
    assert transport.calls == 0
    assert by_endpoint["schedule_last"].manifest.status == (
        ManifestStatus.RETRYABLE_FAILURE
    )
    assert by_endpoint["schedule_last"].manifest.error_type == (
        "DeferredMaterialization"
    )
    assert by_endpoint["standings_total"].manifest.status == (
        ManifestStatus.RETRYABLE_FAILURE
    )
    assert by_endpoint["schedule_next"].manifest.status == (
        ManifestStatus.LEGITIMATE_EMPTY
    )
    assert all(
        by_endpoint[endpoint].manifest.is_terminal
        for endpoint in (
            "rounds",
            "cup_trees",
            "participants",
            "squads",
            "referee_profile",
        )
    )

    materialized = materialize_season_partition(
        plan,
        results,
        canonical_league="ENG-Premier League",
        canonical_season="2025/26",
    )
    assert len(materialized.schedule_rows) == 2
    assert len(materialized.standings_rows) == 4
    assert {key.endpoint for key in materialized.deferred_keys} == {
        "schedule_last",
        "standings_total",
    }
    assert materialized.endpoint_completeness == 1.0

    poisoned = list(results)
    index = next(
        i
        for i, result in enumerate(poisoned)
        if result.manifest.key.endpoint == "schedule_last"
    )
    poisoned[index] = replace(
        poisoned[index],
        manifest=replace(poisoned[index].manifest, error_type="RuntimeError"),
    )
    with pytest.raises(SeasonMaterializationError, match="nonterminal"):
        materialize_season_partition(
            plan,
            poisoned,
            canonical_league="ENG-Premier League",
            canonical_season="2025/26",
        )

    runtime = CaptureRuntime(engine, manifest, raw_store)
    finalize_materialized_results(runtime, results)
    assert all(manifest.get(key).is_terminal for key in materialized.deferred_keys)


@pytest.mark.unit
def test_runner_offline_season_replay_merges_then_noops_without_browser(
    tmp_path,
    monkeypatch,
):
    from dags.scripts import run_sofascore_scraper as runner

    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    _complete_plan_with_expansion_raw(raw_store, manifest)
    engine, transport = _engine(
        tmp_path,
        raw_store=raw_store,
        manifest_store=manifest,
        sink=DeferredCaptureSink(),
    )
    runtime = CaptureRuntime(engine, manifest, raw_store)
    monkeypatch.setenv("SOFASCORE_SEASON_FRESHNESS_KEY", FRESHNESS)
    monkeypatch.setattr(
        runner,
        "_source_context",
        lambda *args: (TOURNAMENT_ID, SEASON_ID),
    )
    scraper = MagicMock()
    scraper.__enter__.return_value = scraper
    scraper.__exit__.return_value = False
    scraper._add_metadata.side_effect = lambda frame, entity: frame.assign(
        _entity_type=entity,
        _ingested_at="fixture",
    )
    scraper.save_to_iceberg.side_effect = lambda **kwargs: (
        "iceberg.bronze." + kwargs["table_name"]
    )
    output = tmp_path / "season-offline.json"

    with patch("scrapers.sofascore.SofaScoreScraper", return_value=scraper):
        rc = runner._run_legacy(
            leagues=["ENG-Premier League"],
            season=2025,
            output_path=str(output),
            capture_runtime=runtime,
            workload_plan=None,
            offline_replay=True,
        )

    assert rc == 0
    assert transport.calls == 0
    assert [
        call.kwargs["table_name"] for call in scraper.save_to_iceberg.call_args_list
    ] == ["sofascore_schedule", "sofascore_league_table"]
    committed_plan = plan_season_partition(
        raw_store,
        manifest,
        source_tournament_id=TOURNAMENT_ID,
        source_season_id=SEASON_ID,
        freshness_key=FRESHNESS,
        event_freshness_key="final",
        paid_proxy=True,
    )
    assert all(manifest.get(spec.key).is_terminal for spec in committed_plan.specs)
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schedule_rows"] == 2
    assert payload["league_table_rows"] == 4
    assert payload["traffic"]["request_count"] == 0
    assert payload["traffic"]["replay_hit_rate"] == 1.0

    replay_engine, replay_transport = _engine(
        tmp_path,
        raw_store=raw_store,
        manifest_store=manifest,
        sink=DeferredCaptureSink(),
    )
    no_op_runtime = CaptureRuntime(replay_engine, manifest, raw_store)
    no_op_output = tmp_path / "season-noop.json"
    browser = MagicMock(side_effect=AssertionError("no-op opened a scraper"))
    with patch("scrapers.sofascore.SofaScoreScraper", browser):
        rc = runner._run_legacy(
            leagues=["ENG-Premier League"],
            season=2025,
            output_path=str(no_op_output),
            capture_runtime=no_op_runtime,
            workload_plan=None,
            offline_replay=False,
        )

    assert rc == 0
    assert replay_transport.calls == 0
    browser.assert_not_called()
    no_op = json.loads(no_op_output.read_text(encoding="utf-8"))
    assert no_op["traffic"] == {
        "paid_proxy_bytes": 0,
        "paid_proxy_mb": 0.0,
        "browser_sessions": 0,
        "browser_navigations": 0,
        "request_count": 0,
        "cache_hit_rate": 1.0,
        "endpoint_completeness": 1.0,
    }


@pytest.mark.unit
def test_runner_live_season_uses_proven_slug_url_and_committed_completeness(
    tmp_path,
    monkeypatch,
):
    from dags.scripts import run_sofascore_scraper as runner

    key = SimpleNamespace(stable_id=lambda: "season-key")
    spec = SimpleNamespace(key=key)

    def plan(*, complete, missing, pending):
        return SimpleNamespace(
            complete=complete,
            specs=(spec,),
            missing_raw_keys=(key,) if missing else (),
            pending_keys=(key,) if pending else (),
            schedule_event_ids=(),
            team_ids=(),
            referee_ids=(),
        )

    initial = plan(complete=False, missing=True, pending=True)
    expanded = plan(complete=False, missing=False, pending=True)
    committed = plan(complete=True, missing=False, pending=False)
    engine = SimpleNamespace(
        budget=object(),
        run_id="run-1",
        metrics=SimpleNamespace(
            snapshot=lambda: {
                "paid_proxy_bytes": 321,
                "paid_proxy_mb": 321 / 1_048_576,
                "browser_sessions": 1,
                "navigations": 1,
                "request_count": 1,
                "endpoint_completeness": 0.0,
            }
        ),
    )
    runtime = CaptureRuntime(engine, object(), object())
    output = tmp_path / "season-live.json"
    captured = {}

    def fake_live(runtime_arg, planner, **kwargs):
        captured.update(kwargs)
        return (
            [],
            expanded,
            {
                "provider_total_bytes": 321,
                "provider_budget_bytes": 1000,
                "provider_budget_exceeded": False,
                "browser_navigations": 1,
                "proxy_exit_hash": None,
            },
        )

    materialized = SimpleNamespace(
        endpoint_completeness=1.0,
        schedule_rows=(),
        standings_rows=(),
    )
    scraper = MagicMock()
    scraper.__enter__.return_value = scraper
    scraper.__exit__.return_value = False
    monkeypatch.setenv("SOFASCORE_SEASON_FRESHNESS_KEY", FRESHNESS)
    monkeypatch.setattr(
        runner, "_source_context", lambda *args: (TOURNAMENT_ID, SEASON_ID)
    )

    with (
        patch(
            "scrapers.sofascore.season_pipeline.plan_season_partition",
            side_effect=[initial, committed],
        ),
        patch(
            "scrapers.sofascore.season_pipeline.replay_season_specs",
            return_value=[],
        ),
        patch(
            "scrapers.sofascore.season_pipeline.materialize_season_partition",
            return_value=materialized,
        ),
        patch("scrapers.sofascore.pipeline.finalize_materialized_results"),
        patch(
            "scrapers.sofascore.live_capture.capture_live_dynamic_specs",
            side_effect=fake_live,
        ),
        patch(
            "dags.scripts.run_sofascore_scraper._tournament_canonical_url",
            return_value=(
                "https://www.sofascore.com/tournament/premier-league/17"
            ),
        ),
        patch(
            "scrapers.sofascore.SofaScoreScraper",
            return_value=scraper,
            create=True,
        ),
    ):
        rc = runner._run_legacy(
            leagues=["ENG-Premier League"],
            season=2025,
            output_path=str(output),
            capture_runtime=runtime,
            workload_plan=None,
            offline_replay=False,
        )

    assert rc == 0
    assert captured["canonical_url"] == (
        "https://www.sofascore.com/tournament/premier-league/17"
    )
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["endpoint_completeness"] == 1.0
    assert payload["traffic"]["endpoint_completeness"] == 1.0


@pytest.mark.unit
def test_runner_live_player_reports_committed_not_deferred_completeness(
    tmp_path,
    monkeypatch,
):
    import pandas as pd

    from dags.scripts import run_sofascore_scraper as runner
    from scrapers.sofascore.pipeline import PLAYER_PATHS

    engine = SimpleNamespace(
        budget=object(),
        run_id="run-1",
        metrics=SimpleNamespace(
            snapshot=lambda: {
                "paid_proxy_bytes": 200,
                "paid_proxy_mb": 200 / 1_048_576,
                "browser_sessions": 1,
                "navigations": 1,
                "request_count": 2,
                # Both raw replays were DeferredMaterialization before the
                # mocked final manifest commit.
                "endpoint_completeness": 0.0,
            }
        ),
    )
    runtime = CaptureRuntime(engine, object(), object())
    pending = {"10": set(PLAYER_PATHS)}
    season_plan = SimpleNamespace(complete=True)
    scraper = MagicMock()
    scraper.__enter__.return_value = scraper
    scraper.__exit__.return_value = False
    scraper._resolve_player_ids_from_bronze.return_value = []
    scraper._add_metadata.side_effect = lambda frame, _entity: frame
    scraper.save_to_iceberg.side_effect = lambda **kwargs: (
        "iceberg.bronze." + kwargs["table_name"]
    )
    output = tmp_path / "player-live.json"

    def materialize(_scraper, _results, *, league, season):
        return {
            "player_profile": pd.DataFrame(
                [
                    {
                        "player_id": "10",
                        "source_season_id": str(SEASON_ID),
                        "league": league,
                        "season": season,
                    }
                ]
            ),
            "player_season_stats": pd.DataFrame(),
        }

    monkeypatch.setattr(
        runner, "_source_context", lambda *args: (TOURNAMENT_ID, SEASON_ID)
    )
    with (
        patch(
            "scrapers.sofascore.season_pipeline.plan_season_partition",
            return_value=season_plan,
        ),
        patch(
            "scrapers.sofascore.season_pipeline.squad_player_ids",
            return_value=("10",),
        ),
        patch(
            "scrapers.sofascore.pipeline.endpoint_resume_plan",
            side_effect=[pending, {}],
        ),
        patch(
            "scrapers.sofascore.live_capture.capture_live_specs",
            return_value=(
                [],
                {
                    "provider_total_bytes": 200,
                    "provider_budget_bytes": 1000,
                    "provider_budget_exceeded": False,
                    "proxy_exit_hash": None,
                },
            ),
        ),
        patch(
            "scrapers.sofascore.pipeline.replay_player_specs",
            return_value=[SimpleNamespace()],
        ),
            patch(
                "scrapers.sofascore.pipeline.materialize_player_datasets",
                side_effect=materialize,
            ),
            patch("scrapers.sofascore.pipeline.finalize_materialized_results"),
            patch("scrapers.sofascore.pipeline.promote_repaired_results"),
            patch("scrapers.sofascore.SofaScoreScraper", return_value=scraper),
        ):
        rc = runner._run_player_capture(
            leagues=["ENG-Premier League"],
            season=2025,
            limit=None,
            output_path=str(output),
            capture_runtime=runtime,
            workload_plan=None,
            offline_replay=False,
        )

    assert rc == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["endpoint_completeness"] == 1.0
    assert payload["traffic"]["endpoint_completeness"] == 1.0


@pytest.mark.unit
def test_runner_refuses_new_local_player_outside_signed_post_match_plan(
    tmp_path,
    monkeypatch,
):
    from dags.scripts import run_sofascore_scraper as runner
    from scrapers.sofascore.workload_plan import qualify_work_unit
    from scrapers.sofascore.workload_runtime import partition_key

    engine = SimpleNamespace(
        run_id="scheduled-1::players",
        metrics=SimpleNamespace(snapshot=lambda: {"paid_proxy_bytes": 0}),
    )
    runtime = SimpleNamespace(
        engine=engine,
        raw_store=MagicMock(),
        manifest_store=MagicMock(),
    )
    scraper = MagicMock()
    scraper.__enter__.return_value = scraper
    scraper.__exit__.return_value = False
    scraper._resolve_player_ids_from_bronze.return_value = []
    signed = SimpleNamespace(
        player_universe_ids=(
            qualify_work_unit(
                partition_key("ENG-Premier League", "2526"),
                "10",
            ),
        ),
        freshness_key=lambda scope: {
            "season": FRESHNESS,
            "match": "final",
            "player": "fixture-week",
        }[scope],
    )
    output = tmp_path / "player-universe-race.json"
    monkeypatch.setattr(
        runner,
        "_source_context",
        lambda *args: (TOURNAMENT_ID, SEASON_ID),
    )
    monkeypatch.setattr(
        runner,
        "_tournament_canonical_url",
        lambda *args: "https://www.sofascore.com/tournament/premier-league/17",
    )

    with (
        patch(
            "scrapers.sofascore.season_pipeline.plan_season_partition",
            return_value=SimpleNamespace(complete=True),
        ),
        patch(
            "scrapers.sofascore.season_pipeline.squad_player_ids",
            return_value=("10", "11"),
        ),
        patch("scrapers.sofascore.SofaScoreScraper", return_value=scraper),
    ):
        rc = runner._run_player_capture(
            leagues=["ENG-Premier League"],
            season=2025,
            limit=None,
            output_path=str(output),
            capture_runtime=runtime,
            workload_plan=signed,
            workload_allocations=(),
            offline_replay=False,
        )

    assert rc == 1
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert "outside the signed player plan" in payload["errors"][0]
    scraper.save_to_iceberg.assert_not_called()


def _replayed_complete_partition(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    plan = _complete_plan_with_expansion_raw(raw_store, manifest)
    engine, transport = _engine(
        tmp_path,
        raw_store=raw_store,
        manifest_store=manifest,
    )
    results = replay_season_specs(engine, plan.specs)
    assert transport.calls == 0
    return plan, results


@pytest.mark.unit
def test_partition_materializer_rejects_duplicate_schedule_natural_key(tmp_path):
    plan, results = _replayed_complete_partition(tmp_path)
    index = next(
        i
        for i, result in enumerate(results)
        if result.manifest.key.endpoint == "schedule_last"
    )
    result = results[index]
    rows = list(result.datasets["schedule"].rows)
    rows.append(dict(rows[0]))
    results[index] = replace(
        result,
        manifest=replace(result.manifest, row_count=len(rows)),
        datasets={"schedule": ParsedDataset("schedule", rows)},
    )

    with pytest.raises(SeasonMaterializationError, match="duplicate schedule"):
        materialize_season_partition(
            plan,
            results,
            canonical_league="ENG-Premier League",
            canonical_season="2025/26",
        )


def _replayed_partition_with_cross_page_repeat(tmp_path, *, mutate=False):
    """Partition whose live-feed pages repeat event 14000001 on BOTH pages.

    A live paginated feed (e.g. the World Cup knockout stage) can shift a
    settled match between page windows, so the same event legally appears on
    two different pages with an identical payload (#951). ``mutate`` makes the
    repeat's payload disagree — a data conflict that must stay a hard error.
    """
    raw_store = _raw_store(tmp_path)
    manifest = InMemoryManifestStore()
    event = _schedule_event(14000001)
    if mutate:
        event["startTimestamp"] = int(event.get("startTimestamp") or 0) + 3600
    _seed_complete_partition_roots(
        raw_store,
        schedule_next_payload={"events": [event], "hasNextPage": False},
    )
    plan = plan_season_partition(raw_store, manifest, **_common())
    for spec in plan.specs:
        if spec.key.endpoint == "squads":
            _seed_raw(raw_store, spec, FIXTURE_PATHS["squads"].read_bytes())
        elif spec.key.endpoint == "referee_profile":
            _seed_raw(
                raw_store,
                spec,
                FIXTURE_PATHS["referee_profile"].read_bytes(),
            )
    engine, transport = _engine(
        tmp_path,
        raw_store=raw_store,
        manifest_store=manifest,
    )
    results = replay_season_specs(engine, plan.specs)
    assert transport.calls == 0
    return plan, results


@pytest.mark.unit
def test_partition_materializer_collapses_cross_page_schedule_repeat(tmp_path):
    """#951: an identical-payload repeat from ANOTHER page collapses to one
    row (page provenance and raw lineage are the only differences); the
    first-seen page wins."""
    plan, results = _replayed_partition_with_cross_page_repeat(tmp_path)
    materialization = materialize_season_partition(
        plan,
        results,
        canonical_league="ENG-Premier League",
        canonical_season="2025/26",
    )
    repeats = [
        row
        for row in materialization.schedule_rows
        if str(row["game_id"]) == "14000001"
    ]
    assert len(repeats) == 1
    assert repeats[0]["source_page_direction"] == "last"


@pytest.mark.unit
def test_partition_materializer_rejects_cross_page_payload_conflict(tmp_path):
    """A cross-page repeat whose match payload disagrees is a data conflict,
    not pagination noise — it must remain a hard error."""
    plan, results = _replayed_partition_with_cross_page_repeat(
        tmp_path,
        mutate=True,
    )
    with pytest.raises(SeasonMaterializationError, match="duplicate schedule"):
        materialize_season_partition(
            plan,
            results,
            canonical_league="ENG-Premier League",
            canonical_season="2025/26",
        )


@pytest.mark.unit
@pytest.mark.parametrize("field", ["source_season_id", "season_id"])
def test_partition_materializer_rejects_normalized_season_mismatch(
    tmp_path,
    field,
):
    plan, results = _replayed_complete_partition(tmp_path)
    index = next(
        i
        for i, result in enumerate(results)
        if result.manifest.key.endpoint == "schedule_last"
    )
    result = results[index]
    rows = [dict(row) for row in result.datasets["schedule"].rows]
    rows[0][field] = 99999
    results[index] = replace(
        result,
        datasets={"schedule": ParsedDataset("schedule", rows)},
    )

    with pytest.raises(SeasonMaterializationError, match="season.*mismatch"):
        materialize_season_partition(
            plan,
            results,
            canonical_league="ENG-Premier League",
            canonical_season="2025/26",
        )
