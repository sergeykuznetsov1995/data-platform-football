"""Focused unit tests for the SofaScore raw-replay worker helpers (#1367)."""

import pytest

from dags.scripts.replay_sofascore_raw import (
    ReplayScopeError,
    _load_scope,
    _normalize_scope,
    _season_plan,
    _season_spec_for,
    _target_ids,
)
from scrapers.sofascore.manifest import (
    EndpointManifest,
    ManifestKey,
    ManifestStatus,
)
from scrapers.sofascore.raw_replay import MODE_FIELDS, ReplayScopeKey

TOURNAMENT = "17"
SEASON = "61627"
FRESHNESS = "2026-11-17"


def _record(
    endpoint,
    target_type,
    target_id,
    *,
    tournament=TOURNAMENT,
    season=SEASON,
    freshness=FRESHNESS,
    **overrides,
):
    values = {
        "key": ManifestKey(
            source_tournament_id=str(tournament),
            source_season_id=str(season),
            target_type=target_type,
            target_id=str(target_id),
            endpoint=endpoint,
            freshness_key=freshness,
        ),
        "status": ManifestStatus.SUCCESS,
        "run_id": "run",
        "task_id": "task",
        "attempts": 1,
        "row_count": 3,
        "http_status": 200,
        "raw_content_hash": "hash",
        "raw_blob_key": "blob",
    }
    values.update(overrides)
    return EndpointManifest(**values)


class _FakeSeason:
    def __init__(self, season_id, canonical_season):
        self.season_id = season_id
        self.canonical_season = canonical_season


class _FakeTournament:
    def __init__(self, canonical_id, seasons):
        self.canonical_id = canonical_id
        self.seasons = seasons


class _FakeCatalog:
    def __init__(self):
        self._tournament = _FakeTournament(
            "england/premier-league", [_FakeSeason(int(SEASON), "25/26")]
        )

    def tournament(self, unique_tournament_id):
        if int(unique_tournament_id) != int(TOURNAMENT):
            raise ValueError(
                f"unknown unique_tournament_id {unique_tournament_id}"
            )
        return self._tournament

    def tournament_map(self):
        return {"england/premier-league": int(TOURNAMENT)}


class _FakeManager:
    def __init__(self):
        self.calls = []

    def _execute(self, sql, fetch=False, params=None):
        self.calls.append((sql, fetch, params))
        return []


class _FakeManifestStore:
    def __init__(self, manager):
        self.manager = manager


class _FakeRuntime:
    def __init__(self, catalog, manager):
        self.catalog = catalog
        self.manifest_store = _FakeManifestStore(manager)


def test_normalize_scope_accepts_key_mapping_and_triple():
    expected = ReplayScopeKey(TOURNAMENT, SEASON, FRESHNESS)
    assert _normalize_scope(expected) == expected
    assert _normalize_scope(
        {
            "source_tournament_id": int(TOURNAMENT),
            "source_season_id": SEASON,
            "freshness_key": f" {FRESHNESS} ",
        }
    ) == expected
    assert _normalize_scope((int(TOURNAMENT), int(SEASON), FRESHNESS)) == (
        expected
    )


@pytest.mark.parametrize(
    "value",
    [
        (True, SEASON, FRESHNESS),
        (TOURNAMENT, True, FRESHNESS),
        (TOURNAMENT, SEASON, True),
        (TOURNAMENT, SEASON, ""),
        (TOURNAMENT, SEASON, "  "),
        (TOURNAMENT, SEASON),
        (TOURNAMENT, SEASON, FRESHNESS, "extra"),
        (None, SEASON, FRESHNESS),
        (TOURNAMENT, 1.5, FRESHNESS),
        TOURNAMENT,
        17,
        b"17:61627:k",
    ],
)
def test_normalize_scope_rejects_invalid_values(value):
    with pytest.raises(ReplayScopeError):
        _normalize_scope(value)


def test_normalize_scope_rejects_extra_and_missing_mapping_fields():
    complete = {
        "source_tournament_id": TOURNAMENT,
        "source_season_id": SEASON,
        "freshness_key": FRESHNESS,
    }
    extra = dict(complete, unexpected="x")
    missing = {
        "source_tournament_id": TOURNAMENT,
        "source_season_id": SEASON,
    }
    for invalid in (extra, missing):
        with pytest.raises(ReplayScopeError):
            _normalize_scope(invalid)


def test_target_ids_dedups_and_sorts_numerically_for_exact_endpoint():
    records = (
        _record("squads", "season_team", "10"),
        _record("squads", "season_team", "2"),
        _record("squads", "season_team", "2", run_id="again"),
        _record("referee_profile", "referee", "9"),
        _record("standings_total", "season", SEASON),
    )
    assert _target_ids(records, "squads") == ("2", "10")
    assert _target_ids(records, "referee_profile") == ("9",)
    assert _target_ids(records, "lineups") == ()


def test_season_spec_for_schedule_last_page_zero():
    record = _record("schedule_last", "season_page", "last:0")
    spec = _season_spec_for(record)
    assert spec.key == record.key
    assert spec.key.endpoint == "schedule_last"
    assert spec.key.target_id == "last:0"
    assert spec.paid_proxy is True


def test_season_spec_for_squads_keeps_key_and_is_raw_only():
    record = _record("squads", "season_team", "42")
    spec = _season_spec_for(record)
    assert spec.key == record.key
    assert spec.key.target_type == "season_team"
    assert spec.key.target_id == "42"
    assert spec.raw_only is True


def test_season_spec_for_referee_profile_uses_target_id():
    record = _record("referee_profile", "referee", "774129")
    spec = _season_spec_for(record)
    assert spec.key == record.key


def test_season_spec_for_rejects_unknown_endpoint():
    record = _record("event", "event", "123")
    with pytest.raises(ReplayScopeError):
        _season_spec_for(record)


def _plan_records():
    return (
        _record("standings_total", "season", SEASON),
        _record("squads", "season_team", "42"),
        _record("squads", "season_team", "10"),
        _record("referee_profile", "referee", "7"),
        _record("referee_profile", "referee", "3"),
    )


def test_season_plan_collects_source_team_and_referee_ids():
    records = _plan_records()
    plan = _season_plan(records, (TOURNAMENT, SEASON, FRESHNESS))
    assert plan.source_tournament_id == TOURNAMENT
    assert plan.source_season_id == SEASON
    assert plan.freshness_key == FRESHNESS
    assert plan.event_freshness_key == "final"
    assert plan.pending_keys == tuple(record.key for record in records)
    assert tuple(spec.key for spec in plan.specs) == plan.pending_keys
    assert plan.missing_raw_keys == ()
    assert plan.schedule_event_ids == ()
    assert plan.team_ids == ("10", "42")
    assert plan.referee_ids == ("3", "7")


def test_season_plan_rejects_duplicate_spec_keys():
    records = (
        _record("rounds", "season", SEASON),
        _record("rounds", "season", SEASON, run_id="later"),
    )
    with pytest.raises(ReplayScopeError):
        _season_plan(records, (TOURNAMENT, SEASON, FRESHNESS))


def test_season_plan_rejects_records_outside_scope():
    records = (
        _record("rounds", "season", SEASON, freshness="2026-11-16"),
    )
    with pytest.raises(ReplayScopeError):
        _season_plan(records, (TOURNAMENT, SEASON, FRESHNESS))


def test_load_scope_prefilters_exact_triple_without_sql():
    manager = _FakeManager()
    runtime = _FakeRuntime(_FakeCatalog(), manager)
    exact = (
        _record("rounds", "season", SEASON),
        _record("standings_total", "season", SEASON),
    )
    foreign_freshness = _record(
        "participants", "season", SEASON, freshness="2026-11-16"
    )
    foreign_tournament = _record(
        "rounds", "season", SEASON, tournament="18"
    )
    resolved, plan = _load_scope(
        (TOURNAMENT, SEASON, FRESHNESS),
        mode=MODE_FIELDS,
        capture_runtime=runtime,
        records=(*exact, foreign_freshness, foreign_tournament),
    )
    assert manager.calls == []
    assert (
        resolved.source_tournament_id,
        resolved.source_season_id,
        resolved.freshness_key,
    ) == (TOURNAMENT, SEASON, FRESHNESS)
    assert resolved.canonical_season == "25/26"
    assert isinstance(resolved.league, str) and resolved.league
    assert plan is not None
    assert plan.scope == ReplayScopeKey(TOURNAMENT, SEASON, FRESHNESS)
    assert plan.mode == MODE_FIELDS
    assert set(plan.records) == set(exact)
    assert len(plan.records) == len(exact)


def test_load_scope_returns_none_plan_without_replayable_records():
    runtime = _FakeRuntime(_FakeCatalog(), _FakeManager())
    skipped = _record(
        "rounds",
        "season",
        SEASON,
        status=ManifestStatus.RETRYABLE_FAILURE,
        http_status=503,
        row_count=0,
        raw_content_hash=None,
        raw_blob_key=None,
    )
    resolved, plan = _load_scope(
        (TOURNAMENT, SEASON, FRESHNESS),
        mode=MODE_FIELDS,
        capture_runtime=runtime,
        records=(skipped,),
    )
    assert plan is None
    assert resolved.freshness_key == FRESHNESS


def test_load_scope_empty_records_yield_none_plan():
    runtime = _FakeRuntime(_FakeCatalog(), _FakeManager())
    resolved, plan = _load_scope(
        (TOURNAMENT, SEASON, FRESHNESS),
        mode=MODE_FIELDS,
        capture_runtime=runtime,
        records=(),
    )
    assert plan is None
    assert resolved.source_season_id == SEASON


def test_load_scope_rejects_unknown_mode():
    runtime = _FakeRuntime(_FakeCatalog(), _FakeManager())
    with pytest.raises(ReplayScopeError):
        _load_scope(
            (TOURNAMENT, SEASON, FRESHNESS),
            mode="bogus",
            capture_runtime=runtime,
            records=(),
        )


def test_load_scope_without_records_requires_trino_manager():
    runtime = _FakeRuntime(_FakeCatalog(), None)
    with pytest.raises(ReplayScopeError, match="trino"):
        _load_scope(
            (TOURNAMENT, SEASON, FRESHNESS),
            mode=MODE_FIELDS,
            capture_runtime=runtime,
        )
