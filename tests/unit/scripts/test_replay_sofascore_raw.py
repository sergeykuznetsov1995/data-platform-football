"""Focused unit tests for the SofaScore raw-replay worker helpers (#1367)."""

import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

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
from scrapers.sofascore.raw_replay import (
    MODE_FIELDS,
    ReplayPlanningError,
    ReplayScopeKey,
)

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
    def __init__(self, season_id, canonical_season, season_format="split_year"):
        self.season_id = season_id
        self.canonical_season = canonical_season
        self.season_format = season_format


class _FakeTournament:
    def __init__(self, canonical_id, seasons):
        self.canonical_id = canonical_id
        self.seasons = seasons


class _FakeCatalog:
    def __init__(self):
        self._tournament = _FakeTournament(
            "england/premier-league", [_FakeSeason(int(SEASON), "2526")]
        )

    def tournament(self, unique_tournament_id):
        if int(unique_tournament_id) != int(TOURNAMENT):
            raise ValueError(
                f"unknown unique_tournament_id {unique_tournament_id}"
            )
        return self._tournament

    def tournament_map(self):
        return {"england/premier-league": int(TOURNAMENT)}

    tournaments = ()


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
    resolved, plan, season_format, overlay_source = _load_scope(
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
    assert resolved.canonical_season == "2526"
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
    resolved, plan, season_format, overlay_source = _load_scope(
        (TOURNAMENT, SEASON, FRESHNESS),
        mode=MODE_FIELDS,
        capture_runtime=runtime,
        records=(skipped,),
    )
    assert plan is None
    assert resolved.freshness_key == FRESHNESS


def test_load_scope_empty_records_yield_none_plan():
    runtime = _FakeRuntime(_FakeCatalog(), _FakeManager())
    resolved, plan, season_format, overlay_source = _load_scope(
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


# End-to-end replay contracts use the existing synthetic payload helpers. The
# replay engine is fresh after fixture ingestion so its traffic measures only
# the replay, never the synthetic seed.  The catalog is a REAL registry-grade
# SofaScoreCatalog: the #1367 per-scope overlay renders from the resolved
# tournament row, so a SimpleNamespace stand-in can no longer carry it.
def _catalog_76986():
    from scrapers.sofascore.catalog import SofaScoreCatalog

    return SofaScoreCatalog.from_mapping(
        {
            "schema_version": 2,
            "generated_at": "2026-11-22T00:00:00Z",
            "tournaments": [
                {
                    "unique_tournament_id": 17,
                    "name": "Premier League",
                    "slug": "premier-league",
                    "category": {
                        "id": 1,
                        "name": "England",
                        "slug": "england",
                    },
                    "sport_slug": "football",
                    "page_path": "football/england/premier-league",
                    "canonical_id": "england/premier-league",
                    "enabled": True,
                    "classification": {
                        "sport": "football",
                        "gender": "male",
                        "age_group": "adult",
                        "team_level": "first_team",
                        "status": "source_confirmed_adult_men",
                        "exclusion_reasons": [],
                        "evidence": [
                            {
                                "type": "source_field",
                                "endpoint": "/unique-tournament/17",
                                "field": "gender",
                                "value": "M",
                            }
                        ],
                    },
                    "review": {
                        "status": "approved",
                        "confirmed": {
                            "sport": "football",
                            "gender": "male",
                            "age_group": "adult",
                            "team_level": "first_team",
                        },
                        "evidence": [
                            {
                                "type": "owner_review",
                                "reference": "#1367 fixture",
                                "note": "offline replay fixture registry",
                            }
                        ],
                        "reviewed_by": "fixture",
                        "reviewed_at": "2026-11-22T00:00:00Z",
                    },
                    "seasons": [
                        {
                            "season_id": 76986,
                            "name": "Premier League 25/26",
                            "source_name": "Premier League 25/26",
                            "year": "25/26",
                            "format": "split_year",
                            "season_format": "split_year",
                            "canonical_season": "2526",
                            "start_date": None,
                            "end_date": None,
                            "aliases": ["2526", "25/26"],
                            "evidence": [
                                {
                                    "type": "source_field",
                                    "endpoint": "/unique-tournament/17/seasons",
                                    "value": "76986",
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    )


def _fresh_replay_runtime(seed, tmp_path):
    from scrapers.sofascore.pipeline import CaptureRuntime, DeferredCaptureSink
    from tests.unit.scrapers.test_sofascore_season_pipeline import _engine

    engine, transport = _engine(
        tmp_path,
        raw_store=seed.raw_store,
        manifest_store=seed.manifest_store,
        sink=DeferredCaptureSink(),
    )
    return CaptureRuntime(engine, seed.manifest_store, seed.raw_store), transport


def _patch_replay_writers(monkeypatch):
    from contextlib import nullcontext

    from dags.scripts import run_sofascore_scraper as runner
    from tests.unit.scrapers.test_sofascore_pipeline import (
        MetadataScraper,
        _runner_player_scraper,
    )

    scraper = _runner_player_scraper()
    scraper._resolve_player_ids_from_bronze.return_value = []
    scraper._add_metadata.side_effect = (
        lambda frame, entity: MetadataScraper._add_metadata(
            frame, entity
        ).assign(_ingested_at="fixture")
    )
    saved = {}

    def save(**kwargs):
        saved.setdefault(kwargs["table_name"], []).append(kwargs)
        return "iceberg.bronze." + kwargs["table_name"]

    scraper.save_to_iceberg.side_effect = save
    monkeypatch.setattr(
        "scrapers.sofascore.catalog.SofaScoreCatalog.load",
        lambda *args, **kwargs: _catalog_76986(),
    )
    monkeypatch.setattr(
        "scrapers.sofascore.SofaScoreScraper", lambda *args, **kwargs: scraper
    )
    monkeypatch.setattr(
        "scrapers.sofascore.writer_lock.bronze_writer_lock", nullcontext
    )
    monkeypatch.setattr(runner, "_source_context", lambda *args: (17, 76986))
    monkeypatch.setattr(
        runner,
        "_tournament_canonical_url",
        lambda *args: "https://example.invalid/tournament",
    )
    monkeypatch.setattr(
        "utils.proxy_traffic.record_traffic_run", lambda *args, **kwargs: True
    )
    monkeypatch.setattr(
        "utils.proxy_traffic.log_traffic_summary", lambda *args, **kwargs: None
    )
    monkeypatch.delenv("SOFASCORE_REFRESH_EVIDENCE_DIR", raising=False)
    monkeypatch.delenv("SOFASCORE_PROXY_BUDGET_ARTIFACT", raising=False)
    monkeypatch.delenv("SOFASCORE_PROXY_BUDGET_LEDGER", raising=False)
    return scraper, saved


def _seed_replay_scope(
    tmp_path,
    *,
    mode,
    include_match=True,
    include_players=False,
    season_endpoints=None,
    payload_overrides=None,
):
    from dataclasses import replace

    from scrapers.sofascore.pipeline import (
        PLAYER_PATHS,
        build_player_spec,
        ingest_prefetched_records,
    )
    from tests.unit.scrapers.test_sofascore_pipeline import (
        _match_batch_runtime,
        _player_records_for,
        _runtime,
    )
    from tests.unit.scrapers.test_sofascore_season_pipeline import (
        FIXTURE_PATHS,
        _seed_raw,
        _specs,
    )

    if include_match:
        seed, _, _ = _match_batch_runtime(tmp_path, ["14023925"], None)
    else:
        seed, _ = _runtime(tmp_path)
    if include_players:
        player_specs = {
            ("11111", endpoint): build_player_spec(
                source_tournament_id=17,
                source_season_id=76986,
                target_id=11111,
                endpoint=endpoint,
                freshness_key="final",
                paid_proxy=False,
            )
            for endpoint in PLAYER_PATHS
        }
        ingest_prefetched_records(
            seed,
            specs=player_specs,
            records=_player_records_for("11111"),
        )
    overrides = payload_overrides or {}
    for original in _specs():
        endpoint = original.key.endpoint
        if season_endpoints is not None and endpoint not in season_endpoints:
            continue
        spec = replace(
            original, key=replace(original.key, freshness_key="final")
        )
        body = overrides.get(endpoint, FIXTURE_PATHS[endpoint].read_bytes())
        _seed_raw(seed.raw_store, spec, body)
        _, raw = seed.raw_store.load_bytes(spec.raw_target)
        status = (
            ManifestStatus.SUCCESS
            if mode == "fields"
            else ManifestStatus.RETRYABLE_FAILURE
        )
        record = EndpointManifest(
            key=spec.key,
            status=status,
            run_id="paid-fixture",
            task_id="capture",
            attempts=1,
            row_count=1 if status == ManifestStatus.SUCCESS else 0,
            http_status=200,
            raw_content_hash=raw.content_hash,
            raw_blob_key=raw.blob_key,
            error_type=(
                None if status == ManifestStatus.SUCCESS else "DeferredMaterialization"
            ),
            error_message=(
                None if status == ManifestStatus.SUCCESS else "awaiting Bronze MERGE"
            ),
        )
        seed.manifest_store.upsert(record)
    return seed, seed.manifest_store.list_for_scope("17", "76986")


def test_fields_replay_end_to_end_is_zero_source_and_idempotent(
    tmp_path, monkeypatch
):
    from dags.scripts.replay_sofascore_raw import run_replay_scope

    seed, records = _seed_replay_scope(tmp_path, mode="fields")
    runtime, transport = _fresh_replay_runtime(seed, tmp_path)
    _, saved = _patch_replay_writers(monkeypatch)

    result = run_replay_scope(
        ("17", "76986", "final"),
        mode="fields",
        capture_runtime=runtime,
        records=records,
    )

    assert result["errors"] == []
    assert result["source_request_count"] == 0
    assert transport.calls == 0
    assert result["selected"]["endpoints"] == {
        "lineups": 1,
        "standings_total": 1,
        "rounds": 1,
        "cup_trees": 1,
        "participants": 1,
        "referee_profile": 1,
    }
    assert result["status_counts"]["success"] == 6
    assert result["status_counts"]["deferred"] == 0
    assert {
        "sofascore_events",
        "sofascore_lineups",
        "sofascore_league_table",
        "sofascore_rounds",
        "sofascore_cup_trees",
        "sofascore_participants",
        "sofascore_referee_profile",
    } <= set(saved)
    lineups = saved["sofascore_lineups"][0]["df"]
    assert lineups["player_name"].fillna("").str.strip().ne("").all()
    assert lineups["formation"].fillna("").str.strip().ne("").all()
    import json

    assert all(
        isinstance(json.loads(value), list)
        for value in lineups["missing_players_json"]
    )
    assert all(
        isinstance(json.loads(value), list)
        for value in lineups["support_staff_json"]
    )
    referee = saved["sofascore_referee_profile"][0]["df"].iloc[0]
    assert referee["requested_referee_id"] == "900"
    assert referee["referee_id"] == "900"
    first_frames = {
        table: [call["df"].copy() for call in calls]
        for table, calls in saved.items()
    }

    runtime, transport = _fresh_replay_runtime(seed, tmp_path)
    saved.clear()
    repeated = run_replay_scope(
        ("17", "76986", "final"),
        mode="fields",
        capture_runtime=runtime,
        records=seed.manifest_store.list_for_scope("17", "76986"),
    )
    assert repeated["errors"] == []
    assert repeated["source_request_count"] == 0
    assert transport.calls == 0
    assert repeated["status_counts"]["deferred"] == 0
    assert set(saved) == set(first_frames)
    for table, calls in saved.items():
        assert len(calls) == len(first_frames[table])
        for call, previous in zip(calls, first_frames[table]):
            assert call["df"].equals(previous), table


def test_fields_replay_publishes_rounds_shaped_cup_trees(tmp_path, monkeypatch):
    """#1367: the World Cup rounds/blocks payload replays into the table.

    The block rows carry round context and the merged standalone roles —
    the exact shape the WC-2026 partition needs after the parser gap fix.
    """
    from dags.scripts.replay_sofascore_raw import run_replay_scope

    rounds_body = (
        Path(__file__).resolve().parents[2]
        / "fixtures"
        / "sofascore_cup_trees_rounds_shape.json"
    ).read_bytes()
    seed, records = _seed_replay_scope(
        tmp_path,
        mode="fields",
        payload_overrides={"cup_trees": rounds_body},
    )
    runtime, transport = _fresh_replay_runtime(seed, tmp_path)
    scraper, saved = _patch_replay_writers(monkeypatch)

    result = run_replay_scope(
        ("17", "76986", "final"),
        mode="fields",
        capture_runtime=runtime,
        records=records,
    )

    assert result["errors"] == []
    assert transport.calls == 0
    assert result["phases"]["season"]["report"]["cup_trees_rows"] == 4
    cup = saved["sofascore_cup_trees"][0]["df"]
    assert list(cup["node_id"]) == [
        "2585779",
        "2585780",
        "2585775",
        "2585806",
    ]
    assert list(cup["block_kind"]) == ["round", "round", "final", "third_place"]
    final_row = cup[cup["node_id"] == "2585775"].iloc[0]
    assert final_row["round_description"] == "Final"
    assert final_row["participants_json"] != "[]"


def test_drain_replays_match_players_and_season_without_squads_table(
    tmp_path, monkeypatch
):
    import json

    from dags.scripts.replay_sofascore_raw import run_replay_scope
    from tests.unit.scrapers.test_sofascore_season_pipeline import (
        PLAYER_EVIDENCE_CASES,
        _payload,
    )

    seed, records = _seed_replay_scope(
        tmp_path,
        mode="drain",
        include_players=True,
        payload_overrides={
            "participants": json.dumps(
                _payload(PLAYER_EVIDENCE_CASES)["full_participants"]
            ).encode()
        },
    )
    assert len(records) == 15
    assert all(
        record.status == ManifestStatus.RETRYABLE_FAILURE
        and record.http_status == 200
        for record in records
    )
    runtime, transport = _fresh_replay_runtime(seed, tmp_path)
    _, saved = _patch_replay_writers(monkeypatch)

    result = run_replay_scope(
        ("17", "76986", "final"),
        mode="drain",
        capture_runtime=runtime,
        records=records,
    )

    assert result["errors"] == []
    assert result["source_request_count"] == 0
    assert transport.calls == 0
    assert result["selected"]["records"] == 15
    assert result["status_counts"]["deferred"] == 0
    assert result["status_counts"]["schema_error"] == 0
    assert result["status_counts"]["success"] == 14
    assert result["status_counts"]["legitimate_empty"] == 1
    assert result["status_counts"]["not_supported"] == 0
    assert {
        "sofascore_player_universe",
        "sofascore_player_profile",
        "sofascore_player_season_stats",
        "sofascore_schedule",
        "sofascore_rounds",
        "sofascore_cup_trees",
        "sofascore_participants",
        "sofascore_referee_profile",
    } <= set(saved)
    assert "sofascore_squads" not in saved


def test_partial_lineups_raw_does_not_publish_or_fake_completion(
    tmp_path, monkeypatch
):
    from dags.scripts.replay_sofascore_raw import run_replay_scope
    from scrapers.sofascore.pipeline import ingest_prefetched_records
    from tests.unit.scrapers.test_sofascore_pipeline import (
        _record as _event_record,
        _runtime,
        _spec,
    )

    seed, _ = _runtime(tmp_path)
    spec = _spec("lineups")
    ingest_prefetched_records(
        seed,
        specs={("14023925", "lineups"): spec},
        records={"14023925:lineups": _event_record("lineups")},
    )
    record = seed.manifest_store.get(spec.key)
    runtime, transport = _fresh_replay_runtime(seed, tmp_path)
    scraper, saved = _patch_replay_writers(monkeypatch)

    result = run_replay_scope(
        ("17", "76986", "final"),
        mode="fields",
        capture_runtime=runtime,
        records=[record],
    )

    assert result["errors"]
    assert result["status_counts"]["deferred"] == 1
    assert runtime.manifest_store.get(spec.key).status == (
        ManifestStatus.RETRYABLE_FAILURE
    )
    assert saved == {}
    assert scraper.save_to_iceberg.call_count == 0
    assert result["source_request_count"] == 0
    assert transport.calls == 0


def test_writer_refusal_keeps_prior_receipt_and_defers_all_selected_raw(
    tmp_path, monkeypatch
):
    from dags.scripts.replay_sofascore_raw import run_replay_scope

    seed, records = _seed_replay_scope(
        tmp_path,
        mode="drain",
        include_match=False,
        season_endpoints={"standings_total", "rounds"},
    )
    runtime, transport = _fresh_replay_runtime(seed, tmp_path)
    scraper, saved = _patch_replay_writers(monkeypatch)
    save = scraper.save_to_iceberg.side_effect

    def refuse_rounds(**kwargs):
        if kwargs["table_name"] == "sofascore_rounds":
            raise RuntimeError("MERGE refused by fixture")
        return save(**kwargs)

    scraper.save_to_iceberg.side_effect = refuse_rounds
    result = run_replay_scope(
        ("17", "76986", "final"),
        mode="drain",
        capture_runtime=runtime,
        records=records,
    )

    assert result["errors"] == ["season_phase: MERGE refused by fixture"]
    assert set(saved) == {"sofascore_league_table"}
    assert [receipt["table"] for receipt in result["publication_receipts"]] == [
        "sofascore_league_table"
    ]
    assert result["status_counts"]["deferred"] == 2
    assert all(
        runtime.manifest_store.get(record.key).status
        == ManifestStatus.RETRYABLE_FAILURE
        for record in records
    )
    assert result["source_request_count"] == 0
    assert transport.calls == 0


def test_standings_schema_error_blocks_season_publication(
    tmp_path, monkeypatch
):
    from dags.scripts.replay_sofascore_raw import run_replay_scope

    seed, records = _seed_replay_scope(
        tmp_path,
        mode="drain",
        include_match=False,
        season_endpoints={"standings_total", "rounds"},
        payload_overrides={"standings_total": b'{"unexpected":[]}'},
    )
    runtime, transport = _fresh_replay_runtime(seed, tmp_path)
    _, saved = _patch_replay_writers(monkeypatch)

    result = run_replay_scope(
        ("17", "76986", "final"),
        mode="drain",
        capture_runtime=runtime,
        records=records,
    )

    assert result["errors"]
    assert result["status_counts"]["schema_error"] == 1
    assert result["status_counts"]["deferred"] == 1
    assert saved == {}
    assert result["source_request_count"] == 0
    assert transport.calls == 0


def test_target_cap_does_not_split_match_endpoint_group(tmp_path, monkeypatch):
    from dags.scripts.replay_sofascore_raw import run_replay_scope

    seed, records = _seed_replay_scope(
        tmp_path, mode="drain", include_players=True
    )
    runtime, transport = _fresh_replay_runtime(seed, tmp_path)
    _, saved = _patch_replay_writers(monkeypatch)

    result = run_replay_scope(
        ("17", "76986", "final"),
        mode="drain",
        target_cap=1,
        capture_runtime=runtime,
        records=records,
    )

    assert result["errors"] == []
    assert result["selected"]["records"] == 5
    assert result["selected"]["targets"] == ["event:14023925"]
    assert result["selected"]["endpoints"] == {
        "event": 1,
        "incidents": 1,
        "lineups": 1,
        "shotmap": 1,
        "statistics": 1,
    }
    assert result["status_counts"]["success"] == 5
    assert result["status_counts"]["deferred"] == 0
    assert "sofascore_player_profile" not in saved
    assert result["source_request_count"] == 0
    assert transport.calls == 0


def test_target_cap_preserves_selector_during_and_after_replay(
    tmp_path, monkeypatch
):
    from dags.scripts import replay_sofascore_raw as replay

    seed, records = _seed_replay_scope(
        tmp_path, mode="drain", include_players=True
    )
    runtime, transport = _fresh_replay_runtime(seed, tmp_path)
    _patch_replay_writers(monkeypatch)
    original_selector = replay.select_replay_scope_plans
    selector_calls = []

    def observed_selector(*args, **kwargs):
        # Check during selection as well as after any temporary replacement.
        assert replay.select_replay_scope_plans is observed_selector
        selector_calls.append(True)
        return original_selector(*args, **kwargs)

    monkeypatch.setattr(replay, "select_replay_scope_plans", observed_selector)
    result = replay.run_replay_scope(
        ("17", "76986", "final"),
        mode="drain",
        target_cap=1,
        capture_runtime=runtime,
        records=records,
    )

    assert selector_calls
    assert replay.select_replay_scope_plans is observed_selector
    assert result["errors"] == []
    assert result["selected"]["targets"] == ["event:14023925"]
    assert result["selected"]["records"] == 5
    assert result["selected"]["endpoints"] == {
        "event": 1,
        "incidents": 1,
        "lineups": 1,
        "shotmap": 1,
        "statistics": 1,
    }
    assert result["status_counts"]["success"] == 5
    assert result["status_counts"]["deferred"] == 0
    assert result["source_request_count"] == 0
    assert transport.calls == 0


def test_non2xx_and_missing_raw_lineage_are_skipped(tmp_path, monkeypatch):
    from dags.scripts.replay_sofascore_raw import run_replay_scope

    records = (
        _record(
            "lineups",
            "event",
            "14023925",
            tournament="17",
            season="76986",
            freshness="final",
            status=ManifestStatus.RETRYABLE_FAILURE,
            row_count=0,
            http_status=503,
        ),
        _record(
            "rounds",
            "season",
            "76986",
            tournament="17",
            season="76986",
            freshness="final",
            status=ManifestStatus.RETRYABLE_FAILURE,
            row_count=0,
            raw_content_hash=None,
            raw_blob_key=None,
        ),
    )
    from tests.unit.scrapers.test_sofascore_pipeline import _runtime

    seed, transport = _runtime(tmp_path)
    _patch_replay_writers(monkeypatch)
    result = run_replay_scope(
        ("17", "76986", "final"),
        mode="fields",
        capture_runtime=seed,
        records=records,
    )
    assert result["errors"] == []
    assert result["selected"]["records"] == 0
    assert result["skipped"] == 2
    assert result["source_request_count"] == 0
    assert transport.calls == 0


@pytest.mark.parametrize("outer_freshness", [None, "outside"])
def test_players_phase_uses_and_restores_exact_season_freshness(
    outer_freshness, monkeypatch
):
    import json
    import os
    from pathlib import Path
    from types import SimpleNamespace

    from dags.scripts import run_sofascore_scraper as runner
    from dags.scripts.replay_sofascore_raw import _players_phase

    observed = {}
    report = {"errors": [], "fixture_marker": "player-report"}

    def fake_capture(*args, **kwargs):
        observed["season"] = os.environ.get(
            "SOFASCORE_SEASON_FRESHNESS_KEY"
        )
        observed["player"] = os.environ.get(
            "SOFASCORE_PLAYER_FRESHNESS_KEY"
        )
        observed["offline"] = kwargs["offline_replay"]
        Path(args[3]).write_text(json.dumps(report), encoding="utf-8")
        return 0

    monkeypatch.setattr(runner, "_run_player_capture", fake_capture)
    for key in (
        "SOFASCORE_SEASON_FRESHNESS_KEY",
        "SOFASCORE_PLAYER_FRESHNESS_KEY",
    ):
        if outer_freshness is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, outer_freshness)
    records = [_record("player_profile", "player", "11111")]
    resolved = SimpleNamespace(
        league="SS-17", canonical_season="2526", freshness_key="final"
    )
    result = _players_phase(
        resolved, records, object(), mode="drain", season_format="split_year"
    )
    assert observed == {"season": "final", "player": "final", "offline": True}
    assert os.environ.get("SOFASCORE_SEASON_FRESHNESS_KEY") == outer_freshness
    assert os.environ.get("SOFASCORE_PLAYER_FRESHNESS_KEY") == outer_freshness
    assert result["exit_code"] == 0
    assert result["report"] == report


def test_terminal_status_counts_are_not_deferred_work():
    from types import SimpleNamespace

    from dags.scripts.replay_sofascore_raw import _selected_status_counts
    from scrapers.sofascore.manifest import InMemoryManifestStore

    records = (
        _record("event", "event", "1"),
        _record(
            "schedule_next",
            "season_page",
            "next:0",
            status=ManifestStatus.LEGITIMATE_EMPTY,
            row_count=0,
        ),
        _record(
            "referee_profile",
            "referee",
            "9",
            status=ManifestStatus.NOT_SUPPORTED,
            row_count=0,
            http_status=404,
        ),
        _record(
            "rounds",
            "season",
            SEASON,
            status=ManifestStatus.SCHEMA_ERROR,
            row_count=0,
        ),
        _record(
            "lineups",
            "event",
            "2",
            status=ManifestStatus.RETRYABLE_FAILURE,
            row_count=0,
            raw_content_hash=None,
            raw_blob_key=None,
        ),
    )
    runtime = SimpleNamespace(manifest_store=InMemoryManifestStore(records))
    assert _selected_status_counts(runtime, records) == {
        "success": 1,
        "legitimate_empty": 1,
        "not_supported": 1,
        "schema_error": 1,
        "deferred": 1,
    }


@pytest.mark.parametrize("phase", ["matches", "players"])
@pytest.mark.parametrize("report", [{}, {"errors": []}])
def test_phase_nonzero_exit_without_report_errors_is_aggregate_error(phase, report):
    from dags.scripts.replay_sofascore_raw import _merge_phase_report

    results = {
        "phases": {},
        "tables": [],
        "publication_receipts": [],
        "rows": 0,
        "errors": [],
    }
    _merge_phase_report(
        results,
        phase,
        {"ran": True, "exit_code": 1, "report": report, "errors": []},
    )
    assert results["errors"] == [f"{phase}: runner exited 1"]


@pytest.mark.parametrize("invalid", ["mode", "json", "integer", "cap"])
def test_cli_rejects_malformed_input_before_runtime_build(
    invalid, tmp_path, monkeypatch
):
    import importlib
    import json
    from unittest.mock import MagicMock

    replay = importlib.import_module("dags.scripts.replay_sofascore_raw")
    output = tmp_path / "replay-result.json"
    monkeypatch.setattr(replay, "_DEFAULT_REPLAY_OUTPUT", str(output))
    build = MagicMock()
    monkeypatch.setattr("scrapers.sofascore.pipeline.build_capture_runtime", build)
    monkeypatch.setattr(
        "utils.proxy_traffic.record_traffic_run", lambda *args, **kwargs: True
    )
    monkeypatch.setattr(
        "utils.proxy_traffic.log_traffic_summary", lambda *args, **kwargs: None
    )
    scope = {
        "source_tournament_id": "17",
        "source_season_id": "76986",
        "freshness_key": "final",
    }
    mode = "bogus" if invalid == "mode" else "fields"
    if invalid == "json":
        scope_json = "{"
    else:
        if invalid == "integer":
            scope["source_tournament_id"] = 17
        scope_json = json.dumps(scope)
    cap = "0" if invalid == "cap" else "25"
    rc = replay.main(
        [
            "--mode",
            mode,
            "--scope-json",
            scope_json,
            "--target-cap",
            cap,
            "--output",
            str(output),
        ]
    )
    assert rc == 1
    assert json.loads(output.read_text())["errors"]
    build.assert_not_called()


def test_drain_single_non_event_match_record_still_runs_match_phase(
    tmp_path, monkeypatch
):
    from dataclasses import replace

    from dags.scripts.replay_sofascore_raw import run_replay_scope

    seed, _ = _seed_replay_scope(tmp_path, mode="fields")
    for record in seed.manifest_store.list_for_scope("17", "76986"):
        if record.key.target_type != "event":
            continue
        retry = record.key.endpoint == "lineups"
        seed.manifest_store.upsert(
            replace(
                record,
                status=(
                    ManifestStatus.RETRYABLE_FAILURE
                    if retry
                    else ManifestStatus.SUCCESS
                ),
                row_count=0 if retry else 1,
                error_type="DeferredMaterialization" if retry else None,
                error_message="awaiting Bronze MERGE" if retry else None,
            )
        )
    records = seed.manifest_store.list_for_scope("17", "76986")
    runtime, transport = _fresh_replay_runtime(seed, tmp_path)
    _, saved = _patch_replay_writers(monkeypatch)

    result = run_replay_scope(
        ("17", "76986", "final"),
        mode="drain",
        capture_runtime=runtime,
        records=records,
    )

    assert result["selected"]["records"] == 1
    assert result["selected"]["endpoints"] == {"lineups": 1}
    assert result["errors"] == []
    assert result["status_counts"]["success"] == 1
    assert result["status_counts"]["deferred"] == 0
    assert "sofascore_lineups" in saved
    assert result["source_request_count"] == 0
    assert transport.calls == 0


@pytest.mark.parametrize("manifest_location", ["env", "flag"])
def test_cli_json_manifest_replays_local_raw_without_sql(
    manifest_location, tmp_path, monkeypatch
):
    import json

    from dags.scripts.replay_sofascore_raw import main
    from scrapers.sofascore.manifest import JsonFileManifestStore

    seed, records = _seed_replay_scope(tmp_path, mode="fields")
    manifest_path = tmp_path / "endpoint-manifest.json"
    store = JsonFileManifestStore(manifest_path)
    for record in records:
        store.upsert(record)
    if manifest_location == "env":
        monkeypatch.setenv("SOFASCORE_MANIFEST_PATH", str(manifest_path))
    else:
        monkeypatch.delenv("SOFASCORE_MANIFEST_PATH", raising=False)
    _patch_replay_writers(monkeypatch)
    output = tmp_path / "cli-result.json"
    args = [
        "--mode",
        "fields",
        "--scope-json",
        json.dumps(
            {
                "source_tournament_id": "17",
                "source_season_id": "76986",
                "freshness_key": "final",
            }
        ),
        "--raw-store-uri",
        f"file://{tmp_path / 'raw'}",
        "--manifest-backend",
        "json",
        "--output",
        str(output),
    ]
    if manifest_location == "flag":
        args.extend(["--manifest-path", str(manifest_path)])

    assert main(args) == 0
    result = json.loads(output.read_text())
    assert result["errors"] == []
    assert result["selected"]["records"] == 6
    assert result["status_counts"]["deferred"] == 0
    assert result["source_request_count"] == 0


# --- #1367 per-scope overlay contracts -------------------------------------


def _single_year_catalog():
    """Registry-grade catalog for the measured SS-16/58210 scope."""
    from scrapers.sofascore.catalog import SofaScoreCatalog

    return SofaScoreCatalog.from_mapping(
        {
            "schema_version": 2,
            "generated_at": "2026-11-22T00:00:00Z",
            "tournaments": [
                {
                    "unique_tournament_id": 16,
                    "name": "FIFA World Cup",
                    "slug": "world-championship",
                    "category": {"id": 1468, "name": "World", "slug": "world"},
                    "sport_slug": "football",
                    "page_path": "football/world/world-championship",
                    "canonical_id": "INT-World Cup",
                    "enabled": True,
                    "classification": {
                        "sport": "football",
                        "gender": "male",
                        "age_group": "adult",
                        "team_level": "first_team",
                        "status": "source_confirmed_adult_men",
                        "exclusion_reasons": [],
                        "evidence": [
                            {
                                "type": "source_field",
                                "endpoint": "/unique-tournament/16",
                                "field": "gender",
                                "value": "M",
                            }
                        ],
                    },
                    "review": {
                        "status": "approved",
                        "confirmed": {
                            "sport": "football",
                            "gender": "male",
                            "age_group": "adult",
                            "team_level": "first_team",
                        },
                        "evidence": [
                            {
                                "type": "owner_review",
                                "reference": "#1367 fixture",
                                "note": "offline replay fixture registry",
                            }
                        ],
                        "reviewed_by": "fixture",
                        "reviewed_at": "2026-11-22T00:00:00Z",
                    },
                    "seasons": [
                        {
                            "season_id": 58210,
                            "name": "World Cup 2026",
                            "source_name": "World Cup 2026",
                            "year": "2026",
                            "format": "calendar_year",
                            "season_format": "single_year",
                            "canonical_season": "2026",
                            "start_date": None,
                            "end_date": None,
                            "aliases": ["2026", "World Cup 2026"],
                            "evidence": [
                                {
                                    "type": "source_field",
                                    "endpoint": "/unique-tournament/16/seasons",
                                    "value": "58210",
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    )


def test_scope_overlay_rekeys_registry_row_and_preserves_eligibility():
    import json

    from dags.scripts.replay_sofascore_raw import _scope_overlay
    from scrapers.sofascore.raw_replay import ReplayScopeKey, resolve_replay_scope

    catalog = _single_year_catalog()
    resolved = resolve_replay_scope(
        ReplayScopeKey("16", "58210", "2026"), catalog
    )
    assert (resolved.league, resolved.canonical_season) == ("SS-16", "2026")

    with _scope_overlay(resolved, ("registry", catalog)) as (registry_path, medallion_dir):
        document = json.loads(
            Path(registry_path).read_text(encoding="utf-8")
        )
        row = document["tournaments"][0]
        assert row["canonical_id"] == "SS-16"
        source = catalog.tournament(16)
        assert row["enabled"] is source.enabled
        assert row["classification"] == dict(source.classification)
        assert row["review"] == dict(source.review)
        assert row["seasons"][0]["canonical_season"] == "2026"
        assert row["seasons"][0]["season_format"] == "single_year"
        import yaml

        competitions = yaml.safe_load(
            (Path(medallion_dir) / "competitions.yaml").read_text(
                encoding="utf-8"
            )
        )["competitions"]
        assert competitions[0]["id"] == "SS-16"
        assert competitions[0]["seasons"][0]["season_format"] == "single_year"
        assert competitions[0]["seasons"][0]["id"] == 2026


@pytest.mark.parametrize("outer_env", [None, "preset"])
def test_scope_overlay_binds_and_restores_both_configurations(outer_env):
    import dags.utils.medallion_config as medallion_config
    from dags.scripts.replay_sofascore_raw import _scope_overlay
    from scrapers.sofascore import scraper as scraper_module
    from scrapers.sofascore.raw_replay import ReplayScopeKey, resolve_replay_scope

    catalog = _single_year_catalog()
    resolved = resolve_replay_scope(
        ReplayScopeKey("16", "58210", "2026"), catalog
    )
    previous_config_dir = medallion_config.CONFIG_DIR
    previous_module_catalog = scraper_module._SOFASCORE_CATALOG
    previous_env = {
        name: os.environ.get(name)
        for name in ("SOFASCORE_REGISTRY_PATH", "MEDALLION_CONFIG_DIR")
    }
    if outer_env is None:
        # Review r2 F6: the absent case must not inherit values some other
        # test left behind; clearing here makes the post-overlay absence
        # assertions meaningful while ``finally`` still restores the
        # process's original values.
        for name in previous_env:
            os.environ.pop(name, None)
    else:
        os.environ["SOFASCORE_REGISTRY_PATH"] = outer_env
        os.environ["MEDALLION_CONFIG_DIR"] = outer_env
    try:
        with pytest.raises(RuntimeError, match="boom"):
            with _scope_overlay(resolved, ("registry", catalog)) as (registry_path, medallion_dir):
                assert os.environ["SOFASCORE_REGISTRY_PATH"] == registry_path
                assert os.environ["MEDALLION_CONFIG_DIR"] == medallion_dir
                assert Path(medallion_config.CONFIG_DIR) == Path(medallion_dir)
                overlay_entry = scraper_module._SOFASCORE_CATALOG.competition("SS-16")
                assert overlay_entry.unique_tournament_id == 16
                assert overlay_entry.capture_allowed is True
                assert any(
                    item["id"] == "SS-16"
                    for item in medallion_config.load_competitions()["competitions"]
                )
                raise RuntimeError("boom")
        # Restoration also covers the exceptional exit (review r1).
        assert os.environ.get("SOFASCORE_REGISTRY_PATH") == outer_env
        assert os.environ.get("MEDALLION_CONFIG_DIR") == outer_env
        assert medallion_config.CONFIG_DIR is previous_config_dir
        assert scraper_module._SOFASCORE_CATALOG is previous_module_catalog
    finally:
        for name, value in previous_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        medallion_config.CONFIG_DIR = previous_config_dir
        scraper_module._SOFASCORE_CATALOG = previous_module_catalog


def test_scope_overlay_refuses_unreplayable_registry_season_format():
    from dags.scripts.replay_sofascore_raw import _scope_overlay
    from scrapers.sofascore.raw_replay import ResolvedReplayScope

    catalog = _single_year_catalog()
    # A registry row whose matched season carries an unreplayable format
    # must fail the overlay loudly instead of producing a broken partition.
    source = catalog.tournament(16)
    seasons = tuple(
        SimpleNamespace(
            season_id=season.season_id,
            canonical_season=season.canonical_season,
            season_format="unknown",
        )
        for season in source.seasons
    )
    patched = SimpleNamespace(
        unique_tournament_id=source.unique_tournament_id,
        name=source.name,
        slug=source.slug,
        category_id=source.category_id,
        category_name=source.category_name,
        category_slug=source.category_slug,
        sport_slug=source.sport_slug,
        page_path=source.page_path,
        enabled=source.enabled,
        classification=source.classification,
        review=source.review,
        seasons=seasons,
    )
    patched_catalog = SimpleNamespace(tournament=lambda _tid: patched)

    with pytest.raises(ReplayScopeError, match="no replayable season format"):
        with _scope_overlay(
            ResolvedReplayScope("16", "58210", "2026", "SS-16", "2026"),
            ("registry", patched_catalog),
        ):
            pass


def test_replay_season_label_guard_uses_overlay_format():
    from dags.scripts.replay_sofascore_raw import (
        _assert_replay_season_label,
        _scope_overlay,
    )
    from scrapers.sofascore.raw_replay import ReplayScopeKey, resolve_replay_scope
    from scrapers.sofascore.scraper import _season_label

    catalog = _single_year_catalog()
    resolved = resolve_replay_scope(
        ReplayScopeKey("16", "58210", "2026"), catalog
    )
    with _scope_overlay(resolved, ("registry", catalog)):
        assert _season_label("SS-16", "2026") == "2026"
        _assert_replay_season_label(resolved, "single_year")
    # Outside the overlay the capture key has no medallion entry, so the
    # club fallback would silently produce a WRONG partition label — the
    # guard must fail loud exactly there.
    with pytest.raises(ReplayScopeError, match="label drift"):
        _assert_replay_season_label(resolved, "single_year")


def test_replay_season_label_guard_accepts_split_year():
    from dags.scripts.replay_sofascore_raw import (
        _assert_replay_season_label,
        _scope_overlay,
    )
    from scrapers.sofascore.raw_replay import ReplayScopeKey, resolve_replay_scope

    catalog = _catalog_76986()
    resolved = resolve_replay_scope(
        ReplayScopeKey("17", "76986", "final"), catalog
    )
    with _scope_overlay(resolved, ("registry", catalog)):
        _assert_replay_season_label(resolved, "split_year")
    # The split-year short form needs no overlay entry, so the guard must
    # not reject it even when the binding is the plain repo configuration.
    _assert_replay_season_label(resolved, "split_year")
    with pytest.raises(ReplayScopeError, match="no registry season format"):
        _assert_replay_season_label(resolved, "")


def test_scope_overlay_rejects_scope_outside_registry():
    from dags.scripts.replay_sofascore_raw import _scope_overlay
    from scrapers.sofascore.raw_replay import ResolvedReplayScope

    resolved = ResolvedReplayScope(
        source_tournament_id="16",
        source_season_id="99999",
        freshness_key="2026",
        league="SS-16",
        canonical_season="2026",
    )
    with pytest.raises(ReplayScopeError, match="unknown for source tournament"):
        with _scope_overlay(resolved, ("registry", _single_year_catalog())):
            pass


# --- stage 3: campaign-snapshot resolution for non-registry tournaments ------


def _campaign_snapshot_fixture(tmp_path, *, campaign_id="unit-campaign"):
    """Digest-valid campaign snapshot with one NON-registry tournament."""
    document = {
        "schema_version": 1,
        "candidate_count": 1,
        "policy_id": "unit-policy",
        "campaign_id": campaign_id,
        "tournaments": [
            {
                "unique_tournament_id": 54,
                "capture_key": "SS-54",
                "name": "Bundesliga (AUT)",
                "slug": "bundesliga",
                "page_path": "football/austria/bundesliga",
                "category": {"id": 9, "name": "Austria", "slug": "austria"},
                "kind": "league",
                "metadata_status": "ready",
                "classification": {
                    "sport": "football",
                    "gender": "male",
                    "age_group": "unknown",
                    "team_level": "unknown",
                    "status": "review_required",
                    "exclusion_reasons": [],
                    "evidence": [
                        {
                            "type": "source_field",
                            "endpoint": "/unique-tournament/54",
                            "field": "gender",
                            "value": "M",
                        }
                    ],
                },
                "eligibility_review": {
                    "status": "approved",
                    "confirmed": {
                        "age_group": "adult",
                        "team_level": "first_team",
                        "professional": True,
                    },
                    "reviewed_by": "unit-owner",
                    "reviewed_at": "2026-08-21",
                },
                "seasons": [
                    {
                        "source_season_id": 616,
                        "source_name": "Bundesliga 25/26",
                        "canonical_season": "2526",
                        "season_format": "split_year",
                        "start_year": 2025,
                        "team_count": 12,
                        "metadata_status": "ready",
                        "team_count_evidence": {
                            "type": "source_team_ids",
                            "endpoint": "/unique-tournament/54/season/616/teams",
                            "count": 12,
                            "team_ids_sha256": "b" * 64,
                        },
                    }
                ],
            }
        ],
    }
    raw = json.dumps(
        document, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    document["snapshot_id"] = hashlib.sha256(raw).hexdigest()
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return str(path)


def test_snapshot_resolution_covers_registry_absent_tournament(tmp_path, monkeypatch):
    from dags.scripts.replay_sofascore_raw import _load_scope

    snapshot = _campaign_snapshot_fixture(tmp_path)
    monkeypatch.setenv("SOFASCORE_ALL_MENS_SNAPSHOT", snapshot)
    monkeypatch.setenv("SOFASCORE_REPLAY_CAMPAIGN_ID", "unit-campaign")

    runtime = _FakeRuntime(_FakeCatalog(), _FakeManager())
    resolved, plan, season_format, overlay_source = _load_scope(
        ("54", "616", "final"),
        mode=MODE_FIELDS,
        capture_runtime=runtime,
        records=(),
    )

    assert resolved.league == "SS-54"
    assert resolved.canonical_season == "2526"
    assert season_format == "split_year"
    assert overlay_source[0] == "snapshot"
    assert overlay_source[1]["capture_key"] == "SS-54"


def test_registry_authority_is_never_displaced_by_snapshot(tmp_path, monkeypatch):
    """A tournament the registry KNOWS keeps registry errors (adjudication)."""
    from dags.scripts.replay_sofascore_raw import _load_scope

    snapshot = _campaign_snapshot_fixture(tmp_path)
    monkeypatch.setenv("SOFASCORE_ALL_MENS_SNAPSHOT", snapshot)

    runtime = _FakeRuntime(_FakeCatalog(), _FakeManager())
    with pytest.raises(ReplayPlanningError, match="unknown for source"):
        _load_scope(
            # 17 is in the fake registry; the season is not.
            (TOURNAMENT, "99999", "final"),
            mode=MODE_FIELDS,
            capture_runtime=runtime,
            records=(),
        )


def test_snapshot_campaign_identity_is_pinned(tmp_path, monkeypatch):
    from dags.scripts.replay_sofascore_raw import (
        ReplayScopeError,
        _resolve_snapshot_scope,
    )

    snapshot = _campaign_snapshot_fixture(tmp_path)
    monkeypatch.setenv("SOFASCORE_ALL_MENS_SNAPSHOT", snapshot)
    monkeypatch.setenv("SOFASCORE_REPLAY_CAMPAIGN_ID", "other-campaign")

    with pytest.raises(ReplayScopeError, match="campaign"):
        _resolve_snapshot_scope(
            __import__(
                "scrapers.sofascore.raw_replay", fromlist=["ReplayScopeKey"]
            ).ReplayScopeKey("54", "616", "final")
        )


def test_snapshot_overlay_renders_and_binds(tmp_path, monkeypatch):
    import dags.utils.medallion_config as medallion_config
    from dags.scripts.replay_sofascore_raw import (
        _resolve_snapshot_scope,
        _scope_overlay,
    )
    from scrapers.sofascore import scraper as scraper_module
    from scrapers.sofascore.raw_replay import ReplayScopeKey

    snapshot = _campaign_snapshot_fixture(tmp_path)
    monkeypatch.setenv("SOFASCORE_ALL_MENS_SNAPSHOT", snapshot)

    resolved, season_format, scope_row = _resolve_snapshot_scope(
        ReplayScopeKey("54", "616", "final")
    )
    previous_config_dir = medallion_config.CONFIG_DIR
    previous_module_catalog = scraper_module._SOFASCORE_CATALOG
    try:
        with _scope_overlay(resolved, ("snapshot", scope_row)) as (
            registry_path,
            medallion_dir,
        ):
            entry = scraper_module._SOFASCORE_CATALOG.competition("SS-54")
            assert entry.unique_tournament_id == 54
            assert entry.capture_allowed is True
            assert any(
                item["id"] == "SS-54"
                for item in medallion_config.load_competitions()["competitions"]
            )
        assert medallion_config.CONFIG_DIR is previous_config_dir
        assert scraper_module._SOFASCORE_CATALOG is previous_module_catalog
    finally:
        medallion_config.CONFIG_DIR = previous_config_dir
        scraper_module._SOFASCORE_CATALOG = previous_module_catalog


# --- stage 4: player-universe freshness key -----------------------------------


class _UniversePlan:
    def __init__(self, ready):
        self.player_universe_ready = ready


def _squads_record(freshness, terminal, updated):
    return SimpleNamespace(
        key=SimpleNamespace(
            endpoint="squads",
            freshness_key=freshness,
        ),
        status=SimpleNamespace(terminal=terminal),
        updated_at=updated,
    )


def test_universe_key_prefers_scope_key_when_ready(monkeypatch):
    from dags.scripts.replay_sofascore_raw import _player_universe_freshness_key

    planned = []

    def fake_plan(raw_store, store, **kwargs):
        planned.append(kwargs["freshness_key"])
        return _UniversePlan(ready=True)

    monkeypatch.setattr(
        "scrapers.sofascore.season_pipeline.plan_season_partition", fake_plan
    )
    runtime = SimpleNamespace(
        raw_store=object(),
        manifest_store=SimpleNamespace(
            list_for_scope=lambda tid, sid: [
                _squads_record("day-2026-07-17", True, "2026-07-17")
            ]
        ),
    )
    resolved = SimpleNamespace(
        source_tournament_id="16",
        source_season_id="58210",
        freshness_key="week-2026-W30",
    )

    key, candidates = _player_universe_freshness_key(runtime, resolved)

    assert key == "week-2026-W30"
    assert planned == ["week-2026-W30"]


def test_universe_key_falls_back_to_terminal_squads_key(monkeypatch):
    from dags.scripts.replay_sofascore_raw import _player_universe_freshness_key

    def fake_plan(raw_store, store, **kwargs):
        return _UniversePlan(ready=kwargs["freshness_key"].startswith("day-"))

    monkeypatch.setattr(
        "scrapers.sofascore.season_pipeline.plan_season_partition", fake_plan
    )
    runtime = SimpleNamespace(
        raw_store=object(),
        manifest_store=SimpleNamespace(
            list_for_scope=lambda tid, sid: [
                _squads_record("day-2026-07-17", True, "2026-07-17"),
                _squads_record("day-2026-07-18", True, "2026-07-18"),
            ]
        ),
    )
    resolved = SimpleNamespace(
        source_tournament_id="16",
        source_season_id="58210",
        freshness_key="week-2026-W30",
    )

    key, candidates = _player_universe_freshness_key(runtime, resolved)

    assert key == "day-2026-07-18"
    assert candidates[0] == "week-2026-W30"


def test_universe_key_keeps_scope_key_when_nothing_passes(monkeypatch):
    from dags.scripts.replay_sofascore_raw import _player_universe_freshness_key

    monkeypatch.setattr(
        "scrapers.sofascore.season_pipeline.plan_season_partition",
        lambda *args, **kwargs: _UniversePlan(ready=False),
    )
    runtime = SimpleNamespace(
        raw_store=object(),
        manifest_store=SimpleNamespace(
            list_for_scope=lambda tid, sid: [
                _squads_record("day-2026-07-17", True, "2026-07-17")
            ]
        ),
    )
    resolved = SimpleNamespace(
        source_tournament_id="16",
        source_season_id="58210",
        freshness_key="week-2026-W30",
    )

    key, candidates = _player_universe_freshness_key(runtime, resolved)

    assert key == "week-2026-W30"
    assert len(candidates) == 2
