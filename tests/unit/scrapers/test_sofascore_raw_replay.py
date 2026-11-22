"""Unit tests for the pure SofaScore raw-replay selection (#1367)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from scrapers.sofascore.adapters import MANIFEST_COLUMNS, manifest_to_row
from scrapers.sofascore.manifest import (
    EndpointManifest,
    ManifestKey,
    ManifestStatus,
)
from scrapers.sofascore.partition_keys import canonical_league
from scrapers.sofascore.raw_replay import (
    FIELDS_BLOCKING_ENDPOINTS,
    FIELDS_ENDPOINTS,
    MODE_DRAIN,
    MODE_FIELDS,
    REPLAY_MANIFEST_TABLE,
    ReplayPlanningError,
    ReplayScopeKey,
    ReplayScopePlan,
    ResolvedReplayScope,
    list_replay_scope_keys,
    load_replay_scope_records,
    replay_eligible_scope_pairs,
    resolve_replay_scope,
    select_replay_scope_plans,
    validate_replay_scope_pair,
)

TOURNAMENT = "17"
SEASON = "52372"
FRESHNESS = "20261117T000000Z"
PAID_FIELDS = tuple(sorted(FIELDS_ENDPOINTS))
ZERO_SKIPS = {"non_2xx": 0, "missing_lineage": 0, "other_status": 0}


def _record(
    *,
    endpoint="lineups",
    target_type="match",
    target_id="1000",
    tournament=TOURNAMENT,
    season=SEASON,
    freshness=FRESHNESS,
    status=ManifestStatus.SUCCESS,
    http_status=200,
    raw_content_hash="hash-1",
    raw_blob_key="raw/sofascore/blob-1",
    run_id="run-1",
    task_id="capture",
    attempts=1,
    row_count=5,
) -> EndpointManifest:
    return EndpointManifest(
        key=ManifestKey(
            source_tournament_id=tournament,
            source_season_id=season,
            target_type=target_type,
            target_id=target_id,
            endpoint=endpoint,
            freshness_key=freshness,
        ),
        status=status,
        run_id=run_id,
        task_id=task_id,
        attempts=attempts,
        row_count=row_count,
        http_status=http_status,
        raw_content_hash=raw_content_hash,
        raw_blob_key=raw_blob_key,
    )


def test_fields_replays_paid_endpoints_with_any_status():
    records = [
        _record(endpoint=endpoint, status=ManifestStatus.SUCCESS)
        for endpoint in PAID_FIELDS
    ]
    records.append(
        _record(
            endpoint="participants",
            status=ManifestStatus.SCHEMA_ERROR,
            run_id="run-new",
            row_count=0,
        )
    )
    records.append(
        _record(endpoint="events", status=ManifestStatus.SUCCESS, run_id="run-events")
    )

    plans = select_replay_scope_plans(records, mode=MODE_FIELDS)

    assert len(plans) == 1
    plan = plans[0]
    assert isinstance(plan, ReplayScopePlan)
    assert plan.scope == ReplayScopeKey(TOURNAMENT, SEASON, FRESHNESS)
    assert plan.mode == MODE_FIELDS
    assert isinstance(plan.records, tuple)
    assert [r.key.endpoint for r in plan.records] == [
        "cup_trees",
        "lineups",
        "participants",
        "participants",
        "referee_profile",
        "rounds",
        "standings_total",
    ]
    participants_runs = [
        r.run_id for r in plan.records if r.key.endpoint == "participants"
    ]
    assert participants_runs == ["run-1", "run-new"]
    assert all(r.run_id != "run-events" for r in plan.records)
    assert plans.skipped == ZERO_SKIPS


def test_drain_replays_generic_http200_deferred_for_any_endpoint():
    deferred = [
        _record(
            endpoint=endpoint,
            status=ManifestStatus.RETRYABLE_FAILURE,
            http_status=200,
            row_count=0,
            attempts=3,
        )
        for endpoint in ("events", "lineups", "odds", "vote")
    ]
    already_parsed = _record(
        endpoint="events",
        target_id="1999",
        status=ManifestStatus.SUCCESS,
    )

    plans = select_replay_scope_plans(deferred + [already_parsed], mode=MODE_DRAIN)

    assert len(plans) == 1
    assert plans[0].mode == MODE_DRAIN
    assert [r.key.endpoint for r in plans[0].records] == [
        "events",
        "lineups",
        "odds",
        "vote",
    ]
    assert all(r.key.target_id == "1000" for r in plans[0].records)
    assert plans.skipped == {"non_2xx": 0, "missing_lineage": 0, "other_status": 1}


def test_drain_skips_non_replayable_raw_with_reasons():
    records = [
        _record(
            endpoint="lineups",
            status=ManifestStatus.RETRYABLE_FAILURE,
            http_status=503,
        ),
        _record(
            endpoint="rounds",
            status=ManifestStatus.RETRYABLE_FAILURE,
            http_status=429,
        ),
        _record(
            endpoint="cup_trees",
            status=ManifestStatus.RETRYABLE_FAILURE,
            http_status=204,
        ),
        _record(
            endpoint="participants",
            status=ManifestStatus.RETRYABLE_FAILURE,
            http_status=200,
            raw_content_hash=None,
        ),
        _record(
            endpoint="referee_profile",
            status=ManifestStatus.RETRYABLE_FAILURE,
            http_status=200,
            raw_blob_key=None,
        ),
        _record(endpoint="standings_total", status=ManifestStatus.SUCCESS),
        _record(
            endpoint="lineups",
            target_id="1002",
            status=ManifestStatus.NOT_SUPPORTED,
            http_status=None,
        ),
    ]

    plans = select_replay_scope_plans(records, mode=MODE_DRAIN)

    assert plans == ()
    assert plans.skipped == {"non_2xx": 4, "missing_lineage": 2, "other_status": 1}


def test_fields_skips_non_replayable_raw_regardless_of_status():
    records = [
        _record(
            endpoint="lineups",
            status=ManifestStatus.NOT_SUPPORTED,
            http_status=None,
        ),
        _record(
            endpoint="rounds",
            status=ManifestStatus.RETRYABLE_FAILURE,
            http_status=200,
            raw_content_hash=None,
        ),
        _record(
            endpoint="standings_total",
            status=ManifestStatus.RETRYABLE_FAILURE,
            http_status=200,
        ),
        _record(
            endpoint="cup_trees",
            target_id="1001",
            status=ManifestStatus.LEGITIMATE_EMPTY,
            http_status=200,
            row_count=0,
        ),
    ]

    plans = select_replay_scope_plans(records, mode=MODE_FIELDS)

    assert len(plans) == 1
    assert [r.key.endpoint for r in plans[0].records] == [
        "standings_total",
        "cup_trees",
    ]
    assert plans.skipped == {"non_2xx": 1, "missing_lineage": 1, "other_status": 0}


def test_fields_ignores_non_paid_endpoints_entirely():
    records = [
        _record(
            endpoint="events",
            status=ManifestStatus.RETRYABLE_FAILURE,
            http_status=503,
        ),
        _record(endpoint="odds", status=ManifestStatus.SUCCESS),
    ]

    plans = select_replay_scope_plans(records, mode=MODE_FIELDS)

    assert plans == ()
    assert plans.skipped == ZERO_SKIPS


def test_scope_cap_keeps_deterministic_scope_order():
    records = [
        _record(tournament="23", season="1"),
        _record(tournament="10", season="9"),
        _record(tournament="17", season="52372"),
    ]

    plans = select_replay_scope_plans(records, mode=MODE_FIELDS, scope_cap=2)

    assert [p.scope[:2] for p in plans] == [("10", "9"), ("17", "52372")]
    assert [len(p.records) for p in plans] == [1, 1]
    again = select_replay_scope_plans(
        list(reversed(records)), mode=MODE_FIELDS, scope_cap=2
    )
    assert list(again) == list(plans)


def test_scope_order_breaks_ties_on_freshness():
    records = [
        _record(tournament="17", season="20", freshness="b"),
        _record(tournament="17", season="20", freshness="a"),
        _record(tournament="17", season="10", freshness="z"),
        _record(tournament="10", season="99", freshness="z"),
    ]

    plans = select_replay_scope_plans(records, mode=MODE_FIELDS, scope_cap=3)

    assert [p.scope for p in plans] == [
        ReplayScopeKey("10", "99", "z"),
        ReplayScopeKey("17", "10", "z"),
        ReplayScopeKey("17", "20", "a"),
    ]


def test_target_cap_selects_whole_targets_only():
    records = []
    for target_id in ("1003", "1001", "1002"):
        records.extend(
            _record(endpoint=endpoint, target_id=target_id)
            for endpoint in PAID_FIELDS
        )

    plans = select_replay_scope_plans(records, mode=MODE_FIELDS, target_cap=2)

    assert len(plans) == 1
    plan = plans[0]
    assert {r.key.target_id for r in plan.records} == {"1001", "1002"}
    for target_id in ("1001", "1002"):
        endpoints = sorted(
            r.key.endpoint for r in plan.records if r.key.target_id == target_id
        )
        assert endpoints == list(PAID_FIELDS)
    assert [r.key.endpoint for r in plan.records] == list(PAID_FIELDS) + list(
        PAID_FIELDS
    )
    assert plans.skipped == ZERO_SKIPS


def test_exact_scope_filters_records_before_grouping():
    records = [
        _record(endpoint="lineups"),
        _record(endpoint="rounds"),
        _record(tournament="23", season="1", endpoint="lineups"),
    ]

    plans = select_replay_scope_plans(
        records, mode=MODE_FIELDS, exact_scope=(TOURNAMENT, SEASON)
    )

    assert len(plans) == 1
    assert plans[0].scope == ReplayScopeKey(TOURNAMENT, SEASON, FRESHNESS)
    assert [r.key.endpoint for r in plans[0].records] == ["lineups", "rounds"]
    assert plans.skipped == ZERO_SKIPS
    assert (
        select_replay_scope_plans(
            records, mode=MODE_FIELDS, exact_scope=("23", "999")
        )
        == ()
    )


def test_empty_input_yields_empty_selection_with_zero_skips():
    plans = select_replay_scope_plans([], mode=MODE_DRAIN)

    assert isinstance(plans, tuple)
    assert plans == ()
    assert plans.skipped == ZERO_SKIPS


def test_accepts_any_iterable_of_records():
    plans = select_replay_scope_plans(iter([_record()]), mode=MODE_FIELDS)

    assert len(plans) == 1
    assert plans[0].records[0].key.endpoint == "lineups"


@pytest.mark.parametrize("mode", ["backfill", "", None])
def test_invalid_mode_rejected(mode):
    with pytest.raises(ReplayPlanningError):
        select_replay_scope_plans([], mode=mode)


@pytest.mark.parametrize("cap", [0, -3, "2", 2.5, None])
def test_invalid_caps_rejected(cap):
    with pytest.raises(ReplayPlanningError):
        select_replay_scope_plans([], mode=MODE_FIELDS, scope_cap=cap)
    with pytest.raises(ReplayPlanningError):
        select_replay_scope_plans([], mode=MODE_DRAIN, target_cap=cap)


@pytest.mark.parametrize(
    "exact_scope",
    [("17",), ("17", "1", "2"), "17", 17, {"17", "52372"}],
)
def test_invalid_exact_scope_rejected(exact_scope):
    with pytest.raises(ReplayPlanningError):
        select_replay_scope_plans([], mode=MODE_FIELDS, exact_scope=exact_scope)


class FakeManager:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.calls = []

    def _execute(self, sql, fetch=False, params=None):
        self.calls.append(
            {"sql": sql, "fetch": fetch, "params": tuple(params or ())}
        )
        return list(self.rows)


def _manifest_row(record: EndpointManifest) -> tuple:
    row = manifest_to_row(record)
    return tuple(row[column] for column in MANIFEST_COLUMNS)


def _fake_catalog(*, enabled=True, season_format="split_year"):
    tournament = SimpleNamespace(
        unique_tournament_id=17,
        canonical_id="ENG-Premier League",
        enabled=enabled,
        capture_allowed=enabled,
        seasons=(
            SimpleNamespace(
                season_id=76986,
                canonical_season="2526",
                season_format=season_format,
            ),
        ),
    )
    disabled = SimpleNamespace(
        unique_tournament_id=1,
        canonical_id="INT-European Championship",
        enabled=False,
        capture_allowed=False,
        seasons=(),
    )

    class Catalog:
        def tournament(self, unique_tournament_id):
            if int(unique_tournament_id) == 17:
                return tournament
            if int(unique_tournament_id) == 1:
                return disabled
            raise ValueError(
                f"unknown unique_tournament_id {unique_tournament_id!r}"
            )

        def tournament_map(self):
            return {
                "ENG-Premier League": 17,
                "INT-European Championship": 1,
            }

    Catalog.tournaments = (tournament, disabled)
    return Catalog()


def test_list_replay_scope_keys_fields_sql_filters_and_params():
    manager = FakeManager([("23", "9", "f2"), ("10", "9", "f1")])

    keys = list_replay_scope_keys(manager, mode=MODE_FIELDS)

    call = manager.calls[0]
    assert call["fetch"] is True
    sql = call["sql"]
    assert f"FROM {REPLAY_MANIFEST_TABLE} m" in sql
    assert "SELECT DISTINCT" in sql
    assert '"http_status" >= 200' in sql
    assert '"http_status" < 300' in sql
    assert '"http_status" <> 204' in sql
    assert "COALESCE(TRIM(\"raw_content_hash\"), '') <> ''" in sql
    assert "COALESCE(TRIM(\"raw_blob_key\"), '') <> ''" in sql
    assert '"endpoint" IN (' in sql
    for endpoint in sorted(FIELDS_ENDPOINTS):
        assert f"'{endpoint}'" in sql
    assert "retryable_failure" not in sql
    assert (
        'ORDER BY m."source_tournament_id", m."source_season_id", '
        'm."freshness_key"' in sql
    )
    assert "LIMIT 1" in sql
    assert call["params"] == ()
    assert keys == (
        ReplayScopeKey("23", "9", "f2"),
        ReplayScopeKey("10", "9", "f1"),
    )


def test_list_replay_scope_keys_fields_suppresses_schema_error_blockers():
    manager = FakeManager()

    list_replay_scope_keys(manager, mode=MODE_FIELDS)

    sql = manager.calls[0]["sql"]
    assert "NOT EXISTS" in sql
    assert "schema_error" in sql
    suppression = sql.split("NOT EXISTS", 1)[1]
    for endpoint in FIELDS_BLOCKING_ENDPOINTS:
        assert f"'{endpoint}'" in suppression
    assert "'lineups'" not in suppression
    assert "'referee_profile'" not in suppression
    for column in (
        "source_tournament_id",
        "source_season_id",
        "freshness_key",
    ):
        assert f'b."{column}" = m."{column}"' in sql
    assert "COALESCE(TRIM(b.\"raw_blob_key\"), '') <> ''" in sql


def test_list_replay_scope_keys_include_blocked_drops_suppression():
    manager = FakeManager()

    list_replay_scope_keys(manager, mode=MODE_FIELDS, include_blocked=True)

    assert "NOT EXISTS" not in manager.calls[0]["sql"]


def test_list_replay_scope_keys_catalog_binds_resolvable_pairs():
    manager = FakeManager([("17", "76986", "f1")])
    catalog = _fake_catalog()

    keys = list_replay_scope_keys(
        manager, mode=MODE_FIELDS, limit=2, catalog=catalog
    )

    sql = manager.calls[0]["sql"]
    params = manager.calls[0]["params"]
    assert (
        '(m."source_tournament_id", m."source_season_id") IN ((?, ?))' in sql
    )
    assert params == ("17", "76986")
    assert keys == (ReplayScopeKey("17", "76986", "f1"),)


def test_list_replay_scope_keys_empty_catalog_pairs_skip_sql():
    manager = FakeManager()

    class EmptyCatalog:
        tournaments = ()

    assert (
        list_replay_scope_keys(
            manager, mode=MODE_FIELDS, catalog=EmptyCatalog()
        )
        == ()
    )
    assert manager.calls == []


def test_list_replay_scope_keys_exact_scope_validates_against_catalog():
    manager = FakeManager([("17", "76986", FRESHNESS)])

    keys = list_replay_scope_keys(
        manager,
        mode=MODE_FIELDS,
        exact_scope=(TOURNAMENT, "76986"),
        limit=2,
        catalog=_fake_catalog(),
    )

    call = manager.calls[0]
    sql, params = call["sql"], call["params"]
    assert '(m."source_tournament_id", m."source_season_id") IN (' not in sql
    assert 'm."source_tournament_id" = ?' in sql
    assert 'm."source_season_id" = ?' in sql
    assert params == (TOURNAMENT, "76986")
    assert keys == (ReplayScopeKey(TOURNAMENT, "76986", FRESHNESS),)


@pytest.mark.parametrize(
    "pair,message",
    [
        (("8", "76986"), "unknown tournament"),
        (("17", "99999"), "unknown for the source tournament"),
        (("1", "76986"), "unknown for the source tournament"),
    ],
)
def test_list_replay_scope_keys_exact_scope_rejects_unresolvable(pair, message):
    manager = FakeManager()

    with pytest.raises(ReplayPlanningError, match=message):
        list_replay_scope_keys(
            manager,
            mode=MODE_FIELDS,
            exact_scope=pair,
            catalog=_fake_catalog(),
        )

    assert manager.calls == []


def test_list_replay_scope_keys_drain_sql_filters_and_params():
    manager = FakeManager([("17", "52372", FRESHNESS)])

    keys = list_replay_scope_keys(manager, mode=MODE_DRAIN, limit=3)

    call = manager.calls[0]
    sql = call["sql"]
    assert "\"status\" = 'retryable_failure'" in sql
    assert '"endpoint" IN (' not in sql
    assert "NOT EXISTS" not in sql
    assert '"http_status" <> 204' in sql
    assert "COALESCE(TRIM(\"raw_blob_key\"), '') <> ''" in sql
    assert "LIMIT 3" in sql
    assert call["params"] == ()
    assert keys == (ReplayScopeKey(TOURNAMENT, SEASON, FRESHNESS),)


def test_list_replay_scope_keys_binds_exact_scope_params():
    manager = FakeManager([("17", "52372", FRESHNESS)])

    keys = list_replay_scope_keys(
        manager, mode=MODE_DRAIN, exact_scope=(TOURNAMENT, SEASON), limit=2
    )

    call = manager.calls[0]
    assert call["params"] == (TOURNAMENT, SEASON)
    assert 'm."source_tournament_id" = ?' in call["sql"]
    assert 'm."source_season_id" = ?' in call["sql"]
    assert keys == (ReplayScopeKey(TOURNAMENT, SEASON, FRESHNESS),)


@pytest.mark.parametrize("limit", [0, -3, "2", 2.5, None])
def test_list_replay_scope_keys_validates_mode_and_limit_without_sql(limit):
    manager = FakeManager()

    with pytest.raises(ReplayPlanningError):
        list_replay_scope_keys(manager, mode="backfill")
    with pytest.raises(ReplayPlanningError):
        list_replay_scope_keys(manager, mode=MODE_FIELDS, limit=limit)

    assert manager.calls == []


def test_load_replay_scope_records_converts_rows_and_keeps_lineage():
    record = _record(endpoint="lineups", run_id="run-1")
    manager = FakeManager([_manifest_row(record)])

    loaded = load_replay_scope_records(
        manager, ReplayScopeKey(TOURNAMENT, SEASON, FRESHNESS), mode=MODE_FIELDS
    )

    call = manager.calls[0]
    assert call["fetch"] is True
    sql = call["sql"]
    assert f"FROM {REPLAY_MANIFEST_TABLE}" in sql
    assert '"source_tournament_id" = ?' in sql
    assert '"source_season_id" = ?' in sql
    assert '"freshness_key" = ?' in sql
    assert '"http_status" <> 204' in sql
    assert "COALESCE(TRIM(\"raw_content_hash\"), '') <> ''" in sql
    assert '"endpoint" IN (' in sql
    assert 'ORDER BY "target_type", "target_id", "endpoint"' in sql
    assert call["params"] == (TOURNAMENT, SEASON, FRESHNESS)
    assert isinstance(loaded, tuple)
    assert loaded == (record,)
    assert loaded[0].raw_content_hash == record.raw_content_hash
    assert loaded[0].raw_blob_key == record.raw_blob_key
    assert loaded[0].http_status == 200
    assert loaded[0].key == record.key


def test_load_replay_scope_records_drain_filters_status_not_endpoints():
    record = _record(
        endpoint="odds",
        status=ManifestStatus.RETRYABLE_FAILURE,
        http_status=200,
        row_count=0,
        attempts=2,
    )
    manager = FakeManager([_manifest_row(record)])

    loaded = load_replay_scope_records(
        manager, (TOURNAMENT, SEASON, FRESHNESS), mode=MODE_DRAIN
    )

    sql = manager.calls[0]["sql"]
    assert "\"status\" = 'retryable_failure'" in sql
    assert '"endpoint" IN (' not in sql
    assert manager.calls[0]["params"] == (TOURNAMENT, SEASON, FRESHNESS)
    assert loaded == (record,)


def test_load_replay_scope_records_validates_mode_and_scope_without_sql():
    manager = FakeManager()

    with pytest.raises(ReplayPlanningError):
        load_replay_scope_records(
            manager, (TOURNAMENT, SEASON, FRESHNESS), mode="backfill"
        )
    with pytest.raises(ReplayPlanningError):
        load_replay_scope_records(manager, (TOURNAMENT, SEASON), mode=MODE_FIELDS)

    assert manager.calls == []


def test_resolve_replay_scope_maps_canonical_league_and_season():
    catalog = _fake_catalog()

    resolved = resolve_replay_scope(
        ReplayScopeKey("17", "76986", FRESHNESS), catalog
    )

    assert canonical_league("ENG-Premier League", catalog.tournament_map()) == (
        "SS-17"
    )
    assert resolved == ResolvedReplayScope(
        source_tournament_id="17",
        source_season_id="76986",
        freshness_key=FRESHNESS,
        league="SS-17",
        canonical_season="2526",
        canonical_id="ENG-Premier League",
        season_format="split_year",
    )
    assert isinstance(resolved, tuple)


def test_resolve_replay_scope_unknown_season_fails():
    with pytest.raises(ReplayPlanningError):
        resolve_replay_scope(
            ReplayScopeKey("17", "99999", FRESHNESS), _fake_catalog()
        )


def test_resolve_replay_scope_unknown_tournament_fails():
    with pytest.raises(ReplayPlanningError):
        resolve_replay_scope(
            ReplayScopeKey("8", "76986", FRESHNESS), _fake_catalog()
        )


def test_resolve_replay_scope_requires_canonical_season():
    catalog = _fake_catalog()
    tournament = catalog.tournament(17)
    tournament.seasons = (
        SimpleNamespace(
            season_id=11111, canonical_season=None, season_format="split_year"
        ),
    )

    with pytest.raises(ReplayPlanningError):
        resolve_replay_scope(
            ReplayScopeKey("17", "11111", FRESHNESS), catalog
        )


@pytest.mark.parametrize("season_format", [None, "", "unknown", "split-year"])
def test_resolve_replay_scope_requires_replayable_season_format(season_format):
    catalog = _fake_catalog()
    catalog.tournament(17).seasons = (
        SimpleNamespace(
            season_id=76986,
            canonical_season="2526",
            season_format=season_format,
        ),
    )

    with pytest.raises(ReplayPlanningError, match="season format"):
        resolve_replay_scope(
            ReplayScopeKey("17", "76986", FRESHNESS), catalog
        )


@pytest.mark.parametrize(
    "scope",
    [("abc", "76986", FRESHNESS), ("17", "x1", FRESHNESS), ("17", "76986", "")],
)
def test_resolve_replay_scope_rejects_bad_scope(scope):
    with pytest.raises(ReplayPlanningError):
        resolve_replay_scope(scope, _fake_catalog())


def test_replay_eligible_scope_pairs_filters_and_sorts():
    catalog = _fake_catalog()

    pairs = replay_eligible_scope_pairs(catalog)

    assert pairs == (("17", "76986"),)


def test_replay_eligible_scope_pairs_requires_catalog_enumeration():
    class BareCatalog:
        def tournament(self, unique_tournament_id):
            raise AssertionError("must not be called")

        def tournament_map(self):
            return {}

    with pytest.raises(ReplayPlanningError, match="tournaments"):
        replay_eligible_scope_pairs(BareCatalog())


def test_replay_eligible_scope_pairs_skips_incomplete_seasons():
    catalog = _fake_catalog()
    catalog.tournament(17).seasons = (
        SimpleNamespace(season_id=1, canonical_season="2526"),
        SimpleNamespace(
            season_id=2, canonical_season=None, season_format="split_year"
        ),
        SimpleNamespace(
            season_id=3, canonical_season="2526", season_format="unknown"
        ),
    )

    assert replay_eligible_scope_pairs(catalog) == ()


def test_validate_replay_scope_pair_accepts_eligible_pair():
    assert validate_replay_scope_pair(("17", "76986"), _fake_catalog()) == (
        "17",
        "76986",
    )


def test_validate_replay_scope_pair_rejects_disabled_tournament():
    catalog = _fake_catalog()
    catalog.tournament(17).seasons = ()
    catalog.tournament(1).seasons = (
        SimpleNamespace(
            season_id=76986,
            canonical_season="2026",
            season_format="single_year",
        ),
    )

    with pytest.raises(ReplayPlanningError, match="capture-allowed"):
        validate_replay_scope_pair(("1", "76986"), catalog)
