"""Unit tests for the pure SofaScore raw-replay selection (#1367)."""

from __future__ import annotations

import pytest

from scrapers.sofascore.manifest import (
    EndpointManifest,
    ManifestKey,
    ManifestStatus,
)
from scrapers.sofascore.raw_replay import (
    FIELDS_ENDPOINTS,
    MODE_DRAIN,
    MODE_FIELDS,
    ReplayPlanningError,
    ReplayScopeKey,
    ReplayScopePlan,
    select_replay_scope_plans,
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
