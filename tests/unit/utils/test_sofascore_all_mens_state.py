from __future__ import annotations

import hashlib
import json

import pytest

from dags.utils.sofascore_all_mens_state import (
    CampaignPlanningError,
    SeasonTarget,
    campaign_scope_key,
    clear_failed,
    current_season_targets,
    env_int,
    mark_failed,
    plan_historical_batch,
    plan_refresh_batch,
    read_failures,
)
from scrapers.sofascore.workload_plan import (
    production_season_shape,
    season_workload_class,
)


def _season(tournament_id, start_year, status):
    return {
        "source_season_id": tournament_id * 100 + start_year % 100,
        "canonical_season": f"{start_year % 100}{(start_year + 1) % 100}",
        "start_year": start_year,
        "season_format": "split_year",
        "team_count": 20,
        "metadata_status": status,
        "team_count_evidence": {"count": 20, "endpoint": "/teams"},
    }


def _snapshot(*, second_status="ready", second_years=(2025, 2024)):
    tournaments = []
    for tournament_id, status, years in (
        (17, "ready", (2025, 2024)), (8, second_status, second_years)
    ):
        tournaments.append({
            "unique_tournament_id": tournament_id,
            "capture_key": f"SS-{tournament_id}",
            "metadata_status": status,
            "seasons": [_season(tournament_id, year, status) for year in years],
        })
    document = {
        "schema_version": 1,
        "candidate_count": 2,
        "policy_id": "test",
        "campaign_id": "campaign-test",
        "tournaments": tournaments,
    }
    document["snapshot_id"] = hashlib.sha256(json.dumps(
        document, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()
    return document


@pytest.mark.unit
def test_planner_is_breadth_first_newest_season():
    snapshot = _snapshot()
    planned = plan_historical_batch(snapshot, completed=set(), batch_size=10)

    assert [item["SOFASCORE_CANONICAL_SEASON"] for item in planned] == [
        "2526", "2526", "2425", "2425"
    ]
    # The gateway ledger keeps one immutable plan per run_id, so two scopes
    # of one DagRun must not share it; an Airflow retry keeps the same id.
    assert [item["SOFASCORE_SCOPE_RUN_ID"] for item in planned] == [
        "manual--8-825", "manual--17-1725", "manual--8-824", "manual--17-1724"
    ]
    assert "SOFASCORE_RATE_LIMIT_PER_MINUTE" not in planned[0]
    assert "SOFASCORE_PROXY_CONTROL_URL" not in planned[0]


@pytest.mark.unit
def test_each_tournaments_newest_season_precedes_deeper_seasons():
    # Tournament 17 has 2025 and 2024, tournament 8 only 2024: the newest
    # season of every tournament comes first, then the campaign goes deeper.
    snapshot = _snapshot(second_years=(2024,))

    planned = plan_historical_batch(snapshot, completed=set(), batch_size=10)

    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:17:1725", "campaign-test:8:824", "campaign-test:17:1724"
    ]


@pytest.mark.unit
@pytest.mark.parametrize("second_status", ["ready", "pending"])
def test_lane_env_is_forwarded_to_every_planned_task(second_status):
    snapshot = _snapshot(second_status=second_status)
    task_env = {
        "SOFASCORE_RATE_LIMIT_PER_MINUTE": "60",
        "SOFASCORE_PROXY_CONTROL_URL": "http://sofascore-gw-history:8080",
    }

    planned = plan_historical_batch(
        snapshot, completed=set(), batch_size=10, task_env=task_env
    )

    assert planned
    for item in planned:
        assert item["SOFASCORE_RATE_LIMIT_PER_MINUTE"] == "60"
        assert item["SOFASCORE_PROXY_CONTROL_URL"] == (
            "http://sofascore-gw-history:8080"
        )
        assert item["SOFASCORE_CAMPAIGN_SNAPSHOT"].endswith("snapshot.json")


@pytest.mark.unit
def test_completed_newest_wave_advances_to_previous_season():
    snapshot = _snapshot()
    campaign_id = snapshot["campaign_id"]
    completed = {
        campaign_scope_key(campaign_id, 17, 1725),
        campaign_scope_key(campaign_id, 8, 825),
    }

    planned = plan_historical_batch(snapshot, completed=completed, batch_size=10)

    assert {item["SOFASCORE_CANONICAL_SEASON"] for item in planned} == {"2425"}


@pytest.mark.unit
def test_pending_seasons_never_yield_a_metadata_task():
    # #1354: metadata is enriched by the refresh lane, so a pending season
    # is skipped; the plan holds only ready capture scopes.
    snapshot = _snapshot(second_status="pending")
    completed = {
        campaign_scope_key(snapshot["campaign_id"], 17, 1725),
    }

    planned = plan_historical_batch(snapshot, completed=completed, batch_size=10)

    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:17:1724"
    ]
    assert all(item["SOFASCORE_CAMPAIGN_ACTION"] == "capture" for item in planned)
    assert not any("SOFASCORE_METADATA_WAVE" in item for item in planned)


@pytest.mark.unit
def test_plan_is_empty_when_only_pending_seasons_remain():
    snapshot = _snapshot(second_status="pending")
    completed = {
        campaign_scope_key(snapshot["campaign_id"], 17, 1725),
        campaign_scope_key(snapshot["campaign_id"], 17, 1724),
    }

    planned = plan_historical_batch(snapshot, completed=completed, batch_size=10)

    assert planned == []


def test_unavailable_season_does_not_block_other_ready_scopes():
    snapshot = _snapshot()
    unavailable = snapshot["tournaments"][0]["seasons"][0]
    unavailable["metadata_status"] = "excluded"
    unavailable["team_count"] = None
    unavailable["team_count_evidence"] = {
        "type": "source_team_ids_unavailable",
        "endpoint": "/teams",
        "reason": "schema_error",
    }
    unsigned = dict(snapshot)
    unsigned.pop("snapshot_id")
    snapshot["snapshot_id"] = hashlib.sha256(json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()

    planned = plan_historical_batch(snapshot, completed=set(), batch_size=10)

    # 17-1724 is now the newest usable season of tournament 17 (depth 0).
    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:8:825", "campaign-test:17:1724", "campaign-test:8:824"
    ]


def test_planner_defers_shapes_the_static_policy_does_not_declare():
    snapshot = _snapshot()
    snapshot["tournaments"][0]["seasons"][0]["team_count"] = 24
    declared_class = season_workload_class(production_season_shape(
        season_format="split_year",
        team_count_band="16_20",
        max_pages_per_direction=50,
    ))
    unsigned = dict(snapshot)
    unsigned.pop("snapshot_id")
    snapshot["snapshot_id"] = hashlib.sha256(json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()

    planned = plan_historical_batch(
        snapshot,
        completed=set(),
        batch_size=10,
        authorized_season_classes=[declared_class],
    )

    # 17-1725's shape is undeclared, so it also holds the deeper 17-1724.
    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:8:825", "campaign-test:8:824"
    ]


def test_planner_does_not_advance_wave_when_only_deferred_shapes_remain():
    snapshot = _snapshot()
    for tournament in snapshot["tournaments"]:
        tournament["seasons"][1]["team_count"] = 24
    unsigned = dict(snapshot)
    unsigned.pop("snapshot_id")
    snapshot["snapshot_id"] = hashlib.sha256(json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()
    declared_class = season_workload_class(production_season_shape(
        season_format="split_year",
        team_count_band="16_20",
        max_pages_per_direction=50,
    ))
    completed = {
        campaign_scope_key(snapshot["campaign_id"], 17, 1725),
        campaign_scope_key(snapshot["campaign_id"], 8, 825),
    }

    planned = plan_historical_batch(
        snapshot,
        completed=completed,
        batch_size=10,
        authorized_season_classes=[declared_class],
    )

    assert planned == []


def test_completed_keys_survive_a_new_metadata_snapshot_revision():
    snapshot = _snapshot()
    campaign_id = snapshot["campaign_id"]
    completed = {
        campaign_scope_key(campaign_id, 17, 1725),
        campaign_scope_key(campaign_id, 8, 825),
    }
    old_snapshot_id = snapshot["snapshot_id"]
    snapshot["metadata_revision_note"] = "2024 wave enriched"
    unsigned = dict(snapshot)
    unsigned.pop("snapshot_id")
    snapshot["snapshot_id"] = hashlib.sha256(json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()

    planned = plan_historical_batch(snapshot, completed=completed, batch_size=10)

    assert snapshot["snapshot_id"] != old_snapshot_id
    assert {item["SOFASCORE_CANONICAL_SEASON"] for item in planned} == {"2425"}


@pytest.mark.unit
def test_scope_parks_after_max_attempts_and_its_tournament_waits(tmp_path):
    snapshot = _snapshot()
    campaign_id = snapshot["campaign_id"]
    failures_path = tmp_path / "failures.json"
    head = campaign_scope_key(campaign_id, 8, 825)

    def _plan():
        return [item["SOFASCORE_SCOPE_KEY"] for item in plan_historical_batch(
            snapshot,
            completed=set(),
            batch_size=10,
            failures=read_failures(failures_path, campaign_id=campaign_id),
            max_scope_attempts=3,
        )]

    assert _plan()[0] == head
    for run_id in ("run-1", "run-2"):
        mark_failed(
            failures_path, campaign_id=campaign_id, scope_key=head, run_id=run_id
        )
    assert _plan()[0] == head
    mark_failed(
        failures_path, campaign_id=campaign_id, scope_key=head, run_id="run-3"
    )

    # Parked: the head of the queue no longer retries forever, the next
    # scope in rank order runs; the parked scope holds its deeper season.
    assert _plan() == [
        campaign_scope_key(campaign_id, 17, 1725),
        campaign_scope_key(campaign_id, 17, 1724),
    ]
    attempts = read_failures(failures_path, campaign_id=campaign_id)
    assert attempts[head]["count"] == 3
    assert attempts[head]["last_run_id"] == "run-3"
    assert attempts[head]["last_at"].endswith("+00:00")
    document = json.loads(failures_path.read_text())
    assert document["schema_version"] == 1
    assert document["campaign_id"] == campaign_id

    clear_failed(failures_path, campaign_id=campaign_id, scope_key=head)

    assert head not in read_failures(failures_path, campaign_id=campaign_id)
    assert _plan()[0] == head


@pytest.mark.unit
def test_a_parked_scope_is_retried_once_the_cooldown_has_passed(tmp_path):
    """A park is a pause, not a grave.

    Nothing but a validated success of the parked scope itself ever cleared a
    park, and a parked scope is never planned — so the success could not
    happen and the whole OLDER history of that tournament, which waits behind
    the blocker on purpose, was buried for good.  Seven tournaments were
    already in that state in the live campaign.
    """

    from datetime import datetime, timedelta, timezone

    snapshot = _snapshot()
    campaign_id = snapshot["campaign_id"]
    failures_path = tmp_path / "failures.json"
    head = campaign_scope_key(campaign_id, 8, 825)

    def _plan(moment=None):
        return [item["SOFASCORE_SCOPE_KEY"] for item in plan_historical_batch(
            snapshot,
            completed=set(),
            batch_size=10,
            failures=read_failures(failures_path, campaign_id=campaign_id),
            max_scope_attempts=3,
            park_cooldown_hours=24,
            moment=moment,
        )]

    for run_id in ("run-1", "run-2", "run-3"):
        mark_failed(
            failures_path, campaign_id=campaign_id, scope_key=head, run_id=run_id
        )
    # Parked right now: the tournament still waits behind it.
    assert head not in _plan()

    later = datetime.now(timezone.utc) + timedelta(hours=25)
    assert _plan(moment=later)[0] == head
    # And the deeper season of that tournament is reachable again with it.
    assert campaign_scope_key(campaign_id, 8, 824) in _plan(moment=later)

    # Inside the window it stays parked, so a broken scope cannot loop on
    # paid traffic.
    assert head not in _plan(moment=datetime.now(timezone.utc) + timedelta(hours=1))


def _refresh_snapshot():
    snapshot = _snapshot()
    # Lane F: the current (pending, unmeasured) season of every tournament
    # sits on top of the measured 2025 seasons.
    for tournament in snapshot["tournaments"]:
        tournament_id = tournament["unique_tournament_id"]
        tournament["seasons"].insert(0, {
            "source_season_id": tournament_id * 100 + 26,
            "canonical_season": "2627",
            "start_year": 2026,
            "season_format": "split_year",
            "team_count": None,
            "metadata_status": "pending",
        })
    unsigned = dict(snapshot)
    unsigned.pop("snapshot_id")
    snapshot["snapshot_id"] = hashlib.sha256(json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()
    return snapshot


def _refresh_pending(league, season, count, timestamp=1_787_788_800):
    """One Bronze pending row: ``timestamp`` is the nearest open deadline (#1359)."""

    return (league, season, count, timestamp)


@pytest.mark.unit
def test_refresh_planner_skips_tournament_with_null_seasons():
    snapshot = _refresh_snapshot()
    snapshot["tournaments"][0]["seasons"] = None
    unsigned = dict(snapshot)
    unsigned.pop("snapshot_id")
    snapshot["snapshot_id"] = hashlib.sha256(json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()

    planned = plan_refresh_batch(
        snapshot,
        [_refresh_pending("SS-17", "2627", 1), _refresh_pending("SS-8", "2526", 10)],
        batch_size=2, queue_mode="deadline",
    )

    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:8:825"
    ]


@pytest.mark.unit
def test_current_season_targets_reject_a_boolean_start_year():
    snapshot = _refresh_snapshot()
    tournament = snapshot["tournaments"][0]
    tournament["seasons"] = [
        {
            "source_season_id": 171,
            "canonical_season": "invalid-boolean",
            "start_year": True,
            "metadata_status": "pending",
        },
        {
            "source_season_id": 170,
            "canonical_season": "valid-integer",
            "start_year": 0,
            "metadata_status": "pending",
        },
    ]

    targets = current_season_targets(snapshot, frozenset())

    assert next(target for target in targets if target.tournament_id == 17) == (
        SeasonTarget(
            tournament_id=17,
            season_id=170,
            league="SS-17",
            canonical_season="valid-integer",
        )
    )


@pytest.mark.unit
def test_current_season_targets_keep_the_first_listed_newest_year_tie():
    snapshot = _refresh_snapshot()
    tournament = snapshot["tournaments"][0]
    tournament["seasons"] = [
        {
            "source_season_id": 171,
            "canonical_season": "first-2627",
            "start_year": 2026,
            "metadata_status": "pending",
        },
        {
            "source_season_id": 172,
            "canonical_season": "second-2026",
            "start_year": 2026,
            "metadata_status": "pending",
        },
    ]

    targets = current_season_targets(snapshot, frozenset())

    assert next(target for target in targets if target.tournament_id == 17) == (
        SeasonTarget(
            tournament_id=17,
            season_id=171,
            league="SS-17",
            canonical_season="first-2627",
        )
    )


@pytest.mark.unit
def test_refresh_planner_skips_malformed_snapshot_records(caplog):
    snapshot = _refresh_snapshot()
    snapshot["tournaments"][0]["seasons"] = [
        None,
        "not-a-season",
        {"source_season_id": "not-an-id", "canonical_season": "bad", "start_year": 2027},
        {"source_season_id": 179, "start_year": 2027},
        snapshot["tournaments"][0]["seasons"][0],
    ]
    snapshot["tournaments"].extend([
        None,
        "not-a-tournament",
        {
            "metadata_status": "ready",
            "unique_tournament_id": "not-an-id",
            "capture_key": "SS-bad",
            "seasons": [],
        },
        {
            "metadata_status": "ready",
            "unique_tournament_id": 19,
            "seasons": [],
        },
        {
            "metadata_status": "ready",
            "unique_tournament_id": 20,
            "capture_key": "SS-20",
            "seasons": {"not": "a list"},
        },
    ])
    unsigned = dict(snapshot)
    unsigned.pop("snapshot_id")
    snapshot["snapshot_id"] = hashlib.sha256(json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()

    planned = plan_refresh_batch(
        snapshot,
        [
            _refresh_pending("SS-17", "2627", 1),
            _refresh_pending("SS-8", "2627", 2, 1_787_788_000),
        ],
        batch_size=2, queue_mode="deadline",
    )

    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:8:826", "campaign-test:17:1726"
    ]
    assert "invalid refresh snapshot" in caplog.text


@pytest.mark.unit
def test_refresh_planner_orders_open_deadlines_nearest_first_with_stable_ties():
    snapshot = _refresh_snapshot()
    tournament = snapshot["tournaments"][0]
    tournament["seasons"].append({
        "source_season_id": 1724,
        "canonical_season": "2425",
        "start_year": 2024,
        "metadata_status": "ready",
    })
    tournament["seasons"].append({
        "source_season_id": 1723,
        "canonical_season": "2324",
        "start_year": 2023,
        "metadata_status": "ready",
    })
    unsigned = dict(snapshot)
    unsigned.pop("snapshot_id")
    snapshot["snapshot_id"] = hashlib.sha256(json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()

    planned = plan_refresh_batch(
        snapshot,
        [
            _refresh_pending("SS-8", "2526", 5, 1_787_356_800),
            _refresh_pending("SS-17", "2627", 7, 1_787_814_000),
            _refresh_pending("SS-17", "2425", 5, 1_787_529_600),
            _refresh_pending("SS-17", "2324", 5, 1_787_529_600),
            _refresh_pending("SS-8", "2627", 10, 1_787_817_600),
            _refresh_pending("SS-17", "2526", 9, 1_787_702_400),
        ],
        batch_size=8, queue_mode="deadline",
    )

    # #1359: the nearest open deadline first, whatever the season or size.
    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:8:825",    # nearest deadline
        "campaign-test:17:1723",  # equal deadline ties: league then season
        "campaign-test:17:1724",
        "campaign-test:17:1725",
        "campaign-test:17:1726",
        "campaign-test:8:826",    # farthest deadline
    ]


@pytest.mark.unit
def test_refresh_planner_open_deadline_small_partition_goes_before_the_debt():
    snapshot = _refresh_snapshot()
    pending = [
        _refresh_pending("SS-17", "2627", 1, 1_787_821_200),
        _refresh_pending("SS-8", "2526", 10, None),
    ]

    planned = plan_refresh_batch(
        snapshot, pending, batch_size=2, queue_mode="deadline"
    )

    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:17:1726",  # open deadline, even with fewer pending matches
        "campaign-test:8:825",     # debt: no deadline left
    ]


@pytest.mark.unit
def test_refresh_planner_prioritizes_an_explicit_calendar_year_current_partition():
    snapshot = _refresh_snapshot()
    current = snapshot["tournaments"][0]["seasons"][0]
    current.update({
        "canonical_season": "2026",
        "start_year": 2026,
        "season_format": "single_year",
        "source_season_id": 1726,
    })
    unsigned = dict(snapshot)
    unsigned.pop("snapshot_id")
    snapshot["snapshot_id"] = hashlib.sha256(json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()
    pending = [
        _refresh_pending("SS-17", "2026", 1, 1_787_821_200),
        _refresh_pending("SS-8", "2526", 10, None),
    ]

    planned = plan_refresh_batch(
        snapshot, pending, batch_size=2, queue_mode="deadline"
    )

    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:17:1726",  # calendar year season with an open deadline
        "campaign-test:8:825",     # older split year debt
    ]


@pytest.mark.unit
def test_refresh_planner_backlog_preserves_largest_old_partition_first():
    snapshot = _refresh_snapshot()
    pending = [
        _refresh_pending("SS-17", "2627", 1, 1_787_821_200),
        _refresh_pending("SS-17", "2526", 3, None),
        _refresh_pending("SS-8", "2526", 10, None),
    ]

    planned = plan_refresh_batch(
        snapshot, pending, batch_size=3, queue_mode="backlog"
    )

    # The manual backlog mode drains the debt alone, the biggest first.
    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:8:825",
        "campaign-test:17:1725",
    ]


@pytest.mark.unit
def test_refresh_planner_puts_the_debt_last_and_breaks_deadline_ties_stably():
    snapshot = _refresh_snapshot()
    snapshot["tournaments"][0]["seasons"].extend([
        {
            "source_season_id": 1724,
            "canonical_season": "2425",
            "start_year": 2024,
            "metadata_status": "ready",
        },
        {
            "source_season_id": 1723,
            "canonical_season": "2324",
            "start_year": 2023,
            "metadata_status": "ready",
        },
    ])
    unsigned = dict(snapshot)
    unsigned.pop("snapshot_id")
    snapshot["snapshot_id"] = hashlib.sha256(json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()

    planned = plan_refresh_batch(
        snapshot,
        [
            _refresh_pending("SS-17", "2526", 99, None),
            _refresh_pending("SS-17", "2324", 5, 1_787_738_400),
            _refresh_pending("SS-17", "2425", 5, 1_787_738_400),
            _refresh_pending("SS-17", "2627", 1, 1_787_824_800),
            _refresh_pending("SS-8", "2526", 6, 1_787_738_400),
            _refresh_pending("SS-8", "2627", 4, 1_787_738_400),
        ],
        batch_size=8,
        queue_mode="deadline",
    )

    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:17:1723",  # nearest deadline: league then canonical season
        "campaign-test:17:1724",
        "campaign-test:8:825",
        "campaign-test:8:826",
        "campaign-test:17:1726",  # later deadline
        "campaign-test:17:1725",  # no open deadline: debt, always last
    ]


@pytest.mark.unit
def test_refresh_planner_filters_poisoned_rows_before_timestamp_normalization():
    snapshot = _refresh_snapshot()
    snapshot["tournaments"][0]["seasons"][1]["metadata_status"] = "excluded"
    snapshot["tournaments"][0]["seasons"].append({
        "source_season_id": 1724,
        "canonical_season": "2425",
        "start_year": 2024,
        "metadata_status": "ready",
    })
    unsigned = dict(snapshot)
    unsigned.pop("snapshot_id")
    snapshot["snapshot_id"] = hashlib.sha256(json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()

    planned = plan_refresh_batch(
        snapshot,
        [
            _refresh_pending("SS-8", "2627", 50, object()),
            _refresh_pending("SS-999", "2627", 40, object()),
            _refresh_pending("SS-17", "2526", 30, object()),
            _refresh_pending("SS-17", "2627", 1, 1_787_824_800),
            _refresh_pending("SS-17", "2425", 99, "not-a-timestamp"),
        ],
        batch_size=8,
        queue_mode="deadline",
        exclude_tournament_ids={8},
    )

    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:17:1726",
        "campaign-test:17:1724",
    ]


@pytest.mark.unit
def test_refresh_planner_requires_an_explicit_queue_mode():
    snapshot = _refresh_snapshot()

    with pytest.raises(TypeError, match="queue_mode"):
        plan_refresh_batch(snapshot, [_refresh_pending("SS-17", "2627", 1)])

    with pytest.raises(CampaignPlanningError, match="queue_mode"):
        plan_refresh_batch(
            snapshot,
            [_refresh_pending("SS-17", "2627", 1)],
            queue_mode="current",
        )


@pytest.mark.unit
def test_refresh_planner_ranks_partitions_by_deadline_and_bounds_batch():
    snapshot = _refresh_snapshot()
    pending = [
        _refresh_pending("SS-8", "2526", 3, 1_787_788_900),
        _refresh_pending("SS-17", "2627", 10, 1_787_788_800),
        _refresh_pending("SS-17", "2526", 1, None),
    ]

    planned = plan_refresh_batch(
        snapshot, pending, batch_size=2, queue_mode="deadline", dag_run_id="scheduled__1",
        task_env={"SOFASCORE_PROXY_CONTROL_URL": "http://gw:8080"},
    )

    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:17:1726", "campaign-test:8:825"
    ]
    head = planned[0]
    assert head["SOFASCORE_CAMPAIGN_ACTION"] == "refresh"
    assert head["SOFASCORE_TOURNAMENT_ID"] == "17"
    assert head["SOFASCORE_SOURCE_SEASON_ID"] == "1726"
    assert head["SOFASCORE_CANONICAL_SEASON"] == "2627"
    assert head["SOFASCORE_EXPECTED_SNAPSHOT_ID"] == snapshot["snapshot_id"]
    assert head["SOFASCORE_EXPECTED_CAMPAIGN_ID"] == "campaign-test"
    # One immutable gateway plan per run_id: scopes of one DagRun differ.
    assert head["SOFASCORE_SCOPE_RUN_ID"] == "scheduled__1--17-1726"
    assert planned[1]["SOFASCORE_SCOPE_RUN_ID"] == "scheduled__1--8-825"
    assert head["SOFASCORE_SCOPE_RESULT_PATH"].startswith(
        "/opt/airflow/runtime/sofascore/all-men/refresh-results/"
    )
    assert head["SOFASCORE_SCOPE_OUTPUT_DIR"] != planned[1]["SOFASCORE_SCOPE_OUTPUT_DIR"]
    assert head["SOFASCORE_PROXY_CONTROL_URL"] == "http://gw:8080"
    assert head["SOFASCORE_WORKLOAD_ARTIFACT"].endswith("proxy_budget_canary.json")


@pytest.mark.unit
def test_refresh_planner_skips_configured_unknown_and_excluded_partitions():
    snapshot = _refresh_snapshot()
    excluded = snapshot["tournaments"][0]["seasons"][1]  # 17 / 2526
    excluded["metadata_status"] = "excluded"
    unsigned = dict(snapshot)
    unsigned.pop("snapshot_id")
    snapshot["snapshot_id"] = hashlib.sha256(json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()
    pending = [
        _refresh_pending("SS-8", "2627", 9),      # configured league: the daily ingest owns it
        _refresh_pending("SS-999", "2627", 8),    # not in the snapshot
        _refresh_pending("SS-17", "1999", 7),     # season the snapshot does not know
        _refresh_pending("SS-17", "2526", 6),     # excluded season: the scope cycle refuses it
        _refresh_pending("ENG-Premier League", "2627", 5),
        _refresh_pending("SS-17", "2627", 1),
    ]

    planned = plan_refresh_batch(
        snapshot, pending, batch_size=8, queue_mode="deadline", exclude_tournament_ids={8}
    )

    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:17:1726"
    ]


@pytest.mark.unit
def test_refresh_planner_rejects_a_stale_snapshot_and_bad_batch_size():
    snapshot = _refresh_snapshot()

    with pytest.raises(CampaignPlanningError, match="batch_size"):
        plan_refresh_batch(snapshot, [], batch_size=0, queue_mode="deadline")
    snapshot["snapshot_id"] = "0" * 64
    with pytest.raises(CampaignPlanningError, match="digest"):
        plan_refresh_batch(
            snapshot, [_refresh_pending("SS-17", "2627", 1)], queue_mode="deadline"
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("raw", "expected"), [("", 8), ("3", 3)]
)
def test_env_int_reads_a_bounded_integer_knob(monkeypatch, raw, expected):
    monkeypatch.setenv("SOFASCORE_TEST_KNOB", raw)

    assert env_int("SOFASCORE_TEST_KNOB", 8, 1, 64) == expected


@pytest.mark.unit
@pytest.mark.parametrize("raw", ["0", "65", "eight"])
def test_env_int_fails_closed_on_an_invalid_knob(monkeypatch, raw):
    monkeypatch.setenv("SOFASCORE_TEST_KNOB", raw)

    with pytest.raises(ValueError, match="SOFASCORE_TEST_KNOB"):
        env_int("SOFASCORE_TEST_KNOB", 8, 1, 64)


def _quarantine_plan(snapshot, failures_path, *, release="aaaaaaaa", moment=None):
    return [item["SOFASCORE_SCOPE_KEY"] for item in plan_historical_batch(
        snapshot,
        completed=set(),
        batch_size=10,
        failures=read_failures(failures_path, campaign_id=snapshot["campaign_id"]),
        max_scope_attempts=3,
        park_cooldown_hours=24,
        moment=moment,
        release=release,
    )]


def _fail(failures_path, campaign_id, scope_key, run_id, *, reason, source_requests,
          release="aaaaaaaa"):
    mark_failed(
        failures_path,
        campaign_id=campaign_id,
        scope_key=scope_key,
        run_id=run_id,
        reason=reason,
        source_requests=source_requests,
        release=release,
    )


SCHEMA_REASON = (
    "season: capture_engine: endpoint standings_total is nonterminal: schema_error"
)


@pytest.mark.unit
def test_three_free_identical_failures_quarantine_the_scope_not_its_tournament(
    tmp_path,
):
    """#1351: 3 red attempts in a row at 0 source requests with the same reason
    are a deterministic dead end (291:84027 — 25 attempts). The scope is
    quarantined: never planned again on this release, no cooldown, and the
    deeper seasons of its tournament do NOT wait behind it."""
    from datetime import datetime, timedelta, timezone

    snapshot = _snapshot()
    campaign_id = snapshot["campaign_id"]
    failures_path = tmp_path / "failures.json"
    head = campaign_scope_key(campaign_id, 8, 825)
    deeper = campaign_scope_key(campaign_id, 8, 824)

    for run_id in ("run-1", "run-2", "run-3"):
        _fail(failures_path, campaign_id, head, run_id,
              reason=SCHEMA_REASON, source_requests=0)

    record = read_failures(failures_path, campaign_id=campaign_id)[head]
    assert record["count"] == 3
    assert record["streak_no_traffic"] == 3
    assert record["last_reason"] == SCHEMA_REASON
    assert record["last_release"] == "aaaaaaaa"
    planned = _quarantine_plan(snapshot, failures_path)
    assert head not in planned
    assert deeper in planned
    # Terminal, unlike a park: a cooldown does not bring it back.
    later = datetime.now(timezone.utc) + timedelta(hours=72)
    assert head not in _quarantine_plan(snapshot, failures_path, moment=later)


@pytest.mark.unit
def test_three_paid_failures_still_park_as_before(tmp_path):
    snapshot = _snapshot()
    campaign_id = snapshot["campaign_id"]
    failures_path = tmp_path / "failures.json"
    head = campaign_scope_key(campaign_id, 8, 825)

    for run_id in ("run-1", "run-2", "run-3"):
        _fail(failures_path, campaign_id, head, run_id,
              reason=SCHEMA_REASON, source_requests=4)

    assert read_failures(
        failures_path, campaign_id=campaign_id
    )[head]["streak_no_traffic"] == 0
    planned = _quarantine_plan(snapshot, failures_path)
    # Parked: the scope and, on purpose, its tournament's deeper season wait.
    assert head not in planned
    assert campaign_scope_key(campaign_id, 8, 824) not in planned


@pytest.mark.unit
def test_a_changed_reason_restarts_the_no_traffic_streak(tmp_path):
    snapshot = _snapshot()
    campaign_id = snapshot["campaign_id"]
    failures_path = tmp_path / "failures.json"
    head = campaign_scope_key(campaign_id, 8, 825)

    _fail(failures_path, campaign_id, head, "run-1",
          reason=SCHEMA_REASON, source_requests=0)
    _fail(failures_path, campaign_id, head, "run-2",
          reason=SCHEMA_REASON, source_requests=0)
    _fail(failures_path, campaign_id, head, "run-3",
          reason="season: capture_engine: something else", source_requests=0)

    record = read_failures(failures_path, campaign_id=campaign_id)[head]
    assert record["streak_no_traffic"] == 1
    # count 3 -> parked (tournament waits), not quarantined.
    assert campaign_scope_key(campaign_id, 8, 824) not in _quarantine_plan(
        snapshot, failures_path
    )


@pytest.mark.unit
def test_a_new_release_lifts_the_quarantine_for_one_attempt(tmp_path):
    snapshot = _snapshot()
    campaign_id = snapshot["campaign_id"]
    failures_path = tmp_path / "failures.json"
    head = campaign_scope_key(campaign_id, 8, 825)
    for run_id in ("run-1", "run-2", "run-3"):
        _fail(failures_path, campaign_id, head, run_id,
              reason=SCHEMA_REASON, source_requests=0)
    assert head not in _quarantine_plan(snapshot, failures_path)

    # A new release may carry the parser fix: one free attempt, even though
    # count >= max_scope_attempts would otherwise park it.
    assert head in _quarantine_plan(snapshot, failures_path, release="bbbbbbbb")

    # The same outcome at 0 traffic on the new release -> quarantined again.
    _fail(failures_path, campaign_id, head, "run-4",
          reason=SCHEMA_REASON, source_requests=0, release="bbbbbbbb")
    assert head not in _quarantine_plan(
        snapshot, failures_path, release="bbbbbbbb"
    )
    # A manual lift is `clear_failed` (runbook: remove the entry).
    clear_failed(failures_path, campaign_id=campaign_id, scope_key=head)
    assert _quarantine_plan(snapshot, failures_path, release="bbbbbbbb")[0] == head


@pytest.mark.unit
def test_old_failure_records_without_quarantine_fields_still_read(tmp_path):
    snapshot = _snapshot()
    campaign_id = snapshot["campaign_id"]
    failures_path = tmp_path / "failures.json"
    head = campaign_scope_key(campaign_id, 8, 825)
    failures_path.write_text(json.dumps({
        "schema_version": 1,
        "campaign_id": campaign_id,
        "attempts": {head: {
            "count": 3, "last_run_id": "old", "last_at": "2099-01-01T00:00:00+00:00",
        }},
    }))

    # Parked (count 3, not cooled), not quarantined: the tournament waits.
    planned = _quarantine_plan(snapshot, failures_path)
    assert head not in planned
    assert campaign_scope_key(campaign_id, 8, 824) not in planned
    _fail(failures_path, campaign_id, head, "run-4",
          reason=SCHEMA_REASON, source_requests=0)
    record = read_failures(failures_path, campaign_id=campaign_id)[head]
    assert record["count"] == 4
    assert record["streak_no_traffic"] == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    ("root", "expected"),
    [
        ("/opt/sofascore/releases/release-1a3d9890", "1a3d9890"),
        ("/opt/sofascore/releases/release-1a3d9890/", "1a3d9890"),
        ("", "unknown"),
        (None, "unknown"),
    ],
)
def test_current_release_is_the_sha8_of_the_release_root(monkeypatch, root, expected):
    from dags.utils.sofascore_all_mens_state import current_release

    if root is None:
        monkeypatch.delenv("SOFASCORE_RELEASE_ROOT", raising=False)
    else:
        monkeypatch.setenv("SOFASCORE_RELEASE_ROOT", root)
    assert current_release() == expected


def _completed_plan(snapshot, failures_path, completed, *, release):
    return [item["SOFASCORE_SCOPE_KEY"] for item in plan_historical_batch(
        snapshot,
        completed=completed,
        batch_size=10,
        failures=read_failures(failures_path, campaign_id=snapshot["campaign_id"]),
        max_scope_attempts=3,
        release=release,
    )]


@pytest.mark.unit
def test_completed_scope_with_rejects_replays_once_per_new_release(tmp_path):
    """#1352 (Astra r3): a green scope that journaled rejects is completed,
    not lost: a NEW release replays it once (raw only, the fix may be in it);
    the same release never replans it — not every cycle."""
    from dags.utils.sofascore_all_mens_state import (
        mark_completed_rejects,
        read_scope_rejects,
    )

    snapshot = _snapshot()
    campaign_id = snapshot["campaign_id"]
    failures_path = tmp_path / "failures.json"
    head = campaign_scope_key(campaign_id, 8, 825)
    completed = {head}
    result = tmp_path / "scope.json"
    result.write_text(json.dumps({"phases": [
        {"phase": "season"}, {"phase": "matches", "rejected_endpoints": 5},
    ]}))
    assert read_scope_rejects(result) == 5
    assert read_scope_rejects(tmp_path / "missing.json") == 0

    mark_completed_rejects(
        failures_path, campaign_id=campaign_id, scope_key=head,
        rejected_endpoints=5, run_id="run-1", release="aaaaaaaa",
    )
    record = read_failures(failures_path, campaign_id=campaign_id)[head]
    assert record["completed_rejected_endpoints"] == 5
    assert record["streak_no_traffic"] == 0

    # Same release: completed, and its tournament advances past it.
    same = _completed_plan(snapshot, failures_path, completed, release="aaaaaaaa")
    assert head not in same
    assert campaign_scope_key(campaign_id, 8, 824) in same
    # New release: one replay.
    assert head in _completed_plan(
        snapshot, failures_path, completed, release="bbbbbbbb"
    )

    # The replay fails on the new release: the marker survives, the scope
    # waits for the next release instead of retrying every cycle.
    _fail(failures_path, campaign_id, head, "run-2",
          reason="matches: boom", source_requests=0, release="bbbbbbbb")
    assert read_failures(
        failures_path, campaign_id=campaign_id
    )[head]["completed_rejected_endpoints"] == 5
    assert head not in _completed_plan(
        snapshot, failures_path, completed, release="bbbbbbbb"
    )
    assert head in _completed_plan(
        snapshot, failures_path, completed, release="cccccccc"
    )

    # A green replay without rejects clears the memory (validation clears,
    # 0 rejects writes nothing): completed for good.
    clear_failed(failures_path, campaign_id=campaign_id, scope_key=head)
    mark_completed_rejects(
        failures_path, campaign_id=campaign_id, scope_key=head,
        rejected_endpoints=0, run_id="run-3", release="cccccccc",
    )
    assert head not in read_failures(failures_path, campaign_id=campaign_id)
    assert head not in _completed_plan(
        snapshot, failures_path, completed, release="dddddddd"
    )


@pytest.mark.unit
def test_completed_scope_without_rejects_is_never_replanned(tmp_path):
    snapshot = _snapshot()
    campaign_id = snapshot["campaign_id"]
    head = campaign_scope_key(campaign_id, 8, 825)

    assert head not in _completed_plan(
        snapshot, tmp_path / "failures.json", {head}, release="bbbbbbbb"
    )


# --- #1353: the denominator file filters and orders both queues ----------


def _denominator(**classes):
    """A denominator with the given ``{"t<id>": class}`` rows."""

    from scrapers.sofascore.denominator import (
        CLASS_PRIORITY,
        Denominator,
        DenominatorRow,
    )

    rows = {}
    for name, tournament_class in classes.items():
        tournament_id = int(name[1:])
        rows[tournament_id] = DenominatorRow(
            tournament_id=tournament_id,
            capture_key=f"SS-{tournament_id}",
            name=name,
            tournament_class=tournament_class,
            queue_priority=CLASS_PRIORITY[tournament_class],
            basis="test",
        )
    return Denominator(rows=rows)


@pytest.mark.unit
def test_esoccer_tournament_is_never_planned_even_with_an_empty_core_queue():
    snapshot = _snapshot()
    campaign_id = snapshot["campaign_id"]
    # All core work is done: only the esoccer tournament has open scopes.
    completed = {
        campaign_scope_key(campaign_id, 17, 1725),
        campaign_scope_key(campaign_id, 17, 1724),
    }

    planned = plan_historical_batch(
        snapshot, completed=completed, batch_size=10,
        denominator=_denominator(t17="core", t8="esoccer"),
    )

    assert planned == []


@pytest.mark.unit
@pytest.mark.parametrize("disputed", ["amateur", "show", "youth", "unknown"])
def test_disputed_bucket_is_planned_only_after_every_core_depth(disputed):
    snapshot = _snapshot()

    planned = plan_historical_batch(
        snapshot, completed=set(), batch_size=10,
        denominator=_denominator(t17="core", t8=disputed),
    )

    # Core 17 at depth 0 AND depth 1 precede the disputed 8 at depth 0.
    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:17:1725", "campaign-test:17:1724",
        "campaign-test:8:825", "campaign-test:8:824",
    ]


@pytest.mark.unit
def test_tournament_missing_from_the_file_is_queued_last_with_a_warning(caplog):
    snapshot = _snapshot()

    planned = plan_historical_batch(
        snapshot, completed=set(), batch_size=10,
        denominator=_denominator(t8="core"),
    )

    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:8:825", "campaign-test:8:824",
        "campaign-test:17:1725", "campaign-test:17:1724",
    ]
    assert "tournament 17 is not in the denominator file" in caplog.text


@pytest.mark.unit
@pytest.mark.parametrize("queue_mode", ["deadline", "backlog"])
def test_refresh_planner_skips_student_and_ranks_core_before_disputed(queue_mode):
    snapshot = _refresh_snapshot()
    # The disputed tournament has the nearer deadline AND the bigger debt.
    open_mode = queue_mode == "deadline"
    pending = [
        _refresh_pending("SS-17", "2627", 1, 1_787_900_000 if open_mode else None),
        _refresh_pending("SS-8", "2627", 50, 1_787_000_000 if open_mode else None),
    ]

    planned = plan_refresh_batch(
        snapshot, pending, batch_size=2, queue_mode=queue_mode,
        denominator=_denominator(t17="core", t8="amateur"),
    )
    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:17:1726", "campaign-test:8:826",
    ]

    planned = plan_refresh_batch(
        snapshot, pending, batch_size=2, queue_mode=queue_mode,
        denominator=_denominator(t17="core", t8="student"),
    )
    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:17:1726",
    ]


@pytest.mark.unit
def test_current_season_targets_skip_esoccer_tournaments():
    snapshot = _refresh_snapshot()

    targets = current_season_targets(
        snapshot, frozenset(), denominator=_denominator(t17="esoccer", t8="core")
    )

    assert [target.tournament_id for target in targets] == [8]


def _window_plan(pending, **kwargs):
    kwargs.setdefault("queue_mode", "deadline")
    return plan_refresh_batch(
        _refresh_snapshot(), pending, batch_size=kwargs.pop("batch_size", 8),
        scope_budget_s=kwargs.pop("scope_budget_s", 7200), **kwargs,
    )


@pytest.mark.unit
def test_refresh_window_takes_every_small_scope_that_fits():
    # #1358: four scopes of 10 matches are 4 x (250 + 180) s of a 2 h window.
    planned = _window_plan([
        _refresh_pending("SS-17", "2627", 10, 1_787_900_004),
        _refresh_pending("SS-8", "2627", 10, 1_787_900_003),
        _refresh_pending("SS-17", "2526", 10, 1_787_900_002),
        _refresh_pending("SS-8", "2526", 10, 1_787_900_001),
    ])

    assert len(planned) == 4
    for env in planned:
        assert env["SOFASCORE_SCOPE_MAX_MATCHES"] == "10"
        assert env["SOFASCORE_SCOPE_BYTE_CAP"] == str(10 * 49_342)
        assert env["SOFASCORE_SCOPE_ESTIMATE_S"] == str(10 * 25 + 180)
        # estimate x 1.5 would leave no room on a small scope for the runner's
        # admission rule (an allocation of up to 25 matches at twice its
        # estimate + 5 min), so the timeout keeps 25 x 25 s + 300 s over it.
        assert env["SOFASCORE_SCOPE_TIMEOUT_S"] == str(430 + 925)
        assert env["SOFASCORE_REFRESH_SECONDS_PER_MATCH"] == "25"


@pytest.mark.unit
def test_refresh_window_slices_the_scope_that_does_not_fit_whole():
    planned = _window_plan([
        _refresh_pending("SS-17", "2627", 100, 1_787_900_001),
        _refresh_pending("SS-8", "2627", 5_920, 1_787_900_002),
        _refresh_pending("SS-17", "2526", 10, 1_787_900_003),
    ])

    assert [env["SOFASCORE_SCOPE_KEY"] for env in planned] == [
        "campaign-test:17:1726", "campaign-test:8:826",
    ]
    head, sliced = planned
    assert head["SOFASCORE_SCOPE_MAX_MATCHES"] == "100"
    # 7200 - (100 x 25 + 180) = 4520 s left -> (4520 - 180) // 25 matches.
    assert sliced["SOFASCORE_SCOPE_MAX_MATCHES"] == str((4520 - 180) // 25)
    assert sliced["SOFASCORE_SCOPE_BYTE_CAP"] == str(173 * 49_342)
    estimate = int(sliced["SOFASCORE_SCOPE_ESTIMATE_S"])
    assert estimate == 173 * 25 + 180
    assert int(head["SOFASCORE_SCOPE_ESTIMATE_S"]) + estimate <= 7200
    assert sliced["SOFASCORE_SCOPE_TIMEOUT_S"] == str((estimate * 3 + 1) // 2)


@pytest.mark.unit
def test_refresh_window_closes_on_a_slice_under_twenty_matches():
    planned = _window_plan([
        # 100 x 25 + 180 = 2680 s of a 2700 s window: 20 s are left.
        _refresh_pending("SS-17", "2627", 100, 1_787_900_001),
        _refresh_pending("SS-8", "2627", 50, 1_787_900_002),
        _refresh_pending("SS-17", "2526", 1, 1_787_900_003),
    ], scope_budget_s=2700)

    assert [env["SOFASCORE_SCOPE_KEY"] for env in planned] == [
        "campaign-test:17:1726"
    ]


@pytest.mark.unit
def test_refresh_window_caps_one_task_at_two_hours_and_logs_a_batch_cut(caplog):
    planned = _window_plan(
        [
            _refresh_pending("SS-17", "2627", 1, 1_787_900_004),
            _refresh_pending("SS-8", "2627", 1, 1_787_900_003),
        ],
        batch_size=1,
    )
    assert len(planned) == 1
    assert "SOFASCORE_REFRESH_BATCH_SIZE=1" in caplog.text

    unbounded = plan_refresh_batch(
        _refresh_snapshot(), [_refresh_pending("SS-17", "2627", 5_000)],
        queue_mode="deadline",
    )
    assert unbounded[0]["SOFASCORE_SCOPE_TIMEOUT_S"] == str(2 * 3600)


@pytest.mark.unit
def test_refresh_queue_a_fresh_far_deadline_season_does_not_overtake_a_near_one():
    # #1359: an American season that finished five minutes ago (deadline in
    # ~24 h) waits behind a European one whose deadline closes in an hour —
    # "freshest first" was what let the far one eat the window.
    planned = _window_plan([
        _refresh_pending("SS-17", "2627", 4, 1_787_900_000 + 24 * 3600),
        _refresh_pending("SS-8", "2627", 40, 1_787_900_000 + 3600),
    ])

    assert [env["SOFASCORE_SCOPE_KEY"] for env in planned] == [
        "campaign-test:8:826", "campaign-test:17:1726",
    ]


@pytest.mark.unit
def test_refresh_queue_an_esoccer_sized_debt_goes_second_tier_and_sliced():
    # A 5 920-match debt with no open deadline fills only what the open tier
    # left of the window, and only as a slice.
    planned = _window_plan([
        _refresh_pending("SS-8", "2627", 5_920, None),
        _refresh_pending("SS-17", "2627", 100, 1_787_900_000),
    ])

    assert [env["SOFASCORE_SCOPE_KEY"] for env in planned] == [
        "campaign-test:17:1726", "campaign-test:8:826",
    ]
    assert planned[0]["SOFASCORE_SCOPE_MAX_MATCHES"] == "100"
    # 7200 - (100 x 25 + 180) = 4520 s left -> (4520 - 180) // 25 matches.
    assert planned[1]["SOFASCORE_SCOPE_MAX_MATCHES"] == str((4520 - 180) // 25)


def _parts(planned):
    return [
        (
            env["SOFASCORE_SCOPE_KEY"],
            env.get("SOFASCORE_SCOPE_DEBT_ONLY") == "1",
            int(env["SOFASCORE_SCOPE_MAX_MATCHES"]),
        )
        for env in planned
    ]


@pytest.mark.unit
def test_refresh_queue_one_urgent_match_does_not_carry_its_seasons_debt_ahead():
    # Astra #1359 finding 3 and r3: SS-17/2627 has one open deadline and 999
    # matches of debt.  Its urgent match keeps its place (nearest deadline
    # first); its debt is a scope of its own after every urgent scope, and it
    # fills the window.
    planned = _window_plan([
        (*_refresh_pending("SS-17", "2627", 1_000, 1_787_900_001), 1),
        (*_refresh_pending("SS-8", "2627", 30, 1_787_900_002), 30),
        (*_refresh_pending("SS-17", "2526", 50, 1_787_900_003), 50),
    ])

    # 7200 - (1 + 30 + 50) x 25 - 4 x 180 = 4455 s -> 178 matches of debt.
    assert _parts(planned) == [
        ("campaign-test:17:1726", False, 1),
        ("campaign-test:8:826", False, 30),
        ("campaign-test:17:1725", False, 50),
        ("campaign-test:17:1726", True, 178),
    ]
    assert sum(int(env["SOFASCORE_SCOPE_ESTIMATE_S"]) for env in planned) > 7200 - 25
    assert sum(int(env["SOFASCORE_SCOPE_ESTIMATE_S"]) for env in planned) <= 7200
    urgent, debt = planned[0], planned[3]
    # Two tasks of one scope: separate gateway plans and result files.
    assert debt["SOFASCORE_SCOPE_RUN_ID"] == urgent["SOFASCORE_SCOPE_RUN_ID"] + "--debt"
    assert debt["SOFASCORE_SCOPE_RESULT_PATH"] != urgent["SOFASCORE_SCOPE_RESULT_PATH"]
    assert debt["SOFASCORE_SCOPE_OUTPUT_DIR"] != urgent["SOFASCORE_SCOPE_OUTPUT_DIR"]
    assert "SOFASCORE_SCOPE_DEBT_ONLY" not in urgent


@pytest.mark.unit
def test_refresh_queue_debt_goes_biggest_first_after_every_open_deadline():
    planned = _window_plan([
        (*_refresh_pending("SS-8", "2627", 60, 1_787_900_001), 10),
        (*_refresh_pending("SS-17", "2526", 90, None), 0),
        (*_refresh_pending("SS-17", "2627", 101, 1_787_900_002), 1),
    ])

    # Tier 1: the open matches of both open seasons; tier 2: the debts,
    # the biggest first — every debt that fits the window is taken.
    assert _parts(planned) == [
        ("campaign-test:8:826", False, 10),
        ("campaign-test:17:1726", False, 1),
        ("campaign-test:17:1726", True, 100),
        ("campaign-test:17:1725", False, 90),
        ("campaign-test:8:826", True, 50),
    ]


@pytest.mark.unit
@pytest.mark.parametrize("pending", [1, 10, 20, 25, 26, 100, 173, 243, 5_000])
def test_refresh_scope_timeout_admits_every_allocation_at_its_estimate(pending):
    """Astra #1358 r2: the planner's timeout and the runner's admission rule
    agree — at the estimated speed no allocation of the scope is refused."""
    from dags.scripts import run_sofascore_scraper as runner
    from utils import sofascore_all_mens_state as state

    assert runner.ALLOCATION_OVERRUN_FACTOR == state.REFRESH_ALLOCATION_OVERRUN_FACTOR
    assert runner.SCOPE_STOP_MARGIN_SECONDS == state.REFRESH_RUNNER_STOP_MARGIN_SECONDS
    (env,) = _window_plan([_refresh_pending("SS-17", "2627", pending, 1_787_900_001)])
    matches = int(env["SOFASCORE_SCOPE_MAX_MATCHES"])
    timeout = int(env["SOFASCORE_SCOPE_TIMEOUT_S"])
    assert timeout <= 2 * 3600
    limits = runner._scope_capture_limits({
        "SOFASCORE_SCOPE_DEADLINE_EPOCH": str(timeout),
        "SOFASCORE_SCOPE_BYTE_CAP": env["SOFASCORE_SCOPE_BYTE_CAP"],
        "SOFASCORE_SCOPE_MAX_MATCHES": env["SOFASCORE_SCOPE_MAX_MATCHES"],
        "SOFASCORE_REFRESH_SECONDS_PER_MATCH": "25",
    })
    now, spent, left = state.REFRESH_SCOPE_OVERHEAD_SECONDS, 0, matches
    while left:
        batch = min(state.REFRESH_ALLOCATION_MATCHES, left)
        assert runner._capture_stop_reason(limits, spent, batch, now=now) is None
        now += batch * 25
        spent += batch * state.REFRESH_BYTES_PER_MATCH
        left -= batch
