from __future__ import annotations

import hashlib
import json

import pytest

from dags.utils.sofascore_all_mens_state import (
    CampaignPlanningError,
    campaign_scope_key,
    clear_failed,
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
def test_pending_metadata_plans_a_serialized_wave_before_capture():
    snapshot = _snapshot(second_status="pending")
    completed = {
        campaign_scope_key(snapshot["campaign_id"], 17, 1725),
    }

    planned = plan_historical_batch(snapshot, completed=completed, batch_size=10)

    assert len(planned) == 1
    assert planned[0]["SOFASCORE_CAMPAIGN_ACTION"] == "metadata"
    assert planned[0]["SOFASCORE_METADATA_WAVE"] == "2025"


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


def test_planner_defers_unmeasured_shapes_and_starts_measured_wave_scope():
    snapshot = _snapshot()
    snapshot["tournaments"][0]["seasons"][0]["team_count"] = 24
    measured_class = season_workload_class(production_season_shape(
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
        authorized_season_classes={measured_class: (8, 17)},
    )

    # The deferred 17-1725 also holds the deeper 17-1724 (canary unlocks it).
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
    measured_class = season_workload_class(production_season_shape(
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
        authorized_season_classes={measured_class: (8, 17)},
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


@pytest.mark.unit
def test_refresh_planner_ranks_partitions_by_pending_matches_and_bounds_batch():
    snapshot = _refresh_snapshot()
    pending = [("SS-8", "2526", 3), ("SS-17", "2627", 10), ("SS-17", "2526", 1)]

    planned = plan_refresh_batch(
        snapshot, pending, batch_size=2, dag_run_id="scheduled__1",
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
        ("SS-8", "2627", 9),      # configured league: the daily ingest owns it
        ("SS-999", "2627", 8),    # not in the snapshot
        ("SS-17", "1999", 7),     # season the snapshot does not know
        ("SS-17", "2526", 6),     # excluded season: the scope cycle refuses it
        ("ENG-Premier League", "2627", 5),
        ("SS-17", "2627", 1),
    ]

    planned = plan_refresh_batch(
        snapshot, pending, batch_size=8, exclude_tournament_ids={8}
    )

    assert [item["SOFASCORE_SCOPE_KEY"] for item in planned] == [
        "campaign-test:17:1726"
    ]


@pytest.mark.unit
def test_refresh_planner_rejects_a_stale_snapshot_and_bad_batch_size():
    snapshot = _refresh_snapshot()

    with pytest.raises(CampaignPlanningError, match="batch_size"):
        plan_refresh_batch(snapshot, [], batch_size=0)
    snapshot["snapshot_id"] = "0" * 64
    with pytest.raises(CampaignPlanningError, match="digest"):
        plan_refresh_batch(snapshot, [("SS-17", "2627", 1)])


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
