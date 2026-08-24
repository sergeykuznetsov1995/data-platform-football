from __future__ import annotations

import hashlib
import json

import pytest

from dags.utils.sofascore_all_mens_state import (
    campaign_scope_key,
    plan_historical_batch,
)
from scrapers.sofascore.workload_plan import (
    production_season_shape,
    season_workload_class,
)


def _snapshot(*, second_status="ready"):
    tournaments = []
    for tournament_id, status in ((17, "ready"), (8, second_status)):
        tournaments.append({
            "unique_tournament_id": tournament_id,
            "capture_key": f"SS-{tournament_id}",
            "metadata_status": status,
            "seasons": [
                {
                    "source_season_id": tournament_id * 100 + 25,
                    "canonical_season": "2526",
                    "start_year": 2025,
                    "season_format": "split_year",
                    "team_count": 20,
                    "metadata_status": status,
                    "team_count_evidence": {"count": 20, "endpoint": "/teams"},
                },
                {
                    "source_season_id": tournament_id * 100 + 24,
                    "canonical_season": "2425",
                    "start_year": 2024,
                    "season_format": "split_year",
                    "team_count": 20,
                    "metadata_status": status,
                    "team_count_evidence": {"count": 20, "endpoint": "/teams"},
                },
            ],
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

    assert {item["SOFASCORE_CANONICAL_SEASON"] for item in planned} == {"2526"}
    assert len(planned) == 2
    assert {item["SOFASCORE_SCOPE_RUN_ID"] for item in planned} == {"manual"}


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

    assert len(planned) == 1
    assert planned[0]["SOFASCORE_TOURNAMENT_ID"] == "8"


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

    assert len(planned) == 1
    assert planned[0]["SOFASCORE_TOURNAMENT_ID"] == "8"


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
