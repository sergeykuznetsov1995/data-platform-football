from datetime import datetime, timezone
from types import SimpleNamespace

from dags.utils.transfermarkt_achievement_dq import (
    achievement_reconciliation_sql,
    build_achievement_dq,
)
from scrapers.transfermarkt.achievement_observations import (
    AchievementRecipient,
    AchievementUniverse,
)
from scrapers.transfermarkt.achievement_state import AchievementState

NOW = datetime(2026, 11, 20, 12, tzinfo=timezone.utc)


def roster():
    return AchievementUniverse("v1", (
        AchievementRecipient("player", 1),
        AchievementRecipient("club", 2),
        AchievementRecipient("coach", 3, "coach-three"),
    ))


def mark_all(state, manifest, recipients):
    for recipient in recipients:
        extraction = SimpleNamespace(
            raw_capture_id="a" * 64,
            rows=({"observation_status": "observed_present"},),
        )
        state.mark_success(manifest, recipient, extraction, {
            "verified": True, "receipt_readback": True,
            "source_capture_id": extraction.raw_capture_id, "receipt_id": "receipt",
        }, NOW)


def test_dq_green_requires_coverage_and_zero_physical_violations(tmp_path):
    universe = roster()
    with AchievementState(tmp_path / "state.sqlite3") as state:
        state.start_assessment(universe, created_at=NOW)
        mark_all(state, universe.assessment_manifest_id, universe.recipients)
        report = build_achievement_dq(
            universe, state, reconciliation_violations=0, evaluated_at=NOW,
        )
        assert report["status"] == "GREEN" and report["completed"] is True
        assert report["verified_recipients"] == 3
        assert [report["per_type"][kind]["conclusive_rate"] for kind in ("player", "club", "coach")] == [1.0] * 3
        red = build_achievement_dq(
            universe, state, reconciliation_violations=1, evaluated_at=NOW,
        )
        assert red["status"] == "RED"


def test_dq_incomplete_and_per_type_unresolved_remain_visible(tmp_path):
    universe = roster()
    with AchievementState(tmp_path / "state.sqlite3") as state:
        state.start_assessment(universe, created_at=NOW)
        mark_all(
            state, universe.assessment_manifest_id,
            (next(item for item in universe.recipients if item.recipient_type == "player"),),
        )
        report = build_achievement_dq(
            universe, state, reconciliation_violations=0, evaluated_at=NOW,
        )
        assert report["status"] == "RED" and report["completed"] is False
        assert report["per_type"]["player"]["conclusive"] == 1
        assert report["per_type"]["club"]["unresolved"] == 1
        assert report["per_type"]["coach"]["unresolved_rate"] == 1.0


def test_reconciliation_sql_covers_counts_duplicates_and_all_lineage():
    sql = achievement_reconciliation_sql()
    for marker in (
        "row_count <> r.row_count", "distinct_rows <> r.row_count",
        "null_lineage <> 0", "duplicate_keys", "source_body_hash",
        "captured_at", "observed_at", "extracted_at", "assessment_manifest_id",
        "source_capture_id", "source_row_id",
    ):
        assert marker in sql
