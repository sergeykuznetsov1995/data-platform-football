from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from scrapers.transfermarkt.achievement_observations import (
    AchievementRecipient,
    AchievementUniverse,
)
from scrapers.transfermarkt.achievement_state import AchievementState

NOW = datetime(2026, 11, 20, 12, tzinfo=timezone.utc)


def universe(count=2):
    recipients = tuple(AchievementRecipient("player", number) for number in range(1, count + 1))
    return AchievementUniverse("v1", recipients)


def success(state, manifest, recipient, at=NOW):
    extraction = SimpleNamespace(
        raw_capture_id="a" * 64,
        rows=({"observation_status": "observed_present"},),
    )
    receipt = {
        "verified": True, "receipt_readback": True,
        "source_capture_id": extraction.raw_capture_id, "receipt_id": "receipt-1",
    }
    state.mark_success(manifest, recipient, extraction, receipt, at)


def test_state_chunks_errors_retry_and_freezes_universe(tmp_path):
    with AchievementState(tmp_path / "state.sqlite3") as state:
        roster = universe()
        assert state.start_assessment(roster, created_at=NOW) is True
        assert state.start_assessment(roster, created_at=NOW) is False
        manifest = roster.assessment_manifest_id
        assert [item.recipient_id for item in state.due_recipients(manifest, limit=1)] == ["1"]
        first = state.due_recipients(manifest, limit=1)[0]
        state.mark_error(manifest, first, "timeout", NOW)
        assert [item.recipient_id for item in state.due_recipients(manifest, limit=2)] == ["1", "2"]
        success(state, manifest, first)
        assert state.assessment(manifest)["completed_at"] is None
        success(state, manifest, state.due_recipients(manifest, limit=1)[0])
        assert state.assessment(manifest)["completed_at"] == NOW.isoformat()
        assert state.active_assessment() is None
        assert state.universe(manifest).recipients == roster.recipients
        assert state.verified_recipients(manifest) == {("player", "1"), ("player", "2")}


def test_success_requires_verified_matching_receipt(tmp_path):
    with AchievementState(tmp_path / "state.sqlite3") as state:
        roster = universe(1)
        state.start_assessment(roster, created_at=NOW)
        recipient = roster.recipients[0]
        extraction = SimpleNamespace(
            raw_capture_id="a" * 64,
            rows=({"observation_status": "observed_present"},),
        )
        with pytest.raises(ValueError, match="verified"):
            state.mark_success(
                roster.assessment_manifest_id, recipient, extraction,
                {"verified": False, "receipt_readback": True}, NOW,
            )
        with pytest.raises(ValueError, match="capture"):
            state.mark_success(
                roster.assessment_manifest_id, recipient, extraction,
                {"verified": True, "receipt_readback": True, "source_capture_id": "b" * 64}, NOW,
            )


def test_assessment_allows_five_percent_unresolved_but_not_more(tmp_path):
    with AchievementState(tmp_path / "state.sqlite3") as state:
        roster = universe(20)
        state.start_assessment(roster, created_at=NOW)
        manifest = roster.assessment_manifest_id
        for recipient in roster.recipients[:18]:
            success(state, manifest, recipient)
        state.mark_error(manifest, roster.recipients[18], "timeout", NOW)
        state.mark_error(manifest, roster.recipients[19], "timeout", NOW)
        assert state.assessment(manifest)["completed_at"] is None
        success(state, manifest, roster.recipients[18])
        assert state.assessment(manifest)["completed_at"] == NOW.isoformat()
        assert state.counts(manifest)["player"]["expected"] == 20


def test_empty_and_naive_state_inputs_fail(tmp_path):
    with AchievementState(tmp_path / "state.sqlite3") as state:
        roster = universe(1)
        with pytest.raises(ValueError, match="timezone"):
            state.start_assessment(roster, created_at=datetime.now())
        with pytest.raises(ValueError, match="nonnegative"):
            state.due_recipients(roster.assessment_manifest_id, limit=-1)
        with pytest.raises(ValueError, match="nonempty"):
            state.start_assessment(roster, created_at=NOW, post_season_proof=" ")
