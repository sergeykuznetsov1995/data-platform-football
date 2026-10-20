"""Offline closure transitions and complete-day evidence, never scope success counts."""
import copy
import hashlib
import json
from datetime import datetime, timedelta, timezone
import pytest

from scrapers.sofascore.season_accounting import (
    PUBLICATION_DEFINITION, apply_observation, digest, summarize_day, summarize_series,
)


IDENTITY = {"campaign_id": "campaign", "snapshot_id": "snapshot"}
ROSTER = [{"tournament_id": 1, "season_id": 2, "group": "2"}]


def observation(stamp, closed=0, finished=10, *, complete=True, ids=True, roster=None):
    roster = roster or ROSTER
    scopes = []
    for entry in roster:
        row = dict(entry, finished=finished, closed=closed, ongoing=False,
                   schedule_complete=complete)
        if ids:
            row["closed_match_ids"] = [str(i) for i in range(closed)]
            row["closed_match_runs"] = {str(i): f"native-{entry['tournament_id']}-scope::targets" for i in range(closed)}
        scopes.append(row)
    inventory = {"observed_at": stamp, "scopes": scopes}
    return {"observation_id": stamp, "observed_at": stamp, "identity": IDENTITY,
            "release": "release-a", "roster": roster, "roster_complete": True, "inventory": inventory,
            "publication_evidence": {"definition": PUBLICATION_DEFINITION,
                                     "inventory_digest": digest(inventory)}}


def day_input():
    return {"day": "2026-10-15", "identity": IDENTITY, "release": "release-a",
            "group_digest": digest(ROSTER),
            "delivered_interval": {"start": "2026-10-14T12:00:00Z", "end": "2026-10-17T00:00:00Z"},
            "expected_runs": [], "run_roster_complete": True,
            "run_roster_digest": digest([])}


def journal_for_day(*, count=10, ids=True):
    state = apply_observation(None, observation("2026-10-15T00:00:00Z", ids=ids))
    state = apply_observation(state, observation("2026-10-15T12:00:00Z", count, ids=ids))
    return apply_observation(state, observation("2026-10-16T00:00:00Z", count, ids=ids))


def save_receipt(path, day, *, slot=0, status="failed", start="2026-10-15T01:00:00Z",
                 end="2026-10-15T02:00:00Z", run="run-a", attempts=1, tid=1):
    env = {"SOFASCORE_CAMPAIGN_ACTION": "capture", "SOFASCORE_SCOPE_RESULT_PATH": f"/{run}.json",
           "SOFASCORE_SCOPE_RUN_ID": f"native-{tid}-scope", "SOFASCORE_EXPECTED_CAMPAIGN_ID": "campaign",
           "SOFASCORE_EXPECTED_SNAPSHOT_ID": "snapshot", "SOFASCORE_TOURNAMENT_ID": str(tid),
           "SOFASCORE_SOURCE_SEASON_ID": "2", "SOFASCORE_HISTORY_GROUP": "2"}
    item = {"slot": slot, "index": 0, "plan_index": 0, "started_at": start, "finished_at": end,
            "accounted": True, "attempts": attempts, "outcome": {"status": status}, "environment": env}
    payload = {"history_scope_attempt": True, "scope_digest": run, "run_id": f"native-{tid}-scope",
               "campaign_id": "campaign", "tournament_id": tid, "source_season_id": 2,
               "history_slot": {"slot": slot, "dag_run_id": run, "started_at": start,
                                "finished_at": end, "terminal_state": status}}
    (path / f"{run}.json").write_text(json.dumps(payload))
    plan_digest = digest([env])
    receipt = {"history_slots_receipt": True, "dag_run_id": run, "campaign_id": "campaign",
               "finalized": True, "claimed_count": 1, "plan_digest": plan_digest,
               "plan_length": 1, "items": [item]}
    receipt["receipt_digest"] = digest(receipt)
    name = f"history-slots-{hashlib.sha256(run.encode()).hexdigest()[:20]}.json"
    raw = json.dumps(receipt).encode()
    (path / name).write_bytes(raw)
    reservation = {"mode": "slots", "run_id": run, "plan": [env], "plan_digest": plan_digest,
                   "finalized": True, "cursor": 1,
                   "items": {"0": {k: v for k, v in item.items() if k not in {"environment", "index"}}}}
    reservation["slot_digest"] = digest(reservation)
    checkpoint = {"schema_version": 1, "campaign_id": "campaign", "snapshot_id": "snapshot",
                  "positions": {}, "run": reservation, "active_group": "2",
                  "groups": {"1": {"finished": 100, "closed": 100, "unknown_scopes": 0, "gate_passed": True}}}
    checkpoint_raw = json.dumps(checkpoint).encode()
    (path / f"checkpoint-{run}.json").write_bytes(checkpoint_raw)
    day["expected_runs"].append({"run_id": run, "release": "release-a", "plan_digest": plan_digest,
                                 "receipt_sha256": hashlib.sha256(raw).hexdigest(),
                                 "slot_digest": reservation["slot_digest"],
                                 "checkpoint_file": f"checkpoint-{run}.json",
                                 "checkpoint_sha256": hashlib.sha256(checkpoint_raw).hexdigest()})
    day["run_roster_digest"] = digest(day["expected_runs"])


def test_partial_success_and_completed_memory_cannot_close_season():
    state = apply_observation(None, observation("2026-10-15T00:00:00Z"))
    partial = observation("2026-10-15T01:00:00Z", 3)
    partial["completed"] = ["campaign:1:2"]
    partial["successful_scope_keys"] = ["campaign:1:2"]
    state = apply_observation(state, partial)
    assert not [e for e in state["events"] if e["kind"] == "first_closure"]


def test_first_closure_idempotence_baseline_and_reopen_are_separate():
    initial = observation("2026-10-15T00:00:00Z", 10)
    baseline = apply_observation(None, initial)
    assert not baseline["events"]
    state = journal_for_day()
    assert len([e for e in state["events"] if e["kind"] == "first_closure"]) == 1
    repeat = observation("2026-10-16T00:00:00Z", 10)
    assert apply_observation(state, repeat) == state
    state = apply_observation(state, observation("2026-10-16T01:00:00Z", 9))
    state = apply_observation(state, observation("2026-10-16T02:00:00Z", 10))
    assert [e["kind"] for e in state["events"]] == ["first_closure", "reopen", "reclose"]
    assert len(state["matches"]) == 10


@pytest.mark.parametrize("change", ["schedule", "manifest", "ongoing", "zero", "unknown"])
def test_incomplete_evidence_never_closes(change):
    initial = observation("2026-10-15T00:00:00Z")
    state = apply_observation(None, initial)
    final = observation("2026-10-15T01:00:00Z", 10)
    if change == "schedule":
        final["inventory"]["scopes"][0]["schedule_complete"] = False
    elif change == "manifest":
        final["publication_evidence"]["definition"] = "successful_scope"
    elif change == "ongoing":
        final["inventory"]["scopes"][0]["ongoing"] = True
    elif change == "zero":
        final = observation("2026-10-15T01:00:00Z", 0, 0)
    else:
        final["inventory"]["scopes"][0]["unknown"] = 1
    if change != "manifest":
        final["publication_evidence"]["inventory_digest"] = digest(final["inventory"])
    state = apply_observation(state, final)
    assert not [e for e in state["events"] if e["kind"] == "first_closure"]


def test_day_uses_all_slots_union_and_preserves_failures_and_retries(tmp_path):
    day = day_input()
    save_receipt(tmp_path, day, start="2026-10-14T23:00:00Z", end="2026-10-15T02:00:00Z")
    save_receipt(tmp_path, day, start="2026-10-15T01:00:00Z", end="2026-10-15T03:00:00Z", run="retry", status="success")
    save_receipt(tmp_path, day, slot=1, run="parallel", tid=3)
    # Additional native season need not be counted, but must be in the roster.
    roster = ROSTER + [{"tournament_id": 3, "season_id": 2, "group": "2"}]
    state = apply_observation(None, observation("2026-10-15T00:00:00Z", roster=roster))
    final = observation("2026-10-15T12:00:00Z", 10, roster=roster)
    final["inventory"]["scopes"][1]["closed"] = 0
    final["inventory"]["scopes"][1]["closed_match_ids"] = []
    final["publication_evidence"]["inventory_digest"] = digest(final["inventory"])
    state = apply_observation(state, final)
    ending = copy.deepcopy(final)
    ending.update(observation_id="end", observed_at="2026-10-16T00:00:00Z")
    ending["inventory"]["observed_at"] = ending["observed_at"]
    ending["publication_evidence"]["inventory_digest"] = digest(ending["inventory"])
    state = apply_observation(state, ending)
    day["group_digest"] = digest(roster)
    report = summarize_day(state, day, tmp_path)
    assert report["verdict"] == "FAIL"
    assert report["closed_seasons"] == 1
    assert report["closed_matches"] == 10
    assert report["slots"]["0"]["occupied_seconds"] == 3 * 3600
    assert report["slots"]["1"]["occupied_seconds"] == 3600
    assert report["slots"]["2"]["occupied_seconds"] == 0
    assert [a["status"] for a in report["attempts"]] == ["failed", "success", "failed"]
    assert report["slot_capacity_seconds"] == 3 * 86400


@pytest.mark.parametrize("change", ["missing", "release", "groups", "roster", "ids", "boundary"])
def test_day_unknown_for_unqualified_evidence(tmp_path, change):
    state = journal_for_day(ids=change != "ids")
    day = day_input()
    save_receipt(tmp_path, day)
    if change == "missing":
        next(tmp_path.glob("history-slots-*.json")).unlink()
    elif change == "release":
        day["delivered_interval"]["start"] = "2026-10-15T01:00:00Z"
    elif change == "groups":
        day["group_digest"] = "other"
    elif change == "roster":
        day["run_roster_complete"] = False
    elif change == "boundary":
        state["observations"] = state["observations"][1:]
    report = summarize_day(state, day, tmp_path)
    assert report["verdict"] == "UNKNOWN"
    assert report["missing_evidence"]
    assert report["acceptance_started_at"] is None


def test_cross_midnight_observation_does_not_invent_closure_day():
    state = apply_observation(None, observation("2026-10-14T23:00:00Z"))
    state = apply_observation(state, observation("2026-10-15T01:00:00Z", 10))
    event = state["events"][0]
    assert event["day"] is None
    assert event["observed_at"] == "2026-10-15T01:00:00+00:00"
    assert event["after"] == "2026-10-14T23:00:00+00:00"


def test_full_saved_evidence_can_pass_joint_criteria(tmp_path):
    roster = [dict(tournament_id=i + 1, season_id=2, group="2") for i in range(67)]
    state = apply_observation(None, observation("2026-10-15T00:00:00Z", finished=151, roster=roster))
    state = apply_observation(state, observation("2026-10-15T23:59:59Z", 151, 151, roster=roster))
    state = apply_observation(state, observation("2026-10-16T00:00:00Z", 151, 151, roster=roster))
    day = day_input()
    day["group_digest"] = digest(roster)
    midnight = datetime(2026, 10, 15, tzinfo=timezone.utc)
    for index in range(67):
        slot, position = index % 3, index // 3
        count = len(range(slot, 67, 3))
        start = midnight + timedelta(seconds=int(position * 86400 / count))
        end = midnight + timedelta(seconds=int((position + 1) * 86400 / count))
        save_receipt(tmp_path, day, slot=slot, start=start.isoformat(), end=end.isoformat(),
                     run=f"native-run-{index}", status="success", tid=index + 1)
    report = summarize_day(state, day, tmp_path)
    assert report["closed_seasons"] == 67
    assert report["closed_matches"] == 10117
    # 10117/72 capacity hours is below 202 even though occupancy and season counts pass.
    assert report["verdict"] == "FAIL"
    assert report["checks"]["matches_per_hour_per_slot"] is False
    # 218 matches/season clears all four fixed criteria together.
    state = apply_observation(None, observation("2026-10-15T00:00:00Z", finished=218, roster=roster))
    state = apply_observation(state, observation("2026-10-15T23:59:59Z", 218, 218, roster=roster))
    state = apply_observation(state, observation("2026-10-16T00:00:00Z", 218, 218, roster=roster))
    assert summarize_day(state, day, tmp_path)["verdict"] == "PASS"


def test_seven_days_must_be_consecutive_same_identity():
    reports = [dict(day=f"2026-10-{i:02}", verdict="PASS", identity=IDENTITY,
                    group_digest=digest(ROSTER), release="release-a") for i in range(15, 22)]
    assert summarize_series(reports)["verdict"] == "PASS"
    assert summarize_series(reports[:6])["verdict"] == "UNKNOWN"
    reports[-1]["release"] = "release-b"
    assert summarize_series(reports)["verdict"] == "UNKNOWN"


def test_immutable_observation_and_native_roster_identity():
    initial = observation("2026-10-15T00:00:00Z")
    state = apply_observation(None, initial)
    changed = copy.deepcopy(initial)
    changed["release"] = "different"
    with pytest.raises(ValueError, match="immutable"):
        apply_observation(state, changed)
    changed["observation_id"] = "later"
    changed["observed_at"] = "2026-10-15T01:00:00Z"
    changed["roster"][0]["group"] = "3"
    with pytest.raises(ValueError, match="identity"):
        apply_observation(state, changed)


def test_exact_saved_closure_times_attribute_cross_midnight_evidence():
    state = apply_observation(None, observation("2026-10-14T23:00:00Z"))
    final = observation("2026-10-15T01:00:00Z", 10)
    row = final["inventory"]["scopes"][0]
    row["closure_at"] = "2026-10-15T00:30:00Z"
    row["match_closed_at"] = {str(i): "2026-10-15T00:30:00Z" for i in range(10)}
    final["publication_evidence"]["inventory_digest"] = digest(final["inventory"])
    state = apply_observation(state, final)
    assert state["events"][0]["day"] == "2026-10-15"
    assert {m["day"] for m in state["matches"].values()} == {"2026-10-15"}


def test_internal_retry_needs_saved_failed_attempt_and_retains_it(tmp_path):
    day = day_input()
    save_receipt(tmp_path, day, status="success", attempts=2)
    report = summarize_day(journal_for_day(), day, tmp_path)
    assert report["verdict"] == "UNKNOWN"
    assert any("retry" in reason for reason in report["missing_evidence"])
    day["expected_runs"][0]["attempt_histories"] = {"0": [
        {"status": "failed", "started_at": "2026-10-15T01:00:00Z", "finished_at": "2026-10-15T01:10:00Z"},
        {"status": "success", "started_at": "2026-10-15T01:12:00Z", "finished_at": "2026-10-15T02:00:00Z"}]}
    day["run_roster_digest"] = digest(day["expected_runs"])
    report = summarize_day(journal_for_day(), day, tmp_path)
    assert report["verdict"] == "FAIL"
    assert report["attempts"][0]["attempt_history"][0]["status"] == "failed"


@pytest.mark.parametrize("change", ["digest", "report", "checkpoint", "slot", "native_group", "capture_gate"])
def test_corrupt_or_cross_identity_receipts_are_unknown(tmp_path, change):
    day = day_input()
    save_receipt(tmp_path, day)
    if change == "digest":
        next(tmp_path.glob("history-slots-*.json")).write_text("{}")
    elif change == "report":
        (tmp_path / "run-a.json").unlink()
    elif change == "checkpoint":
        (tmp_path / "checkpoint-run-a.json").unlink()
    elif change == "slot":
        day["expected_runs"][0]["slot_digest"] = "bad"
        day["run_roster_digest"] = digest(day["expected_runs"])
    elif change == "native_group":
        day["group_digest"] = "bad"
    else:
        checkpoint_path = tmp_path / "checkpoint-run-a.json"
        checkpoint = json.loads(checkpoint_path.read_text())
        checkpoint["groups"]["1"]["closed"] = 98
        raw = json.dumps(checkpoint).encode()
        checkpoint_path.write_bytes(raw)
        day["expected_runs"][0]["checkpoint_sha256"] = hashlib.sha256(raw).hexdigest()
        day["run_roster_digest"] = digest(day["expected_runs"])
    assert summarize_day(journal_for_day(), day, tmp_path)["verdict"] == "UNKNOWN"


@pytest.mark.parametrize("active,rejected", [("2", False), ("3", True)])
def test_only_deeper_groups_need_the_core_gate(tmp_path, active, rejected):
    day = day_input()
    save_receipt(tmp_path, day)
    checkpoint_path = tmp_path / "checkpoint-run-a.json"
    checkpoint = json.loads(checkpoint_path.read_text())
    checkpoint["groups"]["1"].update(closed=98, gate_passed=False)
    checkpoint["active_group"] = active
    raw = json.dumps(checkpoint).encode()
    checkpoint_path.write_bytes(raw)
    day["expected_runs"][0]["checkpoint_sha256"] = hashlib.sha256(raw).hexdigest()
    day["run_roster_digest"] = digest(day["expected_runs"])
    missing = summarize_day(journal_for_day(), day, tmp_path)["missing_evidence"]
    assert any("before core gate" in m for m in missing) is rejected


@pytest.mark.parametrize("status,publication_run", [("failed", "native-1-scope::targets"),
                                                     ("success", "refresh-run::targets"),
                                                     ("success", None)])
def test_started_history_scope_never_credits_refresh_publications(tmp_path, status, publication_run):
    state = apply_observation(None, observation("2026-10-15T00:00:00Z"))
    final = observation("2026-10-15T12:00:00Z", 10)
    final["inventory"]["scopes"][0]["closed_match_runs"] = {str(i): publication_run for i in range(10)}
    final["publication_evidence"]["inventory_digest"] = digest(final["inventory"])
    state = apply_observation(state, final)
    ending = copy.deepcopy(final)
    ending.update(observation_id="end", observed_at="2026-10-16T00:00:00Z")
    ending["inventory"]["observed_at"] = ending["observed_at"]
    ending["publication_evidence"]["inventory_digest"] = digest(ending["inventory"])
    state = apply_observation(state, ending)
    day = day_input()
    save_receipt(tmp_path, day, status=status)
    report = summarize_day(state, day, tmp_path)
    assert report["verdict"] == "UNKNOWN"
    assert any("publication association" in reason for reason in report["missing_evidence"])
