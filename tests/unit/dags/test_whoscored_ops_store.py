"""Immutable S3-compatible control state for WhoScored backfills."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from dags.scripts.whoscored_ops_store import (
    BACKFILL_CHECKPOINT_VERSION,
    CHECKPOINT_DELTAS_PER_SNAPSHOT,
    LEGACY_BACKFILL_CHECKPOINT_VERSION,
    MATCH_CHUNK_SIZE,
    PROFILE_CHUNK_SIZE,
    schedule_request_units,
    WhoScoredBackfillState,
    WhoScoredOpsStore,
)


def _state(monkeypatch, tmp_path: Path) -> WhoScoredBackfillState:
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    return WhoScoredBackfillState.from_env()


def _one_stage_per_scope(scopes):
    return {
        scope: [index]
        for index, scope in enumerate(sorted(set(scopes)), start=1)
    }


def _schedule_outcome(
    work,
    *,
    candidate_game_ids,
    preview_game_ids,
    source_stage_ids=None,
    source_request_attempts=0,
):
    observed_stage_ids = list(
        work["catalog_stage_ids"] if source_stage_ids is None else source_stage_ids
    )
    estimated = int(work["estimated_request_units"])
    return {
        "candidate_game_ids": list(candidate_game_ids),
        "preview_game_ids": list(preview_game_ids),
        "source_stage_ids": observed_stage_ids,
        "source_request_attempts": source_request_attempts,
        "estimated_request_units": estimated,
        "actual_request_units": max(
            estimated,
            schedule_request_units(len(observed_stage_ids)),
            source_request_attempts,
        ),
    }


@pytest.mark.unit
def test_plan_is_immutable_and_controller_is_bounded(monkeypatch, tmp_path):
    state = _state(monkeypatch, tmp_path)
    scopes = [f"WS-{index}=2026" for index in range(130)]
    plan = state.create_plan(
        queue_id="all-men",
        selector={"all_catalog": True},
        scopes=scopes,
        schedule_stage_ids=_one_stage_per_scope(scopes),
    )
    repeated = state.create_plan(
        queue_id="all-men",
        selector={"all_catalog": True},
        scopes=reversed(scopes),
        schedule_stage_ids=_one_stage_per_scope(scopes),
    )

    assert repeated["plan_id"] == plan["plan_id"]
    assert repeated["artifact"]["sha256"] == plan["artifact"]["sha256"]
    work = state.pending_work("all-men", plan["plan_id"])
    assert len(work) == 100
    assert {item["kind"] for item in work} == {"schedule"}
    assert len({item["work_id"] for item in work}) == 100


@pytest.mark.unit
def test_plan_identity_binds_catalog_generation_and_candidate_policy(
    monkeypatch, tmp_path
):
    state = _state(monkeypatch, tmp_path)
    first = state.create_plan(
        queue_id="q",
        selector={"all_catalog": True},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
        provenance={"catalog_batch_id": "wsc2-one"},
    )
    second = state.create_plan(
        queue_id="q",
        selector={"all_catalog": True},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
        provenance={"catalog_batch_id": "wsc2-two"},
    )

    assert first["plan_id"] != second["plan_id"]
    assert first["policy"]["match_candidate_policy"] == (
        "all_completed_schedule_matches"
    )
    assert first["policy"]["availability_version"]
    assert first["policy"]["schedule_capacity_policy"] == (
        "pinned-catalog-stage-count-v1"
    )
    assert first["policy"]["schedule_request_units_per_stage"] == 70
    assert first["policy"]["policy_version"] == 6
    assert first["policy"]["match_capacity_policy"] == (
        "exact-match-plus-preview-cardinality-v1"
    )
    assert first["policy"]["match_request_units_per_game"] == 1
    assert first["policy"]["preview_request_units_per_game"] == 1


@pytest.mark.unit
def test_catalog_backfill_plan_rejects_missing_frozen_stage_identities(
    monkeypatch, tmp_path
):
    state = _state(monkeypatch, tmp_path)

    with pytest.raises(RuntimeError, match="require frozen stage identities"):
        state.create_plan(
            queue_id="q",
            selector={"all_catalog": True},
            scopes=["WS-1=2026"],
            provenance={"catalog_batch_id": "wsc2-generation"},
        )


@pytest.mark.unit
def test_schedule_capacity_is_frozen_per_catalog_stage_and_changes_plan_identity(
    monkeypatch, tmp_path
):
    state = _state(monkeypatch, tmp_path)
    scope = "INT-World Cup=2026"
    stage_ids = list(range(23752, 23765))
    multi = state.create_plan(
        queue_id="q",
        selector={},
        scopes=[scope],
        schedule_stage_ids={scope: stage_ids},
    )
    single = state.create_plan(
        queue_id="q",
        selector={},
        scopes=[scope],
        schedule_stage_ids={scope: [23752]},
    )

    assert multi["plan_id"] != single["plan_id"]
    work = state.pending_work("q", multi["plan_id"])[0]
    assert work["catalog_stage_ids"] == stage_ids
    assert work["estimated_request_units"] == 13 * 70 == 910
    assert state.request_units(work) == 910


@pytest.mark.unit
def test_schedule_receipt_accounts_for_catalog_drift_and_observed_attempts(
    monkeypatch, tmp_path
):
    state = _state(monkeypatch, tmp_path)
    scope = "INT-World Cup=2026"
    plan = state.create_plan(
        queue_id="q",
        selector={},
        scopes=[scope],
        schedule_stage_ids={scope: [23752]},
    )
    work = state.pending_work("q", plan["plan_id"])[0]
    observed = list(range(23752, 23765))
    state.append_receipt(
        queue_id="q",
        plan_id=plan["plan_id"],
        work_item=work,
        outcome=_schedule_outcome(
            work,
            candidate_game_ids=[],
            preview_game_ids=[],
            source_stage_ids=observed,
            source_request_attempts=807,
        ),
    )

    progress = state.progress("q", plan["plan_id"])
    assert progress["estimated_completed_request_units"] == 70
    assert progress["actual_completed_request_units"] == 910
    assert progress["schedule_stage_cardinality_drifts"] == 1
    assert progress["projected_request_units_lower_bound"] == 910


@pytest.mark.unit
def test_match_capacity_uses_exact_match_and_preview_cardinality(
    monkeypatch, tmp_path
):
    state = _state(monkeypatch, tmp_path)
    plan = state.create_plan(
        queue_id="q",
        selector={},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
    )
    schedule = state.pending_work("q", plan["plan_id"])[0]
    state.append_receipt(
        queue_id="q",
        plan_id=plan["plan_id"],
        work_item=schedule,
        outcome=_schedule_outcome(
            schedule,
            candidate_game_ids=[1, 2, 3, 4],
            preview_game_ids=[2, 4],
        ),
    )

    match = state.pending_work("q", plan["plan_id"])[0]

    assert match["game_ids"] == [1, 2, 3, 4]
    assert match["preview_game_ids"] == [2, 4]
    assert state.request_units(match) == 6


@pytest.mark.unit
def test_oversized_multi_stage_schedule_fails_before_exceeding_batch_budget(
    monkeypatch, tmp_path
):
    state = _state(monkeypatch, tmp_path)
    scope = "CUP-Many Stages=2026"
    plan = state.create_plan(
        queue_id="q",
        selector={},
        scopes=[scope],
        schedule_stage_ids={scope: list(range(1, 16))},
    )

    with pytest.raises(RuntimeError, match="required=1050, limit=1000"):
        state.create_batch(
            "q",
            plan["plan_id"],
            batch_id="scheduled__bounded",
            request_unit_limit=1000,
        )


@pytest.mark.unit
def test_parser_deployment_requires_a_new_repair_plan(monkeypatch, tmp_path):
    state = _state(monkeypatch, tmp_path)
    first = state.create_plan(
        queue_id="q",
        selector={},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
    )
    import dags.scripts.whoscored_ops_store as ops

    deployed = dict(first["policy"])
    deployed["parser_version"] = "whoscored-v-next"
    monkeypatch.setattr(ops, "_policy_identity", lambda: dict(deployed))

    with pytest.raises(RuntimeError, match="plan integrity failed"):
        state.load_plan("q", first["plan_id"])
    replacement = state.create_plan(
        queue_id="q",
        selector={},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
    )
    assert replacement["plan_id"] != first["plan_id"]
    assert replacement["policy"]["parser_version"] == "whoscored-v-next"


@pytest.mark.unit
def test_receipts_resume_exact_25_match_chunks_and_200_profiles(monkeypatch, tmp_path):
    state = _state(monkeypatch, tmp_path)
    plan = state.create_plan(
        queue_id="one-scope",
        selector={},
        scopes=["WS-252-2=2026"],
        schedule_stage_ids={"WS-252-2=2026": [1]},
    )
    plan_id = plan["plan_id"]
    schedule = state.pending_work("one-scope", plan_id)[0]
    state.append_receipt(
        queue_id="one-scope",
        plan_id=plan_id,
        work_item=schedule,
        outcome=_schedule_outcome(
            schedule,
            candidate_game_ids=range(1, 53),
            preview_game_ids=range(1, 53),
        ),
    )
    assert state.work_completed("one-scope", plan_id, schedule["work_id"])

    chunks = state.pending_work("one-scope", plan_id)
    assert [len(item["game_ids"]) for item in chunks] == [25, 25, 2]
    assert MATCH_CHUNK_SIZE == 25
    for item in chunks:
        state.append_receipt(
            queue_id="one-scope",
            plan_id=plan_id,
            work_item=item,
            outcome={"game_ids": item["game_ids"]},
        )

    roster_work = state.pending_work("one-scope", plan_id)
    assert [item["kind"] for item in roster_work] == ["roster"]
    state.append_receipt(
        queue_id="one-scope",
        plan_id=plan_id,
        work_item=roster_work[0],
        outcome={"profile_player_ids": list(range(1000, 1205))},
    )

    profile_work = state.pending_work("one-scope", plan_id)
    assert [len(item["player_ids"]) for item in profile_work] == [200, 5]
    assert PROFILE_CHUNK_SIZE == 200
    for item in profile_work:
        state.append_receipt(
            queue_id="one-scope",
            plan_id=plan_id,
            work_item=item,
            outcome={
                "player_ids": item["player_ids"],
                "attempted": len(item["player_ids"]),
                "succeeded": len(item["player_ids"]),
            },
        )

    progress = state.progress("one-scope", plan_id)
    assert progress["status"] == "complete"
    assert progress["completed_match_chunks"] == 3
    assert progress["completed_roster_freezes"] == 1
    assert progress["completed_profile_chunks"] == 2
    assert progress["completed_profile_players"] == 205
    assert progress["next_work_items"] == 0


@pytest.mark.unit
def test_corrupt_receipt_fails_closed(monkeypatch, tmp_path):
    state = _state(monkeypatch, tmp_path)
    plan = state.create_plan(
        queue_id="q",
        selector={},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
    )
    work = state.pending_work("q", plan["plan_id"])[0]
    receipt = state.append_receipt(
        queue_id="q",
        plan_id=plan["plan_id"],
        work_item=work,
        outcome=_schedule_outcome(
            work, candidate_game_ids=[], preview_game_ids=[]
        ),
    )
    path = Path(receipt["artifact"]["uri"])
    value = json.loads(path.read_text(encoding="utf-8"))
    value["status"] = "failed"
    path.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(RuntimeError, match="receipt"):
        state.pending_work("q", plan["plan_id"])


@pytest.mark.unit
def test_tampered_frozen_candidate_ids_fail_hash_verification(monkeypatch, tmp_path):
    state = _state(monkeypatch, tmp_path)
    plan = state.create_plan(
        queue_id="q",
        selector={},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
    )
    work = state.pending_work("q", plan["plan_id"])[0]
    receipt = state.append_receipt(
        queue_id="q",
        plan_id=plan["plan_id"],
        work_item=work,
        outcome=_schedule_outcome(
            work, candidate_game_ids=[1], preview_game_ids=[1]
        ),
    )
    path = Path(receipt["artifact"]["uri"])
    value = json.loads(path.read_text(encoding="utf-8"))
    value["outcome"]["candidate_game_ids"] = [999999]
    value["outcome"]["preview_game_ids"] = [999999]
    path.write_text(
        json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="content-address integrity"):
        state.pending_work("q", plan["plan_id"])


@pytest.mark.unit
def test_zero_roster_population_completes_without_dynamic_sentinel(
    monkeypatch, tmp_path
):
    state = _state(monkeypatch, tmp_path)
    plan = state.create_plan(
        queue_id="q",
        selector={},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
    )
    schedule = state.pending_work("q", plan["plan_id"])[0]
    state.append_receipt(
        queue_id="q",
        plan_id=plan["plan_id"],
        work_item=schedule,
        outcome=_schedule_outcome(
            schedule, candidate_game_ids=[], preview_game_ids=[]
        ),
    )

    roster = state.pending_work("q", plan["plan_id"])
    assert [item["kind"] for item in roster] == ["roster"]
    state.append_receipt(
        queue_id="q",
        plan_id=plan["plan_id"],
        work_item=roster[0],
        outcome={"profile_player_ids": []},
    )
    assert state.pending_work("q", plan["plan_id"]) == []
    assert state.progress("q", plan["plan_id"])["status"] == "complete"


@pytest.mark.unit
def test_conflicting_receipt_for_same_work_is_rejected(monkeypatch, tmp_path):
    state = _state(monkeypatch, tmp_path)
    plan = state.create_plan(
        queue_id="q",
        selector={},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
    )
    schedule = state.pending_work("q", plan["plan_id"])[0]
    state.append_receipt(
        queue_id="q",
        plan_id=plan["plan_id"],
        work_item=schedule,
        outcome=_schedule_outcome(
            schedule, candidate_game_ids=[1], preview_game_ids=[1]
        ),
    )

    with pytest.raises(RuntimeError, match="conflicting backfill receipt"):
        state.append_receipt(
            queue_id="q",
            plan_id=plan["plan_id"],
            work_item=schedule,
            outcome=_schedule_outcome(
                schedule, candidate_game_ids=[2], preview_game_ids=[2]
            ),
        )


@pytest.mark.unit
def test_equivalent_racing_receipts_collapse_deterministically(monkeypatch, tmp_path):
    state = _state(monkeypatch, tmp_path)
    plan = state.create_plan(
        queue_id="q",
        selector={},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
    )
    work = state.pending_work("q", plan["plan_id"])[0]
    first = state.append_receipt(
        queue_id="q",
        plan_id=plan["plan_id"],
        work_item=work,
        outcome=_schedule_outcome(
            work, candidate_game_ids=[], preview_game_ids=[]
        ),
    )
    duplicate = {key: value for key, value in first.items() if key != "artifact"}
    duplicate["finished_at"] = "2099-01-01T00:00:00+00:00"
    duplicate["airflow"] = {"try_number": "2"}
    payload = (
        json.dumps(duplicate, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )
    digest = hashlib.sha256(payload).hexdigest()
    state.store.put_json_immutable(
        f"backfill/q/receipts/{plan['plan_id']}/{work['work_id']}/{digest}.json",
        duplicate,
    )

    assert len(state.receipts("q", plan["plan_id"])) == 1
    assert (
        state.latest_checkpoint("q", plan["plan_id"])["receipts"][0]["work_id"]
        == work["work_id"]
    )


@pytest.mark.unit
def test_content_addressed_traffic_artifact_is_idempotent(monkeypatch, tmp_path):
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", tmp_path.as_uri())
    store = WhoScoredOpsStore.from_env(optional=False)
    assert store is not None
    first = store.put_content_addressed_json("traffic/dag/run", {"urls": {"a": 1}})
    second = store.put_content_addressed_json("traffic/dag/run", {"urls": {"a": 1}})

    assert first == second
    assert Path(first["uri"]).is_file()


@pytest.mark.unit
def test_materialized_checkpoint_advances_only_one_bounded_batch(monkeypatch, tmp_path):
    state = _state(monkeypatch, tmp_path)
    scopes = [f"WS-{index}=2026" for index in range(150)]
    plan = state.create_plan(
        queue_id="q",
        selector={},
        scopes=scopes,
        schedule_stage_ids=_one_stage_per_scope(scopes),
    )
    plan_id = plan["plan_id"]

    first = state.create_batch(
        "q",
        plan_id,
        batch_id="scheduled__one",
        request_unit_limit=1000,
    )
    assert len(first["work_items"]) == 14
    assert first["request_units"] == 980
    assert (
        state.create_batch(
            "q",
            plan_id,
            batch_id="scheduled__one",
            request_unit_limit=1000,
        )["artifact"]
        == first["artifact"]
    )

    for item in first["work_items"]:
        state.append_receipt(
            queue_id="q",
            plan_id=plan_id,
            work_item=item,
            outcome=_schedule_outcome(
                item, candidate_game_ids=[], preview_game_ids=[]
            ),
        )
    progress = state.advance_batch("q", plan_id, batch_id="scheduled__one")
    assert progress["successful_receipts"] == 14
    assert progress["checkpoint_generation"] == 1

    # The normal continuation path must never rebuild by listing/getting every
    # historical receipt after generation zero has been materialized.
    monkeypatch.setattr(
        state,
        "receipts",
        lambda *_args, **_kwargs: pytest.fail("unexpected full receipt rebuild"),
    )
    second = state.create_batch(
        "q",
        plan_id,
        batch_id="scheduled__two",
        request_unit_limit=1000,
    )
    assert len(second["work_items"]) == 14
    assert not (
        {item["work_id"] for item in first["work_items"]}
        & {item["work_id"] for item in second["work_items"]}
    )
    for item in second["work_items"]:
        state.append_receipt(
            queue_id="q",
            plan_id=plan_id,
            work_item=item,
            outcome=_schedule_outcome(
                item, candidate_game_ids=[], preview_game_ids=[]
            ),
        )
    second_progress = state.advance_batch(
        "q", plan_id, batch_id="scheduled__two"
    )
    assert second_progress["successful_receipts"] == 28
    assert second_progress["checkpoint_generation"] == 2


@pytest.mark.unit
def test_checkpoint_tampering_and_generation_conflicts_fail_closed(
    monkeypatch, tmp_path
):
    state = _state(monkeypatch, tmp_path)
    plan = state.create_plan(
        queue_id="q",
        selector={},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
    )
    checkpoint = state.latest_checkpoint("q", plan["plan_id"])
    path = Path(checkpoint["artifact"]["uri"])
    value = json.loads(path.read_text(encoding="utf-8"))
    value["generation"] = 99
    path.write_text(
        json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="content-addressed"):
        state.latest_checkpoint("q", plan["plan_id"])


@pytest.mark.unit
def test_v2_checkpoint_is_read_and_migrated_without_changing_public_api(
    monkeypatch, tmp_path
):
    state = _state(monkeypatch, tmp_path)
    plan = state.create_plan(
        queue_id="legacy",
        selector={},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
    )
    schedule = state.pending_work("legacy", plan["plan_id"])[0]
    receipt = state.append_receipt(
        queue_id="legacy",
        plan_id=plan["plan_id"],
        work_item=schedule,
        outcome=_schedule_outcome(
            schedule, candidate_game_ids=[], preview_game_ids=[]
        ),
    )
    stored_receipt = {key: value for key, value in receipt.items() if key != "artifact"}
    legacy = {
        "schema_version": 1,
        "checkpoint_version": LEGACY_BACKFILL_CHECKPOINT_VERSION,
        "queue_id": "legacy",
        "plan_id": plan["plan_id"],
        "generation": 7,
        "parent_sha256": None,
        "created_at": stored_receipt["finished_at"],
        "receipts": [stored_receipt],
    }
    payload = json.dumps(legacy, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    digest = hashlib.sha256(payload).hexdigest()
    state.store.put_json_immutable(
        f"backfill/legacy/checkpoints/{plan['plan_id']}/000000000007/{digest}.json",
        legacy,
    )

    public = state.latest_checkpoint("legacy", plan["plan_id"])
    assert public["checkpoint_version"] == LEGACY_BACKFILL_CHECKPOINT_VERSION
    assert public["receipts"] == [stored_receipt]

    batch = state.create_batch(
        "legacy",
        plan["plan_id"],
        batch_id="scheduled__migrate",
        request_unit_limit=1000,
    )
    assert [item["kind"] for item in batch["work_items"]] == ["roster"]
    roster = batch["work_items"][0]
    state.append_receipt(
        queue_id="legacy",
        plan_id=plan["plan_id"],
        work_item=roster,
        outcome={"profile_player_ids": []},
    )
    migrated = state.advance_batch(
        "legacy", plan["plan_id"], batch_id="scheduled__migrate"
    )
    checkpoint = state._latest_frontier_checkpoint("legacy", plan["plan_id"])

    assert migrated["checkpoint_generation"] == 8
    assert checkpoint["checkpoint_version"] == BACKFILL_CHECKPOINT_VERSION
    assert checkpoint["deltas"] == []
    assert len(state.latest_checkpoint("legacy", plan["plan_id"])["receipts"]) == 2


@pytest.mark.unit
def test_segment_delta_tampering_fails_closed(monkeypatch, tmp_path):
    state = _state(monkeypatch, tmp_path)
    plan = state.create_plan(
        queue_id="delta-tamper",
        selector={},
        scopes=["WS-1=2026"],
        schedule_stage_ids={"WS-1=2026": [1]},
    )
    batch = state.create_batch(
        "delta-tamper",
        plan["plan_id"],
        batch_id="scheduled__one",
        request_unit_limit=1000,
    )
    schedule = batch["work_items"][0]
    state.append_receipt(
        queue_id="delta-tamper",
        plan_id=plan["plan_id"],
        work_item=schedule,
        outcome=_schedule_outcome(
            schedule, candidate_game_ids=[], preview_game_ids=[]
        ),
    )
    state.advance_batch(
        "delta-tamper", plan["plan_id"], batch_id="scheduled__one"
    )
    checkpoint = state._latest_frontier_checkpoint(
        "delta-tamper", plan["plan_id"]
    )
    delta_path = Path(state.store.object_uri(checkpoint["deltas"][0]["key"]))
    value = json.loads(delta_path.read_text(encoding="utf-8"))
    value["generation"] = 999
    delta_path.write_text(
        json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="content-addressed"):
        state.checkpoint_progress("delta-tamper", plan["plan_id"])


@pytest.mark.unit
def test_75k_frontier_writes_only_bounded_delta_and_does_not_scan_receipts(
    monkeypatch, tmp_path
):
    state = _state(monkeypatch, tmp_path)
    plan = {
        "queue_id": "cardinality",
        "plan_id": "a" * 64,
        "scopes": ["WS-1=2026"],
        "provenance": {},
    }
    work_ids = sorted(f"matches-{index:075d}" for index in range(75_000))
    frontier = state._empty_frontier()
    frontier["completed_work_ids"] = work_ids
    frontier["completed_by_kind"]["matches"] = len(work_ids)
    frontier["estimated_completed_request_units"] = len(work_ids)
    frontier["actual_completed_request_units"] = len(work_ids)
    snapshot_digest = "b" * 64
    parent = {
        "checkpoint_version": BACKFILL_CHECKPOINT_VERSION,
        "created_at": "2026-01-01T00:00:00+00:00",
        "frontier_sha256": "c" * 64,
        "frontier": frontier,
        "snapshot": {
            "key": (
                "backfill/cardinality/checkpoint-data/"
                f"{'a' * 64}/snapshots/000000000000/{snapshot_digest}.json"
            ),
            "sha256": snapshot_digest,
            "bytes": 1,
        },
        "deltas": [],
    }
    receipt = {
        "work_id": "matches-next",
        "kind": "matches",
        "finished_at": "2026-01-02T00:00:00+00:00",
        "work_item": {
            "kind": "matches",
            "game_ids": [1],
            "preview_game_ids": [],
        },
        "outcome": {"game_ids": [1]},
    }
    monkeypatch.setattr(
        state,
        "_validate_receipt_value",
        lambda *, receipt, plan: dict(receipt),
    )

    class CumulativeReceiptsMustNotBeRead(list):
        def __iter__(self):
            raise AssertionError("normal generation scanned cumulative receipts")

    checkpoint = state._write_checkpoint(
        plan=plan,
        receipts=CumulativeReceiptsMustNotBeRead(),
        generation=1,
        parent_sha256="d" * 64,
        parent_checkpoint=parent,
        delta_receipts=[receipt],
    )

    assert checkpoint["receipt_count"] == 75_001
    assert checkpoint["snapshot"] == parent["snapshot"]
    assert len(checkpoint["deltas"]) == 1
    assert checkpoint["artifact"]["bytes"] < 2_000
    assert checkpoint["deltas"][0]["bytes"] < 2_000


@pytest.mark.unit
def test_latest_lookup_is_bounded_for_1700_immutable_generations(
    monkeypatch, tmp_path
):
    state = _state(monkeypatch, tmp_path)
    queue_id = "radix"
    plan_id = "f" * 64
    for generation in range(1_700):
        prefix = state._checkpoint_manifest_prefix(
            queue_id, plan_id, generation
        )
        directory = Path(state.store._path(prefix))
        directory.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(str(generation).encode("ascii")).hexdigest()
        (directory / f"{digest}.json").write_text("{}\n", encoding="utf-8")

    observed_cardinalities = []
    original = state.store.list_children

    def counted(prefix):
        children = original(prefix)
        observed_cardinalities.append(len(children))
        return children

    monkeypatch.setattr(state.store, "list_children", counted)
    latest = state._latest_segmented_manifest_key(queue_id, plan_id)

    assert latest is not None
    assert latest[0] == 1_699
    assert len(observed_cardinalities) == 13
    assert max(observed_cardinalities) <= 10


@pytest.mark.unit
def test_segment_chain_is_periodically_compacted(monkeypatch, tmp_path):
    state = _state(monkeypatch, tmp_path)
    plan = {
        "queue_id": "compact",
        "plan_id": "e" * 64,
        "scopes": ["WS-1=2026"],
        "provenance": {},
    }
    monkeypatch.setattr(
        state,
        "_validate_receipt_value",
        lambda *, receipt, plan: dict(receipt),
    )
    checkpoint = state._write_checkpoint(
        plan=plan,
        receipts=[],
        generation=0,
        parent_sha256=None,
    )
    for generation in range(1, CHECKPOINT_DELTAS_PER_SNAPSHOT + 1):
        receipt = {
            "work_id": f"matches-{generation}",
            "kind": "matches",
            "finished_at": f"2026-01-01T00:00:{generation:02d}+00:00",
            "work_item": {
                "kind": "matches",
                "game_ids": [generation],
                "preview_game_ids": [],
            },
            "outcome": {"game_ids": [generation]},
        }
        checkpoint = state._write_checkpoint(
            plan=plan,
            receipts=[],
            generation=generation,
            parent_sha256=checkpoint["artifact"]["sha256"],
            parent_checkpoint=checkpoint,
            delta_receipts=[receipt],
        )
        assert len(checkpoint["deltas"]) < CHECKPOINT_DELTAS_PER_SNAPSHOT

    assert checkpoint["generation"] == CHECKPOINT_DELTAS_PER_SNAPSHOT
    assert checkpoint["deltas"] == []
    assert f"/{CHECKPOINT_DELTAS_PER_SNAPSHOT:012d}/" in checkpoint["snapshot"]["key"]
