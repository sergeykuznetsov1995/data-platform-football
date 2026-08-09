import copy
import uuid

import pytest

from scrapers.fbref.control.replay_effects import (
    build_replay_control_effects,
    make_replay_control_refresh_id,
    make_replay_control_target_id,
    normalize_replay_control_effects,
)


def _effects(*, targets=2):
    replay_run_id = str(uuid.uuid4())
    source_run_id = str(uuid.uuid4())
    target_rows = []
    dataset_rows = []
    for ordinal in range(targets):
        match_id = f"match-{ordinal}"
        target_id = f"fbref:match:{match_id}"
        source_refresh_id = str(uuid.uuid4())
        replay_target_id = make_replay_control_target_id(
            replay_run_id, target_id
        )
        target_rows.append(
            {
                "ordinal": ordinal,
                "target_id": target_id,
                "replay_target_id": replay_target_id,
                "source_logical_refresh_id": source_refresh_id,
                "logical_refresh_id": make_replay_control_refresh_id(
                    replay_run_id, source_refresh_id
                ),
                "status": "succeeded",
                "target_status": "succeeded",
                "page_kind": "match",
                "source_ids": {"match_id": match_id},
                "frontier_state": "fetched",
                "last_content_hash": "a" * 64,
                "content_hash": "a" * 64,
                "parser_version": "generic-v1",
                "typed_parser_version": "typed-v1",
                "stateful_parser_version": "stateful-v1",
                "observation_status": "succeeded",
                "generic_status": "succeeded",
                "typed_status": "succeeded",
                "stateful_status": "skipped",
                "validation_status": "succeeded",
                "evidence_class": "full_match",
                "latest_guarded": True,
            }
        )
        dataset_rows.extend(
            {
                "ordinal": ordinal,
                "target_id": target_id,
                "replay_target_id": replay_target_id,
                "content_hash": "a" * 64,
                "parser_version": "typed-v1",
                "dataset": f"typed:{dataset}",
                "availability": "available",
                "parse_status": "succeeded",
                "persistence_status": "succeeded",
                "validation_status": "succeeded",
                "row_count": 1,
                "empty_reason": None,
            }
            for dataset in (
                "shot_events",
                "match_events",
                "lineups",
                "match_team_stats",
                "match_managers",
                "match_officials",
                "match_keeper_stats",
                "match_player_stats",
            )
        )
    return build_replay_control_effects(
        control_run_id=replay_run_id,
        source_run_id=source_run_id,
        mode="batch",
        targets=target_rows,
        datasets=dataset_rows,
    )


@pytest.mark.unit
def test_replay_effects_bind_exact_target_and_refresh_lineage():
    effects = _effects()

    normalized = normalize_replay_control_effects(effects)

    assert normalized == effects
    assert len(normalized["targets"]) == 2
    for item in normalized["targets"]:
        assert item["replay_target_id"] == make_replay_control_target_id(
            normalized["control_run_id"], item["target_id"]
        )
        assert item["logical_refresh_id"] == (
            make_replay_control_refresh_id(
                normalized["control_run_id"],
                item["source_logical_refresh_id"],
            )
        )


@pytest.mark.unit
def test_replay_effects_reject_a_swapped_refresh_even_with_a_new_digest():
    effects = _effects()
    targets = copy.deepcopy(effects["targets"])
    targets[1]["logical_refresh_id"] = targets[0]["logical_refresh_id"]

    with pytest.raises(ValueError, match="logical refresh"):
        build_replay_control_effects(
            control_run_id=effects["control_run_id"],
            source_run_id=effects["source_run_id"],
            mode=effects["mode"],
            targets=targets,
            datasets=effects["datasets"],
        )


@pytest.mark.unit
def test_replay_effects_reject_a_changed_artifact_digest():
    effects = _effects()
    effects["artifact_sha256"] = "0" * 64

    with pytest.raises(ValueError, match="digest"):
        normalize_replay_control_effects(effects)


@pytest.mark.unit
def test_replay_effects_preserve_incomplete_actual_outcomes_for_the_gate():
    effects = _effects()
    targets = copy.deepcopy(effects["targets"])
    targets[0]["observation_status"] = "processing"
    targets[0]["latest_guarded"] = False
    datasets = effects["datasets"][:-1]

    incomplete = build_replay_control_effects(
        control_run_id=effects["control_run_id"],
        source_run_id=effects["source_run_id"],
        mode=effects["mode"],
        targets=targets,
        datasets=datasets,
    )

    assert incomplete["targets"][0]["observation_status"] == "processing"
    assert incomplete["targets"][0]["latest_guarded"] is False
    assert len(incomplete["datasets"]) == len(effects["datasets"]) - 1
