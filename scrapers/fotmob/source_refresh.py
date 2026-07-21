"""Immutable issue-930 player source-refresh contract.

The normal replay path is strictly offline.  This one reviewed profile is the
only exception used to fill seven raw/manifest gaps that replay proved.  The
artifact binds every target to its competition, exact source season, team and
player identity so an operator cannot widen the refresh into a squad or
catalog crawl.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


PLAYER_SOURCE_REFRESH_PROFILE = "issue930-player-source-refresh-v1"
PLAYER_SOURCE_REFRESH_SCHEMA = "fotmob-issue-930-player-source-refresh-v1"
REPLAY_MISSING_INPUT_PROOF_SCHEMA = "fotmob-replay-missing-player-inputs-v1"
REPLAY_MISSING_INPUT_PROOF_TASK_ID = "capture_replay_missing_inputs"
PLAYER_SOURCE_REFRESH_ARTIFACT = "configs/fotmob/issue-930-player-source-refresh.json"
PLAYER_SOURCE_REFRESH_SHA256 = (
    "f6cb854c6d60463c899fd9077b61a71d8d0f817741c3a9d6423925b32949045b"
)
PLAYER_SOURCE_REFRESH_TARGET_COUNT = 7

# A stale Next build can require two build-id discoveries and two passes over
# all seven targets.  Each transport call can reserve four attempts, so 64 is
# the smallest conservative ceiling (2 + 14 calls, each at four attempts).
# The exact target artifact still prevents that retry capacity widening scope.
PLAYER_SOURCE_REFRESH_MAX_REQUESTS = 64
PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB = 8


class PlayerSourceRefreshContractError(ValueError):
    pass


def player_source_refresh_plan_signature() -> str:
    """Bind the exceptional runner plan to the exact reviewed target bytes."""

    from scrapers.fotmob.planner import deterministic_plan_signature

    return deterministic_plan_signature(
        {"players"},
        policy={
            "profile": PLAYER_SOURCE_REFRESH_PROFILE,
            "targets_sha256": PLAYER_SOURCE_REFRESH_SHA256,
        },
    )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise PlayerSourceRefreshContractError(
                f"duplicate source-refresh JSON key: {key!r}"
            )
        output[key] = value
    return output


def load_player_source_refresh_contract(path: Path) -> dict[str, Any]:
    """Load the byte-exact reviewed seven-target remediation profile."""

    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise PlayerSourceRefreshContractError(
            f"cannot read player source-refresh artifact: {path}"
        ) from exc
    digest = hashlib.sha256(raw).hexdigest()
    if digest != PLAYER_SOURCE_REFRESH_SHA256:
        raise PlayerSourceRefreshContractError(
            "player source-refresh artifact differs from its reviewed SHA-256"
        )
    try:
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PlayerSourceRefreshContractError(
            "player source-refresh artifact is not canonical UTF-8 JSON"
        ) from exc
    canonical = (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    if raw != canonical:
        raise PlayerSourceRefreshContractError(
            "player source-refresh artifact is not canonical JSON bytes"
        )
    if not isinstance(payload, Mapping) or set(payload) != {
        "schema_version",
        "targets",
    }:
        raise PlayerSourceRefreshContractError(
            "player source-refresh artifact has unexpected top-level fields"
        )
    if payload.get("schema_version") != PLAYER_SOURCE_REFRESH_SCHEMA:
        raise PlayerSourceRefreshContractError(
            "player source-refresh schema version differs"
        )
    raw_targets = payload.get("targets")
    if not isinstance(raw_targets, list) or len(raw_targets) != (
        PLAYER_SOURCE_REFRESH_TARGET_COUNT
    ):
        raise PlayerSourceRefreshContractError(
            "player source-refresh artifact must contain exactly seven targets"
        )

    targets: list[dict[str, Any]] = []
    identities: set[tuple[int, str, int, int]] = set()
    player_ids: set[int] = set()
    for index, item in enumerate(raw_targets):
        if not isinstance(item, Mapping) or set(item) != {
            "competition_id",
            "source_season_key",
            "team_id",
            "player_id",
        }:
            raise PlayerSourceRefreshContractError(
                f"player source-refresh target {index} has unexpected fields"
            )
        competition_id = item.get("competition_id")
        source_season_key = item.get("source_season_key")
        team_id = item.get("team_id")
        player_id = item.get("player_id")
        if (
            type(competition_id) is not int
            or competition_id <= 0
            or not isinstance(source_season_key, str)
            or not source_season_key
            or source_season_key != source_season_key.strip()
            or type(team_id) is not int
            or team_id <= 0
            or type(player_id) is not int
            or player_id <= 0
        ):
            raise PlayerSourceRefreshContractError(
                f"player source-refresh target {index} has invalid identity"
            )
        identity = (competition_id, source_season_key, team_id, player_id)
        if identity in identities or player_id in player_ids:
            raise PlayerSourceRefreshContractError(
                "player source-refresh target identities must be unique"
            )
        identities.add(identity)
        player_ids.add(player_id)
        targets.append(
            {
                "competition_id": competition_id,
                "source_season_key": source_season_key,
                "team_id": team_id,
                "player_id": player_id,
            }
        )

    return {
        "profile": PLAYER_SOURCE_REFRESH_PROFILE,
        "artifact": PLAYER_SOURCE_REFRESH_ARTIFACT,
        "sha256": digest,
        "target_count": len(targets),
        "targets": targets,
        "player_ids": sorted(player_ids),
        "plan_signature": player_source_refresh_plan_signature(),
    }
