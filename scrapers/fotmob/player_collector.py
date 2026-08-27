"""Immutable contract for the bounded missing-player collector."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable


PLAYER_COLLECTOR_MODE = "players"
PLAYER_COLLECTOR_PROFILE = "missing_current_squad_players_v1"
PLAYER_COLLECTOR_ENTITIES = ("players",)
PLAYER_COLLECTOR_PLAYER_LIMIT = 100
PLAYER_COLLECTOR_MAX_REQUESTS = 500
PLAYER_COLLECTOR_MAX_DIRECT_MIB = 64
PLAYER_COLLECTOR_REQUESTS_PER_MINUTE = 30


def normalize_player_collector_ids(player_ids: Iterable[int]) -> tuple[int, ...]:
    """Return the canonical positive, unique, numeric player identity set."""

    normalized: set[int] = set()
    for value in player_ids:
        if type(value) is not int or value < 1:
            raise ValueError("player collector IDs must be positive integers")
        normalized.add(value)
    return tuple(sorted(normalized))


def player_collector_ids_sha256(player_ids: Iterable[int]) -> str:
    normalized = normalize_player_collector_ids(player_ids)
    material = (
        ("\n".join(str(player_id) for player_id in normalized) + "\n").encode()
        if normalized
        else b""
    )
    return hashlib.sha256(material).hexdigest()


def player_collector_plan_signature(player_ids: Iterable[int]) -> str:
    normalized = normalize_player_collector_ids(player_ids)
    material = json.dumps(
        {
            "entities": list(PLAYER_COLLECTOR_ENTITIES),
            "player_count": len(normalized),
            "player_ids_sha256": player_collector_ids_sha256(normalized),
            "profile": PLAYER_COLLECTOR_PROFILE,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return "fmplan1-" + hashlib.sha256(material).hexdigest()


__all__ = [
    "PLAYER_COLLECTOR_ENTITIES",
    "PLAYER_COLLECTOR_MAX_DIRECT_MIB",
    "PLAYER_COLLECTOR_MAX_REQUESTS",
    "PLAYER_COLLECTOR_MODE",
    "PLAYER_COLLECTOR_PLAYER_LIMIT",
    "PLAYER_COLLECTOR_PROFILE",
    "PLAYER_COLLECTOR_REQUESTS_PER_MINUTE",
    "normalize_player_collector_ids",
    "player_collector_ids_sha256",
    "player_collector_plan_signature",
]
