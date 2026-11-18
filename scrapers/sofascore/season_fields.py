"""Parsers for the four SofaScore season paid fields (#1367).

Turns paid-endpoint payloads (rounds, cup trees, participants, referee
profile) into flat Bronze rows: source ids are attached as strings, remaining
scalars are flattened with :func:`scrapers.sofascore._flatten._auto_flatten`,
and natural keys are validated before any row leaves the module.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from scrapers.sofascore._flatten import _auto_flatten

__all__ = [
    "assert_natural_keys",
    "parse_cup_trees",
    "parse_participants",
    "parse_referee_profile",
    "parse_rounds",
]


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _payload_object(payload: Any, endpoint: str) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError(f"{endpoint}: payload must be an object, got {type(payload).__name__}")
    return payload


def _source_list(payload: Mapping[str, Any], field: str, endpoint: str) -> list[Any]:
    value = payload.get(field)
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{endpoint}: field {field!r} must be a list, got {type(value).__name__}")
    return value


def _required_mapping(item: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(item, Mapping):
        raise ValueError(f"{where}: expected an object, got {type(item).__name__}")
    return item


def _required_id(item: Mapping[str, Any], field: str, where: str) -> str:
    value = item.get(field)
    if value is None:
        raise ValueError(f"{where}: missing required field {field!r}")
    return str(value)


def assert_natural_keys(rows: list[dict[str, Any]], key_fields: tuple[str, ...]) -> None:
    """Raise ValueError on missing/null natural keys or duplicate key tuples."""
    seen: set[tuple[Any, ...]] = set()
    for index, row in enumerate(rows):
        values: list[Any] = []
        for field in key_fields:
            value = row.get(field)
            if value is None:
                raise ValueError(f"rows[{index}]: natural key field {field!r} is missing or null")
            values.append(value)
        key = tuple(values)
        if key in seen:
            raise ValueError(f"rows[{index}]: duplicate natural key {key} for fields {key_fields}")
        seen.add(key)
    return None


def _round_is_current(item: Mapping[str, Any], current_round: Mapping[str, Any]) -> bool:
    if not current_round:
        return False
    if current_round.get("round") != item.get("round"):
        return False
    current_name = current_round.get("name")
    if current_name is not None and current_name != item.get("name"):
        return False
    return True


def parse_rounds(
    payload: Mapping[str, Any],
    *,
    source_tournament_id: Any,
    source_season_id: Any,
) -> list[dict[str, Any]]:
    """Parse the season ``rounds`` payload into one row per round."""
    endpoint = "rounds"
    data = _payload_object(payload, endpoint)
    items = _source_list(data, "rounds", endpoint)
    current_round = data.get("currentRound")
    if current_round is None:
        current_round = {}
    if not isinstance(current_round, Mapping):
        raise ValueError(
            f"{endpoint}: field 'currentRound' must be an object, got {type(current_round).__name__}"
        )
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(items):
        where = f"{endpoint}[{index}]"
        _required_mapping(item, where)
        row = {
            "round_id": _required_id(item, "round", where),
            "source_tournament_id": str(source_tournament_id),
            "source_season_id": str(source_season_id),
        }
        _auto_flatten(item, row)
        row["is_current"] = _round_is_current(item, current_round)
        rows.append(row)
    assert_natural_keys(rows, ("round_id",))
    return rows


def _collect_cup_tree_node(
    rows: list[dict[str, Any]],
    visited: set[tuple[str, str]],
    node: Any,
    *,
    cup_tree_id: str,
    parent_node_id: str | None,
    source_tournament_id: str,
    source_season_id: str,
    where: str,
) -> None:
    _required_mapping(node, where)
    node_id = _required_id(node, "id", where)
    if (cup_tree_id, node_id) in visited:
        return
    visited.add((cup_tree_id, node_id))
    row = {
        "cup_tree_id": cup_tree_id,
        "node_id": node_id,
        "source_tournament_id": source_tournament_id,
        "source_season_id": source_season_id,
    }
    _auto_flatten(node, row)
    row["parent_node_id"] = parent_node_id
    children = node.get("children")
    if children is None:
        children = []
    if not isinstance(children, list):
        raise ValueError(f"{where}: field 'children' must be a list, got {type(children).__name__}")
    row["children_json"] = _canonical_json(children)
    rows.append(row)
    for child_index, child in enumerate(children):
        child_where = f"{where}.children[{child_index}]"
        if isinstance(child, Mapping):
            _collect_cup_tree_node(
                rows,
                visited,
                child,
                cup_tree_id=cup_tree_id,
                parent_node_id=node_id,
                source_tournament_id=source_tournament_id,
                source_season_id=source_season_id,
                where=child_where,
            )
        elif isinstance(child, bool) or not isinstance(child, (str, int)):
            raise ValueError(
                f"{child_where}: child must be a scalar id or an object, got {type(child).__name__}"
            )
    return None


def parse_cup_trees(
    payload: Mapping[str, Any],
    *,
    source_tournament_id: Any,
    source_season_id: Any,
) -> list[dict[str, Any]]:
    """Parse the season ``cup-trees`` payload into one row per bracket node."""
    endpoint = "cup_trees"
    data = _payload_object(payload, endpoint)
    source_tournament = str(source_tournament_id)
    source_season = str(source_season_id)
    rows: list[dict[str, Any]] = []
    visited: set[tuple[str, str]] = set()
    for tree_index, tree in enumerate(_source_list(data, "cupTrees", endpoint)):
        tree_where = f"{endpoint}[{tree_index}]"
        _required_mapping(tree, tree_where)
        cup_tree_id = _required_id(tree, "id", tree_where)
        nodes = tree.get("nodes")
        if nodes is None:
            nodes = []
        if not isinstance(nodes, list):
            raise ValueError(f"{tree_where}: field 'nodes' must be a list, got {type(nodes).__name__}")
        for node_index, node in enumerate(nodes):
            _collect_cup_tree_node(
                rows,
                visited,
                node,
                cup_tree_id=cup_tree_id,
                parent_node_id=None,
                source_tournament_id=source_tournament,
                source_season_id=source_season,
                where=f"{tree_where}.nodes[{node_index}]",
            )
    assert_natural_keys(rows, ("cup_tree_id", "node_id"))
    return rows


def parse_participants(
    payload: Mapping[str, Any],
    *,
    source_tournament_id: Any,
    source_season_id: Any,
) -> list[dict[str, Any]]:
    """Parse the season participants payload into one row per team."""
    endpoint = "participants"
    data = _payload_object(payload, endpoint)
    rows: list[dict[str, Any]] = []
    for index, team in enumerate(_source_list(data, "teams", endpoint)):
        where = f"{endpoint}[{index}]"
        _required_mapping(team, where)
        row = {
            "team_id": _required_id(team, "id", where),
            "source_tournament_id": str(source_tournament_id),
            "source_season_id": str(source_season_id),
        }
        _auto_flatten(team, row)
        rows.append(row)
    assert_natural_keys(rows, ("team_id",))
    return rows


def parse_referee_profile(
    payload: Mapping[str, Any],
    *,
    source_tournament_id: Any,
    source_season_id: Any,
    requested_referee_id: Any,
) -> list[dict[str, Any]]:
    """Parse a referee profile payload into a single row ([] when referee is null)."""
    endpoint = "referee_profile"
    data = _payload_object(payload, endpoint)
    referee = data.get("referee")
    if referee is None:
        return []
    if requested_referee_id is None:
        raise ValueError(f"{endpoint}: requested_referee_id must not be None")
    referee_where = f"{endpoint}.referee"
    _required_mapping(referee, referee_where)
    row = {
        "requested_referee_id": str(requested_referee_id),
        "referee_id": _required_id(referee, "id", referee_where),
        "slug": referee.get("slug"),
        "source_tournament_id": str(source_tournament_id),
        "source_season_id": str(source_season_id),
    }
    _auto_flatten(referee, row)
    rows = [row]
    assert_natural_keys(rows, ("requested_referee_id",))
    return rows
