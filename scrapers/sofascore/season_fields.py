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


def _round_id(item: Mapping[str, Any], where: str) -> str:
    round_id = _required_id(item, "round", where)
    # A season split into stages restarts the round numbering in every stage,
    # so a stage prefix the source provides is part of the identity (#2470).
    prefix = item.get("prefix")
    if prefix is not None and str(prefix).strip():
        round_id = f"{str(prefix).strip()}:{round_id}"
    slug = item.get("slug")
    if slug is None or not str(slug).strip():
        return round_id
    return f"{round_id}:{str(slug).strip()}"


def _round_is_current(item: Mapping[str, Any], current_round: Mapping[str, Any]) -> bool:
    if not current_round:
        return False
    if current_round.get("round") != item.get("round"):
        return False
    current_name = current_round.get("name")
    if current_name is not None and current_name != item.get("name"):
        return False
    # The same round number exists once per stage; a currentRound that names
    # its stage does not make that number of another stage current.
    current_prefix = current_round.get("prefix")
    if current_prefix is not None and current_prefix != item.get("prefix"):
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
            "round_id": _round_id(item, where),
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


def _canonical_list(
    block: Mapping[str, Any], field: str, where: str
) -> str:
    """Canonical JSON of an optional list field; ``[]`` only for null/absent.

    A malformed non-list value (object, scalar) raises instead of silently
    encoding as a JSON object/number — a scalar ``events`` id must not
    masquerade as an empty list (#1367 review adjudication).
    """
    value = block.get(field)
    if value is None:
        return "[]"
    if not isinstance(value, list):
        raise ValueError(
            f"{where}: field {field!r} must be a list, "
            f"got {type(value).__name__}"
        )
    return _canonical_json(value)


def _collect_cup_tree_block(
    rows: list[dict[str, Any]],
    visited: set[tuple[str, str]],
    rows_by_node: dict[str, dict[str, Any]],
    block: Any,
    *,
    cup_tree_id: str,
    source_tournament_id: str,
    source_season_id: str,
    where: str,
    block_kind: str,
    round_context: Mapping[str, Any] | None = None,
) -> None:
    """Append one row per bracket block of a rounds-shaped cup tree.

    ``node_id`` is the block id — blocks ARE the bracket nodes in the
    rounds/blocks payload shape (World Cup 2026).  A standalone match block
    (``finalMatchCupBlock``/``thirdPlaceMatchCupBlock``) repeating a round
    block's id does NOT drop its explicit role onto the floor: the existing
    round row keeps its round context and takes the standalone
    ``block_kind`` (#1367 review adjudication).
    """
    _required_mapping(block, where)
    node_id = _required_id(block, "id", where)
    # Validate the list fields on EVERY observation — including a repeated
    # standalone block whose row is already collected: the duplicate branch
    # must not become a validation bypass (review r1).
    participants_json = _canonical_list(block, "participants", where)
    events_json = _canonical_list(block, "events", where)
    if (cup_tree_id, node_id) in visited:
        existing = rows_by_node.get(node_id)
        if existing is not None and block_kind in ("final", "third_place"):
            existing["block_kind"] = block_kind
        return
    visited.add((cup_tree_id, node_id))
    row = {
        "cup_tree_id": cup_tree_id,
        "node_id": node_id,
        "source_tournament_id": source_tournament_id,
        "source_season_id": source_season_id,
        "block_kind": block_kind,
    }
    if round_context is not None:
        row.update(round_context)
    _auto_flatten(block, row)
    row["parent_node_id"] = None
    row["children_json"] = "[]"
    row["participants_json"] = participants_json
    row["events_json"] = events_json
    rows.append(row)
    rows_by_node[node_id] = row
    return None


def _collect_rounds_shaped_tree(
    tree: Mapping[str, Any],
    rows: list[dict[str, Any]],
    visited: set[tuple[str, str]],
    rows_by_node: dict[str, dict[str, Any]],
    *,
    cup_tree_id: str,
    source_tournament_id: str,
    source_season_id: str,
    tree_where: str,
) -> None:
    rounds = tree.get("rounds")
    if rounds is None:
        rounds = []
    if not isinstance(rounds, list):
        raise ValueError(
            f"{tree_where}: field 'rounds' must be a list, "
            f"got {type(rounds).__name__}"
        )
    for round_index, round_item in enumerate(rounds):
        round_where = f"{tree_where}.rounds[{round_index}]"
        _required_mapping(round_item, round_where)
        round_id = _required_id(round_item, "id", round_where)
        round_context = {
            "round_id": round_id,
            "round_order": round_item.get("order"),
            "round_description": round_item.get("description"),
            "round_type": round_item.get("type"),
        }
        blocks = round_item.get("blocks")
        if blocks is None:
            blocks = []
        if not isinstance(blocks, list):
            raise ValueError(
                f"{round_where}: field 'blocks' must be a list, "
                f"got {type(blocks).__name__}"
            )
        for block_index, block in enumerate(blocks):
            _collect_cup_tree_block(
                rows,
                visited,
                rows_by_node,
                block,
                cup_tree_id=cup_tree_id,
                source_tournament_id=source_tournament_id,
                source_season_id=source_season_id,
                where=f"{round_where}.blocks[{block_index}]",
                block_kind="round",
                round_context=round_context,
            )
    for field, kind in (
        ("finalMatchCupBlock", "final"),
        ("thirdPlaceMatchCupBlock", "third_place"),
    ):
        block = tree.get(field)
        if block is None:
            continue
        _collect_cup_tree_block(
            rows,
            visited,
            rows_by_node,
            block,
            cup_tree_id=cup_tree_id,
            source_tournament_id=source_tournament_id,
            source_season_id=source_season_id,
            where=f"{tree_where}.{field}",
            block_kind=kind,
        )
    return None


def parse_cup_trees(
    payload: Mapping[str, Any],
    *,
    source_tournament_id: Any,
    source_season_id: Any,
) -> list[dict[str, Any]]:
    """Parse the season ``cup-trees`` payload into one row per bracket node.

    Two payload shapes (#1367): the ``nodes`` tree (league cups; nested
    nodes with ``children``) and the ``rounds``/blocks tree (World Cup
    2026; one block per match plus standalone final/third-place blocks —
    the blocks are the bracket nodes, so each becomes one row under the
    same ``(cup_tree_id, node_id)`` natural key).  A tree carrying a
    non-empty ``nodes`` list keeps the nodes branch byte-for-byte; the
    blocks branch only runs when ``nodes`` is absent or empty.
    """
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
        if nodes:
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
            continue
        _collect_rounds_shaped_tree(
            tree,
            rows,
            visited,
            {},
            cup_tree_id=cup_tree_id,
            source_tournament_id=source_tournament,
            source_season_id=source_season,
            tree_where=tree_where,
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
