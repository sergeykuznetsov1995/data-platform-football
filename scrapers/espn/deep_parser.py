"""Strict core plays/roster/statistics parsing for the deep stages (#1513)."""
from __future__ import annotations

from dataclasses import dataclass
import math
import re
from typing import Callable

from .core_lists import _count, raise_on_error_body
from .parser_common import (EspnParseError, canonical_json, native_id, required_list,
                            required_mapping)

PARSER_VERSION = "espn-deep-v2"
# Derived names (expectedGoalsConceded, expectedAssistsSetPlay, ...) share the prefixes.
_XG_PREFIXES = ("expectedgoals", "expectedassists")
_XG_SHORT = frozenset({"xg", "xa", "xgot"})


def deep_level(count: int) -> str | None:
    count = _count(count, "plays.count")
    if count >= 1000:
        return "full"
    if 100 <= count <= 400:
        return "medium"
    if count < 50:
        return "light"
    return None


def _xg_key(key: str) -> bool:
    name = re.sub(r"[^a-z]", "", key.lower())
    return name in _XG_SHORT or name.startswith(_XG_PREFIXES)


def number(value):
    if value is None:
        return None
    if type(value) not in (int, float) or not math.isfinite(value):
        raise EspnParseError("deep statistic must be a finite number")
    return float(value)


def xg_support(payloads) -> bool | None:
    """A positive source value proves support; all-zero placeholders do not."""
    values = []

    def visit(node):
        if isinstance(node, dict):
            if _xg_key(str(node.get("name", ""))) and "value" in node:
                values.append(number(node["value"]))
            for key, value in node.items():
                if _xg_key(key):
                    values.append(number(value))
                elif isinstance(value, (dict, list)):
                    visit(value)
        elif isinstance(node, list):
            for value in node:
                visit(value)

    for payload in payloads:
        visit(payload)
    if any(value is not None and value > 0 for value in values):
        return True
    return None if values else False


def mask_xg(node, has_xg: bool | None):
    """Keep real zeros only with confirmed support, including named stats."""
    if isinstance(node, list):
        return [mask_xg(value, has_xg) for value in node]
    if not isinstance(node, dict):
        return node
    named = _xg_key(str(node.get("name", "")))
    return {
        key: None if has_xg is not True and (_xg_key(key) or (named and key in ("value", "displayValue", "perGameValue", "perGameDisplayValue")))
        else mask_xg(value, has_xg)
        for key, value in node.items()
    }


@dataclass(frozen=True)
class PlayPage:
    count: int
    index: int
    pages: int
    items: tuple[dict, ...]


def parse_page(payload) -> PlayPage:
    raise_on_error_body(payload, "plays")
    payload = required_mapping(payload, "plays")
    count, index, pages = (_count(payload.get(key), f"plays.{key}")
                           for key in ("count", "pageIndex", "pageCount"))
    items = payload.get("items")
    if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
        raise EspnParseError("plays must contain an items array of objects")
    if pages == 0:
        if count or items or index:
            raise EspnParseError("empty plays page has inconsistent metadata")
    elif not 1 <= index <= pages or not items or len(items) > count:
        raise EspnParseError("plays page has inconsistent metadata")
    if pages == 1 and len(items) != count:
        raise EspnParseError("single plays page is incomplete")
    return PlayPage(count, index, pages, tuple(items))


def collect_plays(fetch_page: Callable[[int], dict]) -> tuple[dict, ...]:
    first = parse_page(fetch_page(1))
    if first.pages and first.index != 1:
        raise EspnParseError("first plays page is not page 1")
    items = list(first.items)
    for index in range(2, first.pages + 1):
        page = parse_page(fetch_page(index))
        if (page.count, page.pages, page.index) != (first.count, first.pages, index):
            raise EspnParseError("plays pagination changed")
        items.extend(page.items)
    ids = [str(native_id(item.get("id"), "play.id")) for item in items]
    if len(items) != first.count or len(set(ids)) != len(ids):
        raise EspnParseError("plays count mismatch or duplicate play id")
    return tuple(items)


def statistics(payload) -> dict:
    raise_on_error_body(payload, "team statistics")
    payload = required_mapping(payload, "team statistics")
    splits = required_mapping(payload.get("splits"), "team statistics.splits")
    categories = splits.get("categories")
    if not isinstance(categories, list) or not categories:
        raise EspnParseError("team statistics has no categories")
    seen = set()
    for category in categories:
        category = required_mapping(category, "statistics category")
        stats = category.get("stats")
        if not isinstance(stats, list) or not stats:
            raise EspnParseError("statistics category has no stats")
        for stat in stats:
            stat = required_mapping(stat, "statistic")
            key = (category.get("name"), stat.get("name"))
            if not all(isinstance(value, str) and value for value in key) or key in seen:
                raise EspnParseError("missing or duplicate statistic name")
            seen.add(key)
            number(stat.get("value"))
    return dict(payload)


def player_statistics(payload, athlete_id: int) -> dict:
    body = statistics(payload)
    if _ref_id(body.get("athlete"), "athletes") != athlete_id:
        raise EspnParseError("player statistics belong to another athlete")
    return body


def parse_roster(payload) -> dict[int, bool]:
    """Athlete id -> played (starter or substituted in); duplicates are errors."""
    raise_on_error_body(payload, "roster")
    entries = required_list(required_mapping(payload, "roster").get("entries"), "roster.entries")
    if not entries:
        raise EspnParseError("roster has no entries")
    roster = {}
    for entry in entries:
        entry = required_mapping(entry, "roster entry")
        athlete = entry.get("playerId")
        if type(athlete) is not int or athlete < 1:
            raise EspnParseError("roster playerId must be a positive integer")
        flags = [entry.get("starter"), *(required_mapping(entry.get(key), f"roster.{key}").get("didSub")
                                         for key in ("subbedIn", "subbedOut"))]
        if any(type(flag) is not bool for flag in flags):
            raise EspnParseError("roster starter/didSub must be boolean")
        if athlete in roster:
            raise EspnParseError("duplicate roster player")
        roster[athlete] = flags[0] or flags[1]
    return roster


def _ref_id(node, kind):
    if node is None:
        return None
    node = required_mapping(node, kind)
    if "id" in node:
        return native_id(node["id"], kind)
    match = re.search(rf"/{kind}/(\d+)(?:[/?#]|$)", node.get("$ref", ""))
    if match is None:
        raise EspnParseError(f"missing {kind} identity")
    return native_id(match[1], kind)


def play_values(play: dict, has_xg: bool | None) -> dict:
    clock = required_mapping(play.get("clock", {}), "play.clock")
    period = required_mapping(play.get("period", {}), "play.period")
    kind = required_mapping(play.get("type", {}), "play.type")
    participants = play.get("participants", [])
    if not isinstance(participants, list):
        raise EspnParseError("play.participants must be an array")
    flags = {}
    for key in ("scoringPlay", "redCard", "yellowCard", "penaltyKick", "ownGoal"):
        value = play.get(key)
        if value is not None and type(value) is not bool:
            raise EspnParseError(f"play.{key} must be boolean")
        flags[key] = value
    return {
        "play_id": str(native_id(play.get("id"), "play.id")),
        "type_id": str(kind["id"]) if kind.get("id") is not None else None,
        "type_text": kind.get("text"), "text": play.get("text"),
        "period": period.get("number"),
        "clock_value": number(clock.get("value")), "clock_display": clock.get("displayValue"),
        "team_id": _ref_id(play.get("team"), "teams"),
        "participants_json": canonical_json(participants),
        **{column: number(play.get(key)) for column, key in (
            ("x", "fieldPositionX"), ("y", "fieldPositionY"),
            ("x2", "endFieldPositionX"), ("y2", "endFieldPositionY"),
            ("goal_y", "goalPositionY"), ("goal_z", "goalPositionZ"))},
        "xg": number(play.get("expectedGoals")) if has_xg is True else None,
        "xgot": number(play.get("expectedGoalsOnTarget")) if has_xg is True else None,
        "flags_json": canonical_json(flags),
    }
