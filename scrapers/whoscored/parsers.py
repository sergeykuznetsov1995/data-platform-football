"""Pure, transport-free parsers for WhoScored source documents.

The functions in this module accept already-fetched bytes/strings/mappings and
perform no I/O.  Source-shape drift raises :class:`WhoScoredParseError`
instead of being converted to an empty successful run.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Optional, Sequence

from bs4 import BeautifulSoup

from .domain import SeasonFormat, WhoScoredScope, canonical_season_id


PARSER_VERSION = "whoscored-parser-v2"


class WhoScoredParseError(ValueError):
    """A source document does not satisfy the expected parser contract."""


class JavaScriptLiteralError(WhoScoredParseError):
    """An embedded JavaScript value could not be safely extracted or parsed."""


class DatasetStatus(str, Enum):
    AVAILABLE = "available"
    EMPTY = "empty"
    NOT_AVAILABLE = "not_available"


@dataclass(frozen=True)
class ParsedDataset:
    name: str
    status: DatasetStatus
    rows: tuple[dict[str, Any], ...] = ()
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        if self.status is DatasetStatus.AVAILABLE and not self.rows:
            raise ValueError("An available dataset must contain at least one row")
        if self.status is not DatasetStatus.AVAILABLE and self.rows:
            raise ValueError("Only an available dataset may contain rows")

    @property
    def row_count(self) -> int:
        return len(self.rows)


@dataclass(frozen=True)
class MatchParseResult:
    parser_version: str
    parsed_at: str
    game_id: int
    events: ParsedDataset
    lineups: ParsedDataset

    @property
    def datasets(self) -> Mapping[str, ParsedDataset]:
        return {"events": self.events, "lineups": self.lineups}


@dataclass(frozen=True, order=True)
class CalendarMonth:
    year: int
    month: int

    @property
    def token(self) -> str:
        return f"{self.year:04d}{self.month:02d}"


def _dataset(
    name: str,
    rows: Sequence[dict[str, Any]],
    *,
    empty_status: DatasetStatus = DatasetStatus.EMPTY,
    empty_reason: str = "source_contains_no_rows",
) -> ParsedDataset:
    frozen_rows = tuple(rows)
    if frozen_rows:
        return ParsedDataset(name, DatasetStatus.AVAILABLE, frozen_rows)
    return ParsedDataset(name, empty_status, reason=empty_reason)


def canonical_json(value: Any) -> str:
    """Serialize a nested source value deterministically for Bronze storage."""

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise WhoScoredParseError(f"Value is not canonical JSON: {exc}") from exc


# ---------------------------------------------------------------------------
# Embedded JavaScript literals
# ---------------------------------------------------------------------------

_BRACKET_CLOSE = {"{": "}", "[": "]", "(": ")"}
_IDENTIFIER_RE = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")
_NUMBER_RE = re.compile(
    r"[+-]?(?:0[xX][0-9a-fA-F]+|"
    r"(?:(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?))"
)


def _scan_balanced(source: str, start: int) -> tuple[str, int]:
    """Return one balanced object/array literal and its exclusive end index."""

    if start >= len(source) or source[start] not in "[{":
        raise JavaScriptLiteralError("Embedded value must start with '{' or '['")
    stack = [source[start]]
    quote: Optional[str] = None
    escaped = False
    line_comment = False
    block_comment = False
    index = start + 1
    while index < len(source):
        char = source[index]
        nxt = source[index + 1] if index + 1 < len(source) else ""

        if line_comment:
            if char in "\r\n":
                line_comment = False
            index += 1
            continue
        if block_comment:
            if char == "*" and nxt == "/":
                block_comment = False
                index += 2
            else:
                index += 1
            continue
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            index += 1
            continue
        if char in "'\"`":
            quote = char
            index += 1
            continue
        if char == "/" and nxt == "/":
            line_comment = True
            index += 2
            continue
        if char == "/" and nxt == "*":
            block_comment = True
            index += 2
            continue
        if char in _BRACKET_CLOSE:
            stack.append(char)
        elif char in _BRACKET_CLOSE.values():
            if not stack or _BRACKET_CLOSE[stack[-1]] != char:
                raise JavaScriptLiteralError(
                    f"Mismatched bracket {char!r} at character {index}"
                )
            stack.pop()
            if not stack:
                return source[start : index + 1], index + 1
        index += 1
    raise JavaScriptLiteralError("Embedded JavaScript brackets are unmatched")


class _Json5Parser:
    """Small recursive-descent parser for the JSON5 subset WhoScored emits.

    It supports comments, single-quoted strings, identifier/numeric object
    keys, hexadecimal numbers and trailing commas.  It intentionally does not
    execute expressions.  The sole opt-in extension is WhoScored's exact
    ``(new Date(...)).toString()`` calendar metadata, which is represented as
    ``None`` because only ``wsCalendar.mask`` is consumed.
    """

    def __init__(self, text: str, *, allow_date_expressions: bool = False) -> None:
        self.text = text
        self.pos = 0
        self.allow_date_expressions = allow_date_expressions

    def parse(self) -> Any:
        value = self._value()
        self._space()
        if self.pos != len(self.text):
            self._fail("unexpected trailing content")
        return value

    def _fail(self, message: str) -> None:
        raise JavaScriptLiteralError(f"JSON5 {message} at character {self.pos}")

    def _space(self) -> None:
        while self.pos < len(self.text):
            if self.text[self.pos].isspace():
                self.pos += 1
                continue
            if self.text.startswith("//", self.pos):
                newline = self.text.find("\n", self.pos + 2)
                self.pos = len(self.text) if newline < 0 else newline + 1
                continue
            if self.text.startswith("/*", self.pos):
                end = self.text.find("*/", self.pos + 2)
                if end < 0:
                    self._fail("contains an unterminated comment")
                self.pos = end + 2
                continue
            break

    def _value(self) -> Any:
        self._space()
        if self.pos >= len(self.text):
            self._fail("expected a value")
        char = self.text[self.pos]
        if char == "{":
            return self._object()
        if char == "[":
            return self._array()
        if char in "'\"":
            return self._string()
        if char == "(" and self.allow_date_expressions:
            return self._date_expression()
        number = _NUMBER_RE.match(self.text, self.pos)
        if number:
            token = number.group(0)
            self.pos = number.end()
            if token.lower().lstrip("+-").startswith("0x"):
                sign = -1 if token.startswith("-") else 1
                return sign * int(token.lstrip("+-")[2:], 16)
            if any(marker in token for marker in ".eE"):
                result = float(token)
                if not math.isfinite(result):
                    self._fail("contains a non-finite number")
                return result
            return int(token)
        identifier = _IDENTIFIER_RE.match(self.text, self.pos)
        if identifier:
            token = identifier.group(0)
            self.pos = identifier.end()
            if token == "true":
                return True
            if token == "false":
                return False
            if token == "null":
                return None
            self._fail(f"contains unsupported identifier {token!r}")
        self._fail(f"expected a value, found {char!r}")

    def _object(self) -> dict[str, Any]:
        self.pos += 1
        result: dict[str, Any] = {}
        self._space()
        if self._consume("}"):
            return result
        while True:
            self._space()
            if self.pos >= len(self.text):
                self._fail("object is unterminated")
            if self.text[self.pos] in "'\"":
                key = self._string()
            else:
                identifier = _IDENTIFIER_RE.match(self.text, self.pos)
                number = _NUMBER_RE.match(self.text, self.pos)
                match = identifier or number
                if match is None:
                    self._fail("expected an object key")
                key = match.group(0)
                self.pos = match.end()
            self._space()
            if not self._consume(":"):
                self._fail("expected ':' after an object key")
            if key in result:
                self._fail(f"contains duplicate object key {key!r}")
            result[str(key)] = self._value()
            self._space()
            if self._consume("}"):
                return result
            if not self._consume(","):
                self._fail("expected ',' or '}'")
            self._space()
            if self._consume("}"):
                return result

    def _array(self) -> list[Any]:
        self.pos += 1
        result: list[Any] = []
        self._space()
        if self._consume("]"):
            return result
        while True:
            result.append(self._value())
            self._space()
            if self._consume("]"):
                return result
            if not self._consume(","):
                self._fail("expected ',' or ']'")
            self._space()
            if self._consume("]"):
                return result

    def _string(self) -> str:
        quote = self.text[self.pos]
        self.pos += 1
        chars: list[str] = []
        escapes = {
            "b": "\b",
            "f": "\f",
            "n": "\n",
            "r": "\r",
            "t": "\t",
            "v": "\v",
            "0": "\0",
        }
        while self.pos < len(self.text):
            char = self.text[self.pos]
            self.pos += 1
            if char == quote:
                return "".join(chars)
            if char in "\r\n":
                self._fail("contains an unescaped newline in a string")
            if char != "\\":
                chars.append(char)
                continue
            if self.pos >= len(self.text):
                self._fail("string ends after an escape character")
            escaped = self.text[self.pos]
            self.pos += 1
            if escaped in "\r\n":
                if escaped == "\r" and self.pos < len(self.text) and self.text[self.pos] == "\n":
                    self.pos += 1
                continue
            if escaped == "x":
                chars.append(self._hex_escape(2))
            elif escaped == "u":
                chars.append(self._hex_escape(4))
            else:
                chars.append(escapes.get(escaped, escaped))
        self._fail("string is unterminated")

    def _hex_escape(self, width: int) -> str:
        token = self.text[self.pos : self.pos + width]
        if len(token) != width or not re.fullmatch(r"[0-9a-fA-F]+", token):
            self._fail("contains an invalid hexadecimal string escape")
        self.pos += width
        return chr(int(token, 16))

    def _date_expression(self) -> None:
        start = self.pos
        if re.match(r"\(\s*new\s+Date\s*\(", self.text[start:]) is None:
            self._fail("contains an unsupported JavaScript expression")
        depth = 0
        quote: Optional[str] = None
        escaped = False
        index = start
        while index < len(self.text):
            char = self.text[index]
            if quote is not None:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == quote:
                    quote = None
            elif char in "'\"":
                quote = char
            elif char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    end = index + 1
                    suffix = re.match(r"\s*\.\s*toString\s*\(\s*\)", self.text[end:])
                    if suffix is None:
                        self._fail("contains an unsupported Date expression")
                    self.pos = end + suffix.end()
                    return None
            index += 1
        self._fail("contains an unterminated Date expression")

    def _consume(self, token: str) -> bool:
        if self.text.startswith(token, self.pos):
            self.pos += len(token)
            return True
        return False


def parse_js_literal(literal: str, *, allow_date_expressions: bool = False) -> Any:
    """Parse strict JSON first, then a non-executing JSON5 fallback."""

    try:
        return json.loads(literal)
    except json.JSONDecodeError as strict_error:
        try:
            return _Json5Parser(
                literal,
                allow_date_expressions=allow_date_expressions,
            ).parse()
        except JavaScriptLiteralError as relaxed_error:
            raise JavaScriptLiteralError(
                f"Embedded literal is neither JSON nor supported JSON5 "
                f"(JSON: {strict_error.msg}; {relaxed_error})"
            ) from relaxed_error


def _extract_after_pattern(
    source: str,
    pattern: re.Pattern[str],
    *,
    label: str,
    allow_date_expressions: bool,
) -> Any:
    for match in pattern.finditer(source):
        start = match.end()
        while start < len(source) and source[start].isspace():
            start += 1
        if start >= len(source) or source[start] not in "[{":
            continue
        literal, _ = _scan_balanced(source, start)
        return parse_js_literal(
            literal,
            allow_date_expressions=allow_date_expressions,
        )
    raise JavaScriptLiteralError(f"JavaScript {label} not found")


def extract_js_assignment(
    source: str,
    variable_name: str,
    *,
    allow_date_expressions: bool = False,
) -> Any:
    """Extract an object/array assigned to a top-level JavaScript variable."""

    if not _IDENTIFIER_RE.fullmatch(variable_name):
        raise ValueError(f"Unsafe JavaScript variable name {variable_name!r}")
    pattern = re.compile(
        rf"(?<![A-Za-z0-9_$])(?:(?:var|let|const)\s+)?"
        rf"{re.escape(variable_name)}\s*=\s*"
    )
    return _extract_after_pattern(
        source,
        pattern,
        label=f"assignment {variable_name!r}",
        allow_date_expressions=allow_date_expressions,
    )


def extract_matchcentre_data(html: str) -> dict[str, Any]:
    """Extract the inline ``matchCentreData`` object from a match page."""

    if not isinstance(html, str) or not html:
        raise WhoScoredParseError("Match HTML is empty")
    property_pattern = re.compile(
        r"(?:(?:['\"]matchCentreData['\"])|(?:\bmatchCentreData\b))\s*:\s*"
    )
    try:
        value = _extract_after_pattern(
            html,
            property_pattern,
            label="property 'matchCentreData'",
            allow_date_expressions=False,
        )
    except JavaScriptLiteralError:
        value = extract_js_assignment(html, "matchCentreData")
    if not isinstance(value, dict):
        raise WhoScoredParseError("matchCentreData must be an object")
    if "events" not in value:
        raise WhoScoredParseError("matchCentreData has no events field")
    return value


# ---------------------------------------------------------------------------
# Match payload
# ---------------------------------------------------------------------------

_SNAKE_1 = re.compile(r"(.)([A-Z][a-z]+)")
_SNAKE_2 = re.compile(r"([a-z0-9])([A-Z])")


def _snake(name: str) -> str:
    return _SNAKE_2.sub(r"\1_\2", _SNAKE_1.sub(r"\1_\2", name)).lower().replace(
        "-", "_"
    )


def _display(value: Any) -> Optional[str]:
    if isinstance(value, Mapping):
        value = value.get("displayName")
    return value if isinstance(value, str) and value else None


def _optional_int(value: Any, field: str) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise WhoScoredParseError(f"{field} must not be boolean")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise WhoScoredParseError(f"{field} is not numeric: {value!r}") from exc
    if not numeric.is_integer():
        raise WhoScoredParseError(f"{field} is not an integer: {value!r}")
    return int(numeric)


def _required_int(value: Any, field: str) -> int:
    parsed = _optional_int(value, field)
    if parsed is None:
        raise WhoScoredParseError(f"{field} is required")
    return parsed


def _optional_float(value: Any, field: str) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise WhoScoredParseError(f"{field} must not be boolean")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise WhoScoredParseError(f"{field} is not numeric: {value!r}") from exc
    if not math.isfinite(result):
        raise WhoScoredParseError(f"{field} must be finite")
    return result


def _identity_fields(
    scope: WhoScoredScope,
    game_id: int,
    game: Optional[str],
) -> dict[str, Any]:
    return {
        "league": scope.competition_id,
        "season": scope.season_id,
        "game": game,
        "game_id": game_id,
    }


EVENT_FIELDS = (
    "source_event_id",
    "game_id",
    "period",
    "minute",
    "second",
    "expanded_minute",
    "type",
    "outcome_type",
    "team_id",
    "team",
    "player_id",
    "player",
    "x",
    "y",
    "end_x",
    "end_y",
    "goal_mouth_y",
    "goal_mouth_z",
    "blocked_x",
    "blocked_y",
    "qualifiers",
    "is_touch",
    "is_shot",
    "is_goal",
    "card_type",
    "related_event_id",
    "related_player_id",
    "league",
    "season",
    "game",
)


def parse_events(
    data: Mapping[str, Any],
    *,
    scope: WhoScoredScope,
    game_id: int,
    game: Optional[str] = None,
) -> ParsedDataset:
    raw_events = data.get("events")
    if not isinstance(raw_events, list):
        raise WhoScoredParseError("matchCentreData.events must be a list")

    player_names_raw = data.get("playerIdNameDictionary") or {}
    if not isinstance(player_names_raw, Mapping):
        raise WhoScoredParseError("playerIdNameDictionary must be an object")
    player_names: dict[int, str] = {}
    for raw_id, name in player_names_raw.items():
        player_id = _required_int(raw_id, "playerIdNameDictionary key")
        if isinstance(name, str) and name:
            player_names[player_id] = name

    team_names: dict[int, str] = {}
    for side in ("home", "away"):
        side_value = data.get(side) or {}
        if not isinstance(side_value, Mapping):
            raise WhoScoredParseError(f"matchCentreData.{side} must be an object")
        team_id = _optional_int(side_value.get("teamId"), f"{side}.teamId")
        team_name = side_value.get("name")
        if team_id is not None and isinstance(team_name, str) and team_name:
            team_names[team_id] = team_name

    rows: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    identity = _identity_fields(scope, _required_int(game_id, "game_id"), game)
    for index, event in enumerate(raw_events):
        if not isinstance(event, Mapping):
            raise WhoScoredParseError(f"events[{index}] must be an object")
        source_event_id = _required_int(
            event.get("eventId", event.get("id")),
            f"events[{index}].eventId",
        )
        if source_event_id in seen_ids:
            raise WhoScoredParseError(
                f"Duplicate source event id {source_event_id} in game {game_id}"
            )
        seen_ids.add(source_event_id)

        team_id = _optional_int(event.get("teamId"), f"events[{index}].teamId")
        player_id = _optional_int(event.get("playerId"), f"events[{index}].playerId")
        qualifiers = event.get("qualifiers")
        if qualifiers is None:
            qualifiers = []
        if not isinstance(qualifiers, list):
            raise WhoScoredParseError(f"events[{index}].qualifiers must be a list")

        row = {
            **identity,
            "source_event_id": source_event_id,
            "period": _display(event.get("period")),
            "minute": _optional_int(event.get("minute"), f"events[{index}].minute"),
            "second": _optional_int(event.get("second"), f"events[{index}].second"),
            "expanded_minute": _optional_int(
                event.get("expandedMinute"), f"events[{index}].expandedMinute"
            ),
            "type": _display(event.get("type")),
            "outcome_type": _display(event.get("outcomeType")),
            "team_id": team_id,
            "team": team_names.get(team_id),
            "player_id": player_id,
            "player": player_names.get(player_id),
            "x": _optional_float(event.get("x"), f"events[{index}].x"),
            "y": _optional_float(event.get("y"), f"events[{index}].y"),
            "end_x": _optional_float(event.get("endX"), f"events[{index}].endX"),
            "end_y": _optional_float(event.get("endY"), f"events[{index}].endY"),
            "goal_mouth_y": _optional_float(
                event.get("goalMouthY"), f"events[{index}].goalMouthY"
            ),
            "goal_mouth_z": _optional_float(
                event.get("goalMouthZ"), f"events[{index}].goalMouthZ"
            ),
            "blocked_x": _optional_float(
                event.get("blockedX"), f"events[{index}].blockedX"
            ),
            "blocked_y": _optional_float(
                event.get("blockedY"), f"events[{index}].blockedY"
            ),
            "qualifiers": canonical_json(qualifiers),
            "is_touch": bool(event.get("isTouch", False)),
            "is_shot": bool(event.get("isShot", False)),
            "is_goal": bool(event.get("isGoal", False)),
            "card_type": _display(event.get("cardType")),
            "related_event_id": _optional_int(
                event.get("relatedEventId"), f"events[{index}].relatedEventId"
            ),
            "related_player_id": _optional_int(
                event.get("relatedPlayerId"), f"events[{index}].relatedPlayerId"
            ),
        }
        rows.append({field: row.get(field) for field in EVENT_FIELDS})
    return _dataset("events", rows)


def _final_rating(stats: Any, field: str) -> Optional[float]:
    if stats is None:
        return None
    if not isinstance(stats, Mapping):
        raise WhoScoredParseError(f"{field} must be an object")
    ratings = stats.get("ratings")
    if ratings in (None, {}):
        return None
    if not isinstance(ratings, Mapping):
        raise WhoScoredParseError(f"{field}.ratings must be an object")
    numeric: list[tuple[int, Any]] = []
    for minute, value in ratings.items():
        numeric.append((_required_int(minute, f"{field}.ratings key"), value))
    return _optional_float(max(numeric)[1], f"{field}.ratings final value")


def _minutes_played(
    is_starter: bool,
    minute_start: Optional[int],
    minute_end: Optional[int],
    expanded_max: Optional[int],
) -> Optional[int]:
    if expanded_max is None:
        return None
    if is_starter:
        return minute_end if minute_end is not None else expanded_max
    if minute_start is None:
        return 0
    end = minute_end if minute_end is not None else expanded_max
    return max(0, end - minute_start)


def parse_lineups(
    data: Mapping[str, Any],
    *,
    scope: WhoScoredScope,
    game_id: int,
    game: Optional[str] = None,
) -> ParsedDataset:
    expanded_max = _optional_int(data.get("expandedMaxMinute"), "expandedMaxMinute")
    identity = _identity_fields(scope, _required_int(game_id, "game_id"), game)
    rows: list[dict[str, Any]] = []
    player_blocks_present = False
    seen: set[tuple[Optional[int], int]] = set()

    for side in ("home", "away"):
        side_value = data.get(side) or {}
        if not isinstance(side_value, Mapping):
            raise WhoScoredParseError(f"matchCentreData.{side} must be an object")
        players = side_value.get("players")
        if players is None:
            players = []
        else:
            player_blocks_present = True
        if not isinstance(players, list):
            raise WhoScoredParseError(f"{side}.players must be a list")
        team_id = _optional_int(side_value.get("teamId"), f"{side}.teamId")
        team = side_value.get("name") if isinstance(side_value.get("name"), str) else None

        red_minutes: dict[int, int] = {}
        incidents = side_value.get("incidentEvents") or []
        if not isinstance(incidents, list):
            raise WhoScoredParseError(f"{side}.incidentEvents must be a list")
        for index, incident in enumerate(incidents):
            if not isinstance(incident, Mapping):
                raise WhoScoredParseError(f"{side}.incidentEvents[{index}] must be an object")
            if _display(incident.get("cardType")) not in {"Red", "SecondYellow"}:
                continue
            player_id = _optional_int(
                incident.get("playerId"), f"{side}.incidentEvents[{index}].playerId"
            )
            minute = _optional_int(
                incident.get("expandedMinute"),
                f"{side}.incidentEvents[{index}].expandedMinute",
            )
            if player_id is not None and minute is not None:
                red_minutes[player_id] = minute

        for index, player in enumerate(players):
            if not isinstance(player, Mapping):
                raise WhoScoredParseError(f"{side}.players[{index}] must be an object")
            player_id = _required_int(
                player.get("playerId"), f"{side}.players[{index}].playerId"
            )
            key = (team_id, player_id)
            if key in seen:
                raise WhoScoredParseError(
                    f"Duplicate lineup player {player_id} for team {team_id}"
                )
            seen.add(key)
            is_starter = bool(player.get("isFirstEleven", False))
            sub_in = _optional_int(
                player.get("subbedInExpandedMinute"),
                f"{side}.players[{index}].subbedInExpandedMinute",
            )
            sub_out = _optional_int(
                player.get("subbedOutExpandedMinute"),
                f"{side}.players[{index}].subbedOutExpandedMinute",
            )
            effective_end = sub_out if sub_out is not None else red_minutes.get(player_id)
            position = player.get("position")
            if isinstance(position, Mapping):
                position = _display(position)
            rows.append(
                {
                    **identity,
                    "team_id": team_id,
                    "team": team,
                    "side": side,
                    "player_id": player_id,
                    "player": player.get("name") if isinstance(player.get("name"), str) else None,
                    "shirt_no": _optional_int(
                        player.get("shirtNo"), f"{side}.players[{index}].shirtNo"
                    ),
                    "position": position if isinstance(position, str) else None,
                    "is_starter": is_starter,
                    "is_man_of_the_match": bool(player.get("isManOfTheMatch", False)),
                    "subbed_in_expanded_minute": sub_in,
                    "subbed_out_expanded_minute": sub_out,
                    "minutes_played": _minutes_played(
                        is_starter, sub_in, effective_end, expanded_max
                    ),
                    "rating": _final_rating(
                        player.get("stats"), f"{side}.players[{index}].stats"
                    ),
                    "height": _optional_float(
                        player.get("height"), f"{side}.players[{index}].height"
                    ),
                    "weight": _optional_float(
                        player.get("weight"), f"{side}.players[{index}].weight"
                    ),
                    "age": _optional_float(
                        player.get("age"), f"{side}.players[{index}].age"
                    ),
                }
            )

    if not rows and not player_blocks_present:
        return ParsedDataset(
            "lineups",
            DatasetStatus.NOT_AVAILABLE,
            reason="source_player_blocks_absent",
        )
    return _dataset("lineups", rows, empty_reason="source_player_blocks_empty")


def parse_matchcentre_data(
    data: Mapping[str, Any],
    *,
    scope: WhoScoredScope,
    game_id: int,
    game: Optional[str] = None,
    parser_version: str = PARSER_VERSION,
) -> MatchParseResult:
    if not isinstance(data, Mapping):
        raise WhoScoredParseError("matchCentreData must be an object")
    return MatchParseResult(
        parser_version=parser_version,
        parsed_at=datetime.now(timezone.utc).isoformat(),
        game_id=_required_int(game_id, "game_id"),
        events=parse_events(data, scope=scope, game_id=game_id, game=game),
        lineups=parse_lineups(data, scope=scope, game_id=game_id, game=game),
    )


def parse_match_html(
    html: str,
    *,
    scope: WhoScoredScope,
    game_id: int,
    game: Optional[str] = None,
    parser_version: str = PARSER_VERSION,
) -> MatchParseResult:
    return parse_matchcentre_data(
        extract_matchcentre_data(html),
        scope=scope,
        game_id=game_id,
        game=game,
        parser_version=parser_version,
    )


# ---------------------------------------------------------------------------
# Profile and preview HTML
# ---------------------------------------------------------------------------

_PLAYER_URL_RE = re.compile(r"/Players/(\d+)(?:/|$)", re.IGNORECASE)
_TEAM_URL_RE = re.compile(r"/Teams/(\d+)(?:/|$)", re.IGNORECASE)
_DOB_RE = re.compile(r"\b(\d{2})-(\d{2})-(\d{4})\b")
_HEIGHT_RE = re.compile(r"(\d{2,3})\s*cm", re.IGNORECASE)
_LEADING_INT_RE = re.compile(r"(\d+)")


def _text_after_label(parent: Any) -> str:
    full = parent.get_text(" ", strip=True)
    label = parent.find("span", class_="info-label")
    label_text = label.get_text(" ", strip=True) if label else ""
    if label_text and full.startswith(label_text):
        return full[len(label_text) :].strip()
    return full


def _leading_int(value: str) -> Optional[int]:
    match = _LEADING_INT_RE.search(value)
    return int(match.group(1)) if match else None


def parse_profile_html(html: str, *, player_id: int | str) -> ParsedDataset:
    if not isinstance(html, str) or not html:
        raise WhoScoredParseError("Player profile HTML is empty")
    soup = BeautifulSoup(html, "html.parser")
    fields = {
        label.get_text(strip=True).rstrip(":").strip().lower(): label.parent
        for label in soup.select("span.info-label")
    }
    if not fields:
        raise WhoScoredParseError("Player profile has no info-label fields")

    row: dict[str, Any] = {
        "player_id": _required_int(player_id, "player_id"),
        "name": None,
        "current_team_id": None,
        "current_team_name": None,
        "shirt_number": None,
        "age": None,
        "date_of_birth": None,
        "height_cm": None,
        "nationality": None,
        "country_code": None,
        "positions": None,
    }
    if "name" in fields:
        row["name"] = _text_after_label(fields["name"]) or None
    if "current team" in fields:
        anchor = fields["current team"].find("a", href=True)
        if anchor is not None:
            row["current_team_name"] = anchor.get_text(" ", strip=True) or None
            match = _TEAM_URL_RE.search(anchor.get("href", ""))
            if match:
                row["current_team_id"] = int(match.group(1))
    if "shirt number" in fields:
        row["shirt_number"] = _leading_int(_text_after_label(fields["shirt number"]))
    if "age" in fields:
        age_parent = fields["age"]
        row["age"] = _leading_int(_text_after_label(age_parent))
        date_match = _DOB_RE.search(age_parent.get_text(" ", strip=True))
        if date_match:
            day, month, year = date_match.groups()
            row["date_of_birth"] = f"{year}-{month}-{day}"
    if "height" in fields:
        height = _HEIGHT_RE.search(_text_after_label(fields["height"]))
        if height:
            row["height_cm"] = int(height.group(1))
    if "nationality" in fields:
        nationality_parent = fields["nationality"]
        iconize = nationality_parent.find("span", class_="iconize")
        target = iconize or nationality_parent
        row["nationality"] = target.get_text(" ", strip=True) or None
        flag = nationality_parent.find(
            "span", class_=lambda value: value and "country" in value
        )
        if flag is not None:
            for class_name in flag.get("class", ()):
                if class_name.startswith("flg-"):
                    row["country_code"] = class_name[4:]
                    break
    if "positions" in fields:
        positions = re.sub(r"\s+", " ", _text_after_label(fields["positions"])).strip()
        row["positions"] = positions or None
    soup.decompose()
    return _dataset("profiles", [row])


def parse_preview_html(
    html: str,
    *,
    scope: WhoScoredScope,
    game_id: int,
    game: Optional[str],
    home_team: Optional[str],
    away_team: Optional[str],
) -> ParsedDataset:
    if not isinstance(html, str) or not html:
        raise WhoScoredParseError("Preview HTML is empty")
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find(id="missing-players")
    if container is None:
        soup.decompose()
        return ParsedDataset(
            "missing_players",
            DatasetStatus.EMPTY,
            reason="source_has_no_missing_players_section",
        )
    tables = container.find_all("table")
    if len(tables) > 2:
        soup.decompose()
        raise WhoScoredParseError("Missing-player section contains more than two tables")
    teams = (home_team, away_team)
    rows: list[dict[str, Any]] = []
    for table_index, table in enumerate(tables):
        for row_index, table_row in enumerate(table.select("tbody tr")):
            player_cell = table_row.select_one("td.pn")
            anchor = player_cell.find("a", href=True) if player_cell else None
            if anchor is None:
                soup.decompose()
                raise WhoScoredParseError(
                    f"Missing-player row {table_index}:{row_index} has no player link"
                )
            match = _PLAYER_URL_RE.search(anchor.get("href", ""))
            if match is None:
                soup.decompose()
                raise WhoScoredParseError(
                    f"Missing-player row {table_index}:{row_index} has an invalid player URL"
                )
            reason_cell = table_row.select_one("td.reason")
            reason_marker = reason_cell.find("span") if reason_cell else None
            reason = reason_marker.get("title") if reason_marker else None
            if not reason and reason_cell is not None:
                reason = reason_cell.get_text(" ", strip=True) or None
            status_cell = table_row.select_one("td.confirmed")
            rows.append(
                {
                    **_identity_fields(scope, _required_int(game_id, "game_id"), game),
                    "team": teams[table_index],
                    "player": anchor.get_text(" ", strip=True) or None,
                    "player_id": int(match.group(1)),
                    "reason": reason,
                    "status": (
                        status_cell.get_text(" ", strip=True) or None
                        if status_cell is not None
                        else None
                    ),
                }
            )
    soup.decompose()
    return _dataset("missing_players", rows, empty_reason="source_lists_no_missing_players")


# ---------------------------------------------------------------------------
# Competition discovery, stages, calendar and schedule
# ---------------------------------------------------------------------------

_SOURCE_URL_SEGMENTS = {
    "region_id": re.compile(r"/Regions/(\d+)(?:/|$)", re.IGNORECASE),
    "tournament_id": re.compile(r"/Tournaments/(\d+)(?:/|$)", re.IGNORECASE),
    "source_season_id": re.compile(r"/Seasons/(\d+)(?:/|$)", re.IGNORECASE),
    "stage_id": re.compile(r"/Stages/(\d+)(?:/|$)", re.IGNORECASE),
}


def _source_url_ids(url: str) -> dict[str, Optional[int]]:
    matches = {
        key: pattern.search(url or "")
        for key, pattern in _SOURCE_URL_SEGMENTS.items()
    }
    if not any(matches.values()):
        raise WhoScoredParseError(f"Could not parse WhoScored source URL {url!r}")
    return {
        key: int(match.group(1)) if match is not None else None
        for key, match in matches.items()
    }


def parse_regions(html: str) -> ParsedDataset:
    data = extract_js_assignment(html, "allRegions")
    if not isinstance(data, list):
        raise WhoScoredParseError("allRegions must be an array")
    rows: list[dict[str, Any]] = []
    for region_index, region in enumerate(data):
        if not isinstance(region, Mapping):
            raise WhoScoredParseError(f"allRegions[{region_index}] must be an object")
        tournaments = region.get("tournaments") or []
        if not isinstance(tournaments, list):
            raise WhoScoredParseError(
                f"allRegions[{region_index}].tournaments must be an array"
            )
        region_id = _required_int(region.get("id"), f"allRegions[{region_index}].id")
        for tournament_index, tournament in enumerate(tournaments):
            if not isinstance(tournament, Mapping):
                raise WhoScoredParseError(
                    f"allRegions[{region_index}].tournaments[{tournament_index}] "
                    "must be an object"
                )
            rows.append(
                {
                    "region_id": region_id,
                    "region": region.get("name"),
                    "tournament_id": _required_int(
                        tournament.get("id"), "tournament.id"
                    ),
                    "tournament": tournament.get("name"),
                    "url": tournament.get("url"),
                }
            )
    return _dataset("regions", rows)


def _season_label_to_id(label: str, season_format: SeasonFormat) -> str:
    text = re.sub(r"\s+", "", label)
    if season_format is SeasonFormat.SINGLE_YEAR:
        if not re.fullmatch(r"[0-9]{4}", text):
            raise WhoScoredParseError(f"Invalid single-year source label {label!r}")
        return canonical_season_id(text, season_format)
    match = re.fullmatch(r"([0-9]{4})[-/]([0-9]{2}|[0-9]{4})", text)
    if match is None:
        raise WhoScoredParseError(f"Invalid split-year source label {label!r}")
    first, second = match.groups()
    token = first[-2:] + second[-2:]
    return canonical_season_id(token, season_format)


def find_source_season_id(html: str, scope: WhoScoredScope) -> int:
    """Resolve one configured canonical scope against explicit season options."""

    soup = BeautifulSoup(html, "html.parser")
    matches: list[int] = []
    for option in soup.select("select[id*='seasons' i] option[value]"):
        try:
            season_id = _season_label_to_id(
                option.get_text(" ", strip=True), scope.season_format
            )
        except WhoScoredParseError:
            continue
        if season_id != scope.season_id:
            continue
        parsed = _source_url_ids(option.get("value", ""))
        if parsed["source_season_id"] is None:
            raise WhoScoredParseError("Matching season option has no source season id")
        matches.append(parsed["source_season_id"])
    soup.decompose()
    if len(set(matches)) != 1:
        raise WhoScoredParseError(
            f"Expected one source season for {scope.spec}, found {sorted(set(matches))}"
        )
    return matches[0]


def parse_season_stages(
    html: str,
    *,
    scope: WhoScoredScope,
    region_id: int,
    tournament_id: int,
    source_season_id: int,
) -> ParsedDataset:
    soup = BeautifulSoup(html, "html.parser")
    by_id: dict[int, dict[str, Any]] = {}
    base = {
        "league": scope.competition_id,
        "season": scope.season_id,
        "region_id": _required_int(region_id, "region_id"),
        "league_id": _required_int(tournament_id, "tournament_id"),
        "season_id": _required_int(source_season_id, "source_season_id"),
    }

    fixtures = soup.find(
        "a",
        href=True,
        string=lambda value: isinstance(value, str) and value.strip().lower() == "fixtures",
    )
    if fixtures is not None:
        parsed = _source_url_ids(fixtures.get("href", ""))
        if parsed["stage_id"] is not None:
            by_id[parsed["stage_id"]] = {
                **base,
                "stage_id": parsed["stage_id"],
                "stage": None,
            }

    for option in soup.select("select[id*='stages' i] option[value]"):
        parsed = _source_url_ids(option.get("value", ""))
        if parsed["stage_id"] is None:
            continue
        by_id[parsed["stage_id"]] = {
            **base,
            "stage_id": parsed["stage_id"],
            "stage": option.get_text(" ", strip=True) or None,
        }
    soup.decompose()
    if not by_id:
        raise WhoScoredParseError(f"Season page contains no stages for {scope.spec}")
    return _dataset("season_stages", [by_id[key] for key in sorted(by_id)])


def parse_calendar_months(html: str) -> tuple[CalendarMonth, ...]:
    calendar = extract_js_assignment(
        html,
        "wsCalendar",
        allow_date_expressions=True,
    )
    if not isinstance(calendar, Mapping) or not isinstance(calendar.get("mask"), Mapping):
        raise WhoScoredParseError("wsCalendar.mask must be an object")
    months: set[CalendarMonth] = set()
    for raw_year, raw_months in calendar["mask"].items():
        year = _required_int(raw_year, "wsCalendar.mask year")
        if not 1900 <= year <= 2199:
            raise WhoScoredParseError(f"Calendar year is out of range: {year}")
        if not isinstance(raw_months, Mapping):
            raise WhoScoredParseError(f"wsCalendar.mask[{year}] must be an object")
        for raw_month in raw_months:
            zero_based = _required_int(raw_month, f"wsCalendar.mask[{year}] month")
            if not 0 <= zero_based <= 11:
                raise WhoScoredParseError(
                    f"Calendar month is not zero-based 0..11: {zero_based}"
                )
            months.add(CalendarMonth(year, zero_based + 1))
    return tuple(sorted(months))


SCHEDULE_FIELDS = (
    "league",
    "season",
    "game",
    "game_id",
    "date",
    "home_team",
    "away_team",
    "home_team_id",
    "away_team_id",
    "home_score",
    "away_score",
    "status",
    "match_is_opta",
    "has_preview",
    "stage_id",
    "stage",
    "aggregate_winner_field",
    "extra_result_field",
    "home_extratime_score",
    "away_extratime_score",
    "home_penalty_score",
    "away_penalty_score",
    "home_red_cards",
    "away_red_cards",
    "home_yellow_cards",
    "away_yellow_cards",
    "home_team_country_code",
    "away_team_country_code",
    "home_team_country_name",
    "away_team_country_name",
    "bets",
    "incidents",
    "comment_count",
    "elapsed",
    "first_half_ended_at_utc",
    "has_incidents_summary",
    "is_lineup_confirmed",
    "is_stream_available",
    "is_top_match",
    "last_scorer",
    "period",
    "score_changed_at",
    "second_half_started_at_utc",
    "start_time",
    "started_at_utc",
    "winner_field",
)
_SCHEDULE_NESTED_FIELDS = {
    "aggregate_winner_field",
    "extra_result_field",
    "bets",
    "incidents",
}


def _decode_json_document(payload: str | bytes | Mapping[str, Any]) -> Mapping[str, Any]:
    if isinstance(payload, Mapping):
        return payload
    if isinstance(payload, bytes):
        try:
            payload = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise WhoScoredParseError("Schedule JSON is not UTF-8") from exc
    if not isinstance(payload, str):
        raise WhoScoredParseError("Schedule payload must be JSON text or an object")
    # Chromium may render application/json in a <pre>; extracting text through
    # the HTML parser avoids regex-based entity rewriting.
    stripped = payload.strip()
    if stripped.startswith("<"):
        soup = BeautifulSoup(stripped, "html.parser")
        pre = soup.find("pre")
        body = pre or soup.body
        stripped = body.get_text("", strip=True) if body is not None else ""
        soup.decompose()
    try:
        decoded = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise WhoScoredParseError(f"Schedule JSON is invalid: {exc.msg}") from exc
    if not isinstance(decoded, Mapping):
        raise WhoScoredParseError("Schedule JSON root must be an object")
    return decoded


def parse_schedule_json(
    payload: str | bytes | Mapping[str, Any],
    *,
    scope: WhoScoredScope,
    stage_id: int,
    stage: Optional[str] = None,
) -> ParsedDataset:
    data = _decode_json_document(payload)
    tournaments = data.get("tournaments")
    if not isinstance(tournaments, list):
        raise WhoScoredParseError("Schedule JSON must contain a tournaments array")

    by_id: dict[int, dict[str, Any]] = {}
    for tournament_index, tournament in enumerate(tournaments):
        if not isinstance(tournament, Mapping):
            raise WhoScoredParseError(f"tournaments[{tournament_index}] must be an object")
        matches = tournament.get("matches")
        if not isinstance(matches, list):
            raise WhoScoredParseError(
                f"tournaments[{tournament_index}].matches must be an array"
            )
        for match_index, raw_match in enumerate(matches):
            if not isinstance(raw_match, Mapping):
                raise WhoScoredParseError(
                    f"tournaments[{tournament_index}].matches[{match_index}] "
                    "must be an object"
                )
            normalized = {_snake(str(key)): value for key, value in raw_match.items()}
            game_id = _required_int(
                normalized.get("id", normalized.get("game_id")),
                f"matches[{match_index}].id",
            )
            home_team = normalized.get("home_team_name", normalized.get("home_team"))
            away_team = normalized.get("away_team_name", normalized.get("away_team"))
            if home_team is not None and not isinstance(home_team, str):
                raise WhoScoredParseError(f"matches[{match_index}].homeTeamName is invalid")
            if away_team is not None and not isinstance(away_team, str):
                raise WhoScoredParseError(f"matches[{match_index}].awayTeamName is invalid")
            date_value = normalized.get("start_time_utc", normalized.get("date"))
            if date_value is not None and not isinstance(date_value, str):
                raise WhoScoredParseError(f"matches[{match_index}].startTimeUtc is invalid")
            date_token = date_value[:10] if date_value else None
            game = (
                f"{date_token} {home_team}-{away_team}"
                if date_token
                else f"{home_team}-{away_team}"
            )

            row = {field: None for field in SCHEDULE_FIELDS}
            row.update(normalized)
            row.update(
                {
                    "league": scope.competition_id,
                    "season": scope.season_id,
                    "game": game,
                    "game_id": game_id,
                    "date": date_value,
                    "home_team": home_team,
                    "away_team": away_team,
                    "status": normalized.get("status", normalized.get("status_code")),
                    "stage_id": _required_int(stage_id, "stage_id"),
                    "stage": stage,
                }
            )
            for field in _SCHEDULE_NESTED_FIELDS:
                value = row.get(field)
                if isinstance(value, (list, Mapping)):
                    row[field] = canonical_json(value)
            projected = {field: row.get(field) for field in SCHEDULE_FIELDS}
            previous = by_id.get(game_id)
            if previous is not None and canonical_json(previous) != canonical_json(projected):
                raise WhoScoredParseError(
                    f"Schedule contains conflicting rows for game id {game_id}"
                )
            by_id[game_id] = projected
    return _dataset("schedule", [by_id[key] for key in sorted(by_id)])
