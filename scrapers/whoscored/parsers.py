"""Pure, transport-free parsers for WhoScored source documents.

The functions in this module accept already-fetched bytes/strings/mappings and
perform no I/O.  Source-shape drift raises :class:`WhoScoredParseError`
instead of being converted to an empty successful run.
"""

from __future__ import annotations

import json
import hashlib
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, Optional, Sequence

from bs4 import BeautifulSoup

from .catalog import (
    DEFAULT_TOURNAMENT_OVERRIDES,
    TournamentOverride,
    classify_tournament,
)
from .domain import (
    SeasonFormat,
    WhoScoredScope,
    base_season_id,
    canonical_season_id,
    disambiguated_season_id,
    source_season_id_hint,
)


PARSER_VERSION = "whoscored-parser-v7"


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
    matches: ParsedDataset
    events: ParsedDataset
    lineups: ParsedDataset
    substitutions: ParsedDataset
    formations: ParsedDataset
    team_match_stats: ParsedDataset
    player_match_stats: ParsedDataset

    @property
    def datasets(self) -> Mapping[str, ParsedDataset]:
        return {
            "matches": self.matches,
            "events": self.events,
            "lineups": self.lineups,
            "substitutions": self.substitutions,
            "formations": self.formations,
            "team_match_stats": self.team_match_stats,
            "player_match_stats": self.player_match_stats,
        }


@dataclass(frozen=True)
class SeasonParseResult:
    parser_version: str
    stages: ParsedDataset
    standings: ParsedDataset
    forms: ParsedDataset
    streaks: ParsedDataset
    performance: ParsedDataset

    @property
    def datasets(self) -> Mapping[str, ParsedDataset]:
        return {
            "stages": self.stages,
            "standings": self.standings,
            "forms": self.forms,
            "streaks": self.streaks,
            "performance": self.performance,
        }


@dataclass(frozen=True)
class PreviewParseResult:
    parser_version: str
    game_id: int
    missing_players: ParsedDataset
    preview_lineups: ParsedDataset
    preview_sections: ParsedDataset

    @property
    def datasets(self) -> Mapping[str, ParsedDataset]:
        return {
            "missing_players": self.missing_players,
            "preview_lineups": self.preview_lineups,
            "preview_sections": self.preview_sections,
        }


@dataclass(frozen=True)
class ProfileParseResult:
    parser_version: str
    player_id: int
    profiles: ParsedDataset
    participations: ParsedDataset

    @property
    def datasets(self) -> Mapping[str, ParsedDataset]:
        return {
            "player_profile_versions": self.profiles,
            "player_stage_participations": self.participations,
        }


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


def _schema_shape(value: Any) -> Any:
    """Return a deterministic structural signature without source values."""

    if isinstance(value, Mapping):
        return {
            str(key): _schema_shape(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, list):
        shapes = [_schema_shape(item) for item in value]
        if not shapes:
            return {"array": "empty"}
        encoded = [canonical_json(shape) for shape in shapes]
        if len(set(encoded)) == 1:
            return {"array": shapes[0]}
        if all(isinstance(item, (Mapping, list)) for item in value):
            variants = {token: shape for token, shape in zip(encoded, shapes)}
            return {"array_variants": [variants[key] for key in sorted(variants)]}
        # Heterogeneous primitive arrays are positional source contracts (for
        # example standings rows), so field order and arity must affect drift.
        return {"tuple": shapes}
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    return f"unsupported:{type(value).__name__}"


def schema_fingerprint(value: Any) -> str:
    """SHA-256 of the complete source field/type shape."""

    encoded = canonical_json(_schema_shape(value)).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _source_metadata(value: Any) -> dict[str, str]:
    return {
        "source_raw_json": canonical_json(value),
        "source_schema_fingerprint": schema_fingerprint(value),
    }


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
            # WhoScored historical table arrays occasionally contain a real
            # JavaScript elision (`,,`).  Preserve its positional meaning as
            # JSON null; do not execute or rewrite any other expression.
            self._space()
            if self._consume(","):
                result.append(None)
                self._space()
                if self._consume("]"):
                    return result
                continue
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
                if (
                    escaped == "\r"
                    and self.pos < len(self.text)
                    and self.text[self.pos] == "\n"
                ):
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


def extract_js_call_arguments(source: str, call_name: str) -> tuple[Any, ...]:
    """Safely extract object/array literals passed to repeated JS calls.

    WhoScored builds season tables with ``tables.push({...})`` rather than one
    final assignment.  This scanner parses only literal arguments and never
    evaluates JavaScript.
    """

    if not re.fullmatch(
        r"[A-Za-z_$][A-Za-z0-9_$]*(?:\.[A-Za-z_$][A-Za-z0-9_$]*)*", call_name
    ):
        raise ValueError(f"Unsafe JavaScript call name {call_name!r}")
    pattern = re.compile(rf"(?<![A-Za-z0-9_$]){re.escape(call_name)}\s*\(\s*")
    values: list[Any] = []
    for match in pattern.finditer(source):
        start = match.end()
        if start >= len(source) or source[start] not in "[{":
            raise JavaScriptLiteralError(
                f"{call_name} argument must be an object or array literal"
            )
        literal, _ = _scan_balanced(source, start)
        values.append(parse_js_literal(literal))
    return tuple(values)


def _scan_js_string(source: str, start: int) -> tuple[str, int]:
    if start >= len(source) or source[start] not in "'\"":
        raise JavaScriptLiteralError("Expected a JavaScript string literal")
    quote = source[start]
    escaped = False
    index = start + 1
    while index < len(source):
        char = source[index]
        if escaped:
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == quote:
            return source[start : index + 1], index + 1
        index += 1
    raise JavaScriptLiteralError("Embedded JavaScript string is unterminated")


def extract_json_parse_property(source: str, property_name: str) -> Any:
    """Decode the exact ``property: JSON.parse('<json>')`` source idiom."""

    if not _IDENTIFIER_RE.fullmatch(property_name):
        raise ValueError(f"Unsafe JavaScript property name {property_name!r}")
    pattern = re.compile(
        rf"(?:(?:['\"]{re.escape(property_name)}['\"])|"
        rf"(?:\b{re.escape(property_name)}\b))\s*:\s*JSON\.parse\(\s*"
    )
    match = pattern.search(source)
    if match is None:
        raise JavaScriptLiteralError(
            f"JSON.parse property {property_name!r} was not found"
        )
    literal, _ = _scan_js_string(source, match.end())
    decoded_string = parse_js_literal(literal)
    if not isinstance(decoded_string, str):
        raise JavaScriptLiteralError(
            f"JSON.parse property {property_name!r} does not contain a string"
        )
    try:
        return json.loads(decoded_string)
    except json.JSONDecodeError as exc:
        raise JavaScriptLiteralError(
            f"JSON.parse property {property_name!r} contains invalid JSON: {exc.msg}"
        ) from exc


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
    return value


# ---------------------------------------------------------------------------
# Match payload
# ---------------------------------------------------------------------------

_SNAKE_1 = re.compile(r"(.)([A-Z][a-z]+)")
_SNAKE_2 = re.compile(r"([a-z0-9])([A-Z])")


def _snake(name: str) -> str:
    return (
        _SNAKE_2.sub(r"\1_\2", _SNAKE_1.sub(r"\1_\2", name)).lower().replace("-", "_")
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


def _required_source_bigint(value: Any, field: str) -> int:
    """Parse an Opta identity without silently rounding a JSON float."""

    if isinstance(value, bool) or value is None or value == "":
        raise WhoScoredParseError(f"{field} is required")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        token = value.strip()
        if re.fullmatch(r"[+]?[0-9]+", token) is None:
            raise WhoScoredParseError(f"{field} is not an integer: {value!r}")
        parsed = int(token)
    elif isinstance(value, float):
        if (
            not math.isfinite(value)
            or not value.is_integer()
            or abs(value) > 9_007_199_254_740_991
        ):
            raise WhoScoredParseError(
                f"{field} is not an exactly representable integer: {value!r}"
            )
        parsed = int(value)
    else:
        raise WhoScoredParseError(f"{field} is not numeric: {value!r}")
    if parsed <= 0 or parsed > 9_223_372_036_854_775_807:
        raise WhoScoredParseError(f"{field} must be a positive BIGINT")
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


def _optional_preview_rating(value: Any) -> Optional[float]:
    """Parse the preview UI's exact not-yet-rated placeholder."""

    if isinstance(value, str) and value.strip().casefold() == "n/a":
        return None
    return _optional_float(value, "preview.rating")


def _optional_bool(value: Any, field: str) -> Optional[bool]:
    """Parse source booleans without Python's ``bool('false')`` trap."""

    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        token = value.strip().casefold()
        if token in {"true", "1"}:
            return True
        if token in {"false", "0"}:
            return False
    raise WhoScoredParseError(f"{field} is not a boolean: {value!r}")


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
    "team_event_id",
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
    "related_team_event_id",
    "related_player_id",
    "satisfied_events_types",
    "league",
    "season",
    "game",
    "source_raw_json",
    "source_schema_fingerprint",
)


def parse_events(
    data: Mapping[str, Any],
    *,
    scope: WhoScoredScope,
    game_id: int,
    game: Optional[str] = None,
) -> ParsedDataset:
    if "events" not in data or data.get("events") is None:
        return ParsedDataset(
            "events",
            DatasetStatus.NOT_AVAILABLE,
            reason="source_events_absent",
        )
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
    seen_source_ids: set[int] = set()
    seen_team_event_ids: set[tuple[Optional[int], int]] = set()
    identity = _identity_fields(scope, _required_int(game_id, "game_id"), game)
    for index, event in enumerate(raw_events):
        if not isinstance(event, Mapping):
            raise WhoScoredParseError(f"events[{index}] must be an object")
        # ``id`` is the stable, match-unique Opta record identity.  The
        # similarly named ``eventId`` restarts independently for both teams
        # and is therefore only a team-local sequence (verified against live
        # World Cup payloads where hundreds of eventId values occur twice).
        # Never fall back from id to eventId: doing so silently merges real
        # actions from opposite teams.
        source_event_id = _required_source_bigint(
            event.get("id"), f"events[{index}].id"
        )
        if source_event_id in seen_source_ids:
            raise WhoScoredParseError(
                f"Duplicate global source event id {source_event_id} in game {game_id}"
            )
        seen_source_ids.add(source_event_id)

        team_event_id = _required_int(event.get("eventId"), f"events[{index}].eventId")
        if team_event_id <= 0:
            raise WhoScoredParseError(
                f"events[{index}].eventId must be a positive integer"
            )

        team_id = _optional_int(event.get("teamId"), f"events[{index}].teamId")
        team_event_key = (team_id, team_event_id)
        if team_event_key in seen_team_event_ids:
            raise WhoScoredParseError(
                "Duplicate team-local event identity "
                f"{team_event_key!r} in game {game_id}"
            )
        seen_team_event_ids.add(team_event_key)
        player_id = _optional_int(event.get("playerId"), f"events[{index}].playerId")
        qualifiers = event.get("qualifiers")
        if qualifiers is None:
            qualifiers = []
        if not isinstance(qualifiers, list):
            raise WhoScoredParseError(f"events[{index}].qualifiers must be a list")

        row = {
            **identity,
            "source_event_id": source_event_id,
            "team_event_id": team_event_id,
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
            "is_touch": _optional_bool(
                event.get("isTouch", False), f"events[{index}].isTouch"
            ),
            "is_shot": _optional_bool(
                event.get("isShot", False), f"events[{index}].isShot"
            ),
            "is_goal": _optional_bool(
                event.get("isGoal", False), f"events[{index}].isGoal"
            ),
            "card_type": _display(event.get("cardType")),
            "related_team_event_id": _optional_int(
                event.get("relatedEventId"), f"events[{index}].relatedEventId"
            ),
            "related_player_id": _optional_int(
                event.get("relatedPlayerId"), f"events[{index}].relatedPlayerId"
            ),
            "satisfied_events_types": canonical_json(
                event.get("satisfiedEventsTypes") or []
            ),
            **_source_metadata(event),
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
        team = (
            side_value.get("name") if isinstance(side_value.get("name"), str) else None
        )

        red_minutes: dict[int, int] = {}
        incidents = side_value.get("incidentEvents") or []
        if not isinstance(incidents, list):
            raise WhoScoredParseError(f"{side}.incidentEvents must be a list")
        for index, incident in enumerate(incidents):
            if not isinstance(incident, Mapping):
                raise WhoScoredParseError(
                    f"{side}.incidentEvents[{index}] must be an object"
                )
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
            is_starter = bool(
                _optional_bool(
                    player.get("isFirstEleven", False),
                    f"{side}.players[{index}].isFirstEleven",
                )
            )
            sub_in = _optional_int(
                player.get("subbedInExpandedMinute"),
                f"{side}.players[{index}].subbedInExpandedMinute",
            )
            sub_out = _optional_int(
                player.get("subbedOutExpandedMinute"),
                f"{side}.players[{index}].subbedOutExpandedMinute",
            )
            effective_end = (
                sub_out if sub_out is not None else red_minutes.get(player_id)
            )
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
                    "player": player.get("name")
                    if isinstance(player.get("name"), str)
                    else None,
                    "shirt_no": _optional_int(
                        player.get("shirtNo"), f"{side}.players[{index}].shirtNo"
                    ),
                    "position": position if isinstance(position, str) else None,
                    "is_starter": is_starter,
                    "is_man_of_the_match": bool(
                        _optional_bool(
                            player.get("isManOfTheMatch", False),
                            f"{side}.players[{index}].isManOfTheMatch",
                        )
                    ),
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
                    **_source_metadata(player),
                }
            )

    if not rows and not player_blocks_present:
        return ParsedDataset(
            "lineups",
            DatasetStatus.NOT_AVAILABLE,
            reason="source_player_blocks_absent",
        )
    return _dataset("lineups", rows, empty_reason="source_player_blocks_empty")


def _mapping_text(value: Any, *keys: str) -> Optional[str]:
    if isinstance(value, str):
        return value or None
    if isinstance(value, Mapping):
        for key in keys:
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate:
                return candidate
    return None


def parse_match_record(
    data: Mapping[str, Any],
    *,
    scope: WhoScoredScope,
    game_id: int,
    game: Optional[str] = None,
) -> ParsedDataset:
    """Project the match header while retaining the complete source object."""

    identity = _identity_fields(scope, _required_int(game_id, "game_id"), game)
    home = data.get("home") or {}
    away = data.get("away") or {}
    if not isinstance(home, Mapping) or not isinstance(away, Mapping):
        raise WhoScoredParseError("matchCentreData home/away must be objects")
    referee = data.get("referee") or data.get("matchOfficial") or {}
    if referee is not None and not isinstance(referee, (Mapping, str)):
        raise WhoScoredParseError("matchCentreData.referee must be an object or string")
    home_scores = home.get("scores") or {}
    away_scores = away.get("scores") or {}
    if not isinstance(home_scores, Mapping):
        home_scores = {}
    if not isinstance(away_scores, Mapping):
        away_scores = {}
    row = {
        **identity,
        "home_team_id": _optional_int(home.get("teamId"), "home.teamId"),
        "home_team": _mapping_text(home.get("name"), "name", "displayName")
        or (home.get("name") if isinstance(home.get("name"), str) else None),
        "away_team_id": _optional_int(away.get("teamId"), "away.teamId"),
        "away_team": _mapping_text(away.get("name"), "name", "displayName")
        or (away.get("name") if isinstance(away.get("name"), str) else None),
        "home_score": _optional_int(
            data.get("homeScore", home_scores.get("fulltime")), "homeScore"
        ),
        "away_score": _optional_int(
            data.get("awayScore", away_scores.get("fulltime")), "awayScore"
        ),
        "status": _display(data.get("status"))
        or (data.get("status") if isinstance(data.get("status"), str) else None),
        "period": _display(data.get("period")),
        "expanded_max_minute": _optional_int(
            data.get("expandedMaxMinute"), "expandedMaxMinute"
        ),
        "attendance": _optional_int(data.get("attendance"), "attendance"),
        "venue_name": _mapping_text(
            data.get("venueName", data.get("venue")), "name", "displayName"
        ),
        "referee_id": _optional_int(
            referee.get("officialId", referee.get("id"))
            if isinstance(referee, Mapping)
            else None,
            "referee.officialId",
        ),
        "referee_name": _mapping_text(referee, "name", "displayName"),
        "weather": canonical_json(data.get("weather"))
        if data.get("weather") is not None
        else None,
        "start_time": data.get("startTime")
        if isinstance(data.get("startTime"), str)
        else None,
        "home_manager": _mapping_text(home.get("manager"), "name", "displayName"),
        "away_manager": _mapping_text(away.get("manager"), "name", "displayName"),
        **_source_metadata(data),
    }
    if row["home_team_id"] is None and row["home_team"] is None:
        raise WhoScoredParseError("matchCentreData.home has no team identity")
    if row["away_team_id"] is None and row["away_team"] is None:
        raise WhoScoredParseError("matchCentreData.away has no team identity")
    return _dataset("matches", [row])


def _stat_leaf_rows(
    value: Any,
    path: tuple[str, ...] = (),
    path_kinds: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            rows.extend(_stat_leaf_rows(child, (*path, str(key)), (*path_kinds, "key")))
        return rows
    if isinstance(value, list):
        for index, child in enumerate(value):
            rows.extend(
                _stat_leaf_rows(child, (*path, str(index)), (*path_kinds, "index"))
            )
        return rows
    numeric_value: Optional[float] = None
    text_value: Optional[str] = None
    boolean_value: Optional[bool] = None
    if isinstance(value, bool):
        boolean_value = value
    elif isinstance(value, (int, float)):
        numeric_value = _optional_float(value, ".".join(path) or "stat")
    elif isinstance(value, str):
        text_value = value
    rows.append(
        {
            "category": path[0] if path else None,
            "subcategory": path[1] if len(path) > 2 else None,
            "stat": path[-1] if path else None,
            "filter": ".".join(path[1:-1]) or None,
            "minute": next(
                (
                    int(part)
                    for part, kind in reversed(tuple(zip(path, path_kinds)))
                    if kind == "key" and part.isdecimal()
                ),
                None,
            ),
            "numeric_value": numeric_value,
            "text_value": text_value,
            "boolean_value": boolean_value,
            "value_json": canonical_json(value),
            "source_path": ".".join(path),
        }
    )
    return rows


def _parse_match_stats(
    data: Mapping[str, Any],
    *,
    scope: WhoScoredScope,
    game_id: int,
    game: Optional[str],
    players: bool,
) -> ParsedDataset:
    name = "player_match_stats" if players else "team_match_stats"
    identity = _identity_fields(scope, _required_int(game_id, "game_id"), game)
    rows: list[dict[str, Any]] = []
    blocks_present = False
    for side in ("home", "away"):
        team = data.get(side) or {}
        if not isinstance(team, Mapping):
            raise WhoScoredParseError(f"matchCentreData.{side} must be an object")
        team_id = _optional_int(team.get("teamId"), f"{side}.teamId")
        team_name = team.get("name") if isinstance(team.get("name"), str) else None
        entities: Sequence[Any]
        if players:
            raw_players = team.get("players")
            if raw_players is None:
                entities = ()
            elif isinstance(raw_players, list):
                entities = raw_players
            else:
                raise WhoScoredParseError(f"{side}.players must be a list")
        else:
            entities = (team,)
        for entity_index, entity in enumerate(entities):
            if not isinstance(entity, Mapping):
                raise WhoScoredParseError(f"{name} source entity must be an object")
            block_names = ("stats",) if players else ("stats", "shotZones")
            present_blocks = [block for block in block_names if block in entity]
            if not present_blocks:
                continue
            blocks_present = True
            player_id = (
                _required_int(
                    entity.get("playerId"), f"players[{entity_index}].playerId"
                )
                if players
                else None
            )
            player_name = (
                entity.get("name")
                if players and isinstance(entity.get("name"), str)
                else None
            )
            for block_name in present_blocks:
                stats = entity.get(block_name)
                if stats is None:
                    stats = {}
                if not isinstance(stats, (Mapping, list)):
                    raise WhoScoredParseError(
                        f"{name}.{block_name} must be an object or array"
                    )
                initial_path = () if block_name == "stats" else (block_name,)
                initial_kinds = () if block_name == "stats" else ("key",)
                for leaf in _stat_leaf_rows(
                    stats, path=initial_path, path_kinds=initial_kinds
                ):
                    rows.append(
                        {
                            **identity,
                            "side": side,
                            "team_id": team_id,
                            "team": team_name,
                            "player_id": player_id,
                            "player": player_name,
                            **leaf,
                            # The entity object is shared by every flattened
                            # stat leaf. Keep its shape for drift detection;
                            # the complete document remains in raw storage.
                            "source_schema_fingerprint": schema_fingerprint(entity),
                        }
                    )
    if not rows and not blocks_present:
        return ParsedDataset(
            name, DatasetStatus.NOT_AVAILABLE, reason="source_stats_absent"
        )
    return _dataset(name, rows, empty_reason="source_stats_empty")


def parse_substitutions(
    data: Mapping[str, Any],
    *,
    scope: WhoScoredScope,
    game_id: int,
    game: Optional[str] = None,
) -> ParsedDataset:
    identity = _identity_fields(scope, _required_int(game_id, "game_id"), game)
    rows: list[dict[str, Any]] = []
    blocks_present = False
    seen: set[tuple[str, int, str, int]] = set()
    for side in ("home", "away"):
        team = data.get(side) or {}
        if not isinstance(team, Mapping):
            raise WhoScoredParseError(f"matchCentreData.{side} must be an object")
        raw_players = team.get("players")
        if raw_players is None:
            continue
        blocks_present = True
        if not isinstance(raw_players, list):
            raise WhoScoredParseError(f"{side}.players must be a list")
        for index, player in enumerate(raw_players):
            if not isinstance(player, Mapping):
                raise WhoScoredParseError(f"{side}.players[{index}] must be an object")
            player_id = _required_int(
                player.get("playerId"), f"{side}.players[{index}].playerId"
            )
            for action, source_key in (
                ("on", "subbedInExpandedMinute"),
                ("off", "subbedOutExpandedMinute"),
            ):
                minute = _optional_int(
                    player.get(source_key), f"{side}.players[{index}].{source_key}"
                )
                if minute is None:
                    continue
                key = (side, player_id, action, minute)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    {
                        **identity,
                        "side": side,
                        "team_id": _optional_int(team.get("teamId"), f"{side}.teamId"),
                        "team": team.get("name")
                        if isinstance(team.get("name"), str)
                        else None,
                        "player_id": player_id,
                        "player": player.get("name")
                        if isinstance(player.get("name"), str)
                        else None,
                        "action": action,
                        "expanded_minute": minute,
                        "related_player_id": None,
                        **_source_metadata(player),
                    }
                )
    if not rows and not blocks_present:
        return ParsedDataset(
            "substitutions",
            DatasetStatus.NOT_AVAILABLE,
            reason="source_player_blocks_absent",
        )
    return _dataset("substitutions", rows, empty_reason="source_lists_no_substitutions")


def parse_formations(
    data: Mapping[str, Any],
    *,
    scope: WhoScoredScope,
    game_id: int,
    game: Optional[str] = None,
) -> ParsedDataset:
    identity = _identity_fields(scope, _required_int(game_id, "game_id"), game)
    rows: list[dict[str, Any]] = []
    blocks_present = False
    for side in ("home", "away"):
        team = data.get(side) or {}
        if not isinstance(team, Mapping):
            raise WhoScoredParseError(f"matchCentreData.{side} must be an object")
        formations = team.get("formations")
        if formations is None:
            continue
        blocks_present = True
        if not isinstance(formations, list):
            raise WhoScoredParseError(f"{side}.formations must be a list")
        for index, formation in enumerate(formations):
            if not isinstance(formation, Mapping):
                raise WhoScoredParseError(
                    f"{side}.formations[{index}] must be an object"
                )
            rows.append(
                {
                    **identity,
                    "side": side,
                    "team_id": _optional_int(team.get("teamId"), f"{side}.teamId"),
                    "team": team.get("name")
                    if isinstance(team.get("name"), str)
                    else None,
                    "formation_index": index,
                    "formation_id": _optional_int(
                        formation.get("formationId", formation.get("id")),
                        f"{side}.formations[{index}].formationId",
                    ),
                    "formation_name": _mapping_text(
                        formation.get("formationName", formation.get("name")),
                        "name",
                        "displayName",
                    ),
                    "start_expanded_minute": _optional_int(
                        formation.get(
                            "startMinuteExpanded", formation.get("startExpandedMinute")
                        ),
                        f"{side}.formations[{index}].startMinuteExpanded",
                    ),
                    "end_expanded_minute": _optional_int(
                        formation.get(
                            "endMinuteExpanded", formation.get("endExpandedMinute")
                        ),
                        f"{side}.formations[{index}].endMinuteExpanded",
                    ),
                    "captain_player_id": _optional_int(
                        formation.get("captainPlayerId"),
                        f"{side}.formations[{index}].captainPlayerId",
                    ),
                    "player_ids": canonical_json(formation.get("playerIds"))
                    if formation.get("playerIds") is not None
                    else None,
                    "formation_slots": canonical_json(formation.get("formationSlots"))
                    if formation.get("formationSlots") is not None
                    else None,
                    "formation_positions": canonical_json(
                        formation.get("formationPositions")
                    )
                    if formation.get("formationPositions") is not None
                    else None,
                    "jersey_numbers": canonical_json(formation.get("jerseyNumbers"))
                    if formation.get("jerseyNumbers") is not None
                    else None,
                    **_source_metadata(formation),
                }
            )
    if not rows and not blocks_present:
        return ParsedDataset(
            "formations", DatasetStatus.NOT_AVAILABLE, reason="source_formations_absent"
        )
    return _dataset("formations", rows, empty_reason="source_formations_empty")


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
        matches=parse_match_record(data, scope=scope, game_id=game_id, game=game),
        events=parse_events(data, scope=scope, game_id=game_id, game=game),
        lineups=parse_lineups(data, scope=scope, game_id=game_id, game=game),
        substitutions=parse_substitutions(
            data, scope=scope, game_id=game_id, game=game
        ),
        formations=parse_formations(data, scope=scope, game_id=game_id, game=game),
        team_match_stats=_parse_match_stats(
            data, scope=scope, game_id=game_id, game=game, players=False
        ),
        player_match_stats=_parse_match_stats(
            data, scope=scope, game_id=game_id, game=game, players=True
        ),
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


def _parse_profile_core(html: str, *, player_id: int | str) -> ParsedDataset:
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
    row.update(
        _source_metadata(
            {
                key: {
                    "text": _text_after_label(parent),
                    "links": [
                        {
                            "href": anchor.get("href"),
                            "text": anchor.get_text(" ", strip=True),
                        }
                        for anchor in parent.find_all("a", href=True)
                    ],
                }
                for key, parent in fields.items()
            }
        )
    )
    soup.decompose()
    return _dataset("profiles", [row])


_PROFILE_ASSIGNMENTS = (
    "currentParticipations",
    "playerParticipations",
    "playerStatistics",
    "latestMatches",
)
_PROFILE_REQUIRE_ARGS_PATTERN = re.compile(
    r"require\s*\.\s*config\s*\.\s*params\s*"
    r"\[\s*['\"]args['\"]\s*\]\s*=\s*"
)


def _mapping_value_casefold(value: Mapping[str, Any], *candidate_names: str) -> Any:
    """Read one source field while accepting its observed casing only.

    Profile-page ``require.config.params['args']`` records use PascalCase,
    while older embedded assignments used camelCase.  Normalising lookup is
    safer than duplicating every projection and the complete source object is
    still retained alongside the projected fields.
    """

    names = {name.casefold() for name in candidate_names}
    for key, child in value.items():
        if str(key).casefold() in names:
            return child
    return None


def _walk_profile_records(
    value: Any, path: tuple[str, ...] = ()
) -> list[tuple[tuple[str, ...], Mapping[str, Any]]]:
    found: list[tuple[tuple[str, ...], Mapping[str, Any]]] = []
    if isinstance(value, Mapping):
        source_keys = {str(key).casefold() for key in value}
        if source_keys & {
            "stageid",
            "seasonid",
            "tournamentid",
            "matchid",
            "gameid",
        }:
            found.append((path, value))
        for key, child in value.items():
            found.extend(_walk_profile_records(child, (*path, str(key))))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(_walk_profile_records(child, (*path, str(index))))
    return found


def parse_profile_bundle(
    html: str,
    *,
    player_id: int | str,
    parser_version: str = PARSER_VERSION,
) -> ProfileParseResult:
    """Parse global identity plus stage participations/latest-match records."""

    source_player_id = _required_int(player_id, "player_id")
    profile = _parse_profile_core(html, player_id=source_player_id)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    structured_present = False
    structured_sources: list[tuple[str, Any]] = []
    for variable in _PROFILE_ASSIGNMENTS:
        try:
            value = extract_js_assignment(html, variable)
        except JavaScriptLiteralError:
            continue
        structured_present = True
        structured_sources.append((variable, value))

    try:
        profile_args = _extract_after_pattern(
            html,
            _PROFILE_REQUIRE_ARGS_PATTERN,
            label="require.config.params['args'] profile assignment",
            allow_date_expressions=False,
        )
    except JavaScriptLiteralError:
        profile_args = None
    else:
        if not isinstance(profile_args, Mapping):
            raise WhoScoredParseError("Profile require args must be an object")
        tournaments = _mapping_value_casefold(profile_args, "tournaments")
        if tournaments is not None:
            if not isinstance(tournaments, list):
                raise WhoScoredParseError(
                    "Profile require args tournaments must be an array"
                )
            structured_present = True
            structured_sources.append(("profileArgs.tournaments", tournaments))

    for variable, value in structured_sources:
        for path, record in _walk_profile_records(value, (variable,)):
            raw = canonical_json(record)
            if raw in seen:
                continue
            seen.add(raw)
            path_text = ".".join(path)
            rows.append(
                {
                    "player_id": source_player_id,
                    "record_type": "latest_match"
                    if "match" in path_text.casefold()
                    else "participation",
                    "region_id": _optional_int(
                        _mapping_value_casefold(record, "regionId"), "regionId"
                    ),
                    "tournament_id": _optional_int(
                        _mapping_value_casefold(record, "tournamentId"),
                        "tournamentId",
                    ),
                    "source_season_id": _optional_int(
                        _mapping_value_casefold(record, "seasonId"), "seasonId"
                    ),
                    "stage_id": _optional_int(
                        _mapping_value_casefold(record, "stageId"), "stageId"
                    ),
                    "game_id": _optional_int(
                        _mapping_value_casefold(record, "matchId", "gameId"),
                        "gameId",
                    ),
                    "tournament": _mapping_value_casefold(record, "tournamentName")
                    if isinstance(
                        _mapping_value_casefold(record, "tournamentName"), str
                    )
                    else None,
                    "season": _mapping_value_casefold(record, "seasonName")
                    if isinstance(_mapping_value_casefold(record, "seasonName"), str)
                    else None,
                    "stage": _mapping_value_casefold(record, "stageName")
                    if isinstance(_mapping_value_casefold(record, "stageName"), str)
                    else None,
                    "team_id": _optional_int(
                        _mapping_value_casefold(record, "teamId"), "teamId"
                    ),
                    "team": _mapping_value_casefold(record, "teamName")
                    if isinstance(_mapping_value_casefold(record, "teamName"), str)
                    else None,
                    "position": _display(
                        _mapping_value_casefold(
                            record,
                            "position",
                            "positionText",
                            "positionShort",
                            "positionLong",
                            "playedPositionsRaw",
                        )
                    )
                    or (
                        _mapping_value_casefold(
                            record,
                            "position",
                            "positionText",
                            "positionShort",
                            "positionLong",
                            "playedPositionsRaw",
                        )
                        if isinstance(
                            _mapping_value_casefold(
                                record,
                                "position",
                                "positionText",
                                "positionShort",
                                "positionLong",
                                "playedPositionsRaw",
                            ),
                            str,
                        )
                        else None
                    ),
                    "source_path": path_text,
                    **_source_metadata(record),
                }
            )

    soup = BeautifulSoup(html, "html.parser")
    container_pattern = re.compile(
        r"participation|latest.?match|current.?team|tournament", re.IGNORECASE
    )
    containers = soup.find_all(id=container_pattern)
    for container_index, container in enumerate(containers):
        label = str(container.get("id") or "")
        record_type = "latest_match" if "match" in label.casefold() else "participation"
        candidates = container.select("tr") or [container]
        for row_index, element in enumerate(candidates):
            anchors = element.find_all("a", href=True)
            # Only a stage/match URL is evidence of a participation. Generic
            # tournament tab anchors (``#player-tournament-stats-summary``)
            # are UI navigation, not source records.
            source_url = next(
                (
                    anchor.get("href", "")
                    for anchor in anchors
                    if re.search(
                        r"/(?:Stages|Matches)/\d+(?:/|$)",
                        anchor.get("href", ""),
                        re.IGNORECASE,
                    )
                ),
                "",
            )
            if not source_url:
                continue
            structured_present = True
            try:
                ids = _source_url_ids(source_url)
            except WhoScoredParseError:
                ids = {key: None for key in _SOURCE_URL_SEGMENTS}
            game_match = re.search(r"/Matches/(\d+)(?:/|$)", source_url, re.IGNORECASE)
            raw_record = {
                "container": label,
                "text": element.get_text(" ", strip=True),
                "links": [
                    {
                        "href": anchor.get("href"),
                        "text": anchor.get_text(" ", strip=True),
                    }
                    for anchor in anchors
                ],
            }
            encoded = canonical_json(raw_record)
            if encoded in seen:
                continue
            seen.add(encoded)
            rows.append(
                {
                    "player_id": source_player_id,
                    "record_type": record_type,
                    "region_id": ids.get("region_id"),
                    "tournament_id": ids.get("tournament_id"),
                    "source_season_id": ids.get("source_season_id"),
                    "stage_id": ids.get("stage_id"),
                    "game_id": int(game_match.group(1)) if game_match else None,
                    "tournament": None,
                    "season": None,
                    "stage": None,
                    "team_id": None,
                    "team": None,
                    "position": None,
                    "source_path": f"dom.{label}[{container_index}].row[{row_index}]",
                    **_source_metadata(raw_record),
                }
            )
    soup.decompose()
    participations = (
        _dataset(
            "player_stage_participations",
            rows,
            empty_reason="source_participation_sections_empty",
        )
        if structured_present
        else ParsedDataset(
            "player_stage_participations",
            DatasetStatus.NOT_AVAILABLE,
            reason="source_participation_sections_absent",
        )
    )
    return ProfileParseResult(
        parser_version=parser_version,
        player_id=source_player_id,
        profiles=profile,
        participations=participations,
    )


def _parse_missing_players_soup(
    soup: BeautifulSoup,
    *,
    scope: WhoScoredScope,
    game_id: int,
    game: Optional[str],
    home_team: Optional[str],
    away_team: Optional[str],
) -> ParsedDataset:
    container = soup.find(id="missing-players")
    if container is None:
        return ParsedDataset(
            "missing_players",
            DatasetStatus.NOT_AVAILABLE,
            reason="source_missing_players_section_absent",
        )
    tables = container.find_all("table")
    if len(tables) > 2:
        raise WhoScoredParseError(
            "Missing-player section contains more than two tables"
        )
    teams = (home_team, away_team)
    rows: list[dict[str, Any]] = []
    for table_index, table in enumerate(tables):
        for row_index, table_row in enumerate(table.select("tbody tr")):
            player_cell = table_row.select_one("td.pn")
            anchor = player_cell.find("a", href=True) if player_cell else None
            if anchor is None:
                raise WhoScoredParseError(
                    f"Missing-player row {table_index}:{row_index} has no player link"
                )
            match = _PLAYER_URL_RE.search(anchor.get("href", ""))
            if match is None:
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
                    **_source_metadata(
                        {
                            "player_href": anchor.get("href", ""),
                            "player": anchor.get_text(" ", strip=True),
                            "reason": reason,
                            "status": status_cell.get_text(" ", strip=True)
                            if status_cell is not None
                            else None,
                        }
                    ),
                }
            )
    return _dataset(
        "missing_players", rows, empty_reason="source_lists_no_missing_players"
    )


_PREVIEW_ASSIGNMENTS = (
    "matchHeaderJson",
    "previewData",
    "predictedLineup",
    "predictedLineups",
    "teamComparisonData",
    "headToHeadData",
    "topPlayersData",
)


def _walk_preview_players(
    value: Any, path: tuple[str, ...] = ()
) -> list[tuple[tuple[str, ...], Mapping[str, Any]]]:
    found: list[tuple[tuple[str, ...], Mapping[str, Any]]] = []
    if isinstance(value, Mapping):
        context = ".".join(path).casefold()
        if (
            "lineup" in context or "predicted" in context or "formation" in context
        ) and any(key in value for key in ("playerId", "player_id")):
            found.append((path, value))
        for key, child in value.items():
            found.extend(_walk_preview_players(child, (*path, str(key))))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(_walk_preview_players(child, (*path, str(index))))
    return found


def _has_preview_lineup_structure(value: Any, path: tuple[str, ...] = ()) -> bool:
    """Return whether a decoded preview object declares a lineup container.

    Empty predicted-lineup arrays are authoritative empties, but only when the
    source actually exposes that structure.  Merely finding a match header or
    another preview section must not turn an absent lineup feed into EMPTY.
    """

    if isinstance(value, Mapping):
        for key, child in value.items():
            child_path = (*path, str(key))
            token = ".".join(child_path).casefold()
            if any(marker in token for marker in ("predicted", "lineup", "formation")):
                if isinstance(child, (Mapping, list)):
                    return True
            if _has_preview_lineup_structure(child, child_path):
                return True
    elif isinstance(value, list):
        return any(
            _has_preview_lineup_structure(child, (*path, str(index)))
            for index, child in enumerate(value)
        )
    return False


def parse_preview_bundle(
    html: str,
    *,
    scope: WhoScoredScope,
    game_id: int,
    game: Optional[str],
    home_team: Optional[str],
    away_team: Optional[str],
    parser_version: str = PARSER_VERSION,
) -> PreviewParseResult:
    """Parse missing players, predicted XI and all structured preview sections."""

    if not isinstance(html, str) or not html:
        raise WhoScoredParseError("Preview HTML is empty")
    soup = BeautifulSoup(html, "html.parser")
    missing = _parse_missing_players_soup(
        soup,
        scope=scope,
        game_id=game_id,
        game=game,
        home_team=home_team,
        away_team=away_team,
    )
    identity = _identity_fields(scope, _required_int(game_id, "game_id"), game)
    sections: list[dict[str, Any]] = []
    lineup_rows: list[dict[str, Any]] = []
    seen_lineups: set[tuple[Optional[str], int]] = set()
    preview_structure_present = soup.find(id="missing-players") is not None
    lineup_structure_present = False

    for variable in _PREVIEW_ASSIGNMENTS:
        try:
            value = extract_js_assignment(html, variable)
        except JavaScriptLiteralError:
            try:
                value = extract_json_parse_property(html, variable)
            except JavaScriptLiteralError:
                continue
        preview_structure_present = True
        if variable.casefold().startswith("predicted") or _has_preview_lineup_structure(
            value, (variable,)
        ):
            lineup_structure_present = True
        sections.append(
            {
                **identity,
                "section_type": variable,
                "source": "embedded_javascript",
                "heading": None,
                "text": None,
                **_source_metadata(value),
            }
        )
        for path, player in _walk_preview_players(value, (variable,)):
            player_id = _required_int(
                player.get("playerId", player.get("player_id")),
                f"{variable}.{'.'.join(path)}.playerId",
            )
            path_text = ".".join(path).casefold()
            side = (
                "home"
                if "home" in path_text
                else "away"
                if "away" in path_text
                else None
            )
            key = (side, player_id)
            if key in seen_lineups:
                continue
            seen_lineups.add(key)
            position = player.get("position")
            lineup_rows.append(
                {
                    **identity,
                    "side": side,
                    "team": home_team
                    if side == "home"
                    else away_team
                    if side == "away"
                    else None,
                    "player_id": player_id,
                    "player": player.get("playerName", player.get("name"))
                    if isinstance(player.get("playerName", player.get("name")), str)
                    else None,
                    "position": _display(position)
                    or (position if isinstance(position, str) else None),
                    "formation": player.get("formation")
                    if isinstance(player.get("formation"), str)
                    else None,
                    "rating": _optional_preview_rating(player.get("rating")),
                    "source_path": ".".join(path),
                    **_source_metadata(player),
                }
            )

    section_pattern = re.compile(
        r"preview|team.?news|comparison|head.?to.?head|predicted|top.?player|missing",
        re.IGNORECASE,
    )
    dom_sections = soup.find_all(id=section_pattern)
    preview_structure_present = preview_structure_present or bool(dom_sections)
    for index, element in enumerate(dom_sections):
        section_type = str(element.get("id") or f"dom_section_{index}")
        if any(row["section_type"] == section_type for row in sections):
            continue
        structured = {
            "id": element.get("id"),
            "heading": (
                element.find(re.compile(r"^h[1-6]$")).get_text(" ", strip=True)
                if element.find(re.compile(r"^h[1-6]$")) is not None
                else None
            ),
            "rows": [
                [
                    cell.get_text(" ", strip=True)
                    for cell in table_row.find_all(["th", "td"])
                ]
                for table_row in element.select("tr")
            ],
            "text": element.get_text(" ", strip=True),
        }
        sections.append(
            {
                **identity,
                "section_type": section_type,
                "source": "dom",
                "heading": structured["heading"],
                "text": structured["text"] or None,
                **_source_metadata(structured),
            }
        )

    # DOM fallback for pages whose predicted XI is not duplicated in JS.
    lineup_containers = soup.select(
        "[id*='predicted' i], [class*='predicted' i], [id*='lineup' i]"
    )
    lineup_structure_present = lineup_structure_present or bool(lineup_containers)
    preview_structure_present = preview_structure_present or bool(lineup_containers)
    for container_index, container in enumerate(lineup_containers):
        formation_by_side: dict[str, Optional[str]] = {}
        team_by_side: dict[str, Optional[str]] = {}
        for side_name in ("home", "away"):
            header = container.select_one(f".pitch-formation-header .{side_name}")
            formation_marker = header.select_one(".formation-label") if header else None
            team_marker = header.select_one(".team-link") if header else None
            formation_by_side[side_name] = (
                formation_marker.get_text(" ", strip=True) if formation_marker else None
            )
            team_by_side[side_name] = (
                team_marker.get_text(" ", strip=True) if team_marker else None
            )
        for anchor in container.find_all("a", href=True):
            match = _PLAYER_URL_RE.search(anchor.get("href", ""))
            if match is None:
                continue
            player_node = anchor.find_parent(attrs={"data-playerid": True})
            player_id = int(match.group(1))
            if player_node is not None:
                player_id = _required_int(
                    player_node.get("data-playerid"), "preview.data-playerid"
                )
            side = None
            parent = anchor.parent
            while parent is not None and parent is not container:
                classes = set(parent.get("class") or ())
                if "home" in classes:
                    side = "home"
                    break
                if "away" in classes:
                    side = "away"
                    break
                parent = parent.parent
            key = (side, player_id)
            if key in seen_lineups:
                continue
            seen_lineups.add(key)
            title = player_node.get("title") if player_node is not None else None
            position_match = re.search(r"\(([^()]*)\)\s*$", title or "")
            rating_marker = (
                player_node.select_one(".player-rating")
                if player_node is not None
                else None
            )
            raw = {
                "href": anchor.get("href", ""),
                "text": anchor.get_text(" ", strip=True),
                "title": title or anchor.get("title"),
                "data_player_id": player_node.get("data-playerid")
                if player_node is not None
                else None,
                "rating": rating_marker.get_text(" ", strip=True)
                if rating_marker is not None
                else None,
            }
            lineup_rows.append(
                {
                    **identity,
                    "side": side,
                    "team": (
                        home_team or team_by_side.get("home")
                        if side == "home"
                        else away_team or team_by_side.get("away")
                        if side == "away"
                        else None
                    ),
                    "player_id": player_id,
                    "player": anchor.get_text(" ", strip=True) or None,
                    "position": anchor.get("data-position")
                    or (position_match.group(1) if position_match else None),
                    "formation": formation_by_side.get(side) if side else None,
                    "rating": _optional_preview_rating(
                        rating_marker.get_text(" ", strip=True)
                        if rating_marker is not None
                        else None
                    ),
                    "source_path": f"dom.predicted[{container_index}]",
                    **_source_metadata(raw),
                }
            )
    soup.decompose()
    return PreviewParseResult(
        parser_version=parser_version,
        game_id=int(game_id),
        missing_players=missing,
        preview_lineups=(
            _dataset(
                "preview_lineups",
                lineup_rows,
                empty_reason="source_lists_no_predicted_lineup",
            )
            if lineup_structure_present
            else ParsedDataset(
                "preview_lineups",
                DatasetStatus.NOT_AVAILABLE,
                reason="source_predicted_lineup_structure_absent",
            )
        ),
        preview_sections=(
            _dataset(
                "preview_sections",
                sections,
                empty_reason="source_lists_no_structured_preview_sections",
            )
            if preview_structure_present
            else ParsedDataset(
                "preview_sections",
                DatasetStatus.NOT_AVAILABLE,
                reason="source_preview_structure_absent",
            )
        ),
    )


# ---------------------------------------------------------------------------
# Competition discovery, stages, calendar and schedule
# ---------------------------------------------------------------------------

_SOURCE_URL_SEGMENTS = {
    "region_id": re.compile(r"/Regions/(\d+)(?:/|$)", re.IGNORECASE),
    "tournament_id": re.compile(r"/Tournaments/(\d+)(?:/|$)", re.IGNORECASE),
    "source_season_id": re.compile(r"/Seasons/(\d+)(?:/|$)", re.IGNORECASE),
    "stage_id": re.compile(r"/Stages/(\d+)(?:/|$)", re.IGNORECASE),
}


def _decode_discovery_document(
    payload: str | bytes | Mapping[str, Any] | Sequence[Any],
    *,
    variable_names: Sequence[str] = ("allRegions", "allRegionsData"),
) -> Any:
    if isinstance(payload, Mapping) or (
        isinstance(payload, Sequence)
        and not isinstance(payload, (str, bytes, bytearray))
    ):
        return payload
    if isinstance(payload, bytes):
        try:
            payload = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise WhoScoredParseError("Discovery payload is not UTF-8") from exc
    if not isinstance(payload, str) or not payload.strip():
        raise WhoScoredParseError("Discovery payload is empty")
    stripped = payload.strip()
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass
    for variable_name in variable_names:
        try:
            return extract_js_assignment(stripped, variable_name)
        except JavaScriptLiteralError:
            continue
    if stripped.startswith("<"):
        soup = BeautifulSoup(stripped, "html.parser")
        pre = soup.find("pre")
        container = pre or soup.body
        text = container.get_text("", strip=True) if container is not None else ""
        soup.decompose()
        if text:
            try:
                return json.loads(text)
            except json.JSONDecodeError as exc:
                # FlareSolverr sometimes renders an application/json response
                # as a bare text node under <body>, without Chromium's usual
                # <pre>.  Once either container starts like structured JSON,
                # treat malformed content as parser drift rather than silently
                # falling through to "container absent".
                if pre is not None or text.lstrip().startswith(("{", "[")):
                    raise WhoScoredParseError(
                        f"Discovery JSON is invalid: {exc.msg}"
                    ) from exc
    raise WhoScoredParseError("Could not find a structured discovery payload")


def parse_all_regions(
    payload: str | bytes | Mapping[str, Any] | Sequence[Any],
    *,
    overrides: Sequence[TournamentOverride] = DEFAULT_TOURNAMENT_OVERRIDES,
    competition_aliases: Optional[Mapping[tuple[int, int], str]] = None,
) -> ParsedDataset:
    """Parse every source tournament and attach an explicit disposition.

    The all-regions document normally lacks authoritative sex metadata.  Such
    adult-looking rows are retained as ``quarantined`` and are expected to be
    reclassified after schedule metadata is observed; discovery must still
    fan out through them.
    """

    decoded = _decode_discovery_document(payload)
    if isinstance(decoded, Mapping):
        regions = decoded.get("regions", decoded.get("allRegions", decoded.get("data")))
    else:
        regions = decoded
    if not isinstance(regions, list):
        raise WhoScoredParseError("allRegions must be an array")
    rows: list[dict[str, Any]] = []
    seen: dict[tuple[int, int], str] = {}
    override_aliases = {
        int(item.tournament_id): item.canonical_competition_id
        for item in overrides
        if item.canonical_competition_id
    }
    source_aliases = {
        (int(region_id), int(tournament_id)): str(competition_id).strip()
        for (region_id, tournament_id), competition_id in (
            competition_aliases or {}
        ).items()
        if str(competition_id).strip()
    }
    for region_index, region in enumerate(regions):
        if not isinstance(region, Mapping):
            raise WhoScoredParseError(f"allRegions[{region_index}] must be an object")
        region_id = _required_int(
            region.get("id", region.get("regionId")),
            f"allRegions[{region_index}].id",
        )
        region_name = region.get("name", region.get("regionName"))
        if not isinstance(region_name, str) or not region_name.strip():
            raise WhoScoredParseError(
                f"allRegions[{region_index}].name must be a non-empty string"
            )
        tournaments = region.get("tournaments")
        if tournaments is None:
            tournaments = []
        if not isinstance(tournaments, list):
            raise WhoScoredParseError(
                f"allRegions[{region_index}].tournaments must be an array"
            )
        for tournament_index, tournament in enumerate(tournaments):
            if not isinstance(tournament, Mapping):
                raise WhoScoredParseError(
                    f"allRegions[{region_index}].tournaments[{tournament_index}] "
                    "must be an object"
                )
            tournament_id = _required_int(
                tournament.get("id", tournament.get("tournamentId")),
                f"tournaments[{tournament_index}].id",
            )
            tournament_name = tournament.get("name", tournament.get("tournamentName"))
            if not isinstance(tournament_name, str) or not tournament_name.strip():
                raise WhoScoredParseError(
                    f"tournaments[{tournament_index}].name must be a non-empty string"
                )
            classification = classify_tournament(
                tournament_id=tournament_id,
                tournament_name=tournament_name,
                region_name=region_name,
                source_sex=tournament.get("sex", region.get("sex")),
                overrides=overrides,
            )
            source_sex = classification.source_sex
            combined_raw = {
                "region": {
                    key: value for key, value in region.items() if key != "tournaments"
                },
                "tournament": dict(tournament),
            }
            competition_id = (
                source_aliases.get((region_id, tournament_id))
                or override_aliases.get(tournament_id)
                or f"WS-{region_id}-{tournament_id}"
            )
            row = {
                "competition_id": competition_id,
                "region_id": region_id,
                "region_name": region_name.strip(),
                "region_code": region.get("code", region.get("regionCode")),
                "region_flag": region.get("flg", region.get("flag")),
                "tournament_id": tournament_id,
                "tournament_name": tournament_name.strip(),
                "tournament_url": tournament.get("url"),
                "sort_order": _optional_int(
                    tournament.get("sortOrder"),
                    f"tournaments[{tournament_index}].sortOrder",
                ),
                "source_sex": source_sex,
                "eligibility": classification.eligibility.value,
                "classification_reason": classification.reason,
                "classifier_version": classification.classifier_version,
                "override_version": classification.override_version,
                **_source_metadata(combined_raw),
            }
            key = (region_id, tournament_id)
            encoded = canonical_json(row)
            if key in seen and seen[key] != encoded:
                raise WhoScoredParseError(
                    f"allRegions contains conflicting tournament {tournament_id} in region {region_id}"
                )
            if key not in seen:
                rows.append(row)
                seen[key] = encoded
    if not rows:
        raise WhoScoredParseError("allRegions contains no tournaments")
    return _dataset("competitions", rows)


def _infer_source_season(label: str) -> tuple[Optional[str], Optional[SeasonFormat]]:
    compact = re.sub(r"\s+", "", label)
    if re.fullmatch(
        r"[0-9]{4}(?:spring|summer|fall|autumn|winter)?",
        compact,
        re.IGNORECASE,
    ):
        try:
            return _season_label_to_id(
                label, SeasonFormat.SINGLE_YEAR
            ), SeasonFormat.SINGLE_YEAR
        except ValueError:
            return None, None
    if re.fullmatch(r"[0-9]{4}[-/][0-9]{2,4}", compact):
        for season_format in (SeasonFormat.SPLIT_YEAR, SeasonFormat.MULTI_YEAR):
            try:
                return _season_label_to_id(compact, season_format), season_format
            except (WhoScoredParseError, ValueError):
                continue
        return None, None
    return None, None


def parse_tournament_seasons(
    html: str,
    *,
    competition_row: Mapping[str, Any],
) -> ParsedDataset:
    """Return every source season option, including quarantined labels."""

    if not isinstance(html, str) or not html:
        raise WhoScoredParseError("Tournament HTML is empty")
    competition_id = str(competition_row.get("competition_id") or "").strip()
    if not competition_id:
        raise WhoScoredParseError("competition_row.competition_id is required")
    region_id = _required_int(competition_row.get("region_id"), "region_id")
    tournament_id = _required_int(competition_row.get("tournament_id"), "tournament_id")
    inherited_eligibility = str(competition_row.get("eligibility") or "quarantined")
    inherited_reason = str(
        competition_row.get("classification_reason") or "tournament_unclassified"
    )
    soup = BeautifulSoup(html, "html.parser")
    options = soup.select("select[id*='seasons' i] option[value]")
    any_selected = any(option.has_attr("selected") for option in options)
    rows: list[dict[str, Any]] = []
    seen: set[int] = set()
    for index, option in enumerate(options):
        label = option.get_text(" ", strip=True)
        url = option.get("value", "")
        try:
            ids = _source_url_ids(url)
        except WhoScoredParseError:
            # UI placeholders are not source seasons.  Non-placeholder labels
            # with a URL-like value are retained below as quarantined rows.
            if not url or url in {"#", "0", "-1"}:
                continue
            ids = {"source_season_id": None}
        source_season_id = ids.get("source_season_id")
        canonical, fmt = _infer_source_season(label)
        eligibility = inherited_eligibility
        reason = inherited_reason
        if source_season_id is None:
            eligibility = "quarantined"
            reason = "season_url_has_no_source_season_id"
            # A stable negative surrogate is forbidden; keep the actual null.
        elif canonical is None or fmt is None:
            eligibility = "quarantined"
            reason = "unrecognized_source_season_label"
        if source_season_id is not None:
            if source_season_id in seen:
                soup.decompose()
                raise WhoScoredParseError(
                    f"Tournament page repeats source season id {source_season_id}"
                )
            seen.add(source_season_id)
        raw = {
            "label": label,
            "value": url,
            "attributes": {key: value for key, value in option.attrs.items()},
        }
        rows.append(
            {
                "competition_id": competition_id,
                "region_id": region_id,
                "tournament_id": tournament_id,
                "season_id": canonical,
                "source_season_id": source_season_id,
                "source_label": label or None,
                "season_format": fmt.value if fmt else None,
                "source_url": url or None,
                "start": None,
                "end": None,
                # ``selected`` is only the website's default dropdown value;
                # it is not evidence that fixtures are currently active.
                "source_selected": option.has_attr("selected")
                if any_selected
                else False,
                "is_active": None,
                "eligibility": eligibility,
                "classification_reason": reason,
                **_source_metadata(raw),
            }
        )
    canonical_groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        canonical = row.get("season_id")
        if (
            canonical
            and row.get("source_season_id") is not None
            and row.get("season_format")
        ):
            canonical_groups.setdefault(str(canonical), []).append(row)
    for colliding in canonical_groups.values():
        if len(colliding) < 2:
            continue
        for row in colliding:
            row["season_id"] = disambiguated_season_id(
                str(row["season_id"]),
                str(row["season_format"]),
                int(row["source_season_id"]),
            )
    soup.decompose()
    if not rows:
        raise WhoScoredParseError(
            f"Tournament page contains no source seasons for {competition_id}"
        )
    return _dataset("seasons", rows)


def _source_url_ids(url: str) -> dict[str, Optional[int]]:
    matches = {
        key: pattern.search(url or "") for key, pattern in _SOURCE_URL_SEGMENTS.items()
    }
    if not any(matches.values()):
        raise WhoScoredParseError(f"Could not parse WhoScored source URL {url!r}")
    return {
        key: int(match.group(1)) if match is not None else None
        for key, match in matches.items()
    }


def _season_label_to_id(label: str, season_format: SeasonFormat) -> str:
    text = re.sub(r"\s+", "", label)
    if season_format is SeasonFormat.SINGLE_YEAR:
        match = re.fullmatch(
            r"([0-9]{4})(?:spring|summer|fall|autumn|winter)?",
            text,
            re.IGNORECASE,
        )
        if match is None:
            raise WhoScoredParseError(f"Invalid single-year source label {label!r}")
        return canonical_season_id(match.group(1), season_format)
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
    expected_base = base_season_id(scope.season_id)
    expected_source_id = source_season_id_hint(scope.season_id)
    for option in soup.select("select[id*='seasons' i] option[value]"):
        try:
            season_id = _season_label_to_id(
                option.get_text(" ", strip=True), scope.season_format
            )
        except WhoScoredParseError:
            continue
        if season_id != expected_base:
            continue
        parsed = _source_url_ids(option.get("value", ""))
        if parsed["source_season_id"] is None:
            raise WhoScoredParseError("Matching season option has no source season id")
        if (
            expected_source_id is not None
            and parsed["source_season_id"] != expected_source_id
        ):
            continue
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
        "competition_id": scope.competition_id,
        "league": scope.competition_id,
        "season": scope.season_id,
        "region_id": _required_int(region_id, "region_id"),
        "tournament_id": _required_int(tournament_id, "tournament_id"),
        "league_id": _required_int(tournament_id, "tournament_id"),
        "source_season_id": _required_int(source_season_id, "source_season_id"),
        "season_id": _required_int(source_season_id, "source_season_id"),
        "eligibility": "included",
        "classification_reason": "valid_stage_for_configured_scope",
    }

    fixtures = soup.find(
        "a",
        href=True,
        string=lambda value: (
            isinstance(value, str) and value.strip().lower() == "fixtures"
        ),
    )
    if fixtures is not None:
        parsed = _source_url_ids(fixtures.get("href", ""))
        if parsed["stage_id"] is not None:
            by_id[parsed["stage_id"]] = {
                **base,
                "stage_id": parsed["stage_id"],
                "stage": None,
                "source_url": fixtures.get("href", "") or None,
                **_source_metadata(
                    {
                        "label": fixtures.get_text(" ", strip=True),
                        "href": fixtures.get("href", ""),
                    }
                ),
            }

    for option in soup.select("select[id*='stages' i] option[value]"):
        parsed = _source_url_ids(option.get("value", ""))
        if parsed["stage_id"] is None:
            continue
        by_id[parsed["stage_id"]] = {
            **base,
            "stage_id": parsed["stage_id"],
            "stage": option.get_text(" ", strip=True) or None,
            "source_url": option.get("value", "") or None,
            **_source_metadata(
                {
                    "label": option.get_text(" ", strip=True),
                    "value": option.get("value", ""),
                    "attributes": dict(option.attrs),
                }
            ),
        }
    soup.decompose()
    if not by_id:
        raise WhoScoredParseError(f"Season page contains no stages for {scope.spec}")
    return _dataset("season_stages", [by_id[key] for key in sorted(by_id)])


def _table_kind(value: Mapping[str, Any]) -> str:
    hint = " ".join(
        str(value.get(key) or "")
        for key in ("type", "name", "title", "tableType", "statType", "key")
    ).casefold()
    keys = " ".join(str(key).casefold() for key in value)
    combined = f"{hint} {keys}"
    if "streak" in combined:
        return "streaks"
    if "performance" in combined:
        return "performance"
    if "form" in combined:
        return "forms"
    return "standings"


def _season_table_blocks(
    value: Mapping[str, Any],
) -> list[tuple[str, Mapping[str, Any]]]:
    """Split a composite table object without discarding any source field."""

    special: list[tuple[str, Mapping[str, Any]]] = []
    ordinary: dict[str, Any] = {}
    context = {
        key: value.get(key)
        for key in (
            "stageId",
            "stageName",
            "name",
            "title",
            "type",
            "standingsGroupIdx",
            "rankColorings",
            "startDate",
            "endDate",
        )
        if key in value
    }
    for key, child in value.items():
        token = str(key).casefold()
        if "streak" in token:
            special.append(("streaks", {**context, str(key): child}))
        elif "performance" in token:
            special.append(("performance", {**context, str(key): child}))
        elif "form" in token:
            special.append(("forms", {**context, str(key): child}))
        else:
            ordinary[str(key)] = child
    if ordinary and set(ordinary) - set(context):
        special.insert(0, (_table_kind(ordinary), ordinary))
    if not special:
        special.append((_table_kind(value), value))
    return special


def _record_arrays(
    value: Any, path: tuple[str, ...] = ()
) -> list[tuple[str, list[Any]]]:
    found: list[tuple[str, list[Any]]] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            child_path = (*path, str(key))
            if (
                isinstance(child, list)
                and child
                and all(isinstance(item, (Mapping, list, tuple)) for item in child)
            ):
                found.append((".".join(child_path), child))
            elif isinstance(child, Mapping):
                found.extend(_record_arrays(child, child_path))
    return found


def _is_season_table_record(value: Any) -> bool:
    """Distinguish team rows from layout metadata nested beside the table.

    Current season pages place arrays such as ``rankColorings`` in the same
    object as standings rows.  Their first value can be a DOM id (for example
    ``standing-zone-top-1``), so treating every nested array as a positional
    team row both invents data and turns a harmless layout change into an
    invalid ``stageId``.  Source team rows have an explicit team identity;
    positional rows use the stable ``stage id, team id, team name`` prefix.
    """

    if isinstance(value, Mapping):
        team = value.get("team")
        team_id = value.get("teamId")
        if team_id is None and isinstance(team, Mapping):
            team_id = team.get("id", team.get("teamId"))
        try:
            return not isinstance(team_id, bool) and int(team_id) > 0
        except (TypeError, ValueError):
            return False
    if not isinstance(value, (list, tuple)) or len(value) < 3:
        return False
    try:
        stage_id = int(value[0])
        team_id = int(value[1])
    except (TypeError, ValueError):
        return False
    return (
        not isinstance(value[0], bool)
        and not isinstance(value[1], bool)
        and stage_id > 0
        and team_id > 0
        and isinstance(value[2], str)
        and bool(value[2].strip())
    )


def _common_table_fields(record: Any, *, kind: str) -> dict[str, Any]:
    if isinstance(record, (list, tuple)):
        # WhoScored's live season tables are positional arrays.  Only the
        # stable identity prefix and the well-established standings/form
        # columns are named here; the complete array remains in raw_json.
        team_id = record[1] if len(record) > 1 else None
        team_name = record[2] if len(record) > 2 else None
        rank = record[3] if len(record) > 3 else None
        played = (
            record[4] if kind in {"standings", "forms"} and len(record) > 4 else None
        )
        points = (
            record[11] if kind in {"standings", "forms"} and len(record) > 11 else None
        )
        return {
            "team_id": _optional_int(team_id, "season_table[1]"),
            "team": team_name if isinstance(team_name, str) else None,
            "rank": _optional_int(rank, "season_table[3]"),
            "played": _optional_int(played, "season_table[4]"),
            "points": _optional_int(points, "season_table[11]"),
            "group_name": None,
            "source_values_json": canonical_json(record),
        }
    if not isinstance(record, Mapping):
        raise WhoScoredParseError("Season table row must be an object or array")
    team = record.get("team")
    team_id = record.get("teamId")
    team_name = record.get("teamName")
    if isinstance(team, Mapping):
        team_id = team_id if team_id is not None else team.get("id", team.get("teamId"))
        team_name = team_name or team.get("name", team.get("teamName"))
    elif isinstance(team, str):
        team_name = team_name or team
    return {
        "team_id": _optional_int(team_id, "season_table.teamId"),
        "team": team_name if isinstance(team_name, str) else None,
        "rank": _optional_int(
            record.get("rank", record.get("position")), "season_table.rank"
        ),
        "played": _optional_int(
            record.get("played", record.get("gamesPlayed")), "season_table.played"
        ),
        "points": _optional_int(record.get("points"), "season_table.points"),
        "group_name": record.get("groupName")
        if isinstance(record.get("groupName"), str)
        else None,
        "source_values_json": None,
    }


def parse_season_tables(
    html: str,
    *,
    scope: WhoScoredScope,
    source_season_id: int,
) -> Mapping[str, ParsedDataset]:
    """Parse initial ``tables`` plus every ``tables.push`` mutation."""

    payloads: list[Any] = []
    source_present = False
    try:
        initial = extract_js_assignment(html, "tables")
    except JavaScriptLiteralError:
        initial = []
    else:
        source_present = True
    if isinstance(initial, list):
        payloads.extend(initial)
    elif isinstance(initial, Mapping):
        payloads.append(initial)
    else:
        raise WhoScoredParseError("Season tables assignment must be an object or array")
    pushed = extract_js_call_arguments(html, "tables.push")
    source_present = source_present or bool(pushed)
    payloads.extend(pushed)

    if not source_present:
        return {
            kind: ParsedDataset(
                kind,
                DatasetStatus.NOT_AVAILABLE,
                reason=f"source_has_no_{kind}_container",
            )
            for kind in ("standings", "forms", "streaks", "performance")
        }

    rows_by_kind: dict[str, list[dict[str, Any]]] = {
        "standings": [],
        "forms": [],
        "streaks": [],
        "performance": [],
    }
    for table_index, payload in enumerate(payloads):
        if not isinstance(payload, Mapping):
            # Array pushes are legal; preserve each mapping member separately.
            if isinstance(payload, list) and all(
                isinstance(item, Mapping) for item in payload
            ):
                blocks: Sequence[Mapping[str, Any]] = payload
            else:
                raise WhoScoredParseError(
                    f"Season table payload {table_index} is not structured"
                )
        else:
            blocks = (payload,)
        split_blocks = [
            (kind, split)
            for source_block in blocks
            for kind, split in _season_table_blocks(source_block)
        ]
        for block_index, (kind, block) in enumerate(split_blocks):
            arrays = _record_arrays(block)
            records: list[tuple[str, Any]] = []
            for path, array in arrays:
                records.extend(
                    (path, item) for item in array if _is_season_table_record(item)
                )
            if not records and _is_season_table_record(block):
                records = [("", block)]
            for row_index, (source_path, record) in enumerate(records):
                rows_by_kind[kind].append(
                    {
                        "league": scope.competition_id,
                        "season": scope.season_id,
                        "source_season_id": _required_int(
                            source_season_id, "source_season_id"
                        ),
                        "table_index": table_index,
                        "block_index": block_index,
                        "row_index": row_index,
                        "table_type": kind,
                        "source_path": source_path or None,
                        "stage_id": _optional_int(
                            (
                                record.get("stageId", block.get("stageId"))
                                if isinstance(record, Mapping)
                                else record[0]
                                if record
                                else block.get("stageId")
                            ),
                            "season_table.stageId",
                        ),
                        "start_date": block.get("startDate"),
                        "end_date": block.get("endDate"),
                        **_common_table_fields(record, kind=kind),
                        **_source_metadata(record),
                        "table_raw_json": canonical_json(block),
                        "table_schema_fingerprint": schema_fingerprint(block),
                    }
                )
    return {
        kind: _dataset(kind, rows, empty_reason=f"source_has_no_{kind}")
        for kind, rows in rows_by_kind.items()
    }


def parse_season_page(
    html: str,
    *,
    scope: WhoScoredScope,
    region_id: int,
    tournament_id: int,
    source_season_id: int,
    parser_version: str = PARSER_VERSION,
) -> SeasonParseResult:
    stages = parse_season_stages(
        html,
        scope=scope,
        region_id=region_id,
        tournament_id=tournament_id,
        source_season_id=source_season_id,
    )
    tables = parse_season_tables(html, scope=scope, source_season_id=source_season_id)
    return SeasonParseResult(
        parser_version=parser_version,
        stages=stages,
        standings=tables["standings"],
        forms=tables["forms"],
        streaks=tables["streaks"],
        performance=tables["performance"],
    )


def parse_calendar_months(html: str) -> tuple[CalendarMonth, ...]:
    calendar = extract_js_assignment(
        html,
        "wsCalendar",
        allow_date_expressions=True,
    )
    if not isinstance(calendar, Mapping) or not isinstance(
        calendar.get("mask"), Mapping
    ):
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
    "region_id",
    "region_code",
    "region_name",
    "tournament_id",
    "tournament_name",
    "source_season_id",
    "source_season_name",
    "source_sex",
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
    "source_raw_json",
    "source_schema_fingerprint",
)
_SCHEDULE_NESTED_FIELDS = {
    "aggregate_winner_field",
    "extra_result_field",
    "bets",
    "incidents",
}


def _decode_json_document(
    payload: str | bytes | Mapping[str, Any],
) -> Mapping[str, Any]:
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
            raise WhoScoredParseError(
                f"tournaments[{tournament_index}] must be an object"
            )
        matches = tournament.get("matches")
        if not isinstance(matches, list):
            raise WhoScoredParseError(
                f"tournaments[{tournament_index}].matches must be an array"
            )
        tournament_metadata = {
            "region_id": _optional_int(
                tournament.get("regionId"),
                f"tournaments[{tournament_index}].regionId",
            ),
            "region_code": tournament.get("regionCode")
            if isinstance(tournament.get("regionCode"), str)
            else None,
            "region_name": tournament.get("regionName")
            if isinstance(tournament.get("regionName"), str)
            else None,
            "tournament_id": _optional_int(
                tournament.get("tournamentId"),
                f"tournaments[{tournament_index}].tournamentId",
            ),
            "tournament_name": tournament.get("tournamentName")
            if isinstance(tournament.get("tournamentName"), str)
            else None,
            "source_season_id": _optional_int(
                tournament.get("seasonId"),
                f"tournaments[{tournament_index}].seasonId",
            ),
            "source_season_name": tournament.get("seasonName")
            if isinstance(tournament.get("seasonName"), str)
            else None,
            "source_sex": _optional_int(
                tournament.get("sex"), f"tournaments[{tournament_index}].sex"
            ),
        }
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
                raise WhoScoredParseError(
                    f"matches[{match_index}].homeTeamName is invalid"
                )
            if away_team is not None and not isinstance(away_team, str):
                raise WhoScoredParseError(
                    f"matches[{match_index}].awayTeamName is invalid"
                )
            date_value = normalized.get("start_time_utc", normalized.get("date"))
            if date_value is not None and not isinstance(date_value, str):
                raise WhoScoredParseError(
                    f"matches[{match_index}].startTimeUtc is invalid"
                )
            date_token = date_value[:10] if date_value else None
            kickoff = None
            if date_value:
                try:
                    kickoff = datetime.fromisoformat(date_value.replace("Z", "+00:00"))
                except ValueError as exc:
                    raise WhoScoredParseError(
                        f"matches[{match_index}].startTimeUtc is invalid"
                    ) from exc
                if kickoff.tzinfo is not None:
                    kickoff = kickoff.astimezone(timezone.utc).replace(tzinfo=None)
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
                    **tournament_metadata,
                    "game": game,
                    "game_id": game_id,
                    "date": kickoff,
                    "home_team": home_team,
                    "away_team": away_team,
                    "status": normalized.get("status", normalized.get("status_code")),
                    "stage_id": _required_int(stage_id, "stage_id"),
                    "stage": stage,
                    "match_is_opta": _optional_bool(
                        normalized.get("match_is_opta"),
                        f"matches[{match_index}].matchIsOpta",
                    ),
                    "has_preview": _optional_bool(
                        normalized.get("has_preview"),
                        f"matches[{match_index}].hasPreview",
                    ),
                    **_source_metadata(raw_match),
                }
            )
            for field in _SCHEDULE_NESTED_FIELDS:
                value = row.get(field)
                if isinstance(value, (list, Mapping)):
                    row[field] = canonical_json(value)
            projected = {field: row.get(field) for field in SCHEDULE_FIELDS}
            previous = by_id.get(game_id)
            if previous is not None and canonical_json(previous) != canonical_json(
                projected
            ):
                raise WhoScoredParseError(
                    f"Schedule contains conflicting rows for game id {game_id}"
                )
            by_id[game_id] = projected
    return _dataset("schedule", [by_id[key] for key in sorted(by_id)])


def parse_schedule_incidents(schedule: ParsedDataset) -> ParsedDataset:
    """Normalize source incident summaries without pretending they are Opta events.

    The schedule feed is the only structured incident source for many non-Opta
    matches.  It provides no stable event id, so source array order/path is the
    honest identity and the complete incident object remains attached.
    """

    if schedule.name != "schedule":
        raise ValueError("schedule incident parser requires the schedule dataset")
    if schedule.status is DatasetStatus.NOT_AVAILABLE:
        return ParsedDataset(
            "match_incidents",
            DatasetStatus.NOT_AVAILABLE,
            reason="source_schedule_unavailable",
        )

    rows: list[dict[str, Any]] = []
    for schedule_row in schedule.rows:
        raw_incidents = schedule_row.get("incidents")
        if raw_incidents in (None, ""):
            if schedule_row.get("has_incidents_summary") is True:
                raise WhoScoredParseError(
                    f"game {schedule_row.get('game_id')} declares an incident "
                    "summary but incidents are absent"
                )
            continue
        if isinstance(raw_incidents, str):
            try:
                decoded = json.loads(raw_incidents)
            except json.JSONDecodeError as exc:
                raise WhoScoredParseError(
                    f"game {schedule_row.get('game_id')} incidents JSON is invalid"
                ) from exc
        else:
            decoded = raw_incidents
        if not isinstance(decoded, list):
            raise WhoScoredParseError(
                f"game {schedule_row.get('game_id')} incidents must be an array"
            )

        game_id = _required_int(schedule_row.get("game_id"), "game_id")
        for ordinal, incident in enumerate(decoded):
            if not isinstance(incident, Mapping):
                raise WhoScoredParseError(
                    f"game {game_id} incidents[{ordinal}] must be an object"
                )
            normalized = {_snake(str(key)): value for key, value in incident.items()}
            player = normalized.get("player")
            participating = normalized.get("participating_player")
            if player is not None and not isinstance(player, (str, Mapping)):
                raise WhoScoredParseError(
                    f"game {game_id} incidents[{ordinal}].player is invalid"
                )
            if participating is not None and not isinstance(
                participating, (str, Mapping)
            ):
                raise WhoScoredParseError(
                    f"game {game_id} incidents[{ordinal}].participatingPlayer is invalid"
                )
            source_path = f"incidents[{ordinal}]"
            rows.append(
                {
                    "league": schedule_row.get("league"),
                    "season": schedule_row.get("season"),
                    "game_id": game_id,
                    "game": schedule_row.get("game"),
                    "stage_id": schedule_row.get("stage_id"),
                    "stage": schedule_row.get("stage"),
                    "match_is_opta": schedule_row.get("match_is_opta"),
                    "entity_key": f"{game_id}:{source_path}",
                    "source_ordinal": ordinal,
                    "source_path": source_path,
                    "source_incident_id": (
                        str(normalized.get("id"))
                        if normalized.get("id") is not None
                        else None
                    ),
                    "incident_type": _display(normalized.get("type")),
                    "incident_subtype": _display(normalized.get("sub_type")),
                    "minute": _optional_int(normalized.get("minute"), "minute"),
                    "expanded_minute": _optional_int(
                        normalized.get("expanded_minute"), "expandedMinute"
                    ),
                    "period": _display(normalized.get("period")),
                    "field": _display(normalized.get("field")),
                    "team_id": _optional_int(normalized.get("team_id"), "teamId"),
                    "team": _mapping_text(normalized.get("team"), "name", "displayName")
                    or _display(normalized.get("team_name")),
                    "player_id": _optional_int(
                        normalized.get("player_id")
                        or (
                            player.get("playerId", player.get("id"))
                            if isinstance(player, Mapping)
                            else None
                        ),
                        "playerId",
                    ),
                    "player": (
                        player
                        if isinstance(player, str)
                        else _mapping_text(player, "name", "displayName")
                    )
                    or _display(normalized.get("player_name")),
                    "participating_player_id": _optional_int(
                        normalized.get("participating_player_id")
                        or (
                            participating.get("playerId", participating.get("id"))
                            if isinstance(participating, Mapping)
                            else None
                        ),
                        "participatingPlayerId",
                    ),
                    "participating_player": (
                        participating
                        if isinstance(participating, str)
                        else _mapping_text(participating, "name", "displayName")
                    )
                    or _display(normalized.get("participating_player_name")),
                    **_source_metadata(incident),
                }
            )
    return _dataset(
        "match_incidents",
        rows,
        empty_reason="source_schedule_contains_no_incident_rows",
    )


def parse_schedule_bets(schedule: ParsedDataset) -> ParsedDataset:
    """Normalize the source's 1X2 bookmaker offers at match-offer grain.

    The schedule response embeds three markets (home/draw/away), each with a
    stable source bet id and one offer per provider.  Array order is retained
    for provenance, but logical identity uses source ids so a harmless provider
    reordering cannot create duplicate Bronze rows.  Click-out URLs are stored
    as source data only; the scraper never requests them.
    """

    if schedule.name != "schedule":
        raise ValueError("schedule bet parser requires the schedule dataset")
    if schedule.status is DatasetStatus.NOT_AVAILABLE:
        return ParsedDataset(
            "match_bets",
            DatasetStatus.NOT_AVAILABLE,
            reason="source_schedule_unavailable",
        )

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for schedule_row in schedule.rows:
        raw_bets = schedule_row.get("bets")
        if raw_bets in (None, ""):
            continue
        if isinstance(raw_bets, str):
            try:
                decoded = json.loads(raw_bets)
            except json.JSONDecodeError as exc:
                raise WhoScoredParseError(
                    f"game {schedule_row.get('game_id')} bets JSON is invalid"
                ) from exc
        else:
            decoded = raw_bets
        if not isinstance(decoded, Mapping):
            raise WhoScoredParseError(
                f"game {schedule_row.get('game_id')} bets must be an object"
            )

        game_id = _required_int(schedule_row.get("game_id"), "game_id")
        for source_outcome, market in decoded.items():
            if not isinstance(source_outcome, str) or not source_outcome.strip():
                raise WhoScoredParseError(
                    f"game {game_id} bets contains an invalid outcome key"
                )
            if not isinstance(market, Mapping):
                raise WhoScoredParseError(
                    f"game {game_id} bets.{source_outcome} must be an object"
                )
            source_bet_id = market.get("betId")
            if source_bet_id in (None, ""):
                raise WhoScoredParseError(
                    f"game {game_id} bets.{source_outcome}.betId is required"
                )
            source_bet_id = str(source_bet_id)
            bet_name = market.get("betName")
            if bet_name is not None and not isinstance(bet_name, str):
                raise WhoScoredParseError(
                    f"game {game_id} bets.{source_outcome}.betName must be text"
                )
            offers = market.get("offers")
            if not isinstance(offers, list):
                raise WhoScoredParseError(
                    f"game {game_id} bets.{source_outcome}.offers must be an array"
                )
            for offer_ordinal, offer in enumerate(offers):
                if not isinstance(offer, Mapping):
                    raise WhoScoredParseError(
                        f"game {game_id} bets.{source_outcome}.offers"
                        f"[{offer_ordinal}] must be an object"
                    )
                provider_id = _required_int(
                    offer.get("providerId"),
                    f"bets.{source_outcome}.offers[{offer_ordinal}].providerId",
                )
                provider = offer.get("bettingProvider")
                if not isinstance(provider, str) or not provider.strip():
                    raise WhoScoredParseError(
                        f"game {game_id} bets.{source_outcome}.offers"
                        f"[{offer_ordinal}].bettingProvider is required"
                    )
                entity_key = f"{game_id}:{source_outcome}:{source_bet_id}:{provider_id}"
                if entity_key in seen:
                    raise WhoScoredParseError(
                        f"game {game_id} contains duplicate bet offer {entity_key}"
                    )
                seen.add(entity_key)
                clickout_url = offer.get("clickOutUrl")
                if clickout_url is not None and not isinstance(clickout_url, str):
                    raise WhoScoredParseError(
                        f"game {game_id} bets.{source_outcome}.offers"
                        f"[{offer_ordinal}].clickOutUrl must be text"
                    )
                source_path = f"bets.{source_outcome}.offers[{offer_ordinal}]"
                source_record = {
                    "source_outcome": source_outcome,
                    "bet_id": source_bet_id,
                    "bet_name": bet_name,
                    "offer": dict(offer),
                }
                rows.append(
                    {
                        "league": schedule_row.get("league"),
                        "season": schedule_row.get("season"),
                        "game_id": game_id,
                        "game": schedule_row.get("game"),
                        "stage_id": schedule_row.get("stage_id"),
                        "stage": schedule_row.get("stage"),
                        "entity_key": entity_key,
                        "source_outcome": source_outcome,
                        "source_bet_id": source_bet_id,
                        "bet_name": bet_name,
                        "source_offer_ordinal": offer_ordinal,
                        "provider_id": provider_id,
                        "betting_provider": provider,
                        "odds_decimal": _optional_float(
                            offer.get("oddsDecimal"),
                            f"bets.{source_outcome}.offers[{offer_ordinal}]"
                            ".oddsDecimal",
                        ),
                        "odds_fractional": (
                            str(offer["oddsFractional"])
                            if offer.get("oddsFractional") is not None
                            else None
                        ),
                        "odds_us": (
                            str(offer["oddsUS"])
                            if offer.get("oddsUS") is not None
                            else None
                        ),
                        "clickout_url": clickout_url,
                        "source_path": source_path,
                        **_source_metadata(source_record),
                    }
                )
    return _dataset(
        "match_bets",
        rows,
        empty_reason="source_schedule_contains_no_bet_offers",
    )


# ---------------------------------------------------------------------------
# Stage statistics feeds
# ---------------------------------------------------------------------------

_STAGE_STAT_LIST_KEYS = {
    "team": {"teamtablestats", "teams", "teamstats", "data"},
    "player": {"playertablestats", "players", "playerstats", "data"},
    "referee": {"refereetablestats", "referees", "refereestats", "data"},
}


def _find_named_record_lists(
    value: Any, names: set[str]
) -> list[list[Mapping[str, Any]]]:
    matches: list[list[Mapping[str, Any]]] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            if (
                str(key).replace("_", "").casefold() in names
                and isinstance(child, list)
                and all(isinstance(item, Mapping) for item in child)
            ):
                matches.append(child)
            elif isinstance(child, Mapping):
                matches.extend(_find_named_record_lists(child, names))
    return matches


_PAGING_CONTAINER_KEYS = {"paging", "pagination"}
_PAGING_TOTAL_KEYS = {"total", "totalresults", "totalrecords"}
_PAGING_POSITION_KEYS = {
    "page",
    "currentpage",
    "pagenumber",
    "pageindex",
    "pagesize",
    "resultsperpage",
    "recordsperpage",
    "firstrecordindex",
    "lastrecordindex",
    "startindex",
    "endindex",
}


def _stage_stats_paging_contract(
    value: Any,
) -> tuple[Optional[int], Optional[int], bool]:
    """Return the declared total rows/pages and reject ambiguous metadata."""

    total_results: set[int] = set()
    total_pages: set[int] = set()
    paging_values: dict[str, set[int]] = {}

    def visit(node: Any) -> None:
        if not isinstance(node, Mapping):
            return
        for raw_key, child in node.items():
            key = str(raw_key).replace("_", "").casefold()
            if key in _PAGING_CONTAINER_KEYS:
                if not isinstance(child, Mapping):
                    raise WhoScoredParseError(
                        f"statistics {raw_key} metadata must be an object"
                    )
                for paging_key, paging_value in child.items():
                    normalized = str(paging_key).replace("_", "").casefold()
                    if normalized in _PAGING_TOTAL_KEYS:
                        parsed = _required_int(paging_value, f"{raw_key}.{paging_key}")
                        total_results.add(parsed)
                        paging_values.setdefault(normalized, set()).add(parsed)
                    elif normalized == "totalpages":
                        parsed = _required_int(paging_value, f"{raw_key}.{paging_key}")
                        total_pages.add(parsed)
                        paging_values.setdefault(normalized, set()).add(parsed)
                    elif normalized in _PAGING_POSITION_KEYS:
                        parsed = _required_int(paging_value, f"{raw_key}.{paging_key}")
                        paging_values.setdefault(normalized, set()).add(parsed)
                continue
            visit(child)

    visit(value)
    if len(total_results) > 1:
        raise WhoScoredParseError(
            f"statistics paging declares conflicting totals {sorted(total_results)}"
        )
    if len(total_pages) > 1:
        raise WhoScoredParseError(
            f"statistics paging declares conflicting page counts {sorted(total_pages)}"
        )
    conflicting_positions = {
        key: sorted(values) for key, values in paging_values.items() if len(values) > 1
    }
    if conflicting_positions:
        raise WhoScoredParseError(
            f"statistics paging declares conflicting positions {conflicting_positions}"
        )
    # Team feeds are intentionally unpaginated despite zero totals. Most tabs
    # report currentPage=0; the official xG tab reports currentPage=1 while all
    # actual size/index fields remain zero. Keep this source exception exact.
    unpaginated_team_sentinel = (
        total_results == {0}
        and total_pages == {0}
        and bool(paging_values)
        and all(
            next(iter(values)) in ({0, 1} if key == "currentpage" else {0})
            for key, values in paging_values.items()
        )
    )
    return (
        next(iter(total_results), None),
        next(iter(total_pages), None),
        unpaginated_team_sentinel,
    )


def _stage_stat_record_identity(record: Mapping[str, Any]) -> str:
    """Stable identity for one exact source record.

    A stage feed is not guaranteed to contain one row per entity.  The live
    World Cup 2026 team feed, for example, returns the same ``teamId`` more
    than once with different ``apps``/ranking/statistics.  Those are distinct
    source observations and must be preserved.  Only byte-semantically equal
    records indicate a duplicated list/page and can mask truncation.
    """

    return canonical_json(record)


def parse_stage_statistics(
    payload: str | bytes | Mapping[str, Any] | Sequence[Any],
    *,
    scope: WhoScoredScope,
    stage_id: int,
    entity: str,
    source_season_id: Optional[int] = None,
    source_category: Optional[str] = None,
    source_subcategory: Optional[str] = None,
) -> ParsedDataset:
    """Parse team/player/referee feeds into a source-preserving long form."""

    kind = str(entity).strip().casefold()
    if kind not in _STAGE_STAT_LIST_KEYS:
        raise ValueError("entity must be one of: team, player, referee")
    decoded = _decode_discovery_document(payload, variable_names=())
    if isinstance(decoded, list):
        if not all(isinstance(item, Mapping) for item in decoded):
            raise WhoScoredParseError(f"{kind} statistics array contains non-objects")
        records: list[Mapping[str, Any]] = list(decoded)
    elif isinstance(decoded, Mapping):
        lists = _find_named_record_lists(decoded, _STAGE_STAT_LIST_KEYS[kind])
        records = [record for records_list in lists for record in records_list]
        if not records and any(
            key in decoded for key in ("playerId", "teamId", "refereeId", "officialId")
        ):
            records = [decoded]
        elif not lists:
            return ParsedDataset(
                f"{kind}_stage_stats",
                DatasetStatus.NOT_AVAILABLE,
                reason=f"source_{kind}_statistics_container_absent",
            )
    else:
        raise WhoScoredParseError(f"{kind} statistics root must be an object or array")
    if isinstance(decoded, Mapping):
        declared_total, declared_pages, unpaginated_team_paging = (
            _stage_stats_paging_contract(decoded)
        )
        distinct_records = {_stage_stat_record_identity(record) for record in records}
        if len(distinct_records) != len(records):
            raise WhoScoredParseError(
                f"{kind} statistics contains duplicate source records: "
                f"{len(distinct_records)} distinct of {len(records)} returned"
            )
        # The official team endpoint returns all rows in one response while
        # emitting zero totals/size/index (currentPage is 0 or 1). It is a
        # sentinel, not a claim that the response contains zero teams. Keep
        # this exception narrow: player/referee and real paging remain strict.
        team_unpaginated_sentinel = (
            kind == "team" and bool(records) and unpaginated_team_paging
        )
        if (
            not team_unpaginated_sentinel
            and declared_pages is not None
            and declared_pages > 1
        ):
            raise WhoScoredParseError(
                f"{kind} statistics response is paginated across "
                f"{declared_pages} pages; refusing a partial first page"
            )
        if (
            not team_unpaginated_sentinel
            and declared_total is not None
            and len(distinct_records) != declared_total
        ):
            raise WhoScoredParseError(
                f"{kind} statistics response is incomplete: returned "
                f"{len(distinct_records)} distinct records, source declares "
                f"{declared_total}"
            )
    dataset_name = f"{kind}_stage_stats"
    rows: list[dict[str, Any]] = []
    identity_keys = {
        "playerId",
        "playerName",
        "teamId",
        "teamName",
        "refereeId",
        "refereeName",
        "officialId",
        "name",
        "rank",
        "positionText",
    }
    document_fingerprint = schema_fingerprint(decoded)
    for index, record in enumerate(records):
        metrics = {
            key: value for key, value in record.items() if key not in identity_keys
        }
        leaves = (
            _stat_leaf_rows(metrics)
            if metrics
            else [
                {
                    "category": None,
                    "subcategory": None,
                    "stat": None,
                    "filter": None,
                    "minute": None,
                    "numeric_value": None,
                    "text_value": None,
                    "boolean_value": None,
                    "value_json": None,
                    "source_path": None,
                }
            ]
        )
        for leaf in leaves:
            rows.append(
                {
                    "league": scope.competition_id,
                    "season": scope.season_id,
                    "source_season_id": source_season_id,
                    "stage_id": _required_int(stage_id, "stage_id"),
                    "row_index": index,
                    "entity_type": kind,
                    "source_category": source_category,
                    "source_subcategory": source_subcategory,
                    "team_id": _optional_int(record.get("teamId"), "teamId"),
                    "team": record.get("teamName")
                    if isinstance(record.get("teamName"), str)
                    else None,
                    "player_id": _optional_int(record.get("playerId"), "playerId"),
                    "player": record.get("playerName")
                    if isinstance(record.get("playerName"), str)
                    else None,
                    "referee_id": _optional_int(
                        record.get("refereeId", record.get("officialId")),
                        "refereeId",
                    ),
                    "referee": record.get("refereeName", record.get("name"))
                    if isinstance(record.get("refereeName", record.get("name")), str)
                    else None,
                    "rank": _optional_int(record.get("rank"), "rank"),
                    **leaf,
                    "record_schema_fingerprint": schema_fingerprint(record),
                    "document_schema_fingerprint": document_fingerprint,
                }
            )
    return _dataset(dataset_name, rows, empty_reason=f"source_{kind}_statistics_empty")


def parse_team_stage_statistics(
    payload: str | bytes | Mapping[str, Any] | Sequence[Any],
    *,
    scope: WhoScoredScope,
    stage_id: int,
    source_season_id: Optional[int] = None,
    source_category: Optional[str] = None,
    source_subcategory: Optional[str] = None,
) -> ParsedDataset:
    return parse_stage_statistics(
        payload,
        scope=scope,
        stage_id=stage_id,
        entity="team",
        source_season_id=source_season_id,
        source_category=source_category,
        source_subcategory=source_subcategory,
    )


def parse_player_stage_statistics(
    payload: str | bytes | Mapping[str, Any] | Sequence[Any],
    *,
    scope: WhoScoredScope,
    stage_id: int,
    source_season_id: Optional[int] = None,
    source_category: Optional[str] = None,
    source_subcategory: Optional[str] = None,
) -> ParsedDataset:
    return parse_stage_statistics(
        payload,
        scope=scope,
        stage_id=stage_id,
        entity="player",
        source_season_id=source_season_id,
        source_category=source_category,
        source_subcategory=source_subcategory,
    )


def parse_referee_stage_statistics(
    payload: str | bytes | Mapping[str, Any] | Sequence[Any],
    *,
    scope: WhoScoredScope,
    stage_id: int,
    source_season_id: Optional[int] = None,
    source_category: Optional[str] = None,
    source_subcategory: Optional[str] = None,
) -> ParsedDataset:
    return parse_stage_statistics(
        payload,
        scope=scope,
        stage_id=stage_id,
        entity="referee",
        source_season_id=source_season_id,
        source_category=source_category,
        source_subcategory=source_subcategory,
    )


def parse_referee_stage_statistics_html(
    html: str,
    *,
    scope: WhoScoredScope,
    stage_id: int,
    source_season_id: Optional[int] = None,
) -> ParsedDataset:
    """Parse the source-rendered referee tables when no JSON feed is exposed."""
    soup = BeautifulSoup(html, "html.parser")
    records: list[dict[str, Any]] = []
    for table in soup.select("table"):
        marker = " ".join(
            str(value)
            for value in (
                table.get("id"),
                " ".join(table.get("class") or ()),
                getattr(table.parent, "get", lambda *_: None)("id"),
            )
            if value
        ).casefold()
        headers = table.select("thead th")
        header_names: list[str] = []
        for index, header in enumerate(headers):
            raw_name = (
                header.get("data-property")
                or header.get("data-stat-name")
                or header.get_text(" ", strip=True)
                or f"column_{index}"
            )
            header_names.append(_snake(str(raw_name)))
        for row_index, table_row in enumerate(table.select("tbody tr")):
            cells = table_row.find_all("td", recursive=False)
            if not cells:
                continue
            anchor = table_row.find("a", href=True)
            href = anchor.get("href", "") if anchor is not None else ""
            referee_match = re.search(r"/(?:Referees|Officials)/(\d+)", href, re.I)
            if "referee" not in marker and referee_match is None:
                continue
            record: dict[str, Any] = {
                "refereeId": int(referee_match.group(1)) if referee_match else None,
                "refereeName": (
                    anchor.get_text(" ", strip=True) if anchor is not None else None
                ),
            }
            for index, cell in enumerate(cells):
                key = (
                    header_names[index]
                    if index < len(header_names)
                    else next(
                        (
                            _snake(value)
                            for value in cell.get("class") or ()
                            if value not in {"sorted", "grid-abs"}
                        ),
                        f"column_{index}",
                    )
                )
                text = cell.get_text(" ", strip=True)
                if not text or key in {"referee", "referee_name", "name"}:
                    continue
                numeric = text.replace(",", "")
                try:
                    value: Any = float(numeric)
                    if value.is_integer():
                        value = int(value)
                except ValueError:
                    value = text
                record[key] = value
            record["sourceRowIndex"] = row_index
            records.append(record)
    soup.decompose()
    return parse_referee_stage_statistics(
        records,
        scope=scope,
        stage_id=stage_id,
        source_season_id=source_season_id,
        source_category="referee",
        source_subcategory="all",
    )
