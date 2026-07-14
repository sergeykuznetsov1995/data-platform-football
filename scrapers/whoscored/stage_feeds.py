"""Fail-closed contracts for WhoScored's positional stage-team feeds.

The ``/stagestatfeed/{stageId}/stageteams/`` family is not an ordinary JSON
object API.  It returns one positional array whose first item is a list of
team tuples.  Three default responses are also primed into the TeamStatistics
page as ``touchChannels``, ``goalTypes`` and ``teamsPlayed``.  This module
normalizes both representations without evaluating source JavaScript.

Only dimensions demonstrated by the current official browser bundle are
named. Opaque tuple positions remain positional in ``source_path``. The exact
team tuple is retained once per team/feed in ``source_raw_json``; repeating it
on every flattened scalar row would multiply one source response hundreds of
times in memory and Iceberg without adding evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Optional, Sequence

from .domain import WhoScoredScope
from .parsers import (
    DatasetStatus,
    JavaScriptLiteralError,
    ParsedDataset,
    WhoScoredParseError,
    canonical_json,
    extract_js_assignment,
    parse_js_literal,
    schema_fingerprint,
)


STAGE_TEAM_FEED_CATALOG_VERSION = "2026-07-11.1"
STAGE_TEAM_FEED_ENDPOINT_TEMPLATE = (
    "https://www.whoscored.com/stagestatfeed/{stage_id}/stageteams/"
)


class StageTeamFeedShape(str, Enum):
    """Closed set of source tuple grammars observed in the official bundle."""

    VECTOR = "vector"
    GOAL_TYPES = "goal_types"
    PASS_TYPES = "pass_types"
    CARDS = "cards"
    TEAMS_PLAYED = "teams_played"


@dataclass(frozen=True, order=True, slots=True)
class StageTeamFeedSpec:
    """One immutable source request and positional response contract."""

    type_id: int
    source_subcategory: str
    shape: StageTeamFeedShape
    field: int
    against: int
    inline_variable: Optional[str] = None
    dimension_labels: tuple[str, ...] = ()
    catalog_version: str = STAGE_TEAM_FEED_CATALOG_VERSION

    def __post_init__(self) -> None:
        if self.catalog_version != STAGE_TEAM_FEED_CATALOG_VERSION:
            raise ValueError("stage-team feed entry has a mismatched catalog version")
        if self.type_id <= 0:
            raise ValueError("stage-team feed type_id must be positive")
        if not re.fullmatch(r"type_[0-9]+_[a-z][a-z0-9_]*", self.source_subcategory):
            raise ValueError(
                f"invalid stage-team source_subcategory {self.source_subcategory!r}"
            )
        if self.shape is StageTeamFeedShape.VECTOR and not self.dimension_labels:
            raise ValueError("vector stage-team feeds require proven dimensions")
        if self.shape is not StageTeamFeedShape.VECTOR and self.dimension_labels:
            raise ValueError("only vector stage-team feeds accept fixed dimensions")
        if self.inline_variable is not None and not re.fullmatch(
            r"[A-Za-z_$][A-Za-z0-9_$]*", self.inline_variable
        ):
            raise ValueError(f"unsafe inline variable {self.inline_variable!r}")

    @property
    def request_filters(self) -> Mapping[str, int]:
        """The exact source defaults for this feed, excluding stage identity."""

        return {
            "type": self.type_id,
            "teamId": -1,
            "field": self.field,
            "against": self.against,
        }


STAGE_TEAM_FEED_CATALOG: tuple[StageTeamFeedSpec, ...] = (
    StageTeamFeedSpec(
        2,
        "type_2_touch_channels",
        StageTeamFeedShape.VECTOR,
        2,
        0,
        "touchChannels",
        ("left", "centre", "right"),
    ),
    StageTeamFeedSpec(
        3,
        "type_3_touch_zones",
        StageTeamFeedShape.VECTOR,
        2,
        0,
        dimension_labels=(
            "for_defense",
            "for_midfield",
            "for_attack",
            "against_defense",
            "against_midfield",
            "against_attack",
        ),
    ),
    StageTeamFeedSpec(
        6,
        "type_6_attempt_directions",
        StageTeamFeedShape.VECTOR,
        2,
        0,
        dimension_labels=("left", "centre", "right"),
    ),
    StageTeamFeedSpec(
        7,
        "type_7_attempt_zones",
        StageTeamFeedShape.VECTOR,
        2,
        0,
        # Preserve the exact property tokens used by the official grid model;
        # expanding these abbreviations would be an unsupported inference.
        dimension_labels=("Isb", "Ib", "Ob"),
    ),
    StageTeamFeedSpec(
        8,
        "type_8_goal_types",
        StageTeamFeedShape.GOAL_TYPES,
        2,
        0,
        "goalTypes",
    ),
    StageTeamFeedSpec(
        11,
        "type_11_pass_types",
        StageTeamFeedShape.PASS_TYPES,
        2,
        0,
    ),
    StageTeamFeedSpec(
        18,
        "type_18_cards",
        StageTeamFeedShape.CARDS,
        2,
        0,
    ),
    StageTeamFeedSpec(
        25,
        "type_25_teams_played",
        StageTeamFeedShape.TEAMS_PLAYED,
        -1,
        -1,
        "teamsPlayed",
    ),
)


STAGE_TEAM_FEED_BY_TYPE: Mapping[int, StageTeamFeedSpec] = MappingProxyType(
    {spec.type_id: spec for spec in STAGE_TEAM_FEED_CATALOG}
)


def fingerprint_stage_team_feed_catalog(
    catalog: Iterable[StageTeamFeedSpec] = STAGE_TEAM_FEED_CATALOG,
) -> str:
    """Return a deterministic fingerprint of the complete semantic contract."""

    rows = sorted(
        (
            {
                "catalog_version": spec.catalog_version,
                "type": spec.type_id,
                "source_subcategory": spec.source_subcategory,
                "shape": spec.shape.value,
                "field": spec.field,
                "against": spec.against,
                "teamId": -1,
                "inline_variable": spec.inline_variable,
                "dimension_labels": list(spec.dimension_labels),
            }
            for spec in catalog
        ),
        key=lambda row: int(row["type"]),
    )
    encoded = json.dumps(
        rows,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


STAGE_TEAM_FEED_CATALOG_FINGERPRINT = fingerprint_stage_team_feed_catalog()


def stage_team_feed_url(stage_id: int, feed_type: int) -> str:
    """Build the official URL with the catalog's exact default filters."""

    stage = _positive_int(stage_id, "stage_id")
    spec = _feed_spec(feed_type)
    filters = spec.request_filters
    return (
        STAGE_TEAM_FEED_ENDPOINT_TEMPLATE.format(stage_id=stage)
        + f"?type={filters['type']}&stageId={stage}&teamId={filters['teamId']}"
        + f"&field={filters['field']}&against={filters['against']}"
    )


def _feed_spec(feed_type: int) -> StageTeamFeedSpec:
    if isinstance(feed_type, bool) or not isinstance(feed_type, int):
        raise WhoScoredParseError("stagestatfeed type must be an integer")
    try:
        return STAGE_TEAM_FEED_BY_TYPE[feed_type]
    except KeyError as exc:
        raise WhoScoredParseError(
            f"unknown stagestatfeed type {feed_type}; catalog update required"
        ) from exc


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise WhoScoredParseError(f"{field} must be a positive integer")
    return value


def _source_int(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise WhoScoredParseError(
            f"{field} must be an integer greater than or equal to {minimum}"
        )
    return value


def _count(value: Any, field: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise WhoScoredParseError(f"{field} must be a numeric source count")
    if not math.isfinite(float(value)) or value < 0:
        raise WhoScoredParseError(f"{field} must be a finite non-negative count")
    return value


def _nonempty_source_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WhoScoredParseError(f"{field} must be a non-empty source string")
    return value


def _exact_list(value: Any, field: str, length: int) -> list[Any]:
    if not isinstance(value, list) or len(value) != length:
        raise WhoScoredParseError(f"{field} must be an array of length {length}")
    return value


def _extract_inline_value(source: str, variable_name: str) -> Any:
    """Extract one literal object property through the non-evaluating JS parser."""

    pattern = re.compile(
        rf"(?:(?:['\"]{re.escape(variable_name)}['\"])|"
        rf"(?:\b{re.escape(variable_name)}\b))\s*:\s*"
    )
    matches = list(pattern.finditer(source))
    if not matches:
        raise LookupError(variable_name)
    if len(matches) != 1:
        raise WhoScoredParseError(
            f"inline {variable_name} occurs {len(matches)} times; source is ambiguous"
        )
    # Reframe the already-isolated property value as an assignment and let the
    # repository's balanced, non-executing JSON5 parser consume one literal.
    assignment = f"var {variable_name} = " + source[matches[0].end() :]
    try:
        return extract_js_assignment(assignment, variable_name)
    except JavaScriptLiteralError as exc:
        raise WhoScoredParseError(
            f"inline {variable_name} is not a supported literal"
        ) from exc


def _decode_payload(
    payload: str | bytes | Sequence[Any] | None,
    spec: StageTeamFeedSpec,
) -> tuple[Any, bool]:
    """Return decoded source and whether the known inline source was absent."""

    if payload is None:
        return None, False
    if isinstance(payload, Sequence) and not isinstance(
        payload, (str, bytes, bytearray)
    ):
        return payload, False
    if isinstance(payload, bytes):
        try:
            payload = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise WhoScoredParseError("stagestatfeed payload is not UTF-8") from exc
    if not isinstance(payload, str) or not payload.strip():
        raise WhoScoredParseError("stagestatfeed payload is empty")
    source = payload.strip()
    try:
        return json.loads(source), False
    except json.JSONDecodeError:
        pass
    # The live endpoint currently returns a JavaScript array literal with
    # single-quoted team names despite advertising a JSON-like feed. Reuse the
    # project's non-evaluating literal parser; expressions remain forbidden.
    if source.startswith("[") and source.endswith("]"):
        try:
            return parse_js_literal(source), False
        except JavaScriptLiteralError as exc:
            raise WhoScoredParseError(
                f"type {spec.type_id} positional response is not a safe literal"
            ) from exc
    if spec.inline_variable is None:
        raise WhoScoredParseError(
            f"type {spec.type_id} payload is neither a JSON array nor a primed source"
        )
    try:
        return _extract_inline_value(source, spec.inline_variable), False
    except LookupError:
        if not re.search(
            r"require\.config\.params\s*\[\s*['\"]args['\"]\s*\]",
            source,
        ):
            raise WhoScoredParseError(
                f"type {spec.type_id} response has neither JSON nor the "
                "official inline-prime container"
            ) from None
        return None, True


def _filter_json(spec: StageTeamFeedSpec, stage_id: int) -> str:
    return canonical_json(
        {
            "against": spec.against,
            "field": spec.field,
            "stageId": stage_id,
            "teamId": -1,
            "type": spec.type_id,
        }
    )


def _scalar_columns(value: Any, source_path: str) -> dict[str, Any]:
    numeric_value: Optional[float] = None
    text_value: Optional[str] = None
    boolean_value: Optional[bool] = None
    if isinstance(value, bool):
        boolean_value = value
    elif isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            raise WhoScoredParseError(
                f"stagestatfeed scalar at {source_path} is not finite"
            )
        numeric_value = float(value)
    elif isinstance(value, str):
        text_value = value
    elif value is not None:
        raise WhoScoredParseError(
            f"stagestatfeed leaf at {source_path} is not a JSON scalar"
        )
    return {
        "numeric_value": numeric_value,
        "text_value": text_value,
        "boolean_value": boolean_value,
        "value_json": canonical_json(value),
    }


def _base_row(
    *,
    scope: WhoScoredScope,
    source_season_id: Optional[int],
    stage_id: int,
    spec: StageTeamFeedSpec,
    filter_json: str,
    team_index: int,
    team: list[Any],
    document_fingerprint: str,
    source_path: str,
    stat: str,
    value: Any,
    source_raw_json: Optional[str],
    source_record_fingerprint: str,
    subcategory: Optional[str] = None,
) -> dict[str, Any]:
    return {
        "league": scope.competition_id,
        "season": scope.season_id,
        "source_season_id": source_season_id,
        "stage_id": stage_id,
        "row_index": team_index,
        "entity_type": "team",
        "source_category": "stagestatfeed",
        "source_subcategory": spec.source_subcategory,
        "team_id": team[0],
        "team": team[1],
        "player_id": None,
        "player": None,
        "referee_id": None,
        "referee": None,
        "rank": None,
        "category": spec.source_subcategory,
        "subcategory": subcategory,
        "stat": stat,
        "filter": filter_json,
        "minute": None,
        **_scalar_columns(value, source_path),
        "source_path": source_path,
        "source_raw_json": source_raw_json,
        "source_schema_fingerprint": source_record_fingerprint,
        "record_schema_fingerprint": source_record_fingerprint,
        "document_schema_fingerprint": document_fingerprint,
    }


def _vector_rows(
    *,
    team: list[Any],
    team_index: int,
    spec: StageTeamFeedSpec,
    add: Any,
) -> None:
    payload = _exact_list(team[3], f"teams[{team_index}][3]", 1)
    vector = _exact_list(
        payload[0],
        f"teams[{team_index}][3][0]",
        len(spec.dimension_labels),
    )
    for value_index, (label, value) in enumerate(zip(spec.dimension_labels, vector)):
        _count(value, f"teams[{team_index}][3][0][{value_index}]")
        add(f"$[0][{team_index}][3][0][{value_index}]", label, value)


def _goal_type_rows(
    *,
    team: list[Any],
    team_index: int,
    add: Any,
) -> None:
    payload = _exact_list(team[3], f"teams[{team_index}][3]", 1)
    wrapper = _exact_list(payload[0], f"teams[{team_index}][3][0]", 1)
    events = wrapper[0]
    if not isinstance(events, list):
        raise WhoScoredParseError(
            f"teams[{team_index}][3][0][0] must be a goal-type array"
        )
    seen: set[tuple[str, str, str]] = set()
    for event_index, raw_event in enumerate(events):
        event = _exact_list(
            raw_event,
            f"teams[{team_index}][3][0][0][{event_index}]",
            4,
        )
        outcome = _nonempty_source_text(event[0], "goal type outcome")
        situation = _nonempty_source_text(event[1], "goal type situation")
        body_part = _nonempty_source_text(event[2], "goal type body part")
        dimensions = (outcome, situation, body_part)
        if dimensions in seen:
            raise WhoScoredParseError(
                f"team {team[0]} contains duplicate goal-type dimensions {dimensions!r}"
            )
        seen.add(dimensions)
        counts = _exact_list(event[3], "goal type count vector", 1)
        _count(counts[0], "goal type count")
        base = f"$[0][{team_index}][3][0][0][{event_index}]"
        add(f"{base}[0]", "outcome", outcome)
        add(f"{base}[1]", "situation", situation)
        add(f"{base}[2]", "body_part", body_part)
        add(
            f"{base}[3][0]",
            "count",
            counts[0],
            canonical_json(
                {
                    "body_part": body_part,
                    "outcome": outcome,
                    "situation": situation,
                }
            ),
        )


def _pass_type_rows(
    *,
    team: list[Any],
    team_index: int,
    add: Any,
) -> None:
    payload = _exact_list(team[3], f"teams[{team_index}][3]", 1)
    pair = _exact_list(payload[0], f"teams[{team_index}][3][0]", 2)
    # Index zero is source data but its meaning was not established by the
    # inspected bundle.  Preserve every scalar position instead of naming it.
    if not isinstance(pair[0], (str, int, float, bool)) and pair[0] is not None:
        raise WhoScoredParseError("pass-type opaque position must be a scalar")
    add(f"$[0][{team_index}][3][0][0]", "position_0", pair[0])
    events = pair[1]
    if not isinstance(events, list):
        raise WhoScoredParseError("pass-type event collection must be an array")
    seen: set[tuple[str, str]] = set()
    for event_index, raw_event in enumerate(events):
        event = _exact_list(raw_event, "pass-type event", 3)
        position_0 = _nonempty_source_text(event[0], "pass-type event position 0")
        pass_type = _nonempty_source_text(event[1], "pass type")
        semantic_key = (position_0, pass_type)
        if semantic_key in seen:
            raise WhoScoredParseError(
                f"team {team[0]} contains duplicate pass-type tuple {semantic_key!r}"
            )
        seen.add(semantic_key)
        counts = _exact_list(event[2], "pass-type count vector", 1)
        _count(counts[0], "pass-type count")
        base = f"$[0][{team_index}][3][0][1][{event_index}]"
        add(f"{base}[0]", "position_0", position_0)
        add(f"{base}[1]", "pass_type", pass_type)
        add(f"{base}[2][0]", "count", counts[0], pass_type)


def _card_rows(
    *,
    team: list[Any],
    team_index: int,
    add: Any,
) -> None:
    payload = _exact_list(team[3], f"teams[{team_index}][3]", 1)
    wrapper = _exact_list(payload[0], f"teams[{team_index}][3][0]", 1)
    events = wrapper[0]
    if not isinstance(events, list):
        raise WhoScoredParseError("card event collection must be an array")
    seen_codes: set[int] = set()
    seen_metrics: set[str] = set()
    for event_index, raw_event in enumerate(events):
        base = f"$[0][{team_index}][3][0][0][{event_index}]"
        if (
            isinstance(raw_event, list)
            and len(raw_event) == 2
            and isinstance(raw_event[0], int)
        ):
            code = _source_int(raw_event[0], "card source code")
            if code in seen_codes:
                raise WhoScoredParseError(
                    f"team {team[0]} contains duplicate card source code {code}"
                )
            seen_codes.add(code)
            _count(raw_event[1], "card source count")
            add(f"{base}[0]", "source_code", code)
            add(f"{base}[1]", "count", raw_event[1], f"source_code_{code}")
            continue

        wrapper = _exact_list(raw_event, "card named-metric wrapper", 1)
        metrics = wrapper[0]
        if not isinstance(metrics, list):
            raise WhoScoredParseError("card named metrics must be an array")
        for metric_index, raw_metric in enumerate(metrics):
            metric = _exact_list(raw_metric, "card named metric", 2)
            name = _nonempty_source_text(metric[0], "card metric name")
            if name in seen_metrics:
                raise WhoScoredParseError(
                    f"team {team[0]} contains duplicate card metric {name!r}"
                )
            seen_metrics.add(name)
            counts = _exact_list(metric[1], "card named metric count vector", 1)
            _count(counts[0], "card named metric count")
            metric_base = f"{base}[0][{metric_index}]"
            add(f"{metric_base}[0]", "metric_name", name)
            add(f"{metric_base}[1][0]", "count", counts[0], name)


def _teams_played_rows(
    *,
    team: list[Any],
    team_index: int,
    add: Any,
) -> None:
    payload = _exact_list(team[3], f"teams[{team_index}][3]", 1)
    encoded = payload[0]
    if not isinstance(encoded, str):
        raise WhoScoredParseError("teams-played payload must be a JSON string")
    add(f"$[0][{team_index}][3][0]", "source_encoded_counts", encoded)
    try:
        counts = json.loads(encoded)
    except json.JSONDecodeError as exc:
        raise WhoScoredParseError(
            "teams-played payload contains invalid JSON; refusing source eval"
        ) from exc
    counts = _exact_list(counts, "decoded teams-played counts", 3)
    for index, (label, value) in enumerate(zip(("home", "away", "overall"), counts)):
        _source_int(value, f"teams-played {label}")
        add(f"$[0][{team_index}][3][0]#json[{index}]", label, value)


def parse_stage_team_feed(
    payload: str | bytes | Sequence[Any] | None,
    *,
    scope: WhoScoredScope,
    stage_id: int,
    feed_type: int,
    source_season_id: Optional[int] = None,
) -> ParsedDataset:
    """Normalize one catalogued positional stage-team response.

    ``null`` means the source explicitly did not provide the feed and maps to
    ``NOT_AVAILABLE``.  ``[]`` and ``[[]]`` are available-but-empty.  Invalid
    roots, tuple arity/type changes and unknown feed types raise parser drift.
    """

    stage = _positive_int(stage_id, "stage_id")
    if source_season_id is not None:
        _positive_int(source_season_id, "source_season_id")
    spec = _feed_spec(feed_type)
    decoded, inline_absent = _decode_payload(payload, spec)
    name = "team_stage_stats"
    if inline_absent:
        return ParsedDataset(
            name,
            DatasetStatus.NOT_AVAILABLE,
            reason=f"source_inline_{spec.inline_variable}_absent",
        )
    if decoded is None:
        return ParsedDataset(
            name,
            DatasetStatus.NOT_AVAILABLE,
            reason="source_stagestatfeed_unavailable",
        )
    if not isinstance(decoded, list):
        raise WhoScoredParseError("stagestatfeed root must be a positional array")
    if not decoded:
        return ParsedDataset(
            name,
            DatasetStatus.EMPTY,
            reason="source_stagestatfeed_empty",
        )
    root = _exact_list(decoded, "stagestatfeed root", 1)
    teams = root[0]
    if not isinstance(teams, list):
        raise WhoScoredParseError("stagestatfeed root[0] must be a team array")
    if not teams:
        return ParsedDataset(
            name,
            DatasetStatus.EMPTY,
            reason="source_stagestatfeed_empty",
        )

    document_fingerprint = schema_fingerprint(decoded)
    filter_json = _filter_json(spec, stage)
    rows: list[dict[str, Any]] = []
    seen_team_ids: set[int] = set()
    seen_entity_paths: set[tuple[int, str]] = set()

    for team_index, raw_team in enumerate(teams):
        team = _exact_list(raw_team, f"teams[{team_index}]", 4)
        team_id = _positive_int(team[0], f"teams[{team_index}][0]")
        _nonempty_source_text(team[1], f"teams[{team_index}][1]")
        _source_int(team[2], f"teams[{team_index}][2]")
        if team_id in seen_team_ids:
            raise WhoScoredParseError(
                f"stagestatfeed contains duplicate team identity {team_id}"
            )
        seen_team_ids.add(team_id)
        source_raw_json = canonical_json(team)
        source_record_fingerprint = schema_fingerprint(team)
        raw_json_pending = True

        def add(
            source_path: str,
            stat: str,
            value: Any,
            subcategory: Optional[str] = None,
        ) -> None:
            nonlocal raw_json_pending
            key = (team_id, source_path)
            if key in seen_entity_paths:
                raise WhoScoredParseError(
                    f"stagestatfeed contains duplicate entity/path key {key!r}"
                )
            seen_entity_paths.add(key)
            rows.append(
                _base_row(
                    scope=scope,
                    source_season_id=source_season_id,
                    stage_id=stage,
                    spec=spec,
                    filter_json=filter_json,
                    team_index=team_index,
                    team=team,
                    document_fingerprint=document_fingerprint,
                    source_path=source_path,
                    stat=stat,
                    value=value,
                    source_raw_json=(source_raw_json if raw_json_pending else None),
                    source_record_fingerprint=source_record_fingerprint,
                    subcategory=subcategory,
                )
            )
            raw_json_pending = False

        if spec.shape is StageTeamFeedShape.VECTOR:
            _vector_rows(team=team, team_index=team_index, spec=spec, add=add)
        elif spec.shape is StageTeamFeedShape.GOAL_TYPES:
            _goal_type_rows(team=team, team_index=team_index, add=add)
        elif spec.shape is StageTeamFeedShape.PASS_TYPES:
            _pass_type_rows(team=team, team_index=team_index, add=add)
        elif spec.shape is StageTeamFeedShape.CARDS:
            _card_rows(team=team, team_index=team_index, add=add)
        elif spec.shape is StageTeamFeedShape.TEAMS_PLAYED:
            _teams_played_rows(team=team, team_index=team_index, add=add)
        else:  # pragma: no cover - Enum plus static catalog makes this impossible.
            raise WhoScoredParseError(
                f"unsupported stagestatfeed shape {spec.shape.value}"
            )

    if not rows:
        return ParsedDataset(
            name,
            DatasetStatus.EMPTY,
            reason="source_stagestatfeed_contains_no_scalar_values",
        )
    return ParsedDataset(name, DatasetStatus.AVAILABLE, tuple(rows))


def _validate_static_catalog() -> None:
    expected_types = {2, 3, 6, 7, 8, 11, 18, 25}
    if len(STAGE_TEAM_FEED_CATALOG) != 8:
        raise RuntimeError("WhoScored stage-team feed catalog must contain 8 entries")
    if set(STAGE_TEAM_FEED_BY_TYPE) != expected_types:
        raise RuntimeError(
            "WhoScored stage-team feed catalog type set changed without a contract bump"
        )
    if len(STAGE_TEAM_FEED_BY_TYPE) != len(STAGE_TEAM_FEED_CATALOG):
        raise RuntimeError("WhoScored stage-team feed catalog has duplicate type IDs")
    if {(spec.field, spec.against) for spec in STAGE_TEAM_FEED_CATALOG[:-1]} != {
        (2, 0)
    }:
        raise RuntimeError("WhoScored stage-team feed default filters drifted")
    type_25 = STAGE_TEAM_FEED_BY_TYPE[25]
    if (type_25.field, type_25.against) != (-1, -1):
        raise RuntimeError(
            "WhoScored teams-played filters must remain field=-1/against=-1"
        )


_validate_static_catalog()


__all__ = [
    "STAGE_TEAM_FEED_BY_TYPE",
    "STAGE_TEAM_FEED_CATALOG",
    "STAGE_TEAM_FEED_CATALOG_FINGERPRINT",
    "STAGE_TEAM_FEED_CATALOG_VERSION",
    "STAGE_TEAM_FEED_ENDPOINT_TEMPLATE",
    "StageTeamFeedShape",
    "StageTeamFeedSpec",
    "fingerprint_stage_team_feed_catalog",
    "parse_stage_team_feed",
    "stage_team_feed_url",
]
