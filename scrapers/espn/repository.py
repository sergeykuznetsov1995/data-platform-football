"""Manifest-gated Iceberg repository for ESPN Native Bronze v2.

Physical entity tables are append-only.  A generation becomes logically
visible only after exact-scope validation, all physical appends, an optional
physical parity query, and finally one COMPLETE manifest append.  The current
views therefore retain the previous complete generation when a later attempt
fails halfway through its writes.

The repository deliberately accepts the frozen parser rows from
``parser_contracts`` and has no transport or wall-clock dependency.  Every
timestamp and identity is supplied by the caller, which keeps replay byte
stable and makes the publication boundary testable without Trino.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import date, datetime, timezone
from enum import Enum
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Optional, Protocol, Sequence

import pandas as pd

from .models import (
    CapabilityState,
    DispositionState,
    RequestDisposition,
    ScopePlan,
)
from .parser_contracts import LineupRow, MatchsheetRow, ScheduleRow


REPOSITORY_VERSION = "espn-bronze-repository-v2"
MANIFEST_VERSION = "espn-ingest-manifest-v2"
CATALOG_TABLE = "espn_catalog_snapshot_v2"
MANIFEST_TABLE = "espn_ingest_manifest_v2"
CUTOVER_TABLE = "espn_scope_cutover_v2"
ENTITY_TABLES = MappingProxyType(
    {
        "schedule": "espn_schedule_generation_v2",
        "lineup": "espn_lineup_generation_v2",
        "matchsheet": "espn_matchsheet_generation_v2",
    }
)
CURRENT_VIEWS = MappingProxyType(
    {entity: f"espn_{entity}_current" for entity in ENTITY_TABLES}
)

_IDENTIFIER_RE = re.compile(r"[a-z_][a-z0-9_]*")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_SCOPE_RE = re.compile(r"[1-9][0-9]*:[1-9][0-9]*")
_ENTITIES = tuple(ENTITY_TABLES)

PROVENANCE_COLUMNS = (
    "scope_id",
    "generation_id",
    "run_id",
    "registry_snapshot_uri",
    "registry_signature",
    "plan_signature",
    "raw_uri",
    "raw_sha256",
    "parser_version",
    "runtime_version",
    "_source_fetched_at",
    "_ingested_at",
    "_batch_id",
    "_source",
    "_entity_type",
    "_row_sha256",
)


class PublicationError(RuntimeError):
    """A scope failed validation or physical publication."""


class ManifestConflictError(PublicationError):
    """The same logical generation identity already has different semantics."""


class ScopePublicationState(str, Enum):
    PUBLISHED = "published"
    IDEMPOTENT = "idempotent"
    FAILED = "failed"


def _identifier(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a safe lower-case SQL identifier")
    return value


def _required_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lower-case SHA-256")
    return value


def _nonnegative_int(value: object, field_name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _positive_native_id(value: object, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{field_name} must be a positive native integer ID")
    return value


def _aware_utc(value: object, field_name: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{field_name} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return _aware_utc(value, "datetime").isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if is_dataclass(value):
        return {
            field.name: _json_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {
            str(key): _json_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported canonical value {type(value).__name__}")


def canonical_json(value: Any) -> str:
    return json.dumps(
        _json_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class RawLedgerRecord:
    request_id: str
    endpoint: str
    event_id: int | None
    disposition: DispositionState
    raw_uri: str | None
    raw_sha256: str | None
    fetched_at: datetime | None
    direct_bytes: int
    proxy_bytes: int
    event_ids: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        _required_string(self.request_id, "request_id")
        if self.endpoint not in {"catalog", "scoreboard", "summary"}:
            raise ValueError("endpoint must be catalog, scoreboard or summary")
        if self.event_id is not None:
            _positive_native_id(self.event_id, "event_id")
        if not isinstance(self.event_ids, (tuple, list)):
            raise TypeError("event_ids must be a sequence")
        event_ids = tuple(
            _positive_native_id(value, "event_ids item") for value in self.event_ids
        )
        if len(set(event_ids)) != len(event_ids):
            raise ValueError("event_ids must be unique")
        object.__setattr__(self, "event_ids", event_ids)
        if self.endpoint == "scoreboard" and self.event_id is not None:
            raise ValueError("scoreboard bindings use event_ids, not event_id")
        if self.endpoint == "summary" and (self.event_id is None or self.event_ids):
            raise ValueError("Summary ledger rows require one event_id")
        if self.endpoint != "scoreboard" and self.event_ids:
            raise ValueError("only scoreboard ledger rows may bind event_ids")
        if not isinstance(self.disposition, DispositionState):
            raise TypeError("disposition must be DispositionState")
        _nonnegative_int(self.direct_bytes, "direct_bytes")
        _nonnegative_int(self.proxy_bytes, "proxy_bytes")
        if self.raw_uri is not None:
            _required_string(self.raw_uri, "raw_uri")
        if self.raw_sha256 is not None:
            _sha256(self.raw_sha256, "raw_sha256")
        if self.fetched_at is not None:
            _aware_utc(self.fetched_at, "fetched_at")
        captured = self.disposition is DispositionState.CAPTURED
        raw_identity = (self.raw_uri, self.raw_sha256, self.fetched_at)
        if captured and not all(item is not None for item in raw_identity):
            raise ValueError(
                "captured raw ledger rows require URI, SHA-256 and fetched_at"
            )
        if not captured and any(item is not None for item in raw_identity):
            raise ValueError("non-captured raw ledger rows must not claim raw identity")

    def constructor_values(self) -> dict[str, Any]:
        return {field.name: getattr(self, field.name) for field in fields(self)}


_ROW_TYPES = {
    "schedule": ScheduleRow,
    "lineup": LineupRow,
    "matchsheet": MatchsheetRow,
}


@dataclass(frozen=True, slots=True)
class ScopeGeneration:
    plan: ScopePlan
    run_id: str
    generation_id: str
    registry_snapshot_uri: str
    registry_signature: str
    plan_signature: str
    parser_version: str
    runtime_version: str
    ingested_at: datetime
    batch_id: str
    schedule: tuple[ScheduleRow, ...]
    lineup: tuple[LineupRow, ...]
    matchsheet: tuple[MatchsheetRow, ...]
    planned_request_ids: tuple[str, ...]
    raw_ledger: tuple[RawLedgerRecord, ...]
    dispositions: tuple[RequestDisposition, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.plan, ScopePlan):
            raise TypeError("plan must be ScopePlan")
        for field_name in (
            "run_id",
            "generation_id",
            "registry_snapshot_uri",
            "parser_version",
            "runtime_version",
            "batch_id",
        ):
            _required_string(getattr(self, field_name), field_name)
        _sha256(self.registry_signature, "registry_signature")
        _sha256(self.plan_signature, "plan_signature")
        _aware_utc(self.ingested_at, "ingested_at")
        for entity, row_type in _ROW_TYPES.items():
            raw_rows = getattr(self, entity)
            if not isinstance(raw_rows, (tuple, list)):
                raise TypeError(f"{entity} must be a sequence")
            normalized: list[Any] = []
            for row in raw_rows:
                if isinstance(row, Mapping):
                    row = row_type(**dict(row))
                if not isinstance(row, row_type):
                    raise TypeError(f"{entity} rows must be {row_type.__name__}")
                _validate_row_native_types(entity, row)
                if row.parser_version != self.parser_version:
                    raise ValueError(
                        f"{entity}.parser_version conflicts with generation"
                    )
                normalized.append(row)
            object.__setattr__(self, entity, tuple(normalized))
        if not isinstance(self.planned_request_ids, (tuple, list)):
            raise TypeError("planned_request_ids must be a sequence")
        request_ids = tuple(
            _required_string(item, "planned request ID")
            for item in self.planned_request_ids
        )
        object.__setattr__(self, "planned_request_ids", request_ids)
        if not isinstance(self.raw_ledger, (tuple, list)) or not all(
            isinstance(item, RawLedgerRecord) for item in self.raw_ledger
        ):
            raise TypeError("raw_ledger must contain RawLedgerRecord values")
        object.__setattr__(self, "raw_ledger", tuple(self.raw_ledger))
        if not isinstance(self.dispositions, (tuple, list)) or not all(
            isinstance(item, RequestDisposition) for item in self.dispositions
        ):
            raise TypeError("dispositions must contain RequestDisposition values")
        object.__setattr__(self, "dispositions", tuple(self.dispositions))

    def constructor_values(self) -> dict[str, Any]:
        return {field.name: getattr(self, field.name) for field in fields(self)}

    @property
    def manifest_sha256(self) -> str:
        return self.manifest_row()["manifest_sha256"]

    def manifest_row(
        self, report: "ScopeQualityReport | None" = None
    ) -> dict[str, Any]:
        report = report or validate_scope_generation(self)
        base = {
            "manifest_version": MANIFEST_VERSION,
            "repository_version": REPOSITORY_VERSION,
            "scope_id": self.plan.scope_id,
            "competition_id": self.plan.espn_id,
            "source_season_year": self.plan.source_season_year,
            "run_id": self.run_id,
            "generation_id": self.generation_id,
            "_batch_id": self.batch_id,
            "registry_snapshot_uri": self.registry_snapshot_uri,
            "registry_signature": self.registry_signature,
            "plan_signature": self.plan_signature,
            "parser_version": self.parser_version,
            "runtime_version": self.runtime_version,
            "status": "complete",
            "row_counts_json": canonical_json(dict(report.row_counts)),
            "row_hashes_json": canonical_json(dict(report.row_hashes)),
            "raw_ledger_sha256": canonical_sha256(self.raw_ledger),
            "dispositions_json": canonical_json(self.dispositions),
            "quality_json": canonical_json(
                {"passed": report.passed, "failures": report.failures}
            ),
            "completed_at": self.ingested_at,
        }
        return {**base, "manifest_sha256": canonical_sha256(base)}


def _validate_row_native_types(entity: str, row: Any) -> None:
    for field_name in ("competition_id", "event_id", "source_season_year"):
        _positive_native_id(getattr(row, field_name), f"{entity}.{field_name}")
    if getattr(row, "scope_id") != f"{row.competition_id}:{row.source_season_year}":
        raise ValueError(f"{entity}.scope_id conflicts with native identity")
    if entity == "schedule":
        for field_name in ("home_team_id", "away_team_id", "game_id"):
            _positive_native_id(getattr(row, field_name), f"schedule.{field_name}")
        for field_name in ("venue_id",):
            value = getattr(row, field_name)
            if value is not None:
                _positive_native_id(value, f"schedule.{field_name}")
        for field_name in ("kickoff", "date", "match_date"):
            _aware_utc(getattr(row, field_name), f"schedule.{field_name}")
        for field_name in (
            "terminal",
            "played_final",
            "terminal_nonplayed",
            "summary_required",
        ):
            if type(getattr(row, field_name)) is not bool:
                raise TypeError(f"schedule.{field_name} must be boolean")
        for field_name in ("home_score", "away_score", "attendance_value"):
            value = getattr(row, field_name)
            if value is not None:
                _nonnegative_int(value, f"schedule.{field_name}")
    else:
        _positive_native_id(row.team_id, f"{entity}.team_id")
        if type(row.is_home) is not bool:
            raise TypeError(f"{entity}.is_home must be boolean")
        if entity == "lineup":
            _positive_native_id(row.athlete_id, "lineup.athlete_id")
            for field_name in ("starter", "captain", "subbed_in", "subbed_out"):
                value = getattr(row, field_name)
                if value is not None and type(value) is not bool:
                    raise TypeError(f"lineup.{field_name} must be boolean or null")
            for field_name in (
                "appearances",
                "fouls_committed",
                "fouls_suffered",
                "goal_assists",
                "goals_conceded",
                "offsides",
                "own_goals",
                "red_cards",
                "saves",
                "shots_faced",
                "shots_on_target",
                "sub_ins",
                "total_goals",
                "total_shots",
                "yellow_cards",
            ):
                value = getattr(row, field_name)
                if value is not None and (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                ):
                    raise TypeError(
                        f"lineup.{field_name} must be finite numeric or null"
                    )
        if entity == "matchsheet":
            for field_name in ("venue_id", "referee_id"):
                value = getattr(row, field_name)
                if value is not None:
                    _positive_native_id(value, f"matchsheet.{field_name}")
            for field_name in ("score", "attendance"):
                value = getattr(row, field_name)
                if value is not None:
                    _nonnegative_int(value, f"matchsheet.{field_name}")


@dataclass(frozen=True, slots=True)
class ScopeQualityReport:
    scope_id: str
    passed: bool
    failures: tuple[str, ...]
    row_counts: Mapping[str, int]
    row_hashes: Mapping[str, str]

    def __post_init__(self) -> None:
        if _SCOPE_RE.fullmatch(self.scope_id) is None:
            raise ValueError("invalid scope_id")
        if type(self.passed) is not bool:
            raise TypeError("passed must be boolean")
        object.__setattr__(self, "failures", tuple(self.failures))
        object.__setattr__(self, "row_counts", MappingProxyType(dict(self.row_counts)))
        object.__setattr__(self, "row_hashes", MappingProxyType(dict(self.row_hashes)))


def _raw_binding(
    generation: ScopeGeneration, entity: str, event_id: int
) -> RawLedgerRecord:
    if entity == "schedule":
        matches = [
            item
            for item in generation.raw_ledger
            if item.endpoint == "scoreboard"
            and item.disposition is DispositionState.CAPTURED
            and event_id in item.event_ids
        ]
    else:
        matches = [
            item
            for item in generation.raw_ledger
            if item.endpoint == "summary"
            and item.disposition is DispositionState.CAPTURED
            and item.event_id == event_id
        ]
    if len(matches) != 1:
        raise PublicationError(
            f"{entity} raw binding must be exact for event {event_id}"
        )
    return matches[0]


def row_fingerprint(generation: ScopeGeneration, entity: str, row: Any) -> str:
    """Hash the complete immutable physical-row identity, including raw origin."""

    if entity not in _ROW_TYPES or not isinstance(row, _ROW_TYPES[entity]):
        raise TypeError("row does not match the requested ESPN entity")
    raw = _raw_binding(generation, entity, row.event_id)
    return canonical_sha256(
        {
            "row": row,
            "scope_id": generation.plan.scope_id,
            "generation_id": generation.generation_id,
            "run_id": generation.run_id,
            "registry_snapshot_uri": generation.registry_snapshot_uri,
            "registry_signature": generation.registry_signature,
            "plan_signature": generation.plan_signature,
            "raw_uri": raw.raw_uri,
            "raw_sha256": raw.raw_sha256,
            "parser_version": generation.parser_version,
            "runtime_version": generation.runtime_version,
            "source_fetched_at": raw.fetched_at,
            "ingested_at": generation.ingested_at,
            "batch_id": generation.batch_id,
        }
    )


def validate_scope_generation(generation: ScopeGeneration) -> ScopeQualityReport:
    """Run all source-semantic DQ before any physical write."""

    if not isinstance(generation, ScopeGeneration):
        raise TypeError("generation must be ScopeGeneration")
    failures: list[str] = []
    scope = generation.plan
    row_counts = {entity: len(getattr(generation, entity)) for entity in _ENTITIES}

    planned = generation.planned_request_ids
    observed = tuple(item.request_id for item in generation.raw_ledger)
    if len(set(planned)) != len(planned) or len(set(observed)) != len(observed):
        failures.append("planned/raw ledger parity: duplicate request identity")
    if set(planned) != set(observed):
        failures.append("planned/raw ledger parity: request sets differ")
    if any(item.proxy_bytes != 0 for item in generation.raw_ledger):
        failures.append("proxy bytes must be exactly zero")

    schedule_by_event: dict[int, ScheduleRow] = {}
    for row in generation.schedule:
        if (
            row.scope_id != scope.scope_id
            or row.competition_id != scope.espn_id
            or row.source_season_year != scope.source_season_year
        ):
            failures.append("schedule row belongs to a different exact scope")
        if row.competition_slug != scope.slug:
            failures.append("schedule competition slug conflicts with exact scope")
        if row.game_id != row.event_id:
            failures.append("schedule game_id must equal native event_id")
        if (
            not scope.start_date
            <= row.kickoff.astimezone(timezone.utc).date()
            <= scope.end_date
        ):
            failures.append("edition window excludes schedule event")
        if row.event_id in schedule_by_event:
            failures.append("schedule event uniqueness violated")
        schedule_by_event[row.event_id] = row
        if row.home_team_id == row.away_team_id:
            failures.append("schedule event must have two distinct sides")
        if row.played_final and (row.home_score is None or row.away_score is None):
            failures.append("played-final schedule event requires both scores")
    if not generation.schedule:
        failures.append("schedule is required and must not be empty")
    for event_id in schedule_by_event:
        raw_bindings = [
            item
            for item in generation.raw_ledger
            if item.endpoint == "scoreboard"
            and item.disposition is DispositionState.CAPTURED
            and event_id in item.event_ids
        ]
        if len(raw_bindings) != 1:
            failures.append(f"schedule raw binding must be exact for event {event_id}")

    lineup_keys: set[tuple[int, int, int]] = set()
    lineup_by_event: dict[int, set[int]] = {}
    for row in generation.lineup:
        if (
            row.scope_id != scope.scope_id
            or row.competition_id != scope.espn_id
            or row.source_season_year != scope.source_season_year
        ):
            failures.append("lineup row belongs to a different exact scope")
        if row.event_id not in schedule_by_event:
            failures.append("entity schedule FK missing for lineup")
        elif row.team_id not in {
            schedule_by_event[row.event_id].home_team_id,
            schedule_by_event[row.event_id].away_team_id,
        }:
            failures.append("lineup team is not an event side")
        else:
            event = schedule_by_event[row.event_id]
            expected_home = row.team_id == event.home_team_id
            if row.is_home != expected_home or row.home_away != (
                "home" if expected_home else "away"
            ):
                failures.append("lineup side identity conflicts with schedule")
        key = (row.event_id, row.team_id, row.athlete_id)
        if key in lineup_keys:
            failures.append("lineup natural key is not unique")
        lineup_keys.add(key)
        lineup_by_event.setdefault(row.event_id, set()).add(row.team_id)

    matchsheet_keys: set[tuple[int, int]] = set()
    matchsheet_by_event: dict[int, set[int]] = {}
    for row in generation.matchsheet:
        if (
            row.scope_id != scope.scope_id
            or row.competition_id != scope.espn_id
            or row.source_season_year != scope.source_season_year
        ):
            failures.append("matchsheet row belongs to a different exact scope")
        if row.event_id not in schedule_by_event:
            failures.append("entity schedule FK missing for matchsheet")
        elif row.team_id not in {
            schedule_by_event[row.event_id].home_team_id,
            schedule_by_event[row.event_id].away_team_id,
        }:
            failures.append("matchsheet team is not an event side")
        else:
            event = schedule_by_event[row.event_id]
            expected_home = row.team_id == event.home_team_id
            if row.is_home != expected_home or row.home_away != (
                "home" if expected_home else "away"
            ):
                failures.append("matchsheet side identity conflicts with schedule")
        key = (row.event_id, row.team_id)
        if key in matchsheet_keys:
            failures.append("matchsheet natural key is not unique")
        matchsheet_keys.add(key)
        matchsheet_by_event.setdefault(row.event_id, set()).add(row.team_id)

    disposition_index: dict[tuple[str, int], RequestDisposition] = {}
    for item in generation.dispositions:
        if item.endpoint not in {"lineup", "matchsheet"} or item.event_id is None:
            failures.append("played-final disposition has invalid entity or event")
            continue
        if item.event_id not in schedule_by_event:
            failures.append("played-final disposition references an unknown event")
        key = (item.endpoint, item.event_id)
        if key in disposition_index:
            failures.append("played-final disposition is duplicated")
        disposition_index[key] = item

    successful_summary = {
        item.event_id
        for item in generation.raw_ledger
        if item.endpoint == "summary"
        and item.disposition is DispositionState.CAPTURED
        and item.event_id is not None
    }
    captured_summary_counts: dict[int, int] = {}
    for item in generation.raw_ledger:
        if item.endpoint == "summary" and item.disposition is DispositionState.CAPTURED:
            event_id = item.event_id
            if event_id is None:  # defensive: constructor already rejects this
                failures.append("captured Summary raw row has no event ID")
                continue
            captured_summary_counts[event_id] = (
                captured_summary_counts.get(event_id, 0) + 1
            )
            if event_id not in schedule_by_event:
                failures.append(
                    "Summary raw ledger references an unknown schedule event"
                )
    if any(count != 1 for count in captured_summary_counts.values()):
        failures.append("Summary raw binding must be exact per event")

    for (entity, event_id), disposition in disposition_index.items():
        if disposition.state is not DispositionState.CAPTURED:
            continue
        event = schedule_by_event.get(event_id)
        if event is None:
            continue
        expected_sides = {event.home_team_id, event.away_team_id}
        sides = (
            lineup_by_event.get(event_id, set())
            if entity == "lineup"
            else matchsheet_by_event.get(event_id, set())
        )
        if sides != expected_sides:
            label = (
                "matchsheet two-side completeness"
                if entity == "matchsheet"
                else "lineup two-side completeness"
            )
            failures.append(f"{label} failed for {event_id}")
    for event_id, event in schedule_by_event.items():
        if not event.played_final:
            continue
        expected_sides = {event.home_team_id, event.away_team_id}
        for entity, capability, sides in (
            ("lineup", scope.capabilities.lineup, lineup_by_event.get(event_id, set())),
            (
                "matchsheet",
                scope.capabilities.matchsheet,
                matchsheet_by_event.get(event_id, set()),
            ),
        ):
            disposition = disposition_index.get((entity, event_id))
            if disposition is None:
                failures.append(
                    f"played-final disposition missing for {entity}/{event_id}"
                )
                continue
            if disposition.state is DispositionState.CAPTURED:
                if sides != expected_sides:
                    label = (
                        "matchsheet two-side completeness"
                        if entity == "matchsheet"
                        else "lineup two-side completeness"
                    )
                    failures.append(f"{label} failed for {event_id}")
            elif disposition.state is DispositionState.VALID_EMPTY:
                if capability not in {CapabilityState.PARTIAL, CapabilityState.ABSENT}:
                    failures.append(f"valid_empty is forbidden for proven {entity}")
                if event_id not in successful_summary:
                    failures.append(
                        f"valid_empty requires successful raw Summary for {entity}/{event_id}"
                    )
                if sides:
                    failures.append(f"valid_empty {entity} contains physical rows")
            else:
                failures.append(
                    f"played-final disposition unresolved for {entity}/{event_id}"
                )

    row_hashes: dict[str, str] = {}
    for entity in _ENTITIES:
        hashes: list[str] = []
        for row in getattr(generation, entity):
            try:
                hashes.append(row_fingerprint(generation, entity, row))
            except PublicationError as exc:
                failures.append(str(exc))
        row_hashes[entity] = hashlib.sha256(
            "".join(sorted(hashes)).encode("ascii")
        ).hexdigest()

    return ScopeQualityReport(
        scope_id=scope.scope_id,
        passed=not failures,
        failures=tuple(dict.fromkeys(failures)),
        row_counts=row_counts,
        row_hashes=row_hashes,
    )


@dataclass(frozen=True, slots=True)
class ScopePublicationResult:
    scope_id: str
    generation_id: str
    state: ScopePublicationState
    manifest_sha256: str | None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class BatchPublicationResult:
    results: tuple[ScopePublicationResult, ...]
    passed: bool


class WriterProtocol(Protocol):
    def write_dataframe(
        self,
        df: pd.DataFrame,
        *,
        database: str,
        table: str,
        partition_spec: Optional[list[tuple[str, str]]] = None,
        mode: str = "append",
        add_metadata: bool = True,
        source: Optional[str] = None,
    ) -> str: ...


class QueryProtocol(Protocol):
    def execute_query(
        self, sql: str, params: Optional[tuple[Any, ...]] = None
    ) -> Sequence[Any]: ...


def _physical_rows(generation: ScopeGeneration, entity: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for typed_row in getattr(generation, entity):
        row = asdict(typed_row)
        raw = _raw_binding(generation, entity, typed_row.event_id)
        row_hash = row_fingerprint(generation, entity, typed_row)
        row.update(
            {
                "scope_id": generation.plan.scope_id,
                "generation_id": generation.generation_id,
                "run_id": generation.run_id,
                "registry_snapshot_uri": generation.registry_snapshot_uri,
                "registry_signature": generation.registry_signature,
                "plan_signature": generation.plan_signature,
                "raw_uri": raw.raw_uri,
                "raw_sha256": raw.raw_sha256,
                "parser_version": generation.parser_version,
                "runtime_version": generation.runtime_version,
                "_source_fetched_at": raw.fetched_at,
                "_ingested_at": generation.ingested_at,
                "_batch_id": generation.batch_id,
                "_source": "espn",
                "_entity_type": entity,
                "_row_sha256": row_hash,
            }
        )
        output.append(row)
    return output


MANIFEST_COLUMNS = (
    "manifest_version",
    "repository_version",
    "scope_id",
    "competition_id",
    "source_season_year",
    "run_id",
    "generation_id",
    "_batch_id",
    "registry_snapshot_uri",
    "registry_signature",
    "plan_signature",
    "parser_version",
    "runtime_version",
    "status",
    "row_counts_json",
    "row_hashes_json",
    "raw_ledger_sha256",
    "dispositions_json",
    "quality_json",
    "completed_at",
    "manifest_sha256",
)


class EspnBronzeRepository:
    """Production adapter over the platform IcebergWriter and Trino manager."""

    manifest_columns = MANIFEST_COLUMNS

    def __init__(
        self,
        *,
        writer: WriterProtocol | None = None,
        query: QueryProtocol | None = None,
        catalog: str = "iceberg",
        schema: str = "bronze",
        verify_physical: bool = True,
    ) -> None:
        self.catalog = _identifier(catalog, "catalog")
        self.schema = _identifier(schema, "schema")
        if writer is None:
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter(catalog=self.catalog)
        self.writer = writer
        if query is None:
            factory = getattr(writer, "_get_trino_manager", None)
            if not callable(factory):
                raise TypeError("query is required when writer has no Trino manager")
            query = factory()
        self.query = query
        if type(verify_physical) is not bool:
            raise TypeError("verify_physical must be boolean")
        self.verify_physical = verify_physical

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> Sequence[Any]:
        execute = getattr(self.query, "execute_query", None)
        if not callable(execute):
            raise TypeError("query adapter must expose execute_query")
        return execute(sql, params=params) or []

    def ensure_objects(self) -> None:
        self._execute(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        for sql in render_repository_ddl(
            catalog=self.catalog, schema=self.schema
        ).values():
            self._execute(sql)
        for entity in _ENTITIES:
            self._execute(
                render_current_view_sql(
                    entity, catalog=self.catalog, schema=self.schema
                )
            )

    def _existing_manifest(
        self, scope_id: str, generation_id: str
    ) -> Mapping[str, Any] | None:
        columns = ", ".join(f'"{column}"' for column in MANIFEST_COLUMNS)
        rows = self._execute(
            f"SELECT {columns} FROM {self.catalog}.{self.schema}.{MANIFEST_TABLE} "
            'WHERE "scope_id" = ? AND "generation_id" = ? '
            'ORDER BY "completed_at" DESC, "manifest_sha256" DESC',
            (scope_id, generation_id),
        )
        if not rows:
            return None
        normalized: list[dict[str, Any]] = []
        for raw in rows:
            if isinstance(raw, Mapping):
                normalized.append(
                    {column: raw.get(column) for column in MANIFEST_COLUMNS}
                )
            else:
                if len(raw) != len(MANIFEST_COLUMNS):
                    raise PublicationError(
                        "stored manifest has an unexpected column count"
                    )
                normalized.append(dict(zip(MANIFEST_COLUMNS, raw)))
        fingerprints = {str(row["manifest_sha256"]) for row in normalized}
        if len(fingerprints) != 1:
            raise ManifestConflictError(
                "conflicting manifests share one generation identity"
            )
        return normalized[0]

    def _write(self, table: str, rows: Sequence[Mapping[str, Any]]) -> str:
        if not rows:
            return f"{self.catalog}.{self.schema}.{table}"
        return self.writer.write_dataframe(
            pd.DataFrame(list(rows)),
            database=self.schema,
            table=table,
            partition_spec=[("scope_id", "identity")],
            mode="append",
            add_metadata=False,
            source="espn",
        )

    def _physical_row_hashes(
        self, generation: ScopeGeneration, entity: str
    ) -> frozenset[str]:
        table = ENTITY_TABLES[entity]
        rows = self._execute(
            f'SELECT DISTINCT "_row_sha256" '
            f"FROM {self.catalog}.{self.schema}.{table} "
            'WHERE "scope_id" = ? AND "generation_id" = ? AND "run_id" = ? '
            'AND "_batch_id" = ? AND "registry_signature" = ? AND "plan_signature" = ?',
            (
                generation.plan.scope_id,
                generation.generation_id,
                generation.run_id,
                generation.batch_id,
                generation.registry_signature,
                generation.plan_signature,
            ),
        )
        hashes: set[str] = set()
        for raw in rows:
            value = raw.get("_row_sha256") if isinstance(raw, Mapping) else raw[0]
            hashes.add(_sha256(value, "stored _row_sha256"))
        return frozenset(hashes)

    def _verify_physical(
        self, generation: ScopeGeneration, report: ScopeQualityReport
    ) -> None:
        selects: list[str] = []
        params: list[Any] = []
        for entity, table in ENTITY_TABLES.items():
            selects.append(
                f"SELECT '{entity}' AS entity, COUNT(DISTINCT \"_row_sha256\") AS row_count, "
                "COALESCE(lower(to_hex(sha256(to_utf8(array_join(array_sort(array_distinct(array_agg(\"_row_sha256\"))), ''))))), "
                "lower(to_hex(sha256(to_utf8(''))))) AS row_hash "
                f"FROM {self.catalog}.{self.schema}.{table} "
                'WHERE "scope_id" = ? AND "generation_id" = ? AND "run_id" = ? '
                'AND "_batch_id" = ? AND "registry_signature" = ? AND "plan_signature" = ?'
            )
            params.extend(
                (
                    generation.plan.scope_id,
                    generation.generation_id,
                    generation.run_id,
                    generation.batch_id,
                    generation.registry_signature,
                    generation.plan_signature,
                )
            )
        rows = self._execute(" UNION ALL ".join(selects), tuple(params))
        observed: dict[str, tuple[int, str]] = {}
        for raw in rows:
            if isinstance(raw, Mapping):
                entity = str(raw.get("entity"))
                count = int(raw.get("row_count") or 0)
                row_hash = str(raw.get("row_hash") or "")
            else:
                if len(raw) != 3:
                    raise PublicationError(
                        "physical parity query returned malformed row"
                    )
                entity, count, row_hash = str(raw[0]), int(raw[1]), str(raw[2] or "")
            if entity in observed:
                raise PublicationError("physical parity query duplicated an entity")
            observed[entity] = (count, row_hash)
        expected = {
            entity: (report.row_counts[entity], report.row_hashes[entity])
            for entity in _ENTITIES
        }
        if observed != expected:
            raise PublicationError(
                f"physical row/hash parity failed: expected={expected!r}, observed={observed!r}"
            )

    def publish_scope(self, generation: ScopeGeneration) -> ScopePublicationResult:
        report = validate_scope_generation(generation)
        if not report.passed:
            raise PublicationError("; ".join(report.failures))
        manifest = generation.manifest_row(report)
        existing = self._existing_manifest(
            generation.plan.scope_id, generation.generation_id
        )
        if existing is not None:
            if existing.get("manifest_sha256") != manifest["manifest_sha256"]:
                raise ManifestConflictError(
                    "same scope/generation has a conflicting manifest"
                )
            if self.verify_physical:
                self._verify_physical(generation, report)
            return ScopePublicationResult(
                scope_id=generation.plan.scope_id,
                generation_id=generation.generation_id,
                state=ScopePublicationState.IDEMPOTENT,
                manifest_sha256=manifest["manifest_sha256"],
            )
        try:
            for entity, table in ENTITY_TABLES.items():
                rows = _physical_rows(generation, entity)
                if self.verify_physical:
                    existing_hashes = self._physical_row_hashes(generation, entity)
                    expected_hashes = frozenset(row["_row_sha256"] for row in rows)
                    unexpected = existing_hashes - expected_hashes
                    if unexpected:
                        raise ManifestConflictError(
                            f"{entity} generation contains conflicting physical rows"
                        )
                    rows = [
                        row for row in rows if row["_row_sha256"] not in existing_hashes
                    ]
                self._write(table, rows)
            if self.verify_physical:
                self._verify_physical(generation, report)
            self._write(MANIFEST_TABLE, [manifest])
        except ManifestConflictError:
            raise
        except Exception as exc:
            raise PublicationError(
                f"scope {generation.plan.scope_id} generation publication failed: {exc}"
            ) from exc
        return ScopePublicationResult(
            scope_id=generation.plan.scope_id,
            generation_id=generation.generation_id,
            state=ScopePublicationState.PUBLISHED,
            manifest_sha256=manifest["manifest_sha256"],
        )

    def publish_many(
        self, generations: Iterable[ScopeGeneration]
    ) -> BatchPublicationResult:
        results: list[ScopePublicationResult] = []
        seen: set[str] = set()
        for generation in generations:
            if not isinstance(generation, ScopeGeneration):
                raise TypeError("generations must contain ScopeGeneration values")
            if generation.plan.scope_id in seen:
                raise ValueError("a publication batch may contain each scope only once")
            seen.add(generation.plan.scope_id)
            try:
                results.append(self.publish_scope(generation))
            except Exception as exc:
                results.append(
                    ScopePublicationResult(
                        scope_id=generation.plan.scope_id,
                        generation_id=generation.generation_id,
                        state=ScopePublicationState.FAILED,
                        manifest_sha256=None,
                        error=str(exc),
                    )
                )
        return BatchPublicationResult(
            results=tuple(results),
            passed=bool(results)
            and all(
                result.state
                in {ScopePublicationState.PUBLISHED, ScopePublicationState.IDEMPOTENT}
                for result in results
            ),
        )

    def append_catalog_snapshot(self, rows: Sequence[Mapping[str, Any]]) -> str:
        return self._write(CATALOG_TABLE, rows)

    def append_cutover(self, row: Mapping[str, Any]) -> str:
        required = {
            "cutover_id",
            "scope_id",
            "active_source",
            "previous_source",
            "legacy_league",
            "legacy_season",
            "registry_signature",
            "effective_at",
            "rollback_run_id",
            "rollback_reason",
        }
        if set(row) != required:
            raise ValueError(f"cutover row fields must be exactly {sorted(required)}")
        if row["active_source"] not in {"native", "legacy"} or row[
            "previous_source"
        ] not in {"native", "legacy"}:
            raise ValueError("cutover sources must be native or legacy")
        if row["active_source"] == row["previous_source"]:
            raise ValueError("cutover must change active source")
        _sha256(row["registry_signature"], "registry_signature")
        _aware_utc(row["effective_at"], "effective_at")
        return self._write(CUTOVER_TABLE, [dict(row)])


def build_catalog_snapshot(
    *,
    snapshot_id: str,
    registry_signature: str,
    captured_at: datetime,
    run_id: str,
    raw_uri: str,
    raw_sha256: str,
    parser_version: str,
    runtime_version: str,
    ingested_at: datetime,
    batch_id: str,
    competitions: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    _required_string(snapshot_id, "snapshot_id")
    _sha256(registry_signature, "registry_signature")
    _aware_utc(captured_at, "captured_at")
    for field_name, value in (
        ("run_id", run_id),
        ("raw_uri", raw_uri),
        ("parser_version", parser_version),
        ("runtime_version", runtime_version),
        ("batch_id", batch_id),
    ):
        _required_string(value, field_name)
    _sha256(raw_sha256, "raw_sha256")
    _aware_utc(ingested_at, "ingested_at")
    output: list[dict[str, Any]] = []
    seen: set[int] = set()
    for raw in competitions:
        if not isinstance(raw, Mapping):
            raise TypeError("catalog competitions must be mappings")
        espn_id = _positive_native_id(raw.get("espn_id"), "catalog espn_id")
        if espn_id in seen:
            raise ValueError("catalog snapshot has duplicate ESPN competition ID")
        seen.add(espn_id)
        slug = _required_string(raw.get("slug"), "catalog slug")
        payload = canonical_json(dict(raw))
        output.append(
            {
                "snapshot_id": snapshot_id,
                "registry_signature": registry_signature,
                "competition_id": espn_id,
                "competition_slug": slug,
                "record_json": payload,
                "record_sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
                "captured_at": captured_at,
                "run_id": run_id,
                "raw_uri": raw_uri,
                "raw_sha256": raw_sha256,
                "parser_version": parser_version,
                "runtime_version": runtime_version,
                "_source_fetched_at": captured_at,
                "_ingested_at": ingested_at,
                "_batch_id": batch_id,
                "_source": "espn",
                "_entity_type": "catalog",
            }
        )
    return tuple(sorted(output, key=lambda row: row["competition_id"]))


_TRINO_TYPES = {
    "scope_id": "varchar",
    "competition_id": "bigint",
    "competition_slug": "varchar",
    "source_season_year": "bigint",
    "event_id": "bigint",
    "kickoff": "timestamp(6)",
    "date": "timestamp(6)",
    "match_date": "timestamp(6)",
    "status": "varchar",
    "status_map_version": "varchar",
    "terminal": "boolean",
    "played_final": "boolean",
    "terminal_nonplayed": "boolean",
    "summary_required": "boolean",
    "home_team_id": "bigint",
    "away_team_id": "bigint",
    "team_id": "bigint",
    "athlete_id": "bigint",
    "venue_id": "bigint",
    "referee_id": "bigint",
    "game_id": "bigint",
    "home_score": "bigint",
    "away_score": "bigint",
    "attendance_value": "bigint",
    "attendance": "bigint",
    "is_home": "boolean",
    "starter": "boolean",
    "captain": "boolean",
    "subbed_in": "boolean",
    "subbed_out": "boolean",
    "appearances": "double",
    "fouls_committed": "double",
    "fouls_suffered": "double",
    "goal_assists": "double",
    "goals_conceded": "double",
    "offsides": "double",
    "own_goals": "double",
    "red_cards": "double",
    "saves": "double",
    "shots_faced": "double",
    "shots_on_target": "double",
    "sub_ins": "double",
    "total_goals": "double",
    "total_shots": "double",
    "yellow_cards": "double",
    "_source_fetched_at": "timestamp(6)",
    "_ingested_at": "timestamp(6)",
}


def _row_columns(entity: str) -> tuple[str, ...]:
    return tuple(field.name for field in fields(_ROW_TYPES[entity]))


def _column_type(entity: str, column: str) -> str:
    if column == "attendance" and entity == "schedule":
        return "varchar"
    if entity == "matchsheet" and column in _LEGACY_COLUMNS["matchsheet"]:
        if column == "is_home":
            return "boolean"
        if column == "attendance":
            return "bigint"
        if column == "_ingested_at":
            return "timestamp(6)"
        return "varchar"
    if entity == "matchsheet" and column == "score":
        return "bigint"
    if column in _TRINO_TYPES:
        return _TRINO_TYPES[column]
    return "varchar"


def _table_ddl(
    qualified: str, columns: Sequence[tuple[str, str]], partition: str
) -> str:
    body = ",\n    ".join(f'"{name}" {kind}' for name, kind in columns)
    return (
        f"CREATE TABLE IF NOT EXISTS {qualified} (\n    {body}\n)\n"
        f"WITH (format = 'PARQUET', partitioning = ARRAY['{partition}'])"
    )


def render_repository_ddl(
    *, catalog: str = "iceberg", schema: str = "bronze"
) -> Mapping[str, str]:
    catalog = _identifier(catalog, "catalog")
    schema = _identifier(schema, "schema")
    statements: dict[str, str] = {}
    for entity, table in ENTITY_TABLES.items():
        columns = [
            (column, _column_type(entity, column)) for column in _row_columns(entity)
        ]
        for column in PROVENANCE_COLUMNS:
            if column not in {name for name, _ in columns}:
                columns.append((column, _column_type(entity, column)))
        statements[table] = _table_ddl(
            f"{catalog}.{schema}.{table}", columns, "scope_id"
        )
    statements[CATALOG_TABLE] = _table_ddl(
        f"{catalog}.{schema}.{CATALOG_TABLE}",
        (
            ("snapshot_id", "varchar"),
            ("registry_signature", "varchar"),
            ("competition_id", "bigint"),
            ("competition_slug", "varchar"),
            ("record_json", "varchar"),
            ("record_sha256", "varchar"),
            ("captured_at", "timestamp(6)"),
            ("run_id", "varchar"),
            ("raw_uri", "varchar"),
            ("raw_sha256", "varchar"),
            ("parser_version", "varchar"),
            ("runtime_version", "varchar"),
            ("_source_fetched_at", "timestamp(6)"),
            ("_ingested_at", "timestamp(6)"),
            ("_batch_id", "varchar"),
            ("_source", "varchar"),
            ("_entity_type", "varchar"),
        ),
        "snapshot_id",
    )
    manifest_types = {
        "competition_id": "bigint",
        "source_season_year": "bigint",
        "completed_at": "timestamp(6)",
    }
    statements[MANIFEST_TABLE] = _table_ddl(
        f"{catalog}.{schema}.{MANIFEST_TABLE}",
        tuple(
            (column, manifest_types.get(column, "varchar"))
            for column in MANIFEST_COLUMNS
        ),
        "scope_id",
    )
    statements[CUTOVER_TABLE] = _table_ddl(
        f"{catalog}.{schema}.{CUTOVER_TABLE}",
        (
            ("cutover_id", "varchar"),
            ("scope_id", "varchar"),
            ("active_source", "varchar"),
            ("previous_source", "varchar"),
            ("legacy_league", "varchar"),
            ("legacy_season", "varchar"),
            ("registry_signature", "varchar"),
            ("effective_at", "timestamp(6)"),
            ("rollback_run_id", "varchar"),
            ("rollback_reason", "varchar"),
        ),
        "scope_id",
    )
    return MappingProxyType(statements)


_LEGACY_COLUMNS = MappingProxyType(
    {
        "schedule": frozenset(
            {
                "_batch_id",
                "_entity_type",
                "_ingested_at",
                "_source",
                "attendance",
                "away_goals",
                "away_score",
                "away_team",
                "game",
                "game_id",
                "home_goals",
                "home_score",
                "home_team",
                "league",
                "league_id",
                "match_date",
                "season",
                "status",
                "venue",
            }
        ),
        "lineup": frozenset(
            {
                "_batch_id",
                "_entity_type",
                "_ingested_at",
                "_source",
                "appearances",
                "formation_place",
                "fouls_committed",
                "fouls_suffered",
                "game",
                "goal_assists",
                "goals_conceded",
                "is_home",
                "league",
                "offsides",
                "own_goals",
                "player",
                "position",
                "red_cards",
                "saves",
                "season",
                "shots_faced",
                "shots_on_target",
                "sub_in",
                "sub_ins",
                "sub_out",
                "team",
                "total_goals",
                "total_shots",
                "yellow_cards",
            }
        ),
        "matchsheet": frozenset(
            {
                "_batch_id",
                "_entity_type",
                "_ingested_at",
                "_source",
                "accurate_crosses",
                "accurate_long_balls",
                "accurate_passes",
                "attendance",
                "blocked_shots",
                "capacity",
                "cross_pct",
                "effective_clearance",
                "effective_tackles",
                "fouls_committed",
                "game",
                "goal_assists",
                "goal_difference",
                "goals_conceded",
                "interceptions",
                "is_home",
                "league",
                "longball_pct",
                "offsides",
                "pass_pct",
                "penalty_kick_goals",
                "penalty_kick_shots",
                "possession_pct",
                "red_cards",
                "roster",
                "saves",
                "season",
                "shot_pct",
                "shots_on_target",
                "tackle_pct",
                "team",
                "total_clearance",
                "total_crosses",
                "total_goals",
                "total_long_balls",
                "total_passes",
                "total_shots",
                "total_tackles",
                "venue",
                "won_corners",
                "yellow_cards",
            }
        ),
    }
)


def render_current_view_sql(
    entity: str, *, catalog: str = "iceberg", schema: str = "bronze"
) -> str:
    if entity not in ENTITY_TABLES:
        raise ValueError(f"unknown ESPN entity {entity!r}")
    catalog = _identifier(catalog, "catalog")
    schema = _identifier(schema, "schema")
    table = ENTITY_TABLES[entity]
    view = CURRENT_VIEWS[entity]
    columns = tuple(dict.fromkeys((*_row_columns(entity), *PROVENANCE_COLUMNS)))
    native_projection = ",\n        ".join(f'g."{column}"' for column in columns)
    legacy_projection = ",\n        ".join(
        f'l."{column}"'
        if column in _LEGACY_COLUMNS[entity]
        else f'CAST(NULL AS {_column_type(entity, column)}) AS "{column}"'
        for column in columns
    )
    join_keys = (
        "scope_id",
        "competition_id",
        "source_season_year",
        "generation_id",
        "run_id",
        "_batch_id",
        "registry_snapshot_uri",
        "registry_signature",
        "plan_signature",
        "parser_version",
        "runtime_version",
    )
    join = "\n   AND ".join(f'g."{key}" = m."{key}"' for key in join_keys)
    identity_projection = ", ".join(f'"{key}"' for key in join_keys)
    fence_join = "\n   AND ".join(f'f."{key}" = m."{key}"' for key in join_keys)
    return f"""CREATE OR REPLACE VIEW {catalog}.{schema}.{view} AS
WITH ranked_manifests AS (
    SELECT m.*,
           ROW_NUMBER() OVER (
               PARTITION BY scope_id
               ORDER BY completed_at DESC, generation_id DESC, manifest_sha256 DESC
           ) AS rn
    FROM {catalog}.{schema}.{MANIFEST_TABLE} m
    WHERE status = 'complete'
), latest_complete AS (
    SELECT * FROM ranked_manifests WHERE rn = 1
), ranked_generation_rows AS (
    SELECT g.*,
           ROW_NUMBER() OVER (
               PARTITION BY {identity_projection}, "_row_sha256"
               ORDER BY "_ingested_at" DESC, "_row_sha256" DESC
           ) AS physical_rn
    FROM {catalog}.{schema}.{table} g
), generation_rows AS (
    SELECT * FROM ranked_generation_rows WHERE physical_rn = 1
), physical_fence AS (
    SELECT {identity_projection},
           COUNT(*) AS row_count,
           lower(to_hex(sha256(to_utf8(array_join(array_sort(array_agg("_row_sha256")), ''))))) AS row_hash
    FROM generation_rows
    GROUP BY {identity_projection}
), validated_manifests AS (
    SELECT m.*
    FROM latest_complete m
    JOIN physical_fence f
      ON {fence_join}
     AND f.row_count = CAST(json_extract_scalar(m.row_counts_json, '$.{entity}') AS bigint)
     AND f.row_hash = json_extract_scalar(m.row_hashes_json, '$.{entity}')
), ranked_cutovers AS (
    SELECT c.*,
           ROW_NUMBER() OVER (
               PARTITION BY scope_id
               ORDER BY effective_at DESC, cutover_id DESC
           ) AS rn
    FROM {catalog}.{schema}.{CUTOVER_TABLE} c
), latest_cutover AS (
    SELECT * FROM ranked_cutovers WHERE rn = 1
), native_scopes AS (
    SELECT * FROM latest_cutover WHERE active_source = 'native'
), legacy_scopes AS (
    SELECT * FROM latest_cutover WHERE active_source = 'legacy'
), native_rows AS (
    SELECT
        {native_projection}
    FROM generation_rows g
    JOIN validated_manifests m
      ON {join}
    JOIN native_scopes c ON c.scope_id = m.scope_id
), legacy_rows AS (
    SELECT
        {legacy_projection}
    FROM {catalog}.{schema}.espn_{entity} l
    WHERE NOT EXISTS (
        SELECT 1 FROM native_scopes c
        WHERE c.legacy_league = l.league
          AND c.legacy_season = CAST(l.season AS varchar)
    )
      AND (
        NOT EXISTS (
            SELECT 1 FROM latest_cutover c
            WHERE c.legacy_league = l.league
              AND c.legacy_season = CAST(l.season AS varchar)
        )
        OR EXISTS (
            SELECT 1 FROM legacy_scopes c
            WHERE c.legacy_league = l.league
              AND c.legacy_season = CAST(l.season AS varchar)
        )
      )
)
SELECT * FROM native_rows
UNION ALL
SELECT * FROM legacy_rows"""


__all__ = [
    "BatchPublicationResult",
    "CATALOG_TABLE",
    "CURRENT_VIEWS",
    "CUTOVER_TABLE",
    "ENTITY_TABLES",
    "EspnBronzeRepository",
    "MANIFEST_TABLE",
    "ManifestConflictError",
    "PROVENANCE_COLUMNS",
    "PublicationError",
    "RawLedgerRecord",
    "REPOSITORY_VERSION",
    "ScopeGeneration",
    "ScopePublicationResult",
    "ScopePublicationState",
    "ScopeQualityReport",
    "build_catalog_snapshot",
    "canonical_json",
    "canonical_sha256",
    "render_current_view_sql",
    "render_repository_ddl",
    "row_fingerprint",
    "validate_scope_generation",
]
