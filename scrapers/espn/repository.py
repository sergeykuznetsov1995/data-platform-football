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
from typing import Any, Callable, Iterable, Mapping, Optional, Protocol, Sequence

import pandas as pd

from .models import (
    CapabilityState,
    DispositionState,
    RequestDisposition,
    ScopePlan,
)
from .parser_common import source_day_contains
from .parser_contracts import LineupRow, MatchsheetRow, ScheduleRow
from .selection import (
    CURRENT_MANIFEST_ORDER_FIELDS,
    select_current_manifest,
)


REPOSITORY_VERSION = "espn-bronze-repository-v2"
MANIFEST_VERSION = "espn-ingest-manifest-v2"
CATALOG_TABLE = "espn_catalog_snapshot_v2"
MANIFEST_TABLE = "espn_ingest_manifest_v2"
CUTOVER_TABLE = "espn_scope_cutover_v2"
LEDGER_TABLE = "espn_request_ledger_generation_v2"
BASELINE_TABLE = "espn_legacy_baseline_v2"
_CUTOVER_ANCESTRY_COLUMNS = (
    ("ancestor_cutover_sha256_json", "varchar"),
    ("ancestor_lineage_sha256", "varchar"),
)
_CUTOVER_GRAPH_COLUMNS = (
    "cutover_id",
    "scope_id",
    "cutover_sha256",
    "predecessor_cutover_id",
    "predecessor_cutover_sha256",
    "ancestor_cutover_sha256_json",
    "ancestor_lineage_sha256",
)
_CUTOVER_ROUTE_COLUMNS = (
    *_CUTOVER_GRAPH_COLUMNS,
    "active_source",
    "previous_source",
    "legacy_league",
    "legacy_season",
    "registry_signature",
    "native_generation_id",
    "native_generation_signature",
    "native_manifest_sha256",
    "effective_at",
)
_PHYSICAL_IDENTITY_COLUMNS = (
    "scope_id",
    "competition_id",
    "source_season_year",
    "generation_id",
    "generation_signature",
    "run_id",
    "_batch_id",
    "registry_snapshot_uri",
    "registry_signature",
    "plan_signature",
    "parser_version",
    "runtime_version",
)
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
    "generation_signature",
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

TABLE_PARTITIONS = MappingProxyType(
    {
        **{table: ("scope_id",) for table in ENTITY_TABLES.values()},
        LEDGER_TABLE: ("scope_id",),
        MANIFEST_TABLE: ("scope_id",),
        CUTOVER_TABLE: ("scope_id",),
        BASELINE_TABLE: ("scope_id",),
        CATALOG_TABLE: ("snapshot_id",),
    }
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
    if type(value) is not str or not value.strip():
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


def _stored_utc(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _text(value: object, field_name: str, *, optional: bool = False) -> None:
    if value is None and optional:
        return
    if type(value) is not str or not value.strip():
        kind = "non-empty string or null" if optional else "non-empty string"
        raise TypeError(f"{field_name} must be a {kind}")


def _canonical_json_text(value: object, field_name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{field_name} must be canonical JSON text")
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field_name} must be valid JSON") from exc
    if canonical_json(decoded) != value:
        raise ValueError(f"{field_name} must use canonical JSON encoding")


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


def _stored_cutover_ancestry(
    raw_json: object, raw_lineage_sha256: object
) -> tuple[str, ...]:
    if type(raw_json) is not str:
        raise PublicationError("stored cutover ancestry must be canonical JSON")
    try:
        decoded = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise PublicationError("stored cutover ancestry JSON is invalid") from exc
    if not isinstance(decoded, list):
        raise PublicationError("stored cutover ancestry must be a JSON array")
    try:
        ancestors = tuple(
            _sha256(value, "stored ancestor cutover SHA-256") for value in decoded
        )
    except ValueError as exc:
        raise PublicationError(
            "stored cutover ancestry contains an invalid SHA-256"
        ) from exc
    if len(set(ancestors)) != len(ancestors):
        raise PublicationError("stored cutover ancestry contains a cycle")
    if canonical_json(ancestors) != raw_json:
        raise PublicationError("stored cutover ancestry is not canonical")
    try:
        lineage_sha256 = _sha256(raw_lineage_sha256, "stored ancestor lineage SHA-256")
    except ValueError as exc:
        raise PublicationError("stored cutover ancestry hash is invalid") from exc
    if lineage_sha256 != canonical_sha256(ancestors):
        raise PublicationError("stored cutover ancestry hash does not match")
    return ancestors


@dataclass(frozen=True, slots=True)
class _StoredCutoverGraphNode:
    cutover_id: str
    scope_id: str
    cutover_sha256: str
    predecessor_cutover_id: str | None
    predecessor_cutover_sha256: str | None
    ancestor_cutover_sha256s: tuple[str, ...]


def _stored_cutover_graph_node(raw: object) -> _StoredCutoverGraphNode:
    if isinstance(raw, Mapping):
        values = tuple(raw.get(column) for column in _CUTOVER_GRAPH_COLUMNS)
    elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        values = tuple(raw)
    else:
        raise PublicationError(
            "cutover ancestry migration graph row must be a mapping or sequence"
        )
    if len(values) != len(_CUTOVER_GRAPH_COLUMNS):
        raise PublicationError(
            "cutover ancestry migration graph row has an unexpected column count"
        )
    (
        raw_cutover_id,
        raw_scope_id,
        raw_cutover_sha256,
        raw_predecessor_id,
        raw_predecessor_sha256,
        raw_ancestry_json,
        raw_lineage_sha256,
    ) = values
    try:
        cutover_id = _required_string(raw_cutover_id, "stored cutover_id")
        cutover_sha256 = _sha256(raw_cutover_sha256, "stored cutover_sha256")
    except ValueError as exc:
        raise PublicationError(
            "cutover ancestry migration graph identity is malformed"
        ) from exc
    if type(raw_scope_id) is not str or _SCOPE_RE.fullmatch(raw_scope_id) is None:
        raise PublicationError(
            f"cutover ancestry migration graph scope is invalid for {cutover_id!r}"
        )
    predecessor_values = (raw_predecessor_id, raw_predecessor_sha256)
    if any(value is not None for value in predecessor_values) and not all(
        value is not None for value in predecessor_values
    ):
        raise PublicationError(
            f"cutover ancestry migration predecessor pair is incomplete for {cutover_id!r}"
        )
    predecessor_id: str | None = None
    predecessor_sha256: str | None = None
    if raw_predecessor_id is not None:
        try:
            predecessor_id = _required_string(
                raw_predecessor_id, "stored predecessor_cutover_id"
            )
            predecessor_sha256 = _sha256(
                raw_predecessor_sha256, "stored predecessor_cutover_sha256"
            )
        except ValueError as exc:
            raise PublicationError(
                f"cutover ancestry migration predecessor is malformed for {cutover_id!r}"
            ) from exc
    try:
        ancestors = _stored_cutover_ancestry(raw_ancestry_json, raw_lineage_sha256)
    except PublicationError as exc:
        raise PublicationError(
            f"cutover ancestry migration validation failed for {cutover_id!r}: {exc}"
        ) from exc
    if predecessor_sha256 is None:
        if ancestors:
            raise PublicationError(
                f"cutover ancestry migration root {cutover_id!r} must have empty ancestry"
            )
    elif not ancestors or ancestors[-1] != predecessor_sha256:
        raise PublicationError(
            f"cutover ancestry migration child {cutover_id!r} must end with its predecessor hash"
        )
    return _StoredCutoverGraphNode(
        cutover_id=cutover_id,
        scope_id=raw_scope_id,
        cutover_sha256=cutover_sha256,
        predecessor_cutover_id=predecessor_id,
        predecessor_cutover_sha256=predecessor_sha256,
        ancestor_cutover_sha256s=ancestors,
    )


def _validate_stored_cutover_graph(rows: Sequence[object]) -> None:
    """Validate the complete logical control graph before replacing views.

    The startup query projects only graph fields and applies DISTINCT, so this
    uses linear memory in logical cutovers rather than physical retry rows.
    """

    nodes = {_stored_cutover_graph_node(raw) for raw in rows}
    by_id: dict[str, _StoredCutoverGraphNode] = {}
    by_hash: dict[str, _StoredCutoverGraphNode] = {}
    by_identity: dict[tuple[str, str, str], _StoredCutoverGraphNode] = {}
    global_identities: set[tuple[str, str]] = set()
    for node in nodes:
        previous_id = by_id.setdefault(node.cutover_id, node)
        if previous_id != node:
            raise PublicationError(
                f"cutover ancestry migration found conflicting cutover_id {node.cutover_id!r}"
            )
        previous_hash = by_hash.setdefault(node.cutover_sha256, node)
        if previous_hash != node:
            raise PublicationError(
                "cutover ancestry migration found one hash bound to conflicting nodes"
            )
        by_identity[(node.scope_id, node.cutover_id, node.cutover_sha256)] = node
        global_identities.add((node.cutover_id, node.cutover_sha256))

    children: dict[_StoredCutoverGraphNode, set[_StoredCutoverGraphNode]] = {}
    roots: set[_StoredCutoverGraphNode] = set()
    for node in nodes:
        if node.predecessor_cutover_id is None:
            roots.add(node)
            continue
        identity = (
            node.scope_id,
            node.predecessor_cutover_id,
            node.predecessor_cutover_sha256,
        )
        predecessor = by_identity.get(identity)
        if predecessor is None:
            same_global_identity = (
                node.predecessor_cutover_id,
                node.predecessor_cutover_sha256,
            ) in global_identities
            kind = "cross-scope" if same_global_identity else "missing or ambiguous"
            raise PublicationError(
                f"cutover ancestry migration has {kind} predecessor for {node.cutover_id!r}"
            )
        expected_ancestry = (
            *predecessor.ancestor_cutover_sha256s,
            predecessor.cutover_sha256,
        )
        if node.ancestor_cutover_sha256s != expected_ancestry:
            raise PublicationError(
                f"cutover ancestry migration child {node.cutover_id!r} does not extend its predecessor"
            )
        children.setdefault(predecessor, set()).add(node)

    reachable = set(roots)
    pending = list(roots)
    while pending:
        predecessor = pending.pop()
        for child in children.get(predecessor, set()):
            if child not in reachable:
                reachable.add(child)
                pending.append(child)
    if reachable != nodes:
        raise PublicationError(
            "cutover ancestry migration graph contains a cyclic or unreachable node"
        )


def _freeze_json(value: Any, field_name: str) -> Any:
    if isinstance(value, Mapping):
        frozen: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{field_name} keys must be strings")
            frozen[key] = _freeze_json(item, f"{field_name}.{key}")
        return MappingProxyType(frozen)
    if isinstance(value, (tuple, list)):
        return tuple(_freeze_json(item, field_name) for item in value)
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{field_name} must contain finite JSON numbers")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"{field_name} must contain JSON-compatible values")


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

    @classmethod
    def from_physical_row(cls, row: Mapping[str, Any]) -> "RawLedgerRecord":
        if not isinstance(row, Mapping):
            raise TypeError("physical ledger row must be a mapping")
        raw_event_ids = row.get("event_ids_json")
        if not isinstance(raw_event_ids, str):
            raise ValueError("physical ledger event_ids_json must be canonical JSON")
        try:
            event_ids = json.loads(raw_event_ids)
        except json.JSONDecodeError as exc:
            raise ValueError("physical ledger event_ids_json is invalid") from exc
        if canonical_json(event_ids) != raw_event_ids:
            raise ValueError("physical ledger event_ids_json must be canonical JSON")
        raw_disposition = row.get("disposition")
        try:
            disposition = DispositionState(raw_disposition)
        except (TypeError, ValueError) as exc:
            raise ValueError("physical ledger disposition is invalid") from exc
        raw_fetched_at = row.get("fetched_at")
        fetched_at = (
            None
            if raw_fetched_at is None
            else _stored_utc(raw_fetched_at, "physical ledger fetched_at")
        )
        return cls(
            request_id=row.get("request_id"),
            endpoint=row.get("endpoint"),
            event_id=row.get("event_id"),
            disposition=disposition,
            raw_uri=row.get("raw_uri"),
            raw_sha256=row.get("raw_sha256"),
            fetched_at=fetched_at,
            direct_bytes=row.get("direct_bytes"),
            proxy_bytes=row.get("proxy_bytes"),
            event_ids=tuple(event_ids),
        )


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
    def generation_signature(self) -> str:
        return canonical_sha256(
            {
                "repository_version": REPOSITORY_VERSION,
                "plan": self.plan,
                "run_id": self.run_id,
                "generation_id": self.generation_id,
                "registry_snapshot_uri": self.registry_snapshot_uri,
                "registry_signature": self.registry_signature,
                "plan_signature": self.plan_signature,
                "parser_version": self.parser_version,
                "runtime_version": self.runtime_version,
                "ingested_at": self.ingested_at,
                "batch_id": self.batch_id,
                "schedule": self.schedule,
                "lineup": self.lineup,
                "matchsheet": self.matchsheet,
                "planned_request_ids": self.planned_request_ids,
                "raw_ledger": self.raw_ledger,
                "dispositions": self.dispositions,
            }
        )

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
            "generation_signature": self.generation_signature,
            "_batch_id": self.batch_id,
            "registry_snapshot_uri": self.registry_snapshot_uri,
            "registry_signature": self.registry_signature,
            "plan_signature": self.plan_signature,
            "parser_version": self.parser_version,
            "runtime_version": self.runtime_version,
            "status": "complete",
            "row_counts_json": canonical_json(dict(report.row_counts)),
            "row_hashes_json": canonical_json(dict(report.row_hashes)),
            "ledger_count": report.ledger_count,
            "ledger_hash": report.ledger_hash,
            "planned_request_ids_json": canonical_json(
                sorted(self.planned_request_ids)
            ),
            "planned_request_ids_sha256": canonical_sha256(
                sorted(self.planned_request_ids)
            ),
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
        for field_name in (
            "scope_id",
            "competition_slug",
            "status",
            "status_map_version",
            "home_team",
            "away_team",
            "league",
            "season",
            "game",
            "league_id",
            "parser_version",
        ):
            _text(getattr(row, field_name), f"schedule.{field_name}")
        for field_name in (
            "venue",
            "attendance",
            "home_goals",
            "away_goals",
        ):
            _text(
                getattr(row, field_name),
                f"schedule.{field_name}",
                optional=True,
            )
        _canonical_json_text(row.extra_json, "schedule.extra_json")
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
        for field_name in (
            "scope_id",
            "team",
            "home_away",
            "league",
            "season",
            "game",
            "parser_version",
        ):
            _text(getattr(row, field_name), f"{entity}.{field_name}")
        if row.home_away not in {"home", "away"}:
            raise ValueError(f"{entity}.home_away must be home or away")
        if row.is_home != (row.home_away == "home"):
            raise ValueError(f"{entity}.home_away conflicts with is_home")
        if entity == "lineup":
            _positive_native_id(row.athlete_id, "lineup.athlete_id")
            _text(row.player, "lineup.player")
            _text(row.stat_map_version, "lineup.stat_map_version")
            for field_name in (
                "jersey",
                "position",
                "formation_place",
                "sub_in",
                "sub_out",
            ):
                _text(
                    getattr(row, field_name),
                    f"lineup.{field_name}",
                    optional=True,
                )
            for field_name in (
                "substitutions_json",
                "statistics_json",
                "extra_json",
            ):
                _canonical_json_text(getattr(row, field_name), f"lineup.{field_name}")
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
                    type(value) is not float or not math.isfinite(value)
                ):
                    raise TypeError(
                        f"lineup.{field_name} must be finite numeric or null"
                    )
        if entity == "matchsheet":
            _text(row.stat_map_version, "matchsheet.stat_map_version")
            for field_name in (
                "accurate_crosses",
                "accurate_long_balls",
                "accurate_passes",
                "blocked_shots",
                "capacity",
                "cross_pct",
                "effective_clearance",
                "effective_tackles",
                "fouls_committed",
                "goal_assists",
                "goal_difference",
                "goals_conceded",
                "interceptions",
                "longball_pct",
                "offsides",
                "pass_pct",
                "penalty_kick_goals",
                "penalty_kick_shots",
                "possession_pct",
                "red_cards",
                "roster",
                "saves",
                "shot_pct",
                "shots_on_target",
                "tackle_pct",
                "total_clearance",
                "total_crosses",
                "total_goals",
                "total_long_balls",
                "total_passes",
                "total_shots",
                "total_tackles",
                "won_corners",
                "yellow_cards",
                "corner_kicks",
                "venue",
                "referee",
            ):
                _text(
                    getattr(row, field_name),
                    f"matchsheet.{field_name}",
                    optional=True,
                )
            for field_name in ("statistics_json", "extra_json"):
                _canonical_json_text(
                    getattr(row, field_name), f"matchsheet.{field_name}"
                )
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
    ledger_count: int
    ledger_hash: str

    def __post_init__(self) -> None:
        if _SCOPE_RE.fullmatch(self.scope_id) is None:
            raise ValueError("invalid scope_id")
        if type(self.passed) is not bool:
            raise TypeError("passed must be boolean")
        object.__setattr__(self, "failures", tuple(self.failures))
        object.__setattr__(self, "row_counts", MappingProxyType(dict(self.row_counts)))
        object.__setattr__(self, "row_hashes", MappingProxyType(dict(self.row_hashes)))
        _nonnegative_int(self.ledger_count, "ledger_count")
        _sha256(self.ledger_hash, "ledger_hash")


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


def _row_fingerprint(
    generation: ScopeGeneration,
    entity: str,
    row: Any,
    *,
    generation_signature: str,
) -> str:
    if entity not in _ROW_TYPES or not isinstance(row, _ROW_TYPES[entity]):
        raise TypeError("row does not match the requested ESPN entity")
    raw = _raw_binding(generation, entity, row.event_id)
    return canonical_sha256(
        {
            "row": row,
            "scope_id": generation.plan.scope_id,
            "generation_id": generation.generation_id,
            "generation_signature": generation_signature,
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


def row_fingerprint(generation: ScopeGeneration, entity: str, row: Any) -> str:
    """Hash the complete immutable physical-row identity, including raw origin."""

    return _row_fingerprint(
        generation,
        entity,
        row,
        generation_signature=generation.generation_signature,
    )


def _ledger_row_fingerprint(
    generation: ScopeGeneration,
    record: RawLedgerRecord,
    *,
    generation_signature: str,
) -> str:
    if not isinstance(record, RawLedgerRecord):
        raise TypeError("record must be RawLedgerRecord")
    return canonical_sha256(
        {
            "record": record,
            "scope_id": generation.plan.scope_id,
            "generation_id": generation.generation_id,
            "generation_signature": generation_signature,
            "run_id": generation.run_id,
            "registry_snapshot_uri": generation.registry_snapshot_uri,
            "registry_signature": generation.registry_signature,
            "plan_signature": generation.plan_signature,
            "parser_version": generation.parser_version,
            "runtime_version": generation.runtime_version,
            "ingested_at": generation.ingested_at,
            "batch_id": generation.batch_id,
        }
    )


def ledger_row_fingerprint(generation: ScopeGeneration, record: RawLedgerRecord) -> str:
    return _ledger_row_fingerprint(
        generation,
        record,
        generation_signature=generation.generation_signature,
    )


def _ledger_dataset_hash(
    generation: ScopeGeneration, *, generation_signature: str
) -> str:
    hashes = sorted(
        _ledger_row_fingerprint(
            generation,
            record,
            generation_signature=generation_signature,
        )
        for record in generation.raw_ledger
    )
    return hashlib.sha256("".join(hashes).encode("ascii")).hexdigest()


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
    if any(
        item.disposition is not DispositionState.CAPTURED
        for item in generation.raw_ledger
    ):
        failures.append("planned raw request is not captured")
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
        if not source_day_contains(
            row.kickoff.astimezone(timezone.utc).date(),
            scope.start_date,
            scope.end_date,
        ):
            failures.append("edition window excludes schedule event")
        if row.date != row.kickoff or row.match_date != row.kickoff:
            failures.append("schedule legacy timestamps must equal native kickoff")
        if row.event_id in schedule_by_event:
            failures.append("schedule event uniqueness violated")
        schedule_by_event[row.event_id] = row
        if row.home_team_id == row.away_team_id:
            failures.append("schedule event must have two distinct sides")
        if row.played_final and (row.home_score is None or row.away_score is None):
            failures.append("played-final schedule event requires both scores")
        # Task 3's versioned parser owns status validation and intentionally
        # preserves optional source scores for terminal non-played outcomes.
        # Do not reinterpret forfeit, walkover, cancelled, or abandoned scores.
        if (
            row.played_final
            and row.terminal_nonplayed
            or row.terminal != (row.played_final or row.terminal_nonplayed)
            or row.summary_required != row.played_final
        ):
            failures.append("schedule status flag invariant violated")
    scoreboard_records = tuple(
        item for item in generation.raw_ledger if item.endpoint == "scoreboard"
    )
    if not generation.schedule:
        ledger_complete = (
            len(set(planned)) == len(planned)
            and len(set(observed)) == len(observed)
            and set(planned) == set(observed)
            and all(
                item.disposition is DispositionState.CAPTURED
                for item in generation.raw_ledger
            )
        )
        empty_schedule_has_exact_evidence = (
            bool(scoreboard_records)
            and all(
                item.disposition is DispositionState.CAPTURED and not item.event_ids
                for item in scoreboard_records
            )
            and ledger_complete
            and not any(item.endpoint == "summary" for item in generation.raw_ledger)
            and not generation.dispositions
        )
        if not empty_schedule_has_exact_evidence:
            failures.append(
                "empty schedule requires complete successful scoreboard raw evidence "
                "and no Summary/disposition rows"
            )
        if scope.capabilities.schedule is CapabilityState.PROVEN:
            failures.append("empty proven schedule capability")
    scoreboard_event_ids = [
        event_id for item in scoreboard_records for event_id in item.event_ids
    ]
    if set(scoreboard_event_ids) != set(schedule_by_event) or len(
        scoreboard_event_ids
    ) != len(set(scoreboard_event_ids)):
        failures.append("scoreboard event binding parity differs from schedule")
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

    for entity, grouped in (
        ("lineup", lineup_by_event),
        ("matchsheet", matchsheet_by_event),
    ):
        for event_id, sides in grouped.items():
            event = schedule_by_event.get(event_id)
            if event is None:
                continue
            if sides != {event.home_team_id, event.away_team_id}:
                failures.append(f"{entity} two-side completeness failed for {event_id}")

    disposition_index: dict[tuple[str, int], RequestDisposition] = {}
    for item in generation.dispositions:
        if item.endpoint not in {"lineup", "matchsheet"} or item.event_id is None:
            failures.append("entity disposition has invalid endpoint or event")
            continue
        if item.event_id not in schedule_by_event:
            failures.append("entity disposition references an unknown event")
        key = (item.endpoint, item.event_id)
        if key in disposition_index:
            failures.append("entity disposition is duplicated")
        disposition_index[key] = item

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
    for event_id in captured_summary_counts:
        for entity in ("lineup", "matchsheet"):
            if (entity, event_id) not in disposition_index:
                failures.append(
                    f"captured Summary disposition missing for {entity}/{event_id}"
                )

    for (entity, event_id), disposition in disposition_index.items():
        event = schedule_by_event.get(event_id)
        if event is None:
            continue
        sides = (
            lineup_by_event.get(event_id, set())
            if entity == "lineup"
            else matchsheet_by_event.get(event_id, set())
        )
        capability = getattr(scope.capabilities, entity)
        summary_count = captured_summary_counts.get(event_id, 0)
        if not event.summary_required:
            if disposition.state is not DispositionState.NOT_APPLICABLE:
                failures.append(
                    f"nonfinal {entity} must be not_applicable for {event_id}"
                )
            if summary_count or sides:
                failures.append(
                    f"not_applicable {entity} contains Summary/rows for {event_id}"
                )
            continue
        if disposition.state is DispositionState.CAPTURED:
            if summary_count != 1:
                failures.append(
                    f"captured {entity} requires exact raw Summary for {event_id}"
                )
            if sides != {event.home_team_id, event.away_team_id}:
                failures.append(f"{entity} two-side completeness failed for {event_id}")
        elif disposition.state is DispositionState.VALID_EMPTY:
            if capability not in {
                CapabilityState.PARTIAL,
                CapabilityState.ABSENT,
                CapabilityState.UNKNOWN,
            }:
                failures.append(f"valid_empty is forbidden for proven {entity}")
            if summary_count != 1:
                failures.append(
                    f"valid_empty requires successful raw Summary for {entity}/{event_id}"
                )
            if sides:
                failures.append(f"valid_empty {entity} contains physical rows")
        else:
            failures.append(f"entity disposition unresolved for {entity}/{event_id}")

    for entity, grouped in (
        ("lineup", lineup_by_event),
        ("matchsheet", matchsheet_by_event),
    ):
        for event_id in grouped:
            if (entity, event_id) not in disposition_index:
                failures.append(
                    f"emitted {entity} rows lack disposition for {event_id}"
                )

    for event_id, event in schedule_by_event.items():
        for entity in ("lineup", "matchsheet"):
            if (entity, event_id) not in disposition_index:
                failures.append(
                    f"event disposition missing for {entity}/{event_id}"
                )

    generation_signature = generation.generation_signature
    row_hashes: dict[str, str] = {}
    for entity in _ENTITIES:
        hashes: list[str] = []
        for row in getattr(generation, entity):
            try:
                hashes.append(
                    _row_fingerprint(
                        generation,
                        entity,
                        row,
                        generation_signature=generation_signature,
                    )
                )
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
        ledger_count=len(generation.raw_ledger),
        ledger_hash=_ledger_dataset_hash(
            generation,
            generation_signature=generation_signature,
        ),
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


@dataclass(frozen=True, slots=True)
class CatalogSnapshot:
    snapshot_id: str
    registry_signature: str
    captured_at: datetime
    run_id: str
    raw_uri: str
    raw_sha256: str
    parser_version: str
    runtime_version: str
    ingested_at: datetime
    batch_id: str
    competitions: tuple[Mapping[str, Any], ...]

    def __post_init__(self) -> None:
        for field_name in (
            "snapshot_id",
            "run_id",
            "raw_uri",
            "parser_version",
            "runtime_version",
            "batch_id",
        ):
            _required_string(getattr(self, field_name), field_name)
        _sha256(self.registry_signature, "registry_signature")
        _sha256(self.raw_sha256, "raw_sha256")
        _aware_utc(self.captured_at, "captured_at")
        _aware_utc(self.ingested_at, "ingested_at")
        if not isinstance(self.competitions, (tuple, list)):
            raise TypeError("catalog competitions must be a sequence")
        normalized: list[tuple[int, Mapping[str, Any]]] = []
        seen_ids: set[int] = set()
        seen_slugs: set[str] = set()
        for raw in self.competitions:
            if not isinstance(raw, Mapping):
                raise TypeError("catalog competition must be a mapping")
            espn_id = _positive_native_id(raw.get("espn_id"), "catalog espn_id")
            slug = _required_string(raw.get("slug"), "catalog slug")
            if espn_id in seen_ids or slug in seen_slugs:
                raise ValueError("catalog snapshot has duplicate ID or slug")
            seen_ids.add(espn_id)
            seen_slugs.add(slug)
            frozen = _freeze_json(dict(raw), "catalog competition")
            normalized.append((espn_id, frozen))
        if not normalized:
            raise ValueError("catalog snapshot must contain competitions")
        object.__setattr__(
            self,
            "competitions",
            tuple(value for _, value in sorted(normalized, key=lambda item: item[0])),
        )

    @property
    def content_sha256(self) -> str:
        return canonical_sha256(self.competitions)

    @property
    def snapshot_signature(self) -> str:
        return canonical_sha256(
            {
                "snapshot_id": self.snapshot_id,
                "registry_signature": self.registry_signature,
                "captured_at": self.captured_at,
                "run_id": self.run_id,
                "raw_uri": self.raw_uri,
                "raw_sha256": self.raw_sha256,
                "parser_version": self.parser_version,
                "runtime_version": self.runtime_version,
                "ingested_at": self.ingested_at,
                "batch_id": self.batch_id,
                "content_sha256": self.content_sha256,
            }
        )

    @property
    def rows(self) -> tuple[dict[str, Any], ...]:
        output: list[dict[str, Any]] = []
        for raw in self.competitions:
            payload = canonical_json(raw)
            output.append(
                {
                    "snapshot_id": self.snapshot_id,
                    "snapshot_signature": self.snapshot_signature,
                    "snapshot_content_sha256": self.content_sha256,
                    "registry_signature": self.registry_signature,
                    "competition_id": raw["espn_id"],
                    "competition_slug": raw["slug"],
                    "record_json": payload,
                    "record_sha256": hashlib.sha256(
                        payload.encode("utf-8")
                    ).hexdigest(),
                    "captured_at": self.captured_at,
                    "run_id": self.run_id,
                    "raw_uri": self.raw_uri,
                    "raw_sha256": self.raw_sha256,
                    "parser_version": self.parser_version,
                    "runtime_version": self.runtime_version,
                    "_source_fetched_at": self.captured_at,
                    "_ingested_at": self.ingested_at,
                    "_batch_id": self.batch_id,
                    "_source": "espn",
                    "_entity_type": "catalog",
                }
            )
        return tuple(output)


@dataclass(frozen=True, slots=True)
class ScopeCutover:
    """One source transition with an immutable activation-readiness proof.

    The bound native generation proves that native data was complete when the
    source was activated; it is not a version pin.  Once native is active, the
    current views keep selecting the latest validated generation for the scope.
    """

    cutover_id: str
    scope_id: str
    active_source: str
    previous_source: str
    predecessor_cutover_id: str | None
    predecessor_cutover_sha256: str | None
    legacy_league: str | None
    legacy_season: str | None
    registry_signature: str
    effective_at: datetime
    native_generation_id: str | None
    native_generation_signature: str | None
    native_manifest_sha256: str | None
    rollback_run_id: str | None
    rollback_reason: str | None
    metadata: Mapping[str, Any]
    ancestor_cutover_sha256s: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for field_name in ("cutover_id",):
            _required_string(getattr(self, field_name), field_name)
        if (
            not isinstance(self.scope_id, str)
            or _SCOPE_RE.fullmatch(self.scope_id) is None
        ):
            raise ValueError("cutover scope_id is invalid")
        if self.active_source not in {
            "native",
            "legacy",
            "absent",
        } or self.previous_source not in {
            "native",
            "legacy",
            "absent",
        }:
            raise ValueError("cutover sources must be native, legacy or absent")
        if self.active_source == self.previous_source:
            raise ValueError("cutover must change active source")
        predecessor_values = (
            self.predecessor_cutover_id,
            self.predecessor_cutover_sha256,
        )
        if any(value is not None for value in predecessor_values) and not all(
            value is not None for value in predecessor_values
        ):
            raise ValueError(
                "cutover predecessor ID and hash must be supplied together"
            )
        if self.predecessor_cutover_id is not None:
            _required_string(self.predecessor_cutover_id, "predecessor_cutover_id")
            _sha256(
                self.predecessor_cutover_sha256,
                "predecessor_cutover_sha256",
            )
        if not isinstance(self.ancestor_cutover_sha256s, (tuple, list)):
            raise TypeError("cutover ancestry must be a sequence")
        ancestors = tuple(
            _sha256(value, "ancestor cutover SHA-256")
            for value in self.ancestor_cutover_sha256s
        )
        if len(set(ancestors)) != len(ancestors):
            raise ValueError("cutover ancestry must not contain a cycle")
        if self.predecessor_cutover_sha256 is None:
            if ancestors:
                raise ValueError("root cutover ancestry must be empty")
        elif not ancestors or ancestors[-1] != self.predecessor_cutover_sha256:
            raise ValueError("cutover ancestry must end with the predecessor hash")
        object.__setattr__(self, "ancestor_cutover_sha256s", ancestors)
        aliases = (self.legacy_league, self.legacy_season)
        if any(value is None for value in aliases) and not all(
            value is None for value in aliases
        ):
            raise ValueError(
                "legacy fallback aliases must be both present or both null"
            )
        has_legacy_fallback = all(value is not None for value in aliases)
        if has_legacy_fallback:
            _required_string(self.legacy_league, "legacy_league")
            _required_string(self.legacy_season, "legacy_season")
        _sha256(self.registry_signature, "registry_signature")
        _aware_utc(self.effective_at, "effective_at")
        native_values = (
            self.native_generation_id,
            self.native_generation_signature,
            self.native_manifest_sha256,
        )
        if self.active_source == "native":
            if self.previous_source not in {"legacy", "absent"} or not all(
                native_values
            ):
                raise ValueError(
                    "native cutover requires a fallback transition and complete manifest binding"
                )
            if (self.previous_source == "legacy") != has_legacy_fallback:
                raise ValueError(
                    "native cutover fallback source and legacy aliases disagree"
                )
            _required_string(self.native_generation_id, "native_generation_id")
            _sha256(self.native_generation_signature, "native_generation_signature")
            _sha256(self.native_manifest_sha256, "native_manifest_sha256")
            if self.rollback_run_id is not None or self.rollback_reason is not None:
                raise ValueError("native cutover must not claim rollback audit fields")
        else:
            if self.previous_source != "native" or any(native_values):
                raise ValueError(
                    "rollback must transition from native without native binding"
                )
            if (self.active_source == "legacy") != has_legacy_fallback:
                raise ValueError("rollback source and legacy fallback aliases disagree")
            _required_string(self.rollback_run_id, "rollback_run_id")
            _required_string(self.rollback_reason, "rollback_reason")
        if not isinstance(self.metadata, Mapping) or not self.metadata:
            raise ValueError("cutover metadata must be a non-empty mapping")
        object.__setattr__(self, "metadata", _freeze_json(self.metadata, "metadata"))

    def constructor_values(self) -> dict[str, Any]:
        return {field.name: getattr(self, field.name) for field in fields(self)}

    @property
    def cutover_sha256(self) -> str:
        return canonical_sha256(self.constructor_values())

    @property
    def ancestor_lineage_sha256(self) -> str:
        return canonical_sha256(self.ancestor_cutover_sha256s)

    def to_row(self) -> dict[str, Any]:
        return {
            **{
                field.name: getattr(self, field.name)
                for field in fields(self)
                if field.name not in {"metadata", "ancestor_cutover_sha256s"}
            },
            "metadata_json": canonical_json(self.metadata),
            "ancestor_cutover_sha256_json": canonical_json(
                self.ancestor_cutover_sha256s
            ),
            "ancestor_lineage_sha256": self.ancestor_lineage_sha256,
            "cutover_sha256": self.cutover_sha256,
        }


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
        allow_target_ddl: bool = True,
    ) -> str: ...


class QueryProtocol(Protocol):
    def execute_query(
        self, sql: str, params: Optional[tuple[Any, ...]] = None
    ) -> Sequence[Any]: ...


def _physical_rows(generation: ScopeGeneration, entity: str) -> list[dict[str, Any]]:
    generation_signature = generation.generation_signature
    output: list[dict[str, Any]] = []
    for typed_row in getattr(generation, entity):
        row = asdict(typed_row)
        raw = _raw_binding(generation, entity, typed_row.event_id)
        row_hash = _row_fingerprint(
            generation,
            entity,
            typed_row,
            generation_signature=generation_signature,
        )
        row.update(
            {
                "scope_id": generation.plan.scope_id,
                "generation_id": generation.generation_id,
                "generation_signature": generation_signature,
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


def _ledger_physical_rows(generation: ScopeGeneration) -> list[dict[str, Any]]:
    generation_signature = generation.generation_signature
    output: list[dict[str, Any]] = []
    for record in generation.raw_ledger:
        output.append(
            {
                "scope_id": generation.plan.scope_id,
                "competition_id": generation.plan.espn_id,
                "source_season_year": generation.plan.source_season_year,
                "generation_id": generation.generation_id,
                "generation_signature": generation_signature,
                "run_id": generation.run_id,
                "_batch_id": generation.batch_id,
                "registry_snapshot_uri": generation.registry_snapshot_uri,
                "registry_signature": generation.registry_signature,
                "plan_signature": generation.plan_signature,
                "parser_version": generation.parser_version,
                "runtime_version": generation.runtime_version,
                "request_id": record.request_id,
                "planned": record.request_id in generation.planned_request_ids,
                "endpoint": record.endpoint,
                "event_id": record.event_id,
                "event_ids_json": canonical_json(record.event_ids),
                "disposition": record.disposition.value,
                "raw_uri": record.raw_uri,
                "raw_sha256": record.raw_sha256,
                "fetched_at": record.fetched_at,
                "direct_bytes": record.direct_bytes,
                "proxy_bytes": record.proxy_bytes,
                "_ingested_at": generation.ingested_at,
                "_source": "espn",
                "_entity_type": "request_ledger",
                "_row_sha256": _ledger_row_fingerprint(
                    generation,
                    record,
                    generation_signature=generation_signature,
                ),
            }
        )
    return output


MANIFEST_COLUMNS = (
    "manifest_version",
    "repository_version",
    "scope_id",
    "competition_id",
    "source_season_year",
    "run_id",
    "generation_id",
    "generation_signature",
    "_batch_id",
    "registry_snapshot_uri",
    "registry_signature",
    "plan_signature",
    "parser_version",
    "runtime_version",
    "status",
    "row_counts_json",
    "row_hashes_json",
    "ledger_count",
    "ledger_hash",
    "planned_request_ids_json",
    "planned_request_ids_sha256",
    "raw_ledger_sha256",
    "dispositions_json",
    "quality_json",
    "completed_at",
    "manifest_sha256",
)


class EspnBronzeRepository:
    """Production adapter over the platform IcebergWriter and Trino manager.

    Publication is append-only and detects immutable-identity conflicts.  A
    distributed caller must still hold the source/scope lease while appending
    catalog snapshots or cutovers: Iceberg append has no uniqueness primitive.
    Cutover predecessor hashes make a raced fork durable and current views
    suppress it instead of choosing a branch. Direct writes to the cutover
    table are outside the supported contract: every transition must use
    :meth:`append_cutover` under that lease.
    """

    manifest_columns = MANIFEST_COLUMNS

    def __init__(
        self,
        *,
        writer: WriterProtocol | None = None,
        query: QueryProtocol | None = None,
        catalog: str = "iceberg",
        schema: str = "bronze",
        verify_physical: bool = True,
        ensure_objects_on_write: bool = True,
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
        if type(ensure_objects_on_write) is not bool:
            raise TypeError("ensure_objects_on_write must be boolean")
        self.verify_physical = verify_physical
        self.ensure_objects_on_write = ensure_objects_on_write
        self._objects_ensured = False

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> Sequence[Any]:
        execute = getattr(self.query, "execute_query", None)
        if not callable(execute):
            raise TypeError("query adapter must expose execute_query")
        return execute(sql, params=params) or []

    def ensure_objects(self) -> None:
        if self._objects_ensured:
            return
        self._execute(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        for sql in render_repository_ddl(
            catalog=self.catalog, schema=self.schema
        ).values():
            self._execute(sql)
        qualified_cutover = f"{self.catalog}.{self.schema}.{CUTOVER_TABLE}"
        for column, column_type in _CUTOVER_ANCESTRY_COLUMNS:
            self._execute(
                f"ALTER TABLE {qualified_cutover} ADD COLUMN IF NOT EXISTS "
                f'"{column}" {column_type}'
            )
        graph_projection = ", ".join(f'"{column}"' for column in _CUTOVER_GRAPH_COLUMNS)
        stored_cutover_graph = self._execute(
            f"SELECT DISTINCT {graph_projection} "
            "/* cutover_ancestry_rollout_gate */ "
            f"FROM {qualified_cutover}"
        )
        _validate_stored_cutover_graph(stored_cutover_graph)
        for entity in _ENTITIES:
            self._execute(
                render_current_view_sql(
                    entity, catalog=self.catalog, schema=self.schema
                )
            )
        self._objects_ensured = True

    def _existing_manifest(
        self, scope_id: str, generation_id: str
    ) -> Mapping[str, Any] | None:
        columns = ", ".join(f'"{column}"' for column in MANIFEST_COLUMNS)
        rows = self._execute(
            f"SELECT {columns} FROM {self.catalog}.{self.schema}.{MANIFEST_TABLE} "
            'WHERE "scope_id" = ? AND "generation_id" = ? '
            "AND \"status\" = 'complete' "
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
        fingerprints = {
            (
                str(row["manifest_sha256"]),
                str(row["generation_signature"]),
            )
            for row in normalized
        }
        if len(fingerprints) != 1:
            raise ManifestConflictError(
                "conflicting manifests share one generation identity"
            )
        return normalized[0]

    def _write(self, table: str, rows: Sequence[Mapping[str, Any]]) -> str:
        if table not in TABLE_PARTITIONS:
            raise ValueError(f"unsupported ESPN repository table {table!r}")
        if not rows:
            return f"{self.catalog}.{self.schema}.{table}"
        frame = pd.DataFrame(list(rows))
        if table == LEDGER_TABLE and "event_id" in frame:
            frame["event_id"] = frame["event_id"].astype("Int64")
        return self.writer.write_dataframe(
            frame,
            database=self.schema,
            table=table,
            partition_spec=[(column, "identity") for column in TABLE_PARTITIONS[table]],
            mode="append",
            add_metadata=False,
            source="espn",
            allow_target_ddl=self.ensure_objects_on_write,
        )

    def _physical_row_hashes(
        self, generation: ScopeGeneration, entity: str
    ) -> frozenset[str]:
        generation_signature = generation.generation_signature
        table = LEDGER_TABLE if entity == "ledger" else ENTITY_TABLES[entity]
        rows = self._execute(
            f'SELECT DISTINCT "generation_signature", "_row_sha256" '
            f"FROM {self.catalog}.{self.schema}.{table} "
            'WHERE "scope_id" = ? AND "generation_id" = ?',
            (
                generation.plan.scope_id,
                generation.generation_id,
            ),
        )
        hashes: set[str] = set()
        for raw in rows:
            if isinstance(raw, Mapping):
                signature = raw.get("generation_signature")
                value = raw.get("_row_sha256")
            else:
                if len(raw) != 2:
                    raise PublicationError("physical fingerprint row is malformed")
                signature, value = raw
            if signature != generation_signature:
                raise ManifestConflictError(
                    f"{entity} generation identity has conflicting content signature"
                )
            hashes.add(_sha256(value, "stored _row_sha256"))
        return frozenset(hashes)

    def _verify_physical(
        self, generation: ScopeGeneration, report: ScopeQualityReport
    ) -> None:
        generation_signature = generation.generation_signature
        selects: list[str] = []
        params: list[Any] = []
        for entity, table in (*ENTITY_TABLES.items(), ("ledger", LEDGER_TABLE)):
            selects.append(
                f"SELECT '{entity}' AS entity, COUNT(DISTINCT \"_row_sha256\") AS row_count, "
                "COALESCE(lower(to_hex(sha256(to_utf8(array_join(array_sort(array_distinct(array_agg(\"_row_sha256\"))), ''))))), "
                "lower(to_hex(sha256(to_utf8(''))))) AS row_hash "
                f"FROM {self.catalog}.{self.schema}.{table} "
                'WHERE "scope_id" = ? AND "generation_id" = ? AND "run_id" = ? '
                'AND "generation_signature" = ? AND "_batch_id" = ? '
                'AND "registry_signature" = ? AND "plan_signature" = ?'
            )
            params.extend(
                (
                    generation.plan.scope_id,
                    generation.generation_id,
                    generation.run_id,
                    generation_signature,
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
        expected["ledger"] = (report.ledger_count, report.ledger_hash)
        if observed != expected:
            raise PublicationError(
                f"physical row/hash parity failed: expected={expected!r}, observed={observed!r}"
            )

    def _verify_stored_manifest_physical(self, manifest: Mapping[str, Any]) -> None:
        """Prove a stored COMPLETE manifest passes the current-view fence."""

        try:
            scope_id = _required_string(manifest.get("scope_id"), "stored scope_id")
            if _SCOPE_RE.fullmatch(scope_id) is None:
                raise ValueError("stored scope_id is invalid")
            competition_id = _positive_native_id(
                manifest.get("competition_id"), "stored competition_id"
            )
            source_season_year = _positive_native_id(
                manifest.get("source_season_year"), "stored source_season_year"
            )
            if scope_id != f"{competition_id}:{source_season_year}":
                raise ValueError("stored manifest scope identity is inconsistent")
            identity = {
                "scope_id": scope_id,
                "competition_id": competition_id,
                "source_season_year": source_season_year,
                "generation_id": _required_string(
                    manifest.get("generation_id"), "stored generation_id"
                ),
                "generation_signature": _sha256(
                    manifest.get("generation_signature"),
                    "stored generation_signature",
                ),
                "run_id": _required_string(manifest.get("run_id"), "stored run_id"),
                "_batch_id": _required_string(
                    manifest.get("_batch_id"), "stored _batch_id"
                ),
                "registry_snapshot_uri": _required_string(
                    manifest.get("registry_snapshot_uri"),
                    "stored registry_snapshot_uri",
                ),
                "registry_signature": _sha256(
                    manifest.get("registry_signature"),
                    "stored registry_signature",
                ),
                "plan_signature": _sha256(
                    manifest.get("plan_signature"), "stored plan_signature"
                ),
                "parser_version": _required_string(
                    manifest.get("parser_version"), "stored parser_version"
                ),
                "runtime_version": _required_string(
                    manifest.get("runtime_version"), "stored runtime_version"
                ),
            }
            if manifest.get("status") != "complete":
                raise ValueError("stored manifest status is not complete")

            raw_counts = manifest.get("row_counts_json")
            raw_hashes = manifest.get("row_hashes_json")
            if type(raw_counts) is not str or type(raw_hashes) is not str:
                raise ValueError("stored manifest row metrics are not JSON strings")
            counts = json.loads(raw_counts)
            hashes = json.loads(raw_hashes)
            if type(counts) is not dict or set(counts) != set(_ENTITIES):
                raise ValueError("stored manifest row counts have invalid entities")
            if type(hashes) is not dict or set(hashes) != set(_ENTITIES):
                raise ValueError("stored manifest row hashes have invalid entities")
            expected = {
                entity: (
                    _nonnegative_int(counts[entity], f"stored {entity} row count"),
                    _sha256(hashes[entity], f"stored {entity} row hash"),
                )
                for entity in _ENTITIES
            }
            expected["ledger"] = (
                _nonnegative_int(manifest.get("ledger_count"), "stored ledger_count"),
                _sha256(manifest.get("ledger_hash"), "stored ledger_hash"),
            )
        except (json.JSONDecodeError, ValueError) as exc:
            raise PublicationError(
                f"stored COMPLETE manifest is malformed: {exc}"
            ) from exc

        selects: list[str] = []
        params: list[Any] = []
        for entity, table in (*ENTITY_TABLES.items(), ("ledger", LEDGER_TABLE)):
            predicates = " AND ".join(
                f'"{column}" = ?' for column in _PHYSICAL_IDENTITY_COLUMNS
            )
            selects.append(
                f"SELECT '{entity}' AS entity, COUNT(DISTINCT \"_row_sha256\") AS row_count, "
                "COALESCE(lower(to_hex(sha256(to_utf8(array_join(array_sort(array_distinct(array_agg(\"_row_sha256\"))), ''))))), "
                "lower(to_hex(sha256(to_utf8(''))))) AS row_hash "
                f"FROM {self.catalog}.{self.schema}.{table} "
                f"WHERE {predicates} /* stored_manifest_physical */"
            )
            params.extend(identity[column] for column in _PHYSICAL_IDENTITY_COLUMNS)
        rows = self._execute(" UNION ALL ".join(selects), tuple(params))
        observed: dict[str, tuple[int, str]] = {}
        for raw in rows:
            if isinstance(raw, Mapping):
                entity = str(raw.get("entity"))
                count = int(raw.get("row_count") or 0)
                row_hash = str(raw.get("row_hash") or "")
            elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
                if len(raw) != 3:
                    raise PublicationError(
                        "stored manifest physical parity row is malformed"
                    )
                entity, count, row_hash = str(raw[0]), int(raw[1]), str(raw[2] or "")
            else:
                raise PublicationError(
                    "stored manifest physical parity row is malformed"
                )
            if entity in observed:
                raise PublicationError(
                    "stored manifest physical parity duplicated an entity"
                )
            observed[entity] = (count, row_hash)
        if observed != expected:
            raise PublicationError(
                "stored manifest physical row/hash parity failed: "
                f"expected={expected!r}, observed={observed!r}"
            )

    def _scope_has_unresolved_cutover_fork(self, scope_id: str) -> bool:
        rows = self._execute(
            f"""WITH scope_cutovers AS (
    SELECT *
    FROM {self.catalog}.{self.schema}.{CUTOVER_TABLE}
    WHERE "scope_id" = ?
), conflicting_ids AS (
    SELECT "cutover_id"
    FROM {self.catalog}.{self.schema}.{CUTOVER_TABLE}
    GROUP BY "cutover_id"
    HAVING COUNT(DISTINCT "cutover_sha256") > 1
), conflicting_predecessors AS (
    SELECT "predecessor_cutover_sha256"
    FROM scope_cutovers
    GROUP BY "predecessor_cutover_sha256"
    HAVING COUNT(DISTINCT "cutover_sha256") > 1
), unresolved_cutover_forks AS (
    SELECT c."cutover_sha256"
    FROM scope_cutovers c
    JOIN conflicting_ids conflict ON conflict."cutover_id" = c."cutover_id"
    UNION
    SELECT c."cutover_sha256"
    FROM scope_cutovers c
    JOIN conflicting_predecessors fork
      ON fork."predecessor_cutover_sha256" IS NOT DISTINCT FROM c."predecessor_cutover_sha256"
)
SELECT "cutover_sha256" FROM unresolved_cutover_forks LIMIT 1""",
            (scope_id,),
        )
        return bool(rows)

    def publish_scope(
        self,
        generation: ScopeGeneration,
        *,
        publication_fence: Callable[[], None] | None = None,
    ) -> ScopePublicationResult:
        """Publish one generation, rechecking an optional distributed fence.

        The fence is invoked immediately before every append, especially the
        COMPLETE manifest.  Production orchestration supplies a callback
        backed by the scope lease epoch; ordinary callers retain the Task 4
        behavior when no callback is supplied.
        """

        if publication_fence is not None and not callable(publication_fence):
            raise TypeError("publication_fence must be callable or None")

        def assert_fence() -> None:
            if publication_fence is not None:
                publication_fence()

        assert_fence()
        report = validate_scope_generation(generation)
        if not report.passed:
            raise PublicationError("; ".join(report.failures))
        if self.ensure_objects_on_write:
            self.ensure_objects()
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
                assert_fence()
                self._write(table, rows)
            ledger_rows = _ledger_physical_rows(generation)
            if self.verify_physical:
                existing_hashes = self._physical_row_hashes(generation, "ledger")
                expected_hashes = frozenset(row["_row_sha256"] for row in ledger_rows)
                unexpected = existing_hashes - expected_hashes
                if unexpected:
                    raise ManifestConflictError(
                        "ledger generation contains conflicting physical rows"
                    )
                ledger_rows = [
                    row
                    for row in ledger_rows
                    if row["_row_sha256"] not in existing_hashes
                ]
            assert_fence()
            self._write(LEDGER_TABLE, ledger_rows)
            if self.verify_physical:
                self._verify_physical(generation, report)
        except ManifestConflictError:
            raise
        except Exception as exc:
            raise PublicationError(
                f"scope {generation.plan.scope_id} generation publication failed: {exc}"
            ) from exc
        # Keep the lease error outside the generic publication wrapper so the
        # orchestrator can classify a reclaimed owner as a hard alert.  The
        # manifest is the visibility boundary: without it physical rows remain
        # harmless and the previous COMPLETE generation stays current.
        assert_fence()
        try:
            self._write(MANIFEST_TABLE, [manifest])
        except Exception as exc:
            raise PublicationError(
                f"scope {generation.plan.scope_id} manifest publication failed: {exc}"
            ) from exc
        return ScopePublicationResult(
            scope_id=generation.plan.scope_id,
            generation_id=generation.generation_id,
            state=ScopePublicationState.PUBLISHED,
            manifest_sha256=manifest["manifest_sha256"],
        )

    def verify_published_scope(self, generation: ScopeGeneration) -> ScopeQualityReport:
        """Run exact current-generation DQ without invoking any append path."""

        report = validate_scope_generation(generation)
        if not report.passed:
            raise PublicationError("; ".join(report.failures))
        expected = generation.manifest_row(report)
        stored = self._existing_manifest(
            generation.plan.scope_id, generation.generation_id
        )
        if stored is None:
            raise PublicationError("exact COMPLETE scope manifest is missing")
        identity_fields = (
            "manifest_version",
            "scope_id",
            "generation_id",
            "generation_signature",
            "run_id",
            "registry_snapshot_uri",
            "registry_signature",
            "plan_signature",
            "parser_version",
            "runtime_version",
            "_batch_id",
            "status",
            "manifest_sha256",
        )
        mismatched = [
            field
            for field in identity_fields
            if stored.get(field) != expected.get(field)
        ]
        if mismatched:
            raise ManifestConflictError(
                "published manifest identity mismatch: " + ", ".join(mismatched)
            )
        if self.verify_physical:
            self._verify_physical(generation, report)
        return report

    def verify_current_scope_selection(
        self, generation: ScopeGeneration
    ) -> dict[str, int]:
        """Prove the real cutover-gated current views select this generation."""

        report = self.verify_published_scope(generation)
        observed_counts: dict[str, int] = {}
        expected_identity = (
            generation.generation_id,
            generation.generation_signature,
            generation.run_id,
            generation.registry_signature,
            generation.plan_signature,
        )
        for entity, view in CURRENT_VIEWS.items():
            rows = self._execute(
                'SELECT "generation_id", "generation_signature", "run_id", '
                '"registry_signature", "plan_signature", COUNT(*) AS row_count '
                f"FROM {self.catalog}.{self.schema}.{view} "
                'WHERE "scope_id" = ? GROUP BY 1, 2, 3, 4, 5',
                (generation.plan.scope_id,),
            )
            identities = []
            for raw in rows:
                if isinstance(raw, Mapping):
                    identity = tuple(
                        raw.get(field)
                        for field in (
                            "generation_id",
                            "generation_signature",
                            "run_id",
                            "registry_signature",
                            "plan_signature",
                        )
                    )
                    count = int(raw.get("row_count") or 0)
                else:
                    if len(raw) != 6:
                        raise PublicationError("current view identity row is malformed")
                    identity, count = tuple(raw[:5]), int(raw[5])
                identities.append((identity, count))
            expected_count = report.row_counts[entity]
            if expected_count == 0:
                if identities:
                    raise PublicationError(
                        f"{entity} current view contains unexpected scope rows"
                    )
            elif identities != [(expected_identity, expected_count)]:
                raise PublicationError(
                    f"{entity} current view selection differs from control head"
                )
            observed_counts[entity] = expected_count
        return observed_counts

    def current_scope_route(self, scope_id: str) -> str | None:
        """Return the route only after proving its current-view eligibility.

        A native route is not useful evidence by itself: its fallback tuple and
        activation-bound COMPLETE generation must also pass the same physical
        fence as the view.  That activation proof is deliberately not a serving
        pin; current rows may advance to a newer validated generation.
        """

        if type(scope_id) is not str or _SCOPE_RE.fullmatch(scope_id) is None:
            raise ValueError("scope_id is invalid")
        projection = ", ".join(f'"{column}"' for column in _CUTOVER_ROUTE_COLUMNS)
        stored_routes = self._execute(
            f"SELECT DISTINCT {projection} "
            f"FROM {self.catalog}.{self.schema}.{CUTOVER_TABLE}"
        )
        normalized: list[tuple[Any, ...]] = []
        for raw in stored_routes:
            if isinstance(raw, Mapping):
                values = tuple(raw.get(column) for column in _CUTOVER_ROUTE_COLUMNS)
            elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
                values = tuple(raw)
            else:
                raise PublicationError("stored cutover route row is malformed")
            if len(values) != len(_CUTOVER_ROUTE_COLUMNS):
                raise PublicationError(
                    "stored cutover route row has an unexpected column count"
                )
            normalized.append(values)
        _validate_stored_cutover_graph(
            tuple(values[: len(_CUTOVER_GRAPH_COLUMNS)] for values in normalized)
        )
        if self._scope_has_unresolved_cutover_fork(scope_id):
            raise ManifestConflictError(
                "scope has an unresolved cutover fork; current route is ambiguous"
            )
        routes: dict[tuple[str, str], tuple[Any, ...]] = {}
        for values in normalized:
            cutover_id = _required_string(values[0], "stored cutover_id")
            cutover_hash = _sha256(values[2], "stored cutover_sha256")
            active_source = values[7]
            if active_source not in {"native", "legacy", "absent"}:
                raise PublicationError("stored cutover active_source is invalid")
            previous_source = values[8]
            aliases = (values[9], values[10])
            if (aliases[0] is None) != (aliases[1] is None):
                raise PublicationError(
                    "stored cutover legacy aliases must be both present or both null"
                )
            has_legacy_fallback = aliases[0] is not None
            if has_legacy_fallback:
                legacy_league = _required_string(
                    aliases[0], "stored cutover legacy_league"
                )
                legacy_season = _required_string(
                    aliases[1], "stored cutover legacy_season"
                )
            else:
                legacy_league = legacy_season = None
            registry_signature = _sha256(
                values[11], "stored cutover registry_signature"
            )
            native_values = values[12:15]
            if active_source == "native":
                if previous_source not in {"legacy", "absent"}:
                    raise PublicationError(
                        "stored native cutover previous_source is invalid"
                    )
                if (previous_source == "legacy") != has_legacy_fallback:
                    raise PublicationError(
                        "stored native cutover fallback source and aliases disagree"
                    )
                native_generation_id = _required_string(
                    native_values[0], "stored cutover native_generation_id"
                )
                native_generation_signature = _sha256(
                    native_values[1],
                    "stored cutover native_generation_signature",
                )
                native_manifest_sha256 = _sha256(
                    native_values[2], "stored cutover native_manifest_sha256"
                )
            else:
                if previous_source != "native" or any(
                    value is not None for value in native_values
                ):
                    raise PublicationError(
                        "stored rollback must transition from native without native binding"
                    )
                if (active_source == "legacy") != has_legacy_fallback:
                    raise PublicationError(
                        "stored rollback source and legacy aliases disagree"
                    )
                native_generation_id = None
                native_generation_signature = None
                native_manifest_sha256 = None
            effective_at = _stored_utc(values[15], "stored cutover effective_at")
            if values[1] != scope_id:
                continue
            identity = (cutover_id, cutover_hash)
            route = (
                active_source,
                previous_source,
                legacy_league,
                legacy_season,
                registry_signature,
                native_generation_id,
                native_generation_signature,
                native_manifest_sha256,
                effective_at,
            )
            previous = routes.setdefault(identity, route)
            if previous != route:
                raise ManifestConflictError(
                    "stored cutover identity has conflicting route fields"
                )
        if not routes:
            return None
        (cutover_id, cutover_hash), selected = max(
            routes.items(),
            key=lambda item: (item[1][-1], item[0][0], item[0][1]),
        )
        active_source = selected[0]
        if active_source == "native":
            manifest = self._existing_manifest(scope_id, selected[5])
            if (
                manifest is None
                or manifest.get("status") != "complete"
                or manifest.get("registry_signature") != selected[4]
                or manifest.get("generation_signature") != selected[6]
                or manifest.get("manifest_sha256") != selected[7]
            ):
                raise PublicationError(
                    "current native route is not bound to a matching COMPLETE manifest"
                )
            self._verify_stored_manifest_physical(manifest)
        return active_source

    def verify_current_scope_absence(self, scope_id: str) -> dict[str, int]:
        """Prove cutover-gated views expose no native rows for one scope."""

        if type(scope_id) is not str or _SCOPE_RE.fullmatch(scope_id) is None:
            raise ValueError("scope_id is invalid")
        observed: dict[str, int] = {}
        for entity, view in CURRENT_VIEWS.items():
            rows = self._execute(
                f'SELECT COUNT(*) AS "row_count" '
                f"FROM {self.catalog}.{self.schema}.{view} "
                'WHERE "scope_id" = ?',
                (scope_id,),
            )
            if len(rows) != 1:
                raise PublicationError(
                    f"{entity} current view absence query is malformed"
                )
            raw = rows[0]
            if isinstance(raw, Mapping):
                count = raw.get("row_count")
            elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
                if len(raw) != 1:
                    raise PublicationError(
                        f"{entity} current view absence row is malformed"
                    )
                count = raw[0]
            else:
                raise PublicationError(
                    f"{entity} current view absence row is malformed"
                )
            if type(count) is not int or count < 0:
                raise PublicationError(
                    f"{entity} current view absence count is malformed"
                )
            if count:
                raise PublicationError(
                    f"{entity} current view exposes native rows while route uses fallback"
                )
            observed[entity] = count
        return observed

    def exact_complete_exists(self, generation: ScopeGeneration) -> bool:
        """Return whether this exact, physically valid COMPLETE already exists."""

        if self.ensure_objects_on_write:
            self.ensure_objects()
        existing = self._existing_manifest(
            generation.plan.scope_id, generation.generation_id
        )
        if existing is None:
            return False
        self.verify_published_scope(generation)
        return True

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

    def append_catalog_snapshot(self, snapshot: CatalogSnapshot) -> str:
        if not isinstance(snapshot, CatalogSnapshot):
            raise TypeError("snapshot must be CatalogSnapshot")
        if self.ensure_objects_on_write:
            self.ensure_objects()
        stored = self._execute(
            f'SELECT "competition_id", "record_sha256", "snapshot_signature" '
            f"FROM {self.catalog}.{self.schema}.{CATALOG_TABLE} "
            'WHERE "snapshot_id" = ?',
            (snapshot.snapshot_id,),
        )
        existing: dict[int, str] = {}
        signatures: set[str] = set()
        for raw in stored:
            if isinstance(raw, Mapping):
                competition_id = raw.get("competition_id")
                record_hash = raw.get("record_sha256")
                signature = raw.get("snapshot_signature")
            else:
                if len(raw) != 3:
                    raise PublicationError("catalog snapshot row is malformed")
                competition_id, record_hash, signature = raw
            competition_id = _positive_native_id(
                competition_id, "stored catalog competition_id"
            )
            record_hash = _sha256(record_hash, "stored catalog record_sha256")
            signatures.add(_sha256(signature, "stored snapshot_signature"))
            if competition_id in existing and existing[competition_id] != record_hash:
                raise ManifestConflictError(
                    "catalog snapshot contains conflicting competition rows"
                )
            existing[competition_id] = record_hash
        if signatures and signatures != {snapshot.snapshot_signature}:
            raise ManifestConflictError(
                "same catalog snapshot_id has conflicting content"
            )
        expected = {
            int(row["competition_id"]): str(row["record_sha256"])
            for row in snapshot.rows
        }
        if any(expected.get(key) != value for key, value in existing.items()):
            raise ManifestConflictError("catalog snapshot physical rows conflict")
        missing = [
            row for row in snapshot.rows if int(row["competition_id"]) not in existing
        ]
        return self._write(CATALOG_TABLE, missing)

    def append_cutover(self, cutover: ScopeCutover) -> str:
        if not isinstance(cutover, ScopeCutover):
            raise TypeError("cutover must be ScopeCutover")
        if self.ensure_objects_on_write:
            self.ensure_objects()
        existing_rows = self._execute(
            f'SELECT "cutover_sha256" FROM {self.catalog}.{self.schema}.{CUTOVER_TABLE} '
            'WHERE "cutover_id" = ?',
            (cutover.cutover_id,),
        )
        existing_hashes = {
            _sha256(
                raw.get("cutover_sha256") if isinstance(raw, Mapping) else raw[0],
                "stored cutover_sha256",
            )
            for raw in existing_rows
        }
        if existing_hashes:
            if existing_hashes == {cutover.cutover_sha256}:
                return f"{self.catalog}.{self.schema}.{CUTOVER_TABLE}"
            raise ManifestConflictError("same cutover_id has conflicting content")
        if self._scope_has_unresolved_cutover_fork(cutover.scope_id):
            raise ManifestConflictError(
                "scope has an unresolved cutover fork; serialize repair under lease"
            )
        slot_rows = self._execute(
            f'SELECT "cutover_id", "cutover_sha256" '
            f"FROM {self.catalog}.{self.schema}.{CUTOVER_TABLE} "
            'WHERE "scope_id" = ? '
            'AND "predecessor_cutover_sha256" IS NOT DISTINCT FROM ?',
            (cutover.scope_id, cutover.predecessor_cutover_sha256),
        )
        slot_identities: set[tuple[str, str]] = set()
        for raw in slot_rows:
            if isinstance(raw, Mapping):
                cutover_id = raw.get("cutover_id")
                cutover_hash = raw.get("cutover_sha256")
            else:
                if len(raw) != 2:
                    raise PublicationError("cutover predecessor slot row is malformed")
                cutover_id, cutover_hash = raw
            slot_identities.add(
                (
                    _required_string(cutover_id, "stored cutover_id"),
                    _sha256(cutover_hash, "stored cutover_sha256"),
                )
            )
        if slot_identities:
            raise ManifestConflictError(
                "cutover predecessor already has a different successor"
            )

        latest_rows = self._execute(
            f'SELECT "cutover_id", "cutover_sha256", "active_source", "effective_at", '
            '"ancestor_cutover_sha256_json", "ancestor_lineage_sha256", '
            '"legacy_league", "legacy_season" '
            f"FROM {self.catalog}.{self.schema}.{CUTOVER_TABLE} "
            'WHERE "scope_id" = ? '
            'ORDER BY "effective_at" DESC, "cutover_id" DESC, "cutover_sha256" DESC '
            "LIMIT 1",
            (cutover.scope_id,),
        )
        if latest_rows:
            raw = latest_rows[0]
            if isinstance(raw, Mapping):
                latest_id = raw.get("cutover_id")
                latest_hash = raw.get("cutover_sha256")
                active_source = raw.get("active_source")
                effective_at = raw.get("effective_at")
                raw_ancestry = raw.get("ancestor_cutover_sha256_json")
                raw_lineage_hash = raw.get("ancestor_lineage_sha256")
                latest_legacy_league = raw.get("legacy_league")
                latest_legacy_season = raw.get("legacy_season")
            else:
                if len(raw) != 8:
                    raise PublicationError("latest cutover row is malformed")
                (
                    latest_id,
                    latest_hash,
                    active_source,
                    effective_at,
                    raw_ancestry,
                    raw_lineage_hash,
                    latest_legacy_league,
                    latest_legacy_season,
                ) = raw
            latest_identity = (
                _required_string(latest_id, "stored latest cutover_id"),
                _sha256(latest_hash, "stored latest cutover_sha256"),
            )
            if latest_identity != (
                cutover.predecessor_cutover_id,
                cutover.predecessor_cutover_sha256,
            ):
                raise ManifestConflictError(
                    "cutover predecessor does not match latest scope transition"
                )
            latest_ancestry = _stored_cutover_ancestry(raw_ancestry, raw_lineage_hash)
            expected_ancestry = (*latest_ancestry, latest_identity[1])
            if cutover.ancestor_cutover_sha256s != expected_ancestry:
                raise ManifestConflictError(
                    "cutover ancestry does not extend the latest scope transition"
                )
            if active_source != cutover.previous_source:
                raise ManifestConflictError(
                    "cutover previous_source does not match latest active source"
                )
            if (latest_legacy_league, latest_legacy_season) != (
                cutover.legacy_league,
                cutover.legacy_season,
            ):
                raise ManifestConflictError(
                    "cutover fallback aliases do not match latest scope transition"
                )
            if cutover.effective_at <= _stored_utc(
                effective_at, "stored cutover effective_at"
            ):
                raise ManifestConflictError(
                    "cutover effective_at must advance the scope transition chain"
                )
        elif (
            cutover.previous_source not in {"legacy", "absent"}
            or cutover.predecessor_cutover_id is not None
            or cutover.predecessor_cutover_sha256 is not None
            or cutover.ancestor_cutover_sha256s
        ):
            raise ManifestConflictError(
                "first scope transition must start from a fallback without predecessor"
            )

        if cutover.active_source == "native":
            manifest = self._existing_manifest(
                cutover.scope_id, str(cutover.native_generation_id)
            )
            if (
                manifest is None
                or manifest.get("status") != "complete"
                or manifest.get("generation_signature")
                != cutover.native_generation_signature
                or manifest.get("manifest_sha256") != cutover.native_manifest_sha256
                or manifest.get("registry_signature") != cutover.registry_signature
            ):
                raise PublicationError(
                    "native cutover is not bound to a matching COMPLETE manifest"
                )
        return self._write(CUTOVER_TABLE, [cutover.to_row()])


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
) -> CatalogSnapshot:
    return CatalogSnapshot(
        snapshot_id=snapshot_id,
        registry_signature=registry_signature,
        captured_at=captured_at,
        run_id=run_id,
        raw_uri=raw_uri,
        raw_sha256=raw_sha256,
        parser_version=parser_version,
        runtime_version=runtime_version,
        ingested_at=ingested_at,
        batch_id=batch_id,
        competitions=tuple(competitions),
    )


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
    statements[LEDGER_TABLE] = _table_ddl(
        f"{catalog}.{schema}.{LEDGER_TABLE}",
        (
            ("scope_id", "varchar"),
            ("competition_id", "bigint"),
            ("source_season_year", "bigint"),
            ("generation_id", "varchar"),
            ("generation_signature", "varchar"),
            ("run_id", "varchar"),
            ("_batch_id", "varchar"),
            ("registry_snapshot_uri", "varchar"),
            ("registry_signature", "varchar"),
            ("plan_signature", "varchar"),
            ("parser_version", "varchar"),
            ("runtime_version", "varchar"),
            ("request_id", "varchar"),
            ("planned", "boolean"),
            ("endpoint", "varchar"),
            ("event_id", "bigint"),
            ("event_ids_json", "varchar"),
            ("disposition", "varchar"),
            ("raw_uri", "varchar"),
            ("raw_sha256", "varchar"),
            ("fetched_at", "timestamp(6)"),
            ("direct_bytes", "bigint"),
            ("proxy_bytes", "bigint"),
            ("_ingested_at", "timestamp(6)"),
            ("_source", "varchar"),
            ("_entity_type", "varchar"),
            ("_row_sha256", "varchar"),
        ),
        "scope_id",
    )
    statements[CATALOG_TABLE] = _table_ddl(
        f"{catalog}.{schema}.{CATALOG_TABLE}",
        (
            ("snapshot_id", "varchar"),
            ("snapshot_signature", "varchar"),
            ("snapshot_content_sha256", "varchar"),
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
        "ledger_count": "bigint",
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
            ("predecessor_cutover_id", "varchar"),
            ("predecessor_cutover_sha256", "varchar"),
            ("legacy_league", "varchar"),
            ("legacy_season", "varchar"),
            ("registry_signature", "varchar"),
            ("effective_at", "timestamp(6)"),
            ("native_generation_id", "varchar"),
            ("native_generation_signature", "varchar"),
            ("native_manifest_sha256", "varchar"),
            ("rollback_run_id", "varchar"),
            ("rollback_reason", "varchar"),
            ("metadata_json", "varchar"),
            *_CUTOVER_ANCESTRY_COLUMNS,
            ("cutover_sha256", "varchar"),
        ),
        "scope_id",
    )
    statements[BASELINE_TABLE] = _table_ddl(
        f"{catalog}.{schema}.{BASELINE_TABLE}",
        (
            ("baseline_version", "varchar"),
            ("scope_id", "varchar"),
            ("legacy_league", "varchar"),
            ("legacy_season", "varchar"),
            ("captured_at", "timestamp(6)"),
            ("entity_metrics_json", "varchar"),
            ("legacy_snapshot_ids_json", "varchar"),
            ("registry_signature", "varchar"),
            ("durable_manifest_uri", "varchar"),
            ("durable_manifest_sha256", "varchar"),
            ("replay_raw_manifest_uri", "varchar"),
            ("replay_raw_manifest_sha256", "varchar"),
            ("trust_label", "varchar"),
            ("baseline_sha256", "varchar"),
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
    view = CURRENT_VIEWS[entity]
    columns = tuple(dict.fromkeys((*_row_columns(entity), *PROVENANCE_COLUMNS)))
    native_projection = ",\n        ".join(f'g."{column}"' for column in columns)
    legacy_projection = ",\n        ".join(
        f'l."{column}"'
        if column in _LEGACY_COLUMNS[entity]
        else f'CAST(NULL AS {_column_type(entity, column)}) AS "{column}"'
        for column in columns
    )
    join_keys = _PHYSICAL_IDENTITY_COLUMNS
    # The signature is an isolation boundary.  Rows left by an incomplete
    # concurrent attempt with the same generation_id but another signature do
    # not contaminate (or hide) the manifest-bound generation.
    join = "\n   AND ".join(f'g."{key}" = m."{key}"' for key in join_keys)
    identity_projection = ", ".join(f'"{key}"' for key in join_keys)
    ranked_identity_projection = ", ".join(f'r."{key}"' for key in join_keys)
    candidate_join = "\n       AND ".join(
        f'r."{key}" = candidate."{key}"' for key in join_keys
    )
    empty_hash = "lower(to_hex(sha256(to_utf8(''))))"

    relation_tables = {**ENTITY_TABLES, "ledger": LEDGER_TABLE}
    fence_parts: list[str] = []
    for relation, relation_table in relation_tables.items():
        fence_parts.append(
            f"""ranked_{relation}_rows AS (
    SELECT r.*,
           ROW_NUMBER() OVER (
               PARTITION BY {ranked_identity_projection}, r."_row_sha256"
               ORDER BY r."_ingested_at" DESC, r."_row_sha256" DESC
           ) AS physical_rn
    FROM {catalog}.{schema}.{relation_table} r
    JOIN candidate_generation_identities candidate
      ON {candidate_join}
), {relation}_rows AS (
    SELECT * FROM ranked_{relation}_rows WHERE physical_rn = 1
), {relation}_fence AS (
    SELECT {identity_projection},
           COUNT(*) AS row_count,
           lower(to_hex(sha256(to_utf8(array_join(array_sort(array_agg("_row_sha256")), ''))))) AS row_hash
    FROM {relation}_rows
    GROUP BY {identity_projection}
)"""
        )
    fence_sql = ",\n".join(fence_parts)

    def fence_join(relation: str) -> str:
        return "\n   AND ".join(
            f'{relation}_fence."{key}" = m."{key}"' for key in join_keys
        )

    entity_validation = []
    for relation in _ENTITIES:
        entity_validation.extend(
            (
                f"COALESCE({relation}_fence.row_count, 0) = "
                f"TRY_CAST(TRY(json_extract_scalar(m.row_counts_json, '$.{relation}')) AS bigint)",
                f"COALESCE({relation}_fence.row_hash, {empty_hash}) = "
                f"TRY(json_extract_scalar(m.row_hashes_json, '$.{relation}'))",
            )
        )
    entity_validation.extend(
        (
            "COALESCE(ledger_fence.row_count, 0) = m.ledger_count",
            f"COALESCE(ledger_fence.row_hash, {empty_hash}) = m.ledger_hash",
        )
    )
    validation_where = "\n      AND ".join(entity_validation)
    fence_joins = "\n".join(
        f"LEFT JOIN {relation}_fence\n      ON {fence_join(relation)}"
        for relation in relation_tables
    )
    return f"""CREATE OR REPLACE VIEW {catalog}.{schema}.{view} AS
WITH complete_manifest_candidates AS (
    SELECT *
    FROM {catalog}.{schema}.{MANIFEST_TABLE}
    WHERE status = 'complete'
), candidate_generation_identities AS (
    SELECT DISTINCT {identity_projection}
    FROM complete_manifest_candidates
), conflicting_complete_identities AS (
    SELECT scope_id, generation_id
    FROM complete_manifest_candidates
    GROUP BY scope_id, generation_id
    HAVING COUNT(DISTINCT manifest_sha256) > 1
        OR COUNT(DISTINCT generation_signature) > 1
), {fence_sql},
validated_complete AS (
    SELECT m.*
    FROM complete_manifest_candidates m
    LEFT JOIN conflicting_complete_identities conflict
      ON conflict.scope_id = m.scope_id
     AND conflict.generation_id = m.generation_id
    {fence_joins}
    WHERE conflict.scope_id IS NULL
      AND {validation_where}
), ranked_manifests AS (
    SELECT m.*,
           ROW_NUMBER() OVER (
               PARTITION BY scope_id
               ORDER BY {", ".join(f"{field} DESC" for field in CURRENT_MANIFEST_ORDER_FIELDS)}
           ) AS rn
    FROM validated_complete m
), latest_validated AS (
    SELECT * FROM ranked_manifests WHERE rn = 1
), cutover_records AS (
    SELECT parsed.*
    FROM (
        SELECT c.*,
               TRY(CAST(json_parse(c.ancestor_cutover_sha256_json) AS array(varchar)))
                   AS ancestor_cutover_sha256s
        FROM {catalog}.{schema}.{CUTOVER_TABLE} c
    ) parsed
    WHERE parsed.ancestor_cutover_sha256s IS NOT NULL
      AND json_format(CAST(parsed.ancestor_cutover_sha256s AS JSON))
          = parsed.ancestor_cutover_sha256_json
      AND lower(to_hex(sha256(to_utf8(parsed.ancestor_cutover_sha256_json))))
          = parsed.ancestor_lineage_sha256
      AND regexp_like(parsed.cutover_sha256, '^[0-9a-f]{{64}}$')
      AND regexp_like(parsed.ancestor_lineage_sha256, '^[0-9a-f]{{64}}$')
      AND all_match(
          parsed.ancestor_cutover_sha256s,
          ancestor -> regexp_like(ancestor, '^[0-9a-f]{{64}}$')
      )
      AND cardinality(array_distinct(parsed.ancestor_cutover_sha256s))
          = cardinality(parsed.ancestor_cutover_sha256s)
      AND (
          (
              parsed.predecessor_cutover_id IS NULL
              AND parsed.predecessor_cutover_sha256 IS NULL
              AND cardinality(parsed.ancestor_cutover_sha256s) = 0
          )
          OR (
              parsed.predecessor_cutover_id IS NOT NULL
              AND parsed.predecessor_cutover_sha256 IS NOT NULL
              AND regexp_like(
                  parsed.predecessor_cutover_sha256,
                  '^[0-9a-f]{{64}}$'
              )
              AND cardinality(parsed.ancestor_cutover_sha256s) > 0
              AND element_at(parsed.ancestor_cutover_sha256s, -1)
                  = parsed.predecessor_cutover_sha256
          )
      )
), lineage_valid_cutovers AS (
    SELECT child.*
    FROM cutover_records child
    WHERE (
        child.predecessor_cutover_id IS NULL
        AND child.predecessor_cutover_sha256 IS NULL
        AND cardinality(child.ancestor_cutover_sha256s) = 0
    ) OR EXISTS (
        SELECT 1
        FROM cutover_records parent
        WHERE parent.scope_id = child.scope_id
          AND parent.cutover_id = child.predecessor_cutover_id
          AND parent.cutover_sha256 = child.predecessor_cutover_sha256
          AND child.ancestor_cutover_sha256s =
              CONCAT(parent.ancestor_cutover_sha256s, ARRAY[parent.cutover_sha256])
    )
), invalid_lineage_hashes AS (
    SELECT cutover_sha256 FROM cutover_records
    EXCEPT
    SELECT cutover_sha256 FROM lineage_valid_cutovers
), conflicting_cutover_ids AS (
    SELECT cutover_id
    FROM {catalog}.{schema}.{CUTOVER_TABLE}
    GROUP BY cutover_id
    HAVING COUNT(DISTINCT cutover_sha256) > 1
), conflicting_cutover_predecessors AS (
    SELECT scope_id, predecessor_cutover_sha256
    FROM {catalog}.{schema}.{CUTOVER_TABLE}
    GROUP BY scope_id, predecessor_cutover_sha256
    HAVING COUNT(DISTINCT cutover_sha256) > 1
), bad_cutover_hashes AS (
    SELECT DISTINCT c.cutover_sha256
    FROM {catalog}.{schema}.{CUTOVER_TABLE} c
    JOIN conflicting_cutover_ids conflict
      ON conflict.cutover_id = c.cutover_id
    UNION
    SELECT DISTINCT c.cutover_sha256
    FROM {catalog}.{schema}.{CUTOVER_TABLE} c
    JOIN conflicting_cutover_predecessors fork
      ON fork.scope_id = c.scope_id
     AND fork.predecessor_cutover_sha256 IS NOT DISTINCT FROM c.predecessor_cutover_sha256
    UNION
    SELECT cutover_sha256 FROM invalid_lineage_hashes
), eligible_cutovers AS (
    SELECT c.*
    FROM lineage_valid_cutovers c
    WHERE NOT EXISTS (
        SELECT 1
        FROM bad_cutover_hashes bad
        WHERE bad.cutover_sha256 = c.cutover_sha256
           OR CONTAINS(c.ancestor_cutover_sha256s, bad.cutover_sha256)
    )
), ranked_cutovers AS (
    SELECT c.*,
           ROW_NUMBER() OVER (
               PARTITION BY scope_id
               ORDER BY effective_at DESC, cutover_id DESC, cutover_sha256 DESC
           ) AS rn
    FROM eligible_cutovers c
), latest_cutover AS (
    SELECT * FROM ranked_cutovers WHERE rn = 1
), native_ready AS (
    SELECT c.*
    FROM latest_cutover c
    WHERE c.active_source = 'native'
      AND (
          (
              c.previous_source = 'legacy'
              AND c.legacy_league IS NOT NULL
              AND c.legacy_season IS NOT NULL
          )
          OR (
              c.previous_source = 'absent'
              AND c.legacy_league IS NULL
              AND c.legacy_season IS NULL
          )
      )
      AND EXISTS (
          SELECT 1
          FROM validated_complete ready_manifest
          WHERE ready_manifest.scope_id = c.scope_id
            AND ready_manifest.generation_id = c.native_generation_id
            AND ready_manifest.generation_signature = c.native_generation_signature
            AND ready_manifest.manifest_sha256 = c.native_manifest_sha256
            AND ready_manifest.registry_signature = c.registry_signature
      )
), native_rows AS (
    SELECT
        {native_projection}
    FROM {entity}_rows g
    JOIN latest_validated m
      ON {join}
    JOIN native_ready c ON c.scope_id = m.scope_id
), legacy_rows AS (
    SELECT
        {legacy_projection}
    FROM {catalog}.{schema}.espn_{entity} l
    WHERE NOT EXISTS (
        SELECT 1 FROM native_ready c
        WHERE c.legacy_league IS NOT NULL
          AND c.legacy_season IS NOT NULL
          AND c.legacy_league = l.league
          AND c.legacy_season = CAST(l.season AS varchar)
    )
)
SELECT * FROM native_rows
UNION ALL
SELECT * FROM legacy_rows"""


__all__ = [
    "BASELINE_TABLE",
    "BatchPublicationResult",
    "CATALOG_TABLE",
    "CatalogSnapshot",
    "CURRENT_VIEWS",
    "CURRENT_MANIFEST_ORDER_FIELDS",
    "CUTOVER_TABLE",
    "ENTITY_TABLES",
    "EspnBronzeRepository",
    "LEDGER_TABLE",
    "MANIFEST_TABLE",
    "ManifestConflictError",
    "PROVENANCE_COLUMNS",
    "PublicationError",
    "RawLedgerRecord",
    "REPOSITORY_VERSION",
    "ScopeGeneration",
    "ScopeCutover",
    "ScopePublicationResult",
    "ScopePublicationState",
    "ScopeQualityReport",
    "TABLE_PARTITIONS",
    "build_catalog_snapshot",
    "canonical_json",
    "canonical_sha256",
    "ledger_row_fingerprint",
    "render_current_view_sql",
    "render_repository_ddl",
    "row_fingerprint",
    "select_current_manifest",
    "validate_scope_generation",
]
