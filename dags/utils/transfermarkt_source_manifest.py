"""Immutable raw-backed manifests for Transfermarkt Bronze capture."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
import hashlib
import json
import re
from typing import Any, Iterable

from utils.transfermarkt_source import SOURCE_ENTITIES


CAPTURE_REVISION = 'raw-v1'
TERMINAL_STATUSES = frozenset({'ok', 'authoritative_empty', 'not_applicable'})
_SHA256 = re.compile(r'^[0-9a-f]{64}$')
SCOPE_PLAYER_CAPTURE_KEY = (
    'cycle_id', 'scope_id', 'competition_id', 'edition_id', 'club_id',
    'player_id',
)
SCOPE_PLAYER_CAPTURE_COLUMNS = (
    *SCOPE_PLAYER_CAPTURE_KEY,
    'raw_capture_id',
)


class SourceManifestError(ValueError):
    """Raw or typed evidence is incomplete or internally inconsistent."""


def stable_hash(value: Any) -> str:
    raw = json.dumps(
        value,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=False,
        default=str,
    ).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()


def _required(value: Any, field: str) -> str:
    result = str(value or '').strip()
    if not result:
        raise SourceManifestError(f'{field} is required')
    return result


def raw_payload_set_id(capture_ids: Iterable[str]) -> tuple[str, tuple[str, ...]]:
    values = tuple(sorted({_required(item, 'raw capture id') for item in capture_ids}))
    if not values or any(not _SHA256.fullmatch(item) for item in values):
        raise SourceManifestError('raw capture ids must be lowercase sha256 values')
    return stable_hash(values), values


@dataclass(frozen=True, order=True)
class ScopePlayerCapture:
    cycle_id: str
    scope_id: str
    competition_id: str
    edition_id: str
    club_id: str
    player_id: str
    raw_capture_id: str

    def __post_init__(self) -> None:
        for field in SCOPE_PLAYER_CAPTURE_KEY:
            object.__setattr__(self, field, _required(getattr(self, field), field))
        raw_capture_id = str(self.raw_capture_id or '').strip()
        if not _SHA256.fullmatch(raw_capture_id):
            raise SourceManifestError('raw_capture_id must be lowercase sha256')
        object.__setattr__(self, 'raw_capture_id', raw_capture_id)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> 'ScopePlayerCapture':
        """Parse one persisted or DataFrame-produced capture row strictly."""

        if not isinstance(value, Mapping):
            raise SourceManifestError('scope-player capture row must be a mapping')
        missing = [field for field in SCOPE_PLAYER_CAPTURE_COLUMNS if field not in value]
        if missing:
            raise SourceManifestError(
                f'scope-player capture row is missing fields: {missing}'
            )
        return cls(**{
            field: value[field] for field in SCOPE_PLAYER_CAPTURE_COLUMNS
        })

    @property
    def natural_key(self) -> tuple[str, ...]:
        return tuple(getattr(self, field) for field in SCOPE_PLAYER_CAPTURE_KEY)

    def as_dict(self) -> dict[str, str]:
        return asdict(self)


def scope_player_capture_rows(
    value: Any,
) -> tuple[ScopePlayerCapture, ...]:
    """Canonicalize mappings/DataFrames into immutable typed evidence rows."""

    if hasattr(value, 'to_dict'):
        raw_rows = value.to_dict(orient='records')
    else:
        raw_rows = value
    if isinstance(raw_rows, Mapping) or not isinstance(raw_rows, Iterable):
        raise SourceManifestError('scope-player capture rows must be iterable')

    rows = (
        item
        if isinstance(item, ScopePlayerCapture)
        else ScopePlayerCapture.from_mapping(item)
        for item in raw_rows
    )
    indexed: dict[tuple[str, ...], ScopePlayerCapture] = {}
    for row in rows:
        existing = indexed.get(row.natural_key)
        if existing is not None and existing != row:
            raise SourceManifestError(
                f'scope-player capture conflicts at {row.natural_key}'
            )
        indexed[row.natural_key] = row
    return tuple(indexed[key] for key in sorted(indexed))


def scope_player_evidence(
    rows: Any,
) -> tuple[int, str, tuple[ScopePlayerCapture, ...]]:
    ordered = scope_player_capture_rows(rows)
    if not ordered:
        raise SourceManifestError('scope-player evidence must not be empty')
    return len(ordered), stable_hash([item.as_dict() for item in ordered]), ordered


def scope_player_capture_evidence(value: Any) -> dict[str, Any]:
    """Return the exact count/hash embedded into the source scope manifest."""

    row_count, key_hash, _ = scope_player_evidence(value)
    return {'row_count': row_count, 'key_hash': key_hash}


@dataclass(frozen=True)
class EntityEvidence:
    entity: str
    status: str
    row_count: int
    natural_key_hash: str
    content_hash: str
    raw_capture_ids: tuple[str, ...]
    pending_count: int = 0

    def validate(self) -> None:
        if self.entity not in SOURCE_ENTITIES:
            raise SourceManifestError(f'unknown source entity: {self.entity}')
        if self.status not in TERMINAL_STATUSES | {'partial'}:
            raise SourceManifestError(f'{self.entity}: invalid status {self.status!r}')
        if self.row_count < 0 or self.pending_count < 0:
            raise SourceManifestError(f'{self.entity}: negative row/pending count')
        if self.status == 'partial' and self.pending_count <= 0:
            raise SourceManifestError(f'{self.entity}: partial status has no debt')
        if self.status in TERMINAL_STATUSES and self.pending_count:
            raise SourceManifestError(f'{self.entity}: terminal status has debt')
        if self.status in {'authoritative_empty', 'not_applicable'} and self.row_count:
            raise SourceManifestError(f'{self.entity}: empty status contains rows')
        for field, digest in (
            ('natural_key_hash', self.natural_key_hash),
            ('content_hash', self.content_hash),
        ):
            if not _SHA256.fullmatch(str(digest)):
                raise SourceManifestError(f'{self.entity}: {field} is invalid')
        raw_payload_set_id(self.raw_capture_ids)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ScopeManifest:
    parent_cycle_id: str
    child_cycle_id: str
    scope_id: str
    competition_id: str
    edition_id: str
    registry_snapshot_id: str
    parser_revision: str
    schema_revision: str
    entities: tuple[EntityEvidence, ...]
    raw_payload_set_id: str
    raw_response_count: int
    player_capture_count: int
    player_capture_hash: str
    capture_revision: str = CAPTURE_REVISION

    def validate(self) -> None:
        for field in (
            'parent_cycle_id', 'child_cycle_id', 'scope_id', 'competition_id',
            'edition_id', 'registry_snapshot_id', 'parser_revision',
            'schema_revision', 'capture_revision',
        ):
            _required(getattr(self, field), field)
        names = [item.entity for item in self.entities]
        if set(names) != set(SOURCE_ENTITIES) or len(names) != len(set(names)):
            raise SourceManifestError(
                f'entity set differs from source contract: {sorted(names)}'
            )
        for item in self.entities:
            item.validate()
        captures = tuple(
            capture
            for item in self.entities
            for capture in item.raw_capture_ids
        )
        expected_set, unique = raw_payload_set_id(captures)
        if self.raw_payload_set_id != expected_set:
            raise SourceManifestError('scope raw payload-set hash drift')
        if self.raw_response_count != len(unique):
            raise SourceManifestError('scope raw response count drift')
        if self.player_capture_count <= 0 or not _SHA256.fullmatch(
            str(self.player_capture_hash)
        ):
            raise SourceManifestError('scope-player evidence is invalid')

    @property
    def complete(self) -> bool:
        return all(item.status in TERMINAL_STATUSES for item in self.entities)

    @property
    def pending_count(self) -> int:
        return sum(item.pending_count for item in self.entities)

    @property
    def digest(self) -> str:
        return stable_hash(asdict(self))

    def as_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            **asdict(self),
            'manifest_digest': self.digest,
            'status': 'complete' if self.complete else 'partial',
            'pending_count': self.pending_count,
        }
