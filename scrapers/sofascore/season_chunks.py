"""Pure immutable values for signed, bounded season schedule portions."""

from __future__ import annotations

import base64
import binascii
import json
import re
from dataclasses import asdict, dataclass
from typing import Optional


SEASON_SCHEDULE_CHUNK_PAGE_LIMIT = 50
_UNIT_PREFIX = "season-chunk-v1."
_FIELDS = frozenset({"tournament_id", "season_id", "last", "next", "evidence_digest"})
_SOURCE_ID = re.compile(r"[1-9][0-9]*\Z")
_LEGACY_UNIT = re.compile(r"[1-9][0-9]*:[1-9][0-9]*\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_ENCODED = re.compile(r"[A-Za-z0-9_-]+\Z")


class SeasonChunkError(ValueError):
    """A season unit cannot authorize a bounded schedule portion."""


@dataclass(frozen=True)
class SeasonScheduleChunk:
    tournament_id: str
    season_id: str
    last: tuple[int, int]
    next: tuple[int, int]
    evidence_digest: str

    def __post_init__(self) -> None:
        for name in ("tournament_id", "season_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not _SOURCE_ID.fullmatch(value):
                raise SeasonChunkError(f"chunk {name} must be a canonical positive ID")
        size = 0
        for name in ("last", "next"):
            bounds = getattr(self, name)
            if (
                not isinstance(bounds, tuple)
                or len(bounds) != 2
                or any(type(value) is not int for value in bounds)
                or not 0 <= bounds[0] <= bounds[1]
            ):
                raise SeasonChunkError(f"chunk {name} must be non-negative half-open integer bounds")
            size += bounds[1] - bounds[0]
        if size > SEASON_SCHEDULE_CHUNK_PAGE_LIMIT:
            raise SeasonChunkError("chunk schedule ranges exceed the 50-page limit")
        if not isinstance(self.evidence_digest, str) or not _DIGEST.fullmatch(self.evidence_digest):
            raise SeasonChunkError("chunk evidence_digest must be a lowercase SHA-256 digest")


def encode_season_chunk_unit(chunk: SeasonScheduleChunk) -> str:
    """Encode one canonical value inside the existing signed unit envelope."""
    if not isinstance(chunk, SeasonScheduleChunk):
        raise SeasonChunkError("season chunk must be a SeasonScheduleChunk")
    body = json.dumps(asdict(chunk), sort_keys=True, separators=(",", ":"), allow_nan=False)
    encoded = base64.urlsafe_b64encode(body.encode("utf-8")).decode("ascii").rstrip("=")
    return _UNIT_PREFIX + encoded


def decode_season_chunk_unit(unit: str) -> Optional[SeasonScheduleChunk]:
    """Return None only for an exact legacy tournament:season identity.

    New values must have the one canonical wire representation. Comparing the
    encoded roundtrip also rejects duplicate JSON keys and alternate encodings.
    """
    if not isinstance(unit, str):
        raise SeasonChunkError("season unit must be text")
    if _LEGACY_UNIT.fullmatch(unit):
        return None
    if not unit.startswith(_UNIT_PREFIX):
        raise SeasonChunkError("unsupported season chunk unit version or identity")
    encoded = unit[len(_UNIT_PREFIX):]
    if not _ENCODED.fullmatch(encoded):
        raise SeasonChunkError("season chunk unit has invalid base64 encoding")
    try:
        body = base64.b64decode(encoded + "=" * (-len(encoded) % 4), altchars=b"-_", validate=True)
        payload = json.loads(body.decode("utf-8"))
    except (binascii.Error, ValueError, UnicodeDecodeError) as exc:
        raise SeasonChunkError("season chunk unit is corrupt") from exc
    if not isinstance(payload, dict) or set(payload) != _FIELDS:
        raise SeasonChunkError("season chunk unit has invalid fields")
    for direction in ("last", "next"):
        if not isinstance(payload[direction], list):
            raise SeasonChunkError(f"chunk {direction} bounds must be an array")
        payload[direction] = tuple(payload[direction])
    chunk = SeasonScheduleChunk(**payload)
    if encode_season_chunk_unit(chunk) != unit:
        raise SeasonChunkError("season chunk unit is not canonically encoded")
    return chunk
