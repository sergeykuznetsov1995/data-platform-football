"""One canonical logical order for ESPN COMPLETE manifests and control heads."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping, TypeVar


CURRENT_MANIFEST_ORDER_FIELDS = (
    "completed_at",
    "generation_id",
    "manifest_sha256",
)

_T = TypeVar("_T")


def _field(value: object, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def current_manifest_order_key(value: object) -> tuple[datetime, str, str]:
    """Return the fail-closed total-order key used by SQL and Python."""

    completed_at = _field(value, "completed_at")
    generation_id = _field(value, "generation_id")
    manifest_sha256 = _field(value, "manifest_sha256")
    if not isinstance(completed_at, datetime) or completed_at.tzinfo is None:
        raise ValueError("completed_at must be a timezone-aware datetime")
    if not isinstance(generation_id, str) or not generation_id:
        raise ValueError("generation_id must be non-empty")
    if not isinstance(manifest_sha256, str) or not manifest_sha256:
        raise ValueError("manifest_sha256 must be non-empty")
    return completed_at, generation_id, manifest_sha256


def select_current_manifest(rows: Iterable[_T]) -> _T:
    """Select exactly the row that production current views rank first."""

    candidates = tuple(rows)
    if not candidates:
        raise ValueError("cannot select a current manifest from an empty set")
    return max(candidates, key=current_manifest_order_key)


__all__ = [
    "CURRENT_MANIFEST_ORDER_FIELDS",
    "current_manifest_order_key",
    "select_current_manifest",
]
