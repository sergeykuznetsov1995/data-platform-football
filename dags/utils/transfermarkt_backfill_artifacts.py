"""Immutable object-store artifacts for Transfermarkt backfill control data.

The HTTP raw store already owns the dedicated, credential-free filesystem
configuration.  Backfill reports deliberately reuse that filesystem while
keeping their namespace separate from response bodies and attempt envelopes.
Every object is content addressed and verified after publication.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from scrapers.transfermarkt.raw_store import (
    RawCaptureConflict,
    RawResponseStore,
)


_SAFE_KIND = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class BackfillArtifactError(RuntimeError):
    """An immutable backfill artifact could not be published or verified."""


@dataclass(frozen=True)
class BackfillArtifact:
    uri: str
    sha256: str
    size_bytes: int


class BackfillArtifactStore:
    """Content-addressed JSON/file evidence under the raw-store root."""

    def __init__(self, raw_store: RawResponseStore) -> None:
        if not isinstance(raw_store, RawResponseStore):
            raise TypeError("raw_store must be a RawResponseStore")
        self._raw_store = raw_store

    @classmethod
    def from_env(cls) -> "BackfillArtifactStore":
        store = RawResponseStore.from_env()
        assert store is not None
        return cls(store)

    @staticmethod
    def _key(*, kind: str, owner_id: str, digest: str) -> str:
        if _SAFE_KIND.fullmatch(kind) is None:
            raise ValueError("artifact kind must be a safe lower-case token")
        if _SHA256.fullmatch(owner_id) is None:
            raise ValueError("artifact owner_id must be a sha256")
        if _SHA256.fullmatch(digest) is None:
            raise ValueError("artifact digest must be a sha256")
        return f"backfill/v1/{kind}/{owner_id}/{digest}.json"

    def publish_bytes(
        self,
        payload: bytes,
        *,
        kind: str,
        owner_id: str,
    ) -> BackfillArtifact:
        if not isinstance(payload, bytes) or not payload:
            raise ValueError("artifact payload must be non-empty bytes")
        digest = hashlib.sha256(payload).hexdigest()
        key = self._key(kind=kind, owner_id=owner_id, digest=digest)
        try:
            committed = self._raw_store._publish_immutable_bytes(  # noqa: SLF001
                key,
                payload,
                require_exact_bytes=True,
            )
        except RawCaptureConflict as exc:
            raise BackfillArtifactError(
                f"immutable backfill artifact conflict: {digest}"
            ) from exc
        if hashlib.sha256(committed).hexdigest() != digest:
            raise BackfillArtifactError("backfill artifact readback hash drift")
        return BackfillArtifact(
            uri=self._raw_store._uri(key),  # noqa: SLF001
            sha256=digest,
            size_bytes=len(committed),
        )

    def publish_json(
        self,
        payload: Mapping[str, Any],
        *,
        kind: str,
        owner_id: str,
    ) -> BackfillArtifact:
        if not isinstance(payload, Mapping):
            raise TypeError("artifact JSON payload must be an object")
        rendered = (
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
            + b"\n"
        )
        return self.publish_bytes(rendered, kind=kind, owner_id=owner_id)

    def publish_file(
        self,
        path: str | Path,
        *,
        kind: str,
        owner_id: str,
    ) -> BackfillArtifact:
        candidate = Path(path)
        if not candidate.is_absolute() or not candidate.is_file():
            raise BackfillArtifactError(f"artifact file is missing: {path}")
        try:
            payload = candidate.read_bytes()
        except OSError as exc:
            raise BackfillArtifactError(f"artifact file is unreadable: {path}") from exc
        return self.publish_bytes(payload, kind=kind, owner_id=owner_id)

    def load_bytes(self, uri: str, *, expected_sha256: str) -> bytes:
        """Read an artifact through the configured store and verify its digest."""

        if _SHA256.fullmatch(expected_sha256) is None:
            raise ValueError("expected artifact digest must be a sha256")
        prefix = f"{self._raw_store.uri_prefix}/"
        if not isinstance(uri, str) or not uri.startswith(prefix):
            raise BackfillArtifactError("artifact URI is outside the raw store")
        key = uri[len(prefix):]
        if not key.startswith("backfill/v1/") or "/../" in f"/{key}/":
            raise BackfillArtifactError("artifact URI is outside the backfill namespace")
        try:
            payload = self._raw_store._read_bytes(key)  # noqa: SLF001
        except Exception as exc:  # filesystem adapters expose backend errors
            raise BackfillArtifactError("backfill artifact cannot be read") from exc
        if hashlib.sha256(payload).hexdigest() != expected_sha256:
            raise BackfillArtifactError("backfill artifact readback hash drift")
        return payload

    def load_json(self, uri: str, *, expected_sha256: str) -> dict[str, Any]:
        payload = self.load_bytes(uri, expected_sha256=expected_sha256)
        try:
            value = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BackfillArtifactError("backfill artifact JSON is unreadable") from exc
        if not isinstance(value, dict):
            raise BackfillArtifactError("backfill artifact JSON must be an object")
        return value


__all__ = [
    "BackfillArtifact",
    "BackfillArtifactError",
    "BackfillArtifactStore",
]
