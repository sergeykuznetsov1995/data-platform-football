"""Canonical endpoint-level manifest contract for SofaScore capture.

The table adapter used by Airflow can implement :class:`ManifestStore`; the
engine itself deliberately depends only on this narrow interface.  This keeps
CLI, DAG and backfill capture on the same state machine without coupling the
raw/replay logic to Trino.
"""

from __future__ import annotations

import fcntl
import json
import os
import threading
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Protocol


MANIFEST_VERSION = "sofascore-endpoint-v1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _required(value: object, name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


class ManifestStatus(str, Enum):
    SUCCESS = "success"
    LEGITIMATE_EMPTY = "legitimate_empty"
    NOT_SUPPORTED = "not_supported"
    RETRYABLE_FAILURE = "retryable_failure"
    SCHEMA_ERROR = "schema_error"

    @property
    def terminal(self) -> bool:
        return self in {
            ManifestStatus.SUCCESS,
            ManifestStatus.LEGITIMATE_EMPTY,
            ManifestStatus.NOT_SUPPORTED,
        }


@dataclass(frozen=True, order=True)
class ManifestKey:
    source_tournament_id: str
    source_season_id: str
    target_type: str
    target_id: str
    endpoint: str
    freshness_key: str

    def __post_init__(self) -> None:
        for name in (
            "source_tournament_id",
            "source_season_id",
            "target_type",
            "target_id",
            "endpoint",
            "freshness_key",
        ):
            object.__setattr__(self, name, _required(getattr(self, name), name))

    def as_tuple(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.source_tournament_id,
            self.source_season_id,
            self.target_type,
            self.target_id,
            self.endpoint,
            self.freshness_key,
        )

    def stable_id(self) -> str:
        # JSON is collision-free for arbitrary strings and stable across runtimes.
        return json.dumps(self.as_tuple(), ensure_ascii=False, separators=(",", ":"))


@dataclass(frozen=True)
class EndpointManifest:
    key: ManifestKey
    status: ManifestStatus
    run_id: str
    task_id: str
    attempts: int
    row_count: int
    http_status: Optional[int] = None
    raw_content_hash: Optional[str] = None
    raw_blob_key: Optional[str] = None
    request_url: Optional[str] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    duration_ms: int = 0
    provider_bytes: int = 0
    fetched_at: Optional[str] = None
    parsed_at: Optional[str] = None
    updated_at: str = ""
    manifest_version: str = MANIFEST_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.status, ManifestStatus):
            object.__setattr__(self, "status", ManifestStatus(self.status))
        object.__setattr__(self, "run_id", _required(self.run_id, "run_id"))
        object.__setattr__(self, "task_id", _required(self.task_id, "task_id"))
        if self.attempts < 0 or self.row_count < 0:
            raise ValueError("attempts and row_count must be non-negative")
        if self.duration_ms < 0 or self.provider_bytes < 0:
            raise ValueError("duration_ms and provider_bytes must be non-negative")
        if self.manifest_version != MANIFEST_VERSION:
            raise ValueError(f"Unsupported manifest version: {self.manifest_version}")
        if not self.updated_at:
            object.__setattr__(self, "updated_at", utc_now_iso())
        status = self.http_status
        if status in {403, 429} or (status is not None and status >= 500):
            if self.status != ManifestStatus.RETRYABLE_FAILURE:
                raise ValueError(f"HTTP {status} may only be retryable_failure")
        if self.status == ManifestStatus.SUCCESS:
            if status is None or not 200 <= status < 300 or status in {204}:
                raise ValueError("success requires a non-empty 2xx HTTP response")
            if self.row_count <= 0:
                raise ValueError("success requires row_count > 0")
        if self.status in {
            ManifestStatus.SUCCESS,
            ManifestStatus.LEGITIMATE_EMPTY,
            ManifestStatus.SCHEMA_ERROR,
        } and (not self.raw_content_hash or not self.raw_blob_key):
            raise ValueError(f"{self.status.value} requires committed raw lineage")

    @property
    def is_terminal(self) -> bool:
        return self.status.terminal

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["status"] = self.status.value
        payload["key"] = asdict(self.key)
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "EndpointManifest":
        values = dict(payload)
        values["key"] = ManifestKey(**dict(values["key"]))
        values["status"] = ManifestStatus(str(values["status"]))
        return cls(**values)


class ManifestStore(Protocol):
    def get(self, key: ManifestKey) -> Optional[EndpointManifest]: ...

    def upsert(self, record: EndpointManifest) -> None: ...

    def list_for_run(self, run_id: str) -> Iterable[EndpointManifest]: ...


class InMemoryManifestStore:
    """Thread-safe reference adapter used by unit tests and local replay."""

    def __init__(self, records: Iterable[EndpointManifest] = ()) -> None:
        self._records: Dict[ManifestKey, EndpointManifest] = {
            record.key: record for record in records
        }
        self._lock = threading.RLock()

    def get(self, key: ManifestKey) -> Optional[EndpointManifest]:
        with self._lock:
            return self._records.get(key)

    def upsert(self, record: EndpointManifest) -> None:
        with self._lock:
            self._records[record.key] = record

    def list_for_run(self, run_id: str) -> list[EndpointManifest]:
        with self._lock:
            return [r for r in self._records.values() if r.run_id == run_id]


class JsonFileManifestStore:
    """Atomic cross-process adapter for CLI/smoke runs.

    Production Iceberg writers implement the same three methods.  The lock file
    is stable (unlike the replaced JSON inode), so concurrent tasks cannot lose
    each other's endpoint commits.
    """

    def __init__(self, path: os.PathLike[str] | str) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")

    def _read_unlocked(self) -> Dict[str, dict]:
        if not self.path.exists():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Corrupt endpoint manifest: {self.path}") from exc
        if payload.get("manifest_version") != MANIFEST_VERSION:
            raise RuntimeError(f"Unsupported endpoint manifest: {self.path}")
        records = payload.get("records")
        if not isinstance(records, dict):
            raise RuntimeError(f"Invalid endpoint manifest records: {self.path}")
        return records

    def _locked(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        return handle

    def get(self, key: ManifestKey) -> Optional[EndpointManifest]:
        handle = self._locked()
        try:
            payload = self._read_unlocked().get(key.stable_id())
            return EndpointManifest.from_dict(payload) if payload else None
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def upsert(self, record: EndpointManifest) -> None:
        handle = self._locked()
        try:
            records = self._read_unlocked()
            records[record.key.stable_id()] = record.to_dict()
            rendered = json.dumps(
                {"manifest_version": MANIFEST_VERSION, "records": records},
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ) + "\n"
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_name(
                f"{self.path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
            )
            try:
                temporary.write_text(rendered, encoding="utf-8")
                os.replace(temporary, self.path)
            finally:
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def list_for_run(self, run_id: str) -> list[EndpointManifest]:
        handle = self._locked()
        try:
            return [
                record
                for record in map(EndpointManifest.from_dict, self._read_unlocked().values())
                if record.run_id == run_id
            ]
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
