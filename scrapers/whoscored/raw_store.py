"""Durable raw-first storage for WhoScored source responses.

Every successful response is committed as a deterministic gzip blob before a
parser or Iceberg writer sees it.  Target manifests point at immutable,
content-addressed blobs, so parser retries and sink retries require no source
traffic.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import math
import os
import re
import fcntl
import tempfile
import time
import zlib
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Callable, Iterator, Mapping, Optional, TypeVar
from urllib.parse import urlparse

from pyarrow import fs

from .domain import WhoScoredScope

logger = logging.getLogger(__name__)

RAW_MANIFEST_VERSION = "whoscored-raw-v1"
RAW_RECEIPT_VERSION = "whoscored-target-receipt-v2"
FETCHER_VERSION = "whoscored-transport-v3"
_POSITIVE_ID_RE = re.compile(r"^[1-9][0-9]*$")
_OBSERVED_AT_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})$"
)
_T = TypeVar("_T")


class RawStoreError(RuntimeError):
    """Base error for raw storage failures."""


class RawObjectNotFound(RawStoreError):
    """The target has no committed manifest or its blob is absent."""


class RawObjectCorrupt(RawObjectNotFound):
    """A target manifest or compressed blob violates its integrity contract."""


class RawTargetLockTimeout(RawStoreError):
    """Another local worker did not finish the same raw target in time."""


class RawSnapshotLockTimeout(RawStoreError):
    """A writer or snapshot could not acquire the LocalExecutor barrier."""


class _RawWriteVerificationError(OSError):
    """A provider did not make the bytes just written durably observable."""


@dataclass(frozen=True)
class RawTarget:
    source: str
    page_kind: str
    target_id: str
    canonical_url: str
    source_ids: Mapping[str, str]

    def __post_init__(self) -> None:
        if self.source != "whoscored":
            raise ValueError("WhoScored raw targets must use source='whoscored'")
        if not self.page_kind or not self.target_id or not self.canonical_url:
            raise ValueError("Raw target kind, id and canonical URL are required")
        parsed = urlparse(self.canonical_url)
        if parsed.scheme != "https" or parsed.netloc.lower() not in {
            "whoscored.com",
            "www.whoscored.com",
        }:
            raise ValueError(f"Not a canonical WhoScored URL: {self.canonical_url!r}")


@dataclass(frozen=True)
class RawObjectRecord:
    manifest_version: str
    source: str
    page_kind: str
    target_id: str
    canonical_url: str
    source_ids: Mapping[str, str]
    content_hash: str
    hash_algorithm: str
    blob_key: str
    compression: str
    content_type: str
    charset: Optional[str]
    fetched_at: str
    fetcher_version: str
    decoded_bytes: int
    encoded_bytes: int


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _positive_id(value: str | int, field: str) -> str:
    token = str(value).strip()
    if not _POSITIVE_ID_RE.fullmatch(token):
        raise ValueError(f"{field} must be a positive integer, got {value!r}")
    return token


def match_page_target(game_id: str | int) -> RawTarget:
    game = _positive_id(game_id, "game_id")
    return RawTarget(
        source="whoscored",
        page_kind="match",
        target_id=f"whoscored:match:{game}",
        canonical_url=f"https://www.whoscored.com/Matches/{game}/Live",
        source_ids={"game_id": game},
    )


def preview_page_target(game_id: str | int) -> RawTarget:
    game = _positive_id(game_id, "game_id")
    return RawTarget(
        source="whoscored",
        page_kind="preview",
        target_id=f"whoscored:preview:{game}",
        canonical_url=f"https://www.whoscored.com/Matches/{game}/Preview",
        source_ids={"game_id": game},
    )


def profile_page_target(player_id: str | int) -> RawTarget:
    player = _positive_id(player_id, "player_id")
    return RawTarget(
        source="whoscored",
        page_kind="profile",
        target_id=f"whoscored:profile:{player}",
        canonical_url=f"https://www.whoscored.com/Players/{player}/Show",
        source_ids={"player_id": player},
    )


def schedule_month_target(stage_id: str | int, year: int, month: int) -> RawTarget:
    stage = _positive_id(stage_id, "stage_id")
    if not 1900 <= int(year) <= 2199:
        raise ValueError(f"year must be a four-digit calendar year, got {year!r}")
    if not 1 <= int(month) <= 12:
        raise ValueError(f"month must be in 1..12, got {month!r}")
    month_token = f"{int(year):04d}{int(month):02d}"
    return RawTarget(
        source="whoscored",
        page_kind="schedule_month",
        target_id=f"whoscored:schedule:{stage}:{month_token}",
        canonical_url=(
            f"https://www.whoscored.com/tournaments/{stage}/data/?d={month_token}"
        ),
        source_ids={"stage_id": stage, "month": month_token},
    )


def stage_page_target(
    scope: WhoScoredScope,
    *,
    region_id: str | int,
    tournament_id: str | int,
    source_season_id: str | int,
) -> RawTarget:
    region = _positive_id(region_id, "region_id")
    tournament = _positive_id(tournament_id, "tournament_id")
    source_season = _positive_id(source_season_id, "source_season_id")
    return RawTarget(
        source="whoscored",
        page_kind="season_stages",
        target_id=f"whoscored:stages:{scope.spec}:{source_season}",
        canonical_url=(
            f"https://www.whoscored.com/Regions/{region}/Tournaments/{tournament}"
            f"/Seasons/{source_season}"
        ),
        source_ids={
            "competition_id": scope.competition_id,
            "season_id": scope.season_id,
            "region_id": region,
            "tournament_id": tournament,
            "source_season_id": source_season,
        },
    )


class WhoScoredRawStore:
    """Verified content-addressed blobs plus append-only target receipts.

    New commits never depend on an object-store rename.  A gzip blob is put
    directly at its deterministic hash key and read back before an immutable
    target receipt is published.  Readers select the newest valid receipt and
    fall back to the pre-v2 mutable target manifest layout for compatibility.
    """

    def __init__(self, filesystem: fs.FileSystem, root: str) -> None:
        self.filesystem = filesystem
        self.root = root.strip("/") if not root.startswith("/") else root.rstrip("/")

    @classmethod
    def from_uri(cls, uri: str) -> "WhoScoredRawStore":
        parsed = urlparse(uri)
        if parsed.scheme == "s3":
            if not parsed.netloc:
                raise ValueError("S3 raw-store URI must contain a bucket")
            dedicated_access = os.environ.get(
                "WHOSCORED_RAW_S3_ACCESS_KEY", ""
            ).strip()
            dedicated_secret = os.environ.get(
                "WHOSCORED_RAW_S3_SECRET_KEY", ""
            ).strip()
            if bool(dedicated_access) != bool(dedicated_secret):
                raise RawStoreError(
                    "WHOSCORED_RAW_S3_ACCESS_KEY and "
                    "WHOSCORED_RAW_S3_SECRET_KEY must be set together"
                )
            platform_access = os.environ.get("S3_ACCESS_KEY", "").strip()
            platform_secret = os.environ.get("S3_SECRET_KEY", "").strip()
            if bool(platform_access) != bool(platform_secret):
                raise RawStoreError(
                    "S3_ACCESS_KEY and S3_SECRET_KEY must be set together"
                )
            filesystem = fs.S3FileSystem(
                access_key=dedicated_access or platform_access or None,
                secret_key=dedicated_secret or platform_secret or None,
                endpoint_override=os.environ.get(
                    "WHOSCORED_RAW_S3_ENDPOINT", "seaweedfs:8333"
                ),
                scheme=os.environ.get("WHOSCORED_RAW_S3_SCHEME", "http"),
                region=os.environ.get("WHOSCORED_RAW_S3_REGION", "us-east-1"),
                background_writes=False,
            )
            root = f"{parsed.netloc}/{parsed.path.lstrip('/')}".rstrip("/")
            return cls(filesystem, root)
        filesystem, root = fs.FileSystem.from_uri(uri)
        return cls(filesystem, root.rstrip("/"))

    @classmethod
    def from_env(cls, optional: bool = True) -> Optional["WhoScoredRawStore"]:
        uri = os.environ.get("WHOSCORED_RAW_STORE_URI", "").strip()
        if not uri:
            if optional:
                return None
            raise RawStoreError("WHOSCORED_RAW_STORE_URI is required")
        return cls.from_uri(uri)

    def _path(self, relative: str) -> str:
        return str(PurePosixPath(self.root) / relative)

    def object_uri(self, relative: str) -> str:
        """Return a durable URI/path suitable for Iceberg ingest manifests."""
        path = self._path(relative)
        if isinstance(self.filesystem, fs.S3FileSystem):
            return f"s3://{path.lstrip('/')}"
        return path

    def _exists(self, relative: str) -> bool:
        def operation() -> bool:
            return (
                self.filesystem.get_file_info(self._path(relative)).type
                != fs.FileType.NotFound
            )

        return self._retry_io("stat", relative, operation)

    def _read_bytes(self, relative: str) -> bytes:
        def operation() -> bytes:
            path = self._path(relative)
            info = self.filesystem.get_file_info(path)
            if info.type == fs.FileType.NotFound:
                raise RawObjectNotFound(f"Raw object not found: {relative}")
            with self.filesystem.open_input_file(path) as stream:
                return stream.read()

        return self._retry_io("read", relative, operation)

    @staticmethod
    def _retry_settings() -> tuple[int, float]:
        attempts_raw = os.environ.get("WHOSCORED_RAW_IO_ATTEMPTS", "4")
        delay_raw = os.environ.get("WHOSCORED_RAW_RETRY_BASE_SECONDS", "0.2")
        try:
            attempts = int(attempts_raw)
            base_delay = float(delay_raw)
        except ValueError as exc:
            raise RawStoreError("Invalid WhoScored raw-store retry settings") from exc
        if attempts < 1 or attempts > 10:
            raise RawStoreError("WHOSCORED_RAW_IO_ATTEMPTS must be in 1..10")
        if not math.isfinite(base_delay) or not 0 <= base_delay <= 10:
            raise RawStoreError(
                "WHOSCORED_RAW_RETRY_BASE_SECONDS must be in 0..10"
            )
        return attempts, base_delay

    def _retry_io(
        self,
        action: str,
        relative: str,
        operation: Callable[[], _T],
    ) -> _T:
        attempts, base_delay = self._retry_settings()
        for attempt in range(1, attempts + 1):
            try:
                return operation()
            except RawStoreError:
                raise
            except (OSError, TimeoutError) as exc:
                if attempt == attempts:
                    raise RawStoreError(
                        f"Raw-store {action} failed after {attempts} attempts: "
                        f"{relative}"
                    ) from exc
                delay = base_delay * (2 ** (attempt - 1))
                # Deterministic per-object jitter avoids retry herds while
                # keeping unit/fault tests reproducible.
                jitter = (
                    int(hashlib.sha256(relative.encode("utf-8")).hexdigest()[:4], 16)
                    / 0xFFFF
                )
                time.sleep(delay * (0.75 + 0.5 * jitter))
        raise AssertionError("raw-store retry loop exhausted unexpectedly")

    def _write_bytes(self, relative: str, payload: bytes) -> None:
        """Put final bytes directly and verify the exact object after close.

        S3-compatible providers publish a completed PUT atomically.  There is
        intentionally no copy/delete ``move`` in this commit path.  A retry is
        idempotent because production callers use content-addressed blobs or
        deterministic immutable receipt keys.
        """

        if not isinstance(payload, bytes):
            raise TypeError("Raw-store payload must be bytes")
        expected_hash = hashlib.sha256(payload).hexdigest()

        def operation() -> None:
            path = self._path(relative)
            self.filesystem.create_dir(
                str(PurePosixPath(path).parent), recursive=True
            )
            # Arrow auto-detects .gz; compression=None avoids double
            # compression of already encoded raw blobs.
            with self.filesystem.open_output_stream(
                path, compression=None
            ) as stream:
                stream.write(payload)
            with self.filesystem.open_input_file(path) as stream:
                stored = stream.read()
            if (
                len(stored) != len(payload)
                or hashlib.sha256(stored).hexdigest() != expected_hash
            ):
                raise _RawWriteVerificationError(
                    f"Read-after-write mismatch for {relative}"
                )

        self._retry_io("write/verify", relative, operation)

    def _write_immutable_bytes(self, relative: str, payload: bytes) -> None:
        """Create or idempotently confirm one immutable object."""

        if self._exists(relative):
            if self._read_bytes(relative) != payload:
                raise RawStoreError(f"Raw object is immutable: {relative}")
            return
        self._write_bytes(relative, payload)
        # A competing writer can only use this key for the same deterministic
        # payload.  This final comparison also covers an ambiguous PUT result.
        if self._read_bytes(relative) != payload:
            raise RawStoreError(f"Immutable raw object mismatch: {relative}")

    @contextmanager
    def target_lock(
        self,
        target: RawTarget,
        *,
        timeout_seconds: Optional[float] = None,
    ) -> Iterator[None]:
        """Serialize one raw target across LocalExecutor task processes.

        Production uses Airflow LocalExecutor, so a host-local ``flock`` is a
        singleflight boundary without serializing unrelated targets. The
        durable raw object remains the source of truth and callers re-check it
        only after acquiring this lock.
        """

        configured = os.environ.get("WHOSCORED_RAW_LOCK_TIMEOUT_SECONDS", "55")
        wait_seconds = (
            float(configured) if timeout_seconds is None else float(timeout_seconds)
        )
        if not math.isfinite(wait_seconds) or wait_seconds < 0:
            raise ValueError(
                "raw target lock timeout must be a finite non-negative number"
            )
        root = Path(
            os.environ.get(
                "WHOSCORED_RAW_LOCK_DIR",
                str(Path(tempfile.gettempdir()) / "whoscored_raw_locks"),
            )
        )
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(root, 0o700)
        digest = hashlib.sha256(target.target_id.encode("utf-8")).hexdigest()
        handle = (root / f"{digest}.lock").open("a+b")
        os.fchmod(handle.fileno(), 0o600)
        deadline = time.monotonic() + wait_seconds
        try:
            while True:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError as exc:
                    if time.monotonic() >= deadline:
                        raise RawTargetLockTimeout(
                            f"Timed out waiting for raw target {target.target_id}"
                        ) from exc
                    time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
            yield
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()

    @contextmanager
    def snapshot_lock(
        self,
        *,
        exclusive: bool,
        timeout_seconds: Optional[float] = None,
    ) -> Iterator[None]:
        """Coordinate LocalExecutor writers with one consistent inventory.

        Raw commits take a shared lock; the inventory task takes the exclusive
        form.  The lock identity is derived from the configured raw root, so
        independently constructed source/backup store objects still meet at
        the same host-local barrier.
        """

        configured = os.environ.get(
            "WHOSCORED_RAW_SNAPSHOT_LOCK_TIMEOUT_SECONDS", "300"
        )
        try:
            wait_seconds = (
                float(configured)
                if timeout_seconds is None
                else float(timeout_seconds)
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("raw snapshot lock timeout must be numeric") from exc
        if not math.isfinite(wait_seconds) or wait_seconds < 0:
            raise ValueError(
                "raw snapshot lock timeout must be a finite non-negative number"
            )
        root = Path(
            os.environ.get(
                "WHOSCORED_RAW_LOCK_DIR",
                str(Path(tempfile.gettempdir()) / "whoscored_raw_locks"),
            )
        )
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(root, 0o700)
        identity = hashlib.sha256(self.root.encode("utf-8")).hexdigest()
        handle = (root / f"snapshot-{identity}.lock").open("a+b")
        os.fchmod(handle.fileno(), 0o600)
        mode = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        deadline = time.monotonic() + wait_seconds
        try:
            while True:
                try:
                    fcntl.flock(handle.fileno(), mode | fcntl.LOCK_NB)
                    break
                except BlockingIOError as exc:
                    if time.monotonic() >= deadline:
                        kind = "snapshot" if exclusive else "raw writer"
                        raise RawSnapshotLockTimeout(
                            f"Timed out waiting for {kind} lock on {self.root}"
                        ) from exc
                    time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
            yield
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()

    def _read_json(self, relative: str) -> dict:
        try:
            payload = json.loads(self._read_bytes(relative).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RawObjectCorrupt(f"Invalid JSON manifest: {relative}") from exc
        if not isinstance(payload, dict):
            raise RawObjectCorrupt(f"Manifest is not an object: {relative}")
        return payload

    def _write_json(self, relative: str, payload: Mapping[str, object]) -> None:
        self._write_bytes(relative, self._render_json(payload))

    @staticmethod
    def _render_json(payload: Mapping[str, object]) -> bytes:
        return (
            json.dumps(
                payload,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ).encode("utf-8")
            + b"\n"
        )

    def _write_immutable_json(
        self, relative: str, payload: Mapping[str, object]
    ) -> None:
        self._write_immutable_bytes(relative, self._render_json(payload))

    @staticmethod
    def _target_manifest_key(target: RawTarget) -> str:
        """Return the legacy mutable-manifest key (read compatibility only)."""
        digest = hashlib.sha256(target.target_id.encode("utf-8")).hexdigest()
        return f"targets/{target.page_kind}/{digest}.json"

    @staticmethod
    def _target_receipt_prefix(target: RawTarget) -> str:
        digest = hashlib.sha256(target.target_id.encode("utf-8")).hexdigest()
        return f"target-history-v2/{target.page_kind}/{digest}"

    @classmethod
    def _target_receipt_key(
        cls,
        target: RawTarget,
        record_payload: Mapping[str, object],
    ) -> str:
        rendered = json.dumps(
            record_payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        record_hash = hashlib.sha256(rendered).hexdigest()
        try:
            observed = cls._observation_time(record_payload["fetched_at"])
        except (KeyError, RawStoreError) as exc:
            raise RawStoreError("Raw fetched_at must be an ISO-8601 timestamp") from exc
        observed_utc = observed.astimezone(timezone.utc)
        order_token = (
            f"{observed_utc.year:04d}{observed_utc.month:02d}"
            f"{observed_utc.day:02d}T{observed_utc.hour:02d}"
            f"{observed_utc.minute:02d}{observed_utc.second:02d}"
            f"{observed_utc.microsecond:06d}Z"
        )
        return (
            f"{cls._target_receipt_prefix(target)}/{order_token}-"
            f"{record_hash}.json"
        )

    @staticmethod
    def _receipt_payload(record_payload: Mapping[str, object]) -> dict:
        normalized = json.loads(
            json.dumps(record_payload, ensure_ascii=False, sort_keys=True)
        )
        canonical = json.dumps(
            normalized,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return {
            "receipt_version": RAW_RECEIPT_VERSION,
            "record_sha256": hashlib.sha256(canonical).hexdigest(),
            "record": normalized,
        }

    @staticmethod
    def _observation_time(value: object) -> datetime:
        token = str(value).strip()
        if not _OBSERVED_AT_RE.fullmatch(token):
            raise RawStoreError(
                "Raw fetched_at must use ISO-8601 with at most microsecond precision"
            )
        try:
            observed = datetime.fromisoformat(token.replace("Z", "+00:00"))
        except ValueError as exc:
            raise RawStoreError("Raw fetched_at must be an ISO-8601 timestamp") from exc
        if (
            observed.year < 1000
            or observed.tzinfo is None
            or observed.utcoffset() is None
        ):
            raise RawStoreError(
                "Raw fetched_at must be timezone-aware and use year 1000..9999"
            )
        return observed

    @classmethod
    def _record_order_key(cls, record: RawObjectRecord) -> tuple[datetime, str]:
        return (
            cls._observation_time(record.fetched_at).astimezone(timezone.utc),
            cls._record_sha256(record),
        )

    @staticmethod
    def _record_sha256(record: RawObjectRecord) -> str:
        canonical = json.dumps(
            asdict(record),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    @staticmethod
    def _target_invalidation_key(target: RawTarget, record_sha256: str) -> str:
        digest = hashlib.sha256(target.target_id.encode("utf-8")).hexdigest()
        return (
            f"target-invalidations-v2/{target.page_kind}/{digest}/"
            f"{record_sha256}.json"
        )

    def _relative_path(self, path: str) -> str:
        root = self.root.rstrip("/")
        prefix = f"{root}/" if root else ""
        if prefix and path.startswith(prefix):
            return path[len(prefix) :]
        if not root:
            return path.lstrip("/")
        raise RawObjectCorrupt(f"Raw path escaped configured root: {path}")

    @staticmethod
    def _blob_key(content_hash: str) -> str:
        return f"blobs/sha256/{content_hash[:2]}/{content_hash}.raw.gz"

    def quarantine(
        self,
        target: RawTarget,
        *,
        reason: str,
        record: Optional[RawObjectRecord] = None,
    ) -> Optional[str]:
        with self.snapshot_lock(exclusive=False):
            return self._quarantine_unlocked(target, reason=reason, record=record)

    def _quarantine_unlocked(
        self,
        target: RawTarget,
        *,
        reason: str,
        record: Optional[RawObjectRecord] = None,
    ) -> Optional[str]:
        """Append durable corruption evidence without mutating raw history.

        Content-addressed blobs are deliberately retained: another target may
        reference the same valid source response.  Receipts and legacy
        manifests remain available for forensic inspection; a subsequent
        ``store_bytes`` publishes a newer verified receipt.  Only an explicitly
        supplied record is invalidated.  Re-selecting the current receipt here
        would race with a concurrent healthy writer and could invalidate the
        replacement instead of the record that actually failed validation.
        """
        if not self._exists(self._target_manifest_key(target)) and not self._exists(
            self._target_receipt_prefix(target)
        ):
            return None
        digest = hashlib.sha256(target.target_id.encode("utf-8")).hexdigest()
        quarantined_at = utc_now_iso()
        timestamp = datetime.fromisoformat(quarantined_at).strftime(
            "%Y%m%dT%H%M%S%fZ"
        )
        reason_text = str(reason)[:1000]
        evidence_hash = hashlib.sha256(reason_text.encode("utf-8")).hexdigest()[:16]
        quarantine_key = (
            f"quarantine/targets/{target.page_kind}/{digest}/"
            f"{timestamp}-{evidence_hash}.json"
        )
        try:
            invalidated_sha256 = (
                None if record is None else self._record_sha256(record)
            )
            if invalidated_sha256 is not None:
                self._write_immutable_json(
                    self._target_invalidation_key(target, invalidated_sha256),
                    {
                        "invalidation_version": "whoscored-target-invalidation-v1",
                        "record_sha256": invalidated_sha256,
                        "target_id": target.target_id,
                        "fetched_at": record.fetched_at,
                        "content_hash": record.content_hash,
                    },
                )
            self._write_immutable_json(
                quarantine_key,
                {
                    "quarantined_at": quarantined_at,
                    "target_id": target.target_id,
                    "canonical_url": target.canonical_url,
                    "legacy_manifest_uri": self.object_uri(
                        self._target_manifest_key(target)
                    ),
                    "receipt_prefix_uri": self.object_uri(
                        self._target_receipt_prefix(target)
                    ),
                    "invalidated_record_sha256": invalidated_sha256,
                    "reason": reason_text,
                },
            )
        except Exception:
            logger.exception("Could not quarantine raw target %s", target.target_id)
            return None
        logger.warning(
            "Quarantined corrupt WhoScored raw target %s at %s",
            target.target_id,
            quarantine_key,
        )
        return quarantine_key

    def has(self, target: RawTarget) -> bool:
        try:
            self._load_record(target)
        except (RawObjectNotFound, RawObjectCorrupt):
            return False
        return True

    def _record_from_payload(
        self,
        target: RawTarget,
        payload: Mapping[str, object],
        manifest_key: str,
    ) -> RawObjectRecord:
        try:
            record = RawObjectRecord(**payload)
        except TypeError as exc:
            raise RawObjectCorrupt(f"Invalid raw manifest: {manifest_key}") from exc
        string_fields = (
            "manifest_version",
            "source",
            "page_kind",
            "target_id",
            "canonical_url",
            "content_hash",
            "hash_algorithm",
            "blob_key",
            "compression",
            "content_type",
            "fetched_at",
            "fetcher_version",
        )
        if any(not isinstance(getattr(record, field), str) for field in string_fields):
            raise RawObjectCorrupt(
                f"Invalid field types in raw manifest: {manifest_key}"
            )
        if record.charset is not None and not isinstance(record.charset, str):
            raise RawObjectCorrupt(f"Invalid charset in raw manifest: {manifest_key}")
        if not isinstance(record.source_ids, Mapping) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in record.source_ids.items()
        ):
            raise RawObjectCorrupt(
                f"Invalid source_ids in raw manifest: {manifest_key}"
            )
        if any(
            type(value) is not int or value < 0
            for value in (record.decoded_bytes, record.encoded_bytes)
        ):
            raise RawObjectCorrupt(
                f"Invalid byte counts in raw manifest: {manifest_key}"
            )
        if record.manifest_version != RAW_MANIFEST_VERSION:
            raise RawObjectCorrupt(
                f"Unsupported manifest version {record.manifest_version!r}"
            )
        try:
            self._observation_time(record.fetched_at)
        except RawStoreError as exc:
            raise RawObjectCorrupt(
                f"Invalid fetched_at in raw manifest: {manifest_key}"
            ) from exc
        if (
            record.source != target.source
            or record.page_kind != target.page_kind
            or record.target_id != target.target_id
            or record.canonical_url != target.canonical_url
            or dict(record.source_ids) != dict(target.source_ids)
        ):
            raise RawObjectCorrupt(f"Target mismatch in raw manifest: {manifest_key}")
        if (
            record.hash_algorithm != "sha256"
            or not isinstance(record.content_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", record.content_hash) is None
            or record.blob_key != self._blob_key(record.content_hash)
            or record.compression != "gzip"
        ):
            raise RawObjectCorrupt(
                f"Invalid blob identity in raw manifest: {manifest_key}"
            )
        if not self._exists(record.blob_key):
            raise RawObjectNotFound(f"Raw blob not found: {record.blob_key}")
        return record

    def _record_from_receipt(
        self,
        target: RawTarget,
        receipt: Mapping[str, object],
        receipt_key: str,
    ) -> RawObjectRecord:
        if receipt.get("receipt_version") != RAW_RECEIPT_VERSION:
            raise RawObjectCorrupt(f"Unsupported raw receipt: {receipt_key}")
        record_payload = receipt.get("record")
        if not isinstance(record_payload, Mapping):
            raise RawObjectCorrupt(f"Receipt has no record: {receipt_key}")
        canonical = json.dumps(
            dict(record_payload),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if receipt.get("record_sha256") != hashlib.sha256(canonical).hexdigest():
            raise RawObjectCorrupt(f"Receipt checksum mismatch: {receipt_key}")
        return self._record_from_payload(target, record_payload, receipt_key)

    def _latest_receipt_record(
        self,
        target: RawTarget,
        *,
        include_invalidated: bool = False,
    ) -> Optional[RawObjectRecord]:
        prefix = self._target_receipt_prefix(target)

        def operation():
            return self.filesystem.get_file_info(
                fs.FileSelector(
                    self._path(prefix),
                    allow_not_found=True,
                    recursive=False,
                )
            )

        infos = self._retry_io("list", prefix, operation)
        receipt_errors = []
        receipt_count = 0
        invalidated_count = 0
        receipt_infos = sorted(
            (
                info
                for info in infos
                if info.type == fs.FileType.File and info.path.endswith(".json")
            ),
            key=lambda info: info.path,
            reverse=True,
        )
        for info in receipt_infos:
            if info.type != fs.FileType.File or not info.path.endswith(".json"):
                continue
            receipt_count += 1
            key = self._relative_path(info.path)
            try:
                receipt = self._read_json(key)
                record = self._record_from_receipt(target, receipt, key)
                try:
                    expected_key = self._target_receipt_key(target, asdict(record))
                except RawStoreError as exc:
                    raise RawObjectCorrupt(
                        f"Invalid receipt ordering metadata: {key}"
                    ) from exc
                if key != expected_key:
                    raise RawObjectCorrupt(f"Receipt key mismatch: {key}")
                record_sha256 = str(receipt["record_sha256"])
                if (
                    not include_invalidated
                    and self._exists(
                        self._target_invalidation_key(target, record_sha256)
                    )
                ):
                    invalidated_count += 1
                    continue
                # Receipt names start with fixed-width UTC observation time,
                # so descending lexical order is the same as record order.
                return record
            except (RawObjectNotFound, RawObjectCorrupt) as exc:
                receipt_errors.append(exc)
                logger.error("Ignoring invalid WhoScored raw receipt %s: %s", key, exc)
        if receipt_errors and len(receipt_errors) == receipt_count:
            raise receipt_errors[-1]
        if receipt_count and invalidated_count + len(receipt_errors) == receipt_count:
            raise RawObjectNotFound(
                f"All raw receipts are invalidated for {target.target_id}"
            )
        return None

    def _load_record(self, target: RawTarget) -> RawObjectRecord:
        receipt_error: Optional[RawObjectNotFound] = None
        try:
            receipt_record = self._latest_receipt_record(target)
        except RawObjectNotFound as exc:
            # A partially migrated target may still have a valid legacy
            # manifest alongside a damaged v2 receipt.  Preserve that verified
            # fallback; once a healthy v2 receipt is published it wins again.
            receipt_error = exc
            receipt_record = None
        manifest_key = self._target_manifest_key(target)
        legacy_record: Optional[RawObjectRecord] = None
        legacy_error: Optional[RawObjectNotFound] = None
        if self._exists(manifest_key):
            try:
                candidate = self._record_from_payload(
                    target, self._read_json(manifest_key), manifest_key
                )
                if self._exists(
                    self._target_invalidation_key(
                        target, self._record_sha256(candidate)
                    )
                ):
                    raise RawObjectNotFound(
                        f"Legacy raw manifest is invalidated for {target.target_id}"
                    )
                legacy_record = candidate
            except RawObjectNotFound as exc:
                legacy_error = exc

        candidates = [
            record
            for record in (receipt_record, legacy_record)
            if record is not None
        ]
        if candidates:
            # During a rolling v1/v2 cutover neither layout is intrinsically
            # newer. Compare the bound observation timestamp instead of
            # allowing any v2 receipt to shadow a later legacy writer.
            return max(candidates, key=self._record_order_key)
        if receipt_error is not None:
            raise receipt_error
        if legacy_error is not None:
            raise legacy_error
        raise RawObjectNotFound(f"No raw manifest for {target.target_id}")

    def is_fresh(
        self,
        target: RawTarget,
        *,
        max_age: timedelta,
        now: Optional[datetime] = None,
    ) -> bool:
        """Return whether a committed target is safe for a bounded TTL replay.

        Corrupt/missing manifests fail closed as stale so mutable pages can be
        refreshed from the source. The content-addressed blob is deliberately
        not read here: the immediately following cache load performs the full
        gzip/hash/length validation exactly once and falls back to source on
        corruption. Time comparisons are always normalized to UTC; naive
        timestamps are rejected rather than interpreted using the host
        timezone.
        """
        if not isinstance(max_age, timedelta):
            raise TypeError("max_age must be a datetime.timedelta")
        if max_age < timedelta(0):
            raise ValueError("max_age must be non-negative")
        reference = now or datetime.now(timezone.utc)
        if reference.tzinfo is None or reference.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        try:
            record = self._load_record(target)
            fetched_at = datetime.fromisoformat(
                record.fetched_at.strip().replace("Z", "+00:00")
            )
        except (RawStoreError, AttributeError, ValueError):
            return False
        if fetched_at.tzinfo is None or fetched_at.utcoffset() is None:
            return False
        age = reference.astimezone(timezone.utc) - fetched_at.astimezone(timezone.utc)
        return timedelta(0) <= age <= max_age

    def store_bytes(
        self,
        target: RawTarget,
        payload: bytes,
        *,
        content_type: str,
        charset: Optional[str] = None,
        fetched_at: Optional[str] = None,
        fetcher_version: str = FETCHER_VERSION,
    ) -> RawObjectRecord:
        with self.snapshot_lock(exclusive=False):
            return self._store_bytes_unlocked(
                target,
                payload,
                content_type=content_type,
                charset=charset,
                fetched_at=fetched_at,
                fetcher_version=fetcher_version,
            )

    def _store_bytes_unlocked(
        self,
        target: RawTarget,
        payload: bytes,
        *,
        content_type: str,
        charset: Optional[str] = None,
        fetched_at: Optional[str] = None,
        fetcher_version: str = FETCHER_VERSION,
    ) -> RawObjectRecord:
        if not isinstance(payload, bytes):
            raise TypeError("Raw payload must be bytes")
        if not payload:
            raise RawStoreError(
                f"Refusing to store an empty response for {target.target_id}"
            )
        observed_at = fetched_at or utc_now_iso()
        self._observation_time(observed_at)
        content_hash = hashlib.sha256(payload).hexdigest()
        blob_key = self._blob_key(content_hash)
        encoded = gzip.compress(payload, compresslevel=6, mtime=0)
        blob_is_valid = False
        if self._exists(blob_key):
            try:
                existing_encoded = self._read_bytes(blob_key)
                existing = gzip.decompress(existing_encoded)
                blob_is_valid = (
                    hashlib.sha256(existing).hexdigest() == content_hash
                    and existing == payload
                )
                if blob_is_valid:
                    encoded = existing_encoded
            except (gzip.BadGzipFile, EOFError, OSError, zlib.error):
                blob_is_valid = False
        if not blob_is_valid:
            self._write_bytes(blob_key, encoded)
        verified_encoded = self._read_bytes(blob_key)
        try:
            verified_payload = gzip.decompress(verified_encoded)
        except (gzip.BadGzipFile, EOFError, OSError, zlib.error) as exc:
            raise RawObjectCorrupt(f"Invalid gzip blob after write: {blob_key}") from exc
        if (
            len(verified_encoded) != len(encoded)
            or len(verified_payload) != len(payload)
            or hashlib.sha256(verified_payload).hexdigest() != content_hash
        ):
            raise RawObjectCorrupt(f"Raw blob verification failed: {blob_key}")

        record = RawObjectRecord(
            manifest_version=RAW_MANIFEST_VERSION,
            source=target.source,
            page_kind=target.page_kind,
            target_id=target.target_id,
            canonical_url=target.canonical_url,
            source_ids=dict(target.source_ids),
            content_hash=content_hash,
            hash_algorithm="sha256",
            blob_key=blob_key,
            compression="gzip",
            content_type=str(content_type),
            charset=charset,
            fetched_at=observed_at,
            fetcher_version=fetcher_version,
            decoded_bytes=len(payload),
            encoded_bytes=len(encoded),
        )
        if self._exists(
            self._target_invalidation_key(target, self._record_sha256(record))
        ):
            raise RawStoreError(
                "Refusing to republish an invalidated raw observation; "
                "retry with a new source observation timestamp"
            )
        # The immutable receipt is the commit marker and is deliberately last.
        # No mutable latest pointer is required for correctness: readers order
        # verified receipts by source observation time.
        record_payload = asdict(record)
        receipt_key = self._target_receipt_key(target, record_payload)
        self._write_immutable_json(
            receipt_key,
            self._receipt_payload(record_payload),
        )
        committed = self._record_from_receipt(
            target,
            self._read_json(receipt_key),
            receipt_key,
        )
        if committed != record:
            raise RawStoreError(f"Raw receipt verification failed: {receipt_key}")
        if self._exists(
            self._target_invalidation_key(target, self._record_sha256(record))
        ):
            raise RawStoreError(
                "Raw observation was invalidated while its receipt was committed"
            )
        return record

    def store_text(
        self,
        target: RawTarget,
        text: str,
        *,
        content_type: str = "text/html",
        fetched_at: Optional[str] = None,
        fetcher_version: str = FETCHER_VERSION,
    ) -> RawObjectRecord:
        if not isinstance(text, str):
            raise TypeError("Raw text must be str")
        return self.store_bytes(
            target,
            text.encode("utf-8"),
            content_type=content_type,
            charset="utf-8",
            fetched_at=fetched_at,
            fetcher_version=fetcher_version,
        )

    def load_bytes(self, target: RawTarget) -> tuple[bytes, RawObjectRecord]:
        try:
            record = self._load_record(target)
        except RawObjectCorrupt as exc:
            self.quarantine(target, reason=str(exc))
            raise
        try:
            encoded = self._read_bytes(record.blob_key)
            raw = gzip.decompress(encoded)
        except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
            error = RawObjectCorrupt(f"Invalid gzip blob: {record.blob_key}")
            self.quarantine(target, reason=str(error), record=record)
            raise error from exc
        actual_hash = hashlib.sha256(raw).hexdigest()
        if (
            actual_hash != record.content_hash
            or len(raw) != record.decoded_bytes
            or len(encoded) != record.encoded_bytes
        ):
            error = RawObjectCorrupt(
                f"Content integrity mismatch for {target.target_id}: "
                f"expected={record.content_hash}, actual={actual_hash}"
            )
            self.quarantine(target, reason=str(error), record=record)
            raise error
        return raw, record

    def load_text(self, target: RawTarget) -> tuple[str, RawObjectRecord]:
        raw, record = self.load_bytes(target)
        charset = record.charset or "utf-8"
        try:
            return raw.decode(charset), record
        except (LookupError, UnicodeDecodeError) as exc:
            error = RawObjectCorrupt(
                f"Raw object cannot be decoded as {charset}: {record.blob_key}"
            )
            self.quarantine(target, reason=str(error), record=record)
            raise error from exc

    def get_or_fetch(
        self,
        target: RawTarget,
        loader: Callable[[str], bytes | str],
        *,
        content_type: str,
        fetched_at: Optional[str] = None,
        fetcher_version: str = FETCHER_VERSION,
    ) -> tuple[bytes, RawObjectRecord, bool]:
        """Return cached bytes or invoke ``loader`` exactly once when absent."""

        with self.target_lock(target):
            try:
                raw, record = self.load_bytes(target)
            except RawObjectNotFound:
                # Corrupt manifests are quarantined by ``load_bytes`` and are
                # cache misses for the fetch state machine, while callers that
                # explicitly load receive typed corruption.
                pass
            else:
                return raw, record, True
            loaded = loader(target.canonical_url)
            payload = loaded.encode("utf-8") if isinstance(loaded, str) else loaded
            if not isinstance(payload, bytes) or not payload:
                raise RawStoreError(
                    f"Loader returned no payload for {target.target_id}"
                )
            record = self.store_bytes(
                target,
                payload,
                content_type=content_type,
                charset="utf-8" if isinstance(loaded, str) else None,
                fetched_at=fetched_at,
                fetcher_version=fetcher_version,
            )
            return payload, record, False
