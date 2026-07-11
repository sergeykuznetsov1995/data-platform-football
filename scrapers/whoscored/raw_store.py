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
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import Callable, Mapping, Optional
from urllib.parse import urlparse

from pyarrow import fs

from .domain import WhoScoredScope


RAW_MANIFEST_VERSION = "whoscored-raw-v1"
FETCHER_VERSION = "whoscored-transport-v2"
_POSITIVE_ID_RE = re.compile(r"^[1-9][0-9]*$")


class RawStoreError(RuntimeError):
    """Base error for raw storage failures."""


class RawObjectNotFound(RawStoreError):
    """The target has no committed manifest or its blob is absent."""


class RawObjectCorrupt(RawStoreError):
    """A target manifest or compressed blob violates its integrity contract."""


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
    """Content-addressed gzip blobs plus latest-target JSON manifests."""

    def __init__(self, filesystem: fs.FileSystem, root: str) -> None:
        self.filesystem = filesystem
        self.root = root.strip("/") if not root.startswith("/") else root.rstrip("/")

    @classmethod
    def from_uri(cls, uri: str) -> "WhoScoredRawStore":
        parsed = urlparse(uri)
        if parsed.scheme == "s3":
            if not parsed.netloc:
                raise ValueError("S3 raw-store URI must contain a bucket")
            filesystem = fs.S3FileSystem(
                access_key=os.environ.get("S3_ACCESS_KEY"),
                secret_key=os.environ.get("S3_SECRET_KEY"),
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
        return self.filesystem.get_file_info(self._path(relative)).type != fs.FileType.NotFound

    def _read_bytes(self, relative: str) -> bytes:
        path = self._path(relative)
        info = self.filesystem.get_file_info(path)
        if info.type == fs.FileType.NotFound:
            raise RawObjectNotFound(f"Raw object not found: {relative}")
        with self.filesystem.open_input_file(path) as stream:
            return stream.read()

    def _write_bytes(self, relative: str, payload: bytes) -> None:
        path = self._path(relative)
        self.filesystem.create_dir(str(PurePosixPath(path).parent), recursive=True)
        # Arrow auto-detects .gz; compression=None avoids double compression.
        with self.filesystem.open_output_stream(path, compression=None) as stream:
            stream.write(payload)

    def _read_json(self, relative: str) -> dict:
        try:
            payload = json.loads(self._read_bytes(relative).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RawObjectCorrupt(f"Invalid JSON manifest: {relative}") from exc
        if not isinstance(payload, dict):
            raise RawObjectCorrupt(f"Manifest is not an object: {relative}")
        return payload

    def _write_json(self, relative: str, payload: Mapping[str, object]) -> None:
        rendered = json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ).encode("utf-8") + b"\n"
        self._write_bytes(relative, rendered)

    @staticmethod
    def _target_manifest_key(target: RawTarget) -> str:
        digest = hashlib.sha256(target.target_id.encode("utf-8")).hexdigest()
        return f"targets/{target.page_kind}/{digest}.json"

    @staticmethod
    def _blob_key(content_hash: str) -> str:
        return f"blobs/sha256/{content_hash[:2]}/{content_hash}.raw.gz"

    def has(self, target: RawTarget) -> bool:
        return self._exists(self._target_manifest_key(target))

    def _load_record(self, target: RawTarget) -> RawObjectRecord:
        manifest_key = self._target_manifest_key(target)
        if not self._exists(manifest_key):
            raise RawObjectNotFound(f"No raw manifest for {target.target_id}")
        payload = self._read_json(manifest_key)
        try:
            record = RawObjectRecord(**payload)
        except TypeError as exc:
            raise RawObjectCorrupt(f"Invalid raw manifest: {manifest_key}") from exc
        if record.manifest_version != RAW_MANIFEST_VERSION:
            raise RawObjectCorrupt(
                f"Unsupported manifest version {record.manifest_version!r}"
            )
        if record.target_id != target.target_id:
            raise RawObjectCorrupt(f"Target mismatch in raw manifest: {manifest_key}")
        if (
            record.hash_algorithm != "sha256"
            or not isinstance(record.content_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", record.content_hash) is None
            or record.blob_key != self._blob_key(record.content_hash)
            or record.compression != "gzip"
        ):
            raise RawObjectCorrupt(f"Invalid blob identity in raw manifest: {manifest_key}")
        if not self._exists(record.blob_key):
            raise RawObjectNotFound(f"Raw blob not found: {record.blob_key}")
        return record

    def is_fresh(
        self,
        target: RawTarget,
        *,
        max_age: timedelta,
        now: Optional[datetime] = None,
    ) -> bool:
        """Return whether a committed target is safe for a bounded TTL replay.

        Corrupt/missing manifests fail closed as stale so mutable pages can be
        refreshed from the source. Time comparisons are always normalized to
        UTC; naive timestamps are rejected rather than interpreted using the
        host timezone.
        """
        if not isinstance(max_age, timedelta):
            raise TypeError("max_age must be a datetime.timedelta")
        if max_age < timedelta(0):
            raise ValueError("max_age must be non-negative")
        reference = now or datetime.now(timezone.utc)
        if reference.tzinfo is None or reference.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        try:
            _, record = self.load_bytes(target)
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
        if not isinstance(payload, bytes):
            raise TypeError("Raw payload must be bytes")
        if not payload:
            raise RawStoreError(f"Refusing to store an empty response for {target.target_id}")
        content_hash = hashlib.sha256(payload).hexdigest()
        blob_key = self._blob_key(content_hash)
        encoded = gzip.compress(payload, compresslevel=6, mtime=0)
        if not self._exists(blob_key):
            self._write_bytes(blob_key, encoded)

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
            fetched_at=fetched_at or utc_now_iso(),
            fetcher_version=fetcher_version,
            decoded_bytes=len(payload),
            encoded_bytes=len(encoded),
        )
        # The target manifest is the commit marker and is deliberately last.
        self._write_json(self._target_manifest_key(target), asdict(record))
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
        record = self._load_record(target)
        try:
            encoded = self._read_bytes(record.blob_key)
            raw = gzip.decompress(encoded)
        except (gzip.BadGzipFile, EOFError) as exc:
            raise RawObjectCorrupt(f"Invalid gzip blob: {record.blob_key}") from exc
        actual_hash = hashlib.sha256(raw).hexdigest()
        if (
            actual_hash != record.content_hash
            or len(raw) != record.decoded_bytes
            or len(encoded) != record.encoded_bytes
        ):
            raise RawObjectCorrupt(
                f"Content integrity mismatch for {target.target_id}: "
                f"expected={record.content_hash}, actual={actual_hash}"
            )
        return raw, record

    def load_text(self, target: RawTarget) -> tuple[str, RawObjectRecord]:
        raw, record = self.load_bytes(target)
        charset = record.charset or "utf-8"
        try:
            return raw.decode(charset), record
        except (LookupError, UnicodeDecodeError) as exc:
            raise RawObjectCorrupt(
                f"Raw object cannot be decoded as {charset}: {record.blob_key}"
            ) from exc

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

        if self.has(target):
            raw, record = self.load_bytes(target)
            return raw, record, True
        loaded = loader(target.canonical_url)
        payload = loaded.encode("utf-8") if isinstance(loaded, str) else loaded
        if not isinstance(payload, bytes) or not payload:
            raise RawStoreError(f"Loader returned no payload for {target.target_id}")
        record = self.store_bytes(
            target,
            payload,
            content_type=content_type,
            charset="utf-8" if isinstance(loaded, str) else None,
            fetched_at=fetched_at,
            fetcher_version=fetcher_version,
        )
        return payload, record, False
