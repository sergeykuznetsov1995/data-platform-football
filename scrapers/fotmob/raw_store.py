"""Durable content-addressed storage for FotMob JSON responses.

The store deliberately knows nothing about FotMob endpoint semantics.  A target
is the canonical URL (including its sorted query string) plus the SHA-256 key
derived from that URL by :mod:`scrapers.fotmob.transport`.  Response bodies are
kept byte-for-byte so parser replay sees exactly the payload that was fetched.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import threading
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Optional, Protocol
from urllib.parse import urlparse

from pyarrow import fs


RAW_MANIFEST_VERSION = "fotmob-raw-v1"
RAW_STORE_ENV = "FOTMOB_RAW_STORE_URI"


class RawStoreError(RuntimeError):
    """Base class for FotMob raw-store failures."""


class RawTargetNotFound(RawStoreError):
    """No committed manifest/blob exists for a canonical target."""


class RawTargetCorrupt(RawStoreError):
    """A target manifest or its content-addressed blob is invalid."""


class CanonicalTargetLike(Protocol):
    """Minimal target interface, avoiding a raw-store/transport import cycle."""

    canonical_url: str
    target_key: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class RawJsonRecord:
    """Committed metadata for one canonical FotMob response body."""

    manifest_version: str
    source: str
    target_key: str
    canonical_url: str
    content_hash: str
    hash_algorithm: str
    blob_key: str
    raw_uri: Optional[str]
    compression: str
    fetched_at: str
    etag: Optional[str]
    last_modified: Optional[str]
    decoded_bytes: int
    source_encoded_bytes: Optional[int]
    stored_bytes: int
    validated_at: str


class FotMobRawStore:
    """Content-addressed gzip blobs plus atomic target manifests.

    ``pyarrow.fs`` supplies a common local/S3 implementation.  Blobs are
    immutable; only the small target manifest is replaced when a target is
    refreshed.  Local writes use ``os.replace`` and S3 object visibility is
    atomic after a successful PutObject.
    """

    def __init__(
        self,
        filesystem: fs.FileSystem,
        root: str,
        *,
        uri_prefix: Optional[str] = None,
    ) -> None:
        normalized_root = root.rstrip("/")
        if not normalized_root:
            raise ValueError("Raw-store root must not be empty")
        self.filesystem = filesystem
        self.root = normalized_root
        self.uri_prefix = (uri_prefix or normalized_root).rstrip("/")
        self._write_lock = threading.RLock()
        self._verified_blobs: set[str] = set()

    @classmethod
    def from_uri(cls, uri: str) -> "FotMobRawStore":
        candidate = str(uri).strip()
        if not candidate:
            raise ValueError("Raw-store URI must not be empty")
        parsed = urlparse(candidate)
        if parsed.scheme == "s3":
            if not parsed.netloc:
                raise ValueError("S3 raw-store URI must contain a bucket")
            endpoint = os.environ.get("FOTMOB_RAW_S3_ENDPOINT", "seaweedfs:8333")
            scheme = os.environ.get("FOTMOB_RAW_S3_SCHEME", "http")
            filesystem = fs.S3FileSystem(
                access_key=os.environ.get("S3_ACCESS_KEY"),
                secret_key=os.environ.get("S3_SECRET_KEY"),
                endpoint_override=endpoint,
                scheme=scheme,
                region=os.environ.get("FOTMOB_RAW_S3_REGION", "us-east-1"),
                background_writes=False,
            )
            root = f"{parsed.netloc}/{parsed.path.lstrip('/')}".rstrip("/")
            return cls(filesystem, root, uri_prefix=candidate.rstrip("/"))

        filesystem, root = fs.FileSystem.from_uri(candidate)
        root = root.rstrip("/")
        if isinstance(filesystem, fs.LocalFileSystem):
            prefix = f"file://{root}"
        else:
            prefix = candidate.rstrip("/")
        return cls(filesystem, root, uri_prefix=prefix)

    @classmethod
    def from_env(cls, optional: bool = True) -> Optional["FotMobRawStore"]:
        uri = os.environ.get(RAW_STORE_ENV, "").strip()
        if not uri:
            if optional:
                return None
            raise RawStoreError(f"{RAW_STORE_ENV} is required")
        return cls.from_uri(uri)

    def _path(self, relative: str) -> str:
        return str(PurePosixPath(self.root) / relative)

    def _uri(self, relative: str) -> str:
        return f"{self.uri_prefix}/{relative.lstrip('/')}"

    def _exists(self, relative: str) -> bool:
        return (
            self.filesystem.get_file_info(self._path(relative)).type
            != fs.FileType.NotFound
        )

    def _read_bytes(self, relative: str) -> bytes:
        path = self._path(relative)
        info = self.filesystem.get_file_info(path)
        if info.type == fs.FileType.NotFound:
            raise RawTargetNotFound(f"Raw object not found: {relative}")
        with self.filesystem.open_input_file(path) as stream:
            return stream.read()

    def _write_bytes(self, relative: str, payload: bytes) -> None:
        """Commit one object, atomically for the local filesystem."""
        path = self._path(relative)
        parent = str(PurePosixPath(path).parent)
        self.filesystem.create_dir(parent, recursive=True)
        if not isinstance(self.filesystem, fs.LocalFileSystem):
            with self.filesystem.open_output_stream(
                path,
                compression=None,
            ) as stream:
                stream.write(payload)
            return

        temporary = f"{path}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
        try:
            with self.filesystem.open_output_stream(
                temporary,
                compression=None,
            ) as stream:
                stream.write(payload)
            os.replace(temporary, path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    @staticmethod
    def _manifest_key(target_key: str) -> str:
        if len(target_key) != 64 or any(
            char not in "0123456789abcdef" for char in target_key
        ):
            raise ValueError("target_key must be a lowercase SHA-256 hex digest")
        return f"targets/sha256/{target_key[:2]}/{target_key}.json"

    @staticmethod
    def _blob_key(content_hash: str) -> str:
        return f"blobs/sha256/{content_hash[:2]}/{content_hash}.json.gz"

    def has_target(self, target: CanonicalTargetLike) -> bool:
        return self._exists(self._manifest_key(target.target_key))

    def store(
        self,
        target: CanonicalTargetLike,
        body: bytes,
        *,
        fetched_at: Optional[str] = None,
        validated_at: Optional[str] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
        source_encoded_bytes: Optional[int] = None,
    ) -> RawJsonRecord:
        """Write the immutable body first and publish its manifest last."""
        if not isinstance(body, bytes):
            raise TypeError("FotMob raw body must be bytes")
        content_hash = hashlib.sha256(body).hexdigest()
        blob_key = self._blob_key(content_hash)
        compressed = gzip.compress(body, compresslevel=6, mtime=0)
        resolved_fetched_at = fetched_at or utc_now_iso()
        record = RawJsonRecord(
            manifest_version=RAW_MANIFEST_VERSION,
            source="fotmob",
            target_key=target.target_key,
            canonical_url=target.canonical_url,
            content_hash=content_hash,
            hash_algorithm="sha256",
            blob_key=blob_key,
            raw_uri=self._uri(blob_key),
            compression="gzip",
            fetched_at=resolved_fetched_at,
            etag=etag,
            last_modified=last_modified,
            decoded_bytes=len(body),
            source_encoded_bytes=source_encoded_bytes,
            stored_bytes=len(compressed),
            validated_at=validated_at or resolved_fetched_at,
        )
        manifest = (
            json.dumps(
                asdict(record),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ).encode("utf-8")
            + b"\n"
        )
        with self._write_lock:
            needs_write = not self._exists(blob_key)
            if not needs_write and blob_key not in self._verified_blobs:
                # A previous process may have crashed or external storage may
                # have been damaged.  Verify an existing immutable blob once
                # per process before publishing a new manifest that cites it.
                try:
                    existing = self._read_bytes(blob_key)
                    needs_write = (
                        len(existing) != len(compressed)
                        or gzip.decompress(existing) != body
                    )
                except (RawStoreError, OSError, gzip.BadGzipFile, EOFError):
                    needs_write = True
            if needs_write:
                self._write_bytes(blob_key, compressed)
            self._verified_blobs.add(blob_key)
            self._write_bytes(self._manifest_key(target.target_key), manifest)
        return record

    def alias(
        self,
        source: CanonicalTargetLike,
        target: CanonicalTargetLike,
    ) -> RawJsonRecord:
        """Publish ``target`` as a durable alias of a validated raw response.

        FotMob's ``/leagues?id=...`` response is also the exact selected-season
        response.  The service validates ``details.selectedSeason`` before
        calling this method, then aliases the immutable blob to the canonical
        ``id+season`` target.  A later backfill resume can consequently replay
        the exact scope without a network request or a second blob write.
        """

        # Loading first verifies the manifest, gzip payload, hash and lengths.
        _, source_record = self.load(source)
        # Validate the destination key even when source and target happen to
        # be identical.
        target_manifest_key = self._manifest_key(target.target_key)
        if (
            source_record.target_key == target.target_key
            and source_record.canonical_url == target.canonical_url
        ):
            return source_record
        record = replace(
            source_record,
            target_key=target.target_key,
            canonical_url=target.canonical_url,
        )
        manifest = (
            json.dumps(
                asdict(record),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ).encode("utf-8")
            + b"\n"
        )
        with self._write_lock:
            # The referenced blob was validated above and remains immutable;
            # publishing the small target manifest is the only write.
            self._write_bytes(target_manifest_key, manifest)
        return record

    def load(
        self,
        target: CanonicalTargetLike,
    ) -> tuple[bytes, RawJsonRecord]:
        manifest_key = self._manifest_key(target.target_key)
        if not self._exists(manifest_key):
            raise RawTargetNotFound(
                f"No raw FotMob manifest for {target.canonical_url}"
            )
        try:
            payload = json.loads(self._read_bytes(manifest_key).decode("utf-8"))
            record = RawJsonRecord(**payload)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
            raise RawTargetCorrupt(
                f"Invalid raw FotMob manifest: {manifest_key}"
            ) from exc

        if (
            record.manifest_version != RAW_MANIFEST_VERSION
            or record.target_key != target.target_key
            or record.canonical_url != target.canonical_url
        ):
            raise RawTargetCorrupt(
                f"Target mismatch in raw FotMob manifest: {manifest_key}"
            )
        try:
            compressed = self._read_bytes(record.blob_key)
            body = gzip.decompress(compressed)
        except (gzip.BadGzipFile, EOFError) as exc:
            with self._write_lock:
                self._verified_blobs.discard(record.blob_key)
            raise RawTargetCorrupt(f"Invalid gzip blob: {record.blob_key}") from exc
        actual_hash = hashlib.sha256(body).hexdigest()
        if (
            record.hash_algorithm != "sha256"
            or actual_hash != record.content_hash
            or len(body) != record.decoded_bytes
            or len(compressed) != record.stored_bytes
        ):
            with self._write_lock:
                self._verified_blobs.discard(record.blob_key)
            raise RawTargetCorrupt(
                f"Content mismatch for {target.canonical_url}: "
                f"expected={record.content_hash}, actual={actual_hash}"
            )
        with self._write_lock:
            self._verified_blobs.add(record.blob_key)
        return body, record
