"""Immutable, content-addressed storage for native ESPN JSON responses."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import threading
import uuid
import zlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Optional, Protocol
from urllib.parse import urlparse

from pyarrow import fs


RAW_MANIFEST_VERSION = "espn-raw-v1"
RAW_STORE_ENV = "ESPN_RAW_STORE_URI"


class RawStoreError(RuntimeError):
    """Base class for raw-store failures."""


class RawTargetNotFound(RawStoreError):
    """No committed alias exists for a target."""


class RawTargetCorrupt(RawStoreError):
    """An alias or immutable gzip object failed integrity validation."""


class CanonicalTargetLike(Protocol):
    canonical_url: str
    url_fingerprint: str
    sanitized_url: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _endpoint_value(endpoint: object) -> str:
    value = getattr(endpoint, "value", endpoint)
    if not isinstance(value, str) or not value.strip():
        raise ValueError("endpoint must be a non-empty string")
    return value.strip().lower()


def _strict_gzip_decompress(payload: bytes) -> bytes:
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    body = decoder.decompress(payload)
    body += decoder.flush()
    if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise zlib.error("gzip payload is truncated or has trailing data")
    return body


@dataclass(frozen=True, slots=True)
class RawJsonRecord:
    manifest_version: str
    source: str
    endpoint: str
    url_fingerprint: str
    sanitized_url: str
    content_hash: str
    hash_algorithm: str
    blob_key: str
    raw_uri: str
    compression: str
    fetched_at: str
    http_status: int
    decoded_bytes: int
    direct_bytes: int
    stored_bytes: int

    @property
    def canonical_url(self) -> str:
        """Compatibility alias; durable URLs are value-redacted."""
        return self.sanitized_url


class EspnRawStore:
    """Gzip blobs keyed by body SHA-256 and atomic URL-fingerprint aliases."""

    def __init__(
        self,
        filesystem: fs.FileSystem,
        root: str,
        *,
        uri_prefix: Optional[str] = None,
    ) -> None:
        normalized_root = root.rstrip("/")
        if not normalized_root:
            raise ValueError("ESPN raw-store root must not be empty")
        self.filesystem = filesystem
        self.root = normalized_root
        self.uri_prefix = (uri_prefix or normalized_root).rstrip("/")
        self._write_lock = threading.RLock()
        self._verified_blobs: set[str] = set()

    @classmethod
    def from_uri(cls, uri: str) -> "EspnRawStore":
        candidate = str(uri).strip()
        if not candidate:
            raise ValueError("ESPN raw-store URI must not be empty")
        parsed = urlparse(candidate)
        if parsed.scheme == "s3":
            if not parsed.netloc:
                raise ValueError("S3 ESPN raw-store URI must contain a bucket")
            filesystem = fs.S3FileSystem(
                access_key=os.environ.get("S3_ACCESS_KEY"),
                secret_key=os.environ.get("S3_SECRET_KEY"),
                endpoint_override=os.environ.get(
                    "ESPN_RAW_S3_ENDPOINT", "seaweedfs:8333"
                ),
                scheme=os.environ.get("ESPN_RAW_S3_SCHEME", "http"),
                region=os.environ.get("ESPN_RAW_S3_REGION", "us-east-1"),
                background_writes=False,
            )
            root = f"{parsed.netloc}/{parsed.path.lstrip('/')}".rstrip("/")
            return cls(filesystem, root, uri_prefix=candidate.rstrip("/"))

        filesystem, root = fs.FileSystem.from_uri(candidate)
        root = root.rstrip("/")
        prefix = (
            f"file://{root}"
            if isinstance(filesystem, fs.LocalFileSystem)
            else candidate.rstrip("/")
        )
        return cls(filesystem, root, uri_prefix=prefix)

    @classmethod
    def from_env(cls) -> "EspnRawStore":
        uri = os.environ.get(RAW_STORE_ENV, "").strip()
        if not uri:
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
        if self.filesystem.get_file_info(path).type == fs.FileType.NotFound:
            raise RawTargetNotFound(f"ESPN raw object not found: {relative}")
        with self.filesystem.open_input_file(path) as stream:
            return stream.read()

    def _write_bytes(self, relative: str, payload: bytes) -> None:
        """Publish a complete object; local aliases use atomic rename."""
        path = self._path(relative)
        self.filesystem.create_dir(str(PurePosixPath(path).parent), recursive=True)
        if not isinstance(self.filesystem, fs.LocalFileSystem):
            with self.filesystem.open_output_stream(path, compression=None) as stream:
                stream.write(payload)
            return
        temporary = f"{path}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
        try:
            with self.filesystem.open_output_stream(
                temporary, compression=None
            ) as stream:
                stream.write(payload)
            os.replace(temporary, path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    @staticmethod
    def _digest(value: str, field: str) -> str:
        if len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError(f"{field} must be a lowercase SHA-256 digest")
        return value

    @classmethod
    def _alias_key(cls, url_fingerprint: str) -> str:
        digest = cls._digest(url_fingerprint, "url_fingerprint")
        return f"targets/sha256/{digest[:2]}/{digest}.json"

    @classmethod
    def _blob_key(cls, content_hash: str) -> str:
        digest = cls._digest(content_hash, "content_hash")
        return f"blobs/sha256/{digest[:2]}/{digest}.json.gz"

    def _quarantine(self, relative: str) -> None:
        """Move a damaged object aside before publishing any replacement."""
        if not self._exists(relative):
            return
        quarantine = f"quarantine/{relative}.{uuid.uuid4().hex}"
        target = self._path(quarantine)
        self.filesystem.create_dir(str(PurePosixPath(target).parent), recursive=True)
        self.filesystem.move(self._path(relative), target)

    def has_target(self, target: CanonicalTargetLike) -> bool:
        return self._exists(self._alias_key(target.url_fingerprint))

    def store(
        self,
        target: CanonicalTargetLike,
        endpoint: object,
        body: bytes,
        *,
        fetched_at: Optional[str] = None,
        http_status: int = 200,
        direct_bytes: Optional[int] = None,
    ) -> RawJsonRecord:
        """Persist body first, then atomically publish its replay alias."""
        if not isinstance(body, bytes):
            raise TypeError("ESPN raw body must be bytes")
        fingerprint = self._digest(target.url_fingerprint, "url_fingerprint")
        if (
            hashlib.sha256(target.canonical_url.encode("utf-8")).hexdigest()
            != fingerprint
        ):
            raise ValueError("url_fingerprint does not match canonical_url")
        endpoint_value = _endpoint_value(endpoint)
        if type(http_status) is not int or not 100 <= http_status <= 599:
            raise ValueError("http_status must be an HTTP status integer")
        wire_bytes = len(body) if direct_bytes is None else direct_bytes
        if type(wire_bytes) is not int or wire_bytes < 0:
            raise ValueError("direct_bytes must be non-negative")

        content_hash = hashlib.sha256(body).hexdigest()
        blob_key = self._blob_key(content_hash)
        compressed = gzip.compress(body, compresslevel=6, mtime=0)
        record = RawJsonRecord(
            manifest_version=RAW_MANIFEST_VERSION,
            source="espn",
            endpoint=endpoint_value,
            url_fingerprint=fingerprint,
            sanitized_url=target.sanitized_url,
            content_hash=content_hash,
            hash_algorithm="sha256",
            blob_key=blob_key,
            raw_uri=self._uri(blob_key),
            compression="gzip",
            fetched_at=fetched_at or _utc_now_iso(),
            http_status=http_status,
            decoded_bytes=len(body),
            direct_bytes=wire_bytes,
            stored_bytes=len(compressed),
        )
        alias = (
            json.dumps(asdict(record), sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")

        with self._write_lock:
            if self._exists(blob_key) and blob_key not in self._verified_blobs:
                try:
                    current = self._read_bytes(blob_key)
                    valid = (
                        len(current) == len(compressed)
                        and _strict_gzip_decompress(current) == body
                    )
                except (RawStoreError, OSError, EOFError, gzip.BadGzipFile, zlib.error):
                    valid = False
                if not valid:
                    self._quarantine(blob_key)
            if not self._exists(blob_key):
                self._write_bytes(blob_key, compressed)
            self._verified_blobs.add(blob_key)
            self._write_bytes(self._alias_key(fingerprint), alias)
        return record

    def load(self, target: CanonicalTargetLike) -> tuple[bytes, RawJsonRecord]:
        fingerprint = self._digest(target.url_fingerprint, "url_fingerprint")
        alias_key = self._alias_key(fingerprint)
        if not self._exists(alias_key):
            raise RawTargetNotFound(f"No raw ESPN alias for target {fingerprint}")
        try:
            payload = json.loads(self._read_bytes(alias_key).decode("utf-8"))
            record = RawJsonRecord(**payload)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
            raise RawTargetCorrupt(f"Invalid raw ESPN alias: {alias_key}") from exc

        try:
            expected_blob = self._blob_key(record.content_hash)
        except (TypeError, ValueError) as exc:
            raise RawTargetCorrupt(f"Invalid raw ESPN alias hash: {alias_key}") from exc
        if (
            record.manifest_version != RAW_MANIFEST_VERSION
            or record.source != "espn"
            or record.url_fingerprint != fingerprint
            or not isinstance(record.sanitized_url, str)
            or record.sanitized_url != target.sanitized_url
            or record.hash_algorithm != "sha256"
            or record.blob_key != expected_blob
            or record.raw_uri != self._uri(expected_blob)
            or record.compression != "gzip"
            or type(record.http_status) is not int
            or type(record.decoded_bytes) is not int
            or type(record.direct_bytes) is not int
            or type(record.stored_bytes) is not int
            or min(record.decoded_bytes, record.direct_bytes, record.stored_bytes) < 0
        ):
            raise RawTargetCorrupt(f"Raw ESPN alias identity mismatch: {alias_key}")
        try:
            compressed = self._read_bytes(record.blob_key)
            body = _strict_gzip_decompress(compressed)
        except (RawStoreError, OSError, EOFError, gzip.BadGzipFile, zlib.error) as exc:
            with self._write_lock:
                self._verified_blobs.discard(record.blob_key)
            raise RawTargetCorrupt(
                f"Invalid ESPN gzip blob: {record.blob_key}"
            ) from exc
        actual_hash = hashlib.sha256(body).hexdigest()
        if (
            actual_hash != record.content_hash
            or len(body) != record.decoded_bytes
            or len(compressed) != record.stored_bytes
        ):
            with self._write_lock:
                self._verified_blobs.discard(record.blob_key)
            raise RawTargetCorrupt(
                f"Raw ESPN content mismatch: expected {record.content_hash}, "
                f"got {actual_hash}"
            )
        with self._write_lock:
            self._verified_blobs.add(record.blob_key)
        return body, record

    def load_exact(self, raw_uri: str, content_hash: str) -> bytes:
        """Load one immutable blob by its manifest-bound URI and SHA-256.

        Target aliases are intentionally not consulted: aliases move when a
        later source response is captured, while replay and resume must retain
        the exact bytes named by their durable raw manifest.
        """

        digest = self._digest(content_hash, "content_hash")
        blob_key = self._blob_key(digest)
        expected_uri = self._uri(blob_key)
        if raw_uri != expected_uri:
            raise RawTargetCorrupt(
                "Raw ESPN blob URI does not match its content SHA-256"
            )
        try:
            compressed = self._read_bytes(blob_key)
            body = _strict_gzip_decompress(compressed)
        except (RawStoreError, OSError, EOFError, gzip.BadGzipFile, zlib.error) as exc:
            with self._write_lock:
                self._verified_blobs.discard(blob_key)
            raise RawTargetCorrupt(f"Invalid ESPN gzip blob: {blob_key}") from exc
        actual_hash = hashlib.sha256(body).hexdigest()
        if actual_hash != digest:
            with self._write_lock:
                self._verified_blobs.discard(blob_key)
            raise RawTargetCorrupt(
                f"Raw ESPN content mismatch: expected {digest}, got {actual_hash}"
            )
        with self._write_lock:
            self._verified_blobs.add(blob_key)
        return body


__all__ = [
    "EspnRawStore",
    "RAW_MANIFEST_VERSION",
    "RAW_STORE_ENV",
    "RawJsonRecord",
    "RawStoreError",
    "RawTargetCorrupt",
    "RawTargetNotFound",
]
