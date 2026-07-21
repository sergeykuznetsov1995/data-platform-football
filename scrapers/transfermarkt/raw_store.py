"""Immutable raw-first storage for Transfermarkt HTTP responses.

The store persists the exact response bytes before callers parse HTML or JSON.
Bodies are content-addressed deterministic gzip objects and every HTTP attempt
has an immutable manifest suitable for offline replay.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import threading
import uuid
import zlib
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Mapping, Optional
from urllib.parse import parse_qsl, urlparse

from pyarrow import fs


RAW_MANIFEST_VERSION = "transfermarkt-raw-v1"
RAW_STORE_ENV = "TRANSFERMARKT_RAW_STORE_URI"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_RESPONSE_HEADERS = frozenset(
    {
        "cache-control",
        "content-encoding",
        "content-language",
        "content-length",
        "content-type",
        "date",
        "etag",
        "expires",
        "last-modified",
        "retry-after",
        "vary",
    }
)
_CREDENTIAL_QUERY_FIELDS = frozenset(
    {
        "access_token",
        "api_key",
        "apikey",
        "auth",
        "authorization",
        "credential",
        "password",
        "secret",
        "signature",
        "token",
        "x-amz-credential",
        "x-amz-security-token",
        "x-amz-signature",
    }
)


class RawStoreError(RuntimeError):
    """Base error for Transfermarkt raw storage."""


class RawCaptureNotFound(RawStoreError):
    """A capture manifest or referenced blob does not exist."""


class RawCaptureCorrupt(RawStoreError):
    """A committed manifest or blob violates its integrity contract."""


class RawCaptureConflict(RawStoreError):
    """An immutable capture key already contains different evidence."""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _required(value: object, name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def _safe_url(value: object) -> str:
    """Validate a public HTTP(S) URI without ever echoing its value."""

    invalid = "Transfermarkt response URL must be credential-free HTTP(S)"
    candidate = _required(value, "url")
    if any(ord(character) <= 0x20 or ord(character) == 0x7F for character in candidate):
        raise ValueError(invalid)
    try:
        parsed = urlparse(candidate)
        port = parsed.port
    except (TypeError, ValueError):
        raise ValueError(invalid) from None
    if (
        parsed.scheme.lower() not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or "@" in parsed.netloc
        or port not in {None, 80, 443}
        or parsed.fragment
    ):
        raise ValueError(invalid)
    if any(
        name.strip().lower() in _CREDENTIAL_QUERY_FIELDS
        for name, _ in parse_qsl(parsed.query, keep_blank_values=True)
    ):
        raise ValueError(invalid)
    return candidate


def _safe_headers(headers: Optional[Mapping[str, object]]) -> dict[str, str]:
    persisted: dict[str, str] = {}
    for key, value in (headers or {}).items():
        name = str(key).strip().lower()
        if name not in _SAFE_RESPONSE_HEADERS:
            continue
        rendered = str(value).strip()
        if "\r" in rendered or "\n" in rendered or "\x00" in rendered:
            continue
        persisted[name] = rendered
    return persisted


@dataclass(frozen=True)
class RawCaptureRecord:
    """Durable evidence for one Transfermarkt HTTP attempt."""

    manifest_version: str
    capture_id: str
    source: str
    cycle_id: str
    scope_id: str
    endpoint: str
    attempt: int
    url: str
    status_code: int
    headers: Mapping[str, str]
    content_type: Optional[str]
    content_hash: str
    hash_algorithm: str
    blob_key: str
    compression: str
    fetched_at: str
    decoded_bytes: int
    stored_bytes: int


class RawResponseStore:
    """Content-addressed bodies plus immutable per-attempt manifests."""

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

    @classmethod
    def from_uri(cls, uri: str) -> "RawResponseStore":
        invalid = "Invalid credential-free raw-store URI"
        if type(uri) is not str or not uri or uri != uri.strip():
            raise ValueError(invalid)
        if any(ord(character) <= 0x20 or ord(character) == 0x7F for character in uri):
            raise ValueError(invalid)
        try:
            parsed = urlparse(uri)
        except (TypeError, ValueError):
            raise ValueError(invalid) from None
        if (
            parsed.username is not None
            or parsed.password is not None
            or "@" in parsed.netloc
            or parsed.query
            or parsed.fragment
            or parsed.params
        ):
            raise ValueError(invalid)
        if parsed.scheme not in {"", "file", "s3"}:
            raise ValueError(invalid)

        if parsed.scheme == "s3":
            try:
                port = parsed.port
            except ValueError:
                raise ValueError(invalid) from None
            bucket = parsed.hostname
            if not bucket or port is not None or parsed.netloc != bucket:
                raise ValueError(invalid)
            filesystem = fs.S3FileSystem(
                access_key=os.environ.get("S3_ACCESS_KEY"),
                secret_key=os.environ.get("S3_SECRET_KEY"),
                endpoint_override=os.environ.get(
                    "TRANSFERMARKT_RAW_S3_ENDPOINT", "seaweedfs:8333"
                ),
                scheme=os.environ.get("TRANSFERMARKT_RAW_S3_SCHEME", "http"),
                region=os.environ.get(
                    "TRANSFERMARKT_RAW_S3_REGION", "us-east-1"
                ),
                background_writes=False,
            )
            root = f"{bucket}/{parsed.path.lstrip('/')}".rstrip("/")
            if root == bucket:
                raise ValueError("S3 raw-store URI must contain a prefix")
            return cls(filesystem, root, uri_prefix=uri.rstrip("/"))

        try:
            filesystem, root = fs.FileSystem.from_uri(uri)
        except (TypeError, ValueError):
            raise ValueError(invalid) from None
        root = root.rstrip("/")
        if not root:
            raise ValueError(invalid)
        prefix = f"file://{root}" if isinstance(filesystem, fs.LocalFileSystem) else uri
        return cls(filesystem, root, uri_prefix=prefix)

    @classmethod
    def from_env(cls, optional: bool = False) -> Optional["RawResponseStore"]:
        uri = os.environ.get(RAW_STORE_ENV, "").strip()
        if not uri:
            if optional:
                return None
            raise RawStoreError(f"{RAW_STORE_ENV} is required")
        return cls.from_uri(uri)

    def _path(self, relative: str) -> str:
        return str(PurePosixPath(self.root) / relative)

    def _exists(self, relative: str) -> bool:
        return self.filesystem.get_file_info(self._path(relative)).type != fs.FileType.NotFound

    def _read_bytes(self, relative: str) -> bytes:
        path = self._path(relative)
        if self.filesystem.get_file_info(path).type == fs.FileType.NotFound:
            raise RawCaptureNotFound(f"Raw object not found: {relative}")
        with self.filesystem.open_input_file(path) as stream:
            return stream.read()

    def _write_bytes(self, relative: str, payload: bytes) -> None:
        path = self._path(relative)
        self.filesystem.create_dir(str(PurePosixPath(path).parent), recursive=True)
        if not isinstance(self.filesystem, fs.LocalFileSystem):
            with self.filesystem.open_output_stream(path, compression=None) as stream:
                stream.write(payload)
            return
        temporary = f"{path}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
        try:
            with self.filesystem.open_output_stream(temporary, compression=None) as stream:
                stream.write(payload)
            os.replace(temporary, path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    @staticmethod
    def blob_key(content_hash: str) -> str:
        if _SHA256_RE.fullmatch(content_hash) is None:
            raise ValueError("content_hash must be a lowercase SHA-256 digest")
        return f"blobs/sha256/{content_hash[:2]}/{content_hash}.body.gz"

    @staticmethod
    def capture_manifest_key(capture_id: str) -> str:
        if _SHA256_RE.fullmatch(str(capture_id)) is None:
            raise ValueError("capture_id must be a lowercase SHA-256 digest")
        return f"captures/sha256/{capture_id[:2]}/{capture_id}.json"

    @staticmethod
    def allocate_capture_id(
        *,
        cycle_id: str,
        scope_id: str,
        endpoint: str,
        attempt: int,
        url: str,
        status_code: int,
        content_hash: str,
    ) -> str:
        identity = {
            "attempt": int(attempt),
            "content_hash": content_hash,
            "cycle_id": _required(cycle_id, "cycle_id"),
            "endpoint": _required(endpoint, "endpoint"),
            "scope_id": _required(scope_id, "scope_id"),
            "status_code": int(status_code),
            "url": _safe_url(url),
        }
        encoded = json.dumps(
            identity, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _verify_blob(
        self,
        blob_key: str,
        *,
        expected_body: Optional[bytes] = None,
        expected_hash: str,
        expected_decoded_bytes: int,
        expected_stored_bytes: Optional[int] = None,
    ) -> tuple[bytes, bytes]:
        compressed = self._read_bytes(blob_key)
        if expected_stored_bytes is not None and len(compressed) != expected_stored_bytes:
            raise RawCaptureCorrupt(f"Stored length mismatch for {blob_key}")
        try:
            body = gzip.decompress(compressed)
        except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
            raise RawCaptureCorrupt(f"Invalid gzip blob: {blob_key}") from exc
        actual_hash = hashlib.sha256(body).hexdigest()
        if (
            actual_hash != expected_hash
            or len(body) != expected_decoded_bytes
            or (expected_body is not None and body != expected_body)
        ):
            raise RawCaptureCorrupt(
                f"Content-addressed blob mismatch for {blob_key}"
            )
        return body, compressed

    def store_attempt(
        self,
        url: str,
        body: bytes,
        status_code: int,
        headers: Optional[Mapping[str, object]],
        fetched_at: Optional[str],
        cycle_id: str,
        scope_id: str,
        endpoint: str,
        attempt: int,
    ) -> RawCaptureRecord:
        """Commit exact bytes first, then publish immutable attempt evidence."""

        if not isinstance(body, bytes):
            raise TypeError("Transfermarkt raw response body must be bytes")
        safe_url = _safe_url(url)
        safe_cycle = _required(cycle_id, "cycle_id")
        safe_scope = _required(scope_id, "scope_id")
        safe_endpoint = _required(endpoint, "endpoint")
        ordinal = int(attempt)
        if ordinal < 0:
            raise ValueError("attempt must be non-negative")
        status = int(status_code)
        if not 100 <= status <= 599:
            raise ValueError("status_code must be between 100 and 599")
        persisted_headers = _safe_headers(headers)
        content_hash = hashlib.sha256(body).hexdigest()
        blob_key = self.blob_key(content_hash)
        encoded = gzip.compress(body, compresslevel=6, mtime=0)
        capture_id = self.allocate_capture_id(
            cycle_id=safe_cycle,
            scope_id=safe_scope,
            endpoint=safe_endpoint,
            attempt=ordinal,
            url=safe_url,
            status_code=status,
            content_hash=content_hash,
        )
        record = RawCaptureRecord(
            manifest_version=RAW_MANIFEST_VERSION,
            capture_id=capture_id,
            source="transfermarkt",
            cycle_id=safe_cycle,
            scope_id=safe_scope,
            endpoint=safe_endpoint,
            attempt=ordinal,
            url=safe_url,
            status_code=status,
            headers=persisted_headers,
            content_type=persisted_headers.get("content-type"),
            content_hash=content_hash,
            hash_algorithm="sha256",
            blob_key=blob_key,
            compression="gzip",
            fetched_at=fetched_at or utc_now_iso(),
            decoded_bytes=len(body),
            stored_bytes=len(encoded),
        )
        manifest_key = self.capture_manifest_key(capture_id)
        rendered = json.dumps(
            asdict(record), ensure_ascii=False, indent=2, sort_keys=True
        ).encode("utf-8") + b"\n"

        with self._write_lock:
            if self._exists(blob_key):
                _, stored = self._verify_blob(
                    blob_key,
                    expected_body=body,
                    expected_hash=content_hash,
                    expected_decoded_bytes=len(body),
                )
                if stored != encoded:
                    raise RawCaptureCorrupt(
                        f"Non-deterministic content-addressed blob: {blob_key}"
                    )
            else:
                self._write_bytes(blob_key, encoded)
                _, stored = self._verify_blob(
                    blob_key,
                    expected_body=body,
                    expected_hash=content_hash,
                    expected_decoded_bytes=len(body),
                    expected_stored_bytes=len(encoded),
                )
                if stored != encoded:
                    raise RawCaptureCorrupt(f"Blob write mismatch: {blob_key}")

            if self._exists(manifest_key):
                existing_body, existing = self.load_capture(capture_id)
                comparable = (
                    record
                    if fetched_at is not None
                    else replace(record, fetched_at=existing.fetched_at)
                )
                if existing_body != body or asdict(existing) != asdict(comparable):
                    raise RawCaptureConflict(
                        f"Raw capture manifest is immutable: {capture_id}"
                    )
                return existing
            self._write_bytes(manifest_key, rendered)
            if self._read_bytes(manifest_key) != rendered:
                raise RawCaptureConflict(
                    f"Raw capture manifest write mismatch: {capture_id}"
                )
        return record

    def load_capture(self, capture_id: str) -> tuple[bytes, RawCaptureRecord]:
        """Load and fully verify exact response bytes for offline replay."""

        key = self.capture_manifest_key(capture_id)
        try:
            payload = json.loads(self._read_bytes(key).decode("utf-8"))
            record = RawCaptureRecord(**payload)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
            raise RawCaptureCorrupt(f"Invalid raw capture manifest: {key}") from exc
        try:
            expected_capture_id = self.allocate_capture_id(
                cycle_id=record.cycle_id,
                scope_id=record.scope_id,
                endpoint=record.endpoint,
                attempt=record.attempt,
                url=record.url,
                status_code=record.status_code,
                content_hash=record.content_hash,
            )
            expected_blob_key = self.blob_key(record.content_hash)
            persisted_headers = _safe_headers(record.headers)
        except (AttributeError, TypeError, ValueError) as exc:
            raise RawCaptureCorrupt(f"Invalid capture identity: {key}") from exc
        if (
            record.manifest_version != RAW_MANIFEST_VERSION
            or record.source != "transfermarkt"
            or record.capture_id != capture_id
            or expected_capture_id != capture_id
            or record.hash_algorithm != "sha256"
            or record.compression != "gzip"
            or record.blob_key != expected_blob_key
            or dict(record.headers) != persisted_headers
            or record.content_type != record.headers.get("content-type")
        ):
            raise RawCaptureCorrupt(f"Capture identity mismatch: {key}")
        body, _ = self._verify_blob(
            record.blob_key,
            expected_hash=record.content_hash,
            expected_decoded_bytes=record.decoded_bytes,
            expected_stored_bytes=record.stored_bytes,
        )
        return body, record

    def replay(self, capture_id: str) -> bytes:
        """Return verified parser input without any source request."""

        return self.load_capture(capture_id)[0]


RawStore = RawResponseStore
