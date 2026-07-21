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
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Mapping, Optional
from urllib.parse import parse_qsl, urlparse

from pyarrow import fs


RAW_MANIFEST_VERSION = "transfermarkt-raw-v1"
RAW_STORE_ENV = "TRANSFERMARKT_RAW_STORE_URI"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_S3_BUCKET_RE = re.compile(
    r"^[a-z0-9](?:[a-z0-9.-]{1,61}[a-z0-9])$",
    re.ASCII,
)
_TRANSFERMARKT_HTML_HOSTS = frozenset({"transfermarkt.com", "www.transfermarkt.com"})
_TRANSFERMARKT_API_HOST = "tmapi.transfermarkt.technology"
_TRANSFERMARKT_HOSTS = _TRANSFERMARKT_HTML_HOSTS | {_TRANSFERMARKT_API_HOST}
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
_SAFE_TRANSFERMARKT_QUERY_FIELDS = frozenset(
    {"page", "saison_id", "season_id", "sort"}
)
_SAFE_QUERY_VALUE_RE = re.compile(r"^[A-Za-z0-9._~-]{0,128}$")
_API_REGULATION_PATH_RE = re.compile(
    r"^/competition/[A-Za-z0-9_-]+/regulation$", re.ASCII
)
_API_CLUB_PATH_RE = re.compile(r"^/competition/[A-Za-z0-9_-]+/club$", re.ASCII)


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
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def _utc_iso(value: Optional[str]) -> str:
    """Return one canonical, timezone-aware UTC timestamp."""

    if value is None:
        return utc_now_iso()
    candidate = _required(value, "fetched_at")
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("fetched_at must be an ISO-8601 timestamp") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("fetched_at must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat()


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
        or (parsed.hostname or "").lower() not in _TRANSFERMARKT_HOSTS
        or parsed.username is not None
        or parsed.password is not None
        or "@" in parsed.netloc
        or port not in {None, 80, 443}
        or parsed.fragment
    ):
        raise ValueError(invalid)
    try:
        query = parse_qsl(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=True,
            max_num_fields=32,
        )
    except ValueError:
        raise ValueError(invalid) from None
    host = (parsed.hostname or "").lower()
    if host == _TRANSFERMARKT_API_HOST:
        if _API_REGULATION_PATH_RE.fullmatch(parsed.path):
            if query:
                raise ValueError(invalid)
        elif _API_CLUB_PATH_RE.fullmatch(parsed.path):
            if (
                len(query) != 1
                or query[0][0] != "season"
                or re.fullmatch(r"\d{4}", query[0][1]) is None
            ):
                raise ValueError(invalid)
        elif parsed.path == "/clubs":
            if (
                not query
                or len(query) > 250
                or any(
                    name != "ids[]" or re.fullmatch(r"\d+", query_value) is None
                    for name, query_value in query
                )
            ):
                raise ValueError(invalid)
        else:
            raise ValueError(invalid)
    else:
        for name, query_value in query:
            if (
                name.strip().lower() not in _SAFE_TRANSFERMARKT_QUERY_FIELDS
                or _SAFE_QUERY_VALUE_RE.fullmatch(query_value) is None
            ):
                raise ValueError(invalid)
    return candidate


def _safe_headers(headers: Optional[Mapping[str, object]]) -> dict[str, str]:
    if headers is None:
        return {}
    if not isinstance(headers, Mapping):
        raise TypeError("headers must be a mapping")
    persisted: dict[str, str] = {}
    for key, value in headers.items():
        if type(key) is not str or type(value) is not str:
            raise TypeError("header names and values must be strings")
        name = str(key).strip().lower()
        if name not in _SAFE_RESPONSE_HEADERS:
            continue
        rendered = value.strip()
        if "\r" in rendered or "\n" in rendered or "\x00" in rendered:
            continue
        persisted[name] = rendered
    return persisted


def _integer(value: object, name: str, *, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer in {minimum}..{maximum}")
    return value


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
    raw_uri: str
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
        if uri_prefix is None:
            if isinstance(filesystem, fs.LocalFileSystem):
                resolved_prefix = Path(normalized_root).resolve().as_uri()
            elif isinstance(filesystem, fs.S3FileSystem):
                resolved_prefix = f"s3://{normalized_root}"
            else:
                raise ValueError("uri_prefix is required for this filesystem")
        else:
            resolved_prefix = uri_prefix.rstrip("/")
        try:
            parsed_prefix = urlparse(resolved_prefix)
        except (TypeError, ValueError):
            raise ValueError("Invalid credential-free raw-store URI") from None
        if (
            parsed_prefix.scheme not in {"file", "s3"}
            or parsed_prefix.username is not None
            or parsed_prefix.password is not None
            or "@" in parsed_prefix.netloc
            or parsed_prefix.query
            or parsed_prefix.fragment
            or parsed_prefix.params
        ):
            raise ValueError("Invalid credential-free raw-store URI")
        if parsed_prefix.scheme == "file" and parsed_prefix.netloc not in {
            "",
            "localhost",
        }:
            raise ValueError("Invalid credential-free raw-store URI")
        if parsed_prefix.scheme == "s3" and (
            _S3_BUCKET_RE.fullmatch(parsed_prefix.hostname or "") is None
            or parsed_prefix.netloc != parsed_prefix.hostname
        ):
            raise ValueError("Invalid credential-free raw-store URI")
        self.uri_prefix = resolved_prefix
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
            if (
                not bucket
                or port is not None
                or parsed.netloc != bucket
                or _S3_BUCKET_RE.fullmatch(bucket) is None
            ):
                raise ValueError(invalid)
            dedicated_access = os.environ.get(
                "TRANSFERMARKT_RAW_S3_ACCESS_KEY", ""
            ).strip()
            dedicated_secret = os.environ.get(
                "TRANSFERMARKT_RAW_S3_SECRET_KEY", ""
            ).strip()
            if bool(dedicated_access) != bool(dedicated_secret):
                raise RawStoreError(
                    "TRANSFERMARKT_RAW_S3_ACCESS_KEY and "
                    "TRANSFERMARKT_RAW_S3_SECRET_KEY must be set together"
                )
            # A dedicated least-privilege raw-store pair wins.  The shared
            # platform pair remains the documented compatibility fallback.
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

        if parsed.scheme == "file" and parsed.netloc not in {"", "localhost"}:
            raise ValueError(invalid)
        try:
            filesystem, root = fs.FileSystem.from_uri(uri)
        except (TypeError, ValueError):
            raise ValueError(invalid) from None
        root = root.rstrip("/")
        if not root:
            raise ValueError(invalid)
        prefix = (
            Path(root).resolve().as_uri()
            if isinstance(filesystem, fs.LocalFileSystem)
            else uri
        )
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

    def _uri(self, relative: str) -> str:
        return f"{self.uri_prefix}/{relative.lstrip('/')}"

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

    def _publish_immutable_bytes(
        self,
        relative: str,
        payload: bytes,
        *,
        require_exact_bytes: bool,
    ) -> bytes:
        """Create an object once and return the bytes that won publication.

        Local publication uses an atomic hard-link from a complete temporary
        file, so another process can win without either writer replacing the
        other's object.  Arrow's S3 API has no conditional PutObject surface;
        S3 manifests are therefore keyed by every evidence field and verified
        byte-for-byte after Put.  Blob contenders may use different valid gzip
        encodings, but their hash key binds the same decoded response bytes.
        """

        path = self._path(relative)
        parent = str(PurePosixPath(path).parent)
        self.filesystem.create_dir(parent, recursive=True)
        if isinstance(self.filesystem, fs.LocalFileSystem):
            temporary = f"{path}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
            descriptor: Optional[int] = None
            try:
                descriptor = os.open(
                    temporary,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                )
                with os.fdopen(descriptor, "wb") as stream:
                    descriptor = None
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                try:
                    os.link(temporary, path)
                    directory = os.open(parent, os.O_RDONLY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
                except FileExistsError:
                    pass
            finally:
                if descriptor is not None:
                    os.close(descriptor)
                try:
                    os.unlink(temporary)
                except FileNotFoundError:
                    pass
        elif not self._exists(relative):
            self._write_bytes(relative, payload)

        committed = self._read_bytes(relative)
        if require_exact_bytes and committed != payload:
            raise RawCaptureConflict(f"Immutable raw object conflict: {relative}")
        return committed

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
        headers: Mapping[str, object],
        fetched_at: str,
        content_hash: str,
    ) -> str:
        if _SHA256_RE.fullmatch(content_hash) is None:
            raise ValueError("content_hash must be a lowercase SHA-256 digest")
        identity = {
            "attempt": _integer(attempt, "attempt", minimum=0, maximum=1_000_000),
            "content_hash": content_hash,
            "cycle_id": _required(cycle_id, "cycle_id"),
            "endpoint": _required(endpoint, "endpoint"),
            "fetched_at": _utc_iso(fetched_at),
            "headers": _safe_headers(headers),
            "scope_id": _required(scope_id, "scope_id"),
            "status_code": _integer(
                status_code, "status_code", minimum=100, maximum=599
            ),
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
        ordinal = _integer(attempt, "attempt", minimum=0, maximum=1_000_000)
        status = _integer(
            status_code, "status_code", minimum=100, maximum=599
        )
        persisted_headers = _safe_headers(headers)
        observed_at = _utc_iso(fetched_at)
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
            headers=persisted_headers,
            fetched_at=observed_at,
            content_hash=content_hash,
        )
        manifest_key = self.capture_manifest_key(capture_id)

        with self._write_lock:
            self._publish_immutable_bytes(
                blob_key,
                encoded,
                require_exact_bytes=False,
            )
            _, stored = self._verify_blob(
                blob_key,
                expected_body=body,
                expected_hash=content_hash,
                expected_decoded_bytes=len(body),
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
                raw_uri=self._uri(blob_key),
                compression="gzip",
                fetched_at=observed_at,
                decoded_bytes=len(body),
                stored_bytes=len(stored),
            )
            rendered = json.dumps(
                asdict(record), ensure_ascii=False, indent=2, sort_keys=True
            ).encode("utf-8") + b"\n"
            self._publish_immutable_bytes(
                manifest_key,
                rendered,
                require_exact_bytes=True,
            )
            existing_body, existing = self.load_capture(capture_id)
            if existing_body != body or asdict(existing) != asdict(record):
                raise RawCaptureConflict(
                    f"Raw capture manifest is immutable: {capture_id}"
                )
            return existing

    def load_capture(self, capture_id: str) -> tuple[bytes, RawCaptureRecord]:
        """Load and fully verify exact response bytes for offline replay."""

        key = self.capture_manifest_key(capture_id)
        try:
            payload = json.loads(self._read_bytes(key).decode("utf-8"))
            if type(payload) is not dict:
                raise TypeError("manifest must be an object")
            record = RawCaptureRecord(**payload)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
            raise RawCaptureCorrupt(f"Invalid raw capture manifest: {key}") from exc
        try:
            text_fields = (
                record.manifest_version,
                record.capture_id,
                record.source,
                record.cycle_id,
                record.scope_id,
                record.endpoint,
                record.url,
                record.content_hash,
                record.hash_algorithm,
                record.blob_key,
                record.raw_uri,
                record.compression,
                record.fetched_at,
            )
            if any(type(value) is not str or value != value.strip() for value in text_fields):
                raise TypeError("manifest string field has an invalid type")
            if type(record.headers) is not dict:
                raise TypeError("manifest headers must be an object")
            if record.content_type is not None and type(record.content_type) is not str:
                raise TypeError("manifest content_type must be a string or null")
            ordinal = _integer(
                record.attempt, "attempt", minimum=0, maximum=1_000_000
            )
            status = _integer(
                record.status_code,
                "status_code",
                minimum=100,
                maximum=599,
            )
            decoded_bytes = _integer(
                record.decoded_bytes,
                "decoded_bytes",
                minimum=0,
                maximum=2**63 - 1,
            )
            stored_bytes = _integer(
                record.stored_bytes,
                "stored_bytes",
                minimum=1,
                maximum=2**63 - 1,
            )
            normalized_fetched_at = _utc_iso(record.fetched_at)
            safe_record_url = _safe_url(record.url)
            persisted_headers = _safe_headers(record.headers)
            expected_capture_id = self.allocate_capture_id(
                cycle_id=record.cycle_id,
                scope_id=record.scope_id,
                endpoint=record.endpoint,
                attempt=ordinal,
                url=safe_record_url,
                status_code=status,
                headers=persisted_headers,
                fetched_at=normalized_fetched_at,
                content_hash=record.content_hash,
            )
            expected_blob_key = self.blob_key(record.content_hash)
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
            or record.raw_uri != self._uri(expected_blob_key)
            or record.url != safe_record_url
            or record.fetched_at != normalized_fetched_at
            or dict(record.headers) != persisted_headers
            or record.content_type != record.headers.get("content-type")
        ):
            raise RawCaptureCorrupt(f"Capture identity mismatch: {key}")
        body, _ = self._verify_blob(
            record.blob_key,
            expected_hash=record.content_hash,
            expected_decoded_bytes=decoded_bytes,
            expected_stored_bytes=stored_bytes,
        )
        return body, record

    def replay(self, capture_id: str) -> bytes:
        """Return verified parser input without any source request."""

        return self.load_capture(capture_id)[0]


RawStore = RawResponseStore
