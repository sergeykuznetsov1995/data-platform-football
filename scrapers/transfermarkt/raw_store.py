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
ATTEMPT_ENVELOPE_VERSION = "transfermarkt-attempt-v1"
MAX_ATTEMPT_ORDINAL = 1_000_000_000
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
_ERROR_TYPE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$", re.ASCII)
_ATTEMPT_KINDS = frozenset({"response", "transport_error"})
_TRANSPORT_ERROR_KINDS = frozenset(
    {"connection", "dns", "protocol", "proxy", "timeout", "tls", "transport"}
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


def _error_kind(value: object) -> str:
    candidate = _required(value, "error_kind").lower()
    if candidate not in _TRANSPORT_ERROR_KINDS:
        raise ValueError("error_kind is not an allowlisted transport class")
    return candidate


def _error_type(value: object) -> str:
    candidate = _required(value, "error_type")
    if _ERROR_TYPE_RE.fullmatch(candidate) is None:
        raise ValueError("error_type must be a safe exception class name")
    return candidate


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


@dataclass(frozen=True)
class RawAttemptEnvelopeRecord:
    """Immutable, body-free evidence for one HTTP attempt.

    A response envelope references the existing v1 capture contract.  A
    transport-error envelope deliberately contains neither response bytes nor
    third-party exception text: only an allowlisted failure class is durable,
    so proxy credentials and request secrets cannot escape through an
    exception message.
    """

    manifest_version: str
    envelope_id: str
    record_hash: str
    hash_algorithm: str
    source: str
    outcome_kind: str
    cycle_id: str
    scope_id: str
    endpoint: str
    attempt: int
    url: str
    observed_at: str
    capture_id: Optional[str]
    capture_manifest_uri: Optional[str]
    raw_body_hash: Optional[str]
    status_code: Optional[int]
    error_kind: Optional[str]
    error_type: Optional[str]
    envelope_uri: str


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
    def attempt_manifest_key(envelope_id: str) -> str:
        if _SHA256_RE.fullmatch(str(envelope_id)) is None:
            raise ValueError("envelope_id must be a lowercase SHA-256 digest")
        return f"attempts/sha256/{envelope_id[:2]}/{envelope_id}.json"

    @staticmethod
    def _attempt_identity(
        *,
        outcome_kind: str,
        cycle_id: str,
        scope_id: str,
        endpoint: str,
        attempt: int,
        url: str,
        observed_at: str,
        capture_id: Optional[str],
        capture_manifest_uri: Optional[str],
        raw_body_hash: Optional[str],
        status_code: Optional[int],
        error_kind: Optional[str],
        error_type: Optional[str],
    ) -> dict[str, object]:
        kind = _required(outcome_kind, "outcome_kind").lower()
        if kind not in _ATTEMPT_KINDS:
            raise ValueError("outcome_kind is not supported")
        identity: dict[str, object] = {
            "attempt": _integer(
                attempt, "attempt", minimum=0, maximum=MAX_ATTEMPT_ORDINAL
            ),
            "capture_id": capture_id,
            "capture_manifest_uri": capture_manifest_uri,
            "cycle_id": _required(cycle_id, "cycle_id"),
            "endpoint": _required(endpoint, "endpoint"),
            "error_kind": error_kind,
            "error_type": error_type,
            "manifest_version": ATTEMPT_ENVELOPE_VERSION,
            "observed_at": _utc_iso(observed_at),
            "outcome_kind": kind,
            "raw_body_hash": raw_body_hash,
            "scope_id": _required(scope_id, "scope_id"),
            "source": "transfermarkt",
            "status_code": status_code,
            "url": _safe_url(url),
        }
        if kind == "response":
            if (
                not isinstance(capture_id, str)
                or _SHA256_RE.fullmatch(capture_id) is None
                or not isinstance(raw_body_hash, str)
                or _SHA256_RE.fullmatch(raw_body_hash) is None
                or type(capture_manifest_uri) is not str
                or not capture_manifest_uri
            ):
                raise ValueError("response envelope requires capture lineage")
            identity["status_code"] = _integer(
                status_code, "status_code", minimum=100, maximum=599
            )
            if error_kind is not None or error_type is not None:
                raise ValueError("response envelope cannot contain transport error")
        else:
            if any(
                value is not None
                for value in (
                    capture_id,
                    capture_manifest_uri,
                    raw_body_hash,
                    status_code,
                )
            ):
                raise ValueError("transport envelope cannot reference response data")
            identity["error_kind"] = _error_kind(error_kind)
            identity["error_type"] = _error_type(error_type)
        return identity

    @classmethod
    def allocate_attempt_envelope_id(cls, **values: object) -> str:
        """Hash the canonical envelope body, excluding derived ID/URI fields."""

        supplied = dict(values)
        manifest_version = supplied.pop("manifest_version", ATTEMPT_ENVELOPE_VERSION)
        source = supplied.pop("source", "transfermarkt")
        if (
            manifest_version != ATTEMPT_ENVELOPE_VERSION
            or source != "transfermarkt"
        ):
            raise ValueError("attempt envelope contract identity mismatch")
        identity = cls._attempt_identity(**supplied)  # type: ignore[arg-type]
        # Storage locations are verified derived fields, not evidence identity:
        # the same attempt keeps one ID when copied to another raw-store root.
        canonical = dict(identity)
        canonical.pop("capture_manifest_uri", None)
        encoded = json.dumps(
            canonical, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

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
            "attempt": _integer(
                attempt,
                "attempt",
                minimum=0,
                maximum=MAX_ATTEMPT_ORDINAL,
            ),
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
        ordinal = _integer(
            attempt,
            "attempt",
            minimum=0,
            maximum=MAX_ATTEMPT_ORDINAL,
        )
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

    def _publish_attempt_envelope(
        self,
        identity: Mapping[str, object],
    ) -> RawAttemptEnvelopeRecord:
        envelope_id = self.allocate_attempt_envelope_id(**dict(identity))
        manifest_key = self.attempt_manifest_key(envelope_id)
        record = RawAttemptEnvelopeRecord(
            manifest_version=ATTEMPT_ENVELOPE_VERSION,
            envelope_id=envelope_id,
            record_hash=envelope_id,
            hash_algorithm="sha256",
            source="transfermarkt",
            outcome_kind=str(identity["outcome_kind"]),
            cycle_id=str(identity["cycle_id"]),
            scope_id=str(identity["scope_id"]),
            endpoint=str(identity["endpoint"]),
            attempt=int(identity["attempt"]),
            url=str(identity["url"]),
            observed_at=str(identity["observed_at"]),
            capture_id=identity["capture_id"],  # type: ignore[arg-type]
            capture_manifest_uri=identity["capture_manifest_uri"],  # type: ignore[arg-type]
            raw_body_hash=identity["raw_body_hash"],  # type: ignore[arg-type]
            status_code=identity["status_code"],  # type: ignore[arg-type]
            error_kind=identity["error_kind"],  # type: ignore[arg-type]
            error_type=identity["error_type"],  # type: ignore[arg-type]
            envelope_uri=self._uri(manifest_key),
        )
        rendered = json.dumps(
            asdict(record), ensure_ascii=False, indent=2, sort_keys=True
        ).encode("utf-8") + b"\n"
        with self._write_lock:
            self._publish_immutable_bytes(
                manifest_key,
                rendered,
                require_exact_bytes=True,
            )
            existing = self.load_attempt_envelope(envelope_id)
            if asdict(existing) != asdict(record):
                raise RawCaptureConflict(
                    f"Raw attempt envelope is immutable: {envelope_id}"
                )
            return existing

    def store_response_envelope(
        self,
        capture: RawCaptureRecord,
    ) -> RawAttemptEnvelopeRecord:
        """Publish body-free attempt evidence referencing a verified capture."""

        if not isinstance(capture, RawCaptureRecord):
            raise TypeError("capture must be a RawCaptureRecord")
        _, committed = self.load_capture(capture.capture_id)
        if asdict(committed) != asdict(capture):
            raise RawCaptureConflict(
                f"Response envelope capture drift: {capture.capture_id}"
            )
        identity = self._attempt_identity(
            outcome_kind="response",
            cycle_id=committed.cycle_id,
            scope_id=committed.scope_id,
            endpoint=committed.endpoint,
            attempt=committed.attempt,
            url=committed.url,
            observed_at=committed.fetched_at,
            capture_id=committed.capture_id,
            capture_manifest_uri=self._uri(
                self.capture_manifest_key(committed.capture_id)
            ),
            raw_body_hash=committed.content_hash,
            status_code=committed.status_code,
            error_kind=None,
            error_type=None,
        )
        return self._publish_attempt_envelope(identity)

    def store_transport_error(
        self,
        *,
        url: str,
        fetched_at: Optional[str],
        cycle_id: str,
        scope_id: str,
        endpoint: str,
        attempt: int,
        error_kind: str,
        error_type: str,
    ) -> RawAttemptEnvelopeRecord:
        """Publish a body-free transport failure without exception text."""

        identity = self._attempt_identity(
            outcome_kind="transport_error",
            cycle_id=cycle_id,
            scope_id=scope_id,
            endpoint=endpoint,
            attempt=attempt,
            url=url,
            observed_at=_utc_iso(fetched_at),
            capture_id=None,
            capture_manifest_uri=None,
            raw_body_hash=None,
            status_code=None,
            error_kind=error_kind,
            error_type=error_type,
        )
        return self._publish_attempt_envelope(identity)

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
                record.attempt,
                "attempt",
                minimum=0,
                maximum=MAX_ATTEMPT_ORDINAL,
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

    def load_attempt_envelope(
        self,
        envelope_id: str,
    ) -> RawAttemptEnvelopeRecord:
        """Load and verify an envelope and its response capture, if present."""

        key = self.attempt_manifest_key(envelope_id)
        try:
            payload = json.loads(self._read_bytes(key).decode("utf-8"))
            if type(payload) is not dict:
                raise TypeError("manifest must be an object")
            record = RawAttemptEnvelopeRecord(**payload)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
            raise RawCaptureCorrupt(
                f"Invalid raw attempt envelope: {key}"
            ) from exc
        try:
            text_fields = (
                record.manifest_version,
                record.envelope_id,
                record.record_hash,
                record.hash_algorithm,
                record.source,
                record.outcome_kind,
                record.cycle_id,
                record.scope_id,
                record.endpoint,
                record.url,
                record.observed_at,
                record.envelope_uri,
            )
            if any(
                type(value) is not str or not value or value != value.strip()
                for value in text_fields
            ):
                raise TypeError("envelope string field has an invalid type")
            identity = self._attempt_identity(
                outcome_kind=record.outcome_kind,
                cycle_id=record.cycle_id,
                scope_id=record.scope_id,
                endpoint=record.endpoint,
                attempt=record.attempt,
                url=record.url,
                observed_at=record.observed_at,
                capture_id=record.capture_id,
                capture_manifest_uri=record.capture_manifest_uri,
                raw_body_hash=record.raw_body_hash,
                status_code=record.status_code,
                error_kind=record.error_kind,
                error_type=record.error_type,
            )
            expected_id = self.allocate_attempt_envelope_id(**identity)
        except (AttributeError, TypeError, ValueError) as exc:
            raise RawCaptureCorrupt(
                f"Invalid raw attempt envelope identity: {key}"
            ) from exc
        stored_identity = {
            "attempt": record.attempt,
            "capture_id": record.capture_id,
            "capture_manifest_uri": record.capture_manifest_uri,
            "cycle_id": record.cycle_id,
            "endpoint": record.endpoint,
            "error_kind": record.error_kind,
            "error_type": record.error_type,
            "manifest_version": record.manifest_version,
            "observed_at": record.observed_at,
            "outcome_kind": record.outcome_kind,
            "raw_body_hash": record.raw_body_hash,
            "scope_id": record.scope_id,
            "source": record.source,
            "status_code": record.status_code,
            "url": record.url,
        }
        if (
            record.manifest_version != ATTEMPT_ENVELOPE_VERSION
            or record.envelope_id != envelope_id
            or record.record_hash != envelope_id
            or expected_id != envelope_id
            or stored_identity != identity
            or record.hash_algorithm != "sha256"
            or record.source != "transfermarkt"
            or record.envelope_uri != self._uri(key)
        ):
            raise RawCaptureCorrupt(f"Attempt envelope identity mismatch: {key}")

        if record.outcome_kind == "response":
            assert record.capture_id is not None
            _, capture = self.load_capture(record.capture_id)
            if (
                record.capture_manifest_uri
                != self._uri(self.capture_manifest_key(capture.capture_id))
                or record.raw_body_hash != capture.content_hash
                or record.status_code != capture.status_code
                or record.cycle_id != capture.cycle_id
                or record.scope_id != capture.scope_id
                or record.endpoint != capture.endpoint
                or record.attempt != capture.attempt
                or record.url != capture.url
                or record.observed_at != capture.fetched_at
            ):
                raise RawCaptureCorrupt(
                    f"Attempt envelope capture mismatch: {key}"
                )
        return record

    def verify_attempt_envelope(
        self,
        envelope_id: str,
    ) -> RawAttemptEnvelopeRecord:
        """Verify immutable attempt evidence and return its typed record."""

        return self.load_attempt_envelope(envelope_id)

    def load_attempt(self, envelope_id: str) -> RawAttemptEnvelopeRecord:
        """Short alias for callers that already model the object as an attempt."""

        return self.load_attempt_envelope(envelope_id)

    def verify_attempt(self, envelope_id: str) -> RawAttemptEnvelopeRecord:
        """Short alias for full attempt-envelope verification."""

        return self.verify_attempt_envelope(envelope_id)

    def replay_attempt(self, envelope_id: str) -> Optional[bytes]:
        """Replay verified response bytes; transport failures have no body."""

        record = self.load_attempt_envelope(envelope_id)
        if record.capture_id is None:
            return None
        return self.load_capture(record.capture_id)[0]

    def replay(self, capture_id: str) -> bytes:
        """Return verified parser input without any source request."""

        return self.load_capture(capture_id)[0]


RawStore = RawResponseStore
