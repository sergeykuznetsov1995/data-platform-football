"""Durable raw-first storage for FBref pages and parse manifests."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import uuid
import zlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Callable, Mapping, Optional, Sequence
from urllib.parse import urlparse

from pyarrow import fs


RAW_MANIFEST_VERSION = "fbref-raw-v1"
RAW_MANIFEST_VERSION_V2 = "fbref-raw-v2"
FETCHER_VERSION = "fbref-match-loader-v1"
FETCHER_VERSION_V2 = "fbref-warm-http-v2"
RAW_V1_BRIDGE_VERSION = "fbref-raw-v1-bridge-v1"
RAW_V2_RESUME_VERSION = "fbref-raw-v2-resume-v1"
_MATCH_ID_RE = re.compile(r"^[0-9a-fA-F]{8}$")
_MATCH_URL_RE = re.compile(r"/en/matches/([0-9a-fA-F]{8})(?:/|$)")
_FBREF_HOSTS = {"fbref.com", "www.fbref.com"}
_PERSISTED_RESPONSE_HEADERS = {
    "cache-control",
    "cf-cache-status",
    "cf-ray",
    "content-encoding",
    "content-length",
    "content-type",
    "date",
    "etag",
    "last-modified",
    "retry-after",
    "server",
    "vary",
    "x-cache",
}


class RawStoreError(RuntimeError):
    """Base error for raw storage failures."""


class RawPageNotFound(RawStoreError):
    """The target has no committed page manifest or blob."""


class RawPageCorrupt(RawStoreError):
    """A stored blob does not match its page manifest."""


@dataclass(frozen=True)
class PageTarget:
    """Canonical source page identity, reusable for future page kinds."""

    source: str
    page_kind: str
    target_id: str
    canonical_url: str
    source_ids: Mapping[str, str]


@dataclass(frozen=True)
class RawPageRecord:
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
    fetched_at: str
    fetcher_version: str
    decoded_bytes: int
    encoded_bytes: int


@dataclass(frozen=True)
class RawFetchRecord:
    """Append-only evidence for one logical HTTP refresh.

    ``response_*`` identifies the exact bytes returned by HTTP.  ``content_*``
    identifies the effective HTML used by parsers; for a 304 it references the
    previously committed content while the response blob remains the exact
    (normally empty) 304 body.
    """

    manifest_version: str
    logical_refresh_id: str
    attempt_id: Optional[str]
    source: str
    page_kind: str
    target_id: str
    canonical_url: str
    source_ids: Mapping[str, str]
    http_status: int
    content_hash: str
    response_hash: str
    hash_algorithm: str
    blob_key: str
    response_blob_key: str
    compression: str
    fetched_at: str
    fetcher_version: str
    transport_version: Optional[str]
    session_version: Optional[str]
    decoded_bytes: int
    response_bytes: int
    encoded_bytes: int
    response_encoded_bytes: int
    wire_bytes: Optional[int]
    provider_billed_bytes: Optional[int]
    latency_ms: Optional[int]
    etag: Optional[str]
    last_modified: Optional[str]
    headers: Mapping[str, str]
    previous_content_hash: Optional[str]
    content_changed: bool
    not_modified: bool
    imported_from_manifest_key: Optional[str] = None
    # Defaults keep pre-observation raw-v2 manifests readable.  The loader
    # upgrades a missing history to the manifest's final status.
    http_requests: int = 1
    http_status_history: tuple[int, ...] = ()
    browser_bootstrap_attempts: int = 0
    browser_unobserved_bytes: int = 0


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonicalize_fbref_url(url: str) -> str:
    """Return one stable HTTPS URL for an absolute or relative FBref URL."""
    candidate = str(url).strip()
    if not candidate:
        raise ValueError("FBref URL must not be empty")

    parsed = urlparse(candidate)
    if parsed.scheme and parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError(f"Not an HTTP(S) FBref URL: {url!r}")
    if parsed.scheme and not parsed.netloc:
        raise ValueError(f"Invalid absolute FBref URL: {url!r}")
    if parsed.netloc:
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValueError(f"Invalid FBref URL: {url!r}") from exc
        if (
            (parsed.hostname or "").lower() not in _FBREF_HOSTS
            or parsed.username is not None
            or parsed.password is not None
            or port not in {None, 80, 443}
        ):
            raise ValueError(f"Not an FBref URL: {url!r}")

    path = parsed.path or "/"
    if not path.startswith("/"):
        path = f"/{path}"
    if path != "/":
        path = path.rstrip("/")
    return f"https://fbref.com{path}"


def _source_id(value: object, name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def competition_index_target() -> PageTarget:
    """Return the stable target for FBref's competition index."""
    return PageTarget(
        source="fbref",
        page_kind="competition_index",
        target_id="fbref:competition_index:all",
        canonical_url=canonicalize_fbref_url("/en/comps/"),
        source_ids={"competition_index": "all"},
    )


def competition_page_target(comp_id: object, discovered_url: str) -> PageTarget:
    """Build a competition target from the exact URL found during discovery."""
    competition_id = _source_id(comp_id, "comp_id")
    return PageTarget(
        source="fbref",
        page_kind="competition",
        target_id=f"fbref:competition:{competition_id}",
        canonical_url=canonicalize_fbref_url(discovered_url),
        source_ids={"competition_id": competition_id},
    )


def season_page_target(
    comp_id: object,
    season_id: object,
    discovered_url: str,
) -> PageTarget:
    """Build a season target without deriving or rewriting its discovered URL."""
    competition_id = _source_id(comp_id, "comp_id")
    normalized_season_id = _source_id(season_id, "season_id")
    return PageTarget(
        source="fbref",
        page_kind="season",
        target_id=f"fbref:season:{competition_id}:{normalized_season_id}",
        canonical_url=canonicalize_fbref_url(discovered_url),
        source_ids={
            "competition_id": competition_id,
            "season_id": normalized_season_id,
        },
    )


def schedule_page_target(
    comp_id: object,
    season_id: object,
    discovered_url: str,
) -> PageTarget:
    """Build a schedule target without deriving or rewriting its discovered URL."""
    competition_id = _source_id(comp_id, "comp_id")
    normalized_season_id = _source_id(season_id, "season_id")
    return PageTarget(
        source="fbref",
        page_kind="schedule",
        target_id=f"fbref:schedule:{competition_id}:{normalized_season_id}",
        canonical_url=canonicalize_fbref_url(discovered_url),
        source_ids={
            "competition_id": competition_id,
            "season_id": normalized_season_id,
        },
    )


def match_page_target(match_or_url: str) -> PageTarget:
    """Normalize a match id or any FBref match URL to one target."""
    candidate = str(match_or_url).strip()
    match_id = candidate if _MATCH_ID_RE.fullmatch(candidate) else None
    if match_id is None:
        canonical = canonicalize_fbref_url(candidate)
        parsed = urlparse(canonical)
        found = _MATCH_URL_RE.search(parsed.path)
        match_id = found.group(1) if found else None
    if match_id is None:
        raise ValueError(f"Could not extract FBref match id: {match_or_url!r}")
    match_id = match_id.lower()
    return PageTarget(
        source="fbref",
        page_kind="match",
        target_id=f"fbref:match:{match_id}",
        canonical_url=f"https://fbref.com/en/matches/{match_id}",
        source_ids={"match_id": match_id},
    )


class RawPageStore:
    """Content-addressed gzip blobs plus JSON manifests on Arrow filesystems."""

    def __init__(self, filesystem: fs.FileSystem, root: str) -> None:
        self.filesystem = filesystem
        self.root = root.strip("/") if not root.startswith("/") else root.rstrip("/")

    @classmethod
    def from_uri(cls, uri: str) -> "RawPageStore":
        parsed = urlparse(uri)
        if parsed.scheme == "s3":
            endpoint = os.environ.get("FBREF_RAW_S3_ENDPOINT", "seaweedfs:8333")
            scheme = os.environ.get("FBREF_RAW_S3_SCHEME", "http")
            filesystem = fs.S3FileSystem(
                access_key=os.environ.get("S3_ACCESS_KEY"),
                secret_key=os.environ.get("S3_SECRET_KEY"),
                endpoint_override=endpoint,
                scheme=scheme,
                region=os.environ.get("FBREF_RAW_S3_REGION", "us-east-1"),
                background_writes=False,
            )
            root = f"{parsed.netloc}/{parsed.path.lstrip('/')}".rstrip("/")
            if not parsed.netloc:
                raise ValueError("S3 raw-store URI must contain a bucket")
            return cls(filesystem, root)
        filesystem, root = fs.FileSystem.from_uri(uri)
        return cls(filesystem, root.rstrip("/"))

    @classmethod
    def from_env(cls, optional: bool = True) -> Optional["RawPageStore"]:
        uri = os.environ.get("FBREF_RAW_STORE_URI", "").strip()
        if not uri:
            if optional:
                return None
            raise RawStoreError("FBREF_RAW_STORE_URI is required")
        return cls.from_uri(uri)

    def _path(self, relative: str) -> str:
        return str(PurePosixPath(self.root) / relative)

    def _exists(self, relative: str) -> bool:
        return self.filesystem.get_file_info(self._path(relative)).type != fs.FileType.NotFound

    def _read_bytes(self, relative: str) -> bytes:
        path = self._path(relative)
        info = self.filesystem.get_file_info(path)
        if info.type == fs.FileType.NotFound:
            raise RawPageNotFound(f"Raw object not found: {relative}")
        with self.filesystem.open_input_file(path) as stream:
            return stream.read()

    def _write_bytes(self, relative: str, payload: bytes) -> None:
        path = self._path(relative)
        parent = str(PurePosixPath(path).parent)
        self.filesystem.create_dir(parent, recursive=True)
        # Arrow otherwise auto-detects ``.gz`` and compresses our already
        # deterministic gzip payload a second time.
        with self.filesystem.open_output_stream(path, compression=None) as stream:
            stream.write(payload)

    def _read_json(self, relative: str) -> dict:
        try:
            return json.loads(self._read_bytes(relative).decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RawPageCorrupt(f"Invalid JSON manifest: {relative}") from exc

    def _write_json(self, relative: str, payload: dict) -> None:
        rendered = json.dumps(
            payload, ensure_ascii=False, indent=2, sort_keys=True
        ).encode("utf-8") + b"\n"
        if not isinstance(self.filesystem, fs.LocalFileSystem):
            # S3 objects become visible only after a successful PutObject.
            self._write_bytes(relative, rendered)
            return

        path = self._path(relative)
        parent = str(PurePosixPath(path).parent)
        temporary = f"{path}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
        self.filesystem.create_dir(parent, recursive=True)
        try:
            with self.filesystem.open_output_stream(
                temporary,
                compression=None,
            ) as stream:
                stream.write(rendered)
            os.replace(temporary, path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    @staticmethod
    def _safe_component(value: object) -> str:
        """Encode an arbitrary source id as one collision-free path component."""
        raw = _source_id(value, "path component").encode("utf-8")
        return "".join(
            chr(byte)
            if (
                ord("a") <= byte <= ord("z")
                or ord("A") <= byte <= ord("Z")
                or ord("0") <= byte <= ord("9")
                or byte in {ord("-"), ord("_")}
            )
            else f"%{byte:02X}"
            for byte in raw
        )

    @classmethod
    def _target_key(cls, page_kind: str, source_ids: Mapping[str, str], target_id: str) -> str:
        if page_kind == "competition_index":
            return "all"
        if page_kind == "competition":
            return cls._safe_component(source_ids["competition_id"])
        if page_kind in {"season", "schedule"}:
            return "/".join((
                cls._safe_component(source_ids["competition_id"]),
                cls._safe_component(source_ids["season_id"]),
            ))
        if page_kind == "match":
            return cls._safe_component(source_ids["match_id"])
        return cls._safe_component(target_id)

    @staticmethod
    def _target_manifest_key(target: PageTarget) -> str:
        if target.page_kind == "match":
            # Preserve the v1 match-manifest path exactly.
            source_id = target.source_ids.get(target.page_kind)
            if not source_id:
                source_id = target.source_ids.get("match_id")
            if not source_id:
                source_id = hashlib.sha256(target.target_id.encode()).hexdigest()
            return f"targets/{target.page_kind}/{source_id}.json"
        target_key = RawPageStore._target_key(
            target.page_kind,
            target.source_ids,
            target.target_id,
        )
        return f"targets/{target.page_kind}/{target_key}.json"

    @staticmethod
    def _blob_key(content_hash: str) -> str:
        return f"blobs/sha256/{content_hash[:2]}/{content_hash}.html.gz"

    @classmethod
    def _fetch_manifest_key(cls, logical_refresh_id: object) -> str:
        refresh = cls._safe_component(logical_refresh_id)
        return f"manifests/fetches/{refresh}.json"

    @classmethod
    def _v2_target_manifest_key(cls, target: PageTarget) -> str:
        target_key = cls._target_key(
            target.page_kind,
            target.source_ids,
            target.target_id,
        )
        return f"targets-v2/{cls._safe_component(target.page_kind)}/{target_key}.json"

    @classmethod
    def _v2_target_history_prefix(cls, target: PageTarget) -> str:
        target_key = cls._target_key(
            target.page_kind,
            target.source_ids,
            target.target_id,
        )
        return (
            "target-history-v2/"
            f"{cls._safe_component(target.page_kind)}/{target_key}"
        )

    @classmethod
    def _v2_target_history_manifest_key(
        cls,
        target: PageTarget,
        logical_refresh_id: object,
    ) -> str:
        return (
            f"{cls._v2_target_history_prefix(target)}/"
            f"{cls._safe_component(logical_refresh_id)}.json"
        )

    def _relative_path(self, path: str) -> str:
        root = self.root.rstrip("/")
        prefix = f"{root}/" if root else ""
        if prefix and path.startswith(prefix):
            return path[len(prefix):]
        if not root:
            return path.lstrip("/")
        raise RawPageCorrupt(f"Raw path escaped configured root: {path}")

    @staticmethod
    def _fetch_order(record: RawFetchRecord) -> tuple[datetime, str]:
        try:
            fetched = datetime.fromisoformat(
                str(record.fetched_at).replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise RawPageCorrupt(
                f"Invalid fetched_at for {record.logical_refresh_id}"
            ) from exc
        if fetched.tzinfo is None:
            raise RawPageCorrupt(
                f"Naive fetched_at for {record.logical_refresh_id}"
            )
        return fetched.astimezone(timezone.utc), record.logical_refresh_id

    def _latest_history_record(
        self,
        target: PageTarget,
    ) -> Optional[RawFetchRecord]:
        prefix = self._v2_target_history_prefix(target)
        infos = self.filesystem.get_file_info(
            fs.FileSelector(
                self._path(prefix),
                allow_not_found=True,
                recursive=False,
            )
        )
        records = []
        for info in infos:
            if info.type != fs.FileType.File or not info.path.endswith(".json"):
                continue
            key = self._relative_path(info.path)
            record = self._fetch_record(self._read_json(key), key)
            self._validate_recovery_identity(target, record, version="v2 history")
            records.append(record)
        if not records:
            return None
        return max(records, key=self._fetch_order)

    def _target_record_for_content_hash(
        self,
        target: PageTarget,
        content_hash: str,
    ) -> Optional[RawFetchRecord]:
        expected = str(content_hash).strip().lower()
        prefix = self._v2_target_history_prefix(target)
        infos = self.filesystem.get_file_info(
            fs.FileSelector(
                self._path(prefix),
                allow_not_found=True,
                recursive=False,
            )
        )
        matches = []
        for info in infos:
            if info.type != fs.FileType.File or not info.path.endswith(".json"):
                continue
            key = self._relative_path(info.path)
            record = self._fetch_record(self._read_json(key), key)
            self._validate_recovery_identity(target, record, version="v2 history")
            if record.content_hash == expected:
                matches.append(record)
        if matches:
            return max(matches, key=self._fetch_order)

        legacy_key = self._v2_target_manifest_key(target)
        if self._exists(legacy_key):
            record = self._fetch_record(
                self._read_json(legacy_key), legacy_key
            )
            self._validate_recovery_identity(target, record, version="v2")
            if record.content_hash == expected:
                return record
        return None

    def _publish_target_record(
        self,
        target: PageTarget,
        record: RawFetchRecord,
    ) -> None:
        """Publish one immutable pointer candidate, then refresh legacy mirror."""

        history_key = self._v2_target_history_manifest_key(
            target, record.logical_refresh_id
        )
        self._write_immutable_json(history_key, asdict(record))
        mirror_key = self._v2_target_manifest_key(target)
        for _ in range(3):
            latest = self._latest_history_record(target)
            if latest is None:  # pragma: no cover - candidate was just verified
                raise RawStoreError(f"No v2 history for {target.target_id}")
            # Compatibility mirror only. Readers select from immutable
            # history, and this post-write verification repairs ordinary
            # delayed-writer interleavings.
            self._write_json(mirror_key, asdict(latest))
            authoritative = self._latest_history_record(target)
            mirrored = self._fetch_record(
                self._read_json(mirror_key), mirror_key
            )
            if (
                authoritative is not None
                and mirrored.logical_refresh_id
                == authoritative.logical_refresh_id
            ):
                return
        raise RawStoreError(
            f"Could not converge v2 target mirror for {target.target_id}"
        )

    def _write_immutable_json(self, relative: str, payload: dict) -> None:
        normalized = json.loads(json.dumps(payload, ensure_ascii=False))
        if self._exists(relative):
            if self._read_json(relative) != normalized:
                raise RawStoreError(f"Raw manifest is immutable: {relative}")
            return
        self._write_json(relative, normalized)

    def _store_verified_blob(self, raw: bytes) -> tuple[str, str, bytes]:
        content_hash = hashlib.sha256(raw).hexdigest()
        blob_key = self._blob_key(content_hash)
        encoded = gzip.compress(raw, compresslevel=6, mtime=0)
        if not self._exists(blob_key):
            self._write_bytes(blob_key, encoded)
        stored = self._read_bytes(blob_key)
        try:
            decoded = gzip.decompress(stored)
        except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
            raise RawPageCorrupt(f"Invalid gzip blob: {blob_key}") from exc
        actual_hash = hashlib.sha256(decoded).hexdigest()
        if decoded != raw or actual_hash != content_hash:
            raise RawPageCorrupt(
                f"Content-addressed blob mismatch: {blob_key}"
            )
        return content_hash, blob_key, stored

    @staticmethod
    def _fetch_record(payload: Mapping[str, object], key: str) -> RawFetchRecord:
        normalized = dict(payload)
        default_requests = (
            0 if normalized.get("imported_from_manifest_key") else 1
        )
        normalized.setdefault("http_requests", default_requests)
        normalized.setdefault(
            "http_status_history",
            (
                [int(normalized.get("http_status") or 0)]
                if int(normalized["http_requests"])
                else []
            ),
        )
        try:
            record = RawFetchRecord(**normalized)
        except (TypeError, ValueError) as exc:
            raise RawPageCorrupt(f"Invalid v2 raw manifest: {key}") from exc
        if record.manifest_version != RAW_MANIFEST_VERSION_V2:
            raise RawPageCorrupt(f"Unsupported v2 raw manifest: {key}")
        requests = int(record.http_requests)
        history = tuple(int(status) for status in record.http_status_history)
        bootstrap_attempts = int(record.browser_bootstrap_attempts)
        unobserved_bytes = int(record.browser_unobserved_bytes)
        if (
            requests < 0
            or bootstrap_attempts < 0
            or unobserved_bytes < 0
            or len(history) != requests
            or any(not 100 <= status <= 599 for status in history)
            or (history and history[-1] != int(record.http_status))
        ):
            raise RawPageCorrupt(f"Invalid HTTP history in v2 raw manifest: {key}")
        if (
            requests != record.http_requests
            or history != record.http_status_history
            or bootstrap_attempts != record.browser_bootstrap_attempts
            or unobserved_bytes != record.browser_unobserved_bytes
        ):
            normalized["http_requests"] = requests
            normalized["http_status_history"] = history
            normalized["browser_bootstrap_attempts"] = bootstrap_attempts
            normalized["browser_unobserved_bytes"] = unobserved_bytes
            record = RawFetchRecord(**normalized)
        return record

    def _load_record_blob(
        self,
        record: RawFetchRecord,
        *,
        response: bool,
    ) -> bytes:
        blob_key = record.response_blob_key if response else record.blob_key
        expected_hash = record.response_hash if response else record.content_hash
        expected_bytes = record.response_bytes if response else record.decoded_bytes
        expected_encoded = (
            record.response_encoded_bytes if response else record.encoded_bytes
        )
        encoded = self._read_bytes(blob_key)
        if len(encoded) != expected_encoded:
            raise RawPageCorrupt(f"Encoded size mismatch for {blob_key}")
        try:
            raw = gzip.decompress(encoded)
        except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
            raise RawPageCorrupt(f"Invalid gzip blob: {blob_key}") from exc
        actual_hash = hashlib.sha256(raw).hexdigest()
        if actual_hash != expected_hash or len(raw) != expected_bytes:
            raise RawPageCorrupt(
                f"Content hash mismatch for {record.logical_refresh_id}: "
                f"expected={expected_hash}, actual={actual_hash}"
            )
        return raw

    def has_fetch(self, logical_refresh_id: object) -> bool:
        """Return whether one logical refresh has an append-only commit."""
        return self._exists(self._fetch_manifest_key(logical_refresh_id))

    def fetch_manifest_key(self, logical_refresh_id: object) -> str:
        """Return the public durable key persisted in fetch-attempt state."""
        return self._fetch_manifest_key(logical_refresh_id)

    def read_fetch_record(self, logical_refresh_id: object) -> RawFetchRecord:
        """Read v2 fetch evidence without loading its response body."""
        key = self._fetch_manifest_key(logical_refresh_id)
        if not self._exists(key):
            raise RawPageNotFound(
                f"No raw fetch manifest for {logical_refresh_id}"
            )
        return self._fetch_record(self._read_json(key), key)

    def load_response(
        self,
        logical_refresh_id: object,
    ) -> tuple[bytes, RawFetchRecord]:
        """Load and verify the exact bytes returned by the HTTP transport."""
        record = self.read_fetch_record(logical_refresh_id)
        return self._load_record_blob(record, response=True), record

    def load_fetch_content(
        self,
        logical_refresh_id: object,
    ) -> tuple[bytes, RawFetchRecord]:
        """Load effective parser input (prior content for a 304 response)."""
        record = self.read_fetch_record(logical_refresh_id)
        return self._load_record_blob(record, response=False), record

    def load_fetch_html(
        self,
        logical_refresh_id: object,
        *,
        encoding: str = "utf-8",
    ) -> tuple[str, RawFetchRecord]:
        raw, record = self.load_fetch_content(logical_refresh_id)
        try:
            return raw.decode(encoding), record
        except UnicodeDecodeError as exc:
            raise RawPageCorrupt(
                f"Raw page is not {encoding}: {record.blob_key}"
            ) from exc

    def load_latest_response(
        self,
        target: PageTarget,
    ) -> tuple[bytes, RawFetchRecord]:
        """Load the monotonic latest effective content for a target."""
        record = self._latest_history_record(target)
        if record is not None:
            return self._load_record_blob(record, response=False), record

        # Backward compatibility for raw-v2 pointers created before immutable
        # per-refresh target history was introduced.
        key = self._v2_target_manifest_key(target)
        if not self._exists(key):
            raise RawPageNotFound(f"No v2 raw page for {target.target_id}")
        record = self._fetch_record(self._read_json(key), key)
        if record.target_id != target.target_id:
            raise RawPageCorrupt(f"Target mismatch in v2 raw manifest: {key}")
        return self._load_record_blob(record, response=False), record

    @staticmethod
    def _validate_recovery_identity(
        target: PageTarget,
        record: RawPageRecord | RawFetchRecord,
        *,
        version: str,
    ) -> None:
        """Fail closed when a legacy pointer resolves to another source page."""

        if (
            record.source != target.source
            or record.page_kind != target.page_kind
            or record.target_id != target.target_id
            or dict(record.source_ids) != dict(target.source_ids)
        ):
            raise RawPageCorrupt(
                f"Target identity mismatch in {version} raw manifest for "
                f"{target.target_id}"
            )

    def import_fetch_from_available_raw(
        self,
        target: PageTarget,
        *,
        logical_refresh_id: object,
        attempt_id: Optional[object] = None,
    ) -> Optional[RawFetchRecord]:
        """Create a v2 logical commit from verified raw without any network.

        Historical and completed-once targets may outlive their original
        control run.  This bounded, target-addressed bridge first reuses the
        latest v2 content and then falls back to the exact raw-v1 target
        manifest.  Missing raw returns ``None``; present but corrupt or
        identity-mismatched raw fails closed instead of triggering a refetch.
        """

        if self.has_fetch(logical_refresh_id):
            record = self.read_fetch_record(logical_refresh_id)
            self._validate_recovery_identity(target, record, version="v2")
            # A live commit makes the immutable fetch manifest visible before
            # publishing its target-history candidate and compatibility
            # mirror.  If the worker dies in that window, control recovery
            # must finish the publication before it declares the fetch
            # complete.  Verify both referenced blobs first so corrupt raw
            # evidence cannot be promoted while repairing the pointers.
            self._load_record_blob(record, response=True)
            self._load_record_blob(record, response=False)
            if record.imported_from_manifest_key is None:
                self._publish_target_record(target, record)
            return record

        latest_key = self._v2_target_manifest_key(target)
        if self._exists(latest_key):
            body, source = self.load_latest_response(target)
            self._validate_recovery_identity(target, source, version="v2")
            return self.store_response(
                target,
                body,
                logical_refresh_id=logical_refresh_id,
                attempt_id=attempt_id,
                http_status=200,
                fetched_at=source.fetched_at,
                fetcher_version=RAW_V2_RESUME_VERSION,
                transport_version=RAW_V2_RESUME_VERSION,
                wire_bytes=0,
                provider_billed_bytes=0,
                latency_ms=0,
                http_requests=0,
                http_status_history=(),
                headers=source.headers,
                etag=source.etag,
                last_modified=source.last_modified,
                imported_from_manifest_key=self._fetch_manifest_key(
                    source.logical_refresh_id
                ),
                update_latest_target=False,
            )

        legacy_key = self._target_manifest_key(target)
        if not self._exists(legacy_key):
            return None
        html, source = self.load_html(target)
        if source.manifest_version != RAW_MANIFEST_VERSION:
            raise RawPageCorrupt(
                f"Unsupported v1 raw manifest for {target.target_id}"
            )
        self._validate_recovery_identity(target, source, version="v1")
        return self.store_response(
            target,
            html.encode("utf-8"),
            logical_refresh_id=logical_refresh_id,
            attempt_id=attempt_id,
            http_status=200,
            fetched_at=source.fetched_at,
            fetcher_version=RAW_V1_BRIDGE_VERSION,
            transport_version=RAW_V1_BRIDGE_VERSION,
            wire_bytes=0,
            provider_billed_bytes=0,
            latency_ms=0,
            http_requests=0,
            http_status_history=(),
            imported_from_manifest_key=legacy_key,
            update_latest_target=False,
        )

    def store_response(
        self,
        target: PageTarget,
        response_body: bytes,
        *,
        logical_refresh_id: object,
        http_status: int,
        fetched_at: Optional[str] = None,
        attempt_id: Optional[object] = None,
        fetcher_version: str = FETCHER_VERSION_V2,
        transport_version: Optional[str] = None,
        session_version: Optional[str] = None,
        wire_bytes: Optional[int] = None,
        provider_billed_bytes: Optional[int] = None,
        latency_ms: Optional[int] = None,
        http_requests: int = 1,
        http_status_history: Optional[Sequence[int]] = None,
        browser_bootstrap_attempts: int = 0,
        browser_unobserved_bytes: int = 0,
        base_content_hash: Optional[str] = None,
        headers: Optional[Mapping[str, object]] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
        imported_from_manifest_key: Optional[str] = None,
        update_latest_target: bool = True,
    ) -> RawFetchRecord:
        """Commit exact response bytes, then an immutable logical manifest.

        The content-addressed blob is read back and verified before the logical
        manifest becomes visible.  Retrying the same logical refresh returns
        the original record and repairs the latest-target pointer without
        changing append-only evidence.
        """
        if not isinstance(response_body, (bytes, bytearray, memoryview)):
            raise TypeError("response_body must be bytes-like")
        observed_at = fetched_at or utc_now_iso()
        body = bytes(response_body)
        refresh = _source_id(logical_refresh_id, "logical_refresh_id")
        status = int(http_status)
        if not 100 <= status <= 599:
            raise ValueError("http_status must be between 100 and 599")
        request_count = int(http_requests)
        if request_count < 0:
            raise ValueError("http_requests must be non-negative")
        status_history = tuple(
            int(value)
            for value in (
                http_status_history
                if http_status_history is not None
                else ((status,) if request_count else ())
            )
        )
        if any(not 100 <= value <= 599 for value in status_history):
            raise ValueError("http_status_history contains an invalid status")
        if len(status_history) > request_count:
            raise ValueError("http_status_history exceeds http_requests")
        if len(status_history) != request_count:
            raise ValueError("http_status_history must cover every HTTP request")
        if status_history and status_history[-1] != status:
            raise ValueError("http_status_history must end with http_status")
        bootstrap_attempts = int(browser_bootstrap_attempts)
        if bootstrap_attempts < 0:
            raise ValueError("browser_bootstrap_attempts must be non-negative")
        unobserved_bytes = int(browser_unobserved_bytes)
        if unobserved_bytes < 0:
            raise ValueError("browser_unobserved_bytes must be non-negative")
        for value, name in (
            (wire_bytes, "wire_bytes"),
            (provider_billed_bytes, "provider_billed_bytes"),
            (latency_ms, "latency_ms"),
        ):
            if value is not None and int(value) < 0:
                raise ValueError(f"{name} must be non-negative")

        response_hash = hashlib.sha256(body).hexdigest()
        manifest_key = self._fetch_manifest_key(refresh)
        if self._exists(manifest_key):
            existing_body, existing = self.load_response(refresh)
            if (
                existing.target_id != target.target_id
                or existing.http_status != status
                or existing.http_requests != request_count
                or tuple(existing.http_status_history) != status_history
                or existing.browser_bootstrap_attempts != bootstrap_attempts
                or existing.browser_unobserved_bytes != unobserved_bytes
                or existing.response_hash != response_hash
                or existing_body != body
            ):
                raise RawStoreError(
                    f"Logical fetch manifest is immutable: {refresh}"
                )
            if update_latest_target:
                self._publish_target_record(target, existing)
            return existing

        previous = None
        if status == 304 and base_content_hash:
            previous = self._target_record_for_content_hash(
                target, base_content_hash
            )
            if previous is None:
                raise RawStoreError(
                    "A 304 base_content_hash has no committed raw content"
                )
            self._load_record_blob(previous, response=False)
        else:
            try:
                _, previous = self.load_latest_response(target)
            except RawPageNotFound:
                pass

        response_hash, response_blob_key, response_encoded = (
            self._store_verified_blob(body)
        )
        if status == 304:
            if previous is None:
                raise RawStoreError("A 304 requires previously committed content")
            content_hash = previous.content_hash
            blob_key = previous.blob_key
            decoded_bytes = previous.decoded_bytes
            encoded_bytes = previous.encoded_bytes
            content_changed = False
        else:
            content_hash = response_hash
            blob_key = response_blob_key
            decoded_bytes = len(body)
            encoded_bytes = len(response_encoded)
            content_changed = (
                previous is None or previous.content_hash != content_hash
            )

        normalized_headers = {
            str(key).strip().lower(): str(value)
            for key, value in (headers or {}).items()
            if str(key).strip().lower() in _PERSISTED_RESPONSE_HEADERS
        }
        record = RawFetchRecord(
            manifest_version=RAW_MANIFEST_VERSION_V2,
            logical_refresh_id=refresh,
            attempt_id=(None if attempt_id is None else str(attempt_id)),
            source=target.source,
            page_kind=target.page_kind,
            target_id=target.target_id,
            canonical_url=target.canonical_url,
            source_ids=dict(target.source_ids),
            http_status=status,
            content_hash=content_hash,
            response_hash=response_hash,
            hash_algorithm="sha256",
            blob_key=blob_key,
            response_blob_key=response_blob_key,
            compression="gzip",
            fetched_at=observed_at,
            fetcher_version=_source_id(fetcher_version, "fetcher_version"),
            transport_version=transport_version,
            session_version=session_version,
            decoded_bytes=decoded_bytes,
            response_bytes=len(body),
            encoded_bytes=encoded_bytes,
            response_encoded_bytes=len(response_encoded),
            wire_bytes=None if wire_bytes is None else int(wire_bytes),
            provider_billed_bytes=(
                None
                if provider_billed_bytes is None
                else int(provider_billed_bytes)
            ),
            latency_ms=None if latency_ms is None else int(latency_ms),
            etag=etag or normalized_headers.get("etag"),
            last_modified=(
                last_modified or normalized_headers.get("last-modified")
            ),
            headers=normalized_headers,
            previous_content_hash=(
                None if previous is None else previous.content_hash
            ),
            content_changed=content_changed,
            not_modified=status == 304,
            imported_from_manifest_key=imported_from_manifest_key,
            http_requests=request_count,
            http_status_history=status_history,
            browser_bootstrap_attempts=bootstrap_attempts,
            browser_unobserved_bytes=unobserved_bytes,
        )
        payload = asdict(record)
        self._write_immutable_json(manifest_key, payload)
        if update_latest_target:
            self._publish_target_record(target, record)
        return record

    def commit_fetch(
        self,
        target: PageTarget,
        body: bytes,
        **evidence,
    ) -> RawFetchRecord:
        """Orchestration-facing alias for the v2 raw commit operation."""
        return self.store_response(target, body, **evidence)

    def load_fetch(
        self,
        logical_refresh_id: object,
    ) -> tuple[bytes, RawFetchRecord]:
        """Orchestration-facing alias returning effective offline content."""
        return self.load_fetch_content(logical_refresh_id)

    def has_page(self, target: PageTarget) -> bool:
        return self._exists(self._target_manifest_key(target))

    def store_html(
        self,
        target: PageTarget,
        html: str,
        *,
        fetched_at: Optional[str] = None,
        fetcher_version: str = FETCHER_VERSION,
    ) -> RawPageRecord:
        raw = html.encode("utf-8")
        content_hash = hashlib.sha256(raw).hexdigest()
        blob_key = self._blob_key(content_hash)
        encoded = gzip.compress(raw, compresslevel=6, mtime=0)
        if not self._exists(blob_key):
            self._write_bytes(blob_key, encoded)
        record = RawPageRecord(
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
            fetched_at=fetched_at or utc_now_iso(),
            fetcher_version=fetcher_version,
            decoded_bytes=len(raw),
            encoded_bytes=len(encoded),
        )
        self._write_json(self._target_manifest_key(target), asdict(record))
        return record

    def load_html(self, target: PageTarget) -> tuple[str, RawPageRecord]:
        key = self._target_manifest_key(target)
        if not self._exists(key):
            raise RawPageNotFound(f"No raw page manifest for {target.target_id}")
        payload = self._read_json(key)
        try:
            record = RawPageRecord(**payload)
        except TypeError as exc:
            raise RawPageCorrupt(f"Invalid raw page manifest: {key}") from exc
        if record.target_id != target.target_id:
            raise RawPageCorrupt(f"Target mismatch in raw page manifest: {key}")
        try:
            raw = gzip.decompress(self._read_bytes(record.blob_key))
        except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
            raise RawPageCorrupt(f"Invalid gzip blob: {record.blob_key}") from exc
        actual = hashlib.sha256(raw).hexdigest()
        if actual != record.content_hash or len(raw) != record.decoded_bytes:
            raise RawPageCorrupt(
                f"Content hash mismatch for {target.target_id}: "
                f"expected={record.content_hash}, actual={actual}"
            )
        try:
            return raw.decode("utf-8"), record
        except UnicodeDecodeError as exc:
            raise RawPageCorrupt(f"Raw page is not UTF-8: {record.blob_key}") from exc

    def get_or_fetch(
        self,
        target: PageTarget,
        loader: Callable[[str], Optional[str]],
        *,
        fetched_at: Optional[str] = None,
        fetcher_version: str = FETCHER_VERSION,
    ) -> tuple[str, RawPageRecord, bool]:
        """Return stored HTML, or call the loader exactly once when absent."""
        if self.has_page(target):
            html, record = self.load_html(target)
            return html, record, True
        html = loader(target.canonical_url)
        if not html:
            raise RawStoreError(f"Loader returned no HTML for {target.target_id}")
        record = self.store_html(
            target,
            html,
            fetched_at=fetched_at,
            fetcher_version=fetcher_version,
        )
        return html, record, False

    def write_parse_manifests(self, record: RawPageRecord, result) -> str:
        """Write dataset rows first and the match summary last."""
        match_id = record.source_ids.get("match_id", record.target_id)
        dataset_keys = {}
        for name, dataset in sorted(result.datasets.items()):
            key = (
                f"manifests/datasets/{name}/{match_id}/"
                f"{record.content_hash}/{result.parser_version}.json"
            )
            payload = {
                "manifest_version": RAW_MANIFEST_VERSION,
                "target_id": record.target_id,
                "page_kind": record.page_kind,
                "match_id": match_id,
                "content_hash": record.content_hash,
                "parser_version": result.parser_version,
                "parsed_at": result.parsed_at,
                "dataset": name,
                "status": dataset.status.value,
                "row_count": dataset.row_count,
                "reason": dataset.reason,
                "error_type": dataset.error_type,
                "error_message": dataset.error_message,
            }
            self._write_json(key, payload)
            dataset_keys[name] = key

        match_key = (
            f"manifests/matches/{match_id}/{record.content_hash}/"
            f"{result.parser_version}.json"
        )
        self._write_json(match_key, {
            "manifest_version": RAW_MANIFEST_VERSION,
            "target_id": record.target_id,
            "page_kind": record.page_kind,
            "match_id": match_id,
            "canonical_url": record.canonical_url,
            "content_hash": record.content_hash,
            "parser_version": result.parser_version,
            "parsed_at": result.parsed_at,
            "status": result.status.value,
            "row_count": sum(d.row_count for d in result.datasets.values()),
            "datasets": dataset_keys,
        })
        return match_key

    def write_page_parse_manifests(self, record: RawPageRecord, result) -> str:
        """Write generic dataset manifests first and the page summary last."""

        def field(value, name: str, default=None):
            if isinstance(value, Mapping):
                return value.get(name, default)
            return getattr(value, name, default)

        def status_value(value):
            return getattr(value, "value", value)

        page_kind = self._safe_component(record.page_kind)
        target_key = self._target_key(
            record.page_kind,
            record.source_ids,
            record.target_id,
        )
        parser_version = self._safe_component(result.parser_version)
        content_hash = self._safe_component(record.content_hash)
        dataset_keys = {}
        total_rows = 0

        for name, dataset in sorted(result.datasets.items()):
            dataset_name = self._safe_component(name)
            key = (
                f"manifests/datasets/{page_kind}/{dataset_name}/{target_key}/"
                f"{content_hash}/{parser_version}.json"
            )
            row_count = field(dataset, "row_count", 0)
            total_rows += row_count
            payload = {
                "manifest_version": RAW_MANIFEST_VERSION,
                "target_id": record.target_id,
                "page_kind": record.page_kind,
                "source_ids": dict(record.source_ids),
                "content_hash": record.content_hash,
                "parser_version": result.parser_version,
                "parsed_at": result.parsed_at,
                "dataset": name,
                "status": status_value(field(dataset, "status")),
                "row_count": row_count,
                "reason": field(dataset, "reason"),
                "error_type": field(dataset, "error_type"),
                "error_message": field(dataset, "error_message"),
            }
            self._write_json(key, payload)
            dataset_keys[name] = key

        page_key = (
            f"manifests/pages/{page_kind}/{target_key}/"
            f"{content_hash}/{parser_version}.json"
        )
        self._write_json(page_key, {
            "manifest_version": RAW_MANIFEST_VERSION,
            "target_id": record.target_id,
            "page_kind": record.page_kind,
            "source_ids": dict(record.source_ids),
            "canonical_url": record.canonical_url,
            "content_hash": record.content_hash,
            "parser_version": result.parser_version,
            "parsed_at": result.parsed_at,
            "status": status_value(result.status),
            "row_count": total_rows,
            "datasets": dataset_keys,
        })
        return page_key

    def read_manifest(self, relative: str) -> dict:
        """Read a manifest for diagnostics and tests."""
        return self._read_json(relative)
