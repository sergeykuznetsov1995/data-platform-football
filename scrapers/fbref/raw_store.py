"""Durable raw-first storage for FBref pages and parse manifests."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Callable, Mapping, Optional
from urllib.parse import urlparse

from pyarrow import fs


RAW_MANIFEST_VERSION = "fbref-raw-v1"
FETCHER_VERSION = "fbref-match-loader-v1"
_MATCH_ID_RE = re.compile(r"^[0-9a-fA-F]{8}$")
_MATCH_URL_RE = re.compile(r"/en/matches/([0-9a-fA-F]{8})(?:/|$)")
_FBREF_HOSTS = {"fbref.com", "www.fbref.com"}


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
        except (gzip.BadGzipFile, EOFError) as exc:
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

    @classmethod
    def _discovery_queue_prefix(cls, queue_id: object) -> str:
        return f"queues/discovery/{cls._safe_component(queue_id)}"

    def discovery_queue_plan_key(self, queue_id: object) -> str:
        """Return the durable manifest key for one immutable queue plan."""
        return f"{self._discovery_queue_prefix(queue_id)}/plan.json"

    def discovery_queue_item_key(
        self,
        queue_id: object,
        competition_id: object,
    ) -> str:
        """Return the commit-marker key for one competition queue item."""
        competition = self._safe_component(competition_id)
        return f"{self._discovery_queue_prefix(queue_id)}/items/{competition}.json"

    def has_discovery_queue_plan(self, queue_id: object) -> bool:
        return self._exists(self.discovery_queue_plan_key(queue_id))

    def read_discovery_queue_plan(self, queue_id: object) -> dict:
        return self._read_json(self.discovery_queue_plan_key(queue_id))

    def write_discovery_queue_plan(
        self,
        queue_id: object,
        payload: dict,
    ) -> str:
        key = self.discovery_queue_plan_key(queue_id)
        if self._exists(key):
            if self._read_json(key) != payload:
                raise RawStoreError(
                    f"Discovery queue plan is immutable: {queue_id}"
                )
            return key
        self._write_json(key, payload)
        return key

    def has_discovery_queue_item(
        self,
        queue_id: object,
        competition_id: object,
    ) -> bool:
        return self._exists(
            self.discovery_queue_item_key(queue_id, competition_id)
        )

    def read_discovery_queue_item(
        self,
        queue_id: object,
        competition_id: object,
    ) -> dict:
        return self._read_json(
            self.discovery_queue_item_key(queue_id, competition_id)
        )

    def write_discovery_queue_item(
        self,
        queue_id: object,
        competition_id: object,
        payload: dict,
    ) -> str:
        key = self.discovery_queue_item_key(queue_id, competition_id)
        self._write_json(key, payload)
        return key

    def read_manifest(self, relative: str) -> dict:
        """Read a manifest for diagnostics and tests."""
        return self._read_json(relative)
