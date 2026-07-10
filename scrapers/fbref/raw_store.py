"""Durable raw-first storage for FBref pages and parse manifests."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
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


def match_page_target(match_or_url: str) -> PageTarget:
    """Normalize a match id or any FBref match URL to one target."""
    candidate = str(match_or_url).strip()
    match_id = candidate if _MATCH_ID_RE.fullmatch(candidate) else None
    if match_id is None:
        parsed = urlparse(candidate)
        if parsed.netloc.lower() not in {"fbref.com", "www.fbref.com"}:
            raise ValueError(f"Not an FBref match URL: {match_or_url!r}")
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
        self._write_bytes(relative, rendered)

    @staticmethod
    def _target_manifest_key(target: PageTarget) -> str:
        source_id = target.source_ids.get(target.page_kind)
        if not source_id and target.page_kind == "match":
            source_id = target.source_ids.get("match_id")
        if not source_id:
            source_id = hashlib.sha256(target.target_id.encode()).hexdigest()
        return f"targets/{target.page_kind}/{source_id}.json"

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

    def read_manifest(self, relative: str) -> dict:
        """Read a manifest for diagnostics and tests."""
        return self._read_json(relative)
