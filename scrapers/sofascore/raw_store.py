"""Durable, content-addressed raw storage for SofaScore JSON responses.

The capture engine writes the *exact HTTP response bytes* here before parsing or
touching Iceberg.  A small target pointer makes endpoint resume cheap, while the
immutable gzip blob makes every response replayable without source access.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Mapping, Optional
from urllib.parse import urlparse

from pyarrow import fs


RAW_MANIFEST_VERSION = "sofascore-raw-v1"
FETCHER_VERSION = "sofascore-capture-engine-v1"


class RawStoreError(RuntimeError):
    """Base class for raw-store failures."""


class RawPayloadNotFound(RawStoreError):
    """No committed pointer or immutable blob exists for a target."""


class RawPayloadCorrupt(RawStoreError):
    """Stored bytes do not match their committed metadata."""


class RawPayloadSchemaError(RawStoreError):
    """Stored response is not valid JSON."""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _required(value: object, name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


@dataclass(frozen=True)
class PayloadTarget:
    """Natural identity of one SofaScore endpoint response."""

    source_tournament_id: str
    source_season_id: str
    target_type: str
    target_id: str
    endpoint: str
    freshness_key: str

    def __post_init__(self) -> None:
        for name in (
            "source_tournament_id",
            "source_season_id",
            "target_type",
            "target_id",
            "endpoint",
            "freshness_key",
        ):
            object.__setattr__(self, name, _required(getattr(self, name), name))


@dataclass(frozen=True)
class RawPayloadRecord:
    manifest_version: str
    source: str
    source_tournament_id: str
    source_season_id: str
    target_type: str
    target_id: str
    endpoint: str
    freshness_key: str
    request_url: str
    http_status: int
    response_headers: Mapping[str, str]
    content_type: Optional[str]
    content_hash: str
    hash_algorithm: str
    blob_key: str
    compression: str
    fetched_at: str
    fetcher_version: str
    decoded_bytes: int
    encoded_bytes: int

    @property
    def target(self) -> PayloadTarget:
        return PayloadTarget(
            source_tournament_id=self.source_tournament_id,
            source_season_id=self.source_season_id,
            target_type=self.target_type,
            target_id=self.target_id,
            endpoint=self.endpoint,
            freshness_key=self.freshness_key,
        )


class RawPayloadStore:
    """Content-addressed gzip blobs and atomic JSON target pointers."""

    def __init__(self, filesystem: fs.FileSystem, root: str) -> None:
        self.filesystem = filesystem
        self.root = root.strip("/") if not root.startswith("/") else root.rstrip("/")

    @classmethod
    def from_uri(cls, uri: str) -> "RawPayloadStore":
        parsed = urlparse(uri)
        if parsed.scheme == "s3":
            if not parsed.netloc:
                raise ValueError("S3 raw-store URI must contain a bucket")
            filesystem = fs.S3FileSystem(
                access_key=os.environ.get("S3_ACCESS_KEY"),
                secret_key=os.environ.get("S3_SECRET_KEY"),
                endpoint_override=os.environ.get(
                    "SOFASCORE_RAW_S3_ENDPOINT", "seaweedfs:8333"
                ),
                scheme=os.environ.get("SOFASCORE_RAW_S3_SCHEME", "http"),
                region=os.environ.get("SOFASCORE_RAW_S3_REGION", "us-east-1"),
                background_writes=False,
            )
            root = f"{parsed.netloc}/{parsed.path.lstrip('/')}".rstrip("/")
            return cls(filesystem, root)
        filesystem, root = fs.FileSystem.from_uri(uri)
        return cls(filesystem, root.rstrip("/"))

    @classmethod
    def from_env(cls, *, optional: bool = True) -> Optional["RawPayloadStore"]:
        uri = os.environ.get("SOFASCORE_RAW_STORE_URI", "").strip()
        if not uri:
            if optional:
                return None
            raise RawStoreError("SOFASCORE_RAW_STORE_URI is required")
        return cls.from_uri(uri)

    @staticmethod
    def _safe_component(value: object) -> str:
        raw = _required(value, "path component").encode("utf-8")
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

    def _path(self, relative: str) -> str:
        return str(PurePosixPath(self.root) / relative)

    def _exists(self, relative: str) -> bool:
        return self.filesystem.get_file_info(self._path(relative)).type != fs.FileType.NotFound

    def _read_bytes(self, relative: str) -> bytes:
        path = self._path(relative)
        if self.filesystem.get_file_info(path).type == fs.FileType.NotFound:
            raise RawPayloadNotFound(f"Raw object not found: {relative}")
        with self.filesystem.open_input_file(path) as stream:
            return stream.read()

    def _write_bytes(self, relative: str, payload: bytes) -> None:
        path = self._path(relative)
        self.filesystem.create_dir(str(PurePosixPath(path).parent), recursive=True)
        with self.filesystem.open_output_stream(path, compression=None) as stream:
            stream.write(payload)

    def _write_json_atomic(self, relative: str, payload: Mapping[str, object]) -> None:
        rendered = json.dumps(
            payload, ensure_ascii=False, indent=2, sort_keys=True
        ).encode("utf-8") + b"\n"
        if not isinstance(self.filesystem, fs.LocalFileSystem):
            # Object-store PutObject visibility is atomic; never expose a partial body.
            self._write_bytes(relative, rendered)
            return
        path = self._path(relative)
        self.filesystem.create_dir(str(PurePosixPath(path).parent), recursive=True)
        temporary = f"{path}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
        try:
            with self.filesystem.open_output_stream(temporary, compression=None) as stream:
                stream.write(rendered)
            os.replace(temporary, path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def _read_json(self, relative: str) -> dict:
        try:
            value = json.loads(self._read_bytes(relative).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RawPayloadCorrupt(f"Invalid raw manifest: {relative}") from exc
        if not isinstance(value, dict):
            raise RawPayloadCorrupt(f"Raw manifest is not an object: {relative}")
        return value

    @classmethod
    def target_pointer_key(cls, target: PayloadTarget) -> str:
        parts = (
            target.source_tournament_id,
            target.source_season_id,
            target.target_type,
            target.target_id,
            target.endpoint,
            target.freshness_key,
        )
        return "targets/" + "/".join(cls._safe_component(p) for p in parts) + ".json"

    @staticmethod
    def blob_key(content_hash: str) -> str:
        return f"blobs/sha256/{content_hash[:2]}/{content_hash}.json.gz"

    def has_payload(self, target: PayloadTarget) -> bool:
        return self._exists(self.target_pointer_key(target))

    def store_bytes(
        self,
        target: PayloadTarget,
        body: bytes,
        *,
        request_url: str,
        http_status: int,
        response_headers: Optional[Mapping[str, object]] = None,
        fetched_at: Optional[str] = None,
        fetcher_version: str = FETCHER_VERSION,
    ) -> RawPayloadRecord:
        """Commit exact response bytes; the target pointer is written last."""
        if not isinstance(body, bytes):
            raise TypeError("body must be bytes so exact response bytes are preserved")
        headers = {
            str(key).lower(): str(value)
            for key, value in (response_headers or {}).items()
        }
        content_hash = hashlib.sha256(body).hexdigest()
        blob_key = self.blob_key(content_hash)
        encoded = gzip.compress(body, compresslevel=6, mtime=0)
        if not self._exists(blob_key):
            self._write_bytes(blob_key, encoded)
        record = RawPayloadRecord(
            manifest_version=RAW_MANIFEST_VERSION,
            source="sofascore",
            source_tournament_id=target.source_tournament_id,
            source_season_id=target.source_season_id,
            target_type=target.target_type,
            target_id=target.target_id,
            endpoint=target.endpoint,
            freshness_key=target.freshness_key,
            request_url=_required(request_url, "request_url"),
            http_status=int(http_status),
            response_headers=headers,
            content_type=headers.get("content-type"),
            content_hash=content_hash,
            hash_algorithm="sha256",
            blob_key=blob_key,
            compression="gzip",
            fetched_at=fetched_at or utc_now_iso(),
            fetcher_version=fetcher_version,
            decoded_bytes=len(body),
            encoded_bytes=len(encoded),
        )
        self._write_json_atomic(self.target_pointer_key(target), asdict(record))
        return record

    def load_record(self, record: RawPayloadRecord) -> bytes:
        try:
            body = gzip.decompress(self._read_bytes(record.blob_key))
        except (gzip.BadGzipFile, EOFError) as exc:
            raise RawPayloadCorrupt(f"Invalid gzip blob: {record.blob_key}") from exc
        actual_hash = hashlib.sha256(body).hexdigest()
        if actual_hash != record.content_hash or len(body) != record.decoded_bytes:
            raise RawPayloadCorrupt(
                f"Content mismatch: expected={record.content_hash}, actual={actual_hash}"
            )
        return body

    def load_bytes(self, target: PayloadTarget) -> tuple[bytes, RawPayloadRecord]:
        key = self.target_pointer_key(target)
        if not self._exists(key):
            raise RawPayloadNotFound(f"No raw payload for {target}")
        payload = self._read_json(key)
        try:
            record = RawPayloadRecord(**payload)
        except TypeError as exc:
            raise RawPayloadCorrupt(f"Invalid raw payload manifest: {key}") from exc
        if record.manifest_version != RAW_MANIFEST_VERSION or record.target != target:
            raise RawPayloadCorrupt(f"Target/version mismatch in raw manifest: {key}")
        return self.load_record(record), record

    def load_json(self, target: PayloadTarget) -> tuple[object, RawPayloadRecord]:
        body, record = self.load_bytes(target)
        try:
            return json.loads(body.decode("utf-8")), record
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RawPayloadSchemaError(
                f"Response {record.content_hash} is not valid UTF-8 JSON"
            ) from exc
