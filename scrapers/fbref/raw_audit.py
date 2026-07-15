"""Read-only integrity audit for one production FBref control run.

The audit uses bounded native local/S3 listing and Arrow input streams. It
never creates directories, opens an output stream, or deletes an object in the
raw store. Local JSON evidence is written separately by
``write_audit_artifact`` after the raw-store inventory has been proven stable.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
import stat
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

from pyarrow import fs

from scrapers.fbref.control import ControlStore
from scrapers.fbref.raw_store import PageTarget, RawPageStore


class RawAuditError(RuntimeError):
    """Raw evidence is incomplete, corrupt, or changed during the audit."""


RAW_INVENTORY_SCHEMA_VERSION = "fbref-raw-inventory-v3"
RAW_INVENTORY_INDEX_SCHEMA_VERSION = "fbref-raw-inventory-index-v2"
RAW_AUDIT_SCHEMA_VERSION = "fbref-raw-audit-v3"
RAW_INVENTORY_MAX_OBJECTS = 1_000_000
RAW_DIAGNOSTIC_MAX_OBJECTS = 10_000
RAW_INVENTORY_EVIDENCE_LIMIT = 100
RAW_INVENTORY_HASH_CHUNK_BYTES = 1024 * 1024
RAW_AUDIT_MAX_ATTEMPTS = 1_000
RAW_EPHEMERAL_PREFIXES: tuple[str, ...] = ("_health/",)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _raw_store_identity(store: RawPageStore) -> dict[str, str]:
    """Describe the physical backend without credentials."""

    identity: dict[str, str] = {
        "identity_version": "fbref-raw-store-identity-v1",
        "filesystem_type": (
            f"{type(store.filesystem).__module__}."
            f"{type(store.filesystem).__qualname__}"
        ),
        "root": str(store.root),
    }
    if isinstance(store.filesystem, fs.S3FileSystem):
        endpoint = os.environ.get(
            "FBREF_RAW_S3_ENDPOINT", "seaweedfs:8333"
        ).strip()
        identity.update(
            {
                "endpoint": endpoint,
                "scheme": os.environ.get(
                    "FBREF_RAW_S3_SCHEME", "http"
                ).strip(),
                "region": os.environ.get(
                    "FBREF_RAW_S3_REGION", "us-east-1"
                ).strip(),
            }
        )
    return identity


def _raw_store_identity_sha256(store: RawPageStore) -> str:
    """Fingerprint the credential-free physical storage identity."""

    encoded = json.dumps(
        _raw_store_identity(store), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _hash_raw_object(store: RawPageStore, key: str, expected_size: int) -> str:
    """Hash the exact encoded object bytes and detect a torn inventory read."""

    digest = hashlib.sha256()
    observed_size = 0
    with store.filesystem.open_input_stream(
        store._path(key), compression=None
    ) as stream:
        while True:
            chunk = stream.read(RAW_INVENTORY_HASH_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
            observed_size += len(chunk)
    if observed_size != expected_size:
        raise RawAuditError(
            f"raw object changed while inventory was captured: {key} "
            f"(metadata={expected_size}, read={observed_size})"
        )
    return digest.hexdigest()


def _is_ephemeral_raw_key(key: str) -> bool:
    normalized = str(key).lstrip("/")
    return any(
        normalized == prefix.rstrip("/") or normalized.startswith(prefix)
        for prefix in RAW_EPHEMERAL_PREFIXES
    )


def _walk_local_raw_files(store: RawPageStore) -> Iterator[_RawObjectInfo]:
    """Walk local raw files with O(directory depth) Python memory."""

    root = Path(store.root)
    if not root.exists():
        return
    if root.is_symlink() or not root.is_dir():
        raise RawAuditError("local raw root is not a safe directory")

    def visit(directory: Path) -> Iterator[_RawObjectInfo]:
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    relative = Path(entry.path).relative_to(root).as_posix()
                    if _is_ephemeral_raw_key(relative):
                        continue
                    if entry.is_symlink():
                        raise RawAuditError(
                            f"raw store contains an unsafe symlink: {relative}"
                        )
                    if entry.is_dir(follow_symlinks=False):
                        yield from visit(Path(entry.path))
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        raise RawAuditError(
                            "raw store contains an unsupported filesystem "
                            f"entry: {relative}"
                        )
                    metadata = entry.stat(follow_symlinks=False)
                    yield _RawObjectInfo(
                        path=str(entry.path),
                        size=int(metadata.st_size),
                        mtime_ns=int(metadata.st_mtime_ns),
                        version_token=(
                            f"local:{metadata.st_dev}:{metadata.st_ino}:"
                            f"{metadata.st_ctime_ns}"
                        ),
                    )
        except OSError as exc:
            raise RawAuditError(
                f"could not scan local raw directory: {directory}"
            ) from exc

    yield from visit(root)


def _s3_list_client():
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise RawAuditError(
            "boto3 is required for bounded S3 raw inventory listing"
        ) from exc
    endpoint = os.environ.get(
        "FBREF_RAW_S3_ENDPOINT", "seaweedfs:8333"
    ).strip()
    scheme = os.environ.get("FBREF_RAW_S3_SCHEME", "http").strip()
    return boto3.client(
        "s3",
        endpoint_url=f"{scheme}://{endpoint}",
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
        region_name=os.environ.get("FBREF_RAW_S3_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}),
    )


def _walk_s3_raw_files(store: RawPageStore) -> Iterator[_RawObjectInfo]:
    """Use bounded ListObjectsV2 pages instead of directory-per-key walks."""

    bucket, separator, prefix = store.root.partition("/")
    if not bucket:
        raise RawAuditError("S3 raw root has no bucket")
    normalized_prefix = prefix.rstrip("/") if separator else ""
    list_prefix = f"{normalized_prefix}/" if normalized_prefix else ""
    client = _s3_list_client()
    try:
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=bucket,
            Prefix=list_prefix,
            PaginationConfig={"PageSize": 1_000},
        )
        for page in pages:
            for item in page.get("Contents") or ():
                object_key = str(item.get("Key") or "")
                if not object_key or object_key.endswith("/"):
                    continue
                relative = (
                    object_key[len(list_prefix) :]
                    if list_prefix and object_key.startswith(list_prefix)
                    else object_key
                )
                if _is_ephemeral_raw_key(relative):
                    continue
                modified = item.get("LastModified")
                mtime_ns = (
                    int(modified.timestamp() * 1_000_000_000)
                    if modified is not None
                    else 0
                )
                etag = str(item.get("ETag") or "").strip('"')
                yield _RawObjectInfo(
                    path=f"{bucket}/{object_key}",
                    size=int(item.get("Size") or 0),
                    mtime_ns=mtime_ns,
                    version_token=f"s3-etag:{etag}" if etag else "",
                )
    except Exception as exc:
        if isinstance(exc, RawAuditError):
            raise
        raise RawAuditError("could not page through S3 raw objects") from exc


def _walk_raw_files(store: RawPageStore) -> Iterator[_RawObjectInfo]:
    """Enumerate raw objects with bounded pages and strong version tokens."""

    if isinstance(store.filesystem, fs.LocalFileSystem):
        yield from _walk_local_raw_files(store)
        return
    if isinstance(store.filesystem, fs.S3FileSystem):
        yield from _walk_s3_raw_files(store)
        return
    raise RawAuditError(
        "bounded raw inventory is unsupported for filesystem type "
        f"{type(store.filesystem).__qualname__}"
    )


def _update_json_array_hash(
    digest: Any,
    rows: Iterable[Sequence[Any]],
) -> None:
    """Hash canonical JSON incrementally without one store-sized string."""

    digest.update(b"[")
    first = True
    for row in rows:
        if not first:
            digest.update(b",")
        first = False
        digest.update(
            json.dumps(
                tuple(row), separators=(",", ":"), ensure_ascii=True
            ).encode("ascii")
        )
    digest.update(b"]")


@dataclass(frozen=True)
class _InventoryIndex:
    connection: sqlite3.Connection
    summary: dict[str, Any]
    database_path: Path


@dataclass(frozen=True)
class DiskBackedRawInventory:
    """Verified baseline metadata plus its disk-backed object index."""

    summary: Mapping[str, Any]
    baseline_sha256: str
    index_path: Path


@dataclass(frozen=True)
class _RawObjectInfo:
    path: str
    size: int
    mtime_ns: int
    version_token: str


def _new_inventory_connection(path: str) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("PRAGMA temp_store=FILE")
    connection.execute(
        """
        CREATE TABLE current_object (
            key TEXT PRIMARY KEY,
            encoded_bytes INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL,
            version_token TEXT NOT NULL,
            sha256 TEXT
        ) WITHOUT ROWID
        """
    )
    connection.execute(
        """
        CREATE TABLE inventory_summary (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            summary_json TEXT NOT NULL
        ) WITHOUT ROWID
        """
    )
    return connection


def _summarize_inventory_table(
    connection: sqlite3.Connection,
    *,
    raw_root_sha256: str,
    content_hashed: bool,
    raw_store_identity: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    count, encoded_bytes = connection.execute(
        "SELECT count(*), COALESCE(sum(encoded_bytes), 0) FROM current_object"
    ).fetchone()
    metadata_digest = hashlib.sha256()
    _update_json_array_hash(
        metadata_digest,
        connection.execute(
            """
            SELECT key, encoded_bytes, mtime_ns, version_token
            FROM current_object ORDER BY key
            """
        ),
    )
    summary: dict[str, Any] = {
        "schema_version": RAW_INVENTORY_SCHEMA_VERSION,
        "index_schema_version": RAW_INVENTORY_INDEX_SCHEMA_VERSION,
        "captured_at": _utc_now(),
        "raw_root_sha256": raw_root_sha256,
        "raw_store_identity": dict(raw_store_identity or {}),
        "object_count": int(count),
        "encoded_bytes": int(encoded_bytes),
        "metadata_fingerprint_sha256": metadata_digest.hexdigest(),
        "excluded_ephemeral_prefixes": list(RAW_EPHEMERAL_PREFIXES),
        "content_hashed": bool(content_hashed),
    }
    if content_hashed:
        content_digest = hashlib.sha256()
        _update_json_array_hash(
            content_digest,
            connection.execute(
                """
                SELECT key, encoded_bytes, mtime_ns, version_token, sha256
                FROM current_object ORDER BY key
                """
            ),
        )
        semantic_digest = content_digest.hexdigest()
        summary["fingerprint_sha256"] = semantic_digest
        summary["index_semantic_fingerprint_sha256"] = semantic_digest
    return summary


@contextmanager
def _captured_inventory_index(
    store: RawPageStore,
    *,
    content_hashed: bool,
    max_objects: int = RAW_INVENTORY_MAX_OBJECTS,
    temp_directory: Path | None = None,
) -> Iterator[_InventoryIndex]:
    """Build a disk-backed index with bounded Python heap usage."""

    normalized_limit = int(max_objects)
    if normalized_limit <= 0:
        raise ValueError("max_objects must be positive")
    descriptor, database_path = tempfile.mkstemp(
        prefix="fbref-raw-inventory-",
        suffix=".sqlite3",
        dir=str(temp_directory) if temp_directory is not None else None,
    )
    os.close(descriptor)
    connection: sqlite3.Connection | None = None
    try:
        connection = _new_inventory_connection(database_path)
        count = 0
        for info in _walk_raw_files(store):
            count += 1
            if count > normalized_limit:
                raise RawAuditError(
                    "raw inventory object limit exceeded: "
                    f"limit={normalized_limit}"
                )
            key = store._relative_path(info.path)
            size = int(info.size)
            digest = (
                _hash_raw_object(store, key, size)
                if content_hashed
                else None
            )
            try:
                connection.execute(
                    """
                    INSERT INTO current_object (
                        key, encoded_bytes, mtime_ns, version_token, sha256
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        key,
                        size,
                        int(info.mtime_ns or 0),
                        str(info.version_token or ""),
                        digest,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise RawAuditError(
                    f"raw inventory contains duplicate object key: {key}"
                ) from exc
        raw_store_identity = _raw_store_identity(store)
        summary = _summarize_inventory_table(
            connection,
            raw_root_sha256=hashlib.sha256(
                json.dumps(
                    raw_store_identity,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest(),
            content_hashed=content_hashed,
            raw_store_identity=raw_store_identity,
        )
        connection.execute(
            """
            INSERT INTO inventory_summary (singleton, summary_json)
            VALUES (1, ?)
            """,
            (
                json.dumps(
                    summary,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
        )
        connection.commit()
        yield _InventoryIndex(
            connection=connection,
            summary=summary,
            database_path=Path(database_path),
        )
    finally:
        if connection is not None:
            connection.close()
        Path(database_path).unlink(missing_ok=True)


def capture_raw_inventory(
    store: RawPageStore,
    *,
    max_objects: int = RAW_DIAGNOSTIC_MAX_OBJECTS,
) -> dict[str, Any]:
    """Materialize a bounded small-store diagnostic content inventory."""

    with _captured_inventory_index(
        store,
        content_hashed=True,
        max_objects=max_objects,
    ) as index:
        result = dict(index.summary)
        result["objects"] = [
            {
                "key": str(key),
                "encoded_bytes": int(size),
                "mtime_ns": int(mtime_ns),
                "version_token": str(version_token),
                "sha256": str(digest),
            }
            for key, size, mtime_ns, version_token, digest in (
                index.connection.execute(
                    """
                    SELECT key, encoded_bytes, mtime_ns, version_token, sha256
                    FROM current_object ORDER BY key
                    """
                )
            )
        ]
        return result


def _inventory_metadata_identity(
    inventory: Mapping[str, Any],
) -> tuple[Any, ...]:
    return (
        inventory.get("raw_root_sha256"),
        inventory.get("object_count"),
        inventory.get("encoded_bytes"),
        inventory.get("metadata_fingerprint_sha256"),
    )


def _load_validated_baseline(
    connection: sqlite3.Connection,
    inventory: Mapping[str, Any] | DiskBackedRawInventory,
    *,
    expected_root_sha256: str,
) -> None:
    summary = (
        dict(inventory.summary)
        if isinstance(inventory, DiskBackedRawInventory)
        else inventory
    )
    if summary.get("schema_version") != RAW_INVENTORY_SCHEMA_VERSION:
        raise RawAuditError("baseline inventory schema is missing or unsupported")
    if summary.get("raw_root_sha256") != expected_root_sha256:
        raise RawAuditError("baseline inventory belongs to another raw root")
    connection.execute(
        """
        CREATE TABLE baseline_object (
            key TEXT PRIMARY KEY,
            encoded_bytes INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL,
            version_token TEXT NOT NULL,
            sha256 TEXT NOT NULL
        ) WITHOUT ROWID
        """
    )
    if isinstance(inventory, DiskBackedRawInventory):
        if inventory.index_path.is_symlink() or not inventory.index_path.is_file():
            raise RawAuditError(
                "disk-backed baseline index is missing or unsafe"
            )
        connection.execute("ATTACH DATABASE ? AS installed", (str(inventory.index_path),))
        try:
            summary_rows = connection.execute(
                "SELECT summary_json FROM installed.inventory_summary"
            ).fetchall()
            if len(summary_rows) != 1:
                raise RawAuditError(
                    "disk-backed baseline index summary is missing or "
                    "ambiguous"
                )
            try:
                installed_summary = json.loads(str(summary_rows[0][0]))
            except json.JSONDecodeError as exc:
                raise RawAuditError(
                    "disk-backed baseline index summary is invalid"
                ) from exc
            if not isinstance(installed_summary, dict) or installed_summary != dict(
                summary
            ):
                raise RawAuditError(
                    "disk-backed baseline index summary does not match its "
                    "commit evidence"
                )
            connection.execute(
                """
                INSERT INTO baseline_object (
                    key, encoded_bytes, mtime_ns, version_token, sha256
                )
                SELECT key, encoded_bytes, mtime_ns, version_token, sha256
                FROM installed.current_object
                """
            )
            connection.commit()
        except (sqlite3.DatabaseError, sqlite3.IntegrityError) as exc:
            raise RawAuditError(
                "disk-backed baseline index is invalid"
            ) from exc
        finally:
            try:
                connection.execute("DETACH DATABASE installed")
            except sqlite3.DatabaseError:
                pass
    else:
        objects = inventory.get("objects")
        if not isinstance(objects, list):
            raise RawAuditError("baseline inventory objects must be a list")
        if len(objects) > RAW_INVENTORY_MAX_OBJECTS:
            raise RawAuditError(
                "baseline inventory object limit exceeded: "
                f"limit={RAW_INVENTORY_MAX_OBJECTS}"
            )
        for item in objects:
            if not isinstance(item, Mapping) or not str(item.get("key") or ""):
                raise RawAuditError(
                    "baseline inventory contains an invalid object"
                )
            key = str(item["key"])
            if _is_ephemeral_raw_key(key):
                raise RawAuditError(
                    "baseline inventory contains an ephemeral health object"
                )
            size = int(item["encoded_bytes"])
            mtime_ns = int(item["mtime_ns"])
            version_token = str(item.get("version_token") or "")
            if len(version_token) > 512:
                raise RawAuditError(
                    "baseline inventory contains an invalid version token"
                )
            if size < 0 or mtime_ns < 0:
                raise RawAuditError(
                    "baseline inventory contains negative object metadata"
                )
            digest = str(item.get("sha256") or "")
            if len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                raise RawAuditError(
                    "baseline inventory contains an invalid object SHA256"
                )
            try:
                connection.execute(
                    """
                    INSERT INTO baseline_object (
                        key, encoded_bytes, mtime_ns, version_token, sha256
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (key, size, mtime_ns, version_token, digest),
                )
            except sqlite3.IntegrityError as exc:
                raise RawAuditError(
                    "baseline inventory contains duplicate object keys"
                ) from exc
        connection.commit()

    invalid = connection.execute(
        """
        SELECT key FROM baseline_object
        WHERE encoded_bytes < 0 OR mtime_ns < 0 OR length(sha256) <> 64
           OR sha256 GLOB '*[^0-9a-f]*'
           OR length(key) > 4096 OR length(version_token) > 512
           OR key = '_health' OR substr(key, 1, 8) = '_health/'
        LIMIT 1
        """
    ).fetchone()
    if invalid is not None:
        raise RawAuditError("baseline inventory index contains an invalid object")

    count, encoded_bytes = connection.execute(
        """
        SELECT count(*), COALESCE(sum(encoded_bytes), 0)
        FROM baseline_object
        """
    ).fetchone()
    content_digest = hashlib.sha256()
    _update_json_array_hash(
        content_digest,
        connection.execute(
            """
            SELECT key, encoded_bytes, mtime_ns, version_token, sha256
            FROM baseline_object ORDER BY key
            """
        ),
    )
    expected = (
        int(count),
        int(encoded_bytes),
        content_digest.hexdigest(),
    )
    actual = (
        int(summary.get("object_count", -1)),
        int(summary.get("encoded_bytes", -1)),
        str(summary.get("fingerprint_sha256") or ""),
    )
    if actual != expected:
        raise RawAuditError("baseline inventory summary/fingerprint is invalid")


def _hydrate_current_hashes_from_baseline(
    store: RawPageStore,
    connection: sqlite3.Connection,
) -> None:
    """Reuse immutable baseline hashes and read only new/rewritten objects."""

    updates: list[tuple[str, str]] = []
    rows = connection.execute(
        """
        SELECT current.key, current.encoded_bytes, current.version_token,
               baseline.encoded_bytes, baseline.version_token, baseline.sha256
        FROM current_object AS current
        LEFT JOIN baseline_object AS baseline USING (key)
        ORDER BY current.key
        """
    )
    for (
        key,
        current_size,
        current_version,
        baseline_size,
        baseline_version,
        baseline_sha256,
    ) in rows:
        if (
            baseline_sha256 is not None
            and int(current_size) == int(baseline_size)
            and bool(current_version)
            and str(current_version) == str(baseline_version)
        ):
            digest = str(baseline_sha256)
        else:
            digest = _hash_raw_object(store, str(key), int(current_size))
        updates.append((digest, str(key)))
        if len(updates) >= 512:
            connection.executemany(
                "UPDATE current_object SET sha256 = ? WHERE key = ?",
                updates,
            )
            updates.clear()
    if updates:
        connection.executemany(
            "UPDATE current_object SET sha256 = ? WHERE key = ?", updates
        )
    connection.commit()


def _hydrate_all_current_hashes(
    store: RawPageStore,
    connection: sqlite3.Connection,
) -> None:
    """Hash all objects only for explicitly baseline-free diagnostics."""

    updates: list[tuple[str, str]] = []
    for key, size in connection.execute(
        "SELECT key, encoded_bytes FROM current_object ORDER BY key"
    ):
        updates.append(
            (_hash_raw_object(store, str(key), int(size)), str(key))
        )
        if len(updates) >= 512:
            connection.executemany(
                "UPDATE current_object SET sha256 = ? WHERE key = ?",
                updates,
            )
            updates.clear()
    if updates:
        connection.executemany(
            "UPDATE current_object SET sha256 = ? WHERE key = ?", updates
        )
    connection.commit()


def _verify_unversioned_objects_match(
    store: RawPageStore,
    current: sqlite3.Connection,
    baseline: sqlite3.Connection,
) -> None:
    """Rehash objects when metadata exposes no trustworthy version token."""

    for key, size in current.execute(
        """
        SELECT key, encoded_bytes FROM current_object
        WHERE version_token = '' ORDER BY key
        """
    ):
        row = baseline.execute(
            "SELECT sha256 FROM current_object WHERE key = ?", (key,)
        ).fetchone()
        if row is None or _hash_raw_object(
            store, str(key), int(size)
        ) != str(row[0]):
            raise RawAuditError(
                "unversioned raw object changed after an incomplete baseline "
                f"capture: {key}"
            )


def _find_unstable_unversioned_object(
    store: RawPageStore,
    before: sqlite3.Connection,
    after: sqlite3.Connection,
) -> str | None:
    """Return one same-metadata object that changed during the audit."""

    for key, size in after.execute(
        """
        SELECT key, encoded_bytes FROM current_object
        WHERE version_token = '' ORDER BY key
        """
    ):
        row = before.execute(
            """
            SELECT sha256 FROM current_object
            WHERE key = ? AND encoded_bytes = ? AND version_token = ''
            """,
            (key, size),
        ).fetchone()
        if row is not None and row[0] is not None and _hash_raw_object(
            store, str(key), int(size)
        ) != str(row[0]):
            return str(key)
    return None


def _assert_store_matches_captured_index(
    store: RawPageStore,
    baseline: _InventoryIndex,
    *,
    max_objects: int,
    error: str,
) -> None:
    """Run the final metadata/version guard around baseline publication."""

    with _captured_inventory_index(
        store,
        content_hashed=False,
        max_objects=max_objects,
    ) as current:
        if _inventory_metadata_identity(
            baseline.summary
        ) != _inventory_metadata_identity(current.summary):
            raise RawAuditError(error)
        _verify_unversioned_objects_match(
            store,
            current.connection,
            baseline.connection,
        )


def _expected_text(row: Mapping[str, Any], name: str) -> str | None:
    value = row.get(name)
    return None if value is None else str(value)


def _recovery_source_attempt(
    row: Mapping[str, Any], record_attempt_id: str | None
) -> Mapping[str, Any] | None:
    """Validate zero-network recovery and return its original raw attempt."""

    if str(row.get("transport_version") or "") != "raw-recovery":
        return None
    zero_fields = {
        "decoded_bytes": 0,
        "compressed_bytes": 0,
        "wire_bytes": 0,
        "http_request_count": 0,
        "latency_ms": 0,
    }
    for name, expected in zero_fields.items():
        if name not in row or row.get(name) is None:
            raise RawAuditError(
                f"raw recovery attempt is missing zero-network {name}"
            )
        if int(row.get(name) or 0) != expected:
            raise RawAuditError(
                f"raw recovery attempt has non-zero {name}: {row.get(name)}"
            )
    if "provider_billed_bytes" not in row:
        raise RawAuditError(
            "raw recovery attempt is missing provider billing evidence"
        )
    if row.get("provider_billed_bytes") not in (None, 0):
        raise RawAuditError("raw recovery attempt has provider-billed bytes")
    if "http_status_history" not in row:
        raise RawAuditError("raw recovery attempt is missing HTTP history")
    if list(row.get("http_status_history") or []):
        raise RawAuditError("raw recovery attempt has HTTP status history")
    if "session_version" not in row:
        raise RawAuditError("raw recovery attempt is missing session evidence")
    if row.get("session_version") not in (None, ""):
        raise RawAuditError("raw recovery attempt has a live session identity")
    if not record_attempt_id:
        raise RawAuditError("raw recovery manifest has no source attempt identity")

    candidates = row.get("raw_recovery_source_attempts")
    if isinstance(candidates, str):
        try:
            candidates = json.loads(candidates)
        except json.JSONDecodeError as exc:
            raise RawAuditError(
                "raw recovery source-attempt evidence is invalid JSON"
            ) from exc
    if not isinstance(candidates, list):
        raise RawAuditError("raw recovery source-attempt evidence is missing")
    matches = [
        candidate
        for candidate in candidates
        if isinstance(candidate, Mapping)
        and str(candidate.get("attempt_id") or "") == record_attempt_id
    ]
    if len(matches) != 1:
        raise RawAuditError(
            "raw recovery source attempt is missing or ambiguous: "
            f"{record_attempt_id}"
        )
    source = matches[0]
    identity_fields = (
        "run_id",
        "target_id",
        "logical_refresh_id",
        "content_hash",
        "raw_manifest_key",
    )
    for name in identity_fields:
        if _expected_text(source, name) != _expected_text(row, name):
            raise RawAuditError(
                f"raw recovery source {name} differs from recovery attempt"
            )
    if str(source.get("status") or "").casefold() not in {
        "failed",
        "expired",
    }:
        raise RawAuditError(
            "raw recovery source attempt is not failed or expired"
        )
    try:
        source_number = int(source["attempt_number"])
        recovery_number = int(row["attempt_number"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RawAuditError(
            "raw recovery attempt ordering evidence is missing"
        ) from exc
    if source_number <= 0 or recovery_number <= source_number:
        raise RawAuditError(
            "raw recovery source does not predate the recovery attempt"
        )
    if str(source.get("transport_version") or "") == "raw-recovery":
        raise RawAuditError("raw recovery cannot use another recovery as source")
    return source


def _audit_one(store: RawPageStore, row: Mapping[str, Any]) -> dict[str, Any]:
    logical_refresh_id = str(uuid.UUID(str(row["logical_refresh_id"])))
    expected_manifest = store.fetch_manifest_key(logical_refresh_id)
    actual_manifest = str(row.get("raw_manifest_key") or "")
    if actual_manifest != expected_manifest:
        raise RawAuditError(
            f"manifest key mismatch: expected={expected_manifest}, "
            f"actual={actual_manifest or '<missing>'}"
        )

    record = store.read_fetch_record(logical_refresh_id)
    control_attempt_id = _expected_text(row, "attempt_id")
    recovery_source = _recovery_source_attempt(row, record.attempt_id)
    evidence_row = row if recovery_source is None else recovery_source
    expected = {
        "target_id": _expected_text(row, "target_id"),
        "content_hash": _expected_text(row, "content_hash"),
    }
    actual = {
        "target_id": record.target_id,
        "content_hash": record.content_hash,
    }
    for name, expected_value in expected.items():
        if expected_value is not None and actual[name] != expected_value:
            raise RawAuditError(
                f"{name} mismatch for {logical_refresh_id}: "
                f"expected={expected_value}, actual={actual[name]}"
            )
    if recovery_source is None and (
        control_attempt_id is not None
        and record.attempt_id != control_attempt_id
    ):
        raise RawAuditError(
            f"attempt_id mismatch for {logical_refresh_id}: "
            f"expected={control_attempt_id}, actual={record.attempt_id}"
        )
    if record.source != "fbref" or record.logical_refresh_id != logical_refresh_id:
        raise RawAuditError(f"invalid source/refresh identity for {logical_refresh_id}")
    if not 200 <= int(record.http_status) <= 399:
        raise RawAuditError(
            f"successful raw manifest has HTTP {record.http_status}"
        )
    if row.get("http_status") is not None and int(row["http_status"]) != int(
        record.http_status
    ):
        raise RawAuditError(
            f"http_status mismatch for {logical_refresh_id}: "
            f"control={row['http_status']}, raw={record.http_status}"
        )
    if int(record.http_status) == 304:
        if not record.not_modified or record.content_changed:
            raise RawAuditError("304 manifest has inconsistent content flags")
    elif record.not_modified:
        raise RawAuditError("non-304 manifest is marked not_modified")

    content, content_record = store.load_fetch_content(logical_refresh_id)
    response, response_record = store.load_response(logical_refresh_id)
    if content_record != record or response_record != record:
        raise RawAuditError(f"record changed while reading {logical_refresh_id}")

    if recovery_source is not None:
        strict_recovery_evidence = {
            "decoded_bytes": len(content),
            "compressed_bytes": int(record.encoded_bytes),
            "wire_bytes": int(record.wire_bytes or 0),
            "provider_billed_bytes": record.provider_billed_bytes,
            "http_status": int(record.http_status),
            "http_request_count": int(record.http_requests),
            "http_status_history": list(record.http_status_history),
            "etag": record.etag,
            "last_modified": record.last_modified,
            "transport_version": record.transport_version,
            "session_version": record.session_version,
            "latency_ms": record.latency_ms,
        }
        for name, actual_value in strict_recovery_evidence.items():
            if name not in recovery_source:
                raise RawAuditError(
                    f"raw recovery source is missing {name} evidence"
                )
            expected_value = recovery_source.get(name)
            if name == "http_status_history":
                matches = list(expected_value or []) == actual_value
            else:
                matches = expected_value == actual_value
            if not matches:
                raise RawAuditError(
                    f"raw recovery source {name} mismatch for "
                    f"{logical_refresh_id}: control={expected_value}, "
                    f"raw={actual_value}"
                )

    target = PageTarget(
        source=record.source,
        page_kind=record.page_kind,
        target_id=record.target_id,
        canonical_url=record.canonical_url,
        source_ids=record.source_ids,
    )
    history_key = store._v2_target_history_manifest_key(
        target, logical_refresh_id
    )
    if store._exists(history_key):
        history = store._fetch_record(store._read_json(history_key), history_key)
        if history != record:
            raise RawAuditError(
                f"immutable target history differs from fetch manifest: {history_key}"
            )
    elif record.imported_from_manifest_key:
        imported = store._fetch_record(
            store._read_json(record.imported_from_manifest_key),
            record.imported_from_manifest_key,
        )
        if (
            imported.target_id != record.target_id
            or imported.content_hash != record.content_hash
        ):
            raise RawAuditError(
                f"import source differs from resumed fetch: {logical_refresh_id}"
            )
        history_key = None
    else:
        raise RawAuditError(
            f"immutable target history is missing: {history_key}"
        )

    mirror_key = store._v2_target_manifest_key(target)
    latest = store._latest_history_record(target)
    if latest is None or not store._exists(mirror_key):
        raise RawAuditError(f"latest target pointer is missing for {record.target_id}")
    mirrored = store._fetch_record(store._read_json(mirror_key), mirror_key)
    if mirrored != latest:
        raise RawAuditError(
            f"latest target pointer regressed for {record.target_id}"
        )

    numeric_expectations = {
        "decoded_bytes": len(content),
        "compressed_bytes": record.encoded_bytes,
        "wire_bytes": record.wire_bytes,
        "provider_billed_bytes": record.provider_billed_bytes,
    }
    for name, actual_value in numeric_expectations.items():
        expected_value = evidence_row.get(name)
        if expected_value is not None and int(expected_value) != int(
            actual_value or 0
        ):
            raise RawAuditError(
                f"{name} mismatch for {logical_refresh_id}: "
                f"control={expected_value}, raw={actual_value}"
            )
    evidence_expectations = {
        "http_status": int(record.http_status),
        "http_request_count": int(record.http_requests),
        "latency_ms": int(record.latency_ms or 0),
    }
    for name, actual_value in evidence_expectations.items():
        expected_value = evidence_row.get(name)
        if expected_value is not None and int(expected_value) != actual_value:
            raise RawAuditError(
                f"{name} mismatch for {logical_refresh_id}: "
                f"control={expected_value}, raw={actual_value}"
            )
    expected_history = evidence_row.get("http_status_history")
    if expected_history is not None and tuple(expected_history) != tuple(
        record.http_status_history
    ):
        raise RawAuditError(
            f"http_status_history mismatch for {logical_refresh_id}"
        )
    for name in ("transport_version", "session_version"):
        expected_value = evidence_row.get(name)
        actual_value = getattr(record, name)
        if expected_value is not None and str(expected_value) != str(actual_value):
            raise RawAuditError(
                f"{name} mismatch for {logical_refresh_id}: "
                f"control={expected_value}, raw={actual_value}"
            )

    return {
        "logical_refresh_id": logical_refresh_id,
        "attempt_id": control_attempt_id,
        "raw_source_attempt_id": record.attempt_id,
        "recovered_from_raw": recovery_source is not None,
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "http_status": int(record.http_status),
        "manifest_key": expected_manifest,
        "history_key": history_key,
        "target_mirror_key": mirror_key,
        "content_blob_key": record.blob_key,
        "response_blob_key": record.response_blob_key,
        "content_hash": record.content_hash,
        "response_hash": record.response_hash,
        "decoded_bytes": len(content),
        "response_bytes": len(response),
        "encoded_bytes": int(record.encoded_bytes),
        "response_encoded_bytes": int(record.response_encoded_bytes),
        "not_modified": bool(record.not_modified),
    }


def _append_evidence(items: list[str], key: str) -> None:
    if len(items) < RAW_INVENTORY_EVIDENCE_LIMIT:
        items.append(str(key))


def _inventory_delta(
    connection: sqlite3.Connection,
    *,
    referenced_keys: set[str],
    mutable_target_mirrors: set[str],
) -> dict[str, Any]:
    """Compare disk-backed indexes with exact counts and bounded evidence."""

    samples: dict[str, list[str]] = {
        "created_objects": [],
        "deleted_objects": [],
        "content_changed_objects": [],
        "allowed_target_mirror_changes": [],
        "immutable_content_changes": [],
        "metadata_only_changes": [],
        "allowed_target_mirror_metadata_changes": [],
        "immutable_metadata_changes": [],
        "unlinked_created_objects": [],
    }
    counts = {
        "created_object_count": 0,
        "deleted_object_count": 0,
        "content_changed_object_count": 0,
        "allowed_target_mirror_change_count": 0,
        "immutable_content_change_count": 0,
        "metadata_only_change_count": 0,
        "allowed_target_mirror_metadata_change_count": 0,
        "immutable_metadata_change_count": 0,
        "unlinked_created_object_count": 0,
    }

    for (key,) in connection.execute(
        """
        SELECT current.key
        FROM current_object AS current
        LEFT JOIN baseline_object AS baseline USING (key)
        WHERE baseline.key IS NULL
        ORDER BY current.key
        """
    ):
        counts["created_object_count"] += 1
        _append_evidence(samples["created_objects"], key)
        if key not in referenced_keys:
            counts["unlinked_created_object_count"] += 1
            _append_evidence(samples["unlinked_created_objects"], key)

    for (key,) in connection.execute(
        """
        SELECT baseline.key
        FROM baseline_object AS baseline
        LEFT JOIN current_object AS current USING (key)
        WHERE current.key IS NULL
        ORDER BY baseline.key
        """
    ):
        counts["deleted_object_count"] += 1
        _append_evidence(samples["deleted_objects"], key)

    for (key,) in connection.execute(
        """
        SELECT current.key
        FROM current_object AS current
        JOIN baseline_object AS baseline USING (key)
        WHERE current.encoded_bytes <> baseline.encoded_bytes
           OR current.sha256 <> baseline.sha256
        ORDER BY current.key
        """
    ):
        counts["content_changed_object_count"] += 1
        _append_evidence(samples["content_changed_objects"], key)
        if key in mutable_target_mirrors:
            counts["allowed_target_mirror_change_count"] += 1
            _append_evidence(samples["allowed_target_mirror_changes"], key)
        else:
            counts["immutable_content_change_count"] += 1
            _append_evidence(samples["immutable_content_changes"], key)

    for (key,) in connection.execute(
        """
        SELECT current.key
        FROM current_object AS current
        JOIN baseline_object AS baseline USING (key)
        WHERE current.encoded_bytes = baseline.encoded_bytes
          AND current.sha256 = baseline.sha256
          AND (
              current.mtime_ns <> baseline.mtime_ns
              OR current.version_token <> baseline.version_token
          )
        ORDER BY current.key
        """
    ):
        counts["metadata_only_change_count"] += 1
        _append_evidence(samples["metadata_only_changes"], key)
        if key in mutable_target_mirrors:
            counts["allowed_target_mirror_metadata_change_count"] += 1
            _append_evidence(
                samples["allowed_target_mirror_metadata_changes"], key
            )
        else:
            counts["immutable_metadata_change_count"] += 1
            _append_evidence(samples["immutable_metadata_changes"], key)

    result: dict[str, Any] = {
        **counts,
        **samples,
        "evidence_limit_per_category": RAW_INVENTORY_EVIDENCE_LIMIT,
    }
    result["evidence_truncated"] = any(
        counts[count_name] > len(samples[sample_name])
        for count_name, sample_name in (
            ("created_object_count", "created_objects"),
            ("deleted_object_count", "deleted_objects"),
            ("content_changed_object_count", "content_changed_objects"),
            (
                "allowed_target_mirror_change_count",
                "allowed_target_mirror_changes",
            ),
            ("immutable_content_change_count", "immutable_content_changes"),
            ("metadata_only_change_count", "metadata_only_changes"),
            (
                "allowed_target_mirror_metadata_change_count",
                "allowed_target_mirror_metadata_changes",
            ),
            (
                "immutable_metadata_change_count",
                "immutable_metadata_changes",
            ),
            ("unlinked_created_object_count", "unlinked_created_objects"),
        )
    )
    return result


def audit_raw_fetches(
    store: RawPageStore,
    attempts: Iterable[Mapping[str, Any]],
    *,
    control_run_id: object,
    baseline_inventory: Mapping[str, Any] | DiskBackedRawInventory | None = None,
    require_baseline: bool = True,
    require_nonempty: bool = True,
    require_zero_delta: bool = False,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Verify attempts by reusing immutable hashes from the pre-run baseline.

    Production audit performs two bounded metadata walks and hashes only
    objects created or rewritten since the baseline. The full raw store is
    content-hashed once, when that baseline is captured. Evidence lists are
    capped while all verdict counts remain exact.
    """

    normalized_run_id = str(uuid.UUID(str(control_run_id)))
    materialized: list[dict[str, Any]] = []
    for row in attempts:
        if len(materialized) >= RAW_AUDIT_MAX_ATTEMPTS:
            raise RawAuditError(
                "raw audit attempt limit exceeded: "
                f"limit={RAW_AUDIT_MAX_ATTEMPTS}"
            )
        materialized.append(dict(row))
    audited: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    baseline_delta = None

    with _captured_inventory_index(
        store, content_hashed=False
    ) as current_index:
        baseline_valid = False
        current_hashes_hydrated = False
        if baseline_inventory is None:
            if require_baseline or require_zero_delta:
                failures.append(
                    {
                        "error_class": "MissingBaselineInventory",
                        "error": (
                            "production raw acceptance requires the "
                            "content-hashed inventory captured before the run"
                        ),
                    }
                )
            else:
                _hydrate_all_current_hashes(
                    store, current_index.connection
                )
                current_hashes_hydrated = True
        else:
            try:
                _load_validated_baseline(
                    current_index.connection,
                    baseline_inventory,
                    expected_root_sha256=str(
                        current_index.summary["raw_root_sha256"]
                    ),
                )
                _hydrate_current_hashes_from_baseline(
                    store, current_index.connection
                )
                baseline_valid = True
                current_hashes_hydrated = True
            except Exception as exc:
                failures.append(
                    {
                        "error_class": "InvalidBaselineInventory",
                        "error": str(exc)[:1000],
                    }
                )
        before = _summarize_inventory_table(
            current_index.connection,
            raw_root_sha256=str(
                current_index.summary["raw_root_sha256"]
            ),
            content_hashed=current_hashes_hydrated,
            raw_store_identity=current_index.summary["raw_store_identity"],
        )
        seen_refreshes: set[str] = set()
        for row in materialized:
            refresh = str(row.get("logical_refresh_id") or "")
            if refresh in seen_refreshes:
                failures.append(
                    {
                        "logical_refresh_id": refresh,
                        "error_class": "DuplicateControlRow",
                        "error": "duplicate control row",
                    }
                )
                continue
            seen_refreshes.add(refresh)
            try:
                audited.append(_audit_one(store, row))
            except Exception as exc:  # preserve bounded evidence for all rows
                failures.append(
                    {
                        "logical_refresh_id": refresh,
                        "target_id": str(row.get("target_id") or ""),
                        "error_class": type(exc).__name__,
                        "error": str(exc)[:1000],
                    }
                )

        with _captured_inventory_index(
            store, content_hashed=False
        ) as final_index:
            after = dict(final_index.summary)
            unstable_unversioned_key = _find_unstable_unversioned_object(
                store,
                current_index.connection,
                final_index.connection,
            )
        if (
            _inventory_metadata_identity(before)
            != _inventory_metadata_identity(after)
            or unstable_unversioned_key is not None
        ):
            failures.append(
                {
                    "error_class": "RawInventoryChanged",
                    "error": (
                        "raw object keys, sizes, modification times, or strong "
                        "version tokens changed while audit was running"
                    ),
                    "before": before,
                    "after": after,
                    "unstable_unversioned_key": unstable_unversioned_key,
                }
            )

        if require_nonempty and not materialized:
            failures.append(
                {
                    "error_class": "EmptyRunEvidence",
                    "error": (
                        "live/source acceptance requires at least one "
                        "successful attempt"
                    ),
                }
            )

        referenced_keys = {
            str(item[key])
            for item in audited
            for key in (
                "manifest_key",
                "history_key",
                "target_mirror_key",
                "content_blob_key",
                "response_blob_key",
            )
            if item.get(key)
        }
        mutable_target_mirrors = {
            str(item["target_mirror_key"])
            for item in audited
            if item.get("target_mirror_key")
        }
        if baseline_valid:
            baseline_delta = _inventory_delta(
                current_index.connection,
                referenced_keys=referenced_keys,
                mutable_target_mirrors=mutable_target_mirrors,
            )
            if baseline_delta is not None:
                deleted_count = int(
                    baseline_delta["deleted_object_count"]
                )
                immutable_count = int(
                    baseline_delta["immutable_content_change_count"]
                )
                immutable_metadata_count = int(
                    baseline_delta["immutable_metadata_change_count"]
                )
                unlinked_count = int(
                    baseline_delta["unlinked_created_object_count"]
                )
                if deleted_count:
                    failures.append(
                        {
                            "error_class": "RawObjectDeletedSinceBaseline",
                            "error": (
                                f"{deleted_count} baseline raw objects "
                                "disappeared"
                            ),
                            "sample": baseline_delta["deleted_objects"][:25],
                        }
                    )
                if immutable_count:
                    failures.append(
                        {
                            "error_class": "ImmutableRawObjectChanged",
                            "error": (
                                f"{immutable_count} pre-existing raw objects "
                                "changed content"
                            ),
                            "sample": baseline_delta[
                                "immutable_content_changes"
                            ][:25],
                        }
                    )
                if immutable_metadata_count:
                    failures.append(
                        {
                            "error_class": (
                                "ImmutableRawObjectMetadataChanged"
                            ),
                            "error": (
                                f"{immutable_metadata_count} pre-existing raw "
                                "objects were rewritten without a content "
                                "change"
                            ),
                            "sample": baseline_delta[
                                "immutable_metadata_changes"
                            ][:25],
                        }
                    )
                if unlinked_count:
                    failures.append(
                        {
                            "error_class": "UnlinkedRawObject",
                            "error": (
                                f"{unlinked_count} new raw objects are not "
                                "referenced by successful run attempts"
                            ),
                            "sample": baseline_delta[
                                "unlinked_created_objects"
                            ][:25],
                        }
                    )
                delta_count = sum(
                    int(baseline_delta[name])
                    for name in (
                        "created_object_count",
                        "deleted_object_count",
                        "content_changed_object_count",
                        "metadata_only_change_count",
                    )
                )
                if require_zero_delta and delta_count:
                    failures.append(
                        {
                            "error_class": "RawDeltaForbidden",
                            "error": (
                                "replay acceptance requires zero created, "
                                "deleted, content-changed, or metadata-changed "
                                "raw objects"
                            ),
                            "created_object_count": baseline_delta[
                                "created_object_count"
                            ],
                            "deleted_object_count": baseline_delta[
                                "deleted_object_count"
                            ],
                            "content_changed_object_count": baseline_delta[
                                "content_changed_object_count"
                            ],
                            "metadata_only_change_count": baseline_delta[
                                "metadata_only_change_count"
                            ],
                        }
                    )

        unique_blobs = {
            item[key]
            for item in audited
            for key in ("content_blob_key", "response_blob_key")
        }
        return {
            "schema_version": RAW_AUDIT_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "control_run_id": normalized_run_id,
            "status": "passed" if not failures else "failed",
            "successful_attempt_count": len(materialized),
            "audited_attempt_count": len(audited),
            "unique_blob_count": len(unique_blobs),
            "raw_inventory_before": before,
            "raw_inventory_after": after,
            "baseline_delta": baseline_delta,
            "metadata": {
                str(key): value for key, value in dict(metadata or {}).items()
            },
            "failures": failures,
            "attempts": audited,
        }


def load_successful_run_attempts(
    control: ControlStore, control_run_id: object
) -> list[dict[str, Any]]:
    """Read all successful attempts for one immutable run cohort."""

    run_id = str(uuid.UUID(str(control_run_id)))
    rows: list[dict[str, Any]] = []
    after: tuple[int, int, str] | None = None
    while True:
        page = control.list_successful_fetch_attempts(
            run_id, limit=250, after=after
        )
        for item in page:
            row = dict(item)
            if str(row.get("transport_version") or "") == "raw-recovery":
                row["raw_recovery_source_attempts"] = (
                    control.list_fetch_attempts_for_refresh(
                        run_id,
                        row["logical_refresh_id"],
                    )
                )
            rows.append(row)
            if len(rows) > RAW_AUDIT_MAX_ATTEMPTS:
                raise RawAuditError(
                    "raw audit attempt limit exceeded: "
                    f"limit={RAW_AUDIT_MAX_ATTEMPTS}"
                )
        if len(page) < 250:
            return rows
        last = page[-1]
        next_after = (
            int(last["ordinal"]),
            int(last["attempt_number"]),
            str(last["attempt_id"]),
        )
        if next_after == after:
            raise RawAuditError("successful-attempt pagination did not advance")
        after = next_after


def write_audit_artifact(
    result: Mapping[str, Any],
    output_root: str | os.PathLike[str],
    *,
    artifact_id: str | None = None,
) -> tuple[Path, Path]:
    """Atomically write local JSON evidence and its SHA256 sidecar."""

    run_id = str(uuid.UUID(str(result["control_run_id"])))
    directory = Path(output_root).resolve() / run_id
    directory.mkdir(parents=True, exist_ok=True)
    _fsync_directory(directory.parent)
    suffix = ""
    if artifact_id is not None:
        normalized_artifact_id = str(artifact_id).strip()
        if not normalized_artifact_id:
            raise ValueError("artifact_id must not be empty")
        suffix = "-" + hashlib.sha256(
            normalized_artifact_id.encode("utf-8")
        ).hexdigest()[:16]
    payload = (
        json.dumps(result, indent=2, sort_keys=True, default=str).encode("utf-8")
        + b"\n"
    )
    digest = hashlib.sha256(payload).hexdigest()
    filename = f"raw_integrity{suffix}-{digest}.json"
    path = directory / filename
    digest_path = directory / f"{filename}.sha256"
    digest_payload = f"{digest}  {path.name}\n".encode("ascii")
    _atomic_create_or_verify(
        digest_path,
        digest_payload,
        mode=0o440,
        label="FBref raw audit SHA256 sidecar",
    )
    _atomic_create_or_verify(
        path,
        payload,
        mode=0o440,
        label="FBref raw audit artifact",
    )
    if (
        _existing_regular_bytes(path, label="FBref raw audit artifact")
        != payload
        or _existing_regular_bytes(
            digest_path, label="FBref raw audit SHA256 sidecar"
        )
        != digest_payload
    ):
        raise RawAuditError("FBref raw audit artifact pair is inconsistent")
    return path, digest_path


def _normalized_local_destination(path: str | os.PathLike[str]) -> Path:
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    parent = candidate.parent.resolve()
    parent.mkdir(parents=True, exist_ok=True)
    return parent / candidate.name


@contextmanager
def _baseline_capture_lock(destination: Path) -> Iterator[None]:
    """Serialize full-store hashing; kernel releases the lease on crashes."""

    lock_path = destination.with_name(f".{destination.name}.capture.lock")
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | getattr(
        os, "O_NOFOLLOW", 0
    )
    try:
        descriptor = os.open(lock_path, flags, 0o640)
    except OSError as exc:
        raise RawAuditError(
            f"could not open FBref baseline capture lock: {lock_path}"
        ) from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise RawAuditError(
                f"FBref baseline capture lock is unsafe: {lock_path}"
            )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
        except OSError as exc:
            raise RawAuditError(
                f"could not hold FBref baseline capture lock: {lock_path}"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _existing_regular_bytes(path: Path, *, label: str) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise RawAuditError(f"{label} is not a regular non-symlink file: {path}")
    try:
        return path.read_bytes()
    except OSError as exc:
        raise RawAuditError(f"could not read existing {label}: {path}") from exc


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(str(directory), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_create_or_verify(
    destination: Path,
    payload: bytes,
    *,
    mode: int,
    label: str,
) -> bool:
    """Publish a complete inode once; identical concurrent retries succeed."""

    if destination.exists() or destination.is_symlink():
        if _existing_regular_bytes(destination, label=label) != payload:
            raise RawAuditError(
                f"existing {label} differs; refusing to overwrite: "
                f"{destination}"
            )
        return False

    temporary = destination.with_name(
        f".{destination.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    )
    descriptor = os.open(
        str(temporary),
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        mode,
    )
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, mode)
        try:
            os.link(temporary, destination)
        except FileExistsError:
            if _existing_regular_bytes(destination, label=label) != payload:
                raise RawAuditError(
                    f"concurrent {label} differs; refusing to overwrite: "
                    f"{destination}"
                )
            return False
        _fsync_directory(destination.parent)
        return True
    finally:
        temporary.unlink(missing_ok=True)


def _baseline_sidecar_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.sha256")


def _baseline_index_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.sqlite3")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(RAW_INVENTORY_HASH_CHUNK_BYTES)
                if not chunk:
                    return digest.hexdigest()
                digest.update(chunk)
    except OSError as exc:
        raise RawAuditError(f"could not hash local evidence file: {path}") from exc


def _publish_prepared_file(
    prepared: Path,
    destination: Path,
    *,
    digest: str,
    mode: int,
    label: str,
) -> bool:
    if destination.exists() or destination.is_symlink():
        if (
            destination.is_symlink()
            or not destination.is_file()
            or _sha256_file(destination) != digest
        ):
            raise RawAuditError(
                f"existing {label} differs; refusing to overwrite: "
                f"{destination}"
            )
        return False
    os.chmod(prepared, mode)
    try:
        os.link(prepared, destination)
    except FileExistsError:
        if (
            destination.is_symlink()
            or not destination.is_file()
            or _sha256_file(destination) != digest
        ):
            raise RawAuditError(
                f"concurrent {label} differs; refusing to overwrite: "
                f"{destination}"
            )
        return False
    _fsync_directory(destination.parent)
    return True


def _write_streaming_inventory_json(
    index: _InventoryIndex,
    destination: Path,
) -> tuple[Path, str]:
    """Write the small human-readable commit summary, never object rows."""

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.tmp-",
        dir=str(destination.parent),
    )
    payload = (
        json.dumps(
            index.summary,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        + b"\n"
    )
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        return Path(temporary_name), hashlib.sha256(payload).hexdigest()
    except Exception:
        Path(temporary_name).unlink(missing_ok=True)
        raise


def _inventory_content_identity(
    inventory: Mapping[str, Any],
) -> tuple[Any, ...]:
    """Return the semantic identity of an inventory, excluding capture time."""

    return (
        inventory.get("schema_version"),
        inventory.get("index_schema_version"),
        inventory.get("raw_root_sha256"),
        json.dumps(
            inventory.get("raw_store_identity"),
            sort_keys=True,
            separators=(",", ":"),
        ),
        inventory.get("object_count"),
        inventory.get("encoded_bytes"),
        inventory.get("metadata_fingerprint_sha256"),
        inventory.get("fingerprint_sha256"),
        inventory.get("index_semantic_fingerprint_sha256"),
        tuple(inventory.get("excluded_ephemeral_prefixes") or ()),
        inventory.get("content_hashed"),
    )


def _validate_inventory_index(
    connection: sqlite3.Connection,
    *,
    raw_root_sha256: str,
) -> dict[str, Any]:
    try:
        summary_rows = connection.execute(
            "SELECT summary_json FROM inventory_summary"
        ).fetchall()
        if len(summary_rows) != 1:
            raise RawAuditError(
                "FBref raw baseline disk index summary is missing or ambiguous"
            )
        try:
            stored_summary = json.loads(str(summary_rows[0][0]))
        except json.JSONDecodeError as exc:
            raise RawAuditError(
                "FBref raw baseline disk index summary is invalid"
            ) from exc
        if not isinstance(stored_summary, dict):
            raise RawAuditError(
                "FBref raw baseline disk index summary must be an object"
            )
        expected_summary_keys = {
            "schema_version",
            "index_schema_version",
            "captured_at",
            "raw_root_sha256",
            "raw_store_identity",
            "object_count",
            "encoded_bytes",
            "metadata_fingerprint_sha256",
            "excluded_ephemeral_prefixes",
            "content_hashed",
            "fingerprint_sha256",
            "index_semantic_fingerprint_sha256",
        }
        if set(stored_summary) != expected_summary_keys:
            raise RawAuditError(
                "FBref raw baseline disk index summary fields are invalid"
            )
        if stored_summary.get("raw_root_sha256") != raw_root_sha256:
            raise RawAuditError(
                "FBref raw baseline disk index belongs to another raw root"
            )
        identity = stored_summary.get("raw_store_identity")
        if not isinstance(identity, dict):
            raise RawAuditError(
                "FBref raw baseline disk index storage identity is invalid"
            )
        encoded_identity = json.dumps(
            identity, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        if hashlib.sha256(encoded_identity).hexdigest() != raw_root_sha256:
            raise RawAuditError(
                "FBref raw baseline disk index storage identity digest is "
                "invalid"
            )
        if stored_summary.get("content_hashed") is not True:
            raise RawAuditError(
                "FBref raw baseline disk index is not content-hashed"
            )
        invalid = connection.execute(
            """
            SELECT key FROM current_object
            WHERE key = '' OR substr(key, 1, 1) = '/'
               OR length(key) > 4096 OR length(version_token) > 512
               OR encoded_bytes < 0 OR mtime_ns < 0
               OR sha256 IS NULL OR length(sha256) <> 64
               OR sha256 GLOB '*[^0-9a-f]*'
               OR key = '_health' OR substr(key, 1, 8) = '_health/'
            LIMIT 1
            """
        ).fetchone()
        summary = _summarize_inventory_table(
            connection,
            raw_root_sha256=str(stored_summary["raw_root_sha256"]),
            content_hashed=True,
            raw_store_identity=stored_summary["raw_store_identity"],
        )
    except sqlite3.DatabaseError as exc:
        raise RawAuditError(
            "FBref raw baseline disk index is invalid"
        ) from exc
    if invalid is not None:
        raise RawAuditError(
            "FBref raw baseline disk index contains an invalid object"
        )
    if int(summary["object_count"]) > RAW_INVENTORY_MAX_OBJECTS:
        raise RawAuditError(
            "baseline inventory object limit exceeded: "
            f"limit={RAW_INVENTORY_MAX_OBJECTS}"
        )
    summary["captured_at"] = stored_summary["captured_at"]
    if summary != stored_summary:
        raise RawAuditError(
            "FBref raw baseline disk index summary/fingerprint is invalid"
        )
    return summary


@contextmanager
def _opened_inventory_index(
    path: Path,
    *,
    raw_root_sha256: str,
) -> Iterator[_InventoryIndex]:
    if path.is_symlink() or not path.is_file():
        raise RawAuditError(
            "FBref raw baseline disk index is missing or unsafe"
        )
    try:
        connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    except sqlite3.DatabaseError as exc:
        raise RawAuditError(
            "could not open FBref raw baseline disk index"
        ) from exc
    try:
        connection.execute("PRAGMA query_only=ON")
        summary = _validate_inventory_index(
            connection,
            raw_root_sha256=raw_root_sha256,
        )
        yield _InventoryIndex(
            connection=connection,
            summary=summary,
            database_path=path,
        )
    finally:
        connection.close()


def _publish_streaming_baseline_components(
    index: _InventoryIndex,
    destination: Path,
) -> bool:
    """Publish the digest first and the summary JSON commit marker last."""

    prepared_json, baseline_sha256 = _write_streaming_inventory_json(
        index, destination
    )
    try:
        _atomic_create_or_verify(
            _baseline_sidecar_path(destination),
            f"{baseline_sha256}  {destination.name}\n".encode("ascii"),
            mode=0o440,
            label="FBref raw baseline SHA256 sidecar",
        )
        return _publish_prepared_file(
            prepared_json,
            destination,
            digest=baseline_sha256,
            mode=0o440,
            label="FBref raw baseline",
        )
    finally:
        prepared_json.unlink(missing_ok=True)


def open_disk_backed_inventory(
    path: str | os.PathLike[str],
) -> DiskBackedRawInventory:
    """Open streaming baseline evidence without materializing object rows."""

    source = _normalized_local_destination(path)
    payload = _existing_regular_bytes(source, label="FBref raw baseline")
    baseline_sha256 = hashlib.sha256(payload).hexdigest()
    sidecar = _existing_regular_bytes(
        _baseline_sidecar_path(source),
        label="FBref raw baseline SHA256 sidecar",
    )
    try:
        sidecar_digest, sidecar_name = sidecar.decode("ascii").split()
    except (UnicodeDecodeError, ValueError) as exc:
        raise RawAuditError("FBref raw baseline sidecar is invalid") from exc
    if sidecar_digest != baseline_sha256 or sidecar_name != source.name:
        raise RawAuditError("FBref raw baseline sidecar does not match the file")
    try:
        summary = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RawAuditError("FBref raw baseline summary is invalid") from exc
    if not isinstance(summary, dict):
        raise RawAuditError("FBref raw baseline summary must be a JSON object")
    index_path = _baseline_index_path(source)
    with _opened_inventory_index(
        index_path,
        raw_root_sha256=str(summary.get("raw_root_sha256") or ""),
    ) as installed_index:
        if installed_index.summary != summary:
            raise RawAuditError(
                "FBref raw baseline summary does not match its disk index"
            )
    return DiskBackedRawInventory(
        summary=summary,
        baseline_sha256=baseline_sha256,
        index_path=index_path,
    )


def _capture_and_write_raw_inventory_locked(
    store: RawPageStore,
    destination: Path,
    *,
    max_objects: int = RAW_INVENTORY_MAX_OBJECTS,
) -> tuple[Path, DiskBackedRawInventory, bool]:
    """Capture under the per-destination interprocess lease."""

    if destination.exists() or destination.is_symlink():
        return destination, open_disk_backed_inventory(destination), True

    index_path = _baseline_index_path(destination)
    sidecar_path = _baseline_sidecar_path(destination)
    partial_exists = any(
        path.exists() or path.is_symlink()
        for path in (index_path, sidecar_path)
    )
    if partial_exists and not (index_path.exists() or index_path.is_symlink()):
        raise RawAuditError(
            "incomplete FBref raw baseline is missing its disk index"
        )

    if partial_exists:
        with _captured_inventory_index(
            store,
            content_hashed=False,
            max_objects=max_objects,
        ) as current_index:
            with _opened_inventory_index(
                index_path,
                raw_root_sha256=str(
                    current_index.summary["raw_root_sha256"]
                ),
            ) as installed_index:
                if _inventory_metadata_identity(
                    installed_index.summary
                ) != _inventory_metadata_identity(current_index.summary):
                    raise RawAuditError(
                        "raw store changed after an incomplete baseline "
                        "capture"
                    )
                _verify_unversioned_objects_match(
                    store,
                    current_index.connection,
                    installed_index.connection,
                )
                _publish_streaming_baseline_components(
                    installed_index, destination
                )
        return destination, open_disk_backed_inventory(destination), True

    with _captured_inventory_index(
        store,
        content_hashed=True,
        max_objects=max_objects,
        temp_directory=destination.parent,
    ) as captured_index:
        _assert_store_matches_captured_index(
            store,
            captured_index,
            max_objects=max_objects,
            error="raw store changed while baseline content was hashed",
        )
        prepared_index = captured_index.database_path
        with prepared_index.open("rb+") as stream:
            stream.flush()
            os.fsync(stream.fileno())
        index_digest = _sha256_file(prepared_index)
        try:
            _publish_prepared_file(
                prepared_index,
                index_path,
                digest=index_digest,
                mode=0o440,
                label="FBref raw baseline disk index",
            )
        except RawAuditError:
            # SQLite file bytes are not a stable semantic identity. A
            # concurrent writer may have installed the exact same rows with
            # different pager/header bytes; validate it below.
            if not (index_path.exists() or index_path.is_symlink()):
                raise
        with _opened_inventory_index(
            index_path,
            raw_root_sha256=str(
                captured_index.summary["raw_root_sha256"]
            ),
        ) as installed_index:
            if _inventory_content_identity(
                installed_index.summary
            ) != _inventory_content_identity(captured_index.summary):
                raise RawAuditError(
                    "concurrent FBref raw baseline disk index differs"
                )
            _assert_store_matches_captured_index(
                store,
                installed_index,
                max_objects=max_objects,
                error="raw store changed before baseline commit",
            )
            created = _publish_streaming_baseline_components(
                installed_index, destination
            )
    return destination, open_disk_backed_inventory(destination), not created


def capture_and_write_raw_inventory(
    store: RawPageStore,
    path: str | os.PathLike[str],
    *,
    max_objects: int = RAW_INVENTORY_MAX_OBJECTS,
) -> tuple[Path, DiskBackedRawInventory, bool]:
    """Hash once and publish a crash-safe disk-backed raw baseline."""

    destination = _normalized_local_destination(path)
    with _baseline_capture_lock(destination):
        return _capture_and_write_raw_inventory_locked(
            store,
            destination,
            max_objects=max_objects,
        )


def write_inventory_baseline(
    inventory: Mapping[str, Any], path: str | os.PathLike[str]
) -> Path:
    """Create one immutable local baseline and digest sidecar.

    The destination is never replaced. Concurrent identical writers converge;
    a different writer fails closed without changing the installed evidence.
    """

    destination = _normalized_local_destination(path)
    payload = (
        json.dumps(
            inventory, indent=2, sort_keys=True, default=str
        ).encode("utf-8")
        + b"\n"
    )
    _atomic_create_or_verify(
        destination,
        payload,
        mode=0o440,
        label="FBref raw baseline",
    )
    digest = hashlib.sha256(payload).hexdigest()
    sidecar = _baseline_sidecar_path(destination)
    _atomic_create_or_verify(
        sidecar,
        f"{digest}  {destination.name}\n".encode("ascii"),
        mode=0o440,
        label="FBref raw baseline SHA256 sidecar",
    )
    return destination


def load_inventory_baseline(
    path: str | os.PathLike[str],
) -> tuple[dict[str, Any], str]:
    """Load a create-once baseline only after verifying its local sidecar."""

    source = _normalized_local_destination(path)
    payload = _existing_regular_bytes(source, label="FBref raw baseline")
    digest = hashlib.sha256(payload).hexdigest()
    sidecar_payload = _existing_regular_bytes(
        _baseline_sidecar_path(source),
        label="FBref raw baseline SHA256 sidecar",
    )
    try:
        sidecar_digest, sidecar_name = sidecar_payload.decode("ascii").split()
    except (UnicodeDecodeError, ValueError) as exc:
        raise RawAuditError("FBref raw baseline sidecar is invalid") from exc
    if sidecar_digest != digest or sidecar_name != source.name:
        raise RawAuditError("FBref raw baseline sidecar does not match the file")
    try:
        inventory = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RawAuditError("FBref raw baseline is invalid JSON") from exc
    if not isinstance(inventory, dict):
        raise RawAuditError("FBref raw baseline must contain a JSON object")
    return inventory, digest


def raw_baseline_anchor(
    inventory: Mapping[str, Any], baseline_sha256: str
) -> dict[str, Any]:
    """Build the exact control-plane anchor for local baseline bytes."""

    return {
        "schema_version": str(inventory["schema_version"]),
        "raw_root_sha256": str(inventory["raw_root_sha256"]),
        "object_count": int(inventory["object_count"]),
        "encoded_bytes": int(inventory["encoded_bytes"]),
        "fingerprint_sha256": str(inventory["fingerprint_sha256"]),
        "baseline_sha256": str(baseline_sha256),
    }


def successful_attempt_snapshot(
    attempts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Fingerprint an ordered-independent successful-attempt set."""

    try:
        attempt_ids = sorted(
            str(uuid.UUID(str(attempt["attempt_id"])))
            for attempt in attempts
        )
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise RawAuditError(
            "successful FBref attempt evidence has an invalid attempt_id"
        ) from exc
    encoded = json.dumps(
        attempt_ids, sort_keys=True, separators=(",", ":")
    ).encode("ascii")
    return {
        "schema_version": "fbref-raw-attempt-snapshot-v1",
        "successful_attempt_count": len(attempt_ids),
        "successful_attempt_ids_sha256": hashlib.sha256(encoded).hexdigest(),
    }


__all__: Sequence[str] = (
    "DiskBackedRawInventory",
    "RAW_AUDIT_MAX_ATTEMPTS",
    "RawAuditError",
    "audit_raw_fetches",
    "capture_and_write_raw_inventory",
    "capture_raw_inventory",
    "load_inventory_baseline",
    "load_successful_run_attempts",
    "open_disk_backed_inventory",
    "raw_baseline_anchor",
    "successful_attempt_snapshot",
    "write_audit_artifact",
    "write_inventory_baseline",
)
