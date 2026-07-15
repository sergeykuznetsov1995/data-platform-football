"""Fail-closed, secret-safe production preflight checks for FBref.

The checks in this module are deliberately independent from Airflow so they
can be exercised in unit tests and by an operator before a control run is
created.  Health objects live outside the immutable raw namespace and are
always removed in ``finally`` blocks.
"""

from __future__ import annotations

import hashlib
import os
import stat
import uuid
from pathlib import Path
from typing import Any, Callable, Optional

from pyarrow import fs

from scrapers.utils.proxy_manager import ProxyManager


EXPECTED_RAW_STORE_URI = "s3://football/raw/fbref"
_HEALTH_TOKEN_PREFIX = "fbref_health_"


class ReadinessError(RuntimeError):
    """A production dependency is unavailable or configured unsafely."""


def _health_token(value: Optional[object] = None) -> str:
    candidate = uuid.uuid4() if value is None else uuid.UUID(str(value))
    return _HEALTH_TOKEN_PREFIX + candidate.hex


def validate_raw_store_uri(uri: object) -> str:
    """Require the one production raw root; never accept a silent fallback."""

    normalized = str(uri or "").strip().rstrip("/")
    if normalized != EXPECTED_RAW_STORE_URI:
        raise ReadinessError(
            "FBREF_RAW_STORE_URI must be "
            f"{EXPECTED_RAW_STORE_URI!r}, got {normalized or '<missing>'!r}"
        )
    return normalized


def validate_proxy_pool(
    proxy_file: object,
    *,
    minimum_healthy: int,
    timeout_seconds: float = 3.0,
    max_workers: int = 100,
    manager_factory: Callable[..., ProxyManager] = ProxyManager,
) -> dict[str, Any]:
    """Parse and TCP-probe a proxy pool without issuing a paid HTTP request."""

    required = int(minimum_healthy)
    if required <= 0:
        raise ValueError("minimum_healthy must be positive")
    path = Path(str(proxy_file)).expanduser()
    if not path.is_absolute():
        path = path.absolute()
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise ReadinessError(f"FBref proxy file not found: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ReadinessError("FBref proxy path must be a regular non-symlink file")
    if metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise ReadinessError("FBref proxy file must not be group/world writable")

    payload = path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    configured = sum(
        1
        for raw_line in payload.decode("utf-8").splitlines()
        if raw_line.strip() and not raw_line.lstrip().startswith("#")
    )
    if configured <= 0:
        raise ReadinessError("FBref proxy file contains no configured exits")

    manager = manager_factory(rotation_strategy="random")
    loaded = int(manager.load_from_file_custom_format(str(path)))
    if loaded != configured:
        raise ReadinessError(
            "FBref proxy file contains malformed entries: "
            f"configured={configured}, parsed={loaded}"
        )
    evidence = manager.validate_proxies(
        timeout=float(timeout_seconds),
        max_workers=int(max_workers),
        ban_failed=True,
    )
    healthy = int(evidence.get("alive") or 0)
    if healthy < required:
        raise ReadinessError(
            f"FBref proxy pool has {healthy} healthy exits; {required} required"
        )
    return {
        "proxy_file_sha256": digest,
        "configured": configured,
        "parsed": loaded,
        "healthy": healthy,
        "dead": int(evidence.get("dead") or 0),
        "minimum_healthy": required,
        # Deliberately exclude the path, hosts, usernames, and passwords.
        "probe": "tcp_connect_only",
    }


def check_raw_store_roundtrip(raw_store, *, token: Optional[object] = None) -> dict:
    """Write/read/delete one health object outside the immutable raw layout."""

    name = _health_token(token)
    relative = f"_health/{name}.bin"
    path = raw_store._path(relative)
    payload = os.urandom(32)
    expected = hashlib.sha256(payload).hexdigest()
    write_attempted = False
    try:
        parent = str(Path(path).parent).replace(os.sep, "/")
        raw_store.filesystem.create_dir(parent, recursive=True)
        write_attempted = True
        with raw_store.filesystem.open_output_stream(
            path, compression=None
        ) as stream:
            stream.write(payload)
        with raw_store.filesystem.open_input_file(path) as stream:
            observed = stream.read()
        if hashlib.sha256(observed).hexdigest() != expected:
            raise ReadinessError("FBref raw health object failed SHA256 readback")
    finally:
        if write_attempted and (
            raw_store.filesystem.get_file_info(path).type
            != fs.FileType.NotFound
        ):
            raw_store.filesystem.delete_file(path)
    if raw_store.filesystem.get_file_info(path).type != fs.FileType.NotFound:
        raise ReadinessError("FBref raw health object cleanup failed")
    return {
        "status": "passed",
        "health_prefix": "_health",
        "payload_bytes": len(payload),
        "sha256": expected,
        "cleanup_verified": True,
    }


def check_trino_roundtrip(manager, *, token: Optional[object] = None) -> dict:
    """Prove Iceberg write/read/drop capability with a unique health table."""

    name = _health_token(token)
    schema = "bronze"
    if manager.table_exists(schema, name):
        raise ReadinessError(f"Unexpected pre-existing Trino health table: {name}")
    create_attempted = False
    try:
        create_attempted = True
        manager._execute(
            f'CREATE TABLE iceberg."{schema}"."{name}" AS '
            f"SELECT '{name}' AS health_token"
        )
        rows = manager._execute(
            f'SELECT health_token FROM iceberg."{schema}"."{name}"',
            fetch=True,
        )
        if (
            not isinstance(rows, list)
            or len(rows) != 1
            or not isinstance(rows[0], (list, tuple))
            or len(rows[0]) != 1
            or str(rows[0][0]) != name
        ):
            raise ReadinessError("FBref Trino health table readback mismatch")
    finally:
        if create_attempted:
            manager.drop_table(schema, name, if_exists=True)
    if manager.table_exists(schema, name):
        raise ReadinessError("FBref Trino health table cleanup failed")
    return {"status": "passed", "cleanup_verified": True}


__all__ = [
    "EXPECTED_RAW_STORE_URI",
    "ReadinessError",
    "check_raw_store_roundtrip",
    "check_trino_roundtrip",
    "validate_proxy_pool",
    "validate_raw_store_uri",
]
