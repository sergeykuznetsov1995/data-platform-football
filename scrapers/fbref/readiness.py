"""Fail-closed, secret-safe production preflight checks for FBref.

The checks in this module are deliberately independent from Airflow so they
can be exercised in unit tests and by an operator before a control run is
created.  Health objects live outside the immutable raw namespace and are
always removed in ``finally`` blocks.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any, Mapping, Optional
from urllib.parse import urlsplit

from pyarrow import fs

from scrapers.fbref.browser_runtime import (
    CAMOUFOX_BROWSER_RELEASE,
    CAMOUFOX_BROWSER_VERSION,
    CAMOUFOX_PACKAGE_VERSION,
    CURL_CFFI_PACKAGE_VERSION,
    INSTALL_DIR,
    PLAYWRIGHT_PACKAGE_VERSION,
)
from scrapers.fbref.proxy_lease import FBREF_DAG_IDS, METER_ID


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


def validate_camoufox_runtime(
    *,
    package_version: Optional[str] = None,
    playwright_version: Optional[str] = None,
    curl_cffi_version: Optional[str] = None,
    install_dir: Optional[object] = None,
) -> dict[str, Any]:
    """Require the reviewed browser stack before any paid lease is opened."""

    try:
        if (
            package_version is None
            or playwright_version is None
            or curl_cffi_version is None
        ):
            from importlib.metadata import version

            package_version = package_version or version("camoufox")
            playwright_version = playwright_version or version("playwright")
            curl_cffi_version = curl_cffi_version or version("curl_cffi")
        if install_dir is None:
            install_dir = INSTALL_DIR
        root = Path(install_dir)
        document = json.loads((root / "version.json").read_text("utf-8"))
    except Exception as exc:  # noqa: BLE001 - local runtime boundary
        raise ReadinessError(
            f"FBref Camoufox runtime is unavailable: {type(exc).__name__}"
        ) from exc
    if not isinstance(document, Mapping):
        raise ReadinessError("FBref Camoufox version document is invalid")
    browser_version = str(document.get("version") or "")
    browser_release = str(document.get("release") or "")
    executable = root / "camoufox-bin"
    if (
        str(package_version) != CAMOUFOX_PACKAGE_VERSION
        or str(playwright_version) != PLAYWRIGHT_PACKAGE_VERSION
        or str(curl_cffi_version) != CURL_CFFI_PACKAGE_VERSION
        or browser_version != CAMOUFOX_BROWSER_VERSION
        or browser_release != CAMOUFOX_BROWSER_RELEASE
        or not executable.is_file()
        or not os.access(executable, os.X_OK)
    ):
        raise ReadinessError(
            "FBref Camoufox runtime differs from the reviewed production pin"
        )
    try:
        probe = subprocess.run(
            [str(executable), "--version"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=10,
            check=True,
        )
    except Exception as exc:  # noqa: BLE001 - local executable boundary
        raise ReadinessError(
            f"FBref Camoufox executable probe failed: {type(exc).__name__}"
        ) from exc
    expected_probe = f"Camoufox {browser_version}-{browser_release}"
    if expected_probe not in str(probe.stdout or ""):
        raise ReadinessError("FBref Camoufox executable version is invalid")
    return {
        "status": "passed",
        "camoufox_package": str(package_version),
        "camoufox_browser": f"{browser_version}-{browser_release}",
        "playwright": str(playwright_version),
        "curl_cffi": str(curl_cffi_version),
        "executable_verified": True,
        "executable_probe_verified": True,
    }


def validate_fbref_proxy_meter(
    control_url: object,
    *,
    control_token: object,
    required_bytes: int,
    minimum_configured_exits: int,
    required_ttl_seconds: int = 7200,
    timeout_seconds: float = 5.0,
    session: Optional[Any] = None,
) -> dict[str, Any]:
    """Authenticate and validate the dedicated meter without paid traffic."""

    requested = int(required_bytes)
    minimum = int(minimum_configured_exits)
    required_ttl = int(required_ttl_seconds)
    timeout = float(timeout_seconds)
    if requested <= 0 or minimum <= 0 or required_ttl <= 0 or timeout <= 0:
        raise ValueError("FBref proxy readiness limits must be positive")
    base = str(control_url or "").strip().rstrip("/")
    parsed = urlsplit(base)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ReadinessError(
            "FBREF_PROXY_CONTROL_URL must be credential-free absolute HTTP(S)"
        )
    token = str(control_token or "").strip()
    if len(token) < 32:
        raise ReadinessError(
            "FBREF_PROXY_CONTROL_TOKEN must contain at least 32 characters"
        )
    client = session
    if client is None:
        import requests

        client = requests.Session()
        client.trust_env = False
    try:
        response = client.get(
            f"{base}/v1/auth-check",
            headers={"X-Proxy-Control-Token": token},
            timeout=timeout,
        )
    except Exception as exc:  # noqa: BLE001 - secret-safe dependency boundary
        raise ReadinessError(
            f"FBref proxy meter is unavailable: {type(exc).__name__}"
        ) from exc
    status = int(getattr(response, "status_code", 0) or 0)
    if status != 200:
        raise ReadinessError(
            f"FBref proxy meter authentication failed (HTTP {status})"
        )
    try:
        payload = response.json()
    except Exception as exc:  # noqa: BLE001 - untrusted health response
        raise ReadinessError("FBref proxy meter returned invalid JSON") from exc
    if not isinstance(payload, Mapping):
        raise ReadinessError("FBref proxy meter returned a non-object")

    def integer(name: str) -> int:
        value = payload.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ReadinessError(f"FBref proxy meter has invalid {name}")
        return value

    daily_total = integer("daily_total_bytes")
    daily_budget = integer("daily_budget_bytes")
    daily_remaining = integer("daily_remaining_bytes")
    dagrun_budget = integer("dagrun_budget_bytes")
    url_budget = integer("url_budget_bytes")
    max_lease = integer("max_lease_bytes")
    max_ttl = integer("max_lease_ttl_seconds")
    max_active = integer("max_active_leases")
    configured = integer("configured_pool_count")
    lease_proxy_url = urlsplit(str(payload.get("lease_proxy_url") or ""))
    if (
        str(payload.get("status") or "") != "ok"
        or str(payload.get("meter") or "") != METER_ID
        or payload.get("fbref_source_ready") is not True
        or payload.get("fbref_dag_ids") != sorted(FBREF_DAG_IDS)
        or daily_total + daily_remaining != daily_budget
        or min(daily_remaining, dagrun_budget, url_budget, max_lease) < requested
        or max_ttl < required_ttl
        or max_active != 1
        or configured < minimum
        or lease_proxy_url.scheme != parsed.scheme
        or lease_proxy_url.hostname != parsed.hostname
        or lease_proxy_url.username is not None
        or lease_proxy_url.password is not None
        or lease_proxy_url.path not in {"", "/"}
        or lease_proxy_url.query
        or lease_proxy_url.fragment
    ):
        raise ReadinessError(
            "FBref dedicated proxy meter does not satisfy the live hard profile"
        )
    return {
        "status": "passed",
        "meter": METER_ID,
        "configured": configured,
        "minimum_configured": minimum,
        "daily_remaining_bytes": daily_remaining,
        "dagrun_budget_bytes": dagrun_budget,
        "url_budget_bytes": url_budget,
        "max_lease_bytes": max_lease,
        "max_lease_ttl_seconds": max_ttl,
        "max_active_leases": max_active,
        "probe": "authenticated_control_only_zero_paid_bytes",
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
    "validate_camoufox_runtime",
    "validate_fbref_proxy_meter",
    "validate_raw_store_uri",
]
