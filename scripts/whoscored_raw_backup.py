#!/usr/bin/env python3
"""Provider-neutral backup and restore verification for WhoScored raw data.

The workflow is deliberately inventory-driven and append-only:

1. ``inventory`` hashes every object in a stable source snapshot.
2. ``backup`` deduplicates objects into a content-addressed namespace and
   rejects immutable collisions.
3. ``verify-backup`` reads every expected content object before publishing the
   inventory marker.
4. ``verify-restore`` checks an independently restored source-layout tree.

S3 credentials are read from role-specific environment variables and never
stored in the inventory.  Local/file URIs use Arrow's filesystem resolver.
Under the production LocalExecutor topology, a short exclusive lock captures
the immutable path cut; historical bytes are hashed after writers resume.
Legacy writers that do not honor this barrier must be stopped during cutover.
"""

# ruff: noqa: E402 -- the trust anchor must run before every non-built-in import

from __future__ import annotations

import sys as _whoscored_bootstrap_sys

_whoscored_source = __file__
if not _whoscored_source.startswith("/"):
    raise RuntimeError("WhoScored entrypoint requires an absolute source path")
_whoscored_production = _whoscored_source.startswith("/opt/airflow/")
_whoscored_root = (
    "/opt/airflow"
    if _whoscored_production
    else _whoscored_source.rsplit("/scripts/", 1)[0]
)
if _whoscored_production:
    if (
        getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_schema", None)
        != 2
    ):
        raise RuntimeError("image-baked WhoScored startup anchor is required")
elif (
    getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_root", None)
    != _whoscored_root
):
    _whoscored_anchor_path = (
        _whoscored_root + "/docker/images/airflow/whoscored_runtime_startup.py"
    )
    _whoscored_anchor_globals = {
        "__builtins__": __builtins__,
        "sys": _whoscored_bootstrap_sys,
        "_WHOSCORED_RUNTIME_ROOT": _whoscored_root,
        "_WHOSCORED_REQUIRE_FULL_ATTESTATION": False,
    }
    with open(_whoscored_anchor_path, "rb") as _whoscored_anchor_handle:
        _whoscored_anchor_source = _whoscored_anchor_handle.read()
    exec(
        compile(_whoscored_anchor_source, _whoscored_anchor_path, "exec"),
        _whoscored_anchor_globals,
    )
_WHOSCORED_RUNTIME_CONTRACT = _whoscored_bootstrap_sys._load_whoscored_runtime_contract(
    _whoscored_root
)

import argparse
import hashlib
import json
import math
import os
import re
import sys
import tempfile
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Callable, Iterable, Mapping, Optional, TypeVar
from urllib.parse import urlparse
from uuid import uuid4

from pyarrow import fs

from scrapers.whoscored.raw_store import (
    RawObjectNotFound,
    RawStoreError,
    WhoScoredRawStore,
)

INVENTORY_VERSION = "whoscored-raw-inventory-v2"
RESTORE_DRILL_SCHEMA_VERSION = 2
RESTORE_DRILL_RECEIPT_PREFIX = "restore-drill-receipts/v2"
MINIMUM_WORM_RETENTION_HOURS = 24
DEFAULT_WORKERS = 16
MAX_WORKERS = 64
WORKERS_ENV = "WHOSCORED_BACKUP_WORKERS"
_INVENTORY_MARKER_RE = re.compile(
    r"^backup-inventories/[0-9]{8}T[0-9]{12}Z-"
    r"(?P<source>[0-9a-f]{16})-(?P<inventory>[0-9a-f]{64})\.json$"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ROLE_ENV_PREFIX = {
    "source": "WHOSCORED_BACKUP_SOURCE_S3_",
    "destination": "WHOSCORED_BACKUP_DESTINATION_S3_",
    "restore": "WHOSCORED_BACKUP_RESTORE_S3_",
}

_Input = TypeVar("_Input")
_Output = TypeVar("_Output")


def validate_worker_count(value: object) -> int:
    """Return a safe, bounded I/O worker count."""

    if isinstance(value, bool):
        raise ValueError("backup workers must be an integer in 1..64")
    try:
        workers = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError("backup workers must be an integer in 1..64") from exc
    if str(value).strip() != str(workers) or not 1 <= workers <= MAX_WORKERS:
        raise ValueError("backup workers must be an integer in 1..64")
    return workers


def _worker_argument(value: str) -> int:
    try:
        return validate_worker_count(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _bounded_map(
    items: Iterable[_Input],
    operation: Callable[[_Input], _Output],
    *,
    workers: int,
) -> list[_Output]:
    """Execute with at most ``workers`` payloads and futures in flight.

    Results retain input order even when I/O finishes out of order.  On the
    first failing completion no new work is submitted; pending work is
    cancelled and the bounded set of running calls is allowed to finish before
    the original exception is propagated.  Workers return metadata only, so
    object bytes are bounded by the number of active workers.
    """

    limit = validate_worker_count(workers)
    iterator = iter(enumerate(items))
    executor = ThreadPoolExecutor(
        max_workers=limit,
        thread_name_prefix="whoscored-backup",
    )
    in_flight: dict[Future[_Output], int] = {}
    results: dict[int, _Output] = {}

    def submit_one() -> bool:
        try:
            index, item = next(iterator)
        except StopIteration:
            return False
        in_flight[executor.submit(operation, item)] = index
        return True

    try:
        for _ in range(limit):
            if not submit_one():
                break
        while in_flight:
            done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
            completed: list[tuple[int, Future[_Output]]] = sorted(
                ((in_flight.pop(future), future) for future in done),
                key=lambda pair: pair[0],
            )
            failures: list[tuple[int, BaseException]] = []
            for index, future in completed:
                try:
                    results[index] = future.result()
                except BaseException as exc:  # cleanup must also cover interrupts
                    failures.append((index, exc))
            if failures:
                for future in in_flight:
                    future.cancel()
                raise min(failures, key=lambda pair: pair[0])[1]
            for _ in completed:
                submit_one()
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
    return [results[index] for index in sorted(results)]


def _performance(
    *,
    started_at: float,
    object_count: int,
    byte_count: int,
) -> dict[str, float]:
    elapsed = max(0.0, time.perf_counter() - started_at)
    denominator = max(elapsed, 1e-9)
    return {
        "elapsed_seconds": round(elapsed, 6),
        "objects_per_second": round(object_count / denominator, 3),
        "mib_per_second": round(byte_count / (1024 * 1024) / denominator, 3),
    }


def _unique_content_groups(inventory: Mapping[str, object]) -> list[dict]:
    """Group paths by checksum before scheduling content-level work."""

    groups: dict[str, dict] = {}
    for item in inventory["objects"]:  # type: ignore[index]
        checksum = item["sha256"]
        group = groups.setdefault(
            checksum,
            {
                "sha256": checksum,
                "bytes": item["bytes"],
                "paths": [],
            },
        )
        if group["bytes"] != item["bytes"]:
            raise ValueError(f"Inventory checksum has inconsistent sizes: {checksum}")
        group["paths"].append(item["path"])
    return [groups[checksum] for checksum in sorted(groups)]


def _credential_pair(prefix: str) -> tuple[Optional[str], Optional[str]]:
    access_key = os.environ.get(f"{prefix}ACCESS_KEY", "").strip()
    secret_key = os.environ.get(f"{prefix}SECRET_KEY", "").strip()
    if bool(access_key) != bool(secret_key):
        raise ValueError(
            f"{prefix}ACCESS_KEY and {prefix}SECRET_KEY must be set together"
        )
    return access_key or None, secret_key or None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_store_uri(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.username or parsed.password:
        raise ValueError("Credentials must not be embedded in a raw-store URI")
    if parsed.query or parsed.fragment or parsed.params:
        raise ValueError("Raw-store URI must not contain query or fragment data")
    if parsed.scheme == "s3":
        return f"s3://{parsed.netloc}/{parsed.path.lstrip('/')}".rstrip("/")
    return uri


def open_store(
    uri: str,
    *,
    role: str,
    allow_bucket_creation: bool = False,
) -> WhoScoredRawStore:
    """Open one local/S3 store with isolated role-specific credentials."""

    _WHOSCORED_RUNTIME_CONTRACT.require_production_runtime_class(
        operation="WhoScored raw backup storage"
    )
    clean_uri = _safe_store_uri(uri)
    parsed = urlparse(clean_uri)
    if parsed.scheme != "s3":
        return WhoScoredRawStore.from_uri(clean_uri)
    if not parsed.netloc:
        raise ValueError("S3 raw-store URI must contain a bucket")
    try:
        prefix = _ROLE_ENV_PREFIX[role]
    except KeyError as exc:
        raise ValueError(f"Unknown backup store role: {role}") from exc
    endpoint = os.environ.get(f"{prefix}ENDPOINT", "").strip() or None
    scheme = os.environ.get(f"{prefix}SCHEME", "https").strip()
    region = os.environ.get(f"{prefix}REGION", "us-east-1").strip()
    access_key, secret_key = _credential_pair(prefix)
    if role == "source" and access_key is None:
        access_key, secret_key = _credential_pair("WHOSCORED_RAW_S3_")
    if role == "source" and access_key is None:
        access_key, secret_key = _credential_pair("S3_")
    filesystem = fs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        endpoint_override=endpoint,
        scheme=scheme,
        region=region,
        background_writes=False,
        allow_bucket_creation=allow_bucket_creation,
    )
    root = f"{parsed.netloc}/{parsed.path.lstrip('/')}".rstrip("/")
    return WhoScoredRawStore(filesystem, root)


def open_s3_control_client(*, role: str):
    """Open the provider control API used to prove versioning/Object Lock."""

    try:
        prefix = _ROLE_ENV_PREFIX[role]
    except KeyError as exc:
        raise ValueError(f"Unknown backup store role: {role}") from exc
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise RawStoreError(
            "S3 control API is unavailable; Object Lock cannot be verified"
        ) from exc
    endpoint = os.environ.get(f"{prefix}ENDPOINT", "").strip()
    scheme = os.environ.get(f"{prefix}SCHEME", "https").strip()
    region = os.environ.get(f"{prefix}REGION", "us-east-1").strip()
    if scheme not in {"http", "https"} or not region:
        raise ValueError("S3 control endpoint configuration is invalid")
    access_key, secret_key = _credential_pair(prefix)
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=f"{scheme}://{endpoint}" if endpoint else None,
        region_name=region,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if endpoint else "auto"},
        ),
    )


def _destination_object_key(destination_uri: str, relative: str) -> tuple[str, str]:
    parsed = urlparse(_safe_store_uri(destination_uri))
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError("immutable retention verification requires an S3 destination")
    pure = PurePosixPath(relative)
    if not relative or pure.is_absolute() or ".." in pure.parts:
        raise ValueError("immutable retention object key is unsafe")
    prefix = parsed.path.strip("/")
    return parsed.netloc, str(PurePosixPath(prefix) / pure) if prefix else str(pure)


def verify_s3_compliance_retention(
    destination_uri: str,
    objects: Iterable[Mapping[str, object]],
    *,
    client=None,
    now: Optional[datetime] = None,
    workers: int = DEFAULT_WORKERS,
) -> dict[str, object]:
    """Prove live COMPLIANCE retention for every exact referenced object."""

    if os.environ.get("WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE", "").strip() != (
        "object-lock"
    ):
        raise RawStoreError(
            "backup retention mode must be object-lock and provider-verified"
        )
    reference_now = now or datetime.now(timezone.utc)
    if reference_now.tzinfo is None or reference_now.utcoffset() is None:
        raise ValueError("Object Lock verification clock must be timezone-aware")
    reference_now = reference_now.astimezone(timezone.utc)
    minimum_until = reference_now + timedelta(hours=MINIMUM_WORM_RETENTION_HOURS)
    normalized: dict[str, int] = {}
    bucket: Optional[str] = None
    for item in objects:
        if not isinstance(item, Mapping) or frozenset(item) != {"key", "bytes"}:
            raise ValueError("immutable retention object descriptor is invalid")
        relative = item.get("key")
        size = item.get("bytes")
        if (
            not isinstance(relative, str)
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
        ):
            raise ValueError("immutable retention object descriptor is invalid")
        item_bucket, key = _destination_object_key(destination_uri, relative)
        if bucket is None:
            bucket = item_bucket
        elif bucket != item_bucket:
            raise ValueError("immutable retention objects span multiple buckets")
        prior = normalized.setdefault(key, size)
        if prior != size:
            raise ValueError("immutable retention object size is inconsistent")
    if bucket is None or not normalized:
        raise ValueError("immutable retention requires a non-empty exact object set")
    control = client or open_s3_control_client(role="destination")
    try:
        versioning = control.get_bucket_versioning(Bucket=bucket)
        lock = control.get_object_lock_configuration(Bucket=bucket)
    except Exception as exc:
        raise RawStoreError(
            "destination versioning/Object Lock capability cannot be verified"
        ) from exc
    configuration = (
        lock.get("ObjectLockConfiguration") if isinstance(lock, Mapping) else None
    )
    rule = configuration.get("Rule") if isinstance(configuration, Mapping) else None
    default = rule.get("DefaultRetention") if isinstance(rule, Mapping) else None
    days = default.get("Days") if isinstance(default, Mapping) else None
    years = default.get("Years") if isinstance(default, Mapping) else None
    if (
        not isinstance(versioning, Mapping)
        or versioning.get("Status") != "Enabled"
        or not isinstance(configuration, Mapping)
        or configuration.get("ObjectLockEnabled") != "Enabled"
        or not isinstance(default, Mapping)
        or default.get("Mode") != "COMPLIANCE"
        or (days is None) == (years is None)
        or isinstance(days if days is not None else years, bool)
        or not isinstance(days if days is not None else years, int)
        or int(days if days is not None else years) <= 0
    ):
        raise RawStoreError(
            "destination lacks enabled versioning and COMPLIANCE Object Lock"
        )

    def verify_head(item: tuple[str, int]) -> dict[str, object]:
        key, expected_size = item
        try:
            head = control.head_object(Bucket=bucket, Key=key)
        except Exception as exc:
            raise RawStoreError(
                "retained backup object is unavailable from the control API"
            ) from exc
        retain_until = (
            head.get("ObjectLockRetainUntilDate") if isinstance(head, Mapping) else None
        )
        if (
            not isinstance(head, Mapping)
            or head.get("ContentLength") != expected_size
            or not isinstance(head.get("VersionId"), str)
            or not head["VersionId"]
            or head["VersionId"] == "null"
            or head.get("DeleteMarker") is True
            or head.get("ObjectLockMode") != "COMPLIANCE"
            or not isinstance(retain_until, datetime)
            or retain_until.tzinfo is None
            or retain_until.utcoffset() is None
            or retain_until.astimezone(timezone.utc) < minimum_until
        ):
            raise RawStoreError(
                "backup object lacks exact versioned COMPLIANCE retention"
            )
        return {"key": key, "retain_until": retain_until.astimezone(timezone.utc)}

    retained = _bounded_map(sorted(normalized.items()), verify_head, workers=workers)
    earliest = min(item["retain_until"] for item in retained)
    assert isinstance(earliest, datetime)
    default_days = int(days) if days is not None else int(years) * 365
    return {
        "bucket": bucket,
        "checked_object_count": len(retained),
        "default_retention_days": default_days,
        "default_retention_mode": "COMPLIANCE",
        "earliest_retain_until": earliest.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "minimum_remaining_hours": MINIMUM_WORM_RETENTION_HOURS,
        "object_lock_enabled": "Enabled",
        "status": "passed",
        "versioning_status": "Enabled",
    }


def validate_distinct_store_roots(
    source_uri: str,
    destination_uri: str,
    *,
    source_role: str = "source",
    destination_role: str = "destination",
    require_off_host: bool = False,
) -> None:
    """Reject aliases and nested roots that would recursively back up themselves."""

    source = urlparse(_safe_store_uri(source_uri))
    destination = urlparse(_safe_store_uri(destination_uri))

    if require_off_host:
        if source.scheme != "s3" or destination.scheme != "s3":
            raise ValueError("Production backup requires two S3 endpoints")
        source_env = _ROLE_ENV_PREFIX[source_role]
        destination_env = _ROLE_ENV_PREFIX[destination_role]
        source_endpoint = os.environ.get(f"{source_env}ENDPOINT", "").strip().lower()
        destination_endpoint = (
            os.environ.get(f"{destination_env}ENDPOINT", "").strip().lower()
        )
        retention = os.environ.get(
            "WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE", ""
        ).strip()
        if retention != "object-lock":
            raise ValueError(
                "Off-host backup requires provider-verified object-lock retention"
            )
        destination_identity = destination_endpoint or (
            "aws-default:"
            + os.environ.get(f"{destination_env}REGION", "us-east-1").strip().lower()
        )
        if (
            not source_endpoint
            or source_endpoint == destination_identity
            or source.netloc.casefold() == destination.netloc.casefold()
        ):
            raise ValueError(
                "Backup destination must use a distinct S3 endpoint and bucket"
            )

    def location(parsed, role: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
        if parsed.scheme == "s3":
            prefix = _ROLE_ENV_PREFIX[role]
            endpoint = os.environ.get(f"{prefix}ENDPOINT", "").strip().lower()
            scheme = os.environ.get(f"{prefix}SCHEME", "https").strip().lower()
            backend = ("s3", scheme, endpoint, parsed.netloc.lower())
            root = tuple(PurePosixPath(parsed.path.lstrip("/")).parts)
            return backend, root
        if parsed.scheme not in {"", "file"}:
            raise ValueError(
                "Backup root comparison supports only local/file and s3 URIs"
            )
        path = Path(parsed.path if parsed.scheme == "file" else parsed.geturl())
        resolved = path.expanduser().resolve(strict=False)
        return ("file",), tuple(resolved.parts)

    source_backend, source_root = location(source, source_role)
    destination_backend, destination_root = location(destination, destination_role)
    if source_backend != destination_backend:
        return

    shared = min(len(source_root), len(destination_root))
    if source_root[:shared] == destination_root[:shared]:
        raise ValueError(
            "Backup source and destination roots must not be equal or nested"
        )


def _s3_prefixes_overlap(first_uri: str, second_uri: str) -> bool:
    first = urlparse(_safe_store_uri(first_uri))
    second = urlparse(_safe_store_uri(second_uri))
    if first.scheme != "s3" or second.scheme != "s3":
        return False
    if first.netloc.casefold() != second.netloc.casefold():
        return False
    first_parts = tuple(PurePosixPath(first.path.lstrip("/")).parts)
    second_parts = tuple(PurePosixPath(second.path.lstrip("/")).parts)
    shared = min(len(first_parts), len(second_parts))
    return first_parts[:shared] == second_parts[:shared]


def _relative_path(store: WhoScoredRawStore, path: str) -> str:
    root = store.root.rstrip("/")
    prefix = f"{root}/" if root else ""
    if prefix and path.startswith(prefix):
        relative = path[len(prefix) :]
    elif not root:
        relative = path.lstrip("/")
    else:
        raise RawStoreError(f"Inventory path escaped raw root: {path}")
    pure = PurePosixPath(relative)
    if not relative or pure.is_absolute() or ".." in pure.parts:
        raise RawStoreError(f"Unsafe inventory object path: {relative!r}")
    return str(pure)


def _inventory_digest(objects: list[dict]) -> str:
    rendered = json.dumps(
        objects,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


def _document_digest(payload: Mapping[str, object]) -> str:
    fields = {
        key: payload[key]
        for key in (
            "inventory_version",
            "created_at",
            "snapshot_started_at",
            "snapshot_completed_at",
            "snapshot_consistency",
            "source_uri",
            "object_count",
            "total_bytes",
            "objects_sha256",
            "objects",
        )
    }
    rendered = json.dumps(
        fields,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


def build_inventory(
    store: WhoScoredRawStore,
    *,
    source_uri: str,
    workers: int = DEFAULT_WORKERS,
    allow_empty: bool = False,
) -> dict:
    """Capture a stable path cut, then hash immutable objects concurrently."""

    with store.snapshot_lock(exclusive=True):
        snapshot_started_at = _utc_now_iso()

        def list_snapshot():
            return store.filesystem.get_file_info(
                fs.FileSelector(store.root, allow_not_found=True, recursive=True)
            )

        infos = store._retry_io("inventory-list", "", list_snapshot)
        snapshot_completed_at = _utc_now_iso()

    file_infos = sorted(
        (info for info in infos if info.type == fs.FileType.File),
        key=lambda item: item.path,
    )

    def hash_object(info) -> dict:
        relative = _relative_path(store, info.path)
        payload = store._read_bytes(relative)
        return {
            "path": relative,
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }

    objects = _bounded_map(file_infos, hash_object, workers=workers)
    if not objects and not allow_empty:
        raise RawStoreError(
            "Source inventory is empty; pass --allow-empty only for an "
            "explicitly optional state prefix"
        )
    created_at = _utc_now_iso()
    inventory = {
        "inventory_version": INVENTORY_VERSION,
        "created_at": created_at,
        "snapshot_started_at": snapshot_started_at,
        "snapshot_completed_at": snapshot_completed_at,
        "snapshot_consistency": "localexecutor-cut-flock-v1",
        "source_uri": _safe_store_uri(source_uri),
        "object_count": len(objects),
        "total_bytes": sum(item["bytes"] for item in objects),
        "objects_sha256": _inventory_digest(objects),
        "objects": objects,
    }
    inventory["inventory_sha256"] = _document_digest(inventory)
    return inventory


def measure_store_metadata(store: WhoScoredRawStore) -> dict:
    """Return a current recursive object/byte count without reading payloads."""

    started_at = time.perf_counter()
    with store.snapshot_lock(exclusive=True):
        snapshot_started_at = _utc_now_iso()
        infos = store._retry_io(
            "metadata-list",
            "",
            lambda: store.filesystem.get_file_info(
                fs.FileSelector(store.root, allow_not_found=False, recursive=True)
            ),
        )
        snapshot_completed_at = _utc_now_iso()
    files = [info for info in infos if info.type == fs.FileType.File]
    total_bytes = sum(max(0, int(info.size)) for info in files)
    return {
        "object_count": len(files),
        "total_bytes": total_bytes,
        "snapshot_started_at": snapshot_started_at,
        "snapshot_completed_at": snapshot_completed_at,
        **_performance(
            started_at=started_at,
            object_count=len(files),
            byte_count=total_bytes,
        ),
    }


def validate_backup_configuration(
    source_uri: str,
    destination_uri: str,
    *,
    workers: int = DEFAULT_WORKERS,
) -> dict:
    """Fail fast on off-host policy, credential pairs and destination I/O."""

    normalized_workers = validate_worker_count(workers)
    validate_distinct_store_roots(
        source_uri,
        destination_uri,
        require_off_host=True,
    )
    source = open_store(source_uri, role="source")
    destination = open_store(destination_uri, role="destination")
    # The backup reader must be able to list the configured source root. This
    # is deliberately non-recursive: preflight must not spend hours before the
    # inventory task starts.
    source._retry_io(
        "backup-preflight-source",
        "",
        lambda: source.filesystem.get_file_info(
            fs.FileSelector(source.root, allow_not_found=True, recursive=False)
        ),
    )
    # A unique, content-verified object proves *current* destination Put/Get
    # access without requiring Delete (which WORM stores normally forbid).
    # Reusing a stable key would let read-only credentials pass after the first
    # successful run because immutable writes validate an existing object.
    timestamp = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H%M%S")
    probe_key = f"backup-preflight/v1/{timestamp}-{uuid4().hex}.raw"
    destination._write_immutable_bytes(probe_key, b"")
    retention = (
        verify_s3_compliance_retention(
            destination_uri,
            ({"key": probe_key, "bytes": 0},),
            workers=normalized_workers,
        )
        if urlparse(_safe_store_uri(destination_uri)).scheme == "s3"
        else None
    )
    return {
        "passed": True,
        "workers": normalized_workers,
        "source_uri": _safe_store_uri(source_uri),
        "destination_uri": _safe_store_uri(destination_uri),
        "destination_probe": probe_key,
        "retention": retention,
    }


def validate_inventory(payload: Mapping[str, object]) -> dict:
    if payload.get("inventory_version") != INVENTORY_VERSION:
        raise ValueError("Unsupported WhoScored raw inventory version")
    timestamps = {}
    for field in ("created_at", "snapshot_started_at", "snapshot_completed_at"):
        value = payload.get(field)
        if not isinstance(value, str):
            raise ValueError(f"Inventory {field} must be a timestamp")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"Inventory {field} must be ISO-8601") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(f"Inventory {field} must be timezone-aware")
        timestamps[field] = parsed
    if timestamps["snapshot_started_at"] > timestamps["snapshot_completed_at"]:
        raise ValueError("Inventory snapshot interval is inverted")
    if timestamps["snapshot_completed_at"] > timestamps["created_at"]:
        raise ValueError("Inventory was created before its snapshot cut")
    if payload.get("snapshot_consistency") != "localexecutor-cut-flock-v1":
        raise ValueError("Unsupported inventory snapshot consistency contract")
    source_uri = payload.get("source_uri")
    if not isinstance(source_uri, str) or not source_uri:
        raise ValueError("Inventory source_uri is required")
    _safe_store_uri(source_uri)
    objects = payload.get("objects")
    if not isinstance(objects, list):
        raise ValueError("Inventory objects must be a list")
    normalized = []
    seen = set()
    for item in objects:
        if not isinstance(item, Mapping):
            raise ValueError("Inventory object entry must be a mapping")
        path = item.get("path")
        size = item.get("bytes")
        checksum = item.get("sha256")
        if (
            not isinstance(path, str)
            or not path
            or PurePosixPath(path).is_absolute()
            or ".." in PurePosixPath(path).parts
            or path in seen
        ):
            raise ValueError(f"Invalid or duplicate inventory path: {path!r}")
        if type(size) is not int or size < 0:
            raise ValueError(f"Invalid inventory size for {path}")
        if (
            not isinstance(checksum, str)
            or len(checksum) != 64
            or any(char not in "0123456789abcdef" for char in checksum)
        ):
            raise ValueError(f"Invalid inventory checksum for {path}")
        seen.add(path)
        normalized.append({"path": path, "bytes": size, "sha256": checksum})
    if normalized != sorted(normalized, key=lambda item: item["path"]):
        raise ValueError("Inventory objects must be sorted by path")
    if payload.get("object_count") != len(normalized):
        raise ValueError("Inventory object_count mismatch")
    if payload.get("total_bytes") != sum(item["bytes"] for item in normalized):
        raise ValueError("Inventory total_bytes mismatch")
    if payload.get("objects_sha256") != _inventory_digest(normalized):
        raise ValueError("Inventory object-list checksum mismatch")
    validated = {**dict(payload), "objects": normalized}
    if payload.get("inventory_sha256") != _document_digest(validated):
        raise ValueError("Inventory document checksum mismatch")
    return validated


def load_inventory(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not read inventory {path}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError("Inventory document must be a mapping")
    return validate_inventory(payload)


def write_inventory(path: Path, inventory: Mapping[str, object]) -> None:
    validated = validate_inventory(inventory)
    rendered = (
        json.dumps(validated, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        text=True,
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def verify_store(
    store: WhoScoredRawStore,
    inventory: Mapping[str, object],
    *,
    workers: int = DEFAULT_WORKERS,
) -> dict:
    started_at = time.perf_counter()
    validated = validate_inventory(inventory)

    def verify_object(item: Mapping[str, object]) -> dict:
        path = item["path"]
        try:
            payload = store._read_bytes(path)
        except RawObjectNotFound:
            return {"path": path, "status": "missing", "bytes": 0}
        if (
            len(payload) != item["bytes"]
            or hashlib.sha256(payload).hexdigest() != item["sha256"]
        ):
            return {"path": path, "status": "corrupt", "bytes": len(payload)}
        return {"path": path, "status": "ok", "bytes": len(payload)}

    results = _bounded_map(validated["objects"], verify_object, workers=workers)
    missing = sorted(item["path"] for item in results if item["status"] == "missing")
    corrupt = sorted(item["path"] for item in results if item["status"] == "corrupt")
    checked_bytes = sum(item["bytes"] for item in results)
    return {
        "passed": not missing and not corrupt,
        "expected_objects": validated["object_count"],
        "checked_objects": validated["object_count"] - len(missing),
        "checked_bytes": checked_bytes,
        "missing": missing,
        "corrupt": corrupt,
        "objects_sha256": validated["objects_sha256"],
        **_performance(
            started_at=started_at,
            object_count=len(results),
            byte_count=checked_bytes,
        ),
    }


def backup_object_key(checksum: str) -> str:
    """Return the immutable off-host key for one verified payload."""

    if (
        not isinstance(checksum, str)
        or len(checksum) != 64
        or any(char not in "0123456789abcdef" for char in checksum)
    ):
        raise ValueError("Backup object checksum must be lowercase SHA-256")
    return f"backup-objects/sha256/{checksum[:2]}/{checksum}.raw"


def inventory_marker_key(inventory: Mapping[str, object]) -> str:
    validated = validate_inventory(inventory)
    created = datetime.fromisoformat(
        str(validated["created_at"]).replace("Z", "+00:00")
    )
    token = created.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    source_fingerprint = hashlib.sha256(
        validated["source_uri"].encode("utf-8")
    ).hexdigest()[:16]
    return (
        f"backup-inventories/{token}-{source_fingerprint}-"
        f"{validated['inventory_sha256']}.json"
    )


def backup_inventory_object_descriptors(
    inventory: Mapping[str, object],
) -> list[dict[str, object]]:
    """Return every exact retained object required by one committed inventory."""

    validated = validate_inventory(inventory)
    descriptors = [
        {"key": backup_object_key(group["sha256"]), "bytes": group["bytes"]}
        for group in _unique_content_groups(validated)
    ]
    descriptors.append(
        {
            "key": inventory_marker_key(validated),
            "bytes": len(WhoScoredRawStore._render_json(validated)),
        }
    )
    return descriptors


def list_backup_inventories(
    store: WhoScoredRawStore,
    *,
    expected_source_uri: str,
    limit: int = 20,
) -> dict:
    """List recent self-contained inventory markers without reading payloads."""

    if isinstance(limit, bool) or type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("inventory list limit must be an integer in 1..100")
    expected_source = _safe_store_uri(expected_source_uri)
    expected_fingerprint = hashlib.sha256(expected_source.encode("utf-8")).hexdigest()[
        :16
    ]
    infos = store._retry_io(
        "backup-inventory-list",
        "backup-inventories",
        lambda: store.filesystem.get_file_info(
            fs.FileSelector(
                store._path("backup-inventories"),
                allow_not_found=True,
                recursive=False,
            )
        ),
    )
    markers = []
    unexpected = []
    for info in infos:
        if info.type != fs.FileType.File:
            continue
        key = _relative_path(store, info.path)
        match = _INVENTORY_MARKER_RE.fullmatch(key)
        if match is None:
            unexpected.append(key)
            continue
        if match.group("source") != expected_fingerprint:
            continue
        markers.append(
            {
                "inventory_key": key,
                "source_fingerprint": match.group("source"),
                "bytes": max(0, int(info.size)),
                "last_modified": (
                    info.mtime.astimezone(timezone.utc).isoformat()
                    if info.mtime is not None
                    else None
                ),
            }
        )
    markers.sort(key=lambda item: item["inventory_key"], reverse=True)
    return {
        "passed": bool(markers),
        "expected_source_uri": expected_source,
        "source_fingerprint": expected_fingerprint,
        "inventory_count": len(markers),
        "returned_count": min(limit, len(markers)),
        "inventories": markers[:limit],
        "unexpected_keys": sorted(unexpected),
    }


def fetch_backup_inventory(
    store: WhoScoredRawStore,
    inventory_key: str,
    *,
    expected_source_uri: str,
) -> dict:
    """Read and authenticate one explicitly selected off-host marker."""

    if not isinstance(inventory_key, str) or not _INVENTORY_MARKER_RE.fullmatch(
        inventory_key
    ):
        raise ValueError("inventory key must be an exact backup-inventories marker")
    payload = store._read_json(inventory_key)
    if not isinstance(payload, Mapping):
        raise ValueError("Backup inventory marker must contain a mapping")
    validated = validate_inventory(payload)
    if inventory_marker_key(validated) != inventory_key:
        raise ValueError("Backup inventory marker key does not match its contents")
    if validated["source_uri"] != _safe_store_uri(expected_source_uri):
        raise ValueError("Backup inventory source_uri does not match expected source")
    return validated


def verify_backup_store(
    store: WhoScoredRawStore,
    inventory: Mapping[str, object],
    *,
    require_marker: bool = False,
    workers: int = DEFAULT_WORKERS,
) -> dict:
    """Verify the content-addressed backup referenced by one inventory."""

    started_at = time.perf_counter()
    validated = validate_inventory(inventory)
    groups = _unique_content_groups(validated)
    expected_content_bytes = sum(int(group["bytes"]) for group in groups)

    def verify_content(group: Mapping[str, object]) -> dict:
        path = group["paths"][0]  # type: ignore[index]
        key = backup_object_key(group["sha256"])
        try:
            payload = store._read_bytes(key)
        except RawObjectNotFound:
            return {"path": path, "status": "missing", "bytes": 0}
        if (
            len(payload) != group["bytes"]
            or hashlib.sha256(payload).hexdigest() != group["sha256"]
        ):
            return {"path": path, "status": "corrupt", "bytes": len(payload)}
        return {"path": path, "status": "ok", "bytes": len(payload)}

    results = _bounded_map(groups, verify_content, workers=workers)
    missing = sorted(item["path"] for item in results if item["status"] == "missing")
    corrupt = sorted(item["path"] for item in results if item["status"] == "corrupt")
    checked_bytes = sum(item["bytes"] for item in results)
    marker_key = inventory_marker_key(validated)
    marker_present = store._exists(marker_key)
    marker_valid = False
    if marker_present:
        try:
            marker_valid = store._read_json(marker_key) == validated
        except RawStoreError:
            marker_valid = False
    return {
        "passed": not missing and not corrupt and (not require_marker or marker_valid),
        "expected_objects": validated["object_count"],
        "expected_content_objects": len(groups),
        "expected_content_bytes": expected_content_bytes,
        "checked_content_objects": len(results) - len(missing),
        "checked_bytes": checked_bytes,
        "missing": missing,
        "corrupt": corrupt,
        "inventory_marker": marker_key,
        "marker_required": require_marker,
        "marker_present": marker_present,
        "marker_valid": marker_valid,
        "objects_sha256": validated["objects_sha256"],
        **_performance(
            started_at=started_at,
            object_count=len(results),
            byte_count=checked_bytes,
        ),
    }


def backup_inventory(
    source: WhoScoredRawStore,
    destination: WhoScoredRawStore,
    inventory: Mapping[str, object],
    *,
    apply: bool,
    workers: int = DEFAULT_WORKERS,
) -> dict:
    started_at = time.perf_counter()
    validated = validate_inventory(inventory)
    groups = _unique_content_groups(validated)

    def copy_content(group: dict) -> dict:
        source_payload: Optional[bytes] = None
        checked_bytes = 0
        for path in group["paths"]:
            payload = source._read_bytes(path)
            checked_bytes += len(payload)
            if (
                len(payload) != group["bytes"]
                or hashlib.sha256(payload).hexdigest() != group["sha256"]
            ):
                raise RawStoreError(f"Source changed after inventory: {path}")
            if source_payload is None:
                source_payload = payload
        if source_payload is None:  # validated groups are never empty
            raise AssertionError("Empty inventory content group")
        destination_key = backup_object_key(group["sha256"])
        try:
            destination_payload = destination._read_bytes(destination_key)
        except RawObjectNotFound:
            destination_payload = None
        if destination_payload is not None:
            if destination_payload != source_payload:
                raise RawStoreError(f"Immutable backup collision: {destination_key}")
            return {
                "key": destination_key,
                "status": "already_present",
                "checked_bytes": checked_bytes,
            }
        if apply:
            # Different payloads cannot legitimately contend for this key: it
            # is derived from the verified bytes, not from a mutable source
            # path.  The immutable writer performs a final read-back as well.
            destination._write_immutable_bytes(destination_key, source_payload)
        return {
            "key": destination_key,
            "status": "copied",
            "checked_bytes": checked_bytes,
        }

    results = _bounded_map(groups, copy_content, workers=workers)
    copied = sorted(item["key"] for item in results if item["status"] == "copied")
    already_present = sorted(
        item["key"] for item in results if item["status"] == "already_present"
    )

    inventory_key: Optional[str] = None
    if apply:
        verification = verify_backup_store(
            destination,
            validated,
            workers=workers,
        )
        if not verification["passed"]:
            raise RawStoreError("Backup read-after-copy verification failed")
        inventory_key = inventory_marker_key(validated)
        # The marker is the durable statement that the snapshot is complete;
        # publish it only after every referenced object has passed read-back.
        destination._write_immutable_json(inventory_key, validated)
    return {
        "applied": apply,
        "copied_objects": len(copied) if apply else 0,
        "would_copy_objects": len(copied),
        "already_present_objects": len(already_present),
        "inventory_key": inventory_key,
        "inventory_sha256": validated["inventory_sha256"],
        "objects_sha256": validated["objects_sha256"],
        "checked_source_bytes": sum(item["checked_bytes"] for item in results),
        "unique_content_bytes": sum(group["bytes"] for group in groups),
        **_performance(
            started_at=started_at,
            object_count=len(groups),
            byte_count=sum(item["checked_bytes"] for item in results),
        ),
    }


def restore_inventory(
    backup: WhoScoredRawStore,
    destination: WhoScoredRawStore,
    inventory: Mapping[str, object],
    *,
    apply: bool,
    workers: int = DEFAULT_WORKERS,
    require_empty: bool = False,
) -> dict:
    """Reconstruct one inventory into an empty or idempotent source-layout root."""

    started_at = time.perf_counter()
    validated = validate_inventory(inventory)
    committed = verify_backup_store(
        backup,
        validated,
        require_marker=True,
        workers=workers,
    )
    if not committed["passed"]:
        raise RawStoreError(
            "Restore requires a complete, marker-verified backup inventory"
        )
    expected_paths = {item["path"] for item in validated["objects"]}
    groups = _unique_content_groups(validated)
    with destination.snapshot_lock(exclusive=True):
        infos = destination._retry_io(
            "restore-list",
            "",
            lambda: destination.filesystem.get_file_info(
                fs.FileSelector(
                    destination.root,
                    allow_not_found=True,
                    recursive=True,
                )
            ),
        )
        existing_paths = {
            _relative_path(destination, info.path)
            for info in infos
            if info.type == fs.FileType.File
        }
        if require_empty and existing_paths:
            raise RawStoreError("Restore drill requires an empty destination")
        extras = sorted(existing_paths - expected_paths)
        if extras:
            raise RawStoreError(
                "Restore destination contains objects outside the inventory: "
                + ", ".join(extras[:10])
            )

        def restore_content(group: dict) -> dict:
            backup_payload = backup._read_bytes(backup_object_key(group["sha256"]))
            if (
                len(backup_payload) != group["bytes"]
                or hashlib.sha256(backup_payload).hexdigest() != group["sha256"]
            ):
                raise RawStoreError(
                    "Backup object is corrupt for restore: " + group["paths"][0]
                )
            copied_paths = []
            existing = []
            for path in group["paths"]:
                if path in existing_paths:
                    if destination._read_bytes(path) != backup_payload:
                        raise RawStoreError(f"Immutable restore collision: {path}")
                    existing.append(path)
                    continue
                copied_paths.append(path)
                if apply:
                    destination._write_immutable_bytes(path, backup_payload)
            return {
                "copied": copied_paths,
                "already_present": existing,
                "bytes": len(backup_payload),
            }

        results = _bounded_map(groups, restore_content, workers=workers)
        copied = sorted(path for item in results for path in item["copied"])
        already_present = sorted(
            path for item in results for path in item["already_present"]
        )
        if apply:
            verification = verify_store(
                destination,
                validated,
                workers=workers,
            )
            if not verification["passed"]:
                raise RawStoreError("Restore read-after-write verification failed")
    return {
        "applied": apply,
        "copied_objects": len(copied) if apply else 0,
        "would_copy_objects": len(copied),
        "already_present_objects": len(already_present),
        "inventory_sha256": validated["inventory_sha256"],
        "objects_sha256": validated["objects_sha256"],
        "checked_backup_bytes": sum(item["bytes"] for item in results),
        **_performance(
            started_at=started_at,
            object_count=len(groups),
            byte_count=sum(item["bytes"] for item in results),
        ),
    }


def _restore_drill_utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _canonical_restore_drill_time(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("restore-drill clock must be timezone-aware")
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
    )


def _canonical_restore_drill_bytes(value: Mapping[str, object]) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ValueError("restore-drill evidence is not canonical JSON") from exc


def restore_drill_receipt_key(proof: Mapping[str, object]) -> tuple[str, str]:
    """Return the content-addressed off-host key for one completed drill."""

    completed_at = proof.get("completed_at")
    if not isinstance(completed_at, str):
        raise ValueError("restore-drill proof lacks completed_at")
    try:
        completed = datetime.strptime(completed_at, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise ValueError("restore-drill completed_at is not canonical UTC") from exc
    digest = hashlib.sha256(_canonical_restore_drill_bytes(proof)).hexdigest()
    timestamp = completed.strftime("%Y%m%dT%H%M%SZ")
    return f"{RESTORE_DRILL_RECEIPT_PREFIX}/{timestamp}-{digest}.json", digest


def write_restore_drill_evidence(path: Path, evidence: Mapping[str, object]) -> None:
    """Atomically renew the private local projection of an off-host receipt."""

    if not path.is_absolute():
        raise ValueError("restore-drill evidence output must be absolute")
    rendered = _canonical_restore_drill_bytes(evidence)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def revalidate_restore_drill_backup(
    *,
    backup_uri: str,
    evidence: Mapping[str, object],
    backup_store: Optional[WhoScoredRawStore] = None,
    store_opener: Optional[Callable[..., WhoScoredRawStore]] = None,
    s3_control_client=None,
    now: Optional[datetime] = None,
    workers: int = DEFAULT_WORKERS,
) -> dict[str, object]:
    """Re-read/hash the exact raw+ops backup and prove live COMPLIANCE locks."""

    normalized_workers = validate_worker_count(workers)
    destination_uri = _safe_store_uri(backup_uri)
    if evidence.get("backup_destination_uri") != destination_uri:
        raise RawStoreError("restore evidence backup destination has drifted")
    sources = evidence.get("sources")
    receipt = evidence.get("off_host_receipt")
    if (
        not isinstance(sources, list)
        or len(sources) != 2
        or not isinstance(receipt, Mapping)
    ):
        raise RawStoreError("restore evidence lacks exact raw+ops backup identities")
    proof = {
        name: value for name, value in evidence.items() if name != "off_host_receipt"
    }
    expected_receipt_key, expected_receipt_sha256 = restore_drill_receipt_key(proof)
    if receipt != {
        "key": expected_receipt_key,
        "sha256": expected_receipt_sha256,
    }:
        raise RawStoreError("restore evidence receipt identity has drifted")
    backup = backup_store or (store_opener or open_store)(
        destination_uri, role="destination"
    )
    if backup._read_json(expected_receipt_key) != proof:
        raise RawStoreError("off-host restore receipt is missing or changed")
    descriptors: list[dict[str, object]] = [
        {
            "key": expected_receipt_key,
            "bytes": len(WhoScoredRawStore._render_json(proof)),
        }
    ]
    inventories: list[dict[str, object]] = []
    seen_sources: set[str] = set()
    for source in sources:
        if not isinstance(source, Mapping):
            raise RawStoreError("restore evidence source is invalid")
        source_uri = source.get("source_uri")
        inventory_key = source.get("inventory_key")
        if (
            not isinstance(source_uri, str)
            or source_uri in seen_sources
            or not isinstance(inventory_key, str)
        ):
            raise RawStoreError("restore evidence source identity is invalid")
        seen_sources.add(source_uri)
        inventory = fetch_backup_inventory(
            backup,
            inventory_key,
            expected_source_uri=source_uri,
        )
        for field in (
            "inventory_sha256",
            "objects_sha256",
            "snapshot_started_at",
            "snapshot_completed_at",
            "object_count",
            "total_bytes",
        ):
            if inventory.get(field) != source.get(field):
                raise RawStoreError(
                    "live backup inventory differs from restore evidence"
                )
        verification = verify_backup_store(
            backup,
            inventory,
            require_marker=True,
            workers=normalized_workers,
        )
        if (
            verification.get("passed") is not True
            or verification.get("marker_present") is not True
            or verification.get("marker_valid") is not True
            or verification.get("checked_content_objects")
            != verification.get("expected_content_objects")
            or verification.get("checked_bytes")
            != verification.get("expected_content_bytes")
        ):
            raise RawStoreError("live off-host backup object verification failed")
        descriptors.extend(backup_inventory_object_descriptors(inventory))
        inventories.append(
            {
                "checked_bytes": verification["checked_bytes"],
                "checked_content_objects": verification["checked_content_objects"],
                "expected_content_bytes": verification["expected_content_bytes"],
                "expected_content_objects": verification["expected_content_objects"],
                "inventory_key": inventory_key,
                "inventory_sha256": inventory["inventory_sha256"],
                "marker_valid": True,
                "object_count": inventory["object_count"],
                "objects_sha256": inventory["objects_sha256"],
                "source_uri": source_uri,
                "total_bytes": inventory["total_bytes"],
            }
        )
    checked_at = now or datetime.now(timezone.utc)
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise ValueError("live backup validation clock must be timezone-aware")
    capability = verify_s3_compliance_retention(
        destination_uri,
        descriptors,
        client=s3_control_client,
        now=checked_at,
        workers=normalized_workers,
    )
    expected_retained_objects = len(
        {str(descriptor["key"]) for descriptor in descriptors}
    )
    if capability.get("checked_object_count") != expected_retained_objects:
        raise RawStoreError("live Object Lock verification set is incomplete")
    return {
        "capability": capability,
        "checked_at": checked_at.astimezone(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "expected_retained_objects": expected_retained_objects,
        "inventories": sorted(inventories, key=lambda item: str(item["source_uri"])),
        "receipt_key": expected_receipt_key,
        "receipt_sha256": expected_receipt_sha256,
        "status": "passed",
    }


def execute_restore_drill(
    *,
    backup_uri: str,
    raw_source_uri: str,
    raw_inventory_key: str,
    raw_restore_uri: str,
    ops_source_uri: str,
    ops_inventory_key: str,
    ops_restore_uri: str,
    evidence_output: Path,
    apply: bool,
    create_buckets: bool = False,
    workers: int = DEFAULT_WORKERS,
    store_opener: Optional[Callable[..., WhoScoredRawStore]] = None,
    s3_control_client=None,
) -> dict[str, object]:
    """Restore raw+ops into empty targets and seal one off-host proof."""

    if apply is not True:
        raise ValueError("restore-drill requires explicit --apply")
    normalized_workers = validate_worker_count(workers)
    normalized = {
        "backup": _safe_store_uri(backup_uri),
        "raw_source": _safe_store_uri(raw_source_uri),
        "raw_restore": _safe_store_uri(raw_restore_uri),
        "ops_source": _safe_store_uri(ops_source_uri),
        "ops_restore": _safe_store_uri(ops_restore_uri),
    }
    for label, uri in normalized.items():
        parsed = urlparse(uri)
        if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
            raise ValueError(f"restore-drill {label} must be a non-root S3 prefix")
    if normalized["raw_source"] == normalized["ops_source"]:
        raise ValueError("restore-drill raw and ops sources must be distinct")
    protected_roots = (
        normalized["raw_source"],
        normalized["ops_source"],
        normalized["backup"],
    )
    restore_roots = (normalized["raw_restore"], normalized["ops_restore"])
    if any(
        _s3_prefixes_overlap(target, protected)
        for target in restore_roots
        for protected in protected_roots
    ) or _s3_prefixes_overlap(*restore_roots):
        raise ValueError(
            "restore-drill targets must not overlap either source, backup, or each other"
        )
    for source in (normalized["raw_source"], normalized["ops_source"]):
        validate_distinct_store_roots(
            source,
            normalized["backup"],
            require_off_host=True,
        )
    for target in (normalized["raw_restore"], normalized["ops_restore"]):
        for source in (normalized["raw_source"], normalized["ops_source"]):
            validate_distinct_store_roots(
                source,
                target,
                source_role="source",
                destination_role="restore",
            )
        validate_distinct_store_roots(
            normalized["backup"],
            target,
            source_role="destination",
            destination_role="restore",
        )
    validate_distinct_store_roots(
        normalized["raw_restore"],
        normalized["ops_restore"],
        source_role="restore",
        destination_role="restore",
    )

    configured_output = os.environ.get(
        "WHOSCORED_BACKUP_RESTORE_DRILL_EVIDENCE_PATH", ""
    )
    if configured_output and evidence_output != Path(configured_output):
        raise ValueError("restore-drill output differs from the admitted path")
    opener = store_opener or open_store
    runtime = _WHOSCORED_RUNTIME_CONTRACT.validate_runtime_contract(
        report_schema_version=3
    )
    runtime_release = {
        "code_tree_sha256": runtime.get("code_tree_sha256"),
        "manifest_sha256": runtime.get("manifest_sha256"),
        "parser_version": runtime.get("parser_version"),
    }
    if runtime_release["parser_version"] != "whoscored-parser-v8" or any(
        not isinstance(runtime_release[field], str)
        or _SHA256_RE.fullmatch(runtime_release[field]) is None
        for field in ("manifest_sha256", "code_tree_sha256")
    ):
        raise ValueError("restore-drill runtime release is invalid")
    started_at = _restore_drill_utc_now()
    if started_at.tzinfo is None or started_at.utcoffset() is None:
        raise ValueError("restore-drill clock must be timezone-aware")
    started_at = started_at.astimezone(timezone.utc)
    backup = opener(normalized["backup"], role="destination")
    source_specs = (
        (
            "raw",
            normalized["raw_source"],
            raw_inventory_key,
            normalized["raw_restore"],
        ),
        (
            "ops",
            normalized["ops_source"],
            ops_inventory_key,
            normalized["ops_restore"],
        ),
    )
    source_evidence: list[dict[str, object]] = []
    for plane, source_uri, inventory_key, restore_uri in source_specs:
        inventory = fetch_backup_inventory(
            backup,
            inventory_key,
            expected_source_uri=source_uri,
        )
        if inventory["object_count"] <= 0 or inventory["total_bytes"] <= 0:
            raise RawStoreError(f"{plane} restore drill inventory must be non-empty")
        snapshot_started = datetime.fromisoformat(str(inventory["snapshot_started_at"]))
        if snapshot_started.tzinfo is None or snapshot_started.utcoffset() is None:
            raise RawStoreError(f"{plane} restore drill inventory exceeded 24h RPO")
        snapshot_age = (
            started_at - snapshot_started.astimezone(timezone.utc)
        ).total_seconds()
        if not 0 <= snapshot_age <= 24 * 3600:
            raise RawStoreError(f"{plane} restore drill inventory exceeded 24h RPO")
        backup_verification = verify_backup_store(
            backup,
            inventory,
            require_marker=True,
            workers=normalized_workers,
        )
        if not backup_verification["passed"]:
            raise RawStoreError(f"{plane} off-host backup verification failed")
        restore = opener(
            restore_uri,
            role="restore",
            allow_bucket_creation=create_buckets,
        )
        restored = restore_inventory(
            backup,
            restore,
            inventory,
            apply=True,
            workers=normalized_workers,
            require_empty=True,
        )
        verified = verify_store(
            restore,
            inventory,
            workers=normalized_workers,
        )
        restored_inventory = build_inventory(
            restore,
            source_uri=restore_uri,
            workers=normalized_workers,
        )
        exact_tree_match = (
            restored_inventory["object_count"] == inventory["object_count"]
            and restored_inventory["total_bytes"] == inventory["total_bytes"]
            and restored_inventory["objects_sha256"] == inventory["objects_sha256"]
        )
        if (
            restored["copied_objects"] != inventory["object_count"]
            or restored["already_present_objects"] != 0
            or not verified["passed"]
            or not exact_tree_match
        ):
            raise RawStoreError(f"{plane} empty-target restore drill is not exact")
        source_evidence.append(
            {
                "already_present_objects": restored["already_present_objects"],
                "checked_objects": verified["checked_objects"],
                "copied_objects": restored["copied_objects"],
                "corrupt": verified["corrupt"],
                "exact_tree_match": exact_tree_match,
                "expected_objects": verified["expected_objects"],
                "inventory_key": inventory_key,
                "inventory_sha256": inventory["inventory_sha256"],
                "marker_present": backup_verification["marker_present"],
                "marker_valid": backup_verification["marker_valid"],
                "missing": verified["missing"],
                "object_count": inventory["object_count"],
                "objects_sha256": inventory["objects_sha256"],
                "restore_passed": True,
                "restore_uri": restore_uri,
                "restored_inventory_object_count": restored_inventory["object_count"],
                "restored_inventory_objects_sha256": restored_inventory[
                    "objects_sha256"
                ],
                "restored_inventory_total_bytes": restored_inventory["total_bytes"],
                "snapshot_completed_at": inventory["snapshot_completed_at"],
                "snapshot_started_at": inventory["snapshot_started_at"],
                "source_uri": source_uri,
                "total_bytes": inventory["total_bytes"],
            }
        )
    completed_at = _restore_drill_utc_now()
    if completed_at.tzinfo is None or completed_at.utcoffset() is None:
        raise ValueError("restore-drill clock must be timezone-aware")
    completed_at = completed_at.astimezone(timezone.utc)
    duration = (completed_at - started_at).total_seconds()
    started_token = _canonical_restore_drill_time(started_at)
    completed_token = _canonical_restore_drill_time(completed_at)
    if not 0 < duration <= 24 * 3600 or completed_token <= started_token:
        raise RawStoreError("restore drill did not meet the positive <=24h RTO")
    proof: dict[str, object] = {
        "backup_destination_uri": normalized["backup"],
        "completed_at": completed_token,
        "rpo_hours": 24,
        "rto_hours": 24,
        "runtime_release": runtime_release,
        "schema_version": RESTORE_DRILL_SCHEMA_VERSION,
        "sources": source_evidence,
        "started_at": started_token,
        "status": "passed",
    }
    receipt_key, receipt_sha256 = restore_drill_receipt_key(proof)
    backup._write_immutable_json(receipt_key, proof)
    if backup._read_json(receipt_key) != proof:
        raise RawStoreError("off-host restore-drill receipt read-back failed")
    evidence = {
        **proof,
        "off_host_receipt": {
            "key": receipt_key,
            "sha256": receipt_sha256,
        },
    }
    revalidate_restore_drill_backup(
        backup_uri=normalized["backup"],
        evidence=evidence,
        backup_store=backup,
        s3_control_client=s3_control_client,
        now=completed_at,
        workers=normalized_workers,
    )
    write_restore_drill_evidence(evidence_output, evidence)
    return {
        "passed": True,
        "evidence": str(evidence_output),
        "off_host_receipt_key": receipt_key,
        "off_host_receipt_sha256": receipt_sha256,
        "source_uris": sorted((normalized["raw_source"], normalized["ops_source"])),
    }


def estimate_cutover_capacity(
    inventory: Mapping[str, object],
    *,
    expected_source_uri: str,
    max_inventory_age_hours: float,
    inventory_mib_per_second: float,
    backup_mib_per_second: float,
    verify_mib_per_second: float,
    fixed_cutover_overhead_seconds: float,
    max_downtime_seconds: float,
    current_object_count: Optional[int] = None,
    current_total_bytes: Optional[int] = None,
    now: Optional[datetime] = None,
) -> dict:
    """Fail-closed estimate for the quiesced cutover recovery workflow.

    The estimate covers one inventory pass, one backup pass, both the
    pre-cutover backup verification and post-cutover restore verification, plus
    the measured fixed overhead for the complete quiesce/cutover/restart path.
    That overhead includes stopping every writer, stopping the legacy plane,
    marker publication, starting all four new-plane services, health gates and
    restarting the original writers. Rates and overhead must come from a
    representative end-to-end production rehearsal.
    """

    def positive(name: str, value: object, *, allow_zero: bool = False) -> float:
        try:
            normalized = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be numeric") from exc
        lower_ok = normalized >= 0 if allow_zero else normalized > 0
        if not math.isfinite(normalized) or not lower_ok:
            comparison = "non-negative" if allow_zero else "positive"
            raise ValueError(f"{name} must be a finite {comparison} number")
        return normalized

    validated = validate_inventory(inventory)
    expected_source = _safe_store_uri(expected_source_uri)
    if validated["source_uri"] != expected_source:
        raise ValueError("Capacity inventory source_uri does not match expected source")
    maximum_age = positive("max_inventory_age_hours", max_inventory_age_hours)
    checked_at = now or datetime.now(timezone.utc)
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise ValueError("capacity check time must be timezone-aware")
    created_at = datetime.fromisoformat(validated["created_at"].replace("Z", "+00:00"))
    age_hours = (checked_at - created_at).total_seconds() / 3600
    if age_hours < 0:
        raise ValueError("Capacity inventory created_at is in the future")
    if age_hours > maximum_age:
        raise ValueError("Capacity inventory is stale")

    inventory_rate = positive("inventory_mib_per_second", inventory_mib_per_second)
    backup_rate = positive("backup_mib_per_second", backup_mib_per_second)
    verify_rate = positive("verify_mib_per_second", verify_mib_per_second)
    fixed_overhead = positive(
        "fixed_cutover_overhead_seconds",
        fixed_cutover_overhead_seconds,
        allow_zero=True,
    )
    maximum = positive("max_downtime_seconds", max_downtime_seconds)
    if validated["object_count"] <= 0 or validated["total_bytes"] <= 0:
        raise ValueError("Capacity inventory must contain non-empty warehouse data")

    def non_negative_int(name: str, value: object) -> int:
        if isinstance(value, bool):
            raise ValueError(f"{name} must be a non-negative integer")
        try:
            normalized = int(value)  # type: ignore[arg-type]
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be a non-negative integer") from exc
        if str(value).strip() != str(normalized) or normalized < 0:
            raise ValueError(f"{name} must be a non-negative integer")
        return normalized

    current_count = (
        validated["object_count"]
        if current_object_count is None
        else non_negative_int("current_object_count", current_object_count)
    )
    current_bytes = (
        validated["total_bytes"]
        if current_total_bytes is None
        else non_negative_int("current_total_bytes", current_total_bytes)
    )
    if current_count <= 0 or current_bytes <= 0:
        raise ValueError("Current warehouse metadata must be non-empty")
    scale = max(
        1.0,
        current_count / validated["object_count"],
        current_bytes / validated["total_bytes"],
    )
    rehearsal_mib = validated["total_bytes"] / (1024 * 1024)
    phases = {
        "inventory_seconds": rehearsal_mib / inventory_rate * scale,
        "backup_seconds": rehearsal_mib / backup_rate * scale,
        "verify_seconds": 2 * rehearsal_mib / verify_rate * scale,
        "fixed_cutover_overhead_seconds": fixed_overhead,
    }
    estimated = sum(phases.values())
    return {
        "passed": estimated <= maximum,
        "inventory_sha256": validated["inventory_sha256"],
        "source_uri": validated["source_uri"],
        "inventory_age_hours": round(age_hours, 3),
        "max_inventory_age_hours": maximum_age,
        "rehearsal_object_count": validated["object_count"],
        "rehearsal_total_bytes": validated["total_bytes"],
        "current_object_count": current_count,
        "current_total_bytes": current_bytes,
        "capacity_scale": round(scale, 6),
        "estimated_downtime_seconds": round(estimated, 3),
        "max_downtime_seconds": maximum,
        "headroom_seconds": round(maximum - estimated, 3),
        **{name: round(value, 3) for name, value in phases.items()},
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_workers(command: argparse.ArgumentParser) -> None:
        command.add_argument(
            "--workers",
            type=_worker_argument,
            default=os.environ.get(WORKERS_ENV, str(DEFAULT_WORKERS)),
            help=(
                "bounded I/O concurrency in 1..64 "
                f"(default: ${WORKERS_ENV} or {DEFAULT_WORKERS})"
            ),
        )

    preflight = subparsers.add_parser(
        "preflight",
        help="validate off-host policy, credentials and source/destination access",
    )
    preflight.add_argument("--source-uri", required=True)
    preflight.add_argument("--destination-uri", required=True)
    add_workers(preflight)

    list_inventories = subparsers.add_parser(
        "list-inventories",
        help="list recent off-host completion markers for disaster recovery",
    )
    list_inventories.add_argument("--store-uri", required=True)
    list_inventories.add_argument("--expected-source-uri", required=True)
    list_inventories.add_argument("--limit", type=int, default=20)

    fetch_inventory = subparsers.add_parser(
        "fetch-inventory",
        help="authenticate and save one explicitly selected off-host marker",
    )
    fetch_inventory.add_argument("--store-uri", required=True)
    fetch_inventory.add_argument("--inventory-key", required=True)
    fetch_inventory.add_argument("--expected-source-uri", required=True)
    fetch_inventory.add_argument("--output", type=Path, required=True)

    inventory = subparsers.add_parser("inventory", help="hash a stable source snapshot")
    inventory.add_argument("--store-uri", required=True)
    inventory.add_argument("--output", type=Path, required=True)
    inventory.add_argument(
        "--allow-empty",
        action="store_true",
        help="allow an explicit optional prefix (for example pre-bootstrap ops)",
    )
    add_workers(inventory)

    backup = subparsers.add_parser("backup", help="copy one inventory append-only")
    backup.add_argument("--source-uri", required=True)
    backup.add_argument("--destination-uri", required=True)
    backup.add_argument("--inventory", type=Path, required=True)
    backup.add_argument("--apply", action="store_true")
    add_workers(backup)

    restore = subparsers.add_parser(
        "restore", help="reconstruct one inventory from an off-host backup"
    )
    restore.add_argument("--backup-uri", required=True)
    restore.add_argument("--restore-uri", required=True)
    restore.add_argument("--inventory", type=Path, required=True)
    restore.add_argument("--apply", action="store_true")
    restore.add_argument(
        "--create-bucket",
        action="store_true",
        help="allow creation of a missing empty recovery bucket",
    )
    add_workers(restore)

    restore_drill = subparsers.add_parser(
        "restore-drill",
        help="restore raw+ops into empty targets and seal off-host evidence",
    )
    restore_drill.add_argument("--backup-uri", required=True)
    restore_drill.add_argument("--raw-source-uri", required=True)
    restore_drill.add_argument("--raw-inventory-key", required=True)
    restore_drill.add_argument("--raw-restore-uri", required=True)
    restore_drill.add_argument("--ops-source-uri", required=True)
    restore_drill.add_argument("--ops-inventory-key", required=True)
    restore_drill.add_argument("--ops-restore-uri", required=True)
    restore_drill.add_argument("--evidence-output", type=Path, required=True)
    restore_drill.add_argument("--apply", action="store_true")
    restore_drill.add_argument(
        "--create-buckets",
        action="store_true",
        help="allow creation of missing empty recovery buckets",
    )
    add_workers(restore_drill)

    verify = subparsers.add_parser(
        "verify-restore", help="verify every restored object against an inventory"
    )
    verify.add_argument("--store-uri", required=True)
    verify.add_argument("--inventory", type=Path, required=True)
    add_workers(verify)

    verify_backup = subparsers.add_parser(
        "verify-backup", help="verify a content-addressed off-host backup"
    )
    verify_backup.add_argument("--store-uri", required=True)
    verify_backup.add_argument("--inventory", type=Path, required=True)
    add_workers(verify_backup)

    capacity = subparsers.add_parser(
        "capacity-check",
        help="estimate and gate quiesced cutover downtime from rehearsal rates",
    )
    capacity.add_argument("--inventory", type=Path, required=True)
    capacity.add_argument("--expected-source-uri", required=True)
    capacity.add_argument("--current-store-uri", required=True)
    capacity.add_argument("--max-inventory-age-hours", type=float, required=True)
    capacity.add_argument("--inventory-mib-per-second", type=float, required=True)
    capacity.add_argument("--backup-mib-per-second", type=float, required=True)
    capacity.add_argument("--verify-mib-per-second", type=float, required=True)
    capacity.add_argument(
        "--fixed-cutover-overhead-seconds",
        type=float,
        required=True,
        help=(
            "measured non-transfer time for the complete writer quiesce, plane "
            "cutover, health gates and writer restart sequence"
        ),
    )
    capacity.add_argument("--max-downtime-seconds", type=float, required=True)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = _parser().parse_args(argv)
    _WHOSCORED_RUNTIME_CONTRACT.require_production_runtime_class(
        operation="WhoScored raw backup entrypoint"
    )
    try:
        if args.command == "preflight":
            result = validate_backup_configuration(
                args.source_uri,
                args.destination_uri,
                workers=args.workers,
            )
        elif args.command == "list-inventories":
            store = open_store(args.store_uri, role="destination")
            result = list_backup_inventories(
                store,
                expected_source_uri=args.expected_source_uri,
                limit=args.limit,
            )
        elif args.command == "fetch-inventory":
            store = open_store(args.store_uri, role="destination")
            report = fetch_backup_inventory(
                store,
                args.inventory_key,
                expected_source_uri=args.expected_source_uri,
            )
            if args.output.exists():
                if load_inventory(args.output) != report:
                    raise ValueError(
                        "Inventory output already exists with different contents"
                    )
            else:
                write_inventory(args.output, report)
            result = {
                "passed": True,
                "inventory": str(args.output),
                "inventory_key": args.inventory_key,
                "inventory_sha256": report["inventory_sha256"],
                "source_uri": report["source_uri"],
                "object_count": report["object_count"],
                "total_bytes": report["total_bytes"],
            }
        elif args.command == "inventory":
            started_at = time.perf_counter()
            store = open_store(args.store_uri, role="source")
            report = build_inventory(
                store,
                source_uri=args.store_uri,
                workers=args.workers,
                allow_empty=args.allow_empty,
            )
            write_inventory(args.output, report)
            result = {
                "passed": True,
                "inventory": str(args.output),
                "object_count": report["object_count"],
                "total_bytes": report["total_bytes"],
                "inventory_sha256": report["inventory_sha256"],
                "objects_sha256": report["objects_sha256"],
                "workers": args.workers,
                **_performance(
                    started_at=started_at,
                    object_count=report["object_count"],
                    byte_count=report["total_bytes"],
                ),
            }
        elif args.command == "backup":
            validate_distinct_store_roots(
                args.source_uri,
                args.destination_uri,
                require_off_host=True,
            )
            report = load_inventory(args.inventory)
            source = open_store(args.source_uri, role="source")
            destination = open_store(args.destination_uri, role="destination")
            backup_result = backup_inventory(
                source,
                destination,
                report,
                apply=args.apply,
                workers=args.workers,
            )
            result = {
                "passed": True,
                **backup_result,
                "workers": args.workers,
            }
            if (
                args.apply
                and urlparse(_safe_store_uri(args.destination_uri)).scheme == "s3"
            ):
                result["retention"] = verify_s3_compliance_retention(
                    args.destination_uri,
                    backup_inventory_object_descriptors(report),
                    workers=args.workers,
                )
        elif args.command == "restore":
            if args.create_bucket and not args.apply:
                raise ValueError("--create-bucket requires --apply")
            validate_distinct_store_roots(
                args.backup_uri,
                args.restore_uri,
                source_role="destination",
                destination_role="restore",
            )
            report = load_inventory(args.inventory)
            backup = open_store(args.backup_uri, role="destination")
            destination = open_store(
                args.restore_uri,
                role="restore",
                allow_bucket_creation=args.create_bucket,
            )
            result = {
                "passed": True,
                **restore_inventory(
                    backup,
                    destination,
                    report,
                    apply=args.apply,
                    workers=args.workers,
                ),
                "workers": args.workers,
            }
        elif args.command == "restore-drill":
            result = execute_restore_drill(
                backup_uri=args.backup_uri,
                raw_source_uri=args.raw_source_uri,
                raw_inventory_key=args.raw_inventory_key,
                raw_restore_uri=args.raw_restore_uri,
                ops_source_uri=args.ops_source_uri,
                ops_inventory_key=args.ops_inventory_key,
                ops_restore_uri=args.ops_restore_uri,
                evidence_output=args.evidence_output,
                apply=args.apply,
                create_buckets=args.create_buckets,
                workers=args.workers,
            )
        elif args.command == "verify-backup":
            report = load_inventory(args.inventory)
            store = open_store(args.store_uri, role="destination")
            result = {
                **verify_backup_store(
                    store,
                    report,
                    require_marker=True,
                    workers=args.workers,
                ),
                "workers": args.workers,
            }
            if urlparse(_safe_store_uri(args.store_uri)).scheme == "s3":
                result["retention"] = verify_s3_compliance_retention(
                    args.store_uri,
                    backup_inventory_object_descriptors(report),
                    workers=args.workers,
                )
        elif args.command == "verify-restore":
            report = load_inventory(args.inventory)
            store = open_store(args.store_uri, role="restore")
            result = {
                **verify_store(store, report, workers=args.workers),
                "workers": args.workers,
            }
        else:
            report = load_inventory(args.inventory)
            if _safe_store_uri(args.current_store_uri) != _safe_store_uri(
                args.expected_source_uri
            ):
                raise ValueError(
                    "Current capacity store URI does not match expected source"
                )
            current_store = open_store(args.current_store_uri, role="source")
            current = measure_store_metadata(current_store)
            result = estimate_cutover_capacity(
                report,
                expected_source_uri=args.expected_source_uri,
                max_inventory_age_hours=args.max_inventory_age_hours,
                inventory_mib_per_second=args.inventory_mib_per_second,
                backup_mib_per_second=args.backup_mib_per_second,
                verify_mib_per_second=args.verify_mib_per_second,
                fixed_cutover_overhead_seconds=(args.fixed_cutover_overhead_seconds),
                max_downtime_seconds=args.max_downtime_seconds,
                current_object_count=current["object_count"],
                current_total_bytes=current["total_bytes"],
            )
            result["current_metadata_snapshot"] = current
    except (OSError, RawStoreError, ValueError) as exc:
        print(json.dumps({"passed": False, "error": str(exc)}, sort_keys=True))
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("passed") else 2


if __name__ == "__main__":
    sys.exit(main())
