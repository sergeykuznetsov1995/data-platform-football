"""Twice-daily immutable off-host backup for WhoScored raw and ops data."""

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
    else _whoscored_source.rsplit("/dags/", 1)[0]
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

import hashlib
import json
import os
import re
import stat
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dags.scripts.whoscored_identity import stable_safe_token
from utils.config import DAG_TAGS, SCHEDULES
from utils.default_args import SCRAPER_ARGS


BACKUP_POOL = "whoscored_storage_pool"
BACKUP_RPO_HOURS = 24
BACKUP_RTO_HOURS = 24
BACKUP_DAGRUN_LIMIT_HOURS = 10
BACKUP_SCHEDULED_START_LAG_MINUTES = 15
RESTORE_DRILL_MAX_BYTES = 256 * 1024
RESTORE_DRILL_SCHEMA_VERSION = 2
RESTORE_DRILL_EVIDENCE_PATH = (
    "/opt/airflow/logs/whoscored_backup/restore-drill-evidence.json"
)
RUN_DIR = "/opt/airflow/logs/whoscored_backup/{{ run_id | stable_safe_token }}"
RAW_INVENTORY = f"{RUN_DIR}/raw-inventory.json"
OPS_INVENTORY = f"{RUN_DIR}/ops-inventory.json"

_ENV = {
    "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
}
_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_INVENTORY_KEY_RE = re.compile(
    r"\Abackup-inventories/[0-9]{8}T[0-9]{12}Z-"
    r"([0-9a-f]{16})-([0-9a-f]{64})\.json\Z"
)
_RESTORE_DRILL_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "started_at",
        "completed_at",
        "rpo_hours",
        "rto_hours",
        "backup_destination_uri",
        "off_host_receipt",
        "runtime_release",
        "sources",
    }
)
_OFF_HOST_RECEIPT_FIELDS = frozenset({"key", "sha256"})
_OFF_HOST_RECEIPT_KEY_RE = re.compile(
    r"\Arestore-drill-receipts/v2/[0-9]{8}T[0-9]{6}Z-([0-9a-f]{64})\.json\Z"
)
_RUNTIME_RELEASE_FIELDS = frozenset(
    {"parser_version", "manifest_sha256", "code_tree_sha256"}
)
_RESTORE_SOURCE_FIELDS = frozenset(
    {
        "source_uri",
        "restore_uri",
        "inventory_key",
        "inventory_sha256",
        "objects_sha256",
        "snapshot_started_at",
        "snapshot_completed_at",
        "object_count",
        "total_bytes",
        "expected_objects",
        "copied_objects",
        "already_present_objects",
        "checked_objects",
        "missing",
        "corrupt",
        "marker_present",
        "marker_valid",
        "restore_passed",
        "restored_inventory_object_count",
        "restored_inventory_total_bytes",
        "restored_inventory_objects_sha256",
        "exact_tree_match",
    }
)


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for name, item in pairs:
        if name in value:
            raise RuntimeError(f"duplicate restore-drill evidence field: {name}")
        value[name] = item
    return value


def _canonical_utc(value: object, *, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise RuntimeError(f"{label} must be canonical UTC")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise RuntimeError(f"{label} must be canonical UTC") from exc
    if parsed.microsecond or parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise RuntimeError(f"{label} must be canonical UTC")
    return parsed


def _inventory_utc(value: object, *, label: str) -> datetime:
    """Parse the exact UTC ``datetime.isoformat`` emitted by inventory v2."""

    if not isinstance(value, str):
        raise RuntimeError(f"{label} must be an inventory UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise RuntimeError(f"{label} must be an inventory UTC timestamp") from exc
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() != timedelta(0)
        or parsed.isoformat() != value
    ):
        raise RuntimeError(f"{label} must be an inventory UTC timestamp")
    return parsed.astimezone(timezone.utc)


def _canonical_s3_uri(value: object, *, label: str) -> str:
    match = (
        re.fullmatch(
            r"s3://([a-z0-9][a-z0-9.-]{1,61}[a-z0-9])/([A-Za-z0-9._/-]+)",
            value,
        )
        if isinstance(value, str)
        else None
    )
    if match is None:
        raise RuntimeError(f"{label} must be a canonical S3 prefix")
    bucket, key = match.groups()
    if (
        ".." in bucket
        or ".-" in bucket
        or "-." in bucket
        or re.fullmatch(r"[0-9]+(?:\.[0-9]+){3}", bucket) is not None
        or any(part in {"", ".", ".."} for part in key.split("/"))
    ):
        raise RuntimeError(f"{label} must be a canonical S3 prefix")
    return value


def _s3_prefixes_overlap(first: str, second: str) -> bool:
    first_bucket, first_key = first.removeprefix("s3://").split("/", 1)
    second_bucket, second_key = second.removeprefix("s3://").split("/", 1)
    if first_bucket != second_bucket:
        return False
    first_parts = tuple(first_key.split("/"))
    second_parts = tuple(second_key.split("/"))
    shared = min(len(first_parts), len(second_parts))
    return first_parts[:shared] == second_parts[:shared]


def _bounded_nonnegative_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or type(value) is not int or value < 0:
        raise RuntimeError(f"{label} must be a non-negative integer")
    return value


def _validated_runtime_release(
    value: object,
    *,
    label: str,
) -> dict[str, str]:
    if (
        not isinstance(value, Mapping)
        or frozenset(value) != _RUNTIME_RELEASE_FIELDS
        or value.get("parser_version") != "whoscored-parser-v8"
        or any(
            not isinstance(value.get(field), str)
            or _SHA256_RE.fullmatch(value[field]) is None
            for field in ("manifest_sha256", "code_tree_sha256")
        )
    ):
        raise RuntimeError(f"{label} runtime release identity is invalid")
    return {field: str(value[field]) for field in sorted(_RUNTIME_RELEASE_FIELDS)}


def _current_runtime_release() -> dict[str, str]:
    report = _WHOSCORED_RUNTIME_CONTRACT.validate_runtime_contract(
        report_schema_version=3
    )
    return _validated_runtime_release(
        {
            "parser_version": report.get("parser_version"),
            "manifest_sha256": report.get("manifest_sha256"),
            "code_tree_sha256": report.get("code_tree_sha256"),
        },
        label="current",
    )


def _read_restore_drill_evidence(evidence_path: Path) -> bytes:
    if not evidence_path.is_absolute():
        raise RuntimeError("restore-drill evidence path must be absolute")
    descriptor = -1
    try:
        descriptor = os.open(
            evidence_path,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or not 0 < before.st_size <= RESTORE_DRILL_MAX_BYTES
        ):
            raise RuntimeError("restore-drill evidence file is not private and bounded")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(descriptor, 64 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > RESTORE_DRILL_MAX_BYTES:
                raise RuntimeError(
                    "restore-drill evidence file is not private and bounded"
                )
            chunks.append(chunk)
        after = os.fstat(descriptor)
        entry = os.stat(evidence_path, follow_symlinks=False)
    except OSError as exc:
        raise RuntimeError("restore-drill evidence is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    stable_fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_uid",
        "st_gid",
        "st_nlink",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if any(
        getattr(before, field) != getattr(candidate, field)
        for candidate in (after, entry)
        for field in stable_fields
    ):
        raise RuntimeError("restore-drill evidence changed while it was read")
    return b"".join(chunks)


def validate_restore_drill_evidence(
    evidence_path: Path,
    *,
    raw_store_uri: str,
    ops_store_uri: str,
    backup_destination_uri: str,
    expected_runtime_release: Mapping[str, str],
    now: datetime | None = None,
    max_age_hours: int = BACKUP_RPO_HOURS,
    off_host_receipt_loader: Callable[[str], Mapping[str, Any]],
    backup_revalidator: Callable[[Mapping[str, Any], datetime], Mapping[str, Any]]
    | None = None,
) -> dict[str, Any]:
    """Validate one recent, empty-target raw+ops restore rehearsal."""

    payload = _read_restore_drill_evidence(evidence_path)
    try:
        document = json.loads(payload, object_pairs_hook=_unique_json_object)
    except (UnicodeDecodeError, json.JSONDecodeError, RuntimeError) as exc:
        raise RuntimeError("restore-drill evidence is not strict JSON") from exc
    canonical = json.dumps(
        document,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    if canonical != payload or not isinstance(document, Mapping):
        raise RuntimeError("restore-drill evidence is not canonical JSON")
    if frozenset(document) != _RESTORE_DRILL_FIELDS:
        raise RuntimeError("restore-drill evidence fields are not exact")
    if (
        document.get("schema_version") != RESTORE_DRILL_SCHEMA_VERSION
        or document.get("status") != "passed"
        or document.get("rpo_hours") != BACKUP_RPO_HOURS
        or document.get("rto_hours") != BACKUP_RTO_HOURS
    ):
        raise RuntimeError("restore-drill evidence does not meet the 24h contract")
    evidence_release = _validated_runtime_release(
        document.get("runtime_release"),
        label="restore-drill",
    )
    current_release = _validated_runtime_release(
        expected_runtime_release,
        label="expected",
    )
    if evidence_release != current_release:
        raise RuntimeError(
            "restore-drill evidence belongs to a different runtime release"
        )
    if (
        isinstance(max_age_hours, bool)
        or type(max_age_hours) is not int
        or max_age_hours != BACKUP_RPO_HOURS
    ):
        raise RuntimeError("restore-drill evidence maximum age must be exactly 24h")

    started = _canonical_utc(document.get("started_at"), label="started_at")
    completed = _canonical_utc(document.get("completed_at"), label="completed_at")
    reference_now = now or datetime.now(timezone.utc)
    if reference_now.tzinfo is None:
        raise RuntimeError("restore-drill validation clock must be timezone-aware")
    reference_now = reference_now.astimezone(timezone.utc)
    duration = (completed - started).total_seconds()
    age = (reference_now - completed).total_seconds()
    if not 0 < duration <= BACKUP_RTO_HOURS * 3600:
        raise RuntimeError("restore drill exceeded the 24h RTO")
    if not 0 <= age <= max_age_hours * 3600:
        raise RuntimeError("restore-drill evidence is stale or from the future")

    expected_sources = {
        _canonical_s3_uri(raw_store_uri, label="raw_store_uri"),
        _canonical_s3_uri(ops_store_uri, label="ops_store_uri"),
    }
    destination = _canonical_s3_uri(
        backup_destination_uri,
        label="backup_destination_uri",
    )
    if len(expected_sources) != 2 or any(
        _s3_prefixes_overlap(destination, source) for source in expected_sources
    ):
        raise RuntimeError("backup destination must be distinct from raw and ops")
    if document.get("backup_destination_uri") != destination:
        raise RuntimeError("restore drill used a different backup destination")
    receipt = document.get("off_host_receipt")
    if (
        not isinstance(receipt, Mapping)
        or frozenset(receipt) != _OFF_HOST_RECEIPT_FIELDS
    ):
        raise RuntimeError("restore-drill off-host receipt identity is invalid")
    receipt_key = receipt.get("key")
    receipt_sha256 = receipt.get("sha256")
    receipt_match = (
        _OFF_HOST_RECEIPT_KEY_RE.fullmatch(receipt_key)
        if isinstance(receipt_key, str)
        else None
    )
    proof = {
        name: value for name, value in document.items() if name != "off_host_receipt"
    }
    proof_bytes = json.dumps(
        proof,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    proof_sha256 = hashlib.sha256(proof_bytes).hexdigest()
    expected_receipt_timestamp = completed.strftime("%Y%m%dT%H%M%SZ")
    if (
        receipt_match is None
        or receipt_match.group(1) != proof_sha256
        or receipt_sha256 != proof_sha256
        or not receipt_key.startswith(
            f"restore-drill-receipts/v2/{expected_receipt_timestamp}-"
        )
    ):
        raise RuntimeError("restore-drill off-host receipt identity is invalid")
    try:
        persisted_proof = off_host_receipt_loader(receipt_key)
    except Exception as exc:
        raise RuntimeError("restore-drill off-host receipt is unavailable") from exc
    if not isinstance(persisted_proof, Mapping) or dict(persisted_proof) != proof:
        raise RuntimeError("restore-drill off-host receipt differs from local evidence")
    sources = document.get("sources")
    if not isinstance(sources, list) or len(sources) != 2:
        raise RuntimeError("restore drill must prove exactly raw and ops")

    actual_sources: set[str] = set()
    restore_targets: set[str] = set()
    for item in sources:
        if not isinstance(item, Mapping) or frozenset(item) != _RESTORE_SOURCE_FIELDS:
            raise RuntimeError("restore-drill source fields are not exact")
        source_uri = _canonical_s3_uri(item.get("source_uri"), label="source_uri")
        restore_uri = _canonical_s3_uri(item.get("restore_uri"), label="restore_uri")
        protected_roots = (*expected_sources, destination, *restore_targets)
        if source_uri in actual_sources or any(
            _s3_prefixes_overlap(restore_uri, root) for root in protected_roots
        ):
            raise RuntimeError("restore-drill source and target roots are not distinct")
        actual_sources.add(source_uri)
        restore_targets.add(restore_uri)
        snapshot_started = _inventory_utc(
            item.get("snapshot_started_at"),
            label="snapshot_started_at",
        )
        snapshot_completed = _inventory_utc(
            item.get("snapshot_completed_at"),
            label="snapshot_completed_at",
        )
        snapshot_age = (started - snapshot_started).total_seconds()
        if not snapshot_started <= snapshot_completed <= started:
            raise RuntimeError("restore drill inventory snapshot interval is invalid")
        if not 0 <= snapshot_age <= BACKUP_RPO_HOURS * 3600:
            raise RuntimeError("restore drill inventory exceeded the 24h RPO")
        if any(
            _SHA256_RE.fullmatch(str(item.get(name) or "")) is None
            for name in ("inventory_sha256", "objects_sha256")
        ):
            raise RuntimeError("restore-drill inventory identity is invalid")
        inventory_key = item.get("inventory_key")
        marker = (
            _INVENTORY_KEY_RE.fullmatch(inventory_key)
            if isinstance(inventory_key, str)
            else None
        )
        source_fingerprint = hashlib.sha256(source_uri.encode("utf-8")).hexdigest()[:16]
        if (
            marker is None
            or marker.group(1) != source_fingerprint
            or marker.group(2) != item.get("inventory_sha256")
        ):
            raise RuntimeError("restore-drill inventory marker identity is invalid")
        object_count = _bounded_nonnegative_integer(
            item.get("object_count"),
            label="object_count",
        )
        total_bytes = _bounded_nonnegative_integer(
            item.get("total_bytes"),
            label="total_bytes",
        )
        expected_objects = _bounded_nonnegative_integer(
            item.get("expected_objects"),
            label="expected_objects",
        )
        copied_objects = _bounded_nonnegative_integer(
            item.get("copied_objects"),
            label="copied_objects",
        )
        already_present = _bounded_nonnegative_integer(
            item.get("already_present_objects"),
            label="already_present_objects",
        )
        checked_objects = _bounded_nonnegative_integer(
            item.get("checked_objects"),
            label="checked_objects",
        )
        restored_object_count = _bounded_nonnegative_integer(
            item.get("restored_inventory_object_count"),
            label="restored_inventory_object_count",
        )
        restored_total_bytes = _bounded_nonnegative_integer(
            item.get("restored_inventory_total_bytes"),
            label="restored_inventory_total_bytes",
        )
        if (
            object_count < 1
            or expected_objects != object_count
            or copied_objects != object_count
            or already_present != 0
            or checked_objects != object_count
            or item.get("missing") != []
            or item.get("corrupt") != []
            or item.get("marker_present") is not True
            or item.get("marker_valid") is not True
            or item.get("restore_passed") is not True
            or restored_object_count != object_count
            or restored_total_bytes != total_bytes
            or item.get("restored_inventory_objects_sha256")
            != item.get("objects_sha256")
            or item.get("exact_tree_match") is not True
        ):
            raise RuntimeError(
                "restore drill is incomplete or not an empty-target restore"
            )
    if actual_sources != expected_sources:
        raise RuntimeError("restore drill does not match admitted raw and ops stores")
    result = {
        "status": "passed",
        "rpo_hours": BACKUP_RPO_HOURS,
        "rto_hours": BACKUP_RTO_HOURS,
        "duration_seconds": int(duration),
        "evidence_age_seconds": int(age),
        "runtime_release": evidence_release,
        "source_uris": sorted(actual_sources),
        "off_host_receipt_key": receipt_key,
        "off_host_receipt_sha256": proof_sha256,
    }
    if backup_revalidator is not None:
        try:
            live_backup = backup_revalidator(document, reference_now)
        except Exception as exc:
            raise RuntimeError("live off-host backup revalidation failed") from exc
        if not isinstance(live_backup, Mapping):
            raise RuntimeError("live off-host backup revalidation is invalid")
        result["live_backup"] = dict(live_backup)
    return result


def _validate_whoscored_backup_environment() -> str:
    if os.environ.get("WHOSCORED_BACKUP_RPO_HOURS") != str(BACKUP_RPO_HOURS):
        raise RuntimeError("WHOSCORED_BACKUP_RPO_HOURS must be exactly 24")
    if os.environ.get("WHOSCORED_BACKUP_RTO_HOURS") != str(BACKUP_RTO_HOURS):
        raise RuntimeError("WHOSCORED_BACKUP_RTO_HOURS must be exactly 24")
    if os.environ.get("WHOSCORED_BACKUP_RESTORE_DRILL_MAX_AGE_HOURS") != str(
        BACKUP_RPO_HOURS
    ):
        raise RuntimeError(
            "WHOSCORED_BACKUP_RESTORE_DRILL_MAX_AGE_HOURS must be exactly 24"
        )
    evidence_path = os.environ.get("WHOSCORED_BACKUP_RESTORE_DRILL_EVIDENCE_PATH", "")
    if evidence_path != RESTORE_DRILL_EVIDENCE_PATH:
        raise RuntimeError("restore-drill evidence path differs from production")
    return evidence_path


def validate_whoscored_backup_schedule_contract(**_context: Any) -> dict[str, Any]:
    """Gate backup start without allowing stale RTO proof to block a new RPO."""

    _validate_whoscored_backup_environment()
    dag_run = _context.get("dag_run")
    run_type = str(getattr(dag_run, "run_type", "") or "").casefold()
    if "scheduled" in run_type:
        interval_end = _context.get("data_interval_end")
        if not isinstance(interval_end, datetime) or interval_end.tzinfo is None:
            raise RuntimeError(
                "scheduled backup requires a timezone-aware interval end"
            )
        started = datetime.now(timezone.utc)
        lag_seconds = (started - interval_end.astimezone(timezone.utc)).total_seconds()
        if not 0 <= lag_seconds <= BACKUP_SCHEDULED_START_LAG_MINUTES * 60:
            raise RuntimeError("scheduled backup missed its 15-minute start window")
    return {
        "status": "passed",
        "rpo_hours": BACKUP_RPO_HOURS,
        "rto_hours": BACKUP_RTO_HOURS,
    }


def validate_whoscored_backup_recovery_contract(
    *, full_revalidation: bool = False, **_context: Any
) -> dict[str, Any]:
    """Validate exact restore evidence without blocking new backup markers."""

    evidence_path = _validate_whoscored_backup_environment()
    destination_uri = os.environ.get("WHOSCORED_BACKUP_DESTINATION_URI", "")
    from scripts.whoscored_raw_backup import (
        open_store,
        revalidate_restore_drill_backup,
    )

    destination = open_store(destination_uri, role="destination")
    backup_revalidator = None
    if full_revalidation:

        def backup_revalidator(
            document: Mapping[str, Any], now: datetime
        ) -> Mapping[str, Any]:
            return revalidate_restore_drill_backup(
                backup_uri=destination_uri,
                evidence=document,
                backup_store=destination,
                now=now,
                workers=int(os.environ.get("WHOSCORED_BACKUP_WORKERS", "16")),
            )

    return validate_restore_drill_evidence(
        Path(evidence_path),
        raw_store_uri=os.environ.get("WHOSCORED_RAW_STORE_URI", ""),
        ops_store_uri=os.environ.get("WHOSCORED_OPS_STORE_URI", ""),
        backup_destination_uri=destination_uri,
        expected_runtime_release=_current_runtime_release(),
        off_host_receipt_loader=destination._read_json,
        backup_revalidator=backup_revalidator,
    )


def validate_whoscored_backup_completion(
    *,
    raw_inventory_path: str,
    ops_inventory_path: str,
    validation_now: datetime | None = None,
    **context: Any,
) -> dict[str, Any]:
    """Publish a terminal recovery point only inside the fixed RPO deadline."""

    from scripts.whoscored_raw_backup import load_inventory

    now = validation_now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise RuntimeError("backup completion clock must be timezone-aware")
    now = now.astimezone(timezone.utc)
    interval_end = context.get("data_interval_end")
    if not isinstance(interval_end, datetime) or interval_end.tzinfo is None:
        raise RuntimeError("backup completion requires a timezone-aware interval end")
    completion_seconds = (now - interval_end.astimezone(timezone.utc)).total_seconds()
    if not 0 <= completion_seconds <= BACKUP_DAGRUN_LIMIT_HOURS * 3600:
        raise RuntimeError("backup completion missed the 10h recovery-point deadline")
    expected = {
        "raw": _canonical_s3_uri(
            os.environ.get("WHOSCORED_RAW_STORE_URI", ""),
            label="WHOSCORED_RAW_STORE_URI",
        ),
        "ops": _canonical_s3_uri(
            os.environ.get("WHOSCORED_OPS_STORE_URI", ""),
            label="WHOSCORED_OPS_STORE_URI",
        ),
    }
    inventories: dict[str, dict[str, Any]] = {}
    for plane, path_value in (
        ("raw", raw_inventory_path),
        ("ops", ops_inventory_path),
    ):
        inventory = load_inventory(Path(path_value))
        if inventory.get("source_uri") != expected[plane]:
            raise RuntimeError(
                f"{plane} backup inventory source differs from admission"
            )
        object_count = inventory.get("object_count")
        if (
            isinstance(object_count, bool)
            or type(object_count) is not int
            or object_count < 1
        ):
            raise RuntimeError(f"{plane} backup inventory must be non-empty")
        snapshot_started = _inventory_utc(
            inventory.get("snapshot_started_at"),
            label=f"{plane}.snapshot_started_at",
        )
        age_seconds = (now - snapshot_started).total_seconds()
        if not 0 <= age_seconds <= BACKUP_RPO_HOURS * 3600:
            raise RuntimeError(f"{plane} recovery point exceeded the 24h RPO")
        inventories[plane] = {
            "source_uri": inventory["source_uri"],
            "inventory_sha256": inventory["inventory_sha256"],
            "objects_sha256": inventory["objects_sha256"],
            "snapshot_started_at": inventory["snapshot_started_at"],
            "object_count": object_count,
            "total_bytes": inventory["total_bytes"],
            "age_seconds": int(age_seconds),
        }
    return {
        "status": "passed",
        "rpo_hours": BACKUP_RPO_HOURS,
        "completed_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "inventories": inventories,
    }


with DAG(
    dag_id="dag_backup_whoscored_storage",
    default_args={**SCRAPER_ARGS, "retries": 2, "retry_delay": timedelta(minutes=10)},
    description="Inventory, copy and read-back verify immutable WhoScored raw+ops",
    schedule=SCHEDULES["dag_backup_whoscored_storage"],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=BACKUP_DAGRUN_LIMIT_HOURS),
    is_paused_upon_creation=True,
    user_defined_filters={"stable_safe_token": stable_safe_token},
    tags=DAG_TAGS.get("whoscored", ["whoscored", "storage"]),
) as dag:
    schedule_contract = PythonOperator(
        task_id="validate_whoscored_backup_schedule_contract",
        python_callable=validate_whoscored_backup_schedule_contract,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=5),
    )
    recovery_contract = PythonOperator(
        task_id="validate_whoscored_backup_recovery_contract",
        python_callable=validate_whoscored_backup_recovery_contract,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=5),
    )
    preflight = BashOperator(
        task_id="validate_whoscored_backup_config",
        bash_command=(
            "set -euo pipefail; "
            'test -n "${WHOSCORED_BACKUP_DESTINATION_URI:-}" || '
            "{ echo 'WHOSCORED_BACKUP_DESTINATION_URI is required' >&2; exit 2; }; "
            'case "${WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE:-}" in '
            "(object-lock) ;; (*) echo 'provider-verified object-lock retention "
            "is required' >&2; exit 2;; esac; "
            "python /opt/airflow/scripts/whoscored_raw_backup.py preflight "
            '--source-uri "$WHOSCORED_RAW_STORE_URI" '
            '--destination-uri "$WHOSCORED_BACKUP_DESTINATION_URI" '
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"; '
            "python /opt/airflow/scripts/whoscored_raw_backup.py preflight "
            '--source-uri "$WHOSCORED_OPS_STORE_URI" '
            '--destination-uri "$WHOSCORED_BACKUP_DESTINATION_URI" '
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"; '
            f"mkdir -p {RUN_DIR}"
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=5),
    )
    inventory = BashOperator(
        task_id="inventory_whoscored_raw",
        bash_command=(
            "python /opt/airflow/scripts/whoscored_raw_backup.py inventory "
            '--store-uri "$WHOSCORED_RAW_STORE_URI" '
            f"--output {RAW_INVENTORY} "
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"'
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=4),
    )
    backup = BashOperator(
        task_id="backup_whoscored_raw",
        bash_command=(
            "python /opt/airflow/scripts/whoscored_raw_backup.py backup "
            '--source-uri "$WHOSCORED_RAW_STORE_URI" '
            '--destination-uri "$WHOSCORED_BACKUP_DESTINATION_URI" '
            f"--inventory {RAW_INVENTORY} --apply "
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"'
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=6),
    )
    verify = BashOperator(
        task_id="verify_whoscored_backup",
        bash_command=(
            "python /opt/airflow/scripts/whoscored_raw_backup.py verify-backup "
            '--store-uri "$WHOSCORED_BACKUP_DESTINATION_URI" '
            f"--inventory {RAW_INVENTORY} "
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"'
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=6),
    )
    ops_inventory = BashOperator(
        task_id="inventory_whoscored_ops",
        bash_command=(
            "python /opt/airflow/scripts/whoscored_raw_backup.py inventory "
            '--store-uri "$WHOSCORED_OPS_STORE_URI" '
            f"--output {OPS_INVENTORY} "
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"'
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=4),
    )
    ops_backup = BashOperator(
        task_id="backup_whoscored_ops",
        bash_command=(
            "python /opt/airflow/scripts/whoscored_raw_backup.py backup "
            '--source-uri "$WHOSCORED_OPS_STORE_URI" '
            '--destination-uri "$WHOSCORED_BACKUP_DESTINATION_URI" '
            f"--inventory {OPS_INVENTORY} --apply "
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"'
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=6),
    )
    ops_verify = BashOperator(
        task_id="verify_whoscored_ops_backup",
        bash_command=(
            "python /opt/airflow/scripts/whoscored_raw_backup.py verify-backup "
            '--store-uri "$WHOSCORED_BACKUP_DESTINATION_URI" '
            f"--inventory {OPS_INVENTORY} "
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"'
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=6),
    )
    cleanup = BashOperator(
        task_id="cleanup_whoscored_backup_local_inventories",
        bash_command=(
            'days="${WHOSCORED_BACKUP_LOCAL_RETENTION_DAYS:-14}"; '
            'case "$days" in (*[!0-9]*|"") echo "invalid retention" >&2; exit 2;; esac; '
            "find /opt/airflow/logs/whoscored_backup -mindepth 1 -maxdepth 1 "
            '-type d -mtime "+$days" -exec rm -rf -- {} +'
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=10),
    )
    terminal_gate = PythonOperator(
        task_id="propagate_whoscored_backup_status",
        python_callable=validate_whoscored_backup_completion,
        op_kwargs={
            "raw_inventory_path": RAW_INVENTORY,
            "ops_inventory_path": OPS_INVENTORY,
        },
        pool=BACKUP_POOL,
        do_xcom_push=False,
        # cleanup is intentionally all_done, but it must never hide a failed
        # inventory/copy/verify task by becoming the DAG's only successful
        # leaf. A failed or upstream_failed verify/cleanup propagates to this
        # terminal leaf and therefore to the DagRun.
        trigger_rule="none_failed_min_one_success",
        execution_timeout=timedelta(minutes=5),
    )

    schedule_contract >> preflight
    schedule_contract >> recovery_contract
    preflight >> inventory >> backup >> verify
    preflight >> ops_inventory >> ops_backup >> ops_verify
    verify >> cleanup
    ops_verify >> cleanup
    verify >> terminal_gate
    ops_verify >> terminal_gate
    cleanup >> terminal_gate
    recovery_contract >> terminal_gate
