#!/usr/bin/env python3
"""Seal the final, human-owned WhoScored production GO decision.

The running-admission report is evidence, not a production decision.  This
helper binds that exact report to the operational owner's acknowledged
Telegram notification and emits one canonical HMAC-signed GO artifact.
"""

from __future__ import annotations

import argparse
from datetime import datetime, time, timedelta, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse


EXIT_CONFIG = 78
SOURCE = "whoscored"
OPERATIONAL_OWNER = "sergeykuznetsov1995"
CHANNEL = "telegram"
SIGNATURE_ALGORITHM = "hmac-sha256"
GO_SCHEMA_VERSION = 1
ACK_SLA = timedelta(hours=1)
ADMISSION_MAX_AGE = ACK_SLA + timedelta(minutes=15)
FINAL_RUN_MAX_AGE = timedelta(hours=36)
MAX_PROVIDER_SITE_EVIDENCE_BYTES = 16 * 1024 * 1024
CUTOVER_START = time(6, 0, tzinfo=timezone.utc)
CUTOVER_END = time(9, 0, tzinfo=timezone.utc)
PROTECTED_SERVICES = frozenset(
    {
        "airflow-scheduler",
        "flaresolverr",
        "flaresolverr_whoscored_paid",
        "whoscored_paid_gateway",
        "whoscored_proxy_filter",
    }
)
_DIGEST = re.compile(r"[0-9a-f]{64}")
_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_RESTORE_KEY = re.compile(
    r"restore-drill-receipts/v2/([0-9]{8}T[0-9]{6}Z)-([0-9a-f]{64})\.json"
)
_DECISION_FIELDS = frozenset(
    {
        "schema_version",
        "source",
        "decision",
        "operational_owner",
        "channel",
        "message_id",
        "delivered_at",
        "acked_at",
        "acked_by",
        "decision_at",
        "admission_report_sha256",
        "rollout_id",
        "final_wave_receipt_sha256",
        "rollout_manifest_sha256",
        "charter_sha256",
        "provider_policy_sha256",
        "backup_restore_receipt_sha256",
        "off_host_site_attestation_sha256",
        "signature_algorithm",
    }
)
_SITE_ATTESTATION_FIELDS = frozenset(
    {
        "schema_version",
        "source",
        "attestation_type",
        "operational_owner",
        "production_bucket",
        "backup_bucket",
        "production_endpoint_sha256",
        "backup_endpoint_sha256",
        "production_failure_domain",
        "backup_failure_domain",
        "provider_evidence_sha256",
        "valid_from",
        "valid_until",
    }
)


class GoDecisionError(RuntimeError):
    """Raised when the final production decision cannot be proven."""


class _DuplicateKey(ValueError):
    pass


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKey(key)
        result[key] = value
    return result


def _canonical_bytes(value: object, *, newline: bool) -> bytes:
    try:
        rendered = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise GoDecisionError("GO evidence is not canonical JSON") from exc
    return (rendered + ("\n" if newline else "")).encode("utf-8")


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _open_protected_parent(path: Path, *, label: str) -> tuple[int, str]:
    absolute = Path(os.path.abspath(path))
    if path != absolute or not absolute.parts[1:]:
        raise GoDecisionError(f"{label} path must be absolute and canonical")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    directory_fd = -1
    try:
        directory_fd = os.open("/", flags)
        for component in absolute.parts[1:-1]:
            child_fd = os.open(component, flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = child_fd
            metadata = os.fstat(directory_fd)
            writable = metadata.st_mode & 0o022
            sticky_root = (
                metadata.st_uid == 0
                and metadata.st_mode & stat.S_ISVTX
                and metadata.st_mode & 0o002
            )
            if metadata.st_uid != 0 or (writable and not sticky_root):
                raise GoDecisionError(f"{label} has an unsafe parent directory")
        return directory_fd, absolute.name
    except GoDecisionError:
        if directory_fd >= 0:
            os.close(directory_fd)
        raise
    except OSError as exc:
        if directory_fd >= 0:
            os.close(directory_fd)
        raise GoDecisionError(f"cannot resolve protected {label}") from exc


def _read_protected(path: Path, *, label: str, maximum: int) -> tuple[bytes, int]:
    directory_fd = -1
    descriptor = -1
    try:
        directory_fd, name = _open_protected_parent(path, label=label)
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
            dir_fd=directory_fd,
        )
    except OSError as exc:
        if directory_fd >= 0:
            os.close(directory_fd)
        raise GoDecisionError(f"cannot open protected {label}") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != 0
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) not in {0o400, 0o600}
            or not 0 < before.st_size <= maximum
        ):
            raise GoDecisionError(f"{label} is not a protected root-owned file")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        entry = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except GoDecisionError:
        raise
    except OSError as exc:
        raise GoDecisionError(f"cannot read protected {label}") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if directory_fd >= 0:
            os.close(directory_fd)
    payload = b"".join(chunks)
    if (
        len(payload) > maximum
        or _identity(before) != _identity(after)
        or _identity(after) != _identity(entry)
    ):
        raise GoDecisionError(f"{label} changed while it was read")
    return payload, after.st_mtime_ns


def _strict_document(raw: bytes, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (_DuplicateKey, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GoDecisionError(f"{label} is not strict JSON") from exc
    if not isinstance(value, dict) or raw != _canonical_bytes(value, newline=True):
        raise GoDecisionError(f"{label} is not canonical JSON")
    return value


def _utc(value: object, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise GoDecisionError(f"{field} is not a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise GoDecisionError(f"{field} is not a UTC timestamp") from exc
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() != timedelta(0)
        or parsed.isoformat().replace("+00:00", "Z") != value
    ):
        raise GoDecisionError(f"{field} is not a canonical UTC timestamp")
    return parsed.astimezone(timezone.utc)


def _digest(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise GoDecisionError(f"{field} is not a lowercase SHA-256")
    return value


def _admission_projection(admission: Mapping[str, Any]) -> dict[str, str]:
    images = admission.get("images")
    if (
        admission.get("schema_version") != 2
        or admission.get("status") != "admitted-running-v1"
        or not isinstance(images, list)
        or len(images) != len(PROTECTED_SERVICES)
        or {item.get("service") for item in images if isinstance(item, Mapping)}
        != PROTECTED_SERVICES
    ):
        raise GoDecisionError(
            "admission report does not prove all five running services"
        )
    acceptance = admission.get("rollout_acceptance")
    if (
        not isinstance(acceptance, Mapping)
        or acceptance.get("status") != "accepted"
        or acceptance.get("authority_binding") != "current-signed-rollout"
        or acceptance.get("accepted_waves") != ["wave-20", "wave-70", "wave-all"]
        or acceptance.get("missing_waves") != []
    ):
        raise GoDecisionError("admission report is not a final accepted rollout")
    rollout_id = acceptance.get("rollout_id")
    if not isinstance(rollout_id, str) or _TOKEN.fullmatch(rollout_id) is None:
        raise GoDecisionError("accepted rollout id is invalid")
    authority = acceptance.get("rollout_authority")
    backup = acceptance.get("backup_recovery")
    provider_policy = admission.get("provider_policy")
    if (
        not isinstance(authority, Mapping)
        or authority.get("authority_binding") != "current-signed-rollout"
        or not isinstance(backup, Mapping)
        or backup.get("status") != "passed"
        or not isinstance(provider_policy, Mapping)
    ):
        raise GoDecisionError("admission authority or recovery evidence is missing")
    live_backup = backup.get("live_backup")
    capability = (
        live_backup.get("capability") if isinstance(live_backup, Mapping) else None
    )
    backup_bucket = (
        capability.get("bucket") if isinstance(capability, Mapping) else None
    )
    if (
        not isinstance(live_backup, Mapping)
        or live_backup.get("status") != "passed"
        or not isinstance(backup_bucket, str)
        or not backup_bucket
    ):
        raise GoDecisionError("admission lacks live off-host backup verification")
    source_uris = backup.get("source_uris")
    source_buckets = (
        {
            urlparse(uri).netloc
            for uri in source_uris
            if isinstance(uri, str) and urlparse(uri).scheme == "s3"
        }
        if isinstance(source_uris, list)
        else set()
    )
    if len(source_buckets) != 1 or "" in source_buckets:
        raise GoDecisionError("admission production backup source bucket is invalid")
    restore_key = backup.get("off_host_receipt_key")
    restore_sha256 = _digest(
        backup.get("off_host_receipt_sha256"),
        field="backup restore receipt",
    )
    match = _RESTORE_KEY.fullmatch(str(restore_key or ""))
    if match is None or match.group(2) != restore_sha256:
        raise GoDecisionError("backup restore receipt identity is invalid")
    latest = acceptance.get("latest_scheduled_run")
    if not isinstance(latest, Mapping) or latest.get("state") != "success":
        raise GoDecisionError("latest accepted scheduled run is missing")
    return {
        "rollout_id": rollout_id,
        "final_wave_receipt_sha256": _digest(
            acceptance.get("final_wave_receipt_sha256"),
            field="final wave receipt",
        ),
        "rollout_manifest_sha256": _digest(
            authority.get("rollout_manifest_sha256"),
            field="rollout manifest",
        ),
        "charter_sha256": _digest(authority.get("charter_sha256"), field="charter"),
        "provider_policy_sha256": _digest(
            provider_policy.get("document_sha256"), field="provider policy"
        ),
        "backup_restore_receipt_sha256": restore_sha256,
        "production_bucket": next(iter(source_buckets)),
        "backup_bucket": backup_bucket,
        "restore_timestamp": match.group(1),
        "final_completed_at": str(latest.get("completed_at") or ""),
    }


def _validate_site_attestation(
    value: Mapping[str, Any],
    *,
    production_bucket: str,
    backup_bucket: str,
    decided_at: datetime,
) -> None:
    if (
        frozenset(value) != _SITE_ATTESTATION_FIELDS
        or value.get("schema_version") != 1
        or value.get("source") != SOURCE
        or value.get("attestation_type") != "off-host-backup-site"
        or value.get("operational_owner") != OPERATIONAL_OWNER
        or value.get("production_bucket") != production_bucket
        or value.get("backup_bucket") != backup_bucket
        or production_bucket == backup_bucket
    ):
        raise GoDecisionError("off-host site attestation identity is invalid")
    for field in (
        "production_endpoint_sha256",
        "backup_endpoint_sha256",
        "provider_evidence_sha256",
    ):
        _digest(value.get(field), field=f"site attestation {field}")
    if value["production_endpoint_sha256"] == value["backup_endpoint_sha256"]:
        raise GoDecisionError("backup endpoint is not independent from production")
    production_domain = value.get("production_failure_domain")
    backup_domain = value.get("backup_failure_domain")
    if (
        not isinstance(production_domain, str)
        or _TOKEN.fullmatch(production_domain) is None
        or not isinstance(backup_domain, str)
        or _TOKEN.fullmatch(backup_domain) is None
        or production_domain == backup_domain
    ):
        raise GoDecisionError("backup site is not a distinct failure domain")
    valid_from = _utc(value.get("valid_from"), field="site attestation valid_from")
    valid_until = _utc(value.get("valid_until"), field="site attestation valid_until")
    if (
        not valid_from <= decided_at < valid_until
        or valid_until - valid_from > timedelta(days=366)
    ):
        raise GoDecisionError("off-host site attestation is not active")


def _read_secret(path: Path) -> str:
    raw, _mtime = _read_protected(path, label="owner secret", maximum=4 * 1024)
    try:
        secret = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise GoDecisionError("owner secret is not UTF-8") from exc
    if secret.endswith("\n"):
        secret = secret[:-1]
    if not 32 <= len(secret) <= 4_096 or secret != secret.strip():
        raise GoDecisionError("owner secret is invalid")
    return secret


def _write_new(path: Path, payload: bytes) -> None:
    directory_fd = -1
    descriptor = -1
    try:
        directory_fd, name = _open_protected_parent(path, label="GO artifact output")
        descriptor = os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=directory_fd,
        )
        try:
            offset = 0
            while offset < len(payload):
                written = os.write(descriptor, payload[offset:])
                if written <= 0:
                    raise OSError("short write")
                offset += written
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
            descriptor = -1
        os.fsync(directory_fd)
    except FileExistsError as exc:
        raise GoDecisionError("GO artifact output already exists") from exc
    except OSError as exc:
        raise GoDecisionError("cannot publish GO artifact") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if directory_fd >= 0:
            os.close(directory_fd)


def finalize_go_decision(
    *,
    admission_path: Path,
    decision_path: Path,
    off_host_site_attestation_path: Path,
    provider_site_evidence_path: Path,
    owner_secret_path: Path,
    output_path: Path,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Validate and seal exactly one final production GO artifact."""

    observed_now = now or datetime.now(timezone.utc)
    if observed_now.tzinfo is None or observed_now.utcoffset() is None:
        raise GoDecisionError("current time must be timezone-aware")
    current = observed_now.astimezone(timezone.utc)
    admission_raw, admission_mtime_ns = _read_protected(
        admission_path, label="admission report", maximum=2 * 1024 * 1024
    )
    admission = _strict_document(admission_raw, label="admission report")
    projection = _admission_projection(admission)
    decision_raw, _decision_mtime_ns = _read_protected(
        decision_path, label="unsigned GO decision", maximum=64 * 1024
    )
    decision = _strict_document(decision_raw, label="unsigned GO decision")
    if frozenset(decision) != _DECISION_FIELDS:
        raise GoDecisionError("unsigned GO decision fields are invalid")
    if (
        decision.get("schema_version") != GO_SCHEMA_VERSION
        or decision.get("source") != SOURCE
        or decision.get("decision") != "GO"
        or decision.get("operational_owner") != OPERATIONAL_OWNER
        or decision.get("channel") != CHANNEL
        or decision.get("acked_by") != OPERATIONAL_OWNER
        or decision.get("signature_algorithm") != SIGNATURE_ALGORITHM
        or not isinstance(decision.get("message_id"), str)
        or _TOKEN.fullmatch(decision["message_id"]) is None
    ):
        raise GoDecisionError("GO owner acknowledgement identity is invalid")
    delivered = _utc(decision.get("delivered_at"), field="delivered_at")
    acknowledged = _utc(decision.get("acked_at"), field="acked_at")
    decided = _utc(decision.get("decision_at"), field="decision_at")
    site_raw, _site_mtime_ns = _read_protected(
        off_host_site_attestation_path,
        label="off-host site attestation",
        maximum=64 * 1024,
    )
    site_attestation = _strict_document(site_raw, label="off-host site attestation")
    _validate_site_attestation(
        site_attestation,
        production_bucket=projection["production_bucket"],
        backup_bucket=projection["backup_bucket"],
        decided_at=decided,
    )
    provider_site_evidence_raw, _provider_site_evidence_mtime_ns = _read_protected(
        provider_site_evidence_path,
        label="provider site evidence",
        maximum=MAX_PROVIDER_SITE_EVIDENCE_BYTES,
    )
    if not hmac.compare_digest(
        hashlib.sha256(provider_site_evidence_raw).hexdigest(),
        str(site_attestation["provider_evidence_sha256"]),
    ):
        raise GoDecisionError(
            "provider site evidence differs from the off-host attestation"
        )
    expected = {
        **projection,
        "admission_report_sha256": hashlib.sha256(admission_raw).hexdigest(),
        "off_host_site_attestation_sha256": hashlib.sha256(site_raw).hexdigest(),
    }
    for field in (
        "admission_report_sha256",
        "rollout_id",
        "final_wave_receipt_sha256",
        "rollout_manifest_sha256",
        "charter_sha256",
        "provider_policy_sha256",
        "backup_restore_receipt_sha256",
        "off_host_site_attestation_sha256",
    ):
        if decision.get(field) != expected[field]:
            raise GoDecisionError(f"GO decision {field} differs from admission")
    completed = _utc(projection["final_completed_at"], field="final completed_at")
    try:
        restore_time = datetime.strptime(
            projection["restore_timestamp"], "%Y%m%dT%H%M%SZ"
        ).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise GoDecisionError("restore receipt timestamp is invalid") from exc
    admission_mtime = datetime.fromtimestamp(
        admission_mtime_ns / 1_000_000_000, tz=timezone.utc
    )
    if not timedelta(0) <= acknowledged - delivered <= ACK_SLA:
        raise GoDecisionError("Telegram acknowledgement exceeded the one-hour SLA")
    if admission_mtime > delivered or delivered < completed or decided < acknowledged:
        raise GoDecisionError("GO acknowledgement is out of operational order")
    if not timedelta(0) <= decided - admission_mtime <= ADMISSION_MAX_AGE:
        raise GoDecisionError("running admission is stale for the GO ceremony")
    if not timedelta(0) <= decided - completed <= FINAL_RUN_MAX_AGE:
        raise GoDecisionError("final accepted scheduled run is stale for GO")
    if not timedelta(0) <= decided - restore_time <= timedelta(hours=24):
        raise GoDecisionError("off-host restore proof is stale for GO")
    if not CUTOVER_START <= decided.timetz() < CUTOVER_END:
        raise GoDecisionError("GO decision is outside 06:00..09:00 UTC")
    if abs(current - decided) > timedelta(minutes=5):
        raise GoDecisionError("GO decision timestamp is not current")
    secret = _read_secret(owner_secret_path)
    digest = hashlib.sha256(_canonical_bytes(decision, newline=False)).hexdigest()
    signed_body = {**decision, "document_sha256": digest}
    signature = hmac.new(
        secret.encode("utf-8"),
        _canonical_bytes(signed_body, newline=False),
        hashlib.sha256,
    ).hexdigest()
    artifact = {**signed_body, "signature": signature}
    _write_new(output_path, _canonical_bytes(artifact, newline=True))
    return artifact


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admission-report", required=True, type=Path)
    parser.add_argument("--decision-input", required=True, type=Path)
    parser.add_argument("--off-host-site-attestation", required=True, type=Path)
    parser.add_argument("--provider-site-evidence", required=True, type=Path)
    parser.add_argument("--owner-secret-file", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    if (
        not sys.flags.isolated
        or not sys.flags.no_site
        or not sys.flags.ignore_environment
    ):
        print(
            "WhoScored GO decision blocked: invoke exact Python with -I -S",
            file=sys.stderr,
        )
        return EXIT_CONFIG
    args = _parser().parse_args(argv)
    try:
        artifact = finalize_go_decision(
            admission_path=args.admission_report,
            decision_path=args.decision_input,
            off_host_site_attestation_path=args.off_host_site_attestation,
            provider_site_evidence_path=args.provider_site_evidence,
            owner_secret_path=args.owner_secret_file,
            output_path=args.output,
        )
    except GoDecisionError as exc:
        print(f"WhoScored GO decision blocked: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    sys.stdout.write(
        json.dumps(
            {
                "document_sha256": artifact["document_sha256"],
                "output": str(args.output),
                "status": "production-go-sealed-v1",
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
