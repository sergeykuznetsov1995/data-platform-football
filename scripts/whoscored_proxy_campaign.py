#!/usr/bin/env python3
"""Build and administer signed WhoScored paid-proxy campaign approvals.

The CLI intentionally never accepts an HMAC secret on the command line.  Read
it from a protected environment variable or file so it is absent from shell
history and process listings.  Every generated document is written atomically
with mode ``0600``; existing files are never overwritten unless explicitly
requested.
"""

# ruff: noqa: E402 -- the trust anchor must run before every non-built-in import

from __future__ import annotations

import sys as _whoscored_bootstrap_sys

_whoscored_source = __file__
if not _whoscored_source.startswith("/"):
    raise RuntimeError("WhoScored entrypoint requires an absolute source path")
_whoscored_production = _whoscored_source.startswith("/opt/airflow/")
_whoscored_root = "/opt/airflow" if _whoscored_production else _whoscored_source.rsplit("/scripts/", 1)[0]
if _whoscored_production:
    if getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_schema", None) != 2:
        raise RuntimeError("image-baked WhoScored startup anchor is required")
elif getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_root", None) != _whoscored_root:
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
_WHOSCORED_RUNTIME_CONTRACT = (
    _whoscored_bootstrap_sys._load_whoscored_runtime_contract(_whoscored_root)
)

import argparse
import hashlib
import json
import os
import stat
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping, Sequence

from scrapers.whoscored.proxy_campaign import (
    MAX_PROXY_CAMPAIGN_VALIDITY,
    PROXY_APPROVAL_HMAC_SECRET_ENV,
    PROXY_CAMPAIGN_METER,
    PROXY_CAMPAIGN_SCHEMA_VERSION,
    PROXY_CAMPAIGN_SIGNATURE_ALGORITHM,
    PROXY_CAMPAIGN_SOURCE,
    PROXY_LEDGER_HMAC_SECRET_ENV,
    TRANSPORT_POLICY_DIRECT_THEN_PAID,
    WHOSCORED_CANARY_ALLOWED_PATH_FAMILIES,
    WHOSCORED_CANARY_CAP_BYTES,
    WHOSCORED_CANARY_CAPTURE_ALLOCATION_ID,
    WHOSCORED_CANARY_CAPTURE_CAP_BYTES,
    WHOSCORED_CANARY_CAPTURE_LEASE_LIMIT,
    WHOSCORED_CANARY_CAPTURE_REQUEST_LIMIT,
    WHOSCORED_CANARY_CAPTURE_WORK_ITEM_ID,
    WHOSCORED_CANARY_DAG_ID,
    WHOSCORED_CANARY_DISCOVERY_ALLOCATION_ID,
    WHOSCORED_CANARY_DISCOVERY_CAP_BYTES,
    WHOSCORED_CANARY_DISCOVERY_LEASE_LIMIT,
    WHOSCORED_CANARY_DISCOVERY_PATH_FAMILIES,
    WHOSCORED_CANARY_DISCOVERY_REQUEST_LIMIT,
    WHOSCORED_CANARY_DISCOVERY_WORK_ITEM_ID,
    WHOSCORED_CANARY_TASK_ID,
    WHOSCORED_PROXY_ALLOWED_HOSTS,
    ProxyCampaignApproval,
    ProxyCampaignLedger,
    ProxyCampaignSignatureError,
    ProxyCampaignValidationError,
    canonical_json_bytes,
    sign_proxy_campaign_approval,
    strict_json_loads,
    whoscored_canary_run_id,
)


MAX_DOCUMENT_BYTES = 4 * 1024 * 1024
DEFAULT_APPROVAL_ROOT = "/opt/airflow/config/whoscored_proxy_approvals"
DEFAULT_LEDGER_PATH = "/opt/airflow/logs/proxy_filter/whoscored_campaigns.json"
CANARY_TASK_ID = WHOSCORED_CANARY_TASK_ID
CANARY_WORK_ITEM_ID = WHOSCORED_CANARY_CAPTURE_WORK_ITEM_ID
CANARY_ALLOCATION_ID = WHOSCORED_CANARY_CAPTURE_ALLOCATION_ID
CANARY_DISCOVERY_WORK_ITEM_ID = WHOSCORED_CANARY_DISCOVERY_WORK_ITEM_ID
CANARY_DISCOVERY_ALLOCATION_ID = WHOSCORED_CANARY_DISCOVERY_ALLOCATION_ID
CANARY_DISCOVERY_CAP_BYTES = WHOSCORED_CANARY_DISCOVERY_CAP_BYTES
CANARY_CAPTURE_CAP_BYTES = WHOSCORED_CANARY_CAPTURE_CAP_BYTES
CANARY_DISCOVERY_PATH_FAMILIES = WHOSCORED_CANARY_DISCOVERY_PATH_FAMILIES
CANARY_ALLOWED_PATH_FAMILIES = WHOSCORED_CANARY_ALLOWED_PATH_FAMILIES
_VALIDATION_SECRET = b"unsigned-template-validation-key-000000000000000000"


class CampaignCliError(RuntimeError):
    """A safe operator-facing campaign command failed."""


def _utc(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise CampaignCliError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise CampaignCliError(f"{field} must use UTC")
    return parsed.astimezone(timezone.utc)


def _read_json(path: Path) -> Mapping[str, object]:
    if path.is_symlink():
        raise CampaignCliError(f"refusing symlink input: {path}")
    try:
        size = path.stat().st_size
        if size <= 0 or size > MAX_DOCUMENT_BYTES:
            raise CampaignCliError(f"document has an invalid size: {path}")
        value = strict_json_loads(path.read_bytes().decode("utf-8"))
    except CampaignCliError:
        raise
    except ProxyCampaignValidationError as exc:
        raise CampaignCliError(f"cannot read JSON document: {path}: {exc}") from exc
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise CampaignCliError(f"cannot read JSON document: {path}") from exc
    if not isinstance(value, Mapping):
        raise CampaignCliError(f"JSON document must be an object: {path}")
    return value


def _atomic_write_json(
    path: Path,
    value: Mapping[str, object],
    *,
    replace: bool = False,
) -> None:
    """Publish canonical JSON through a same-directory fsync/rename."""

    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise CampaignCliError(f"refusing symlink output: {path}")
    if path.exists() and not replace:
        raise CampaignCliError(f"output already exists: {path}")
    payload = canonical_json_bytes(dict(value)) + b"\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _require_private_file(path: Path) -> None:
    if path.is_symlink():
        raise CampaignCliError(f"refusing symlink approval: {path}")
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
    except OSError as exc:
        raise CampaignCliError(f"cannot stat approval: {path}") from exc
    if mode != 0o600:
        raise CampaignCliError(
            f"approval must have mode 0600 (found {mode:04o}): {path}"
        )


def _read_secret(*, secret_file: str | None, secret_env: str) -> str:
    if secret_file:
        path = Path(secret_file).expanduser()
        if path.is_symlink():
            raise CampaignCliError("refusing symlink HMAC secret file")
        try:
            mode = stat.S_IMODE(path.stat().st_mode)
            if mode & 0o077:
                raise CampaignCliError(
                    "HMAC secret file must not be group/world readable"
                )
            value = path.read_text(encoding="utf-8").strip()
        except CampaignCliError:
            raise
        except OSError as exc:
            raise CampaignCliError("cannot read HMAC secret file") from exc
    else:
        value = str(os.environ.get(secret_env, "")).strip()
    if len(value.encode("utf-8")) < 32:
        raise CampaignCliError("HMAC secret must contain at least 32 bytes")
    return value


def _secret(args: argparse.Namespace) -> str:
    return _read_secret(
        secret_file=args.secret_file,
        secret_env=args.secret_env,
    )


def _ledger_secret(args: argparse.Namespace) -> str:
    return _read_secret(
        secret_file=args.ledger_secret_file,
        secret_env=args.ledger_secret_env,
    )


def _safe_summary(
    approval: ProxyCampaignApproval,
    *,
    signature_status: str,
) -> dict[str, object]:
    return {
        "schema_version": approval.to_dict()["schema_version"],
        "source": PROXY_CAMPAIGN_SOURCE,
        "approval_id": approval.approval_id,
        "campaign_id": approval.campaign_id,
        "run_id": approval.run_id,
        "issued_at": approval.issued_at,
        "expires_at": approval.expires_at,
        "approval_sha256": approval.approval_sha256,
        "signature_status": signature_status,
        "transport_policy": approval.transport_policy,
        "meter": PROXY_CAMPAIGN_METER,
        "total_provider_bytes": approval.caps.total_provider_bytes,
        "daily_provider_bytes": approval.caps.daily_provider_bytes,
        "request_limit": approval.limits.requests,
        "lease_limit": approval.limits.leases,
        "concurrency": approval.limits.concurrency,
        "allowed_dag_ids": list(approval.allowed_dag_ids),
        "allocation_count": len(approval.allocations),
        "exact_measurement_canary": approval.is_exact_canary,
        "required_run_id": approval.run_id,
    }


def _print_summary(value: Mapping[str, object]) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


def _unsigned_canary(args: argparse.Namespace) -> dict[str, object]:
    issued = _utc(args.issued_at, "issued_at")
    expires = _utc(args.expires_at, "expires_at")
    if expires <= issued:
        raise CampaignCliError("expires_at must be after issued_at")
    if expires - issued > MAX_PROXY_CAMPAIGN_VALIDITY:
        raise CampaignCliError("canary validity may not exceed 24 hours")
    concurrency = int(args.concurrency)
    if not 1 <= concurrency <= 2:
        raise CampaignCliError("canary concurrency must be one or two")
    discovery_requests = WHOSCORED_CANARY_DISCOVERY_REQUEST_LIMIT
    discovery_leases = WHOSCORED_CANARY_DISCOVERY_LEASE_LIMIT
    capture_requests = WHOSCORED_CANARY_CAPTURE_REQUEST_LIMIT
    capture_leases = WHOSCORED_CANARY_CAPTURE_LEASE_LIMIT
    request_limit = discovery_requests + capture_requests
    lease_limit = discovery_leases + capture_leases
    paths = CANARY_ALLOWED_PATH_FAMILIES
    unsigned: dict[str, object] = {
        "schema_version": PROXY_CAMPAIGN_SCHEMA_VERSION,
        "source": PROXY_CAMPAIGN_SOURCE,
        "approval_id": args.approval_id,
        "campaign_id": args.campaign_id,
        "run_id": whoscored_canary_run_id(args.campaign_id),
        "issued_at": issued.isoformat(),
        "expires_at": expires.isoformat(),
        "transport_policy": TRANSPORT_POLICY_DIRECT_THEN_PAID,
        "runtime_sha256": args.runtime_sha256,
        "classifier_sha256": args.classifier_sha256,
        "caps": {
            "total_provider_bytes": WHOSCORED_CANARY_CAP_BYTES,
            "discovery_provider_bytes": CANARY_DISCOVERY_CAP_BYTES,
            "capture_provider_bytes": CANARY_CAPTURE_CAP_BYTES,
            "daily_provider_bytes": WHOSCORED_CANARY_CAP_BYTES,
        },
        "limits": {
            "requests": request_limit,
            "leases": lease_limit,
            "concurrency": concurrency,
        },
        "allowed_dag_ids": [WHOSCORED_CANARY_DAG_ID],
        "allowed_hosts": sorted(WHOSCORED_PROXY_ALLOWED_HOSTS),
        "allowed_path_families": list(paths),
        "allocations": [
            {
                "allocation_id": CANARY_DISCOVERY_ALLOCATION_ID,
                "phase": "discovery",
                "workload_class": "catalog_discovery",
                "work_item_id": CANARY_DISCOVERY_WORK_ITEM_ID,
                "task_id": CANARY_TASK_ID,
                "budget_bytes": CANARY_DISCOVERY_CAP_BYTES,
                "request_limit": discovery_requests,
                "lease_limit": discovery_leases,
                "allowed_path_families": list(
                    CANARY_DISCOVERY_PATH_FAMILIES
                ),
            },
            {
                "allocation_id": CANARY_ALLOCATION_ID,
                "phase": "capture",
                "workload_class": "representative_cohort",
                "work_item_id": CANARY_WORK_ITEM_ID,
                "task_id": CANARY_TASK_ID,
                "budget_bytes": CANARY_CAPTURE_CAP_BYTES,
                "request_limit": capture_requests,
                "lease_limit": capture_leases,
                "allowed_path_families": list(paths),
            }
        ],
        "meter": PROXY_CAMPAIGN_METER,
        "signature_algorithm": PROXY_CAMPAIGN_SIGNATURE_ALGORITHM,
    }
    # Strictly validate every field without producing operator authority.
    validated = sign_proxy_campaign_approval(unsigned, _VALIDATION_SECRET)
    approval = ProxyCampaignApproval.from_dict(validated)
    if not approval.is_exact_canary:
        raise CampaignCliError("generated template is not an exact canary")
    return approval.unsigned_dict()


def command_template(args: argparse.Namespace) -> None:
    value = _unsigned_canary(args)
    _atomic_write_json(Path(args.output), value, replace=args.force)
    digest = hashlib.sha256(canonical_json_bytes(value)).hexdigest()
    _print_summary(
        {
            "status": "unsigned_template_written",
            "output": str(Path(args.output)),
            "unsigned_sha256": digest,
            "campaign_id": value["campaign_id"],
            "approval_id": value["approval_id"],
            "required_run_id": whoscored_canary_run_id(str(value["campaign_id"])),
            "total_provider_bytes": WHOSCORED_CANARY_CAP_BYTES,
        }
    )


def command_sign(args: argparse.Namespace) -> None:
    unsigned = _read_json(Path(args.input))
    secret = _secret(args)
    signed = sign_proxy_campaign_approval(unsigned, secret)
    approval = ProxyCampaignApproval.from_dict(signed)
    approval.verify(secret, now=_utc(approval.issued_at, "approval.issued_at"))
    if args.require_exact_canary and not approval.is_exact_canary:
        raise CampaignCliError("refusing to sign a non-canary approval")
    _atomic_write_json(Path(args.output), signed, replace=args.force)
    _print_summary(_safe_summary(approval, signature_status="valid_at_issue_time"))


def _load_signed(path: Path) -> ProxyCampaignApproval:
    _require_private_file(path)
    return ProxyCampaignApproval.from_dict(_read_json(path))


def command_verify(args: argparse.Namespace) -> None:
    approval = _load_signed(Path(args.approval))
    approval.verify(_secret(args))
    if args.require_exact_canary and not approval.is_exact_canary:
        raise CampaignCliError("approval is valid but is not an exact canary")
    _print_summary(_safe_summary(approval, signature_status="valid_now"))


def command_inspect(args: argparse.Namespace) -> None:
    value = _read_json(Path(args.approval))
    approval = ProxyCampaignApproval.from_dict(value)
    summary = _safe_summary(approval, signature_status="not_verified")
    if args.ledger:
        summary["ledger_path"] = str(Path(args.ledger))
        summary["ledger_present"] = Path(args.ledger).is_file()
    _print_summary(summary)


def command_revoke(args: argparse.Namespace) -> None:
    approval = _load_signed(Path(args.approval))
    approval_secret = _secret(args)
    # Authenticate the document at its signed issue instant so an expiry can
    # never prevent an emergency kill-switch action.
    approval.verify(
        approval_secret,
        now=_utc(approval.issued_at, "approval.issued_at"),
    )
    if args.approval_id and args.approval_id != approval.approval_id:
        raise CampaignCliError("approval ID pin does not match the document")
    if args.approval_sha256 and args.approval_sha256 != approval.approval_sha256:
        raise CampaignCliError("approval SHA-256 pin does not match the document")
    ledger = ProxyCampaignLedger(
        Path(args.ledger),
        secret=_ledger_secret(args),
        approval_secret=approval_secret,
    )
    ledger.revoke(approval.campaign_id, reason=args.reason)
    receipt = {
        "schema_version": 1,
        "event_type": "whoscored_proxy_campaign_revoked",
        "campaign_id": approval.campaign_id,
        "approval_id": approval.approval_id,
        "approval_sha256": approval.approval_sha256,
        "reason": args.reason.strip(),
        "revoked_at": datetime.now(timezone.utc).isoformat(),
        "ledger_path_sha256": hashlib.sha256(
            str(Path(args.ledger)).encode("utf-8")
        ).hexdigest(),
    }
    _atomic_write_json(Path(args.output), receipt, replace=args.force)
    _print_summary(
        {
            "status": "revoked",
            "campaign_id": approval.campaign_id,
            "approval_id": approval.approval_id,
            "receipt": str(Path(args.output)),
        }
    )


def _secret_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--secret-file")
    group.add_argument(
        "--secret-env",
        default=PROXY_APPROVAL_HMAC_SECRET_ENV,
        help="environment variable containing the approval HMAC key",
    )


def _ledger_secret_arguments(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--ledger-secret-file")
    group.add_argument(
        "--ledger-secret-env",
        default=PROXY_LEDGER_HMAC_SECRET_ENV,
        help="environment variable containing the ledger HMAC key",
    )


def _output_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output", required=True)
    parser.add_argument("--force", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build, sign, inspect, verify and revoke WhoScored campaigns"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    template = subparsers.add_parser(
        "template", help="write an unsigned exact 1 GB canary template"
    )
    template.add_argument("--campaign-id", required=True)
    template.add_argument("--approval-id", required=True)
    template.add_argument("--runtime-sha256", required=True)
    template.add_argument("--classifier-sha256", required=True)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    template.add_argument("--issued-at", default=now.isoformat())
    template.add_argument(
        "--expires-at", default=(now + timedelta(hours=24)).isoformat()
    )
    template.add_argument("--concurrency", type=int, default=1)
    _output_arguments(template)
    template.set_defaults(handler=command_template)

    sign = subparsers.add_parser("sign", help="sign one unsigned template")
    sign.add_argument("--input", required=True)
    sign.add_argument("--require-exact-canary", action="store_true", default=True)
    _secret_arguments(sign)
    _output_arguments(sign)
    sign.set_defaults(handler=command_sign)

    verify = subparsers.add_parser("verify", help="verify signature and validity")
    verify.add_argument("--approval", required=True)
    verify.add_argument("--require-exact-canary", action="store_true")
    _secret_arguments(verify)
    verify.set_defaults(handler=command_verify)

    inspect = subparsers.add_parser(
        "inspect", help="show non-secret structural metadata"
    )
    inspect.add_argument("--approval", required=True)
    inspect.add_argument("--ledger")
    inspect.set_defaults(handler=command_inspect)

    revoke = subparsers.add_parser("revoke", help="activate the durable kill switch")
    revoke.add_argument("--approval", required=True)
    revoke.add_argument("--approval-id")
    revoke.add_argument("--approval-sha256")
    revoke.add_argument(
        "--ledger",
        default=os.environ.get(
            "WHOSCORED_PROXY_CAMPAIGN_LEDGER_PATH", DEFAULT_LEDGER_PATH
        ),
    )
    revoke.add_argument("--reason", required=True)
    _secret_arguments(revoke)
    _ledger_secret_arguments(revoke)
    _output_arguments(revoke)
    revoke.set_defaults(handler=command_revoke)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _WHOSCORED_RUNTIME_CONTRACT.require_production_runtime_class(
        operation="WhoScored proxy campaign administration"
    )
    try:
        args.handler(args)
    except (
        CampaignCliError,
        ProxyCampaignSignatureError,
        ProxyCampaignValidationError,
    ) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    sys.exit(main())
