#!/usr/bin/env python3
"""Plan or atomically install the two one-shot registry discovery approvals."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
DAGS_ROOT = PROJECT_ROOT / "dags"
if str(DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(DAGS_ROOT))

try:
    from dags.utils.transfermarkt_approval import ApprovalJournal, ApprovalPacket
except ModuleNotFoundError:  # Airflow has /opt/airflow/dags on PYTHONPATH.
    from utils.transfermarkt_approval import ApprovalJournal, ApprovalPacket


DAG_ID = "dag_discover_transfermarkt_registry"
DISCOVERY_TASK_ID = "discover_registry"
DISCOVERY_SCRIPT = "/opt/airflow/dags/scripts/run_transfermarkt_discovery.py"
STATE_ROOT = Path("/opt/airflow/logs/transfermarkt-registry")
OUTPUT_ROOT = STATE_ROOT / "manifests"
CACHE_PATH = STATE_ROOT / "cache" / "http.json"
APPROVAL_ROOT = Path("/opt/airflow/logs/transfermarkt-approvals")
APPROVAL_JOURNAL = APPROVAL_ROOT / "journal.json"
BRONZE_TABLES = (
    "iceberg.bronze.transfermarkt_competitions",
    "iceberg.bronze.transfermarkt_competition_editions",
)
PROVIDER_HARD_CAP_BYTES = 15 * 1024 * 1024
PROXY_REQUEST_LIMIT = 1024
PROXY_RETRY_LIMIT = 12
PROXY_CONCURRENCY = 1
EXPECTED_DURATION_SECONDS = 2 * 60 * 60
APPROVAL_TTL_SECONDS = 3 * 60 * 60
PROXY_CONTROL_URL = "http://proxy_filter:8899"
CACHE_TTL_SECONDS = 24 * 60 * 60
LEASE_TTL_SECONDS = 60 * 60


def _cycle_id(run_id: str) -> str:
    raw = str(run_id or "").strip()
    if not raw:
        raise ValueError("run_id is required")
    digest = hashlib.sha256(f"{DAG_ID}:{raw}".encode()).hexdigest()[:24]
    return f"tm-registry-{digest}"


def _discovery_argv(
    *,
    cycle_id: str,
    run_id: str,
    proxy_control_url: str,
    checkpoint: Path,
    cache: Path,
    output_root: Path,
    paid_packet: Path,
    bronze_packet: Path,
    journal: Path,
) -> tuple[str, ...]:
    return (
        DISCOVERY_SCRIPT,
        "--cycle-id", cycle_id,
        "--dag-id", DAG_ID,
        "--run-id", run_id,
        "--task-id", DISCOVERY_TASK_ID,
        "--proxy-control-url", proxy_control_url,
        "--checkpoint", str(checkpoint),
        "--cache", str(cache),
        "--output-root", str(output_root),
        "--request-limit", str(PROXY_REQUEST_LIMIT),
        "--retry-limit", str(PROXY_RETRY_LIMIT),
        "--cache-ttl-seconds", str(CACHE_TTL_SECONDS),
        "--lease-ttl-seconds", str(LEASE_TTL_SECONDS),
        "--paid-proxy-approval-packet", str(paid_packet),
        "--production-write-approval-packet", str(bronze_packet),
        "--approval-journal", str(journal),
    )


def _packet_paths(approval_root: Path, cycle_id: str) -> dict[str, Path]:
    return {
        "paid": approval_root / f"registry-paid-{cycle_id}.json",
        "bronze": approval_root / f"registry-bronze-{cycle_id}.json",
        "promotion": approval_root / f"registry-promotion-{cycle_id}.json",
    }


def build_plan(
    run_id: str,
    *,
    approval_root: Path = APPROVAL_ROOT,
    state_root: Path = STATE_ROOT,
    output_root: Path = OUTPUT_ROOT,
    cache_path: Path = CACHE_PATH,
    journal_path: Path = APPROVAL_JOURNAL,
) -> dict[str, Any]:
    """Build deterministic packets and the exact Airflow trigger conf."""

    cycle_id = _cycle_id(run_id)
    paths = _packet_paths(approval_root, cycle_id)
    checkpoint = state_root / "checkpoints" / f"{cycle_id}.json"
    argv = _discovery_argv(
        cycle_id=cycle_id,
        run_id=run_id,
        proxy_control_url=PROXY_CONTROL_URL,
        checkpoint=checkpoint,
        cache=cache_path,
        output_root=output_root,
        paid_packet=paths["paid"],
        bronze_packet=paths["bronze"],
        journal=journal_path,
    )
    affected_files = (
        str(checkpoint),
        str(cache_path),
        str(output_root),
        str(journal_path),
    )
    stop_conditions = (
        "stop before paid I/O on lease, metering, or direct-fallback failure",
        "stop before the next request at 14680064 provider bytes",
        "stop at 15728640 provider bytes, 1024 requests, or 12 retries",
        "stop before Bronze on empty, partial, unknown, conflicting, schema, or hash DQ",
        "stop on checkpoint drift or either Bronze write failure",
        "stop after Bronze; Silver and registry CAS require a fresh manifest-bound approval",
    )
    backup_commands = ((
        "python",
        "/opt/airflow/scripts/transfermarkt_native_v2.py",
        "registry-backup-status",
    ),)
    rollback_commands = ((
        "python",
        "/opt/airflow/scripts/transfermarkt_native_v2.py",
        "rollback-registry-discovery",
        "--cycle-id",
        cycle_id,
        "--apply",
    ),)
    common = dict(
        argv=argv,
        byte_cap_bytes=PROVIDER_HARD_CAP_BYTES,
        byte_cap_mib=15,
        request_limit=PROXY_REQUEST_LIMIT,
        retry_limit=PROXY_RETRY_LIMIT,
        concurrency=PROXY_CONCURRENCY,
        expected_duration_seconds=EXPECTED_DURATION_SECONDS,
        affected_files=affected_files,
        stop_conditions=stop_conditions,
        backup_commands=backup_commands,
        rollback_commands=rollback_commands,
    )
    paid = ApprovalPacket(
        packet_id=f"tm-registry-paid-{cycle_id}",
        action="paid_proxy",
        affected_tables=(),
        **common,
    )
    bronze = ApprovalPacket(
        packet_id=f"tm-registry-bronze-{cycle_id}",
        action="production_write",
        affected_tables=BRONZE_TABLES,
        **common,
    )
    conf = {
        "paid_proxy_packet_path": str(paths["paid"]),
        "paid_proxy_packet_hash": paid.packet_hash,
        "bronze_write_packet_path": str(paths["bronze"]),
        "bronze_write_packet_hash": bronze.packet_hash,
        "promotion_write_packet_path": str(paths["promotion"]),
        "promotion_write_packet_hash": "",
        "approval_journal": str(journal_path),
        "expected_registry_revision": 0,
    }
    return {
        "status": "planned",
        "run_id": run_id,
        "cycle_id": cycle_id,
        "paid_packet_path": str(paths["paid"]),
        "paid_packet_hash": paid.packet_hash,
        "bronze_packet_path": str(paths["bronze"]),
        "bronze_packet_hash": bronze.packet_hash,
        "promotion_packet_path": str(paths["promotion"]),
        "journal_path": str(journal_path),
        "internal_argv": list(argv),
        "trigger_argv": [
            "airflow",
            "dags",
            "trigger",
            "--run-id",
            run_id,
            "--conf",
            json.dumps(conf, sort_keys=True, separators=(",", ":")),
            DAG_ID,
        ],
        "packets": {"paid": paid, "bronze": bronze},
    }


def _atomic_create(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"refusing to overwrite approval packet: {path}")
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent,
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        os.unlink(temporary)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def apply_plan(
    plan: dict[str, Any],
    *,
    presented_paid_hash: str,
    presented_bronze_hash: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Install, issue and approve both exact packets once."""

    paid: ApprovalPacket = plan["packets"]["paid"]
    bronze: ApprovalPacket = plan["packets"]["bronze"]
    if presented_paid_hash != paid.packet_hash:
        raise ValueError("presented paid packet hash drift")
    if presented_bronze_hash != bronze.packet_hash:
        raise ValueError("presented Bronze packet hash drift")
    promotion_path = Path(plan["promotion_packet_path"])
    if promotion_path.exists():
        raise FileExistsError(
            f"future promotion packet path already exists: {promotion_path}"
        )
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("approval clock must be timezone-aware")
    current = current.astimezone(timezone.utc)
    created: list[Path] = []
    # The caller-supplied clock is part of deterministic approval generation.
    # Journal issue/approve must observe the same instant; consulting wall time
    # here makes old fixtures and reproducible operator packets expire mid-call.
    journal = ApprovalJournal(plan["journal_path"], clock=lambda: current)
    expiry = current + timedelta(seconds=APPROVAL_TTL_SECONDS)
    try:
        for packet, key in ((paid, "paid_packet_path"), (bronze, "bronze_packet_path")):
            path = Path(plan[key])
            _atomic_create(path, packet.payload())
            created.append(path)
            journal.issue(packet, expires_at=expiry)
            journal.approve(packet, presented_hash=packet.packet_hash)
    except BaseException:
        for path in reversed(created):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        raise
    return {
        **{key: value for key, value in plan.items() if key != "packets"},
        "status": "approved",
        "expires_at": expiry.astimezone(timezone.utc).isoformat(),
    }


def _public(plan: dict[str, Any]) -> dict[str, Any]:
    result = {key: value for key, value in plan.items() if key != "packets"}
    result["packet_payloads"] = {
        key: packet.payload() for key, packet in plan["packets"].items()
    }
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--present-paid-hash", default="")
    parser.add_argument("--present-bronze-hash", default="")
    args = parser.parse_args(argv)
    plan = build_plan(args.run_id)
    if args.apply:
        result = apply_plan(
            plan,
            presented_paid_hash=args.present_paid_hash,
            presented_bronze_hash=args.present_bronze_hash,
        )
    else:
        result = _public(plan)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
