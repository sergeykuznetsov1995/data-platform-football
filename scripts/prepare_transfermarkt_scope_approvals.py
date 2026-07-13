#!/usr/bin/env python3
"""Plan or atomically install the per-scope approvals for one paid ingest batch.

`dag_ingest_transfermarkt` maps one child cycle per competition x edition and
demands a separate one-shot paid-proxy and production-write packet for each of
them, whose argv must match the child's own operation argv byte for byte. That
argv is built here by the child's own builder, never by hand: a drifted argv is
rejected at consume time, after the batch has already been triggered.

Emits the exact `airflow dags trigger --conf` payload for the batch.
"""

from __future__ import annotations

import argparse
import hashlib
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
DAGS_ROOT = PROJECT_ROOT / "dags"
if str(DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(DAGS_ROOT))
SCRIPTS_ROOT = Path(__file__).resolve().parent
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from prepare_transfermarkt_registry_approval import (  # noqa: E402
    APPROVAL_JOURNAL,
    APPROVAL_ROOT,
    APPROVAL_TTL_SECONDS,
    _atomic_create,
)

try:
    from dags.utils.transfermarkt_approval import ApprovalJournal, ApprovalPacket
    from dags.utils.transfermarkt_scope_planner import (
        build_promoted_registry_query,
        plan_transfermarkt_scopes,
    )
    from dags.utils.transfermarkt_native_v2 import (
        connect,
        inactive_slot,
        read_reader_state,
    )
except ModuleNotFoundError:  # Airflow has /opt/airflow/dags on PYTHONPATH.
    from utils.transfermarkt_approval import ApprovalJournal, ApprovalPacket
    from utils.transfermarkt_scope_planner import (
        build_promoted_registry_query,
        plan_transfermarkt_scopes,
    )
    from utils.transfermarkt_native_v2 import (
        connect,
        inactive_slot,
        read_reader_state,
    )

sys.path.insert(0, str(DAGS_ROOT / "scripts"))
from run_transfermarkt_scope_cycle import (  # noqa: E402
    approved_operation_argv,
    required_write_tables,
)

CHILD_SCRIPT = "/opt/airflow/dags/scripts/run_transfermarkt_scope_cycle.py"
PROVIDER_HARD_CAP_BYTES = 15 * 1024 * 1024
PROVIDER_SOFT_STOP_BYTES = 14 * 1024 * 1024
PROXY_REQUEST_LIMIT = 316
PROXY_RETRY_LIMIT = 2
MV_HISTORY_DAILY_LIMIT = 100
COACH_HISTORY_TTL_DAYS = 28
CHECKPOINT_TTL_DAYS = 35
LEASE_TTL_SECONDS = 3600
ENTITY_TIMEOUT_SECONDS = 3600
EXPECTED_DURATION_SECONDS = 4 * 60 * 60


def _read_promoted_registry(cursor, registry_snapshot_id: str) -> list[dict]:
    query = build_promoted_registry_query(
        registry_snapshot_id=registry_snapshot_id or None,
    )
    cursor.execute(query)
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _child_argv(
    payload: dict,
    *,
    reader_revision: int,
    candidate_slot: str,
    write_mode: str,
    refresh_mode: str,
    journal_path: Path,
) -> tuple[str, ...]:
    """Mirror the DAG's BashOperator command, minus the approval references."""

    return approved_operation_argv(
        (
            "--payload-json",
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
            "--reader-revision",
            str(reader_revision),
            "--candidate-slot",
            candidate_slot,
            "--write-mode",
            write_mode,
            "--approval-journal",
            str(journal_path),
            "--career-window-limit",
            str(MV_HISTORY_DAILY_LIMIT),
            "--refresh-mode",
            refresh_mode,
            "--coach-history-ttl-days",
            str(COACH_HISTORY_TTL_DAYS),
            "--checkpoint-ttl-days",
            str(CHECKPOINT_TTL_DAYS),
            "--lease-ttl-seconds",
            str(LEASE_TTL_SECONDS),
            "--entity-timeout-seconds",
            str(ENTITY_TIMEOUT_SECONDS),
            "--cycle-budget-bytes",
            str(PROVIDER_HARD_CAP_BYTES),
            "--soft-byte-stop-bytes",
            str(PROVIDER_SOFT_STOP_BYTES),
            "--request-limit",
            str(PROXY_REQUEST_LIMIT),
            "--retry-limit",
            str(PROXY_RETRY_LIMIT),
        ),
        script_path=CHILD_SCRIPT,
    )


def _packets(
    scope_id: str,
    argv: tuple[str, ...],
    *,
    cycle_tag: str,
    write_mode: str,
    approval_root: Path,
    journal_path: Path,
) -> dict:
    tables = tuple(sorted(required_write_tables(write_mode)))
    common = {
        "argv": argv,
        "concurrency": 1,
        "expected_duration_seconds": EXPECTED_DURATION_SECONDS,
        "affected_tables": tables,
        "affected_files": (str(journal_path),),
        "stop_conditions": (
            "stop on the 15 MiB parent provider cap or the soft stop",
            "stop on request/retry limits, DQ, schema or reader drift",
        ),
        "backup_commands": (
            ("python", "/opt/airflow/scripts/transfermarkt_native_v2.py", "backup-status"),
        ),
        "rollback_commands": (
            ("python", "/opt/airflow/scripts/transfermarkt_native_v2.py", "rollback", "--apply"),
        ),
    }
    paid = ApprovalPacket(
        packet_id=f"tm-scope-paid-{cycle_tag}-{scope_id}",
        action="paid_proxy",
        byte_cap_bytes=PROVIDER_HARD_CAP_BYTES,
        byte_cap_mib=PROVIDER_HARD_CAP_BYTES / 1024 / 1024,
        request_limit=PROXY_REQUEST_LIMIT,
        retry_limit=PROXY_RETRY_LIMIT,
        **common,
    )
    write = ApprovalPacket(
        packet_id=f"tm-scope-write-{cycle_tag}-{scope_id}",
        action="production_write",
        byte_cap_bytes=0,
        byte_cap_mib=0,
        request_limit=0,
        retry_limit=0,
        **common,
    )
    return {
        "paid": paid,
        "write": write,
        "paid_path": approval_root / f"scope-paid-{cycle_tag}-{scope_id}.json",
        "write_path": approval_root / f"scope-write-{cycle_tag}-{scope_id}.json",
    }


def build_plan(
    parent_cycle_id: str,
    *,
    scopes: tuple[str, ...] = (),
    max_batch: int = 8,
    registry_snapshot_id: str = "",
    approval_root: Path = APPROVAL_ROOT,
    journal_path: Path = APPROVAL_JOURNAL,
    cursor=None,
) -> dict:
    cursor = cursor or connect().cursor()
    state = read_reader_state(cursor)
    reader_revision = int(state.revision)
    candidate_slot = inactive_slot(state)
    write_mode = "dual" if state.active_version == "legacy" else "native"

    registry_rows = _read_promoted_registry(cursor, registry_snapshot_id)
    params = {
        "scopes": list(scopes),
        "leagues": [],
        "season": None,
        "registry_snapshot_id": registry_snapshot_id,
        "max_batch": max_batch,
        "refresh_mode": "auto",
        "mv_transfers_limit": MV_HISTORY_DAILY_LIMIT,
        "coach_history_ttl_days": COACH_HISTORY_TTL_DAYS,
        "checkpoint_ttl_days": CHECKPOINT_TTL_DAYS,
        "proxy_lease_ttl_seconds": LEASE_TTL_SECONDS,
        "proxy_request_limit": PROXY_REQUEST_LIMIT,
        "proxy_retry_limit": PROXY_RETRY_LIMIT,
        "entity_timeout_seconds": ENTITY_TIMEOUT_SECONDS,
    }
    plan = plan_transfermarkt_scopes(
        params,
        parent_cycle_id=parent_cycle_id,
        registry_rows=registry_rows,
        max_batch_size=max_batch,
    )
    if not plan.mapped_payloads:
        raise ValueError("promoted registry produced no due scope")

    if not scopes:
        # The payload states how the batch was selected (its selection hash, the
        # due remainder, whether a continuation is owed), and the DAG re-plans
        # from the exact scopes this conf names. Packets are bound to the child's
        # argv byte for byte, so they must be built from that same second plan.
        params["scopes"] = [
            f'{item["competition_id"]}:{item["edition_id"]}'
            for item in plan.mapped_payloads
        ]
        plan = plan_transfermarkt_scopes(
            params,
            parent_cycle_id=parent_cycle_id,
            registry_rows=registry_rows,
            max_batch_size=max_batch,
        )

    # A scope is crawled again every refresh cycle, and a one-shot packet id can
    # never be reused, so the packet belongs to the (parent cycle, scope) pair.
    cycle_tag = hashlib.sha256(parent_cycle_id.encode()).hexdigest()[:12]

    bundles: dict[str, dict[str, str]] = {}
    packets: dict[str, dict] = {}
    for payload in plan.mapped_payloads:
        scope_id = str(payload["scope_id"])
        edition = payload.get("edition_record") or {}
        refresh_mode = "current" if bool(edition.get("current")) else "historical"
        argv = _child_argv(
            payload,
            reader_revision=reader_revision,
            candidate_slot=candidate_slot,
            write_mode=write_mode,
            refresh_mode=refresh_mode,
            journal_path=journal_path,
        )
        bundle = _packets(
            scope_id,
            argv,
            cycle_tag=cycle_tag,
            write_mode=write_mode,
            approval_root=approval_root,
            journal_path=journal_path,
        )
        packets[scope_id] = bundle
        bundles[scope_id] = {
            "paid_proxy_packet_id": bundle["paid"].packet_id,
            "paid_proxy_packet_hash": bundle["paid"].packet_hash,
            "production_write_packet_id": bundle["write"].packet_id,
            "production_write_packet_hash": bundle["write"].packet_hash,
        }

    conf = {
        # The DAG's planner re-selects from these, and it speaks competition:edition.
        "scopes": [
            f'{p["competition_id"]}:{p["edition_id"]}' for p in plan.mapped_payloads
        ],
        "max_batch": max_batch,
        "registry_snapshot_id": registry_snapshot_id,
        "approval_journal": str(journal_path),
        "approval_bundles": bundles,
        "refresh_mode": "auto",
    }
    return {
        "parent_cycle_id": parent_cycle_id,
        "reader_revision": reader_revision,
        "candidate_slot": candidate_slot,
        "write_mode": write_mode,
        "scope_count": len(plan.mapped_payloads),
        "scope_ids": list(bundles),
        "journal_path": str(journal_path),
        "trigger_argv": [
            "airflow",
            "dags",
            "trigger",
            "--run-id",
            parent_cycle_id,
            "--conf",
            json.dumps(conf, sort_keys=True, separators=(",", ":")),
            "dag_ingest_transfermarkt",
        ],
        "packets": packets,
    }


def apply_plan(plan: dict, *, now: datetime | None = None) -> dict:
    """Install, issue and approve every packet in the batch once."""

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("approval clock must be timezone-aware")
    current = current.astimezone(timezone.utc)
    journal = ApprovalJournal(plan["journal_path"], clock=lambda: current)
    expiry = current + timedelta(seconds=APPROVAL_TTL_SECONDS)
    created: list[Path] = []
    try:
        for bundle in plan["packets"].values():
            for key, path_key in (("paid", "paid_path"), ("write", "write_path")):
                packet = bundle[key]
                path = Path(bundle[path_key])
                _atomic_create(path, packet.payload())
                created.append(path)
                journal.issue(packet, expires_at=expiry)
                journal.approve(packet, presented_hash=packet.packet_hash)
    except BaseException:
        for path in reversed(created):
            path.unlink(missing_ok=True)
        raise
    return {
        **{key: value for key, value in plan.items() if key != "packets"},
        "status": "approved",
        "expires_at": expiry.isoformat(),
    }


def _public(plan: dict) -> dict:
    return {key: value for key, value in plan.items() if key != "packets"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parent-cycle-id", required=True)
    parser.add_argument("--registry-snapshot-id", default="")
    parser.add_argument("--max-batch", type=int, default=8)
    parser.add_argument("--scope", action="append", default=[])
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    plan = build_plan(
        args.parent_cycle_id,
        scopes=tuple(args.scope),
        max_batch=args.max_batch,
        registry_snapshot_id=args.registry_snapshot_id,
    )
    result = apply_plan(plan) if args.apply else _public(plan)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
