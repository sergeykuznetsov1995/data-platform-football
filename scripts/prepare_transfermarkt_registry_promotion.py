#!/usr/bin/env python3
"""Plan or atomically install the one-shot registry promotion approval.

The discovery packets are issued before the crawl; this third packet can only be
built afterwards, because it binds the exact discovery manifest and the Silver
plan derived from it. Run it once discovery reports ``status=success``, then
clear only ``publish_registry``.
"""

from __future__ import annotations

import argparse
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
    OUTPUT_ROOT,
    _atomic_create,
    _cycle_id,
)

try:
    from dags.utils.transfermarkt_approval import ApprovalJournal, ApprovalPacket
    from dags.utils import transfermarkt_registry_publish as registry_publish
except ModuleNotFoundError:  # Airflow has /opt/airflow/dags on PYTHONPATH.
    from utils.transfermarkt_approval import ApprovalJournal, ApprovalPacket
    from utils import transfermarkt_registry_publish as registry_publish

import dag_discover_transfermarkt_registry as discovery_dag  # noqa: E402

EXPECTED_DURATION_SECONDS = 30 * 60
CONTROL_SCRIPT = "/opt/airflow/scripts/transfermarkt_native_v2.py"


def _load_discovery_manifest(output_root: Path, cycle_id: str) -> tuple[Path, str, dict]:
    paths = sorted((output_root / cycle_id).glob("transfermarkt-discovery-*.json"))
    if not paths:
        raise FileNotFoundError(f"no discovery manifest for cycle {cycle_id}")
    if len(paths) > 1:
        raise ValueError(f"ambiguous discovery manifests for cycle {cycle_id}")
    wrapper = json.loads(paths[0].read_text(encoding="utf-8"))
    manifest = wrapper["manifest"]
    manifest_hash = str(wrapper["manifest_hash"])
    if manifest.get("status") != "success":
        raise ValueError(
            f"discovery cycle {cycle_id} is {manifest.get('status')!r}; "
            "only a successful crawl can be promoted"
        )
    return paths[0], manifest_hash, manifest


def build_plan(
    run_id: str,
    *,
    expected_revision: int,
    approval_root: Path = APPROVAL_ROOT,
    output_root: Path = OUTPUT_ROOT,
    journal_path: Path = APPROVAL_JOURNAL,
) -> dict:
    cycle_id = _cycle_id(run_id)
    manifest_path, manifest_hash, manifest = _load_discovery_manifest(
        output_root, cycle_id
    )
    rows = manifest["rows"]
    # Renders SQL only: it cannot open a connection or execute a statement.
    planned = registry_publish.publish_registry(
        manifest,
        manifest_hash=manifest_hash,
        snapshot_id=str(manifest.get("snapshot_id") or ""),
        competition_count=int(rows["competitions"]),
        edition_count=int(rows["competition_editions"]),
        expected_revision=expected_revision,
        apply=False,
    )
    registry_manifest_hash = planned.plan.registry_manifest_hash
    publication_path = discovery_dag._publication_manifest_path(
        cycle_id=cycle_id,
        registry_manifest_hash=registry_manifest_hash,
    )
    packet_path = approval_root / f"registry-promotion-{cycle_id}.json"
    packet = ApprovalPacket(
        packet_id=f"tm-registry-promotion-{cycle_id}",
        action="production_write",
        argv=discovery_dag._promotion_argv(
            run_id=run_id,
            cycle_id=cycle_id,
            expected_revision=expected_revision,
            manifest_hash=manifest_hash,
            registry_manifest_hash=registry_manifest_hash,
        ),
        byte_cap_bytes=0,
        byte_cap_mib=0,
        request_limit=0,
        retry_limit=0,
        concurrency=1,
        expected_duration_seconds=EXPECTED_DURATION_SECONDS,
        affected_tables=tuple(
            sorted(
                {
                    registry_publish.COMPETITIONS_TABLE,
                    registry_publish.EDITIONS_TABLE,
                    registry_publish.REGISTRY_STATE_TABLE,
                    *(table for _, table in planned.plan.staging_tables),
                }
            )
        ),
        affected_files=tuple(
            sorted(
                (
                    str(journal_path),
                    str(manifest_path),
                    str(publication_path),
                )
            )
        ),
        stop_conditions=(
            "zero proxy I/O",
            "staging and target DQ must be green before the CAS",
            "unknown or conflicting active classification blocks publication",
            "CAS readback mismatch rolls the publication back",
        ),
        backup_commands=(
            ("python", CONTROL_SCRIPT, "registry-backup-status"),
        ),
        rollback_commands=(
            (
                "python",
                CONTROL_SCRIPT,
                "rollback-registry-discovery",
                "--cycle-id",
                cycle_id,
                "--apply",
            ),
        ),
    )
    return {
        "cycle_id": cycle_id,
        "run_id": run_id,
        "expected_revision": expected_revision,
        "discovery_manifest_hash": manifest_hash,
        "registry_manifest_hash": registry_manifest_hash,
        "journal_path": str(journal_path),
        "promotion_packet_path": str(packet_path),
        "promotion_packet_hash": packet.packet_hash,
        "rows": {
            "competitions": int(rows["competitions"]),
            "competition_editions": int(rows["competition_editions"]),
        },
        "clear_argv": [
            "airflow",
            "tasks",
            "clear",
            discovery_dag.DAG_ID,
            "--task-regex",
            "^publish_registry$",
            "--yes",
        ],
        "packet": packet,
    }


def apply_plan(
    plan: dict,
    *,
    presented_hash: str,
    now: datetime | None = None,
) -> dict:
    """Install, issue and approve the exact promotion packet once."""

    packet: ApprovalPacket = plan["packet"]
    if presented_hash != packet.packet_hash:
        raise ValueError("presented promotion packet hash drift")
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("approval clock must be timezone-aware")
    current = current.astimezone(timezone.utc)
    path = Path(plan["promotion_packet_path"])
    journal = ApprovalJournal(plan["journal_path"], clock=lambda: current)
    expiry = current + timedelta(seconds=APPROVAL_TTL_SECONDS)
    _atomic_create(path, packet.payload())
    try:
        journal.issue(packet, expires_at=expiry)
        journal.approve(packet, presented_hash=packet.packet_hash)
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    return {
        **{key: value for key, value in plan.items() if key != "packet"},
        "status": "approved",
        "expires_at": expiry.isoformat(),
    }


def _public(plan: dict) -> dict:
    result = {key: value for key, value in plan.items() if key != "packet"}
    result["packet_payload"] = plan["packet"].payload()
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected-revision", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--present-hash", default="")
    args = parser.parse_args(argv)
    plan = build_plan(args.run_id, expected_revision=args.expected_revision)
    if args.apply:
        result = apply_plan(plan, presented_hash=args.present_hash)
    else:
        result = _public(plan)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
