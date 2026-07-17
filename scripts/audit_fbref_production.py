#!/usr/bin/env python3
"""Audit immutable raw evidence for one completed FBref control run."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from scrapers.fbref.control import ControlStore
from scrapers.fbref.raw_audit import (
    audit_raw_fetches,
    capture_and_write_raw_inventory,
    load_successful_run_attempts,
    open_disk_backed_inventory,
    raw_baseline_anchor,
    successful_attempt_snapshot,
    write_audit_artifact,
)
from scrapers.fbref.raw_store import RawPageStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-run-id")
    parser.add_argument(
        "--output-root", default="logs/fbref_acceptance"
    )
    parser.add_argument(
        "--baseline",
        help="pre-run content inventory used to detect all raw-store deltas",
    )
    parser.add_argument(
        "--capture-baseline",
        metavar="PATH",
        help="capture a read-only raw inventory to PATH and exit",
    )
    parser.add_argument("--airflow-run-id")
    parser.add_argument(
        "--artifact-id",
        help="separate artifact identity (for example the replay Airflow run id)",
    )
    parser.add_argument("--git-sha", default=os.environ.get("GIT_SHA"))
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="diagnostic only; live/source acceptance must remain non-empty",
    )
    parser.add_argument(
        "--replay-zero-delta",
        action="store_true",
        help="fail if replay created, deleted, or changed any raw object",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.control_run_id:
        raise SystemExit(
            "--control-run-id is required for capture and audit"
        )
    if args.capture_baseline and args.baseline:
        raise SystemExit(
            "--capture-baseline and --baseline are mutually exclusive"
        )
    if not args.capture_baseline and not args.baseline:
        raise SystemExit(
            "--baseline is required for production acceptance; capture it "
            "before the source run"
        )

    control = ControlStore.from_env()
    raw_store = RawPageStore.from_env(optional=False)
    if args.capture_baseline:
        path, inventory, idempotent = capture_and_write_raw_inventory(
            raw_store, Path(args.capture_baseline)
        )
        anchor = raw_baseline_anchor(
            inventory.summary, inventory.baseline_sha256
        )
        anchored = control.record_raw_baseline(args.control_run_id, anchor)
        print(
            json.dumps(
                {
                    "status": "captured",
                    "control_run_id": args.control_run_id,
                    "object_count": inventory.summary["object_count"],
                    "encoded_bytes": inventory.summary["encoded_bytes"],
                    "inventory": str(path),
                    "baseline_sha256": inventory.baseline_sha256,
                    "control_anchored": True,
                    "idempotent": bool(
                        idempotent or anchored.get("idempotent")
                    ),
                },
                sort_keys=True,
            )
        )
        return 0

    baseline = open_disk_backed_inventory(args.baseline)
    expected_anchor = control.get_raw_baseline(args.control_run_id)
    if expected_anchor is None:
        raise RuntimeError(
            "FBref raw baseline has no immutable control-plane anchor"
        )
    actual_anchor = raw_baseline_anchor(
        baseline.summary, baseline.baseline_sha256
    )
    if dict(expected_anchor) != actual_anchor:
        raise RuntimeError(
            "FBref raw baseline does not match its immutable control-plane "
            "anchor"
        )

    sealed_snapshot = control.seal_raw_fetch_attempts(args.control_run_id)
    attempts = load_successful_run_attempts(control, args.control_run_id)
    loaded_snapshot = successful_attempt_snapshot(attempts)
    comparable_seal = {
        key: sealed_snapshot[key]
        for key in (
            "schema_version",
            "successful_attempt_count",
            "successful_attempt_ids_sha256",
        )
    }
    if loaded_snapshot != comparable_seal:
        raise RuntimeError(
            "FBref successful-attempt evidence differs from its sealed "
            "control-plane snapshot"
        )
    result = audit_raw_fetches(
        raw_store,
        attempts,
        control_run_id=args.control_run_id,
        baseline_inventory=baseline,
        require_baseline=True,
        require_nonempty=not args.allow_empty,
        require_zero_delta=args.replay_zero_delta,
        metadata={
            "airflow_run_id": args.airflow_run_id,
            "git_sha": args.git_sha,
            "raw_attempt_snapshot_sha256": loaded_snapshot[
                "successful_attempt_ids_sha256"
            ],
        },
    )
    resealed_snapshot = control.seal_raw_fetch_attempts(args.control_run_id)
    if {
        key: resealed_snapshot[key] for key in comparable_seal
    } != comparable_seal:
        raise RuntimeError(
            "FBref successful-attempt evidence changed during raw audit"
        )
    path, digest_path = write_audit_artifact(
        result,
        args.output_root,
        artifact_id=args.artifact_id,
    )
    print(
        json.dumps(
            {
                "status": result["status"],
                "control_run_id": result["control_run_id"],
                "successful_attempt_count": result["successful_attempt_count"],
                "audited_attempt_count": result["audited_attempt_count"],
                "failure_count": len(result["failures"]),
                "artifact": str(path),
                "sha256_sidecar": str(digest_path),
            },
            sort_keys=True,
        )
    )
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
