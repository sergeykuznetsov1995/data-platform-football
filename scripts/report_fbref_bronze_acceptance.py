#!/usr/bin/env python3
"""Write a redacted evidence bundle for one FBref Bronze acceptance run."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from pathlib import Path
from typing import Any, Mapping

from scrapers.base.trino_manager import TrinoTableManager
from scrapers.fbref.constants import UNAVAILABLE_SEASON_STAT_ROUTES
from scrapers.fbref.control import ControlStore
from scrapers.fbref.typed_bronze import (
    MATCH_AVAILABILITY_TABLE,
    MATCH_DATASET_TABLES,
    SEASON_DATASET_TABLES,
    SEASON_ROUTE_DATASETS,
)


GENERIC_TABLES = (
    "fbref_page_manifest",
    "fbref_table_inventory",
    "fbref_table_cells",
)
_GIT_SHA = re.compile(r"[0-9a-f]{40}")
_IMAGE_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_LIVE_REQUEST_LIMIT = 100
_LIVE_BYTE_LIMIT = 50 * 1024 * 1024


def _policy_exempt_datasets() -> set[str]:
    datasets: set[str] = set()
    for route in UNAVAILABLE_SEASON_STAT_ROUTES:
        for category, stat_type in SEASON_ROUTE_DATASETS.get(route, ()):
            datasets.add(f"{category}_{stat_type}")
    return datasets


def _typed_tables() -> dict[str, tuple[str, str]]:
    exempt = _policy_exempt_datasets()
    result = {
        "schedule": ("fbref_schedule", "supported"),
        **{
            dataset: (
                table,
                "policy_exempt" if dataset in exempt else "supported",
            )
            for dataset, table in SEASON_DATASET_TABLES.items()
        },
        **{
            dataset: (table, "supported")
            for dataset, table in MATCH_DATASET_TABLES.items()
        },
        "dataset_availability": (MATCH_AVAILABILITY_TABLE, "evidence"),
    }
    return dict(sorted(result.items()))


def _single_value(rows: list[Any], *, label: str) -> int:
    if len(rows) != 1 or len(rows[0]) != 1:
        raise RuntimeError(f"Unexpected Trino result for {label}")
    return int(rows[0][0] or 0)


def _generic_evidence(manager: TrinoTableManager, run_id: str) -> dict:
    manifest = manager.execute_query(
        """
        SELECT count(*), coalesce(sum(table_count), 0),
               coalesce(sum(cell_count), 0),
               count_if(
                   parse_status <> 'success'
                   OR persist_status <> 'success'
                   OR validation_status <> 'success'
               )
        FROM iceberg.bronze.fbref_page_manifest
        WHERE run_id = ?
        """,
        (run_id,),
    )
    if len(manifest) != 1 or len(manifest[0]) != 4:
        raise RuntimeError("Unexpected Trino result for generic manifest")
    page_rows, declared_tables, declared_cells, failed_pages = (
        int(value or 0) for value in manifest[0]
    )
    inventory_rows = _single_value(
        manager.execute_query(
            """
            SELECT count(*)
            FROM iceberg.bronze.fbref_table_inventory
            WHERE run_id = ?
            """,
            (run_id,),
        ),
        label="generic inventory",
    )
    cell_rows = _single_value(
        manager.execute_query(
            """
            SELECT count(*)
            FROM iceberg.bronze.fbref_table_cells
            WHERE run_id = ?
            """,
            (run_id,),
        ),
        label="generic cells",
    )
    passed = (
        page_rows > 0
        and declared_tables == inventory_rows
        and declared_cells == cell_rows
        and failed_pages == 0
    )
    return {
        "status": "passed" if passed else "failed",
        "page_rows": page_rows,
        "declared_tables": declared_tables,
        "inventory_rows": inventory_rows,
        "declared_cells": declared_cells,
        "cell_rows": cell_rows,
        "failed_pages": failed_pages,
    }


def _typed_evidence(manager: TrinoTableManager, run_id: str) -> list[dict]:
    evidence = []
    for dataset, (table, policy) in _typed_tables().items():
        exists = manager.table_exists("bronze", table)
        rows = 0
        if exists:
            rows = _single_value(
                manager.execute_query(
                    f'SELECT count(*) FROM iceberg.bronze."{table}" '
                    "WHERE _batch_id = ?",
                    (run_id,),
                ),
                label=f"typed table {table}",
            )
        evidence.append(
            {
                "dataset": dataset,
                "table": table,
                "policy": policy,
                "table_exists": bool(exists),
                "batch_rows": rows,
            }
        )
    return evidence


def _provenance_errors(*, git_sha: str, image_digest: str) -> list[str]:
    errors = []
    if not _GIT_SHA.fullmatch(git_sha):
        errors.append("git_sha_not_full_lowercase_sha1")
    if not _IMAGE_DIGEST.fullmatch(image_digest):
        errors.append("image_digest_not_sha256_id")
    return errors


def _run_gate_errors(
    *,
    run: Mapping[str, Any],
    summary: Mapping[str, Any],
    metadata: Mapping[str, Any],
    strict: object,
    scope: str,
    control_run_id: str,
) -> list[str]:
    errors: list[str] = []
    if str(run.get("status") or "").casefold() != "succeeded":
        errors.append("run_not_succeeded")
    if not isinstance(strict, Mapping):
        return [*errors, "strict_acceptance_marker_missing"]

    replay = scope == "replay"
    expected_schema = (
        "fbref-bronze-acceptance-replay-v1"
        if replay
        else "fbref-bronze-acceptance-v1"
    )
    if strict.get("schema_version") != expected_schema:
        errors.append("strict_acceptance_schema_mismatch")
    if str(strict.get("status") or "").casefold() != "passed":
        errors.append("strict_acceptance_not_passed")
    if str(strict.get("processing_control_run_id") or "") != control_run_id:
        errors.append("strict_acceptance_run_id_mismatch")
    if not isinstance(strict.get("strict_gates"), Mapping):
        errors.append("strict_acceptance_gates_missing")

    if replay:
        if str(run.get("run_type") or "").casefold() != "replay":
            errors.append("replay_run_type_mismatch")
        for name in ("request_limit", "byte_limit", "requests_used", "bytes_used"):
            if int(run.get(name) or 0) != 0:
                errors.append(f"replay_{name}_not_zero")
        traffic = summary.get("traffic_totals")
        if not isinstance(traffic, Mapping) or int(
            traffic.get("network_attempts") or 0
        ) != 0:
            errors.append("replay_network_attempts_not_zero")
        if metadata.get("acceptance_replay") is not True:
            errors.append("replay_profile_metadata_missing")
        try:
            uuid.UUID(str(strict.get("source_control_run_id") or ""))
        except (TypeError, ValueError):
            errors.append("replay_source_control_run_id_invalid")
        return errors

    expected_run_type = "current" if scope == "current" else "backfill"
    if str(run.get("run_type") or "").casefold() != expected_run_type:
        errors.append("live_run_type_mismatch")
    if int(run.get("request_limit") or 0) != _LIVE_REQUEST_LIMIT:
        errors.append("live_request_limit_mismatch")
    if int(run.get("byte_limit") or 0) != _LIVE_BYTE_LIMIT:
        errors.append("live_byte_limit_mismatch")
    if int(run.get("requests_used") or 0) > _LIVE_REQUEST_LIMIT:
        errors.append("live_requests_exceeded")
    if int(run.get("bytes_used") or 0) > _LIVE_BYTE_LIMIT:
        errors.append("live_bytes_exceeded")
    if (
        metadata.get("acceptance_profile") is not True
        or metadata.get("publication_eligible") is not False
        or str(metadata.get("acceptance_scope") or "").casefold() != scope
    ):
        errors.append("live_acceptance_profile_mismatch")
    cohort = metadata.get("acceptance_cohort")
    if not isinstance(cohort, Mapping):
        errors.append("acceptance_cohort_missing")
    else:
        if str(cohort.get("scope") or "").casefold() != scope:
            errors.append("acceptance_cohort_scope_mismatch")
        if str(cohort.get("cohort_sha256") or "") != str(
            strict.get("cohort_sha256") or ""
        ):
            errors.append("acceptance_cohort_hash_mismatch")
    if str(strict.get("scope") or "").casefold() != scope:
        errors.append("strict_acceptance_scope_mismatch")
    return errors


def build_evidence(
    *,
    control: ControlStore,
    manager: TrinoTableManager,
    control_run_id: str,
    scope: str,
    git_sha: str,
    image_digest: str,
) -> dict:
    run = control.get_run(control_run_id)
    if run is None:
        raise RuntimeError(f"Unknown FBref control run {control_run_id}")
    summary = control.get_run_summary(control_run_id)
    if summary is None:
        raise RuntimeError(f"Missing FBref summary for {control_run_id}")
    metadata = run.get("metadata")
    metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
    strict_key = (
        "bronze_acceptance_replay"
        if scope == "replay"
        else "bronze_acceptance"
    )
    strict = metadata.get(strict_key)
    if scope == "replay":
        generic = {
            "status": "not_applicable",
            "reason": "replay validates zero network/raw delta",
        }
        typed = []
    else:
        generic = _generic_evidence(manager, control_run_id)
        typed = _typed_evidence(manager, control_run_id)
    gate_failures = _provenance_errors(
        git_sha=git_sha, image_digest=image_digest
    )
    gate_failures.extend(
        _run_gate_errors(
            run=run,
            summary=summary,
            metadata=metadata,
            strict=strict,
            scope=scope,
            control_run_id=control_run_id,
        )
    )
    if scope != "replay" and generic.get("status") != "passed":
        gate_failures.append("generic_bronze_parity_failed")
    return {
        "schema_version": "fbref-bronze-acceptance-report-v1",
        "verdict": "GO" if not gate_failures else "NO-GO",
        "gate_failures": gate_failures,
        "scope": scope,
        "control_run_id": control_run_id,
        "git_sha": git_sha,
        "image_digest": image_digest,
        "run": {
            "status": run.get("status"),
            "run_type": run.get("run_type"),
            "request_limit": run.get("request_limit"),
            "byte_limit": run.get("byte_limit"),
            "requests_used": run.get("requests_used"),
            "bytes_used": run.get("bytes_used"),
            "metadata": metadata,
        },
        "control_summary": summary,
        "generic_bronze": generic,
        "typed_bronze": typed,
    }


def render_markdown(evidence: Mapping[str, Any]) -> str:
    generic = evidence["generic_bronze"]
    run = evidence["run"]
    rows = [
        "# FBref Bronze production acceptance",
        "",
        f"- Verdict: **{evidence['verdict']}**",
        f"- Scope: `{evidence['scope']}`",
        f"- Control run: `{evidence['control_run_id']}`",
        f"- Git SHA: `{evidence['git_sha']}`",
        f"- Image: `{evidence['image_digest']}`",
        f"- Traffic: `{run.get('requests_used', 0)}` requests / "
        f"`{run.get('bytes_used', 0)}` bytes",
        f"- Failed gates: `{len(evidence.get('gate_failures') or ())}`",
        "",
        "## Generic Bronze",
        "",
        f"- Status: **{generic['status']}**",
    ]
    if generic["status"] == "not_applicable":
        rows.append(f"- Reason: {generic['reason']}")
    else:
        rows.extend(
            [
                f"- Pages: `{generic['page_rows']}`",
                f"- Tables: `{generic['declared_tables']}` declared / "
                f"`{generic['inventory_rows']}` saved",
                f"- Cells: `{generic['declared_cells']}` declared / "
                f"`{generic['cell_rows']}` saved",
                f"- Failed pages: `{generic['failed_pages']}`",
            ]
        )
    rows.extend(
        [
        "",
        "## Typed Bronze",
        "",
        "| Dataset | Table | Policy | Batch rows |",
        "|---|---|---:|---:|",
        ]
    )
    for item in evidence["typed_bronze"]:
        rows.append(
            f"| {item['dataset']} | {item['table']} | {item['policy']} | "
            f"{item['batch_rows']} |"
        )
    rows.append("")
    return "\n".join(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-run-id", required=True)
    parser.add_argument(
        "--scope", required=True, choices=("current", "history", "replay")
    )
    parser.add_argument("--git-sha", default=os.environ.get("GIT_SHA"))
    parser.add_argument(
        "--image-digest", default=os.environ.get("FBREF_IMAGE_DIGEST")
    )
    parser.add_argument(
        "--output-root", default="logs/fbref_acceptance"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run_id = str(uuid.UUID(args.control_run_id))
    except (TypeError, ValueError) as exc:
        raise SystemExit("--control-run-id must be a UUID") from exc
    if not args.git_sha:
        raise SystemExit("--git-sha or GIT_SHA is required")
    if not args.image_digest:
        raise SystemExit("--image-digest or FBREF_IMAGE_DIGEST is required")
    git_sha = str(args.git_sha).strip().lower()
    image_digest = str(args.image_digest).strip().lower()
    identity_errors = _provenance_errors(
        git_sha=git_sha, image_digest=image_digest
    )
    if identity_errors:
        raise SystemExit("invalid release identity: " + ", ".join(identity_errors))

    evidence = build_evidence(
        control=ControlStore.from_env(),
        manager=TrinoTableManager(),
        control_run_id=run_id,
        scope=args.scope,
        git_sha=git_sha,
        image_digest=image_digest,
    )
    root = Path(args.output_root)
    root.mkdir(parents=True, exist_ok=True)
    stem = f"{args.scope}-{run_id}"
    json_path = root / f"{stem}.json"
    markdown_path = root / f"{stem}.md"
    json_path.write_text(
        json.dumps(evidence, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_markdown(evidence), encoding="utf-8")
    print(
        json.dumps(
            {
                "verdict": evidence["verdict"],
                "json": str(json_path),
                "markdown": str(markdown_path),
            },
            sort_keys=True,
        )
    )
    return 0 if evidence["verdict"] == "GO" else 1


if __name__ == "__main__":
    sys.exit(main())
