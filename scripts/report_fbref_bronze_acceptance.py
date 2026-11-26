#!/usr/bin/env python3
"""Write a redacted evidence bundle for one FBref Bronze acceptance run."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from scrapers.base.trino_manager import TrinoTableManager
from scrapers.fbref.constants import UNAVAILABLE_SEASON_STAT_ROUTES
from scrapers.fbref.control import ControlStore
from scrapers.fbref.policy import (
    PAGE_DOCUMENT_VERSION,
    RESTRICTED_DATA_REVIEW_INTERVAL,
    SEASON_ROUTE_REVIEW_DATES,
)
from scrapers.fbref.typed_bronze import (
    MATCH_AVAILABILITY_TABLE,
    MATCH_DATASET_TABLES,
    SEASON_DATASET_TABLES,
    SEASON_ROUTE_DATASETS,
    SQUAD_ROSTER_TABLE,
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

# The isolated acceptance image ships scripts, but does not ship docs. Its two
# executable queries also appear in the DataGrip companions, checked in tests.
PAGE_RECEIPTS_SQL = """
WITH selected_run AS (
    SELECT run_id,
           CASE WHEN run_type <> 'replay' THEN run_id
                WHEN metadata -> 'raw_audit' ->> 'audited_control_run_id'
                     ~* '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
                THEN (metadata -> 'raw_audit' ->> 'audited_control_run_id')::uuid
           END AS evidence_run_id
    FROM fbref_control.crawl_run WHERE run_id = CAST(:control_run_id AS uuid)
), pages AS (
    SELECT target.target_id, target.logical_refresh_id, run_fetch.content_hash,
           frontier.source_ids ->> 'competition_id' AS competition_id,
           frontier.source_ids ->> 'season_id' AS season_id,
           CASE frontier.page_kind WHEN 'match' THEN 'match_meta'
                WHEN 'squad' THEN 'squad_roster' ELSE 'standings' END AS dataset,
           CASE WHEN frontier.page_kind = 'squad' THEN
                    scope.is_complete AND NOT scope.pending AND scope.gender_classified
                    AND scope.gender_class = 'male' AND NOT scope.competition_missing
                    AND NOT scope.has_unknown AND NOT scope.has_female
                    AND NOT scope.inactive_competition AND NOT scope.invalid_season
                    AND (scope.has_current_ownership OR frontier.refresh_policy = 'historical_once')
                    AND jsonb_array_length(scope.scopes) > 0
                    AND NOT EXISTS (SELECT 1 FROM fbref_control.page_scope_registry_dirty)
                ELSE competition.gender = 'male' AND competition.crawl_state = 'active'
                    AND competition.lifecycle_state IN ('present', 'missing_once') AND competition.present
                    AND season.lifecycle_state = 'present' AND season.present
                    AND (frontier.page_kind <> 'match'
                         OR target.target_id = 'fbref:match:' || (frontier.source_ids ->> 'match_id'))
           END AS scope_ok
    FROM selected_run
    JOIN fbref_control.run_target target ON target.run_id = selected_run.evidence_run_id
    JOIN fbref_control.page_frontier frontier ON frontier.target_id = target.target_id
    JOIN LATERAL (
        SELECT attempt.content_hash FROM fbref_control.fetch_attempt attempt
        WHERE attempt.run_id = target.run_id AND attempt.target_id = target.target_id
          AND attempt.logical_refresh_id = target.logical_refresh_id
          AND attempt.status = 'succeeded' AND attempt.raw_manifest_key IS NOT NULL
          AND attempt.content_hash IS NOT NULL
        ORDER BY COALESCE(attempt.finished_at, attempt.heartbeat_at) DESC,
                 attempt.attempt_number DESC LIMIT 1
    ) run_fetch ON true
    LEFT JOIN fbref_control.page_scope_summary scope ON scope.target_id = target.target_id
    LEFT JOIN fbref_control.competition_registry competition ON competition.source = frontier.source
         AND competition.competition_id = frontier.source_ids ->> 'competition_id'
    LEFT JOIN fbref_control.season_alias alias ON alias.source = competition.source
         AND alias.competition_id = competition.competition_id AND alias.alias = frontier.source_ids ->> 'season_id'
    LEFT JOIN fbref_control.season_registry season ON season.source = competition.source
         AND season.competition_id = competition.competition_id
         AND season.season_id = COALESCE(alias.season_id, frontier.source_ids ->> 'season_id')
    WHERE frontier.source = 'fbref' AND (
        frontier.page_kind IN ('match', 'season', 'standings')
        OR (frontier.page_kind = 'squad' AND split_part(split_part(frontier.canonical_url, '?', 1), '#', 1)
            !~ '/+squads/+[^/]+/+(?:[^/]+/+)*(?:matchlogs|goallogs|history|wages)(?:/|$)')
    )
), receipts AS (
    SELECT pages.dataset, pages.target_id, pages.content_hash, pages.competition_id, pages.season_id,
           manifest.row_count AS expected_rows,
           COALESCE(pages.scope_ok AND observation.status = 'succeeded'
               AND observation.generic_status = 'succeeded' AND observation.typed_status = 'succeeded'
               AND observation.validation_status = 'succeeded' AND completion.dataset IS NOT NULL
               AND manifest.parse_status = 'succeeded' AND manifest.validation_status = 'succeeded'
               AND ((manifest.availability = 'available' AND manifest.persistence_status = 'succeeded'
                    AND manifest.row_count > 0 AND (pages.dataset <> 'match_meta' OR manifest.row_count = 1))
                    OR (manifest.availability IN ('empty', 'restricted', 'not_applicable')
                        AND manifest.persistence_status IN ('succeeded', 'skipped') AND manifest.row_count = 0
                        AND (pages.dataset <> 'match_meta' OR completion.error_message IS DISTINCT FROM 'scored_match_requires_rows')
                        AND NULLIF(btrim(manifest.error_message), '') IS NOT NULL)), false) AS ready
    FROM pages
    LEFT JOIN fbref_control.observation_processing observation
      ON observation.logical_refresh_id = pages.logical_refresh_id AND observation.target_id = pages.target_id
     AND observation.content_hash = pages.content_hash AND observation.parser_version = 'fbref-page-document-v6'
     AND observation.typed_parser_version = 'fbref-typed-bronze-v8'
     AND observation.stateful_parser_version = 'fbref-discovery-parser-v6'
    LEFT JOIN fbref_control.dataset_manifest completion ON completion.target_id = pages.target_id
     AND completion.content_hash = pages.content_hash AND completion.parser_version = observation.typed_parser_version
     AND completion.dataset = 'typed:__complete__' AND completion.parse_status = 'succeeded'
     AND completion.persistence_status = 'succeeded' AND completion.validation_status = 'succeeded'
    LEFT JOIN fbref_control.dataset_manifest manifest ON manifest.target_id = pages.target_id
     AND manifest.content_hash = pages.content_hash AND manifest.parser_version = observation.typed_parser_version
     AND manifest.dataset = 'typed:' || pages.dataset
)
SELECT jsonb_build_object(
    'control_run_id', run_id, 'evidence_run_id', evidence_run_id,
    'generic_parser_version', 'fbref-page-document-v6', 'typed_parser_version', 'fbref-typed-bronze-v8',
    'receipts', COALESCE((SELECT jsonb_agg(to_jsonb(receipts) ORDER BY dataset, target_id) FROM receipts), '[]'::jsonb)
) AS control_page_receipts_json
FROM selected_run WHERE evidence_run_id IS NOT NULL
"""

PAGE_DATASET_KEYS = {
    "match_meta": "'fbref:match:' || match_id",
    "standings": "source_target_id",
    "squad_roster": "source_target_id",
}

PAGE_READBACK_SQL = """
WITH args AS (
    SELECT CAST(:control_run_id AS varchar) AS run_id,
           json_parse(CAST(:control_page_receipts_json AS varchar)) AS envelope,
           '%s' AS dataset
), expected AS (
    SELECT json_extract_scalar(item, '$.target_id') AS target_id,
           json_extract_scalar(item, '$.content_hash') AS content_hash,
           json_extract_scalar(item, '$.competition_id') AS competition_id,
           json_extract_scalar(item, '$.season_id') AS season_id,
           CAST(json_extract_scalar(item, '$.expected_rows') AS bigint) AS expected_rows,
           CAST(json_extract_scalar(item, '$.ready') AS boolean) AS ready
    FROM args CROSS JOIN UNNEST(CAST(json_extract(envelope, '$.receipts') AS array(json))) AS receipts(item)
    WHERE json_extract_scalar(item, '$.dataset') = args.dataset
), physical AS (
    %s
), counts AS (
    SELECT p.target_id, sum(p.n) AS physical_rows,
           count_if(args.dataset <> 'squad_roster' AND
               (p.competition_id IS DISTINCT FROM e.competition_id OR p.season_id IS DISTINCT FROM e.season_id))
               AS wrong_scope
    FROM physical p CROSS JOIN args LEFT JOIN expected e ON e.target_id = p.target_id
    GROUP BY p.target_id
), pages AS (
    SELECT page.target_id, min(page.content_hash) AS content_hash, count(*) AS versions
    FROM iceberg.bronze.fbref_page_manifest page JOIN args ON page.run_id = args.run_id
    WHERE page.parser_version = 'fbref-page-document-v6'
      AND page.parse_status = 'success' AND page.persist_status = 'success' AND page.validation_status = 'success'
    GROUP BY page.target_id
), checked AS (
    SELECT COALESCE(e.target_id, c.target_id) AS target_id,
           e.expected_rows, COALESCE(c.physical_rows, 0) AS physical_rows,
           COALESCE(e.ready AND page.versions = 1 AND page.content_hash = e.content_hash
               AND e.expected_rows = COALESCE(c.physical_rows, 0) AND COALESCE(c.wrong_scope, 0) = 0, false) AS ok
    FROM expected e FULL OUTER JOIN counts c ON c.target_id = e.target_id
    LEFT JOIN pages page ON page.target_id = e.target_id
)
SELECT args.dataset, CASE WHEN json_extract_scalar(envelope, '$.control_run_id') = run_id
           AND json_extract_scalar(envelope, '$.generic_parser_version') = 'fbref-page-document-v6'
           AND json_extract_scalar(envelope, '$.typed_parser_version') = 'fbref-typed-bronze-v8'
           AND cardinality(CAST(json_extract(envelope, '$.receipts') AS array(json))) IS NOT NULL
           AND failures = 0 THEN 'PASS' ELSE 'FAIL' END AS verdict,
       expected_rows, physical_rows, failures, failed_targets
FROM args CROSS JOIN (
    SELECT COALESCE(sum(expected_rows), 0) AS expected_rows, COALESCE(sum(physical_rows), 0) AS physical_rows,
           COALESCE(count_if(NOT ok), 0) AS failures, array_agg(target_id ORDER BY target_id) FILTER (WHERE NOT ok) AS failed_targets
    FROM checked
) result
"""


def _policy_exempt_datasets() -> set[str]:
    datasets: set[str] = set()
    for route in UNAVAILABLE_SEASON_STAT_ROUTES:
        for category, stat_type in SEASON_ROUTE_DATASETS.get(route, ()):
            datasets.add(f"{category}_{stat_type}")
    return datasets


def _run_date_utc(run: Mapping[str, Any]) -> date | None:
    for key in ("started_at", "created_at"):
        value = run.get(key)
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                continue
        if isinstance(value, datetime) and value.utcoffset() is not None:
            return value.astimezone(timezone.utc).date()
    return None


def _route_reviews(as_of: date | None, required_routes: set[str]) -> list[dict]:
    reviews = []
    for route, observed in sorted(SEASON_ROUTE_REVIEW_DATES.items()):
        review_after = observed + RESTRICTED_DATA_REVIEW_INTERVAL
        if as_of is None:
            status = "run_date_unknown"
        elif as_of < observed:
            status = "not_yet_observed"
        elif as_of < review_after:
            status = "within_review_period"
        else:
            status = "review_required"
        reviews.append({
            "route": route,
            "observed_on": observed.isoformat(),
            "review_after": review_after.isoformat(),
            "claimed": route in required_routes,
            "current": status == "within_review_period",
            "status": status,
        })
    return reviews


def _typed_tables() -> dict[str, tuple[str, str]]:
    exempt = _policy_exempt_datasets()
    result = {
        "schedule": ("fbref_schedule", "supported"),
        "squad_roster": (SQUAD_ROSTER_TABLE, "supported"),
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
                   parser_version IS DISTINCT FROM ?
                   OR parse_status IS DISTINCT FROM 'success'
                   OR persist_status IS DISTINCT FROM 'success'
                   OR validation_status IS DISTINCT FROM 'success'
               )
        FROM iceberg.bronze.fbref_page_manifest
        WHERE run_id = ?
        """,
        (PAGE_DOCUMENT_VERSION, run_id),
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


def _page_readback(
    control: ControlStore, manager: TrinoTableManager, run_id: str, inventory: list[dict],
) -> list[dict]:
    with control._transaction() as cursor:
        cursor.execute(PAGE_RECEIPTS_SQL.replace(":control_run_id", "%s"), (run_id,))
        row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"Missing FBref page receipts for {run_id}")
    envelope = json.dumps(row["control_page_receipts_json"])
    evidence = []
    for item in inventory:
        target_key = PAGE_DATASET_KEYS.get(item["dataset"])
        if target_key is None:
            continue
        # A zero-row typed SELECT works before the first populated page creates its table.
        source = (
            f"SELECT {target_key} AS target_id, source_competition_id AS competition_id, "
            f'source_season_id AS season_id, count(*) AS n FROM iceberg.bronze."{item["table"]}" '
            "WHERE _batch_id = (SELECT run_id FROM args) GROUP BY 1, 2, 3"
            if item["table_exists"] else
            "SELECT CAST(NULL AS varchar) AS target_id, CAST(NULL AS varchar) AS competition_id, "
            "CAST(NULL AS varchar) AS season_id, CAST(0 AS bigint) AS n WHERE false"
        )
        query = (PAGE_READBACK_SQL % (item["dataset"], source)).replace(
            ":control_run_id", "?"
        ).replace(":control_page_receipts_json", "?")
        result = manager.execute_query(query, (run_id, envelope))
        if len(result) != 1 or len(result[0]) != 6 or result[0][0] != item["dataset"]:
            raise RuntimeError(f"Unexpected page readback for {item['dataset']}")
        dataset, verdict, expected, physical, failures, targets = result[0]
        evidence.append({
            "dataset": dataset, "verdict": verdict, "expected_rows": expected,
            "physical_rows": physical, "failed_pages": failures, "failed_targets": targets or [],
        })
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
    as_of = _run_date_utc(run)
    if scope == "replay":
        generic = {
            "status": "not_applicable",
            "reason": "replay validates zero network/raw delta",
        }
        typed = []
        pages = []
        reviews = []
    else:
        generic = _generic_evidence(manager, control_run_id)
        typed = _typed_evidence(manager, control_run_id)
        pages = _page_readback(control, manager, control_run_id, typed)
        cohort = metadata.get("acceptance_cohort")
        required_routes = set(cohort.get("required_routes") or ()) if isinstance(cohort, Mapping) else set()
        reviews = _route_reviews(as_of, required_routes)
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
    if any(item["verdict"] != "PASS" for item in pages):
        gate_failures.append("typed_page_readback_failed")
    if any(review["claimed"] and not review["current"] for review in reviews):
        gate_failures.append("season_route_review_due")
    return {
        "schema_version": "fbref-bronze-acceptance-report-v1",
        "verdict": "GO" if not gate_failures else "NO-GO",
        "gate_failures": gate_failures,
        "scope": scope,
        "verdict_scope": "source_replay" if scope == "replay" else "frozen_cohort",
        "run_date_utc": as_of.isoformat() if as_of else None,
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
        "typed_page_readback": pages,
        "season_route_reviews": reviews,
    }


def render_markdown(evidence: Mapping[str, Any]) -> str:
    generic = evidence["generic_bronze"]
    run = evidence["run"]
    rows = [
        "# FBref Bronze production acceptance",
        "",
        f"- Verdict: **{evidence['verdict']}**",
        f"- Scope: `{evidence['scope']}`",
        f"- Verdict covers: `{evidence['verdict_scope']}`",
        f"- Run date (UTC): `{evidence['run_date_utc'] or 'unknown'}`",
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
    if evidence["typed_page_readback"]:
        rows.extend(["", "## Page receipts", "", "| Dataset | Control rows | Physical rows | Failed pages | Verdict |",
                     "|---|---:|---:|---:|---|"])
        for item in evidence["typed_page_readback"]:
            rows.append(f"| {item['dataset']} | {item['expected_rows']} | {item['physical_rows']} "
                        f"| {item['failed_pages']} | {item['verdict']} |")
    if evidence["season_route_reviews"]:
        rows.extend(["", "## Season route reviews", "",
                     "| Route | Observed | Review due | At run date | Required by cohort |", "|---|---|---|---|---|"])
        for item in evidence["season_route_reviews"]:
            status = item["status"].replace("_", " ")
            rows.append(f"| {item['route']} | {item['observed_on']} | {item['review_after']} | {status} "
                        f"| {'yes' if item['claimed'] else 'no'} |")
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
