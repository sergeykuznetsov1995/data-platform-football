#!/usr/bin/env python3
"""Plan and execute the ESPN Bronze ``legacy14`` -> ``compact6`` cutover.

The module deliberately separates immutable evidence construction from
mutation.  ``plan`` captures exact Iceberg source snapshots and native heads;
``apply`` materialises those snapshots, verifies parity, then replaces the
three public legacy tables with explicit-column security-definer views.  A
hash-chained local journal (optionally mirrored to Iceberg by the caller)
supports crash-safe resume.  Logical rollback only repoints public wrappers to
the frozen emergency archive; it never deletes lineage or restores old code.

No command infers a layout.  Operators must set ``ESPN_BRONZE_LAYOUT_MODE``
and the command checks that the requested transition agrees with the catalog
before any writer is invoked.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Callable, Mapping, Sequence

from scrapers.espn.layout import (
    ARCHIVE_MANIFEST_COLUMNS,
    ARCHIVE_MANIFEST_TABLE,
    ARCHIVE_MANIFEST_VERSION,
    BRONZE_SCHEMA,
    COMPACT6,
    COMPACT6_INTERNAL_REQUIRED_OBJECTS,
    COMPACT6_PUBLIC_OBJECTS,
    COMPACT_SHADOW_TABLES,
    CURRENT_VIEWS,
    EMERGENCY_LEGACY_VIEWS,
    GENERATION_TABLES,
    INTERNAL_SCHEMA,
    JOURNAL_TABLE,
    LAYOUT_STATE_TABLE,
    LEGACY14,
    LEGACY_ARCHIVE_TABLES,
    LEGACY_DISPOSITION_COLUMNS,
    LEGACY_DISPOSITION_TABLE,
    LEGACY_TABLES,
    RETAINED_LEGACY_MAIN_TABLES,
    REVIEWED_NATIVE_REPLACEMENTS,
    catalog_inventory_sql,
    require_layout_mode,
    validate_catalog_layout,
)
from scrapers.espn.parser_contracts import PARSER_VERSION
from scrapers.espn.migration import BASELINE_VERSION
from scrapers.espn.repository import (
    BASELINE_TABLE,
    CUTOVER_TABLE,
    ENTITY_TABLES,
    LEDGER_TABLE,
    MANIFEST_TABLE,
    PROVENANCE_COLUMNS,
    ScopeCutover,
    _PHYSICAL_IDENTITY_COLUMNS,
    _column_type,
    _row_columns,
    canonical_json,
    canonical_sha256,
    render_current_view_sql,
    render_public_canonical_view_sql,
)
from scrapers.espn.runner import RUNTIME_VERSION


UTC = timezone.utc
PLAN_VERSION = "espn-compact6-plan-v2"
JOURNAL_VERSION = "espn-compact6-journal-v2"
METRIC_VERSION = "espn-legacy-multiset-v1"
EXPECTED_NATIVE_SCOPE_COUNT = 181
RUN_GUARD_VERSION = "espn-compact6-run-guard-v1"
RUN_GUARD_MAX_AGE = timedelta(minutes=2)
RUN_GUARD_FUTURE_SKEW = timedelta(seconds=15)
REQUIRED_PAUSED_DAGS = (
    "dag_backfill_espn",
    "dag_discover_espn_registry",
    "dag_ingest_espn",
    "dag_monitor_espn",
    "dag_repair_espn",
    "dag_replay_espn",
    "dag_trigger_espn_daily",
    "dag_transform_e3",
    "dag_e3_backfill",
    "dag_transform_xref",
    "dag_transform_fbref_gold",
)
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_SCOPE_RE = re.compile(r"[1-9][0-9]*:[1-9][0-9]*")
_IDENTIFIER_RE = re.compile(r"[a-z_][a-z0-9_]*")
_ENTITY_NAMES = ("schedule", "lineup", "matchsheet")
_ARCHIVE_BY_ENTITY = dict(zip(_ENTITY_NAMES, LEGACY_ARCHIVE_TABLES))
_SOURCE_BY_ENTITY = {entity: f"espn_{entity}" for entity in _ENTITY_NAMES}
_LEGACY_KEY_COLUMNS = {
    "schedule": ("game",),
    "lineup": ("game", "team", "player"),
    "matchsheet": ("game", "team"),
}
_DISPOSITIONS = frozenset(
    {"compatibility_only", "native_current_replaced", "quarantined"}
)
_BASELINE_COLUMNS = (
    "baseline_version",
    "scope_id",
    "legacy_league",
    "legacy_season",
    "captured_at",
    "entity_metrics_json",
    "legacy_snapshot_ids_json",
    "registry_signature",
    "durable_manifest_uri",
    "durable_manifest_sha256",
    "replay_raw_manifest_uri",
    "replay_raw_manifest_sha256",
    "trust_label",
    "baseline_sha256",
)


class Compact6Error(RuntimeError):
    """The cutover evidence, topology, or execution state is unsafe."""


class InjectedFailure(Compact6Error):
    """Deterministic failure used to exercise every journal boundary."""


def _require_mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise Compact6Error(f"{field} must be an object")
    return value


def _require_sequence(value: object, field: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise Compact6Error(f"{field} must be an array")
    return value


def _require_text(value: object, field: str) -> str:
    if type(value) is not str or not value.strip():
        raise Compact6Error(f"{field} must be a non-empty string")
    return value


def _require_sha(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise Compact6Error(f"{field} must be a lower-case SHA-256")
    return value


def _require_positive_int(value: object, field: str) -> int:
    if type(value) is not int or value <= 0:
        raise Compact6Error(f"{field} must be a positive integer")
    return value


def _require_timestamp(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        try:
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise Compact6Error(f"{field} must be an ISO-8601 timestamp") from exc
    else:
        raise Compact6Error(f"{field} must be an ISO-8601 timestamp")
    if result.tzinfo is None or result.utcoffset() is None:
        raise Compact6Error(f"{field} must include a timezone")
    return result.astimezone(UTC)


def _stored_utc_timestamp(value: object, field: str) -> datetime:
    """Normalize Trino ``timestamp`` values, whose DBAPI form is timezone-naive."""

    if isinstance(value, datetime) and value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return _require_timestamp(value, field)


def _canonical_json_field(value: object, field: str) -> Any:
    if type(value) is not str:
        raise Compact6Error(f"{field} must be canonical JSON")
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise Compact6Error(f"{field} must be valid JSON") from exc
    if canonical_json(decoded) != value:
        raise Compact6Error(f"{field} must be canonical JSON")
    return decoded


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if type(value) is int:
        return str(value)
    if isinstance(value, datetime):
        normalized = value.astimezone(UTC).replace(tzinfo=None).isoformat(sep=" ")
        return f"TIMESTAMP {_sql_string(normalized)}"
    return _sql_string(str(value))


def _qualified(catalog: str, schema: str, relation: str) -> str:
    for value, field in (
        (catalog, "catalog"),
        (schema, "schema"),
        (relation, "relation"),
    ):
        if _IDENTIFIER_RE.fullmatch(value) is None:
            raise Compact6Error(f"{field} is not a safe SQL identifier")
    return f"{catalog}.{schema}.{relation}"


def _view_postcondition_sql(
    *,
    catalog: str,
    schema: str,
    relation: str,
    required_tokens: Sequence[str],
) -> str:
    token_predicates = " AND ".join(
        "lower(view_definition) LIKE "
        + _sql_string(f"%{_require_text(token, 'view identity token').lower()}%")
        for token in required_tokens
    )
    return (
        f"SELECT count(*) = 1 FROM {catalog}.information_schema.views "
        f"WHERE table_schema = {_sql_string(schema)} "
        f"AND table_name = {_sql_string(relation)} AND {token_predicates}"
    )


def _view_and_schema_postcondition_sql(
    *,
    catalog: str,
    schema: str,
    relation: str,
    required_tokens: Sequence[str],
    expected_columns: Sequence[tuple[str, str]],
) -> str:
    token_predicates = " AND ".join(
        "lower(view_definition) LIKE "
        + _sql_string(f"%{_require_text(token, 'view identity token').lower()}%")
        for token in required_tokens
    )
    expected = ",\n    ".join(
        f"({index}, {_sql_string(name)}, {_sql_string(data_type)})"
        for index, (name, data_type) in enumerate(expected_columns, 1)
    )
    return f"""WITH definition_match AS (
    SELECT 1
    FROM {catalog}.information_schema.views
    WHERE table_schema = {_sql_string(schema)}
      AND table_name = {_sql_string(relation)}
      AND {token_predicates}
), expected(ordinal_position, column_name, data_type) AS (VALUES
    {expected}
), observed AS (
    SELECT ordinal_position, column_name, data_type
    FROM {catalog}.information_schema.columns
    WHERE table_schema = {_sql_string(schema)}
      AND table_name = {_sql_string(relation)}
), mismatch AS (
    (SELECT * FROM expected EXCEPT SELECT * FROM observed)
    UNION ALL
    (SELECT * FROM observed EXCEPT SELECT * FROM expected)
)
SELECT (SELECT count(*) = 1 FROM definition_match)
   AND NOT EXISTS (SELECT 1 FROM mismatch)"""


def _disposition_hash_base(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        column: row.get(column)
        for column in LEGACY_DISPOSITION_COLUMNS
        if column != "disposition_sha256"
    }


def build_dispositions(
    archive_id: str,
    observed_pairs: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Classify every observed legacy pair without losing historical seasons."""

    archive_id = _require_text(archive_id, "archive_id")
    replacement_by_pair = {
        (league, season): scope
        for scope, league, season in REVIEWED_NATIVE_REPLACEMENTS
    }
    merged: dict[tuple[str | None, str | None], set[str]] = {}
    for index, raw in enumerate(_require_sequence(observed_pairs, "observed_pairs")):
        row = _require_mapping(raw, f"observed_pairs[{index}]")
        league = row.get("league")
        season = row.get("season")
        if league is not None:
            league = _require_text(league, f"observed_pairs[{index}].league")
        if season is not None:
            season = _require_text(season, f"observed_pairs[{index}].season")
        entities = _require_sequence(
            row.get("observed_entities"), f"observed_pairs[{index}].observed_entities"
        )
        normalized_entities = {
            _require_text(value, "observed entity") for value in entities
        }
        if not normalized_entities or not normalized_entities <= set(_ENTITY_NAMES):
            raise Compact6Error("observed legacy entities are invalid")
        merged.setdefault((league, season), set()).update(normalized_entities)

    rows: list[dict[str, Any]] = []
    for league, season in sorted(
        merged,
        key=lambda pair: (
            pair[0] is None,
            "" if pair[0] is None else pair[0],
            pair[1] is None,
            "" if pair[1] is None else pair[1],
        ),
    ):
        replacement_scope = replacement_by_pair.get((league, season))
        if league is None or season is None:
            disposition = "quarantined"
            replacement_scope = None
        elif replacement_scope is not None:
            disposition = "native_current_replaced"
        else:
            disposition = "compatibility_only"
        base = {
            "archive_id": archive_id,
            "league": league,
            "season": season,
            "disposition": disposition,
            "replacement_scope_id": replacement_scope,
            "observed_entities_json": canonical_json(sorted(merged[(league, season)])),
        }
        rows.append({**base, "disposition_sha256": canonical_sha256(base)})

    replacements = {
        (row["replacement_scope_id"], row["league"], row["season"])
        for row in rows
        if row["disposition"] == "native_current_replaced"
    }
    if replacements != set(REVIEWED_NATIVE_REPLACEMENTS):
        raise Compact6Error(
            "legacy observations do not contain the exact reviewed six replacements"
        )
    return rows


def build_legacy_archive_id(legacy_tables: Mapping[str, Any]) -> str:
    """Return a transition-independent identity for one frozen global source."""

    if set(legacy_tables) != set(_ENTITY_NAMES):
        raise Compact6Error("legacy archive identity requires all three entities")
    snapshots: dict[str, int] = {}
    for entity in _ENTITY_NAMES:
        table = _require_mapping(legacy_tables[entity], f"legacy_tables.{entity}")
        source = table.get("source_table")
        if source != _SOURCE_BY_ENTITY[entity]:
            raise Compact6Error("legacy archive identity has an invalid source table")
        snapshots[source] = _require_positive_int(
            table.get("snapshot_id"), f"legacy archive snapshot {source}"
        )
    identity = canonical_sha256(
        {
            "manifest_version": ARCHIVE_MANIFEST_VERSION,
            "legacy_snapshot_ids": snapshots,
        }
    )
    return f"espn-global-legacy-{identity[:32]}"


def seal_native_route(route: Mapping[str, Any]) -> dict[str, Any]:
    base = dict(route)
    base.pop("route_sha256", None)
    return {**base, "route_sha256": canonical_sha256(base)}


def seal_plan(plan: Mapping[str, Any]) -> dict[str, Any]:
    base = dict(plan)
    base.pop("plan_sha256", None)
    return {**base, "plan_sha256": canonical_sha256(base)}


def seal_run_guard(guard: Mapping[str, Any]) -> dict[str, Any]:
    base = dict(guard)
    base.pop("evidence_sha256", None)
    return {**base, "evidence_sha256": canonical_sha256(base)}


def _validate_metric(raw: object, field: str) -> dict[str, Any]:
    metric = _require_mapping(raw, field)
    if set(metric) != {"row_count", "row_hash", "distinct_key_count"}:
        raise Compact6Error(f"{field} has an invalid whole-row metric schema")
    count = metric["row_count"]
    distinct_count = metric["distinct_key_count"]
    if (
        type(count) is not int
        or count < 0
        or type(distinct_count) is not int
        or not 0 <= distinct_count <= count
    ):
        raise Compact6Error(f"{field} has invalid whole-row metric counts")
    _require_sha(metric["row_hash"], f"{field}.row_hash")
    return dict(metric)


def _validate_dispositions(rows: object) -> tuple[dict[str, Any], ...]:
    normalized: list[dict[str, Any]] = []
    seen_pairs: set[tuple[object, object]] = set()
    for index, raw in enumerate(_require_sequence(rows, "legacy_dispositions")):
        row = dict(_require_mapping(raw, f"legacy_dispositions[{index}]"))
        if set(row) != set(LEGACY_DISPOSITION_COLUMNS):
            raise Compact6Error("legacy disposition columns are incomplete")
        if row["disposition"] not in _DISPOSITIONS:
            raise Compact6Error("legacy disposition value is invalid")
        pair = (row["league"], row["season"])
        if pair in seen_pairs:
            raise Compact6Error("legacy disposition contains a duplicate pair")
        seen_pairs.add(pair)
        _canonical_json_field(row["observed_entities_json"], "observed_entities_json")
        expected = canonical_sha256(_disposition_hash_base(row))
        if row["disposition_sha256"] != expected:
            raise Compact6Error("legacy disposition hash does not match")
        if row["disposition"] == "native_current_replaced":
            if not row["replacement_scope_id"]:
                raise Compact6Error("replacement disposition has no scope")
        elif row["replacement_scope_id"] is not None:
            raise Compact6Error("non-replacement disposition names a scope")
        if (row["league"] is None or row["season"] is None) != (
            row["disposition"] == "quarantined"
        ):
            raise Compact6Error("NULL legacy pairs must be quarantined")
        normalized.append(row)

    actual = {
        (row["replacement_scope_id"], row["league"], row["season"])
        for row in normalized
        if row["disposition"] == "native_current_replaced"
    }
    if actual != set(REVIEWED_NATIVE_REPLACEMENTS):
        raise Compact6Error("legacy dispositions do not bind the reviewed six")
    return tuple(normalized)


def _validate_native_routes(plan: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    raw_targets = _require_sequence(plan.get("target_scope_ids"), "target_scope_ids")
    targets = tuple(_require_text(value, "target scope ID") for value in raw_targets)
    if len(targets) != EXPECTED_NATIVE_SCOPE_COUNT or len(set(targets)) != len(targets):
        raise Compact6Error("target route set must contain exactly 181 unique scopes")
    if any(_SCOPE_RE.fullmatch(scope) is None for scope in targets):
        raise Compact6Error("target route set contains an invalid scope ID")
    expected_target_sha = canonical_sha256(sorted(targets))
    if plan.get("target_scope_sha256") != expected_target_sha:
        raise Compact6Error("target scope SHA-256 does not match exact route set")
    if plan.get("native_scope_count") != EXPECTED_NATIVE_SCOPE_COUNT:
        raise Compact6Error("native scope count must equal 181")

    routes: list[dict[str, Any]] = []
    route_scopes: list[str] = []
    replacements = {
        scope: (league, season)
        for scope, league, season in REVIEWED_NATIVE_REPLACEMENTS
    }
    for index, raw in enumerate(
        _require_sequence(plan.get("native_routes"), "native_routes")
    ):
        row = dict(_require_mapping(raw, f"native_routes[{index}]"))
        stored_hash = row.pop("route_sha256", None)
        if stored_hash != canonical_sha256(row):
            raise Compact6Error("native route hash does not match")
        scope = _require_text(row.get("scope_id"), "native route scope_id")
        route_scopes.append(scope)
        expected_pair = replacements.get(scope)
        if expected_pair is None:
            if (
                row.get("previous_source") != "absent"
                or row.get("legacy_league") is not None
                or row.get("legacy_season") is not None
                or row.get("route_action") != "append_root"
            ):
                raise Compact6Error("non-replacement route must start from absent")
        elif (
            row.get("previous_source") != "legacy"
            or (row.get("legacy_league"), row.get("legacy_season")) != expected_pair
            or row.get("route_action") != "retain_existing"
        ):
            raise Compact6Error("reviewed replacement route has wrong legacy pair")
        _require_text(row.get("generation_id"), "native route generation_id")
        _require_sha(
            row.get("generation_signature"), "native route generation_signature"
        )
        _require_sha(row.get("manifest_sha256"), "native route manifest_sha256")
        if (
            row.get("registry_signature") != plan.get("registry_signature")
            or row.get("registry_snapshot_uri") != plan.get("registry_snapshot_uri")
            or row.get("registry_snapshot_sha256")
            != plan.get("registry_snapshot_sha256")
        ):
            raise Compact6Error("native route registry identity is mixed")
        if row.get("target_scope_sha256") != plan.get("target_scope_sha256"):
            raise Compact6Error("native route target identity is mixed")
        if (row.get("parser_version"), row.get("runtime_version")) != (
            PARSER_VERSION,
            RUNTIME_VERSION,
        ):
            raise Compact6Error("native route parser/runtime identity is mixed")
        _require_timestamp(row.get("effective_at"), "native route effective_at")
        routes.append({**row, "route_sha256": stored_hash})

    if len(route_scopes) != len(set(route_scopes)):
        raise Compact6Error("native route set contains a duplicate scope")
    if set(route_scopes) != set(targets) or len(routes) != EXPECTED_NATIVE_SCOPE_COUNT:
        raise Compact6Error("native route set does not match exact 181 targets")
    return tuple(routes)


def _validate_source_cutover_heads(
    plan: Mapping[str, Any], routes: Sequence[Mapping[str, Any]]
) -> tuple[dict[str, Any], ...]:
    if plan.get("source_cutover_row_count") != len(REVIEWED_NATIVE_REPLACEMENTS):
        raise Compact6Error("source cutover inventory must contain the reviewed six")
    raw_heads = _require_sequence(
        plan.get("source_cutover_heads"), "source_cutover_heads"
    )
    if len(raw_heads) != len(REVIEWED_NATIVE_REPLACEMENTS):
        raise Compact6Error("source cutover heads must contain the reviewed six")
    route_by_scope = {row["scope_id"]: row for row in routes}
    expected_scopes = {
        scope for scope, _league, _season in REVIEWED_NATIVE_REPLACEMENTS
    }
    normalized: list[dict[str, Any]] = []
    seen_scopes: set[str] = set()
    seen_cutover_ids: set[str] = set()
    for index, raw in enumerate(raw_heads):
        head = dict(_require_mapping(raw, f"source_cutover_heads[{index}]"))
        scope = _require_text(head.get("scope_id"), "source cutover scope_id")
        cutover_id = _require_text(head.get("cutover_id"), "source cutover ID")
        _require_sha(head.get("cutover_sha256"), "source cutover SHA-256")
        if scope in seen_scopes or cutover_id in seen_cutover_ids:
            raise Compact6Error("source cutover heads contain a duplicate or fork")
        seen_scopes.add(scope)
        seen_cutover_ids.add(cutover_id)
        route = route_by_scope.get(scope)
        if route is None or route.get("route_action") != "retain_existing":
            raise Compact6Error("source cutover head is outside the reviewed six")
        expected = (
            "native",
            "legacy",
            route["legacy_league"],
            route["legacy_season"],
            route["registry_signature"],
            route["generation_id"],
            route["generation_signature"],
            route["manifest_sha256"],
        )
        actual = tuple(
            head.get(field)
            for field in (
                "active_source",
                "previous_source",
                "legacy_league",
                "legacy_season",
                "registry_signature",
                "native_generation_id",
                "native_generation_signature",
                "native_manifest_sha256",
            )
        )
        if actual != expected:
            raise Compact6Error(
                "source cutover head differs from its reviewed native route"
            )
        normalized.append(head)
    if seen_scopes != expected_scopes:
        raise Compact6Error("source cutover heads do not equal the reviewed six")
    return tuple(normalized)


def _validate_baseline_evidence(
    plan: Mapping[str, Any], expected_snapshots: Mapping[str, int]
) -> dict[str, dict[str, Any]]:
    evidence = _require_mapping(
        plan.get("baseline_evidence_by_scope"), "baseline_evidence_by_scope"
    )
    expected_pairs = {
        scope: (league, season)
        for scope, league, season in REVIEWED_NATIVE_REPLACEMENTS
    }
    if set(evidence) != set(expected_pairs):
        raise Compact6Error("baseline evidence must bind the exact reviewed six scopes")
    normalized: dict[str, dict[str, Any]] = {}
    for scope, raw in evidence.items():
        row = dict(_require_mapping(raw, f"baseline evidence {scope}"))
        if set(row) != set(_BASELINE_COLUMNS):
            raise Compact6Error("baseline evidence columns are incomplete")
        if (
            row["baseline_version"] != BASELINE_VERSION
            or row["scope_id"] != scope
            or (row["legacy_league"], row["legacy_season"]) != expected_pairs[scope]
            or row["trust_label"] != "trusted"
            or row["registry_signature"] != plan.get("registry_signature")
        ):
            raise Compact6Error(
                "baseline evidence identity differs from reviewed scope"
            )
        captured_at = _require_timestamp(
            row["captured_at"], f"baseline captured_at {scope}"
        )
        metrics = _canonical_json_field(
            row["entity_metrics_json"], f"baseline metrics {scope}"
        )
        if not isinstance(metrics, Mapping) or set(metrics) != set(_ENTITY_NAMES):
            raise Compact6Error("baseline entity metrics are incomplete")
        for entity in _ENTITY_NAMES:
            metric = _require_mapping(metrics[entity], f"baseline {entity} metrics")
            if not {"row_count", "distinct_key_count", "max_ingested_at"} <= set(
                metric
            ):
                raise Compact6Error("baseline entity metric columns are incomplete")
            row_count = metric["row_count"]
            distinct_count = metric["distinct_key_count"]
            if (
                type(row_count) is not int
                or row_count < 0
                or type(distinct_count) is not int
                or not 0 <= distinct_count <= row_count
            ):
                raise Compact6Error("baseline entity metric counts are invalid")
            archive_scope_metrics = [
                item
                for item in plan["legacy_tables"][entity]["per_scope_metrics"]
                if (item["league"], item["season"]) == expected_pairs[scope]
            ]
            if len(archive_scope_metrics) != 1 or (
                metric["row_count"],
                metric["distinct_key_count"],
            ) != (
                archive_scope_metrics[0]["row_count"],
                archive_scope_metrics[0]["distinct_key_count"],
            ):
                raise Compact6Error(
                    "baseline entity metrics differ from global archive scope"
                )
        if metrics["schedule"]["row_count"] <= 0:
            raise Compact6Error("reviewed legacy baseline schedule is empty")
        snapshots = _canonical_json_field(
            row["legacy_snapshot_ids_json"], f"baseline snapshots {scope}"
        )
        if snapshots != dict(expected_snapshots):
            raise Compact6Error(
                "baseline evidence snapshots differ from global archive"
            )
        for field in ("durable_manifest_uri", "replay_raw_manifest_uri"):
            value = _require_text(row[field], f"baseline {field}")
            if "://" not in value:
                raise Compact6Error(
                    "baseline artifact URI must be immutable and absolute"
                )
        for field in (
            "durable_manifest_sha256",
            "replay_raw_manifest_sha256",
            "baseline_sha256",
        ):
            _require_sha(row[field], f"baseline {field}")
        hash_base = {
            key: (captured_at if key == "captured_at" else row[key])
            for key in _BASELINE_COLUMNS
            if key != "baseline_sha256"
        }
        if canonical_sha256(hash_base) != row["baseline_sha256"]:
            raise Compact6Error("baseline evidence SHA-256 does not match")
        normalized[scope] = row
    return normalized


def validate_plan(plan: Mapping[str, Any]) -> Mapping[str, Any]:
    """Validate the complete immutable transition plan before any SQL runs."""

    plan = _require_mapping(plan, "plan")
    if plan.get("schema_version") != PLAN_VERSION:
        raise Compact6Error("unsupported compact6 plan schema")
    stored_plan_sha = _require_sha(plan.get("plan_sha256"), "plan_sha256")
    if (
        canonical_sha256({k: v for k, v in plan.items() if k != "plan_sha256"})
        != stored_plan_sha
    ):
        raise Compact6Error("plan SHA-256 does not match")
    if plan.get("source_layout") != LEGACY14 or plan.get("target_layout") != COMPACT6:
        raise Compact6Error("plan must transition legacy14 to compact6")
    if (
        plan.get("bronze_schema") != BRONZE_SCHEMA
        or plan.get("internal_schema") != INTERNAL_SCHEMA
    ):
        raise Compact6Error("plan schemas do not match the compact6 contract")
    _qualified(
        _require_text(plan.get("catalog"), "catalog"),
        BRONZE_SCHEMA,
        LEGACY_TABLES[0],
    )
    _require_text(plan.get("transition_id"), "transition_id")
    _require_timestamp(plan.get("created_at"), "created_at")
    _require_sha(plan.get("registry_signature"), "registry_signature")
    _require_text(plan.get("registry_snapshot_uri"), "registry_snapshot_uri")
    _require_sha(plan.get("registry_snapshot_sha256"), "registry_snapshot_sha256")

    legacy_tables = _require_mapping(plan.get("legacy_tables"), "legacy_tables")
    if set(legacy_tables) != set(_ENTITY_NAMES):
        raise Compact6Error("legacy plan must bind all three entity tables")
    expected_snapshots: dict[str, int] = {}
    observed_scope_entities: dict[tuple[object, object], set[str]] = {}
    for entity in _ENTITY_NAMES:
        table = _require_mapping(legacy_tables[entity], f"legacy_tables.{entity}")
        if table.get("source_table") != _SOURCE_BY_ENTITY[entity]:
            raise Compact6Error(f"legacy {entity} source table is invalid")
        expected_snapshots[_SOURCE_BY_ENTITY[entity]] = _require_positive_int(
            table.get("snapshot_id"), f"legacy_tables.{entity}.snapshot_id"
        )
        columns = _require_sequence(
            table.get("columns"), f"legacy_tables.{entity}.columns"
        )
        if not columns:
            raise Compact6Error(f"legacy {entity} column contract is empty")
        names: list[str] = []
        for raw_column in columns:
            column = _require_mapping(raw_column, f"legacy {entity} column")
            if set(column) != {"name", "type"}:
                raise Compact6Error(f"legacy {entity} column contract is invalid")
            names.append(_require_text(column["name"], "legacy column name"))
            _require_text(column["type"], "legacy column type")
        if len(names) != len(set(names)):
            raise Compact6Error(f"legacy {entity} column contract has duplicates")
        whole_metric = _validate_metric(
            table.get("whole_rowset_metrics"), f"legacy_tables.{entity}"
        )
        per_scope_rows = _require_sequence(
            table.get("per_scope_metrics"), f"legacy_tables.{entity}.per_scope_metrics"
        )
        seen_pairs: set[tuple[object, object]] = set()
        per_scope_count = 0
        for index, raw_metric in enumerate(per_scope_rows):
            metric = dict(
                _require_mapping(
                    raw_metric, f"legacy_tables.{entity}.per_scope_metrics[{index}]"
                )
            )
            if set(metric) != {
                "league",
                "season",
                "row_count",
                "row_hash",
                "distinct_key_count",
            }:
                raise Compact6Error("legacy per-scope metric schema is invalid")
            pair = (metric.pop("league"), metric.pop("season"))
            if pair in seen_pairs:
                raise Compact6Error("legacy per-scope metric pair is duplicated")
            seen_pairs.add(pair)
            normalized_metric = _validate_metric(
                metric, f"legacy_tables.{entity}.per_scope_metrics[{index}]"
            )
            per_scope_count += normalized_metric["row_count"]
            observed_scope_entities.setdefault(pair, set()).add(entity)
        if per_scope_count != whole_metric["row_count"]:
            raise Compact6Error("legacy per-scope row counts differ from whole metrics")

    dispositions = _validate_dispositions(plan.get("legacy_dispositions"))
    archive_ids = {row["archive_id"] for row in dispositions}
    expected_archive_id = build_legacy_archive_id(legacy_tables)
    if archive_ids != {expected_archive_id}:
        raise Compact6Error(
            "all dispositions must bind the deterministic global archive ID"
        )
    disposition_inventory = {
        (row["league"], row["season"]): set(
            _canonical_json_field(
                row["observed_entities_json"], "disposition observed entities"
            )
        )
        for row in dispositions
    }
    if disposition_inventory != observed_scope_entities:
        raise Compact6Error(
            "legacy dispositions differ from the complete per-scope inventory"
        )
    replacements = _require_sequence(
        plan.get("native_replacements"), "native_replacements"
    )
    normalized_replacements = {
        (
            row.get("scope_id"),
            row.get("legacy_league"),
            row.get("legacy_season"),
        )
        for row in (
            _require_mapping(value, "native replacement") for value in replacements
        )
    }
    if (
        normalized_replacements != set(REVIEWED_NATIVE_REPLACEMENTS)
        or len(replacements) != 6
    ):
        raise Compact6Error("plan does not contain the exact reviewed six replacements")

    native_routes = _validate_native_routes(plan)
    _require_positive_int(plan.get("manifest_snapshot_id"), "manifest snapshot ID")
    _validate_source_cutover_heads(plan, native_routes)
    state_snapshots = _require_mapping(plan.get("state_snapshots"), "state_snapshots")
    expected_state = {*GENERATION_TABLES, CUTOVER_TABLE, BASELINE_TABLE}
    if set(state_snapshots) != expected_state:
        raise Compact6Error("state snapshot set is incomplete")
    for relation, snapshot in state_snapshots.items():
        _require_positive_int(snapshot, f"state snapshot {relation}")

    baseline_hashes = _require_mapping(
        plan.get("baseline_sha256_by_scope"), "baseline_sha256_by_scope"
    )
    replacement_scopes = {
        scope for scope, _league, _season in REVIEWED_NATIVE_REPLACEMENTS
    }
    if set(baseline_hashes) != replacement_scopes:
        raise Compact6Error("baseline hashes must bind the exact reviewed six scopes")
    for scope, value in baseline_hashes.items():
        _require_sha(value, f"baseline SHA-256 for {scope}")
    evidence_map = _validate_baseline_evidence(plan, expected_snapshots)
    for scope, item in evidence_map.items():
        if item["baseline_sha256"] != baseline_hashes[scope]:
            raise Compact6Error("baseline evidence SHA-256 does not match")
    return plan


def _disposition_metrics(dispositions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    hashes = sorted(
        _require_sha(row.get("disposition_sha256"), "disposition SHA-256")
        for row in dispositions
    )
    return {
        "row_count": len(hashes),
        "row_hash": hashlib.sha256("".join(hashes).encode("ascii")).hexdigest(),
    }


def _pinned_disposition_integrity_sql(
    plan: Mapping[str, Any], manifest: Mapping[str, Any]
) -> str:
    metrics = _canonical_json_field(
        manifest["legacy_disposition_metrics_json"],
        "legacy_disposition_metrics_json",
    )
    table = _qualified(
        plan["catalog"], plan["internal_schema"], LEGACY_DISPOSITION_TABLE
    )
    return f"""SELECT 1
FROM {table} FOR VERSION AS OF {manifest["legacy_disposition_snapshot_id"]}
WHERE archive_id = {_sql_string(manifest["archive_id"])}
HAVING COUNT(*) <> {metrics["row_count"]}
    OR COUNT(*) <> COUNT(DISTINCT (
        COALESCE(league, '<NULL>') || chr(31) || COALESCE(season, '<NULL>')
    ))
    OR lower(to_hex(sha256(to_utf8(array_join(
           array_sort(array_agg(disposition_sha256)), ''
       ))))) <> {_sql_string(metrics["row_hash"])}"""


def build_archive_manifest(
    plan: Mapping[str, Any],
    *,
    archive_snapshot_ids: Mapping[str, int],
    disposition_snapshot_id: int,
    captured_at: datetime,
) -> dict[str, Any]:
    validate_plan(plan)
    expected_archive_tables = set(LEGACY_ARCHIVE_TABLES)
    if set(archive_snapshot_ids) != expected_archive_tables:
        raise Compact6Error("archive snapshot set is incomplete")
    normalized_archive_snapshots = {
        table: _require_positive_int(snapshot, f"archive snapshot {table}")
        for table, snapshot in sorted(archive_snapshot_ids.items())
    }
    disposition_snapshot_id = _require_positive_int(
        disposition_snapshot_id, "legacy disposition snapshot ID"
    )
    captured_at = _require_timestamp(captured_at, "archive captured_at")
    dispositions = _validate_dispositions(plan["legacy_dispositions"])
    archive_id = dispositions[0]["archive_id"]
    whole_metrics = {
        entity: dict(plan["legacy_tables"][entity]["whole_rowset_metrics"])
        for entity in _ENTITY_NAMES
    }
    legacy_snapshots = {
        _SOURCE_BY_ENTITY[entity]: plan["legacy_tables"][entity]["snapshot_id"]
        for entity in _ENTITY_NAMES
    }
    replacements = [
        {
            "scope_id": scope,
            "legacy_league": league,
            "legacy_season": season,
        }
        for scope, league, season in REVIEWED_NATIVE_REPLACEMENTS
    ]
    base = {
        "manifest_version": ARCHIVE_MANIFEST_VERSION,
        "archive_id": archive_id,
        "captured_at": captured_at,
        "registry_signature": plan["registry_signature"],
        "legacy_snapshot_ids_json": canonical_json(legacy_snapshots),
        "archive_snapshot_ids_json": canonical_json(normalized_archive_snapshots),
        "whole_rowset_metrics_json": canonical_json(whole_metrics),
        "legacy_disposition_snapshot_id": disposition_snapshot_id,
        "legacy_disposition_metrics_json": canonical_json(
            _disposition_metrics(dispositions)
        ),
        "legacy_dispositions_json": canonical_json(list(dispositions)),
        "native_replacements_json": canonical_json(replacements),
        "plan_sha256": plan["plan_sha256"],
    }
    return {**base, "manifest_sha256": canonical_sha256(base)}


def validate_archive_manifest(
    manifest: Mapping[str, Any],
    dispositions: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    manifest = _require_mapping(manifest, "archive manifest")
    if set(manifest) != set(ARCHIVE_MANIFEST_COLUMNS):
        raise Compact6Error("archive manifest columns are incomplete")
    if manifest.get("manifest_version") != ARCHIVE_MANIFEST_VERSION:
        raise Compact6Error("archive manifest version is invalid")
    _require_text(manifest.get("archive_id"), "archive_id")
    _require_timestamp(manifest.get("captured_at"), "captured_at")
    _require_sha(manifest.get("registry_signature"), "registry_signature")
    _require_sha(manifest.get("plan_sha256"), "plan_sha256")
    stored_sha = _require_sha(manifest.get("manifest_sha256"), "manifest_sha256")
    if (
        canonical_sha256({k: v for k, v in manifest.items() if k != "manifest_sha256"})
        != stored_sha
    ):
        raise Compact6Error("archive manifest hash does not match")

    legacy_snapshots = _canonical_json_field(
        manifest["legacy_snapshot_ids_json"], "legacy_snapshot_ids_json"
    )
    if set(legacy_snapshots) != set(_SOURCE_BY_ENTITY.values()):
        raise Compact6Error("legacy snapshot set is incomplete")
    for value in legacy_snapshots.values():
        _require_positive_int(value, "legacy snapshot ID")
    archive_snapshots = _canonical_json_field(
        manifest["archive_snapshot_ids_json"], "archive_snapshot_ids_json"
    )
    if set(archive_snapshots) != set(LEGACY_ARCHIVE_TABLES):
        raise Compact6Error("archive snapshot set is incomplete")
    for value in archive_snapshots.values():
        _require_positive_int(value, "archive snapshot ID")
    whole_metrics = _canonical_json_field(
        manifest["whole_rowset_metrics_json"], "whole_rowset_metrics_json"
    )
    if set(whole_metrics) != set(_ENTITY_NAMES):
        raise Compact6Error("archive whole-row metrics are incomplete")
    for entity, metric in whole_metrics.items():
        _validate_metric(metric, f"archive whole-row metrics {entity}")

    normalized_dispositions = _validate_dispositions(dispositions)
    if {row["archive_id"] for row in normalized_dispositions} != {
        manifest["archive_id"]
    }:
        raise Compact6Error("archive dispositions belong to another archive")
    embedded = _canonical_json_field(
        manifest["legacy_dispositions_json"], "legacy_dispositions_json"
    )
    if embedded != list(normalized_dispositions):
        raise Compact6Error("archive embedded dispositions do not match")
    metrics = _canonical_json_field(
        manifest["legacy_disposition_metrics_json"],
        "legacy_disposition_metrics_json",
    )
    if metrics != _disposition_metrics(normalized_dispositions):
        raise Compact6Error("archive disposition metrics do not match")
    _require_positive_int(
        manifest["legacy_disposition_snapshot_id"],
        "legacy disposition snapshot ID",
    )
    expected_replacements = [
        {
            "scope_id": scope,
            "legacy_league": league,
            "legacy_season": season,
        }
        for scope, league, season in REVIEWED_NATIVE_REPLACEMENTS
    ]
    if (
        _canonical_json_field(
            manifest["native_replacements_json"], "native_replacements_json"
        )
        != expected_replacements
    ):
        raise Compact6Error("archive manifest does not bind the reviewed six")
    return manifest


def _validate_archive_manifest_for_plan(
    manifest: Mapping[str, Any], plan: Mapping[str, Any]
) -> Mapping[str, Any]:
    """Prove that a self-consistent manifest is the exact plan evidence.

    A manifest hash only proves internal consistency.  Publication additionally
    binds every source snapshot, whole-row metric, registry identity, archive
    identity, and the complete disposition payload to the sealed plan.
    """

    validate_plan(plan)
    validate_archive_manifest(manifest, plan["legacy_dispositions"])
    expected_legacy_snapshots = {
        _SOURCE_BY_ENTITY[entity]: plan["legacy_tables"][entity]["snapshot_id"]
        for entity in _ENTITY_NAMES
    }
    expected_whole_metrics = {
        entity: plan["legacy_tables"][entity]["whole_rowset_metrics"]
        for entity in _ENTITY_NAMES
    }
    expected_archive_id = plan["legacy_dispositions"][0]["archive_id"]
    if (
        manifest["plan_sha256"] != plan["plan_sha256"]
        or manifest["registry_signature"] != plan["registry_signature"]
        or manifest["archive_id"] != expected_archive_id
        or _canonical_json_field(
            manifest["legacy_snapshot_ids_json"], "legacy_snapshot_ids_json"
        )
        != expected_legacy_snapshots
        or _canonical_json_field(
            manifest["whole_rowset_metrics_json"], "whole_rowset_metrics_json"
        )
        != expected_whole_metrics
    ):
        raise Compact6Error("archive manifest differs from its sealed plan")
    return manifest


@dataclass(frozen=True, slots=True)
class MigrationStep:
    index: int
    name: str
    sql: str
    postcondition_sql: str | None = None
    result_must_be_empty: bool = False
    capture_result: bool = False

    def __post_init__(self) -> None:
        if type(self.index) is not int or self.index < 0:
            raise ValueError("migration step index must be non-negative")
        _require_text(self.name, "migration step name")
        _require_text(self.sql, "migration step SQL")
        if self.postcondition_sql is not None:
            _require_text(self.postcondition_sql, "migration postcondition SQL")

    @property
    def sql_sha256(self) -> str:
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()


def _step_builder() -> Callable[..., MigrationStep]:
    counter = 0

    def add(
        name: str,
        sql: str,
        *,
        postcondition_sql: str | None = None,
        result_must_be_empty: bool = False,
        capture_result: bool = False,
    ) -> MigrationStep:
        nonlocal counter
        step = MigrationStep(
            counter,
            name,
            sql,
            postcondition_sql=postcondition_sql,
            result_must_be_empty=result_must_be_empty,
            capture_result=capture_result,
        )
        counter += 1
        return step

    return add


def _column_projection(plan: Mapping[str, Any], entity: str) -> str:
    return ", ".join(
        f'"{column["name"]}"' for column in plan["legacy_tables"][entity]["columns"]
    )


def _current_columns(entity: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys((*_row_columns(entity), *PROVENANCE_COLUMNS)))


def _whole_metric_predicate(
    relation: str,
    columns: Sequence[str],
    expected: Mapping[str, Any],
    *,
    key_columns: Sequence[str],
) -> str:
    # The plan's row hash is produced by the same deterministic null/type-aware
    # JSON multiset expression during ``plan``.  The hash is order independent
    # but duplicate sensitive.
    row_expr = (
        "json_format(CAST(ROW("
        + ", ".join(f'"{column}"' for column in columns)
        + ") AS JSON))"
    )
    hash_expr = (
        "COALESCE(lower(to_hex(sha256(to_utf8(array_join(array_sort(array_agg("
        f"lower(to_hex(sha256(to_utf8({row_expr})))))), ''))))), "
        "lower(to_hex(sha256(to_utf8('')))))"
    )
    return f"""SELECT 1
FROM {relation}
HAVING COUNT(*) <> {expected["row_count"]}
    OR COUNT(DISTINCT ROW({", ".join(f'"{column}"' for column in key_columns)}))
       <> {expected["distinct_key_count"]}
    OR {hash_expr} <> '{expected["row_hash"]}'"""


def _multiset_parity_sql(
    left_relation: str,
    right_relation: str,
    *,
    columns: Sequence[str],
) -> str:
    row_expr = (
        "json_format(CAST(ROW("
        + ", ".join(f'"{column}"' for column in columns)
        + ") AS JSON))"
    )
    row_fingerprint = f"lower(to_hex(sha256(to_utf8({row_expr}))))"
    grouping = "scope_id, league, CAST(season AS varchar)"

    def metrics(relation: str) -> str:
        return f"""SELECT scope_id, league, CAST(season AS varchar) season,
           COUNT(*) row_count,
           lower(to_hex(sha256(to_utf8(array_join(
               array_sort(array_agg({row_fingerprint})), ''
           ))))) row_hash
    FROM {relation}
    GROUP BY {grouping}"""

    return f"""WITH expected AS (
    {metrics(left_relation)}
), observed AS (
    {metrics(right_relation)}
)
SELECT 1
FROM expected
FULL OUTER JOIN observed
  ON expected.scope_id IS NOT DISTINCT FROM observed.scope_id
 AND expected.league IS NOT DISTINCT FROM observed.league
 AND expected.season IS NOT DISTINCT FROM observed.season
WHERE expected.row_count IS DISTINCT FROM observed.row_count
   OR expected.row_hash IS DISTINCT FROM observed.row_hash
LIMIT 1"""


def _all_row_parity_sql(left_relation: str, right_relation: str) -> str:
    """Return rows only when two relations differ, preserving duplicates."""

    return f"""SELECT 1 FROM (
    (SELECT * FROM {left_relation} EXCEPT ALL SELECT * FROM {right_relation})
    UNION ALL
    (SELECT * FROM {right_relation} EXCEPT ALL SELECT * FROM {left_relation})
) mismatch
LIMIT 1"""


def _expected_schema_gate_sql(
    *,
    catalog: str,
    schema: str,
    relation: str,
    columns: Sequence[Mapping[str, Any]],
) -> str:
    expected = ",\n    ".join(
        f"({index}, {_sql_string(column['name'])}, {_sql_string(column['type'])})"
        for index, column in enumerate(columns, 1)
    )
    return f"""WITH expected(ordinal_position, column_name, data_type) AS (VALUES
    {expected}
), observed AS (
    SELECT ordinal_position, column_name, data_type
    FROM {catalog}.information_schema.columns
    WHERE table_schema = {_sql_string(schema)}
      AND table_name = {_sql_string(relation)}
), mismatch AS (
    (SELECT * FROM expected EXCEPT SELECT * FROM observed)
    UNION ALL
    (SELECT * FROM observed EXCEPT SELECT * FROM expected)
)
SELECT * FROM mismatch"""


def _relation_schema_parity_sql(
    *,
    catalog: str,
    source_schema: str,
    source_relation: str,
    target_schema: str,
    target_relation: str,
) -> str:
    return f"""WITH source_schema AS (
    SELECT ordinal_position, column_name, data_type
    FROM {catalog}.information_schema.columns
    WHERE table_schema = {_sql_string(source_schema)}
      AND table_name = {_sql_string(source_relation)}
), target_schema AS (
    SELECT ordinal_position, column_name, data_type
    FROM {catalog}.information_schema.columns
    WHERE table_schema = {_sql_string(target_schema)}
      AND table_name = {_sql_string(target_relation)}
), mismatch AS (
    (SELECT * FROM source_schema EXCEPT SELECT * FROM target_schema)
    UNION ALL
    (SELECT * FROM target_schema EXCEPT SELECT * FROM source_schema)
)
SELECT * FROM mismatch"""


def _relation_evidence_sql(entity: str, relation: str, columns: Sequence[str]) -> str:
    row_expr = (
        "json_format(CAST(ROW("
        + ", ".join(f'"{column}"' for column in columns)
        + ") AS JSON))"
    )
    row_hash = (
        "COALESCE(lower(to_hex(sha256(to_utf8(array_join(array_sort(array_agg("
        f"lower(to_hex(sha256(to_utf8({row_expr})))))), ''))))), "
        "lower(to_hex(sha256(to_utf8('')))))"
    )
    return (
        f"SELECT {_sql_string(entity)} entity, COUNT(*) row_count, "
        f"{row_hash} row_hash FROM {relation}"
    )


def _archive_per_scope_parity_sql(
    plan: Mapping[str, Any], manifest: Mapping[str, Any], entity: str
) -> str:
    metrics = plan["legacy_tables"][entity]["per_scope_metrics"]
    expected_values = ",\n    ".join(
        "("
        + ", ".join(
            (
                f"CAST({_sql_literal(row['league'])} AS varchar)",
                f"CAST({_sql_literal(row['season'])} AS varchar)",
                str(row["row_count"]),
                _sql_string(row["row_hash"]),
                str(row["distinct_key_count"]),
            )
        )
        + ")"
        for row in metrics
    )
    archive_snapshots = json.loads(manifest["archive_snapshot_ids_json"])
    relation = (
        f"{_qualified(plan['catalog'], plan['internal_schema'], _ARCHIVE_BY_ENTITY[entity])} "
        f"FOR VERSION AS OF {archive_snapshots[_ARCHIVE_BY_ENTITY[entity]]}"
    )
    columns = [column["name"] for column in plan["legacy_tables"][entity]["columns"]]
    observed = _captured_metric_sql(
        relation,
        columns=columns,
        key_columns=_LEGACY_KEY_COLUMNS[entity],
        group_by_scope=True,
    )
    return f"""WITH expected(
    league, season, row_count, row_hash, distinct_key_count
) AS (VALUES
    {expected_values}
), observed AS (
    {observed}
), mismatch AS (
    (SELECT * FROM expected EXCEPT SELECT * FROM observed)
    UNION ALL
    (SELECT * FROM observed EXCEPT SELECT * FROM expected)
)
SELECT * FROM mismatch"""


def _current_composition_parity_sql(
    plan: Mapping[str, Any], manifest: Mapping[str, Any], entity: str
) -> str:
    catalog = plan["catalog"]
    internal = plan["internal_schema"]
    columns = _current_columns(entity)
    native_projection = ", ".join(f'g."{column}"' for column in columns)
    captured_columns = {
        column["name"] for column in plan["legacy_tables"][entity]["columns"]
    }
    legacy_projection = ", ".join(
        f'l."{column}"'
        if column in captured_columns
        else f'CAST(NULL AS {_column_type(entity, column)}) AS "{column}"'
        for column in columns
    )
    route_values = ",\n        ".join(
        "("
        + ", ".join(
            _sql_literal(route[field])
            for field in ("scope_id", "generation_id", "generation_signature")
        )
        + ")"
        for route in sorted(plan["native_routes"], key=lambda row: row["scope_id"])
    )
    physical_identity = (
        "scope_id",
        "competition_id",
        "source_season_year",
        "generation_id",
        "generation_signature",
        "run_id",
        "_batch_id",
        "registry_snapshot_uri",
        "registry_signature",
        "plan_signature",
        "parser_version",
        "runtime_version",
        "_row_sha256",
    )
    generation = _qualified(
        catalog, internal, GENERATION_TABLES[_ENTITY_NAMES.index(entity)]
    )
    archive_snapshots = json.loads(manifest["archive_snapshot_ids_json"])
    archive = (
        f"{_qualified(catalog, internal, _ARCHIVE_BY_ENTITY[entity])} "
        f"FOR VERSION AS OF {archive_snapshots[_ARCHIVE_BY_ENTITY[entity]]}"
    )
    disposition = (
        f"{_qualified(catalog, internal, LEGACY_DISPOSITION_TABLE)} "
        f"FOR VERSION AS OF {manifest['legacy_disposition_snapshot_id']}"
    )
    current = _qualified(catalog, internal, CURRENT_VIEWS[_ENTITY_NAMES.index(entity)])
    expected = f"""WITH routes(
    scope_id, generation_id, generation_signature
) AS (VALUES
        {route_values}
), ranked_native AS (
    SELECT g.*, ROW_NUMBER() OVER (
        PARTITION BY {", ".join(f'g."{column}"' for column in physical_identity)}
        ORDER BY g."_ingested_at" DESC, g."_row_sha256" DESC
    ) physical_rn
    FROM {generation} g
    JOIN routes r
      ON r.scope_id = g.scope_id
     AND r.generation_id = g.generation_id
     AND r.generation_signature = g.generation_signature
), native_expected AS (
    SELECT {native_projection} FROM ranked_native g WHERE physical_rn = 1
), legacy_expected AS (
    SELECT {legacy_projection}
    FROM {archive} l
    JOIN {disposition} d
      ON d.archive_id = {_sql_string(manifest["archive_id"])}
     AND d.league IS NOT DISTINCT FROM l.league
     AND d.season IS NOT DISTINCT FROM CAST(l.season AS varchar)
     AND d.disposition = 'compatibility_only'
)
SELECT * FROM native_expected
UNION ALL
SELECT * FROM legacy_expected"""
    return _multiset_parity_sql(
        f"({expected}) expected_composition",
        current,
        columns=columns,
    )


def _dynamic_native_route_gate_sql(plan: Mapping[str, Any]) -> str:
    """Prove the current post-cutover head set without freezing generation IDs."""

    expected_values = ", ".join(
        f"({_sql_string(scope)})" for scope in sorted(plan["target_scope_ids"])
    )
    catalog = plan["catalog"]
    bronze = plan["bronze_schema"]
    internal = plan["internal_schema"]
    manifest = _qualified(catalog, bronze, MANIFEST_TABLE)
    cutover = _qualified(catalog, internal, CUTOVER_TABLE)
    identity_projection = ", ".join(
        f'"{column}"' for column in _PHYSICAL_IDENTITY_COLUMNS
    )
    ranked_identity_projection = ", ".join(
        f'r."{column}"' for column in _PHYSICAL_IDENTITY_COLUMNS
    )
    candidate_join = "\n       AND ".join(
        f'r."{column}" = candidate."{column}"' for column in _PHYSICAL_IDENTITY_COLUMNS
    )
    empty_hash = "lower(to_hex(sha256(to_utf8(''))))"
    relation_tables = {**ENTITY_TABLES, "ledger": LEDGER_TABLE}
    fence_ctes = []
    fence_joins = []
    fence_predicates = []
    for relation, table in relation_tables.items():
        qualified = _qualified(
            catalog, bronze if relation == "ledger" else internal, table
        )
        fence_ctes.append(
            f"""ranked_{relation}_rows AS (
    SELECT r.*,
           ROW_NUMBER() OVER (
               PARTITION BY {ranked_identity_projection}, r."_row_sha256"
               ORDER BY r."_ingested_at" DESC, r."_row_sha256" DESC
           ) physical_rn
    FROM {qualified} r
    JOIN candidate_generation_identities candidate
      ON {candidate_join}
), {relation}_rows AS (
    SELECT * FROM ranked_{relation}_rows WHERE physical_rn = 1
), {relation}_fence AS (
    SELECT {identity_projection}, COUNT(*) row_count,
           lower(to_hex(sha256(to_utf8(array_join(
               array_sort(array_agg("_row_sha256")), ''
           ))))) row_hash
    FROM {relation}_rows
    GROUP BY {identity_projection}
)"""
        )
        fence_join = "\n   AND ".join(
            f'{relation}_fence."{column}" = m."{column}"'
            for column in _PHYSICAL_IDENTITY_COLUMNS
        )
        fence_joins.append(f"LEFT JOIN {relation}_fence\n      ON {fence_join}")
        if relation == "ledger":
            fence_predicates.extend(
                (
                    "COALESCE(ledger_fence.row_count, 0) = m.ledger_count",
                    f"COALESCE(ledger_fence.row_hash, {empty_hash}) = m.ledger_hash",
                )
            )
        else:
            fence_predicates.extend(
                (
                    f"COALESCE({relation}_fence.row_count, 0) = "
                    "TRY_CAST(TRY(json_extract_scalar("
                    f"m.row_counts_json, '$.{relation}')) AS bigint)",
                    f"COALESCE({relation}_fence.row_hash, {empty_hash}) = "
                    f"TRY(json_extract_scalar(m.row_hashes_json, '$.{relation}'))",
                )
            )
    physical_fences = ",\n".join(fence_ctes)
    physical_joins = "\n".join(fence_joins)
    physical_predicate = "\n      AND ".join(fence_predicates)
    return f"""WITH expected(scope_id) AS (VALUES {expected_values}),
complete_manifests AS (
    SELECT * FROM {manifest} WHERE status = 'complete'
), candidate_generation_identities AS (
    SELECT DISTINCT {identity_projection}
    FROM complete_manifests
), manifest_conflicts AS (
    SELECT scope_id, generation_id
    FROM complete_manifests
    GROUP BY scope_id, generation_id
    HAVING COUNT(DISTINCT manifest_sha256) > 1
        OR COUNT(DISTINCT generation_signature) > 1
), {physical_fences},
valid_manifests AS (
    SELECT m.*
    FROM complete_manifests m
    LEFT JOIN manifest_conflicts conflict
      ON conflict.scope_id = m.scope_id
     AND conflict.generation_id = m.generation_id
    {physical_joins}
    WHERE conflict.scope_id IS NULL
      AND {physical_predicate}
), parsed_cutovers AS (
    SELECT parsed.*
    FROM (
        SELECT c.*,
               TRY(CAST(json_parse(c.ancestor_cutover_sha256_json) AS array(varchar)))
                   ancestor_cutover_sha256s
        FROM {cutover} c
    ) parsed
    WHERE parsed.ancestor_cutover_sha256s IS NOT NULL
      AND json_format(CAST(parsed.ancestor_cutover_sha256s AS JSON))
          = parsed.ancestor_cutover_sha256_json
      AND lower(to_hex(sha256(to_utf8(parsed.ancestor_cutover_sha256_json))))
          = parsed.ancestor_lineage_sha256
      AND regexp_like(parsed.cutover_sha256, '^[0-9a-f]{{64}}$')
      AND cardinality(array_distinct(parsed.ancestor_cutover_sha256s))
          = cardinality(parsed.ancestor_cutover_sha256s)
), lineage_valid AS (
    SELECT child.*
    FROM parsed_cutovers child
    WHERE (
        child.predecessor_cutover_id IS NULL
        AND child.predecessor_cutover_sha256 IS NULL
        AND cardinality(child.ancestor_cutover_sha256s) = 0
    ) OR EXISTS (
        SELECT 1 FROM parsed_cutovers parent
        WHERE parent.scope_id = child.scope_id
          AND parent.cutover_id = child.predecessor_cutover_id
          AND parent.cutover_sha256 = child.predecessor_cutover_sha256
          AND child.ancestor_cutover_sha256s =
              CONCAT(parent.ancestor_cutover_sha256s, ARRAY[parent.cutover_sha256])
    )
), conflicting_ids AS (
    SELECT cutover_id FROM {cutover}
    GROUP BY cutover_id HAVING COUNT(DISTINCT cutover_sha256) > 1
), conflicting_predecessors AS (
    SELECT scope_id, predecessor_cutover_sha256 FROM {cutover}
    GROUP BY scope_id, predecessor_cutover_sha256
    HAVING COUNT(DISTINCT cutover_sha256) > 1
), invalid_lineage_hashes AS (
    SELECT cutover_sha256 FROM parsed_cutovers
    EXCEPT
    SELECT cutover_sha256 FROM lineage_valid
), bad_cutover_hashes AS (
    SELECT c.cutover_sha256
    FROM {cutover} c
    JOIN conflicting_ids conflict ON conflict.cutover_id = c.cutover_id
    UNION
    SELECT c.cutover_sha256
    FROM {cutover} c
    JOIN conflicting_predecessors fork
      ON fork.scope_id = c.scope_id
     AND fork.predecessor_cutover_sha256 IS NOT DISTINCT FROM c.predecessor_cutover_sha256
    UNION
    SELECT cutover_sha256 FROM invalid_lineage_hashes
), eligible AS (
    SELECT c.*
    FROM lineage_valid c
    WHERE NOT EXISTS (
        SELECT 1 FROM bad_cutover_hashes bad
        WHERE bad.cutover_sha256 = c.cutover_sha256
           OR CONTAINS(c.ancestor_cutover_sha256s, bad.cutover_sha256)
    )
), ranked AS (
    SELECT c.*, ROW_NUMBER() OVER (
        PARTITION BY scope_id
        ORDER BY effective_at DESC, cutover_id DESC, cutover_sha256 DESC
    ) rn
    FROM eligible c
), ready AS (
    SELECT c.scope_id
    FROM ranked c
    JOIN valid_manifests m
      ON m.scope_id = c.scope_id
     AND m.generation_id = c.native_generation_id
     AND m.generation_signature = c.native_generation_signature
     AND m.manifest_sha256 = c.native_manifest_sha256
     AND m.registry_signature = c.registry_signature
    WHERE c.rn = 1
      AND c.active_source = 'native'
      AND (
          (c.previous_source = 'legacy' AND c.legacy_league IS NOT NULL
                                          AND c.legacy_season IS NOT NULL)
          OR
          (c.previous_source = 'absent' AND c.legacy_league IS NULL
                                        AND c.legacy_season IS NULL)
      )
    GROUP BY c.scope_id
    HAVING COUNT(DISTINCT ROW(
        m.generation_id, m.generation_signature, m.manifest_sha256
    )) = 1
), mismatch AS (
    (SELECT * FROM expected EXCEPT SELECT * FROM ready)
    UNION ALL
    (SELECT * FROM ready EXCEPT SELECT * FROM expected)
)
SELECT * FROM mismatch"""


def _metadata_ddl(catalog: str, internal: str) -> tuple[tuple[str, str], ...]:
    disposition_columns = ",\n    ".join(
        f'"{column}" varchar' for column in LEGACY_DISPOSITION_COLUMNS
    )
    manifest_types = {
        "captured_at": "timestamp(6)",
        "legacy_disposition_snapshot_id": "bigint",
    }
    manifest_columns = ",\n    ".join(
        f'"{column}" {manifest_types.get(column, "varchar")}'
        for column in ARCHIVE_MANIFEST_COLUMNS
    )
    return (
        (
            JOURNAL_TABLE,
            f"CREATE TABLE IF NOT EXISTS {_qualified(catalog, internal, JOURNAL_TABLE)} (journal_version varchar, transition_id varchar, plan_sha256 varchar, command varchar, step_index bigint, step_name varchar, status varchar, statement_sha256 varchar, recorded_at timestamp(6), detail_json varchar, checkpoint_sha256 varchar) WITH (format = 'PARQUET')",
        ),
        (
            LEGACY_DISPOSITION_TABLE,
            f"CREATE TABLE IF NOT EXISTS {_qualified(catalog, internal, LEGACY_DISPOSITION_TABLE)} (\n    {disposition_columns}\n) WITH (format = 'PARQUET')",
        ),
        (
            ARCHIVE_MANIFEST_TABLE,
            f"CREATE TABLE IF NOT EXISTS {_qualified(catalog, internal, ARCHIVE_MANIFEST_TABLE)} (\n    {manifest_columns}\n) WITH (format = 'PARQUET')",
        ),
        (
            LAYOUT_STATE_TABLE,
            f"CREATE TABLE IF NOT EXISTS {_qualified(catalog, internal, LAYOUT_STATE_TABLE)} (layout_version varchar, layout_mode varchar, archive_id varchar, transition_id varchar, effective_at timestamp(6), plan_sha256 varchar, archive_manifest_sha256 varchar, state_sha256 varchar) WITH (format = 'PARQUET')",
        ),
    )


def _disposition_insert_sql(
    catalog: str, internal: str, dispositions: Sequence[Mapping[str, Any]]
) -> str:
    columns = ", ".join(f'"{column}"' for column in LEGACY_DISPOSITION_COLUMNS)
    values = ",\n    ".join(
        "("
        + ", ".join(_sql_literal(row[column]) for column in LEGACY_DISPOSITION_COLUMNS)
        + ")"
        for row in dispositions
    )
    table = _qualified(catalog, internal, LEGACY_DISPOSITION_TABLE)
    return f"""INSERT INTO {table} ({columns})
SELECT {columns}
FROM (VALUES
    {values}
) incoming ({columns})
WHERE NOT EXISTS (
    SELECT 1 FROM {table}
)"""


def _manifest_insert_sql(
    catalog: str, internal: str, manifest: Mapping[str, Any]
) -> str:
    columns = ", ".join(f'"{column}"' for column in ARCHIVE_MANIFEST_COLUMNS)
    values = ", ".join(
        _sql_literal(
            _require_timestamp(manifest[column], "archive captured_at")
            if column == "captured_at"
            else manifest[column]
        )
        for column in ARCHIVE_MANIFEST_COLUMNS
    )
    table = _qualified(catalog, internal, ARCHIVE_MANIFEST_TABLE)
    return f"""INSERT INTO {table} ({columns})
SELECT {values}
WHERE NOT EXISTS (
    SELECT 1 FROM {table}
)"""


def _empty_or_exact_rows_sql(
    table: str,
    columns: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
    *,
    existing_where: str | None = None,
) -> str:
    projection = ", ".join(f'"{column}"' for column in columns)
    values = ",\n    ".join(
        "(" + ", ".join(_sql_literal(row[column]) for column in columns) + ")"
        for row in rows
    )
    return f"""WITH expected({projection}) AS (VALUES
    {values}
), existing AS (
    SELECT {projection} FROM {table}{" WHERE " + existing_where if existing_where else ""}
), mismatch AS (
    (SELECT * FROM expected EXCEPT ALL SELECT * FROM existing)
    UNION ALL
    (SELECT * FROM existing EXCEPT ALL SELECT * FROM expected)
)
SELECT * FROM mismatch
WHERE (SELECT count(*) FROM existing) > 0"""


def _cutover_rows(plan: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    output: list[dict[str, Any]] = []
    transition = str(plan["transition_id"])
    for route in plan["native_routes"]:
        if route["route_action"] == "retain_existing":
            continue
        scope = route["scope_id"]
        cutover = ScopeCutover(
            cutover_id=f"{transition}-{scope.replace(':', '-')}",
            scope_id=scope,
            active_source="native",
            previous_source=route["previous_source"],
            predecessor_cutover_id=None,
            predecessor_cutover_sha256=None,
            legacy_league=route["legacy_league"],
            legacy_season=route["legacy_season"],
            registry_signature=route["registry_signature"],
            effective_at=_require_timestamp(
                route["effective_at"], "route effective_at"
            ),
            native_generation_id=route["generation_id"],
            native_generation_signature=route["generation_signature"],
            native_manifest_sha256=route["manifest_sha256"],
            rollback_run_id=None,
            rollback_reason=None,
            metadata={
                "transition_id": transition,
                "plan_sha256": plan["plan_sha256"],
                "route_sha256": route["route_sha256"],
                "registry_snapshot_uri": route["registry_snapshot_uri"],
                "registry_snapshot_sha256": route["registry_snapshot_sha256"],
                "target_scope_sha256": route["target_scope_sha256"],
                "parser_version": route["parser_version"],
                "runtime_version": route["runtime_version"],
            },
        )
        output.append(cutover.to_row())
    return tuple(output)


def _cutover_insert_sql(plan: Mapping[str, Any], catalog: str, internal: str) -> str:
    rows = _cutover_rows(plan)
    if len(rows) != EXPECTED_NATIVE_SCOPE_COUNT - len(REVIEWED_NATIVE_REPLACEMENTS):
        raise Compact6Error("compact6 must append exact 175 absent native routes")
    columns = tuple(rows[0])
    projection = ", ".join(f'"{column}"' for column in columns)
    values = ",\n    ".join(
        "(" + ", ".join(_sql_literal(row[column]) for column in columns) + ")"
        for row in rows
    )
    table = _qualified(catalog, internal, CUTOVER_TABLE)
    return f"""INSERT INTO {table} ({projection})
WITH incoming ({projection}) AS (VALUES
    {values}
), conflicts AS (
    SELECT 1
    FROM {table} existing
    JOIN incoming
      ON existing.cutover_id = incoming.cutover_id
    WHERE existing.cutover_sha256 <> incoming.cutover_sha256
)
SELECT {projection}
FROM incoming
WHERE NOT EXISTS (
    SELECT 1 FROM {table} existing
    WHERE existing.cutover_id = incoming.cutover_id
)
AND NOT EXISTS (
    SELECT 1 FROM conflicts
)"""


def render_apply_steps(
    plan: Mapping[str, Any], manifest: Mapping[str, Any]
) -> tuple[MigrationStep, ...]:
    """Render the complete, replay-safe compact6 SQL transition."""

    _validate_archive_manifest_for_plan(manifest, plan)
    catalog = plan["catalog"]
    bronze = plan["bronze_schema"]
    internal = plan["internal_schema"]
    add = _step_builder()
    steps: list[MigrationStep] = []

    steps.append(
        add(
            "create_internal_schema",
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{internal}",
            postcondition_sql=(
                f"SELECT count(*) = 1 FROM {catalog}.information_schema.schemata "
                f"WHERE schema_name = {_sql_string(internal)}"
            ),
        )
    )
    for relation, ddl in _metadata_ddl(catalog, internal):
        steps.append(
            add(
                f"create_{relation}",
                ddl,
                postcondition_sql=(
                    f"SELECT count(*) = 1 FROM {catalog}.information_schema.tables "
                    f"WHERE table_schema = {_sql_string(internal)} "
                    f"AND table_name = {_sql_string(relation)} AND table_type = 'BASE TABLE'"
                ),
            )
        )

    # Freeze every legacy row, including pre-1617 and malformed-scope rows, at
    # the exact source snapshots.  Disposition affects serving only, never copy.
    for entity in _ENTITY_NAMES:
        source = _qualified(catalog, bronze, _SOURCE_BY_ENTITY[entity])
        archive = _qualified(catalog, internal, _ARCHIVE_BY_ENTITY[entity])
        snapshot_id = plan["legacy_tables"][entity]["snapshot_id"]
        projection = _column_projection(plan, entity)
        steps.append(
            add(
                f"archive_{entity}",
                f"CREATE TABLE IF NOT EXISTS {archive} WITH (format = 'PARQUET') AS\n"
                f"SELECT {projection}\nFROM {source} FOR VERSION AS OF {snapshot_id}",
                postcondition_sql=_whole_metric_predicate(
                    archive,
                    [
                        column["name"]
                        for column in plan["legacy_tables"][entity]["columns"]
                    ],
                    plan["legacy_tables"][entity]["whole_rowset_metrics"],
                    key_columns=_LEGACY_KEY_COLUMNS[entity],
                ),
                result_must_be_empty=True,
            )
        )
        archive_schema_gate = _expected_schema_gate_sql(
            catalog=catalog,
            schema=internal,
            relation=_ARCHIVE_BY_ENTITY[entity],
            columns=plan["legacy_tables"][entity]["columns"],
        )
        steps.append(
            add(
                f"verify_archive_schema_{entity}",
                archive_schema_gate,
                postcondition_sql=archive_schema_gate,
                result_must_be_empty=True,
            )
        )
        retained = _qualified(
            catalog, internal, RETAINED_LEGACY_MAIN_TABLES[_ENTITY_NAMES.index(entity)]
        )
        retained_parity = _all_row_parity_sql(
            f"{source} FOR VERSION AS OF {snapshot_id}", retained
        )
        steps.append(
            add(
                f"retain_legacy_main_{entity}",
                f"CREATE TABLE IF NOT EXISTS {retained} WITH (format = 'PARQUET') AS\n"
                f"SELECT {projection}\nFROM {source} FOR VERSION AS OF {snapshot_id}",
                postcondition_sql=retained_parity,
                result_must_be_empty=True,
            )
        )
        retained_schema_gate = _expected_schema_gate_sql(
            catalog=catalog,
            schema=internal,
            relation=RETAINED_LEGACY_MAIN_TABLES[_ENTITY_NAMES.index(entity)],
            columns=plan["legacy_tables"][entity]["columns"],
        )
        steps.append(
            add(
                f"verify_retained_schema_{entity}",
                retained_schema_gate,
                postcondition_sql=retained_schema_gate,
                result_must_be_empty=True,
            )
        )

    # Copy the five mutable repository relations at exact snapshots before the
    # old public objects can be removed.
    for relation, snapshot_id in sorted(plan["state_snapshots"].items()):
        target = _qualified(catalog, internal, relation)
        source = _qualified(catalog, bronze, relation)
        state_parity = _all_row_parity_sql(
            f"{source} FOR VERSION AS OF {snapshot_id}", target
        )
        steps.append(
            add(
                f"copy_state_{relation}",
                f"CREATE TABLE IF NOT EXISTS {target} WITH (format = 'PARQUET') AS\n"
                f"SELECT * FROM {source} FOR VERSION AS OF {snapshot_id}",
                postcondition_sql=state_parity,
                result_must_be_empty=True,
            )
        )
        state_schema_gate = _relation_schema_parity_sql(
            catalog=catalog,
            source_schema=bronze,
            source_relation=relation,
            target_schema=internal,
            target_relation=relation,
        )
        steps.append(
            add(
                f"verify_state_schema_{relation}",
                state_schema_gate,
                postcondition_sql=state_schema_gate,
                result_must_be_empty=True,
            )
        )

    disposition_table = _qualified(catalog, internal, LEGACY_DISPOSITION_TABLE)
    disposition_preflight = _empty_or_exact_rows_sql(
        disposition_table,
        LEGACY_DISPOSITION_COLUMNS,
        plan["legacy_dispositions"],
    )
    steps.append(
        add(
            "preflight_legacy_dispositions",
            disposition_preflight,
            postcondition_sql=disposition_preflight,
            result_must_be_empty=True,
        )
    )
    steps.append(
        add(
            "insert_legacy_dispositions",
            _disposition_insert_sql(catalog, internal, plan["legacy_dispositions"]),
            postcondition_sql=(
                f"SELECT count(*) = {len(plan['legacy_dispositions'])} FROM "
                f"{_qualified(catalog, internal, LEGACY_DISPOSITION_TABLE)} "
                f"FOR VERSION AS OF {manifest['legacy_disposition_snapshot_id']} "
                f"WHERE archive_id = {_sql_string(plan['legacy_dispositions'][0]['archive_id'])}"
            ),
        )
    )
    archive_snapshots = json.loads(manifest["archive_snapshot_ids_json"])
    for entity in _ENTITY_NAMES:
        archive = (
            f"{_qualified(catalog, internal, _ARCHIVE_BY_ENTITY[entity])} "
            f"FOR VERSION AS OF {archive_snapshots[_ARCHIVE_BY_ENTITY[entity]]}"
        )
        whole_gate = _whole_metric_predicate(
            archive,
            [column["name"] for column in plan["legacy_tables"][entity]["columns"]],
            plan["legacy_tables"][entity]["whole_rowset_metrics"],
            key_columns=_LEGACY_KEY_COLUMNS[entity],
        )
        steps.append(
            add(
                f"verify_pinned_archive_whole_{entity}",
                whole_gate,
                postcondition_sql=whole_gate,
                result_must_be_empty=True,
            )
        )
        scope_gate = _archive_per_scope_parity_sql(plan, manifest, entity)
        steps.append(
            add(
                f"verify_pinned_archive_per_scope_{entity}",
                scope_gate,
                postcondition_sql=scope_gate,
                result_must_be_empty=True,
            )
        )

    # Six legacy baselines must be the exact per-scope evidence captured at the
    # bound baseline table snapshot.  They are cross-checks, not the archive.
    baseline_snapshot = plan["state_snapshots"][BASELINE_TABLE]
    baseline_values = ", ".join(
        f"({_sql_string(scope)}, {_sql_string(value)})"
        for scope, value in sorted(plan["baseline_sha256_by_scope"].items())
    )
    baseline_source = _qualified(catalog, bronze, BASELINE_TABLE)
    baseline_target = _qualified(catalog, internal, BASELINE_TABLE)
    baseline_gate = f"""WITH expected(scope_id, baseline_sha256) AS (VALUES {baseline_values}),
source_observed AS (
    SELECT scope_id, baseline_sha256
    FROM {baseline_source} FOR VERSION AS OF {baseline_snapshot}
    WHERE scope_id IN (SELECT scope_id FROM expected)
), target_observed AS (
    SELECT scope_id, baseline_sha256
    FROM {baseline_target}
    WHERE scope_id IN (SELECT scope_id FROM expected)
), mismatch AS (
    (SELECT * FROM expected EXCEPT SELECT * FROM source_observed)
    UNION ALL
    (SELECT * FROM source_observed EXCEPT SELECT * FROM expected)
    UNION ALL
    (SELECT * FROM expected EXCEPT SELECT * FROM target_observed)
    UNION ALL
    (SELECT * FROM target_observed EXCEPT SELECT * FROM expected)
)
SELECT * FROM mismatch"""
    steps.append(
        add(
            "verify_exact_six_baselines",
            baseline_gate,
            postcondition_sql=baseline_gate,
            result_must_be_empty=True,
        )
    )

    manifest_table = _qualified(catalog, bronze, "espn_ingest_manifest_v2")
    native_values = ",\n    ".join(
        "("
        + ", ".join(
            _sql_literal(value)
            for value in (
                route["scope_id"],
                route["generation_id"],
                route["generation_signature"],
                route["manifest_sha256"],
                route["registry_snapshot_uri"],
                route["registry_signature"],
                route["parser_version"],
                route["runtime_version"],
            )
        )
        + ")"
        for route in sorted(plan["native_routes"], key=lambda row: row["scope_id"])
    )
    native_head_gate = f"""WITH expected(
    scope_id, generation_id, generation_signature, manifest_sha256,
    registry_snapshot_uri, registry_signature, parser_version, runtime_version
) AS (VALUES
    {native_values}
), complete_manifests AS (
    SELECT m.*
    FROM {manifest_table} FOR VERSION AS OF {plan["manifest_snapshot_id"]} AS m
    WHERE status = 'complete'
), conflicts AS (
    SELECT scope_id, generation_id
    FROM complete_manifests
    GROUP BY scope_id, generation_id
    HAVING COUNT(DISTINCT manifest_sha256) > 1
        OR COUNT(DISTINCT generation_signature) > 1
), ranked AS (
    SELECT complete_manifests.*,
           ROW_NUMBER() OVER (
               PARTITION BY complete_manifests.scope_id
               ORDER BY complete_manifests.completed_at DESC,
                        complete_manifests.generation_id DESC,
                        complete_manifests.manifest_sha256 DESC
           ) AS rn
    FROM complete_manifests
    LEFT JOIN conflicts
      ON conflicts.scope_id = complete_manifests.scope_id
     AND conflicts.generation_id = complete_manifests.generation_id
    WHERE conflicts.scope_id IS NULL
), observed AS (
    SELECT scope_id, generation_id, generation_signature, manifest_sha256,
           registry_snapshot_uri, registry_signature, parser_version, runtime_version
    FROM ranked WHERE rn = 1
), mismatch AS (
    (SELECT * FROM expected EXCEPT SELECT * FROM observed)
    UNION ALL
    (SELECT * FROM observed EXCEPT SELECT * FROM expected)
)
SELECT * FROM mismatch"""
    steps.append(
        add(
            "verify_exact_181_native_heads",
            native_head_gate,
            postcondition_sql=native_head_gate,
            result_must_be_empty=True,
        )
    )

    source_cutover_values = ", ".join(
        "("
        + ", ".join(
            _sql_literal(head[field])
            for field in (
                "scope_id",
                "cutover_id",
                "cutover_sha256",
                "native_generation_id",
                "native_generation_signature",
                "native_manifest_sha256",
            )
        )
        + ")"
        for head in sorted(
            plan["source_cutover_heads"], key=lambda row: row["scope_id"]
        )
    )
    cutover_table = _qualified(catalog, internal, CUTOVER_TABLE)
    source_cutover_gate = f"""WITH expected(
    scope_id, cutover_id, cutover_sha256, native_generation_id,
    native_generation_signature, native_manifest_sha256
) AS (VALUES {source_cutover_values}), observed AS (
    SELECT scope_id, cutover_id, cutover_sha256, native_generation_id,
           native_generation_signature, native_manifest_sha256
    FROM {cutover_table}
), mismatch AS (
    (SELECT * FROM expected EXCEPT SELECT * FROM observed)
    UNION ALL
    (SELECT * FROM observed EXCEPT SELECT * FROM expected)
)
SELECT * FROM mismatch"""
    steps.append(
        add(
            "verify_exact_six_source_cutover_heads",
            source_cutover_gate,
            postcondition_sql=source_cutover_gate,
            result_must_be_empty=True,
        )
    )

    cutover_rows = _cutover_rows(plan)
    cutover_ids = ", ".join(_sql_string(row["cutover_id"]) for row in cutover_rows)
    cutover_preflight = _empty_or_exact_rows_sql(
        cutover_table,
        ("cutover_id", "cutover_sha256"),
        cutover_rows,
        existing_where=f"cutover_id IN ({cutover_ids})",
    )
    steps.append(
        add(
            "preflight_exact_175_native_routes",
            cutover_preflight,
            postcondition_sql=cutover_preflight,
            result_must_be_empty=True,
        )
    )

    # The global manifest is append-only.  Persist it only after every frozen
    # archive, per-scope, baseline, native-head, and source-cutover gate has
    # passed, so a rejected/corrected plan cannot poison the singleton.
    manifest_table_internal = _qualified(catalog, internal, ARCHIVE_MANIFEST_TABLE)
    expected_manifest_row = {
        **manifest,
        "captured_at": _require_timestamp(
            manifest["captured_at"], "archive captured_at"
        ),
    }
    manifest_preflight = _empty_or_exact_rows_sql(
        manifest_table_internal,
        ARCHIVE_MANIFEST_COLUMNS,
        (expected_manifest_row,),
    )
    steps.append(
        add(
            "preflight_archive_manifest",
            manifest_preflight,
            postcondition_sql=manifest_preflight,
            result_must_be_empty=True,
        )
    )
    steps.append(
        add(
            "insert_archive_manifest",
            _manifest_insert_sql(catalog, internal, manifest),
            postcondition_sql=(
                f"SELECT count(*) = 1 FROM {manifest_table_internal} "
                f"WHERE manifest_sha256 = {_sql_string(manifest['manifest_sha256'])}"
            ),
        )
    )
    steps.append(
        add(
            "append_exact_181_native_routes",
            _cutover_insert_sql(plan, catalog, internal),
            postcondition_sql=(
                f"SELECT count(*) = {EXPECTED_NATIVE_SCOPE_COUNT} "
                f"AND count(DISTINCT scope_id) = {EXPECTED_NATIVE_SCOPE_COUNT} "
                f"FROM {_qualified(catalog, internal, CUTOVER_TABLE)}"
            ),
        )
    )

    # Seal the verified immutable archive/route identity before rendering the
    # gated serving views.  Public legacy objects still exist at this point;
    # any interruption therefore leaves legacy14 serving and writers paused.
    state_base = {
        "layout_version": "espn-layout-state-v2",
        "layout_mode": COMPACT6,
        "archive_id": manifest["archive_id"],
        "transition_id": plan["transition_id"],
        "effective_at": _require_timestamp(
            manifest["captured_at"], "archive captured_at"
        ),
        "plan_sha256": plan["plan_sha256"],
        "archive_manifest_sha256": manifest["manifest_sha256"],
    }
    state_sha = canonical_sha256(state_base)
    state_table = _qualified(catalog, internal, LAYOUT_STATE_TABLE)
    state_row = {**state_base, "state_sha256": state_sha}
    state_preflight = _empty_or_exact_rows_sql(
        state_table,
        tuple(state_row),
        (state_row,),
    )
    steps.append(
        add(
            "preflight_layout_state",
            state_preflight,
            postcondition_sql=state_preflight,
            result_must_be_empty=True,
        )
    )
    steps.append(
        add(
            "seal_layout_state",
            f"INSERT INTO {state_table} (layout_version, layout_mode, archive_id, transition_id, effective_at, plan_sha256, archive_manifest_sha256, state_sha256) "
            f"SELECT {', '.join(_sql_literal(value) for value in (*state_base.values(), state_sha))} "
            f"WHERE NOT EXISTS (SELECT 1 FROM {state_table})",
            postcondition_sql=f"SELECT count(*) = 1 FROM {state_table} WHERE state_sha256 = {_sql_string(state_sha)}",
        )
    )

    disposition_metrics = json.loads(manifest["legacy_disposition_metrics_json"])
    for entity in _ENTITY_NAMES:
        current_sql = render_current_view_sql(
            entity,
            catalog=catalog,
            schema=bronze,
            internal_schema=internal,
            layout_mode=COMPACT6,
            archive_snapshot_id=archive_snapshots[_ARCHIVE_BY_ENTITY[entity]],
            disposition_snapshot_id=manifest["legacy_disposition_snapshot_id"],
            disposition_count=disposition_metrics["row_count"],
            disposition_hash=disposition_metrics["row_hash"],
            archive_id=manifest["archive_id"],
            archive_manifest_sha256=manifest["manifest_sha256"],
            whole_rowset_metrics=json.loads(manifest["whole_rowset_metrics_json"]),
            archive_plan_sha256=manifest["plan_sha256"],
            layout_state_sha256=state_sha,
        )
        current_name = CURRENT_VIEWS[_ENTITY_NAMES.index(entity)]
        steps.append(
            add(
                f"create_internal_current_{entity}",
                current_sql,
                postcondition_sql=_view_postcondition_sql(
                    catalog=catalog,
                    schema=internal,
                    relation=current_name,
                    required_tokens=(
                        manifest["archive_id"],
                        manifest["manifest_sha256"],
                        state_sha,
                    ),
                ),
            )
        )
        composition_gate = _current_composition_parity_sql(plan, manifest, entity)
        steps.append(
            add(
                f"verify_current_composition_{entity}",
                composition_gate,
                postcondition_sql=composition_gate,
                result_must_be_empty=True,
            )
        )
        shadow = _qualified(
            catalog, internal, COMPACT_SHADOW_TABLES[_ENTITY_NAMES.index(entity)]
        )
        current = _qualified(catalog, internal, current_name)
        columns = _current_columns(entity)
        projection = ", ".join(f'"{column}"' for column in columns)
        steps.append(
            add(
                f"materialize_shadow_{entity}",
                f"CREATE TABLE IF NOT EXISTS {shadow} WITH (format = 'PARQUET') AS\n"
                f"SELECT {projection} FROM {current}",
                postcondition_sql=_multiset_parity_sql(
                    current,
                    shadow,
                    columns=columns,
                ),
                result_must_be_empty=True,
            )
        )
        # Explicit schema/order check catches silent SELECT-list drift before a
        # public wrapper is replaced.
        expected_columns = ", ".join(
            f"ROW({_sql_string(column)}, {_sql_string(_column_type(entity, column))})"
            for column in columns
        )
        schema_check_sql = f"""WITH expected(columns) AS (VALUES ARRAY[{expected_columns}]),
observed AS (
    SELECT array_agg(ROW(column_name, data_type) ORDER BY ordinal_position) columns
    FROM {catalog}.information_schema.columns
    WHERE table_schema = {_sql_string(internal)}
      AND table_name = {_sql_string(COMPACT_SHADOW_TABLES[_ENTITY_NAMES.index(entity)])}
)
SELECT 1 FROM expected CROSS JOIN observed WHERE expected.columns <> observed.columns"""
        steps.append(
            add(
                f"verify_schema_and_multiset_{entity}",
                schema_check_sql,
                postcondition_sql=schema_check_sql,
                result_must_be_empty=True,
            )
        )

        emergency = _qualified(
            catalog, internal, EMERGENCY_LEGACY_VIEWS[_ENTITY_NAMES.index(entity)]
        )
        archive = _qualified(catalog, internal, _ARCHIVE_BY_ENTITY[entity])
        legacy_projection = _column_projection(plan, entity)
        steps.append(
            add(
                f"create_emergency_legacy_{entity}",
                f"CREATE OR REPLACE VIEW {emergency} SECURITY DEFINER AS\n"
                f"SELECT {legacy_projection} FROM {archive} FOR VERSION AS OF "
                f"{archive_snapshots[_ARCHIVE_BY_ENTITY[entity]]}",
                postcondition_sql=_view_postcondition_sql(
                    catalog=catalog,
                    schema=internal,
                    relation=EMERGENCY_LEGACY_VIEWS[_ENTITY_NAMES.index(entity)],
                    required_tokens=(
                        _ARCHIVE_BY_ENTITY[entity],
                        str(archive_snapshots[_ARCHIVE_BY_ENTITY[entity]]),
                    ),
                ),
            )
        )

    # Each canonical object is recoverable independently: DROP legacy base,
    # then publish an explicit-column SECURITY DEFINER wrapper.  A failed apply
    # invokes render_rollback_steps and points all wrappers at frozen emergency
    # views, never at live/retained legacy mains.
    for entity in _ENTITY_NAMES:
        public = _qualified(catalog, bronze, f"espn_{entity}")
        steps.append(
            add(
                f"drop_legacy_main_{entity}",
                f"DROP TABLE IF EXISTS {public}",
                postcondition_sql=(
                    f"SELECT count(*) = 0 FROM {catalog}.information_schema.tables "
                    f"WHERE table_schema = {_sql_string(bronze)} "
                    f"AND table_name = {_sql_string(f'espn_{entity}')} "
                    "AND table_type = 'BASE TABLE'"
                ),
            )
        )
        steps.append(
            add(
                f"publish_{entity}",
                render_public_canonical_view_sql(
                    entity,
                    catalog=catalog,
                    schema=bronze,
                    internal_schema=internal,
                ),
                postcondition_sql=_view_postcondition_sql(
                    catalog=catalog,
                    schema=bronze,
                    relation=f"espn_{entity}",
                    required_tokens=(f"espn_{entity}_current",),
                ),
            )
        )

    # Keep the complete legacy14 topology until the serving-name cutover has
    # converged.  Only then retire copied public state; controls intentionally
    # remain public.  A failure here rewinds publication and compensates every
    # canonical name to the frozen emergency views.
    for relation in (*GENERATION_TABLES, *CURRENT_VIEWS, CUTOVER_TABLE, BASELINE_TABLE):
        kind = "VIEW" if relation in CURRENT_VIEWS else "TABLE"
        steps.append(
            add(
                f"remove_public_{relation}",
                f"DROP {kind} IF EXISTS {_qualified(catalog, bronze, relation)}",
                postcondition_sql=(
                    f"SELECT count(*) = 0 FROM {catalog}.information_schema.tables "
                    f"WHERE table_schema = {_sql_string(bronze)} "
                    f"AND table_name = {_sql_string(relation)}"
                ),
            )
        )

    smoke_union = "\nUNION ALL\n".join(
        f"SELECT {_sql_string(entity)} entity FROM (\n"
        + _multiset_parity_sql(
            _qualified(catalog, internal, CURRENT_VIEWS[index]),
            _qualified(catalog, bronze, f"espn_{entity}"),
            columns=_current_columns(entity),
        )
        + "\n) mismatch"
        for index, entity in enumerate(_ENTITY_NAMES)
    )
    steps.append(
        add(
            "publication_smoke",
            smoke_union,
            postcondition_sql=smoke_union,
            result_must_be_empty=True,
        )
    )
    publication_evidence_sql = "\nUNION ALL\n".join(
        _relation_evidence_sql(
            entity,
            _qualified(catalog, bronze, f"espn_{entity}"),
            _current_columns(entity),
        )
        for entity in _ENTITY_NAMES
    )
    publication_parity_postcondition = (
        f"SELECT NOT EXISTS (SELECT 1 FROM ({smoke_union}) mismatch)"
    )
    steps.append(
        add(
            "capture_publication_evidence",
            publication_evidence_sql,
            postcondition_sql=publication_parity_postcondition,
            capture_result=True,
        )
    )
    expected_public_values = ", ".join(
        f"({_sql_string(name)}, {_sql_string(kind)})"
        for name, kind in sorted(COMPACT6_PUBLIC_OBJECTS.items())
    )
    expected_internal_values = ", ".join(
        f"({_sql_string(name)}, {_sql_string(kind)})"
        for name, kind in sorted(COMPACT6_INTERNAL_REQUIRED_OBJECTS.items())
    )
    inventory_gate = f"""-- expected_public_object_count = 6
WITH expected_public(table_name, table_type) AS (VALUES {expected_public_values}),
expected_internal(table_name, table_type) AS (VALUES {expected_internal_values}),
observed AS (
    SELECT table_schema, table_name, table_type
    FROM {catalog}.information_schema.tables
    WHERE table_schema IN ({_sql_string(bronze)}, {_sql_string(internal)})
      AND table_name LIKE 'espn\\_%' ESCAPE '\\'
), mismatch AS (
    (SELECT table_name, table_type FROM expected_public
    EXCEPT SELECT table_name, table_type FROM observed WHERE table_schema = {_sql_string(bronze)})
    UNION ALL
    (SELECT table_name, table_type FROM observed WHERE table_schema = {_sql_string(bronze)}
    EXCEPT SELECT table_name, table_type FROM expected_public)
    UNION ALL
    (SELECT table_name, table_type FROM expected_internal
    EXCEPT SELECT table_name, table_type FROM observed WHERE table_schema = {_sql_string(internal)})
    UNION ALL
    (SELECT table_name, table_type FROM observed WHERE table_schema = {_sql_string(internal)}
    EXCEPT SELECT table_name, table_type FROM expected_internal)
)
SELECT * FROM mismatch"""
    steps.append(
        add(
            "audit_exact_compact6_inventory",
            inventory_gate,
            postcondition_sql=inventory_gate,
            result_must_be_empty=True,
        )
    )
    return tuple(steps)


def render_materialization_steps(plan: Mapping[str, Any]) -> tuple[MigrationStep, ...]:
    """Render the pre-manifest phase whose output snapshot IDs are dynamic.

    The full renderer is reused with a syntactically valid sentinel manifest;
    only the prefix through immutable archive/state materialisation is retained.
    None of these steps changes a public object, and all are repeated
    idempotently by the final manifest-bound phase.
    """

    sentinel_manifest = build_archive_manifest(
        plan,
        archive_snapshot_ids={table: 1 for table in LEGACY_ARCHIVE_TABLES},
        disposition_snapshot_id=1,
        captured_at=_require_timestamp(plan["created_at"], "created_at"),
    )
    rendered = list(render_apply_steps(plan, sentinel_manifest))
    stop = next(
        index
        for index, step in enumerate(rendered)
        if step.name == "preflight_legacy_dispositions"
    )
    return tuple(rendered[:stop])


def _reindex_steps(steps: Sequence[MigrationStep]) -> tuple[MigrationStep, ...]:
    return tuple(replace(step, index=index) for index, step in enumerate(steps))


def render_precommit_validation_steps(
    plan: Mapping[str, Any], *, archive_snapshot_ids: Mapping[str, int]
) -> tuple[MigrationStep, ...]:
    """Render read-only immutable-evidence gates before singleton inserts."""

    provisional = build_archive_manifest(
        plan,
        archive_snapshot_ids=archive_snapshot_ids,
        disposition_snapshot_id=1,
        captured_at=_require_timestamp(plan["created_at"], "created_at"),
    )
    selected = [
        step
        for step in render_apply_steps(plan, provisional)
        if step.name.startswith("verify_pinned_archive_")
        or step.name
        in {
            "verify_exact_six_baselines",
            "verify_exact_181_native_heads",
            "verify_exact_six_source_cutover_heads",
            "preflight_exact_175_native_routes",
        }
    ]
    return _reindex_steps(selected)


def render_disposition_persistence_steps(
    plan: Mapping[str, Any], *, archive_snapshot_ids: Mapping[str, int]
) -> tuple[MigrationStep, ...]:
    """Render the append-once disposition phase after all immutable gates."""

    provisional = build_archive_manifest(
        plan,
        archive_snapshot_ids=archive_snapshot_ids,
        disposition_snapshot_id=1,
        captured_at=_require_timestamp(plan["created_at"], "created_at"),
    )
    selected = [
        step
        for step in render_apply_steps(plan, provisional)
        if step.name in {"preflight_legacy_dispositions", "insert_legacy_dispositions"}
    ]
    disposition_index = next(
        index
        for index, step in enumerate(selected)
        if step.name == "insert_legacy_dispositions"
    )
    disposition = selected[disposition_index]
    table = _qualified(
        plan["catalog"], plan["internal_schema"], LEGACY_DISPOSITION_TABLE
    )
    archive_id = plan["legacy_dispositions"][0]["archive_id"]
    selected[disposition_index] = replace(
        disposition,
        postcondition_sql=(
            f"SELECT count(*) = {len(plan['legacy_dispositions'])} FROM {table} "
            f"WHERE archive_id = {_sql_string(archive_id)}"
        ),
    )
    return _reindex_steps(selected)


def _bound_manifest_from_checkpoint(
    checkpoint: Mapping[str, Any], plan: Mapping[str, Any]
) -> dict[str, Any]:
    if (
        checkpoint.get("transition_id") != plan["transition_id"]
        or checkpoint.get("plan_sha256") != plan["plan_sha256"]
        or checkpoint.get("command") != "apply"
    ):
        raise Compact6Error("full journal belongs to another apply operation")
    context = _require_mapping(checkpoint.get("context"), "journal context")
    if set(context) != {"archive_manifest"}:
        raise Compact6Error("full journal does not durably bind one archive manifest")
    manifest = dict(
        _require_mapping(context["archive_manifest"], "journal archive manifest")
    )
    _validate_archive_manifest_for_plan(manifest, plan)
    return manifest


def _load_durable_phase_checkpoint(
    sink: IcebergJournalSink,
    *,
    plan: Mapping[str, Any],
    command: str,
) -> Mapping[str, Any] | None:
    try:
        return sink.load(
            transition_id=plan["transition_id"],
            plan_sha256=plan["plan_sha256"],
            command=command,
        )
    except Exception as exc:
        if getattr(exc, "error_name", None) in {
            "TABLE_NOT_FOUND",
            "SCHEMA_NOT_FOUND",
        }:
            return None
        raise


def _empty_journal_bootstrap_exists(client: object, *, plan: Mapping[str, Any]) -> bool:
    """Recognize the one safe CREATE-journal/checkpoint crash boundary."""

    rows = _execute(
        client,
        f"SELECT table_type FROM {plan['catalog']}.information_schema.tables "
        f"WHERE table_schema = {_sql_string(plan['internal_schema'])} "
        f"AND table_name = {_sql_string(JOURNAL_TABLE)}",
    )
    if not rows:
        schema_rows = _execute(
            client,
            f"SELECT count(*) FROM {plan['catalog']}.information_schema.schemata "
            f"WHERE schema_name = {_sql_string(plan['internal_schema'])}",
        )
        if len(schema_rows) != 1:
            raise Compact6Error("journal bootstrap schema probe is ambiguous")
        schema_row = schema_rows[0]
        schema_count = (
            schema_row.get("_col0", schema_row.get("count"))
            if isinstance(schema_row, Mapping)
            else schema_row[0]
        )
        if schema_count == 0:
            return False
        if schema_count != 1:
            raise Compact6Error("journal bootstrap schema probe is invalid")
        internal_rows = _execute(
            client,
            f"SELECT count(*) FROM {plan['catalog']}.information_schema.tables "
            f"WHERE table_schema = {_sql_string(plan['internal_schema'])} "
            "AND table_name LIKE 'espn\\_%' ESCAPE '\\'",
        )
        if len(internal_rows) != 1:
            raise Compact6Error("journal bootstrap inventory probe is ambiguous")
        internal_row = internal_rows[0]
        internal_count = (
            internal_row.get("_col0", internal_row.get("count"))
            if isinstance(internal_row, Mapping)
            else internal_row[0]
        )
        if internal_count != 0:
            raise Compact6Error(
                "pre-journal internal ESPN objects cannot be recovered safely"
            )
        return True
    if len(rows) != 1:
        raise Compact6Error("journal bootstrap relation is ambiguous")
    row = rows[0]
    kind = row.get("table_type") if isinstance(row, Mapping) else row[0]
    if str(kind).upper() != "BASE TABLE":
        raise Compact6Error("journal bootstrap relation is not a base table")
    checkpoints = _execute(
        client,
        f"SELECT count(*) FROM "
        f"{_qualified(plan['catalog'], plan['internal_schema'], JOURNAL_TABLE)}",
    )
    if len(checkpoints) != 1:
        raise Compact6Error("journal bootstrap count is ambiguous")
    count_row = checkpoints[0]
    count = (
        count_row.get("_col0", count_row.get("count"))
        if isinstance(count_row, Mapping)
        else count_row[0]
    )
    if type(count) is not int or count < 0:
        raise Compact6Error("journal bootstrap count is invalid")
    if count != 0:
        raise Compact6Error(
            "journal table contains checkpoints outside the selected transition"
        )
    return True


def apply_compaction(
    *,
    plan: Mapping[str, Any],
    client: object,
    run_guard: Callable[[], Mapping[str, Any]],
    journal_path: Path,
    manifest_path: Path,
    command: str = "apply",
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    fail_after_step: int | None = None,
    fail_before_execute_step: int | None = None,
    fail_after_execute_step: int | None = None,
) -> dict[str, Any]:
    """Materialise, dynamically bind snapshots, and perform one guarded cutover."""

    validate_plan(plan)
    if command not in {"apply", "resume"}:
        raise Compact6Error("compaction command must be apply or resume")
    _validate_run_guard(
        run_guard(),
        transition_id=plan["transition_id"],
        plan_sha256=plan["plan_sha256"],
    )
    materialization_journal = journal_path.with_name(f"{journal_path.name}.materialize")
    validation_journal = journal_path.with_name(f"{journal_path.name}.validate")
    disposition_journal = journal_path.with_name(f"{journal_path.name}.disposition")
    if command == "apply" and (
        materialization_journal.exists()
        or validation_journal.exists()
        or disposition_journal.exists()
        or journal_path.exists()
        or manifest_path.exists()
    ):
        raise Compact6Error("apply refuses existing transition artifacts; use resume")
    checkpoint_sink = IcebergJournalSink(
        client,
        catalog=plan["catalog"],
        internal_schema=plan["internal_schema"],
        clock=clock,
    )
    materialization_sink = IcebergJournalSink(
        client,
        catalog=plan["catalog"],
        internal_schema=plan["internal_schema"],
        clock=clock,
        available_after_step=f"create_{JOURNAL_TABLE}",
    )
    validation_sink = IcebergJournalSink(
        client,
        catalog=plan["catalog"],
        internal_schema=plan["internal_schema"],
        clock=clock,
    )
    disposition_sink = IcebergJournalSink(
        client,
        catalog=plan["catalog"],
        internal_schema=plan["internal_schema"],
        clock=clock,
    )
    full_checkpoint: Mapping[str, Any] | None = None
    if journal_path.exists():
        full_checkpoint = _read_journal(journal_path)
    elif command == "resume":
        try:
            full_checkpoint = checkpoint_sink.load(
                transition_id=plan["transition_id"],
                plan_sha256=plan["plan_sha256"],
                command="apply",
            )
        except Exception as exc:
            if getattr(exc, "error_name", None) not in {
                "TABLE_NOT_FOUND",
                "SCHEMA_NOT_FOUND",
            }:
                raise

    if full_checkpoint is not None:
        manifest = _bound_manifest_from_checkpoint(full_checkpoint, plan)
        if manifest_path.exists():
            local_manifest = dict(_load_json(manifest_path))
            if canonical_json(local_manifest) != canonical_json(manifest):
                raise Compact6Error("local manifest differs from durable journal")
        else:
            _write_json(manifest_path, manifest)
        if not _journal_payload_public_swap_started(full_checkpoint):
            _assert_catalog_layout(client, LEGACY14, catalog=plan["catalog"])
    else:
        durable_materialization = _load_durable_phase_checkpoint(
            materialization_sink, plan=plan, command="materialize"
        )
        durable_validation = _load_durable_phase_checkpoint(
            validation_sink, plan=plan, command="validate"
        )
        durable_disposition = _load_durable_phase_checkpoint(
            disposition_sink, plan=plan, command="disposition"
        )
        local_phase_exists = any(
            path.exists()
            for path in (
                materialization_journal,
                validation_journal,
                disposition_journal,
                manifest_path,
            )
        )
        durable_phase_exists = any(
            checkpoint is not None
            for checkpoint in (
                durable_materialization,
                durable_validation,
                durable_disposition,
            )
        )
        bootstrap_exists = (
            _empty_journal_bootstrap_exists(client, plan=plan)
            if not (local_phase_exists or durable_phase_exists)
            else False
        )
        if command == "apply" and (durable_phase_exists or bootstrap_exists):
            raise Compact6Error(
                "durable transition phase already exists; explicit resume is required"
            )
        if command == "resume" and not (
            local_phase_exists or durable_phase_exists or bootstrap_exists
        ):
            raise Compact6Error("resume requires a local or durable phase checkpoint")
        _assert_catalog_layout(client, LEGACY14, catalog=plan["catalog"])
        materialization_steps = render_materialization_steps(plan)
        run_journaled_steps(
            plan_sha256=plan["plan_sha256"],
            transition_id=plan["transition_id"],
            command=(
                "resume"
                if materialization_journal.exists()
                or durable_materialization is not None
                else "materialize"
            ),
            steps=materialization_steps,
            client=client,
            journal_path=materialization_journal,
            checkpoint_sink=materialization_sink,
            resume_from_command="materialize",
            journal_context={"phase": "materialize"},
        )
        archive_snapshots = capture_archive_snapshot_ids(
            client,
            catalog=plan["catalog"],
            internal_schema=plan["internal_schema"],
        )
        validation_steps = render_precommit_validation_steps(
            plan, archive_snapshot_ids=archive_snapshots
        )
        run_journaled_steps(
            plan_sha256=plan["plan_sha256"],
            transition_id=plan["transition_id"],
            command=(
                "resume"
                if validation_journal.exists() or durable_validation is not None
                else "validate"
            ),
            steps=validation_steps,
            client=client,
            journal_path=validation_journal,
            checkpoint_sink=validation_sink,
            resume_from_command="validate",
            journal_context={
                "phase": "validate",
                "archive_snapshot_ids": archive_snapshots,
            },
        )
        disposition_steps = render_disposition_persistence_steps(
            plan, archive_snapshot_ids=archive_snapshots
        )
        run_journaled_steps(
            plan_sha256=plan["plan_sha256"],
            transition_id=plan["transition_id"],
            command=(
                "resume"
                if disposition_journal.exists() or durable_disposition is not None
                else "disposition"
            ),
            steps=disposition_steps,
            client=client,
            journal_path=disposition_journal,
            checkpoint_sink=disposition_sink,
            resume_from_command="disposition",
            journal_context={
                "phase": "disposition",
                "archive_snapshot_ids": archive_snapshots,
            },
        )
        disposition_snapshot = capture_latest_snapshot_id(
            client,
            _qualified(
                plan["catalog"],
                plan["internal_schema"],
                LEGACY_DISPOSITION_TABLE,
            ),
        )
        if manifest_path.exists():
            manifest = dict(_load_json(manifest_path))
            _validate_archive_manifest_for_plan(manifest, plan)
            if (
                json.loads(manifest["archive_snapshot_ids_json"]) != archive_snapshots
                or manifest["legacy_disposition_snapshot_id"] != disposition_snapshot
            ):
                raise Compact6Error("durable archive manifest conflicts on resume")
        else:
            manifest = json.loads(
                canonical_json(
                    build_archive_manifest(
                        plan,
                        archive_snapshot_ids=archive_snapshots,
                        disposition_snapshot_id=disposition_snapshot,
                        captured_at=clock(),
                    )
                )
            )
            _write_json(manifest_path, manifest)

    _validate_archive_manifest_for_plan(manifest, plan)
    steps = render_apply_steps(plan, manifest)
    journal_context = {"archive_manifest": manifest}
    journal = apply_with_guard(
        plan=plan,
        steps=steps,
        client=client,
        journal_path=journal_path,
        run_guard=run_guard,
        command="resume" if full_checkpoint is not None else "apply",
        fail_after_step=fail_after_step,
        fail_before_execute_step=fail_before_execute_step,
        fail_after_execute_step=fail_after_execute_step,
        checkpoint_sink=checkpoint_sink,
        journal_context=journal_context,
        emergency_manifest=manifest,
    )
    return {
        "status": journal["status"],
        "transition_id": plan["transition_id"],
        "plan_sha256": plan["plan_sha256"],
        "manifest": manifest,
        "journal": journal,
    }


def render_rollback_steps(
    plan: Mapping[str, Any],
    manifest: Mapping[str, Any],
    *,
    reason: str,
) -> tuple[MigrationStep, ...]:
    """Render logical recovery wrappers over the frozen emergency archive."""

    _validate_archive_manifest_for_plan(manifest, plan)
    _require_text(reason, "rollback reason")
    catalog = plan["catalog"]
    bronze = plan["bronze_schema"]
    internal = plan["internal_schema"]
    add = _step_builder()
    steps: list[MigrationStep] = []
    archive_snapshots = _canonical_json_field(
        manifest["archive_snapshot_ids_json"], "archive_snapshot_ids_json"
    )
    disposition_snapshot_id = manifest["legacy_disposition_snapshot_id"]
    disposition_table = _qualified(catalog, internal, LEGACY_DISPOSITION_TABLE)
    disposition_integrity = _pinned_disposition_integrity_sql(plan, manifest)
    steps.append(
        add(
            "verify_pinned_disposition_integrity",
            disposition_integrity,
            postcondition_sql=disposition_integrity,
            result_must_be_empty=True,
        )
    )
    for entity in _ENTITY_NAMES:
        emergency_name = EMERGENCY_LEGACY_VIEWS[_ENTITY_NAMES.index(entity)]
        emergency = _qualified(catalog, internal, emergency_name)
        archive_name = _ARCHIVE_BY_ENTITY[entity]
        archive = _qualified(catalog, internal, archive_name)
        snapshot_id = archive_snapshots[archive_name]
        projection = _column_projection(plan, entity)
        legacy_columns = [
            column["name"] for column in plan["legacy_tables"][entity]["columns"]
        ]
        archive_projection = ", ".join(f'l."{column}"' for column in legacy_columns)
        archive_relation = f"{archive} FOR VERSION AS OF {snapshot_id}"
        archive_whole_gate = _whole_metric_predicate(
            archive_relation,
            legacy_columns,
            plan["legacy_tables"][entity]["whole_rowset_metrics"],
            key_columns=_LEGACY_KEY_COLUMNS[entity],
        )
        steps.append(
            add(
                f"verify_frozen_archive_whole_{entity}",
                archive_whole_gate,
                postcondition_sql=archive_whole_gate,
                result_must_be_empty=True,
            )
        )
        filtered_archive = f"""(
    SELECT {archive_projection}
    FROM {archive_relation} l
    JOIN {disposition_table} FOR VERSION AS OF {disposition_snapshot_id} d
      ON d.archive_id = {_sql_string(manifest["archive_id"])}
     AND d.league IS NOT DISTINCT FROM l.league
     AND d.season IS NOT DISTINCT FROM CAST(l.season AS varchar)
    WHERE d.disposition IN ('compatibility_only', 'native_current_replaced')
      AND d.league IS NOT NULL
      AND d.season IS NOT NULL
) filtered_archive"""
        steps.append(
            add(
                f"recreate_frozen_emergency_{entity}",
                f"CREATE OR REPLACE VIEW {emergency} SECURITY DEFINER AS\n"
                f"SELECT * FROM {filtered_archive}",
                postcondition_sql=_view_postcondition_sql(
                    catalog=catalog,
                    schema=internal,
                    relation=emergency_name,
                    required_tokens=(
                        archive_name,
                        str(snapshot_id),
                        LEGACY_DISPOSITION_TABLE,
                        str(disposition_snapshot_id),
                        manifest["archive_id"],
                        "native_current_replaced",
                    ),
                ),
            )
        )
        emergency_data_gate = _all_row_parity_sql(
            filtered_archive,
            f"(SELECT {projection} FROM {emergency})",
        )
        steps.append(
            add(
                f"verify_frozen_emergency_{entity}",
                emergency_data_gate,
                postcondition_sql=emergency_data_gate,
                result_must_be_empty=True,
            )
        )
    for entity in _ENTITY_NAMES:
        columns = _current_columns(entity)
        legacy_columns = {
            item["name"] for item in plan["legacy_tables"][entity]["columns"]
        }
        projection = ",\n    ".join(
            f'CAST("{column}" AS {_column_type(entity, column)}) AS "{column}"'
            if column in legacy_columns
            else f'CAST(NULL AS {_column_type(entity, column)}) AS "{column}"'
            for column in columns
        )
        public = _qualified(catalog, bronze, f"espn_{entity}")
        emergency = _qualified(
            catalog,
            internal,
            EMERGENCY_LEGACY_VIEWS[_ENTITY_NAMES.index(entity)],
        )
        steps.append(
            add(
                f"rollback_{entity}_to_frozen_emergency",
                "-- logical rollback; reason is sealed in the durable journal\n"
                f"CREATE OR REPLACE VIEW {public} SECURITY DEFINER AS\n"
                f"SELECT\n    {projection}\nFROM {emergency}",
                postcondition_sql=_view_and_schema_postcondition_sql(
                    catalog=catalog,
                    schema=bronze,
                    relation=f"espn_{entity}",
                    required_tokens=(
                        EMERGENCY_LEGACY_VIEWS[_ENTITY_NAMES.index(entity)],
                    ),
                    expected_columns=tuple(
                        (column, _column_type(entity, column)) for column in columns
                    ),
                ),
            )
        )
    return tuple(steps)


def recover_public_wrappers_to_emergency(
    plan: Mapping[str, Any], manifest: Mapping[str, Any], client: object
) -> None:
    """Converge every canonical public name to its frozen emergency view.

    Trino deliberately does not let ``DROP TABLE IF EXISTS`` target a view.
    Querying each object's kind first keeps compensation type-safe at every
    crash boundary, including a mix of untouched bases and published views.
    """

    _validate_archive_manifest_for_plan(manifest, plan)
    catalog = plan["catalog"]
    bronze = plan["bronze_schema"]
    rollback_steps = render_rollback_steps(
        plan,
        manifest,
        reason="automatic compensation after interrupted compact6 swap",
    )
    for step in rollback_steps:
        if not step.name.startswith(
            (
                "verify_pinned_disposition_integrity",
                "verify_frozen_archive_whole_",
                "recreate_frozen_emergency_",
                "verify_frozen_emergency_",
            )
        ):
            continue
        rows = _execute(client, step.sql)
        if step.result_must_be_empty and rows:
            raise Compact6Error(f"emergency recovery gate {step.name} failed")
        if not _postcondition_satisfied(step, client):
            raise Compact6Error(f"emergency recovery postcondition {step.name} failed")
    rollback_by_entity = {
        entity: next(
            step
            for step in rollback_steps
            if step.name == f"rollback_{entity}_to_frozen_emergency"
        )
        for entity in _ENTITY_NAMES
    }
    for entity in _ENTITY_NAMES:
        relation = f"espn_{entity}"
        kind_sql = (
            f"SELECT table_type FROM {catalog}.information_schema.tables "
            f"WHERE table_schema = {_sql_string(bronze)} "
            f"AND table_name = {_sql_string(relation)}"
        )
        rows = _execute(client, kind_sql)
        if len(rows) > 1:
            raise Compact6Error(f"public relation {relation} is ambiguous")
        kind: object | None = None
        if rows:
            row = rows[0]
            kind = (
                row.get("table_type")
                if isinstance(row, Mapping)
                else row[0]
                if isinstance(row, Sequence) and not isinstance(row, (str, bytes))
                else row
            )
        if kind in {"BASE TABLE", "TABLE"}:
            _execute(client, f"DROP TABLE {_qualified(catalog, bronze, relation)}")
        elif kind not in {None, "VIEW"}:
            raise Compact6Error(
                f"public relation {relation} has unsupported kind {kind!r}"
            )
        _execute(client, rollback_by_entity[entity].sql)
        if not _postcondition_satisfied(rollback_by_entity[entity], client):
            raise Compact6Error(
                f"emergency compensation schema contract failed for {relation}"
            )
        verified = _execute(client, kind_sql)
        verified_kind = (
            verified[0].get("table_type")
            if len(verified) == 1 and isinstance(verified[0], Mapping)
            else verified[0][0]
            if len(verified) == 1
            and isinstance(verified[0], Sequence)
            and not isinstance(verified[0], (str, bytes))
            else None
        )
        if verified_kind != "VIEW":
            raise Compact6Error(
                f"emergency compensation did not publish {relation} as a view"
            )


def render_repromotion_steps(
    plan: Mapping[str, Any], manifest: Mapping[str, Any]
) -> tuple[MigrationStep, ...]:
    """Repoint logically rolled-back wrappers to the already verified current views."""

    full = render_apply_steps(plan, manifest)
    state_base = {
        "layout_version": "espn-layout-state-v2",
        "layout_mode": COMPACT6,
        "archive_id": manifest["archive_id"],
        "transition_id": plan["transition_id"],
        "effective_at": _require_timestamp(
            manifest["captured_at"], "archive captured_at"
        ),
        "plan_sha256": plan["plan_sha256"],
        "archive_manifest_sha256": manifest["manifest_sha256"],
    }
    state_sha = canonical_sha256(state_base)
    state_table = _qualified(
        plan["catalog"], plan["internal_schema"], LAYOUT_STATE_TABLE
    )
    exact_state_gate = (
        f"SELECT count(*) = 1 AND (SELECT count(*) FROM {state_table}) = 1 "
        f"FROM {state_table} WHERE state_sha256 = {_sql_string(state_sha)}"
    )
    archive_manifest_table = _qualified(
        plan["catalog"], plan["internal_schema"], ARCHIVE_MANIFEST_TABLE
    )
    exact_manifest_gate = (
        f"SELECT count(*) = 1 AND (SELECT count(*) FROM {archive_manifest_table}) = 1 "
        f"FROM {archive_manifest_table} WHERE manifest_sha256 = "
        f"{_sql_string(manifest['manifest_sha256'])}"
    )
    disposition_gate = _pinned_disposition_integrity_sql(plan, manifest)
    prepublish = [
        MigrationStep(
            0,
            name="verify_exact_layout_state",
            sql=exact_state_gate,
            postcondition_sql=exact_state_gate,
        ),
        MigrationStep(
            0,
            name="verify_exact_archive_manifest",
            sql=exact_manifest_gate,
            postcondition_sql=exact_manifest_gate,
        ),
        MigrationStep(
            0,
            name="verify_pinned_disposition_integrity",
            sql=disposition_gate,
            postcondition_sql=disposition_gate,
            result_must_be_empty=True,
        ),
        *[step for step in full if step.name.startswith("verify_pinned_archive_")],
        *[step for step in full if step.name.startswith(("create_internal_current_",))],
    ]
    for entity in _ENTITY_NAMES:
        schema_gate = _expected_schema_gate_sql(
            catalog=plan["catalog"],
            schema=plan["internal_schema"],
            relation=CURRENT_VIEWS[_ENTITY_NAMES.index(entity)],
            columns=tuple(
                {
                    "name": column,
                    "type": _column_type(entity, column),
                }
                for column in _current_columns(entity)
            ),
        )
        prepublish.append(
            MigrationStep(
                0,
                f"verify_dynamic_current_schema_{entity}",
                schema_gate,
                postcondition_sql=schema_gate,
                result_must_be_empty=True,
            )
        )
    dynamic_route_gate = _dynamic_native_route_gate_sql(plan)
    prepublish.append(
        MigrationStep(
            0,
            "verify_dynamic_exact_181_native_routes",
            dynamic_route_gate,
            postcondition_sql=dynamic_route_gate,
            result_must_be_empty=True,
        )
    )
    current_relations = [
        _qualified(
            plan["catalog"],
            plan["internal_schema"],
            CURRENT_VIEWS[_ENTITY_NAMES.index(entity)],
        )
        for entity in _ENTITY_NAMES
    ]
    nonempty_gate = "\nUNION ALL\n".join(
        f"SELECT {_sql_string(entity)} entity WHERE NOT EXISTS "
        f"(SELECT 1 FROM {relation} LIMIT 1)"
        for entity, relation in zip(_ENTITY_NAMES, current_relations)
    )
    prepublish.append(
        MigrationStep(
            0,
            "verify_repromotion_serving_nonempty",
            nonempty_gate,
            postcondition_sql=nonempty_gate,
            result_must_be_empty=True,
        )
    )
    publication = [
        step
        for step in full
        if step.name.startswith("publish_")
        or step.name
        in {
            "publication_smoke",
            "capture_publication_evidence",
            "audit_exact_compact6_inventory",
        }
    ]
    selected = [*prepublish, *publication]
    return tuple(replace(step, index=index) for index, step in enumerate(selected))


def repromote_with_guard(
    *,
    plan: Mapping[str, Any],
    manifest: Mapping[str, Any],
    client: object,
    journal_path: Path,
    run_guard: Callable[[], Mapping[str, Any]],
    command: str | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    _validate_archive_manifest_for_plan(manifest, plan)
    resolved_command = (
        ("repromote" if not journal_path.exists() else "resume")
        if command is None
        else command
    )
    if resolved_command not in {"repromote", "resume"}:
        raise Compact6Error("repromotion command is invalid")
    _validate_run_guard(
        run_guard(),
        transition_id=plan["transition_id"],
        plan_sha256=plan["plan_sha256"],
    )
    _assert_catalog_layout(client, COMPACT6, catalog=plan["catalog"])
    steps = render_repromotion_steps(plan, manifest)

    def recheck(_step: MigrationStep) -> None:
        _validate_run_guard(
            run_guard(),
            transition_id=plan["transition_id"],
            plan_sha256=plan["plan_sha256"],
        )

    return run_journaled_steps(
        plan_sha256=plan["plan_sha256"],
        transition_id=plan["transition_id"],
        command=resolved_command,
        steps=steps,
        client=client,
        journal_path=journal_path,
        before_step=recheck,
        compensation_steps=render_rollback_steps(
            plan,
            manifest,
            reason="automatic compensation after interrupted repromotion",
        ),
        checkpoint_sink=IcebergJournalSink(
            client,
            catalog=plan["catalog"],
            internal_schema=plan["internal_schema"],
            clock=clock,
        ),
        resume_from_command="repromote",
        journal_context={"archive_manifest": json.loads(canonical_json(manifest))},
    )


def _execute(client: object, sql: str) -> Sequence[Any]:
    query = getattr(client, "query", None)
    if callable(query):
        return query(sql) or []
    execute = getattr(client, "execute_query", None)
    if callable(execute):
        return execute(sql, params=()) or []
    raise TypeError("client must expose query or execute_query")


def _assert_catalog_layout(
    client: object, layout_mode: str, *, catalog: str
) -> dict[str, Any]:
    rows = _execute(client, catalog_inventory_sql(catalog=catalog))
    return validate_catalog_layout(layout_mode, rows)


def _journal_payload(base: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(base)
    payload.pop("journal_sha256", None)
    previous_sequence = payload.get("checkpoint_sequence")
    if previous_sequence is None:
        payload["checkpoint_sequence"] = 0
    elif type(previous_sequence) is int and previous_sequence >= 0:
        payload["checkpoint_sequence"] = previous_sequence + 1
    else:
        raise Compact6Error("journal checkpoint sequence is invalid")
    return {**payload, "journal_sha256": canonical_sha256(payload)}


def _write_journal(path: Path, journal: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = canonical_json(journal) + "\n"
    descriptor, temporary = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _read_journal(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Compact6Error("compact6 journal is missing or corrupt") from exc
    if not isinstance(raw, dict):
        raise Compact6Error("compact6 journal is corrupt")
    stored = raw.get("journal_sha256")
    if stored != canonical_sha256(
        {k: v for k, v in raw.items() if k != "journal_sha256"}
    ):
        raise Compact6Error("compact6 journal hash does not match")
    if (
        type(raw.get("checkpoint_sequence")) is not int
        or raw["checkpoint_sequence"] < 0
    ):
        raise Compact6Error("compact6 journal checkpoint sequence is invalid")
    return raw


class IcebergJournalSink:
    """Append hash-bound local checkpoints to the internal Iceberg journal."""

    def __init__(
        self,
        client: object,
        *,
        catalog: str = "iceberg",
        internal_schema: str = INTERNAL_SCHEMA,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        available_after_step: str | None = None,
    ) -> None:
        self.client = client
        self.table = _qualified(catalog, internal_schema, JOURNAL_TABLE)
        self.clock = clock
        self.available_after_step = available_after_step

    def record(self, journal: Mapping[str, Any]) -> None:
        started = journal.get("started_step")
        completed = journal.get("completed_steps") or []
        availability_completed = False
        if self.available_after_step is not None:
            availability_completed = any(
                isinstance(record, Mapping)
                and record.get("name") == self.available_after_step
                for record in completed
            )
            reached = availability_completed or (
                isinstance(started, Mapping)
                and started.get("name") == self.available_after_step
            )
            if not reached:
                return
        latest = started or (completed[-1] if completed else {})
        detail_json = canonical_json(journal)
        base = {
            "journal_version": JOURNAL_VERSION,
            "transition_id": journal["transition_id"],
            "plan_sha256": journal["plan_sha256"],
            "command": journal["command"],
            "step_index": latest.get("index", -1),
            "step_name": latest.get("name", "journal_start"),
            "status": journal["status"],
            "statement_sha256": latest.get("sql_sha256", "0" * 64),
            "recorded_at": self.clock(),
            "detail_json": detail_json,
        }
        checkpoint = canonical_sha256(base)
        columns = (*base, "checkpoint_sha256")
        values = (*base.values(), checkpoint)
        try:
            _execute(
                self.client,
                f"INSERT INTO {self.table} ({', '.join(columns)}) "
                f"SELECT {', '.join(_sql_literal(value) for value in values)} "
                f"WHERE NOT EXISTS (SELECT 1 FROM {self.table} "
                f"WHERE checkpoint_sha256 = {_sql_string(checkpoint)})",
            )
        except Exception as exc:
            if not (
                self.available_after_step is not None
                and not availability_completed
                and isinstance(started, Mapping)
                and started.get("name") == self.available_after_step
                and getattr(exc, "error_name", None) == "TABLE_NOT_FOUND"
            ):
                raise

    def load(
        self, *, transition_id: str, plan_sha256: str, command: str
    ) -> dict[str, Any] | None:
        rows = _execute(
            self.client,
            f"SELECT journal_version, transition_id, plan_sha256, command, "
            f"step_index, step_name, status, statement_sha256, recorded_at, "
            f"detail_json, checkpoint_sha256 FROM {self.table} "
            f"WHERE transition_id = {_sql_string(transition_id)} "
            f"AND plan_sha256 = {_sql_string(plan_sha256)} "
            f"AND command = {_sql_string(command)} "
            "ORDER BY recorded_at, checkpoint_sha256",
        )
        if not rows:
            return None
        columns = (
            "journal_version",
            "transition_id",
            "plan_sha256",
            "command",
            "step_index",
            "step_name",
            "status",
            "statement_sha256",
            "recorded_at",
            "detail_json",
            "checkpoint_sha256",
        )
        by_sequence: dict[int, dict[str, Any]] = {}
        for row in _mapping_rows(rows, columns, field="Iceberg journal"):
            recorded_at = row["recorded_at"]
            if isinstance(recorded_at, datetime) and (
                recorded_at.tzinfo is None or recorded_at.utcoffset() is None
            ):
                row["recorded_at"] = recorded_at.replace(tzinfo=UTC)
            base = {key: row[key] for key in columns if key != "checkpoint_sha256"}
            if canonical_sha256(base) != row["checkpoint_sha256"]:
                raise Compact6Error("Iceberg journal checkpoint hash does not match")
            detail = _canonical_json_field(row["detail_json"], "journal detail_json")
            if not isinstance(detail, dict):
                raise Compact6Error("Iceberg journal detail is malformed")
            if detail.get("journal_sha256") != canonical_sha256(
                {key: value for key, value in detail.items() if key != "journal_sha256"}
            ):
                raise Compact6Error("Iceberg journal detail hash does not match")
            sequence = detail.get("checkpoint_sequence")
            if type(sequence) is not int or sequence < 0:
                raise Compact6Error("Iceberg journal sequence is invalid")
            existing = by_sequence.get(sequence)
            if existing is not None and canonical_json(existing) != canonical_json(
                detail
            ):
                raise Compact6Error("Iceberg journal contains a checkpoint fork")
            by_sequence[sequence] = detail
        return by_sequence[max(by_sequence)]


def _postcondition_satisfied(step: MigrationStep, client: object) -> bool:
    if step.postcondition_sql is None:
        return False
    try:
        rows = _execute(client, step.postcondition_sql)
    except Exception as exc:
        if getattr(exc, "error_name", None) == "TABLE_NOT_FOUND":
            return False
        raise
    if step.result_must_be_empty:
        return len(rows) == 0
    if len(rows) != 1:
        return False
    row = rows[0]
    value = (
        next(iter(row.values()))
        if isinstance(row, Mapping) and len(row) == 1
        else row[0]
        if isinstance(row, Sequence)
        and not isinstance(row, (str, bytes))
        and len(row) == 1
        else row
    )
    return value is True or value == 1


def _validate_journal_steps(
    journal: Mapping[str, Any], steps: Sequence[MigrationStep]
) -> None:
    completed = _require_sequence(journal.get("completed_steps"), "completed_steps")
    if len(completed) > len(steps):
        raise Compact6Error("journal contains unknown completed steps")
    for index, raw in enumerate(completed):
        record = _require_mapping(raw, f"completed_steps[{index}]")
        step = steps[index]
        if (
            record.get("index") != step.index
            or record.get("name") != step.name
            or record.get("sql_sha256") != step.sql_sha256
        ):
            raise Compact6Error("journal step identity does not match plan")
        if (
            type(record.get("result_row_count")) is not int
            or record["result_row_count"] < 0
        ):
            raise Compact6Error("journal step result count is invalid")
        _require_sha(record.get("result_sha256"), "journal step result SHA-256")


def run_journaled_steps(
    *,
    plan_sha256: str,
    transition_id: str,
    command: str,
    steps: Sequence[MigrationStep],
    client: object,
    journal_path: Path,
    fail_after_step: int | None = None,
    fail_before_execute_step: int | None = None,
    fail_after_execute_step: int | None = None,
    fail_after_compensation: bool = False,
    before_step: Callable[[MigrationStep], None] | None = None,
    compensation_steps: Sequence[MigrationStep] = (),
    compensation_callback: Callable[[], None] | None = None,
    compensate_after_step: int = 0,
    checkpoint_sink: IcebergJournalSink | None = None,
    resume_from_command: str | None = None,
    journal_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute a fixed step list with crash-window-aware durable checkpoints."""

    plan_sha256 = _require_sha(plan_sha256, "plan_sha256")
    transition_id = _require_text(transition_id, "transition_id")
    stored_commands = {
        "apply",
        "rollback",
        "repromote",
        "materialize",
        "validate",
        "disposition",
    }
    if command not in {*stored_commands, "resume"}:
        raise Compact6Error("journal command is invalid")
    expected_stored_command = (
        resume_from_command
        if command == "resume" and resume_from_command is not None
        else "apply"
        if command == "resume"
        else command
    )
    if expected_stored_command not in stored_commands:
        raise Compact6Error("resume source journal command is invalid")
    expected_context = dict(journal_context or {})
    # Fail before writing a checkpoint if caller supplied a non-canonical value.
    canonical_json(expected_context)
    steps = tuple(steps)
    if [step.index for step in steps] != list(range(len(steps))):
        raise Compact6Error("migration steps must be contiguous and ordered")
    if (
        type(compensate_after_step) is not int
        or compensate_after_step < 0
        or compensate_after_step > len(steps) + 1
    ):
        raise Compact6Error("compensation boundary is invalid")

    def execute_compensation() -> None:
        if compensation_callback is not None:
            compensation_callback()
            return
        if not compensation_steps:
            raise Compact6Error("compensation plan is missing")
        for compensation in compensation_steps:
            compensation_rows = _execute(client, compensation.sql)
            if compensation.result_must_be_empty and compensation_rows:
                raise Compact6Error(f"compensation gate {compensation.name} failed")
            if (
                compensation.postcondition_sql is not None
                and not _postcondition_satisfied(compensation, client)
            ):
                raise Compact6Error(
                    f"compensation postcondition failed for {compensation.name}"
                )

    if journal_path.exists():
        journal = _read_journal(journal_path)
        if journal.get("plan_sha256") != plan_sha256:
            raise Compact6Error("journal plan SHA-256 mismatch")
        if journal.get("transition_id") != transition_id:
            raise Compact6Error("journal transition identity mismatch")
        stored_command = journal.get("command")
        if command == "resume":
            if stored_command != expected_stored_command:
                raise Compact6Error("resume journal command mismatch")
        elif stored_command != command:
            raise Compact6Error("journal command mismatch")
        if journal.get("context", {}) != expected_context:
            raise Compact6Error("journal context does not match operation evidence")
        _validate_journal_steps(journal, steps)
    else:
        recovered_journal = (
            checkpoint_sink.load(
                transition_id=transition_id,
                plan_sha256=plan_sha256,
                command=expected_stored_command,
            )
            if checkpoint_sink is not None
            else None
        )
        if recovered_journal is not None:
            if command != "resume":
                raise Compact6Error(
                    "Iceberg journal already exists; explicit resume is required"
                )
            journal = recovered_journal
            _write_journal(journal_path, journal)
            if journal.get("context", {}) != expected_context:
                raise Compact6Error(
                    "durable journal context does not match operation evidence"
                )
            _validate_journal_steps(journal, steps)
        else:
            if command == "resume":
                raise Compact6Error("resume requires an existing journal")
            journal = _journal_payload(
                {
                    "journal_version": JOURNAL_VERSION,
                    "transition_id": transition_id,
                    "command": command,
                    "plan_sha256": plan_sha256,
                    "status": "running",
                    "context": expected_context,
                    "started_step": None,
                    "completed_steps": [],
                }
            )
            _write_journal(journal_path, journal)
            if checkpoint_sink is not None:
                checkpoint_sink.record(journal)

    if journal.get("status") not in {
        "running",
        "interrupted",
        "compensating",
        "compensated",
        "complete",
    }:
        raise Compact6Error("journal status is invalid")
    if journal.get("status") == "compensating":
        if compensation_callback is None and not compensation_steps:
            raise Compact6Error(
                "compensating journal requires its manifest-bound recovery plan"
            )
        try:
            execute_compensation()
            journal = _journal_payload(
                {
                    **{
                        key: value
                        for key, value in journal.items()
                        if key != "journal_sha256"
                    },
                    "status": "compensated",
                    "started_step": None,
                    "completed_steps": [
                        record
                        for record in journal["completed_steps"]
                        if record["index"] < compensate_after_step
                    ],
                }
            )
            _write_journal(journal_path, journal)
            if checkpoint_sink is not None:
                checkpoint_sink.record(journal)
        except Exception as compensation_exc:
            raise Compact6Error(
                "automatic emergency-wrapper compensation failed"
            ) from compensation_exc

    start = len(journal["completed_steps"])
    pending_started = journal.get("started_step")
    for step in steps[start:]:
        destructive_effect_preexists = any(
            isinstance(record, Mapping)
            and type(record.get("index")) is int
            and record["index"] >= compensate_after_step
            for record in journal["completed_steps"]
        )
        statement_may_have_executed = False
        try:
            if before_step is not None:
                before_step(step)
            if pending_started is not None:
                if not isinstance(pending_started, Mapping) or (
                    pending_started.get("index") != step.index
                    or pending_started.get("name") != step.name
                    or pending_started.get("sql_sha256") != step.sql_sha256
                ):
                    raise Compact6Error(
                        "journal started-step identity does not match plan"
                    )
            # Persist intent first.  On restart an exact postcondition may seal a
            # DDL that succeeded before the completion checkpoint reached disk.
            journal = _journal_payload(
                {
                    **{
                        key: value
                        for key, value in journal.items()
                        if key != "journal_sha256"
                    },
                    "status": "running",
                    "started_step": {
                        "index": step.index,
                        "name": step.name,
                        "sql_sha256": step.sql_sha256,
                    },
                }
            )
            _write_journal(journal_path, journal)
            if checkpoint_sink is not None:
                checkpoint_sink.record(journal)
            # A started-step probe closes the SQL-success/checkpoint-loss
            # window.  Future public base drops are also safe to probe because
            # their information-schema predicate lets resume advance from
            # automatic emergency views.  Fresh CTAS/audit steps must execute:
            # their targets may not exist yet and some audit postconditions are
            # intentionally only one part of the full statement.
            may_precheck = pending_started is not None or step.name.startswith(
                "drop_legacy_main_"
            )
            recovered = may_precheck and _postcondition_satisfied(step, client)
            if recovered and step.index >= compensate_after_step:
                destructive_effect_preexists = True
            statement_rows: Sequence[Any] = ()
            if not recovered:
                if fail_before_execute_step == step.index:
                    raise InjectedFailure(
                        f"injected failure after intent before SQL for {step.name}"
                    )
                statement_may_have_executed = True
                statement_rows = _execute(client, step.sql)
                if step.result_must_be_empty and statement_rows:
                    raise Compact6Error(f"step {step.name} returned parity mismatches")
                if step.postcondition_sql is not None and not _postcondition_satisfied(
                    step, client
                ):
                    raise Compact6Error(f"step {step.name} postcondition did not hold")
            elif step.capture_result:
                statement_rows = _execute(client, step.sql)
            if fail_after_execute_step == step.index:
                raise InjectedFailure(
                    f"injected failure after SQL before checkpoint for {step.name}"
                )
            completed = [
                *journal["completed_steps"],
                {
                    "index": step.index,
                    "name": step.name,
                    "sql_sha256": step.sql_sha256,
                    "recovered_from_postcondition": recovered,
                    "result_row_count": len(statement_rows),
                    "result_sha256": canonical_sha256(statement_rows),
                },
            ]
            journal = _journal_payload(
                {
                    **{k: v for k, v in journal.items() if k != "journal_sha256"},
                    "started_step": None,
                    "completed_steps": completed,
                }
            )
            _write_journal(journal_path, journal)
            if checkpoint_sink is not None:
                checkpoint_sink.record(journal)
            if fail_after_step == step.index:
                raise InjectedFailure(f"injected failure after {step.name}")
            pending_started = None
        except Exception:
            try:
                journal = _journal_payload(
                    {
                        **{
                            key: value
                            for key, value in journal.items()
                            if key != "journal_sha256"
                        },
                        "status": "interrupted",
                    }
                )
                _write_journal(journal_path, journal)
                if checkpoint_sink is not None:
                    checkpoint_sink.record(journal)
            except Exception:
                # The original operation error remains primary; emergency
                # compensation below does not depend on a writable journal.
                pass
            should_compensate = (
                bool(compensation_callback is not None or compensation_steps)
                and step.index >= compensate_after_step
                and (destructive_effect_preexists or statement_may_have_executed)
            )
            if should_compensate:
                try:
                    # Persist the recovery intent before the first emergency
                    # mutation.  A crash anywhere in compensation can then
                    # safely rerun the idempotent, manifest-bound recovery and
                    # rewind the public-swap prefix.
                    journal = _journal_payload(
                        {
                            **{
                                key: value
                                for key, value in journal.items()
                                if key != "journal_sha256"
                            },
                            "status": "compensating",
                        }
                    )
                    _write_journal(journal_path, journal)
                    if checkpoint_sink is not None:
                        checkpoint_sink.record(journal)
                    execute_compensation()
                    if fail_after_compensation:
                        raise InjectedFailure(
                            "injected failure after emergency compensation "
                            "before rewind checkpoint"
                        )
                    journal = _journal_payload(
                        {
                            **{
                                key: value
                                for key, value in journal.items()
                                if key != "journal_sha256"
                            },
                            "status": "compensated",
                            "started_step": None,
                            "completed_steps": [
                                record
                                for record in journal["completed_steps"]
                                if record["index"] < compensate_after_step
                            ],
                        }
                    )
                    _write_journal(journal_path, journal)
                    if checkpoint_sink is not None:
                        checkpoint_sink.record(journal)
                except Exception as compensation_exc:
                    raise Compact6Error(
                        "automatic emergency-wrapper compensation failed"
                    ) from compensation_exc
            raise

    journal = _journal_payload(
        {
            **{k: v for k, v in journal.items() if k != "journal_sha256"},
            "status": "complete",
            "started_step": None,
        }
    )
    _write_journal(journal_path, journal)
    if checkpoint_sink is not None:
        checkpoint_sink.record(journal)
    return journal


def _validate_run_guard(
    raw: Mapping[str, Any],
    *,
    transition_id: str | None = None,
    plan_sha256: str | None = None,
    now: datetime | None = None,
) -> None:
    guard = dict(_require_mapping(raw, "run guard"))
    expected_columns = {
        "schema_version",
        "captured_at",
        "transition_id",
        "plan_sha256",
        "paused_dags",
        "all_paused",
        "active_runs",
        "evidence_sha256",
    }
    if set(guard) != expected_columns:
        raise Compact6Error("run guard evidence schema is invalid")
    if guard.get("schema_version") != RUN_GUARD_VERSION:
        raise Compact6Error("run guard evidence version is invalid")
    stored_hash = _require_sha(
        guard.get("evidence_sha256"), "run guard evidence_sha256"
    )
    if stored_hash != canonical_sha256(
        {key: value for key, value in guard.items() if key != "evidence_sha256"}
    ):
        raise Compact6Error("run guard evidence SHA-256 does not match")
    captured_at = _require_timestamp(guard.get("captured_at"), "run guard captured_at")
    checked_at = datetime.now(UTC) if now is None else _require_timestamp(now, "now")
    if captured_at > checked_at + RUN_GUARD_FUTURE_SKEW:
        raise Compact6Error("run guard evidence is from the future")
    if checked_at - captured_at > RUN_GUARD_MAX_AGE:
        raise Compact6Error("run guard evidence is stale")
    guard_transition = _require_text(
        guard.get("transition_id"), "run guard transition_id"
    )
    if transition_id is not None and guard_transition != transition_id:
        raise Compact6Error("run guard transition identity does not match")
    guard_plan_sha = guard.get("plan_sha256")
    if guard_plan_sha is not None:
        _require_sha(guard_plan_sha, "run guard plan_sha256")
    if plan_sha256 is not None and guard_plan_sha != plan_sha256:
        raise Compact6Error("run guard plan SHA-256 does not match")
    paused = _require_sequence(guard.get("paused_dags"), "paused_dags")
    if (
        any(type(dag_id) is not str or not dag_id for dag_id in paused)
        or len(paused) != len(set(paused))
        or set(paused) != set(REQUIRED_PAUSED_DAGS)
    ):
        raise Compact6Error("run guard must bind the exact paused DAG set")
    if guard.get("all_paused") is not True:
        raise Compact6Error("all ESPN/downstream DAGs must be paused")
    active = _require_sequence(guard.get("active_runs"), "active_runs")
    if any(not isinstance(row, Mapping) for row in active):
        raise Compact6Error("active run evidence rows must be objects")
    if active:
        raise Compact6Error("compact6 requires zero active runs")


def apply_with_guard(
    *,
    plan: Mapping[str, Any],
    steps: Sequence[MigrationStep],
    client: object,
    journal_path: Path,
    run_guard: Callable[[], Mapping[str, Any]],
    command: str | None = None,
    fail_after_step: int | None = None,
    fail_before_execute_step: int | None = None,
    fail_after_execute_step: int | None = None,
    fail_after_compensation: bool = False,
    compensation_steps: Sequence[MigrationStep] = (),
    checkpoint_sink: IcebergJournalSink | None = None,
    journal_context: Mapping[str, Any] | None = None,
    emergency_manifest: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    validate_plan(plan)
    resolved_command = (
        ("apply" if not journal_path.exists() else "resume")
        if command is None
        else command
    )
    if resolved_command not in {"apply", "resume"}:
        raise Compact6Error("guarded command must be apply or resume")
    if resolved_command == "apply" and journal_path.exists():
        raise Compact6Error("apply refuses an existing journal; use resume")
    _validate_run_guard(
        run_guard(),
        transition_id=plan["transition_id"],
        plan_sha256=plan["plan_sha256"],
    )

    def recheck(step: MigrationStep) -> None:
        if step.name.startswith(("drop_legacy_main_", "publish_", "remove_public_")):
            _validate_run_guard(
                run_guard(),
                transition_id=plan["transition_id"],
                plan_sha256=plan["plan_sha256"],
            )

    resolved_compensation = tuple(compensation_steps)
    swap_indexes = [
        step.index for step in steps if step.name.startswith("drop_legacy_main_")
    ]
    if swap_indexes and not resolved_compensation and emergency_manifest is None:
        raise Compact6Error(
            "public swap requires a manifest-bound emergency compensation plan"
        )
    compensation_callback = (
        None
        if resolved_compensation or not swap_indexes
        else lambda: recover_public_wrappers_to_emergency(
            plan, emergency_manifest, client
        )
    )
    compensate_after = min(swap_indexes) if swap_indexes else len(steps) + 1
    return run_journaled_steps(
        plan_sha256=plan["plan_sha256"],
        transition_id=plan["transition_id"],
        command=resolved_command,
        steps=steps,
        client=client,
        journal_path=journal_path,
        fail_after_step=fail_after_step,
        fail_before_execute_step=fail_before_execute_step,
        fail_after_execute_step=fail_after_execute_step,
        fail_after_compensation=fail_after_compensation,
        before_step=recheck,
        compensation_steps=resolved_compensation,
        compensation_callback=compensation_callback,
        compensate_after_step=compensate_after,
        checkpoint_sink=checkpoint_sink,
        resume_from_command="apply",
        journal_context=journal_context,
    )


def capture_latest_snapshot_id(client: object, qualified_table: str) -> int:
    """Read the exact current Iceberg ``main`` branch snapshot.

    ``$snapshots`` commit order is not a current-state contract when tags,
    branches, or WAP commits exist.  Every source and newly materialised target
    is therefore pinned through its single ``main`` branch reference.
    """

    parts = qualified_table.split(".")
    if len(parts) != 3:
        raise Compact6Error("snapshot table must be fully qualified")
    catalog, schema, table = parts
    _qualified(catalog, schema, table)
    rows = _execute(
        client,
        f'SELECT snapshot_id FROM {catalog}.{schema}."{table}$refs" '
        "WHERE name = 'main' AND type = 'BRANCH'",
    )
    if len(rows) != 1:
        raise Compact6Error("Iceberg main snapshot identity is missing or ambiguous")
    row = rows[0]
    value = row.get("snapshot_id") if isinstance(row, Mapping) else row[0]
    return _require_positive_int(value, "post-CTAS snapshot ID")


def capture_archive_snapshot_ids(
    client: object, *, catalog: str = "iceberg", internal_schema: str = INTERNAL_SCHEMA
) -> dict[str, int]:
    return {
        table: capture_latest_snapshot_id(
            client, _qualified(catalog, internal_schema, table)
        )
        for table in LEGACY_ARCHIVE_TABLES
    }


def load_catalog_archive_manifest(
    client: object, plan: Mapping[str, Any]
) -> dict[str, Any] | None:
    """Load and verify the one immutable catalog manifest for local recovery."""

    rows = _execute(
        client,
        f"SELECT {', '.join(ARCHIVE_MANIFEST_COLUMNS)} FROM "
        f"{_qualified(plan['catalog'], plan['internal_schema'], ARCHIVE_MANIFEST_TABLE)}",
    )
    if not rows:
        return None
    mapped = _mapping_rows(rows, ARCHIVE_MANIFEST_COLUMNS, field="archive manifest")
    if len(mapped) != 1:
        raise Compact6Error("global archive manifest is forked or ambiguous")
    manifest = mapped[0]
    manifest["captured_at"] = (
        _stored_utc_timestamp(manifest["captured_at"], "archive manifest captured_at")
        .isoformat()
        .replace("+00:00", "Z")
    )
    manifest["legacy_disposition_snapshot_id"] = _require_positive_int(
        manifest["legacy_disposition_snapshot_id"],
        "legacy disposition snapshot ID",
    )
    normalized = json.loads(canonical_json(manifest))
    _validate_archive_manifest_for_plan(normalized, plan)
    return normalized


def _mapping_rows(
    rows: Sequence[Any], columns: Sequence[str], *, field: str
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for raw in rows:
        if isinstance(raw, Mapping):
            values = [raw.get(column) for column in columns]
        elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
            values = list(raw)
        else:
            raise Compact6Error(f"{field} query row is malformed")
        if len(values) != len(columns):
            raise Compact6Error(f"{field} query row is malformed")
        output.append(dict(zip(columns, values)))
    return output


def _captured_metric_sql(
    relation: str,
    *,
    columns: Sequence[str],
    key_columns: Sequence[str],
    group_by_scope: bool = False,
) -> str:
    row_expr = (
        "json_format(CAST(ROW("
        + ", ".join(f'"{column}"' for column in columns)
        + ") AS JSON))"
    )
    row_hash = (
        "COALESCE(lower(to_hex(sha256(to_utf8(array_join(array_sort(array_agg("
        f"lower(to_hex(sha256(to_utf8({row_expr})))))), ''))))), "
        "lower(to_hex(sha256(to_utf8('')))))"
    )
    distinct = (
        "COUNT(DISTINCT ROW("
        + ", ".join(f'"{column}"' for column in key_columns)
        + "))"
    )
    if group_by_scope:
        return f"""SELECT league, CAST(season AS varchar) season,
       COUNT(*) row_count, {row_hash} row_hash,
       {distinct} distinct_key_count
FROM {relation}
GROUP BY league, CAST(season AS varchar)
ORDER BY league, CAST(season AS varchar)"""
    return f"""SELECT COUNT(*) row_count, {row_hash} row_hash,
       {distinct} distinct_key_count
FROM {relation}"""


def capture_plan_from_catalog(
    client: object,
    *,
    transition_id: str,
    registry_snapshot_uri: str,
    registry_snapshot_sha256: str,
    registry_signature: str,
    target_scope_ids: Sequence[str],
    created_at: datetime,
    catalog: str = "iceberg",
    bronze_schema: str = BRONZE_SCHEMA,
    internal_schema: str = INTERNAL_SCHEMA,
) -> dict[str, Any]:
    """Capture one non-mutating plan from exact legacy14 Iceberg snapshots.

    The caller supplies the separately verified frozen registry artifact.  All
    catalog evidence is queried with explicit snapshot IDs; writers must remain
    paused between this capture and ``apply_compaction``.
    """

    transition_id = _require_text(transition_id, "transition_id")
    registry_snapshot_uri = _require_text(
        registry_snapshot_uri, "registry_snapshot_uri"
    )
    registry_snapshot_sha256 = _require_sha(
        registry_snapshot_sha256, "registry_snapshot_sha256"
    )
    registry_signature = _require_sha(registry_signature, "registry_signature")
    created_at = _require_timestamp(created_at, "created_at")
    target_scopes = tuple(
        sorted(_require_text(value, "target scope") for value in target_scope_ids)
    )
    if len(target_scopes) != EXPECTED_NATIVE_SCOPE_COUNT or len(
        set(target_scopes)
    ) != len(target_scopes):
        raise Compact6Error("frozen registry must contain exact 181 unique scopes")
    if any(_SCOPE_RE.fullmatch(scope) is None for scope in target_scopes):
        raise Compact6Error("frozen registry contains an invalid scope")
    target_sha = canonical_sha256(target_scopes)

    _assert_catalog_layout(client, LEGACY14, catalog=catalog)

    legacy_tables: dict[str, dict[str, Any]] = {}
    legacy_snapshots: dict[str, int] = {}
    observed_pairs: dict[tuple[str | None, str | None], set[str]] = {}
    for entity in _ENTITY_NAMES:
        table = _SOURCE_BY_ENTITY[entity]
        qualified = _qualified(catalog, bronze_schema, table)
        snapshot_id = capture_latest_snapshot_id(client, qualified)
        legacy_snapshots[table] = snapshot_id
        column_rows = _mapping_rows(
            _execute(
                client,
                f"SELECT column_name, data_type FROM {catalog}.information_schema.columns "
                f"WHERE table_schema = {_sql_string(bronze_schema)} "
                f"AND table_name = {_sql_string(table)} ORDER BY ordinal_position",
            ),
            ("name", "type"),
            field=f"{table} columns",
        )
        if not column_rows:
            raise Compact6Error(f"{table} has no captured schema")
        column_names = [row["name"] for row in column_rows]
        if not set(_LEGACY_KEY_COLUMNS[entity]) <= set(column_names):
            raise Compact6Error(f"{table} is missing legacy key columns")
        pinned = f"{qualified} FOR VERSION AS OF {snapshot_id}"
        metric_rows = _mapping_rows(
            _execute(
                client,
                _captured_metric_sql(
                    pinned,
                    columns=column_names,
                    key_columns=_LEGACY_KEY_COLUMNS[entity],
                ),
            ),
            ("row_count", "row_hash", "distinct_key_count"),
            field=f"{table} whole metrics",
        )
        if len(metric_rows) != 1:
            raise Compact6Error(f"{table} whole metrics are incomplete")
        whole_metrics = _validate_metric(metric_rows[0], f"{table} whole metrics")
        scope_rows = _mapping_rows(
            _execute(
                client,
                _captured_metric_sql(
                    pinned,
                    columns=column_names,
                    key_columns=_LEGACY_KEY_COLUMNS[entity],
                    group_by_scope=True,
                ),
            ),
            ("league", "season", "row_count", "row_hash", "distinct_key_count"),
            field=f"{table} per-scope metrics",
        )
        per_scope: list[dict[str, Any]] = []
        for metrics in scope_rows:
            normalized_metric = _validate_metric(
                {
                    key: metrics[key]
                    for key in ("row_count", "row_hash", "distinct_key_count")
                },
                f"{table} per-scope metrics",
            )
            league = metrics["league"]
            season = metrics["season"]
            per_scope.append({"league": league, "season": season, **normalized_metric})
            observed_pairs.setdefault((league, season), set()).add(entity)
        legacy_tables[entity] = {
            "source_table": table,
            "snapshot_id": snapshot_id,
            "columns": column_rows,
            "whole_rowset_metrics": whole_metrics,
            "per_scope_metrics": per_scope,
        }

    archive_id = build_legacy_archive_id(legacy_tables)
    dispositions = build_dispositions(
        archive_id,
        [
            {
                "league": league,
                "season": season,
                "observed_entities": sorted(entities),
            }
            for (league, season), entities in observed_pairs.items()
        ],
    )

    state_relations = (*GENERATION_TABLES, CUTOVER_TABLE, BASELINE_TABLE)
    state_snapshots = {
        relation: capture_latest_snapshot_id(
            client, _qualified(catalog, bronze_schema, relation)
        )
        for relation in state_relations
    }
    manifest_snapshot_id = capture_latest_snapshot_id(
        client, _qualified(catalog, bronze_schema, MANIFEST_TABLE)
    )

    source_cutover_columns = (
        "scope_id",
        "cutover_id",
        "cutover_sha256",
        "active_source",
        "previous_source",
        "legacy_league",
        "legacy_season",
        "registry_signature",
        "native_generation_id",
        "native_generation_signature",
        "native_manifest_sha256",
    )
    source_cutover_heads = _mapping_rows(
        _execute(
            client,
            f"SELECT {', '.join(source_cutover_columns)} FROM "
            f"{_qualified(catalog, bronze_schema, CUTOVER_TABLE)} "
            f"FOR VERSION AS OF {state_snapshots[CUTOVER_TABLE]}",
        ),
        source_cutover_columns,
        field="source cutover heads",
    )
    if len(source_cutover_heads) != len(REVIEWED_NATIVE_REPLACEMENTS):
        raise Compact6Error("source cutover table must contain the reviewed six")

    route_columns = (
        "scope_id",
        "generation_id",
        "generation_signature",
        "manifest_sha256",
        "registry_snapshot_uri",
        "registry_signature",
        "parser_version",
        "runtime_version",
        "completed_at",
    )
    heads_sql = f"""WITH complete_manifests AS (
    SELECT *
    FROM {_qualified(catalog, bronze_schema, MANIFEST_TABLE)}
         FOR VERSION AS OF {manifest_snapshot_id}
    WHERE status = 'complete'
), conflicts AS (
    SELECT scope_id, generation_id
    FROM complete_manifests
    GROUP BY scope_id, generation_id
    HAVING COUNT(DISTINCT manifest_sha256) > 1
        OR COUNT(DISTINCT generation_signature) > 1
), ranked AS (
    SELECT complete_manifests.*, ROW_NUMBER() OVER (
        PARTITION BY complete_manifests.scope_id
        ORDER BY complete_manifests.completed_at DESC,
                 complete_manifests.generation_id DESC,
                 complete_manifests.manifest_sha256 DESC
    ) rn
    FROM complete_manifests
    LEFT JOIN conflicts
      ON conflicts.scope_id = complete_manifests.scope_id
     AND conflicts.generation_id = complete_manifests.generation_id
    WHERE conflicts.scope_id IS NULL
)
SELECT {", ".join(route_columns)}
FROM ranked
WHERE rn = 1
ORDER BY scope_id"""
    head_rows = _mapping_rows(
        _execute(client, heads_sql), route_columns, field="native manifest heads"
    )
    if len(head_rows) != EXPECTED_NATIVE_SCOPE_COUNT or {
        row["scope_id"] for row in head_rows
    } != set(target_scopes):
        raise Compact6Error(
            "latest native manifest heads do not match exact 181 targets"
        )
    replacements = {
        scope: (league, season)
        for scope, league, season in REVIEWED_NATIVE_REPLACEMENTS
    }
    native_routes: list[dict[str, Any]] = []
    for head in head_rows:
        scope = head["scope_id"]
        legacy = replacements.get(scope)
        if (
            head["registry_snapshot_uri"] != registry_snapshot_uri
            or head["registry_signature"] != registry_signature
            or (head["parser_version"], head["runtime_version"])
            != (PARSER_VERSION, RUNTIME_VERSION)
        ):
            raise Compact6Error("native manifest head registry/parser identity differs")
        native_routes.append(
            seal_native_route(
                {
                    "scope_id": scope,
                    "previous_source": "legacy" if legacy else "absent",
                    "legacy_league": legacy[0] if legacy else None,
                    "legacy_season": legacy[1] if legacy else None,
                    "generation_id": head["generation_id"],
                    "generation_signature": head["generation_signature"],
                    "manifest_sha256": head["manifest_sha256"],
                    "registry_signature": registry_signature,
                    "registry_snapshot_uri": registry_snapshot_uri,
                    "registry_snapshot_sha256": registry_snapshot_sha256,
                    "target_scope_sha256": target_sha,
                    "parser_version": PARSER_VERSION,
                    "runtime_version": RUNTIME_VERSION,
                    "route_action": "retain_existing" if legacy else "append_root",
                    "effective_at": created_at.isoformat(),
                }
            )
        )

    replacement_scope_values = ", ".join(
        _sql_string(scope) for scope in sorted(replacements)
    )
    baseline_rows = _mapping_rows(
        _execute(
            client,
            f"SELECT {', '.join(_BASELINE_COLUMNS)} FROM "
            f"{_qualified(catalog, bronze_schema, BASELINE_TABLE)} "
            f"FOR VERSION AS OF {state_snapshots[BASELINE_TABLE]} "
            f"WHERE scope_id IN ({replacement_scope_values})",
        ),
        _BASELINE_COLUMNS,
        field="legacy baselines",
    )
    if len(baseline_rows) != len(replacements) or {
        row["scope_id"] for row in baseline_rows
    } != set(replacements):
        raise Compact6Error("legacy baselines do not bind the exact reviewed six")
    baseline_hashes: dict[str, str] = {}
    baseline_evidence: dict[str, dict[str, Any]] = {}
    for row in baseline_rows:
        scope = row["scope_id"]
        baseline_hash = _require_sha(
            row["baseline_sha256"], f"baseline SHA-256 for {scope}"
        )
        snapshots = _canonical_json_field(
            row["legacy_snapshot_ids_json"],
            f"baseline snapshots for {scope}",
        )
        if snapshots != legacy_snapshots:
            raise Compact6Error("legacy baseline snapshots differ from global archive")
        baseline_hashes[scope] = baseline_hash
        row["captured_at"] = _stored_utc_timestamp(
            row["captured_at"], f"baseline captured_at {scope}"
        ).isoformat()
        baseline_evidence[scope] = row

    base = {
        "schema_version": PLAN_VERSION,
        "transition_id": transition_id,
        "created_at": created_at.isoformat(),
        "catalog": catalog,
        "bronze_schema": bronze_schema,
        "internal_schema": internal_schema,
        "source_layout": LEGACY14,
        "target_layout": COMPACT6,
        "legacy_tables": legacy_tables,
        "legacy_dispositions": dispositions,
        "native_replacements": [
            {
                "scope_id": scope,
                "legacy_league": league,
                "legacy_season": season,
            }
            for scope, league, season in REVIEWED_NATIVE_REPLACEMENTS
        ],
        "registry_signature": registry_signature,
        "registry_snapshot_uri": registry_snapshot_uri,
        "registry_snapshot_sha256": registry_snapshot_sha256,
        "native_scope_count": EXPECTED_NATIVE_SCOPE_COUNT,
        "target_scope_ids": list(target_scopes),
        "target_scope_sha256": target_sha,
        "native_routes": native_routes,
        "manifest_snapshot_id": manifest_snapshot_id,
        "source_cutover_row_count": len(source_cutover_heads),
        "source_cutover_heads": source_cutover_heads,
        "state_snapshots": state_snapshots,
        "baseline_sha256_by_scope": baseline_hashes,
        "baseline_evidence_by_scope": baseline_evidence,
    }
    sealed = seal_plan(base)
    validate_plan(sealed)
    return sealed


def _load_json(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Compact6Error(f"cannot read {path}") from exc
    return _require_mapping(value, str(path))


def _load_or_recover_manifest(
    path: Path,
    *,
    plan: Mapping[str, Any],
    client_factory: Callable[[], object],
) -> dict[str, Any]:
    if path.exists():
        manifest = dict(_load_json(path))
        _validate_archive_manifest_for_plan(manifest, plan)
        return manifest
    try:
        manifest = load_catalog_archive_manifest(client_factory(), plan)
    except Exception as exc:
        if getattr(exc, "error_name", None) == "TABLE_NOT_FOUND":
            manifest = None
        else:
            raise
    if manifest is None:
        raise Compact6Error("local and catalog archive manifests are missing")
    _write_json(path, manifest)
    return manifest


def _journal_payload_public_swap_started(journal: Mapping[str, Any]) -> bool:
    if journal.get("status") in {"compensating", "compensated"}:
        return True
    names = {
        record.get("name")
        for record in _require_sequence(
            journal.get("completed_steps"), "completed_steps"
        )
        if isinstance(record, Mapping)
    }
    started = journal.get("started_step")
    if isinstance(started, Mapping):
        names.add(started.get("name"))
    return any(
        isinstance(name, str) and name.startswith("drop_legacy_main_") for name in names
    )


def _journal_public_swap_started(path: Path) -> bool:
    return path.exists() and _journal_payload_public_swap_started(_read_journal(path))


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    _write_journal(path, value)


def _load_target_scope_ids(path: Path) -> tuple[str, ...]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Compact6Error(f"cannot read target scopes from {path}") from exc
    if isinstance(value, Mapping):
        value = value.get("target_scope_ids")
    sequence = _require_sequence(value, "target_scope_ids")
    return tuple(_require_text(item, "target scope ID") for item in sequence)


def _live_trino_client() -> object:
    from scrapers.base.trino_manager import TrinoTableManager

    return TrinoTableManager(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ["TRINO_PORT"]) if os.environ.get("TRINO_PORT") else None,
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog="iceberg",
    )


def _run_guard_reader(path: Path) -> Callable[[], Mapping[str, Any]]:
    def read() -> Mapping[str, Any]:
        return _load_json(path)

    return read


def rollback_with_guard(
    *,
    plan: Mapping[str, Any],
    manifest: Mapping[str, Any],
    client: object,
    journal_path: Path,
    run_guard: Callable[[], Mapping[str, Any]],
    reason: str,
    command: str | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    """Journal a logical rollback and compensate every failure to emergency views."""

    _validate_archive_manifest_for_plan(manifest, plan)
    resolved_command = (
        ("rollback" if not journal_path.exists() else "resume")
        if command is None
        else command
    )
    if resolved_command not in {"rollback", "resume"}:
        raise Compact6Error("rollback command is invalid")
    checkpoint_sink = IcebergJournalSink(
        client,
        catalog=plan["catalog"],
        internal_schema=plan["internal_schema"],
        clock=clock,
    )
    if resolved_command == "resume":
        checkpoint = (
            _read_journal(journal_path)
            if journal_path.exists()
            else checkpoint_sink.load(
                transition_id=plan["transition_id"],
                plan_sha256=plan["plan_sha256"],
                command="rollback",
            )
        )
        if checkpoint is None:
            raise Compact6Error("rollback resume requires a durable checkpoint")
        context = _require_mapping(checkpoint.get("context"), "rollback context")
        if set(context) != {"archive_manifest", "rollback_reason"}:
            raise Compact6Error("rollback journal context is incomplete")
        bound_manifest = dict(
            _require_mapping(context["archive_manifest"], "rollback manifest")
        )
        _validate_archive_manifest_for_plan(bound_manifest, plan)
        if canonical_json(bound_manifest) != canonical_json(manifest):
            raise Compact6Error("rollback journal binds another archive manifest")
        reason = _require_text(context["rollback_reason"], "rollback reason")
    else:
        reason = _require_text(reason, "rollback reason")
    journal_context = {
        "archive_manifest": json.loads(canonical_json(manifest)),
        "rollback_reason": reason,
    }
    _validate_run_guard(
        run_guard(),
        transition_id=plan["transition_id"],
        plan_sha256=plan["plan_sha256"],
    )
    _assert_catalog_layout(client, COMPACT6, catalog=plan["catalog"])
    steps = render_rollback_steps(plan, manifest, reason=reason)

    def recheck(_step: MigrationStep) -> None:
        _validate_run_guard(
            run_guard(),
            transition_id=plan["transition_id"],
            plan_sha256=plan["plan_sha256"],
        )

    return run_journaled_steps(
        plan_sha256=plan["plan_sha256"],
        transition_id=plan["transition_id"],
        command=resolved_command,
        steps=steps,
        client=client,
        journal_path=journal_path,
        before_step=recheck,
        compensation_steps=steps,
        compensate_after_step=0,
        checkpoint_sink=checkpoint_sink,
        resume_from_command="rollback",
        journal_context=journal_context,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("plan", "apply", "resume", "rollback", "repromote")
    )
    parser.add_argument(
        "--resume-operation",
        choices=("apply", "rollback", "repromote"),
        default="apply",
        help="durable operation journal selected by the resume command",
    )
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--journal", type=Path)
    parser.add_argument("--run-guard", type=Path)
    parser.add_argument("--reason")
    parser.add_argument("--render-sql", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--transition-id")
    parser.add_argument("--registry-snapshot-uri")
    parser.add_argument("--registry-snapshot-sha256")
    parser.add_argument("--registry-signature")
    parser.add_argument("--target-scopes", type=Path)
    parser.add_argument("--created-at")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    client_factory: Callable[[], object] | None = None,
) -> int:
    args = build_parser().parse_args(argv)
    if args.execute and args.render_sql:
        raise Compact6Error("--execute and --render-sql are mutually exclusive")
    mode = require_layout_mode()
    client_factory = client_factory or _live_trino_client
    if args.command == "plan":
        if mode != LEGACY14:
            raise Compact6Error("plan requires the legacy14 source layout")
        if args.capture:
            required = {
                "--transition-id": args.transition_id,
                "--registry-snapshot-uri": args.registry_snapshot_uri,
                "--registry-snapshot-sha256": args.registry_snapshot_sha256,
                "--registry-signature": args.registry_signature,
                "--target-scopes": args.target_scopes,
                "--run-guard": args.run_guard,
            }
            missing = [flag for flag, value in required.items() if value is None]
            if missing:
                raise Compact6Error(
                    "plan --capture requires " + ", ".join(sorted(missing))
                )
            run_guard = _run_guard_reader(args.run_guard)
            _validate_run_guard(run_guard(), transition_id=args.transition_id)
            created_at = (
                _require_timestamp(args.created_at, "created_at")
                if args.created_at
                else datetime.now(UTC)
            )
            plan = capture_plan_from_catalog(
                client_factory(),
                transition_id=args.transition_id,
                registry_snapshot_uri=args.registry_snapshot_uri,
                registry_snapshot_sha256=args.registry_snapshot_sha256,
                registry_signature=args.registry_signature,
                target_scope_ids=_load_target_scope_ids(args.target_scopes),
                created_at=created_at,
            )
            _write_json(args.plan, plan)
        else:
            plan = _load_json(args.plan)
            validate_plan(plan)
        print(canonical_json(plan))
        return 0
    plan = _load_json(args.plan)
    validate_plan(plan)
    if args.manifest is None or args.journal is None:
        raise Compact6Error(
            "apply/resume/rollback/repromote requires --manifest and --journal"
        )
    if args.command == "repromote" or (
        args.command == "resume" and args.resume_operation in {"rollback", "repromote"}
    ):
        operation = (
            "repromote" if args.command == "repromote" else args.resume_operation
        )
        if mode != COMPACT6:
            raise Compact6Error(f"{operation} requires compact6 layout mode")
        manifest = _load_or_recover_manifest(
            args.manifest, plan=plan, client_factory=client_factory
        )
        reason = args.reason or "operator-requested logical rollback"
        steps = (
            render_rollback_steps(plan, manifest, reason=reason)
            if operation == "rollback"
            else render_repromotion_steps(plan, manifest)
        )
        if args.render_sql:
            print(
                "\n\n".join(
                    f"-- {step.index}: {step.name}\n{step.sql};" for step in steps
                )
            )
            return 0
        if not args.execute:
            raise Compact6Error("resume requires --render-sql or --execute")
        if args.run_guard is None:
            raise Compact6Error("--execute requires --run-guard")
        client = client_factory()
        if operation == "rollback":
            result = rollback_with_guard(
                plan=plan,
                manifest=manifest,
                client=client,
                journal_path=args.journal,
                run_guard=_run_guard_reader(args.run_guard),
                reason=reason,
                command="resume",
            )
        else:
            result = repromote_with_guard(
                plan=plan,
                manifest=manifest,
                client=client,
                journal_path=args.journal,
                run_guard=_run_guard_reader(args.run_guard),
                command="resume" if args.command == "resume" else "repromote",
            )
        print(canonical_json(result))
        return 0
    if args.command in {"apply", "resume"}:
        if args.command == "apply" and mode != LEGACY14:
            raise Compact6Error("fresh apply requires legacy14 layout mode")
        if args.render_sql:
            if args.manifest.exists():
                manifest = _load_json(args.manifest)
                _validate_archive_manifest_for_plan(manifest, plan)
                steps = render_apply_steps(plan, manifest)
            else:
                steps = render_materialization_steps(plan)
        elif args.execute:
            if args.run_guard is None:
                raise Compact6Error("--execute requires --run-guard")
            client = client_factory()
            if args.command == "resume":
                durable_journal: Mapping[str, Any] | None = None
                if not args.journal.exists():
                    try:
                        durable_journal = IcebergJournalSink(
                            client,
                            catalog=plan["catalog"],
                            internal_schema=plan["internal_schema"],
                        ).load(
                            transition_id=plan["transition_id"],
                            plan_sha256=plan["plan_sha256"],
                            command="apply",
                        )
                    except Exception as exc:
                        if getattr(exc, "error_name", None) not in {
                            "TABLE_NOT_FOUND",
                            "SCHEMA_NOT_FOUND",
                        }:
                            raise
                swap_started = _journal_public_swap_started(args.journal) or (
                    durable_journal is not None
                    and _journal_payload_public_swap_started(durable_journal)
                )
                if not swap_started:
                    if mode != LEGACY14:
                        raise Compact6Error(
                            "pre-swap resume requires legacy14 layout mode"
                        )
                    _assert_catalog_layout(client, LEGACY14, catalog=plan["catalog"])
            result = apply_compaction(
                plan=plan,
                client=client,
                run_guard=_run_guard_reader(args.run_guard),
                journal_path=args.journal,
                manifest_path=args.manifest,
                command=args.command,
            )
            print(canonical_json(result))
            return 0
        else:
            raise Compact6Error("apply/resume requires --render-sql or --execute")
    else:
        if mode != COMPACT6:
            raise Compact6Error("rollback requires compact6")
        manifest = _load_or_recover_manifest(
            args.manifest, plan=plan, client_factory=client_factory
        )
        steps = render_rollback_steps(
            plan,
            manifest,
            reason=args.reason or "operator-requested logical rollback",
        )
        if args.execute:
            if args.run_guard is None:
                raise Compact6Error("--execute requires --run-guard")
            result = rollback_with_guard(
                plan=plan,
                manifest=manifest,
                client=client_factory(),
                journal_path=args.journal,
                run_guard=_run_guard_reader(args.run_guard),
                reason=args.reason or "operator-requested logical rollback",
            )
            print(canonical_json(result))
            return 0
        if not args.render_sql:
            raise Compact6Error("rollback requires --render-sql or --execute")
    if args.render_sql:
        print(
            "\n\n".join(f"-- {step.index}: {step.name}\n{step.sql};" for step in steps)
        )
        return 0
    raise AssertionError("unreachable compact6 CLI branch")


if __name__ == "__main__":  # pragma: no cover - CLI boundary
    raise SystemExit(main())


__all__ = [
    "Compact6Error",
    "InjectedFailure",
    "IcebergJournalSink",
    "MigrationStep",
    "apply_compaction",
    "apply_with_guard",
    "build_archive_manifest",
    "build_dispositions",
    "build_legacy_archive_id",
    "capture_archive_snapshot_ids",
    "capture_latest_snapshot_id",
    "capture_plan_from_catalog",
    "render_apply_steps",
    "render_materialization_steps",
    "render_precommit_validation_steps",
    "render_disposition_persistence_steps",
    "render_repromotion_steps",
    "render_rollback_steps",
    "recover_public_wrappers_to_emergency",
    "repromote_with_guard",
    "rollback_with_guard",
    "run_journaled_steps",
    "seal_native_route",
    "seal_plan",
    "seal_run_guard",
    "validate_archive_manifest",
    "validate_plan",
]
