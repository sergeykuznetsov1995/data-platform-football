"""Authoritative ESPN Bronze layout and catalog-inventory contract.

``legacy14`` is the pre-compaction topology.  ``compact6`` exposes only the
three canonical serving views and the three writer-facing control tables in
``iceberg.bronze``; immutable generations and recovery state live in
``iceberg.espn_internal``.  Writers must validate both the configured value and
the observed catalog before emitting any DDL or data.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import os
import re
from types import MappingProxyType
from typing import Any


LAYOUT_MODE_ENV = "ESPN_BRONZE_LAYOUT_MODE"
LEGACY14 = "legacy14"
COMPACT6 = "compact6"
BRONZE_SCHEMA = "bronze"
INTERNAL_SCHEMA = "espn_internal"

LEGACY_TABLES = (
    "espn_schedule",
    "espn_lineup",
    "espn_matchsheet",
)
GENERATION_TABLES = (
    "espn_schedule_generation_v2",
    "espn_lineup_generation_v2",
    "espn_matchsheet_generation_v2",
)
CURRENT_VIEWS = (
    "espn_schedule_current",
    "espn_lineup_current",
    "espn_matchsheet_current",
)
PUBLIC_CONTROL_TABLES = (
    "espn_ingest_manifest_v2",
    "espn_request_ledger_generation_v2",
    "espn_catalog_snapshot_v2",
)

# Exact downstream-enabled mappings frozen in configs/espn/season_mapping.yaml.
# Compact6 may publish native data for all 181 Bronze scopes, but only these six
# replace a legacy (league, season) pair.
REVIEWED_NATIVE_REPLACEMENTS = (
    ("606:2026", "INT-World Cup", "2026"),
    ("700:2026", "ENG-Premier League", "2627"),
    ("710:2026", "FRA-Ligue 1", "2627"),
    ("720:2026", "GER-Bundesliga", "2627"),
    ("730:2026", "ITA-Serie A", "2627"),
    ("740:2026", "ESP-La Liga", "2627"),
)
INTERNAL_CONTROL_TABLES = (
    "espn_scope_cutover_v2",
    "espn_legacy_baseline_v2",
)

ARCHIVE_MANIFEST_TABLE = "espn_legacy_archive_manifest_v1"
LEGACY_DISPOSITION_TABLE = "espn_legacy_disposition_v1"
LAYOUT_STATE_TABLE = "espn_layout_state_v2"
JOURNAL_TABLE = "espn_compact6_journal_v2"
LAYOUT_STATE_VERSION = "espn-layout-state-v2"
LAYOUT_STATE_COLUMNS = (
    "layout_version",
    "layout_mode",
    "archive_id",
    "transition_id",
    "effective_at",
    "plan_sha256",
    "archive_manifest_sha256",
    "state_sha256",
)
JOURNAL_COLUMNS = (
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
ARCHIVE_MANIFEST_VERSION = "espn-legacy-archive-manifest-v1"
ARCHIVE_MANIFEST_COLUMNS = (
    "manifest_version",
    "archive_id",
    "captured_at",
    "registry_signature",
    "legacy_snapshot_ids_json",
    "archive_snapshot_ids_json",
    "whole_rowset_metrics_json",
    "legacy_disposition_snapshot_id",
    "legacy_disposition_metrics_json",
    "legacy_dispositions_json",
    "native_replacements_json",
    "plan_sha256",
    "manifest_sha256",
)
LEGACY_DISPOSITION_COLUMNS = (
    "archive_id",
    "league",
    "season",
    "disposition",
    "replacement_scope_id",
    "observed_entities_json",
    "disposition_sha256",
)
LEGACY_ARCHIVE_TABLES = tuple(
    f"espn_{entity}_legacy_archive_v1"
    for entity in ("schedule", "lineup", "matchsheet")
)
RETAINED_LEGACY_MAIN_TABLES = tuple(
    f"espn_{entity}_legacy_main_retained_v1"
    for entity in ("schedule", "lineup", "matchsheet")
)
COMPACT_SHADOW_TABLES = tuple(
    f"espn_{entity}_compact6_shadow_v1"
    for entity in ("schedule", "lineup", "matchsheet")
)
EMERGENCY_LEGACY_VIEWS = tuple(
    f"espn_{entity}_emergency_legacy_v1"
    for entity in ("schedule", "lineup", "matchsheet")
)


class LayoutError(RuntimeError):
    """The requested layout is absent, unknown, or differs from the catalog."""


def require_layout_mode(
    value: object | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> str:
    """Return one exact layout value; never infer a production default."""

    if value is None:
        source = os.environ if environ is None else environ
        value = source.get(LAYOUT_MODE_ENV)
    if not isinstance(value, str) or not value.strip():
        raise LayoutError(f"{LAYOUT_MODE_ENV} is required")
    normalized = value.strip()
    if normalized not in {LEGACY14, COMPACT6}:
        raise LayoutError(
            f"{LAYOUT_MODE_ENV} must be legacy14 or compact6, got {normalized!r}"
        )
    return normalized


LEGACY14_PUBLIC_OBJECTS = MappingProxyType(
    {
        **{name: "BASE TABLE" for name in LEGACY_TABLES},
        **{name: "BASE TABLE" for name in GENERATION_TABLES},
        **{name: "VIEW" for name in CURRENT_VIEWS},
        **{name: "BASE TABLE" for name in PUBLIC_CONTROL_TABLES},
        **{name: "BASE TABLE" for name in INTERNAL_CONTROL_TABLES},
    }
)
COMPACT6_PUBLIC_OBJECTS = MappingProxyType(
    {
        **{name: "VIEW" for name in LEGACY_TABLES},
        **{name: "BASE TABLE" for name in PUBLIC_CONTROL_TABLES},
    }
)
COMPACT6_INTERNAL_REQUIRED_OBJECTS = MappingProxyType(
    {
        **{name: "BASE TABLE" for name in GENERATION_TABLES},
        **{name: "BASE TABLE" for name in INTERNAL_CONTROL_TABLES},
        **{name: "VIEW" for name in CURRENT_VIEWS},
        **{name: "BASE TABLE" for name in LEGACY_ARCHIVE_TABLES},
        **{name: "BASE TABLE" for name in RETAINED_LEGACY_MAIN_TABLES},
        **{name: "BASE TABLE" for name in COMPACT_SHADOW_TABLES},
        **{name: "VIEW" for name in EMERGENCY_LEGACY_VIEWS},
        ARCHIVE_MANIFEST_TABLE: "BASE TABLE",
        LEGACY_DISPOSITION_TABLE: "BASE TABLE",
        LAYOUT_STATE_TABLE: "BASE TABLE",
        JOURNAL_TABLE: "BASE TABLE",
    }
)

_SAFE_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")
_KNOWN_RELATIONS = frozenset(
    {
        *LEGACY_TABLES,
        *GENERATION_TABLES,
        *CURRENT_VIEWS,
        *PUBLIC_CONTROL_TABLES,
        *INTERNAL_CONTROL_TABLES,
        *COMPACT6_INTERNAL_REQUIRED_OBJECTS,
    }
)


def relation_location(name: str, layout_mode: object) -> tuple[str, str]:
    """Resolve one logical repository relation to its physical schema/name."""

    if not isinstance(name, str) or name not in _KNOWN_RELATIONS:
        raise LayoutError(f"unknown ESPN relation {name!r}")
    mode = require_layout_mode(layout_mode)
    if mode == LEGACY14:
        if name not in LEGACY14_PUBLIC_OBJECTS:
            raise LayoutError(f"{name} does not exist in legacy14")
        return BRONZE_SCHEMA, name
    if name in COMPACT6_PUBLIC_OBJECTS:
        return BRONZE_SCHEMA, name
    return INTERNAL_SCHEMA, name


def qualified_relation(name: str, layout_mode: object, *, catalog: str) -> str:
    if not isinstance(catalog, str) or _SAFE_IDENTIFIER.fullmatch(catalog) is None:
        raise LayoutError("catalog must be a safe lower-case identifier")
    schema, physical_name = relation_location(name, layout_mode)
    return f"{catalog}.{schema}.{physical_name}"


def _normalize_kind(value: object) -> str:
    kind = str(value).strip().upper()
    if kind == "TABLE":
        kind = "BASE TABLE"
    if kind not in {"BASE TABLE", "VIEW"}:
        raise LayoutError(f"unsupported ESPN catalog object kind {value!r}")
    return kind


def _inventory_row(raw: object) -> tuple[str, str, str]:
    if isinstance(raw, Mapping):
        values = (
            raw.get("table_schema"),
            raw.get("table_name"),
            raw.get("table_type"),
        )
    elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        values = tuple(raw)
    else:
        raise LayoutError("ESPN catalog inventory row is malformed")
    if len(values) != 3:
        raise LayoutError("ESPN catalog inventory row is malformed")
    schema, name, kind = values
    if (
        not isinstance(schema, str)
        or schema not in {BRONZE_SCHEMA, INTERNAL_SCHEMA}
        or not isinstance(name, str)
        or _SAFE_IDENTIFIER.fullmatch(name) is None
    ):
        raise LayoutError("ESPN catalog inventory identity is malformed")
    return schema, name, _normalize_kind(kind)


def validate_catalog_layout(
    layout_mode: object,
    rows: Sequence[object],
) -> dict[str, Any]:
    """Validate exact public inventory and all compact6 recovery objects."""

    mode = require_layout_mode(layout_mode)
    observed: dict[tuple[str, str], str] = {}
    for raw in rows:
        schema, name, kind = _inventory_row(raw)
        key = (schema, name)
        if key in observed:
            raise LayoutError(
                f"duplicate ESPN catalog inventory object {schema}.{name}"
            )
        observed[key] = kind

    expected_public = (
        LEGACY14_PUBLIC_OBJECTS if mode == LEGACY14 else COMPACT6_PUBLIC_OBJECTS
    )
    public = {
        name: kind
        for (schema, name), kind in observed.items()
        if schema == BRONZE_SCHEMA and name.startswith("espn_")
    }
    missing = sorted(set(expected_public) - set(public))
    unexpected = sorted(set(public) - set(expected_public))
    wrong_kind = sorted(
        name
        for name in set(public) & set(expected_public)
        if public[name] != expected_public[name]
    )
    if wrong_kind:
        raise LayoutError(
            f"catalog does not match {mode}; kind mismatch for: "
            + ", ".join(wrong_kind)
        )
    if unexpected:
        raise LayoutError(
            f"unexpected public ESPN objects for {mode}: {', '.join(unexpected)}"
        )
    if missing:
        raise LayoutError(
            f"catalog does not match {mode}; missing public objects: "
            + ", ".join(missing)
        )

    internal_count = 0
    if mode == COMPACT6:
        internal = {
            name: kind
            for (schema, name), kind in observed.items()
            if schema == INTERNAL_SCHEMA and name.startswith("espn_")
        }
        missing_internal = sorted(
            set(COMPACT6_INTERNAL_REQUIRED_OBJECTS) - set(internal)
        )
        unexpected_internal = sorted(
            set(internal) - set(COMPACT6_INTERNAL_REQUIRED_OBJECTS)
        )
        wrong_internal = sorted(
            name
            for name in set(internal) & set(COMPACT6_INTERNAL_REQUIRED_OBJECTS)
            if internal[name] != COMPACT6_INTERNAL_REQUIRED_OBJECTS[name]
        )
        if wrong_internal:
            raise LayoutError(
                "catalog compact6 internal kind mismatch for: "
                + ", ".join(wrong_internal)
            )
        if unexpected_internal:
            raise LayoutError(
                "unexpected internal ESPN objects for compact6: "
                + ", ".join(unexpected_internal)
            )
        if missing_internal:
            raise LayoutError(
                "catalog does not match compact6; missing internal objects: "
                + ", ".join(missing_internal)
            )
        internal_count = len(internal)
    return {
        "layout_mode": mode,
        "public_object_count": len(public),
        "internal_object_count": internal_count,
    }


def catalog_inventory_sql(*, catalog: str = "iceberg") -> str:
    if not isinstance(catalog, str) or _SAFE_IDENTIFIER.fullmatch(catalog) is None:
        raise LayoutError("catalog must be a safe lower-case identifier")
    return f"""SELECT table_schema, table_name, table_type
FROM {catalog}.information_schema.tables
WHERE table_schema IN ('{BRONZE_SCHEMA}', '{INTERNAL_SCHEMA}')
  AND table_name LIKE 'espn\\_%' ESCAPE '\\'
ORDER BY table_schema, table_name"""


def validate_query_catalog_layout(
    query: object,
    layout_mode: object,
    *,
    catalog: str = "iceberg",
) -> dict[str, Any]:
    execute = getattr(query, "execute_query", None)
    if not callable(execute):
        raise TypeError("query adapter must expose execute_query")
    rows = execute(catalog_inventory_sql(catalog=catalog), params=()) or []
    return validate_catalog_layout(layout_mode, rows)


__all__ = [
    "ARCHIVE_MANIFEST_TABLE",
    "ARCHIVE_MANIFEST_COLUMNS",
    "ARCHIVE_MANIFEST_VERSION",
    "BRONZE_SCHEMA",
    "COMPACT6",
    "COMPACT6_INTERNAL_REQUIRED_OBJECTS",
    "COMPACT6_PUBLIC_OBJECTS",
    "COMPACT_SHADOW_TABLES",
    "CURRENT_VIEWS",
    "EMERGENCY_LEGACY_VIEWS",
    "GENERATION_TABLES",
    "INTERNAL_CONTROL_TABLES",
    "INTERNAL_SCHEMA",
    "JOURNAL_TABLE",
    "JOURNAL_COLUMNS",
    "LAYOUT_MODE_ENV",
    "LAYOUT_STATE_TABLE",
    "LAYOUT_STATE_COLUMNS",
    "LAYOUT_STATE_VERSION",
    "LEGACY14",
    "LEGACY14_PUBLIC_OBJECTS",
    "LEGACY_ARCHIVE_TABLES",
    "LEGACY_DISPOSITION_TABLE",
    "LEGACY_DISPOSITION_COLUMNS",
    "LEGACY_TABLES",
    "LayoutError",
    "PUBLIC_CONTROL_TABLES",
    "REVIEWED_NATIVE_REPLACEMENTS",
    "RETAINED_LEGACY_MAIN_TABLES",
    "catalog_inventory_sql",
    "qualified_relation",
    "relation_location",
    "require_layout_mode",
    "validate_catalog_layout",
    "validate_query_catalog_layout",
]
