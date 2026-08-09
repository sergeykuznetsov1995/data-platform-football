"""Dependency-free ESPN Native Bronze v2 object inventory.

This module is intentionally safe to load from operational audit images.  It
contains no Airflow, pandas, Trino, network, or scraper imports.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Iterable


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

CONTROL_TABLES = (
    "espn_catalog_snapshot_v2",
    "espn_request_ledger_generation_v2",
    "espn_ingest_manifest_v2",
    "espn_scope_cutover_v2",
    "espn_legacy_baseline_v2",
)

LAYOUT_MODES = frozenset({"legacy14", "compact6"})
BRONZE_SCHEMA = "bronze"
INTERNAL_SCHEMA = "espn_internal"
BASE_TABLE = "BASE TABLE"
VIEW = "VIEW"

_LEGACY_COMMON = frozenset({"league", "season", "game", "_batch_id"})
_NATIVE_COMMON = frozenset(
    {
        "scope_id",
        "competition_id",
        "source_season_year",
        "event_id",
        "generation_id",
        "generation_signature",
        "run_id",
        "registry_snapshot_uri",
        "registry_signature",
        "plan_signature",
        "raw_uri",
        "raw_sha256",
        "parser_version",
        "runtime_version",
        "_source_fetched_at",
        "_ingested_at",
        "_batch_id",
        "_source",
        "_entity_type",
        "_row_sha256",
    }
)

_SCHEDULE = _NATIVE_COMMON | {
    "competition_slug",
    "kickoff",
    "status",
    "terminal",
    "home_team_id",
    "away_team_id",
    "home_team",
    "away_team",
    "game_id",
    "match_date",
}
_LINEUP = _NATIVE_COMMON | {
    "team_id",
    "athlete_id",
    "home_away",
    "is_home",
    "team",
    "player",
}
_MATCHSHEET = _NATIVE_COMMON | {
    "team_id",
    "home_away",
    "is_home",
    "team",
    "statistics_json",
}

REQUIRED_COLUMNS = MappingProxyType(
    {
        "espn_schedule": _LEGACY_COMMON
        | {"match_date", "home_team", "away_team", "game_id", "league_id"},
        "espn_lineup": _LEGACY_COMMON
        | {"team", "player", "position", "formation_place", "sub_in", "sub_out"},
        "espn_matchsheet": _LEGACY_COMMON | {"team", "is_home", "venue", "attendance"},
        "espn_schedule_generation_v2": _SCHEDULE,
        "espn_lineup_generation_v2": _LINEUP,
        "espn_matchsheet_generation_v2": _MATCHSHEET,
        "espn_schedule_current": _SCHEDULE,
        "espn_lineup_current": _LINEUP,
        "espn_matchsheet_current": _MATCHSHEET,
        "espn_catalog_snapshot_v2": frozenset(
            {
                "snapshot_id",
                "snapshot_signature",
                "registry_signature",
                "competition_id",
                "competition_slug",
                "record_sha256",
                "raw_uri",
                "raw_sha256",
                "_batch_id",
            }
        ),
        "espn_request_ledger_generation_v2": frozenset(
            {
                "scope_id",
                "competition_id",
                "source_season_year",
                "generation_id",
                "generation_signature",
                "run_id",
                "registry_signature",
                "plan_signature",
                "request_id",
                "endpoint",
                "disposition",
                "raw_uri",
                "raw_sha256",
                "direct_bytes",
                "proxy_bytes",
                "_row_sha256",
            }
        ),
        "espn_ingest_manifest_v2": frozenset(
            {
                "scope_id",
                "competition_id",
                "source_season_year",
                "run_id",
                "generation_id",
                "generation_signature",
                "registry_signature",
                "plan_signature",
                "status",
                "row_counts_json",
                "row_hashes_json",
                "ledger_count",
                "ledger_hash",
                "quality_json",
                "manifest_sha256",
            }
        ),
        "espn_scope_cutover_v2": frozenset(
            {
                "cutover_id",
                "scope_id",
                "active_source",
                "previous_source",
                "legacy_league",
                "legacy_season",
                "registry_signature",
                "native_generation_id",
                "native_generation_signature",
                "native_manifest_sha256",
                "ancestor_lineage_sha256",
                "cutover_sha256",
            }
        ),
        "espn_legacy_baseline_v2": frozenset(
            {
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
            }
        ),
    }
)


class ObjectInventoryError(ValueError):
    """The observed ESPN topology differs from its reviewed layout contract."""


def _identifier(value: object, field: str) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.lower()
        or not all(
            character.islower() or character.isdigit() or character == "_"
            for character in value
        )
    ):
        raise ValueError(f"{field} must be a lower-case SQL identifier")
    return value


def _columns(value: Iterable[str], field: str) -> frozenset[str]:
    if isinstance(value, (str, bytes)):
        raise TypeError(f"{field} must be an iterable of column names")
    try:
        result = frozenset(value)
    except TypeError as exc:
        raise TypeError(f"{field} must be an iterable of column names") from exc
    if not result:
        raise ValueError(f"{field} must not be empty")
    for column in result:
        _identifier(column, field)
    return result


@dataclass(frozen=True, slots=True)
class RelationContract:
    """One expected relation in the mandatory ESPN topology."""

    schema: str
    name: str
    kind: str
    required_columns: frozenset[str]

    def __post_init__(self) -> None:
        _identifier(self.schema, "relation schema")
        _identifier(self.name, "relation name")
        if self.kind not in {BASE_TABLE, VIEW}:
            raise ValueError("relation kind must be BASE TABLE or VIEW")
        object.__setattr__(
            self,
            "required_columns",
            _columns(self.required_columns, "relation required_columns"),
        )


@dataclass(frozen=True, slots=True)
class RelationInventory:
    """One observed relation, normalized from an information-schema audit."""

    schema: str
    name: str
    kind: str
    columns: frozenset[str]

    def __post_init__(self) -> None:
        _identifier(self.schema, "inventory schema")
        _identifier(self.name, "inventory name")
        if type(self.kind) is not str or not self.kind:
            raise ValueError("inventory kind must be a non-empty string")
        object.__setattr__(
            self, "columns", _columns(self.columns, "inventory columns")
        )


def _relation(
    schema: str, name: str, kind: str, required_columns: Iterable[str]
) -> RelationContract:
    return RelationContract(
        schema=schema,
        name=name,
        kind=kind,
        required_columns=frozenset(required_columns),
    )


_ARCHIVE_MANIFEST_COLUMNS = frozenset(
    {
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
    }
)
LEGACY_DISPOSITION_VALUES = frozenset(
    {"compatibility_only", "native_current_replaced", "quarantined"}
)
_DISPOSITION_COLUMNS = frozenset(
    {
        "archive_id",
        "league",
        "season",
        "disposition",
        "replacement_scope_id",
        "observed_entities_json",
        "disposition_sha256",
    }
)
_LAYOUT_STATE_COLUMNS = frozenset(
    {
        "layout_version",
        "layout_mode",
        "archive_id",
        "transition_id",
        "effective_at",
        "plan_sha256",
        "archive_manifest_sha256",
        "state_sha256",
    }
)
_JOURNAL_COLUMNS = frozenset(
    {
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
    }
)


LEGACY14_PUBLIC_RELATIONS = (
    *(
        _relation(BRONZE_SCHEMA, table, BASE_TABLE, REQUIRED_COLUMNS[table])
        for table in LEGACY_TABLES
    ),
    *(
        _relation(BRONZE_SCHEMA, table, BASE_TABLE, REQUIRED_COLUMNS[table])
        for table in GENERATION_TABLES
    ),
    *(
        _relation(BRONZE_SCHEMA, table, VIEW, REQUIRED_COLUMNS[table])
        for table in CURRENT_VIEWS
    ),
    *(
        _relation(BRONZE_SCHEMA, table, BASE_TABLE, REQUIRED_COLUMNS[table])
        for table in CONTROL_TABLES
    ),
)

COMPACT6_PUBLIC_RELATIONS = (
    *(
        _relation(
            BRONZE_SCHEMA,
            f"espn_{entity}",
            VIEW,
            REQUIRED_COLUMNS[f"espn_{entity}_current"],
        )
        for entity in ("schedule", "lineup", "matchsheet")
    ),
    *(
        _relation(BRONZE_SCHEMA, table, BASE_TABLE, REQUIRED_COLUMNS[table])
        for table in (
            "espn_ingest_manifest_v2",
            "espn_request_ledger_generation_v2",
            "espn_catalog_snapshot_v2",
        )
    ),
)

COMPACT6_INTERNAL_RELATIONS = (
    *(
        _relation(INTERNAL_SCHEMA, table, BASE_TABLE, REQUIRED_COLUMNS[table])
        for table in GENERATION_TABLES
    ),
    _relation(
        INTERNAL_SCHEMA,
        "espn_scope_cutover_v2",
        BASE_TABLE,
        REQUIRED_COLUMNS["espn_scope_cutover_v2"],
    ),
    _relation(
        INTERNAL_SCHEMA,
        "espn_legacy_baseline_v2",
        BASE_TABLE,
        REQUIRED_COLUMNS["espn_legacy_baseline_v2"],
    ),
    _relation(
        INTERNAL_SCHEMA,
        "espn_legacy_archive_manifest_v1",
        BASE_TABLE,
        _ARCHIVE_MANIFEST_COLUMNS,
    ),
    _relation(
        INTERNAL_SCHEMA,
        "espn_legacy_disposition_v1",
        BASE_TABLE,
        _DISPOSITION_COLUMNS,
    ),
    *(
        _relation(
            INTERNAL_SCHEMA,
            f"espn_{entity}_legacy_archive_v1",
            BASE_TABLE,
            REQUIRED_COLUMNS[f"espn_{entity}"],
        )
        for entity in ("schedule", "lineup", "matchsheet")
    ),
    *(
        _relation(
            INTERNAL_SCHEMA,
            f"espn_{entity}_legacy_main_retained_v1",
            BASE_TABLE,
            REQUIRED_COLUMNS[f"espn_{entity}"],
        )
        for entity in ("schedule", "lineup", "matchsheet")
    ),
    *(
        _relation(
            INTERNAL_SCHEMA,
            f"espn_{entity}_compact6_shadow_v1",
            BASE_TABLE,
            REQUIRED_COLUMNS[f"espn_{entity}_current"],
        )
        for entity in ("schedule", "lineup", "matchsheet")
    ),
    *(
        _relation(
            INTERNAL_SCHEMA,
            table,
            VIEW,
            REQUIRED_COLUMNS[table],
        )
        for table in CURRENT_VIEWS
    ),
    *(
        _relation(
            INTERNAL_SCHEMA,
            f"espn_{entity}_emergency_legacy_v1",
            VIEW,
            REQUIRED_COLUMNS[f"espn_{entity}_current"],
        )
        for entity in ("schedule", "lineup", "matchsheet")
    ),
    _relation(
        INTERNAL_SCHEMA,
        "espn_layout_state_v2",
        BASE_TABLE,
        _LAYOUT_STATE_COLUMNS,
    ),
    _relation(
        INTERNAL_SCHEMA,
        "espn_compact6_journal_v2",
        BASE_TABLE,
        _JOURNAL_COLUMNS,
    ),
)

PUBLIC_RELATIONS_BY_LAYOUT = MappingProxyType(
    {
        "legacy14": LEGACY14_PUBLIC_RELATIONS,
        "compact6": COMPACT6_PUBLIC_RELATIONS,
    }
)


def required_layout_relations(layout_mode: str) -> tuple[RelationContract, ...]:
    """Return the exact ESPN relations allowed for a completed layout mode."""

    if type(layout_mode) is not str or layout_mode not in LAYOUT_MODES:
        raise ObjectInventoryError("layout mode must be legacy14 or compact6")
    public = PUBLIC_RELATIONS_BY_LAYOUT[layout_mode]
    internal = COMPACT6_INTERNAL_RELATIONS if layout_mode == "compact6" else ()
    return (*public, *internal)


def audit_layout_inventory(
    layout_mode: str, relations: Iterable[RelationInventory]
) -> None:
    """Fail closed unless the supplied ESPN inventory exactly matches a layout.

    Callers must pass the complete ESPN-only relation inventory from both
    ``iceberg.bronze`` and ``iceberg.espn_internal``.  Other source relations
    are intentionally outside this source-specific contract.
    """

    expected = {
        (relation.schema, relation.name): relation
        for relation in required_layout_relations(layout_mode)
    }
    observed: dict[tuple[str, str], RelationInventory] = {}
    for relation in relations:
        if not isinstance(relation, RelationInventory):
            raise ObjectInventoryError("inventory contains an invalid relation record")
        key = (relation.schema, relation.name)
        if key in observed:
            raise ObjectInventoryError(
                "inventory contains duplicate relation "
                f"{relation.schema}.{relation.name}"
            )
        observed[key] = relation

    missing = sorted(set(expected) - set(observed))
    if missing:
        raise ObjectInventoryError(
            f"inventory is missing required relations: {missing}"
        )
    unexpected = sorted(set(observed) - set(expected))
    if unexpected:
        raise ObjectInventoryError(f"inventory has unexpected relations: {unexpected}")
    for key in sorted(expected):
        required = expected[key]
        actual = observed[key]
        if actual.kind != required.kind:
            raise ObjectInventoryError(
                f"inventory kind mismatch for {actual.schema}.{actual.name}: "
                f"expected {required.kind}, got {actual.kind}"
            )
        missing_columns = sorted(required.required_columns - actual.columns)
        if missing_columns:
            raise ObjectInventoryError(
                f"inventory columns missing for {actual.schema}.{actual.name}: "
                f"{missing_columns}"
            )

# Empty lineage/matchsheet datasets are meaningful only when the immutable
# edition registry says ``partial`` or ``absent``.  The manifest DQ owns that
# per-scope decision; a global column audit must not invent a non-empty rule.
CAPABILITY_GATED_TABLES = frozenset(
    {
        "espn_lineup_generation_v2",
        "espn_matchsheet_generation_v2",
        "espn_lineup_current",
        "espn_matchsheet_current",
    }
)


__all__ = [
    "BASE_TABLE",
    "BRONZE_SCHEMA",
    "CAPABILITY_GATED_TABLES",
    "COMPACT6_INTERNAL_RELATIONS",
    "COMPACT6_PUBLIC_RELATIONS",
    "CONTROL_TABLES",
    "CURRENT_VIEWS",
    "GENERATION_TABLES",
    "INTERNAL_SCHEMA",
    "LAYOUT_MODES",
    "LEGACY14_PUBLIC_RELATIONS",
    "LEGACY_DISPOSITION_VALUES",
    "LEGACY_TABLES",
    "ObjectInventoryError",
    "PUBLIC_RELATIONS_BY_LAYOUT",
    "REQUIRED_COLUMNS",
    "RelationContract",
    "RelationInventory",
    "VIEW",
    "audit_layout_inventory",
    "required_layout_relations",
]
