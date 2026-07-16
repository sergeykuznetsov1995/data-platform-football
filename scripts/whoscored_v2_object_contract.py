"""Authoritative object inventory for the WhoScored V2 cutover tools.

This module deliberately contains no Trino calls.  Migration and cleanup use
the same immutable inventory, while a unit test compares it with the runtime
repository constants.  A new WhoScored dataset therefore cannot be added to
the parser and silently omitted from rollback or cleanup validation.
"""

from __future__ import annotations

# ruff: noqa: E402 -- the trust anchor must run before every non-built-in import

import sys as _whoscored_bootstrap_sys

_whoscored_source = __file__
if not _whoscored_source.startswith("/"):
    raise RuntimeError("WhoScored entrypoint requires an absolute source path")
_whoscored_production = _whoscored_source.startswith("/opt/airflow/")
_whoscored_root = (
    "/opt/airflow"
    if _whoscored_production
    else _whoscored_source.rsplit("/scripts/", 1)[0]
)
if _whoscored_production:
    if getattr(
        _whoscored_bootstrap_sys, "_whoscored_runtime_startup_schema", None
    ) != 2:
        raise RuntimeError("image-baked WhoScored startup anchor is required")
elif (
    getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_root", None)
    != _whoscored_root
):
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
_WHOSCORED_RUNTIME_CONTRACT.require_production_runtime_class(
    operation="WhoScored V2 object contract"
)

from typing import Mapping, Sequence


CATALOG_TABLES = (
    "whoscored_competitions",
    "whoscored_seasons",
    "whoscored_stages",
)

SCOPE_TABLES = (
    "whoscored_schedule",
    "whoscored_match_incidents",
    "whoscored_match_bets",
    "whoscored_stage_standings",
    "whoscored_stage_forms",
    "whoscored_stage_streaks",
    "whoscored_stage_performance",
    "whoscored_team_stage_stats",
    "whoscored_player_stage_stats",
    "whoscored_referee_stage_stats",
)

MATCH_TABLES = (
    "whoscored_matches",
    "whoscored_events",
    "whoscored_lineups",
    "whoscored_substitutions",
    "whoscored_formations",
    "whoscored_team_match_stats",
    "whoscored_player_match_stats",
)

PREVIEW_TABLES = (
    "whoscored_preview_lineups",
    "whoscored_missing_players",
    "whoscored_preview_sections",
)

PROFILE_TABLES = (
    "whoscored_player_profile_versions",
    "whoscored_player_stage_participations",
)

BUSINESS_TABLES = (
    *CATALOG_TABLES,
    *SCOPE_TABLES,
    *MATCH_TABLES,
    *PREVIEW_TABLES,
    *PROFILE_TABLES,
)

MANIFEST_TABLES = (
    "whoscored_catalog_manifest",
    "whoscored_scope_ingest_manifest",
    "whoscored_match_ingest_manifest",
    "whoscored_preview_ingest_manifest",
    "whoscored_profile_ingest_manifest",
)

CATALOG_VIEWS = (
    "whoscored_catalog_latest_success",
    *(f"{table}_current" for table in CATALOG_TABLES),
)
SCOPE_VIEWS = (
    "whoscored_scope_ingest_latest_success",
    *(f"{table}_current" for table in SCOPE_TABLES),
)
MATCH_VIEWS = (
    "whoscored_match_ingest_latest",
    "whoscored_match_ingest_latest_success",
    *(f"{table}_current" for table in MATCH_TABLES),
)
PREVIEW_VIEWS = (
    "whoscored_preview_ingest_latest",
    "whoscored_preview_ingest_latest_success",
    *(f"{table}_current" for table in PREVIEW_TABLES),
)
PROFILE_BRONZE_VIEWS = (
    "whoscored_player_roster",
    "whoscored_player_stage_participations_current",
)

BRONZE_VIEWS = (
    *CATALOG_VIEWS,
    *SCOPE_VIEWS,
    *MATCH_VIEWS,
    *PREVIEW_VIEWS,
    *PROFILE_BRONZE_VIEWS,
)
SILVER_VIEWS = ("whoscored_player_profile_current",)

REQUIRED_BRONZE_OBJECTS = frozenset({*BUSINESS_TABLES, *MANIFEST_TABLES, *BRONZE_VIEWS})
REQUIRED_SILVER_OBJECTS = frozenset(SILVER_VIEWS)

# Column contracts are intentionally limited to commit identity and provenance.
# Parser payload columns evolve additively and are validated by parser/schema
# fingerprints rather than by this migration utility.
MANIFEST_REQUIRED_COLUMNS: Mapping[str, Sequence[str]] = {
    "whoscored_catalog_manifest": (
        "batch_id",
        "payload_sha256",
        "raw_uri",
        "raw_inputs_json",
        "raw_provenance_sha256",
        "discovery_mode",
        "as_of_date",
        "parent_catalog_batch_id",
        "parent_catalog_payload_sha256",
        "parent_catalog_raw_provenance_sha256",
        "parser_version",
        "state",
        "competitions_count",
        "seasons_count",
        "stages_count",
        "quarantined_count",
        "schema_fingerprint",
    ),
    "whoscored_scope_ingest_manifest": (
        "league",
        "season",
        "entity_group",
        "batch_id",
        "payload_sha256",
        "raw_uris_json",
        "parser_version",
        "state",
        "entity_counts_json",
        "dataset_states_json",
        "schema_fingerprint",
    ),
    "whoscored_match_ingest_manifest": (
        "league",
        "season",
        "game_id",
        "batch_id",
        "payload_sha256",
        "raw_uri",
        "parser_version",
        "availability_version",
        "state",
        "entity_counts_json",
        "dataset_statuses_json",
        "schema_fingerprint",
    ),
    "whoscored_preview_ingest_manifest": (
        "league",
        "season",
        "game_id",
        "batch_id",
        "payload_sha256",
        "raw_uri",
        "parser_version",
        "state",
        "entity_counts_json",
        "dataset_statuses_json",
        "schema_fingerprint",
    ),
    "whoscored_profile_ingest_manifest": (
        "player_id",
        "payload_sha256",
        "raw_uri",
        "parser_version",
        "state",
        "participations_count",
        "_profile_batch_id",
    ),
}

BATCH_COLUMN_BY_TABLE = {
    **{table: "_catalog_batch_id" for table in CATALOG_TABLES},
    **{table: "_scope_batch_id" for table in SCOPE_TABLES},
    **{table: "_game_batch_id" for table in MATCH_TABLES},
    **{table: "_preview_batch_id" for table in PREVIEW_TABLES},
    **{table: "_profile_batch_id" for table in PROFILE_TABLES},
}

BUSINESS_REQUIRED_COLUMNS: dict[str, Sequence[str]] = {
    table: (
        batch_column,
        "_source",
        "_entity_type",
        "_ingested_at",
        "_batch_id",
    )
    for table, batch_column in BATCH_COLUMN_BY_TABLE.items()
}
BUSINESS_REQUIRED_COLUMNS["whoscored_events"] = (
    *BUSINESS_REQUIRED_COLUMNS["whoscored_events"],
    "source_event_id",
    "team_event_id",
    "related_team_event_id",
    "source_raw_json",
    "source_schema_fingerprint",
)

# Only these pre-V2 physical tables are rewritten by the reversible migration.
# The remaining business tables are additive V2 objects created by
# WhoScoredRepository.ensure_schema().
LEGACY_MIGRATION_KEYS: Mapping[str, Sequence[str]] = {
    "whoscored_events": (
        "league",
        "season",
        "game_id",
        "source_event_id",
        "period",
        "minute",
        "second",
        "expanded_minute",
        "type",
        "outcome_type",
        "team_id",
        "player_id",
        "x",
        "y",
        "end_x",
        "end_y",
        "qualifiers",
        "related_event_id",
        "related_player_id",
        "team",
    ),
    "whoscored_lineups": ("league", "season", "game_id", "player_id"),
    "whoscored_schedule": ("league", "season", "game_id"),
    "whoscored_missing_players": (
        "league",
        "season",
        "game_id",
        "team",
        "player_id",
        "reason",
        "status",
    ),
    "whoscored_season_stages": ("league", "season", "stage_id"),
    "whoscored_player_profile": ("league", "season", "player_id"),
}

LEGACY_BUSINESS_TABLES = tuple(
    table for table in LEGACY_MIGRATION_KEYS if table in BUSINESS_TABLES
)
ADDITIVE_V2_TABLES = tuple(
    table for table in BUSINESS_TABLES if table not in LEGACY_BUSINESS_TABLES
)
ROLLBACK_STATE_TABLES = (*MANIFEST_TABLES, *ADDITIVE_V2_TABLES)

DEPRECATED_ACTIVE_TABLES = (
    "whoscored_season_stages",
    "whoscored_player_profile",
    # Retired before production cutover: no observed WhoScored page exposes
    # the synthetic ``playerAssistData`` contract. Cleanup remains separately
    # confirmation-gated because an earlier dry run may have created it.
    "whoscored_player_assist_pairs",
)


def validate_definition() -> None:
    """Fail at import time when the static contract is internally ambiguous."""

    if len(BUSINESS_TABLES) != 25:
        raise RuntimeError(
            f"WhoScored contract must contain 25 business tables, got {len(BUSINESS_TABLES)}"
        )
    if len(set(BUSINESS_TABLES)) != len(BUSINESS_TABLES):
        raise RuntimeError("WhoScored business-table contract contains duplicates")
    if set(MANIFEST_TABLES) & set(BUSINESS_TABLES):
        raise RuntimeError("manifest tables must not be counted as business datasets")
    if set(BATCH_COLUMN_BY_TABLE) != set(BUSINESS_TABLES):
        raise RuntimeError("every business table must have a logical commit column")
    if len(set(BRONZE_VIEWS)) != len(BRONZE_VIEWS):
        raise RuntimeError("WhoScored Bronze view contract contains duplicates")
    if set(LEGACY_BUSINESS_TABLES) | set(ADDITIVE_V2_TABLES) != set(BUSINESS_TABLES):
        raise RuntimeError("legacy/additive business-table decomposition is incomplete")
    if len(set(ROLLBACK_STATE_TABLES)) != len(ROLLBACK_STATE_TABLES):
        raise RuntimeError("WhoScored rollback-state contract contains duplicates")
    if len(set(DEPRECATED_ACTIVE_TABLES)) != len(DEPRECATED_ACTIVE_TABLES):
        raise RuntimeError("WhoScored deprecated-active contract contains duplicates")
    if set(DEPRECATED_ACTIVE_TABLES) & set(BUSINESS_TABLES):
        raise RuntimeError("deprecated active tables cannot remain business datasets")


validate_definition()
