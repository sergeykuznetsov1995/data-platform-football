"""Dependency-free ESPN Native Bronze v2 object inventory.

This module is intentionally safe to load from operational audit images.  It
contains no Airflow, pandas, Trino, network, or scraper imports.
"""

from __future__ import annotations

from types import MappingProxyType


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
    "CAPABILITY_GATED_TABLES",
    "CONTROL_TABLES",
    "CURRENT_VIEWS",
    "GENERATION_TABLES",
    "LEGACY_TABLES",
    "REQUIRED_COLUMNS",
]
