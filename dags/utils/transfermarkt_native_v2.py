"""Control-plane and readiness contracts for Transfermarkt native v2.

This module deliberately has no Airflow imports.  It is shared by the
operator CLI and DAG preconditions so cutover, direct DAG triggers and cleanup
all interpret the same singleton state and the same cycle-scoped evidence.

The canonical reader contract is fail-safe:

* a missing state row means legacy during bootstrap/deploy ordering;
* only one valid ``state_key='canonical'`` row may authorize v2;
* readiness and state mutations never infer a cycle or revision.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence


STATE_KEY = 'canonical'
STATE_TABLE = 'iceberg.ops.transfermarkt_reader_state_v2'
HISTORY_TABLE = 'iceberg.ops.transfermarkt_reader_state_history_v2'
DUAL_WRITE_MANIFEST_TABLE = (
    'iceberg.ops.transfermarkt_dual_write_manifest_v2'
)
MODEL_MANIFEST_TABLE = 'iceberg.ops.transfermarkt_model_build_manifest_v2'
NATIVE_WRITE_MANIFEST_TABLE = (
    'iceberg.ops.transfermarkt_native_write_manifest_v2'
)
RETENTION_DAYS = 30
READINESS_MAX_AGE_DAYS = 7
SLOTS = ('a', 'b')
# A full inactive slot may be assembled from many bounded paid crawl batches.
# The limits below apply independently to every parent cycle, never to the
# aggregate ScopeSet traffic metric.
#
# The slot must cover the whole eligible target, and the promoted registry states
# it: 705 senior-men competitions across 9778 active editions. The cap bounds the
# manifest, not the crawl — every batch stays bounded by the byte, request and
# retry limits below.
MAX_SCOPE_SET_SIZE = 16_384
SCOPE_SET_HARD_BYTE_CAP = 15_728_640
SCOPE_SET_SOFT_BYTE_STOP = 14_680_064
SCOPE_SET_REQUEST_LIMIT = 316
SCOPE_SET_RETRY_LIMIT = 2

NATIVE_ENTITIES = (
    'squad_memberships',
    'player_attribute_observations',
    'player_contract_observations',
    'market_value_points',
    'transfer_events',
    'coach_profiles',
    'coach_stints',
)
REGISTRY_ENTITIES = ('competitions', 'competition_editions')
ALL_NATIVE_ENTITIES = (*REGISTRY_ENTITIES, *NATIVE_ENTITIES)

# These are blocking production floors, not smoke-test expectations.  In
# particular the market-value contract also checks the Transfermarkt branch;
# a large FotMob branch cannot hide an empty native Transfermarkt model.
GOLD_MIN_ROWS = {
    'fct_transfer_v2': 500,
    'fct_player_market_value_v2': 1000,
    'dim_manager_v2': 20,
    'team_season_market_value_v2': 80,
}
TRANSFERMARKT_MV_MIN_ROWS = 1000


class StateInvariantError(RuntimeError):
    """The persisted reader state is missing, ambiguous or malformed."""


class ReadinessError(RuntimeError):
    """A requested state transition has not passed all readiness gates."""


class RevisionConflict(RuntimeError):
    """A compare-and-set revision no longer matches persisted state."""


@dataclass(frozen=True)
class ReaderState:
    exists: bool
    state_key: str = STATE_KEY
    active_version: str = 'legacy'
    active_slot: str | None = None
    approved_cycle_id: str | None = None
    approved_league: str | None = None
    approved_season: int | None = None
    approved_model_revision: int | None = None
    approved_scope_set_id: str | None = None
    previous_slot: str | None = None
    previous_cycle_id: str | None = None
    previous_league: str | None = None
    previous_season: int | None = None
    previous_model_revision: int | None = None
    previous_scope_set_id: str | None = None
    revision: int = 0
    activated_at: Any = None
    retention_until: Any = None
    rollback_verified_at: Any = None
    updated_at: Any = None
    updated_by: str | None = None
    legacy_writers_disabled_at: Any = None
    cleanup_completed_at: Any = None
    slot_rollback_verified_at: Any = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ParityPair:
    name: str
    legacy_table: str
    native_table: str
    legacy_columns: tuple[str, ...]
    native_columns: tuple[str, ...]
    native_legacy_scope_keys: tuple[tuple[str, str], ...] = ()
    fingerprint_legacy_columns: tuple[str, ...] = ()
    fingerprint_native_columns: tuple[str, ...] = ()

    def _projection(
        self,
        *,
        table: str,
        columns: Sequence[str],
        batch_id: str,
        extra_predicate: str | None = None,
    ) -> str:
        predicate = (
            f' AND ({extra_predicate})' if extra_predicate else ''
        )
        return (
            f"SELECT DISTINCT {','.join(columns)} FROM iceberg.bronze.{table} "
            f"WHERE _batch_id = {_sql_literal(batch_id)}{predicate}"
        )

    def projections(
        self,
        *,
        legacy_batch_id: str,
        native_batch_id: str,
        legacy_columns: Sequence[str] | None = None,
        native_columns: Sequence[str] | None = None,
    ) -> tuple[str, str]:
        legacy = self._projection(
            table=self.legacy_table,
            columns=legacy_columns or self.legacy_columns,
            batch_id=legacy_batch_id,
        )
        native_scope = None
        if self.native_legacy_scope_keys:
            native_keys = ','.join(
                native for native, _ in self.native_legacy_scope_keys
            )
            legacy_keys = ','.join(
                legacy for _, legacy in self.native_legacy_scope_keys
            )
            native_not_null = ' AND '.join(
                f'{native} IS NOT NULL'
                for native, _ in self.native_legacy_scope_keys
            )
            legacy_not_null = ' AND '.join(
                f'{legacy} IS NOT NULL'
                for _, legacy in self.native_legacy_scope_keys
            )
            native_scope = (
                f'({native_keys}) IN (SELECT {legacy_keys} '
                f'FROM iceberg.bronze.{self.legacy_table} '
                f'WHERE _batch_id = {_sql_literal(legacy_batch_id)} '
                f'AND {legacy_not_null}) AND {native_not_null}'
            )
        native = self._projection(
            table=self.native_table,
            columns=native_columns or self.native_columns,
            batch_id=native_batch_id,
            extra_predicate=native_scope,
        )
        return legacy, native

    def queries(
        self,
        *,
        legacy_batch_id: str,
        native_batch_id: str,
    ) -> tuple[str, str]:
        legacy, native = self.projections(
            legacy_batch_id=legacy_batch_id,
            native_batch_id=native_batch_id,
        )
        return (
            f'SELECT COUNT(*) FROM ({legacy} EXCEPT {native}) AS parity_delta',
            f'SELECT COUNT(*) FROM ({native} EXCEPT {legacy}) AS parity_delta',
        )


PARITY_PAIRS = (
    ParityPair(
        'squad_memberships',
        'transfermarkt_players',
        'transfermarkt_squad_memberships',
        (
            'league', 'season', 'CAST(current_club_id AS varchar)',
            'current_club_name', 'CAST(player_id AS varchar)',
            'player_slug', 'name',
        ),
        (
            'league', 'season', 'club_id', 'club_name', 'player_id',
            'player_slug', 'player_name',
        ),
    ),
    ParityPair(
        'player_attribute_observations',
        'transfermarkt_players',
        'transfermarkt_player_attribute_observations',
        (
            'CAST(player_id AS varchar)', 'player_slug', 'name', 'position',
            'dob', 'age', 'height_cm', 'foot', 'nationality',
            'contract_until', 'market_value_eur', 'league', 'season',
            'CAST(current_club_id AS varchar)', 'current_club_name',
        ),
        (
            'player_id', 'player_slug', 'name', 'position', 'dob', 'age',
            'height_cm', 'foot', 'nationality', 'contract_until',
            'market_value_eur', 'league', 'season', 'club_id', 'club_name',
        ),
    ),
    ParityPair(
        'player_contract_observations',
        'transfermarkt_players',
        'transfermarkt_player_contract_observations',
        (
            'CAST(player_id AS varchar)', 'contract_until',
            'CAST(current_club_id AS varchar)', 'current_club_name',
        ),
        ('player_id', 'contract_until', 'team_id', 'team_name'),
    ),
    ParityPair(
        'market_value_points',
        'transfermarkt_market_value_history',
        'transfermarkt_market_value_points',
        (
            'CAST(player_id AS varchar)', 'mv_date', 'value_eur',
            'club_name', 'age', 'mv_raw',
        ),
        ('player_id', 'mv_date', 'value_eur', 'club_name', 'age', 'mv_raw'),
    ),
    ParityPair(
        'transfer_events',
        'transfermarkt_transfers',
        'transfermarkt_transfer_events',
        (
            'CAST(player_id AS varchar)', 'transfer_date',
            'CAST(from_club_id AS varchar)', 'from_club_name',
            'CAST(to_club_id AS varchar)', 'to_club_name',
            'fee_text', 'is_upcoming', 'fee_eur', 'market_value_eur',
        ),
        (
            'player_id', 'transfer_date', 'from_club_id', 'from_club_name',
            'to_club_id', 'to_club_name', 'fee_text', 'is_upcoming',
            'fee_eur', 'market_value_eur',
        ),
    ),
    ParityPair(
        'coach_profiles',
        'transfermarkt_coaches',
        'transfermarkt_coach_profiles',
        (
            'CAST(coach_id AS varchar)', 'coach_slug', 'name', 'dob',
            'nationality',
        ),
        ('coach_id', 'coach_slug', 'name', 'dob', 'nationality'),
        (('coach_id', 'CAST(coach_id AS varchar)'),),
    ),
    ParityPair(
        'coach_stints',
        'transfermarkt_coaches',
        'transfermarkt_coach_stints',
        (
            'CAST(current_club_id AS varchar)', 'current_club_name',
            'CAST(coach_id AS varchar)', 'coach_slug', 'name', 'role',
        ),
        ('club_id', 'club_name', 'coach_id', 'coach_slug', 'name', 'role'),
        (
            ('club_id', 'CAST(current_club_id AS varchar)'),
            ('coach_id', 'CAST(coach_id AS varchar)'),
        ),
    ),
)
PARITY_BY_NAME = {pair.name: pair for pair in PARITY_PAIRS}


@dataclass(frozen=True)
class ModelContract:
    name: str
    output_table: str
    key_columns: tuple[str, ...]
    source_tables: tuple[str, ...]


@dataclass(frozen=True)
class TableContract:
    """Executable source-to-consumer contract for one native relation.

    This registry is deliberately stricter than ``MODEL_CONTRACTS``.  The
    latter is kept for the existing A/B build code, while this contract is the
    single audit surface that prevents a table from being advertised without
    a grain, deterministic key, lineage, DQ, an Airflow task and catalog docs.
    """

    layer: str
    name: str
    output_table: str
    grain: str
    key_columns: tuple[str, ...]
    source_tables: tuple[str, ...]
    dedup_order: tuple[str, ...]
    lineage_columns: tuple[str, ...]
    dq_checks: tuple[str, ...]
    airflow_task_id: str
    openmetadata_path: str
    consumers: tuple[str, ...]
    empty_policy: str = 'authoritative_empty_or_not_applicable'


_RAW_LINEAGE = (
    'source_url', 'source_body_hash', 'fetched_at', '_ingested_at',
    'parser_revision', 'schema_revision', 'cycle_id', 'scope_id', '_batch_id',
)
_MODEL_LINEAGE = ('input_snapshot_ids', 'build_id', 'scope_set_id')


TABLE_CONTRACTS = (
    TableContract(
        'bronze', 'competitions', 'iceberg.bronze.transfermarkt_competitions',
        'one discovered Transfermarkt competition', ('competition_id',), (),
        ('discovered_at DESC', 'source_body_hash ASC'), _RAW_LINEAGE,
        ('schema', 'classification', 'completeness', 'freshness'),
        'discover_registry',
        'configs/openmetadata/descriptions/bronze_transfermarkt_competitions.yaml',
        ('iceberg.silver.transfermarkt_competitions_v2',),
    ),
    TableContract(
        'bronze', 'competition_editions',
        'iceberg.bronze.transfermarkt_competition_editions',
        'one source edition of one competition',
        ('competition_id', 'edition_id'),
        ('iceberg.bronze.transfermarkt_competitions',),
        ('discovered_at DESC', 'source_body_hash ASC'), _RAW_LINEAGE,
        ('schema', 'season_semantics', 'completeness', 'freshness'),
        'discover_registry',
        'configs/openmetadata/descriptions/bronze_transfermarkt_competition_editions.yaml',
        ('iceberg.silver.transfermarkt_competition_editions_v2',),
    ),
    TableContract(
        'bronze', 'squad_memberships',
        'iceberg.bronze.transfermarkt_squad_memberships',
        'one player membership in one team and edition',
        ('competition_id', 'edition_id', 'club_id', 'player_id'),
        ('iceberg.bronze.transfermarkt_competition_editions',),
        ('observed_at DESC', 'source_body_hash ASC'), _RAW_LINEAGE,
        ('schema', 'natural_key', 'completeness', 'freshness'),
        'run_exact_child_cycle',
        'configs/openmetadata/descriptions/bronze_transfermarkt_squad_memberships.yaml',
        ('iceberg.silver.transfermarkt_squad_memberships_v2',),
    ),
    TableContract(
        'bronze', 'player_attribute_observations',
        'iceberg.bronze.transfermarkt_player_attribute_observations',
        'one scoped player observation at a source timestamp',
        ('competition_id', 'edition_id', 'club_id', 'player_id', 'observed_at'),
        ('iceberg.bronze.transfermarkt_squad_memberships',),
        ('fetched_at DESC', 'source_body_hash ASC'), _RAW_LINEAGE,
        ('schema', 'natural_key', 'field_ranges', 'completeness', 'freshness'),
        'run_exact_child_cycle',
        'configs/openmetadata/descriptions/bronze_transfermarkt_player_attribute_observations.yaml',
        ('iceberg.silver.transfermarkt_player_attribute_observations_v2',),
    ),
    TableContract(
        'bronze', 'player_contract_observations',
        'iceberg.bronze.transfermarkt_player_contract_observations',
        'one scoped player contract observation at a source timestamp',
        ('competition_id', 'edition_id', 'team_id', 'player_id', 'observed_at'),
        ('iceberg.bronze.transfermarkt_squad_memberships',),
        ('fetched_at DESC', 'source_body_hash ASC'), _RAW_LINEAGE,
        ('schema', 'natural_key', 'applicability', 'completeness', 'freshness'),
        'run_exact_child_cycle',
        'configs/openmetadata/descriptions/bronze_transfermarkt_player_contract_observations.yaml',
        ('iceberg.silver.transfermarkt_player_contract_observations_v2',),
    ),
    TableContract(
        'bronze', 'market_value_points',
        'iceberg.bronze.transfermarkt_market_value_points',
        'one Transfermarkt valuation for a player and date',
        ('player_id', 'mv_date'),
        ('iceberg.bronze.transfermarkt_squad_memberships',),
        ('fetched_at DESC', 'source_body_hash ASC'), _RAW_LINEAGE,
        ('schema', 'natural_key', 'value_range', 'completeness', 'freshness'),
        'run_exact_child_cycle',
        'configs/openmetadata/descriptions/bronze_transfermarkt_market_value_points.yaml',
        ('iceberg.silver.transfermarkt_market_value_points_v2',),
    ),
    TableContract(
        'bronze', 'transfer_events',
        'iceberg.bronze.transfermarkt_transfer_events',
        'one stable Transfermarkt transfer event', ('transfer_id',),
        ('iceberg.bronze.transfermarkt_squad_memberships',),
        ('fetched_at DESC', 'source_body_hash ASC'), _RAW_LINEAGE,
        ('schema', 'natural_key', 'semantic_duplicates', 'completeness', 'freshness'),
        'run_exact_child_cycle',
        'configs/openmetadata/descriptions/bronze_transfermarkt_transfer_events.yaml',
        ('iceberg.silver.transfermarkt_transfer_events_v2',),
    ),
    TableContract(
        'bronze', 'coach_profiles',
        'iceberg.bronze.transfermarkt_coach_profiles',
        'one Transfermarkt coach profile', ('coach_id',),
        ('iceberg.bronze.transfermarkt_squad_memberships',),
        ('fetched_at DESC', 'source_body_hash ASC'), _RAW_LINEAGE,
        ('schema', 'natural_key', 'completeness', 'freshness'),
        'run_exact_child_cycle',
        'configs/openmetadata/descriptions/bronze_transfermarkt_coach_profiles.yaml',
        ('iceberg.silver.transfermarkt_coach_profiles_v2',),
    ),
    TableContract(
        'bronze', 'coach_stints',
        'iceberg.bronze.transfermarkt_coach_stints',
        'one coach tenure at one team',
        ('club_id', 'coach_id', 'appointed_date', 'left_date'),
        ('iceberg.bronze.transfermarkt_coach_profiles',),
        ('fetched_at DESC', 'source_body_hash ASC'), _RAW_LINEAGE,
        ('schema', 'natural_key', 'tenure_bounds', 'completeness', 'freshness'),
        'run_exact_child_cycle',
        'configs/openmetadata/descriptions/bronze_transfermarkt_coach_stints.yaml',
        ('iceberg.silver.transfermarkt_coach_stints_v2',),
    ),
)


def _silver_contract(
    name: str,
    key: tuple[str, ...],
    sources: tuple[str, ...],
    consumers: tuple[str, ...],
) -> TableContract:
    return TableContract(
        'silver', name, f'iceberg.silver.{name}',
        f'validated {name} model grain', key, sources,
        ('model_precedence DESC', 'source_hash ASC'), _MODEL_LINEAGE,
        ('schema', 'natural_key', 'lineage', 'completeness', 'freshness'),
        f"native_v2_transforms.{name.removeprefix('transfermarkt_')}",
        f'configs/openmetadata/descriptions/silver_{name}.yaml', consumers,
    )


TABLE_CONTRACTS += (
    _silver_contract(
        'transfermarkt_competitions_v2', ('competition_id',),
        ('iceberg.bronze.transfermarkt_competitions',),
        ('transfermarkt_scope_planner',),
    ),
    _silver_contract(
        'transfermarkt_competition_editions_v2',
        ('competition_id', 'edition_id'),
        ('iceberg.bronze.transfermarkt_competition_editions',),
        ('transfermarkt_scope_planner',),
    ),
    _silver_contract(
        'transfermarkt_player_xref_global_v2', ('player_id',),
        (
            'iceberg.bronze.transfermarkt_squad_memberships',
            'iceberg.silver.xref_player',
        ),
        ('silver.transfermarkt_players', 'gold.fct_player_market_value'),
    ),
    _silver_contract(
        'transfermarkt_squad_memberships_v2',
        ('competition_id', 'edition_id', 'club_id', 'player_id'),
        ('iceberg.bronze.transfermarkt_squad_memberships',),
        ('silver.transfermarkt_players', 'gold.transfermarkt_team_season_market_value'),
    ),
    _silver_contract(
        'transfermarkt_player_attribute_observations_v2',
        ('competition_id', 'edition_id', 'club_id', 'player_id', 'observed_at'),
        ('iceberg.bronze.transfermarkt_player_attribute_observations',),
        ('iceberg.silver.transfermarkt_player_attributes_v2',),
    ),
    _silver_contract(
        'transfermarkt_player_contract_observations_v2',
        ('competition_id', 'edition_id', 'team_id', 'player_id', 'observed_at'),
        ('iceberg.bronze.transfermarkt_player_contract_observations',),
        ('silver.transfermarkt_players',),
    ),
    _silver_contract(
        'transfermarkt_player_attributes_v2', ('player_id',),
        (
            'iceberg.silver.transfermarkt_player_attribute_observations_v2',
            'iceberg.silver.transfermarkt_player_contract_observations_v2',
            'iceberg.silver.transfermarkt_player_xref_global_v2',
        ),
        ('silver.transfermarkt_players', 'gold.dim_player_attributes'),
    ),
    _silver_contract(
        'transfermarkt_market_value_points_v2', ('player_id', 'mv_date'),
        ('iceberg.bronze.transfermarkt_market_value_points',),
        ('gold.fct_player_market_value', 'gold.transfermarkt_team_season_market_value'),
    ),
    _silver_contract(
        'transfermarkt_transfer_events_v2', ('transfer_id',),
        ('iceberg.bronze.transfermarkt_transfer_events',),
        ('gold.fct_transfer',),
    ),
    _silver_contract(
        'transfermarkt_coach_profiles_v2', ('coach_id',),
        ('iceberg.bronze.transfermarkt_coach_profiles',),
        ('silver.transfermarkt_coaches', 'gold.dim_manager'),
    ),
    _silver_contract(
        'transfermarkt_coach_stints_v2',
        ('club_id', 'coach_id', 'appointed_date', 'left_date'),
        ('iceberg.bronze.transfermarkt_coach_stints',),
        ('silver.transfermarkt_coaches', 'gold.dim_manager'),
    ),
    _silver_contract(
        'transfermarkt_player_team_season_assignment_v2',
        ('competition_id', 'edition_id', 'player_id'),
        (
            'iceberg.silver.transfermarkt_squad_memberships_v2',
            'iceberg.silver.transfermarkt_transfer_events_v2',
        ),
        ('gold.transfermarkt_team_season_market_value',),
    ),
)


def _gold_contract(
    name: str,
    key: tuple[str, ...],
    sources: tuple[str, ...],
    canonical: str,
) -> TableContract:
    return TableContract(
        'gold', name, f'iceberg.gold.{name}', f'canonical {name} fact grain',
        key, sources, ('model_precedence DESC',), _MODEL_LINEAGE,
        (
            'schema', 'natural_key', 'lineage', 'coverage', 'completeness',
            'freshness', 'downstream_dq',
        ),
        f"native_v2_gold.{name.removeprefix('transfermarkt_')}",
        f'configs/openmetadata/descriptions/{name}.yaml', (canonical,),
    )


TABLE_CONTRACTS += (
    _gold_contract(
        'dim_manager_v2', ('manager_id',),
        ('iceberg.silver.transfermarkt_coach_profiles_v2',),
        'gold.dim_manager',
    ),
    _gold_contract(
        'fct_transfer_v2', ('transfer_id',),
        ('iceberg.silver.transfermarkt_transfer_events_v2',),
        'gold.fct_transfer',
    ),
    _gold_contract(
        'fct_player_market_value_v2',
        ('player_id', 'valuation_date', 'source'),
        ('iceberg.silver.transfermarkt_market_value_points_v2',),
        'gold.fct_player_market_value',
    ),
    _gold_contract(
        'transfermarkt_team_season_market_value_v2',
        ('team_id', 'league', 'season'),
        (
            'iceberg.silver.transfermarkt_player_team_season_assignment_v2',
            'iceberg.silver.transfermarkt_market_value_points_v2',
            'iceberg.silver.transfermarkt_competition_editions_v2',
        ),
        'gold.transfermarkt_team_season_market_value',
    ),
)


TABLE_CONTRACT_BY_RELATION = {
    contract.output_table: contract for contract in TABLE_CONTRACTS
}


def validate_table_contracts() -> dict[str, Any]:
    """Fail closed when the declared E2E matrix is incomplete or ambiguous."""

    errors: list[str] = []
    seen: set[str] = set()
    for contract in TABLE_CONTRACTS:
        if contract.output_table in seen:
            errors.append(f'duplicate relation: {contract.output_table}')
        seen.add(contract.output_table)
        required = {
            'grain': contract.grain,
            'key_columns': contract.key_columns,
            'dedup_order': contract.dedup_order,
            'lineage_columns': contract.lineage_columns,
            'dq_checks': contract.dq_checks,
            'airflow_task_id': contract.airflow_task_id,
            'openmetadata_path': contract.openmetadata_path,
            'consumers': contract.consumers,
            'empty_policy': contract.empty_policy,
        }
        missing = sorted(name for name, value in required.items() if not value)
        if missing:
            errors.append(f'{contract.output_table}: missing {missing}')
    counts = {
        layer: sum(item.layer == layer for item in TABLE_CONTRACTS)
        for layer in ('bronze', 'silver', 'gold')
    }
    if counts != {'bronze': 9, 'silver': 12, 'gold': 4}:
        errors.append(f'unexpected layer counts: {counts}')
    if errors:
        raise StateInvariantError('; '.join(errors))
    return {'passed': True, 'counts': counts, 'relations': len(seen)}


# Every table built by the native shadow graph is recorded.  The snapshot map
# is evidence about the actual physical inputs, not a free-form DAG marker.
MODEL_CONTRACTS = (
    ModelContract(
        'transfermarkt_competitions_v2',
        'iceberg.silver.transfermarkt_competitions_v2',
        ('competition_id',),
        ('iceberg.bronze.transfermarkt_competitions',),
    ),
    ModelContract(
        'transfermarkt_competition_editions_v2',
        'iceberg.silver.transfermarkt_competition_editions_v2',
        ('competition_id', 'edition_id'),
        ('iceberg.bronze.transfermarkt_competition_editions',),
    ),
    ModelContract(
        'transfermarkt_player_xref_global_v2',
        'iceberg.silver.transfermarkt_player_xref_global_v2',
        ('player_id',),
        (
            'iceberg.bronze.transfermarkt_squad_memberships',
            'iceberg.bronze.transfermarkt_player_attribute_observations',
            'iceberg.bronze.transfermarkt_market_value_points',
            'iceberg.bronze.transfermarkt_transfer_events',
            'iceberg.silver.xref_player',
        ),
    ),
    ModelContract(
        'transfermarkt_squad_memberships_v2',
        'iceberg.silver.transfermarkt_squad_memberships_v2',
        ('competition_id', 'edition_id', 'club_id', 'player_id'),
        ('iceberg.bronze.transfermarkt_squad_memberships',),
    ),
    ModelContract(
        'transfermarkt_player_attribute_observations_v2',
        'iceberg.silver.transfermarkt_player_attribute_observations_v2',
        (
            'competition_id', 'edition_id', 'club_id', 'player_id',
            'observed_at',
        ),
        ('iceberg.bronze.transfermarkt_player_attribute_observations',),
    ),
    ModelContract(
        'transfermarkt_player_contract_observations_v2',
        'iceberg.silver.transfermarkt_player_contract_observations_v2',
        ('competition_id', 'edition_id', 'team_id', 'player_id', 'observed_at'),
        (
            'iceberg.bronze.transfermarkt_player_contract_observations',
            'iceberg.silver.transfermarkt_player_xref_global_v2',
        ),
    ),
    ModelContract(
        'transfermarkt_player_attributes_v2',
        'iceberg.silver.transfermarkt_player_attributes_v2',
        ('player_id',),
        (
            'iceberg.silver.transfermarkt_player_attribute_observations_v2',
            'iceberg.silver.transfermarkt_player_contract_observations_v2',
            'iceberg.silver.transfermarkt_competition_editions_v2',
            'iceberg.silver.transfermarkt_player_xref_global_v2',
            'iceberg.silver.transfermarkt_market_value_points_v2',
        ),
    ),
    ModelContract(
        'transfermarkt_market_value_points_v2',
        'iceberg.silver.transfermarkt_market_value_points_v2',
        ('player_id', 'mv_date'),
        (
            'iceberg.bronze.transfermarkt_market_value_points',
            'iceberg.silver.transfermarkt_player_xref_global_v2',
        ),
    ),
    ModelContract(
        'transfermarkt_transfer_events_v2',
        'iceberg.silver.transfermarkt_transfer_events_v2',
        ('transfer_id',),
        (
            'iceberg.bronze.transfermarkt_transfer_events',
            'iceberg.silver.transfermarkt_player_xref_global_v2',
        ),
    ),
    ModelContract(
        'transfermarkt_coach_profiles_v2',
        'iceberg.silver.transfermarkt_coach_profiles_v2',
        ('coach_id',),
        ('iceberg.bronze.transfermarkt_coach_profiles',),
    ),
    ModelContract(
        'transfermarkt_coach_stints_v2',
        'iceberg.silver.transfermarkt_coach_stints_v2',
        ('club_id', 'coach_id', 'appointed_date', 'left_date'),
        (
            'iceberg.bronze.transfermarkt_coach_stints',
            'iceberg.silver.xref_manager',
            'iceberg.silver.xref_team',
        ),
    ),
    ModelContract(
        'transfermarkt_player_team_season_assignment_v2',
        'iceberg.silver.transfermarkt_player_team_season_assignment_v2',
        ('league', 'season', 'player_id'),
        (
            'iceberg.silver.transfermarkt_squad_memberships_v2',
            'iceberg.silver.transfermarkt_transfer_events_v2',
            'iceberg.silver.transfermarkt_player_xref_global_v2',
            'iceberg.silver.xref_team',
        ),
    ),
    ModelContract(
        'dim_manager_v2',
        'iceberg.gold.dim_manager_v2',
        ('manager_id',),
        (
            'iceberg.silver.transfermarkt_coach_profiles_v2',
            'iceberg.silver.fotmob_manager_profile',
            'iceberg.silver.xref_manager',
        ),
    ),
    ModelContract(
        'fct_transfer_v2',
        'iceberg.gold.fct_transfer_v2',
        ('transfer_id',),
        ('iceberg.silver.transfermarkt_transfer_events_v2',),
    ),
    ModelContract(
        'fct_player_market_value_v2',
        'iceberg.gold.fct_player_market_value_v2',
        ('player_id', 'valuation_date', 'source'),
        (
            'iceberg.silver.transfermarkt_market_value_points_v2',
            'iceberg.silver.fotmob_player_market_value_history',
            'iceberg.silver.xref_player',
        ),
    ),
    ModelContract(
        'transfermarkt_team_season_market_value_v2',
        'iceberg.gold.transfermarkt_team_season_market_value_v2',
        ('team_id', 'league', 'season'),
        (
            'iceberg.silver.transfermarkt_player_team_season_assignment_v2',
            'iceberg.silver.transfermarkt_market_value_points_v2',
            'iceberg.silver.transfermarkt_competition_editions_v2',
        ),
    ),
)
MODEL_BY_NAME = {contract.name: contract for contract in MODEL_CONTRACTS}


@dataclass(frozen=True)
class ReaderRelation:
    """Stable canonical relation and its identical-schema versioned inputs."""

    canonical: str
    legacy: str
    v2: str

    def for_slot(self, slot: str) -> str:
        return slotted_relation(self.v2, slot)

    @property
    def v2_a(self) -> str:
        return self.for_slot('a')

    @property
    def v2_b(self) -> str:
        return self.for_slot('b')


def _versioned_reader(canonical: str) -> ReaderRelation:
    return ReaderRelation(
        canonical=canonical,
        legacy=f'{canonical}_legacy',
        v2=f'{canonical}_v2',
    )


# Runtime code reads only these stable names.  The versioned inputs are views
# (adapters where the physical v2 grain differs), so both UNION branches have
# exactly the canonical schema.  Keeping the registry here makes cutover
# verification exhaustive and machine-readable.
CANONICAL_READER_RELATIONS = tuple(map(_versioned_reader, (
    'iceberg.silver.transfermarkt_players',
    'iceberg.silver.transfermarkt_coaches',
    'iceberg.gold.dim_manager',
    'iceberg.gold.fct_transfer',
    'iceberg.gold.fct_player_market_value',
    'iceberg.gold.transfermarkt_team_season_market_value',
)))

LEGACY_SOURCE_RELATIONS = {
    'iceberg.silver.transfermarkt_players': (
        'iceberg.silver.transfermarkt_players_legacy'
    ),
    'iceberg.silver.transfermarkt_coaches': (
        'iceberg.silver.transfermarkt_coaches_legacy'
    ),
    'iceberg.gold.dim_manager': 'iceberg.gold.dim_manager_legacy',
    'iceberg.gold.fct_transfer': (
        'iceberg.gold.fct_transfer_legacy_source'
    ),
    'iceberg.gold.fct_player_market_value': (
        'iceberg.gold.fct_player_market_value_legacy'
    ),
    'iceberg.gold.transfermarkt_team_season_market_value': (
        'iceberg.gold.transfermarkt_team_season_market_value_legacy'
    ),
}

READER_PREFLIGHT_RELATIONS = (
    'iceberg.silver.transfermarkt_competitions_v2',
    'iceberg.silver.transfermarkt_competition_editions_v2',
    'iceberg.silver.transfermarkt_squad_memberships_v2',
    'iceberg.silver.transfermarkt_player_contract_observations_v2',
    'iceberg.silver.transfermarkt_player_attributes_v2',
    'iceberg.silver.transfermarkt_coach_profiles_v2',
    'iceberg.silver.transfermarkt_coach_stints_v2',
    'iceberg.silver.xref_manager',
    'iceberg.silver.xref_team',
    'iceberg.gold.dim_manager_v2',
    'iceberg.gold.fct_transfer_v2',
    'iceberg.gold.fct_player_market_value_v2',
    'iceberg.gold.transfermarkt_team_season_market_value_v2',
)


def _sql_literal(value: Any) -> str:
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _normalise_cycle_id(cycle_id: str) -> str:
    value = str(cycle_id or '').strip()
    if not value:
        raise ValueError('cycle_id is required; latest/inferred cycles are forbidden')
    return value


def _normalise_revision(revision: int) -> int:
    try:
        value = int(revision)
    except (TypeError, ValueError) as exc:
        raise ValueError('expected_revision must be an integer') from exc
    if value < 0:
        raise ValueError('expected_revision must be non-negative')
    return value


def _normalise_scope_set_id(scope_set_id: str) -> str:
    value = str(scope_set_id or '').strip()
    if not re.fullmatch(r'[0-9a-f]{64}', value):
        raise ValueError('scope_set_id must be a lowercase sha256 digest')
    return value


def _normalise_slot(slot: str) -> str:
    value = str(slot or '').strip().lower()
    if value not in SLOTS:
        raise ValueError(f'candidate/active slot must be one of {SLOTS}, got {slot!r}')
    return value


def inactive_slot(state: ReaderState) -> str:
    """Return the only slot a new build may target.

    ``active_slot`` is intentionally retained while routing legacy, so a
    rollback followed by a new build still chooses the opposite physical
    slot.  A never-promoted deployment deterministically starts with slot A.
    """
    return 'b' if state.active_slot == 'a' else 'a'


def slotted_relation(relation: str, slot: str) -> str:
    return f'{relation}_{_normalise_slot(slot)}'


def contract_output_table(contract: ModelContract, slot: str) -> str:
    return slotted_relation(contract.output_table, slot)


def contract_source_tables(contract: ModelContract, slot: str) -> tuple[str, ...]:
    """Resolve native-model dependencies to the same immutable build slot."""
    native_outputs = {item.output_table for item in MODEL_CONTRACTS}
    return tuple(
        slotted_relation(table, slot) if table in native_outputs else table
        for table in contract.source_tables
    )


def required_pinned_source_tables() -> frozenset[str]:
    """Root Iceberg inputs that must be pinned before any A/B model build."""

    native_outputs = {item.output_table for item in MODEL_CONTRACTS}
    return frozenset(
        source
        for contract in MODEL_CONTRACTS
        for source in contract.source_tables
        if source not in native_outputs
    )


def rewrite_native_relations(sql: str, slot: str) -> str:
    """Deterministically pin every native model reference to one slot.

    The rewrite is deliberately limited to the exact fully-qualified relation
    registry.  It cannot rewrite Bronze, xref, or arbitrary identifiers, and
    it asserts that no unslotted native relation survives.
    """
    selected = _normalise_slot(slot)
    result = str(sql)
    outputs = sorted(
        (item.output_table for item in MODEL_CONTRACTS), key=len, reverse=True,
    )
    for relation in outputs:
        result = re.sub(
            rf'(?<![A-Za-z0-9_]){re.escape(relation)}(?![A-Za-z0-9_])',
            slotted_relation(relation, selected),
            result,
        )
    leftovers = [relation for relation in outputs if re.search(
        rf'(?<![A-Za-z0-9_]){re.escape(relation)}(?![A-Za-z0-9_])', result,
    )]
    if leftovers:
        raise StateInvariantError(
            f'unslotted native relations remain after rewrite: {leftovers}'
        )
    return result


def _normalise_scope(league: str, season: int) -> tuple[str, int]:
    league_value = str(league or '').strip()
    if not league_value:
        raise ValueError('league is required for cycle-scoped readiness')
    try:
        season_value = int(season)
    except (TypeError, ValueError) as exc:
        raise ValueError('season must be an integer start year') from exc
    if season_value < 2000 or season_value > 2100:
        raise ValueError(f'unsupported season start year: {season_value}')
    return league_value, season_value


def _scalar(cur, sql: str):
    cur.execute(sql)
    rows = cur.fetchall()
    return rows[0][0] if rows else None


def _table_missing(exc: BaseException) -> bool:
    message = str(exc).lower()
    return any(
        token in message
        for token in ('table_not_found', 'table not found', 'does not exist')
    )


def connect():
    """Build the production Trino connection without importing Airflow."""
    import trino

    host = os.environ.get('TRINO_HOST', 'localhost')
    password = os.environ.get('TRINO_PASSWORD')
    port = int(os.environ.get('TRINO_PORT', '8443' if password else '8080'))
    user = os.environ.get('TRINO_USER', 'airflow')
    kwargs: dict[str, Any] = {
        'host': host, 'port': port, 'user': user, 'catalog': 'iceberg',
    }
    if password:
        from trino.auth import BasicAuthentication

        kwargs.update(
            http_scheme='https',
            auth=BasicAuthentication(user, password),
            verify=False,
        )
    return trino.dbapi.connect(**kwargs)


def reader_selector_sql() -> str:
    """Return the canonical-view selector contract.

    Duplicate/malformed rows deliberately select legacy.  Operator readiness
    uses :func:`read_reader_state` and rejects the same ambiguity instead of
    silently authorizing a mutation.
    """
    complete_v2 = """active_version = 'v2'
             AND active_slot IN ('a', 'b')
             AND NULLIF(TRIM(approved_cycle_id), '') IS NOT NULL
             AND NULLIF(TRIM(approved_league), '') IS NOT NULL
             AND approved_season IS NOT NULL
             AND approved_model_revision >= 0
             AND REGEXP_LIKE(approved_scope_set_id, '^[0-9a-f]{64}$')
             AND revision >= 0"""
    return f"""
SELECT
    CASE
        WHEN COUNT(*) = 1
         AND COUNT_IF({complete_v2}) = 1
        THEN 'v2'
        ELSE 'legacy'
    END AS active_version,
    CASE
        WHEN COUNT(*) = 1
         AND COUNT_IF({complete_v2}) = 1
        THEN MAX(active_slot)
    END AS active_slot,
    CASE WHEN COUNT(*) = 1 THEN MAX(revision) ELSE CAST(-1 AS bigint) END AS revision,
    CASE WHEN COUNT(*) = 1 THEN MAX(approved_cycle_id) END AS approved_cycle_id,
    CASE WHEN COUNT(*) = 1 THEN MAX(approved_league) END AS approved_league,
    CASE WHEN COUNT(*) = 1 THEN MAX(approved_season) END AS approved_season,
    CASE WHEN COUNT(*) = 1 THEN MAX(approved_scope_set_id) END
        AS approved_scope_set_id
FROM {STATE_TABLE}
WHERE state_key = '{STATE_KEY}'
""".strip()


def canonical_reader_view_sql(relation: ReaderRelation) -> str:
    """Build one stable state-selected canonical view.

    The versioned relations must expose identical schemas.  State ambiguity
    resolves to the legacy branch through :func:`reader_selector_sql`.
    """
    return f"""
CREATE OR REPLACE VIEW {relation.canonical} AS
WITH tm_reader_state AS (
{reader_selector_sql()}
)
SELECT legacy.*
FROM {relation.legacy} legacy
CROSS JOIN tm_reader_state state
WHERE state.active_version = 'legacy'
UNION ALL
SELECT native_a.*
FROM {relation.v2_a} native_a
CROSS JOIN tm_reader_state state
WHERE state.active_version = 'v2' AND state.active_slot = 'a'
UNION ALL
SELECT native_b.*
FROM {relation.v2_b} native_b
CROSS JOIN tm_reader_state state
WHERE state.active_version = 'v2' AND state.active_slot = 'b'
""".strip()


def canonical_reader_view_sql_all() -> list[str]:
    return [
        canonical_reader_view_sql(relation)
        for relation in CANONICAL_READER_RELATIONS
    ]


def post_cleanup_reader_view_sql(relation: ReaderRelation) -> str:
    """Remove the legacy branch while preserving one-CAS A/B promotion.

    A malformed/missing singleton makes ``active_slot`` NULL, so neither v2
    branch returns rows.  Post-cleanup cannot silently fall back to deleted
    legacy data or guess a slot.
    """
    return f"""
CREATE OR REPLACE VIEW {relation.canonical} AS
WITH tm_reader_state AS (
{reader_selector_sql()}
)
SELECT native_a.*
FROM {relation.v2_a} native_a
CROSS JOIN tm_reader_state state
WHERE state.active_version = 'v2' AND state.active_slot = 'a'
UNION ALL
SELECT native_b.*
FROM {relation.v2_b} native_b
CROSS JOIN tm_reader_state state
WHERE state.active_version = 'v2' AND state.active_slot = 'b'
""".strip()


def post_cleanup_reader_view_sql_all() -> list[str]:
    return [post_cleanup_reader_view_sql(item) for item in CANONICAL_READER_RELATIONS]


def legacy_transfer_adapter_sql() -> str:
    """Adapt the retained legacy Gold fact to the native transfer contract."""
    source = LEGACY_SOURCE_RELATIONS['iceberg.gold.fct_transfer']
    identity = (
        "CONCAT_WS(CHR(31), "
        "COALESCE(CAST(player_id AS varchar), '__NULL__'), "
        "COALESCE(CAST(transfer_date AS varchar), '__NULL__'), "
        "COALESCE(CAST(from_team_id AS varchar), '__NULL__'), "
        "COALESCE(CAST(to_team_id AS varchar), '__NULL__'))"
    )
    event_season = (
        "CASE WHEN transfer_date IS NOT NULL THEN "
        "SUBSTR(CAST(IF(MONTH(transfer_date) >= 7, YEAR(transfer_date), "
        "YEAR(transfer_date) - 1) AS varchar), 3, 2) || "
        "SUBSTR(CAST(IF(MONTH(transfer_date) >= 7, YEAR(transfer_date), "
        "YEAR(transfer_date) - 1) + 1 AS varchar), 3, 2) "
        "ELSE NULL END"
    )
    return f"""
CREATE OR REPLACE VIEW iceberg.gold.fct_transfer_legacy AS
SELECT
    'tml_' || LOWER(TO_HEX(SHA256(TO_UTF8({identity})))) AS transfer_id,
    player_id,
    transfer_date,
    {event_season} AS event_season,
    from_team_id,
    to_team_id,
    from_club_name,
    to_club_name,
    fee_eur,
    market_value_at_transfer_eur,
    is_loan,
    is_upcoming,
    _bronze_ingested_at
FROM {source}
""".strip()


def transfermarkt_players_adapter_sql(slot: str = 'a') -> str:
    """Legacy-shaped Silver player adapter backed only by native v2 models."""
    selected = _normalise_slot(slot)
    sql = """
CREATE OR REPLACE VIEW iceberg.silver.transfermarkt_players_v2 AS
WITH membership AS (
    SELECT *
    FROM (
        SELECT
            m.*,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, league, season
                ORDER BY observed_at DESC, _bronze_ingested_at DESC, club_id ASC
            ) AS rn
        FROM iceberg.silver.transfermarkt_squad_memberships_v2 m
    )
    WHERE rn = 1
)
SELECT
    m.player_id,
    a.canonical_id,
    COALESCE(a.name, m.player_name) AS name,
    COALESCE(a.player_slug, m.player_slug) AS slug,
    a.position,
    a.dob,
    a.age,
    a.height_cm,
    a.foot,
    a.nationality,
    a.contract_until,
    a.current_market_value_eur,
    a.current_market_value_date AS mv_last_update,
    m.club_id AS current_club_id,
    m.club_name AS current_club_name,
    COALESCE(
        GREATEST(m._bronze_ingested_at, a._bronze_ingested_at),
        m._bronze_ingested_at,
        a._bronze_ingested_at
    )
        AS _bronze_ingested_at,
    m.league,
    m.season
FROM membership m
LEFT JOIN iceberg.silver.transfermarkt_player_attributes_v2 a
  ON a.player_id = m.player_id
""".strip()
    sql = rewrite_native_relations(sql, selected)
    # The adapter itself is not a model output and therefore needs an explicit
    # slot suffix after model-reference rewriting.
    return sql.replace(
        'iceberg.silver.transfermarkt_players_v2 AS',
        f'iceberg.silver.transfermarkt_players_v2_{selected} AS',
        1,
    )


def transfermarkt_coaches_adapter_sql(slot: str = 'a') -> str:
    """Legacy-shaped seasonal coach adapter from native profiles and stints."""
    selected = _normalise_slot(slot)
    sql = """
CREATE OR REPLACE VIEW iceberg.silver.transfermarkt_coaches_v2 AS
WITH edition_bounds AS (
    SELECT
        competition_id,
        edition_id,
        COALESCE(
            start_date,
            CASE
                WHEN season_format = 'single_year' THEN CAST(
                    CONCAT(canonical_season, '-01-01') AS date
                )
                WHEN season_format = 'split_year' THEN CAST(
                    CONCAT(edition_id, '-07-01') AS date
                )
            END
        ) AS edition_start_date,
        COALESCE(
            end_date,
            CASE
                WHEN season_format = 'single_year' THEN CAST(
                    CONCAT(canonical_season, '-12-31') AS date
                )
                WHEN season_format = 'split_year' THEN CAST(
                    CONCAT(
                        CAST(TRY_CAST(edition_id AS integer) + 1 AS varchar),
                        '-06-30'
                    ) AS date
                )
            END
        ) AS edition_end_date
    FROM iceberg.silver.transfermarkt_competition_editions_v2
), club_scopes AS (
    SELECT DISTINCT
        m.club_id,
        m.competition_id,
        m.edition_id,
        m.league,
        m.season,
        b.edition_start_date AS season_start,
        b.edition_end_date AS season_end
    FROM iceberg.silver.transfermarkt_squad_memberships_v2 m
    JOIN edition_bounds b
      ON b.competition_id = m.competition_id
     AND b.edition_id = m.edition_id
    WHERE b.edition_start_date IS NOT NULL
      AND b.edition_end_date IS NOT NULL
), scoped AS (
    SELECT
        s.*,
        c.league,
        c.season,
        c.season_start,
        c.season_end
    FROM iceberg.silver.transfermarkt_coach_stints_v2 s
    JOIN club_scopes c ON c.club_id = s.club_id
), ranked AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY coach_id, league, season
            ORDER BY COALESCE(left_date, DATE '9999-12-31') DESC,
                     COALESCE(appointed_date, DATE '0001-01-01') DESC,
                     _bronze_ingested_at DESC,
                     club_id ASC
        ) AS rn
    FROM scoped s
    WHERE (appointed_date IS NULL OR appointed_date <= season_end)
      AND (left_date IS NULL OR left_date >= season_start)
), manager_xref AS (
    SELECT
        CAST(source_id AS varchar) AS coach_id,
        CASE WHEN COUNT(DISTINCT canonical_id) FILTER (
                       WHERE confidence <> 'orphan'
                   ) = 1
             THEN MAX(canonical_id) FILTER (WHERE confidence <> 'orphan')
        END AS canonical_id
    FROM iceberg.silver.xref_manager
    WHERE source = 'transfermarkt'
    GROUP BY CAST(source_id AS varchar)
)
SELECT
    r.coach_id,
    x.canonical_id,
    COALESCE(p.name, r.name) AS name,
    r.role,
    p.dob,
    p.nationality,
    r.club_id AS current_club_id,
    r.club_name AS current_club_name,
    COALESCE(
        GREATEST(r._bronze_ingested_at, p._bronze_ingested_at),
        r._bronze_ingested_at,
        p._bronze_ingested_at
    )
        AS _bronze_ingested_at,
    r.league,
    r.season
FROM ranked r
LEFT JOIN iceberg.silver.transfermarkt_coach_profiles_v2 p
  ON p.coach_id = r.coach_id
LEFT JOIN manager_xref x ON x.coach_id = r.coach_id
WHERE r.rn = 1
""".strip()
    sql = rewrite_native_relations(sql, selected)
    return sql.replace(
        'iceberg.silver.transfermarkt_coaches_v2 AS',
        f'iceberg.silver.transfermarkt_coaches_v2_{selected} AS',
        1,
    )


def legacy_team_season_market_value_adapter_sql() -> str:
    """Retain the legacy current-snapshot SUM behind the native-shaped grain."""
    return """
CREATE OR REPLACE VIEW iceberg.gold.transfermarkt_team_season_market_value_legacy AS
WITH team_xref AS (
    SELECT
        source_id AS club_name,
        league,
        season,
        CASE WHEN COUNT(DISTINCT canonical_id) FILTER (
                       WHERE confidence <> 'orphan'
                   ) = 1
             THEN MAX(canonical_id) FILTER (WHERE confidence <> 'orphan')
        END AS team_id
    FROM iceberg.silver.xref_team
    WHERE source = 'transfermarkt'
    GROUP BY source_id, league, season
)
SELECT
    COALESCE(
        x.team_id,
        'tm_' || LOWER(REGEXP_REPLACE(
            p.current_club_name, '[^a-zA-Z0-9]+', '_'
        ))
    ) AS team_id,
    MAX(p.current_club_name) AS team_name,
    CAST(SUM(p.current_market_value_eur) AS bigint)
        AS squad_market_value_eur,
    COUNT_IF(p.current_market_value_eur IS NOT NULL) AS valued_players,
    COUNT(*) AS assigned_players,
    CAST(0 AS bigint) AS ambiguous_players_excluded,
    p.league,
    p.season
FROM iceberg.silver.transfermarkt_players_legacy p
LEFT JOIN team_xref x
  ON x.club_name = p.current_club_name
 AND x.league = p.league
 AND x.season = p.season
WHERE p.current_club_name IS NOT NULL
GROUP BY
    COALESCE(
        x.team_id,
        'tm_' || LOWER(REGEXP_REPLACE(
            p.current_club_name, '[^a-zA-Z0-9]+', '_'
        ))
    ),
    p.league,
    p.season
""".strip()


def compatibility_adapter_sql_all() -> list[str]:
    return [
        *(transfermarkt_players_adapter_sql(slot) for slot in SLOTS),
        *(transfermarkt_coaches_adapter_sql(slot) for slot in SLOTS),
        legacy_transfer_adapter_sql(),
        legacy_team_season_market_value_adapter_sql(),
    ]


def _relation_inventory(cur) -> dict[str, str]:
    cur.execute("""
SELECT table_schema, table_name, table_type
FROM iceberg.information_schema.tables
WHERE table_schema IN ('silver', 'gold')
""")
    return {
        f'iceberg.{schema}.{name}': str(table_type).upper()
        for schema, name, table_type in cur.fetchall()
    }


def _probe_relation(cur, relation: str) -> None:
    cur.execute(f'SELECT * FROM {relation} WHERE false')
    cur.fetchall()


def _relation_columns(cur, relation: str) -> list[tuple[str, str]]:
    _, schema, name = relation.split('.', 2)
    cur.execute(
        'SELECT column_name, data_type '
        'FROM iceberg.information_schema.columns '
        f'WHERE table_schema = {_sql_literal(schema)} '
        f'AND table_name = {_sql_literal(name)} ORDER BY ordinal_position'
    )
    return [(str(name), str(data_type)) for name, data_type in cur.fetchall()]


def verify_versioned_reader_schemas(cur) -> dict[str, Any]:
    report = {}
    for relation in CANONICAL_READER_RELATIONS:
        legacy = _relation_columns(cur, relation.legacy)
        native_a = _relation_columns(cur, relation.v2_a)
        native_b = _relation_columns(cur, relation.v2_b)
        report[relation.canonical] = {
            'legacy': legacy,
            'v2_a': native_a,
            'v2_b': native_b,
            'passed': (
                bool(legacy) and legacy == native_a and native_a == native_b
            ),
        }
    return {
        'passed': all(item['passed'] for item in report.values()),
        'relations': report,
    }


def reader_view_bootstrap_plan(cur) -> list[str]:
    """Plan safe one-time physical renames and stable reader views.

    Existing versioned sources make the operation idempotent.  If deployment
    ordering left both a canonical base table and a fresh legacy target, the
    canonical base is retained under a deterministic pre-v2 backup name.
    """
    inventory = _relation_inventory(cur)
    statements: list[str] = []

    # Seed both immutable build slots for every native model before any
    # canonical/legacy rename.  An already-built unslotted shadow is preserved
    # as slot A; the opposite slot is an empty schema clone and cannot become
    # active until a complete exact-cycle build + readiness manifest exists.
    for contract in MODEL_CONTRACTS:
        base = contract.output_table
        slot_a = slotted_relation(base, 'a')
        slot_b = slotted_relation(base, 'b')
        base_type = inventory.get(base)
        if base_type is not None:
            _probe_relation(cur, base)
        if inventory.get(slot_a) is not None:
            _probe_relation(cur, slot_a)
        if inventory.get(slot_b) is not None:
            _probe_relation(cur, slot_b)
        if inventory.get(slot_a) is None:
            if base_type in {'BASE TABLE', 'TABLE'}:
                _, _, slot_a_name = slot_a.split('.', 2)
                statements.append(f'ALTER TABLE {base} RENAME TO {slot_a_name}')
                inventory[slot_a] = base_type
                inventory.pop(base, None)
            elif inventory.get(slot_b) is not None:
                statements.append(
                    f"CREATE TABLE {slot_a} WITH (format = 'PARQUET') AS "
                    f'SELECT * FROM {slot_b} WHERE false'
                )
                inventory[slot_a] = 'BASE TABLE'
            else:
                raise StateInvariantError(
                    f'native model has no seed relation for slots: {base}'
                )
        elif base_type in {'BASE TABLE', 'TABLE'}:
            backup = f'{base}_pre_slots_backup'
            if inventory.get(backup) is not None:
                raise StateInvariantError(
                    f'unslotted native table and backup both exist: {base}, {backup}'
                )
            _, _, backup_name = backup.split('.', 2)
            statements.append(f'ALTER TABLE {base} RENAME TO {backup_name}')
            inventory[backup] = base_type
            inventory.pop(base, None)
        if inventory.get(slot_b) is None:
            statements.append(
                f"CREATE TABLE {slot_b} WITH (format = 'PARQUET') AS "
                f'SELECT * FROM {slot_a} WHERE false'
            )
            inventory[slot_b] = 'BASE TABLE'

    # These compatibility views are created later in the returned plan but
    # count as planned v2 slot relations for the fail-closed inventory check.
    for slot in SLOTS:
        inventory[f'iceberg.silver.transfermarkt_players_v2_{slot}'] = 'VIEW'
        inventory[f'iceberg.silver.transfermarkt_coaches_v2_{slot}'] = 'VIEW'
    planned_relations = {
        'iceberg.gold.transfermarkt_team_season_market_value_legacy',
    }
    for planned in planned_relations:
        inventory[planned] = 'VIEW'
    for relation in CANONICAL_READER_RELATIONS:
        legacy_source = LEGACY_SOURCE_RELATIONS[relation.canonical]
        canonical_type = inventory.get(relation.canonical)
        source_type = inventory.get(legacy_source)
        if source_type is not None and legacy_source not in planned_relations:
            _probe_relation(cur, legacy_source)
        elif canonical_type in {'BASE TABLE', 'TABLE'}:
            _probe_relation(cur, relation.canonical)
        if source_type is None:
            if canonical_type not in {'BASE TABLE', 'TABLE'}:
                raise StateInvariantError(
                    f'cannot seed {legacy_source}: canonical base table '
                    f'{relation.canonical} is missing'
                )
            _, _, target_name = legacy_source.split('.', 2)
            statements.append(
                f'ALTER TABLE {relation.canonical} RENAME TO {target_name}'
            )
            inventory[legacy_source] = canonical_type
            inventory.pop(relation.canonical, None)
        elif canonical_type in {'BASE TABLE', 'TABLE'}:
            backup = f'{relation.canonical}_pre_native_v2_backup'
            if inventory.get(backup) is not None:
                raise StateInvariantError(
                    f'canonical base and retained backup both exist: '
                    f'{relation.canonical}, {backup}'
                )
            _, _, backup_name = backup.split('.', 2)
            statements.append(
                f'ALTER TABLE {relation.canonical} RENAME TO {backup_name}'
            )
            inventory[backup] = canonical_type
            inventory.pop(relation.canonical, None)
        for slot in SLOTS:
            if inventory.get(relation.for_slot(slot)) is None:
                raise StateInvariantError(
                    'native versioned slot relation is missing: '
                    f'{relation.for_slot(slot)}'
                )

    # fct_transfer is the sole schema-changing migration.  Its retained
    # physical legacy table remains rebuildable; this adapter gives both
    # dynamic branches the exact native contract.
    statements.extend(compatibility_adapter_sql_all())
    inventory['iceberg.gold.fct_transfer_legacy'] = 'VIEW'
    statements.extend(canonical_reader_view_sql_all())
    return statements


def apply_reader_view_bootstrap(cur) -> dict[str, Any]:
    """Apply planned reader DDL with schema gate and reverse-rename recovery."""
    initial = _relation_inventory(cur)
    statements = reader_view_bootstrap_plan(cur)
    renames: list[tuple[str, str]] = []
    created_tables: list[str] = []
    schema_checked = False
    executed = 0
    try:
        for statement in statements:
            if (
                statement.startswith('CREATE OR REPLACE VIEW')
                and any(
                    statement.startswith(f'CREATE OR REPLACE VIEW {item.canonical} ')
                    for item in CANONICAL_READER_RELATIONS
                )
                and not schema_checked
            ):
                schema_report = verify_versioned_reader_schemas(cur)
                if not schema_report['passed']:
                    raise StateInvariantError(
                        f'versioned reader schema mismatch: {schema_report}'
                    )
                schema_checked = True
            pending_rename = None
            if statement.startswith('ALTER TABLE') and ' RENAME TO ' in statement:
                old = statement.split()[2]
                new_name = statement.rsplit(' ', 1)[-1]
                catalog, schema, _ = old.split('.', 2)
                pending_rename = (old, f'{catalog}.{schema}.{new_name}')
            pending_create = None
            if statement.startswith('CREATE TABLE '):
                pending_create = statement.split()[2]
            _drain(cur, statement)
            if pending_rename is not None:
                renames.append(pending_rename)
            if pending_create is not None and pending_create not in initial:
                created_tables.append(pending_create)
            executed += 1
        verified = verify_reader_views(cur)
        if not verified['passed']:
            raise StateInvariantError(
                f'canonical reader verification failed: {verified}'
            )
        return {
            'status': 'applied',
            'statements': executed,
            'schemas': verify_versioned_reader_schemas(cur),
            'views': verified,
        }
    except Exception as original:
        recovery_errors: list[str] = []
        # Remove only canonical views that were absent/base tables initially;
        # adapter views are harmless and keep the recovery observable.
        for relation in CANONICAL_READER_RELATIONS:
            if initial.get(relation.canonical) != 'VIEW':
                try:
                    _drain(cur, f'DROP VIEW IF EXISTS {relation.canonical}')
                except Exception as exc:  # noqa: BLE001 - recovery audit
                    recovery_errors.append(
                        f'drop {relation.canonical}: {exc}'
                    )
        for created in reversed(created_tables):
            try:
                _drain(cur, f'DROP TABLE IF EXISTS {created}')
            except Exception as exc:  # noqa: BLE001 - recovery audit
                recovery_errors.append(f'drop created {created}: {exc}')
        for old, renamed in reversed(renames):
            try:
                _, _, old_name = old.split('.', 2)
                _drain(cur, f'ALTER TABLE {renamed} RENAME TO {old_name}')
            except Exception as exc:  # noqa: BLE001 - recovery audit
                recovery_errors.append(f'reverse {renamed} -> {old}: {exc}')
        if recovery_errors:
            raise StateInvariantError(
                'reader bootstrap failed and reverse compensation was partial: '
                + '; '.join(recovery_errors)
            ) from original
        raise


def verify_reader_views(
    cur,
    *,
    expected_version: str | None = None,
    expected_revision: int | None = None,
    expected_slot: str | None = None,
    allow_static_slot: bool = False,
    require_no_legacy: bool = False,
) -> dict[str, Any]:
    """Verify every registered canonical view and, optionally, active route."""
    relations: dict[str, Any] = {}
    for relation in CANONICAL_READER_RELATIONS:
        cur.execute(f'SHOW CREATE VIEW {relation.canonical}')
        rows = list(cur.fetchall())
        ddl = ' '.join(str(value) for row in rows for value in row)
        normalised_ddl = ''.join(ddl.replace('"', '').lower().split())
        dynamic_passed = all(
            ''.join(item.lower().split()) in normalised_ddl
            for item in (
                STATE_TABLE, relation.legacy, relation.v2_a, relation.v2_b,
            )
        )
        v2_only_passed = bool(
            allow_static_slot
            and all(
                ''.join(item.lower().split()) in normalised_ddl
                for item in (STATE_TABLE, relation.v2_a, relation.v2_b)
            )
            and ''.join(relation.legacy.lower().split()) not in normalised_ddl
        )
        passed = all((
            bool(rows),
            v2_only_passed if require_no_legacy else (
                dynamic_passed or v2_only_passed
            ),
        ))
        relations[relation.canonical] = {
            'legacy': relation.legacy,
            'v2_a': relation.v2_a,
            'v2_b': relation.v2_b,
            'passed': passed,
        }
    route = None
    if expected_version is not None or expected_revision is not None:
        cur.execute(reader_selector_sql())
        rows = list(cur.fetchall())
        route = {
            'active_version': rows[0][0] if len(rows) == 1 else None,
            'active_slot': rows[0][1] if len(rows) == 1 else None,
            'revision': int(rows[0][2]) if len(rows) == 1 else None,
            'approved_cycle_id': rows[0][3] if len(rows) == 1 else None,
            'passed': bool(
                len(rows) == 1
                and (
                    expected_version is None
                    or rows[0][0] == expected_version
                )
                and (
                    expected_revision is None
                    or int(rows[0][2]) == int(expected_revision)
                )
                and (
                    expected_slot is None
                    or rows[0][1] == expected_slot
                )
            ),
        }
    return {
        'passed': (
            all(item['passed'] for item in relations.values())
            and (route is None or route['passed'])
        ),
        'relations': relations,
        'route': route,
    }


def read_reader_state(cur, *, allow_missing: bool = True) -> ReaderState:
    sql = f"""
SELECT state_key, active_version, active_slot,
       approved_cycle_id, approved_league, approved_season,
       approved_model_revision, approved_scope_set_id,
       previous_slot, previous_cycle_id, previous_league,
       previous_season, previous_model_revision, previous_scope_set_id,
       revision,
       activated_at, retention_until, rollback_verified_at,
       updated_at, updated_by, legacy_writers_disabled_at,
       cleanup_completed_at, slot_rollback_verified_at
FROM {STATE_TABLE}
WHERE state_key = '{STATE_KEY}'
"""
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    except Exception as exc:  # noqa: BLE001 - connector exception hierarchy varies
        if allow_missing and _table_missing(exc):
            return ReaderState(exists=False)
        raise
    if not rows:
        if allow_missing:
            return ReaderState(exists=False)
        raise StateInvariantError('Transfermarkt reader singleton is missing')
    if len(rows) != 1:
        raise StateInvariantError(
            f'Transfermarkt reader state is not a singleton: rows={len(rows)}'
        )
    row = rows[0]
    # Old-shape rows are accepted only for local rolling-deploy compatibility;
    # they remain legacy and cannot authorize a v2 route.  Production bootstrap
    # adds all slot columns before the state-selected views are installed.
    if len(row) == 11:
        state = ReaderState(
            exists=True,
            state_key=str(row[0]),
            active_version=str(row[1]),
            approved_cycle_id=(str(row[2]) if row[2] is not None else None),
            approved_league=(str(row[3]) if row[3] is not None else None),
            approved_season=(int(row[4]) if row[4] is not None else None),
            revision=int(row[5]),
            activated_at=row[6], retention_until=row[7],
            rollback_verified_at=row[8], updated_at=row[9],
            updated_by=(str(row[10]) if row[10] is not None else None),
        )
    elif len(row) == 15:
        state = ReaderState(
            exists=True,
            state_key=str(row[0]), active_version=str(row[1]),
            active_slot=(str(row[2]) if row[2] is not None else None),
            approved_cycle_id=(str(row[3]) if row[3] is not None else None),
            approved_league=(str(row[4]) if row[4] is not None else None),
            approved_season=(int(row[5]) if row[5] is not None else None),
            approved_model_revision=(int(row[6]) if row[6] is not None else None),
            revision=int(row[7]), activated_at=row[8], retention_until=row[9],
            rollback_verified_at=row[10], updated_at=row[11],
            updated_by=(str(row[12]) if row[12] is not None else None),
            legacy_writers_disabled_at=row[13], cleanup_completed_at=row[14],
        )
    elif len(row) == 21:
        state = ReaderState(
            exists=True,
            state_key=str(row[0]), active_version=str(row[1]),
            active_slot=(str(row[2]) if row[2] is not None else None),
            approved_cycle_id=(str(row[3]) if row[3] is not None else None),
            approved_league=(str(row[4]) if row[4] is not None else None),
            approved_season=(int(row[5]) if row[5] is not None else None),
            approved_model_revision=(int(row[6]) if row[6] is not None else None),
            previous_slot=(str(row[7]) if row[7] is not None else None),
            previous_cycle_id=(str(row[8]) if row[8] is not None else None),
            previous_league=(str(row[9]) if row[9] is not None else None),
            previous_season=(int(row[10]) if row[10] is not None else None),
            previous_model_revision=(int(row[11]) if row[11] is not None else None),
            revision=int(row[12]), activated_at=row[13], retention_until=row[14],
            rollback_verified_at=row[15], updated_at=row[16],
            updated_by=(str(row[17]) if row[17] is not None else None),
            legacy_writers_disabled_at=row[18], cleanup_completed_at=row[19],
            slot_rollback_verified_at=row[20],
        )
    elif len(row) == 23:
        state = ReaderState(
            exists=True,
            state_key=str(row[0]), active_version=str(row[1]),
            active_slot=(str(row[2]) if row[2] is not None else None),
            approved_cycle_id=(str(row[3]) if row[3] is not None else None),
            approved_league=(str(row[4]) if row[4] is not None else None),
            approved_season=(int(row[5]) if row[5] is not None else None),
            approved_model_revision=(int(row[6]) if row[6] is not None else None),
            approved_scope_set_id=(str(row[7]) if row[7] is not None else None),
            previous_slot=(str(row[8]) if row[8] is not None else None),
            previous_cycle_id=(str(row[9]) if row[9] is not None else None),
            previous_league=(str(row[10]) if row[10] is not None else None),
            previous_season=(int(row[11]) if row[11] is not None else None),
            previous_model_revision=(int(row[12]) if row[12] is not None else None),
            previous_scope_set_id=(
                str(row[13]) if row[13] is not None else None
            ),
            revision=int(row[14]), activated_at=row[15], retention_until=row[16],
            rollback_verified_at=row[17], updated_at=row[18],
            updated_by=(str(row[19]) if row[19] is not None else None),
            legacy_writers_disabled_at=row[20], cleanup_completed_at=row[21],
            slot_rollback_verified_at=row[22],
        )
    else:
        raise StateInvariantError(
            f'unsupported Transfermarkt reader state shape: columns={len(row)}'
        )
    if state.state_key != STATE_KEY:
        raise StateInvariantError(f'unexpected state key: {state.state_key!r}')
    if state.active_version not in {'legacy', 'v2'}:
        raise StateInvariantError(
            f'unsupported active_version: {state.active_version!r}'
        )
    if state.revision < 0:
        raise StateInvariantError(f'invalid negative revision: {state.revision}')
    if state.active_slot is not None and state.active_slot not in SLOTS:
        raise StateInvariantError(f'invalid active_slot: {state.active_slot!r}')
    if state.active_version == 'v2' and not all((
        state.active_slot in SLOTS,
        state.approved_cycle_id,
        state.approved_league,
        state.approved_season is not None,
        state.approved_model_revision is not None,
        state.approved_scope_set_id,
    )):
        raise StateInvariantError('v2 state has no complete approved cycle scope')
    for field_name, scope_set_id in (
        ('approved_scope_set_id', state.approved_scope_set_id),
        ('previous_scope_set_id', state.previous_scope_set_id),
    ):
        if scope_set_id is not None and not re.fullmatch(
            r'[0-9a-f]{64}', scope_set_id
        ):
            raise StateInvariantError(f'{field_name} is not a sha256 digest')
    previous_values = (
        state.previous_slot, state.previous_cycle_id, state.previous_league,
        state.previous_season, state.previous_model_revision,
        state.previous_scope_set_id,
    )
    if any(value is not None for value in previous_values) and not all(
        value is not None for value in previous_values
    ):
        raise StateInvariantError('previous-slot evidence is partial/malformed')
    if state.previous_slot is not None and (
        state.previous_slot not in SLOTS or state.previous_slot == state.active_slot
    ):
        raise StateInvariantError('previous_slot must be the inactive A/B slot')
    return state


def assert_reader_revision(cur, expected_revision: int) -> ReaderState:
    expected = _normalise_revision(expected_revision)
    state = read_reader_state(cur, allow_missing=(expected == 0))
    if state.revision != expected:
        raise RevisionConflict(
            f'Transfermarkt reader revision drift: expected={expected}, '
            f'actual={state.revision}'
        )
    return state


def candidate_build_slot(cur, expected_revision: int) -> str:
    """Pin the inactive build slot under the caller's reader CAS revision."""
    state = assert_reader_revision(cur, expected_revision)
    return inactive_slot(state)


def cycle_id_from_context(context: Mapping[str, Any]) -> str:
    """Read an explicitly propagated ingest cycle; never use this DAG's run id."""
    dag_run = context.get('dag_run')
    conf = getattr(dag_run, 'conf', None) or context.get('dag_run_conf') or {}
    value = conf.get('transfermarkt_cycle_id') or conf.get('cycle_id')
    return _normalise_cycle_id(value)


def cycle_scope_from_context(
    context: Mapping[str, Any],
) -> tuple[str, str, int]:
    """Read the explicit upstream cycle and its requested league/season scope."""
    cycle = cycle_id_from_context(context)
    dag_run = context.get('dag_run')
    conf = getattr(dag_run, 'conf', None) or context.get('dag_run_conf') or {}
    params = context.get('params') or {}
    league = conf.get('transfermarkt_league') or conf.get('league')
    season = conf.get('transfermarkt_season')
    if season is None:
        season = conf.get('season')
    # Airflow params are explicit trigger inputs propagated by the ingest DAG;
    # they are not inferred from timestamps/current season.
    if league is None:
        league = params.get('transfermarkt_league')
    if season is None:
        season = params.get('transfermarkt_season')
    league_value, season_value = _normalise_scope(league, season)
    return cycle, league_value, season_value


def parity_pairs() -> tuple[ParityPair, ...]:
    return PARITY_PAIRS


def _manifest_rows(
    cur,
    cycle_id: str,
    *,
    league: str,
    season: int,
) -> list[tuple[Any, ...]]:
    cycle = _normalise_cycle_id(cycle_id)
    expected_league, expected_season = _normalise_scope(league, season)
    sql = f"""
WITH ranked AS (
    SELECT m.*,
           ROW_NUMBER() OVER (
               PARTITION BY cycle_id, league, season, entity
               ORDER BY committed_at DESC
           ) AS rn
    FROM {DUAL_WRITE_MANIFEST_TABLE} m
    WHERE cycle_id = {_sql_literal(cycle)}
)
SELECT league, season, entity, legacy_table, native_table,
       legacy_batch_id, native_batch_id,
       legacy_rows, native_rows, legacy_hash, native_hash, status,
       committed_at,
       committed_at >= CURRENT_TIMESTAMP - INTERVAL '{READINESS_MAX_AGE_DAYS}' DAY
           AS is_fresh
FROM ranked
WHERE rn = 1
ORDER BY entity
"""
    cur.execute(sql)
    rows = list(cur.fetchall())
    # Include expected literals in the function contract and query text for
    # production audit logs while intentionally reading every scope attached
    # to the cycle.  Extra scopes then make the exact-set gate fail.
    _ = expected_league, expected_season
    return rows


def _validate_manifest_rows(
    rows: Sequence[Sequence[Any]],
    *,
    league: str,
    season: int,
    require_fresh: bool = True,
) -> tuple[dict, dict]:
    expected_league, expected_season = _normalise_scope(league, season)
    expected = {pair.name: pair for pair in PARITY_PAIRS}
    by_name = {str(row[2]): row for row in rows}
    exact_entities = set(by_name) == set(expected) and len(rows) == len(expected)
    report: dict[str, Any] = {}
    evidence: dict[str, dict[str, str]] = {}
    for name, pair in expected.items():
        row = by_name.get(name)
        if row is None:
            report[name] = {'passed': False, 'reason': 'missing'}
            continue
        (
            row_league, row_season, _, legacy_table, native_table,
            legacy_batch, native_batch,
            legacy_rows, native_rows, legacy_hash, native_hash, status,
            committed_at, is_fresh,
        ) = row
        passed = all((
            str(row_league) == expected_league,
            int(row_season) == expected_season,
            str(legacy_table) == pair.legacy_table,
            str(native_table) == pair.native_table,
            bool(legacy_batch),
            legacy_batch == native_batch,
            int(legacy_rows) == int(native_rows),
            bool(legacy_hash),
            legacy_hash == native_hash,
            status == 'success',
            bool(is_fresh) or not require_fresh,
        ))
        report[name] = {
            'passed': passed,
            'league': row_league,
            'season': int(row_season),
            'legacy_table': legacy_table,
            'native_table': native_table,
            'legacy_batch_id': legacy_batch,
            'native_batch_id': native_batch,
            'legacy_rows': int(legacy_rows),
            'native_rows': int(native_rows),
            'status': status,
            'committed_at': committed_at,
            'fresh': bool(is_fresh),
        }
        evidence[name] = {
            'legacy_batch_id': str(legacy_batch or ''),
            'native_batch_id': str(native_batch or ''),
            'legacy_rows': int(legacy_rows),
            'native_rows': int(native_rows),
            'legacy_hash': str(legacy_hash or ''),
            'native_hash': str(native_hash or ''),
        }
    summary = {
        'passed': exact_entities and all(item['passed'] for item in report.values()),
        'exact_entity_set': exact_entities,
        'expected_league': expected_league,
        'expected_season': expected_season,
        'freshness_required': require_fresh,
        'entities': report,
    }
    return summary, evidence


def _native_manifest_rows(
    cur, cycle_id: str, *, league: str, season: int,
) -> list[tuple[Any, ...]]:
    cycle = _normalise_cycle_id(cycle_id)
    expected_league, expected_season = _normalise_scope(league, season)
    cur.execute(f"""
WITH ranked AS (
    SELECT m.*, ROW_NUMBER() OVER (
        PARTITION BY cycle_id, league, season, entity
        ORDER BY committed_at DESC
    ) rn
    FROM {NATIVE_WRITE_MANIFEST_TABLE} m
    WHERE cycle_id = {_sql_literal(cycle)}
)
SELECT league, season, entity, native_table, native_batch_id,
       native_rows, native_hash, writer_revision, write_mode, status,
       committed_at,
       committed_at >= CURRENT_TIMESTAMP - INTERVAL '{READINESS_MAX_AGE_DAYS}' DAY
FROM ranked WHERE rn = 1 ORDER BY entity
""")
    _ = expected_league, expected_season
    return list(cur.fetchall())


def _validate_native_manifest(
    cur,
    rows: Sequence[Sequence[Any]],
    *,
    league: str,
    season: int,
    expected_revision: int,
    require_fresh: bool,
    require_live_batch: bool = True,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    expected_league, expected_season = _normalise_scope(league, season)
    revision = _normalise_revision(expected_revision)
    by_name = {str(row[2]): row for row in rows}
    exact = set(by_name) == set(PARITY_BY_NAME) and len(rows) == len(PARITY_BY_NAME)
    report: dict[str, Any] = {}
    evidence: dict[str, dict[str, Any]] = {}
    for name, pair in PARITY_BY_NAME.items():
        row = by_name.get(name)
        if row is None:
            report[name] = {'passed': False, 'reason': 'missing'}
            continue
        (
            row_league, row_season, _, native_table, batch_id,
            manifested_rows, manifested_hash, writer_revision, write_mode,
            status, committed_at, fresh,
        ) = row
        if require_live_batch:
            projection = pair._projection(
                table=pair.native_table,
                columns=pair.fingerprint_native_columns or pair.native_columns,
                batch_id=str(batch_id or ''),
            )
            cur.execute(projection)
            live_rows = list(cur.fetchall())
            live_count, live_hash = _fingerprint_rows(live_rows)
        else:
            live_count, live_hash = int(manifested_rows), str(manifested_hash or '')
        passed = all((
            str(row_league) == expected_league,
            int(row_season) == expected_season,
            str(native_table) == pair.native_table,
            bool(batch_id), int(manifested_rows) > 0,
            int(manifested_rows) == live_count,
            bool(manifested_hash), str(manifested_hash) == live_hash,
            int(writer_revision) == revision,
            write_mode == 'native-only', status == 'success',
            bool(fresh) or not require_fresh,
        ))
        report[name] = {
            'passed': passed, 'native_table': native_table,
            'native_batch_id': batch_id, 'native_rows': int(manifested_rows),
            'live_rows': live_count, 'live_hash': live_hash,
            'writer_revision': int(writer_revision), 'status': status,
            'fresh': bool(fresh), 'committed_at': committed_at,
            'live_batch_required': require_live_batch,
        }
        evidence[name] = {
            'native_batch_id': str(batch_id or ''),
            'native_rows': int(manifested_rows),
            'native_hash': str(manifested_hash or ''),
        }
    summary = {
        'passed': exact and all(item['passed'] for item in report.values()),
        'mode': 'native-only', 'exact_entity_set': exact,
        'live_batch_required': require_live_batch,
        'entities': report, 'expected_state_revision': revision,
    }
    return summary, evidence


def _fingerprint_rows(rows: Sequence[Sequence[Any]]) -> tuple[int, str]:
    """Match the runner's sorted DISTINCT JSON compatibility fingerprint."""
    normalised = sorted({
        tuple('__NULL__' if value is None else str(value) for value in row)
        for row in rows
    })
    payload = json.dumps(
        normalised, ensure_ascii=False, separators=(',', ':'),
    )
    return len(normalised), hashlib.sha256(payload.encode('utf-8')).hexdigest()


def run_parity(
    cur,
    cycle_id: str,
    *,
    league: str,
    season: int,
    manifest_evidence: Mapping[str, Mapping[str, str]] | None = None,
    require_fresh: bool = True,
) -> dict[str, dict[str, int | bool]]:
    cycle = _normalise_cycle_id(cycle_id)
    expected_league, expected_season = _normalise_scope(league, season)
    if manifest_evidence is None:
        manifest, manifest_evidence = _validate_manifest_rows(
            _manifest_rows(
                cur, cycle, league=expected_league, season=expected_season,
            ),
            league=expected_league,
            season=expected_season,
            require_fresh=require_fresh,
        )
        if not manifest['passed']:
            return {
                pair.name: {
                    'legacy_only': -1,
                    'native_only': -1,
                    'passed': False,
                    'manifest_ready': False,
                }
                for pair in PARITY_PAIRS
            }
    report: dict[str, dict[str, int | bool]] = {}
    for pair in PARITY_PAIRS:
        evidence = manifest_evidence.get(pair.name)
        if not evidence:
            report[pair.name] = {
                'legacy_only': -1, 'native_only': -1, 'passed': False,
            }
            continue
        legacy_sql, native_sql = pair.queries(
            legacy_batch_id=evidence['legacy_batch_id'],
            native_batch_id=evidence['native_batch_id'],
        )
        legacy_only = int(_scalar(cur, legacy_sql) or 0)
        native_only = int(_scalar(cur, native_sql) or 0)
        legacy_projection, native_projection = pair.projections(
            legacy_batch_id=evidence['legacy_batch_id'],
            native_batch_id=evidence['native_batch_id'],
            legacy_columns=(
                pair.fingerprint_legacy_columns or pair.legacy_columns
            ),
            native_columns=(
                pair.fingerprint_native_columns or pair.native_columns
            ),
        )
        cur.execute(legacy_projection)
        live_legacy_rows = list(cur.fetchall())
        cur.execute(native_projection)
        live_native_rows = list(cur.fetchall())
        legacy_count, legacy_hash = _fingerprint_rows(live_legacy_rows)
        native_count, native_hash = _fingerprint_rows(live_native_rows)
        manifest_live_match = all((
            legacy_count == int(evidence['legacy_rows']),
            native_count == int(evidence['native_rows']),
            legacy_hash == evidence['legacy_hash'],
            native_hash == evidence['native_hash'],
            legacy_count > 0,
            native_count > 0,
        ))
        report[pair.name] = {
            'legacy_only': legacy_only,
            'native_only': native_only,
            'live_legacy_rows': legacy_count,
            'live_native_rows': native_count,
            'live_manifest_match': manifest_live_match,
            'passed': (
                legacy_only == 0 and native_only == 0 and manifest_live_match
            ),
        }
    return report


def _model_manifest_report(
    cur,
    cycle_id: str,
    *,
    league: str | None = None,
    season: int | None = None,
    expected_revision: int,
    expected_slot: str,
    require_fresh: bool = True,
    require_current_snapshots: bool = True,
    scope_set_id: str | None = None,
) -> dict[str, Any]:
    cycle = _normalise_cycle_id(cycle_id)
    if league is None and season is None:
        expected_league, expected_season = None, None
    elif league is None or season is None:
        raise ValueError('league and season must be supplied together')
    else:
        expected_league, expected_season = _normalise_scope(league, season)
    revision_expected = _normalise_revision(expected_revision)
    expected_scope_set = (
        _normalise_scope_set_id(scope_set_id)
        if scope_set_id is not None else None
    )
    if expected_scope_set is not None:
        write_evidence_timestamp = (
            '(SELECT MAX(ss.committed_at) FROM '
            'iceberg.ops.transfermarkt_scope_set_manifest_v2 ss '
            f'WHERE ss.scope_set_id = {_sql_literal(expected_scope_set)})'
        )
    else:
        write_evidence_timestamp = (
            'COALESCE('
            f'(SELECT MAX(dw.committed_at) FROM {DUAL_WRITE_MANIFEST_TABLE} dw '
            f'WHERE dw.cycle_id = {_sql_literal(cycle)} '
            f'AND dw.league = {_sql_literal(expected_league)} '
            f'AND dw.season = {expected_season}), '
            f'(SELECT MAX(nw.committed_at) FROM {NATIVE_WRITE_MANIFEST_TABLE} nw '
            f'WHERE nw.cycle_id = {_sql_literal(cycle)} '
            f'AND nw.league = {_sql_literal(expected_league)} '
            f'AND nw.season = {expected_season})'
            ')'
        )
    sql = f"""
WITH ranked AS (
    SELECT m.*,
           ROW_NUMBER() OVER (
               PARTITION BY cycle_id, scope_set_id, model_name, candidate_slot
               ORDER BY committed_at DESC
           ) AS rn
    FROM {MODEL_MANIFEST_TABLE} m
    WHERE cycle_id = {_sql_literal(cycle)}
      {f"AND scope_set_id = {_sql_literal(expected_scope_set)}" if expected_scope_set else ''}
)
SELECT league, season, candidate_slot, model_name, source_tables, output_table,
       input_snapshot_ids, output_snapshot_id,
       row_count, key_hash, dq_status, state_revision, build_id, committed_at,
       scope_set_id, pinned_input_snapshot_ids,
       committed_at >= CURRENT_TIMESTAMP - INTERVAL '{READINESS_MAX_AGE_DAYS}' DAY
           AS is_fresh,
       committed_at >= {write_evidence_timestamp} AS after_dual_write
FROM ranked
WHERE rn = 1
ORDER BY model_name
"""
    cur.execute(sql)
    rows = list(cur.fetchall())
    by_name = {str(row[3]): row for row in rows}
    exact = set(by_name) == set(MODEL_BY_NAME) and len(rows) == len(MODEL_BY_NAME)
    report: dict[str, Any] = {}
    revisions: set[int] = set()
    build_ids: set[str] = set()
    candidate_slots: set[str] = set()
    scope_sets: set[str] = set()
    pinned_sets: set[str] = set()
    for name, contract in MODEL_BY_NAME.items():
        row = by_name.get(name)
        if row is None:
            report[name] = {'passed': False, 'reason': 'missing'}
            continue
        (
            row_league, row_season, candidate_slot, _, source_tables,
            output_table,
            input_snapshots, output_snapshot_id,
            row_count, key_hash, dq_status, revision, build_id,
            committed_at, row_scope_set, pinned_input_snapshots,
            is_fresh, after_dual_write,
        ) = row
        try:
            parsed_sources = json.loads(source_tables or '[]')
        except (TypeError, ValueError):
            parsed_sources = []
        if not isinstance(parsed_sources, list):
            parsed_sources = []
        try:
            parsed_inputs = json.loads(input_snapshots or '{}')
        except (TypeError, ValueError):
            parsed_inputs = {}
        if not isinstance(parsed_inputs, Mapping):
            parsed_inputs = {}
        try:
            parsed_pinned_inputs = json.loads(pinned_input_snapshots or '{}')
        except (TypeError, ValueError):
            parsed_pinned_inputs = {}
        if not isinstance(parsed_pinned_inputs, Mapping):
            parsed_pinned_inputs = {}
        revisions.add(int(revision))
        build_ids.add(str(build_id))
        candidate_slots.add(str(candidate_slot))
        scope_sets.add(str(row_scope_set or ''))
        pinned_sets.add(str(pinned_input_snapshots or ''))
        selected_slot = str(candidate_slot)
        required_sources = set(contract_source_tables(contract, selected_slot))
        expected_sources = list(contract_source_tables(contract, selected_slot))
        required_pins = required_pinned_source_tables()
        expected_output = contract_output_table(contract, selected_slot)
        current_output_snapshot = _table_snapshot_id(cur, expected_output)
        current_input_snapshots = {
            table: _table_snapshot_id(cur, table)
            for table in required_sources
        }
        current_output_snapshot_match = bool(
            int(output_snapshot_id) == current_output_snapshot
        )
        current_input_snapshots_match = bool(
            parsed_inputs == current_input_snapshots
        )
        current_pinned_inputs_match = bool(
            parsed_pinned_inputs
            and required_pins.issubset(parsed_pinned_inputs)
            and all(
                isinstance(table, str)
                and table
                and not isinstance(snapshot_id, bool)
                and int(snapshot_id) > 0
                for table, snapshot_id in parsed_pinned_inputs.items()
            )
            and all(
                _table_snapshot_id(cur, table) == int(snapshot_id)
                for table, snapshot_id in parsed_pinned_inputs.items()
            )
        )
        passed = all((
            (
                row_league is None and row_season is None
                if expected_league is None
                else (
                    str(row_league) == expected_league
                    and int(row_season) == expected_season
                )
            ),
            selected_slot == _normalise_slot(expected_slot),
            parsed_sources == expected_sources,
            str(output_table) == expected_output,
            output_snapshot_id is not None,
            required_sources == set(parsed_inputs),
            all(value is not None for value in parsed_inputs.values()),
            int(row_count) >= 0,
            bool(key_hash) or int(row_count) == 0,
            dq_status == 'success',
            bool(is_fresh) or not require_fresh,
            bool(after_dual_write),
            int(revision) == revision_expected,
            bool(re.fullmatch(r'[0-9a-f]{64}', str(row_scope_set or ''))),
            (
                expected_scope_set is None
                or str(row_scope_set) == expected_scope_set
            ),
            current_output_snapshot_match,
            current_input_snapshots_match or not require_current_snapshots,
            current_pinned_inputs_match or not require_current_snapshots,
        ))
        report[name] = {
            'passed': passed,
            'league': row_league,
            'season': int(row_season) if row_season is not None else None,
            'candidate_slot': selected_slot,
            'source_tables': parsed_sources,
            'output_table': output_table,
            'output_snapshot_id': output_snapshot_id,
            'row_count': int(row_count),
            'dq_status': dq_status,
            'state_revision': int(revision),
            'build_id': str(build_id),
            'scope_set_id': row_scope_set,
            'pinned_input_snapshot_ids': pinned_input_snapshots,
            'committed_at': committed_at,
            'fresh': bool(is_fresh),
            'after_dual_write': bool(after_dual_write),
            'current_output_snapshot_match': current_output_snapshot_match,
            'current_input_snapshots_match': current_input_snapshots_match,
            'current_pinned_inputs_match': current_pinned_inputs_match,
        }
    one_revision = revisions == {revision_expected}
    one_build = len(build_ids) == 1 and '' not in build_ids
    one_candidate_slot = candidate_slots == {_normalise_slot(expected_slot)}
    one_scope_set = len(scope_sets) == 1 and '' not in scope_sets
    one_pinned_set = len(pinned_sets) == 1 and '' not in pinned_sets
    return {
        'passed': exact and one_revision and one_build and one_candidate_slot
        and one_scope_set and one_pinned_set and all(
            item['passed'] for item in report.values()
        ),
        'exact_model_set': exact,
        'single_state_revision': one_revision,
        'single_build_id': one_build,
        'single_candidate_slot': one_candidate_slot,
        'single_scope_set_id': one_scope_set,
        'single_pinned_input_snapshot_set': one_pinned_set,
        'scope_set_id': next(iter(scope_sets)) if one_scope_set else None,
        'candidate_slot': (
            next(iter(candidate_slots)) if len(candidate_slots) == 1 else None
        ),
        'build_id': next(iter(build_ids)) if one_build else None,
        'state_revision': next(iter(revisions)) if one_revision else None,
        'expected_state_revision': revision_expected,
        'expected_league': expected_league,
        'expected_season': expected_season,
        'freshness_required': require_fresh,
        'current_snapshots_required': require_current_snapshots,
        'models': report,
    }


def _gold_contract_report(
    cur,
    *,
    league: str | None = None,
    season: int | None = None,
    candidate_slot: str,
) -> dict[str, Any]:
    slot = _normalise_slot(candidate_slot)
    contracts = {
        'fct_transfer_v2': (
            slotted_relation('iceberg.gold.fct_transfer_v2', slot),
            ('transfer_id', 'player_id'),
            ('transfer_id',), GOLD_MIN_ROWS['fct_transfer_v2'], None, None,
        ),
        'fct_player_market_value_v2': (
            slotted_relation('iceberg.gold.fct_player_market_value_v2', slot),
            ('player_id', 'valuation_date', 'source', 'market_value_eur'),
            ('player_id', 'valuation_date', 'source'),
            GOLD_MIN_ROWS['fct_player_market_value_v2'],
            ('source_transfermarkt_rows', "source = 'transfermarkt'", TRANSFERMARKT_MV_MIN_ROWS),
            'market_value_eur < 0',
        ),
        'dim_manager_v2': (
            slotted_relation('iceberg.gold.dim_manager_v2', slot),
            ('manager_id',), ('manager_id',),
            GOLD_MIN_ROWS['dim_manager_v2'], None, None,
        ),
        'team_season_market_value_v2': (
            slotted_relation(
                'iceberg.gold.transfermarkt_team_season_market_value_v2', slot,
            ),
            ('team_id', 'league', 'season', 'squad_market_value_eur'),
            ('team_id', 'league', 'season'),
            GOLD_MIN_ROWS['team_season_market_value_v2'], None,
            'squad_market_value_eur <= 0',
        ),
    }
    report: dict[str, Any] = {}
    for name, (
        table, required_columns, pk_columns, minimum_rows, branch,
        invalid_value_predicate,
    ) in contracts.items():
        row_count = int(_scalar(cur, f'SELECT COUNT(*) FROM {table}') or 0)
        null_predicate = ' OR '.join(
            f'{column} IS NULL' for column in required_columns
        )
        nulls = int(_scalar(
            cur, f'SELECT COUNT(*) FROM {table} WHERE {null_predicate}',
        ) or 0)
        pk_csv = ','.join(pk_columns)
        duplicates = int(_scalar(
            cur,
            f'SELECT COUNT(*) FROM (SELECT {pk_csv} FROM {table} '
            f'GROUP BY {pk_csv} HAVING COUNT(*) > 1)',
        ) or 0)
        branch_report = None
        branch_ok = True
        if branch:
            branch_name, predicate, branch_minimum = branch
            branch_rows = int(_scalar(
                cur, f'SELECT COUNT(*) FROM {table} WHERE {predicate}',
            ) or 0)
            branch_report = {
                'name': branch_name,
                'row_count': branch_rows,
                'minimum_rows': branch_minimum,
                'passed': branch_rows >= branch_minimum,
            }
            branch_ok = branch_report['passed']
        invalid_values = 0
        if invalid_value_predicate:
            invalid_values = int(_scalar(
                cur,
                f'SELECT COUNT(*) FROM {table} '
                f'WHERE {invalid_value_predicate}',
            ) or 0)
        report[name] = {
            'row_count': row_count,
            'minimum_rows': minimum_rows,
            'null_required_key_violations': nulls,
            'duplicate_pk_violations': duplicates,
            'branch': branch_report,
            'invalid_value_violations': invalid_values,
            'passed': (
                row_count >= minimum_rows
                and nulls == 0
                and duplicates == 0
                and branch_ok
                and invalid_values == 0
            ),
        }
    transfer_table = slotted_relation('iceberg.gold.fct_transfer_v2', slot)
    semantic_duplicates = int(_scalar(
        cur,
        'SELECT COUNT(*) FROM ('
        'SELECT player_id, transfer_date, event_season, from_team_id, to_team_id '
        f'FROM {transfer_table} '
        'GROUP BY player_id, transfer_date, event_season, from_team_id, to_team_id '
        'HAVING COUNT(*) > 1)',
    ) or 0)
    report['fct_transfer_v2']['semantic_duplicate_violations'] = semantic_duplicates
    report['fct_transfer_v2']['passed'] = bool(
        report['fct_transfer_v2']['passed'] and semantic_duplicates == 0
    )
    if (league is None) != (season is None):
        raise ValueError('league and season must be supplied together')
    requested_scope = league is not None
    if requested_scope:
        expected_league, expected_season = _normalise_scope(league, season)
        slug = _season_slug(expected_season)
        team_table = slotted_relation(
            'iceberg.gold.transfermarkt_team_season_market_value_v2', slot,
        )
        scoped_team_rows = int(_scalar(
            cur,
            f'SELECT COUNT(*) FROM {team_table} '
            f'WHERE league = {_sql_literal(expected_league)} '
            f'AND season = {_sql_literal(slug)} '
            'AND squad_market_value_eur > 0 AND valued_players > 0',
        ) or 0)
        report['team_season_market_value_v2']['requested_scope'] = {
            'league': expected_league,
            'season': slug,
            'row_count': scoped_team_rows,
            'minimum_rows': 15,
            'passed': scoped_team_rows >= 15,
        }
        report['team_season_market_value_v2']['passed'] = bool(
            report['team_season_market_value_v2']['passed']
            and scoped_team_rows >= 15
        )
    coach_stints = int(_scalar(
        cur,
        'SELECT COUNT(*) FROM '
        + slotted_relation('iceberg.silver.transfermarkt_coach_stints_v2', slot),
    ) or 0)
    report['native_coach_stints'] = {
        'row_count': coach_stints,
        'minimum_rows': GOLD_MIN_ROWS['dim_manager_v2'],
        'duplicate_pk_violations': 0,
        'passed': coach_stints >= GOLD_MIN_ROWS['dim_manager_v2'],
    }
    coach_stints_table = slotted_relation(
        'iceberg.silver.transfermarkt_coach_stints_v2', slot,
    )
    memberships_table = slotted_relation(
        'iceberg.silver.transfermarkt_squad_memberships_v2', slot,
    )
    if requested_scope:
        season_start = f'{expected_season}-07-01'
        season_end = f'{expected_season + 1}-06-30'
        scoped_coaches = int(_scalar(
            cur,
            'SELECT COUNT(DISTINCT s.coach_id) '
            f'FROM {coach_stints_table} s '
            f'JOIN (SELECT DISTINCT club_id FROM {memberships_table} '
            f'WHERE league = {_sql_literal(expected_league)} '
            f'AND season = {_sql_literal(slug)}) c ON c.club_id = s.club_id '
            'WHERE s.coach_id IS NOT NULL AND s.name IS NOT NULL '
            'AND (s.appointed_date IS NOT NULL OR s.left_date IS NOT NULL) '
            f"AND (s.appointed_date IS NULL OR s.appointed_date <= DATE '{season_end}') "
            f"AND (s.left_date IS NULL OR s.left_date >= DATE '{season_start}')",
        ) or 0)
        report['requested_scope_coaches'] = {
            'league': expected_league,
            'season': slug,
            'row_count': scoped_coaches,
            'minimum_rows': 15,
            'passed': scoped_coaches >= 15,
        }
    return report


def _season_slug(season: int) -> str:
    value = int(season)
    return f'{value % 100:02d}{(value + 1) % 100:02d}'


def _coverage_result(
    cur,
    sql: str,
    *,
    warn_threshold: float = 0.80,
    error_threshold: float = 0.60,
) -> dict[str, Any]:
    cur.execute(sql)
    rows = list(cur.fetchall())
    total = int(rows[0][0]) if rows else 0
    matched = int(rows[0][1]) if rows else 0
    ratio = (matched / total) if total else 0.0
    return {
        'total_rows': total,
        'matched_rows': matched,
        'ratio': ratio,
        'warn_threshold': warn_threshold,
        'error_threshold': error_threshold,
        'severity': (
            'PASS'
            if ratio >= warn_threshold
            else 'WARNING'
            if ratio >= error_threshold
            else 'ERROR'
        ),
        'passed': total > 0 and ratio >= error_threshold,
    }


def _canonical_coverage_report(
    cur,
    *,
    league: str,
    season: int,
    manifest_evidence: Mapping[str, Mapping[str, str]],
    candidate_slot: str,
) -> dict[str, Any]:
    slot = _normalise_slot(candidate_slot)
    league_value, season_value = _normalise_scope(league, season)
    slug = _season_slug(season_value)
    transfer_batch = manifest_evidence.get('transfer_events', {}).get(
        'native_batch_id',
    )
    memberships = slotted_relation(
        'iceberg.silver.transfermarkt_squad_memberships_v2', slot,
    )
    xref = slotted_relation(
        'iceberg.silver.transfermarkt_player_xref_global_v2', slot,
    )
    attributes = slotted_relation(
        'iceberg.silver.transfermarkt_player_attributes_v2', slot,
    )
    transfers = slotted_relation(
        'iceberg.silver.transfermarkt_transfer_events_v2', slot,
    )
    reports = {
        'squad_memberships': _coverage_result(cur, f"""
SELECT COUNT(*), COUNT_IF(EXISTS (
    SELECT 1 FROM {xref} x
    WHERE x.player_id = m.player_id AND x.resolution_status = 'resolved'
))
FROM {memberships} m
WHERE m.league = {_sql_literal(league_value)}
  AND m.season = {_sql_literal(slug)}
"""),
        'player_attributes': _coverage_result(cur, f"""
SELECT COUNT(*), COUNT_IF(canonical_id IS NOT NULL)
FROM {attributes}
WHERE observed_league = {_sql_literal(league_value)}
  AND observed_season = {_sql_literal(slug)}
"""),
    }
    if transfer_batch:
        reports['transfer_events'] = _coverage_result(cur, f"""
SELECT COUNT(*), COUNT_IF(canonical_id IS NOT NULL)
FROM {transfers}
WHERE _batch_id = {_sql_literal(transfer_batch)}
""")
    else:
        reports['transfer_events'] = {
            'total_rows': 0,
            'matched_rows': 0,
            'ratio': 0.0,
            'warn_threshold': 0.80,
            'error_threshold': 0.60,
            'severity': 'ERROR',
            'passed': False,
            'reason': 'missing manifest batch',
        }
    return reports


def _scope_set_evidence(
    cur,
    *,
    scope_set_id: str,
    expected_revision: int,
    require_fresh: bool,
    parent_cycle_id: str | None = None,
) -> tuple[dict[str, Any], tuple[Any, ...]]:
    """Rebuild exact child digests and validate every paid parent cycle."""

    from . import transfermarkt_scope_state as scope_state

    requested_parent = (
        _normalise_cycle_id(parent_cycle_id)
        if parent_cycle_id is not None else None
    )
    scope_set = _normalise_scope_set_id(scope_set_id)
    revision = _normalise_revision(expected_revision)
    cur.execute(f"""
SELECT registry_snapshot_id, capture_revision, parser_revision,
       schema_revision, reader_revision, scope_digests_json, traffic_json,
       status, committed_at,
       committed_at >= CURRENT_TIMESTAMP
           - INTERVAL '{READINESS_MAX_AGE_DAYS}' DAY AS is_fresh
FROM {scope_state.SCOPE_SET_MANIFEST_TABLE}
WHERE scope_set_id = {_sql_literal(scope_set)}
""")
    set_rows = list(cur.fetchall())
    if len(set_rows) != 1:
        raise ReadinessError(
            f'scope_set_id must have exactly one ops row; got {len(set_rows)}'
        )
    (
        registry_snapshot, capture_revision, parser_revision,
        schema_revision, row_revision, scope_digests_json, traffic_json,
        set_status, committed_at, set_fresh,
    ) = set_rows[0]
    try:
        raw_digests = json.loads(scope_digests_json)
        raw_traffic = json.loads(traffic_json)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ReadinessError('persisted scope-set JSON is invalid') from exc
    if (
        not isinstance(raw_digests, list)
        or not 1 <= len(raw_digests) <= MAX_SCOPE_SET_SIZE
    ):
        raise ReadinessError(
            f'persisted scope digests must contain 1..{MAX_SCOPE_SET_SIZE} rows'
        )
    scope_digests_list: list[tuple[str, str]] = []
    for item in raw_digests:
        if not isinstance(item, list) or len(item) != 2:
            raise ReadinessError('each persisted scope digest must be a two-item array')
        scope_id, digest = str(item[0]).strip(), str(item[1]).strip()
        if not scope_id or not re.fullmatch(r'[0-9a-f]{64}', digest):
            raise ReadinessError('persisted scope digest identity is invalid')
        scope_digests_list.append((scope_id, digest))
    scope_digests = tuple(scope_digests_list)
    if tuple(sorted(scope_digests)) != scope_digests:
        raise ReadinessError('persisted scope digests are unordered')
    if (
        len({item[0] for item in scope_digests}) != len(scope_digests)
        or len({item[1] for item in scope_digests}) != len(scope_digests)
    ):
        raise ReadinessError('persisted scope digests contain duplicates')
    traffic_fields = {
        'decoded_bytes', 'wire_bytes', 'provider_metered_bytes', 'requests',
        'retries', 'cache_hits', 'duration_ms',
    }
    if not isinstance(raw_traffic, Mapping) or set(raw_traffic) != traffic_fields:
        raise ReadinessError('scope-set traffic JSON has an unbound field set')
    persisted_set = scope_state.ScopeSetManifest(
        scope_set_id=scope_set,
        registry_snapshot_id=str(registry_snapshot),
        capture_revision=str(capture_revision),
        parser_revision=str(parser_revision),
        schema_revision=str(schema_revision),
        reader_revision=int(row_revision),
        scope_digests=scope_digests,
    )
    identity_payload = {
        'registry_snapshot_id': persisted_set.registry_snapshot_id,
        'capture_revision': persisted_set.capture_revision,
        'parser_revision': persisted_set.parser_revision,
        'schema_revision': persisted_set.schema_revision,
        'reader_revision': persisted_set.reader_revision,
        'scope_digests': persisted_set.scope_digests,
    }
    if scope_state.stable_hash(identity_payload) != scope_set:
        raise ReadinessError('persisted scope-set identity hash drifted')
    if int(row_revision) != revision:
        raise RevisionConflict(
            'scope-set reader revision differs from requested revision'
        )
    if set_status != 'success':
        raise ReadinessError('scope-set ops row is not successful')
    if require_fresh and not bool(set_fresh):
        raise ReadinessError('scope-set ops row is stale')

    digest_sql = ', '.join(_sql_literal(item[1]) for item in scope_digests)
    cur.execute(f"""
SELECT parent_cycle_id, child_cycle_id, scope_id, competition_id, edition_id,
       canonical_competition_id, canonical_season, registry_snapshot_id,
       capture_revision, parser_revision, schema_revision, reader_revision,
       entity_manifest_json, manifest_digest, status, committed_at,
       committed_at >= CURRENT_TIMESTAMP
           - INTERVAL '{READINESS_MAX_AGE_DAYS}' DAY AS is_fresh
FROM {scope_state.SCOPE_MANIFEST_TABLE}
WHERE manifest_digest IN ({digest_sql})
ORDER BY scope_id, committed_at
""")
    rows = list(cur.fetchall())
    expected_digests = dict(scope_digests)
    if len(rows) != len(expected_digests):
        raise ReadinessError(
            'exact scope-row count differs from immutable scope set'
        )
    manifests = []
    row_evidence: dict[str, Any] = {}
    for row in rows:
        (
            row_parent, child, scope_id, competition, edition,
            canonical_competition, canonical_season, row_registry,
            row_capture, row_parser, row_schema, row_reader,
            entity_json, manifest_digest, status, row_committed_at, row_fresh,
        ) = row
        try:
            entity_value = json.loads(entity_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ReadinessError('scope entity manifest JSON is invalid') from exc
        if isinstance(entity_value, Mapping):
            if set(entity_value) != {'entities', 'dq_evidence'}:
                raise ReadinessError('scope entity JSON contains unbound fields')
            dq_evidence = entity_value['dq_evidence']
            entity_value = entity_value['entities']
        else:
            raise ReadinessError('scope entity JSON lacks immutable DQ evidence')
        value = {
            'parent_cycle_id': row_parent,
            'child_cycle_id': child,
            'scope_id': scope_id,
            'competition_id': competition,
            'edition_id': edition,
            'canonical_competition_id': canonical_competition,
            'canonical_season': canonical_season,
            'registry_snapshot_id': row_registry,
            'capture_revision': row_capture,
            'parser_revision': row_parser,
            'schema_revision': row_schema,
            'reader_revision': row_reader,
            'entities': entity_value,
            'dq_evidence': dq_evidence,
        }
        try:
            manifest = scope_state.ScopeManifest.from_mapping(value)
            manifest.validate(NATIVE_ENTITIES)
        except (scope_state.ScopeManifestError, TypeError, ValueError) as exc:
            raise ReadinessError(f'invalid child scope manifest: {exc}') from exc
        if status != scope_state.SCOPE_COMPLETION_STATUS:
            raise ReadinessError(f'{scope_id}: child scope is not successful')
        if require_fresh and not bool(row_fresh):
            raise ReadinessError(f'{scope_id}: child scope manifest is stale')
        if manifest.digest != str(manifest_digest):
            raise ReadinessError(f'{scope_id}: child manifest digest drifted')
        if expected_digests.get(str(scope_id)) != manifest.digest:
            raise ReadinessError(f'{scope_id}: child is outside immutable set')
        for entity in manifest.entities:
            if entity.expected_rows is None or int(entity.expected_rows) != int(
                entity.dedup_rows
            ):
                raise ReadinessError(
                    f'{scope_id}:{entity.entity}: expected row evidence drifted'
                )
            if entity.applicability_status != 'ok' and (
                entity.raw_rows != 0 or entity.dedup_rows != 0
            ):
                raise ReadinessError(
                    f'{scope_id}:{entity.entity}: terminal empty contains rows'
                )
            if not all(re.fullmatch(r'[0-9a-f]{64}', value or '') for value in (
                entity.key_hash, entity.content_hash,
            )):
                raise ReadinessError(
                    f'{scope_id}:{entity.entity}: content hash is not sha256'
                )
        manifests.append(manifest)
        row_evidence[str(scope_id)] = {
            'parent_cycle_id': str(row_parent),
            'child_cycle_id': str(child),
            'manifest_digest': manifest.digest,
            'participant_dq': dict(
                manifest.dq_evidence['participant_contract']
            ),
            'committed_at': row_committed_at,
            'fresh': bool(row_fresh),
        }
    if len({item.child_cycle_id for item in manifests}) != len(manifests):
        raise ReadinessError('scope set reuses a child cycle id')
    identities = {
        (item.competition_id, item.edition_id) for item in manifests
    }
    if len(identities) != len(manifests):
        raise ReadinessError(
            'scope set duplicates a competition/edition identity'
        )
    try:
        rebuilt = scope_state.ScopeSetManifest.build(
            manifests,
            expected_entities=NATIVE_ENTITIES,
            reader_revision=revision,
        )
    except scope_state.ScopeManifestError as exc:
        raise ReadinessError(f'scope-set rebuild failed: {exc}') from exc
    if rebuilt != persisted_set:
        raise ReadinessError('child rows do not rebuild persisted scope set')
    traffic = scope_state.aggregate_traffic(manifests)
    expected_traffic = {key: int(value) for key, value in traffic.items()}
    try:
        persisted_traffic = {
            key: int(raw_traffic[key]) for key in expected_traffic
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise ReadinessError('scope-set traffic JSON is incomplete') from exc
    if persisted_traffic != expected_traffic:
        raise ReadinessError('scope-set traffic differs from child manifests')
    hard_cap, soft_cap = SCOPE_SET_HARD_BYTE_CAP, SCOPE_SET_SOFT_BYTE_STOP
    manifests_by_parent = {
        parent: tuple(
            item for item in manifests if item.parent_cycle_id == parent
        )
        for parent in sorted({item.parent_cycle_id for item in manifests})
    }
    parent_traffic = {
        parent: scope_state.aggregate_traffic(parent_manifests)
        for parent, parent_manifests in manifests_by_parent.items()
    }
    for parent, metrics in parent_traffic.items():
        if metrics['provider_metered_bytes'] > hard_cap:
            raise ReadinessError(
                f'{parent}: parent cycle exceeds the provider hard byte cap'
            )
        if metrics['requests'] > SCOPE_SET_REQUEST_LIMIT:
            raise ReadinessError(
                f'{parent}: parent cycle exceeds the approved request limit'
            )
        if metrics['retries'] > SCOPE_SET_RETRY_LIMIT:
            raise ReadinessError(
                f'{parent}: parent cycle exceeds the approved retry limit'
            )

    parent_sql = ', '.join(
        _sql_literal(parent) for parent in manifests_by_parent
    )
    cur.execute(f"""
SELECT parent_cycle_id, entity, decoded_bytes, wire_bytes,
       provider_metered_bytes, requests,
       retries, cache_hits, duration_ms, hard_limit_bytes, soft_limit_bytes
FROM {scope_state.PROXY_LEDGER_TABLE}
WHERE parent_cycle_id IN ({parent_sql})
ORDER BY parent_cycle_id, entity
""")
    ledger_rows = list(cur.fetchall())
    by_parent_entity = {
        (parent, entity): {
            field: sum(
                int(getattr(manifest_entity, field))
                for manifest in parent_manifests
                for manifest_entity in manifest.entities
                if manifest_entity.entity == entity
            )
            for field in (
                'decoded_bytes', 'wire_bytes', 'provider_metered_bytes',
                'requests', 'retries', 'cache_hits', 'duration_ms',
            )
        }
        for parent, parent_manifests in manifests_by_parent.items()
        for entity in NATIVE_ENTITIES
    }
    if len(ledger_rows) != len(by_parent_entity):
        raise ReadinessError('per-parent proxy ledger entity set is incomplete')
    seen_ledger = set()
    for row in ledger_rows:
        row_parent, entity, *values, row_hard_cap, row_soft_cap = row
        key = (str(row_parent), str(entity))
        if key in seen_ledger or key not in by_parent_entity:
            raise ReadinessError(
                'per-parent proxy ledger has duplicate/unknown entity'
            )
        seen_ledger.add(key)
        actual = dict(zip(
            (
                'decoded_bytes', 'wire_bytes', 'provider_metered_bytes',
                'requests', 'retries', 'cache_hits', 'duration_ms',
            ),
            (int(value) for value in values),
            strict=True,
        ))
        if actual != by_parent_entity[key]:
            raise ReadinessError(
                f'{key[0]}:{key[1]}: parent proxy ledger drifted'
            )
        if int(row_hard_cap) != hard_cap or int(row_soft_cap) != soft_cap:
            raise ReadinessError(
                f'{key[0]}: parent proxy ledger budget drifted'
            )

    parent_reports = {
        parent: {
            'traffic': metrics,
            'hard_provider_byte_cap': hard_cap,
            'soft_provider_byte_stop': soft_cap,
            'request_limit': SCOPE_SET_REQUEST_LIMIT,
            'retry_limit': SCOPE_SET_RETRY_LIMIT,
            'proxy_ledger_exact': True,
        }
        for parent, metrics in parent_traffic.items()
    }

    return ({
        'passed': True,
        'scope_set_id': scope_set,
        'requested_parent_cycle_id': requested_parent,
        'parent_cycle_ids': list(manifests_by_parent),
        'reader_revision': revision,
        'registry_snapshot_id': persisted_set.registry_snapshot_id,
        'scope_count': len(manifests),
        'scope_digests': [list(item) for item in scope_digests],
        'traffic': traffic,
        'aggregate_traffic_is_metric_only': True,
        'parent_cycles': parent_reports,
        'committed_at': committed_at,
        'fresh': bool(set_fresh),
        'scopes': row_evidence,
        'proxy_ledger_exact': True,
    }, tuple(manifests))


def _scope_write_manifest_report(
    cur,
    *,
    manifests: Sequence[Any],
    expected_revision: int,
    write_mode: str,
    require_fresh: bool,
) -> dict[str, Any]:
    """Validate each exact child parity/native manifest and its live batches."""

    if write_mode not in {'dual', 'native-only'}:
        raise ValueError(f'unsupported scope-set write mode: {write_mode}')
    child_ids = tuple(str(item.child_cycle_id) for item in manifests)
    child_sql = ','.join(_sql_literal(value) for value in child_ids)
    if write_mode == 'dual':
        cur.execute(f"""
SELECT cycle_id, entity, legacy_table, native_table,
       legacy_batch_id, native_batch_id,
       legacy_rows, native_rows, legacy_hash, native_hash, status,
       committed_at,
       committed_at >= CURRENT_TIMESTAMP
           - INTERVAL '{READINESS_MAX_AGE_DAYS}' DAY AS is_fresh
FROM {DUAL_WRITE_MANIFEST_TABLE}
WHERE cycle_id IN ({child_sql})
ORDER BY cycle_id, entity
""")
    else:
        cur.execute(f"""
SELECT cycle_id, entity, native_table, native_batch_id, native_rows,
       native_hash, writer_revision, write_mode, status, committed_at,
       committed_at >= CURRENT_TIMESTAMP
           - INTERVAL '{READINESS_MAX_AGE_DAYS}' DAY AS is_fresh
FROM {NATIVE_WRITE_MANIFEST_TABLE}
WHERE cycle_id IN ({child_sql})
ORDER BY cycle_id, entity
""")
    rows = list(cur.fetchall())
    expected = {
        (str(manifest.child_cycle_id), entity.entity): entity
        for manifest in manifests
        for entity in manifest.entities
    }
    child_revisions = {
        str(manifest.child_cycle_id): int(manifest.reader_revision)
        for manifest in manifests
    }
    if len(rows) != len(expected):
        raise ReadinessError('exact child write-manifest row count differs')
    report: dict[str, Any] = {}
    seen = set()
    for row in rows:
        if write_mode == 'dual':
            (
                child, entity, legacy_table, native_table,
                legacy_batch, native_batch,
                legacy_rows, native_rows, legacy_hash, native_hash, status,
                committed_at, is_fresh,
            ) = row
            writer_revision = child_revisions[str(child)]
        else:
            (
                child, entity, native_table, native_batch, native_rows,
                native_hash, writer_revision, row_write_mode, status,
                committed_at, is_fresh,
            ) = row
            legacy_batch = legacy_rows = legacy_hash = None
        key = (str(child), str(entity))
        evidence = expected.get(key)
        if evidence is None or key in seen:
            raise ReadinessError('write manifest has duplicate/unknown child entity')
        seen.add(key)
        if status != 'success' or (require_fresh and not bool(is_fresh)):
            raise ReadinessError(f'{key}: write manifest is red or stale')
        if (
            int(native_rows) != int(evidence.dedup_rows)
            or str(native_hash) != evidence.key_hash
            or int(writer_revision) != child_revisions[str(child)]
            or int(writer_revision) > int(expected_revision)
        ):
            raise ReadinessError(f'{key}: native write evidence drifted')
        pair = PARITY_BY_NAME[str(entity)]
        if str(native_table) != pair.native_table:
            raise ReadinessError(f'{key}: native write table drifted')
        if write_mode == 'dual':
            if str(legacy_table) != pair.legacy_table:
                raise ReadinessError(f'{key}: legacy write table drifted')
            if not all((
                str(legacy_batch) == str(native_batch),
                int(legacy_rows) == int(native_rows),
                str(legacy_hash) == str(native_hash),
            )):
                raise ReadinessError(f'{key}: bidirectional parity is red')
            legacy_sql, native_sql = pair.queries(
                legacy_batch_id=str(legacy_batch),
                native_batch_id=str(native_batch),
            )
            legacy_only = int(_scalar(cur, legacy_sql) or 0)
            native_only = int(_scalar(cur, native_sql) or 0)
            legacy_projection, native_projection = pair.projections(
                legacy_batch_id=str(legacy_batch),
                native_batch_id=str(native_batch),
                legacy_columns=(
                    pair.fingerprint_legacy_columns or pair.legacy_columns
                ),
                native_columns=(
                    pair.fingerprint_native_columns or pair.native_columns
                ),
            )
            cur.execute(legacy_projection)
            live_legacy = _fingerprint_rows(list(cur.fetchall()))
        else:
            if str(row_write_mode) != 'native-only':
                raise ReadinessError(f'{key}: native write mode drifted')
            native_projection = pair._projection(
                table=pair.native_table,
                columns=(
                    pair.fingerprint_native_columns or pair.native_columns
                ),
                batch_id=str(native_batch),
            )
            legacy_only = native_only = 0
            live_legacy = None
        cur.execute(native_projection)
        live_native = _fingerprint_rows(list(cur.fetchall()))
        expected_fingerprint = (int(native_rows), str(native_hash))
        live_match = live_native == expected_fingerprint
        if write_mode == 'dual':
            live_match = bool(
                live_match
                and live_legacy == expected_fingerprint
                and legacy_only == 0
                and native_only == 0
            )
        if not live_match:
            raise ReadinessError(f'{key}: live Bronze batch changed')
        report[f'{key[0]}:{key[1]}'] = {
            'passed': True,
            'native_rows': int(native_rows),
            'native_hash': str(native_hash),
            'legacy_only': legacy_only,
            'native_only': native_only,
            'fresh': bool(is_fresh),
            'committed_at': committed_at,
        }
    return {
        'passed': len(seen) == len(expected),
        'write_mode': write_mode,
        'expected_rows': len(expected),
        'rows': report,
    }


def _scope_set_coverage_report(
    cur,
    *,
    manifests: Sequence[Any],
    candidate_slot: str,
) -> dict[str, Any]:
    """Recheck canonical player coverage separately for every exact scope."""

    slot = _normalise_slot(candidate_slot)
    memberships = slotted_relation(
        'iceberg.silver.transfermarkt_squad_memberships_v2', slot,
    )
    observations = slotted_relation(
        'iceberg.silver.transfermarkt_player_attribute_observations_v2', slot,
    )
    transfers = slotted_relation(
        'iceberg.silver.transfermarkt_transfer_events_v2', slot,
    )
    xref = slotted_relation(
        'iceberg.silver.transfermarkt_player_xref_global_v2', slot,
    )
    report: dict[str, Any] = {}
    for manifest in manifests:
        participant_contract = manifest.dq_evidence['participant_contract']
        competition_type = str(participant_contract['competition_type'])
        strict = bool(participant_contract['strict'])
        predicate = (
            f"competition_id = {_sql_literal(manifest.competition_id)} "
            f"AND edition_id = {_sql_literal(manifest.edition_id)}"
        )
        transfer_predicate = (
            f"source_competition_id = {_sql_literal(manifest.competition_id)} "
            f"AND source_edition_id = {_sql_literal(manifest.edition_id)}"
        )
        entities = {item.entity: item for item in manifest.entities}
        strict_thresholds = {
            'warn_threshold': 1.0,
            'error_threshold': 1.0,
        }
        default_thresholds = {
            'warn_threshold': 0.80,
            'error_threshold': 0.60,
        }
        transfer_evidence = entities['transfer_events']
        transfer_is_nonempty = bool(
            transfer_evidence.applicability_status == 'ok'
            and transfer_evidence.dedup_rows > 0
        )
        checks = {
            'squad_memberships': _coverage_result(cur, f"""
SELECT COUNT(*), COUNT_IF(EXISTS (
    SELECT 1 FROM {xref} x
    WHERE x.player_id = m.player_id AND x.resolution_status = 'resolved'
)) FROM {memberships} m WHERE {predicate}
""", **(strict_thresholds if strict else default_thresholds)),
            'player_attribute_observations': _coverage_result(cur, f"""
SELECT COUNT(*), COUNT_IF(EXISTS (
    SELECT 1 FROM {xref} x
    WHERE x.player_id = o.player_id AND x.resolution_status = 'resolved'
)) FROM {observations} o WHERE {predicate}
""", **(strict_thresholds if strict else default_thresholds)),
            'transfer_events': _coverage_result(cur, f"""
SELECT COUNT(*), COUNT_IF(canonical_id IS NOT NULL)
FROM {transfers} WHERE {transfer_predicate}
""", **(
                strict_thresholds
                if strict and transfer_is_nonempty
                else default_thresholds
            )),
        }
        for entity, item in checks.items():
            evidence = entities[entity]
            if evidence.applicability_status == 'authoritative_empty':
                proof = manifest.dq_evidence[
                    'authoritative_empty_evidence'
                ].get(entity)
                proof_valid = bool(
                    isinstance(proof, Mapping)
                    and set(proof) == {'kind', 'result_sha256'}
                    and proof['kind'] in {'typed_fetch_state', 'cache_complete'}
                    and re.fullmatch(
                        r'[0-9a-f]{64}', str(proof['result_sha256'])
                    )
                )
                item['passed'] = bool(
                    item['total_rows'] == 0 and proof_valid
                )
                item['severity'] = 'PASS' if item['passed'] else 'ERROR'
                item['terminal_status'] = evidence.applicability_status
                item['authoritative_empty_proof_valid'] = proof_valid
            elif evidence.applicability_status == 'not_applicable':
                item['passed'] = item['total_rows'] == 0
                item['severity'] = 'PASS' if item['passed'] else 'ERROR'
                item['terminal_status'] = evidence.applicability_status
            elif item['total_rows'] <= 0:
                item['passed'] = False
                item['severity'] = 'ERROR'
                item['reason'] = 'applicable scope materialized zero rows'
        report[manifest.scope_id] = {
            'passed': all(item['passed'] for item in checks.values()),
            'competition_type': competition_type,
            'strict': strict,
            'entities': checks,
        }
    return {
        'passed': bool(report) and all(item['passed'] for item in report.values()),
        'scopes': report,
    }


def _scope_competition_type_dq_report(
    manifests: Sequence[Any],
) -> dict[str, Any]:
    """Expose the immutable per-type participant/entity thresholds at readiness."""

    scopes: dict[str, Any] = {}
    for manifest in manifests:
        dq = manifest.dq_evidence
        participant = dq['participant_contract']
        contracts = dq['entity_contracts']
        statuses = dq['entity_statuses']
        entity_rows = {
            item.entity: int(item.dedup_rows) for item in manifest.entities
        }
        required = {
            entity for entity, contract in contracts.items()
            if int(contract['minimum_rows']) > 0
        }
        met = {
            entity for entity in required
            if statuses[entity] == 'ok'
            and entity_rows[entity] >= int(contracts[entity]['minimum_rows'])
        }
        required_coverage = len(met) / len(required) if required else 1.0
        empty_entities = {
            entity for entity, status in statuses.items()
            if status == 'authoritative_empty'
        }
        proven_empty = set(dq['authoritative_empty_evidence'])
        empty_coverage = (
            len(proven_empty & empty_entities) / len(empty_entities)
            if empty_entities else 1.0
        )
        participant_coverage = float(participant['participant_coverage'])
        participant_minimum = float(
            participant['minimum_participant_coverage']
        )
        passed = bool(
            participant['passed']
            and participant_coverage >= participant_minimum
            and required_coverage >= 1.0
            and empty_coverage >= 1.0
            and proven_empty == empty_entities
        )
        scopes[manifest.scope_id] = {
            'passed': passed,
            'competition_type': participant['competition_type'],
            'strict': bool(participant['strict']),
            'participant_coverage': participant_coverage,
            'minimum_participant_coverage': participant_minimum,
            'required_entity_coverage': required_coverage,
            'minimum_required_entity_coverage': 1.0,
            'authoritative_empty_evidence_coverage': empty_coverage,
            'minimum_authoritative_empty_evidence_coverage': 1.0,
            'required_entities': sorted(required),
            'authoritative_empty_entities': sorted(empty_entities),
        }
    return {
        'passed': bool(scopes) and all(item['passed'] for item in scopes.values()),
        'scopes': scopes,
    }


def _scope_set_readiness(
    cur,
    cycle_id: str,
    *,
    scope_set_id: str,
    parent_cycle_id: str | None,
    expected_revision: int,
    require_fresh: bool,
    require_current_snapshots: bool,
    candidate_slot_override: str | None,
    allow_retained_rollback_slot: bool,
    allow_previous_slot: bool,
) -> dict[str, Any]:
    """Evaluate production readiness for a bounded immutable set of scopes."""

    cycle = _normalise_cycle_id(cycle_id)
    requested_parent = (
        _normalise_cycle_id(parent_cycle_id)
        if parent_cycle_id is not None else None
    )
    scope_set = _normalise_scope_set_id(scope_set_id)
    revision = _normalise_revision(expected_revision)
    state = read_reader_state(cur, allow_missing=False)
    approved_active_cycle = bool(
        state.active_version == 'v2'
        and state.approved_cycle_id == cycle
        and state.approved_scope_set_id == scope_set
        and state.approved_model_revision == revision
        and state.active_slot in SLOTS
    )
    retained_rollback_cycle = bool(
        allow_retained_rollback_slot
        and state.active_version == 'legacy'
        and state.rollback_verified_at is not None
        and state.approved_cycle_id == cycle
        and state.approved_scope_set_id == scope_set
        and state.approved_model_revision == revision
        and state.active_slot in SLOTS
        and candidate_slot_override == state.active_slot
    )
    previous_cycle = bool(
        allow_previous_slot
        and state.active_version == 'v2'
        and state.previous_slot in SLOTS
        and candidate_slot_override == state.previous_slot
        and state.previous_cycle_id == cycle
        and state.previous_scope_set_id == scope_set
        and state.previous_model_revision == revision
    )
    if approved_active_cycle or retained_rollback_cycle:
        candidate_slot = str(state.active_slot)
    elif previous_cycle:
        candidate_slot = str(state.previous_slot)
    else:
        if state.revision != revision:
            raise RevisionConflict(
                'scope-set readiness must match the current reader revision; '
                f'expected={revision}, actual={state.revision}'
            )
        candidate_slot = inactive_slot(state)
    if (
        candidate_slot_override is not None
        and _normalise_slot(candidate_slot_override) != candidate_slot
    ):
        raise StateInvariantError(
            f'readiness candidate slot mismatch: derived={candidate_slot}, '
            f'requested={candidate_slot_override}'
        )
    historical = bool(
        approved_active_cycle or retained_rollback_cycle or previous_cycle
    )
    promotion_history_events = 0
    if historical:
        promotion_history_events = int(_scalar(
            cur,
            f"SELECT COUNT(*) FROM {HISTORY_TABLE} "
            "WHERE event_type = 'applied' "
            "AND action IN ('cutover','advance_cycle','restore_v2','rollback_slot') "
            "AND to_version = 'v2' "
            "AND readiness_digest IS NOT NULL "
            f"AND cycle_id = {_sql_literal(cycle)} "
            f"AND to_scope_set_id = {_sql_literal(scope_set)} "
            f"AND to_slot = {_sql_literal(candidate_slot)} "
            f"AND to_revision <= {int(state.revision)}",
        ) or 0)
    scope_report, manifests = _scope_set_evidence(
        cur,
        parent_cycle_id=requested_parent,
        scope_set_id=scope_set,
        expected_revision=revision,
        require_fresh=require_fresh,
    )
    competition_type_dq = _scope_competition_type_dq_report(manifests)
    write_mode = (
        'native-only' if state.legacy_writers_disabled_at is not None else 'dual'
    )
    writes = _scope_write_manifest_report(
        cur,
        manifests=manifests,
        expected_revision=revision,
        write_mode=write_mode,
        require_fresh=require_fresh,
    )
    conflicts_table = slotted_relation(
        'iceberg.silver.transfermarkt_player_xref_global_v2', candidate_slot,
    )
    conflicts = int(_scalar(
        cur,
        f'SELECT COUNT(*) FROM {conflicts_table} '
        "WHERE resolution_status IN ('source_conflict','canonical_conflict')",
    ) or 0)
    model_manifest = _model_manifest_report(
        cur,
        cycle,
        expected_revision=revision,
        expected_slot=candidate_slot,
        require_fresh=require_fresh,
        require_current_snapshots=require_current_snapshots,
        scope_set_id=scope_set,
    )
    gold = _gold_contract_report(cur, candidate_slot=candidate_slot)
    coverage = _scope_set_coverage_report(
        cur, manifests=manifests, candidate_slot=candidate_slot,
    )
    gold_ok = all(item['passed'] for item in gold.values())
    ready = bool(
        scope_report['passed']
        and competition_type_dq['passed']
        and writes['passed']
        and conflicts == 0
        and model_manifest['passed']
        and gold_ok
        and coverage['passed']
        and (not historical or promotion_history_events > 0)
    )
    return {
        'cycle_id': cycle,
        'parent_cycle_id': requested_parent or cycle,
        'source_parent_cycle_ids': scope_report['parent_cycle_ids'],
        'scope_set_id': scope_set,
        'scope_count': len(manifests),
        'expected_state_revision': revision,
        'candidate_slot': candidate_slot,
        'write_mode': write_mode,
        'historical_promoted_cycle': historical,
        'promotion_history_events': promotion_history_events,
        'freshness_required': require_fresh,
        'current_snapshots_required': require_current_snapshots,
        'ready': ready,
        'scope_set_manifest': scope_report,
        'competition_type_dq': competition_type_dq,
        'complete_successful_manifest': scope_report['passed'],
        'child_write_manifests': writes,
        'bidirectional_parity': writes,
        'xref_conflicts': conflicts,
        'model_build_manifest': model_manifest,
        'shadow_gold_pk_violations': {
            name: item.get('duplicate_pk_violations', 0)
            for name, item in gold.items()
        },
        'shadow_gold_contracts': gold,
        'canonical_player_coverage': coverage,
        'proxy_traffic': scope_report['traffic'],
    }


def readiness(
    cur,
    cycle_id: str,
    *,
    league: str | None = None,
    season: int | None = None,
    expected_revision: int,
    require_fresh: bool = True,
    require_current_snapshots: bool = True,
    candidate_slot_override: str | None = None,
    allow_retained_rollback_slot: bool = False,
    allow_previous_slot: bool = False,
    scope_set_id: str | None = None,
    parent_cycle_id: str | None = None,
) -> dict[str, Any]:
    """Evaluate every gate for exactly one caller-supplied production cycle."""
    if scope_set_id is not None or parent_cycle_id is not None:
        if scope_set_id is None:
            raise ValueError('scope_set_id is required with parent_cycle_id')
        return _scope_set_readiness(
            cur,
            cycle_id,
            scope_set_id=scope_set_id,
            parent_cycle_id=parent_cycle_id,
            expected_revision=expected_revision,
            require_fresh=require_fresh,
            require_current_snapshots=require_current_snapshots,
            candidate_slot_override=candidate_slot_override,
            allow_retained_rollback_slot=allow_retained_rollback_slot,
            allow_previous_slot=allow_previous_slot,
        )
    if league is None or season is None:
        raise ValueError(
            'single-scope readiness requires league and season; '
            'multi-scope readiness requires scope_set_id'
        )
    cycle = _normalise_cycle_id(cycle_id)
    expected_league, expected_season = _normalise_scope(league, season)
    revision_expected = _normalise_revision(expected_revision)
    state = read_reader_state(cur, allow_missing=False)
    approved_active_cycle = bool(
        state.active_version == 'v2'
        and state.approved_cycle_id == cycle
        and state.approved_league == expected_league
        and state.approved_season == expected_season
        and state.approved_model_revision == revision_expected
        and state.active_slot in SLOTS
    )
    retained_rollback_cycle = bool(
        allow_retained_rollback_slot
        and state.active_version == 'legacy'
        and state.rollback_verified_at is not None
        and state.approved_cycle_id == cycle
        and state.approved_league == expected_league
        and state.approved_season == expected_season
        and state.approved_model_revision == revision_expected
        and state.active_slot in SLOTS
        and candidate_slot_override == state.active_slot
    )
    previous_cycle = bool(
        allow_previous_slot
        and state.active_version == 'v2'
        and state.previous_slot in SLOTS
        and candidate_slot_override == state.previous_slot
        and state.previous_cycle_id == cycle
        and state.previous_league == expected_league
        and state.previous_season == expected_season
        and state.previous_model_revision == revision_expected
    )
    if approved_active_cycle:
        candidate_slot = str(state.active_slot)
    elif retained_rollback_cycle:
        candidate_slot = str(state.active_slot)
    elif previous_cycle:
        candidate_slot = str(state.previous_slot)
    else:
        if state.revision != revision_expected:
            raise RevisionConflict(
                'candidate readiness must match the current reader revision; '
                f'expected={revision_expected}, actual={state.revision}'
            )
        candidate_slot = inactive_slot(state)
    if (
        candidate_slot_override is not None
        and _normalise_slot(candidate_slot_override) != candidate_slot
    ):
        raise StateInvariantError(
            f'readiness candidate slot mismatch: derived={candidate_slot}, '
            f'requested={candidate_slot_override}'
        )
    historical_promoted_cycle = bool(
        approved_active_cycle or retained_rollback_cycle or previous_cycle
    )
    promotion_history_events = 0
    if historical_promoted_cycle:
        promotion_history_events = int(_scalar(
            cur,
            f"SELECT COUNT(*) FROM {HISTORY_TABLE} "
            "WHERE event_type = 'applied' "
            "AND action IN ('cutover','advance_cycle','restore_v2','rollback_slot') "
            "AND readiness_digest IS NOT NULL "
            f"AND cycle_id = {_sql_literal(cycle)} "
            f"AND league = {_sql_literal(expected_league)} "
            f"AND season = {expected_season} "
            f"AND to_slot = {_sql_literal(candidate_slot)}",
        ) or 0)
    dual_rows = _manifest_rows(
        cur, cycle, league=expected_league, season=expected_season,
    )
    historical_approved_dual = bool(historical_promoted_cycle and dual_rows)
    dual_required = state.legacy_writers_disabled_at is None
    if dual_required or historical_approved_dual:
        manifest, evidence = _validate_manifest_rows(
            dual_rows, league=expected_league, season=expected_season,
            require_fresh=require_fresh,
        )
        if historical_promoted_cycle:
            parity = {
                name: {
                    'passed': item['passed'], 'mode': 'promoted-manifest',
                    'live_batch_required': False,
                }
                for name, item in manifest['entities'].items()
            }
        else:
            parity = run_parity(
                cur, cycle, league=expected_league, season=expected_season,
                manifest_evidence=evidence, require_fresh=require_fresh,
            )
        write_mode = 'dual'
    else:
        if dual_rows:
            raise ReadinessError(
                'native-only candidate cycle contains forbidden dual-write evidence'
            )
        manifest, evidence = _validate_native_manifest(
            cur,
            _native_manifest_rows(
                cur, cycle, league=expected_league, season=expected_season,
            ),
            league=expected_league, season=expected_season,
            expected_revision=revision_expected, require_fresh=require_fresh,
            require_live_batch=not historical_promoted_cycle,
        )
        parity = {
            name: {
                'native_manifest_live_match': item['passed'],
                'passed': item['passed'], 'mode': 'native-only',
            }
            for name, item in manifest['entities'].items()
        }
        write_mode = 'native-only'
    conflicts_table = slotted_relation(
        'iceberg.silver.transfermarkt_player_xref_global_v2', candidate_slot,
    )
    conflicts = int(_scalar(
        cur,
        f"SELECT COUNT(*) FROM {conflicts_table} "
        "WHERE resolution_status IN ('source_conflict','canonical_conflict')",
    ) or 0)
    model_manifest = _model_manifest_report(
        cur,
        cycle,
        league=expected_league,
        season=expected_season,
        expected_revision=revision_expected,
        expected_slot=candidate_slot,
        require_fresh=require_fresh,
        require_current_snapshots=require_current_snapshots,
    )
    gold = _gold_contract_report(
        cur, league=expected_league, season=expected_season,
        candidate_slot=candidate_slot,
    )
    coverage = _canonical_coverage_report(
        cur,
        league=expected_league,
        season=expected_season,
        manifest_evidence=evidence,
        candidate_slot=candidate_slot,
    )
    parity_ok = len(parity) == len(PARITY_PAIRS) and all(
        item['passed'] for item in parity.values()
    )
    gold_ok = all(item['passed'] for item in gold.values())
    coverage_ok = all(item['passed'] for item in coverage.values())
    return {
        'cycle_id': cycle,
        'league': expected_league,
        'season': expected_season,
        'expected_state_revision': revision_expected,
        'candidate_slot': candidate_slot,
        'write_mode': write_mode,
        'historical_promoted_cycle': historical_promoted_cycle,
        'promotion_history_events': promotion_history_events,
        'freshness_required': require_fresh,
        'current_snapshots_required': require_current_snapshots,
        'ready': bool(
            manifest['passed']
            and parity_ok
            and conflicts == 0
            and model_manifest['passed']
            and gold_ok
            and coverage_ok
            and (
                not historical_promoted_cycle or promotion_history_events > 0
            )
        ),
        'dual_write_manifest': manifest,
        'complete_successful_manifest': manifest['passed'],
        'xref_conflicts': conflicts,
        'bidirectional_parity': parity,
        'model_build_manifest': model_manifest,
        'shadow_gold_pk_violations': {
            name: item.get('duplicate_pk_violations', 0)
            for name, item in gold.items()
        },
        'shadow_gold_contracts': gold,
        'canonical_player_coverage': coverage,
    }


def _table_snapshot_id(cur, table: str) -> int:
    catalog, schema, name = table.split('.', 2)
    snapshot_table = f'{catalog}.{schema}."{name}$snapshots"'
    value = _scalar(
        cur,
        f'SELECT snapshot_id FROM {snapshot_table} '
        'ORDER BY committed_at DESC LIMIT 1',
    )
    if value is None:
        raise ReadinessError(f'no Iceberg snapshot for {table}')
    return int(value)


def _key_fingerprint(cur, contract: ModelContract) -> tuple[int, str]:
    return _key_fingerprint_table(cur, contract, contract.output_table)


def _key_fingerprint_table(
    cur, contract: ModelContract, output_table: str,
) -> tuple[int, str]:
    expressions = ','.join(
        f"COALESCE(CAST({column} AS varchar), '__NULL__')"
        for column in contract.key_columns
    )
    sql = (
        'SELECT COUNT(*), '
        f"COALESCE(LOWER(TO_HEX(CHECKSUM(CONCAT_WS(CHR(31), {expressions})))), '') "
        f'FROM {output_table}'
    )
    cur.execute(sql)
    rows = cur.fetchall()
    if not rows:
        raise ReadinessError(f'cannot fingerprint {output_table}')
    return int(rows[0][0]), str(rows[0][1] or '')


def record_model_build_manifest(
    cur,
    *,
    cycle_id: str,
    league: str | None = None,
    season: int | None = None,
    build_id: str,
    expected_revision: int,
    candidate_slot: str,
    scope_set_id: str,
    pinned_input_snapshot_ids: Mapping[str, int],
    dq_status: str = 'success',
) -> dict[str, Any]:
    """Append snapshot/key evidence after all shadow DQ tasks have passed."""
    cycle = _normalise_cycle_id(cycle_id)
    if league is None and season is None:
        expected_league, expected_season = None, None
    elif league is None or season is None:
        raise ValueError('league and season must be supplied together')
    else:
        expected_league, expected_season = _normalise_scope(league, season)
    revision = _normalise_revision(expected_revision)
    selected_slot = _normalise_slot(candidate_slot)
    scope_set = _normalise_scope_set_id(scope_set_id)
    if dq_status != 'success':
        raise ReadinessError('only a successful DQ result may be manifested')
    if not str(build_id or '').strip():
        raise ValueError('build_id is required')
    if not isinstance(pinned_input_snapshot_ids, Mapping):
        raise ValueError('pinned_input_snapshot_ids must be a mapping')
    try:
        pinned_inputs = {
            str(table): int(snapshot_id)
            for table, snapshot_id in pinned_input_snapshot_ids.items()
            if str(table).strip()
            and not isinstance(snapshot_id, bool)
            and int(snapshot_id) > 0
        }
    except (TypeError, ValueError) as exc:
        raise ValueError(
            'pinned_input_snapshot_ids must contain positive integer snapshots'
        ) from exc
    if len(pinned_inputs) != len(pinned_input_snapshot_ids):
        raise ValueError(
            'pinned_input_snapshot_ids contains an empty table or invalid snapshot'
        )
    required_pinned_sources = required_pinned_source_tables()
    missing_pins = sorted(required_pinned_sources - set(pinned_inputs))
    if missing_pins:
        raise ReadinessError(
            f'pre-build snapshot set is missing sources: {missing_pins}'
        )
    drifted_pins = {}
    for table, snapshot_id in pinned_inputs.items():
        current_snapshot_id = _table_snapshot_id(cur, table)
        if current_snapshot_id != snapshot_id:
            drifted_pins[table] = {
                'pinned': snapshot_id,
                'current': current_snapshot_id,
            }
    if drifted_pins:
        raise ReadinessError(
            f'input snapshots changed during inactive-slot build: {drifted_pins}'
        )
    pinned_json = json.dumps(pinned_inputs, sort_keys=True)
    state = assert_reader_revision(cur, revision)
    if selected_slot != inactive_slot(state):
        raise StateInvariantError(
            f'candidate slot {selected_slot!r} is not inactive for '
            f'active_slot={state.active_slot!r}'
        )
    rows: list[dict[str, Any]] = []
    for contract in MODEL_CONTRACTS:
        source_tables = contract_source_tables(contract, selected_slot)
        output_table = contract_output_table(contract, selected_slot)
        input_snapshots = {
            table: _table_snapshot_id(cur, table)
            for table in source_tables
        }
        output_snapshot = _table_snapshot_id(cur, output_table)
        row_count, key_hash = _key_fingerprint_table(cur, contract, output_table)
        payload = {
            'cycle_id': cycle,
            'league': expected_league,
            'season': expected_season,
            'build_id': str(build_id),
            'candidate_slot': selected_slot,
            'model_name': contract.name,
            'source_tables': json.dumps(source_tables),
            'input_snapshot_ids': json.dumps(input_snapshots, sort_keys=True),
            'output_table': output_table,
            'output_snapshot_id': output_snapshot,
            'row_count': row_count,
            'key_hash': key_hash,
            'dq_status': dq_status,
            'state_revision': revision,
            'scope_set_id': scope_set,
            'pinned_input_snapshot_ids': pinned_json,
        }
        sql = f"""
INSERT INTO {MODEL_MANIFEST_TABLE} (
    cycle_id, league, season, build_id, candidate_slot, model_name,
    source_tables, input_snapshot_ids,
    output_table, output_snapshot_id, row_count, key_hash, dq_status,
    state_revision, scope_set_id, pinned_input_snapshot_ids, committed_at
) VALUES (
    {_sql_literal(payload['cycle_id'])}, {_sql_literal(payload['league'])},
    {_sql_literal(payload['season'])}, {_sql_literal(payload['build_id'])},
    {_sql_literal(payload['candidate_slot'])},
    {_sql_literal(payload['model_name'])}, {_sql_literal(payload['source_tables'])},
    {_sql_literal(payload['input_snapshot_ids'])}, {_sql_literal(payload['output_table'])},
    {payload['output_snapshot_id']}, {payload['row_count']},
    {_sql_literal(payload['key_hash'])}, {_sql_literal(payload['dq_status'])},
    {payload['state_revision']}, {_sql_literal(payload['scope_set_id'])},
    {_sql_literal(payload['pinned_input_snapshot_ids'])}, CURRENT_TIMESTAMP
)
"""
        cur.execute(sql)
        cur.fetchall()
        rows.append(payload)
    assert_reader_revision(cur, revision)
    return {
        'cycle_id': cycle,
        'league': expected_league,
        'season': expected_season,
        'build_id': build_id,
        'candidate_slot': selected_slot,
        'scope_set_id': scope_set,
        'pinned_input_snapshot_ids': pinned_inputs,
        'rows': rows,
    }


def _readiness_digest(report: Mapping[str, Any]) -> str:
    encoded = json.dumps(report, sort_keys=True, default=str, separators=(',', ':'))
    return hashlib.sha256(encoded.encode('utf-8')).hexdigest()


def _transition_sql(
    *,
    action: str,
    cycle_id: str,
    league: str,
    season: int,
    expected_revision: int,
    actor: str,
    candidate_slot: str | None = None,
    from_state: ReaderState | None = None,
    scope_set_id: str | None = None,
) -> str:
    cycle = _normalise_cycle_id(cycle_id)
    expected_league, expected_season = _normalise_scope(league, season)
    revision = _normalise_revision(expected_revision)
    if action in {'cutover', 'advance_cycle'}:
        approved_scope_set = (
            _normalise_scope_set_id(scope_set_id)
            if scope_set_id is not None else None
        )
        slot = _normalise_slot(candidate_slot)
        retained = from_state if from_state and from_state.active_slot in SLOTS else None
        previous_season = (
            str(int(retained.approved_season))
            if retained and retained.approved_season is not None
            else 'CAST(NULL AS integer)'
        )
        previous_model_revision = (
            str(int(retained.approved_model_revision))
            if retained and retained.approved_model_revision is not None
            else 'CAST(NULL AS bigint)'
        )
        version_predicate = (
            "active_version = 'legacy'" if action == 'cutover'
            else f"active_version = 'v2' AND active_slot <> {_sql_literal(slot)}"
        )
        return f"""
UPDATE {STATE_TABLE}
SET active_version = 'v2',
    active_slot = {_sql_literal(slot)},
    approved_cycle_id = {_sql_literal(cycle)},
    approved_league = {_sql_literal(expected_league)},
    approved_season = {expected_season},
    approved_model_revision = {revision},
    approved_scope_set_id = {_sql_literal(approved_scope_set)},
    previous_slot = {_sql_literal(retained.active_slot if retained else None)},
    previous_cycle_id = {_sql_literal(retained.approved_cycle_id if retained else None)},
    previous_league = {_sql_literal(retained.approved_league if retained else None)},
    previous_season = {previous_season},
    previous_model_revision = {previous_model_revision},
    previous_scope_set_id = {_sql_literal(
        retained.approved_scope_set_id if retained else None
    )},
    slot_rollback_verified_at = CAST(NULL AS timestamp(6)),
    revision = revision + 1,
    activated_at = COALESCE(activated_at, CURRENT_TIMESTAMP),
    retention_until = COALESCE(
        retention_until, CURRENT_TIMESTAMP + INTERVAL '{RETENTION_DAYS}' DAY
    ),
    updated_at = CURRENT_TIMESTAMP,
    updated_by = {_sql_literal(actor)}
WHERE state_key = '{STATE_KEY}'
  AND {version_predicate}
  AND revision = {revision}
""".strip()
    if action == 'rollback':
        return f"""
UPDATE {STATE_TABLE}
SET active_version = 'legacy',
    rollback_verified_at = CAST(NULL AS timestamp(6)),
    slot_rollback_verified_at = CAST(NULL AS timestamp(6)),
    revision = revision + 1,
    updated_at = CURRENT_TIMESTAMP,
    updated_by = {_sql_literal(actor)}
WHERE state_key = '{STATE_KEY}'
  AND active_version = 'v2'
  AND approved_cycle_id = {_sql_literal(cycle)}
  AND approved_league = {_sql_literal(expected_league)}
  AND approved_season = {expected_season}
  AND approved_scope_set_id IS NOT DISTINCT FROM {_sql_literal(
        from_state.approved_scope_set_id if from_state else None
    )}
  AND revision = {revision}
""".strip()
    if action == 'restore_v2':
        slot = _normalise_slot(candidate_slot)
        return f"""
UPDATE {STATE_TABLE}
SET active_version = 'v2',
    revision = revision + 1,
    updated_at = CURRENT_TIMESTAMP,
    updated_by = {_sql_literal(actor)}
WHERE state_key = '{STATE_KEY}'
  AND active_version = 'legacy'
  AND active_slot = {_sql_literal(slot)}
  AND approved_cycle_id = {_sql_literal(cycle)}
  AND approved_league = {_sql_literal(expected_league)}
  AND approved_season = {expected_season}
  AND approved_scope_set_id IS NOT DISTINCT FROM {_sql_literal(
        from_state.approved_scope_set_id if from_state else None
    )}
  AND rollback_verified_at IS NOT NULL
  AND revision = {revision}
""".strip()
    if action == 'rollback_slot':
        if from_state is None:
            raise ValueError('rollback_slot requires from_state')
        slot = _normalise_slot(candidate_slot)
        if not all((
            from_state.previous_cycle_id, from_state.previous_league,
            from_state.previous_season is not None,
            from_state.previous_model_revision is not None,
        )):
            raise StateInvariantError('rollback_slot has no complete previous evidence')
        return f"""
UPDATE {STATE_TABLE}
SET active_slot = {_sql_literal(slot)},
    approved_cycle_id = {_sql_literal(from_state.previous_cycle_id)},
    approved_league = {_sql_literal(from_state.previous_league)},
    approved_season = {int(from_state.previous_season)},
    approved_model_revision = {int(from_state.previous_model_revision)},
    approved_scope_set_id = {_sql_literal(from_state.previous_scope_set_id)},
    previous_slot = {_sql_literal(from_state.active_slot)},
    previous_cycle_id = {_sql_literal(from_state.approved_cycle_id)},
    previous_league = {_sql_literal(from_state.approved_league)},
    previous_season = {int(from_state.approved_season)},
    previous_model_revision = {int(from_state.approved_model_revision)},
    previous_scope_set_id = {_sql_literal(from_state.approved_scope_set_id)},
    slot_rollback_verified_at = CURRENT_TIMESTAMP,
    revision = revision + 1,
    updated_at = CURRENT_TIMESTAMP,
    updated_by = {_sql_literal(actor)}
WHERE state_key = '{STATE_KEY}'
  AND active_version = 'v2'
  AND active_slot = {_sql_literal(from_state.active_slot)}
  AND previous_slot = {_sql_literal(slot)}
  AND revision = {revision}
""".strip()
    raise ValueError(f'unsupported transition action: {action}')


def _history_sql(
    *,
    transition_id: str,
    event_type: str,
    action: str,
    from_state: ReaderState,
    to_version: str,
    to_slot: str | None,
    cycle_id: str,
    league: str,
    season: int,
    expected_revision: int,
    actor: str,
    reason: str,
    readiness_digest: str | None,
    to_revision: int | None = None,
    downstream_dq_run_id: str | None = None,
    scope_set_id: str | None = None,
) -> str:
    target_revision = (
        expected_revision + 1 if to_revision is None else int(to_revision)
    )
    return f"""
INSERT INTO {HISTORY_TABLE} (
    transition_id, event_type, action, from_version, to_version,
    from_slot, to_slot,
    cycle_id, league, season,
    from_scope_set_id, to_scope_set_id,
    from_revision, to_revision, actor, reason, readiness_digest,
    downstream_dq_run_id, occurred_at
) VALUES (
    {_sql_literal(transition_id)}, {_sql_literal(event_type)}, {_sql_literal(action)},
    {_sql_literal(from_state.active_version)}, {_sql_literal(to_version)},
    {_sql_literal(from_state.active_slot)}, {_sql_literal(to_slot)},
    {_sql_literal(cycle_id)}, {_sql_literal(league)}, {season},
    {_sql_literal(from_state.approved_scope_set_id)},
    {_sql_literal(scope_set_id or from_state.approved_scope_set_id)},
    {expected_revision}, {target_revision},
    {_sql_literal(actor)}, {_sql_literal(reason)}, {_sql_literal(readiness_digest)},
    {_sql_literal(downstream_dq_run_id)},
    CURRENT_TIMESTAMP
)
""".strip()


def _drain(cur, sql: str) -> list[tuple[Any, ...]]:
    cur.execute(sql)
    return list(cur.fetchall())


def _timestamp_restore(value: Any) -> str:
    if value is None:
        return 'CAST(NULL AS timestamp(6))'
    return f'TRY_CAST({_sql_literal(value)} AS timestamp(6))'


def _compensation_sql(
    *,
    from_state: ReaderState,
    failed_revision: int,
    actor: str,
) -> str:
    season = (
        'CAST(NULL AS integer)'
        if from_state.approved_season is None
        else str(int(from_state.approved_season))
    )
    return f"""
UPDATE {STATE_TABLE}
SET active_version = {_sql_literal(from_state.active_version)},
    active_slot = {_sql_literal(from_state.active_slot)},
    approved_cycle_id = {_sql_literal(from_state.approved_cycle_id)},
    approved_league = {_sql_literal(from_state.approved_league)},
    approved_season = {season},
    approved_model_revision = {_sql_literal(from_state.approved_model_revision)},
    approved_scope_set_id = {_sql_literal(from_state.approved_scope_set_id)},
    previous_slot = {_sql_literal(from_state.previous_slot)},
    previous_cycle_id = {_sql_literal(from_state.previous_cycle_id)},
    previous_league = {_sql_literal(from_state.previous_league)},
    previous_season = {_sql_literal(from_state.previous_season)},
    previous_model_revision = {_sql_literal(from_state.previous_model_revision)},
    previous_scope_set_id = {_sql_literal(from_state.previous_scope_set_id)},
    revision = revision + 1,
    activated_at = {_timestamp_restore(from_state.activated_at)},
    retention_until = {_timestamp_restore(from_state.retention_until)},
    rollback_verified_at = {_timestamp_restore(from_state.rollback_verified_at)},
    legacy_writers_disabled_at = {_timestamp_restore(from_state.legacy_writers_disabled_at)},
    cleanup_completed_at = {_timestamp_restore(from_state.cleanup_completed_at)},
    slot_rollback_verified_at = {_timestamp_restore(from_state.slot_rollback_verified_at)},
    updated_at = CURRENT_TIMESTAMP,
    updated_by = {_sql_literal(actor)}
WHERE state_key = '{STATE_KEY}'
  AND revision = {int(failed_revision)}
""".strip()


def _apply_transition(
    cur,
    *,
    action: str,
    cycle_id: str,
    league: str,
    season: int,
    expected_revision: int,
    actor: str,
    reason: str,
    report: Mapping[str, Any] | None,
    apply: bool,
    quiesced: bool = False,
    scope_set_id: str | None = None,
) -> dict[str, Any]:
    cycle = _normalise_cycle_id(cycle_id)
    expected_league, expected_season = _normalise_scope(league, season)
    revision = _normalise_revision(expected_revision)
    actor = str(actor or '').strip()
    if not actor:
        raise ValueError('actor is required for audited state transitions')
    state = read_reader_state(cur, allow_missing=False)
    if action in {'cutover', 'advance_cycle'}:
        proposed_scope_set = scope_set_id or (
            report.get('scope_set_id') if report else None
        )
    elif action == 'rollback_slot':
        proposed_scope_set = state.previous_scope_set_id
    else:
        proposed_scope_set = state.approved_scope_set_id
    normalised_scope_set = (
        _normalise_scope_set_id(proposed_scope_set)
        if proposed_scope_set is not None else None
    )
    if action in {'cutover', 'advance_cycle'} and apply and normalised_scope_set is None:
        raise ReadinessError(
            'production promotion requires an immutable approved scope_set_id'
        )
    if state.revision != revision:
        raise RevisionConflict(
            f'CAS revision mismatch: expected={revision}, actual={state.revision}'
        )
    if action == 'cutover' and state.active_version != 'legacy':
        raise StateInvariantError('cutover requires active_version=legacy')
    if action == 'advance_cycle' and state.active_version != 'v2':
        raise StateInvariantError('advance-cycle requires active_version=v2')
    if action == 'restore_v2' and not (
        state.active_version == 'legacy'
        and state.active_slot in SLOTS
        and state.rollback_verified_at is not None
        and state.approved_cycle_id == cycle
        and state.approved_league == expected_league
        and state.approved_season == expected_season
    ):
        raise StateInvariantError(
            'restore-v2 requires exactly verified retained rollback state'
        )
    if action == 'rollback_slot' and not (
        state.active_version == 'v2'
        and state.previous_slot in SLOTS
        and state.previous_cycle_id == cycle
        and state.previous_league == expected_league
        and state.previous_season == expected_season
        and state.previous_model_revision is not None
    ):
        raise StateInvariantError(
            'rollback-slot requires exact persisted previous-slot evidence'
        )
    if action == 'rollback' and (
        state.active_version != 'v2'
        or state.approved_cycle_id != cycle
        or state.approved_league != expected_league
        or state.approved_season != expected_season
    ):
        raise StateInvariantError(
            'rollback requires active v2 state for the same approved cycle'
        )
    if action == 'rollback' and state.cleanup_completed_at is not None:
        raise StateInvariantError('rollback is unavailable after verified cleanup')
    if action in {'cutover', 'advance_cycle', 'restore_v2', 'rollback_slot'} and (
        not report or not report.get('ready')
    ):
        raise ReadinessError(f'cycle {cycle!r} has not passed readiness')
    if action in {'cutover', 'advance_cycle', 'restore_v2', 'rollback_slot'} and apply and not quiesced:
        raise ReadinessError(
            'promotion apply requires explicit quiesced-DAG confirmation'
        )

    candidate_slot = (
        str(report.get('candidate_slot'))
        if report is not None and report.get('candidate_slot') is not None
        else (
            inactive_slot(state)
            if action in {'cutover', 'advance_cycle'}
            else state.previous_slot if action == 'rollback_slot'
            else state.active_slot
        )
    )
    if action in {'cutover', 'advance_cycle', 'rollback_slot'}:
        candidate_slot = _normalise_slot(candidate_slot)
        if candidate_slot != inactive_slot(state):
            raise ReadinessError(
                f'candidate slot {candidate_slot!r} is not inactive for '
                f'active/retained slot {state.active_slot!r}'
            )

    view_preflight = verify_reader_views(
        cur,
        expected_version=state.active_version,
        expected_revision=state.revision,
        expected_slot=(state.active_slot if state.active_version == 'v2' else None),
        allow_static_slot=state.cleanup_completed_at is not None,
    )
    if not view_preflight['passed']:
        raise ReadinessError(
            'canonical reader view preflight failed; refusing state mutation'
        )
    if action in {'cutover', 'advance_cycle', 'restore_v2', 'rollback_slot'}:
        readiness_revision = (
            int(state.approved_model_revision)
            if action == 'restore_v2' else revision
        )
        if action == 'rollback_slot':
            readiness_revision = int(state.previous_model_revision)
        rechecked = readiness(
            cur,
            cycle,
            league=expected_league,
            season=expected_season,
            expected_revision=readiness_revision,
            scope_set_id=normalised_scope_set,
            parent_cycle_id=(cycle if normalised_scope_set is not None else None),
            candidate_slot_override=(
                candidate_slot
                if action in {'restore_v2', 'rollback_slot'} else None
            ),
            allow_retained_rollback_slot=(action == 'restore_v2'),
            allow_previous_slot=(action == 'rollback_slot'),
            # Retained rollback targets must remain restorable for the full
            # 30-day window.  Fresh candidate evidence is a promotion gate,
            # not a reason to buy another scrape during a rollback drill.
            # Output snapshots, current Gold/DQ/coverage and audited promotion
            # history are still checked by readiness.
            require_fresh=(action not in {'restore_v2', 'rollback_slot'}),
            require_current_snapshots=(
                action not in {'restore_v2', 'rollback_slot'}
            ),
        )
        if (
            not rechecked['ready']
            or _readiness_digest(rechecked) != _readiness_digest(report)
        ):
            raise ReadinessError(
                'readiness evidence changed during cutover preflight; '
                'refusing CAS'
            )
        report = rechecked

    sql = _transition_sql(
        action=action,
        cycle_id=cycle,
        league=expected_league,
        season=expected_season,
        expected_revision=revision,
        actor=actor,
        candidate_slot=candidate_slot,
        from_state=state,
        scope_set_id=normalised_scope_set,
    )
    result = {
        'status': 'dry_run' if not apply else 'pending',
        'action': action,
        'cycle_id': cycle,
        'league': expected_league,
        'season': expected_season,
        'expected_revision': revision,
        'next_revision': revision + 1,
        'from_version': state.active_version,
        'to_version': (
            'v2'
            if action in {
                'cutover', 'advance_cycle', 'restore_v2', 'rollback_slot'
            } else 'legacy'
        ),
        'from_slot': state.active_slot,
        'to_slot': candidate_slot,
        'scope_set_id': normalised_scope_set,
        'sql': sql,
        'reader_views_preflight': view_preflight,
        'quiesced_confirmed': bool(quiesced),
    }
    if not apply:
        return result

    transition_id = str(uuid.uuid4())
    digest = _readiness_digest(report) if report is not None else None
    requested = _history_sql(
        transition_id=transition_id,
        event_type='requested',
        action=action,
        from_state=state,
        to_version=result['to_version'],
        to_slot=candidate_slot,
        cycle_id=cycle,
        league=expected_league,
        season=expected_season,
        expected_revision=revision,
        actor=actor,
        reason=reason,
        readiness_digest=digest,
        scope_set_id=normalised_scope_set,
    )
    _drain(cur, requested)
    dml_rows = _drain(cur, sql)
    affected = None
    if dml_rows and dml_rows[0] and dml_rows[0][0] is not None:
        try:
            affected = int(dml_rows[0][0])
        except (TypeError, ValueError):
            affected = None
    failure_details: list[str] = []
    new_state = None
    route_verification: dict[str, Any] = {'passed': False}
    try:
        new_state = read_reader_state(cur, allow_missing=False)
        route_verification = verify_reader_views(
            cur,
            expected_version=result['to_version'],
            expected_revision=revision + 1,
            expected_slot=(
                candidate_slot if result['to_version'] == 'v2' else None
            ),
            allow_static_slot=(
                new_state.cleanup_completed_at is not None
                if new_state is not None else False
            ),
        )
    except Exception as exc:  # noqa: BLE001 - must compensate a changed route
        failure_details.append(f'postcondition: {exc}')
    applied = bool(
        affected == 1
        and new_state is not None
        and new_state.revision == revision + 1
        and new_state.active_version == result['to_version']
        and (
            action not in {'advance_cycle', 'rollback_slot'}
            or (
                new_state.activated_at == state.activated_at
                and new_state.retention_until == state.retention_until
            )
        )
        and (
            action == 'rollback'
            or (
                new_state.approved_cycle_id == cycle
                and new_state.approved_league == expected_league
                and new_state.approved_season == expected_season
                and new_state.active_slot == candidate_slot
                and new_state.approved_model_revision == int(
                    report.get('expected_state_revision', revision)
                )
                and new_state.approved_scope_set_id == normalised_scope_set
            )
        )
        and (
            action != 'rollback_slot'
            or (
                new_state.previous_slot == state.active_slot
                and new_state.previous_cycle_id == state.approved_cycle_id
                and new_state.previous_model_revision
                == state.approved_model_revision
                and new_state.slot_rollback_verified_at is not None
            )
        )
        and route_verification['passed']
    )
    history_applied = False
    if applied:
        try:
            _drain(cur, _history_sql(
                transition_id=transition_id,
                event_type='applied',
                action=action,
                from_state=state,
                to_version=result['to_version'],
                to_slot=candidate_slot,
                cycle_id=cycle,
                league=expected_league,
                season=expected_season,
                expected_revision=revision,
                actor=actor,
                reason=reason,
                readiness_digest=digest,
                scope_set_id=normalised_scope_set,
            ))
            history_applied = True
        except Exception as exc:  # noqa: BLE001 - audit is part of success
            failure_details.append(f'applied-history: {exc}')
            applied = False

    event_type = 'rejected'
    compensated = False
    if not applied and affected == 1:
        try:
            compensation_rows = _drain(cur, _compensation_sql(
                from_state=state,
                failed_revision=revision + 1,
                actor=actor,
            ))
            compensation_affected = (
                int(compensation_rows[0][0])
                if compensation_rows and compensation_rows[0] else 0
            )
            restored = read_reader_state(cur, allow_missing=False)
            restored_views = verify_reader_views(
                cur,
                expected_version=state.active_version,
                expected_revision=revision + 2,
                expected_slot=(
                    state.active_slot if state.active_version == 'v2' else None
                ),
                allow_static_slot=state.cleanup_completed_at is not None,
            )
            compensated = bool(
                compensation_affected == 1
                and restored.revision == revision + 2
                and restored.active_version == state.active_version
                and restored_views['passed']
            )
        except Exception as exc:  # noqa: BLE001 - surface partial recovery
            failure_details.append(f'compensation: {exc}')
        event_type = 'compensated' if compensated else 'compensation_failed'
    if not history_applied:
        try:
            _drain(cur, _history_sql(
                transition_id=transition_id,
                event_type=event_type,
                action=action,
                from_state=state,
                to_version=result['to_version'],
                to_slot=candidate_slot,
                cycle_id=cycle,
                league=expected_league,
                season=expected_season,
                expected_revision=revision,
                actor=actor,
                reason=reason,
                readiness_digest=digest,
                to_revision=(revision + 2 if compensated else revision + 1),
                scope_set_id=normalised_scope_set,
            ))
        except Exception as exc:  # noqa: BLE001 - include audit failure
            failure_details.append(f'final-history: {exc}')
    if not applied:
        actual = new_state.revision if new_state is not None else 'unknown'
        raise RevisionConflict(
            f'{action} CAS did not apply exactly once; actual revision={actual}; '
            + '; '.join(failure_details)
        )
    result.update(
        status='applied',
        transition_id=transition_id,
        state=new_state.to_dict(),
        reader_views=route_verification,
    )
    return result


def cutover(
    cur,
    *,
    cycle_id: str,
    league: str,
    season: int,
    expected_revision: int,
    actor: str,
    apply: bool = False,
    reason: str = 'approved native-v2 cutover',
    quiesced: bool = False,
    scope_set_id: str | None = None,
) -> dict[str, Any]:
    report = readiness(
        cur,
        cycle_id,
        league=league,
        season=season,
        expected_revision=expected_revision,
        scope_set_id=scope_set_id,
        parent_cycle_id=(cycle_id if scope_set_id is not None else None),
    )
    return _apply_transition(
        cur,
        action='cutover',
        cycle_id=cycle_id,
        league=league,
        season=season,
        expected_revision=expected_revision,
        actor=actor,
        reason=reason,
        report=report,
        apply=apply,
        quiesced=quiesced,
        scope_set_id=scope_set_id,
    )


def advance_cycle(
    cur,
    *,
    cycle_id: str,
    league: str,
    season: int,
    expected_revision: int,
    actor: str,
    apply: bool = False,
    reason: str = 'approved native-v2 cycle advance',
    quiesced: bool = False,
    scope_set_id: str | None = None,
) -> dict[str, Any]:
    """Atomically flip active v2 to an inactive, fully-ready candidate slot.

    Unlike initial cutover this never rewrites ``activated_at`` or extends the
    original rollback-retention deadline.
    """
    report = readiness(
        cur, cycle_id, league=league, season=season,
        expected_revision=expected_revision,
        scope_set_id=scope_set_id,
        parent_cycle_id=(cycle_id if scope_set_id is not None else None),
    )
    return _apply_transition(
        cur, action='advance_cycle', cycle_id=cycle_id, league=league,
        season=season, expected_revision=expected_revision, actor=actor,
        reason=reason, report=report, apply=apply, quiesced=quiesced,
        scope_set_id=scope_set_id,
    )


def restore_v2(
    cur,
    *,
    cycle_id: str,
    league: str,
    season: int,
    expected_revision: int,
    actor: str,
    apply: bool = False,
    reason: str = 'restore verified retained native-v2 slot',
    quiesced: bool = False,
) -> dict[str, Any]:
    state = assert_reader_revision(cur, expected_revision)
    if state.approved_model_revision is None or state.active_slot not in SLOTS:
        raise StateInvariantError('no retained approved v2 model to restore')
    report = readiness(
        cur, cycle_id, league=league, season=season,
        expected_revision=int(state.approved_model_revision),
        scope_set_id=state.approved_scope_set_id,
        parent_cycle_id=(
            cycle_id if state.approved_scope_set_id is not None else None
        ),
        candidate_slot_override=state.active_slot,
        allow_retained_rollback_slot=True,
        require_fresh=False,
        require_current_snapshots=False,
    )
    return _apply_transition(
        cur, action='restore_v2', cycle_id=cycle_id, league=league,
        season=season, expected_revision=expected_revision, actor=actor,
        reason=reason, report=report, apply=apply, quiesced=quiesced,
    )


def rollback_slot(
    cur,
    *,
    cycle_id: str,
    league: str,
    season: int,
    expected_revision: int,
    actor: str,
    apply: bool = False,
    reason: str = 'rollback active v2 to retained previous slot',
    quiesced: bool = False,
) -> dict[str, Any]:
    """No-proxy v2->v2 rollback to the exact persisted previous build."""
    state = assert_reader_revision(cur, expected_revision)
    if state.previous_slot not in SLOTS or state.previous_model_revision is None:
        raise StateInvariantError('no retained previous v2 slot is available')
    report = readiness(
        cur, cycle_id, league=league, season=season,
        expected_revision=int(state.previous_model_revision),
        scope_set_id=state.previous_scope_set_id,
        parent_cycle_id=(
            cycle_id if state.previous_scope_set_id is not None else None
        ),
        require_fresh=False, require_current_snapshots=False,
        candidate_slot_override=state.previous_slot,
        allow_previous_slot=True,
    )
    return _apply_transition(
        cur, action='rollback_slot', cycle_id=cycle_id, league=league,
        season=season, expected_revision=expected_revision, actor=actor,
        reason=reason, report=report, apply=apply, quiesced=quiesced,
    )


def rollback(
    cur,
    *,
    cycle_id: str,
    league: str,
    season: int,
    expected_revision: int,
    actor: str,
    apply: bool = False,
    reason: str = 'native-v2 rollback drill',
) -> dict[str, Any]:
    # Rollback is intentionally independent of current readiness.  An
    # emergency escape hatch must remain available when downstream v2 DQ is
    # the very thing that failed.
    return _apply_transition(
        cur,
        action='rollback',
        cycle_id=cycle_id,
        league=league,
        season=season,
        expected_revision=expected_revision,
        actor=actor,
        reason=reason,
        report=None,
        apply=apply,
        quiesced=False,
    )


def _rollback_dq_report(
    cur,
    *,
    league: str,
    season: int,
) -> dict[str, Any]:
    """Run blocking DQ through canonical readers while they route legacy."""
    expected_league, expected_season = _normalise_scope(league, season)
    slug = _season_slug(expected_season)
    safe_league = _sql_literal(expected_league)

    def fact_contract(
        table: str,
        *,
        minimum: int,
        required: Sequence[str],
        pk: Sequence[str],
        branch: tuple[str, int] | None = None,
        invalid_value_predicate: str | None = None,
    ) -> dict[str, Any]:
        rows = int(_scalar(cur, f'SELECT COUNT(*) FROM {table}') or 0)
        nulls = int(_scalar(
            cur,
            f"SELECT COUNT(*) FROM {table} WHERE "
            + ' OR '.join(f'{column} IS NULL' for column in required),
        ) or 0)
        keys = ','.join(pk)
        duplicates = int(_scalar(
            cur,
            f'SELECT COUNT(*) FROM (SELECT {keys} FROM {table} '
            f'GROUP BY {keys} HAVING COUNT(*) > 1)',
        ) or 0)
        branch_rows = None
        branch_ok = True
        if branch:
            predicate, branch_minimum = branch
            branch_rows = int(_scalar(
                cur, f'SELECT COUNT(*) FROM {table} WHERE {predicate}',
            ) or 0)
            branch_ok = branch_rows >= branch_minimum
        invalid_values = (
            int(_scalar(
                cur,
                f'SELECT COUNT(*) FROM {table} '
                f'WHERE {invalid_value_predicate}',
            ) or 0)
            if invalid_value_predicate else 0
        )
        return {
            'row_count': rows,
            'minimum_rows': minimum,
            'null_violations': nulls,
            'duplicate_violations': duplicates,
            'branch_rows': branch_rows,
            'invalid_value_violations': invalid_values,
            'passed': (
                rows >= minimum and nulls == 0 and duplicates == 0
                and branch_ok and invalid_values == 0
            ),
        }

    contracts = {
        'fct_transfer': fact_contract(
            'iceberg.gold.fct_transfer_legacy',
            minimum=GOLD_MIN_ROWS['fct_transfer_v2'],
            required=('transfer_id', 'player_id'),
            pk=('transfer_id',),
        ),
        'fct_player_market_value': fact_contract(
            'iceberg.gold.fct_player_market_value_legacy',
            minimum=GOLD_MIN_ROWS['fct_player_market_value_v2'],
            required=('player_id', 'valuation_date', 'source', 'market_value_eur'),
            pk=('player_id', 'valuation_date', 'source'),
            branch=("source = 'transfermarkt'", TRANSFERMARKT_MV_MIN_ROWS),
            invalid_value_predicate='market_value_eur < 0',
        ),
        'dim_manager': fact_contract(
            'iceberg.gold.dim_manager_legacy',
            minimum=GOLD_MIN_ROWS['dim_manager_v2'],
            required=('manager_id',),
            pk=('manager_id',),
        ),
        'team_season_market_value': fact_contract(
            'iceberg.gold.transfermarkt_team_season_market_value_legacy',
            minimum=GOLD_MIN_ROWS['team_season_market_value_v2'],
            required=('team_id', 'league', 'season', 'squad_market_value_eur'),
            pk=('team_id', 'league', 'season'),
            invalid_value_predicate='squad_market_value_eur <= 0',
        ),
    }
    legacy_team_scope_rows = int(_scalar(
        cur,
        'SELECT COUNT(*) FROM '
        'iceberg.gold.transfermarkt_team_season_market_value_legacy '
        f'WHERE league = {safe_league} AND season = {_sql_literal(slug)} '
        'AND squad_market_value_eur > 0 AND valued_players > 0',
    ) or 0)
    contracts['team_season_market_value']['requested_scope'] = {
        'league': expected_league,
        'season': slug,
        'row_count': legacy_team_scope_rows,
        'minimum_rows': 15,
        'passed': legacy_team_scope_rows >= 15,
    }
    contracts['team_season_market_value']['passed'] = bool(
        contracts['team_season_market_value']['passed']
        and legacy_team_scope_rows >= 15
    )
    players = _coverage_result(cur, f"""
SELECT COUNT(*), COUNT_IF(canonical_id IS NOT NULL)
FROM iceberg.silver.transfermarkt_players_legacy
WHERE league = {safe_league} AND season = {_sql_literal(slug)}
""")
    coach_rows = int(_scalar(
        cur,
        'SELECT COUNT(*) FROM iceberg.silver.transfermarkt_coaches_legacy '
        f'WHERE league = {safe_league} AND season = {_sql_literal(slug)}',
    ) or 0)
    return {
        'passed': (
            all(item['passed'] for item in contracts.values())
            and players['passed']
            and coach_rows >= GOLD_MIN_ROWS['dim_manager_v2']
        ),
        'contracts': contracts,
        'player_canonical_coverage': players,
        'coach_rows': coach_rows,
        'coach_minimum_rows': GOLD_MIN_ROWS['dim_manager_v2'],
    }


def verify_rollback(
    cur,
    *,
    cycle_id: str,
    league: str,
    season: int,
    expected_revision: int,
    actor: str,
    downstream_dq_run_id: str,
    apply: bool = False,
) -> dict[str, Any]:
    """DQ-backed rollback drill attestation, separate from emergency routing."""
    cycle = _normalise_cycle_id(cycle_id)
    expected_league, expected_season = _normalise_scope(league, season)
    revision = _normalise_revision(expected_revision)
    dq_run_id = str(downstream_dq_run_id or '').strip()
    if not dq_run_id:
        raise ValueError('downstream_dq_run_id is required')
    state = read_reader_state(cur, allow_missing=False)
    if (
        state.active_version != 'legacy' or state.revision != revision
        or state.approved_cycle_id != cycle
        or state.approved_league != expected_league
        or state.approved_season != expected_season
        or state.active_slot not in SLOTS
    ):
        raise StateInvariantError(
            'rollback verification requires the expected active legacy revision'
        )
    rollback_events = int(_scalar(
        cur,
        f"SELECT COUNT(*) FROM {HISTORY_TABLE} "
        "WHERE action = 'rollback' AND event_type = 'applied' "
        f"AND cycle_id = {_sql_literal(cycle)} "
        f"AND league = {_sql_literal(expected_league)} "
        f"AND season = {expected_season} "
        f"AND to_revision = {revision}",
    ) or 0)
    views = verify_reader_views(
        cur, expected_version='legacy', expected_revision=revision,
    )
    dq = _rollback_dq_report(
        cur, league=expected_league, season=expected_season,
    )
    if rollback_events < 1 or not views['passed'] or not dq['passed']:
        raise ReadinessError(
            f'rollback verification failed: history={rollback_events} '
            f'views={views} dq={dq}'
        )
    sql = f"""
UPDATE {STATE_TABLE}
SET rollback_verified_at = CURRENT_TIMESTAMP,
    revision = revision + 1,
    updated_at = CURRENT_TIMESTAMP,
    updated_by = {_sql_literal(actor)}
WHERE state_key = '{STATE_KEY}'
  AND active_version = 'legacy'
  AND revision = {revision}
""".strip()
    result = {
        'status': 'dry_run' if not apply else 'pending',
        'action': 'rollback_verify',
        'cycle_id': cycle,
        'league': expected_league,
        'season': expected_season,
        'expected_revision': revision,
        'next_revision': revision + 1,
        'downstream_dq_run_id': dq_run_id,
        'dq': dq,
        'reader_views': views,
        'sql': sql,
    }
    if not apply:
        return result
    transition_id = str(uuid.uuid4())
    _drain(cur, _history_sql(
        transition_id=transition_id,
        event_type='requested',
        action='rollback_verify',
        from_state=state,
        to_version='legacy',
        to_slot=state.active_slot,
        cycle_id=cycle,
        league=expected_league,
        season=expected_season,
        expected_revision=revision,
        actor=actor,
        reason='DQ-backed rollback verification',
        readiness_digest=_readiness_digest(dq),
        downstream_dq_run_id=dq_run_id,
    ))
    dml = _drain(cur, sql)
    affected = int(dml[0][0]) if dml and dml[0] else 0
    applied = False
    failure = None
    try:
        new_state = read_reader_state(cur, allow_missing=False)
        post_views = verify_reader_views(
            cur, expected_version='legacy', expected_revision=revision + 1,
        )
        applied = bool(
            affected == 1 and new_state.revision == revision + 1
            and new_state.rollback_verified_at is not None
            and post_views['passed']
        )
    except Exception as exc:  # noqa: BLE001 - compensate the attestation CAS
        failure = exc
    compensated = False
    if not applied and affected == 1:
        compensation = _drain(cur, _compensation_sql(
            from_state=state, failed_revision=revision + 1, actor=actor,
        ))
        compensation_affected = int(compensation[0][0]) if compensation and compensation[0] else 0
        restored = read_reader_state(cur, allow_missing=False)
        compensated = bool(
            compensation_affected == 1 and restored.revision == revision + 2
            and restored.rollback_verified_at == state.rollback_verified_at
        )
    _drain(cur, _history_sql(
        transition_id=transition_id,
        event_type=(
            'applied' if applied else 'compensated' if compensated else 'rejected'
        ),
        action='rollback_verify', from_state=state, to_version='legacy',
        to_slot=state.active_slot, cycle_id=cycle, league=expected_league,
        season=expected_season, expected_revision=revision, actor=actor,
        reason='DQ-backed rollback verification',
        readiness_digest=_readiness_digest(dq),
        downstream_dq_run_id=dq_run_id,
        to_revision=(revision + 2 if compensated else revision + 1),
    ))
    if not applied:
        raise RevisionConflict(
            f'rollback verification CAS/postcondition failed: {failure}'
        )
    result.update(status='applied', transition_id=transition_id)
    return result


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_aware(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def cleanup_check(
    cur,
    *,
    cycle_id: str,
    league: str,
    season: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    cycle = _normalise_cycle_id(cycle_id)
    expected_league, expected_season = _normalise_scope(league, season)
    state = read_reader_state(cur, allow_missing=False)
    retention_until = _as_aware(state.retention_until)
    now = _as_aware(now) or _utc_now()
    rollback_history = int(_scalar(
        cur,
        f"SELECT COUNT(*) FROM {HISTORY_TABLE} "
        "WHERE action = 'rollback_verify' AND event_type = 'applied' "
        "AND downstream_dq_run_id IS NOT NULL",
    ) or 0)
    rollback_dq = _rollback_dq_report(
        cur, league=expected_league, season=expected_season,
    )
    model_revision = state.approved_model_revision
    if model_revision is None:
        raise StateInvariantError('active route has no approved model revision')
    ready = readiness(
        cur,
        cycle,
        league=expected_league,
        season=expected_season,
        expected_revision=model_revision,
        require_fresh=False,
        require_current_snapshots=False,
    )
    previous_ready: dict[str, Any] = {'ready': False, 'reason': 'missing'}
    if all((
        state.previous_slot in SLOTS, state.previous_cycle_id,
        state.previous_league, state.previous_season is not None,
        state.previous_model_revision is not None,
    )):
        previous_ready = readiness(
            cur, str(state.previous_cycle_id),
            league=str(state.previous_league), season=int(state.previous_season),
            expected_revision=int(state.previous_model_revision),
            require_fresh=False, require_current_snapshots=False,
            candidate_slot_override=state.previous_slot,
            allow_previous_slot=True,
        )
    slot_rollback_history = int(_scalar(
        cur,
        f"SELECT COUNT(*) FROM {HISTORY_TABLE} "
        "WHERE action = 'rollback_slot' AND event_type = 'applied' "
        f"AND to_slot = {_sql_literal(state.active_slot)} "
        f"AND cycle_id = {_sql_literal(state.approved_cycle_id)}",
    ) or 0)
    reader_views = verify_reader_views(
        cur,
        expected_version='v2',
        expected_revision=state.revision,
        expected_slot=state.active_slot,
    )
    guards = {
        'active_v2': state.active_version == 'v2',
        'approved_cycle_matches': state.approved_cycle_id == cycle,
        'approved_league_matches': state.approved_league == expected_league,
        'approved_season_matches': state.approved_season == expected_season,
        'retention_timestamp_present': retention_until is not None,
        'retention_expired': bool(retention_until and retention_until <= now),
        'rollback_state_evidence': state.rollback_verified_at is not None,
        'rollback_history_evidence': rollback_history > 0,
        'physical_legacy_dq_still_green': bool(rollback_dq['passed']),
        'readiness_still_green': bool(ready['ready']),
        'canonical_reader_views_verified': bool(reader_views['passed']),
        'legacy_writers_disabled_persisted': (
            state.legacy_writers_disabled_at is not None
        ),
        'active_slot_native_only_evidence': bool(
            ready['ready'] and ready.get('write_mode') == 'native-only'
        ),
        'previous_slot_native_only_evidence': bool(
            previous_ready.get('ready')
            and previous_ready.get('write_mode') == 'native-only'
        ),
        'post_cleanup_slot_rollback_verified': bool(
            state.slot_rollback_verified_at is not None
            and slot_rollback_history > 0
        ),
    }
    return {
        'cycle_id': cycle,
        'league': expected_league,
        'season': expected_season,
        'eligible': all(guards.values()),
        'guards': guards,
        'state': state.to_dict(),
        'reader_views': reader_views,
        'physical_legacy_dq': rollback_dq,
        'previous_slot_readiness': previous_ready,
        'slot_rollback_history_events': slot_rollback_history,
    }


def disable_legacy_writers(
    cur,
    *,
    cycle_id: str,
    league: str,
    season: int,
    expected_revision: int,
    actor: str,
    apply: bool = False,
    quiesced: bool = False,
) -> dict[str, Any]:
    """Persist the writer shutdown only after every other cleanup guard passes."""
    revision = _normalise_revision(expected_revision)
    state = assert_reader_revision(cur, revision)
    report = cleanup_check(
        cur, cycle_id=cycle_id, league=league, season=season,
    )
    prerequisite_guards = {
        key: value for key, value in report['guards'].items()
        if key not in {
            'legacy_writers_disabled_persisted',
            'active_slot_native_only_evidence',
            'previous_slot_native_only_evidence',
            'post_cleanup_slot_rollback_verified',
        }
    }
    if not all(prerequisite_guards.values()):
        raise ReadinessError(
            f'legacy writer shutdown prerequisites failed: {prerequisite_guards}'
        )
    if state.legacy_writers_disabled_at is not None:
        raise StateInvariantError('legacy writers are already disabled')
    if apply and not quiesced:
        raise ReadinessError(
            'legacy-writer shutdown requires explicit quiesced-DAG confirmation'
        )
    sql = f"""
UPDATE {STATE_TABLE}
SET legacy_writers_disabled_at = CURRENT_TIMESTAMP,
    revision = revision + 1,
    updated_at = CURRENT_TIMESTAMP,
    updated_by = {_sql_literal(actor)}
WHERE state_key = '{STATE_KEY}'
  AND active_version = 'v2'
  AND active_slot = {_sql_literal(state.active_slot)}
  AND revision = {revision}
""".strip()
    result = {
        'status': 'dry_run' if not apply else 'pending',
        'action': 'disable_legacy_writers',
        'expected_revision': revision,
        'active_slot': state.active_slot,
        'sql': sql,
        'prerequisite_guards': prerequisite_guards,
        'quiesced_confirmed': bool(quiesced),
    }
    if not apply:
        return result
    rows = _drain(cur, sql)
    affected = int(rows[0][0]) if rows and rows[0] else 0
    new_state = read_reader_state(cur, allow_missing=False)
    views = verify_reader_views(
        cur, expected_version='v2', expected_revision=revision + 1,
        expected_slot=state.active_slot,
    )
    if not (
        affected == 1 and new_state.revision == revision + 1
        and new_state.legacy_writers_disabled_at is not None
        and views['passed']
    ):
        raise RevisionConflict('legacy-writer shutdown CAS/postcondition failed')
    result.update(status='applied', state=new_state.to_dict(), reader_views=views)
    return result


def post_cleanup_verify(cur) -> dict[str, Any]:
    """Read-only verifier for dynamic v2-only A/B views after legacy removal."""
    state = read_reader_state(cur, allow_missing=False)
    if state.active_version != 'v2' or state.active_slot not in SLOTS:
        raise StateInvariantError('post-cleanup verification requires active v2 slot')
    if state.legacy_writers_disabled_at is None:
        raise StateInvariantError('legacy writer shutdown is not persisted')
    views = verify_reader_views(
        cur, expected_version='v2', expected_revision=state.revision,
        expected_slot=state.active_slot, allow_static_slot=True,
        require_no_legacy=True,
    )
    cur.execute("""
SELECT table_schema, table_name, table_type
FROM iceberg.information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold')
""")
    inventory = {
        f'iceberg.{schema}.{name}': str(kind).upper()
        for schema, name, kind in cur.fetchall()
    }
    legacy_relations = {
        relation.legacy for relation in CANONICAL_READER_RELATIONS
    } | {
        'iceberg.gold.fct_transfer_legacy_source',
        'iceberg.silver.transfermarkt_market_value_history',
        'iceberg.silver.transfermarkt_transfers',
        'iceberg.bronze.transfermarkt_players',
        'iceberg.bronze.transfermarkt_market_value_history',
        'iceberg.bronze.transfermarkt_transfers',
        'iceberg.bronze.transfermarkt_coaches',
    }
    legacy_relations.update(
        f'{relation.canonical}_pre_native_v2_backup'
        for relation in CANONICAL_READER_RELATIONS
    )
    legacy_relations.update(
        f'{contract.output_table}_pre_slots_backup'
        for contract in MODEL_CONTRACTS
    )
    present = sorted(legacy_relations.intersection(inventory))
    return {
        'passed': bool(views['passed'] and not present),
        'active_slot': state.active_slot,
        'legacy_relations_present': present,
        'reader_views': views,
    }


def complete_cleanup(
    cur,
    *,
    expected_revision: int,
    actor: str,
    apply: bool = False,
) -> dict[str, Any]:
    """Attest successful cleanup only after v2-only views/absence verification."""
    revision = _normalise_revision(expected_revision)
    state = assert_reader_revision(cur, revision)
    if state.cleanup_completed_at is not None:
        raise StateInvariantError('cleanup is already completed')
    if not (
        state.active_version == 'v2'
        and state.active_slot in SLOTS
        and state.previous_slot in SLOTS
        and state.legacy_writers_disabled_at is not None
        and state.slot_rollback_verified_at is not None
    ):
        raise StateInvariantError('cleanup completion has no durable slot/writer evidence')
    active = readiness(
        cur, str(state.approved_cycle_id), league=str(state.approved_league),
        season=int(state.approved_season),
        expected_revision=int(state.approved_model_revision),
        require_fresh=False, require_current_snapshots=False,
    )
    previous = readiness(
        cur, str(state.previous_cycle_id), league=str(state.previous_league),
        season=int(state.previous_season),
        expected_revision=int(state.previous_model_revision),
        require_fresh=False, require_current_snapshots=False,
        candidate_slot_override=state.previous_slot, allow_previous_slot=True,
    )
    verified = post_cleanup_verify(cur)
    if not (
        verified['passed'] and active['ready'] and previous['ready']
        and active.get('write_mode') == 'native-only'
        and previous.get('write_mode') == 'native-only'
    ):
        raise ReadinessError(
            f'post-cleanup completion preconditions failed: views={verified} '
            f'active={active} previous={previous}'
        )
    sql = f"""
UPDATE {STATE_TABLE}
SET cleanup_completed_at = CURRENT_TIMESTAMP,
    revision = revision + 1,
    updated_at = CURRENT_TIMESTAMP,
    updated_by = {_sql_literal(actor)}
WHERE state_key = '{STATE_KEY}'
  AND active_version = 'v2'
  AND active_slot = {_sql_literal(state.active_slot)}
  AND previous_slot = {_sql_literal(state.previous_slot)}
  AND legacy_writers_disabled_at IS NOT NULL
  AND slot_rollback_verified_at IS NOT NULL
  AND cleanup_completed_at IS NULL
  AND revision = {revision}
""".strip()
    result = {
        'status': 'dry_run' if not apply else 'pending',
        'action': 'complete_cleanup', 'expected_revision': revision,
        'active_slot': state.active_slot, 'previous_slot': state.previous_slot,
        'sql': sql, 'post_cleanup_verifier': verified,
        'active_readiness': active, 'previous_readiness': previous,
    }
    if not apply:
        return result
    transition_id = str(uuid.uuid4())
    digest = _readiness_digest({
        'post_cleanup': verified, 'active': active, 'previous': previous,
    })
    _drain(cur, _history_sql(
        transition_id=transition_id, event_type='requested',
        action='complete_cleanup', from_state=state, to_version='v2',
        to_slot=state.active_slot, cycle_id=str(state.approved_cycle_id),
        league=str(state.approved_league), season=int(state.approved_season),
        expected_revision=revision, actor=actor,
        reason='verified post-retention legacy cleanup', readiness_digest=digest,
    ))
    dml = _drain(cur, sql)
    affected = int(dml[0][0]) if dml and dml[0] else 0
    applied = False
    failure: BaseException | None = None
    try:
        new_state = read_reader_state(cur, allow_missing=False)
        post = post_cleanup_verify(cur)
        applied = bool(
            affected == 1 and new_state.revision == revision + 1
            and new_state.cleanup_completed_at is not None and post['passed']
        )
    except Exception as exc:  # noqa: BLE001 - compensate marker CAS
        failure = exc
    compensated = False
    if not applied and affected == 1:
        rows = _drain(cur, _compensation_sql(
            from_state=state, failed_revision=revision + 1, actor=actor,
        ))
        restored = read_reader_state(cur, allow_missing=False)
        compensated = bool(
            rows and rows[0] and int(rows[0][0]) == 1
            and restored.revision == revision + 2
            and restored.cleanup_completed_at is None
        )
    final_history = _history_sql(
        transition_id=transition_id,
        event_type='applied' if applied else 'compensated' if compensated else 'rejected',
        action='complete_cleanup', from_state=state, to_version='v2',
        to_slot=state.active_slot, cycle_id=str(state.approved_cycle_id),
        league=str(state.approved_league), season=int(state.approved_season),
        expected_revision=revision, actor=actor,
        reason='verified post-retention legacy cleanup', readiness_digest=digest,
        to_revision=(revision + 2 if compensated else revision + 1),
    )
    try:
        _drain(cur, final_history)
    except Exception as exc:  # noqa: BLE001 - audit is part of the CAS
        if applied:
            rows = _drain(cur, _compensation_sql(
                from_state=state, failed_revision=revision + 1, actor=actor,
            ))
            restored = read_reader_state(cur, allow_missing=False)
            compensated = bool(
                rows and rows[0] and int(rows[0][0]) == 1
                and restored.revision == revision + 2
                and restored.cleanup_completed_at is None
            )
            try:
                _drain(cur, _history_sql(
                    transition_id=transition_id,
                    event_type='compensated' if compensated else 'compensation_failed',
                    action='complete_cleanup', from_state=state, to_version='v2',
                    to_slot=state.active_slot,
                    cycle_id=str(state.approved_cycle_id),
                    league=str(state.approved_league),
                    season=int(state.approved_season),
                    expected_revision=revision, actor=actor,
                    reason='verified post-retention legacy cleanup',
                    readiness_digest=digest, to_revision=revision + 2,
                ))
            except Exception:
                pass
        raise RevisionConflict(
            f'complete-cleanup audit failed; compensated={compensated}: {exc}'
        ) from exc
    if not applied:
        raise RevisionConflict(f'complete-cleanup CAS/postcondition failed: {failure}')
    result.update(status='applied', transition_id=transition_id)
    return result


def status(cur) -> dict[str, Any]:
    state = read_reader_state(cur, allow_missing=True)
    return {
        'state': state.to_dict(),
        'effective_active_version': (
            state.active_version if state.exists else 'legacy'
        ),
        'bootstrap_required': not state.exists,
        'selector_sql': reader_selector_sql(),
    }


def control_plane_bootstrap_sql() -> list[str]:
    """Idempotent ops DDL plus a safe legacy singleton seed."""
    try:
        from utils import transfermarkt_scope_state as scope_state
    except ImportError:  # package import in host-side tests
        from dags.utils import transfermarkt_scope_state as scope_state

    return [
        f"""CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
            state_key varchar,
            active_version varchar,
            active_slot varchar,
            approved_cycle_id varchar,
            approved_league varchar,
            approved_season integer,
            approved_model_revision bigint,
            approved_scope_set_id varchar,
            previous_slot varchar,
            previous_cycle_id varchar,
            previous_league varchar,
            previous_season integer,
            previous_model_revision bigint,
            previous_scope_set_id varchar,
            revision bigint,
            activated_at timestamp(6),
            retention_until timestamp(6),
            rollback_verified_at timestamp(6),
            updated_at timestamp(6),
            updated_by varchar,
            legacy_writers_disabled_at timestamp(6),
            cleanup_completed_at timestamp(6),
            slot_rollback_verified_at timestamp(6)
        ) WITH (format = 'PARQUET')""",
        f"ALTER TABLE {STATE_TABLE} ADD COLUMN IF NOT EXISTS active_slot varchar",
        f"ALTER TABLE {STATE_TABLE} ADD COLUMN IF NOT EXISTS approved_model_revision bigint",
        f"ALTER TABLE {STATE_TABLE} ADD COLUMN IF NOT EXISTS approved_scope_set_id varchar",
        f"ALTER TABLE {STATE_TABLE} ADD COLUMN IF NOT EXISTS legacy_writers_disabled_at timestamp(6)",
        f"ALTER TABLE {STATE_TABLE} ADD COLUMN IF NOT EXISTS cleanup_completed_at timestamp(6)",
        f"ALTER TABLE {STATE_TABLE} ADD COLUMN IF NOT EXISTS previous_slot varchar",
        f"ALTER TABLE {STATE_TABLE} ADD COLUMN IF NOT EXISTS previous_cycle_id varchar",
        f"ALTER TABLE {STATE_TABLE} ADD COLUMN IF NOT EXISTS previous_league varchar",
        f"ALTER TABLE {STATE_TABLE} ADD COLUMN IF NOT EXISTS previous_season integer",
        f"ALTER TABLE {STATE_TABLE} ADD COLUMN IF NOT EXISTS previous_model_revision bigint",
        f"ALTER TABLE {STATE_TABLE} ADD COLUMN IF NOT EXISTS previous_scope_set_id varchar",
        f"ALTER TABLE {STATE_TABLE} ADD COLUMN IF NOT EXISTS slot_rollback_verified_at timestamp(6)",
        f"""MERGE INTO {STATE_TABLE} t
        USING (VALUES (
            '{STATE_KEY}', 'legacy', CAST(NULL AS varchar),
            CAST(NULL AS varchar), CAST(NULL AS integer),
            CAST(0 AS bigint), CURRENT_TIMESTAMP, 'bootstrap'
        )) s(state_key, active_version, approved_cycle_id, approved_league,
             approved_season, revision, updated_at, updated_by)
        ON t.state_key = s.state_key
        WHEN NOT MATCHED THEN INSERT (
            state_key, active_version, active_slot, approved_cycle_id,
            approved_league, approved_season, approved_model_revision,
            approved_scope_set_id,
            previous_slot, previous_cycle_id, previous_league,
            previous_season, previous_model_revision, previous_scope_set_id,
            revision,
            activated_at, retention_until, rollback_verified_at,
            updated_at, updated_by, legacy_writers_disabled_at,
            cleanup_completed_at, slot_rollback_verified_at
        ) VALUES (
            s.state_key, s.active_version, CAST(NULL AS varchar), s.approved_cycle_id,
            s.approved_league, s.approved_season, CAST(NULL AS bigint),
            CAST(NULL AS varchar),
            CAST(NULL AS varchar), CAST(NULL AS varchar), CAST(NULL AS varchar),
            CAST(NULL AS integer), CAST(NULL AS bigint), CAST(NULL AS varchar),
            s.revision,
            CAST(NULL AS timestamp(6)), CAST(NULL AS timestamp(6)),
            CAST(NULL AS timestamp(6)), s.updated_at, s.updated_by,
            CAST(NULL AS timestamp(6)), CAST(NULL AS timestamp(6)),
            CAST(NULL AS timestamp(6))
        )""",
        f"""CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
            transition_id varchar,
            event_type varchar,
            action varchar,
            from_version varchar,
            to_version varchar,
            from_slot varchar,
            to_slot varchar,
            cycle_id varchar,
            league varchar,
            season integer,
            from_scope_set_id varchar,
            to_scope_set_id varchar,
            from_revision bigint,
            to_revision bigint,
            actor varchar,
            reason varchar,
            readiness_digest varchar,
            downstream_dq_run_id varchar,
            occurred_at timestamp(6)
        ) WITH (format = 'PARQUET')""",
        f"ALTER TABLE {HISTORY_TABLE} ADD COLUMN IF NOT EXISTS from_scope_set_id varchar",
        f"ALTER TABLE {HISTORY_TABLE} ADD COLUMN IF NOT EXISTS to_scope_set_id varchar",
        f"""CREATE TABLE IF NOT EXISTS {MODEL_MANIFEST_TABLE} (
            cycle_id varchar,
            league varchar,
            season integer,
            build_id varchar,
            candidate_slot varchar,
            model_name varchar,
            source_tables varchar,
            input_snapshot_ids varchar,
            output_table varchar,
            output_snapshot_id bigint,
            row_count bigint,
            key_hash varchar,
            dq_status varchar,
            state_revision bigint,
            scope_set_id varchar,
            pinned_input_snapshot_ids varchar,
            committed_at timestamp(6)
        ) WITH (format = 'PARQUET')""",
        f"""CREATE TABLE IF NOT EXISTS {NATIVE_WRITE_MANIFEST_TABLE} (
            cycle_id varchar,
            league varchar,
            season integer,
            entity varchar,
            native_table varchar,
            native_batch_id varchar,
            native_rows bigint,
            native_hash varchar,
            writer_revision bigint,
            write_mode varchar,
            status varchar,
            committed_at timestamp(6)
        ) WITH (format = 'PARQUET')""",
        f"ALTER TABLE {DUAL_WRITE_MANIFEST_TABLE} "
        "ADD COLUMN IF NOT EXISTS league varchar",
        f"ALTER TABLE {DUAL_WRITE_MANIFEST_TABLE} "
        "ADD COLUMN IF NOT EXISTS season integer",
        f"ALTER TABLE {HISTORY_TABLE} "
        "ADD COLUMN IF NOT EXISTS downstream_dq_run_id varchar",
        f"ALTER TABLE {HISTORY_TABLE} ADD COLUMN IF NOT EXISTS from_slot varchar",
        f"ALTER TABLE {HISTORY_TABLE} ADD COLUMN IF NOT EXISTS to_slot varchar",
        f"ALTER TABLE {MODEL_MANIFEST_TABLE} ADD COLUMN IF NOT EXISTS candidate_slot varchar",
        f"ALTER TABLE {MODEL_MANIFEST_TABLE} ADD COLUMN IF NOT EXISTS scope_set_id varchar",
        f"ALTER TABLE {MODEL_MANIFEST_TABLE} ADD COLUMN IF NOT EXISTS pinned_input_snapshot_ids varchar",
        *scope_state.ddl_statements(),
    ]


def model_contract_mapping() -> dict[str, dict[str, Any]]:
    """Machine-readable physical relation mapping for routing/DDL tooling."""
    return {
        item.name: {
            'output_table': item.output_table,
            'key_columns': list(item.key_columns),
            'source_tables': list(item.source_tables),
        }
        for item in MODEL_CONTRACTS
    }


def print_sql(statements: Iterable[str]) -> str:
    return '\n'.join(
        f'-- [{index}]\n{statement.rstrip()};'
        for index, statement in enumerate(statements, 1)
    )
