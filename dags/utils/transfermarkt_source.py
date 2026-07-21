"""Production contracts shared by the Transfermarkt source pipeline.

This module intentionally contains no Airflow imports and no migration or
reader-cutover state.  Discovery, capture, DQ and operator tooling use these
contracts directly so the source can remain native-Bronze-only.
"""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


RAW_RESPONSES_TABLE = 'iceberg.bronze.transfermarkt_raw_responses'
COMPETITIONS_TABLE = 'iceberg.bronze.transfermarkt_competitions'
EDITIONS_TABLE = 'iceberg.bronze.transfermarkt_competition_editions'
PARTICIPANTS_TABLE = 'iceberg.bronze.transfermarkt_competition_participants'
MEMBERSHIPS_TABLE = 'iceberg.bronze.transfermarkt_squad_memberships'
ATTRIBUTE_OBSERVATIONS_TABLE = (
    'iceberg.bronze.transfermarkt_player_attribute_observations'
)
CONTRACT_OBSERVATIONS_TABLE = (
    'iceberg.bronze.transfermarkt_player_contract_observations'
)
MARKET_VALUE_POINTS_TABLE = 'iceberg.bronze.transfermarkt_market_value_points'
TRANSFER_EVENTS_TABLE = 'iceberg.bronze.transfermarkt_transfer_events'
COACH_PROFILES_TABLE = 'iceberg.bronze.transfermarkt_coach_profiles'
COACH_STINTS_TABLE = 'iceberg.bronze.transfermarkt_coach_stints'

FETCH_STATE_TABLE = 'iceberg.ops.transfermarkt_fetch_state'
REGISTRY_STATE_TABLE = 'iceberg.ops.transfermarkt_registry_state'
SCOPE_MANIFEST_TABLE = 'iceberg.ops.transfermarkt_scope_manifest'
SCOPE_PLAYER_CAPTURE_TABLE = 'iceberg.ops.transfermarkt_scope_player_capture'
PROXY_LEDGER_TABLE = 'iceberg.ops.transfermarkt_proxy_ledger'

SOURCE_ENTITY_TABLES = {
    'squad_memberships': MEMBERSHIPS_TABLE,
    'player_attribute_observations': ATTRIBUTE_OBSERVATIONS_TABLE,
    'player_contract_observations': CONTRACT_OBSERVATIONS_TABLE,
    'market_value_points': MARKET_VALUE_POINTS_TABLE,
    'transfer_events': TRANSFER_EVENTS_TABLE,
    'coach_profiles': COACH_PROFILES_TABLE,
    'coach_stints': COACH_STINTS_TABLE,
}
SOURCE_ENTITIES = tuple(SOURCE_ENTITY_TABLES)

SUPPORTED_ENDPOINTS = frozenset({
    'competition_registry',
    'competition_listing',
    'squad',
    'market_value_points',
    'transfer_events',
    'coach_history',
    'coach_profile',
})

FORBIDDEN_LEGACY_RELATIONS = frozenset({
    'iceberg.bronze.transfermarkt_players',
    'iceberg.bronze.transfermarkt_market_value_history',
    'iceberg.bronze.transfermarkt_transfers',
    'iceberg.bronze.transfermarkt_coaches',
    'iceberg.ops.transfermarkt_reader_state_v2',
    'iceberg.ops.transfermarkt_reader_state_history_v2',
    'iceberg.ops.transfermarkt_dual_write_manifest_v2',
    'iceberg.ops.transfermarkt_native_write_manifest_v2',
    'iceberg.ops.transfermarkt_model_build_manifest_v2',
    'iceberg.ops.transfermarkt_scope_set_manifest_v2',
})

RAW_LINEAGE_COLUMNS = (
    'raw_capture_id',
    'source_url',
    'source_body_hash',
    'fetched_at',
    'parser_revision',
    'schema_revision',
    'cycle_id',
    'scope_id',
    '_batch_id',
    '_ingested_at',
)


@dataclass(frozen=True)
class SourceTableContract:
    name: str
    relation: str
    grain: str
    natural_key: tuple[str, ...]
    parent_relations: tuple[str, ...] = ()
    raw_backed: bool = True

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


SOURCE_TABLE_CONTRACTS = (
    SourceTableContract(
        'raw_responses', RAW_RESPONSES_TABLE,
        'one immutable HTTP capture attempt', ('capture_id',), (), False,
    ),
    SourceTableContract(
        'competitions', COMPETITIONS_TABLE,
        'one competition in one registry snapshot',
        ('registry_snapshot_id', 'competition_id'),
        (RAW_RESPONSES_TABLE,),
    ),
    SourceTableContract(
        'competition_editions', EDITIONS_TABLE,
        'one edition in one registry snapshot',
        ('registry_snapshot_id', 'competition_id', 'edition_id'),
        (COMPETITIONS_TABLE,),
    ),
    SourceTableContract(
        'competition_participants', PARTICIPANTS_TABLE,
        'one participating team in one competition edition capture',
        ('competition_id', 'edition_id', 'team_id'),
        (EDITIONS_TABLE,),
    ),
    SourceTableContract(
        'squad_memberships', MEMBERSHIPS_TABLE,
        'one current player membership in one team and edition',
        ('competition_id', 'edition_id', 'club_id', 'player_id'),
        (PARTICIPANTS_TABLE,),
    ),
    SourceTableContract(
        'player_attribute_observations', ATTRIBUTE_OBSERVATIONS_TABLE,
        'one raw-backed player observation',
        ('raw_capture_id', 'competition_id', 'edition_id', 'club_id', 'player_id'),
        (MEMBERSHIPS_TABLE, SCOPE_PLAYER_CAPTURE_TABLE),
    ),
    SourceTableContract(
        'player_contract_observations', CONTRACT_OBSERVATIONS_TABLE,
        'one raw-backed player contract observation',
        ('raw_capture_id', 'competition_id', 'edition_id', 'team_id', 'player_id'),
        (MEMBERSHIPS_TABLE, SCOPE_PLAYER_CAPTURE_TABLE),
    ),
    SourceTableContract(
        'market_value_points', MARKET_VALUE_POINTS_TABLE,
        'one valuation for a player and source date',
        ('player_id', 'mv_date'),
        (SCOPE_PLAYER_CAPTURE_TABLE,),
    ),
    SourceTableContract(
        'transfer_events', TRANSFER_EVENTS_TABLE,
        'one stable source transfer event', ('transfer_id',),
        (SCOPE_PLAYER_CAPTURE_TABLE,),
    ),
    SourceTableContract(
        'coach_profiles', COACH_PROFILES_TABLE,
        'one Transfermarkt coach profile', ('coach_id',),
        (PARTICIPANTS_TABLE,),
    ),
    SourceTableContract(
        'coach_stints', COACH_STINTS_TABLE,
        'one coach tenure at one team',
        ('club_id', 'coach_id', 'appointed_date', 'left_date'),
        (COACH_PROFILES_TABLE, PARTICIPANTS_TABLE),
    ),
)
SOURCE_CONTRACT_BY_RELATION = {
    contract.relation: contract for contract in SOURCE_TABLE_CONTRACTS
}


def validate_source_contracts() -> dict[str, Any]:
    """Fail closed when the native source contract is incomplete."""

    names = [contract.name for contract in SOURCE_TABLE_CONTRACTS]
    relations = [contract.relation for contract in SOURCE_TABLE_CONTRACTS]
    if len(names) != len(set(names)):
        raise ValueError('duplicate Transfermarkt source contract name')
    if len(relations) != len(set(relations)):
        raise ValueError('duplicate Transfermarkt source relation')
    for contract in SOURCE_TABLE_CONTRACTS:
        if not contract.name or not contract.relation or not contract.grain:
            raise ValueError(f'incomplete source contract: {contract!r}')
        if not contract.natural_key or any(not item for item in contract.natural_key):
            raise ValueError(f'{contract.name}: natural key is required')
        if contract.raw_backed and RAW_RESPONSES_TABLE not in (
            contract.parent_relations or (RAW_RESPONSES_TABLE,)
        ):
            # Raw ancestry can be direct on the registry relations or carried
            # by the typed parent/scope-capture chain for entity facts.
            if not contract.parent_relations:
                raise ValueError(f'{contract.name}: raw ancestry is required')
    overlap = set(relations) & FORBIDDEN_LEGACY_RELATIONS
    if overlap:
        raise ValueError(f'legacy relations entered source contract: {sorted(overlap)}')
    return {
        'passed': True,
        'relation_count': len(relations),
        'entity_count': len(SOURCE_ENTITIES),
    }


def _tls_verify_value() -> bool | str:
    bundle = os.environ.get('TRINO_TLS_CA_BUNDLE', '').strip()
    if not bundle:
        return True
    path = Path(bundle)
    if not path.is_absolute() or not path.is_file():
        raise RuntimeError(
            'TRINO_TLS_CA_BUNDLE must reference a readable absolute CA file'
        )
    return str(path)


def connect():
    """Create a certificate-verifying production Trino connection."""

    import trino

    host = os.environ.get('TRINO_HOST', 'localhost')
    password = os.environ.get('TRINO_PASSWORD')
    port = int(os.environ.get('TRINO_PORT', '8443' if password else '8080'))
    user = os.environ.get('TRINO_USER', 'airflow')
    kwargs: dict[str, Any] = {
        'host': host,
        'port': port,
        'user': user,
        'catalog': 'iceberg',
    }
    if password:
        from trino.auth import BasicAuthentication

        kwargs.update(
            http_scheme='https',
            auth=BasicAuthentication(user, password),
            verify=_tls_verify_value(),
        )
    return trino.dbapi.connect(**kwargs)


validate_source_contracts()
