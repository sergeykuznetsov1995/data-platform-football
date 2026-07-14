"""
Transfermarkt Silver Layer Transformation DAG
=============================================

Transforms one exact immutable Transfermarkt scope set into inactive-slot
Silver and Gold models. It is trigger-only and never infers a latest cycle or
a single league/season. Every root input is time-travel pinned once before any
CTAS, and every native output carries input_snapshot_ids/build_id/scope_set_id.

Architecture:
    validate exact ops scope manifests from bounded paid parent cycles
        |
    capture reader revision + inactive A/B slot + root input snapshots
        |
    exact bounded legacy compatibility transforms + 12 native Silver CTAS
        |
    Silver DQ + 4 native Gold CTAS + Gold DQ + model manifests
        |
    return exact scope_set_id for cutover

Silver Tables Created:
    iceberg.silver.transfermarkt_players               — typed snapshot + canonical_id (issue #60)
    iceberg.silver.transfermarkt_market_value_history  — typed MV timeline + canonical_id (issue #61)
    iceberg.silver.transfermarkt_transfers             — typed transfer events + canonical_ids (issue #62)
"""

import json
import logging
import os
import re
import tempfile
from datetime import datetime
from typing import Any, Dict, Mapping, Sequence

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup

from utils.default_args import SILVER_ARGS
from utils import transfermarkt_native_v2 as tm_v2
from utils import transfermarkt_scope_planner as tm_planner
from utils import transfermarkt_scope_state as tm_scope

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Silver transform definitions
# ---------------------------------------------------------------------------
# Each entry: (task_id, sql_file relative to /opt/airflow/, target table name).
# When sql_file ends with `.sql.j2`, _run_transform renders it via
# medallion_config.render_sql_template() before passing to run_silver_transform
# (same pattern as dag_transform_xref._run_xref_team).
SILVER_TRANSFORMS = [
    (
        'players',
        'dags/sql/silver/transfermarkt_players.sql',
        'transfermarkt_players_legacy',
    ),
    (
        'market_value_history',
        'dags/sql/silver/transfermarkt_market_value_history.sql',
        'transfermarkt_market_value_history',
    ),
    (
        'transfers',
        'dags/sql/silver/transfermarkt_transfers.sql.j2',
        'transfermarkt_transfers',
    ),
    # issue #434/#619: head coaches (dob/nationality) for gold.dim_manager
    # enrichment. `.sql.j2` — embeds manager_aliases VALUES (issue #619).
    (
        'coaches',
        'dags/sql/silver/transfermarkt_coaches.sql.j2',
        'transfermarkt_coaches_legacy',
    ),
]

# Native, global-grain contracts are built into the one inactive A/B slot.
# Global tables explicitly pass ``partition_columns=[]``; scoped facts use
# source (competition_id, edition_id), including single-year tournaments.
NATIVE_V2_TRANSFORMS = [
    # task_id, SQL path, target table, partition columns
    (
        'competitions_v2',
        'dags/sql/silver/transfermarkt_competitions_v2.sql',
        'transfermarkt_competitions_v2',
        [],
    ),
    (
        'competition_editions_v2',
        'dags/sql/silver/transfermarkt_competition_editions_v2.sql',
        'transfermarkt_competition_editions_v2',
        [],
    ),
    (
        'player_xref_global_v2',
        'dags/sql/silver/transfermarkt_player_xref_global_v2.sql',
        'transfermarkt_player_xref_global_v2',
        [],
    ),
    (
        'squad_memberships_v2',
        'dags/sql/silver/transfermarkt_squad_memberships_v2.sql',
        'transfermarkt_squad_memberships_v2',
        ['competition_id', 'edition_id'],
    ),
    (
        'player_attribute_observations_v2',
        'dags/sql/silver/transfermarkt_player_attribute_observations_v2.sql',
        'transfermarkt_player_attribute_observations_v2',
        ['competition_id', 'edition_id'],
    ),
    (
        'player_contract_observations_v2',
        'dags/sql/silver/transfermarkt_player_contract_observations_v2.sql',
        'transfermarkt_player_contract_observations_v2',
        ['competition_id', 'edition_id'],
    ),
    (
        'player_attributes_v2',
        'dags/sql/silver/transfermarkt_player_attributes_v2.sql',
        'transfermarkt_player_attributes_v2',
        [],
    ),
    (
        'market_value_points_v2',
        'dags/sql/silver/transfermarkt_market_value_points_v2.sql',
        'transfermarkt_market_value_points_v2',
        [],
    ),
    (
        'transfer_events_v2',
        'dags/sql/silver/transfermarkt_transfer_events_v2.sql.j2',
        'transfermarkt_transfer_events_v2',
        [],
    ),
    (
        'coach_profiles_v2',
        'dags/sql/silver/transfermarkt_coach_profiles_v2.sql',
        'transfermarkt_coach_profiles_v2',
        [],
    ),
    (
        'coach_stints_v2',
        'dags/sql/silver/transfermarkt_coach_stints_v2.sql',
        'transfermarkt_coach_stints_v2',
        [],
    ),
    (
        'player_team_season_assignment_v2',
        'dags/sql/silver/transfermarkt_player_team_season_assignment_v2.sql',
        'transfermarkt_player_team_season_assignment_v2',
        ['competition_id', 'edition_id'],
    ),
]

MAX_TRANSFORM_SCOPES = tm_scope.MAX_SCOPE_SET_SIZE
_DIGEST_RE = re.compile(r'^[0-9a-f]{64}$')
_LEGACY_BRONZE_RELATIONS = (
    'iceberg.bronze.transfermarkt_players',
    'iceberg.bronze.transfermarkt_market_value_history',
    'iceberg.bronze.transfermarkt_transfers',
    'iceberg.bronze.transfermarkt_coaches',
)
_NATIVE_BRONZE_SCOPE_COLUMNS = {
    'iceberg.bronze.transfermarkt_competitions': 'competition',
    'iceberg.bronze.transfermarkt_competition_editions': 'edition',
    'iceberg.bronze.transfermarkt_squad_memberships': 'edition',
    'iceberg.bronze.transfermarkt_player_attribute_observations': 'edition',
    'iceberg.bronze.transfermarkt_player_contract_observations': 'edition',
    'iceberg.bronze.transfermarkt_market_value_points': 'source_edition',
    'iceberg.bronze.transfermarkt_transfer_events': 'source_edition',
    'iceberg.bronze.transfermarkt_coach_profiles': 'source_edition',
    'iceberg.bronze.transfermarkt_coach_stints': 'source_edition',
}

NATIVE_V2_GOLD_TRANSFORMS = [
    # task_id, SQL path, target table, partition columns
    (
        'dim_manager_v2',
        'dags/sql/gold/dim_manager_v2.sql',
        'dim_manager_v2',
        [],
    ),
    (
        'fct_transfer_v2',
        'dags/sql/gold/fct_transfer_v2.sql',
        'fct_transfer_v2',
        [],
    ),
    (
        'fct_player_market_value_v2',
        'dags/sql/gold/fct_player_market_value_v2.sql',
        'fct_player_market_value_v2',
        [],
    ),
    (
        'team_season_market_value_v2',
        'dags/sql/gold/transfermarkt_team_season_market_value_v2.sql',
        'transfermarkt_team_season_market_value_v2',
        ['league', 'season'],
    ),
]

# Conservative APL cutover floors.  These are intentionally below normal
# production volume but high enough that a one-row/one-team shadow collapse
# cannot pass the readiness gate merely because its PK is unique.
NATIVE_V2_GOLD_MIN_ROWS = {
    'fct_transfer_v2': tm_v2.GOLD_MIN_ROWS['fct_transfer_v2'],
    'fct_player_market_value_v2': (
        tm_v2.GOLD_MIN_ROWS['fct_player_market_value_v2']
    ),
    'dim_manager_v2': tm_v2.GOLD_MIN_ROWS['dim_manager_v2'],
    'transfermarkt_team_season_market_value_v2': (
        tm_v2.GOLD_MIN_ROWS['team_season_market_value_v2']
    ),
}

# Expected minimum row counts per Silver table (for validation)
# transfermarkt_players: APL 2025/26 Bronze = 528 rows; DoD floor = 400.
# transfermarkt_market_value_history: ~21 точка на игрока × 528 ≈ 10 888
#   rows full-state APL 2025/26; live floor = 1000 защищает от broken CTAS,
#   DoD-инвариант ≥5000 проверяется отдельным DQ check (issue #61).
# transfermarkt_transfers: live = 750 rows / 100 игроков — Bronze ограничен
#   TRANSFERS_DAILY_LIMIT=100 + replace_partitions wipe (#486); полный ростер
#   ≈4 116 rows (DoD #62) недостижим до фикса #486. Floor = 600 защищает от
#   broken CTAS / коллапса скрейпа; revisit after #486 (#493).
SILVER_MIN_ROWS = {
    'transfermarkt_players': 400,
    'transfermarkt_market_value_history': 1000,
    'transfermarkt_transfers': 600,
    # transfermarkt_coaches: ~20 head coaches per APL season (1 per club).
    # Floor 15 protects against a broken CTAS / scrape collapse.
    'transfermarkt_coaches': 15,
}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _sql_literal(value: Any) -> str:
    if value is None:
        return 'NULL'
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _dag_conf(context: Mapping[str, Any]) -> Mapping[str, Any]:
    dag_run = context.get('dag_run')
    conf = getattr(dag_run, 'conf', None) or {}
    if not isinstance(conf, Mapping):
        from airflow.exceptions import AirflowException
        raise AirflowException('Transfermarkt transform conf must be an object')
    return conf


def _json_object(value: Any, *, label: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{label} must be a JSON object')
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f'{label} is not valid JSON') from exc
    if not isinstance(parsed, dict):
        raise ValueError(f'{label} must be a JSON object')
    return parsed


def _parse_scope_set_conf(conf: Mapping[str, Any]) -> tm_scope.ScopeSetManifest:
    """Parse and re-hash the immutable scope set sent by the ingest DAG."""

    values = [
        conf.get('transfermarkt_scope_set'),
        conf.get('transfermarkt_scope_set_manifest'),
    ]
    supplied = [value for value in values if value not in (None, '')]
    if not supplied:
        raise ValueError('transfermarkt_scope_set is required')
    parsed = [_json_object(value, label='transfermarkt_scope_set') for value in supplied]
    if any(value != parsed[0] for value in parsed[1:]):
        raise ValueError('scope-set aliases disagree')
    value = parsed[0]
    required = {
        'scope_set_id', 'registry_snapshot_id', 'capture_revision',
        'parser_revision', 'schema_revision', 'reader_revision',
        'scope_digests',
    }
    if set(value) != required:
        raise ValueError(
            'scope set fields differ: '
            f'missing={sorted(required - set(value))}, '
            f'extra={sorted(set(value) - required)}'
        )
    digests_value = value['scope_digests']
    if (
        not isinstance(digests_value, Sequence)
        or isinstance(digests_value, (str, bytes))
    ):
        raise ValueError('scope_digests must be an array')
    scope_digests: list[tuple[str, str]] = []
    for item in digests_value:
        if (
            not isinstance(item, Sequence)
            or isinstance(item, (str, bytes))
            or len(item) != 2
        ):
            raise ValueError('each scope digest must be [scope_id, sha256]')
        scope_id, digest = str(item[0]).strip(), str(item[1]).strip()
        if not scope_id or not _DIGEST_RE.fullmatch(digest):
            raise ValueError('scope digest identity is invalid')
        scope_digests.append((scope_id, digest))
    if not 1 <= len(scope_digests) <= MAX_TRANSFORM_SCOPES:
        raise ValueError(
            f'scope set must contain 1..{MAX_TRANSFORM_SCOPES} scopes'
        )
    if scope_digests != sorted(scope_digests):
        raise ValueError('scope_digests must use canonical scope-id order')
    if len({item[0] for item in scope_digests}) != len(scope_digests):
        raise ValueError('scope set contains duplicate scope ids')
    if len({item[1] for item in scope_digests}) != len(scope_digests):
        raise ValueError('scope set contains duplicate manifest digests')
    for field in (
        'registry_snapshot_id', 'capture_revision', 'parser_revision',
        'schema_revision',
    ):
        if not str(value[field]).strip():
            raise ValueError(f'scope-set {field} is empty')
    try:
        if isinstance(value['reader_revision'], bool):
            raise TypeError
        reader_revision = int(value['reader_revision'])
    except (TypeError, ValueError) as exc:
        raise ValueError('scope-set reader revision must be an integer') from exc
    if reader_revision < 0:
        raise ValueError('scope-set reader revision must be non-negative')
    scope_set = tm_scope.ScopeSetManifest(
        scope_set_id=str(value['scope_set_id']),
        registry_snapshot_id=str(value['registry_snapshot_id']),
        capture_revision=str(value['capture_revision']),
        parser_revision=str(value['parser_revision']),
        schema_revision=str(value['schema_revision']),
        reader_revision=reader_revision,
        scope_digests=tuple(scope_digests),
    )
    identity = {
        'registry_snapshot_id': scope_set.registry_snapshot_id,
        'capture_revision': scope_set.capture_revision,
        'parser_revision': scope_set.parser_revision,
        'schema_revision': scope_set.schema_revision,
        'reader_revision': scope_set.reader_revision,
        'scope_digests': scope_set.scope_digests,
    }
    expected_id = tm_scope.stable_hash(identity)
    if not _DIGEST_RE.fullmatch(scope_set.scope_set_id):
        raise ValueError('scope_set_id must be a lowercase sha256 digest')
    if scope_set.scope_set_id != expected_id:
        raise ValueError('scope_set_id does not match immutable scope contents')
    conf_id = str(conf.get('transfermarkt_scope_set_id') or '')
    if conf_id != scope_set.scope_set_id:
        raise ValueError('transfermarkt_scope_set_id disagrees with manifest')
    conf_revision = conf.get('transfermarkt_reader_revision')
    if conf_revision is None or int(conf_revision) != scope_set.reader_revision:
        raise ValueError('transfermarkt_reader_revision disagrees with scope set')
    return scope_set


def _entity_json(value: Any) -> Mapping[str, Any]:
    """Parse the exact child evidence object, including immutable scope DQ."""

    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError('entity_manifest_json is invalid JSON') from exc
    if not isinstance(value, Mapping) or set(value) != {
        'entities', 'dq_evidence',
    }:
        raise ValueError(
            'entity_manifest_json must contain exact entities and dq_evidence'
        )
    entities = value['entities']
    if not isinstance(entities, Sequence) or isinstance(entities, (str, bytes)):
        raise ValueError('entity_manifest_json entities must be an array')
    if not all(isinstance(item, Mapping) for item in entities):
        raise ValueError('entity_manifest_json contains a non-object entity')
    if not isinstance(value['dq_evidence'], Mapping):
        raise ValueError('entity_manifest_json dq_evidence must be an object')
    return {
        'entities': entities,
        'dq_evidence': value['dq_evidence'],
    }


def _load_scope_manifests(
    cur: Any,
    *,
    scope_set: tm_scope.ScopeSetManifest,
) -> tuple[tm_scope.ScopeManifest, ...]:
    """Read only content-addressed child rows, across any paid parent cycle."""

    expected = dict(scope_set.scope_digests)
    digest_sql = ', '.join(_sql_literal(item) for item in expected.values())
    cur.execute(f"""
SELECT parent_cycle_id, child_cycle_id, scope_id, competition_id, edition_id,
       canonical_competition_id, canonical_season, registry_snapshot_id,
       capture_revision, parser_revision, schema_revision, reader_revision,
       entity_manifest_json, manifest_digest, status
FROM {tm_scope.SCOPE_MANIFEST_TABLE}
WHERE manifest_digest IN ({digest_sql})
ORDER BY scope_id
""")
    rows = list(cur.fetchall())
    if len(rows) != len(expected):
        raise tm_scope.ScopeManifestError(
            'scope manifest row count differs from immutable scope set'
        )

    manifests: list[tm_scope.ScopeManifest] = []
    seen: set[str] = set()
    for row in rows:
        if len(row) != 15:
            raise tm_scope.ScopeManifestError('scope manifest query shape drift')
        (
            parent, child, scope_id, competition, edition,
            canonical_competition, canonical_season, registry_snapshot,
            capture_revision, parser_revision, schema_revision,
            reader_revision, entity_json, manifest_digest, status,
        ) = row
        scope_id = str(scope_id)
        if scope_id in seen:
            raise tm_scope.ScopeManifestError('duplicate scope manifest row')
        seen.add(scope_id)
        if str(status) != tm_scope.SCOPE_COMPLETION_STATUS:
            raise tm_scope.ScopeManifestError(
                f'{scope_id}: scope manifest is not complete/successful'
            )
        manifest_evidence = _entity_json(entity_json)
        value = {
            'parent_cycle_id': parent,
            'child_cycle_id': child,
            'scope_id': scope_id,
            'competition_id': competition,
            'edition_id': edition,
            'canonical_competition_id': canonical_competition,
            'canonical_season': canonical_season,
            'registry_snapshot_id': registry_snapshot,
            'capture_revision': capture_revision,
            'parser_revision': parser_revision,
            'schema_revision': schema_revision,
            'reader_revision': reader_revision,
            **manifest_evidence,
        }
        manifest = tm_scope.ScopeManifest.from_mapping(value)
        manifest.validate(tm_v2.NATIVE_ENTITIES)
        if manifest.digest != str(manifest_digest):
            raise tm_scope.ScopeManifestError(
                f'{scope_id}: persisted manifest digest mismatch'
            )
        if expected.get(scope_id) != manifest.digest:
            raise tm_scope.ScopeManifestError(
                f'{scope_id}: digest is absent from immutable scope set'
            )
        manifests.append(manifest)

    rebuilt = tm_scope.ScopeSetManifest.build(
        manifests,
        expected_entities=tm_v2.NATIVE_ENTITIES,
        reader_revision=scope_set.reader_revision,
    )
    if rebuilt != scope_set:
        raise tm_scope.ScopeManifestError(
            'persisted manifests do not rebuild the supplied scope set'
        )
    if len({item.child_cycle_id for item in manifests}) != len(manifests):
        raise tm_scope.ScopeManifestError('scope set reuses a child cycle id')
    identities = {(item.competition_id, item.edition_id) for item in manifests}
    if len(identities) != len(manifests):
        raise tm_scope.ScopeManifestError(
            'scope set duplicates a competition/edition identity'
        )
    return tuple(manifests)


def _assert_complete_promoted_registry_target(
    cur: Any,
    *,
    scope_set: tm_scope.ScopeSetManifest,
    manifests: Sequence[tm_scope.ScopeManifest],
) -> dict[str, Any]:
    """Block a direct partial-slot trigger before its first production write."""

    snapshot = scope_set.registry_snapshot_id
    cur.execute(f"""
SELECT registry_snapshot_id, status, unknown_active_count
FROM {tm_planner.REGISTRY_STATE_TABLE}
WHERE state_key = 'canonical'
""")
    state_rows = list(cur.fetchall())
    if len(state_rows) != 1:
        raise tm_scope.ScopeManifestError(
            'promoted registry must have exactly one canonical state row'
        )
    row_snapshot, status, unknown_active_count = state_rows[0]
    if (
        str(row_snapshot) != snapshot
        or str(status) != 'promoted'
        or int(unknown_active_count) != 0
    ):
        raise tm_scope.ScopeManifestError(
            'scope set is not bound to the green promoted registry snapshot'
        )

    cur.execute(f"""
SELECT c.competition_id, e.edition_id
FROM {tm_planner.COMPETITIONS_TABLE} c
JOIN {tm_planner.EDITIONS_TABLE} e
  ON e.competition_id = c.competition_id
 AND e.registry_snapshot_id = c.registry_snapshot_id
WHERE c.registry_snapshot_id = {_sql_literal(snapshot)}
  AND c.active = true
  AND c.classification_status = 'eligible'
  AND c.gender = 'men'
  AND c.age_category = 'senior'
  AND c.team_type IN ('club', 'national_team')
  AND c.competition_type IN (
      'domestic_league', 'domestic_cup', 'continental_club',
      'national_team_tournament'
  )
  AND e.active = true
ORDER BY c.competition_id, e.edition_id
""")
    target_rows = list(cur.fetchall())
    if any(len(row) != 2 for row in target_rows):
        raise tm_scope.ScopeManifestError('promoted registry target shape drift')
    targets = {(str(row[0]), str(row[1])) for row in target_rows}
    if not targets or len(targets) != len(target_rows):
        raise tm_scope.ScopeManifestError(
            'promoted registry target is empty or duplicated'
        )
    actual = {
        (item.competition_id, item.edition_id) for item in manifests
    }
    if actual != targets:
        raise tm_scope.ScopeManifestError(
            'scope set is not the complete promoted registry target: '
            f'missing={sorted(targets - actual)}, '
            f'extra={sorted(actual - targets)}'
        )
    return {
        'registry_snapshot_id': snapshot,
        'target_scope_count': len(targets),
        'complete': True,
    }


def _manual_scope_bundle(conf: Mapping[str, Any]) -> dict[str, Any]:
    """Explicit unscheduled compatibility path; never enables native build."""

    if conf.get('transfermarkt_manual_single_scope') is not True:
        raise ValueError('production transform requires an immutable scope set')
    cycle_id = str(conf.get('transfermarkt_cycle_id') or '').strip()
    league = str(conf.get('transfermarkt_league') or '').strip()
    season_value = conf.get('transfermarkt_season')
    if not cycle_id or not league or season_value is None:
        raise ValueError('manual compatibility scope is incomplete')
    season = int(season_value)
    if not 2000 <= season <= 2100:
        raise ValueError('manual compatibility season is invalid')
    revision = conf.get('transfermarkt_reader_revision')
    if revision is None or int(revision) < 0:
        raise ValueError('manual compatibility reader revision is required')
    return {
        'mode': 'manual_single_scope',
        'parent_cycle_id': cycle_id,
        'scope_set_id': None,
        'reader_revision': int(revision),
        'candidate_slot': None,
        'registry_snapshot_id': None,
        'capture_revision': None,
        'parser_revision': None,
        'schema_revision': None,
        'scope_digests': [],
        'scopes': [{
            'scope_id': f'manual:{league}:{season}',
            'competition_id': league,
            'edition_id': str(season),
            'canonical_competition_id': league,
            'canonical_season': f'{season % 100:02d}{(season + 1) % 100:02d}',
        }],
        'traffic': None,
    }


def _validate_transform_scope_set(**context) -> dict[str, Any]:
    """Blocking production entry gate before any Silver/Gold write."""

    from airflow.exceptions import AirflowException

    conf = _dag_conf(context)
    try:
        if conf.get('transfermarkt_manual_single_scope') is True:
            return _manual_scope_bundle(conf)
        parent_cycle_id = str(
            conf.get('transfermarkt_parent_cycle_id') or ''
        ).strip()
        if not parent_cycle_id:
            raise ValueError('transfermarkt_parent_cycle_id is required')
        scope_set = _parse_scope_set_conf(conf)
        candidate_slot = tm_v2._normalise_slot(
            conf.get('transfermarkt_candidate_slot')
        )
        conn = tm_v2.connect()
        cur = conn.cursor()
        try:
            manifests = _load_scope_manifests(
                cur,
                scope_set=scope_set,
            )
            registry_coverage = _assert_complete_promoted_registry_target(
                cur,
                scope_set=scope_set,
                manifests=manifests,
            )
        finally:
            cur.close()
            conn.close()
        traffic = tm_scope.aggregate_traffic(manifests)
        parent_cycle_ids = sorted({
            item.parent_cycle_id for item in manifests
        })
        traffic_by_parent = {
            parent: tm_scope.aggregate_traffic([
                item for item in manifests
                if item.parent_cycle_id == parent
            ])
            for parent in parent_cycle_ids
        }
        return {
            'mode': 'scope_set',
            'parent_cycle_id': parent_cycle_id,
            'scope_set_id': scope_set.scope_set_id,
            'reader_revision': scope_set.reader_revision,
            'candidate_slot': candidate_slot,
            'registry_snapshot_id': scope_set.registry_snapshot_id,
            'capture_revision': scope_set.capture_revision,
            'parser_revision': scope_set.parser_revision,
            'schema_revision': scope_set.schema_revision,
            'scope_digests': [list(item) for item in scope_set.scope_digests],
            'source_parent_cycle_ids': parent_cycle_ids,
            'scopes': [
                {
                    'scope_id': item.scope_id,
                    'competition_id': item.competition_id,
                    'edition_id': item.edition_id,
                    'canonical_competition_id': item.canonical_competition_id,
                    'canonical_season': item.canonical_season,
                    'child_cycle_id': item.child_cycle_id,
                    'manifest_digest': item.digest,
                }
                for item in manifests
            ],
            'traffic': traffic,
            'traffic_by_parent': traffic_by_parent,
            'registry_coverage': registry_coverage,
        }
    except (TypeError, ValueError, tm_scope.ScopeManifestError) as exc:
        raise AirflowException(
            f'Transfermarkt scope-set preflight failed closed: {exc}'
        ) from exc


def _transform_scope(context: Mapping[str, Any]) -> dict[str, Any]:
    ti = context.get('ti')
    value = (
        ti.xcom_pull(task_ids='validate_transform_scope_set')
        if ti is not None else None
    )
    if not isinstance(value, Mapping):
        raise RuntimeError('validate_transform_scope_set XCom is missing')
    return dict(value)


def _scope_predicate(
    scopes: Sequence[Mapping[str, Any]],
    *,
    competition_column: str,
    edition_column: str,
) -> str:
    return ' OR '.join(
        '('
        f'{competition_column} = {_sql_literal(item["competition_id"])} AND '
        f'{edition_column} = {_sql_literal(item["edition_id"])}'
        ')'
        for item in scopes
    )


def _replace_relation(sql: str, relation: str, replacement: str) -> str:
    return re.sub(
        rf'(?<![A-Za-z0-9_"])({re.escape(relation)})(?![A-Za-z0-9_"])',
        replacement,
        sql,
    )


def _scope_native_sql(
    sql: str,
    scopes: Sequence[Mapping[str, Any]],
    *,
    registry_snapshot_id: str,
) -> str:
    """Restrict every raw native source to the immutable scope membership."""

    result = sql
    competition_ids = sorted({str(item['competition_id']) for item in scopes})
    for relation, kind in _NATIVE_BRONZE_SCOPE_COLUMNS.items():
        if kind == 'competition':
            ids = ', '.join(_sql_literal(item) for item in competition_ids)
            predicate = (
                f'competition_id IN ({ids}) AND registry_snapshot_id = '
                f'{_sql_literal(registry_snapshot_id)}'
            )
        elif kind == 'edition':
            predicate = _scope_predicate(
                scopes,
                competition_column='competition_id',
                edition_column='edition_id',
            )
        else:
            predicate = _scope_predicate(
                scopes,
                competition_column='source_competition_id',
                edition_column='source_edition_id',
            )
        replacement = f'(SELECT * FROM {relation} WHERE {predicate})'
        result = _replace_relation(result, relation, replacement)
    return result


def _scope_legacy_sql(
    sql: str,
    scopes: Sequence[Mapping[str, Any]],
) -> str:
    predicate = ' OR '.join(
        '('
        f'league = {_sql_literal(item["canonical_competition_id"])} AND '
        f'season = {_sql_literal(item["canonical_season"])}'
        ')'
        for item in scopes
    )
    result = sql
    for relation in _LEGACY_BRONZE_RELATIONS:
        result = _replace_relation(
            result,
            relation,
            f'(SELECT * FROM {relation} WHERE {predicate})',
        )
    return result


def _pin_sql_relations(sql: str, snapshots: Mapping[str, Any]) -> str:
    """Apply Trino time travel to every external table in the pinned set."""

    result = sql
    for relation in sorted(snapshots, key=len, reverse=True):
        snapshot_id = int(snapshots[relation])
        if snapshot_id <= 0:
            raise RuntimeError(f'invalid pinned snapshot for {relation}')
        result = _replace_relation(
            result,
            relation,
            f'{relation} FOR VERSION AS OF {snapshot_id}',
        )
    return result


def _captured_pins(context: Mapping[str, Any]) -> dict[str, Any]:
    ti = context.get('ti')
    value = (
        ti.xcom_pull(task_ids='pin_transform_input_snapshots')
        if ti is not None else None
    )
    if not isinstance(value, Mapping):
        raise RuntimeError('pin_transform_input_snapshots XCom is missing')
    scope = _transform_scope(context)
    if value.get('scope_set_id') != scope.get('scope_set_id'):
        raise RuntimeError('pinned snapshots belong to another scope set')
    expected = tm_scope.stable_hash({
        'scope_set_id': value.get('scope_set_id'),
        'parent_cycle_id': value.get('parent_cycle_id'),
        'input_snapshot_ids': value.get('input_snapshot_ids'),
    })
    if value.get('snapshot_set_id') != expected:
        raise RuntimeError('pinned input snapshot set digest mismatch')
    return dict(value)


def _pin_transform_input_snapshots(**context) -> dict[str, Any]:
    """Capture every root Iceberg input exactly once before candidate CTAS."""

    scope = _transform_scope(context)
    reader = _captured_reader_state(context)
    if int(reader['revision']) != int(scope['reader_revision']):
        raise RuntimeError('reader revision drift before input snapshot pin')
    candidate_slot = reader.get('candidate_slot')
    if scope['mode'] == 'scope_set':
        selected_slot = tm_v2._normalise_slot(candidate_slot)
        outputs = {
            tm_v2.contract_output_table(contract, selected_slot)
            for contract in tm_v2.MODEL_CONTRACTS
        }
        sources = {
            source
            for contract in tm_v2.MODEL_CONTRACTS
            for source in tm_v2.contract_source_tables(contract, selected_slot)
        }
        root_tables = sources - outputs
        root_tables.add(tm_scope.SCOPE_MANIFEST_TABLE)
        # #948: the Bronze DQ gate anti-joins native Bronze against the
        # non-slotted promoted-registry relations; pin them as roots so the
        # gate and the candidate CTAS read one consistent registry snapshot.
        # Safe wrt _run_transform rewrites: rewrite_native_relations() runs
        # before _pin_sql_relations(), so native SQL only contains slotted
        # *_v2_<slot> names which these non-slotted pins can never touch.
        root_tables.add(tm_planner.COMPETITIONS_TABLE)
        root_tables.add(tm_planner.EDITIONS_TABLE)
    else:
        root_tables = set()
    if reader.get('legacy_writers_disabled_at') is None:
        root_tables.update(_LEGACY_BRONZE_RELATIONS)
    if not root_tables:
        raise RuntimeError('transform input snapshot set is empty')

    conn = tm_v2.connect()
    cur = conn.cursor()
    try:
        tm_v2.assert_reader_revision(cur, int(reader['revision']))
        snapshots = {}
        for table in sorted(root_tables):
            catalog, schema, name = table.split('.', 2)
            cur.execute(
                f'SELECT snapshot_id FROM {catalog}.{schema}."{name}$snapshots" '
                'ORDER BY committed_at DESC LIMIT 1'
            )
            rows = list(cur.fetchall())
            if len(rows) != 1 or int(rows[0][0]) <= 0:
                raise RuntimeError(f'no unique Iceberg snapshot for {table}')
            snapshots[table] = int(rows[0][0])
        tm_v2.assert_reader_revision(cur, int(reader['revision']))
    finally:
        cur.close()
        conn.close()
    payload = {
        'scope_set_id': scope.get('scope_set_id'),
        'parent_cycle_id': scope['parent_cycle_id'],
        'input_snapshot_ids': snapshots,
    }
    return {
        **payload,
        'snapshot_set_id': tm_scope.stable_hash(payload),
    }


def _transform_write_tables(
    *,
    phase: str,
    candidate_slot: str,
    include_legacy: bool,
    include_native: bool = True,
) -> tuple[str, ...]:
    if phase not in {'silver', 'gold'}:
        raise ValueError('write approval phase must be silver or gold')
    layer = f'iceberg.{phase}.'
    tables = set()
    if include_native:
        tables.update(
            tm_v2.contract_output_table(contract, candidate_slot)
            for contract in tm_v2.MODEL_CONTRACTS
            if contract.output_table.startswith(layer)
        )
        if phase == 'silver':
            tables.add(tm_scope.SCOPE_SET_MANIFEST_TABLE)
        else:
            tables.add(tm_v2.MODEL_MANIFEST_TABLE)
    if phase == 'silver' and include_legacy:
        tables.update(
            f'iceberg.silver.{table_name}'
            for _, _, table_name in SILVER_TRANSFORMS
        )
    return tuple(sorted(tables))


def _transform_write_argv(
    *,
    phase: str,
    parent_cycle_id: str,
    scope_set_id: str,
    reader_revision: int,
    candidate_slot: str,
    build_id: str,
) -> tuple[str, ...]:
    """Canonical execution identity stored in the one-shot approval packet."""

    return (
        'airflow',
        'tasks',
        'run',
        'dag_transform_transfermarkt_silver',
        f'authorize_{phase}_writes',
        str(build_id),
        '--parent-cycle-id',
        str(parent_cycle_id),
        '--scope-set-id',
        str(scope_set_id),
        '--reader-revision',
        str(int(reader_revision)),
        '--candidate-slot',
        str(candidate_slot),
    )


def _authorize_write_phase(
    *, phase: str, context: Mapping[str, Any],
) -> dict[str, Any]:
    """Consume one exact phase approval before its first production write."""

    from pathlib import Path

    from airflow.exceptions import AirflowException
    from utils.transfermarkt_approval import (
        ApprovalJournal,
        ApprovalPacket,
        ApprovalStateError,
        ApprovalValidationError,
    )

    scope = _transform_scope(context)
    if scope.get('mode') != 'scope_set':
        # The explicitly requested manual compatibility path is unscheduled,
        # but it still writes. It therefore uses the same fresh approval gate.
        scope_set_id = 'manual-' + tm_scope.stable_hash(scope)[:57]
    else:
        scope_set_id = str(scope['scope_set_id'])
    reader = _captured_reader_state(context)
    # Pins are intentionally not part of the pre-issued packet argv: upstream
    # cannot know post-Bronze Iceberg snapshot IDs. They are nevertheless a
    # required, rehashed XCom before approval consumption and every write.
    _captured_pins(context)
    conf = _dag_conf(context)
    packet_id = str(conf.get(
        f'transfermarkt_{phase}_write_approval_packet_id'
    ) or '').strip()
    packet_hash = str(conf.get(
        f'transfermarkt_{phase}_write_approval_packet_hash'
    ) or '').strip()
    journal_value = str(conf.get('transfermarkt_approval_journal') or '').strip()
    if not packet_id or not _DIGEST_RE.fullmatch(packet_hash) or not journal_value:
        raise AirflowException(
            f'fresh {phase} write approval packet id/hash/journal are required'
        )
    journal_path = Path(journal_value)
    if not journal_path.is_absolute():
        raise AirflowException('transform approval journal path must be absolute')
    dag_run = context.get('dag_run')
    build_id = getattr(dag_run, 'run_id', None)
    if not build_id:
        raise AirflowException('transform build run_id is missing')
    candidate_slot = (
        tm_v2._normalise_slot(reader.get('candidate_slot'))
        if scope.get('mode') == 'scope_set'
        else tm_v2.inactive_slot(tm_v2.ReaderState(**{
            key: value
            for key, value in reader.items()
            if key in tm_v2.ReaderState.__dataclass_fields__
        }))
    )
    expected_argv = _transform_write_argv(
        phase=phase,
        parent_cycle_id=scope['parent_cycle_id'],
        scope_set_id=scope_set_id,
        reader_revision=int(reader['revision']),
        candidate_slot=candidate_slot,
        build_id=str(build_id),
    )
    expected_tables = _transform_write_tables(
        phase=phase,
        candidate_slot=candidate_slot,
        include_legacy=(
            phase == 'silver'
            and reader.get('legacy_writers_disabled_at') is None
        ),
        include_native=scope.get('mode') == 'scope_set',
    )
    journal = ApprovalJournal(journal_path)
    try:
        record = journal.get(packet_hash)
        packet_value = json.loads(record.canonical_json)
        packet = ApprovalPacket(**packet_value)
    except (
        ApprovalStateError, ApprovalValidationError, TypeError,
        json.JSONDecodeError,
    ) as exc:
        raise AirflowException(f'transform approval packet is invalid: {exc}') from exc
    if record.packet_id != packet_id or packet.packet_id != packet_id:
        raise AirflowException('transform approval packet id drift')
    if packet.packet_hash != packet_hash:
        raise AirflowException('transform approval packet hash drift')
    if packet.action != 'production_write':
        raise AirflowException('transform approval action is not production_write')
    if packet.argv != expected_argv:
        raise AirflowException('transform approval argv drift')
    if packet.byte_cap_bytes != 0 or packet.request_limit != 0:
        raise AirflowException('transform write approval must authorize zero proxy I/O')
    if packet.retry_limit != 0 or packet.concurrency != 1:
        raise AirflowException('transform write retry/concurrency approval drift')
    if tuple(sorted(packet.affected_tables)) != expected_tables:
        raise AirflowException('transform write table assets drift')
    if tuple(packet.affected_files) != (str(journal_path.resolve()),):
        raise AirflowException('transform write file assets drift')
    try:
        consumed = journal.consume(
            packet,
            presented_hash=packet_hash,
            execution_argv=expected_argv,
        )
    except ApprovalStateError as exc:
        raise AirflowException(
            f'transform write approval is not consumable: {exc}'
        ) from exc
    return {
        'packet_id': packet_id,
        'packet_hash': packet_hash,
        'status': consumed.status,
        'scope_set_id': scope.get('scope_set_id'),
        'candidate_slot': candidate_slot,
        'phase': phase,
        'affected_tables': list(expected_tables),
    }


def _validate_bronze_quality(**context) -> dict[str, Any]:
    """Blocking cross-table Bronze DQ before the Silver write approval (#948).

    Runs between ``pin_transform_input_snapshots`` and
    ``authorize_silver_writes`` so a red Bronze fails the run BEFORE the
    one-shot write-approval packet is consumed.  ERROR results gate; WARNING
    results are observability-only.  The manual compatibility path writes
    only legacy tables, so it runs only the legacy Bronze zone.
    """

    from airflow.exceptions import AirflowException
    from utils import transfermarkt_bronze_dq as bronze_dq
    from utils.alerts import telegram_dq_summary
    from utils.medallion_config import load_competitions

    scope = _transform_scope(context)
    pins = _captured_pins(context)
    input_snapshots = dict(pins.get('input_snapshot_ids') or {})
    legacy_allowlist = sorted({
        (str(competition['id']), str(season['id']))
        for competition in load_competitions()['competitions']
        for season in competition.get('seasons', [])
    })

    zone = 'scope_set' if scope.get('mode') == 'scope_set' else 'legacy'
    conn = tm_v2.connect()
    cur = conn.cursor()
    try:
        if zone == 'scope_set':
            scope_set = _parse_scope_set_conf(_dag_conf(context))
            manifests = _load_scope_manifests(cur, scope_set=scope_set)
            results = bronze_dq.run_bronze_dq(
                cur,
                registry_snapshot_id=scope['registry_snapshot_id'],
                pins=input_snapshots,
                zone='scope_set',
                manifests=manifests,
                legacy_allowlist=legacy_allowlist,
            )
        else:
            results = bronze_dq.run_bronze_dq(
                cur,
                registry_snapshot_id=scope.get('registry_snapshot_id'),
                pins=input_snapshots,
                zone='legacy',
                legacy_allowlist=legacy_allowlist,
            )
    except (TypeError, ValueError, tm_scope.ScopeManifestError) as exc:
        raise AirflowException(
            f'Transfermarkt Bronze DQ failed closed: {exc}'
        ) from exc
    finally:
        cur.close()
        conn.close()

    report = bronze_dq.BronzeDqReport(list(results))
    logger.info('TM Bronze DQ: %s', report.summary())
    telegram_dq_summary(report, header='TM Bronze DQ')

    if report.errors:
        raise AirflowException(
            f'TM Bronze DQ failed: {len(report.errors)} error(s): '
            + '; '.join(
                f'{r.name}: {r.details or r.error}' for r in report.errors[:5]
            )
        )

    def _value(name: str) -> Any:
        return next((r.value for r in results if r.name == name), None)

    return {
        'zone': zone,
        'passed': len(report.passed),
        'total': len(report.results),
        'errors': [r.name for r in report.errors],
        'warnings': [r.name for r in report.warnings],
        'coverage': _value('tm_target_scope_coverage'),
        'career_debt': _value('tm_career_debt'),
    }


def _authorize_silver_writes(**context) -> dict[str, Any]:
    return _authorize_write_phase(phase='silver', context=context)


def _authorize_gold_writes(**context) -> dict[str, Any]:
    scope = _transform_scope(context)
    if scope.get('mode') != 'scope_set':
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException('manual compatibility path has no Gold writes')
    return _authorize_write_phase(phase='gold', context=context)


def _persist_scope_set_manifest(**context) -> dict[str, Any]:
    """Idempotently persist the exact rehashed scope set after approval."""

    scope = _transform_scope(context)
    if scope.get('mode') != 'scope_set':
        return {'status': 'not_applicable', 'scope_set_id': None}
    reader = _captured_reader_state(context)
    ti = context.get('ti')
    approval = (
        ti.xcom_pull(task_ids='authorize_silver_writes')
        if ti is not None else None
    )
    if not isinstance(approval, Mapping) or approval.get('status') != 'consumed':
        raise RuntimeError('scope-set persist requires consumed write approval')
    scope_digests_json = json.dumps(
        scope['scope_digests'], sort_keys=True, separators=(',', ':'),
    )
    traffic_json = json.dumps(
        scope['traffic'], sort_keys=True, separators=(',', ':'),
    )
    expected = (
        str(scope['registry_snapshot_id']),
        str(scope['capture_revision']),
        str(scope['parser_revision']),
        str(scope['schema_revision']),
        int(scope['reader_revision']),
        scope_digests_json,
        traffic_json,
        'success',
    )
    conn = tm_v2.connect()
    cur = conn.cursor()
    try:
        tm_v2.assert_reader_revision(cur, int(reader['revision']))
        select_sql = (
            'SELECT registry_snapshot_id, capture_revision, parser_revision, '
            'schema_revision, reader_revision, scope_digests_json, '
            f'traffic_json, status FROM {tm_scope.SCOPE_SET_MANIFEST_TABLE} '
            f"WHERE scope_set_id = {_sql_literal(scope['scope_set_id'])}"
        )
        cur.execute(select_sql)
        rows = list(cur.fetchall())
        if len(rows) > 1:
            raise RuntimeError('duplicate persisted scope_set_id rows')
        if rows:
            actual = tuple(rows[0])
            if actual != expected:
                raise RuntimeError('persisted scope-set row content drift')
            status = 'already_persisted'
        else:
            cur.execute(f"""
INSERT INTO {tm_scope.SCOPE_SET_MANIFEST_TABLE} (
    scope_set_id, registry_snapshot_id, capture_revision, parser_revision,
    schema_revision, reader_revision, scope_digests_json, traffic_json,
    status, committed_at
) VALUES (
    {_sql_literal(scope['scope_set_id'])},
    {_sql_literal(scope['registry_snapshot_id'])},
    {_sql_literal(scope['capture_revision'])},
    {_sql_literal(scope['parser_revision'])},
    {_sql_literal(scope['schema_revision'])},
    {int(scope['reader_revision'])},
    {_sql_literal(scope_digests_json)}, {_sql_literal(traffic_json)},
    'success', CURRENT_TIMESTAMP
)
""")
            cur.fetchall()
            status = 'persisted'
        tm_v2.assert_reader_revision(cur, int(reader['revision']))
    finally:
        cur.close()
        conn.close()
    return {
        'status': status,
        'scope_set_id': scope['scope_set_id'],
        'scope_digests': scope['scope_digests'],
        'traffic': scope['traffic'],
    }


def _inject_model_lineage(
    sql: str,
    *,
    input_snapshot_ids: Mapping[str, int],
    build_id: str,
    scope_set_id: str,
) -> str:
    """Add the three mandatory model-lineage columns exactly once."""

    for column in ('input_snapshot_ids', 'build_id', 'scope_set_id'):
        if re.search(rf'(?i)\b{column}\b', sql):
            raise RuntimeError(
                f'native model SQL already exposes {column}; refusing duplicate lineage'
            )
    if not str(build_id).strip():
        raise RuntimeError('native model build_id is missing')
    if not _DIGEST_RE.fullmatch(str(scope_set_id)):
        raise RuntimeError('native model scope_set_id is invalid')
    snapshots_json = json.dumps(
        dict(sorted(input_snapshot_ids.items())),
        sort_keys=True,
        separators=(',', ':'),
    )
    return f"""SELECT
    model_rows.*,
    CAST({_sql_literal(snapshots_json)} AS varchar) AS input_snapshot_ids,
    CAST({_sql_literal(build_id)} AS varchar) AS build_id,
    CAST({_sql_literal(scope_set_id)} AS varchar) AS scope_set_id
FROM (
{sql}
) model_rows"""


def _model_lineage_violation_where(context: Mapping[str, Any]) -> str:
    scope = _transform_scope(context)
    pins = _captured_pins(context)
    dag_run = context.get('dag_run')
    build_id = getattr(dag_run, 'run_id', None)
    if not build_id:
        raise RuntimeError('model lineage DQ build_id is missing')
    snapshots_json = json.dumps(
        dict(sorted(pins['input_snapshot_ids'].items())),
        sort_keys=True,
        separators=(',', ':'),
    )
    return ' OR '.join((
        f'input_snapshot_ids IS DISTINCT FROM {_sql_literal(snapshots_json)}',
        f'build_id IS DISTINCT FROM {_sql_literal(build_id)}',
        f"scope_set_id IS DISTINCT FROM {_sql_literal(scope['scope_set_id'])}",
    ))


def _run_transform(
    sql_file: str,
    table_name: str,
    partition_columns=None,
    schema: str = 'silver',
    use_candidate_slot: bool = False,
    **context,
) -> Dict[str, Any]:
    """PythonOperator callable — run a single Silver CTAS transform.

    If sql_file ends with `.sql.j2`, render it through
    ``medallion_config.render_sql_template`` (embedding the team-alias
    VALUES via ``{{ team_aliases_values_sql }}``) and pass the rendered
    SQL to ``run_silver_transform`` via a tempfile. This mirrors the
    pattern used in ``dag_transform_xref._run_xref_team``.
    """
    from pathlib import Path

    from utils.silver_tasks import run_silver_transform

    scope = _transform_scope(context)
    pins = _captured_pins(context)
    input_snapshots = pins.get('input_snapshot_ids')
    if not isinstance(input_snapshots, Mapping) or not input_snapshots:
        raise RuntimeError('pinned input snapshot map is empty')
    slot = None
    reader = _captured_reader_state(context)
    if use_candidate_slot:
        if scope.get('mode') != 'scope_set':
            from airflow.exceptions import AirflowSkipException
            raise AirflowSkipException(
                'native v2 build requires an immutable production scope set'
            )
        slot = tm_v2._normalise_slot(reader.get('candidate_slot'))
        conn = tm_v2.connect()
        cur = conn.cursor()
        try:
            state = tm_v2.assert_reader_revision(
                cur, int(reader['revision']),
            )
            if slot != tm_v2.inactive_slot(state):
                raise RuntimeError(
                    f'refusing to overwrite served/retained slot {slot!r}; '
                    f'active_slot={state.active_slot!r}'
                )
        finally:
            cur.close()
            conn.close()
        table_name = f'{table_name}_{slot}'
    else:
        if reader.get('legacy_writers_disabled_at') is not None:
            from airflow.exceptions import AirflowSkipException
            raise AirflowSkipException(
                'legacy Transfermarkt writers are persistently disabled'
            )

    template_path = Path('/opt/airflow') / sql_file
    if not template_path.exists():
        raise FileNotFoundError(f"Silver template not found: {template_path}")

    if sql_file.endswith('.sql.j2'):
        from utils.medallion_config import (
            get_manager_alias_sql_values,
            get_team_alias_sql_values,
            render_sql_template,
        )
        # Each template references exactly one alias placeholder.
        if 'transfermarkt_coaches' in sql_file:
            render_kwargs = {
                'manager_aliases_values_sql': get_manager_alias_sql_values(
                    source='transfermarkt',
                ),
            }
        else:
            render_kwargs = {
                'team_aliases_values_sql': get_team_alias_sql_values()
            }
        rendered_sql = render_sql_template(template_path, **render_kwargs)
    else:
        rendered_sql = template_path.read_text(encoding='utf-8')
    if use_candidate_slot:
        rendered_sql = tm_v2.rewrite_native_relations(rendered_sql, slot)
        rendered_sql = _scope_native_sql(
            rendered_sql,
            scope['scopes'],
            registry_snapshot_id=scope['registry_snapshot_id'],
        )
    else:
        rendered_sql = _scope_legacy_sql(rendered_sql, scope['scopes'])
    rendered_sql = _pin_sql_relations(rendered_sql, input_snapshots)
    if use_candidate_slot:
        dag_run = context.get('dag_run')
        build_id = getattr(dag_run, 'run_id', None)
        rendered_sql = _inject_model_lineage(
            rendered_sql,
            input_snapshot_ids=input_snapshots,
            build_id=str(build_id or ''),
            scope_set_id=str(scope['scope_set_id']),
        )
    logger.info(
        "Rendered %s — %d chars (%d alias pairs embedded)",
        template_path.name,
        len(rendered_sql),
        rendered_sql.count("),\n"),
    )

    with tempfile.NamedTemporaryFile(
        mode='w',
        suffix=f'_{table_name}.sql',
        delete=False,
        encoding='utf-8',
    ) as tmp:
        tmp.write(rendered_sql)
        tmp_path = tmp.name

    try:
        return run_silver_transform(
            sql_file=tmp_path,
            table_name=table_name,
            schema=schema,
            partition_columns=partition_columns,
        )
    finally:
        try:
            os.unlink(tmp_path)
        except OSError as e:
            logger.warning("Failed to cleanup temp file %s: %s", tmp_path, e)


def _validate_silver(**context) -> Dict[str, Any]:
    """PythonOperator callable — validate row counts in Silver tables."""
    import logging

    from airflow.exceptions import AirflowException, AirflowSkipException
    from utils.silver_tasks import validate_silver_tables

    logger = logging.getLogger(__name__)

    if context.get('ti') is not None:
        snapshot = _captured_reader_state(context)
        if snapshot.get('legacy_writers_disabled_at') is not None:
            raise AirflowSkipException(
                'legacy Silver validation is disabled after retention cleanup'
            )

    validation = validate_silver_tables(
        tables=SILVER_MIN_ROWS,
        min_rows=1,
    )

    logger.info(f"Silver validation: {validation['status']}")
    logger.info(f"Details: {validation['details']}")

    if validation['warnings']:
        for w in validation['warnings']:
            logger.warning(f"  {w}")
        raise AirflowException(
            f"Silver validation FAILED: {len(validation['warnings'])} "
            f"table(s) below threshold. {validation['warnings']}"
        )

    return validation


def _validate_silver_quality(**context) -> Dict[str, Any]:
    """PythonOperator callable — run DQ checks on Silver tables.

    ERROR-level: PK NULLs / PK uniqueness — block DAG to prevent dirty Gold.
    WARNING-level: ref_integrity (orphan tolerated), freshness, value ranges,
                   canonical_id coverage (orphan-rate proxy).
    """
    import logging

    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    logger = logging.getLogger(__name__)

    if context.get('ti') is not None:
        snapshot = _captured_reader_state(context)
        if snapshot.get('legacy_writers_disabled_at') is not None:
            from airflow.exceptions import AirflowSkipException
            raise AirflowSkipException(
                'legacy Silver DQ is disabled after retention cleanup'
            )

    # TM ingest is weekly (Monday 04:00 UTC, per utils/config.py SCHEDULES).
    # 48h grace covers Monday→Wednesday; staleness past that is observable
    # but not a blocker (severity=WARNING).
    FRESH_HOURS = 48

    checks = [
        # PK + critical NULLs — ERROR (PK breaks downstream JOINs).
        # canonical_id is EXCLUDED — TM has ~10% structural orphans (loan-out,
        # new transfers) per feedback_xref_player_tm_capology.md.
        CHECK.no_nulls(
            'silver.transfermarkt_players',
            cols=['player_id', 'name', 'league', 'season'],
        ),

        # PK uniqueness — ERROR (duplicates would explode downstream facts).
        # PK is (player_id, league, season), NOT canonical_id — orphans
        # carry canonical_id=NULL which would break uniqueness on that key.
        CHECK.no_duplicates(
            'silver.transfermarkt_players',
            pk=['player_id', 'league', 'season'],
        ),

        # canonical_id coverage — WARNING/ERROR by ratio.
        # Two structural regimes (live measurements):
        #   in-season squad view: 89.8% non-orphan (2025/26 mid-season,
        #     feedback_xref_player_tm_capology.md);
        #   post-season view (TM flips its current season in early July,
        #     CURRENT_SEASON flips in August): the squad page lists EVERY
        #     player of the season incl. youth/loan-out who never made the
        #     FBref spine — historical seasons sit at 66-72%; live 2026-07-01
        #     the current season dropped to 61.5% for exactly this reason.
        #   warn_threshold=0.80 → the July-August window shows up as a
        #     WARNING (expected; self-heals with the new season + xref rerun);
        #   error_threshold=0.60 → below the post-season structural floor
        #     something is genuinely broken.
        CHECK.coverage(
            'silver.transfermarkt_players',
            column='canonical_id',
            warn_threshold=0.80,
            error_threshold=0.60,
            severity='WARNING',
            # #788: меряем покрытие только за последний (текущий) сезон — это
            # health-сигнал на толстом current-season FBref-spine. Canonical теперь
            # историзирован за все сезоны (#788), но за старые сезоны покрытие
            # структурно низкое (тонкий spine) и красило бы ERROR не по вине
            # резолва — исторический градиент сглаживается в #825.
            where="season = (SELECT max(season) FROM iceberg.silver.transfermarkt_players)",
            name='canonical_coverage[silver.transfermarkt_players]',
        ),

        # Ref integrity to xref_player — WARNING (orphan rows expected).
        CHECK.ref_integrity(
            'silver.transfermarkt_players',
            'silver.xref_player',
            'canonical_id',
            severity='WARNING',
        ),

        # Freshness — WARNING (weekly ingest, 48h grace).
        CHECK.freshness(
            'silver.transfermarkt_players',
            ts_col='_bronze_ingested_at',
            max_age_hours=FRESH_HOURS,
            severity='WARNING',
        ),

        # Value ranges — WARNING (outlier observability).
        CHECK.value_range(
            'silver.transfermarkt_players', 'height_cm',
            min_val=150, max_val=220, severity='WARNING',
        ),
        CHECK.value_range(
            'silver.transfermarkt_players', 'age',
            min_val=14, max_val=50, severity='WARNING',
        ),
        CHECK.value_range(
            'silver.transfermarkt_players', 'current_market_value_eur',
            min_val=0, severity='WARNING',
        ),

        # ---------------------------------------------------------------
        # silver.transfermarkt_market_value_history (issue #61)
        # ---------------------------------------------------------------
        # Row_count floor — WARNING. Live = 2 121 rows: Bronze ограничен
        # MV_HISTORY_DAILY_LIMIT=100 + replace_partitions wipe (#486), а не
        # «догоняет постепенно»; DoD ~10 888 rows недостижим до фикса #486.
        # Floor 1500 ловит коллапс; revisit after #486 (#493).
        CHECK.row_count(
            'silver.transfermarkt_market_value_history',
            min_rows=1500, severity='WARNING',
        ),

        # PK + critical NULLs — ERROR. canonical_id EXCLUDED (TM orphan
        # rate ≈ 10% — see coverage check below).
        CHECK.no_nulls(
            'silver.transfermarkt_market_value_history',
            cols=['player_id', 'mv_date', 'value_eur', 'league', 'season'],
        ),

        # Bronze-grain dedup — ERROR (defensive against replace_partitions
        # double-write).
        CHECK.no_duplicates(
            'silver.transfermarkt_market_value_history',
            pk=['player_id', 'mv_date', 'league', 'season'],
        ),

        # DoD canonical-grain dedup — ERROR. WHERE filter обязателен:
        # canonical_id NULL для orphans, иначе чек упадёт на множественных
        # NULL-ах. См. feedback_xref_player_tm_capology.md.
        CHECK.no_duplicates(
            'silver.transfermarkt_market_value_history',
            pk=['canonical_id', 'mv_date'],
            where='canonical_id IS NOT NULL',
        ),

        # canonical_id coverage — WARNING-only сигнал (#835). После историзации
        # большинство MV-точек = карьера игрока ВНЕ АПЛ: Bronze хранит ПОЛНЫЙ
        # career-graph каждого экс-АПЛ игрока за 10 сезонов (live: 59 523 точки),
        # а canonical ставится только за АПЛ-сезон самой точки → ~23% покрытие
        # ОЖИДАЕМО и не является поломкой резолвера
        # (feedback_xref_resolver_historical_backfill). error_threshold=0.0 →
        # проверка НИКОГДА не эскалирует в ERROR; warn=0.20 ловит лишь полный
        # коллапс резолвера (coverage → 0).
        CHECK.coverage(
            'silver.transfermarkt_market_value_history',
            column='canonical_id',
            warn_threshold=0.20,
            error_threshold=0.0,
            severity='WARNING',
            name='canonical_coverage[silver.transfermarkt_market_value_history]',
        ),

        # Ref integrity to xref_player — WARNING.
        CHECK.ref_integrity(
            'silver.transfermarkt_market_value_history',
            'silver.xref_player',
            'canonical_id',
            severity='WARNING',
        ),

        # Freshness — WARNING. DoD: ≤14 дней (336h).
        CHECK.freshness(
            'silver.transfermarkt_market_value_history',
            ts_col='_bronze_ingested_at',
            max_age_hours=336,
            severity='WARNING',
        ),

        # Value ranges — WARNING.
        CHECK.value_range(
            'silver.transfermarkt_market_value_history', 'value_eur',
            min_val=0, severity='WARNING',
        ),
        CHECK.value_range(
            'silver.transfermarkt_market_value_history', 'age',
            min_val=14, max_val=50, severity='WARNING',
        ),

        # ---------------------------------------------------------------
        # silver.transfermarkt_transfers (issue #62)
        # ---------------------------------------------------------------
        # Row_count floor — WARNING. Live = 750 rows (100 игроков, cap #486);
        # full APL ≈ 4 116 rows недостижим до фикса #486. Floor 600 ловит
        # broken CTAS / scrape collapse; revisit after #486 (#493).
        CHECK.row_count(
            'silver.transfermarkt_transfers',
            min_rows=600, severity='WARNING',
        ),

        # PK + critical NULLs — ERROR. canonical_id EXCLUDED (TM orphan
        # rate ≈10% по xref_player_resolver).
        CHECK.no_nulls(
            'silver.transfermarkt_transfers',
            cols=['player_id', 'transfer_date', 'league', 'season'],
        ),

        # PK uniqueness — ERROR. Natural key is per-event:
        # (player_id, transfer_date, from_club_name, to_club_name). Per
        # DoD spec — orphan rows (canonical_id NULL) don't break this
        # since PK doesn't include canonical_id.
        CHECK.no_duplicates(
            'silver.transfermarkt_transfers',
            pk=['player_id', 'transfer_date', 'from_club_name', 'to_club_name'],
        ),

        # canonical_id coverage (player) — sibling-policy (players/mv:
        # 0.88/0.80). Per-event orphan rate структурно выше per-player
        # (~10-15%): orphan'ы (youth/loan/backup GK) имеют непропорционально
        # много transfer-событий. Live 2026-06-12: 614/750 = 81.9%
        # (15 структурных orphan-игроков из 100) → WARNING. DoD #62 (≥95%)
        # на event-grain недостижим; revisit after #486 full-roster (#493).
        # Thresholds share the post-season rationale of the players check
        # above (66-78% structural floor in the July-August window; live
        # 2026-07-01: 78.0%).
        CHECK.coverage(
            'silver.transfermarkt_transfers',
            column='canonical_id',
            warn_threshold=0.80,
            error_threshold=0.60,
            severity='WARNING',
            # #788: health-сигнал на current-season spine. Canonical историзирован
            # за все сезоны, исторический градиент покрытия сглаживается в #825.
            where="season = (SELECT max(season) FROM iceberg.silver.transfermarkt_transfers)",
            name='canonical_coverage[silver.transfermarkt_transfers]',
        ),

        # Ref integrity to xref_player — WARNING.
        CHECK.ref_integrity(
            'silver.transfermarkt_transfers',
            'silver.xref_player',
            'canonical_id',
            severity='WARNING',
        ),

        # Team canonical coverage — WARNING-only (per DoD: TM transfers
        # contain non-APL клубы которые xref_team / team_aliases.yaml
        # не покрывают; high orphan tolerated for observability).
        CHECK.coverage(
            'silver.transfermarkt_transfers',
            column='from_club_id_canonical',
            warn_threshold=0.0,
            error_threshold=0.0,
            severity='WARNING',
            name='canonical_coverage[silver.transfermarkt_transfers.from_club_id_canonical]',
        ),
        CHECK.coverage(
            'silver.transfermarkt_transfers',
            column='to_club_id_canonical',
            warn_threshold=0.0,
            error_threshold=0.0,
            severity='WARNING',
            name='canonical_coverage[silver.transfermarkt_transfers.to_club_id_canonical]',
        ),

        # Freshness — WARNING (weekly ingest, 48h grace).
        CHECK.freshness(
            'silver.transfermarkt_transfers',
            ts_col='_bronze_ingested_at',
            max_age_hours=FRESH_HOURS,
            severity='WARNING',
        ),

        # Value ranges — WARNING. fee_eur может быть NULL (free transfer);
        # value_range игнорирует NULL по definition.
        CHECK.value_range(
            'silver.transfermarkt_transfers', 'fee_eur',
            min_val=0, severity='WARNING',
        ),
        CHECK.value_range(
            'silver.transfermarkt_transfers', 'market_value_eur',
            min_val=0, severity='WARNING',
        ),
    ]

    # Run with raise_on_error=False so the Telegram summary always lands.
    report = run_checks(checks, raise_on_error=False)
    logger.info(f"Silver DQ: {report.summary()}")

    telegram_dq_summary(report, header="TM Silver DQ")

    if report.errors:
        from airflow.exceptions import AirflowException
        raise AirflowException(
            f"Silver DQ failed: {len(report.errors)} error(s). "
            + "; ".join(
                f"{r.name}: {r.details or r.error}" for r in report.errors[:5]
            )
        )

    return {
        'passed': len(report.passed),
        'total': len(report.results),
        'errors': [r.name for r in report.errors],
        'warnings': [r.name for r in report.warnings],
    }


def _native_v2_enabled(**context) -> bool:
    """Runtime deployment gate for shadow/native transforms.

    Defaulting to false is intentional: a code deploy can precede creation of
    the native Bronze tables without breaking the canonical weekly pipeline.
    Enable only after the dual-writing ingest is deployed.
    """
    shadow_enabled = os.environ.get(
        'TM_NATIVE_V2_ENABLED', 'false',
    ).strip().lower() in {
        '1', 'true', 'yes', 'on',
    }
    if not context:
        return shadow_enabled
    scope = _transform_scope(context)
    # The explicit manual branch is legacy-only by construction.  An env flag
    # cannot widen it into a native candidate build.
    return shadow_enabled and scope.get('mode') == 'scope_set'


def _capture_reader_state(**context) -> Dict[str, Any]:
    """Pin the exact pre-ingest reader revision and inactive candidate slot."""
    from airflow.exceptions import AirflowException

    conf = _dag_conf(context)
    try:
        if conf.get('transfermarkt_manual_single_scope') is True:
            requested = _manual_scope_bundle(conf)
        else:
            scope_set = _parse_scope_set_conf(conf)
            requested = {
                'mode': 'scope_set',
                'scope_set_id': scope_set.scope_set_id,
                'parent_cycle_id': str(
                    conf.get('transfermarkt_parent_cycle_id') or ''
                ).strip(),
                'reader_revision': scope_set.reader_revision,
                'candidate_slot': tm_v2._normalise_slot(
                    conf.get('transfermarkt_candidate_slot')
                ),
            }
            if not requested['parent_cycle_id']:
                raise ValueError('transfermarkt_parent_cycle_id is required')
    except (TypeError, ValueError) as exc:
        raise AirflowException(f'invalid pinned transform conf: {exc}') from exc

    conn = tm_v2.connect()
    cur = conn.cursor()
    try:
        state = tm_v2.read_reader_state(cur, allow_missing=False)
        if int(requested['reader_revision']) != int(state.revision):
            raise AirflowException(
                'reader revision drifted since exact-scope ingest: '
                f"pinned={requested['reader_revision']} current={state.revision}"
            )
        result = state.to_dict()
        if requested['mode'] == 'scope_set':
            candidate_slot = tm_v2.inactive_slot(state)
            if requested['candidate_slot'] != candidate_slot:
                raise AirflowException(
                    'candidate slot drifted since exact-scope ingest: '
                    f"pinned={requested['candidate_slot']} current={candidate_slot}"
                )
            result['candidate_slot'] = candidate_slot
            result['model_revision'] = int(state.revision)
            result['scope_set_id'] = requested['scope_set_id']
            result['parent_cycle_id'] = requested['parent_cycle_id']
        else:
            result['candidate_slot'] = None
            result['model_revision'] = state.approved_model_revision
            result['scope_set_id'] = None
            result['parent_cycle_id'] = requested['parent_cycle_id']
        return result
    finally:
        cur.close()
        conn.close()


def _captured_reader_state(context) -> Dict[str, Any]:
    ti = context.get('ti')
    snapshot = (
        ti.xcom_pull(task_ids='capture_reader_state') if ti is not None else None
    )
    if not snapshot:
        raise RuntimeError('capture_reader_state XCom is missing')
    return snapshot


def _validate_native_v2_quality(**context) -> Dict[str, Any]:
    """Blocking structural checks for the shadow native model.

    These checks validate build safety.  Cross-model legacy parity and the
    common-batch manifest are enforced by ``scripts/transfermarkt_native_v2.py
    readiness`` before cutover.
    """
    from airflow.exceptions import AirflowException
    from utils.data_quality import CHECK, run_checks

    scope = _transform_scope(context)
    if scope.get('mode') != 'scope_set':
        raise AirflowException('native DQ requires an immutable scope set')
    scopes = scope['scopes']
    snapshot = _captured_reader_state(context)
    slot = tm_v2._normalise_slot(snapshot.get('candidate_slot'))
    def s(name: str) -> str:
        return f'silver.{name}_{slot}'
    compatibility_where = ' OR '.join(
        '('
        f"observed_league = {_sql_literal(item['canonical_competition_id'])} "
        f"AND observed_season = {_sql_literal(item['canonical_season'])}"
        ')'
        for item in scopes
    )
    membership_scope_where = _scope_predicate(
        scopes, competition_column='competition_id', edition_column='edition_id',
    )
    transfer_scope_where = _scope_predicate(
        scopes,
        competition_column='source_competition_id',
        edition_column='source_edition_id',
    )
    competition_count = len({item['competition_id'] for item in scopes})
    lineage_violation_where = _model_lineage_violation_where(context)

    checks = [
        CHECK.no_duplicates(
            s('transfermarkt_competitions_v2'), pk=['competition_id'],
        ),
        CHECK.no_nulls(
            s('transfermarkt_competitions_v2'),
            cols=['competition_id', 'classification_status'],
        ),
        CHECK.row_count(
            s('transfermarkt_competitions_v2'),
            min_rows=competition_count,
            max_rows=competition_count,
            name='tm_native_v2_exact_competition_scope_count',
        ),
        CHECK.no_duplicates(
            s('transfermarkt_competition_editions_v2'),
            pk=['competition_id', 'edition_id'],
        ),
        CHECK.no_nulls(
            s('transfermarkt_competition_editions_v2'),
            cols=['competition_id', 'edition_id', 'season_format'],
        ),
        CHECK.row_count(
            s('transfermarkt_competition_editions_v2'),
            min_rows=len(scopes),
            max_rows=len(scopes),
            name='tm_native_v2_exact_edition_scope_count',
        ),
        CHECK.no_duplicates(
            s('transfermarkt_player_xref_global_v2'), pk=['player_id'],
        ),
        CHECK.row_count(
            s('transfermarkt_player_xref_global_v2'),
            min_rows=0,
            max_rows=0,
            where="resolution_status IN ('source_conflict', 'canonical_conflict')",
            name='tm_native_v2_global_xref_conflicts',
        ),
        CHECK.no_duplicates(
            s('transfermarkt_squad_memberships_v2'),
            pk=['competition_id', 'edition_id', 'club_id', 'player_id'],
        ),
        CHECK.no_nulls(
            s('transfermarkt_squad_memberships_v2'),
            cols=['club_name', 'player_name'],
        ),
        CHECK.no_duplicates(
            s('transfermarkt_player_attribute_observations_v2'),
            pk=[
                'competition_id', 'edition_id', 'club_id', 'player_id',
                'observed_at',
            ],
        ),
        CHECK.no_nulls(
            s('transfermarkt_player_attribute_observations_v2'),
            cols=['name', 'position'],
        ),
        CHECK.no_duplicates(
            s('transfermarkt_player_contract_observations_v2'),
            pk=[
                'competition_id', 'edition_id', 'team_id', 'player_id',
                'observed_at',
            ],
        ),
        CHECK.no_nulls(
            s('transfermarkt_player_contract_observations_v2'),
            cols=[
                'competition_id', 'edition_id', 'player_id',
                'applicability_status',
            ],
        ),
        CHECK.no_duplicates(
            s('transfermarkt_player_attributes_v2'), pk=['player_id'],
        ),
        CHECK.no_nulls(
            s('transfermarkt_player_attributes_v2'), cols=['player_id', 'name'],
        ),
        CHECK.no_duplicates(
            s('transfermarkt_market_value_points_v2'),
            pk=['player_id', 'mv_date'],
        ),
        CHECK.no_nulls(
            s('transfermarkt_market_value_points_v2'),
            cols=['value_eur'],
        ),
        CHECK.value_range(
            s('transfermarkt_market_value_points_v2'),
            'value_eur', min_val=0, severity='ERROR',
        ),
        CHECK.no_duplicates(
            s('transfermarkt_transfer_events_v2'),
            pk=['player_id', 'transfer_id'],
        ),
        CHECK.no_duplicates(
            s('transfermarkt_transfer_events_v2'),
            pk=['transfer_id'],
            name='tm_native_v2_transfer_id_global_uniqueness',
        ),
        CHECK.value_range(
            s('transfermarkt_transfer_events_v2'),
            'fee_eur', min_val=0, severity='ERROR',
        ),
        CHECK.value_range(
            s('transfermarkt_transfer_events_v2'),
            'market_value_eur', min_val=0, severity='ERROR',
        ),
        CHECK.no_duplicates(
            s('transfermarkt_coach_profiles_v2'), pk=['coach_id'],
        ),
        CHECK.no_nulls(
            s('transfermarkt_coach_profiles_v2'), cols=['name'],
        ),
        CHECK.no_duplicates(
            s('transfermarkt_coach_stints_v2'),
            pk=['club_id', 'coach_id', 'appointed_date', 'left_date'],
        ),
        CHECK.no_nulls(
            s('transfermarkt_coach_stints_v2'),
            cols=['club_name', 'name', 'role'],
        ),
        CHECK.row_count(
            s('transfermarkt_coach_stints_v2'),
            min_rows=tm_v2.GOLD_MIN_ROWS['dim_manager_v2'],
        ),
        CHECK.row_count(
            s('transfermarkt_coach_stints_v2'),
            min_rows=0,
            max_rows=0,
            where='appointed_date IS NULL AND left_date IS NULL',
            name='tm_native_v2_coach_stint_has_boundary',
        ),
        CHECK.no_duplicates(
            s('transfermarkt_player_team_season_assignment_v2'),
            pk=['competition_id', 'edition_id', 'player_id'],
        ),
        CHECK.coverage(
            s('transfermarkt_squad_memberships_v2'),
            condition=(
                'EXISTS (SELECT 1 FROM '
                f'iceberg.silver.transfermarkt_player_xref_global_v2_{slot} x '
                'WHERE x.player_id = '
                f'transfermarkt_squad_memberships_v2_{slot}.player_id '
                "AND x.resolution_status = 'resolved')"
            ),
            where=membership_scope_where,
            warn_threshold=0.80,
            error_threshold=0.60,
            name='tm_native_v2_squad_canonical_coverage',
        ),
        CHECK.coverage(
            s('transfermarkt_player_attributes_v2'),
            column='canonical_id',
            where=compatibility_where,
            warn_threshold=0.80,
            error_threshold=0.60,
            name='tm_native_v2_player_attributes_canonical_coverage',
        ),
        CHECK.coverage(
            s('transfermarkt_transfer_events_v2'),
            column='canonical_id',
            where=transfer_scope_where,
            warn_threshold=0.80,
            error_threshold=0.60,
            name='tm_native_v2_transfer_canonical_coverage',
        ),
    ]
    for _, _, table_name, _ in NATIVE_V2_TRANSFORMS:
        checks.append(CHECK.no_nulls(
            s(table_name),
            cols=['input_snapshot_ids', 'build_id', 'scope_set_id'],
            name=f'tm_native_v2_{table_name}_model_lineage',
        ))
        checks.append(CHECK.row_count(
            s(table_name),
            min_rows=0,
            max_rows=0,
            where=lineage_violation_where,
            name=f'tm_native_v2_{table_name}_exact_model_lineage',
        ))
    report = run_checks(checks, raise_on_error=False)
    if report.errors:
        raise AirflowException(
            f"TM native v2 DQ failed: {len(report.errors)} error(s): "
            + '; '.join(r.name for r in report.errors[:5])
        )
    return {
        'passed': len(report.passed),
        'total': len(report.results),
        'errors': [r.name for r in report.errors],
        'warnings': [r.name for r in report.warnings],
    }


def _validate_native_v2_gold(**context) -> Dict[str, Any]:
    """Validate shadow Gold natural keys before exposing cutover readiness."""
    from airflow.exceptions import AirflowException
    from utils.data_quality import CHECK, run_checks
    scope = _transform_scope(context)
    if scope.get('mode') != 'scope_set':
        raise AirflowException('native Gold DQ requires an immutable scope set')
    snapshot = _captured_reader_state(context)
    slot = tm_v2._normalise_slot(snapshot.get('candidate_slot'))
    def g(name: str) -> str:
        return f'gold.{name}_{slot}'
    compatibility_where = ' OR '.join(
        '('
        f"league = {_sql_literal(item['canonical_competition_id'])} AND "
        f"season = {_sql_literal(item['canonical_season'])}"
        ')'
        for item in scope['scopes']
    )
    lineage_violation_where = _model_lineage_violation_where(context)

    checks = [
        CHECK.row_count(
            g('fct_transfer_v2'),
            min_rows=NATIVE_V2_GOLD_MIN_ROWS['fct_transfer_v2'],
        ),
        CHECK.row_count(
            g('transfermarkt_team_season_market_value_v2'),
            min_rows=15,
            where=(
                f'({compatibility_where}) AND squad_market_value_eur > 0 '
                'AND valued_players > 0'
            ),
            name='tm_native_v2_team_value_requested_scope_set',
        ),
        CHECK.no_nulls(g('fct_transfer_v2'), cols=['transfer_id', 'player_id']),
        CHECK.no_duplicates(g('fct_transfer_v2'), pk=['transfer_id']),
        CHECK.no_duplicates(
            g('fct_transfer_v2'),
            pk=[
                'player_id', 'transfer_date', 'event_season',
                'from_team_id', 'to_team_id',
            ],
            name='tm_native_v2_transfer_semantic_event_uniqueness',
        ),
        CHECK.row_count(
            g('fct_player_market_value_v2'),
            min_rows=NATIVE_V2_GOLD_MIN_ROWS['fct_player_market_value_v2'],
        ),
        CHECK.row_count(
            g('fct_player_market_value_v2'),
            min_rows=tm_v2.TRANSFERMARKT_MV_MIN_ROWS,
            where="source = 'transfermarkt'",
            name='tm_native_v2_market_value_transfermarkt_branch',
        ),
        CHECK.no_nulls(
            g('fct_player_market_value_v2'),
            cols=['player_id', 'valuation_date', 'source', 'market_value_eur'],
        ),
        CHECK.value_range(
            g('fct_player_market_value_v2'),
            'market_value_eur',
            min_val=0,
            severity='ERROR',
        ),
        CHECK.no_duplicates(
            g('fct_player_market_value_v2'),
            pk=['player_id', 'valuation_date', 'source'],
        ),
        CHECK.row_count(
            g('dim_manager_v2'),
            min_rows=NATIVE_V2_GOLD_MIN_ROWS['dim_manager_v2'],
        ),
        CHECK.no_nulls(g('dim_manager_v2'), cols=['manager_id']),
        CHECK.no_duplicates(g('dim_manager_v2'), pk=['manager_id']),
        CHECK.row_count(
            g('transfermarkt_team_season_market_value_v2'),
            min_rows=NATIVE_V2_GOLD_MIN_ROWS[
                'transfermarkt_team_season_market_value_v2'
            ],
        ),
        CHECK.no_nulls(
            g('transfermarkt_team_season_market_value_v2'),
            cols=[
                'team_id', 'league', 'season', 'squad_market_value_eur',
            ],
        ),
        CHECK.value_range(
            g('transfermarkt_team_season_market_value_v2'),
            'squad_market_value_eur',
            min_val=1,
            severity='ERROR',
        ),
        CHECK.row_count(
            g('transfermarkt_team_season_market_value_v2'),
            min_rows=0,
            max_rows=0,
            where='valued_players <= 0',
            name='tm_native_v2_team_value_has_valued_players',
        ),
        CHECK.no_duplicates(
            g('transfermarkt_team_season_market_value_v2'),
            pk=['team_id', 'league', 'season'],
        ),
    ]
    for _, _, table_name, _ in NATIVE_V2_GOLD_TRANSFORMS:
        checks.append(CHECK.no_nulls(
            g(table_name),
            cols=['input_snapshot_ids', 'build_id', 'scope_set_id'],
            name=f'tm_native_v2_{table_name}_model_lineage',
        ))
        checks.append(CHECK.row_count(
            g(table_name),
            min_rows=0,
            max_rows=0,
            where=lineage_violation_where,
            name=f'tm_native_v2_{table_name}_exact_model_lineage',
        ))
    report = run_checks(checks, raise_on_error=False)
    if report.errors:
        raise AirflowException(
            f"TM native v2 Gold DQ failed: {len(report.errors)} error(s): "
            + '; '.join(r.name for r in report.errors[:5])
        )
    return {
        'passed': len(report.passed),
        'total': len(report.results),
        'errors': [r.name for r in report.errors],
    }


def _record_native_v2_model_manifest(**context) -> Dict[str, Any]:
    """Persist every model against the same scope set and root snapshots."""
    scope = _transform_scope(context)
    if scope.get('mode') != 'scope_set':
        raise RuntimeError('native model manifest requires an immutable scope set')
    pins = _captured_pins(context)
    snapshot = _captured_reader_state(context)
    dag_run = context.get('dag_run')
    build_id = getattr(dag_run, 'run_id', None)
    if not build_id:
        raise RuntimeError('model build_id (child DAG run_id) is missing')
    conn = tm_v2.connect()
    cur = conn.cursor()
    try:
        return tm_v2.record_model_build_manifest(
            cur,
            cycle_id=str(scope['parent_cycle_id']),
            build_id=str(build_id),
            expected_revision=int(snapshot['revision']),
            candidate_slot=str(snapshot['candidate_slot']),
            scope_set_id=str(scope['scope_set_id']),
            pinned_input_snapshot_ids=dict(pins['input_snapshot_ids']),
        )
    finally:
        cur.close()
        conn.close()


def _tm_model_ready_v2(**context) -> Dict[str, Any]:
    """Return the exact scope-set identity consumed by cutover."""
    from airflow.exceptions import AirflowException

    scope = _transform_scope(context)
    snapshot = _captured_reader_state(context)
    native_enabled = _native_v2_enabled(**context)
    if not native_enabled:
        return {
            'status': 'native_disabled',
            'ready': True,
            'scope_set_id': scope.get('scope_set_id'),
            'parent_cycle_id': scope['parent_cycle_id'],
            'state': snapshot,
        }
    if scope.get('mode') != 'scope_set':
        raise AirflowException('native readiness requires an immutable scope set')
    conn = tm_v2.connect()
    cur = conn.cursor()
    try:
        tm_v2.assert_reader_revision(cur, int(snapshot['revision']))
        report = tm_v2.readiness(
            cur,
            str(scope['parent_cycle_id']),
            expected_revision=int(snapshot['revision']),
            scope_set_id=str(scope['scope_set_id']),
            parent_cycle_id=str(scope['parent_cycle_id']),
            candidate_slot_override=str(snapshot['candidate_slot']),
        )
        tm_v2.assert_reader_revision(cur, int(snapshot['revision']))
    finally:
        cur.close()
        conn.close()
    if not report['ready']:
        raise AirflowException(f'TM native v2 readiness failed: {report}')
    if report.get('scope_set_id') != scope['scope_set_id']:
        raise AirflowException('readiness returned another scope_set_id')
    return {
        **report,
        'status': 'scope_set_ready',
        'scope_set_id': scope['scope_set_id'],
        'parent_cycle_id': scope['parent_cycle_id'],
        'candidate_slot': snapshot['candidate_slot'],
    }


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id='dag_transform_transfermarkt_silver',
    default_args=SILVER_ARGS,
    description='Transform Bronze Transfermarkt data into Silver Iceberg tables via Trino CTAS',
    schedule=None,  # Trigger-only (called after dag_ingest_transfermarkt)
    start_date=datetime(2026, 5, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=['transform', 'transfermarkt', 'silver', 'football', 'trino'],
    max_active_runs=1,
    max_active_tasks=1,  # Sequential CTAS to prevent OOM (~1.2GB per task)
    doc_md="""
    ## Transfermarkt Silver Transformation

    Transforms Bronze TM player snapshots into typed Silver tables with
    `canonical_id` bridging via `silver.xref_player`.

    ### Trigger

    Triggered by `dag_ingest_transfermarkt` via TriggerDagRunOperator.

    ### Silver Tables

    | Table | Description | Bronze sources |
    |-------|-------------|----------------|
    | `transfermarkt_players` | Typed player snapshot + canonical_id (issue #60) | `transfermarkt_players` |
    | `transfermarkt_market_value_history` | Typed MV timeline + canonical_id (issue #61) | `transfermarkt_market_value_history` |
    | `transfermarkt_transfers` | Typed transfer events + player/club canonical_ids (issue #62) | `transfermarkt_transfers` |

    ### Data Quality Checks

    - **PK NULLs / uniqueness** (ERROR): blocks DAG to protect downstream Gold
    - **canonical_id coverage** (WARNING/ERROR by ratio): ≥88% non-orphan PASS,
      80–88% WARNING, <80% ERROR — единая политика для всех трёх таблиц (#493).
    - **ref_integrity to xref_player** (WARNING): orphan TM players expected
    - **Freshness** (WARNING): 48h grace post-Monday ingest
    - **Value ranges** (WARNING): height_cm 150–220, age 14–50, MV ≥ 0
    """,
) as dag:

    validate_transform_scope_set = PythonOperator(
        task_id='validate_transform_scope_set',
        python_callable=_validate_transform_scope_set,
    )

    capture_reader_state = PythonOperator(
        task_id='capture_reader_state',
        python_callable=_capture_reader_state,
    )

    pin_transform_input_snapshots = PythonOperator(
        task_id='pin_transform_input_snapshots',
        python_callable=_pin_transform_input_snapshots,
    )

    validate_bronze_quality = PythonOperator(
        task_id='validate_bronze_quality',
        python_callable=_validate_bronze_quality,
    )

    authorize_silver_writes = PythonOperator(
        task_id='authorize_silver_writes',
        python_callable=_authorize_silver_writes,
    )

    persist_scope_set_manifest = PythonOperator(
        task_id='persist_scope_set_manifest',
        python_callable=_persist_scope_set_manifest,
    )

    # =========================================================================
    # TaskGroup: Silver Transforms (single task for now — extend as TM_mv,
    # TM_transfers, etc. land per #59 closing note).
    # =========================================================================
    with TaskGroup(group_id='silver_transforms') as transforms_group:
        for task_id, sql_file, table_name in SILVER_TRANSFORMS:
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs={
                    'sql_file': sql_file,
                    'table_name': table_name,
                },
            )

    # Shadow native model.  Keep this graph explicit because the global bridge
    # must exist before player facts, and the end-of-season assignment depends
    # on both memberships and transfer events.
    native_v2_gate = ShortCircuitOperator(
        task_id='native_v2_enabled',
        python_callable=_native_v2_enabled,
        ignore_downstream_trigger_rules=False,
    )

    with TaskGroup(group_id='native_v2_transforms') as native_v2_group:
        native_tasks = {}
        for task_id, sql_file, table_name, partition_columns in NATIVE_V2_TRANSFORMS:
            native_tasks[task_id] = PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs={
                    'sql_file': sql_file,
                    'table_name': table_name,
                    'partition_columns': partition_columns,
                    'use_candidate_slot': True,
                },
            )

        native_tasks['player_attribute_observations_v2'] >> native_tasks['player_attributes_v2']
        native_tasks['player_contract_observations_v2'] >> native_tasks['player_attributes_v2']
        native_tasks['competition_editions_v2'] >> native_tasks['player_attributes_v2']
        native_tasks['player_xref_global_v2'] >> native_tasks['player_attributes_v2']
        native_tasks['player_xref_global_v2'] >> native_tasks['player_contract_observations_v2']
        native_tasks['player_xref_global_v2'] >> native_tasks['market_value_points_v2']
        native_tasks['market_value_points_v2'] >> native_tasks['player_attributes_v2']
        native_tasks['player_xref_global_v2'] >> native_tasks['transfer_events_v2']
        native_tasks['squad_memberships_v2'] >> native_tasks['player_team_season_assignment_v2']
        native_tasks['transfer_events_v2'] >> native_tasks['player_team_season_assignment_v2']
        native_tasks['competition_editions_v2'] >> native_tasks['player_team_season_assignment_v2']

    validate_native_v2 = PythonOperator(
        task_id='validate_native_v2_quality',
        python_callable=_validate_native_v2_quality,
    )

    authorize_gold_writes = PythonOperator(
        task_id='authorize_gold_writes',
        python_callable=_authorize_gold_writes,
    )

    with TaskGroup(group_id='native_v2_gold') as native_v2_gold_group:
        for task_id, sql_file, table_name, partition_columns in NATIVE_V2_GOLD_TRANSFORMS:
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs={
                    'sql_file': sql_file,
                    'table_name': table_name,
                    'partition_columns': partition_columns,
                    'schema': 'gold',
                    'use_candidate_slot': True,
                },
            )

    validate_native_v2_gold = PythonOperator(
        task_id='validate_native_v2_gold',
        python_callable=_validate_native_v2_gold,
    )

    record_native_v2_model_manifest = PythonOperator(
        task_id='record_native_v2_model_manifest',
        python_callable=_record_native_v2_model_manifest,
    )

    # =========================================================================
    # Validation: row count
    # =========================================================================
    validate_silver = PythonOperator(
        task_id='validate_silver',
        python_callable=_validate_silver,
        trigger_rule='all_done',
    )

    # =========================================================================
    # Quality: DQ checks
    # =========================================================================
    validate_quality = PythonOperator(
        task_id='validate_silver_quality',
        python_callable=_validate_silver_quality,
        trigger_rule='all_done',
    )

    tm_model_ready_v2 = PythonOperator(
        task_id='tm_model_ready_v2',
        python_callable=_tm_model_ready_v2,
        # A successful canonical branch must not let this leaf hide an
        # upstream_failed shadow build.  When shadowing is disabled, the native
        # branch is merely skipped and the marker still executes as a no-op.
        trigger_rule='none_failed_min_one_success',
    )

    validate_transform_scope_set >> capture_reader_state
    capture_reader_state >> pin_transform_input_snapshots
    # #948: the Bronze DQ gate fails the run BEFORE the one-shot Silver
    # write-approval packet is consumed.
    pin_transform_input_snapshots >> validate_bronze_quality
    validate_bronze_quality >> authorize_silver_writes
    authorize_silver_writes >> persist_scope_set_manifest
    persist_scope_set_manifest >> transforms_group
    persist_scope_set_manifest >> native_v2_gate
    # Keep the readiness leaf runnable when both materialisation branches are
    # intentionally skipped (the normal post-cleanup, cache-only daily path).
    # The other direct upstreams still make any real legacy/native failure
    # block the marker under none_failed_min_one_success.
    persist_scope_set_manifest >> tm_model_ready_v2
    transforms_group >> validate_silver >> validate_quality >> tm_model_ready_v2
    (
        native_v2_gate
        >> native_v2_group
        >> validate_native_v2
        >> authorize_gold_writes
        >> native_v2_gold_group
        >> validate_native_v2_gold
        >> record_native_v2_model_manifest
        >> tm_model_ready_v2
    )
