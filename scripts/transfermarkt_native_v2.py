#!/usr/bin/env python3
"""Bootstrap and verify the Transfermarkt native-v2 model.

The default behaviour is non-mutating.  ``plan`` prints the idempotent
bootstrap SQL; ``bootstrap`` also only prints it unless ``--apply`` is supplied.
``parity`` and ``readiness`` are read-only.  Legacy cleanup is deliberately a
separate SQL generator and can never execute SQL itself.

Examples::

    python scripts/transfermarkt_native_v2.py plan
    python scripts/transfermarkt_native_v2.py bootstrap --apply
    python scripts/transfermarkt_native_v2.py parity \
        --cycle-id tm-cycle --league 'ENG-Premier League' --season 2025
    python scripts/transfermarkt_native_v2.py readiness \
        --cycle-id tm-cycle --league 'ENG-Premier League' --season 2025 \
        --expected-revision 0
    python scripts/transfermarkt_native_v2.py cleanup-sql \
        --cycle-id tm-native-cycle --league 'ENG-Premier League' --season 2025 \
        --confirm-legacy-retention-expired \
        --confirm-canonical-readers-switched \
        --confirm-legacy-writers-disabled
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
DAGS_ROOT = PROJECT_ROOT / 'dags'
if str(DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(DAGS_ROOT))

try:
    from dags.utils import transfermarkt_native_v2 as control
except ModuleNotFoundError:  # Airflow has /opt/airflow/dags on PYTHONPATH.
    from utils import transfermarkt_native_v2 as control


NATIVE_ENTITIES = control.NATIVE_ENTITIES

LEGACY_TABLES = (
    'transfermarkt_players',
    'transfermarkt_market_value_history',
    'transfermarkt_transfers',
    'transfermarkt_coaches',
)

LEGACY_BACKUP_RELATIONS = tuple(
    f'iceberg.bronze.{table}' for table in LEGACY_TABLES
)

REGISTRY_DISCOVERY_RELATIONS = (
    'iceberg.bronze.transfermarkt_competitions',
    'iceberg.bronze.transfermarkt_competition_editions',
)
_REGISTRY_CYCLE_ID = re.compile(r'^tm-registry-[0-9a-f]{24}$')

GOLD_MIN_ROWS = control.GOLD_MIN_ROWS


def _event_id_sql(alias: str = 'b') -> str:
    """Runtime-compatible fallback id for a legacy row.

    The parser hashes compact, sorted-key JSON.  Legacy has no upstream source
    id or stable occurrence index, so bootstrap uses source_id=NULL and
    occurrence=0 after collapsing identical compatibility rows.  Mutable fee,
    value and upcoming flags are deliberately excluded from identity.
    """
    event_season = _event_season_sql(alias)

    def json_string(expr: str) -> str:
        return f"COALESCE(JSON_FORMAT(CAST({expr} AS JSON)), 'null')"

    from_club = (
        f"COALESCE(NULLIF(CAST({alias}.from_club_id AS varchar), ''), "
        f"{alias}.from_club_name)"
    )
    to_club = (
        f"COALESCE(NULLIF(CAST({alias}.to_club_id AS varchar), ''), "
        f"{alias}.to_club_name)"
    )
    identity_json = (
        "CONCAT('{\"event_season\":', " + json_string(event_season)
        + ", ',\"from_club\":', " + json_string(from_club)
        + ", ',\"occurrence\":0,\"player_id\":', "
        + json_string(f"CAST({alias}.player_id AS varchar)")
        + ", ',\"source_id\":null,\"to_club\":', " + json_string(to_club)
        + ", ',\"transfer_date\":', "
        + json_string(f"CAST({alias}.transfer_date AS varchar)") + ", '}')"
    )
    return f"LOWER(TO_HEX(SHA256(TO_UTF8({identity_json}))))"


def _event_season_sql(alias: str = 'b') -> str:
    return (
        "CASE WHEN " + alias + ".transfer_date IS NOT NULL THEN "
        "SUBSTR(CAST(IF(MONTH(" + alias + ".transfer_date) >= 7, "
        "YEAR(" + alias + ".transfer_date), YEAR(" + alias + ".transfer_date) - 1) "
        "AS varchar), 3, 2) || "
        "SUBSTR(CAST(IF(MONTH(" + alias + ".transfer_date) >= 7, "
        "YEAR(" + alias + ".transfer_date), YEAR(" + alias + ".transfer_date) - 1) + 1 "
        "AS varchar), 3, 2) ELSE NULL END"
    )


def bootstrap_sql() -> list[str]:
    """Return idempotent DDL/MERGE statements; never execute them here."""
    event_id = _event_id_sql('b')
    event_season = _event_season_sql('b')
    return [
        "CREATE SCHEMA IF NOT EXISTS iceberg.ops",
        """CREATE TABLE IF NOT EXISTS iceberg.ops.transfermarkt_fetch_state (
            endpoint varchar,
            source_id varchar,
            parser_version varchar,
            schema_version varchar,
            status varchar,
            run_key varchar,
            first_attempt_at timestamp(6),
            last_attempt_at timestamp(6),
            last_success_at timestamp(6),
            row_count bigint,
            payload_hash varchar,
            error varchar
        ) WITH (format = 'PARQUET')""",
        """CREATE TABLE IF NOT EXISTS iceberg.ops.transfermarkt_dual_write_manifest_v2 (
            cycle_id varchar,
            league varchar,
            season integer,
            entity varchar,
            legacy_table varchar,
            native_table varchar,
            legacy_batch_id varchar,
            native_batch_id varchar,
            legacy_rows bigint,
            native_rows bigint,
            legacy_hash varchar,
            native_hash varchar,
            status varchar,
            committed_at timestamp(6)
        ) WITH (format = 'PARQUET')""",
        *control.control_plane_bootstrap_sql(),
        """CREATE TABLE IF NOT EXISTS iceberg.bronze.transfermarkt_competitions (
            competition_id varchar, slug varchar, name varchar,
            country varchar, confederation varchar, competition_type varchar,
            gender varchar, team_type varchar, age_category varchar,
            season_format varchar, active boolean, source_url varchar,
            discovered_at timestamp(6), canonical_competition_id varchar,
            classification_status varchar, classification_evidence varchar,
            registry_snapshot_id varchar, source_body_hash varchar,
            parser_revision varchar, schema_revision varchar,
            fetched_at timestamp(6), cycle_id varchar, scope_id varchar,
            _source varchar, _entity_type varchar,
            _ingested_at timestamp(6), _batch_id varchar
        ) WITH (format = 'PARQUET')""",
        """CREATE TABLE IF NOT EXISTS iceberg.bronze.transfermarkt_competition_editions (
            competition_id varchar, edition_id varchar, edition_label varchar,
            canonical_season varchar, season_format varchar,
            start_date date, end_date date, active boolean, current boolean,
            participant_count bigint, participant_hash varchar,
            source_url varchar, discovered_at timestamp(6),
            registry_snapshot_id varchar, source_body_hash varchar,
            parser_revision varchar, schema_revision varchar,
            fetched_at timestamp(6), cycle_id varchar, scope_id varchar,
            _source varchar, _entity_type varchar,
            _ingested_at timestamp(6), _batch_id varchar
        ) WITH (format = 'PARQUET')""",
        """CREATE TABLE IF NOT EXISTS iceberg.bronze.transfermarkt_player_contract_observations (
            competition_id varchar, edition_id varchar, team_id varchar,
            team_name varchar, player_id varchar, contract_until date,
            observed_at timestamp(6), applicability_status varchar,
            source_url varchar, source_body_hash varchar,
            parser_revision varchar, schema_revision varchar,
            fetched_at timestamp(6), cycle_id varchar, scope_id varchar,
            _source varchar, _entity_type varchar,
            _ingested_at timestamp(6), _batch_id varchar
        ) WITH (
            format = 'PARQUET',
            partitioning = ARRAY['competition_id', 'edition_id']
        )""",
        """CREATE TABLE IF NOT EXISTS iceberg.bronze.transfermarkt_squad_memberships (
            competition_id varchar, edition_id varchar, league varchar,
            season varchar, club_id varchar, club_slug varchar,
            club_name varchar, player_id varchar, player_slug varchar,
            player_name varchar, observed_at timestamp(6),
            source_competition_id varchar, source_edition_id varchar,
            source_url varchar, source_body_hash varchar,
            fetched_at timestamp(6), parser_revision varchar,
            schema_revision varchar, cycle_id varchar, scope_id varchar,
            _source varchar, _entity_type varchar,
            _ingested_at timestamp(6), _batch_id varchar
        ) WITH (
            format = 'PARQUET',
            partitioning = ARRAY['competition_id', 'edition_id']
        )""",
        """MERGE INTO iceberg.bronze.transfermarkt_squad_memberships t
        USING (
            SELECT league, season, CAST(current_club_id AS varchar) club_id,
                   CAST(NULL AS varchar) club_slug,
                   current_club_name club_name, CAST(player_id AS varchar) player_id,
                   player_slug, name player_name,
                   CAST(_ingested_at AS timestamp(6)) observed_at,
                   _source, CAST('squad_memberships' AS varchar) _entity_type,
                   CAST(_ingested_at AS timestamp(6)) _ingested_at, _batch_id
            FROM iceberg.bronze.transfermarkt_players
            WHERE current_club_id IS NOT NULL AND player_id IS NOT NULL
        ) s ON t.league=s.league AND t.season=s.season AND t.club_id=s.club_id
           AND t.player_id=s.player_id
        WHEN MATCHED AND s._ingested_at >= t._ingested_at THEN UPDATE SET
          club_slug=s.club_slug,club_name=s.club_name,player_slug=s.player_slug,
          player_name=s.player_name,
          observed_at=s.observed_at,_source=s._source,_entity_type=s._entity_type,
          _ingested_at=s._ingested_at,_batch_id=s._batch_id
        WHEN NOT MATCHED THEN INSERT
          (league,season,club_id,club_slug,club_name,player_id,player_slug,player_name,
           observed_at,_source,_entity_type,_ingested_at,_batch_id)
        VALUES
          (s.league,s.season,s.club_id,s.club_slug,s.club_name,s.player_id,s.player_slug,
           s.player_name,s.observed_at,s._source,s._entity_type,s._ingested_at,s._batch_id)""",
        """CREATE TABLE IF NOT EXISTS iceberg.bronze.transfermarkt_player_attribute_observations (
            competition_id varchar, edition_id varchar, player_id varchar,
            player_slug varchar, name varchar, position varchar, dob date,
            age integer, height_cm integer, foot varchar, nationality varchar,
            contract_until date, market_value_eur bigint, league varchar,
            season varchar, club_id varchar, club_name varchar,
            observed_at timestamp(6), source_competition_id varchar,
            source_edition_id varchar, source_url varchar,
            source_body_hash varchar, fetched_at timestamp(6),
            parser_revision varchar, schema_revision varchar,
            cycle_id varchar, scope_id varchar, _source varchar,
            _entity_type varchar, _ingested_at timestamp(6), _batch_id varchar
        ) WITH (
            format = 'PARQUET',
            partitioning = ARRAY['competition_id', 'edition_id']
        )""",
        """MERGE INTO iceberg.bronze.transfermarkt_player_attribute_observations t
        USING (
            SELECT CAST(player_id AS varchar) player_id, player_slug, name, position,
                   dob, CAST(age AS integer) age, CAST(height_cm AS integer) height_cm,
                   foot, nationality, contract_until,
                   CAST(market_value_eur AS bigint) market_value_eur,
                   league, season, CAST(current_club_id AS varchar) club_id,
                   current_club_name club_name,
                   CAST(_ingested_at AS timestamp(6)) observed_at,
                   _source, CAST('player_attribute_observations' AS varchar) _entity_type,
                   CAST(_ingested_at AS timestamp(6)) _ingested_at, _batch_id
            FROM iceberg.bronze.transfermarkt_players
            WHERE current_club_id IS NOT NULL AND player_id IS NOT NULL
        ) s ON t.league=s.league AND t.season=s.season AND t.club_id=s.club_id
           AND t.player_id=s.player_id AND t.observed_at=s.observed_at
        WHEN MATCHED AND s._ingested_at >= t._ingested_at THEN UPDATE SET
          player_slug=s.player_slug,name=s.name,position=s.position,dob=s.dob,
          age=s.age,height_cm=s.height_cm,foot=s.foot,nationality=s.nationality,
          contract_until=s.contract_until,market_value_eur=s.market_value_eur,
          club_name=s.club_name,_source=s._source,_entity_type=s._entity_type,
          _ingested_at=s._ingested_at,_batch_id=s._batch_id
        WHEN NOT MATCHED THEN INSERT
          (player_id,player_slug,name,position,dob,age,height_cm,foot,nationality,
           contract_until,market_value_eur,league,season,club_id,club_name,
           observed_at,_source,_entity_type,_ingested_at,_batch_id)
        VALUES
          (s.player_id,s.player_slug,s.name,s.position,s.dob,s.age,s.height_cm,
           s.foot,s.nationality,s.contract_until,s.market_value_eur,s.league,
           s.season,s.club_id,s.club_name,s.observed_at,s._source,s._entity_type,
           s._ingested_at,s._batch_id)""",
        """CREATE TABLE IF NOT EXISTS iceberg.bronze.transfermarkt_market_value_points
        WITH (format = 'PARQUET') AS
        SELECT CAST(player_id AS varchar) player_id, mv_date,
               CAST(value_eur AS bigint) value_eur, club_name,
               CAST(age AS integer) age, mv_raw,
               _source, CAST('market_value_points' AS varchar) _entity_type,
               CAST(_ingested_at AS timestamp(6)) _ingested_at, _batch_id
        FROM iceberg.bronze.transfermarkt_market_value_history WHERE false""",
        """MERGE INTO iceberg.bronze.transfermarkt_market_value_points t
        USING (
            SELECT player_id,mv_date,value_eur,club_name,age,mv_raw,_source,
                   CAST('market_value_points' AS varchar) _entity_type,
                   _ingested_at,_batch_id
            FROM (
                SELECT b.*, ROW_NUMBER() OVER (
                    PARTITION BY player_id,mv_date ORDER BY _ingested_at DESC,_batch_id DESC
                ) rn
                FROM iceberg.bronze.transfermarkt_market_value_history b
                WHERE player_id IS NOT NULL AND mv_date IS NOT NULL
            ) WHERE rn=1
        ) s ON t.player_id=s.player_id AND t.mv_date=s.mv_date
        WHEN MATCHED AND s._ingested_at >= t._ingested_at THEN UPDATE SET
          value_eur=s.value_eur,club_name=s.club_name,age=s.age,mv_raw=s.mv_raw,
          _source=s._source,_entity_type=s._entity_type,
          _ingested_at=s._ingested_at,_batch_id=s._batch_id
        WHEN NOT MATCHED THEN INSERT
          (player_id,mv_date,value_eur,club_name,age,mv_raw,_source,_entity_type,
           _ingested_at,_batch_id)
        VALUES
          (s.player_id,s.mv_date,s.value_eur,s.club_name,s.age,s.mv_raw,s._source,
           s._entity_type,s._ingested_at,s._batch_id)""",
        f"""CREATE TABLE IF NOT EXISTS iceberg.bronze.transfermarkt_transfer_events
        WITH (format = 'PARQUET') AS
        SELECT {event_id} transfer_id, CAST(b.player_id AS varchar) player_id,
               b.transfer_date, {event_season} event_season,
               CAST(b.from_club_id AS varchar) from_club_id,b.from_club_name,
               CAST(b.to_club_id AS varchar) to_club_id,b.to_club_name,
               b.fee_text,CAST(b.fee_eur AS bigint) fee_eur,
               CAST(b.market_value_eur AS bigint) market_value_eur,b.is_upcoming,
               b._source,CAST('transfer_events' AS varchar) _entity_type,
               CAST(b._ingested_at AS timestamp(6)) _ingested_at,b._batch_id
        FROM iceberg.bronze.transfermarkt_transfers b WHERE false""",
        f"""MERGE INTO iceberg.bronze.transfermarkt_transfer_events t
        USING (
            WITH projected AS (
                SELECT {event_id} transfer_id,
                       CAST(b.player_id AS varchar) player_id,
                       b.transfer_date, {event_season} event_season,
                       CAST(b.from_club_id AS varchar) from_club_id,b.from_club_name,
                       CAST(b.to_club_id AS varchar) to_club_id,b.to_club_name,
                       b.fee_text,CAST(b.fee_eur AS bigint) fee_eur,
                       CAST(b.market_value_eur AS bigint) market_value_eur,b.is_upcoming,
                       b._source,CAST('transfer_events' AS varchar) _entity_type,
                       CAST(b._ingested_at AS timestamp(6)) _ingested_at,b._batch_id
                FROM iceberg.bronze.transfermarkt_transfers b
                WHERE b.player_id IS NOT NULL
            ), dedup AS (
                SELECT p.*, ROW_NUMBER() OVER (
                    PARTITION BY player_id,transfer_id
                    ORDER BY _ingested_at DESC,_batch_id DESC
                ) rn
                FROM projected p
            )
            SELECT transfer_id,player_id,transfer_date,event_season,
                   from_club_id,from_club_name,to_club_id,to_club_name,
                   fee_text,fee_eur,market_value_eur,is_upcoming,_source,
                   _entity_type,_ingested_at,_batch_id
            FROM dedup WHERE rn=1
        ) s ON t.player_id=s.player_id AND t.transfer_id=s.transfer_id
        WHEN MATCHED AND s._ingested_at >= t._ingested_at THEN UPDATE SET
          transfer_date=s.transfer_date,event_season=s.event_season,
          from_club_id=s.from_club_id,from_club_name=s.from_club_name,
          to_club_id=s.to_club_id,to_club_name=s.to_club_name,fee_text=s.fee_text,
          fee_eur=s.fee_eur,market_value_eur=s.market_value_eur,
          is_upcoming=s.is_upcoming,_source=s._source,_entity_type=s._entity_type,
          _ingested_at=s._ingested_at,_batch_id=s._batch_id
        WHEN NOT MATCHED THEN INSERT
          (transfer_id,player_id,transfer_date,event_season,from_club_id,
           from_club_name,to_club_id,to_club_name,fee_text,fee_eur,
           market_value_eur,is_upcoming,_source,_entity_type,_ingested_at,_batch_id)
        VALUES
          (s.transfer_id,s.player_id,s.transfer_date,s.event_season,s.from_club_id,
           s.from_club_name,s.to_club_id,s.to_club_name,s.fee_text,s.fee_eur,
           s.market_value_eur,s.is_upcoming,s._source,s._entity_type,
           s._ingested_at,s._batch_id)""",
        """CREATE TABLE IF NOT EXISTS iceberg.bronze.transfermarkt_coach_profiles
        WITH (format = 'PARQUET') AS
        SELECT CAST(coach_id AS varchar) coach_id,coach_slug,name,dob,nationality,
               _source,CAST('coach_profiles' AS varchar) _entity_type,
               CAST(_ingested_at AS timestamp(6)) _ingested_at,_batch_id
        FROM iceberg.bronze.transfermarkt_coaches WHERE false""",
        """MERGE INTO iceberg.bronze.transfermarkt_coach_profiles t
        USING (
            SELECT coach_id,coach_slug,name,dob,nationality,_source,
                   CAST('coach_profiles' AS varchar) _entity_type,_ingested_at,_batch_id
            FROM (
                SELECT b.*,ROW_NUMBER() OVER (
                    PARTITION BY coach_id ORDER BY _ingested_at DESC,_batch_id DESC
                ) rn FROM iceberg.bronze.transfermarkt_coaches b
                WHERE coach_id IS NOT NULL
            ) WHERE rn=1
        ) s ON t.coach_id=s.coach_id
        WHEN MATCHED AND s._ingested_at >= t._ingested_at THEN UPDATE SET
          coach_slug=s.coach_slug,name=s.name,dob=s.dob,nationality=s.nationality,
          _source=s._source,_entity_type=s._entity_type,
          _ingested_at=s._ingested_at,_batch_id=s._batch_id
        WHEN NOT MATCHED THEN INSERT
          (coach_id,coach_slug,name,dob,nationality,_source,_entity_type,_ingested_at,_batch_id)
        VALUES
          (s.coach_id,s.coach_slug,s.name,s.dob,s.nationality,s._source,
           s._entity_type,s._ingested_at,s._batch_id)""",
        """CREATE TABLE IF NOT EXISTS iceberg.bronze.transfermarkt_coach_stints
        WITH (format = 'PARQUET') AS
        SELECT CAST(current_club_id AS varchar) club_id,current_club_name club_name,
               CAST(coach_id AS varchar) coach_id,coach_slug,name,role,
               CAST(NULL AS date) appointed_date,CAST(NULL AS date) left_date,
               _source,CAST('coach_stints' AS varchar) _entity_type,
               CAST(_ingested_at AS timestamp(6)) _ingested_at,_batch_id
        FROM iceberg.bronze.transfermarkt_coaches WHERE false""",
        # Do not manufacture unbounded historical stints from the legacy
        # season snapshot: it discarded appointed/left dates.  The empty typed
        # contract is populated opportunistically by the native scraper.
        *(
            f"ALTER TABLE iceberg.bronze.{table} ADD COLUMN IF NOT EXISTS {column} {dtype}"
            for table in (
                'transfermarkt_squad_memberships',
                'transfermarkt_player_attribute_observations',
                'transfermarkt_market_value_points',
                'transfermarkt_transfer_events',
                'transfermarkt_coach_profiles',
                'transfermarkt_coach_stints',
            )
            for column, dtype in (
                ('source_competition_id', 'varchar'),
                ('source_edition_id', 'varchar'),
                ('source_url', 'varchar'),
                ('source_body_hash', 'varchar'),
                ('fetched_at', 'timestamp(6)'),
                ('parser_revision', 'varchar'),
                ('schema_revision', 'varchar'),
                ('cycle_id', 'varchar'),
                ('scope_id', 'varchar'),
            )
        ),
        "ALTER TABLE iceberg.bronze.transfermarkt_squad_memberships ADD COLUMN IF NOT EXISTS competition_id varchar",
        "ALTER TABLE iceberg.bronze.transfermarkt_squad_memberships ADD COLUMN IF NOT EXISTS edition_id varchar",
        "ALTER TABLE iceberg.bronze.transfermarkt_player_attribute_observations ADD COLUMN IF NOT EXISTS competition_id varchar",
        "ALTER TABLE iceberg.bronze.transfermarkt_player_attribute_observations ADD COLUMN IF NOT EXISTS edition_id varchar",
        "ALTER TABLE iceberg.bronze.transfermarkt_squad_memberships SET PROPERTIES partitioning = ARRAY['competition_id', 'edition_id']",
        "ALTER TABLE iceberg.bronze.transfermarkt_player_attribute_observations SET PROPERTIES partitioning = ARRAY['competition_id', 'edition_id']",
    ]


ParityPair = control.ParityPair


def parity_pairs() -> tuple[ParityPair, ...]:
    return control.parity_pairs()


def _connect():
    return control.connect()


def legacy_backup_status(cur) -> dict[str, dict[str, int | None]]:
    """Return exact read-only restore points for retained legacy Bronze."""

    report: dict[str, dict[str, int | None]] = {}
    for relation in LEGACY_BACKUP_RELATIONS:
        catalog, schema, table = relation.split('.')
        cur.execute(f'SELECT COUNT(*) FROM {relation}')
        count_rows = list(cur.fetchall())
        if len(count_rows) != 1 or len(count_rows[0]) != 1:
            raise RuntimeError(f'{relation}: row-count query shape drift')
        cur.execute(
            f'SELECT MAX(snapshot_id) FROM '
            f'{catalog}.{schema}."{table}$snapshots"'
        )
        snapshot_rows = list(cur.fetchall())
        if len(snapshot_rows) != 1 or len(snapshot_rows[0]) != 1:
            raise RuntimeError(f'{relation}: snapshot query shape drift')
        snapshot = snapshot_rows[0][0]
        report[relation] = {
            'row_count': int(count_rows[0][0]),
            'snapshot_id': int(snapshot) if snapshot is not None else None,
        }
    return report


def registry_backup_status(cur) -> dict[str, dict[str, int | None]]:
    """Return exact read-only restore points for registry discovery tables."""

    report: dict[str, dict[str, int | None]] = {}
    for relation in REGISTRY_DISCOVERY_RELATIONS:
        catalog, schema, table = relation.split('.')
        cur.execute(f'SELECT COUNT(*) FROM {relation}')
        count_rows = list(cur.fetchall())
        if len(count_rows) != 1 or len(count_rows[0]) != 1:
            raise RuntimeError(f'{relation}: row-count query shape drift')
        cur.execute(
            f'SELECT MAX(snapshot_id) FROM '
            f'{catalog}.{schema}."{table}$snapshots"'
        )
        snapshot_rows = list(cur.fetchall())
        if len(snapshot_rows) != 1 or len(snapshot_rows[0]) != 1:
            raise RuntimeError(f'{relation}: snapshot query shape drift')
        snapshot = snapshot_rows[0][0]
        report[relation] = {
            'row_count': int(count_rows[0][0]),
            'snapshot_id': int(snapshot) if snapshot is not None else None,
        }
    return report


def _normalise_registry_cycle_id(value: str) -> str:
    cycle_id = str(value or '')
    if not _REGISTRY_CYCLE_ID.fullmatch(cycle_id):
        raise ValueError(
            'cycle_id must match tm-registry-[0-9a-f]{24}'
        )
    return cycle_id


def registry_discovery_rollback_sql(cycle_id: str) -> tuple[str, ...]:
    """Return the only two mutations allowed by the bounded rollback."""

    exact_cycle = _normalise_registry_cycle_id(cycle_id)
    return tuple(
        f"DELETE FROM {relation} WHERE cycle_id = '{exact_cycle}'"
        for relation in REGISTRY_DISCOVERY_RELATIONS
    )


def rollback_registry_discovery(cur, cycle_id: str) -> dict[str, object]:
    """Delete one exact discovery cycle and prove no target rows remain."""

    exact_cycle = _normalise_registry_cycle_id(cycle_id)
    statements = registry_discovery_rollback_sql(exact_cycle)
    for statement in statements:
        cur.execute(statement)
        cur.fetchall()

    remaining: dict[str, int] = {}
    for relation in REGISTRY_DISCOVERY_RELATIONS:
        cur.execute(
            f"SELECT COUNT(*) FROM {relation} "
            f"WHERE cycle_id = '{exact_cycle}'"
        )
        rows = list(cur.fetchall())
        if len(rows) != 1 or len(rows[0]) != 1:
            raise RuntimeError(f'{relation}: rollback count query shape drift')
        remaining[relation] = int(rows[0][0])
    if any(remaining.values()):
        raise RuntimeError(
            f'registry discovery rollback left rows behind: {remaining}'
        )
    return {
        'status': 'rolled_back',
        'cycle_id': exact_cycle,
        'deleted_relations': list(REGISTRY_DISCOVERY_RELATIONS),
        'remaining_rows': remaining,
        'retained_evidence': ['manifests', 'cache'],
    }


def run_parity(cur, cycle_id: str, league: str, season: int):
    return control.run_parity(cur, cycle_id, league=league, season=season)


def readiness(
    cur,
    cycle_id: str,
    league: str,
    season: int,
    expected_revision: int,
) -> dict:
    return control.readiness(
        cur,
        cycle_id,
        league=league,
        season=season,
        expected_revision=expected_revision,
    )


def cleanup_sql(active_slot: str = 'a') -> str:
    # Dynamic views must stop resolving the legacy branch before retained
    # relations are removed; otherwise Trino analysis fails even in v2 state.
    control._normalise_slot(active_slot)
    statements = control.post_cleanup_reader_view_sql_all()
    statements.extend([
        'DROP VIEW IF EXISTS iceberg.gold.fct_transfer_legacy',
        'DROP VIEW IF EXISTS '
        'iceberg.gold.transfermarkt_team_season_market_value_legacy',
        'DROP TABLE IF EXISTS iceberg.silver.transfermarkt_players_legacy',
        'DROP TABLE IF EXISTS iceberg.silver.transfermarkt_coaches_legacy',
        'DROP TABLE IF EXISTS iceberg.gold.fct_transfer_legacy_source',
        'DROP TABLE IF EXISTS iceberg.gold.fct_player_market_value_legacy',
        'DROP TABLE IF EXISTS iceberg.gold.dim_manager_legacy',
        'DROP TABLE IF EXISTS iceberg.silver.transfermarkt_market_value_history',
        'DROP TABLE IF EXISTS iceberg.silver.transfermarkt_transfers',
    ])
    statements.extend(
        f'DROP TABLE IF EXISTS {relation.canonical}_pre_native_v2_backup'
        for relation in control.CANONICAL_READER_RELATIONS
    )
    statements.extend(
        f'DROP TABLE IF EXISTS {contract.output_table}_pre_slots_backup'
        for contract in control.MODEL_CONTRACTS
    )
    statements.extend(
        f'DROP TABLE IF EXISTS iceberg.bronze.{table}' for table in LEGACY_TABLES
    )
    return '\n'.join(f'{sql};' for sql in statements)


def _print_statements(statements: Iterable[str]) -> None:
    for index, statement in enumerate(statements, 1):
        print(f'-- [{index}]')
        print(statement.rstrip() + ';')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('plan', help='print idempotent bootstrap SQL')
    boot = sub.add_parser('bootstrap', help='print SQL; execute only with --apply')
    boot.add_argument('--apply', action='store_true')
    views = sub.add_parser(
        'reader-views',
        help='plan safe legacy renames/adapters/canonical views; --apply writes',
    )
    views.add_argument('--apply', action='store_true')
    sub.add_parser('status', help='read-only canonical reader state')
    sub.add_parser(
        'backup-status',
        help='read-only legacy row counts and Iceberg snapshot restore points',
    )
    sub.add_parser(
        'registry-backup-status',
        help='read-only registry row counts, snapshots and reader status',
    )
    registry_rollback = sub.add_parser(
        'rollback-registry-discovery',
        help='delete one exact registry discovery cycle; dry-run by default',
    )
    registry_rollback.add_argument(
        '--cycle-id',
        required=True,
        type=_normalise_registry_cycle_id,
    )
    registry_rollback.add_argument('--apply', action='store_true')

    def add_scope(command, *, revision: bool = False):
        command.add_argument(
            '--cycle-id', required=True,
            help='production dual-write cycle id; never inferred',
        )
        command.add_argument('--league', required=True)
        command.add_argument('--season', required=True, type=int)
        if revision:
            command.add_argument('--expected-revision', required=True, type=int)

    parity = sub.add_parser(
        'parity', help='read-only batch-scoped bidirectional EXCEPT checks',
    )
    add_scope(parity)
    ready = sub.add_parser('readiness', help='run the blocking read-only cutover gate')
    add_scope(ready, revision=True)
    cut = sub.add_parser(
        'cutover', help='read-only plan by default; CAS write only with --apply',
    )
    add_scope(cut, revision=True)
    cut.add_argument('--actor', default=os.environ.get('USER', 'operator'))
    cut.add_argument('--reason', default='approved native-v2 cutover')
    cut.add_argument('--confirm-quiesced', action='store_true')
    cut.add_argument('--scope-set-id', required=True)
    cut.add_argument('--apply', action='store_true')
    advance = sub.add_parser(
        'advance-cycle', help='CAS active v2 to an inactive ready slot',
    )
    add_scope(advance, revision=True)
    advance.add_argument('--actor', default=os.environ.get('USER', 'operator'))
    advance.add_argument('--reason', default='approved native-v2 cycle advance')
    advance.add_argument('--confirm-quiesced', action='store_true')
    advance.add_argument('--scope-set-id', required=True)
    advance.add_argument('--apply', action='store_true')
    roll = sub.add_parser(
        'rollback', help='read-only plan by default; CAS write only with --apply',
    )
    add_scope(roll, revision=True)
    roll.add_argument('--actor', default=os.environ.get('USER', 'operator'))
    roll.add_argument('--reason', default='native-v2 rollback drill')
    roll.add_argument('--apply', action='store_true')
    restore = sub.add_parser(
        'restore-v2', help='restore retained verified v2 slot without proxy I/O',
    )
    add_scope(restore, revision=True)
    restore.add_argument('--actor', default=os.environ.get('USER', 'operator'))
    restore.add_argument('--reason', default='restore verified retained native-v2 slot')
    restore.add_argument('--confirm-quiesced', action='store_true')
    restore.add_argument('--apply', action='store_true')
    slot_roll = sub.add_parser(
        'rollback-slot', help='CAS active v2 to exact retained previous slot',
    )
    add_scope(slot_roll, revision=True)
    slot_roll.add_argument('--actor', default=os.environ.get('USER', 'operator'))
    slot_roll.add_argument('--reason', default='rollback active v2 to retained previous slot')
    slot_roll.add_argument('--confirm-quiesced', action='store_true')
    slot_roll.add_argument('--apply', action='store_true')
    verify_roll = sub.add_parser(
        'verify-rollback',
        help='DQ-backed rollback attestation; CAS write only with --apply',
    )
    add_scope(verify_roll, revision=True)
    verify_roll.add_argument('--actor', default=os.environ.get('USER', 'operator'))
    verify_roll.add_argument('--downstream-dq-run-id', required=True)
    verify_roll.add_argument('--apply', action='store_true')
    cleanup = sub.add_parser(
        'cleanup-check', help='read-only retention/rollback/readiness guard',
    )
    add_scope(cleanup)
    disable = sub.add_parser(
        'disable-legacy-writers', help='persist writer shutdown with CAS',
    )
    add_scope(disable, revision=True)
    disable.add_argument('--actor', default=os.environ.get('USER', 'operator'))
    disable.add_argument('--confirm-quiesced', action='store_true')
    disable.add_argument('--apply', action='store_true')
    sub.add_parser('post-cleanup-verify', help='read-only v2-only A/B view verifier')
    complete = sub.add_parser(
        'complete-cleanup', help='CAS cleanup attestation after read-only verifier',
    )
    complete.add_argument('--expected-revision', required=True, type=int)
    complete.add_argument('--actor', default=os.environ.get('USER', 'operator'))
    complete.add_argument('--apply', action='store_true')
    clean = sub.add_parser('cleanup-sql', help='generate, never execute, legacy DROP SQL')
    add_scope(clean)
    clean.add_argument('--confirm-legacy-retention-expired', action='store_true')
    clean.add_argument('--confirm-canonical-readers-switched', action='store_true')
    clean.add_argument('--confirm-legacy-writers-disabled', action='store_true')
    args = parser.parse_args(argv)

    if args.command == 'plan' or (args.command == 'bootstrap' and not args.apply):
        _print_statements(bootstrap_sql())
        if args.command == 'bootstrap':
            print('-- DRY RUN: pass --apply to execute bootstrap statements', file=sys.stderr)
        return 0
    if (
        args.command == 'rollback-registry-discovery'
        and not args.apply
    ):
        for statement in registry_discovery_rollback_sql(args.cycle_id):
            print(statement + ';')
        return 0

    conn = _connect()
    cur = conn.cursor()
    try:
        if args.command == 'bootstrap':
            for statement in bootstrap_sql():
                cur.execute(statement)
                # Trino requires the result stream (including DDL/DML update
                # counts) to be consumed before the next statement; otherwise
                # the coordinator may cancel it as USER_CANCELED.
                cur.fetchall()
            print(json.dumps({'status': 'bootstrapped', 'statements': len(bootstrap_sql())}))
            return 0
        if args.command == 'reader-views':
            current_state = control.read_reader_state(cur, allow_missing=False)
            statements = control.reader_view_bootstrap_plan(cur)
            if not args.apply:
                _print_statements(statements)
                print('-- DRY RUN: pass --apply to execute reader DDL', file=sys.stderr)
                return 0
            report = control.apply_reader_view_bootstrap(cur)
            route = control.verify_reader_views(
                cur, expected_version=current_state.active_version,
                expected_revision=current_state.revision,
            )
            report['route'] = route
            print(json.dumps(report, indent=2, sort_keys=True, default=str))
            return 0 if route['passed'] else 1
        if args.command == 'status':
            print(json.dumps(
                control.status(cur), indent=2, sort_keys=True, default=str,
            ))
            return 0
        if args.command == 'backup-status':
            print(json.dumps({
                'reader': control.status(cur),
                'legacy_tables': legacy_backup_status(cur),
            }, indent=2, sort_keys=True, default=str))
            return 0
        if args.command == 'registry-backup-status':
            print(json.dumps({
                'reader': control.status(cur),
                'registry_tables': registry_backup_status(cur),
            }, indent=2, sort_keys=True, default=str))
            return 0
        if args.command == 'rollback-registry-discovery':
            report = rollback_registry_discovery(cur, args.cycle_id)
            print(json.dumps(report, indent=2, sort_keys=True, default=str))
            return 0
        if args.command == 'parity':
            report = run_parity(
                cur, args.cycle_id, args.league, args.season,
            )
            print(json.dumps(report, indent=2, sort_keys=True, default=str))
            return 0 if all(x['passed'] for x in report.values()) else 1
        if args.command == 'readiness':
            report = readiness(
                cur,
                args.cycle_id,
                args.league,
                args.season,
                args.expected_revision,
            )
            print(json.dumps(report, indent=2, sort_keys=True, default=str))
            return 0 if report['ready'] else 1
        if args.command == 'cutover':
            report = control.cutover(
                cur,
                cycle_id=args.cycle_id,
                league=args.league,
                season=args.season,
                expected_revision=args.expected_revision,
                actor=args.actor,
                reason=args.reason,
                quiesced=args.confirm_quiesced,
                apply=args.apply,
                scope_set_id=args.scope_set_id,
            )
            print(json.dumps(report, indent=2, sort_keys=True, default=str))
            if not args.apply:
                print('-- DRY RUN: pass --apply for the audited CAS', file=sys.stderr)
            return 0
        if args.command == 'advance-cycle':
            report = control.advance_cycle(
                cur, cycle_id=args.cycle_id, league=args.league,
                season=args.season, expected_revision=args.expected_revision,
                actor=args.actor, reason=args.reason,
                quiesced=args.confirm_quiesced, apply=args.apply,
                scope_set_id=args.scope_set_id,
            )
            print(json.dumps(report, indent=2, sort_keys=True, default=str))
            return 0
        if args.command == 'rollback':
            report = control.rollback(
                cur,
                cycle_id=args.cycle_id,
                league=args.league,
                season=args.season,
                expected_revision=args.expected_revision,
                actor=args.actor,
                reason=args.reason,
                apply=args.apply,
            )
            print(json.dumps(report, indent=2, sort_keys=True, default=str))
            if not args.apply:
                print('-- DRY RUN: pass --apply for the audited CAS', file=sys.stderr)
            return 0
        if args.command == 'restore-v2':
            report = control.restore_v2(
                cur, cycle_id=args.cycle_id, league=args.league,
                season=args.season, expected_revision=args.expected_revision,
                actor=args.actor, reason=args.reason,
                quiesced=args.confirm_quiesced, apply=args.apply,
            )
            print(json.dumps(report, indent=2, sort_keys=True, default=str))
            return 0
        if args.command == 'rollback-slot':
            report = control.rollback_slot(
                cur, cycle_id=args.cycle_id, league=args.league,
                season=args.season, expected_revision=args.expected_revision,
                actor=args.actor, reason=args.reason,
                quiesced=args.confirm_quiesced, apply=args.apply,
            )
            print(json.dumps(report, indent=2, sort_keys=True, default=str))
            return 0
        if args.command == 'verify-rollback':
            report = control.verify_rollback(
                cur,
                cycle_id=args.cycle_id,
                league=args.league,
                season=args.season,
                expected_revision=args.expected_revision,
                actor=args.actor,
                downstream_dq_run_id=args.downstream_dq_run_id,
                apply=args.apply,
            )
            print(json.dumps(report, indent=2, sort_keys=True, default=str))
            if not args.apply:
                print('-- DRY RUN: pass --apply for rollback attestation', file=sys.stderr)
            return 0
        if args.command == 'disable-legacy-writers':
            report = control.disable_legacy_writers(
                cur, cycle_id=args.cycle_id, league=args.league,
                season=args.season, expected_revision=args.expected_revision,
                actor=args.actor, apply=args.apply,
                quiesced=args.confirm_quiesced,
            )
            print(json.dumps(report, indent=2, sort_keys=True, default=str))
            return 0
        if args.command == 'post-cleanup-verify':
            report = control.post_cleanup_verify(cur)
            print(json.dumps(report, indent=2, sort_keys=True, default=str))
            return 0 if report['passed'] else 1
        if args.command == 'complete-cleanup':
            report = control.complete_cleanup(
                cur, expected_revision=args.expected_revision,
                actor=args.actor, apply=args.apply,
            )
            print(json.dumps(report, indent=2, sort_keys=True, default=str))
            return 0
        cleanup_report = control.cleanup_check(
            cur,
            cycle_id=args.cycle_id,
            league=args.league,
            season=args.season,
        )
        if args.command == 'cleanup-check':
            print(json.dumps(
                cleanup_report, indent=2, sort_keys=True, default=str,
            ))
            return 0 if cleanup_report['eligible'] else 1
        confirmations = all((
            args.confirm_legacy_retention_expired,
            args.confirm_canonical_readers_switched,
            args.confirm_legacy_writers_disabled,
        ))
        if not cleanup_report['eligible'] or not confirmations:
            print(json.dumps(
                cleanup_report, indent=2, sort_keys=True, default=str,
            ), file=sys.stderr)
            print(
                'Refusing cleanup SQL: live guards, persisted writer shutdown, '
                'and explicit retention/reader confirmations are required',
                file=sys.stderr,
            )
            return 2
        print(cleanup_sql(cleanup_report['state']['active_slot']))
        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())
