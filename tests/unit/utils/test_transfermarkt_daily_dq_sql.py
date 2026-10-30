"""Execute daily DQ SQL against producer schemas, with offline DuckDB data.

SQLGlot translates Trino expressions. Iceberg versions are removed only in
this adapter: these tests validate columns and counter semantics, not runtime
Iceberg time travel or production acceptance.
"""
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path

import duckdb
import pytest
import sqlglot
from sqlglot import exp

from dags.scripts import run_transfermarkt_discovery as discovery
from scrapers.transfermarkt import scraper
from scrapers.transfermarkt.first_seen import KEYS, fact_key
from utils import transfermarkt_bronze_dq as bronze
from utils import transfermarkt_daily_dq as daily
from utils import transfermarkt_registry_publish as publication

NOW = datetime(2026, 10, 12, 10, tzinfo=timezone.utc)
CLOCK = NOW.replace(tzinfo=None)
SCHEMAS = {
    'competitions': discovery.COMPETITION_COLUMNS,
    'competition_editions': discovery.EDITION_COLUMNS,
    'squad_memberships': scraper.SQUAD_MEMBERSHIP_COLUMNS + scraper._METADATA_COLUMNS,
    'player_attribute_observations': scraper.PLAYER_ATTRIBUTE_OBSERVATION_COLUMNS + scraper._METADATA_COLUMNS,
    'player_contract_observations': scraper.PLAYER_CONTRACT_OBSERVATION_COLUMNS + scraper._METADATA_COLUMNS,
    'market_value_points': scraper.MARKET_VALUE_POINT_COLUMNS + scraper._METADATA_COLUMNS,
    'transfer_events': scraper.TRANSFER_EVENT_COLUMNS + scraper._METADATA_COLUMNS,
    'coach_profiles': scraper.COACH_PROFILE_COLUMNS + scraper._METADATA_COLUMNS,
    'coach_stints': scraper.COACH_STINT_COLUMNS + scraper._METADATA_COLUMNS,
}
DATE_COLUMNS = {'dob', 'mv_date', 'transfer_date', 'start_date', 'end_date',
                'contract_until', 'appointed_date', 'left_date'}
CLOCK_COLUMNS = {'fetched_at', '_ingested_at', 'observed_at', 'discovered_at', 'first_seen_at'}
NUMBER_COLUMNS = {'age', 'height_cm', 'value_eur', 'fee_eur', 'market_value_eur', 'participant_count'}
BOOL_COLUMNS = {'active', 'current', 'is_upcoming'}


def _type(column):
    if column in CLOCK_COLUMNS:
        return 'TIMESTAMP'
    if column in DATE_COLUMNS:
        return 'DATE'
    if column in NUMBER_COLUMNS:
        return 'BIGINT'
    if column in BOOL_COLUMNS:
        return 'BOOLEAN'
    return 'VARCHAR'


def _table(entity):
    return f'iceberg.bronze.transfermarkt_{entity}'


def _row(entity, suffix='1', **overrides):
    values = {}
    for column in SCHEMAS[entity]:
        kind = _type(column)
        values[column] = (CLOCK if kind == 'TIMESTAMP' else date(2026, 10, 1)
                          if kind == 'DATE' else 100 if kind == 'BIGINT'
                          else True if kind == 'BOOLEAN' else 'known')
    for column in ('player_id', 'club_id', 'coach_id', 'transfer_id'):
        if column in values:
            values[column] = suffix
    values.update(competition_id='GB1') if 'competition_id' in values else None
    values.update(edition_id='2026') if 'edition_id' in values else None
    values.update(source_competition_id='GB1') if 'source_competition_id' in values else None
    values.update(source_edition_id='2026') if 'source_edition_id' in values else None
    if 'applicability_status' in values:
        values['applicability_status'] = 'ok'
    if 'mv_raw' in values:
        values['mv_raw'] = '€100'
    if 'fee_text' in values:
        values['fee_text'] = '€100'
    values.update(overrides)
    return values


def _insert(conn, entity, row):
    columns = list(row)
    conn.execute(f'INSERT INTO {_table(entity)} (' + ', '.join(f'"{column}"' for column in columns)
                 + ') VALUES (' + ', '.join('?' for _ in columns) + ')', list(row.values()))


class SqlCursor:
    def __init__(self, conn):
        self.conn = conn
        self.sql = []

    def execute(self, sql):
        self.sql.append(sql)
        expression = sqlglot.parse_one(sql, read='trino')
        for table in expression.find_all(exp.Table):
            table.set('version', None)
            if table.db == 'information_schema':
                table.set('catalog', None)
        self.result = self.conn.execute(expression.sql(dialect='duckdb'))

    def fetchall(self):
        return self.result.fetchall()


@pytest.fixture
def warehouse():
    conn = duckdb.connect()
    conn.execute("ATTACH ':memory:' AS iceberg")
    conn.execute('CREATE SCHEMA iceberg.bronze')
    conn.execute('CREATE SCHEMA iceberg.ops')
    for entity, columns in SCHEMAS.items():
        conn.execute(f'CREATE TABLE {_table(entity)} (' + ', '.join(f'"{column}" {_type(column)}' for column in columns) + ')')
    conn.execute(f'CREATE TABLE {daily.FIRST_SEEN_REGISTRY} (table_name VARCHAR, key_hash VARCHAR, key_json VARCHAR, first_seen_at TIMESTAMP)')
    conn.execute(f'CREATE TABLE {bronze.SCOPE_MANIFEST_TABLE} (competition_id VARCHAR, edition_id VARCHAR, child_cycle_id VARCHAR, status VARCHAR, committed_at TIMESTAMP, entity_manifest_json VARCHAR)')
    try:
        yield conn
    finally:
        conn.close()


def _frozen(conn, *, registry=False):
    pins = {table: 123 for table in bronze.NATIVE_BRONZE_KEYS}
    pins[bronze.SCOPE_MANIFEST_TABLE] = 123
    if registry:
        pins[daily.FIRST_SEEN_REGISTRY] = 123
    return daily._PinnedCursor(SqlCursor(conn), pins), pins


@pytest.mark.parametrize('entity', tuple(SCHEMAS))
def test_all_nine_field_queries_execute_on_actual_producer_schemas(warehouse, entity):
    conn = warehouse
    old = CLOCK - timedelta(days=1)
    _insert(conn, entity, _row(entity, 'old', fetched_at=old, _ingested_at=old))
    _insert(conn, entity, _row(entity, 'bad', fetched_at=old, source_url=None))
    _insert(conn, entity, _row(entity, 'new'))
    _insert(conn, entity, _row(entity, 'clockless', fetched_at=None, _ingested_at=None))
    cur, _ = _frozen(conn)
    cur.execute(daily.build_field_metrics_sql(entity, NOW))
    counts = cur.fetchall()[0]
    assert counts[:6] == (4, 0, 2, 2, 1, 1)
    assert len(counts) == 6 + 3 * len(daily.FIELDS[entity])
    for index, _field in enumerate(daily.FIELDS[entity]):
        assert counts[6 + 3 * index:9 + 3 * index] == (4, 4, 0)


def test_legacy_scope_unknown_is_separate_and_missing_lineage_is_only_new(warehouse):
    entity = 'squad_memberships'
    old = CLOCK - timedelta(days=1)
    _insert(warehouse, entity, _row(entity, 'legacy', competition_id=None, edition_id=None,
                                   fetched_at=old, _ingested_at=old))
    _insert(warehouse, entity, _row(entity, 'old', fetched_at=old, _ingested_at=old, source_url=None))
    _insert(warehouse, entity, _row(entity, 'new', source_body_hash=None))
    cur, _ = _frozen(warehouse)
    cur.execute(daily.build_field_metrics_sql(entity, NOW))
    assert cur.fetchall()[0][:6] == (3, 1, 1, 1, 1, 0)


@pytest.mark.parametrize('entity', tuple(SCHEMAS))
@pytest.mark.parametrize('missing', ['first', 'all'])
def test_fresh_missing_scope_is_new_and_has_null_lineage(warehouse, entity, missing):
    columns = ('competition_id',) if entity == 'competitions' else bronze._entity_scope_columns(_table(entity))
    absent = {column: None for column in (columns[:1] if missing == 'first' else columns)}
    old = CLOCK - timedelta(days=1)
    _insert(warehouse, entity, _row(entity, 'old-unknown', **absent, fetched_at=old, _ingested_at=old))
    # All common metadata are valid. Missing scope provenance alone is a violation.
    _insert(warehouse, entity, _row(entity, 'fresh-unknown', **absent))
    cur, _ = _frozen(warehouse)
    cur.execute(daily.build_field_metrics_sql(entity, NOW))
    assert cur.fetchall()[0][:6] == (2, 1, 0, 1, 1, 0)


def test_contract_applicability_and_placeholder_zero_execute(warehouse):
    _insert(warehouse, 'player_contract_observations', _row('player_contract_observations', '1'))
    _insert(warehouse, 'player_contract_observations', _row('player_contract_observations', '2', applicability_status='not_applicable', contract_until=None))
    for value, raw in ((0, '?'), (0, '€0'), (None, '-'), (100, '€100')):
        _insert(warehouse, 'market_value_points', _row('market_value_points', str(raw), value_eur=value, mv_raw=raw))
    for fee, text in ((0, 'free transfer'), (0, '-'), (None, '?'), (100, '€100')):
        _insert(warehouse, 'transfer_events', _row('transfer_events', str(text), fee_eur=fee, fee_text=text))
    cur, _ = _frozen(warehouse)
    cur.execute(daily.build_field_metrics_sql('player_contract_observations', NOW))
    row = cur.fetchall()[0]
    assert row[6:] == (1, 1, 1, 1, 1, 1, 1, 1, 1)
    cur.execute(daily.build_field_metrics_sql('market_value_points', NOW))
    assert cur.fetchall()[0][-3:] == (2, 2, 2)
    cur.execute(daily.build_field_metrics_sql('transfer_events', NOW))
    assert cur.fetchall()[0][-3:] == (2, 2, 2)
    cur.execute(daily.build_placeholder_zero_sql())
    assert cur.fetchall()[0] == (1, 1)


def test_route_counts_use_latest_outcome_and_keep_unknown(warehouse):
    for edition, committed, status in [('one', CLOCK - timedelta(days=1), 'authoritative_empty'),
                                       ('one', CLOCK, 'ok'), ('two', CLOCK, 'authoritative_empty'),
                                       ('three', CLOCK, None)]:
        payload = json.dumps({'dq_evidence': {'scope_capture': {'listing_status': status}}})
        warehouse.execute(f'INSERT INTO {bronze.SCOPE_MANIFEST_TABLE} VALUES (?, ?, ?, ?, ?, ?)',
                          ['FAC', edition, f'{edition}-{committed}', 'complete', committed, payload])
    cur, _ = _frozen(warehouse)
    cur.execute(daily.build_empty_routes_sql({'FAC': 'pokal'}))
    assert cur.fetchall() == [('pokal', 3, 1, 1)]


def test_first_seen_completeness_and_invariance_execute_with_nullable_key(warehouse):
    for table in KEYS:
        entity = table.removeprefix('transfermarkt_')
        warehouse.execute(f'ALTER TABLE {_table(entity)} ADD COLUMN first_seen_at TIMESTAMP')
        rows = [
            _row(entity, 'null', first_seen_at=None),
            _row(entity, 'drift', first_seen_at=CLOCK + timedelta(seconds=1)),
            _row(entity, 'old-drift', _ingested_at=CLOCK - timedelta(days=1),
                 fetched_at=CLOCK - timedelta(days=1), first_seen_at=CLOCK + timedelta(seconds=1)),
        ]
        for row in rows:
            if entity == 'coach_stints':
                row['left_date'] = None
            _insert(warehouse, entity, row)
            key_hash, key_json = fact_key(table, row)
            warehouse.execute(f'INSERT INTO {daily.FIRST_SEEN_REGISTRY} VALUES (?, ?, ?, ?)',
                              [table, key_hash, key_json, CLOCK])
    cur, pins = _frozen(warehouse, registry=True)
    metrics, checks = daily.collect_first_seen_metrics(cur, NOW, pins)
    assert len(metrics) == 4
    assert all(item['new_rows'] == 2 and item['null_new_rows'] == 1
               and item['registry_mismatches'] == 2 and item['status'] == 'RED'
               for item in metrics.values())
    assert all(not item.passed for item in checks)


@pytest.mark.parametrize('table,keys', KEYS.items())
def test_first_seen_invariance_joins_on_plain_equalities(table, keys):
    """#1971: IS NOT DISTINCT FROM over JSON extracts planned a nested loop."""
    sql = daily.build_first_seen_invariance_sql(table, keys, 'iceberg.bronze.' + table)
    join = sqlglot.parse_one(sql, read='trino').find(exp.Join)
    on = join.args['on']
    assert not list(on.find_all(exp.NullSafeEQ, exp.JSONExtractScalar))
    conditions = list(on.flatten()) if isinstance(on, exp.And) else [on]
    assert len(conditions) == len(keys)
    assert all(isinstance(item, exp.EQ) and all(isinstance(side, exp.Coalesce) for side in item.args.values())
               for item in conditions)
    registry = join.this.this
    assert registry.find(exp.Where).sql(dialect='trino') == f"WHERE table_name = '{table}'"


def test_missing_first_seen_schema_executes_and_stays_unknown(warehouse):
    cur, pins = _frozen(warehouse)
    metrics, checks = daily.collect_first_seen_metrics(cur, NOW, pins)
    assert not checks
    assert all(item['status'] == 'UNKNOWN' and item['null_new_rows'] is None
               for item in metrics.values())


@pytest.mark.parametrize('entity,output_columns', [
    ('competitions', publication._COMPETITION_COLUMNS),
    ('competition_editions', publication._EDITION_COLUMNS),
])
def test_real_silver_projection_proves_bronze_clock_name(warehouse, entity, output_columns):
    _insert(warehouse, entity, _row(entity))
    path = Path(__file__).resolve().parents[3] / 'dags/sql/silver' / f'transfermarkt_{entity}_v2.sql'
    sql = path.read_text()
    cur = SqlCursor(warehouse)
    cur.execute(sql)
    values = dict(zip([item[0] for item in cur.result.description], cur.fetchall()[0]))
    assert set(values) == set(output_columns)
    assert '_ingested_at' in SCHEMAS[entity] and '_bronze_ingested_at' not in SCHEMAS[entity]
    assert values['_bronze_ingested_at'] == CLOCK


@pytest.mark.parametrize('empty_count,expected_red', [(1, False), (2, True)])
def test_full_daily_metrics_execute_for_all_nine_and_real_pokal_threshold(warehouse, empty_count, expected_red):
    for entity in SCHEMAS:
        _insert(warehouse, entity, _row(entity))
    for index in range(20):
        status = 'authoritative_empty' if index < empty_count else 'ok'
        payload = json.dumps({'dq_evidence': {'scope_capture': {'listing_status': status}}})
        warehouse.execute(f'INSERT INTO {bronze.SCOPE_MANIFEST_TABLE} VALUES (?, ?, ?, ?, ?, ?)',
                          ['FAC', str(index), str(index), 'complete', CLOCK, payload])
    cur, _ = _frozen(warehouse)
    metrics, checks = daily.collect_daily_metrics(cur, NOW, {'FAC': 'pokal'})
    assert set(metrics['fields']) == set(SCHEMAS)
    assert set(metrics['cohorts']) == set(SCHEMAS)
    assert all(item['after_cutoff'] == 1 and item['lineage_null_after_cutoff'] == 0
               for item in metrics['cohorts'].values())
    route = metrics['empty_by_route']['pokal']
    assert route['ratio'] == empty_count / 20
    check = next(item for item in checks if item.kind == 'daily_empty_route')
    assert check.passed is not expected_red
    assert all(item.passed for item in checks if item.kind != 'daily_empty_route')
