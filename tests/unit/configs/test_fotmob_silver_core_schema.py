"""FotMob Silver core schema (#2016) and its model-1.1 field assignment stay consistent."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest
import yaml

CONFIG_DIR = Path(__file__).resolve().parents[3] / 'configs' / 'medallion' / 'fotmob_silver'
SCHEMA_PATH = CONFIG_DIR / 'core_schema.yaml'
ASSIGNMENT_PATH = CONFIG_DIR / 'field_assignment.tsv'

ALLOWED_TYPES = {
    'BIGINT', 'INTEGER', 'DOUBLE', 'DECIMAL(38,8)', 'BOOLEAN', 'VARCHAR', 'DATE',
    'TIMESTAMP(6)', 'TIMESTAMP(6) WITH TIME ZONE', 'ARRAY(VARCHAR)', 'ARRAY(BIGINT)',
}
TABLE_KEYS = {
    'scope', 'task', 'grain', 'natural_key', 'partitioning', 'sources', 'dedup_order', 'columns', 'raw_json',
}
COLUMN_KEYS = {'name', 'type', 'nullable', 'from'}
TSV_HEADER = [
    'registry_id', 'registry_bundle', 'source_family', 'model11_table', 'model11_field', 'json_path',
    'bronze_source_short', 'assignment', 'core_table', 'core_column', 'reason',
]
MODEL_11_FIELDS = 4929


@pytest.fixture(scope='module')
def schema():
    return yaml.safe_load(SCHEMA_PATH.read_text(encoding='utf-8'))


@pytest.fixture(scope='module')
def tables(schema):
    return schema['tables']


@pytest.fixture(scope='module')
def assignment_rows():
    with ASSIGNMENT_PATH.open(encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
        assert reader.fieldnames == TSV_HEADER
        return list(reader)


def _columns(table):
    return {c['name']: c for c in table['columns']}


def _raw(table):
    return {r['name']: r for r in table['raw_json']}


def test_top_level(schema):
    assert schema['version'] == 1
    assert schema['source'] == 'fotmob'
    assert schema['context_columns'] == ['competition_id', 'source_season_key']
    lineage = {c['name']: c for c in schema['lineage_columns']}
    assert set(lineage) == {
        '_bronze_batch_id', '_bronze_observed_at', '_bronze_ingested_at', '_parser_version', '_conform_issues_json',
    }
    assert all(c['type'] in ALLOWED_TYPES for c in lineage.values())


def test_table_count_and_names(tables):
    assert 15 <= len(tables) <= 22
    assert all(name.startswith('fotmob_') for name in tables)


@pytest.mark.parametrize('key', sorted(TABLE_KEYS))
def test_tables_have_required_keys(tables, key):
    missing = [name for name, t in tables.items() if key not in t]
    assert not missing, f'{key} missing in {missing}'


def test_task_and_scope_values(tables):
    for name, t in tables.items():
        assert t['task'] in {2017, 2018, 2019}, name
        assert t['scope'] in {'context', 'global'}, name
        assert t['sources'] and all(s.startswith('fotmob_') and s.endswith('_current') for s in t['sources']), name


def test_columns_well_formed(tables, schema):
    lineage = {c['name'] for c in schema['lineage_columns']}
    for name, t in tables.items():
        names = [c['name'] for c in t['columns']] + [r['name'] for r in t['raw_json']]
        assert len(names) == len(set(names)), f'{name}: duplicate column names'
        assert not lineage & set(names), f'{name}: lineage columns are global, not per table'
        for c in t['columns']:
            assert COLUMN_KEYS <= set(c), (name, c)
            assert c['type'] in ALLOWED_TYPES, (name, c['name'], c['type'])
            assert isinstance(c['nullable'], bool), (name, c['name'])
            assert c.get('conform', 'try') == 'try', (name, c['name'])
            assert c['from'].startswith(('fotmob_', 'derived:')), (name, c['name'], c['from'])


def test_raw_json_columns(tables):
    for name, t in tables.items():
        for r in t['raw_json']:
            assert r['name'].endswith('_json'), (name, r['name'])
            assert r.get('type', 'VARCHAR') == 'VARCHAR', (name, r['name'])
            assert r['from'] and r['holds'], (name, r['name'])


def test_keys_and_partitions_are_columns(tables):
    for name, t in tables.items():
        cols = _columns(t)
        assert t['natural_key'], name
        assert set(t['natural_key']) <= set(cols), name
        assert set(t['partitioning']) <= set(cols), name
        for key in t['natural_key']:
            assert cols[key]['nullable'] is False, (name, key)
            assert 'conform' not in cols[key], f'{name}.{key}: key errors drop the row, they are not conformed'


def test_context_tables_partition_by_context(tables, schema):
    context = schema['context_columns']
    for name, t in tables.items():
        if t['scope'] != 'context':
            continue
        assert t['partitioning'] == context, name
        assert t['natural_key'][:2] == context, name


def test_global_tables_have_no_partitions_or_context(tables, schema):
    context = set(schema['context_columns'])
    for name, t in tables.items():
        if t['scope'] != 'global':
            continue
        assert t['partitioning'] == [], name
        if t.get('context_as_key'):
            assert set(t['natural_key']) & context, f'{name}: context_as_key without context in key'
        else:
            assert not context & set(_columns(t)), f'{name}: global table carries context columns'


def test_assignment_covers_model_11_once(assignment_rows):
    ids = [r['registry_id'] for r in assignment_rows]
    assert len(ids) == MODEL_11_FIELDS
    assert len(set(ids)) == MODEL_11_FIELDS


def test_assignment_targets_exist(assignment_rows, tables):
    for r in assignment_rows:
        kind, table, column = r['assignment'], r['core_table'], r['core_column']
        assert kind in {'column', 'raw', 'excluded'}, r['registry_id']
        if kind == 'excluded':
            assert not table and not column, r['registry_id']
            assert len(r['reason']) >= 10, f"{r['registry_id']}: excluded needs a concrete reason"
            continue
        assert table in tables, r['registry_id']
        target = _columns(tables[table]) if kind == 'column' else _raw(tables[table])
        assert column in target, (r['registry_id'], kind, table, column)


def test_yaml_registry_matches_assignment(assignment_rows, tables):
    by_id = {r['registry_id']: r for r in assignment_rows}
    seen = set()
    for name, t in tables.items():
        for c in t['columns']:
            for rid in c.get('registry', []):
                assert rid not in seen, f'{rid} listed twice in YAML'
                seen.add(rid)
                row = by_id.get(rid)
                assert row is not None, f'{name}.{c["name"]}: {rid} not in TSV'
                assert (row['assignment'], row['core_table'], row['core_column']) == ('column', name, c['name']), rid
    in_tsv = {r['registry_id'] for r in assignment_rows if r['assignment'] == 'column'}
    assert in_tsv == seen, sorted(in_tsv ^ seen)[:10]


def test_match_family_takes_season_from_calendar_not_payload(tables):
    # Split seasons (112): the payload sits under the parent key, the calendar under
    # Apertura/Clausura. Every match table must take the key from the calendar
    # mapping, and the payload joins without source_season_key (review #2016 D1).
    for name, t in tables.items():
        if t['task'] != 2017:
            continue
        frm = _columns(t)['source_season_key']['from']
        assert frm.startswith('derived:') and 'календар' in frm, name
    assert 'БЕЗ source_season_key' in tables['fotmob_matches']['row_source']


def test_match_stat_keys_are_never_null_in_source(tables):
    # top_stats.Shotmap has key=null for every player (D3); stats title rows reuse
    # the data row's key (D2).
    ps = tables['fotmob_match_player_stats']
    assert 'stat_key' not in ps['natural_key'] and 'stat_title' in ps['natural_key']
    assert "type='title'" in tables['fotmob_match_team_stats']['row_source']
