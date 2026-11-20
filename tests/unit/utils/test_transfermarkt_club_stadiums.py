import hashlib
import json
import re
import sqlite3
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from dags.utils import transfermarkt_club_stadiums as writer
from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
from tests.unit.utils.test_transfermarkt_current_write import PREFLIGHT


AT = datetime(2025, 9, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)
SCOPE = {'scope_id': 'example-scope', 'source_competition_id': 'EX',
         'source_edition_id': '2025'}


def _sqlite_sql(sql):
    sql = sql.replace('iceberg.bronze.', '').replace('iceberg.ops.', '')
    return re.sub(r"\b(?:TIMESTAMP|DATE)\s+('.*?')", r'\1', sql, flags=re.I)


def _param(value):
    if isinstance(value, datetime):
        return writer._clock(value).isoformat(sep=' ', timespec='microseconds')
    if pd.isna(value):
        return None
    return value


class _Store:
    def __init__(self):
        self.captures = {}
        self.reads = []

    def outcome(self, url, payload, at, code):
        body = json.dumps(payload).encode()
        digest = hashlib.sha256(body).hexdigest()
        capture_id = hashlib.sha256((url + digest + at.isoformat() + str(code)).encode()).hexdigest()
        manifest = {'capture_id': capture_id, 'url': url, 'status_code': code,
                    'content_hash': digest, 'fetched_at': at.isoformat(), 'cycle_id': 'test',
                    'scope_id': SCOPE['scope_id']}
        self.captures[capture_id] = SimpleNamespace(manifest=manifest, body=body)
        return FetchOutcome(status=FetchStatus.OK if code == 200 else FetchStatus.RETRY_EXHAUSTED,
                            status_code=code, value=payload, raw_capture_id=capture_id,
                            raw_body_hash=digest, raw_fetched_at=at.isoformat())

    def load_capture(self, capture_id):
        self.reads.append(capture_id)
        return self.captures[capture_id]


class _Database:
    def __init__(self, path):
        self.path = path
        self.sql = []
        self.writes = []
        self.fail_table = None
        self.drop_table_write = None

    def connect(self):
        return sqlite3.connect(self.path, isolation_level=None)

    def execute(self, connection, sql, params=None, fetch=False):
        self.sql.append(sql)
        cursor = connection.execute(_sqlite_sql(sql), [_param(v) for v in params or ()])
        return cursor.fetchall() if fetch else None

    def query(self, connection, sql, params=None):
        self.sql.append(sql)
        return pd.read_sql_query(_sqlite_sql(sql), connection,
                                 params=[_param(v) for v in params or ()])

    def write_dataframe(self, *, df, database, table, merge_keys, add_metadata, source):
        qualified = f'iceberg.{database}.{table}'
        self.writes.append(qualified)
        assert tuple(merge_keys) == writer.KEYS[qualified]
        assert add_metadata is False and source == 'transfermarkt'
        assert list(df.columns) == list(writer.SCHEMAS[qualified])
        if qualified == self.fail_table:
            raise IOError('injected merge failure')
        if qualified == self.drop_table_write:
            return
        connection = self.connect()
        try:
            keys = ', '.join(merge_keys)
            connection.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS ix_{table} ON {table} ({keys})')
            columns = ', '.join(df.columns)
            params = ', '.join('?' for _ in df.columns)
            assignments = ', '.join(f'{col} = excluded.{col}' for col in df if col not in merge_keys)
            sql = (f'INSERT INTO {table} ({columns}) VALUES ({params}) '
                   f'ON CONFLICT ({keys}) DO UPDATE SET {assignments}')
            for row in df.itertuples(index=False, name=None):
                connection.execute(sql, [_param(value) for value in row])
        finally:
            connection.close()

    def rows(self, table):
        connection = self.connect()
        try:
            return self.query(connection, f'SELECT * FROM {table}').to_dict('records')
        finally:
            connection.close()

    def change(self, sql):
        connection = self.connect()
        try:
            self.execute(connection, sql)
        finally:
            connection.close()


@pytest.fixture
def env(tmp_path, monkeypatch):
    db = _Database(tmp_path / 'warehouse.sqlite')
    store = _Store()
    policy = tmp_path / 'policy.json'
    policy.write_text(json.dumps({'allowed_write_tables': list(writer.KEYS)}))
    monkeypatch.setenv('TM_STANDING_POLICY_PATH', str(policy))
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    monkeypatch.setattr(writer, 'load_standing_policy', lambda path: json.loads(policy.read_text()))
    monkeypatch.setattr(writer.current, '_scope', lambda scraper, scope: dict(scope))
    monkeypatch.setattr(writer.current, '_authorize', lambda preflight: preflight == PREFLIGHT)
    monkeypatch.setattr(writer.current.run, '_execute_cursor', db.execute)
    monkeypatch.setattr(writer.current.run, '_query_dataframe', db.query)
    instance = SimpleNamespace(_http_client=SimpleNamespace(_raw_store=store),
                               _bronze_connection=db.connect, _iceberg_writer=db)
    return SimpleNamespace(db=db, store=store, scraper=instance, policy=policy)


def _inputs(env, *, club='17', venue='701', at=AT, code=200, name='Example FC',
            stadium_name='Example Ground'):
    profile_url = f'https://tmapi.transfermarkt.technology/club/{club}'
    stadium_url = profile_url + '/stadium'
    profile = {'success': True, 'data': {
        'id': club, 'name': name, 'relativeUrl': '/example', 'crestUrl': 'example.png',
        'preferences': {'clubCode': 'EFC'},
        'baseDetails': {'shortName': 'Example', 'abbreviation': 'EX', 'isNationalTeam': False,
                        'superiorClub': {'name': name, 'colors': {'primary': 'red'},
                                         'location': {'street': ' Road ', 'postcode': '01',
                                                      'city': 'City', 'countryId': '4'}}},
    }}
    ground = {'name': stadium_name, 'capacity': 12345}
    if venue is not None:
        ground['id'] = venue
    stadium = {'success': True, 'data': {**ground, 'clubId': club, 'stadium': dict(ground)}}
    if code != 200:
        stadium = {'success': False}
    return {
        'club_id': club, 'profile_url': profile_url, 'stadium_url': stadium_url,
        'profile_outcome': env.store.outcome(profile_url, profile, at, 200),
        'stadium_outcome': env.store.outcome(stadium_url, stadium, at + timedelta(seconds=4), code),
        'country_names': {'4': 'Exampleland'}, 'universe_version': 'universe-1', 'run_id': 'run-1',
    }


def _write(env, inputs):
    return writer.write_current_club_stadium(env.scraper, SCOPE, PREFLIGHT, 'test', **inputs)


def test_full_profile_stadium_relation_and_receipt(env):
    inputs = _inputs(env)
    result = _write(env, inputs)
    assert result['verified'] is True
    assert env.db.writes == list(writer.KEYS)
    profile, = env.db.rows(writer.CLUB_PROFILE_TABLE)
    venue, = env.db.rows(writer.STADIUM_TABLE)
    relation, = env.db.rows(writer.RELATION_TABLE)
    receipt, = env.db.rows(writer.RECEIPTS_TABLE)
    expected = writer.parser.parse_club_profile(
        inputs['profile_outcome'].value, club_id='17', observed_at=AT.isoformat(),
        country_names=inputs['country_names']).as_dict()
    assert json.loads(profile['canonical_json']) == expected
    for column in writer.PROFILE_DATA:
        value = json.loads(profile[column]) if column == 'colors' else profile[column]
        assert value == expected[column]
    parsed = writer.parser.parse_club_stadium(inputs['stadium_outcome'].value, club_id='17',
                                              observed_at=(AT + timedelta(seconds=4)).isoformat(),
                                              country_names=inputs['country_names'])
    for row, model in [(venue, parsed.stadium), (relation, parsed.relation)]:
        for column, value in asdict(model).items():
            if column not in {*writer.LINEAGE, *writer.VENUE_KEYS, 'status', 'valid_from', 'valid_to'}:
                if isinstance(value, (list, tuple, dict)):
                    value = writer._json(value)
                assert row[column] == value, column
        assert row['stadium_observation_key'] == 'stadium:701' and row['stadium_id'] == '701'
    assert relation['valid_from'] is relation['valid_to'] is None
    for table, row, endpoint in [(writer.CLUB_PROFILE_TABLE, profile, 'profile'),
                                 (writer.STADIUM_TABLE, venue, 'stadium'),
                                 (writer.RELATION_TABLE, relation, 'stadium')]:
        outcome = inputs[endpoint + '_outcome']
        assert row['raw_capture_id'] == outcome.raw_capture_id
        assert row['raw_body_hash'] == outcome.raw_body_hash
        assert row['source_url'] == inputs[endpoint + '_url']
        assert writer._clock(row['observed_at']) == writer._clock(outcome.raw_fetched_at)
        assert row['captured_at'] == row['observed_at']
        assert row['extracted_at'] == row['_ingested_at'] == venue['observed_at']
        assert row['scope_id'] == SCOPE['scope_id'] and row['cycle_id'] == 'test'
        assert row['source_competition_id'] == 'EX' and row['source_edition_id'] == '2025'
        assert row['run_id'] == 'run-1' and row['universe_version'] == 'universe-1'
        assert row['parser_revision'] == writer.parser.PARSER_REVISION
        assert row['schema_revision'] == writer.parser.SCHEMA_REVISION
        assert row['_source'] == 'transfermarkt' and row['_entity_type'] == writer.ENTITY[table]
        assert row['_batch_id'] == receipt['batch_id']
        for leg in ('profile', 'stadium'):
            assert row[leg + '_raw_capture_id'] == inputs[leg + '_outcome'].raw_capture_id
            assert row[leg + '_raw_body_hash'] == inputs[leg + '_outcome'].raw_body_hash
        assert set(json.loads(receipt['key_maps_json'])[table][0]) == set(writer.KEYS[table])
    assert [receipt['expected_' + key + '_count'] for key in
            ('profile', 'stadium', 'relation', 'present', 'empty')] == [1, 1, 1, 1, 0]
    assert all(receipt[col] for col in ('profile_physical_hash', 'stadium_physical_hash',
                                        'relation_physical_hash', 'receipt_sha256', 'plan_sha256'))
    assert all('delete' not in sql.lower() and 'coach' not in sql.lower() for sql in env.db.sql)


def test_empty_requires_durable_404_and_profile(env):
    inputs = _inputs(env, code=404)
    receipt = _write(env, inputs)
    assert receipt['stadium_status'] == 'observed_empty'
    assert json.loads(receipt['empty_proof_json']) == {
        'profile_raw_capture_id': inputs['profile_outcome'].raw_capture_id,
        'profile_status_code': 200, 'stadium_raw_capture_id': inputs['stadium_outcome'].raw_capture_id,
        'stadium_status_code': 404,
    }
    assert receipt['expected_empty_count'] == 1
    assert receipt['expected_stadium_count'] == receipt['expected_relation_count'] == 0
    assert env.db.writes == [writer.CLUB_PROFILE_TABLE, writer.RECEIPTS_TABLE]
    for table in (writer.STADIUM_TABLE, writer.RELATION_TABLE):
        assert env.db.rows(table) == []
        assert any(table in sql and '_batch_id = ?' in sql for sql in env.db.sql)


@pytest.mark.parametrize('code', [204, 301, 403, 429, 500])
def test_other_stadium_status_never_confirms_empty(env, code):
    with pytest.raises(ValueError):
        _write(env, _inputs(env, code=code))
    assert not env.db.sql and not env.db.writes


@pytest.mark.parametrize('defect', ['missing_raw', 'url', 'scope_id', 'cycle_id', 'status_code',
                                     'content_hash', 'capture_id', 'fetched_at', 'body', 'decoded',
                                     'bad_profile', 'query_url'])
def test_raw_proof_rejects_mismatches_before_sql(env, defect):
    inputs = _inputs(env, code=404)
    raw = env.store.captures[inputs['stadium_outcome'].raw_capture_id]
    if defect == 'missing_raw':
        del env.store.captures[inputs['stadium_outcome'].raw_capture_id]
    elif defect == 'body':
        raw.body = b'wrong'
    elif defect == 'decoded':
        inputs['profile_outcome'].value['data']['name'] = 'changed'
    elif defect == 'bad_profile':
        inputs['profile_outcome'] = env.store.outcome(inputs['profile_url'], {}, AT, 403)
    elif defect == 'query_url':
        inputs['stadium_url'] += '?test=1'
    else:
        raw.manifest[defect] = 'wrong'
    with pytest.raises((KeyError, ValueError)):
        _write(env, inputs)
    assert not env.db.sql and not env.db.writes


def test_unidentified_and_shared_venues_keep_club_grain(env):
    for club, venue in [('1', None), ('2', None), ('3', '701'), ('4', '701')]:
        _write(env, _inputs(env, club=club, venue=venue))
    for table in (writer.STADIUM_TABLE, writer.RELATION_TABLE):
        assert {(row['club_id'], row['stadium_observation_key'],
                 None if pd.isna(row['stadium_id']) else row['stadium_id'])
                for row in env.db.rows(table)} == {
                    ('1', 'club:1:unidentified', None), ('2', 'club:2:unidentified', None),
                    ('3', 'stadium:701', '701'), ('4', 'stadium:701', '701'),
                }


def test_replay_and_later_append(env):
    inputs = _inputs(env)
    first = _write(env, inputs)
    assert _write(env, inputs) == first
    assert env.db.writes == list(writer.KEYS)
    second = _write(env, _inputs(env, at=AT + timedelta(days=1)))
    assert second['receipt_id'] != first['receipt_id'] and second['batch_id'] != first['batch_id']
    assert all(len(env.db.rows(table)) == 2 for table in writer.KEYS)


@pytest.mark.parametrize('changed', ['older', 'profile', 'stadium'])
def test_stale_and_equal_clock_hash_conflict(env, changed):
    _write(env, _inputs(env))
    changes = {'older': {'at': AT - timedelta(seconds=1)}, 'profile': {'name': 'Different'},
               'stadium': {'stadium_name': 'Different'}}
    with pytest.raises(ValueError, match='stale|conflicting'):
        _write(env, _inputs(env, **changes[changed]))
    assert env.db.writes == list(writer.KEYS)


@pytest.mark.parametrize('denied', list(writer.KEYS))
def test_policy_denies_each_target_before_raw_or_sql(env, denied):
    env.policy.write_text(json.dumps({'allowed_write_tables': [t for t in writer.KEYS if t != denied]}))
    with pytest.raises(writer.current.CurrentWriteError):
        _write(env, _inputs(env))
    assert not env.store.reads and not env.db.sql and not env.db.writes


def test_revoked_policy_after_raw_read_prevents_ddl(env, monkeypatch):
    inputs = _inputs(env, code=404)
    load = env.store.load_capture

    def revoke(capture_id):
        raw = load(capture_id)
        if capture_id == inputs['stadium_outcome'].raw_capture_id:
            env.policy.write_text(json.dumps({'allowed_write_tables': []}))
        return raw

    monkeypatch.setattr(env.store, 'load_capture', revoke)
    with pytest.raises(writer.current.CurrentWriteError):
        _write(env, inputs)
    assert not env.db.sql and not env.db.writes


@pytest.mark.parametrize('after', [writer.CLUB_PROFILE_TABLE, writer.RELATION_TABLE])
def test_policy_rechecked_before_each_merge(env, monkeypatch, after):
    merge = env.db.write_dataframe

    def revoke(**kwargs):
        merge(**kwargs)
        if env.db.writes[-1] == after:
            env.policy.write_text(json.dumps({'allowed_write_tables': []}))

    monkeypatch.setattr(env.db, 'write_dataframe', revoke)
    with pytest.raises(writer.current.CurrentWriteError):
        _write(env, _inputs(env))
    assert env.db.writes[-1] == after and not env.db.rows(writer.RECEIPTS_TABLE)


@pytest.mark.parametrize('failed', list(writer.KEYS))
def test_failed_merge_has_no_ack_and_same_plan_recovers(env, failed):
    inputs = _inputs(env)
    env.db.fail_table = failed
    with pytest.raises(IOError, match='merge failure'):
        _write(env, inputs)
    assert not env.db.rows(writer.RECEIPTS_TABLE)
    env.db.fail_table = None
    assert _write(env, inputs)['verified'] is True
    assert all(len(env.db.rows(table)) == 1 for table in writer.KEYS)


@pytest.mark.parametrize('failed', list(writer.KEYS))
def test_successful_writer_return_without_physical_rows_has_no_ack(env, failed):
    env.db.drop_table_write = failed
    with pytest.raises(ValueError, match='readback'):
        _write(env, _inputs(env))
    assert not env.db.rows(writer.RECEIPTS_TABLE)


@pytest.mark.parametrize(('table', 'change'), [
    (writer.CLUB_PROFILE_TABLE, "name = 'changed'"),
    (writer.STADIUM_TABLE, "canonical_json = '{}'"),
    (writer.RELATION_TABLE, "stadium_observation_key = 'wrong'"),
    (writer.RECEIPTS_TABLE, "run_id = 'wrong'"),
    (writer.RECEIPTS_TABLE, "committed_at = '2024-01-01 00:00:00'"),
])
def test_receipt_replay_detects_physical_tampering_without_repair(env, table, change):
    inputs = _inputs(env)
    _write(env, inputs)
    env.db.change(f'UPDATE {table} SET {change}')
    with pytest.raises(ValueError, match='readback'):
        _write(env, inputs)
    assert env.db.writes == list(writer.KEYS)


def test_confirmed_empty_replay_detects_unexpected_business_rows(env):
    _write(env, _inputs(env))
    empty = _inputs(env, code=404, at=AT + timedelta(days=1))
    receipt = _write(env, empty)
    batch = receipt['batch_id']
    env.db.change(f"UPDATE {writer.STADIUM_TABLE} SET _batch_id = '{batch}'")
    with pytest.raises(ValueError, match='batch physical readback'):
        _write(env, empty)


def test_receipt_read_error_after_commit_requires_verified_retry(env, monkeypatch):
    query = env.db.query

    def interrupted(connection, sql, params=None):
        if writer.RECEIPTS_TABLE in env.db.writes and writer.RECEIPTS_TABLE in sql:
            raise IOError('injected receipt read error')
        return query(connection, sql, params)

    inputs = _inputs(env)
    with monkeypatch.context() as patch:
        patch.setattr(writer.current.run, '_query_dataframe', interrupted)
        with pytest.raises(IOError, match='receipt read error'):
            _write(env, inputs)
    assert len(env.db.rows(writer.RECEIPTS_TABLE)) == 1
    assert _write(env, inputs)['verified'] is True
    assert env.db.writes == list(writer.KEYS)


def test_policy_contract_uses_real_standing_policy_field():
    policy = SimpleNamespace(allowed_write_tables=set(writer.KEYS))
    writer.validate_club_stadium_write_policy(policy)
    policy.allowed_write_tables.remove(writer.RECEIPTS_TABLE)
    with pytest.raises(writer.current.CurrentWriteError, match='authorize club stadium'):
        writer.validate_club_stadium_write_policy(policy)
