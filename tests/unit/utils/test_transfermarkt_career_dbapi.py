"""Exercise immutable readback with Trino's actual list-row DBAPI contract.

Only SQL execution is replaced by memory storage. Cursor.fetchall/fetchone are
the real driver methods; there is no connection, socket, or production write.
"""
import json
import re
from types import SimpleNamespace

import pandas as pd
import pytest
from trino.dbapi import Cursor

from scrapers.transfermarkt import career_refs


class MemoryTrinoCursor(Cursor):
    def __init__(self):
        self.records = {}
        self._iterator = iter(())

    def execute(self, sql):
        rows = []
        if 'MERGE INTO' in sql:
            table = re.search(r'MERGE INTO (\S+)', sql).group(1)
            values = sql.split('USING (VALUES (', 1)[1].split('))', 1)[0]
            tokens = re.findall(r"'((?:''|[^'])*)'|\b(NULL|[0-9]+)\b", values)
            values = [a.replace("''", "'") if a else None if b == 'NULL' else int(b)
                      for a, b in tokens]
            columns = re.search(r'\)\)\s+s\(([^)]*)\)', sql).group(1).split(',')
            self.records.setdefault(table, dict(zip(map(str.strip, columns), values, strict=True)))
        elif sql.startswith('SELECT') and ' FROM iceberg.ops.' in sql:
            columns, table = sql[7:].split(' FROM ', 1)
            record = self.records.get(table.split()[0])
            rows = [[record[name.strip()] for name in columns.split(',')]] if record else []
        elif sql.startswith('SELECT committed_at'):
            rows = [['2026-10-09 09:05:00']]
        self._iterator = iter(rows)
        return self

    def close(self):
        pass


@pytest.fixture
def database(monkeypatch):
    cursor = MemoryTrinoCursor()
    monkeypatch.setattr(career_refs, 'execute_statement', lambda cur, sql: cur.execute(sql))
    return SimpleNamespace(cursor=lambda: cursor), cursor


@pytest.mark.parametrize('parent', [None, 42])
def test_original_anchor_and_retry_accept_trino_list_rows(database, parent):
    connection, cursor = database
    payload = {'intent_sha256': 'a' * 64,
               'tables': {'transfermarkt_market_value_points': parent},
               'capture_times': {'7': '2026-10-09T09:00:00.000000+00:00'}}
    career_refs.persist_snapshot_anchors(connection, payload)
    original = dict(cursor.records[career_refs.ANCHORS_TABLE])
    career_refs.persist_snapshot_anchors(connection, payload)
    assert cursor.records[career_refs.ANCHORS_TABLE] == original
    assert original['parent_snapshot_id'] == parent
    assert json.loads(original['capture_times_json']) == payload['capture_times']


@pytest.mark.parametrize('field,value', [
    ('parent_snapshot_id', 43), ('parent_snapshot_id', None),
    ('capture_times_json', '{"7":"2026-10-10T09:00:00+00:00"}'),
])
def test_real_anchor_drift_still_fails(database, field, value):
    connection, cursor = database
    payload = {'intent_sha256': 'a' * 64,
               'tables': {'transfermarkt_market_value_points': 42},
               'capture_times': {'7': '2026-10-09T09:00:00.000000+00:00'}}
    cursor.records[career_refs.ANCHORS_TABLE] = {
        'parent_snapshot_id': 42,
        'capture_times_json': json.dumps(payload['capture_times'], sort_keys=True, separators=(',', ':')),
    }
    cursor.records[career_refs.ANCHORS_TABLE][field] = value
    with pytest.raises(RuntimeError, match='immutable career pre-write anchor drifted'):
        career_refs.persist_snapshot_anchors(connection, payload)
    assert cursor.records[career_refs.ANCHORS_TABLE][field] == value


def test_original_refs_and_retry_accept_trino_list_rows(database):
    connection, cursor = database
    frame = pd.DataFrame({'player_id': ['7'], '_batch_id': ['original-batch'],
                          'fetched_at': ['2026-10-09T09:00:00+00:00']})
    kwargs = dict(connection=connection, cycle='original-cycle', entity='market_value_points',
                  table='transfermarkt_market_value_points', frame=frame,
                  snapshot_proof={'native_snapshot_id': 42, 'legacy_snapshot_id': 41})
    proof = career_refs.persist_capture_refs(**kwargs)
    original = dict(cursor.records[career_refs.TABLE])
    replay = career_refs.persist_capture_refs(**{**kwargs, 'snapshot_proof': {
        'native_snapshot_id': 99, 'legacy_snapshot_id': 98}})
    assert replay == proof
    assert proof['native_snapshot_id'] == 42
    assert proof['legacy_snapshot_id'] == 41
    assert proof['capture_times'] == {'7': '2026-10-09T09:00:00.000000+00:00'}
    assert cursor.records[career_refs.TABLE] == original
    cursor.records[career_refs.TABLE]['capture_times_json'] = '{}'
    with pytest.raises(RuntimeError, match='immutable career capture references drifted'):
        career_refs.persist_capture_refs(**kwargs)
