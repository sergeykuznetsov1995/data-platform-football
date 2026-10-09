"""Original cached physical receipts across partial captures and current replacement."""
import hashlib
import json
import re
import sqlite3
from types import SimpleNamespace

import pandas as pd
import pytest

from dags.utils import transfermarkt_backfill_dq as dq
from scrapers.transfermarkt import career_refs
from scrapers.transfermarkt.raw_store import RawResponseStore


def test_two_units_keep_same_logical_child_and_immutable_original_refs(monkeypatch):
    records = {}
    class Cursor:
        rows = []
        def fetchall(self): return self.rows
        def close(self): pass
    cur = Cursor()
    def execute(cur, sql):
        if sql.startswith('SELECT native_table'):
            batch = re.search(r"native_batch_id = '([^']+)'", sql).group(1)
            cur.rows = [records[batch]] if batch in records else []
        elif sql.startswith('SELECT snapshot_id'):
            cur.rows = [(10,)]
        elif sql.lstrip().startswith('MERGE INTO'):
            values = sql.split('USING (VALUES (', 1)[1].split('))', 1)[0]
            tokens = re.findall(r"'((?:''|[^'])*)'|\b(NULL|[0-9]+)\b", values)
            values = [a.replace("''", "'") if a else None if b == 'NULL' else int(b) for a, b in tokens]
            cycle, entity, table, refs, digest, snapshot, legacy, batch, unit, empty_proof = values
            records[batch] = (table, refs, digest, snapshot, legacy, empty_proof)
    monkeypatch.setattr(career_refs, 'execute_statement', execute)
    conn = SimpleNamespace(cursor=lambda: cur)
    first = pd.DataFrame({'player_id': ['1', '2'], '_batch_id': ['portion-a', 'portion-a']})
    first.attrs.update(tm_captured_player_ids=['2'], tm_original_capture_refs=[{'player_id': '1', 'batch_id': 'current-original'}])
    second = pd.DataFrame({'player_id': ['1', '2', '3'], '_batch_id': ['portion-b'] * 3})
    second.attrs.update(tm_captured_player_ids=['3'], tm_original_capture_refs=[
        {'player_id': '1', 'batch_id': 'current-original'}, {'player_id': '2', 'batch_id': 'portion-a'}])
    kwargs = dict(connection=conn, cycle='stable-child', entity='market_value_points',
                  table='transfermarkt_market_value_points')
    old = career_refs.persist_capture_refs(frame=first, **kwargs)
    fresh = career_refs.persist_capture_refs(frame=second, **kwargs)
    assert old['capture_unit_id'] != fresh['capture_unit_id']
    assert len(records) == 2
    assert fresh['physical_refs'] == [['1', 'current-original', 1], ['2', 'portion-a', 1], ['3', 'portion-b', 1]]
    # A fresh current replaces live rows; replay still resolves the original proof.
    replay = career_refs.persist_capture_refs(frame=first, snapshot_proof={'native_snapshot_id': 99, 'legacy_snapshot_id': None}, **kwargs)
    assert replay == old and replay['native_snapshot_id'] == 10
    empty = pd.DataFrame(columns=['player_id', '_batch_id'])
    typed = [{'player_id': '4', 'status': 'authoritative_empty', 'expected_rows': 0,
              'captured_at': '2026-10-08T09:00:00+00:00', 'payload_hash': 'b' * 64,
              'intent_sha256': 'c' * 64, 'raw_attempts': [], 'cache_sources': []}]
    empty_kwargs = dict(frame=empty, empty_refs=typed, batch_id='portion-empty', **kwargs)
    original_empty = career_refs.persist_capture_refs(snapshot_proof={'native_snapshot_id': None, 'legacy_snapshot_id': None}, **empty_kwargs)
    # Later current created a target snapshot. Original absence must not restamp.
    replay_empty = career_refs.persist_capture_refs(snapshot_proof={'native_snapshot_id': 99, 'legacy_snapshot_id': None}, **empty_kwargs)
    assert replay_empty == original_empty
    assert replay_empty['native_snapshot_id'] is None
    assert replay_empty['physical_refs'] == [['4', 'portion-empty', 0]]


def test_mixed_cached_and_fresh_two_portions_verify_original_snapshots_and_external_raw(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    db = sqlite3.connect(':memory:')
    business = []
    physical = []
    envelopes = []
    for pid in ('1', '2', '3', '4'):
        scope, cycle = ('current-original', 'current-cycle') if pid == '1' else ('history', 'stable-child')
        capture = store.store_attempt(f'https://www.transfermarkt.com/player/{pid}', ('original-' + pid).encode(), 200, {},
                                      '2026-10-09T09:00:00+00:00', cycle, scope, 'market_value_points', 1)
        row = (pid, '2020-01-01', int(pid) * 100, 'Club', 20, 'EUR100')
        business.append(row)
        batch = 'current-batch' if pid == '1' else 'portion-a' if pid == '2' else 'portion-b'
        physical.append((*row, batch, capture.capture_id, capture.content_hash, scope, cycle))
        if pid != '1':
            envelopes.append(store.store_response_envelope(capture))
    columns = 'player_id text,mv_date text,value_eur int,club_name text,age int,mv_raw text,_batch_id text,raw_capture_id text,source_body_hash text,scope_id text,cycle_id text'
    for snapshot, rows in ((10, physical[:2]), (20, physical), (99, [('1', '2026-10-09', 999, 'NEW', 26, 'EUR999', 'new-current', 'x', 'y', 'current', 'new-cycle')])):
        db.execute(f'CREATE TABLE snapshot_{snapshot} ({columns})')
        db.executemany(f'INSERT INTO snapshot_{snapshot} VALUES (?,?,?,?,?,?,?,?,?,?,?)', rows)
    class Cursor:
        sql = []
        rows = []
        def execute(self, sql):
            self.sql.append(sql)
            if 'transfermarkt_market_value_points FOR VERSION AS OF' in sql:
                sql = re.sub(r'iceberg\.bronze\.transfermarkt_market_value_points FOR VERSION AS OF ([0-9]+)', r'snapshot_\1', sql)
                self.rows = db.execute(sql).fetchall()
            else:
                self.rows = []
        def fetchall(self): return self.rows
    cur = Cursor()
    receipts = []
    for snapshot, rows in ((10, physical[:2]), (20, physical)):
        receipts.append({'snapshot_id': snapshot, 'player_ids': [row[0] for row in rows],
            'physical_refs': [[row[0], row[6], 1] for row in rows], 'row_count': len(rows),
            'key_hash': dq._fingerprint_rows([row[:6] for row in rows])[1],
            'capture_unit_id': hashlib.sha256(str(snapshot).encode()).hexdigest(),
            'native_batch_id': 'portion-a' if snapshot == 10 else 'portion-b', 'scope_id': 'history', 'cycle_id': 'stable-child'})
    assert dq.historical_career_rows(cur, 'market_value_points', receipts) == business
    assert all('cycle_id =' not in sql for sql in cur.sql)
    manifest = SimpleNamespace(dq_evidence={'historical_career_receipts': {'market_value_points': receipts}})
    lineage = dq.verify_raw_lineage(cur, pins={table: 99 for table in dq.BACKFILL_PIN_TABLES},
        child_cycle_ids=['stable-child'], raw_store=store, attempt_envelopes=envelopes,
        manifest_scope_cycles=[('history', 'stable-child')], scope_statuses={'history': 'complete'}, manifests=[manifest])
    assert lineage['capture_count'] == 4 and lineage['partial_capture_count'] == 0
    db.close()


def test_original_ref_per_player_count_cannot_be_replaced_by_another_player():
    rows = [('1', '2020-01-01', 100, 'Club', 20, 'EUR100'), ('1', '2020-02-01', 200, 'Club', 20, 'EUR200')]
    receipt = {'snapshot_id': 10, 'player_ids': ['1', '2'], 'physical_refs': [['1', 'a', 1], ['2', 'b', 1]],
               'row_count': 2, 'key_hash': dq._fingerprint_rows(rows)[1]}
    cur = SimpleNamespace(execute=lambda _: None, fetchall=lambda: rows)
    with pytest.raises(dq.BackfillDqError):
        dq.historical_career_rows(cur, 'market_value_points', [receipt])


def test_original_empty_without_table_survives_new_current_rows(tmp_path):
    from dataclasses import asdict
    from unittest.mock import Mock
    from scrapers.transfermarkt.client import _payload_hash
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    original = {'transfers': []}
    capture = store.store_attempt('https://www.transfermarkt.com/ceapi/transferHistory/list/1',
        json.dumps(original).encode(), 200, {}, '2026-10-08T09:00:00+00:00',
        'original-cycle', 'original-scope', 'transfer_events', 1)
    envelope = store.store_response_envelope(capture)
    proof = {'player_id': '1', 'status': 'authoritative_empty', 'expected_rows': 0,
             'captured_at': capture.fetched_at, 'payload_hash': _payload_hash(original),
             'intent_sha256': 'a' * 64, 'raw_attempts': [asdict(envelope)], 'cache_sources': []}
    receipt = {'snapshot_id': None, 'player_ids': ['1'], 'physical_refs': [['1', 'original-batch', 0]],
               'row_count': 0, 'key_hash': dq._fingerprint_rows([])[1], 'empty_capture_refs': [proof]}
    # Current has since created the table and installed a real nonempty career.
    db = sqlite3.connect(':memory:')
    db.execute('CREATE TABLE current_transfers(player_id text, fee_eur int)')
    db.execute("INSERT INTO current_transfers VALUES ('1', 1000000)")
    cur = Mock()
    assert dq.historical_career_rows(cur, 'transfer_events', [receipt]) == []
    cur.execute.assert_not_called()
    dq._verify_original_empty_careers(store, 'transfer_events', receipt)
    assert db.execute('SELECT * FROM current_transfers').fetchall() == [('1', 1000000)]
    proof['captured_at'] = '2026-10-09T09:00:00+00:00'
    with pytest.raises(dq.BackfillDqError, match='cannot be verified'):
        dq._verify_original_empty_careers(store, 'transfer_events', receipt)
    db.close()
