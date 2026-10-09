"""Physical original-batch references for career-cache manifest hydration."""
from __future__ import annotations

import hashlib
import json
from scrapers.transfermarkt.writer import execute_statement

TABLE = 'iceberg.ops.transfermarkt_career_capture_refs_v1'
NATIVE_TABLES = {'transfermarkt_market_value_points', 'transfermarkt_transfer_events'}
LEGACY_TABLES = {'transfermarkt_market_value_points': 'transfermarkt_market_value_history', 'transfermarkt_transfer_events': 'transfermarkt_transfers'}


def _q(value):
    return "'" + str(value).replace("'", "''") + "'"


def persist_capture_refs(connection, cycle, entity, table, frame, *, legacy=False):
    if table not in NATIVE_TABLES or frame is None or frame.empty:
        return {}
    captured = set(frame.attrs.get('tm_captured_player_ids', frame['player_id'].astype(str)))
    original = frame.attrs.get('tm_original_capture_refs', [])
    refs = {(str(item['player_id']), str(item['batch_id'])) for item in original}
    refs.update((str(player), str(batch)) for player, batch in
                frame[['player_id', '_batch_id']].drop_duplicates().itertuples(index=False, name=None)
                if str(player) in captured)
    if not refs or {player for player, _ in refs} != set(frame['player_id'].astype(str)):
        raise RuntimeError('career physical capture references are incomplete')
    counts = frame.groupby('player_id').size()
    payload = json.dumps([[player, batch, int(counts.loc[player])] for player, batch in sorted(refs)], separators=(',', ':'))
    digest = hashlib.sha256(payload.encode()).hexdigest()
    cur = connection.cursor()
    try:
        execute_statement(cur, f"CREATE TABLE IF NOT EXISTS {TABLE} (cycle_id varchar, entity varchar, native_table varchar, refs_json varchar, refs_sha256 varchar, committed_at timestamp(6), snapshot_id bigint, legacy_snapshot_id bigint) WITH (format = 'PARQUET')")
        for field in ('committed_at timestamp(6)', 'snapshot_id bigint', 'legacy_snapshot_id bigint'):
            execute_statement(cur, f'ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS {field}')
        # Recovery may have rewritten equivalent data into a newer snapshot.
        # The immutable original receipt remains the authority if it exists.
        execute_statement(cur, f'SELECT native_table, refs_json, refs_sha256, snapshot_id, legacy_snapshot_id FROM {TABLE} WHERE cycle_id = {_q(cycle)} AND entity = {_q(entity)}')
        existing = cur.fetchall()
        if existing:
            if len(existing) != 1 or tuple(existing[0][:3]) != (table, payload, digest) or not existing[0][3]:
                raise RuntimeError('immutable career capture references drifted')
            return {'native_snapshot_id': int(existing[0][3]), 'legacy_snapshot_id': existing[0][4]}
        def snapshot(target):
            execute_statement(cur, f'SELECT snapshot_id FROM iceberg.bronze."{target}$snapshots" ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1')
            rows = cur.fetchall()
            if len(rows) != 1 or int(rows[0][0]) < 1:
                raise RuntimeError('exact committed career snapshot is unavailable')
            return int(rows[0][0])
        native_snapshot = snapshot(table)
        legacy_snapshot = snapshot(LEGACY_TABLES[table]) if legacy else None
        legacy_sql = 'NULL' if legacy_snapshot is None else str(legacy_snapshot)
        execute_statement(cur, f"""MERGE INTO {TABLE} t USING (VALUES ({_q(cycle)}, {_q(entity)}, {_q(table)}, {_q(payload)}, {_q(digest)}, {native_snapshot}, {legacy_sql}))
            s(cycle_id, entity, native_table, refs_json, refs_sha256, snapshot_id, legacy_snapshot_id)
            ON t.cycle_id = s.cycle_id AND t.entity = s.entity
            WHEN NOT MATCHED THEN INSERT (cycle_id, entity, native_table, refs_json, refs_sha256, committed_at, snapshot_id, legacy_snapshot_id)
            VALUES (s.cycle_id, s.entity, s.native_table, s.refs_json, s.refs_sha256, current_timestamp, s.snapshot_id, s.legacy_snapshot_id)""")
        execute_statement(cur, f'SELECT native_table, refs_json, refs_sha256, snapshot_id, legacy_snapshot_id FROM {TABLE} WHERE cycle_id = {_q(cycle)} AND entity = {_q(entity)}')
        if cur.fetchall() != [(table, payload, digest, native_snapshot, legacy_snapshot)]:
            raise RuntimeError('immutable career capture references drifted')
        return {'native_snapshot_id': native_snapshot, 'legacy_snapshot_id': legacy_snapshot}
    finally:
        cur.close()


def physical_predicate(cur, cycle, entity, table):
    if table not in NATIVE_TABLES:
        return None
    try:
        execute_statement(cur, f'SELECT native_table, refs_json, refs_sha256, snapshot_id, legacy_snapshot_id FROM {TABLE} WHERE cycle_id = {_q(cycle)} AND entity = {_q(entity)}')
        rows = cur.fetchall()
    except Exception as exc:
        if any(token in str(exc).lower() for token in ('table_not_found', 'table not found', 'does not exist')):
            return None  # Existing pre-1399 manifests keep their batch contract.
        raise
    if not rows:
        return None
    if len(rows) != 1:
        raise RuntimeError('duplicate career capture reference rows')
    native, payload, digest, snapshot, legacy_snapshot = rows[0]
    if snapshot is None or int(snapshot) < 1 or (legacy_snapshot is not None and int(legacy_snapshot) < 1):
        raise RuntimeError('career capture reference has no exact retained snapshot')
    if native != table or hashlib.sha256(payload.encode()).hexdigest() != digest:
        raise RuntimeError('career capture reference identity/checksum differs')
    refs = json.loads(payload)
    if not refs or any(not isinstance(item, list) or len(item) != 3 or not all(isinstance(v, str) and v for v in item[:2]) or not isinstance(item[2], int) or item[2] < 1 for item in refs):
        raise RuntimeError('invalid career capture reference pairs')
    predicate = CapturePredicate(snapshot_id=int(snapshot) if snapshot else None, legacy_snapshot_id=int(legacy_snapshot) if legacy_snapshot else None)
    predicate.text = '(' + ' OR '.join(f'(player_id = {_q(player)} AND _batch_id = {_q(batch)})' for player, batch, _count in refs) + ')'
    return predicate


class CapturePredicate:
    def __init__(self, *, snapshot_id, legacy_snapshot_id):
        self.snapshot_id, self.legacy_snapshot_id = snapshot_id, legacy_snapshot_id
        self.text = ''


def apply_physical_refs(sql, *, table, batch, predicate):
    if predicate is None:
        return sql
    marker = f'FROM iceberg.bronze.{table} WHERE _batch_id = {_q(batch)}'
    if marker in sql:
        snapshot = f' FOR VERSION AS OF {predicate.snapshot_id}' if predicate.snapshot_id else ''
        sql = sql.replace(marker, f'FROM iceberg.bronze.{table}{snapshot} WHERE {predicate.text}')
    legacy_table = LEGACY_TABLES[table]
    if predicate.legacy_snapshot_id:
        sql = sql.replace(f'FROM iceberg.bronze.{legacy_table} WHERE',
            f'FROM iceberg.bronze.{legacy_table} FOR VERSION AS OF {predicate.legacy_snapshot_id} WHERE')
    return sql


def retained_bundle_snapshots(connection, cycle, outputs, frames):
    """Verify a fully committed original career bundle without overwriting newer rows."""
    import pandas as pd
    native = next((output for output in outputs if output.table_name in NATIVE_TABLES), None)
    if native is None or frames[native.key].empty:
        return {}
    cursor = connection.cursor()
    try:
        capture = physical_predicate(cursor, cycle, native.key, native.table_name)
        if capture is None:
            return {}
        snapshots = {}
        for output in outputs:
            if output.table_name == native.table_name:
                snapshot = capture.snapshot_id
                expected = frames[output.key].copy()
                original = {item['player_id']: item['batch_id'] for item in expected.attrs.get('tm_original_capture_refs', [])}
                if original:
                    mapped = expected.player_id.astype(str).map(original)
                    expected['_batch_id'] = mapped.fillna(expected['_batch_id'])
                predicate = capture.text
            elif output.table_name == LEGACY_TABLES[native.table_name]:
                snapshot = capture.legacy_snapshot_id
                if snapshot is None:
                    raise RuntimeError('original dual career receipt lacks its legacy snapshot')
                expected = frames[output.key]
                batches = set(expected['_batch_id'].astype(str)) if not expected.empty else set()
                predicate = '_batch_id IN (' + ', '.join(_q(batch) for batch in sorted(batches)) + ')' if batches else 'false'
            else:
                raise RuntimeError('retained career bundle contains an unrelated output')
            execute_statement(cursor, 'SELECT ' + ', '.join(expected.columns) + ' FROM iceberg.bronze.'
                + output.table_name + f' FOR VERSION AS OF {snapshot} WHERE ' + predicate)
            actual = pd.DataFrame(cursor.fetchall(), columns=expected.columns)
            from dags.utils.transfermarkt_current_write import _rows
            if _rows(actual) != _rows(expected):
                raise RuntimeError('original career snapshot no longer proves exact business and lineage')
            snapshots[output.key] = snapshot
        return snapshots
    finally:
        cursor.close()
