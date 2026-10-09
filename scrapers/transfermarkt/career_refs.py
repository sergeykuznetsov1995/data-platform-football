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


def persist_capture_refs(connection, cycle, entity, table, frame, *, legacy=False, snapshot_proof=None, empty_refs=None, batch_id=None):
    if table not in NATIVE_TABLES or frame is None:
        return {}
    empty_refs = empty_refs or []
    if frame.empty and not empty_refs:
        return {}
    batches = sorted(set(frame['_batch_id'].astype(str)))
    if frame.empty and batch_id:
        batches = [str(batch_id)]
    if len(batches) != 1:
        raise RuntimeError('career capture unit has ambiguous logical batch')
    logical_batch = batches[0]
    captured = set(frame.attrs.get('tm_captured_player_ids', frame['player_id'].astype(str)))
    original = frame.attrs.get('tm_original_capture_refs', [])
    refs = {(str(item['player_id']), str(item['batch_id'])) for item in original}
    refs.update((str(player), str(batch)) for player, batch in
                frame[['player_id', '_batch_id']].drop_duplicates().itertuples(index=False, name=None)
                if str(player) in captured)
    if not frame.empty and (not refs or {player for player, _ in refs} != set(frame['player_id'].astype(str))):
        raise RuntimeError('career physical capture references are incomplete')
    counts = frame.groupby('player_id').size()
    physical_refs = [[player, batch, int(counts.loc[player])] for player, batch in sorted(refs)]
    physical_refs += [[str(item['player_id']), logical_batch, 0] for item in empty_refs]
    payload = json.dumps(sorted(physical_refs), separators=(',', ':'))
    empty_proof = json.dumps(empty_refs, sort_keys=True, separators=(',', ':'))
    import pandas as pd
    clocks = {}
    if not frame.empty:
        column = 'fetched_at' if 'fetched_at' in frame else '_ingested_at'
        for player, rows in frame.groupby('player_id'):
            times = pd.to_datetime(rows[column], utc=True)
            if times.isna().any():
                raise RuntimeError('career reference lacks its original source clock')
            clocks[str(player)] = times.max().isoformat(timespec='microseconds')
    clocks.update({str(item['player_id']): item['captured_at'] for item in empty_refs})
    capture_times = json.dumps(clocks, sort_keys=True, separators=(',', ':'))
    digest = hashlib.sha256(payload.encode()).hexdigest()
    unit_id = hashlib.sha256((str(cycle) + '\0' + entity + '\0' + logical_batch + '\0' + digest + '\0' + empty_proof + '\0' + capture_times).encode()).hexdigest()
    cur = connection.cursor()
    try:
        execute_statement(cur, f"CREATE TABLE IF NOT EXISTS {TABLE} (cycle_id varchar, entity varchar, native_table varchar, refs_json varchar, refs_sha256 varchar, committed_at timestamp(6), snapshot_id bigint, legacy_snapshot_id bigint, native_batch_id varchar, capture_unit_id varchar, empty_proof_json varchar, capture_times_json varchar) WITH (format = 'PARQUET')")
        for field in ('committed_at timestamp(6)', 'snapshot_id bigint', 'legacy_snapshot_id bigint', 'native_batch_id varchar', 'capture_unit_id varchar', 'empty_proof_json varchar', 'capture_times_json varchar'):
            execute_statement(cur, f'ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS {field}')
        # Recovery may have rewritten equivalent data into a newer snapshot.
        # The immutable original receipt remains the authority if it exists.
        execute_statement(cur, f'SELECT native_table, refs_json, refs_sha256, snapshot_id, legacy_snapshot_id, empty_proof_json, capture_times_json FROM {TABLE} WHERE cycle_id = {_q(cycle)} AND entity = {_q(entity)} AND native_batch_id = {_q(logical_batch)}')
        existing = cur.fetchall()
        if existing:
            if len(existing) != 1 or tuple(existing[0][:3]) != (table, payload, digest) or existing[0][5] != empty_proof or existing[0][6] != capture_times or (not existing[0][3] and not frame.empty):
                raise RuntimeError('immutable career capture references drifted')
            return {'native_snapshot_id': int(existing[0][3]) if existing[0][3] else None, 'legacy_snapshot_id': existing[0][4], 'capture_unit_id': unit_id, 'physical_refs': json.loads(payload), 'empty_capture_refs': empty_refs, 'capture_times': clocks}
        def snapshot(target):
            try:
                execute_statement(cur, f'SELECT snapshot_id FROM iceberg.bronze."{target}$snapshots" ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1')
                rows = cur.fetchall()
            except Exception as exc:
                if frame.empty and any(token in str(exc).lower() for token in ('table_not_found', 'table not found', 'does not exist')):
                    return None
                raise
            if not rows and frame.empty:
                return None
            if len(rows) != 1 or int(rows[0][0]) < 1:
                raise RuntimeError('exact committed career snapshot is unavailable')
            return int(rows[0][0])
        native_snapshot = snapshot_proof['native_snapshot_id'] if snapshot_proof else snapshot(table)
        legacy_snapshot = snapshot_proof['legacy_snapshot_id'] if snapshot_proof else snapshot(LEGACY_TABLES[table]) if legacy else None
        legacy_sql = 'NULL' if legacy_snapshot is None else str(legacy_snapshot)
        # The Bronze commit clock is immutable even if ops reconciles later.
        # It breaks equal source-clock ties when the same captured raw body
        # legitimately commits another batch; ops retry time must not win.
        if native_snapshot is not None:
            execute_statement(cur, f'SELECT committed_at FROM iceberg.bronze."{table}$snapshots" WHERE snapshot_id = {native_snapshot}')
            committed_rows = cur.fetchall()
            if len(committed_rows) != 1:
                raise RuntimeError('original career snapshot commit clock is unavailable')
            committed = pd.to_datetime(committed_rows[0][0], utc=True)
        else:
            committed = max(pd.to_datetime(stamp, utc=True) for stamp in clocks.values())
        commit_sql = "TIMESTAMP " + _q(committed.tz_localize(None).isoformat(sep=' ', timespec='microseconds'))
        execute_statement(cur, f"""MERGE INTO {TABLE} t USING (VALUES ({_q(cycle)}, {_q(entity)}, {_q(table)}, {_q(payload)}, {_q(digest)}, {'NULL' if native_snapshot is None else native_snapshot}, {legacy_sql}, {_q(logical_batch)}, {_q(unit_id)}, {_q(empty_proof)}, {_q(capture_times)}))
            s(cycle_id, entity, native_table, refs_json, refs_sha256, snapshot_id, legacy_snapshot_id, native_batch_id, capture_unit_id, empty_proof_json, capture_times_json)
            ON t.cycle_id = s.cycle_id AND t.entity = s.entity AND t.native_batch_id = s.native_batch_id
            WHEN NOT MATCHED THEN INSERT (cycle_id, entity, native_table, refs_json, refs_sha256, committed_at, snapshot_id, legacy_snapshot_id, native_batch_id, capture_unit_id, empty_proof_json, capture_times_json)
            VALUES (s.cycle_id, s.entity, s.native_table, s.refs_json, s.refs_sha256, {commit_sql}, s.snapshot_id, s.legacy_snapshot_id, s.native_batch_id, s.capture_unit_id, s.empty_proof_json, s.capture_times_json)""")
        execute_statement(cur, f'SELECT native_table, refs_json, refs_sha256, snapshot_id, legacy_snapshot_id, empty_proof_json, capture_times_json FROM {TABLE} WHERE cycle_id = {_q(cycle)} AND entity = {_q(entity)} AND native_batch_id = {_q(logical_batch)}')
        if cur.fetchall() != [(table, payload, digest, native_snapshot, legacy_snapshot, empty_proof, capture_times)]:
            raise RuntimeError('immutable career capture references drifted')
        return {'native_snapshot_id': native_snapshot, 'legacy_snapshot_id': legacy_snapshot, 'capture_unit_id': unit_id, 'physical_refs': json.loads(payload), 'empty_capture_refs': empty_refs, 'capture_times': clocks}
    finally:
        cur.close()


def physical_predicate(cur, cycle, entity, table, *, batch_id=None):
    if table not in NATIVE_TABLES:
        return None
    try:
        batch_filter = f' AND native_batch_id = {_q(batch_id)}' if batch_id is not None else ''
        execute_statement(cur, f'SELECT native_table, refs_json, refs_sha256, snapshot_id, legacy_snapshot_id, empty_proof_json, capture_unit_id, native_batch_id, capture_times_json FROM {TABLE} WHERE cycle_id = {_q(cycle)} AND entity = {_q(entity)}' + batch_filter)
        rows = cur.fetchall()
    except Exception as exc:
        if any(token in str(exc).lower() for token in ('table_not_found', 'table not found', 'does not exist')):
            return None  # Existing pre-1399 manifests keep their batch contract.
        raise
    if not rows:
        return None
    if len(rows) != 1:
        raise RuntimeError('duplicate career capture reference rows')
    native, payload, digest, snapshot, legacy_snapshot, empty_proof, unit, logical_batch, capture_times = rows[0]
    empty_refs = json.loads(empty_proof or "[]")
    if (snapshot is None and not empty_refs) or (snapshot is not None and int(snapshot) < 1) or (legacy_snapshot is not None and int(legacy_snapshot) < 1):
        raise RuntimeError('career capture reference has no exact retained snapshot')
    if native != table or hashlib.sha256(payload.encode()).hexdigest() != digest:
        raise RuntimeError('career capture reference identity/checksum differs')
    expected_unit = hashlib.sha256((str(cycle) + '\0' + entity + '\0' + str(logical_batch) + '\0' + digest + '\0' + (empty_proof or '[]') + '\0' + capture_times).encode()).hexdigest()
    if unit != expected_unit:
        raise RuntimeError('career capture unit original proof checksum differs')
    refs = json.loads(payload)
    if not refs or any(not isinstance(item, list) or len(item) != 3 or not all(isinstance(v, str) and v for v in item[:2]) or not isinstance(item[2], int) or item[2] < 0 for item in refs):
        raise RuntimeError('invalid career capture reference pairs')
    if snapshot is None and any(item[2] > 0 for item in refs):
        raise RuntimeError('nonempty career capture reference lacks retained snapshot')
    predicate = CapturePredicate(snapshot_id=int(snapshot) if snapshot else None, legacy_snapshot_id=int(legacy_snapshot) if legacy_snapshot else None)
    if {item[0] for item in refs if item[2] == 0} != {str(item['player_id']) for item in empty_refs}:
        raise RuntimeError('career empty references lack original typed capture proof')
    predicate.refs = refs
    predicate.empty_refs = empty_refs
    predicate.capture_times = json.loads(capture_times)
    predicate.text = '(' + ' OR '.join(f'(player_id = {_q(player)} AND _batch_id = {_q(batch)})' for player, batch, count in refs if count > 0) + ')'
    if predicate.text == '()':
        predicate.text = 'false'
    return predicate


class CapturePredicate:
    def __init__(self, *, snapshot_id, legacy_snapshot_id):
        self.snapshot_id, self.legacy_snapshot_id = snapshot_id, legacy_snapshot_id
        self.text = ''
        self.refs = []
        self.empty_refs = []
        self.capture_times = {}


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
        capture = physical_predicate(cursor, cycle, native.key, native.table_name, batch_id=next(iter(set(frames[native.key]['_batch_id'].astype(str)))))
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

ANCHORS_TABLE = 'iceberg.ops.transfermarkt_career_snapshot_anchors_v1'


def persist_snapshot_anchors(connection, payload):
    """Immutable pre-write boundaries also protect fully captured empty clocks."""
    cur = connection.cursor()
    clocks = json.dumps(payload['capture_times'], sort_keys=True, separators=(',', ':'))
    try:
        execute_statement(cur, f"CREATE TABLE IF NOT EXISTS {ANCHORS_TABLE} (intent_sha256 varchar, table_name varchar, parent_snapshot_id bigint, capture_times_json varchar) WITH (format = 'PARQUET')")
        for table, parent in payload['tables'].items():
            parent_sql = 'NULL' if parent is None else str(parent)
            execute_statement(cur, f'''MERGE INTO {ANCHORS_TABLE} t USING (VALUES ({_q(payload['intent_sha256'])}, {_q(table)}, {parent_sql}, {_q(clocks)}))
                s(intent_sha256, table_name, parent_snapshot_id, capture_times_json)
                ON t.intent_sha256=s.intent_sha256 AND t.table_name=s.table_name
                WHEN NOT MATCHED THEN INSERT (intent_sha256, table_name, parent_snapshot_id, capture_times_json)
                VALUES (s.intent_sha256, s.table_name, s.parent_snapshot_id, s.capture_times_json)''')
            execute_statement(cur, f'SELECT parent_snapshot_id, capture_times_json FROM {ANCHORS_TABLE} WHERE intent_sha256={_q(payload["intent_sha256"])} AND table_name={_q(table)}')
            if cur.fetchall() != [(parent, clocks)]:
                raise RuntimeError('immutable career pre-write anchor drifted')
    finally:
        cur.close()


def latest_capture_times(connection, table, player_ids):
    """Source clocks remain available when the latest full response has no rows."""
    if table not in NATIVE_TABLES or not player_ids:
        return {}
    cur = connection.cursor()
    try:
        try:
            placeholders = ', '.join('?' for _ in player_ids)
            execute_statement(cur, f'''SELECT source_id, MAX(capture_clock) FROM {ANCHORS_TABLE} a
                CROSS JOIN UNNEST(CAST(JSON_PARSE(a.capture_times_json) AS map(varchar, varchar))) AS clocks(source_id, capture_clock)
                WHERE a.table_name = ? AND source_id IN ({placeholders}) GROUP BY source_id''', (table, *player_ids))
            rows = cur.fetchall()
        except Exception as exc:
            if not any(token in str(exc).lower() for token in ('table_not_found', 'table not found', 'does not exist')):
                raise
            rows = []
        clocks = {str(player): stamp for player, stamp in rows}
        endpoint = 'market_value_points' if table.endswith('market_value_points') else 'transfer_events'
        try:
            execute_statement(cur, 'SELECT source_id, MAX(last_success_at) FROM iceberg.ops.transfermarkt_fetch_state '
                f"WHERE endpoint = ? AND status IN ('success', 'valid_empty', 'authoritative_empty') AND source_id IN ({placeholders}) GROUP BY source_id", (endpoint, *player_ids))
            import pandas as pd
            for player, stamp in cur.fetchall():
                if stamp is not None and (str(player) not in clocks or pd.to_datetime(stamp, utc=True) > pd.to_datetime(clocks[str(player)], utc=True)):
                    clocks[str(player)] = stamp
        except Exception as exc:
            if not any(token in str(exc).lower() for token in ('table_not_found', 'table not found', 'does not exist')):
                raise
        return clocks
    finally:
        cur.close()


def intent_empty_capture_refs(path):
    """Original typed outcomes and raw references; never inferred from no rows."""
    from pathlib import Path
    path = Path(path)
    body = path.read_text()
    if hashlib.sha256(body.encode()).hexdigest() != path.stem:
        raise RuntimeError('empty career journal checksum differs')
    journal = json.loads(body)
    evidence = journal['evidence']
    entity = journal['identity'].get('entity')
    endpoint_path = ['transferHistory', 'list'] if entity in {'transfers', 'transfer_events'} else ['marketValueDevelopment', 'graph']
    ids = evidence.get('empty', evidence.get('valid_empty', []))
    if not ids:
        return []
    selected = evidence.get('processed', evidence.get('checkpoint_ids', []))
    rows = dict(zip(selected, evidence['state_rows'], strict=True))
    refs = []
    for player in sorted(set(ids)):
        if rows[player][0] not in {'authoritative_empty', 'valid_empty'} or rows[player][1] != 0:
            raise RuntimeError('empty career receipt lacks a complete typed endpoint')
        clock = evidence['captured_at_by_id'].get(player)
        if not clock:
            raise RuntimeError('empty career receipt lacks its original source clock')
        from urllib.parse import urlsplit
        def belongs(record):
            parts = urlsplit(str(record.get('url', ''))).path.rstrip('/').split('/')
            return len(parts) >= 4 and parts[-1] == str(player) and parts[-3:-1] == endpoint_path
        raw = [record for record in evidence.get('raw_attempts', []) if belongs(record)]
        cached = [record for record in evidence.get('cache_sources', []) if belongs(record)]
        if not raw and not cached:
            raise RuntimeError('typed-empty career receipt has no original raw source proof')
        refs.append({'player_id': player, 'status': 'authoritative_empty', 'expected_rows': 0,
            'captured_at': clock, 'payload_hash': rows[player][2], 'intent_sha256': path.stem,
            'raw_attempts': raw, 'cache_sources': cached})
    return refs


def recover_bundle_snapshots(connection, cycle, outputs, frames, intent_path, *, batch_id=None):
    """Find the anchored original snapshot and at most two consecutive children.

    The writer holds the common lock from boundary capture through replacement.
    One target MERGE and an optional typed-empty DELETE follow that boundary.
    The candidate set stays
    bounded even after thousands of later writes; full business and lineage,
    including the original batch, must match the durable parsed bundle.
    """
    from scrapers.transfermarkt.write_intents import read_snapshot_anchors
    from pathlib import Path
    import pandas as pd
    anchors = read_snapshot_anchors(intent_path) if intent_path is not None else {}
    native = next((output for output in outputs if output.table_name in NATIVE_TABLES), None)
    empty_refs = intent_empty_capture_refs(intent_path) if intent_path is not None else []
    if native is None or (frames[native.key].empty and not empty_refs) or not anchors:
        return {}
    cur = connection.cursor()
    snapshots = {}
    try:
        for output in outputs:
            table = output.table_name
            if table not in anchors:
                raise RuntimeError('incomplete original career snapshot boundary')
            expected = frames[output.key].copy()
            original = {item['player_id']: item['batch_id'] for item in expected.attrs.get('tm_original_capture_refs', [])} if not output.is_legacy else {}
            if original:
                mapped = expected.player_id.astype(str).map(original)
                expected['_batch_id'] = mapped.fillna(expected['_batch_id'])
            pairs = expected[['player_id', '_batch_id']].drop_duplicates().itertuples(index=False, name=None)
            terms = [f'(player_id={_q(player)} AND _batch_id={_q(batch)})' for player, batch in pairs]
            terms += [f'player_id={_q(item["player_id"])}' for item in empty_refs]
            predicate = ' OR '.join(terms) or 'false'
            parent = anchors[table]
            parent_filter = 'parent_id IS NULL' if parent is None else 'parent_id = ' + str(parent)
            try:
                execute_statement(cur, f'SELECT snapshot_id FROM iceberg.bronze."{table}$snapshots" WHERE {parent_filter} ORDER BY committed_at, snapshot_id LIMIT 2')
                candidates = [int(row[0]) for row in cur.fetchall()]
            except Exception as exc:
                if not any(token in str(exc).lower() for token in ('table_not_found', 'table not found', 'does not exist')):
                    raise
                if frames[native.key].empty and parent is None:
                    snapshots[output.key] = 0  # Immutable pre-write boundary proves absent table.
                    continue
                return {}
            if len(candidates) > 1:
                raise RuntimeError('ambiguous direct child of original career snapshot boundary')
            if empty_refs and candidates:
                execute_statement(cur, f'SELECT snapshot_id FROM iceberg.bronze."{table}$snapshots" WHERE parent_id = {candidates[0]} ORDER BY committed_at, snapshot_id LIMIT 2')
                following = [int(row[0]) for row in cur.fetchall()]
                if len(following) > 1:
                    raise RuntimeError('ambiguous second career snapshot commit')
                candidates = following + candidates
            if frames[native.key].empty and parent is None:
                snapshots[output.key] = 0  # Typed response was empty when the table was absent.
                continue
            if parent is not None:
                candidates.append(parent)  # Cache-only projections retain this snapshot.
            for snapshot in candidates:
                execute_statement(cur, 'SELECT ' + ', '.join(expected.columns) + ' FROM iceberg.bronze.' + table
                    + f' FOR VERSION AS OF {snapshot} WHERE ' + predicate)
                actual = pd.DataFrame(cur.fetchall(), columns=expected.columns)
                from dags.utils.transfermarkt_current_write import _rows
                if _rows(actual) == _rows(expected):
                    snapshots[output.key] = snapshot
                    break
            if output.key not in snapshots:
                return {}
        proof = {'native_snapshot_id': snapshots[native.key] or None, 'legacy_snapshot_id': None}
        for output in outputs:
            if output.is_legacy:
                proof['legacy_snapshot_id'] = snapshots[output.key] or None
        persist_capture_refs(connection, cycle, native.key, native.table_name, frames[native.key],
            legacy=any(output.is_legacy for output in outputs), snapshot_proof=proof, empty_refs=empty_refs,
            batch_id=batch_id or (json.loads(Path(intent_path).read_text())['evidence']['batch_id'] if frames[native.key].empty else None))
        return snapshots
    finally:
        cur.close()
