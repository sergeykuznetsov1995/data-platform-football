"""Offline acceptance for TM committing boundaries and physical capture refs."""
from datetime import date, datetime, timezone
import json
import multiprocessing
import sqlite3
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from scrapers.base.trino_manager import TrinoAmbiguousWriteError, TrinoError
from scrapers.transfermarkt import native_writer, writer
from scrapers.transfermarkt.write_intents import save_intent, pending_intents, unpack_frames, finish_intent
from tests.unit.scrapers.test_trino_ambiguous_replace import FakeConflictError, FakeConnection
from dags.scripts import run_transfermarkt_scraper as run
from dags.utils import transfermarkt_bronze_dq as dq

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def isolated_paths(monkeypatch, tmp_path):
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    monkeypatch.setenv('TM_WRITE_INTENT_DIR', str(tmp_path / 'intents'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)


class Conflict(RuntimeError):
    error_name = 'ICEBERG_COMMIT_ERROR'


def _commit_worker(database, first):
    conn = sqlite3.connect(database)
    attempts = 0
    def operation():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise Conflict('injected optimistic commit conflict')
        conn.execute('BEGIN IMMEDIATE')
        for player in [first, 'shared']:
            conn.execute('INSERT INTO careers VALUES (?, ?) ON CONFLICT(player_id) DO UPDATE SET value=excluded.value', (player, first))
        conn.commit()
    writer._execute_committing(operation)
    conn.close()


def test_two_processes_commit_same_table_retry_without_duplicates_or_lost_updates(tmp_path):
    database = tmp_path / 'warehouse.db'
    conn = sqlite3.connect(database)
    conn.execute('CREATE TABLE careers(player_id TEXT PRIMARY KEY, value TEXT)')
    conn.commit()
    ctx = multiprocessing.get_context('fork')
    processes = [ctx.Process(target=_commit_worker, args=(str(database), str(i))) for i in (1, 2)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    rows = dict(conn.execute('SELECT * FROM careers'))
    assert rows['1'] == '1' and rows['2'] == '2'
    assert rows['shared'] in {'1', '2'} and len(rows) == 3
    conn.close()


def test_committing_retries_poll_failure_drains_before_return_and_bounds_retry(monkeypatch):
    cursor = MagicMock()
    cursor.fetchall.side_effect = [Conflict('first poll conflict'), []]
    monkeypatch.setattr(writer.time, 'sleep', lambda _: None)
    writer.execute_statement(cursor, 'MERGE INTO iceberg.ops.tm t USING x ON true WHEN MATCHED THEN DELETE')
    assert cursor.execute.call_count == cursor.fetchall.call_count == 2
    cursor.reset_mock()
    cursor.fetchall.side_effect = Conflict('persistent conflict')
    with pytest.raises(Conflict):
        writer.execute_statement(cursor, 'DELETE FROM iceberg.bronze.transfermarkt_transfers WHERE player_id = ?', ('1',))
    assert cursor.execute.call_count == 4


def test_unrelated_sql_failure_is_not_retried(monkeypatch):
    cursor = MagicMock()
    cursor.fetchall.side_effect = RuntimeError('invalid metadata')
    with pytest.raises(RuntimeError, match='invalid metadata'):
        writer.execute_statement(cursor, 'MERGE INTO iceberg.ops.tm t USING x ON true WHEN MATCHED THEN DELETE')
    assert cursor.execute.call_count == 1


@pytest.mark.parametrize('table', sorted(writer.CAREER_TABLES))
def test_all_four_career_replacements_use_one_target_merge(table):
    manager = writer.TransfermarktTrinoManager()
    frame = pd.DataFrame({'player_id': ['1'], 'value': [9]})
    def response(sql, *args, **kwargs):
        if kwargs.get('fetch'):
            return [(1,)]
        return None
    with patch.object(manager, '_execute', side_effect=response) as execute, patch.object(manager, 'insert_dataframe', return_value=1), patch.object(manager, 'drop_table'):
        assert manager.insert_dataframe_atomic('bronze', table, frame, delete_filter="player_id = '1'") == 1
    statements = [item.args[0] for item in execute.call_args_list]
    assert not any(sql.startswith('DELETE FROM iceberg.bronze.' + table) for sql in statements)
    merge = next(sql for sql in statements if sql.startswith('MERGE INTO iceberg.bronze.' + table))
    assert 'WHEN MATCHED THEN DELETE' in merge and "= 'insert'" in merge


def test_parsed_intent_preserves_types_lineage_original_time_after_49_hours():
    old = datetime(2026, 10, 1, tzinfo=timezone.utc)
    frame = pd.DataFrame({'player_id': ['1'], 'mv_date': [date(2025, 1, 1)], 'fetched_at': [old], '_batch_id': ['original']})
    path = save_intent({'scope': 'GB1/2025'}, {'market_value_points': frame}, original_cycle='first')
    matches = pending_intents({'scope': 'GB1/2025'})
    restored = unpack_frames(matches[0][1]['frames'])['market_value_points']
    pd.testing.assert_frame_equal(frame, restored)
    assert matches[0][1]['evidence']['original_cycle'] == 'first'
    finish_intent(path)
    assert pending_intents({'scope': 'GB1/2025'}) == []


def test_corrupt_intent_fails_closed_without_recovery():
    path = save_intent({'scope': 'x'}, {'frame': pd.DataFrame({'x': [1]})})
    path.write_text(path.read_text().replace('"x":[', '"bad":[', 1) + ' ')
    with pytest.raises(RuntimeError, match='checksum'):
        pending_intents({'scope': 'x'})


def test_stale_capture_refuses_bundle_before_first_save():
    spec = run.ENTITY_SPECS[run.ENTITY_MV_HISTORY]
    frame = pd.DataFrame({'player_id': ['1'], 'fetched_at': [datetime(2026, 10, 1, tzinfo=timezone.utc)], '_ingested_at': [datetime(2026, 10, 1)]})
    scraper = MagicMock()
    scraper._tm_career_intent_path = None
    scraper._bronze_connection.return_value.cursor.return_value.fetchall.return_value = [('1', datetime(2026, 10, 2))]
    scraper._build_partition_delete_filter.return_value = "player_id = '1'"
    with pytest.raises(writer.StaleTransfermarktWrite):
        run._save_frames(scraper, spec, {'market_value_points': frame, 'legacy_market_value_history': frame}, False, {'outputs': {}, 'tables': []})
    scraper.save_to_iceberg.assert_not_called()


def test_dq_detects_physical_keys_across_batches_and_latest_reference_loss():
    assert set(dq.CAREER_WRITE_TABLE_KEYS) == {'iceberg.bronze.transfermarkt_market_value_points',
                                               'iceberg.bronze.transfermarkt_transfer_events'}
    for table in dq.CAREER_WRITE_TABLE_KEYS:
        sql = dq.build_career_write_duplicates_sql(table)
        assert 'HAVING COUNT(*) > 1' in sql and '_batch_id' not in sql
    loss = dq.build_career_write_loss_sql('iceberg.bronze.transfermarkt_market_value_points')
    assert 'latest_refs' in loss and 'ORDER BY capture_clock DESC' in loss
    assert 'CASE WHEN r.expected_rows = 0 THEN COALESCE(total.actual_rows, 0)' in loss
    assert 'empty_receipt.last_success_at >= r.capture_clock' in loss


def test_shared_maintenance_preserves_tm_original_snapshots_and_other_thresholds(monkeypatch):
    from utils import maintenance_tasks as maintenance
    conn = MagicMock()
    monkeypatch.setattr(writer, 'shared_writer_lock_ready', lambda: True)
    monkeypatch.setattr(maintenance, '_fetch_scalar', lambda _conn, _sql: 0 if 'snapshot_anchors' in _sql else 3)
    execute = MagicMock()
    monkeypatch.setattr(maintenance, '_exec_alter', execute)
    protected = maintenance._maintain_one(conn, 'iceberg."bronze"."transfermarkt_market_value_points"', '30d')
    assert protected['retention_skipped'] and protected['protected_receipt_count'] == 3
    execute.assert_not_called()
    maintenance._maintain_one(conn, 'iceberg.bronze.other_source', '30d')
    assert execute.call_count == 2
    assert all("retention_threshold => '30d'" in call.args[1] for call in execute.call_args_list)


def test_tm_bounded_compaction_retries_commit_conflict(monkeypatch):
    from utils import maintenance_tasks as maintenance
    monkeypatch.setattr(writer, 'shared_writer_lock_ready', lambda: True)
    cursor = MagicMock()
    cursor.fetchall.side_effect = [Conflict('poll conflict'), []]
    conn = MagicMock()
    conn.cursor.return_value = cursor
    monkeypatch.setattr(writer.time, 'sleep', lambda _: None)
    maintenance._exec_alter(conn, '''ALTER TABLE iceberg."bronze"."transfermarkt_transfer_events" EXECUTE optimize(file_size_threshold => '64MB') WHERE "$path" IN ('s3://bounded/file.parquet')''')
    assert cursor.execute.call_count == 2


def test_shared_maintenance_skips_tm_without_real_shared_host_lock(monkeypatch):
    from utils import maintenance_tasks as maintenance
    conn = MagicMock()
    result = maintenance._maintain_one(conn, 'iceberg.bronze.transfermarkt_transfer_events', '30d')
    assert result['retention_skipped'] and result['reason'] == 'shared_tm_writer_lock_required'
    result = maintenance._exec_alter(conn, 'ALTER TABLE iceberg.bronze.transfermarkt_transfer_events EXECUTE optimize')
    assert result['maintenance_skipped']
    conn.cursor.assert_not_called()


def test_shared_maintenance_skips_tm_when_writer_module_absent(monkeypatch):
    """Trees without the TM scraper stack (shared scheduler, P0 15.11) skip
    TM-protected maintenance instead of failing the weekly sweep."""
    import sys

    from utils import maintenance_tasks as maintenance

    # Importing a module mapped to None raises ImportError in `from ... import`.
    monkeypatch.setitem(sys.modules, 'scrapers.transfermarkt.writer', None)
    conn = MagicMock()
    result = maintenance._maintain_one(conn, 'iceberg.bronze.transfermarkt_transfer_events', '30d')
    assert result['retention_skipped'] and result['reason'] == 'shared_tm_writer_lock_required'
    result = maintenance._exec_alter(conn, 'ALTER TABLE iceberg.bronze.transfermarkt_transfer_events EXECUTE optimize')
    assert result['maintenance_skipped']
    conn.cursor.assert_not_called()


def test_shared_lock_contract_checks_file_identity_and_rejects_other_volume(monkeypatch, tmp_path):
    path = tmp_path / 'shared.lock'
    path.touch()
    stat = path.stat()
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(path))
    assert not writer.shared_writer_lock_ready()
    monkeypatch.setenv('TM_WRITER_LOCK_SHARED_FILE_ID', f'{stat.st_dev}:{stat.st_ino}')
    assert writer.shared_writer_lock_ready()
    other = tmp_path / 'different-volume.lock'
    other.touch()
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(other))
    with pytest.raises(RuntimeError, match='approved shared host file'):
        writer.shared_writer_lock_ready()


def test_committing_cursor_preserves_affected_rows_for_native_cas():
    from utils.transfermarkt_native_v2 import _drain
    raw = MagicMock()
    raw.fetchall.return_value = [(1,)]
    sql = 'MERGE INTO iceberg.ops.transfermarkt_reader_state t USING x ON true WHEN MATCHED THEN UPDATE SET revision=2'
    assert _drain(raw, sql) == [(1,)]
    assert raw.fetchall.call_count == 1
    raw.reset_mock()
    adapted = writer.CommittingCursor(raw)
    assert _drain(adapted, sql) == [(1,)]
    assert raw.fetchall.call_count == 1


def test_parent_immutable_events_append_only_new_cumulative_delta(monkeypatch):
    from dags.scripts import run_transfermarkt_scope_cycle as cycle
    from tests.unit.utils.test_transfermarkt_current_write import MemoryDB
    from copy import deepcopy
    db = MemoryDB()
    fields = cycle.PROXY_EVENT_FIELDS
    initial = {'parent_cycle_id': 'parent', 'scopes': {'scope': {'child_cycle_id': 'child',
        'manifest_digest': 'first', 'by_entity': {'market_value_points': dict.fromkeys(fields, 10)}}}}
    # Exercise the event journal's real SQL without unrelated seven-entity shadow.
    monkeypatch.setattr(cycle, 'proxy_ledger_merge_sql', lambda _: 'SELECT 1')
    cycle.persist_parent_proxy_ledger(initial, connection_factory=lambda: db)
    next_receipt = deepcopy(initial)
    next_receipt['scopes']['scope']['manifest_digest'] = 'second'
    next_receipt['scopes']['scope']['by_entity']['market_value_points'] = dict.fromkeys(fields, 16)
    cycle.persist_parent_proxy_ledger(next_receipt, connection_factory=lambda: db)
    cycle.persist_parent_proxy_ledger(next_receipt, connection_factory=lambda: db)
    rows = db.sql.execute('SELECT requests FROM transfermarkt_proxy_ledger_events_v1 ORDER BY requests').fetchall()
    assert rows == [(6,), (10,)]
    assert db.sql.execute('SELECT SUM(requests) FROM transfermarkt_proxy_ledger_events_v1').fetchone()[0] == 16
    old = deepcopy(next_receipt)
    old['scopes']['scope']['by_entity']['market_value_points']['requests'] = 15
    with pytest.raises(cycle.ScopeCycleError, match='regressed'):
        cycle.persist_parent_proxy_ledger(old, connection_factory=lambda: db)
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_proxy_ledger_events_v1').fetchone()[0] == 2


@pytest.mark.parametrize('policy_name', ['standing_approval_policy.json', 'standing_backfill_policy.json'])
def test_additive_safety_ops_permission_migration_preserves_only_exact_old_body(policy_name):
    from dags.scripts import run_transfermarkt_scope_cycle as cycle
    from dags.utils.transfermarkt_approval import load_standing_policy
    from dataclasses import replace
    from pathlib import Path
    policy = load_standing_policy(Path(cycle.__file__).parents[1] / ('configs/transfermarkt/' + policy_name))
    original = replace(policy, allowed_write_tables=tuple(table for table in policy.allowed_write_tables if table not in cycle.CAREER_SAFETY_OPS_TABLES))
    assert cycle.standing_policy_hash_compatible(policy, original.policy_hash)
    assert cycle.CAREER_SAFETY_OPS_TABLES <= cycle.required_write_tables('dual')
    assert cycle.CAREER_SAFETY_OPS_TABLES <= cycle.required_write_tables('native-only')
    changed = replace(policy, paid_proxy=replace(policy.paid_proxy, request_limit=policy.paid_proxy.request_limit + 1))
    assert not cycle.standing_policy_hash_compatible(changed, original.policy_hash)
    assert not cycle.standing_policy_hash_compatible(policy, '0' * 64)


def test_pending_prewrite_anchor_protects_compaction_retention_gap(monkeypatch):
    from utils import maintenance_tasks as maintenance
    monkeypatch.setattr(writer, 'shared_writer_lock_ready', lambda: True)
    monkeypatch.setattr(maintenance, '_fetch_scalar', lambda _conn, sql: 1 if 'snapshot_anchors' in sql else 0)
    execute = MagicMock()
    monkeypatch.setattr(maintenance, '_exec_alter', execute)
    result = maintenance._maintain_one(MagicMock(), 'iceberg.bronze.transfermarkt_transfer_events', '30d')
    assert result['retention_skipped'] and result['protected_receipt_count'] == 1
    execute.assert_not_called()


def test_squad_context_columns_keep_types_when_a_batch_is_all_null():
    # #1406 adds the columns to live tables: whichever batch writes first may
    # carry no loan, no injury and no known number at all.
    from scrapers.transfermarkt import scraper as tm
    from scrapers.transfermarkt.writer import TransfermarktIcebergWriter, TransfermarktTrinoManager

    row = {column: None for column in tm.SQUAD_MEMBERSHIP_COLUMNS}
    row.update({'competition_id': 'GB1', 'edition_id': '2026', 'club_id': '11', 'player_id': '1'})
    frame = tm._with_metadata([row], tm.SQUAD_MEMBERSHIP_COLUMNS, entity_type='squad_memberships', batch_id='b')
    types = TransfermarktTrinoManager().arrow_schema_to_trino(TransfermarktIcebergWriter()._pandas_to_arrow(frame).schema)
    assert {column: types[column] for column in ('shirt_number', 'signed_from_fee_eur')} == {
        'shirt_number': 'BIGINT', 'signed_from_fee_eur': 'BIGINT'}
    assert types['is_on_loan'] == types['is_captain'] == 'BOOLEAN'
    assert {types[column] for column in tm.SQUAD_DATE_COLUMNS} == {'DATE'}


def _fake_trino(monkeypatch, calls):
    monkeypatch.setattr(
        writer.base_trino,
        'trino',
        SimpleNamespace(
            dbapi=SimpleNamespace(connect=lambda **kwargs: calls.append(kwargs)),
            auth=SimpleNamespace(BasicAuthentication=lambda *args: args),
        ),
        raising=False,
    )


@pytest.mark.parametrize('password', [None, 'secret'])
def test_tm_connection_disables_dynamic_filtering(monkeypatch, password):
    calls = []
    _fake_trino(monkeypatch, calls)
    if password:
        monkeypatch.setenv('TRINO_PASSWORD', password)
    else:
        monkeypatch.delenv('TRINO_PASSWORD', raising=False)

    manager = writer.TransfermarktTrinoManager(host='trino')
    manager._create_connection()

    assert calls[-1]['session_properties'] == {'enable_dynamic_filtering': 'false'}
    assert calls[-1]['host'] == 'trino'
    assert calls[-1]['port'] == manager.port
    assert calls[-1]['user'] == manager.user
    assert calls[-1]['catalog'] == manager.catalog
    if password:
        assert calls[-1]['http_scheme'] == 'https'
        assert calls[-1]['auth'] == ('airflow', password)
        assert calls[-1]['verify'] is False
    else:
        assert 'http_scheme' not in calls[-1]
        assert 'auth' not in calls[-1]


def test_base_trino_manager_still_receives_no_session_properties(monkeypatch):
    calls = []
    _fake_trino(monkeypatch, calls)
    monkeypatch.delenv('TRINO_PASSWORD', raising=False)

    writer.base_trino.TrinoTableManager(host='trino')._create_connection()

    assert 'session_properties' not in calls[-1]


# ---------------------------------------------------------------- review round 6, finding 2


class _TombstoneWarehouse:
    """A scripted Trino whose statements land in DuckDB as Trino would run them."""

    def __init__(self, table, rows):
        import duckdb

        kinds = {str: 'VARCHAR', int: 'BIGINT', bool: 'BOOLEAN', type(None): 'VARCHAR'}
        self.table = table
        self.engine = duckdb.connect(':memory:')
        columns = list(rows[0])
        self.engine.execute(f'CREATE TABLE {table} (' + ', '.join(
            f'{column} {kinds[type(rows[0][column])]}' for column in columns) + ')')
        self.engine.executemany(
            f'INSERT INTO {table} VALUES ({", ".join("?" for _ in columns)})',
            [tuple(row[column] for column in columns) for row in rows])
        self.executed = []
        self.lost = False

    def run(self, sql):
        # Trino names a table by its catalog and schema; DuckDB has neither.
        sql = sql.replace('iceberg.bronze.', '')
        self.executed.append(sql)
        return self.engine.execute(sql).fetchall()

    def connection(self):
        return _TombstoneConnection(self)

    def players(self):
        rows = self.engine.execute(f'SELECT player_id FROM {self.table}').fetchall()
        return sorted(row[0] for row in rows)

    @property
    def merges(self):
        return [sql for sql in self.executed if sql.startswith('MERGE INTO')]


def _roster(players):
    return _TombstoneWarehouse('roster', [{'club_id': '11', 'player_id': player} for player in players])


class _TombstoneCursor:
    def __init__(self, warehouse):
        self.warehouse = warehouse
        self.sql = None
        self.rows = None

    def execute(self, sql, params=None):
        self.sql = sql
        if sql.startswith('MERGE INTO'):
            self.query_id = 'q_target_merge'
        self.rows = self.warehouse.run(sql)

    def fetchall(self):
        if self.sql.startswith('MERGE INTO') and not self.warehouse.lost:
            # The server committed the replacement; its response never came.
            self.warehouse.lost = True
            raise RuntimeError('Connection reset by peer')
        return self.rows

    def close(self):
        pass


class _TombstoneConnection:
    def __init__(self, warehouse):
        self.warehouse = warehouse

    def cursor(self):
        return _TombstoneCursor(self.warehouse)

    def close(self):
        pass


def _scripted(warehouse):
    """The production manager on the scripted server, its slow stage insert done."""
    manager = writer.TransfermarktTrinoManager()
    manager._conn = warehouse.connection()
    manager._create_connection = warehouse.connection  # a reset reaches the same server

    def staged(schema, table, df, *args, **kwargs):
        kinds = {row[0]: row[1] for row in warehouse.engine.execute(f'DESCRIBE {table}').fetchall()}
        columns = ', '.join(f'CAST("{column}" AS {kind})' for column, kind in kinds.items())
        warehouse.engine.execute(f'INSERT INTO {table} SELECT {columns} FROM df')
        return len(df)

    manager.insert_dataframe = staged
    return manager


def _replace_roster(warehouse, players):
    """One replacement of the roster through the production staged path."""
    frame = pd.DataFrame({'club_id': ['11'] * len(players), 'player_id': list(players)})
    return _scripted(warehouse).insert_dataframe_atomic(
        'bronze', 'roster', frame, delete_filter="club_id = '11'")


def test_a_replayed_replacement_merge_inserts_its_rows_a_second_time():
    # Round 6 finding 2, the hazard itself. The staged replacement deletes the
    # rows the scope held when its stage was built and inserts the staged rows
    # on every execution. Replayed after the response to a committed MERGE was
    # lost, it reports one row and leaves two. The replay-safe default is what
    # the donor stack keeps; a journalled core write must not use it.
    warehouse = _roster(['A'])

    assert _replace_roster(warehouse, ['B']) == 1

    assert len(warehouse.merges) == 2
    assert warehouse.players() == ['B', 'B']


def test_a_journalled_replacement_stops_at_a_lost_response_and_is_never_replayed():
    # Round 6 finding 2, the fix. Inside the core writer's commit boundary the
    # replacement MERGE runs once: an uncertain outcome is raised for the
    # journal, whose replay reads the scope back before it writes again.
    warehouse = _roster(['A'])

    with native_writer.journalled_write(), pytest.raises(TrinoAmbiguousWriteError):
        _replace_roster(warehouse, ['B'])

    assert len(warehouse.merges) == 1
    assert warehouse.players() == ['B']  # committed once, before the response was lost
    assert not [sql for sql in warehouse.executed if sql.startswith('DROP TABLE')]  # stage retained
    # A replacement issued again builds its stage from the rows the scope holds
    # now: it lands the same rows once more, and once only.
    assert _replace_roster(warehouse, ['B']) == 1
    assert warehouse.players() == ['B']


def test_a_journalled_replacement_is_not_retried_while_its_commit_state_is_unknown():
    # "Failed to commit the transaction" may stand for a commit that landed.
    unknown = FakeConflictError('Failed to commit the transaction')
    connection = FakeConnection(outcomes=[unknown])
    manager = writer.TransfermarktTrinoManager()
    manager._conn = connection
    sql = 'MERGE INTO iceberg.bronze.roster t USING iceberg.bronze.roster__stg_x s'

    with native_writer.journalled_write(), patch.object(writer.base_trino.time, 'sleep') as sleep:
        with pytest.raises(TrinoError) as refused:
            manager._execute_replacement(sql)

    assert connection.executed == [sql]
    assert not isinstance(refused.value, TrinoAmbiguousWriteError)
    assert refused.value.__cause__ is unknown
    sleep.assert_not_called()


def test_a_journalled_replacement_still_retries_a_rejected_commit():
    conflict = FakeConflictError('Found conflicting files')
    connection = FakeConnection(outcomes=[conflict, []])
    manager = writer.TransfermarktTrinoManager()
    manager._conn = connection
    sql = 'MERGE INTO iceberg.bronze.roster t USING iceberg.bronze.roster__stg_x s'

    with native_writer.journalled_write(), patch.object(writer.base_trino.time, 'sleep') as sleep:
        manager._execute_replacement(sql)

    assert connection.executed == [sql, sql]
    assert sleep.call_count == 1


def _trino_response(query_id, **fields):
    from requests import Response

    response = Response()
    response.status_code = 200
    response._content = json.dumps({
        'id': query_id, 'stats': {}, 'infoUri': 'http://offline.invalid/query', **fields,
    }).encode()
    return response


@pytest.mark.parametrize('boundary', ['manager', 'pages', 'receipt'])
def test_one_accepted_post_per_journalled_dbapi_call(monkeypatch, tmp_path, boundary):
    # Round 8 finding 1. Count accepted HTTP POSTs below the installed DBAPI.
    import requests
    from trino import dbapi
    from trino.exceptions import TrinoConnectionError

    submissions = []
    sql = ('MERGE INTO target USING stage ON true WHEN MATCHED THEN DELETE'
           if boundary == 'manager' else f'INSERT INTO {boundary} VALUES (?)')
    params = None if boundary == 'manager' else ('capture',)

    class LostResponses(requests.Session):
        def send(self, request, **kwargs):
            assert request.method == 'POST'
            submissions.append(request.body)
            raise requests.ConnectionError('Connection reset after accepted POST')

    connect = dbapi.connect

    def offline(**options):
        return connect(http_session=LostResponses(), **options)

    monkeypatch.setattr(dbapi, 'connect', offline)
    monkeypatch.setattr(writer.time, 'sleep', lambda _: None)
    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    monkeypatch.delenv('TRINO_PASSWORD', raising=False)
    manager = writer.TransfermarktTrinoManager(host='offline.invalid')
    # The same manager may have been warmed by the donor stack.
    manager._conn = manager._create_connection()
    donor = manager.connection
    with pytest.raises(TrinoConnectionError):
        donor.cursor().execute('DELETE FROM control')
    assert len(submissions) == 3 and len(set(submissions)) == 1
    submissions.clear()

    with native_writer.journalled_write():
        if boundary == 'manager':
            # Skip only the connection health check (it would also lose its response).
            monkeypatch.setattr(manager, '_connect_with_retry',
                                lambda: setattr(manager, '_conn', manager._create_connection()))
            with pytest.raises(TrinoAmbiguousWriteError):
                manager._execute_replacement(sql)
        else:
            connection = native_writer._journal_connection(lambda: native_writer.CommittingConnection(
                dbapi.connect(host='offline.invalid', user='test')))
            key = ('ops', boundary)
            landed = []

            def insert():
                native_writer._insert_once(connection, key, sql, params, lambda: tuple(landed), params)

            with pytest.raises(TrinoConnectionError):
                insert()
            with pytest.raises(RuntimeError, match='still pending'):
                insert()
            landed.append('capture')  # the only accepted submission finally commits
            insert()
    assert len(submissions) == 1
    assert sql.encode() in submissions[0]
    if boundary == 'manager':
        assert manager.connection.max_attempts == donor.max_attempts
        assert manager.connection.legacy_prepared_statements is donor.legacy_prepared_statements


def test_parameterized_insert_keeps_its_dml_id_across_a_lost_poll(monkeypatch, tmp_path):
    # Round 8 finding 2. Legacy cleanup would return q_deallocate while the
    # INSERT was still queued. Exercise execute() polling inside the DBAPI.
    import requests
    from trino import dbapi
    from trino.exceptions import TrinoConnectionError, TrinoUserError

    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    posts = []
    rejected = dict(errorName='QUERY_QUEUE_FULL', errorCode=1, errorType='USER_ERROR',
                    message='rejected at admission')

    class Coordinator(requests.Session):
        def send(self, request, **kwargs):
            if request.method == 'GET':
                raise requests.ConnectionError('poll response lost')
            if request.method == 'DELETE':  # best effort cursor cleanup
                response = _trino_response('q_insert')
                response.status_code = 204
                return response
            assert request.method == 'POST'
            sql = request.body.decode()
            posts.append(sql)
            if sql.startswith("EXECUTE IMMEDIATE 'SELECT 1'"):
                return _trino_response('q_probe', error=rejected)
            if sql.startswith('PREPARE '):
                return _trino_response('q_prepare')
            if sql.startswith('DEALLOCATE '):
                return _trino_response('q_deallocate', error=rejected)
            return _trino_response('q_insert', nextUri='http://offline.invalid/results/q_insert')

    def connect():
        return dbapi.connect(host='offline.invalid', user='test', http_session=Coordinator(),
                             max_attempts=1, legacy_prepared_statements=True)

    sql = 'INSERT INTO receipts VALUES (?)'
    legacy = connect().cursor()
    with pytest.raises(TrinoUserError) as cleanup:
        legacy.execute(sql, ('capture',))
    assert cleanup.value.query_id == 'q_deallocate' and legacy.query_id == 'q_insert'
    assert [post.split()[0] for post in posts] == ['PREPARE', 'EXECUTE', 'DEALLOCATE']
    posts.clear()
    connection = native_writer._journal_connection(connect)
    key = ('ops', 'receipt')
    with pytest.raises(TrinoConnectionError):
        native_writer._insert_once(connection, key, sql, ('capture',), lambda: None, ('capture',))
    assert posts == ["EXECUTE IMMEDIATE 'INSERT INTO receipts VALUES (?)' USING 'capture'"]
    journal = native_writer.write_journal
    assert journal.statement_query(journal.pending_statement(key)) == 'q_insert'
    from tests.unit.scrapers.test_transfermarkt_native_writer import CoreDB
    history = CoreDB()
    history.sql.executemany('INSERT INTO test_query_history VALUES (?, ?, ?)', [
        ('q_deallocate', 'FAILED', 'QUERY_QUEUE_FULL'), ('q_insert', 'QUEUED', None)])
    with pytest.raises(RuntimeError, match='still pending'):
        native_writer._settle_statement(history, key, native_writer._digest(('capture',)), lambda: False)


@pytest.fixture
def staged_bundle(monkeypatch, tmp_path, request):
    from tests.unit.scrapers import test_transfermarkt_native_writer as native

    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'stage-journal'))
    db = native.CoreDB()
    old = native.membership_plan(['1'])
    fresh = native.membership_plan(getattr(request, 'param', ['2']), clock=native.LATER, capture='next', run='next')
    native.write(db, [old])
    warehouse = _TombstoneWarehouse(old.table, [dict(old.rows[0])])
    warehouse.lost = True
    manager = _scripted(warehouse)
    production = writer.TransfermarktIcebergWriter()
    production._trino_manager = manager
    monkeypatch.setattr(manager, 'table_exists', lambda *args: True)
    monkeypatch.setattr(native.IcebergWriter, '_evolve_schema', lambda *args: None)

    def commit():
        return native_writer.write_core_bundle(lambda: db, production, [fresh], run_id='next',
                                                universe_version='u', committed_at=native.LATER)

    def replay():
        return native_writer.replay_pending_intents(lambda: db, production, run_id='next')

    def land(sql):
        result = warehouse.engine.execute(sql.replace('iceberg.bronze.', '')).fetchall()
        if sql.startswith('MERGE INTO'):
            native._copy_warehouse_scope(warehouse, db, fresh)
        return result

    return warehouse, db, fresh, commit, replay, land


def test_fresh_stage_has_no_drop_that_can_land_behind_queued_merge(staged_bundle, monkeypatch):
    # Round 10 finding 9
    warehouse, db, plan, commit, replay, land = staged_bundle
    drops, queued = [], []
    run = warehouse.run
    def delay(sql):
        if sql.startswith('DROP TABLE'):
            drops.append(sql)
            return []
        if sql.startswith('MERGE INTO'):
            queued.append(sql)
            raise ConnectionResetError('Connection reset after queued MERGE')
        return run(sql)
    with monkeypatch.context() as patch:
        patch.setattr(warehouse, 'run', delay)
        with pytest.raises(TrinoAmbiguousWriteError):
            commit()
        with pytest.raises(RuntimeError, match='still pending'):
            replay()
    assert len(queued) == 1
    assert drops == []
    assert len(warehouse.engine.execute('SHOW TABLES').fetchall()) == 2
    land(queued[0])
    db.sql.execute("INSERT INTO test_query_history VALUES ('q_target_merge', 'FINISHED', NULL)")
    assert replay()[0]['verified'] and warehouse.players() == ['2']
    assert warehouse.engine.execute('SHOW TABLES').fetchall() == [(plan.table,)]


@pytest.mark.parametrize('outcome', ['queued', 'untracked', 'analysis', 'missing_source', 'legacy'])
def test_late_stage_create_is_dropped_after_its_terminal_result(staged_bundle, monkeypatch, outcome):
    # Round 10 finding 16
    # Round 11 finding 8. Cleanup survives the verified bundle independently.
    from tests.unit.scrapers.test_transfermarkt_native_writer import CoreCursor

    warehouse, db, plan, commit, replay, land = staged_bundle
    # Round 12 finding 8. A bound failed CTAS can leave no stage to probe.
    analysis_errors = {'analysis': 'TYPE_MISMATCH', 'missing_source': 'TABLE_NOT_FOUND'}
    queued, stages = [], []
    execute = _TombstoneCursor.execute
    read = CoreCursor.execute
    def read_stage(self, sql, params=()):
        if stages and sql == f'SELECT * FROM {stages[0]} WHERE 1 = 0':
            if (stages[0].split('.')[-1],) in warehouse.engine.execute('SHOW TABLES').fetchall():
                return land(sql)
            raise RuntimeError('TABLE_NOT_FOUND: ' + stages[0])
        return read(self, sql, params)

    def delay(self, sql, params=None):
        if sql.startswith('CREATE TABLE ') and '__stg_' in sql:
            stages.append(sql.split()[2])
            if not queued:
                queued.append(sql)
                if outcome == 'queued' or outcome in analysis_errors:
                    self.query_id = 'q_stage_create'
                    db.sql.execute("INSERT INTO test_query_history VALUES ('q_stage_create', 'QUEUED', NULL)")
                raise ConnectionResetError('Connection reset after queued CREATE')
        if sql.startswith('MERGE INTO'):
            self.sql = sql
            self.query_id = 'q_target_merge'
            self.rows = land(sql)
            return
        return execute(self, sql, params)
    with monkeypatch.context() as patch:
        patch.setattr(_TombstoneCursor, 'execute', delay)
        patch.setattr(CoreCursor, 'execute', read_stage)
        with pytest.raises(TrinoAmbiguousWriteError):
            commit()
        journal = native_writer.write_journal
        if outcome == 'legacy':
            old, = (journal._root() / 'stages').glob('*/*.json')
            record = journal._read_record(old)
            old.unlink()
            del record['fenced_create']
            journal._immutable_record(old, record)
            journal.finish_statement(journal.pending_statement(('stage-create', stages[0])))
        if outcome in analysis_errors:
            db.sql.execute("UPDATE test_query_history SET state='FAILED', error_code=? WHERE query_id='q_stage_create'",
                           (analysis_errors[outcome],))
        assert replay()[0]['verified']
        assert len(set(stages)) == len(stages) == 2
        assert not journal.pending_intents({'run_id': 'next'})
        assert warehouse.players() == ['2']
        assert bool(list((journal._root() / 'abandoned-stages').glob('*/*.json'))) == (outcome not in analysis_errors)
        if outcome in analysis_errors:
            assert journal.pending_statement(('stage-create', stages[0])) is None
        else:
            land(queued[0])
            db.sql.execute("UPDATE test_query_history SET state = 'FINISHED' WHERE query_id = 'q_stage_create'")
            assert replay() == []
    assert warehouse.engine.execute('SHOW TABLES').fetchall() == [(plan.table,)]
    assert not native_writer.write_journal.pending_intents({})


@pytest.mark.parametrize('name,code', [('COLUMN_NOT_FOUND', 47), ('TABLE_NOT_FOUND', 46)])
def test_create_analysis_response_settles_only_its_stage(staged_bundle, monkeypatch, name, code):
    # Round 11 finding 8. A typed, bound CTAS rejection needs no later query history.
    # Round 12 finding 8. The coordinator rejected analysis of the source table.
    from trino.exceptions import TrinoUserError
    from scrapers.base.trino_manager import TrinoError

    warehouse, _, plan, commit, replay, land = staged_bundle
    execute, attempts = _TombstoneCursor.execute, []
    def reject(self, sql, params=None):
        if sql.startswith('CREATE TABLE ') and '__stg_' in sql and not attempts:
            attempts.append(sql.split()[2])
            raise TrinoUserError({'errorName': name, 'errorType': 'USER_ERROR',
                                  'message': 'missing CTAS source', 'errorCode': code}, 'q_analysis')
        if sql.startswith('MERGE INTO'):
            self.sql, self.rows = sql, land(sql)
            return
        return execute(self, sql, params)
    with monkeypatch.context() as patch:
        patch.setattr(_TombstoneCursor, 'execute', reject)
        with pytest.raises(TrinoError, match='missing CTAS source'):
            commit()
        assert native_writer.write_journal.pending_statement(('stage-create', attempts[0])) is None
        assert replay()[0]['verified']
        assert not list((native_writer.write_journal._root() / 'abandoned-stages').glob('*/*.json'))
    assert warehouse.players() == ['2']
    assert warehouse.engine.execute('SHOW TABLES').fetchall() == [(plan.table,)]


def test_queued_merge_keeps_its_stage_and_retires_abandoned_sibling(staged_bundle, monkeypatch):
    # Round 11 finding 8. Both stages belong to one receipt, but only one feeds this MERGE.
    warehouse, db, plan, commit, replay, land = staged_bundle
    journal = native_writer.write_journal
    execute, queued = _TombstoneCursor.execute, []
    def delay(self, sql, params=None):
        if sql.startswith('MERGE INTO'):
            queued.append(sql)
            self.query_id = 'q_target_merge'
            raise ConnectionResetError('Connection reset after queued MERGE')
        return execute(self, sql, params)
    with monkeypatch.context() as patch:
        patch.setattr(_TombstoneCursor, 'execute', delay)
        with pytest.raises(TrinoAmbiguousWriteError):
            commit()
    target = journal.pending_statement(native_writer._target_scope(plan))
    retained = target['stage']
    receipt_id = target['expected']['single_submit']
    orphan = plan.table + '__stg_tm_' + 'a' * 32
    warehouse.engine.execute(f'CREATE TABLE {orphan} AS SELECT * FROM {plan.table} WHERE FALSE')
    journal._immutable_record(journal.stage_record(receipt_id, orphan), {
        'receipt_id': receipt_id, 'table': plan.full_table, 'stage': orphan, 'fenced_create': True})
    # Round 12 finding 8. The cleanup sweep must also honor the live MERGE
    # when an abandoned-stage record names its retained input.
    journal._immutable_record(journal._record_path('abandoned-stages/' + receipt_id, retained), {
        'run_id': 'next', 'stage': journal._read_record(journal.stage_record(receipt_id, retained))})
    with pytest.raises(RuntimeError, match='still pending'):
        replay()
    assert {row[0] for row in warehouse.engine.execute('SHOW TABLES').fetchall()} == {plan.table, retained}
    submission, = queued
    assert retained in submission
    land(submission)
    db.sql.execute("INSERT INTO test_query_history VALUES ('q_target_merge', 'FINISHED', NULL)")
    assert replay()[0]['verified']
    assert warehouse.engine.execute('SHOW TABLES').fetchall() == [(plan.table,)]


def test_restored_38_rows_do_not_release_a_queued_repair_merge(monkeypatch, tmp_path):
    # Round 12 finding 5
    from dataclasses import replace
    from tests.unit.scrapers import test_transfermarkt_native_writer as native

    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    db = native.CoreDB()
    plan = native._squad_observation('player_attribute_observations', native.CLOCK)
    plan = replace(plan, rows=tuple({**plan.rows[0], 'player_id': str(n)} for n in range(1, 39)))
    ack = native.write(db, [plan])
    db.sql.execute(f'CREATE TABLE original_rows AS SELECT * FROM {plan.table}')
    db.sql.execute(f'DELETE FROM {plan.table}')
    warehouse = _TombstoneWarehouse(plan.table, [dict(plan.rows[0])])
    warehouse.engine.execute(f'DELETE FROM {plan.table}')
    production = writer.TransfermarktIcebergWriter()
    production._trino_manager = _scripted(warehouse)
    monkeypatch.setattr(production._trino_manager, 'table_exists', lambda *args: True)
    monkeypatch.setattr(native.IcebergWriter, '_evolve_schema', lambda *args: None)
    journal = native_writer.write_journal
    queued = []
    run = warehouse.run

    def queue(sql):
        if sql.startswith('MERGE INTO'):
            queued.append(sql)
            raise ConnectionResetError('Connection reset after queued MERGE')
        return run(sql)

    def replay():
        return native_writer.replay_pending_intents(lambda: db, production, run_id='run-1')

    with monkeypatch.context() as patch:
        patch.setattr(warehouse, 'run', queue)
        with pytest.raises(TrinoAmbiguousWriteError):
            native_writer.write_core_bundle(lambda: db, production, [plan], run_id='run-1',
                                            universe_version='core-universe-v1', committed_at=native.CLOCK)
        target = journal.pending_statement(native_writer._target_scope(plan))
        assert journal.statement_query(target) == 'q_target_merge'
        stage = target['stage']
        db.sql.execute("INSERT INTO test_query_history VALUES ('q_target_merge', 'QUEUED', NULL)")
        db.sql.execute(f'INSERT INTO {plan.table} SELECT * FROM original_rows')
        with pytest.raises(RuntimeError, match='still pending'):
            replay()
        assert native_writer._target_matches(db, plan)
        assert journal.pending_statement(native_writer._target_scope(plan)) == target
        assert (stage,) in warehouse.engine.execute('SHOW TABLES').fetchall()
        assert journal.stage_record(target['expected']['single_submit'], stage).exists()
        submission, = queued
        run(submission)
        db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='q_target_merge'")
        db.sql.execute(f"DELETE FROM {plan.table} WHERE player_id='38'")
        with pytest.raises(RuntimeError, match='still pending'):
            replay()
        db.sql.execute(f"INSERT INTO {plan.table} SELECT * FROM original_rows WHERE player_id='38'")
        assert replay() == [ack]
    assert len(queued) == 1 and len(warehouse.players()) == 38
    assert warehouse.engine.execute('SHOW TABLES').fetchall() == [(plan.table,)]
    assert native._receipt_count(db) == 1 and not journal.pending_intents({})


@pytest.mark.parametrize('staged_bundle', [['2', '3']], indirect=True)
def test_pre_binding_stage_survives_loss_of_target_statement(staged_bundle, monkeypatch):
    # Round 13 finding 1
    from tests.unit.scrapers import test_transfermarkt_native_writer as native

    warehouse, db, plan, commit, replay, land = staged_bundle
    journal = native_writer.write_journal
    scope = native_writer._target_scope(plan)
    execute, queued = _TombstoneCursor.execute, []

    def lose_first_response(cursor, sql, params=None):
        if sql.startswith('MERGE INTO'):
            queued.append(sql)
            raise ConnectionResetError('Connection reset after accepted MERGE')
        return execute(cursor, sql, params)

    with monkeypatch.context() as patch:
        patch.setattr(_TombstoneCursor, 'execute', lose_first_response)
        with pytest.raises(TrinoAmbiguousWriteError):
            commit()
        statement = journal.pending_statement(scope)
        stage = statement['stage']
        assert journal.statement_query(statement) is None
        assert warehouse.engine.execute(f'SELECT count(*) FROM {stage}').fetchone() == (3,)
        # This archive predates the redundant dispatch-attempts record.
        journal._attempt_path(scope).unlink()
        journal._statement_path(scope).unlink()
        with pytest.raises(RuntimeError, match='still pending'):
            replay()
        with pytest.raises(RuntimeError, match='still pending'):
            native.write(db, [native.membership_plan(['4'], clock=native.NEWEST, capture='newer')], run_id='newer')
        assert journal.stage_record(statement['expected']['single_submit'], stage).exists()
        submission, = queued
        land(submission)
        with pytest.raises(RuntimeError, match='still pending'):
            replay()
        assert warehouse.players() == ['2', '3'] and native._receipt_count(db) == 1
        # Restoring the original bound evidence permits exact settlement.
        journal._immutable_record(journal._statement_path(scope), statement)
        journal.remember_query(statement, 'q_original')
        db.sql.execute("INSERT INTO test_query_history VALUES ('q_original','FINISHED',NULL)")
        assert replay()[0]['verified'] and len(queued) == 1
    assert warehouse.engine.execute('SHOW TABLES').fetchall() == [(plan.table,)]


def test_pre_walk_backup_with_scope_owner_cannot_repeat_single_page_repair(tmp_path, monkeypatch):
    # Round 14 finding 1
    _exercise_pre_walk_restore(tmp_path, monkeypatch, 1, False)


@pytest.mark.parametrize(('page_count', 'lost_receipt'), [(1, True), (2, False)])
def test_orphan_walk_fences_repair_until_verified_target_survives_unlink(tmp_path, monkeypatch, page_count, lost_receipt):
    # Round 15 finding 1
    _exercise_pre_walk_restore(tmp_path, monkeypatch, page_count, lost_receipt)


@pytest.mark.parametrize('unresolved_walk', [False, True])
def test_verified_walk_reacquires_its_missing_scope(tmp_path, monkeypatch, unresolved_walk):
    # Round 16 finding 5
    _exercise_pre_walk_restore(tmp_path, monkeypatch, 1, False, restore_owner=True,
                               unresolved_walk=unresolved_walk)


def test_restore_missing_registration_for_bound_merge_input(tmp_path, monkeypatch):
    # Round 16 finding 7
    _exercise_pre_walk_restore(tmp_path, monkeypatch, 1, False, restore_stage=True)


def _exercise_pre_walk_restore(tmp_path, monkeypatch, page_count, lost_receipt, *,
                               restore_owner=False, restore_stage=False, unresolved_walk=False):
    import shutil
    from tests.unit.scrapers import test_transfermarkt_native_writer as native
    from tests.unit.scrapers import test_transfermarkt_sections as lanes

    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    db = native.CoreDB()
    journal = native_writer.write_journal
    body = (lanes.ATTENDANCE_FIXTURES / 'single_page.html').read_bytes()

    def snapshot(name, clock):
        if page_count == 2:
            start, tail, page = native._walk_pages()
            return lanes._attendance_plan((page(start, 'page_1.html', clock, name + '1'),
                                           page(tail, 'page_2.html', clock, name + '2')), extracted_at=clock)
        page = lanes._page(lanes.ATTENDANCE_START, body + name.encode(), clock, name)
        return lanes._attendance_plan((page,), extracted_at=clock)

    a, b, c = snapshot('A', native.EARLIER), snapshot('B', native.CLOCK), snapshot('C', native.NEWEST)
    assert lanes._bundle(db, [a], native.EARLIER)['verified']
    root = journal._root()
    backup = tmp_path / 'before-walk-b'
    before_owner = tmp_path / 'before-scope-b'
    record_pages = native_writer._record_pages
    write_plan = native_writer._write_plan

    def backup_before_owner(*args):
        shutil.copytree(root, before_owner)
        return write_plan(*args)

    def backup_before_walk(*args, **kwargs):
        shutil.copytree(root, backup)
        record_pages(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_record_pages', backup_before_walk)
        if restore_owner:
            patch.setattr(native_writer, '_write_plan', backup_before_owner)
        ack = lanes._bundle(db, [b], native.CLOCK)
    key = ack['walks'][0]['receipt_id']
    scope = native_writer._target_scope(b)
    warehouse = _TombstoneWarehouse(b.table, [dict(row) for row in a.rows])
    warehouse.lost = True
    native._copy_warehouse_scope(warehouse, db, a)
    assert native_writer._target_matches(db, a)
    assert native_writer._stored_receipts(db, [key]) and native_writer._stored_walk(db, key)
    production = writer.TransfermarktIcebergWriter()
    production._trino_manager = _scripted(warehouse)
    monkeypatch.setattr(production._trino_manager, 'table_exists', lambda *args: True)
    monkeypatch.setattr(native.IcebergWriter, '_evolve_schema', lambda *args: None)
    run_sql, submissions = warehouse.run, []

    def dispatch(sql):
        if sql.startswith('MERGE INTO'):
            submissions.append(sql)
            if len(submissions) == 1:
                db.sql.execute("INSERT INTO test_query_history VALUES ('q_target_merge','QUEUED',NULL)")
                raise ConnectionResetError('Connection reset after accepted repair MERGE')
            result = run_sql(sql)
            native._copy_warehouse_scope(warehouse, db, b)
            return result
        return run_sql(sql)

    monkeypatch.setattr(warehouse, 'run', dispatch)

    def commit(plan):
        return native_writer.write_core_bundle(lambda: db, production, [plan], run_id=plan.rows[0]['run_id'],
                                               universe_version=lanes.UNIVERSE, committed_at=plan.captured_at)

    def replay_b():
        return native_writer.replay_pending_intents(lambda: db, production, run_id=ack['run_id'])

    with pytest.raises(TrinoAmbiguousWriteError):
        commit(b)
    statement = journal.pending_statement(scope)
    assert journal.statement_query(statement) == 'q_target_merge'
    stage = statement['stage']
    stage_path = journal.stage_record(key, stage)
    stage_record = journal._read_record(stage_path)
    root.rename(tmp_path / 'in-flight')
    shutil.copytree(backup, root)
    if lost_receipt:
        db.sql.execute(f'CREATE TABLE saved_receipt AS SELECT * FROM {native.RECEIPTS} WHERE receipt_id=?', (key,))
        db.sql.execute(f'DELETE FROM {native.RECEIPTS} WHERE receipt_id=?', (key,))
    (intent, _), = journal.pending_intents({'run_id': ack['run_id']})
    assert journal.read_receipt(ack['plan_sha256']) is None
    assert journal.scope_owner(scope) == key and journal.pending_statement(scope) is None
    try:
        with pytest.raises(native.StaleTransfermarktWrite, match='local dispatch history'):
            replay_b()
        with pytest.raises(native.StaleTransfermarktWrite):
            commit(c)
        assert intent.exists() and journal.read_receipt(ack['plan_sha256']) is None
        assert journal.scope_owner(scope) == key and native_writer._target_matches(db, a)
        assert (stage,) in warehouse.engine.execute('SHOW TABLES').fetchall()
        assert native.write(db, [native._attendance_edition(c, '2024')], run_id='disjoint',
                            clock=native.NEWEST)['verified']
    finally:
        run_sql(submissions[0])
        native._copy_warehouse_scope(warehouse, db, b)
    assert len(submissions) == 1 and native_writer._target_matches(db, b)
    assert warehouse.engine.execute(f'SELECT club_id, COUNT(*) FROM {b.table} GROUP BY club_id '
                                    'ORDER BY club_id').fetchall() == [('11', 1), ('631', 1)]
    # Exact Bronze cells still cannot settle the orphan. Recover just the
    # bound attempt, its coordinator ID and its retained stage from the backup.
    with pytest.raises(native.StaleTransfermarktWrite, match='local dispatch history'):
        replay_b()
    journal._immutable_record(journal._attempt_path(scope), statement)
    journal.remember_query(statement, 'q_target_merge')
    if restore_stage:
        assert not stage_path.exists()
        # A genuine name with a conflicting receipt binding cannot become a DROP.
        journal._immutable_record(stage_path, {**stage_record, 'receipt_id': 'wrong'})
        with pytest.raises(RuntimeError, match='stage identity differs'):
            replay_b()
        assert (stage,) in warehouse.engine.execute('SHOW TABLES').fetchall()
        stage_path.unlink()
    else:
        journal._immutable_record(stage_path, stage_record)
    with pytest.raises(RuntimeError, match='still pending'):
        replay_b()
    assert journal.read_statement(scope) == statement
    assert stage_path.exists() == (not restore_stage)
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='q_target_merge'")
    if lost_receipt:
        # Round 27 finding 3
        files = native._r27_dispatch_files(statement)
        before = native._r26_file_bytes(files)
        with pytest.raises(RuntimeError, match='receipt readback differs'):
            replay_b()
        assert native._r26_file_bytes(files) == before and not journal.terminal_target(intent.stem, key)
        db.sql.execute(f'INSERT INTO {native.RECEIPTS} SELECT * FROM saved_receipt')
    # Round 15 finding 5
    finish = journal.finish_statement

    def interrupt_after_target(resolved):
        finish(resolved)
        if resolved == statement:
            raise RuntimeError('after verified target unlink')

    with monkeypatch.context() as patch:
        patch.setattr(journal, 'finish_statement', interrupt_after_target)
        with pytest.raises(RuntimeError, match='after verified target unlink'):
            replay_b()
    assert journal.pending_statement(scope) is None and intent.exists()
    assert journal._read_record(stage_path) == stage_record
    db.sql.execute("DELETE FROM test_query_history WHERE query_id='q_target_merge'")
    assert journal.terminal_target(intent.stem, key, statement)
    if restore_owner:
        root.rename(tmp_path / 'after-target-unlink')
        shutil.copytree(before_owner, root)
        assert journal.scope_owner(scope) is None and journal.pending_statement(scope) is None
        journal.hold_scope(scope, 'competing')
        with pytest.raises(native.StaleTransfermarktWrite):
            replay_b()
        assert journal.scope_owner(scope) == 'competing'
        journal.release_scope(scope, 'competing')
        if unresolved_walk:
            # The same packet could have dispatched a repair of A's historical walk.
            historical = next(k for k in native_writer._scope_receipt_ids(db, b) if k != key)
            operation = {**statement['operation'], 'member': historical, 'plan_sha256': None}
            page_key = (native_writer.CORE_PAGES_TABLE, historical)
            held = native_writer._stored_walk(db, historical)
            pending = journal.begin_statement(page_key, {'single_submit': native_writer._digest(held)},
                                              observable=False, operation=operation)
            with pytest.raises(RuntimeError, match='still pending'):
                replay_b()
            assert journal.pending_statement(page_key) == pending and journal.scope_owner(scope) is None
            assert journal.read_receipt(ack['plan_sha256']) is None
            return
    db.sql.execute(f"UPDATE {b.table} SET club_id='wrong' WHERE club_id='11'")
    with pytest.raises(RuntimeError, match='still pending'):
        replay_b()
    assert journal.read_receipt(ack['plan_sha256']) is None and len(submissions) == 1
    if restore_owner:
        assert journal.scope_owner(scope) is None
    db.sql.execute(f"UPDATE {b.table} SET club_id='11' WHERE club_id='wrong'")
    if lost_receipt:
        # Round 25 finding 2
        # The later FINISHED target binds this repair to an existing publication.
        db.sql.execute(f'DELETE FROM {native.RECEIPTS} WHERE receipt_id=?', (key,))
        start = len(db.events)
        with pytest.raises(RuntimeError, match='receipt readback differs'):
            replay_b()
        assert intent.exists() and not native_writer._stored_receipts(db, [key])
        assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
        db.sql.execute(f'INSERT INTO {native.RECEIPTS} SELECT * FROM saved_receipt')
    assert replay_b() == [ack]
    assert len(submissions) == 1 and not intent.exists()
    assert warehouse.engine.execute('SHOW TABLES').fetchall() == [(b.table,)]


@pytest.mark.parametrize('roster_receipt', [True, False])
@pytest.mark.parametrize('legacy_observable', [False, True, 'already-observed'])
def test_all_squad_receipts_survive_pre_target_restore(tmp_path, monkeypatch, legacy_observable, roster_receipt):
    # Round 16 finding 1
    import shutil
    from dataclasses import replace
    from tests.unit.scrapers import test_transfermarkt_native_writer as native

    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    journal, db = native_writer.write_journal, native.CoreDB()
    players = [str(n) for n in range(1, 39)]
    roster = native.membership_plan(players, capture='squad-body', run='squad')
    plans = [roster]
    for entity in ('player_attribute_observations', 'player_contract_observations'):
        plan = native._squad_observation(entity, native.CLOCK)
        plans.append(replace(plan, rows=tuple({**plan.rows[0], 'player_id': p} for p in players)))

    root, backup = journal._root(), tmp_path / 'before-target'
    write_plan = native_writer._write_plan

    def snapshot(*args):
        if not backup.exists():
            shutil.copytree(root, backup)
        return write_plan(*args)

    with monkeypatch.context() as patch:
        patch.setattr(native_writer, '_write_plan', snapshot)
        ack = native.write(db, plans)
    assert native._receipt_count(db) == 3
    # Round 17 finding 1
    if not roster_receipt:
        db.sql.execute(f'DELETE FROM {native.RECEIPTS} WHERE entity=?', (roster.entity,))
        assert native._receipt_count(db) == 2
    root.rename(tmp_path / 'published')
    shutil.copytree(backup, root)
    warehouse = _TombstoneWarehouse(roster.table, [dict(r) for r in roster.rows])
    warehouse.engine.execute(f'CREATE TEMP TABLE exact AS SELECT * FROM {roster.table}')
    warehouse.engine.execute(f'DELETE FROM {roster.table}')
    native._copy_warehouse_scope(warehouse, db, roster)
    production = writer.TransfermarktIcebergWriter()
    production._trino_manager = _scripted(warehouse)
    monkeypatch.setattr(production._trino_manager, 'table_exists', lambda *args: True)
    monkeypatch.setattr(native.IcebergWriter, '_evolve_schema', lambda *args: None)
    run, queued = warehouse.run, []

    def delay(sql):
        if sql.startswith('MERGE INTO'):
            queued.append(sql)
            raise ConnectionResetError('Connection reset after queued roster MERGE')
        return run(sql)

    def replay():
        return native_writer.replay_pending_intents(lambda: db, production, run_id='run-1')

    def counts():
        return db.sql.execute(f'SELECT count(*), count(DISTINCT player_id) FROM {roster.table} '
                              f'WHERE {roster.scope_sql()}', roster.scope_values).fetchone()

    scope = native_writer._target_scope(roster)
    # Round 20 finding 1. Today's writer sees the retired original sequence.
    start = len(db.events)
    with pytest.raises(RuntimeError, match='physical readback differs'):
        replay()
    assert not warehouse.merges and not any(native_writer.is_mutation(sql) for sql in db.events[start:])
    with monkeypatch.context() as patch:
        # Model the already submitted MERGE from r19 for the late-landing checks below.
        def retired_prefix(head, intent, plans, members):
            assert journal.submission_retired(head) and head['statement']['operation']['intent'] == intent
            return None

        patch.setattr(native_writer, '_receipt_prefix', retired_prefix)
        patch.setattr(warehouse, 'run', delay)
        if legacy_observable:
            begin = journal.begin_statement

            def old_statement(key, expected, **kwargs):
                if key == scope:
                    kwargs['observable'] = True
                return begin(key, expected, **kwargs)

            patch.setattr(journal, 'begin_statement', old_statement)
        with pytest.raises(TrinoAmbiguousWriteError):
            replay()
    target = journal.pending_statement(scope)
    assert target['observable'] is bool(legacy_observable) and journal.statement_query(target) == 'q_target_merge'
    db.sql.execute("INSERT INTO test_query_history VALUES ('q_target_merge', 'QUEUED', NULL)")
    warehouse.engine.execute(f'INSERT INTO {roster.table} SELECT * FROM exact')
    native._copy_warehouse_scope(warehouse, db, roster)
    assert counts() == (38, 38)
    if legacy_observable == 'already-observed':
        # An earlier writer could have released the observation-based fence
        # before the packet's surviving receipt evidence was considered.
        with native_writer.writer_lock():
            journal.note_observed(target)
            journal.finish_statement(target)
    with pytest.raises(RuntimeError, match='still pending'):
        replay()
    assert journal.pending_statement(scope) == (None if legacy_observable == 'already-observed' else target)
    assert journal.read_receipt(ack['plan_sha256']) is None
    assert native.write(db, [native._club_plan('77', 'disjoint', native.NEWEST)], run_id='other',
                        clock=native.NEWEST)['verified']
    submission, = queued
    run(submission)
    db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='q_target_merge'")
    native._copy_warehouse_scope(warehouse, db, roster)
    with pytest.raises(RuntimeError, match='still pending'):
        replay()
    assert counts() == (76, 38) and journal.read_receipt(ack['plan_sha256']) is None


@pytest.mark.parametrize('boundary', ['old_receipt', 'old_target', 'after_publication'])
def test_pair_restore_fences_queued_attendance_merge(tmp_path, monkeypatch, boundary):
    # Round 16 finding 2
    from tests.unit.scrapers import test_transfermarkt_native_writer as native

    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    db, journal = native.CoreDB(), native_writer.write_journal
    old = native._snapshot(native.EARLIER, 'old')
    plan = native._snapshot(native.CLOCK, 'replacement')
    native.write(db, [old], run_id='old', clock=native.EARLIER)
    scope = native_writer._target_scope(plan)
    backup = tmp_path / 'original'
    finish = journal.finish_statement

    def backup_finished(statement, **options):
        table = statement['key'][0]
        if table == {'old_receipt': native_writer.CORE_RECEIPTS_TABLE, 'old_target': scope[0]}.get(boundary):
            journal.remember_query(statement, boundary)
            db.sql.execute('INSERT INTO test_query_history VALUES (?, ?, NULL)', (boundary, 'FINISHED'))
            native._backup_pair(backup)
        return finish(statement, **options)

    with monkeypatch.context() as patch:
        patch.setattr(journal, 'finish_statement', backup_finished)
        ack = native.write(db, [plan])
    # Round 17 finding 2
    if boundary == 'after_publication':
        native._backup_pair(backup)
    warehouse = _TombstoneWarehouse(plan.table, [dict(row) for row in old.rows])
    warehouse.lost = True
    native._copy_warehouse_scope(warehouse, db, old)
    production = writer.TransfermarktIcebergWriter()
    production._trino_manager = _scripted(warehouse)
    monkeypatch.setattr(production._trino_manager, 'table_exists', lambda *args: True)
    monkeypatch.setattr(native.IcebergWriter, '_evolve_schema', lambda *args: None)
    execute, submissions = warehouse.run, []

    def queue(sql):
        if sql.startswith('MERGE INTO'):
            submissions.append(sql)
            db.sql.execute("INSERT INTO test_query_history VALUES ('q_target_merge','QUEUED',NULL)")
            raise ConnectionResetError('Connection reset after accepted MERGE')
        return execute(sql)

    def retry():
        return native_writer.write_core_bundle(lambda: db, production, [plan], run_id='run-1',
                                               universe_version='core-universe-v1', committed_at=native.CLOCK)

    def replay():
        return native_writer.replay_pending_intents(lambda: db, production, run_id='run-1')

    with monkeypatch.context() as patch:
        patch.setattr(warehouse, 'run', queue)
        with pytest.raises(TrinoAmbiguousWriteError):
            retry()
        native._restore_pair(backup, tmp_path / 'queued')
        start = len(db.events)
        if boundary == 'after_publication':
            assert not journal.pending_intents({}) and replay() == []
        for call in ((retry,) if boundary == 'after_publication' else (replay, replay, retry)):
            with pytest.raises(RuntimeError, match='still pending'):
                call()
        with pytest.raises(native.StaleTransfermarktWrite):
            native.write(db, [native._snapshot(native.NEWEST, 'fresh')], run_id='fresh', clock=native.NEWEST)
        assert native_writer._target_matches(db, old)
        assert bool(journal.read_receipt(ack['plan_sha256'])) == (boundary == 'after_publication')
        assert not any(native_writer.is_mutation(sql) for sql in db.events[start:])
        submission, = submissions
        execute(submission)
        native._copy_warehouse_scope(warehouse, db, plan)
        db.sql.execute("UPDATE test_query_history SET state='FINISHED' WHERE query_id='q_target_merge'")
        assert native_writer._target_matches(db, plan)
        with pytest.raises(RuntimeError, match='still pending'):
            retry()
        assert bool(journal.read_receipt(ack['plan_sha256'])) == (boundary == 'after_publication')
        assert len(submissions) == 1
    assert native.write(db, [native._attendance_edition(plan, '2024')], run_id='disjoint')['verified']


def test_returned_merge_recovers_only_its_bound_stage(tmp_path, monkeypatch):
    # Round 17 finding 7
    from tests.unit.scrapers import test_transfermarkt_native_writer as native

    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    db, journal = native.CoreDB(), native_writer.write_journal
    old, plan = native._snapshot(native.EARLIER, 'old'), native._snapshot(native.CLOCK, 'next')
    native.write(db, [old], run_id='old', clock=native.EARLIER)
    warehouse = _TombstoneWarehouse(plan.table, [dict(row) for row in old.rows])
    warehouse.lost = True
    production = writer.TransfermarktIcebergWriter()
    production._trino_manager = _scripted(warehouse)
    monkeypatch.setattr(production._trino_manager, 'table_exists', lambda *args: True)
    monkeypatch.setattr(native.IcebergWriter, '_evolve_schema', lambda *args: None)
    sentinel = plan.table + '__stg_tm_' + 'a' * 32
    warehouse.engine.execute(f'CREATE TABLE {sentinel} AS SELECT * FROM {plan.table}')
    scope = native_writer._target_scope(plan)
    finish = journal.finish_statement

    def stop(statement, **kwargs):
        finish(statement, **kwargs)
        if statement['key'] == list(scope):
            native._copy_warehouse_scope(warehouse, db, plan)
            raise RuntimeError('before stage DROP')

    with monkeypatch.context() as patch:
        patch.setattr(journal, 'finish_statement', stop)
        with pytest.raises(RuntimeError, match='before stage DROP'):
            native_writer.write_core_bundle(lambda: db, production, [plan], run_id='run-1',
                                            universe_version='core-universe-v1', committed_at=native.CLOCK)
    operation, = journal.scope_operations(scope)
    assert operation['state'] == 'returned' and operation['query_id'] == 'q_target_merge'
    stage = operation['statement']['stage']
    assert (stage,) in warehouse.engine.execute('SHOW TABLES').fetchall()
    (journal._root() / 'stages').rename(tmp_path / 'old-registry')
    assert native_writer.replay_pending_intents(lambda: db, production)[0]['verified']
    assert native_writer.replay_pending_intents(lambda: db, production) == []
    assert set(warehouse.engine.execute('SHOW TABLES').fetchall()) == {(plan.table,), (sentinel,)}
    assert not list(journal.retained_stages(operation['operation']['member']))


@pytest.mark.parametrize('boundary', ['original', 'repair', 'before-reservation', 'reserved-tail', 'rejected-mapped', 'rejected-cleanup',
                                      'linked-worklist', 'linked-general', 'linked-original', 'general-with-original',
                                      'general-before-receipt-reservation', 'exact-step-predecessor',
                                      'retired-receipt-predecessor', 'reserved-general-predecessor',
                                      'reserved-worklist-predecessor', 'reserved-step-predecessor', 'completed-worklist-predecessor'])
def test_two_retrying_writers_submit_one_receipt_tail(tmp_path, monkeypatch, boundary):
    # Round 19 finding 1
    # Round 19 finding 2
    # Round 21 finding 4
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from tests.unit.scrapers import test_transfermarkt_native_writer as native

    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    db = native.CoreDB()
    shared = sqlite3.connect(':memory:', check_same_thread=False)
    db.sql.backup(shared)
    db.sql.close()
    db.sql = shared
    plans = native._representation_packet(('7', '9'), native.CLOCK)
    if boundary == 'repair':
        native.write(db, plans)
        db.sql.execute(f'DELETE FROM {native.RECEIPTS}')
    hashes, digest = native_writer._bundle_identity(plans, 'run-1')
    members = [native_writer._receipt_id(h, 'run-1', digest) for h in hashes]
    if boundary in ('before-reservation', 'reserved-tail'):
        # Round 24 finding 2
        # Round 23 finding 3
        native.write(db, plans)
        db.sql.execute(f'DELETE FROM {native.RECEIPTS} WHERE receipt_id=?', (members[-1],))
        with monkeypatch.context() as patch:
            if boundary == 'before-reservation':
                patch.setattr(native.write_journal, 'reserve_submission', native._r22_stop)
            else:
                patch.setattr(native_writer, '_merge_receipts', native._r22_stop)
            with pytest.raises(OSError):
                native.write(db, plans)
        head = native.write_journal.submission_head(members[0])
        if boundary == 'before-reservation':
            (_, body), = native.write_journal.pending_intents({})
            assert body['payload']['predecessor_head'] == native_writer._digest(head)
        else:
            assert 'statement' not in head and head['receipt_repair']['missing'] == [members[-1]]
    elif boundary == 'completed-worklist-predecessor':
        # Round 36 finding 1
        _, plans, _, _, record, attempts = native._r32_repaired_packet_missing_receipt(monkeypatch, db=db)
        native._r32_stop_before_receipt_reservation(monkeypatch, lambda: native.write(db, plans))
        db.sql.execute(f'INSERT INTO {native.RECEIPTS} SELECT * FROM nine')
        native._r28_interrupt(monkeypatch, native.write_journal, 'release_obsolete_guards', lambda: native.write(db, plans))
        (completed, _), = native.write_journal.pending_intents({})
        db.sql.execute(f'DELETE FROM {native.RECEIPTS} WHERE receipt_id=?', (members[-1],))
        record.write_bytes(attempts[1])
    elif boundary == 'reserved-worklist-predecessor':
        # Round 35 finding 5
        _, plans, _, _, record, attempts = native._r32_repaired_packet_missing_receipt(monkeypatch, db=db)
        native._r28_interrupt(monkeypatch, native_writer, '_merge_receipts', lambda: native.write(db, plans))
        record.write_bytes(attempts[1])
    elif boundary == 'reserved-step-predecessor':
        # Round 35 finding 6
        _, record = native._r33_restore_step_predecessor(db, plans, monkeypatch, unreserved=False)
        previous = record.read_bytes()
        record.unlink()
        table = plans[0].table
        db.sql.execute(f"CREATE TABLE seven AS SELECT * FROM {table} WHERE player_id='7'")
        db.sql.execute(f"DELETE FROM {table} WHERE player_id='7'")
        native._r28_interrupt(monkeypatch, native.write_journal, 'reserve_scopes', lambda: native.write(db, plans))
        db.sql.execute(f'INSERT INTO {table} SELECT * FROM seven')
        record.write_bytes(previous)
    elif boundary == 'reserved-general-predecessor':
        # Round 34 finding 1
        _, plans, _, _, record, attempts = native._r32_repaired_packet_missing_receipt(monkeypatch, db=db)
        table = plans[0].table
        db.sql.execute(f"CREATE TABLE seven AS SELECT * FROM {table} WHERE player_id='7'")
        db.sql.execute(f"DELETE FROM {table} WHERE player_id='7'")
        native._r28_interrupt(monkeypatch, native.write_journal, 'reserve_scopes', lambda: native.write(db, plans))
        db.sql.execute(f'INSERT INTO {table} SELECT * FROM seven')
        record.write_bytes(attempts[1])
    elif boundary == 'retired-receipt-predecessor':
        # Round 34 finding 5
        native._r34_restore_receipt_predecessor(db, plans, monkeypatch)
    elif boundary == 'exact-step-predecessor':
        # Round 33 finding 7
        native._r33_restore_step_predecessor(db, plans, monkeypatch, unreserved=True)
    elif boundary == 'general-before-receipt-reservation':
        # Round 32 finding 6
        native.write(db, plans)
        db.sql.execute(f"DELETE FROM {plans[0].table} WHERE player_id='7'")
        db.sql.execute(f'DELETE FROM {native.RECEIPTS} WHERE receipt_id=?', (members[0],))
        native._r32_stop_before_receipt_reservation(monkeypatch, lambda: native.write(db, plans))
    elif boundary == 'general-with-original':
        # Round 31 finding 7
        native._r28_overlapping_worklists(tmp_path, monkeypatch, db, plans, 'original', general=True)
    elif boundary.startswith('linked-'):
        # Round 29 finding 9
        # Round 28 finding 7
        native._r28_overlapping_worklists(tmp_path, monkeypatch, db, plans, boundary.removeprefix('linked-'))
    elif boundary.startswith('rejected-'):
        # Round 22 finding 3
        native._restore_rejected_tail_mapping(monkeypatch, db, plans, members[-1], cleanup=boundary == 'rejected-cleanup')
    else:
        with monkeypatch.context() as patch:
            submitted = native._interrupt_ops_insert(patch, db, native_writer.CORE_RECEIPTS_TABLE, 'first',
                                                      landed=True, member=members[0])
            with pytest.raises(TrinoAmbiguousWriteError):
                native.write(db, plans)
        assert len(submitted) == 1
    start = len(db.events)
    ready = Barrier(2)

    def retry():
        writer = native.CoreWriter(db)
        ready.wait(timeout=10)
        ack = native_writer.write_core_bundle(lambda: db, writer, plans, run_id='run-1',
                                              universe_version='core-universe-v1', committed_at=native.CLOCK)
        return ack, writer.writes

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = [pool.submit(retry) for _ in range(2)]
        ack, writes = first.result(timeout=20)
        assert ack['verified'] and writes == [] and second.result(timeout=20) == (ack, [])
    inserts = [sql for sql in db.events[start:] if sql.startswith('INSERT INTO ')]
    assert len(inserts) == 1 and inserts[0].startswith('INSERT INTO ' + native_writer.CORE_RECEIPTS_TABLE)
    assert native._receipt_count(db) == 2 and native_writer._stored_receipts(db, members).keys() == set(members)
    assert not native.write_journal.pending_intents({})
    if boundary == 'completed-worklist-predecessor':
        assert native.write_journal.submission_head(members[0])['statement']['operation']['intent'] != completed.stem
    db.sql.close()
