"""Offline acceptance for TM committing boundaries and physical capture refs."""
from datetime import date, datetime, timezone
import multiprocessing
import sqlite3
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from scrapers.transfermarkt import writer
from scrapers.transfermarkt.write_intents import save_intent, pending_intents, unpack_frames, finish_intent
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
    scraper._bronze_connection.return_value.cursor.return_value.fetchall.return_value = [('1', datetime(2026, 10, 2))]
    scraper._build_partition_delete_filter.return_value = "player_id = '1'"
    with pytest.raises(writer.StaleTransfermarktWrite):
        run._save_frames(scraper, spec, {'market_value_points': frame, 'legacy_market_value_history': frame}, False, {'outputs': {}, 'tables': []})
    scraper.save_to_iceberg.assert_not_called()


def test_dq_detects_physical_keys_across_batches_and_latest_reference_loss():
    for table in dq.CAREER_WRITE_TABLE_KEYS:
        sql = dq.build_career_write_duplicates_sql(table)
        assert 'HAVING COUNT(*) > 1' in sql and '_batch_id' not in sql
    loss = dq.build_career_write_loss_sql('iceberg.bronze.transfermarkt_market_value_points')
    assert 'latest_refs' in loss and 'COALESCE(l.actual_rows, 0) <> r.expected_rows' in loss
    assert 'empty_receipt.last_success_at >= r.committed_at' in loss


def test_shared_maintenance_preserves_tm_original_snapshots_and_other_thresholds(monkeypatch):
    from utils import maintenance_tasks as maintenance
    conn = MagicMock()
    monkeypatch.setattr(writer, 'shared_writer_lock_ready', lambda: True)
    monkeypatch.setattr(maintenance, '_fetch_scalar', lambda _conn, _sql: 3)
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
