"""Stage-B characterization for the writer primitives being moved (#2404).

These tests freeze the exact behaviour of the lock/commit-retry surface of
``scrapers.transfermarkt.writer`` BEFORE the code moves into the mono-native
writer module. The move (and the donor facade that keeps ``writer`` imports
working) must keep every test here green without editing it.
"""
import threading
import time
from unittest.mock import MagicMock

import pytest

from scrapers.transfermarkt import writer


pytestmark = pytest.mark.unit

# The public surface the mono-native writer must keep providing, verbatim,
# after the move (writer.py stays as the donor facade).
MOVED_PRIMITIVES = (
    'writer_lock',
    'writer_deadline',
    'execute_statement',
    'is_mutation',
    'CommittingCursor',
    'CommittingConnection',
    'locked_write',
    'StaleTransfermarktWrite',
)


@pytest.fixture(autouse=True)
def isolated_lock_path(monkeypatch, tmp_path):
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)


@pytest.mark.parametrize('name', MOVED_PRIMITIVES)
def test_writer_module_exposes_every_moved_primitive(name):
    assert callable(getattr(writer, name))


class Conflict(RuntimeError):
    error_name = 'ICEBERG_COMMIT_ERROR'


def test_writer_lock_is_reentrant_within_one_thread():
    with writer.writer_lock():
        with writer.writer_lock():
            with writer.writer_lock():
                pass
    # After the outermost exit the lock is free for another thread/process.
    acquired = []

    def grab():
        with writer.writer_lock():
            acquired.append(True)

    thread = threading.Thread(target=grab)
    thread.start()
    thread.join(timeout=5)
    assert acquired == [True]


def test_writer_lock_blocks_another_thread_until_release():
    # The thread mutex serializes in-process contenders before any flock
    # polling, so a second thread simply waits for the holder to finish.
    with writer.writer_lock():
        started = threading.Event()
        acquired = []

        def blocked():
            started.set()
            with writer.writer_lock():
                acquired.append(True)

        thread = threading.Thread(target=blocked)
        thread.start()
        started.wait(timeout=5)
        thread.join(timeout=0.3)
        assert thread.is_alive() and acquired == []
    thread.join(timeout=5)
    assert acquired == [True]


def _lock_holder(lock_path, ready_path):
    from scrapers.transfermarkt import writer as holder_writer
    import os
    os.environ['TM_WRITER_LOCK_PATH'] = str(lock_path)
    with holder_writer.writer_lock():
        ready_path.write_text('held')
        time.sleep(30)


def test_writer_lock_deadline_expires_while_another_process_holds_it(monkeypatch, tmp_path):
    import multiprocessing
    lock_path = tmp_path / 'cross.lock'
    ready_path = tmp_path / 'ready'
    context = multiprocessing.get_context('fork')
    process = context.Process(target=_lock_holder, args=(str(lock_path), ready_path))
    process.start()
    try:
        for _ in range(100):
            if ready_path.exists():
                break
            time.sleep(0.05)
        assert ready_path.exists()
        monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(lock_path))
        with writer.writer_deadline(time.monotonic() - 1):
            with pytest.raises(TimeoutError, match='writer lock deadline exceeded'):
                with writer.writer_lock():
                    raise AssertionError('lock must not be acquired')
    finally:
        process.terminate()
        process.join(timeout=5)


def test_writer_deadline_restores_the_previous_value():
    sentinel = 12345.0
    with writer.writer_deadline(sentinel):
        assert getattr(writer._LOCAL, 'deadline', None) == sentinel
        with writer.writer_deadline(None):
            assert getattr(writer._LOCAL, 'deadline', None) is None
        assert getattr(writer._LOCAL, 'deadline', None) == sentinel
    assert getattr(writer._LOCAL, 'deadline', None) is None


def test_execute_committing_retries_only_commit_conflicts_with_backoff(monkeypatch):
    sleeps = []
    monkeypatch.setattr(writer.time, 'sleep', sleeps.append)
    operation = MagicMock(side_effect=[Conflict('first'), Conflict('second'), 'done'])
    assert writer._execute_committing(operation) == 'done'
    assert operation.call_count == 3
    assert sleeps and all(delay > 0 for delay in sleeps)


def test_execute_committing_stops_retrying_unrelated_failures(monkeypatch):
    monkeypatch.setattr(writer.time, 'sleep', lambda _: None)
    operation = MagicMock(side_effect=RuntimeError('connection refused'))
    with pytest.raises(RuntimeError, match='connection refused'):
        writer._execute_committing(operation)
    assert operation.call_count == 1


def test_execute_committing_bounds_attempts_at_four(monkeypatch):
    monkeypatch.setattr(writer.time, 'sleep', lambda _: None)
    operation = MagicMock(side_effect=Conflict('persistent'))
    with pytest.raises(Conflict):
        writer._execute_committing(operation, attempts=4)
    assert operation.call_count == 4


def test_execute_committing_honours_the_deadline_before_sleeping(monkeypatch):
    monkeypatch.setattr(writer.time, 'sleep', lambda _: None)
    operation = MagicMock(side_effect=Conflict('conflict'))
    with writer.writer_deadline(time.monotonic()):
        with pytest.raises(TimeoutError, match='commit retry deadline'):
            writer._execute_committing(operation)
    assert operation.call_count == 1


def test_execute_statement_passes_reads_straight_to_the_cursor():
    cursor = MagicMock()
    writer.execute_statement(cursor, 'SELECT 1')
    cursor.execute.assert_called_once_with('SELECT 1')
    cursor.fetchall.assert_not_called()
    writer.execute_statement(cursor, 'SELECT ? WHERE true', (1,))
    cursor.execute.assert_called_with('SELECT ? WHERE true', (1,))


def test_execute_statement_drains_mutation_results_and_retries_conflicts(monkeypatch):
    monkeypatch.setattr(writer.time, 'sleep', lambda _: None)
    cursor = MagicMock()
    cursor.fetchall.side_effect = [Conflict('poll conflict'), [(3,)]]
    assert writer.execute_statement(cursor, "DELETE FROM iceberg.bronze.t WHERE x = 'y'") == [(3,)]
    assert cursor.execute.call_count == cursor.fetchall.call_count == 2


@pytest.mark.parametrize('sql', [
    'MERGE INTO t USING x ON true WHEN MATCHED THEN DELETE',
    '  /* planner hint */ INSERT INTO t VALUES (1)',
    'insert into t values (1)',
    'DELETE FROM t',
    'UPDATE t SET x = 1',
    'CREATE TABLE t (x int)',
    'ALTER TABLE t ADD COLUMN y int',
    'DROP TABLE t',
    'TRUNCATE TABLE t',
])
def test_is_mutation_recognises_every_mutation_knee(sql):
    assert writer.is_mutation(sql)


@pytest.mark.parametrize('sql', [
    'SELECT * FROM t',
    'WITH cte AS (SELECT 1) SELECT * FROM cte',
    'SHOW TABLES',
    'DESCRIBE t',
    ' -- comment\nSELECT 1',
])
def test_is_mutation_never_flags_reads(sql):
    assert not writer.is_mutation(sql)


def test_committing_cursor_serves_mutation_rows_exactly_once():
    raw = MagicMock()
    raw.fetchall.return_value = [(4,)]
    adapted = writer.CommittingCursor(raw)
    result = adapted.execute('MERGE INTO iceberg.ops.t USING x ON true WHEN MATCHED THEN UPDATE SET r = 2')
    assert result is adapted
    assert adapted.fetchall() == [(4,)]  # the drained mutation rows, served once
    raw.fetchall.return_value = [(9,)]
    assert adapted.fetchall() == [(9,)]  # buffer empty: delegates to the raw cursor
    raw.fetchone.return_value = (7,)
    assert adapted.fetchone() == (7,)


def test_committing_cursor_passes_reads_and_attributes_through():
    raw = MagicMock()
    raw.execute.return_value = 'ok'
    adapted = writer.CommittingCursor(raw)
    assert adapted.execute('SELECT 1') == 'ok'
    assert adapted.description == raw.description


def test_committing_connection_wraps_cursors():
    raw_connection = MagicMock()
    adapted = writer.CommittingConnection(raw_connection)
    cursor = adapted.cursor()
    assert isinstance(cursor, writer.CommittingCursor)
    assert adapted.host == raw_connection.host


def test_locked_write_decorator_holds_the_lock_during_the_call():
    seen = []

    def probe():
        # Re-entering from the same thread proves the decorator's lock is held.
        with writer.writer_lock():
            seen.append('inside')

    @writer.locked_write
    def task():
        probe()

    task()
    assert seen == ['inside']
