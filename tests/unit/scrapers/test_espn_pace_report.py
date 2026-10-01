from datetime import datetime, timedelta, timezone
import threading

import duckdb
import pytest
import sqlglot

from scrapers.espn.attempts import AttemptJournal, ATTEMPT_COLUMNS
from scrapers.espn.gate import load_transport_policy
from scrapers.espn.pace_report import render_attempt_sql, render_load_sql, format_rows

START = datetime(2026, 9, 30, 23, 55, tzinfo=timezone.utc)
WEB = 'https://site.web.api.espn.com'


def begin(journal, at=START, **changes):
    data = dict(run_id='run', task_id='task', requested_at=at, origin=WEB,
                endpoint='summary', lane='history', step=1)
    data.update(changes)
    return journal.begin(**data)


def finish(journal, id_, status=200, timeout=False, http_ms=100):
    journal.finish(id_, status=status, timeout=timeout, http_ms=http_ms, direct_bytes=10)


class Connection:
    """Execute generated Trino MERGE against a local DuckDB fixture."""
    def __init__(self):
        self.db = duckdb.connect()
        self.db.execute("ATTACH ':memory:' AS iceberg")
        self.db.execute('CREATE SCHEMA iceberg.ops')
        self.statements = []
        self.fail_after_commit = False
        self.entered = self.release = None

    def cursor(self): return self
    def execute(self, sql):
        self.statements.append(sql)
        if sql.startswith('MERGE') and self.entered is not None:
            self.entered.set()
            assert self.release.wait(5)
        self.db.execute(sqlglot.transpile(sql, read='trino', write='duckdb')[0])
        if sql.startswith('MERGE') and self.fail_after_commit:
            self.fail_after_commit = False
            raise RuntimeError('connection lost after remote commit')
    def fetchall(self): return self.db.fetchall()
    def close(self): pass


def query(conn, sql):
    return conn.db.execute(sqlglot.transpile(sql, read='trino', write='duckdb')[0]).fetchall()


def test_attempt_sql_retry_reserve_timeout_cache_and_midnight(tmp_path):
    journal = AttemptJournal(tmp_path/'attempts.sqlite3')
    # A retry is two real attempts, even with one logical request result.
    finish(journal, begin(journal), status=502)
    finish(journal, begin(journal, START+timedelta(seconds=1)), status=200)
    finish(journal, begin(journal, START+timedelta(minutes=6), origin='https://site.api.espn.com'), status=403)
    finish(journal, begin(journal, START+timedelta(minutes=7), origin='https://sports.core.api.espn.com'), status=None, timeout=True)
    # No begin for a cache hit. End is exclusive.
    finish(journal, begin(journal, START+timedelta(minutes=10)), status=429)
    conn = Connection()
    assert journal.flush(conn) == 5
    assert journal.flush(conn) == 0
    rows = query(conn, render_attempt_sql(START, START+timedelta(minutes=10)))
    assert rows[0] == ('A', 'history', 1, 3, 0, 2, 100.0, 30, 0)
    assert rows[1] == ('B', 'history', 1, 1, 0, 1, 100.0, 10, 0)
    assert 'attempts=3' in format_rows(rows)[0]
    loads = query(conn, render_load_sql(START, START+timedelta(minutes=15), step=1, policy=load_transport_policy()))
    assert [r[1:] for r in loads] == [(2, False), (2, False), (1, False)]


def test_load_sql_includes_empty_full_bins(tmp_path):
    conn = Connection(); journal = AttemptJournal(tmp_path/'attempts.sqlite3'); journal.flush(conn)
    rows = query(conn, render_load_sql(START+timedelta(seconds=1), START+timedelta(minutes=16), step=0, policy=load_transport_policy()))
    assert len(rows) == 2
    assert [r[1:] for r in rows] == [(0, False), (0, False)]


def test_restart_incomplete_and_replayed_delivery_are_not_success(tmp_path):
    path = tmp_path/'attempts.sqlite3'; journal = AttemptJournal(path)
    first = begin(journal)
    second = begin(journal, START+timedelta(seconds=1)); finish(journal, second)
    conn = Connection(); conn.fail_after_commit = True
    with pytest.raises(RuntimeError): journal.flush(conn)
    restarted = AttemptJournal(path)
    assert restarted.flush(conn) == 2
    assert conn.db.execute('SELECT COUNT(*) FROM iceberg.ops.espn_http_attempt_v1').fetchone()[0] == 2
    rows = query(conn, render_attempt_sql(START, START+timedelta(minutes=10)))
    assert rows[0][-1] == 1
    assert not restarted.coverage(START, START+timedelta(minutes=10))
    finish(restarted, first)
    assert restarted.flush(conn) == 1
    assert query(conn, render_attempt_sql(START, START+timedelta(minutes=10)))[0][-1] == 0


def test_begin_finish_continue_during_remote_publication_and_update_is_replayed(tmp_path):
    journal = AttemptJournal(tmp_path/'attempts.sqlite3'); first = begin(journal)
    conn = Connection(); conn.entered = threading.Event(); conn.release = threading.Event()
    errors = []
    def publish():
        try: journal.flush(conn)
        except BaseException as exc: errors.append(exc)
    thread = threading.Thread(target=publish); thread.start()
    assert conn.entered.wait(3)
    # This would time out with BEGIN IMMEDIATE held over the remote MERGE.
    second = begin(journal); finish(journal, first); finish(journal, second)
    conn.release.set(); thread.join(5)
    assert not thread.is_alive() and not errors
    assert conn.db.execute('SELECT COUNT(*), SUM(CASE WHEN complete THEN 1 ELSE 0 END) FROM iceberg.ops.espn_http_attempt_v1').fetchone() == (2, 2)


def test_report_rejects_naive_timestamps():
    with pytest.raises(ValueError): render_attempt_sql(START.replace(tzinfo=None), START+timedelta(hours=1))


def test_spool_contains_no_url_query_or_exception_secret(tmp_path):
    journal = AttemptJournal(tmp_path/'attempts.sqlite3')
    with pytest.raises(ValueError): begin(journal, origin=WEB+'/?token=secret')
    assert journal.rows() == []
