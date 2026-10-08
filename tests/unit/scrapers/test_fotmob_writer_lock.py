"""Offline regressions for the bounded FotMob writer lease."""
import threading
from types import SimpleNamespace

import pytest

from scrapers.fotmob import writer_lock as locks


class Connection:
    def __init__(self, acquired=True):
        self.closed = False
        self.acquired = acquired
        self.owns = True
        self.sql = []

    def close(self):
        self.closed = True


def fake_pg(monkeypatch, connection):
    monkeypatch.setattr(locks, "_connect", lambda env, timeout: connection)
    def query(conn, sql, params, timeout):
        conn.sql.append((sql, params))
        if conn.closed:
            raise OSError("connection closed")
        return (conn.acquired if "pg_try_advisory_lock" in sql else conn.owns,)
    monkeypatch.setattr(locks, "_query", query)


def test_live_check_never_reacquires_and_loss_is_irreversible(monkeypatch):
    conn = Connection()
    fake_pg(monkeypatch, conn)
    with locks.writer_lock({}, key=12, wait_seconds=0, poll_seconds=.01) as lease:
        lease.check()
        conn.owns = False
        with pytest.raises(locks.WriterLockLost):
            lease.check()

        with pytest.raises(locks.WriterLockLost):
            lease.check()
        before = len(conn.sql)
        conn.owns = True
        with pytest.raises(locks.WriterLockLost):
            lease.check()
        assert len(conn.sql) == before
    assert conn.closed
    assert sum("pg_try_advisory_lock" in sql for sql, _ in conn.sql) == 1
    assert "pg_backend_pid()" in conn.sql[1][0]


def test_busy_wait_is_bounded_and_closes_connection(monkeypatch):
    conn = Connection(acquired=False)
    fake_pg(monkeypatch, conn)
    ticks = iter([0., 0., 2.])
    monkeypatch.setattr(locks.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(locks.time, "sleep", lambda duration: None)
    with pytest.raises(locks.WriterLockBusy):
        with locks.writer_lock({}, key=12, wait_seconds=1, poll_seconds=.1):
            pytest.fail("busy lock entered")
    assert conn.closed


@pytest.mark.parametrize("value", ["0", "false", "no"])
def test_explicit_offline_aliases_disable_lock(monkeypatch, value):
    monkeypatch.setattr(locks, "_connect", lambda *args: pytest.fail("opened DB"))
    with locks.writer_lock({"FOTMOB_WRITER_LOCK": value}, key=12,
                           wait_seconds=0, poll_seconds=.01) as lease:
        assert lease is False


def test_heartbeat_cancels_active_query_on_lost_owner(monkeypatch):
    conn = Connection()
    fake_pg(monkeypatch, conn)
    canceled = threading.Event()
    with locks.writer_lock({}, key=12, wait_seconds=0, poll_seconds=.01) as lease:
        with pytest.raises(locks.WriterLockLost):
            with lease.watch(canceled.set):
                conn.owns = False
                assert canceled.wait(1), "heartbeat did not cancel active SQL"


def test_failed_probe_loses_authority_forever(monkeypatch):
    conn = Connection()
    fake_pg(monkeypatch, conn)
    with locks.writer_lock({}, key=12, wait_seconds=0, poll_seconds=.01) as lease:
        monkeypatch.setattr(locks, "_query", lambda *args: (_ for _ in ()).throw(TimeoutError()))
        with pytest.raises(locks.WriterLockLost):
            lease.check()


def test_socket_poll_obeys_explicit_deadline(monkeypatch):
    from types import SimpleNamespace
    from psycopg2.extensions import POLL_READ
    connection = SimpleNamespace(poll=lambda: POLL_READ, fileno=lambda: 42)
    waits = []
    def select(read, write, errors, timeout):
        waits.append(timeout)
        return [], [], []
    monkeypatch.setattr(locks.select, "select", select)
    with pytest.raises(TimeoutError):
        locks._poll(connection, .05)
    assert 0 < waits[0] <= .05


@pytest.mark.parametrize("value", ["1", "", "true"])
def test_other_values_do_not_disable_production_authority(monkeypatch, value):
    conn = Connection()
    fake_pg(monkeypatch, conn)
    with locks.writer_lock({"FOTMOB_WRITER_LOCK": value}, key=12,
                           wait_seconds=0, poll_seconds=.01) as lease:
        assert isinstance(lease, locks.WriterLease)


@pytest.mark.parametrize("wait,poll", [(float("inf"), 1), (1, float("nan")), (-1, 1), (1, 0)])
def test_invalid_or_unbounded_wait_rejected_before_connect(monkeypatch, wait, poll):
    monkeypatch.setattr(locks, "_connect", lambda *args: pytest.fail("opened DB"))
    with pytest.raises(ValueError):
        with locks.writer_lock({}, key=12, wait_seconds=wait, poll_seconds=poll):
            pass


def test_released_session_cannot_be_used_again(monkeypatch):
    conn = Connection()
    fake_pg(monkeypatch, conn)
    with locks.writer_lock({}, key=12, wait_seconds=0, poll_seconds=.01) as lease:
        lease.check()
    before = list(conn.sql)
    with pytest.raises(locks.WriterLockLost):
        lease.check()
    assert conn.sql == before


def test_zero_wait_still_gives_the_single_acquisition_probe_a_bounded_io_budget(monkeypatch):
    connection = SimpleNamespace(close=lambda: None)
    monkeypatch.setattr(locks, '_connect', lambda env, timeout: connection)
    deadlines = []
    def query(conn, sql, params, timeout):
        deadlines.append(timeout)
        return (True,)
    monkeypatch.setattr(locks, '_query', query)
    with locks.writer_lock({}, key=1, wait_seconds=0, poll_seconds=2) as lease:
        assert isinstance(lease, locks.WriterLease)
    assert deadlines == [locks.PROBE_SECONDS]
