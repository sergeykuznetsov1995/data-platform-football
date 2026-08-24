"""D5: cross-process bronze writer lock (advisory lock in the metadata DB)."""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from scrapers.sofascore import writer_lock


DSN = "postgresql+psycopg2://airflow:secret@airflow-metadb:5432/airflow"
NORMALIZED_DSN = "postgresql://airflow:secret@airflow-metadb:5432/airflow"


class _FakeCursor:
    def __init__(self, connection):
        self._connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql, params=None):
        self._connection.executed.append((sql, params))

    def fetchone(self):
        return (self._connection.answers.pop(0),)


class _FakeConnection:
    def __init__(self, answers):
        self.autocommit = False
        self.closed = False
        self.answers = list(answers)
        self.executed = []

    def cursor(self):
        return _FakeCursor(self)

    def close(self):
        self.closed = True


class _FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def _fake_psycopg2(monkeypatch, *, answers):
    connections = []

    def connect(dsn):
        connection = _FakeConnection(answers)
        connections.append((dsn, connection))
        return connection

    monkeypatch.setitem(sys.modules, "psycopg2", SimpleNamespace(connect=connect))
    return connections


def _refusing_psycopg2(monkeypatch):
    def connect(_dsn):
        raise AssertionError("the lock must not open a connection here")

    monkeypatch.setitem(sys.modules, "psycopg2", SimpleNamespace(connect=connect))


@pytest.mark.unit
def test_lock_is_taken_and_released_by_closing_the_connection(monkeypatch):
    connections = _fake_psycopg2(monkeypatch, answers=[True])
    clock = _FakeClock()
    environ = {writer_lock.WRITER_LOCK_DSN_ENV: DSN}

    with writer_lock.bronze_writer_lock(
        environ, sleep=clock.sleep, clock=clock
    ) as acquired:
        (dsn, connection), = connections
        assert acquired is True
        assert connection.autocommit is True
        assert connection.closed is False

    # SQLAlchemy dialect prefix is stripped: psycopg2 does not understand it.
    assert dsn == NORMALIZED_DSN
    assert connection.executed == [
        ("SELECT pg_try_advisory_lock(%s)", (writer_lock.WRITER_LOCK_KEY,))
    ]
    # Closing the session releases the advisory lock; no explicit unlock.
    assert connection.closed is True
    assert clock.sleeps == []


@pytest.mark.unit
def test_busy_lock_waits_and_then_acquires(monkeypatch):
    connections = _fake_psycopg2(monkeypatch, answers=[False, False, True])
    clock = _FakeClock()
    environ = {writer_lock.WRITER_LOCK_DSN_ENV: DSN}

    with writer_lock.bronze_writer_lock(
        environ, sleep=clock.sleep, clock=clock
    ) as acquired:
        assert acquired is True

    (_, connection), = connections
    assert len(connection.executed) == 3
    assert clock.sleeps == [writer_lock.WRITER_LOCK_POLL_SECONDS] * 2
    assert connection.closed is True


@pytest.mark.unit
def test_busy_lock_times_out_and_closes_the_connection(monkeypatch):
    connections = _fake_psycopg2(monkeypatch, answers=[False] * 10)
    clock = _FakeClock()
    environ = {
        writer_lock.WRITER_LOCK_DSN_ENV: DSN,
        writer_lock.WRITER_LOCK_TIMEOUT_ENV: "12",
    }

    with pytest.raises(writer_lock.WriterLockTimeout) as excinfo:
        with writer_lock.bronze_writer_lock(
            environ, sleep=clock.sleep, clock=clock
        ):
            raise AssertionError("the body must not run without the lock")

    assert "12" in str(excinfo.value)
    (_, connection), = connections
    # Polled at t=0, 5, 10, 15: the deadline is only checked after a refusal.
    assert len(connection.executed) == 4
    assert clock.sleeps == [writer_lock.WRITER_LOCK_POLL_SECONDS] * 3
    assert connection.closed is True


@pytest.mark.unit
@pytest.mark.parametrize("value", ["0", "false", "No"])
def test_lock_is_disabled_only_by_an_explicit_switch(monkeypatch, value):
    _refusing_psycopg2(monkeypatch)
    environ = {writer_lock.WRITER_LOCK_ENV: value}

    with writer_lock.bronze_writer_lock(environ) as acquired:
        assert acquired is False


@pytest.mark.unit
@pytest.mark.parametrize("value", ["1", "", "off"])
def test_unrecognised_switch_values_keep_the_lock_enabled(monkeypatch, value):
    _fake_psycopg2(monkeypatch, answers=[True])
    environ = {
        writer_lock.WRITER_LOCK_ENV: value,
        writer_lock.WRITER_LOCK_DSN_ENV: DSN,
    }

    with writer_lock.bronze_writer_lock(environ) as acquired:
        assert acquired is True


@pytest.mark.unit
def test_missing_dsn_fails_closed_when_the_lock_is_enabled(monkeypatch):
    _refusing_psycopg2(monkeypatch)

    with pytest.raises(RuntimeError, match=writer_lock.WRITER_LOCK_DSN_ENV):
        with writer_lock.bronze_writer_lock({}):
            raise AssertionError("the body must not run without the lock")
