"""No live Trino: boundary/load failures must prevent mutations and switching."""
from datetime import datetime, timedelta

import pytest

from utils.fotmob_silver_admission import (
    AdmittedCursor, AdmissionClosed, LOAD_SQL, MSK, remaining_seconds,
)

pytestmark = pytest.mark.unit


def at(hour, minute, second=0):
    return datetime(2026, 11, 6, hour, minute, second, tzinfo=MSK)


@pytest.mark.parametrize('value', [at(0, 29, 59), at(2, 45), at(23, 59)])
def test_closed_window(value):
    with pytest.raises(AdmissionClosed):
        remaining_seconds(value)


def test_boundaries_and_timezone():
    assert remaining_seconds(at(0, 30)) == 1800
    assert remaining_seconds(at(2, 44, 59)) == 1
    assert remaining_seconds(datetime.fromisoformat('2026-11-05T21:30:00+00:00')) == 1800
    with pytest.raises(ValueError):
        remaining_seconds(datetime(2026, 11, 6))


class Cursor:
    def __init__(self, rows=None, after_execute=None):
        self.statements = []
        self.rows = [(0,)] if rows is None else rows
        self.after_execute = after_execute or (lambda sql: None)

    def execute(self, sql):
        self.statements.append(sql)
        self.after_execute(sql)

    def fetchall(self):
        return self.rows if self.statements[-1] == LOAD_SQL else []


@pytest.mark.parametrize('sql', ['SELECT 1', 'DELETE FROM inactive', 'INSERT INTO inactive SELECT 1',
                               'UPDATE reader_state SET revision = 2'])
def test_every_statement_guarded_at_threshold(sql):
    raw = Cursor([(5,)])
    cur = AdmittedCursor(raw, now=lambda: at(2, 44))
    cur.execute(sql)
    assert raw.statements == ["SET SESSION query_max_run_time = '55s'", LOAD_SQL,
                              "SET SESSION query_max_run_time = '55s'", sql]
    assert cur.fetchall() == []


@pytest.mark.parametrize('rows', [[(6,)], [], [(None,)], [('0',)], [(-1,)]])
def test_busy_or_unknown_load_prevents_publication(rows):
    raw = Cursor(rows)
    with pytest.raises(AdmissionClosed):
        AdmittedCursor(raw, now=lambda: at(1, 0)).execute('UPDATE reader_state SET revision = 2')
    assert not any(sql.startswith('UPDATE') for sql in raw.statements)


def test_probe_consumes_window_and_never_submits_write_after_deadline():
    clock = [at(2, 44, 50)]
    raw = Cursor(after_execute=lambda sql: clock.__setitem__(0, at(2, 45)) if sql == LOAD_SQL else None)
    with pytest.raises(AdmissionClosed):
        AdmittedCursor(raw, now=lambda: clock[0]).execute('INSERT INTO inactive SELECT 1')
    assert len(raw.statements) == 2


def test_load_probe_time_reduces_statement_timeout():
    clock = [at(2, 44)]
    raw = Cursor(after_execute=lambda sql: clock.__setitem__(0, clock[0] + timedelta(seconds=10))
                 if sql == LOAD_SQL else None)
    AdmittedCursor(raw, now=lambda: clock[0]).execute('SELECT 1')
    assert raw.statements[2] == "SET SESSION query_max_run_time = '45s'"


def test_probe_failure_propagates_without_mutation():
    def fail(sql):
        if sql == LOAD_SQL:
            raise OSError('Trino unavailable')
    raw = Cursor(after_execute=fail)
    with pytest.raises(OSError):
        AdmittedCursor(raw, now=lambda: at(1, 0)).execute('DELETE FROM inactive')
    assert len(raw.statements) == 2


def test_first_query_outside_window_does_not_contact_trino():
    raw = Cursor()
    with pytest.raises(AdmissionClosed):
        AdmittedCursor(raw, now=lambda: at(3, 0)).execute('SELECT 1')
    assert raw.statements == []


def test_set_completion_after_boundary_never_admits_statement():
    clock = [at(2, 44)]
    calls = []
    def delay_second_set(sql):
        if sql.startswith('SET SESSION'):
            calls.append(sql)
            if len(calls) == 2:
                clock[0] = at(2, 45)
    raw = Cursor(after_execute=delay_second_set)
    with pytest.raises(AdmissionClosed):
        AdmittedCursor(raw, now=lambda: clock[0]).execute('UPDATE reader_state SET revision = 2')
    assert len(raw.statements) == 3


def test_slow_set_inside_window_does_not_admit_stale_timeout():
    clock = [at(2, 44)]
    calls = []
    def delay_second_set(sql):
        if sql.startswith('SET SESSION'):
            calls.append(sql)
            if len(calls) == 2:
                clock[0] = at(2, 44, 30)
    raw = Cursor(after_execute=delay_second_set)
    with pytest.raises(AdmissionClosed, match='transport reserve'):
        AdmittedCursor(raw, now=lambda: clock[0]).execute('INSERT INTO inactive SELECT 1')
    assert len(raw.statements) == 3
