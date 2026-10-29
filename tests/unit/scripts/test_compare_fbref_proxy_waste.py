import sqlite3
from pathlib import Path

import pytest


QUERY = Path(__file__).resolve().parents[3] / 'scripts/compare_fbref_proxy_waste.sql'


@pytest.fixture
def db():
    with sqlite3.connect(':memory:') as connection:
        connection.row_factory = sqlite3.Row
        connection.executescript('''
            ATTACH DATABASE ':memory:' AS fbref_control;
            CREATE TABLE fbref_control.fetch_attempt (
                run_id, target_id, logical_refresh_id, status, started_at
            );
            CREATE TABLE fbref_control.page_frontier (target_id, page_kind);
            CREATE TABLE proxy_waste_windows (period, started_at, ended_at);
            CREATE TABLE proxy_waste_runs (period, run_id);
            CREATE TABLE proxy_waste_leases (period, page_kind, lease_id);
            CREATE TABLE proxy_waste_ledger (
                event_id, event_version, source, event_type, lease_id, bytes
            );
            INSERT INTO proxy_waste_windows VALUES
                ('before', '2026-10-20', '2026-10-23'),
                ('after', '2026-10-25', '2026-10-28');
            INSERT INTO proxy_waste_runs VALUES ('before', 'b'), ('after', 'a');
            INSERT INTO fbref_control.page_frontier VALUES ('one', 'match');
            INSERT INTO fbref_control.fetch_attempt VALUES
                ('b', 'one', 'refresh-b', 'failed', '2026-10-20'),
                ('b', 'one', 'refresh-b', 'succeeded', '2026-10-20'),
                ('a', 'one', 'refresh-a', 'failed', '2026-10-25'),
                ('b', 'one', 'outside-window', 'succeeded', '2026-10-23'),
                ('foreign', 'one', 'outside-run', 'succeeded', '2026-10-20');
            INSERT INTO proxy_waste_leases VALUES
                ('before', 'match', 'b'), ('before', 'match', 'failed-warmup'),
                ('after', 'match', 'a');
        ''')
        yield connection


def _bytes(db, lease, count, event_id, source='fbref', event_type='bytes'):
    db.execute('INSERT INTO proxy_waste_ledger VALUES (?, ?, ?, ?, ?, ?)',
               (event_id, 'paid-proxy-v2', source, event_type, lease, count))


def _report(db):
    return [dict(row) for row in db.execute(QUERY.read_text())]


def test_comparison_includes_warmup_failures_and_tails_without_retry_fanout(db):
    # All byte events of the mapped leases count, including a failed warmup
    # without a page and a tail after the attempt/window has ended.
    _bytes(db, 'b', 60, 'document')
    _bytes(db, 'failed-warmup', 30, 'warmup')
    _bytes(db, 'b', 10, 'tail')
    _bytes(db, 'a', 80, 'after')
    _bytes(db, 'foreign', 999, 'outside-contour')
    _bytes(db, 'b', 999, 'foreign-source', source='other')
    _bytes(db, 'b', 999, 'counter', event_type='closed')
    row, = _report(db)
    assert row == {
        'page_kind': 'match', 'before_pages': 1, 'after_pages': 1,
        'before_attempts': 2, 'after_attempts': 1,
        'before_failed': 1, 'after_failed': 1,
        'before_bytes': 100, 'after_bytes': 80,
        'before_bytes_per_page': 100, 'after_bytes_per_page': 80,
        'reduction_percent': pytest.approx(20),
        'traffic_reduction_ge_20_percent': 1,
    }


@pytest.mark.parametrize('before,after,expected', [(100, 81, 0), (100, 0, 1),
                                                 (0, 0, None)])
def test_threshold_and_zero_baseline(db, before, after, expected):
    _bytes(db, 'b', before, 'before')
    _bytes(db, 'a', after, 'after')
    row, = _report(db)
    assert row['traffic_reduction_ge_20_percent'] == expected
    if before == 0:
        assert row['reduction_percent'] is None


def test_missing_ledger_or_page_kind_does_not_pass(db):
    _bytes(db, 'b', 100, 'before')
    db.execute("INSERT INTO proxy_waste_leases VALUES ('after', 'season', 's')")
    _bytes(db, 's', 50, 'season')
    rows = _report(db)
    assert [row['page_kind'] for row in rows] == ['match', 'season']
    for row in rows:
        assert row['reduction_percent'] is None
        assert row['traffic_reduction_ge_20_percent'] is None
