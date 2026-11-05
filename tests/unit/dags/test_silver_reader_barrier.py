from unittest.mock import Mock

import pytest

from dags.utils import silver_reader_barrier as barrier


def cursor(*results):
    cur = Mock()
    cur.fetchall.side_effect = results
    return cur


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(barrier.time, 'sleep', lambda _: None)


def test_waits_for_all_old_transactions_not_only_autocommit():
    cur = cursor([(1,)], [('2026-11-05T13:00:00.123Z',)], [('old',)], [])
    barrier.drain_readers(cur)
    statements = [call.args[0] for call in cur.execute.call_args_list]
    assert len(statements) == 4
    assert statements[2] == statements[3]
    assert "create_time <= from_iso8601_timestamp('2026-11-05T13:00:00.123Z')" in statements[2]
    assert 'auto_commit_context' not in statements[2]
    assert 'catalogs' not in statements[2]
    assert not any('kill_query' in sql for sql in statements)


def test_timeout_does_not_mutate_or_cancel_anything():
    cur = cursor([[1]], [['2026-11-05T13:00:00Z']], [['idle-explicit']])
    with pytest.raises(TimeoutError, match='1 older transaction'):
        barrier.drain_readers(cur, timeout_seconds=0)
    assert all(c.args[0].startswith('SELECT') for c in cur.execute.call_args_list)


@pytest.mark.parametrize('nodes', [0, 2])
def test_multiple_or_missing_coordinator_refuses(nodes):
    cur = cursor([(nodes,)])
    with pytest.raises(RuntimeError, match='one coordinator'):
        barrier.drain_readers(cur)
    assert cur.execute.call_count == 1


def test_observation_error_is_not_empty_registry():
    cur = cursor([(1,)], [('2026-11-05T13:00:00Z',)], RuntimeError('not authorized'))
    with pytest.raises(RuntimeError, match='not authorized'):
        barrier.drain_readers(cur)


def test_ambiguous_timestamp_refuses():
    cur = cursor([(1,)], [('2026-11-05T13:00:00',)])
    with pytest.raises(ValueError, match='timezone'):
        barrier.drain_readers(cur)
