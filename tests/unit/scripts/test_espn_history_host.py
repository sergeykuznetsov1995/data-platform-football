import contextlib
import io
import subprocess
import sys
import types
from unittest.mock import Mock

import pytest
from scripts import espn_history_host as host


def remote(monkeypatch, queue=None, journal=None, capability=None, fail=None):
    calls = []
    results = [queue if queue is not None else [[10, 1, 5, 0, 380, 0, 0]],
               journal if journal is not None else [[400, 10000]],
               capability if capability is not None else []]
    def execute(sql):
        index = len(calls)
        calls.append(sql)
        if index == fail:
            raise RuntimeError('query unavailable')
        return results[index]
    module = types.ModuleType('scrapers.espn.trino_manager')
    module.EspnTrinoTableManager = lambda: types.SimpleNamespace(execute_query=execute)
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(sys, 'argv', ['-c', '2026-11-03'])
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        exec(host.REMOTE_CODE, {})
    return out.getvalue().strip(), calls


def test_remote_healthy_fixed_queries(monkeypatch):
    from scrapers.espn import history_report as report
    line, calls = remote(monkeypatch)
    assert 'прогонов с ошибкой 0' in line and line.endswith('✅')
    assert calls == [report.render_queue_sql('2026-11-03'),
                     report.render_journal_sql('2026-11-03'), report.render_capability_sql()]


def test_three_errors_are_red_even_without_red_season(monkeypatch):
    line, _ = remote(monkeypatch, queue=[[10, 0, 5, 0, 0, 0, 3]])
    assert 'прогонов с ошибкой 3' in line and line.endswith('‼️')


@pytest.mark.parametrize('fail', [0, 1])
def test_required_query_failure_unknown(monkeypatch, fail):
    line, _ = remote(monkeypatch, fail=fail)
    assert 'не посчитано ⚠️' in line and '✅' not in line


@pytest.mark.parametrize('queue', [[], [[1, 2]], [[10, 0, 0, 0, 0, 0, None]],
                                   [[10, 0, 0, 0, 0, 0, -1]]])
def test_bad_queue_unknown(monkeypatch, queue):
    line, _ = remote(monkeypatch, queue=queue)
    assert 'не посчитано ⚠️' in line


def test_bad_journal_unknown(monkeypatch):
    assert 'не посчитано ⚠️' in remote(monkeypatch, journal=[])[0]


@pytest.mark.parametrize('errors', [0, 3])
def test_capability_failure_preserves_counts_without_green(monkeypatch, errors):
    line, _ = remote(monkeypatch, queue=[[10, 0, 5, 0, 0, 0, errors]], fail=2)
    assert f'прогонов с ошибкой {errors}' in line
    assert 'пустые части не посчитаны ⚠️' in line and '✅' not in line
    assert ('‼️' in line) == bool(errors)


def test_host_argv_and_output(monkeypatch):
    line = '• ESPN история 03.11: прогонов с ошибкой 3 ‼️'
    run = Mock(return_value=types.SimpleNamespace(returncode=0, stdout=line+'\n'))
    monkeypatch.setattr(host.subprocess, 'run', run)
    assert host.history_line('2026-11-03') == line
    args, kwargs = run.call_args
    assert args[0][-1] == '2026-11-03' and args[0][-2] == host.REMOTE_CODE
    assert args[0][:7] == ['docker', 'exec', '-w', '/tmp', '-e', 'PYTHONDONTWRITEBYTECODE=1', 'espn-live-airflow-scheduler-1']
    assert kwargs == dict(capture_output=True, text=True, timeout=240)


@pytest.mark.parametrize('day', ['20261103', '2026-11-03; id', '2026-13-01'])
def test_invalid_day_never_runs(monkeypatch, day):
    run = Mock()
    monkeypatch.setattr(host.subprocess, 'run', run)
    with pytest.raises(ValueError): host.history_line(day)
    run.assert_not_called()


@pytest.mark.parametrize('problem', [OSError(), subprocess.TimeoutExpired('docker', 240)])
def test_host_failure_unknown(monkeypatch, problem):
    monkeypatch.setattr(host.subprocess, 'run', Mock(side_effect=problem))
    assert 'не посчитано ⚠️' in host.history_line('2026-11-03')


@pytest.mark.parametrize('code,stdout', [(1, '• ESPN история 03.11: ✅'), (0, ''),
                                        (0, 'unrelated'), (0, '• ESPN история 03.11: ✅\nnoise')])
def test_host_bad_response_unknown(monkeypatch, code, stdout):
    monkeypatch.setattr(host.subprocess, 'run', Mock(return_value=types.SimpleNamespace(returncode=code, stdout=stdout)))
    assert 'не посчитано ⚠️' in host.history_line('2026-11-03')


def test_errors_before_inventory_still_red(monkeypatch):
    line, _ = remote(monkeypatch, queue=[[0, 0, 0, 0, 0, 0, 3]])
    assert 'прогонов с ошибкой 3' in line and line.endswith('‼️')
