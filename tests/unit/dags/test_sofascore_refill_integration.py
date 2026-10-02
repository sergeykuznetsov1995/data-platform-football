from __future__ import annotations

import importlib
import json
import sys
from types import SimpleNamespace

import pytest


def _module():
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator
    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    sys.modules.pop('dags.dag_refresh_sofascore_all_mens', None)
    return importlib.import_module('dags.dag_refresh_sofascore_all_mens')


def test_refill_and_metadata_wait_for_all_mapped_scope_tasks():
    module = _module()
    from airflow.operators.python import PythonOperator
    ops = {op.task_id: op for op in PythonOperator._instances}
    assert {'run_refresh_scope', 'validate_refresh_scope'} <= ops['refill_refresh_window'].upstream_task_ids
    assert {'run_refresh_scope', 'validate_refresh_scope', 'refill_refresh_window'} <= ops['enrich_season_metadata'].upstream_task_ids
    assert 'refill_refresh_window' in module.REFRESH_TASK_IDS


def test_refill_never_retries_a_failed_initial_scope(monkeypatch):
    module = _module()
    monkeypatch.setattr(module, '_pending_refresh_partitions', lambda: pytest.fail('paid refill after failure'))
    result = module._refill_refresh_window(dag_run=SimpleNamespace(get_task_instances=lambda: [
        SimpleNamespace(task_id='run_refresh_scope', state='failed')]))
    assert result['stop_reason'] == 'upstream_failure'


def test_refill_uses_new_plan_ids_and_common_deadline(tmp_path, monkeypatch):
    module = _module()
    initial = tmp_path / 'initial.json'
    initial.write_text(json.dumps({'status':'partial','campaign_id':'c','tournament_id':8,
                                  'source_season_id':825,'elapsed_s':2539.3}))
    env = {'SOFASCORE_CAMPAIGN_ACTION':'refresh','SOFASCORE_SCOPE_KEY':'c:8:825',
           'SOFASCORE_SCOPE_RESULT_PATH':str(initial),
           'SOFASCORE_REFRESH_WINDOW_DEADLINE_EPOCH':'15000'}
    monkeypatch.setattr(module, 'RESULT_DIR', str(tmp_path))
    monkeypatch.setattr(module.time, 'time', lambda: 1000)
    monkeypatch.setattr(module.state, 'read_snapshot', lambda *a, **k: {'campaign_id':'c'})
    monkeypatch.setattr(module, '_configured_tournament_ids', lambda: frozenset())
    rows = [[('SS-8','2026',593,None,0,825)], []]
    monkeypatch.setattr(module, '_pending_refresh_partitions', lambda: rows.pop(0))
    plans = []
    def plan(snapshot, pending, **kwargs):
        plans.append(kwargs)
        return [{'SOFASCORE_SCOPE_KEY':'c:8:825','SOFASCORE_TOURNAMENT_ID':'8',
                 'SOFASCORE_CANONICAL_SEASON':'2026','SOFASCORE_SOURCE_SEASON_ID':'825',
                 'SOFASCORE_SCOPE_RUN_ID':kwargs['dag_run_id']+'--8-825',
                 'SOFASCORE_SCOPE_TIMEOUT_S':'3000'}]
    monkeypatch.setattr(module.state, 'plan_refresh_batch', plan)
    executed = []
    def execute(environment):
        executed.append(environment)
        return {'status':'refreshed','elapsed_s':1000,'scope_key':'c:8:825'}
    monkeypatch.setattr(module, '_execute_refill_scope', execute)
    result = module._refill_refresh_window(run_id='run',
        dag_run=SimpleNamespace(run_type='scheduled', get_task_instances=lambda: [
            SimpleNamespace(task_id='run_refresh_scope',state='success')]),
        ti=SimpleNamespace(xcom_pull=lambda task_ids: [{'env':env}]))
    assert result['stop_reason'] == 'empty_queue'
    assert plans[0]['scope_budget_s'] == 4660
    assert plans[0]['dag_run_id'] == 'run:refill:1'
    assert executed[0]['SOFASCORE_REFRESH_WINDOW_DEADLINE_EPOCH'] == '15000'
    assert executed[0]['SOFASCORE_SCOPE_TIMEOUT_S'] == '3000'
    assert env['SOFASCORE_SCOPE_RESULT_PATH'] == str(initial)


def test_refill_child_timeout_terminates_process_group(monkeypatch):
    module = _module()
    class Child:
        pid = 1234
        waits = 0
        def wait(self, timeout=None):
            self.waits += 1
            if self.waits <= 2:
                raise module.subprocess.TimeoutExpired('scope', timeout)
            return -9
    child = Child()
    killed = []
    kwargs_seen = []
    def popen(command, **kwargs):
        kwargs_seen.append(kwargs)
        return child
    monkeypatch.setattr(module.subprocess, 'Popen', popen)
    monkeypatch.setattr(module.os, 'killpg', lambda pid,sig:killed.append((pid,sig)))
    monkeypatch.setattr(module.time, 'time', lambda:1000)
    env = {key:'value' for key in (
        'SOFASCORE_CAMPAIGN_SNAPSHOT','SOFASCORE_TOURNAMENT_ID','SOFASCORE_SOURCE_SEASON_ID',
        'SOFASCORE_EXPECTED_SNAPSHOT_ID','SOFASCORE_EXPECTED_CAMPAIGN_ID',
        'SOFASCORE_SCOPE_OUTPUT_DIR','SOFASCORE_SCOPE_RESULT_PATH',
        'SOFASCORE_WORKLOAD_ARTIFACT','SOFASCORE_SCOPE_RUN_ID','SOFASCORE_SCOPE_KEY')}
    env.update(SOFASCORE_REFRESH_WINDOW_DEADLINE_EPOCH='1100',SOFASCORE_SCOPE_TIMEOUT_S='1000')
    with pytest.raises(module.subprocess.TimeoutExpired):
        module._execute_refill_scope(env)
    assert killed == [(1234,module.signal.SIGTERM),(1234,module.signal.SIGKILL)]
    assert kwargs_seen[0]['start_new_session'] is True
    assert child.waits == 3


def test_summary_includes_refill_against_original_7200_seconds(monkeypatch):
    module = _module()
    values = {'validate_refresh_scope':[{'status':'partial','elapsed_s':2539.3}],
              'refill_refresh_window':{'outcomes':[{'status':'refreshed','elapsed_s':3000}],
                                       'stop_reason':'empty_queue'}}
    report = module._window_summary({'ti':SimpleNamespace(xcom_pull=lambda task_ids:values[task_ids])})
    assert report['window_used_s'] == 5539.3
    assert report['window_budget_s'] == 7200
    assert report['window_use'] == 0.769
    assert report['refill_stop_reason'] == 'empty_queue'


def test_failed_refill_report_is_counted_without_return_xcom(tmp_path, monkeypatch, capsys):
    module = _module()
    from utils import sofascore_refresh_window

    run_id = 'scheduled__2026-10-02T08:30:00+00:00'
    env = {'SOFASCORE_REFRESH_WINDOW_DEADLINE_EPOCH': '15000'}
    monkeypatch.setattr(module, 'RESULT_DIR', str(tmp_path))
    monkeypatch.setattr(module, '_validate_refresh_scope', lambda env: {
        'status': 'partial', 'elapsed_s': 4098.2})
    failed = {'status': 'failed', 'stop_reason': 'scope_failure',
              'outcomes': [{'status': 'failed', 'elapsed_s': 1653.2}]}

    def refill_window(**kwargs):
        kwargs['persist'](failed)
        return failed

    monkeypatch.setattr(sofascore_refresh_window, 'refill_window', refill_window)
    dag_run = SimpleNamespace(run_type='scheduled', get_task_instances=lambda: [])
    with pytest.raises(module.AirflowException, match='refresh refill failed'):
        module._refill_refresh_window(
            run_id=run_id, dag_run=dag_run,
            ti=SimpleNamespace(xcom_pull=lambda task_ids: [{'env': env}]))
    values = {'validate_refresh_scope': [
        {'status': 'partial', 'elapsed_s': 4098.2 / 25} for _ in range(25)],
        'refill_refresh_window': None}
    context = {'run_id': run_id, 'dag_run': dag_run,
               'ti': SimpleNamespace(xcom_pull=lambda task_ids: values[task_ids])}
    summary = module._window_summary(context)
    assert summary['scopes'] == 26
    assert summary['window_used_s'] == 5751.4
    assert summary['window_use'] == 0.799
    assert summary['refill_stop_reason'] == 'scope_failure'
    dag_run.get_task_instances = lambda: [
        SimpleNamespace(task_id='refill_refresh_window', state='failed')]
    with pytest.raises(module.AirflowException, match='attempt failed: refill_refresh_window'):
        module._propagate_status(**context)
    assert '"window_used_s": 5751.4' in capsys.readouterr().out


@pytest.mark.parametrize('contents', [None, '{', '[]', json.dumps({
    'run_id': 'other-run', 'outcomes': [{'elapsed_s': 1000}], 'stop_reason': 'scope_failure'})])
def test_summary_ignores_unavailable_or_wrong_run_refill_report(tmp_path, monkeypatch, contents):
    module = _module()
    monkeypatch.setattr(module, 'RESULT_DIR', str(tmp_path))
    if contents is not None:
        (tmp_path / 'refill-run.json').write_text(contents)
    values = {'validate_refresh_scope': [{'status': 'partial', 'elapsed_s': 100}],
              'refill_refresh_window': None}
    summary = module._window_summary({
        'run_id': 'run', 'ti': SimpleNamespace(xcom_pull=lambda task_ids: values[task_ids])})
    assert summary['window_used_s'] == 100
    assert summary['refill_stop_reason'] is None


def test_summary_does_not_count_refill_report_twice(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, 'RESULT_DIR', str(tmp_path))
    refill = {'run_id': 'run', 'outcomes': [{'status': 'refreshed', 'elapsed_s': 200}],
              'stop_reason': 'empty_queue'}
    (tmp_path / 'refill-run.json').write_text(json.dumps(refill))
    values = {'validate_refresh_scope': [{'status': 'partial', 'elapsed_s': 100}],
              'refill_refresh_window': refill}
    summary = module._window_summary({
        'run_id': 'run', 'ti': SimpleNamespace(xcom_pull=lambda task_ids: values[task_ids])})
    assert summary['scopes'] == 2
    assert summary['window_used_s'] == 300


def test_refill_timeout_kills_group_even_when_parent_exits_on_term(monkeypatch):
    module = _module()
    class Child:
        pid = 1234
        calls = 0
        def wait(self, timeout=None):
            self.calls += 1
            if self.calls == 1:
                raise module.subprocess.TimeoutExpired('scope', timeout)
            return -15
    child = Child()
    killed = []
    monkeypatch.setattr(module.subprocess, 'Popen', lambda *a,**k:child)
    monkeypatch.setattr(module.os, 'killpg', lambda pid,sig:killed.append(sig))
    monkeypatch.setattr(module.time, 'time', lambda:1000)
    env = {key:'value' for key in (
        'SOFASCORE_CAMPAIGN_SNAPSHOT','SOFASCORE_TOURNAMENT_ID','SOFASCORE_SOURCE_SEASON_ID',
        'SOFASCORE_EXPECTED_SNAPSHOT_ID','SOFASCORE_EXPECTED_CAMPAIGN_ID',
        'SOFASCORE_SCOPE_OUTPUT_DIR','SOFASCORE_SCOPE_RESULT_PATH',
        'SOFASCORE_WORKLOAD_ARTIFACT','SOFASCORE_SCOPE_RUN_ID','SOFASCORE_SCOPE_KEY')}
    env.update(SOFASCORE_REFRESH_WINDOW_DEADLINE_EPOCH='1100',SOFASCORE_SCOPE_TIMEOUT_S='1000')
    with pytest.raises(module.subprocess.TimeoutExpired):
        module._execute_refill_scope(env)
    # Parent termination does not prove all browser grandchildren exited.
    assert killed == [module.signal.SIGTERM,module.signal.SIGKILL]
