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
