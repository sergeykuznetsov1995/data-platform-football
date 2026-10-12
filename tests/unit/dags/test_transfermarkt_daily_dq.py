"""Daily DQ is a separate read-only root of the current ingest DAG."""
from datetime import timedelta
import importlib
import sys

import pytest


def _module():
    from airflow.operators.python import PythonOperator
    PythonOperator._instances.clear()
    sys.modules.pop('dag_ingest_transfermarkt', None)
    return importlib.import_module('dag_ingest_transfermarkt')


def test_daily_dq_has_no_ingest_or_paid_preflight_dependency():
    _module()
    from airflow.operators.python import PythonOperator
    task = next(task for task in PythonOperator._instances if task.task_id == 'daily_bronze_dq')
    assert not task.upstream_task_ids
    assert not task.downstream_task_ids
    assert task._init_kwargs['pool'] == 'transfermarkt_control'
    assert task._init_kwargs['execution_timeout'] == timedelta(minutes=10)
    assert task._init_kwargs['retries'] == 1
    assert task._init_kwargs['retry_delay'] == timedelta(minutes=5)
    assert task._init_kwargs['do_xcom_push'] is False
    assert task._init_kwargs['on_failure_callback'] is None


@pytest.mark.parametrize('report,should_fail', [
    ({'completed': True, 'status': 'GREEN'}, False),
    ({'completed': True, 'status': 'UNKNOWN'}, False),
    ({'completed': True, 'status': 'RED', 'errors': ['NULL lineage']}, True),
    ({'completed': False, 'status': 'UNKNOWN', 'execution_errors': ['query failed']}, True),
])
def test_daily_dq_without_qualification_uses_only_connection_and_persisted_runner(monkeypatch, report, should_fail):
    mod = _module()
    from utils import transfermarkt_daily_dq as daily, transfermarkt_native_v2 as native
    events = []
    class Cursor:
        def close(self):
            events.append('cursor closed')
    class Connection:
        def cursor(self):
            return Cursor()
        def close(self):
            events.append('connection closed')
    monkeypatch.setattr(native, 'connect', lambda: Connection())
    monkeypatch.setattr(mod, '_current_qualification', lambda: None)
    monkeypatch.setattr(mod, '_probe_gateway_exit', lambda **kwargs: pytest.fail('paid I/O forbidden'))
    monkeypatch.setattr(daily, 'run_daily_bronze_dq', lambda cur, **kwargs: report)
    from airflow.exceptions import AirflowException
    if should_fail:
        with pytest.raises(AirflowException, match='daily Bronze DQ'):
            mod._daily_bronze_dq(run_id='scheduled-no-qualification')
    else:
        assert mod._daily_bronze_dq(run_id='scheduled-no-qualification') == report
    assert events == ['cursor closed', 'connection closed']
