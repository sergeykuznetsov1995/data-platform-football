"""Unit tests for ``dags/dag_ingest_clubelo.py`` (#1463 daily, #1462 history).

Asserts the daily chain (schedule literal, one result file per run, pools,
fail-closed ``validate_data``) and the manual club-page history branch.

Airflow is not installed on the host; ``tests/unit/dags/conftest.py`` installs
stub ``airflow`` modules into ``sys.modules`` so the DAG module body (operators
+ ``>>`` wiring) executes and can be asserted on.
"""
from __future__ import annotations

import importlib
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest


def _reload_dag_module():
    """Force a fresh import of the ClubElo ingest DAG module."""
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()

    sys.modules.pop("dag_ingest_clubelo", None)
    sys.modules.pop("dags.dag_ingest_clubelo", None)

    return importlib.import_module("dag_ingest_clubelo")


@pytest.fixture
def dag_module():
    return _reload_dag_module()


def _python_task(task_id):
    from airflow.operators.python import PythonOperator

    for t in PythonOperator._instances:
        if t.task_id == task_id:
            return t
    return None


def _bash_task(task_id):
    from airflow.operators.bash import BashOperator

    for t in BashOperator._instances:
        if t.task_id == task_id:
            return t
    return None


@pytest.mark.unit
class TestDailyChain:
    """#1463: gate_daily >> scrape_daily >> validate_data, HTML snapshot."""

    def test_dag_id_unchanged(self, dag_module):
        # master_pipeline triggers by this id.
        assert dag_module.dag.dag_id == 'dag_ingest_clubelo'

    def test_schedule_literal_twice_a_day(self, dag_module):
        assert dag_module.dag._dag_kwargs['schedule'] == '30 9,21 * * *'

    def test_old_api_tasks_and_params_are_gone(self, dag_module):
        assert _bash_task('scrape_current_ratings') is None
        assert _python_task('gate_full_ratings') is None
        assert _bash_task('scrape_full_ratings') is None
        assert set(dag_module.dag._dag_kwargs['params']) == {'run_history'}
        source = open(dag_module.__file__, encoding='utf-8').read()
        assert 'LEAGUES' not in source and 'SCHEDULES' not in source

    def test_graph(self, dag_module):
        scrape = _bash_task('scrape_daily')
        validate = _python_task('validate_data')
        assert scrape.upstream_task_ids == {'gate_daily'}
        assert scrape.downstream_task_ids == {'validate_data'}
        assert validate.upstream_task_ids == {'scrape_daily'}
        assert validate.python_callable is dag_module.validate_data

    def test_daily_command_uses_a_per_run_result_file(self, dag_module):
        cmd = _bash_task('scrape_daily').bash_command
        path = "/tmp/clubelo_daily_{{ run_id | replace(':', '_') }}.json"
        assert '/opt/legacy-scraper-venv/bin/python dags/scripts/run_clubelo_scraper.py' in cmd
        assert '--mode daily' in cmd and f'--output {path}' in cmd
        assert f'rm -f {path}' in cmd
        assert '/tmp/clubelo_result.json' not in cmd and '--leagues' not in cmd
        assert _python_task('validate_data')._init_kwargs['op_kwargs'] == {'results_path': path}

    def test_timeout_covers_three_results_retries(self, dag_module):
        from datetime import timedelta

        assert _bash_task('scrape_daily')._init_kwargs['execution_timeout'] == timedelta(minutes=45)

    def test_every_task_is_in_the_default_pool(self, dag_module):
        tasks = [_python_task(t) for t in ('gate_daily', 'validate_data', 'gate_history')]
        tasks += [_bash_task(t) for t in ('scrape_daily', 'scrape_history')]
        assert all(t._init_kwargs['pool'] == 'default_pool' for t in tasks)


def _dag_run(start):
    return SimpleNamespace(start_date=start)


@pytest.mark.unit
class TestValidateData:
    """#1463: only this run's file, fresh, a date, >= 95 % of 1741 clubs."""

    START = datetime(2026, 9, 25, 9, 30, tzinfo=timezone.utc)

    @staticmethod
    def _payload(**overrides):
        payload = {"rating_date": "2026-09-23", "fetched_at": "2026-09-25T09:30:04",
                   "rows": 1741, "provisional": 53, "written": True, "check": None,
                   "error": None, "blocked": None}
        payload.update(overrides)
        return payload

    def _validate(self, tmp_path, payload, start=START):
        import json

        import utils.clubelo_tasks as ct

        path = tmp_path / 'clubelo_daily_scheduled__2026-09-25T09_30_00+00_00.json'
        path.write_text(json.dumps(payload))
        return ct.validate_data(str(path), dag_run=_dag_run(start))

    def test_healthy_result_passes(self, tmp_path):
        out = self._validate(tmp_path, self._payload())
        assert out['status'] == 'success' and out['summary']['rows'] == 1741

    @pytest.mark.parametrize('overrides, problem', [
        ({'fetched_at': '2026-09-24T21:30:04'}, 'stale result'),
        ({'rating_date': None}, 'rating_date is not a date'),
        ({'rating_date': '22.09.2026'}, 'rating_date is not a date'),
        ({'fetched_at': None}, 'fetched_at is not a timestamp'),
        ({'rows': 1653}, 'rows 1653 < 95% of 1741'),
        ({'written': False, 'check': 'C6 levels matched 1500/1741'}, 'C6 levels'),
        ({'error': 'TrinoError: boom'}, 'TrinoError'),
    ])
    def test_any_violation_fails(self, tmp_path, overrides, problem):
        from airflow.exceptions import AirflowException

        with pytest.raises(AirflowException, match=problem):
            self._validate(tmp_path, self._payload(**overrides))

    def test_missing_file_fails(self, tmp_path):
        from airflow.exceptions import AirflowException

        import utils.clubelo_tasks as ct

        with pytest.raises(AirflowException, match='not found'):
            ct.validate_data(str(tmp_path / 'nope.json'), dag_run=_dag_run(self.START))

    def test_unknown_run_start_fails(self, tmp_path):
        from airflow.exceptions import AirflowException

        with pytest.raises(AirflowException, match='start is unknown'):
            self._validate(tmp_path, self._payload(), start=None)


@pytest.mark.unit
class TestHistoryBranch:
    """#1462: manual-only club-page history branch; it skips the daily chain."""

    def test_default_param_off(self, dag_module):
        assert dag_module.dag._dag_kwargs['params']['run_history'] is False

    def test_history_branch_is_its_own_root(self, dag_module):
        gate = _python_task('gate_history')
        scrape = _bash_task('scrape_history')
        assert gate.python_callable is dag_module.gate_history
        assert gate.upstream_task_ids == set()  # not after the daily chain
        assert gate.downstream_task_ids == {'scrape_history'}
        assert scrape.upstream_task_ids == {'gate_history'}
        assert gate._init_kwargs.get('trigger_rule') in (None, 'all_success')
        assert gate._init_kwargs['ignore_downstream_trigger_rules'] is False

    def test_history_command_and_timeout(self, dag_module):
        from datetime import timedelta

        scrape = _bash_task('scrape_history')
        cmd = scrape.bash_command
        assert '/opt/legacy-scraper-venv/bin/python dags/scripts/run_clubelo_scraper.py' in cmd
        assert '--mode history' in cmd and '--batch-size 200' in cmd
        assert '/tmp/clubelo_history_result.json' in cmd
        assert 'rm -f /tmp/clubelo_history_result.json' in cmd
        # LIGHT_ARGS' 5 min would kill the ~15 min branch (R-57)
        assert scrape._init_kwargs['execution_timeout'] == timedelta(minutes=30)
        # no automatic retry of a blocked / partial run (LIGHT_ARGS has retries)
        assert scrape._init_kwargs['retries'] == 0

    def test_daily_chain_is_gated(self, dag_module):
        gate = _python_task('gate_daily')
        assert gate.python_callable is dag_module.gate_daily
        assert _bash_task('scrape_daily').upstream_task_ids == {'gate_daily'}
        # default True: a skip reaches every downstream task
        assert 'ignore_downstream_trigger_rules' not in gate._init_kwargs

    def test_gate_history_only_on_param(self, dag_module):
        assert dag_module.gate_history(params={'run_history': True}) is True
        assert dag_module.gate_history(params={'run_history': False}) is False
        # master pipeline triggers without conf; Sunday does not matter
        assert dag_module.gate_history(
            params={}, logical_date=datetime(2024, 1, 7),
            dag_run=SimpleNamespace(external_trigger=True),
        ) is False

    def test_gate_daily_skips_only_history_runs(self, dag_module):
        assert dag_module.gate_daily(params={'run_history': True}) is False
        assert dag_module.gate_daily(params={'run_history': False}) is True
        assert dag_module.gate_daily() is True
