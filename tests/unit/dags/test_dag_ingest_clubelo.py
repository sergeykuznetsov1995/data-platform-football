"""Unit tests for ``dags/dag_ingest_clubelo.py`` (#716).

Covers the one-source-one-DAG consolidation: the former weekly
``dag_ingest_clubelo_full`` is folded into ``dag_ingest_clubelo`` as a gated
branch. Asserts the Sunday/manual gate (``gate_full_ratings``), the gated
historical task, its parametrized backfill command, and the default params.

Airflow is not installed on the host; ``tests/unit/dags/conftest.py`` installs
stub ``airflow`` modules into ``sys.modules`` so the DAG module body (operators
+ ``>>`` wiring) executes and can be asserted on.
"""
from __future__ import annotations

import importlib
import sys
from datetime import datetime
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
class TestDagTopology:
    """The consolidated DAG keeps its id and exposes daily + gated tasks."""

    def test_dag_id_unchanged(self, dag_module):
        # master_pipeline triggers by this id — must NOT change on consolidation.
        assert dag_module.dag.dag_id == 'dag_ingest_clubelo'

    def test_daily_and_gated_tasks_exist(self, dag_module):
        assert _bash_task('scrape_current_ratings') is not None
        assert _python_task('validate_data') is not None
        assert _python_task('gate_full_ratings') is not None
        assert _bash_task('scrape_full_ratings') is not None

    def test_gate_callable_is_wired(self, dag_module):
        gate = _python_task('gate_full_ratings')
        assert gate.python_callable is dag_module.gate_full_ratings


@pytest.mark.unit
class TestBackfillCommand:
    """The gated task renders the parametrized backfill command; daily does not."""

    def test_daily_task_is_not_full_mode(self, dag_module):
        daily = _bash_task('scrape_current_ratings')
        assert '--mode full' not in daily.bash_command

    def test_full_task_renders_backfill_params(self, dag_module):
        full = _bash_task('scrape_full_ratings')
        cmd = full.bash_command
        assert '--mode full' in cmd
        # days_back + force_replace come from params at runtime (Jinja, not f-string).
        assert '--days-back {{ params.days_back }}' in cmd
        assert "{{ '--force-replace' if params.force_replace else '' }}" in cmd
        # separate output file so it never races the daily validate on result.json
        assert '/tmp/clubelo_full_result.json' in cmd

    def test_default_params(self, dag_module):
        params = dag_module.dag._dag_kwargs['params']
        # gated off by default; recurring weekly depth; guard armed.
        assert params['run_full'] is False
        assert params['days_back'] == 365
        assert params['force_replace'] is False


@pytest.mark.unit
class TestGateFullRatings:
    """gate_full_ratings: Sunday OWN run or manual run_full=True; else skip."""

    def test_run_full_param_forces_scrape(self, dag_module):
        assert dag_module.gate_full_ratings(params={'run_full': True}) is True

    def test_sunday_scheduled_run_triggers_scrape(self, dag_module):
        # 2024-01-07 is a Sunday (weekday()==6).
        assert dag_module.gate_full_ratings(
            params={'run_full': False},
            logical_date=datetime(2024, 1, 7),
            dag_run=SimpleNamespace(external_trigger=False),
        ) is True

    def test_weekday_scheduled_run_skips_scrape(self, dag_module):
        # 2024-01-01 is a Monday (weekday()==0).
        assert dag_module.gate_full_ratings(
            params={'run_full': False},
            logical_date=datetime(2024, 1, 1),
            dag_run=SimpleNamespace(external_trigger=False),
        ) is False

    def test_external_trigger_skips_even_on_sunday(self, dag_module):
        # dag_master_pipeline fires this DAG → must stay fast, skip the heavy scrape.
        assert dag_module.gate_full_ratings(
            params={'run_full': False},
            logical_date=datetime(2024, 1, 7),
            dag_run=SimpleNamespace(external_trigger=True),
        ) is False
