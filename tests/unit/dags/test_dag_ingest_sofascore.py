"""Unit tests for ``dags/dag_ingest_sofascore.py`` — #751 Bronze freshness gate.

Airflow is not installed on the host; ``tests/unit/dags/conftest.py`` installs
stub ``airflow`` modules into ``sys.modules`` so the DAG module body (operators
+ ``>>`` wiring) executes and can be asserted on.
"""
from __future__ import annotations

import importlib
import sys

import pytest
from unittest.mock import MagicMock


def _reload_dag_module():
    """Force a fresh import of the SofaScore ingest DAG module."""
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()

    sys.modules.pop("dag_ingest_sofascore", None)
    sys.modules.pop("dags.dag_ingest_sofascore", None)

    return importlib.import_module("dag_ingest_sofascore")


@pytest.fixture
def dag_module():
    return _reload_dag_module()


def _python_task(task_id):
    from airflow.operators.python import PythonOperator

    for t in PythonOperator._instances:
        if t.task_id == task_id:
            return t
    return None


class TestBronzeFreshnessGate:
    """#751: a ``validate_bronze_freshness`` task must exist, wired after
    ``validate_data``, alerting on stale ``bronze.sofascore_*`` ingestion."""

    def test_dag_module_imports_and_exposes_callable(self, dag_module):
        assert hasattr(dag_module, 'validate_bronze_freshness')

    def test_freshness_task_wired_after_validate_data(self, dag_module):
        fresh = _python_task('validate_bronze_freshness')
        assert fresh is not None
        assert fresh.python_callable is dag_module.validate_bronze_freshness
        assert 'validate_data' in fresh.upstream_task_ids

    def test_freshness_checks_cover_match_grain_tables_as_warning(
        self, dag_module, monkeypatch,
    ):
        captured = {}

        def fake_run_checks(checks, raise_on_error=True):
            captured['checks'] = checks
            captured['raise_on_error'] = raise_on_error
            return MagicMock()

        import utils.alerts as al
        import utils.data_quality as dq

        monkeypatch.setattr(dq, 'run_checks', fake_run_checks)
        monkeypatch.setattr(
            al, 'telegram_dq_summary',
            lambda *a, **k: captured.setdefault('telegram', True),
        )

        dag_module.validate_bronze_freshness()

        checks = captured['checks']
        assert {c.params['table'] for c in checks} == {
            'bronze.sofascore_match_stats',
            'bronze.sofascore_event_player_stats',
            'bronze.sofascore_player_ratings',
        }
        assert all(c.kind == 'freshness' for c in checks)
        assert all(c.params['ts_col'] == '_ingested_at' for c in checks)
        assert all(c.severity == 'WARNING' for c in checks)
        # WARNING-only gate must not hard-fail the DAG.
        assert captured['raise_on_error'] is False
        assert captured.get('telegram') is True
