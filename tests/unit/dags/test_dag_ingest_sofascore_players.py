"""Unit tests for ``dags/dag_ingest_sofascore_players.py`` — #751 PR3 weekly
per-player capture DAG.

Airflow is not installed on the host; ``tests/unit/dags/conftest.py`` installs
stub ``airflow`` modules so the DAG module body (operators + ``>>`` wiring)
executes and can be asserted on.
"""
from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

import pytest
from unittest.mock import MagicMock


def _reload_dag_module():
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()

    sys.modules.pop("dag_ingest_sofascore_players", None)
    sys.modules.pop("dags.dag_ingest_sofascore_players", None)

    return importlib.import_module("dag_ingest_sofascore_players")


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


class TestWeeklyPlayerDag:
    def test_module_imports_and_exposes_callables(self, dag_module):
        assert hasattr(dag_module, 'validate_data')
        assert hasattr(dag_module, 'validate_bronze_freshness')

    def test_capture_task_runs_player_capture_entity(self, dag_module):
        task = _bash_task('scrape_player_capture')
        assert task is not None
        assert '--entity player_capture' in task.bash_command
        # exit-2 (R0.2B_FALLBACK) is mapped to soft success by the wrapper.
        assert 'exit 0' in task.bash_command

    def test_pipeline_wired_validate_then_freshness(self, dag_module):
        # The Bash→Python edge isn't modelled by the stub (BashOperator.__rshift__
        # is a no-op); we assert the Python→Python edge the stub does track and
        # that the capture task exists (covered by the test above).
        validate = _python_task('validate_data')
        fresh = _python_task('validate_bronze_freshness')
        assert validate is not None and fresh is not None
        assert 'validate_data' in fresh.upstream_task_ids

    def test_freshness_checks_cover_player_tables_as_error(
        self, dag_module, monkeypatch,
    ):
        captured = {}

        def fake_run_checks(checks, raise_on_error=True):
            captured['checks'] = checks
            captured['raise_on_error'] = raise_on_error
            return MagicMock(errors=[])

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
            'bronze.sofascore_player_profile',
            'bronze.sofascore_player_season_stats',
        }
        assert all(c.kind == 'freshness' for c in checks)
        assert all(c.severity == 'ERROR' for c in checks)
        # Weekly cadence → 8-day window before alerting.
        assert all(c.params['max_age_hours'] == 192 for c in checks)
        # raise_on_error stays False; the function re-raises manually so the
        # Telegram summary lands before the hard-fail.
        assert captured['raise_on_error'] is False
        assert captured.get('telegram') is True

    def test_freshness_stale_player_tables_raises(
        self, dag_module, monkeypatch,
    ):
        from airflow.exceptions import AirflowException

        captured = {}
        stale = SimpleNamespace(
            name='freshness[bronze.sofascore_player_profile._ingested_at<192h]',
            details='age 300h > 192h', error=None,
        )

        def fake_run_checks(checks, raise_on_error=True):
            return MagicMock(errors=[stale])

        import utils.alerts as al
        import utils.data_quality as dq

        monkeypatch.setattr(dq, 'run_checks', fake_run_checks)
        monkeypatch.setattr(
            al, 'telegram_dq_summary',
            lambda *a, **k: captured.setdefault('telegram', True),
        )

        with pytest.raises(AirflowException):
            dag_module.validate_bronze_freshness()

        # Telegram must fire before the hard-fail.
        assert captured.get('telegram') is True


class TestValidateData:
    def _write(self, dag_module, monkeypatch, tmp_path, result):
        import json

        p = tmp_path / "res.json"
        p.write_text(json.dumps(result))
        monkeypatch.setattr(dag_module, 'PLAYER_CAPTURE_RESULT_PATH', str(p))

    def test_low_rows_without_fallback_warns_but_succeeds(
        self, dag_module, monkeypatch, tmp_path,
    ):
        self._write(dag_module, monkeypatch, tmp_path, {
            'rows': 10, 'profile_players': 10, 'fallback': False,
            'tables': ['t'], 'errors': []})
        out = dag_module.validate_data()
        assert out['status'] == 'success'   # low rows are WARN-only, not failed
        assert any('Low player_profile' in w for w in out['warnings'])

    def test_fallback_with_some_rows_is_partial(
        self, dag_module, monkeypatch, tmp_path,
    ):
        self._write(dag_module, monkeypatch, tmp_path, {
            'rows': 10, 'profile_players': 10, 'fallback': True,
            'tables': ['t'], 'errors': ['R0_2B_FALLBACK: http_403']})
        out = dag_module.validate_data()
        assert out['status'] == 'partial_success'
        assert any('R0.2B_FALLBACK' in w for w in out['warnings'])

    def test_zero_rows_with_errors_raises(
        self, dag_module, monkeypatch, tmp_path,
    ):
        self._write(dag_module, monkeypatch, tmp_path, {
            'rows': 0, 'profile_players': 0, 'fallback': True,
            'tables': [], 'errors': ['R0_2B_FALLBACK: http_403']})
        with pytest.raises(Exception):
            dag_module.validate_data()

    def test_low_season_stats_warns_but_succeeds(
        self, dag_module, monkeypatch, tmp_path,
    ):
        # Full profile coverage, but the Season-tab picker captured few overall
        # rows (#751 PR3b) → WARN-only, the run still succeeds.
        self._write(dag_module, monkeypatch, tmp_path, {
            'rows': 520, 'profile_players': 520,
            'season_stats_rows': 12, 'season_stats_players': 12,
            'fallback': False, 'tables': ['t'], 'errors': []})
        out = dag_module.validate_data()
        assert out['status'] == 'success'
        assert out['summary']['player_season_stats_rows'] == 12
        assert any('player_season_stats' in w for w in out['warnings'])

    def test_full_season_stats_no_warning(
        self, dag_module, monkeypatch, tmp_path,
    ):
        self._write(dag_module, monkeypatch, tmp_path, {
            'rows': 520, 'profile_players': 520,
            'season_stats_rows': 500, 'season_stats_players': 500,
            'fallback': False, 'tables': ['t'], 'errors': []})
        out = dag_module.validate_data()
        assert out['status'] == 'success'
        assert not any('player_season_stats' in w for w in out['warnings'])
