"""Unit tests for ``dags/dag_ingest_sofascore.py``.

Covers:
- #751 Bronze freshness gate (match-grain tables, WARNING).
- #782 folded-in per-player capture: the Saturday/manual gate
  (``_gate_player_capture``), the gated player tasks, and the player freshness
  check (ERROR), migrated from the former ``dag_ingest_sofascore_players``.

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


def _bash_task(task_id):
    from airflow.operators.bash import BashOperator

    for t in BashOperator._instances:
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
        # #842: freshness now reads the match_capture result file to detect a
        # skip-existing no-op — pin it to "no result" so the checks always run.
        monkeypatch.setattr(dag_module, '_load_result', lambda *a, **k: {})

        dag_module.validate_bronze_freshness()

        checks = captured['checks']
        freshness = [c for c in checks if c.kind == 'freshness']
        assert {c.params['table'] for c in freshness} == {
            'bronze.sofascore_match_stats',
            'bronze.sofascore_event_player_stats',
            'bronze.sofascore_player_ratings',
        }
        assert all(c.params['ts_col'] == '_ingested_at' for c in freshness)
        # #711: the gate also carries ONE labelled-stats coverage check —
        # a partial /statistics capture writes rows without name/stat_name.
        coverage = [c for c in checks if c.kind == 'coverage']
        assert [c.params['table'] for c in coverage] == [
            'bronze.sofascore_match_stats']
        assert len(checks) == len(freshness) + len(coverage)
        assert all(c.severity == 'WARNING' for c in checks)
        # WARNING-only gate must not hard-fail the DAG.
        assert captured['raise_on_error'] is False
        assert captured.get('telegram') is True


class TestValidateDataIncrementalNoop:
    """#842 incremental match_capture: a clean skip-existing no-op run (all
    resolved matches already in bronze) reports 0 captured rows by design —
    the capture row-floors must not WARN and the freshness gate is skipped.
    A genuinely-low non-noop run still WARNs (incl. the new venue floor)."""

    SCHEDULE_OK = {'schedule_rows': 381, 'league_table_rows': 20,
                   'tables': [], 'errors': []}

    @staticmethod
    def _patch_results(dag_module, monkeypatch, schedule_result, capture_result):
        monkeypatch.setattr(
            dag_module, '_load_result',
            lambda path, logger: (
                capture_result
                if path == dag_module.MATCH_CAPTURE_RESULT_PATH
                else schedule_result
            ),
        )

    @staticmethod
    def _noop_capture():
        return {'rows': 0, 'eps_rows': 0, 'match_stats_rows': 0,
                'shotmap_rows': 0, 'venue_rows': 0,
                'matches_total': 380, 'matches_skipped_existing': 380,
                'fallback': False, 'errors': [], 'tables': []}

    def test_noop_run_skips_capture_row_floors(self, dag_module, monkeypatch):
        self._patch_results(dag_module, monkeypatch,
                            dict(self.SCHEDULE_OK), self._noop_capture())
        validation = dag_module.validate_data()
        assert validation['status'] == 'success'
        assert validation['warnings'] == []

    def test_zero_rows_without_skip_still_warns(self, dag_module, monkeypatch):
        capture = self._noop_capture()
        capture['matches_skipped_existing'] = 0    # nothing skipped → real gap
        self._patch_results(dag_module, monkeypatch,
                            dict(self.SCHEDULE_OK), capture)
        validation = dag_module.validate_data()
        warned = ' '.join(validation['warnings'])
        for table in ('player_ratings', 'shotmap', 'event_player_stats',
                      'match_stats', 'venue'):
            assert table in warned

    def test_venue_floor_warns_when_low(self, dag_module, monkeypatch):
        capture = {'rows': 25000, 'matches_with_ratings': 380,
                   'eps_rows': 12000, 'match_stats_rows': 34000,
                   'shotmap_rows': 9500, 'venue_rows': 5, 'venue_matches': 5,
                   'matches_total': 380, 'matches_skipped_existing': 0,
                   'fallback': False, 'errors': [], 'tables': []}
        self._patch_results(dag_module, monkeypatch,
                            dict(self.SCHEDULE_OK), capture)
        validation = dag_module.validate_data()
        assert any('venue' in w for w in validation['warnings'])
        assert len(validation['warnings']) == 1

    def test_freshness_skipped_on_noop(self, dag_module, monkeypatch):
        captured = {}
        import utils.alerts as al
        import utils.data_quality as dq
        monkeypatch.setattr(
            dq, 'run_checks',
            lambda *a, **k: captured.setdefault('ran', True) and MagicMock())
        monkeypatch.setattr(
            al, 'telegram_dq_summary',
            lambda *a, **k: captured.setdefault('telegram', True))
        monkeypatch.setattr(dag_module, '_load_result',
                            lambda *a, **k: self._noop_capture())
        dag_module.validate_bronze_freshness()
        assert 'ran' not in captured and 'telegram' not in captured

    def test_freshness_runs_on_fallback_capture(self, dag_module, monkeypatch):
        """A failed/fallback capture is NOT a no-op — the stall alert must
        still fire even when every match was 'skipped'."""
        capture = self._noop_capture()
        capture['fallback'] = True
        captured = {}
        import utils.alerts as al
        import utils.data_quality as dq

        def fake_run_checks(checks, raise_on_error=True):
            captured['ran'] = True
            return MagicMock()

        monkeypatch.setattr(dq, 'run_checks', fake_run_checks)
        monkeypatch.setattr(
            al, 'telegram_dq_summary',
            lambda *a, **k: captured.setdefault('telegram', True))
        monkeypatch.setattr(dag_module, '_load_result',
                            lambda *a, **k: capture)
        dag_module.validate_bronze_freshness()
        assert captured.get('ran') is True


class TestPlayerCaptureGate:
    """#782: the per-player capture is gated to the DAG's own Saturday run or a
    manual ``run_players=True``; skipped on weekdays and external triggers."""

    def test_gate_callable_exposed(self, dag_module):
        assert hasattr(dag_module, '_gate_player_capture')

    def test_gate_task_exists_as_shortcircuit(self, dag_module):
        gate = _python_task('gate_player_capture')
        assert gate is not None
        assert gate.python_callable is dag_module._gate_player_capture

    def test_run_players_param_forces_capture(self, dag_module):
        assert dag_module._gate_player_capture(params={'run_players': True}) is True

    def test_saturday_scheduled_run_triggers_capture(self, dag_module):
        # 2024-01-06 is a Saturday (weekday()==5).
        assert dag_module._gate_player_capture(
            params={},
            dag_run=SimpleNamespace(external_trigger=False),
            logical_date=datetime(2024, 1, 6),
        ) is True

    def test_weekday_scheduled_run_skips_capture(self, dag_module):
        # 2024-01-01 is a Monday (weekday()==0).
        assert dag_module._gate_player_capture(
            params={},
            dag_run=SimpleNamespace(external_trigger=False),
            logical_date=datetime(2024, 1, 1),
        ) is False

    def test_external_trigger_skips_capture_even_on_saturday(self, dag_module):
        # master_pipeline fires an external trigger → keep the daily pipeline
        # fast, skip players regardless of the day.
        assert dag_module._gate_player_capture(
            params={},
            dag_run=SimpleNamespace(external_trigger=True),
            logical_date=datetime(2024, 1, 6),
        ) is False


class TestPlayerCaptureTasks:
    """#782: the player capture task + validation wiring folded in from the
    former ``dag_ingest_sofascore_players``."""

    def test_capture_task_runs_player_capture_entity(self, dag_module):
        task = _bash_task('scrape_player_capture')
        assert task is not None
        assert '--entity player_capture' in task.bash_command
        # exit-2 (R0.2B_FALLBACK) is mapped to soft success by the wrapper.
        assert 'exit 0' in task.bash_command

    def test_player_validate_then_freshness_wired(self, dag_module):
        # The Bash→Python edge isn't modelled by the stub (BashOperator.__rshift__
        # is a no-op); assert the Python→Python edge the stub tracks.
        validate = _python_task('validate_player_data')
        fresh = _python_task('validate_player_freshness')
        assert validate is not None and fresh is not None
        assert 'validate_player_data' in fresh.upstream_task_ids

    def test_player_freshness_checks_cover_player_tables_as_error(
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

        dag_module.validate_player_freshness()

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

    def test_player_freshness_stale_tables_raises(
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
            dag_module.validate_player_freshness()

        # Telegram must fire before the hard-fail.
        assert captured.get('telegram') is True


class TestSeasonParam:
    """#711 (epic #708): the season must be a UI Param defaulting to
    CURRENT_SEASON so the scheduled daily run keeps ingesting the current
    season unchanged, while a "Trigger DAG w/ config" override can backfill a
    past season."""

    def test_season_param_default_is_current_season(self, dag_module):
        from utils.config import CURRENT_SEASON

        season_param = dag_module.dag._dag_kwargs['params']['season']
        # conftest's _Param stub stores the default (real Param also exposes it).
        assert season_param.default == CURRENT_SEASON


class TestSeasonRenderedFromParams:
    """#711: every scrape task must inject the season via Jinja so an
    overridden season (backfill) reaches the scraper — not a baked-in current
    season. Covers the three bash tasks that scrape per (league, season):
    schedule (legacy), match_capture (fouls → match_stats), player_capture."""

    @pytest.mark.parametrize('task_id', [
        'scrape_sofascore_data',
        'scrape_match_capture',
        'scrape_player_capture',
    ])
    def test_scrape_task_renders_season_from_params(self, dag_module, task_id):
        task = _bash_task(task_id)
        assert task is not None, f"missing task {task_id}"
        # f-string collapses {{{{ }}}} -> {{ }}, so the literal Jinja tag
        # survives into the rendered bash_command.
        assert '--season {{ params.season }}' in task.bash_command, (
            f"{task_id} does not render season from params"
        )


class TestValidatePlayerData:
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
        out = dag_module.validate_player_data()
        assert out['status'] == 'success'   # low rows are WARN-only, not failed
        assert any('Low player_profile' in w for w in out['warnings'])

    def test_fallback_with_some_rows_is_partial(
        self, dag_module, monkeypatch, tmp_path,
    ):
        self._write(dag_module, monkeypatch, tmp_path, {
            'rows': 10, 'profile_players': 10, 'fallback': True,
            'tables': ['t'], 'errors': ['R0_2B_FALLBACK: http_403']})
        out = dag_module.validate_player_data()
        assert out['status'] == 'partial_success'
        assert any('R0.2B_FALLBACK' in w for w in out['warnings'])

    def test_zero_rows_with_errors_raises(
        self, dag_module, monkeypatch, tmp_path,
    ):
        self._write(dag_module, monkeypatch, tmp_path, {
            'rows': 0, 'profile_players': 0, 'fallback': True,
            'tables': [], 'errors': ['R0_2B_FALLBACK: http_403']})
        with pytest.raises(Exception):
            dag_module.validate_player_data()

    def test_low_season_stats_warns_but_succeeds(
        self, dag_module, monkeypatch, tmp_path,
    ):
        # Full profile coverage, but the Season-tab picker captured few overall
        # rows (#751 PR3b) → WARN-only, the run still succeeds.
        self._write(dag_module, monkeypatch, tmp_path, {
            'rows': 520, 'profile_players': 520,
            'season_stats_rows': 12, 'season_stats_players': 12,
            'fallback': False, 'tables': ['t'], 'errors': []})
        out = dag_module.validate_player_data()
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
        out = dag_module.validate_player_data()
        assert out['status'] == 'success'
        assert not any('player_season_stats' in w for w in out['warnings'])
