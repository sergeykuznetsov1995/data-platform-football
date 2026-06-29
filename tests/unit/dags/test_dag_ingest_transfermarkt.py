"""Unit tests for ``dags/dag_ingest_transfermarkt.py`` — #717 season backfill.

Airflow is not installed on the host; ``tests/unit/dags/conftest.py`` installs
stub ``airflow`` modules (including ``airflow.models.param.Param``) into
``sys.modules`` so the DAG module body executes and can be asserted on.

These tests pin the UI-configurable season wiring added for the 10-season
Transfermarkt backfill (issue #717, epic #708): the daily/weekly run must be
unchanged (default = CURRENT_SEASON), every scrape task must render
``--season {{ params.season }}`` (so a "Trigger DAG w/ config" override flows
through instead of a hardcoded current season), and the Bronze DQ gate must
check the season it actually scraped — not CURRENT_SEASON.
"""
from __future__ import annotations

import importlib
import sys

import pytest
from unittest.mock import MagicMock


SCRAPE_TASK_IDS = [
    'scrape_players',
    'scrape_market_value_history',
    'scrape_transfers',
    'scrape_coaches',
]


def _reload_dag_module():
    """Force a fresh import of the Transfermarkt ingest DAG module."""
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()

    sys.modules.pop("dag_ingest_transfermarkt", None)
    sys.modules.pop("dags.dag_ingest_transfermarkt", None)

    return importlib.import_module("dag_ingest_transfermarkt")


@pytest.fixture
def dag_module():
    return _reload_dag_module()


def _bash_task(task_id):
    from airflow.operators.bash import BashOperator

    for t in BashOperator._instances:
        if t.task_id == task_id:
            return t
    return None


class TestSeasonParam:
    """The season must be a UI Param defaulting to CURRENT_SEASON so the
    scheduled weekly run keeps ingesting the current season unchanged."""

    def test_dag_module_imports(self, dag_module):
        assert hasattr(dag_module, 'validate_data')

    def test_season_param_default_is_current_season(self, dag_module):
        from utils.config import CURRENT_SEASON

        season_param = dag_module.dag._dag_kwargs['params']['season']
        # conftest's _Param stub stores the default (real Param also exposes it).
        assert season_param.default == CURRENT_SEASON


class TestSeasonRenderedFromParams:
    """Every scrape task must inject the season via Jinja so an overridden
    season (backfill) reaches the scraper — not a baked-in current season."""

    def test_all_scrape_tasks_render_season_from_params(self, dag_module):
        for task_id in SCRAPE_TASK_IDS:
            task = _bash_task(task_id)
            assert task is not None, f"missing task {task_id}"
            # f-string collapses {{{{ }}}} -> {{ }}, so the literal Jinja tag
            # survives into the rendered bash_command.
            assert '--season {{ params.season }}' in task.bash_command, (
                f"{task_id} does not render season from params"
            )


class TestBronzeQualityGateFollowsParamSeason:
    """The Bronze DQ gate must query the partition of the season actually
    scraped (#717) — otherwise a backfill of 2018/19 would check the empty
    current-season partition and the ERROR row_count gate would fail."""

    def test_gate_where_clause_uses_param_season(self, dag_module, monkeypatch):
        captured = {}

        def fake_run_checks(checks, raise_on_error=True):
            captured['checks'] = checks
            return MagicMock()

        import utils.data_quality as dq

        monkeypatch.setattr(dq, 'run_checks', fake_run_checks)

        # 2018 (start year) -> season_short '1819'.
        dag_module._validate_bronze_quality(params={'season': 2018})

        wheres = {c.params.get('where') for c in captured['checks']}
        assert all("season = '1819'" in w for w in wheres), wheres
        # And it must NOT silently fall back to the current season.
        assert not any("season = '2526'" in (w or '') for w in wheres), wheres

    def test_players_no_duplicates_pk_includes_club(self, dag_module, monkeypatch):
        # A player can appear in two clubs within one historical season
        # (mid-season transfer/loan), so the no_duplicates PK must include
        # current_club_id — keying on player_id alone false-flags those as
        # duplicates and fails the ERROR gate on every backfill (#717).
        captured = {}

        def fake_run_checks(checks, raise_on_error=True):
            captured['checks'] = checks
            return MagicMock()

        import utils.data_quality as dq

        monkeypatch.setattr(dq, 'run_checks', fake_run_checks)
        dag_module._validate_bronze_quality(params={'season': 2024})

        dup = [
            c for c in captured['checks']
            if c.kind == 'no_duplicates'
            and c.params.get('table') == 'bronze.transfermarkt_players'
        ]
        assert dup, "no_duplicates check on players missing"
        assert dup[0].params['pk'] == [
            'league', 'season', 'player_id', 'current_club_id',
        ]


class TestMvTransfersLimitParam:
    """market_value_history / transfers are capped per run and rotate a roster
    window (#620). The cap must be a UI Param defaulting to the weekly value so
    the scheduled run is unchanged, while a historical backfill can override it
    with 0 = no cap = full roster in one run (#793)."""

    def test_limit_param_default_is_weekly_cap(self, dag_module):
        param = dag_module.dag._dag_kwargs['params']['mv_transfers_limit']
        # Default keeps the weekly rotating-window behaviour (#620) unchanged.
        assert param.default == dag_module.MV_HISTORY_DAILY_LIMIT == 100

    def test_mv_and_transfers_render_limit_from_param(self, dag_module):
        # Only the per-player fan-out entities take the cap; players (full crawl)
        # and coaches carry no --limit, so the Param must reach exactly these two.
        for task_id in ('scrape_market_value_history', 'scrape_transfers'):
            task = _bash_task(task_id)
            assert task is not None, f"missing task {task_id}"
            assert '--limit {{ params.mv_transfers_limit }}' in task.bash_command, (
                f"{task_id} does not render the limit from params"
            )
            # Must not bake in the weekly cap — a hardcoded 100 is exactly what
            # capped the #793 backfill to one shallow window per season.
            assert '--limit 100' not in task.bash_command, (
                f"{task_id} still hardcodes the 100-player cap"
            )
