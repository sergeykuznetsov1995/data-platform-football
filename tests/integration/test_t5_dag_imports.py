"""
Smoke integration test for T5 (cross-source per-season stats) wiring inside
``dag_transform_fbref_gold``.

Verifies:
* the Gold DAG still loads after T5 added two new tasks to STAGE_2_DIMS,
* the two task_ids `fct_player_season_stats` and `fct_keeper_season_stats`
  are present, namespaced under s2_dimensions.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DAGS_FOLDER = PROJECT_ROOT / "dags"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(DAGS_FOLDER))


@pytest.fixture(scope="module")
def dag_bag():
    os.environ.setdefault("AIRFLOW_HOME", str(PROJECT_ROOT / "airflow_home"))
    os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS_FOLDER))
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    os.environ.setdefault(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:///airflow.db"
    )
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("Airflow not installed")
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


@pytest.mark.integration
class TestT5GoldDagSeasonStats:
    DAG_ID = "dag_transform_fbref_gold"
    EXPECTED_T5_TASKS = (
        "fct_player_season_stats",
        "fct_keeper_season_stats",
        "fct_player_season_stats_audit",
        "fct_keeper_season_stats_audit",
    )

    def test_dag_imports_without_errors(self, dag_bag):
        relevant = {
            k: v for k, v in dag_bag.import_errors.items()
            if "transform_fbref_gold" in k
        }
        if relevant:
            pytest.fail(
                "Gold DAG import errors after T5: "
                + "; ".join(f"{k}: {v}" for k, v in relevant.items())
            )

    def test_t5_tasks_present_in_s2_dimensions(self, dag_bag):
        dag = dag_bag.dags.get(self.DAG_ID)
        assert dag is not None, (
            f"DAG '{self.DAG_ID}' not found. "
            f"Loaded: {sorted(dag_bag.dags.keys())}"
        )
        task_ids = {t.task_id for t in dag.tasks}
        for t5 in self.EXPECTED_T5_TASKS:
            present = (
                f"s2_dimensions.{t5}" in task_ids
                or any(tid.endswith(f".{t5}") or tid == t5 for tid in task_ids)
            )
            assert present, (
                f"T5 task '{t5}' missing from {self.DAG_ID}. "
                f"s2_dimensions children: "
                f"{sorted(t for t in task_ids if t.startswith('s2_dimensions.'))}"
            )

    def test_t5_tasks_run_after_dim_player_attributes(self, dag_bag):
        """T5 fct_*_season_stats читают gold.dim_player_attributes (ref_integrity
        check), поэтому они должны выполняться ПОСЛЕ dim_player_attributes
        в той же группе."""
        dag = dag_bag.dags.get(self.DAG_ID)
        assert dag is not None
        # STAGE_2_DIMS — sequential через for loop в TaskGroup, и dependencies
        # внутри группы: dim_team >> dim_player >> dim_player_attributes >>
        # fct_player_season_stats >> fct_keeper_season_stats >> dim_match.
        # Проверяем порядок через task ordering в STAGE_2_DIMS.
        from dag_transform_fbref_gold import STAGE_2_DIMS
        names = [t[0] for t in STAGE_2_DIMS]
        assert names.index('dim_player_attributes') < names.index(
            'fct_player_season_stats'
        ), "fct_player_season_stats must come AFTER dim_player_attributes"
        assert names.index('fct_player_season_stats') < names.index(
            'fct_keeper_season_stats'
        ), "fct_keeper_season_stats follows player variant for predictable order"
        # _audit таблицы должны идти ПОСЛЕ main fct (ref_integrity audit→main).
        assert names.index('fct_keeper_season_stats') < names.index(
            'fct_player_season_stats_audit'
        ), "audit таблицы должны строиться ПОСЛЕ main fct (ref_integrity)"
        assert names.index('fct_player_season_stats_audit') < names.index(
            'fct_keeper_season_stats_audit'
        ), "predictable audit order"
