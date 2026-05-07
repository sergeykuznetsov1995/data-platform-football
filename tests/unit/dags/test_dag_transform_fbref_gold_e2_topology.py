"""
Topology tests for ``dags/dag_transform_fbref_gold.py`` — E2 master-data
dims (2026-05).

Verifies the integration of the new ``s2b_master_dims`` TaskGroup:
  * 5 new task IDs exist (``dim_venue``, ``dim_referee``, ``dim_standings``,
    ``dim_competition``, ``dim_season``).
  * The group is downstream of ``s2_dimensions`` (specifically the
    pre-existing ``dim_team``/``dim_player``/``dim_match`` tasks).
  * The group is upstream of the s3 facts (``fct_team_match`` etc.).
  * ``dag.max_active_tasks == 1`` is preserved.

Runs on the host without real Airflow — relies on the lightweight stubs
installed by ``tests/unit/dags/conftest.py`` (``_PythonOperator``,
``_TaskGroup``, ``_StubDAG``). Those stubs were extended in this commit
to track operator instances + cross-group ``>>`` edges so this test can
make topology assertions without DagBag.
"""

from __future__ import annotations

import importlib
import sys

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reload_gold_dag():
    """Force a fresh import of dag_transform_fbref_gold.

    Clears the global ``_PythonOperator._instances`` registry so each test
    sees only the operators created by this DAG load.
    """
    from airflow.operators.python import PythonOperator  # stub
    from airflow.operators.bash import BashOperator  # stub

    PythonOperator._instances.clear()
    BashOperator._instances.clear()

    sys.modules.pop("dag_transform_fbref_gold", None)
    sys.modules.pop("dags.dag_transform_fbref_gold", None)

    # The DAG file lives in /root/data_platform/dags/ which is on sys.path
    # via the conftest.
    return importlib.import_module("dag_transform_fbref_gold")


def _all_task_ids():
    """Return the set of task_id strings registered by the most recent load."""
    from airflow.operators.python import PythonOperator  # stub

    return {op.task_id for op in PythonOperator._instances}


def _by_task_id(task_id: str):
    """Return the (single) operator instance for ``task_id``, or raise."""
    from airflow.operators.python import PythonOperator  # stub

    matches = [op for op in PythonOperator._instances if op.task_id == task_id]
    assert len(matches) == 1, (
        f"expected exactly 1 operator with task_id={task_id!r}, "
        f"found {len(matches)}: {matches}"
    )
    return matches[0]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestE2DagLoad:
    """The DAG module imports cleanly under the stubbed Airflow."""

    def test_dag_loads_without_errors(self):
        mod = _reload_gold_dag()
        assert hasattr(mod, "dag")
        assert mod.dag.dag_id == "dag_transform_fbref_gold"

    def test_max_active_tasks_is_one(self):
        """E2 must NOT bump max_active_tasks — Trino RAM budget assumes 1."""
        mod = _reload_gold_dag()
        assert mod.dag._dag_kwargs.get("max_active_tasks") == 1, (
            "max_active_tasks must remain 1 for predictable RAM"
        )


@pytest.mark.unit
class TestE2NewTaskIds:
    """The 5 new master-data dim task IDs exist in the DAG."""

    EXPECTED_E2_TASK_IDS = {
        "s2b_master_dims.dim_venue",
        "s2b_master_dims.dim_referee",
        "s2b_master_dims.dim_standings",
        "s2b_master_dims.dim_competition",
        "s2b_master_dims.dim_season",
    }

    def test_all_e2_task_ids_present(self):
        _reload_gold_dag()
        task_ids = _all_task_ids()
        missing = self.EXPECTED_E2_TASK_IDS - task_ids
        assert not missing, (
            f"missing E2 task IDs: {missing}. Found: "
            f"{[t for t in task_ids if 's2b' in t]}"
        )

    def test_e2_tasks_are_under_s2b_group(self):
        """All 5 new tasks live under the s2b_master_dims group_id prefix."""
        _reload_gold_dag()
        task_ids = _all_task_ids()
        s2b_tasks = {t for t in task_ids if t.startswith("s2b_master_dims.")}
        assert s2b_tasks == self.EXPECTED_E2_TASK_IDS, (
            f"unexpected s2b tasks. Expected {self.EXPECTED_E2_TASK_IDS}, "
            f"got {s2b_tasks}"
        )


@pytest.mark.unit
class TestE2GroupDependencies:
    """``s2b_master_dims`` is downstream of ``s2_dimensions`` and upstream of
    ``s3_facts`` (verified per-task via ``upstream_task_ids``)."""

    def test_dim_venue_downstream_of_dim_team(self):
        """dim_venue runs AFTER dim_team (s2 → s2b)."""
        _reload_gold_dag()
        venue = _by_task_id("s2b_master_dims.dim_venue")
        # The cross-group edge ``g2 >> g2b`` propagates as: every leaf in g2
        # is upstream of every root in g2b. dim_team is one such leaf.
        assert "s2_dimensions.dim_team" in venue.upstream_task_ids, (
            f"dim_venue must be downstream of dim_team. "
            f"upstream_task_ids={venue.upstream_task_ids}"
        )

    def test_all_e2_tasks_downstream_of_s2_dimensions(self):
        """Every task in s2b is downstream of every task in s2 (full fanout)."""
        _reload_gold_dag()
        s2_task_ids = {
            "s2_dimensions.dim_team",
            "s2_dimensions.dim_player",
            "s2_dimensions.dim_match",
        }
        e2_tasks = [
            _by_task_id(tid) for tid in TestE2NewTaskIds.EXPECTED_E2_TASK_IDS
        ]
        for op in e2_tasks:
            missing = s2_task_ids - op.upstream_task_ids
            assert not missing, (
                f"{op.task_id} missing s2 upstreams: {missing}. "
                f"Have: {op.upstream_task_ids}"
            )

    def test_dim_venue_upstream_of_fct_team_match(self):
        """dim_venue runs BEFORE fct_team_match (s2b → s3)."""
        _reload_gold_dag()
        venue = _by_task_id("s2b_master_dims.dim_venue")
        # s3_facts is the next group; fct_team_match is one of its roots.
        assert "s3_facts.fct_team_match" in venue.downstream_task_ids, (
            f"dim_venue must be upstream of fct_team_match. "
            f"downstream_task_ids={venue.downstream_task_ids}"
        )

    def test_all_e2_tasks_upstream_of_s3_facts(self):
        """Every E2 task fans out to every s3 task (full fanout)."""
        _reload_gold_dag()
        s3_task_ids = {
            "s3_facts.fct_team_match",
            "s3_facts.fct_player_match",
            "s3_facts.match_outcomes",
        }
        e2_tasks = [
            _by_task_id(tid) for tid in TestE2NewTaskIds.EXPECTED_E2_TASK_IDS
        ]
        for op in e2_tasks:
            missing = s3_task_ids - op.downstream_task_ids
            assert not missing, (
                f"{op.task_id} missing s3 downstreams: {missing}. "
                f"Have: {op.downstream_task_ids}"
            )


@pytest.mark.unit
class TestE2InlineRendererWiring:
    """The two inline-rendered dims (dim_competition, dim_season) wire into
    the DAG via PythonOperator(python_callable=run_inline_ctas)."""

    def test_dim_competition_uses_run_inline_ctas(self):
        _reload_gold_dag()
        comp = _by_task_id("s2b_master_dims.dim_competition")
        # python_callable points at run_inline_ctas (or its module-level alias)
        assert comp.python_callable is not None
        assert comp.python_callable.__name__ == "run_inline_ctas", (
            f"expected run_inline_ctas, got {comp.python_callable.__name__}"
        )
        # op_kwargs must include the renderer + template path
        assert "renderer" in comp.op_kwargs
        assert "template_sql" in comp.op_kwargs
        assert comp.op_kwargs.get("table_name") == "dim_competition"
        # Master dim is NOT partitioned
        assert comp.op_kwargs.get("partition_cols") is None

    def test_dim_season_uses_run_inline_ctas(self):
        _reload_gold_dag()
        season = _by_task_id("s2b_master_dims.dim_season")
        assert season.python_callable.__name__ == "run_inline_ctas"
        assert season.op_kwargs.get("table_name") == "dim_season"
        assert season.op_kwargs.get("partition_cols") is None

    def test_dim_standings_partitioned_by_league_season(self):
        """dim_standings IS partitioned by (league, season) per the SQL contract."""
        _reload_gold_dag()
        st = _by_task_id("s2b_master_dims.dim_standings")
        assert st.op_kwargs.get("partition_cols") == ["league", "season"], (
            f"dim_standings partition_cols should be ['league','season'], "
            f"got {st.op_kwargs.get('partition_cols')!r}"
        )
