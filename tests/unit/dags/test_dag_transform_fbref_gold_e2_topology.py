"""
Topology tests for ``dags/dag_transform_fbref_gold.py`` — star-schema dims
(issue #425, supersedes the E2 master-dims layout).

Verifies the staged dim build order from the design (§7):
  * s2a_config_dims  — dim_competition / dim_season / dim_venue (inline j2)
  * s2b_xref_dims    — dim_player / dim_referee / dim_manager (.sql)
                       + dim_team (inline j2)
  * s2c_dim_match    — dim_match (inline j2, the star centre)
  * s2d_season_blocks — dim_standings / dim_player_attributes / fct_*_season
  * group chaining: s2a >> s2b >> s2c >> s2d >> s3_facts
  * ALL 8 star dims are unpartitioned (partition_cols=None).
  * ``dag.max_active_tasks == 1`` is preserved.

Runs on the host without real Airflow — relies on the lightweight stubs
installed by ``tests/unit/dags/conftest.py`` (``_PythonOperator``,
``_TaskGroup``, ``_StubDAG``).
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


CONFIG_DIM_IDS = {
    "s2a_config_dims.dim_competition",
    "s2a_config_dims.dim_season",
    "s2a_config_dims.dim_venue",
}
XREF_DIM_IDS = {
    "s2b_xref_dims.dim_player",
    "s2b_xref_dims.dim_referee",
    "s2b_xref_dims.dim_manager",
    "s2b_xref_dims.dim_team",
}
DIM_MATCH_ID = "s2c_dim_match.dim_match"
ALL_STAR_DIM_IDS = CONFIG_DIM_IDS | XREF_DIM_IDS | {DIM_MATCH_ID}

# (task_id, expected renderer template suffix) — every star dim that renders
# through run_inline_ctas.
INLINE_DIMS = [
    ("s2a_config_dims.dim_competition", "dim_competition.sql.j2"),
    ("s2a_config_dims.dim_season",      "dim_season.sql.j2"),
    ("s2a_config_dims.dim_venue",       "dim_venue.sql.j2"),
    ("s2b_xref_dims.dim_team",          "dim_team.sql.j2"),
    ("s2c_dim_match.dim_match",         "dim_match.sql.j2"),
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestDagLoad:
    """The DAG module imports cleanly under the stubbed Airflow."""

    def test_dag_loads_without_errors(self):
        mod = _reload_gold_dag()
        assert hasattr(mod, "dag")
        assert mod.dag.dag_id == "dag_transform_fbref_gold"

    def test_max_active_tasks_is_one(self):
        """#425 must NOT bump max_active_tasks — Trino RAM budget assumes 1."""
        mod = _reload_gold_dag()
        assert mod.dag._dag_kwargs.get("max_active_tasks") == 1, (
            "max_active_tasks must remain 1 for predictable RAM"
        )


@pytest.mark.unit
class TestStarDimTaskIds:
    """All 8 star dims exist under their design-stage groups."""

    def test_all_star_dim_task_ids_present(self):
        _reload_gold_dag()
        task_ids = _all_task_ids()
        missing = ALL_STAR_DIM_IDS - task_ids
        assert not missing, (
            f"missing star-dim task IDs: {missing}. Found: "
            f"{[t for t in task_ids if 's2' in t]}"
        )

    def test_season_blocks_under_s2d(self):
        """dim_standings + dim_player_attributes stayed in the season-block
        stage (their redesign is #428, not #425)."""
        _reload_gold_dag()
        task_ids = _all_task_ids()
        assert "s2d_season_blocks.dim_standings" in task_ids
        assert "s2d_season_blocks.dim_player_attributes" in task_ids


@pytest.mark.unit
class TestStageChaining:
    """Design §7 build order via group chaining (s2a >> s2b >> s2c >> ...)."""

    def test_xref_dims_downstream_of_all_config_dims(self):
        """Every s2b task is downstream of every s2a task (full fanout)."""
        _reload_gold_dag()
        for tid in XREF_DIM_IDS:
            op = _by_task_id(tid)
            missing = CONFIG_DIM_IDS - op.upstream_task_ids
            assert not missing, (
                f"{tid} missing s2a upstreams: {missing}. "
                f"Have: {op.upstream_task_ids}"
            )

    def test_dim_match_downstream_of_all_xref_dims(self):
        """dim_match (star centre) builds after every FK-target dim."""
        _reload_gold_dag()
        match = _by_task_id(DIM_MATCH_ID)
        missing = XREF_DIM_IDS - match.upstream_task_ids
        assert not missing, (
            f"dim_match missing xref-dim upstreams: {missing}. "
            f"Have: {match.upstream_task_ids}"
        )

    def test_dim_match_upstream_of_season_blocks(self):
        _reload_gold_dag()
        match = _by_task_id(DIM_MATCH_ID)
        assert "s2d_season_blocks.fct_team_season_stats" in match.downstream_task_ids

    def test_season_blocks_upstream_of_s3_facts(self):
        """s2d fans out to the s3 fact roots."""
        _reload_gold_dag()
        s3_task_ids = {
            "s3_facts.fct_team_match",
            "s3_facts.fct_player_match",
            "s3_facts.match_outcomes",
        }
        op = _by_task_id("s2d_season_blocks.dim_standings")
        missing = s3_task_ids - op.downstream_task_ids
        assert not missing, (
            f"dim_standings missing s3 downstreams: {missing}. "
            f"Have: {op.downstream_task_ids}"
        )


@pytest.mark.unit
class TestDimWiring:
    """Renderer wiring + the design rule 'dims are unpartitioned'."""

    def test_inline_dims_use_run_inline_ctas(self):
        _reload_gold_dag()
        for tid, tpl_suffix in INLINE_DIMS:
            op = _by_task_id(tid)
            assert op.python_callable is not None, tid
            assert op.python_callable.__name__ == "run_inline_ctas", (
                f"{tid}: expected run_inline_ctas, "
                f"got {op.python_callable.__name__}"
            )
            assert "renderer" in op.op_kwargs, tid
            assert op.op_kwargs.get("template_sql", "").endswith(tpl_suffix), tid

    def test_all_star_dims_unpartitioned(self):
        """Star design rule: dims carry NO partitions (incl. the previously
        (league, season)-partitioned dim_team/dim_player/dim_match)."""
        _reload_gold_dag()
        for tid in ALL_STAR_DIM_IDS:
            op = _by_task_id(tid)
            assert op.op_kwargs.get("partition_cols") is None, (
                f"{tid}: star dims must be unpartitioned, "
                f"got {op.op_kwargs.get('partition_cols')!r}"
            )

    def test_dim_standings_partitioned_by_league_season(self):
        """dim_standings IS partitioned by (league, season) per the SQL contract."""
        _reload_gold_dag()
        st = _by_task_id("s2d_season_blocks.dim_standings")
        assert st.op_kwargs.get("partition_cols") == ["league", "season"], (
            f"dim_standings partition_cols should be ['league','season'], "
            f"got {st.op_kwargs.get('partition_cols')!r}"
        )
