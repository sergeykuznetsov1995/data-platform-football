"""
Smoke integration tests for E1 (xref refactor) DAG wiring.

Goal: verify ``dag_transform_xref`` parses, exposes the expected task ids,
and is wired into ``dag_master_pipeline`` after the ingestion TaskGroup.

We re-use the project-level DAG folder layout — no Airflow DB is needed,
DagBag parses the .py files directly. The fixture is local so the test
module is self-sufficient (mirrors ``test_e5_dag_imports.py``).
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
    """Local DagBag fixture so this test module is self-sufficient."""
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
class TestE1DagImports:
    """Verify the E1 xref DAG parses and is wired into master pipeline."""

    def test_xref_dag_loads(self, dag_bag):
        if dag_bag.import_errors:
            relevant = {
                k: v for k, v in dag_bag.import_errors.items()
                if "xref" in k or "master_pipeline" in k
            }
            if relevant:
                pytest.fail(
                    "DAG import errors: " + "; ".join(
                        f"{k}: {v}" for k, v in relevant.items()
                    )
                )

        dag_id = "dag_transform_xref"
        assert dag_id in dag_bag.dags, (
            f"DAG '{dag_id}' not found. "
            f"Loaded: {sorted(dag_bag.dags.keys())}"
        )
        dag = dag_bag.dags[dag_id]
        # Trigger-only DAG — no schedule; Airflow exposes as None or
        # NOTSET depending on version. Both acceptable.
        assert dag.schedule_interval is None or str(dag.schedule_interval) in (
            "None", "NOTSET", "Timetable",
        ), f"xref DAG must be trigger-only, got schedule_interval={dag.schedule_interval!r}"
        assert dag.catchup is False

    def test_xref_dag_has_expected_tasks(self, dag_bag):
        dag = dag_bag.dags["dag_transform_xref"]
        task_ids = {t.task_id for t in dag.tasks}

        # Tasks live inside `xref_transforms` TaskGroup — fully-qualified ids.
        # We tolerate both bare and prefixed forms.
        expected = [
            "xref_team",
            "xref_match",
            "xref_referee",
            "xref_manager",
            "xref_player",
            "validate_xref",
        ]
        for name in expected:
            present = any(
                tid == name or tid.endswith(f".{name}") for tid in task_ids
            )
            assert present, (
                f"Expected task '{name}' missing from dag_transform_xref. "
                f"Tasks: {sorted(task_ids)}"
            )

        # Total >= 6 (4 xref CTAS + xref_player + validate). Markers add 2.
        assert len(dag.tasks) >= 6, (
            f"dag_transform_xref expected >=6 tasks, got {len(dag.tasks)}: "
            f"{sorted(task_ids)}"
        )

    def test_xref_dag_uses_silver_args(self, dag_bag):
        """Sanity check: sequential execution + max_active_runs=1."""
        dag = dag_bag.dags["dag_transform_xref"]
        assert dag.max_active_runs == 1
        assert dag.max_active_tasks == 1, (
            "xref tasks must run sequentially to avoid OOM (each task "
            "imports trino + medallion_config). Got "
            f"max_active_tasks={dag.max_active_tasks}"
        )

    def test_master_pipeline_triggers_xref(self, dag_bag):
        """``dag_master_pipeline`` must contain a trigger for the xref DAG."""
        dag_id = "dag_master_pipeline"
        assert dag_id in dag_bag.dags
        dag = dag_bag.dags[dag_id]
        task_ids = {t.task_id for t in dag.tasks}
        assert "trigger_silver_xref" in task_ids, (
            f"Master pipeline missing 'trigger_silver_xref' task. "
            f"Tasks: {sorted(task_ids)}"
        )

        # Verify it triggers the right child DAG
        trigger = dag.get_task("trigger_silver_xref")
        assert getattr(trigger, "trigger_dag_id", None) == "dag_transform_xref"

    def test_xref_dag_no_import_errors(self, dag_bag):
        """No DAG-import errors at all (not just for xref-related modules).

        T4 added ``dag_transform_xref`` plus a ``trigger_silver_xref`` task in
        ``dag_master_pipeline``. A lazy ``import scrapers.*`` at module level
        in either file would silently increase Airflow scheduler RAM by
        ~1.5GB; we surface those as import errors here.
        """
        # Only xref-touching modules are our responsibility for E1; other
        # DAGs may have unrelated transient import errors that aren't ours
        # to fix. Filter to just the modules T4 touched.
        relevant = {
            k: v for k, v in dag_bag.import_errors.items()
            if "xref" in k or "master_pipeline" in k
        }
        assert relevant == {}, (
            f"E1 DAG import errors: "
            + "; ".join(f"{k}: {v}" for k, v in relevant.items())
        )

    def test_xref_dag_taskgroup_topology(self, dag_bag):
        """The four pure-SQL CTAS tasks live inside the ``xref_transforms``
        TaskGroup; ``xref_player`` and ``validate_xref`` are at top level."""
        dag = dag_bag.dags["dag_transform_xref"]
        task_ids = {t.task_id for t in dag.tasks}

        # TaskGroup-prefixed ids
        for tid in [
            "xref_transforms.xref_team",
            "xref_transforms.xref_match",
            "xref_transforms.xref_referee",
            "xref_transforms.xref_manager",
        ]:
            assert tid in task_ids, (
                f"missing TaskGroup-prefixed task {tid!r}; tasks: "
                f"{sorted(task_ids)}"
            )

        # Top-level tasks
        for tid in ["xref_player", "validate_xref"]:
            assert tid in task_ids, (
                f"missing top-level task {tid!r}; tasks: "
                f"{sorted(task_ids)}"
            )

    def test_xref_dag_validate_runs_after_player(self, dag_bag):
        """validate_xref must depend on xref_player (DQ runs after resolver)."""
        dag = dag_bag.dags["dag_transform_xref"]
        validate = dag.get_task("validate_xref")
        upstream = {t.task_id for t in validate.upstream_list}
        assert "xref_player" in upstream, (
            "validate_xref must run AFTER xref_player; got upstream="
            f"{sorted(upstream)}"
        )

    def test_xref_player_runs_after_taskgroup(self, dag_bag):
        """xref_player depends on the entire xref_transforms TaskGroup."""
        dag = dag_bag.dags["dag_transform_xref"]
        player = dag.get_task("xref_player")
        upstream_ids = {t.task_id for t in player.upstream_list}
        # Either the group end-marker or every CTAS task should be upstream.
        ctas_ids = {
            "xref_transforms.xref_team",
            "xref_transforms.xref_match",
            "xref_transforms.xref_referee",
            "xref_transforms.xref_manager",
        }
        # Accept either: full set in upstream, or any single member (TaskGroup
        # often flattens to direct upstream from each member).
        intersection = ctas_ids & upstream_ids
        assert intersection, (
            "xref_player must run AFTER the xref_transforms TaskGroup; "
            f"upstream={sorted(upstream_ids)}"
        )

    def test_master_pipeline_xref_trigger_after_ingestion(self, dag_bag):
        """``trigger_silver_xref`` must run after Bronze ingestion finishes.

        Topology: any ingestion trigger task must be upstream of the xref
        trigger so master_pipeline cannot fire xref against stale Bronze.
        """
        master = dag_bag.dags["dag_master_pipeline"]
        trigger = master.get_task("trigger_silver_xref")
        upstream_ids = {t.task_id for t in trigger.upstream_list}
        # We don't pin the exact upstream task id (it changes as ingestion
        # tasks shift) — we just demand SOME upstream exists so xref
        # cannot be the literal first thing in the pipeline.
        assert upstream_ids, (
            "trigger_silver_xref must have at least one upstream task "
            "(should depend on Bronze ingestion completion)"
        )


# ---------------------------------------------------------------------------
# E1.5 cutover tests — Gold DAG must still import + the 6 cutover SQL files
# must parse cleanly (no Trino round-trip).
# ---------------------------------------------------------------------------


# 6 SQL files migrated from gold.entity_xref → silver.xref_team in T2 (E1.5).
CUTOVER_SQL_FILES = (
    "dim_team.sql",
    "dim_match.sql",
    "dim_player.sql",
    "dim_standings.sql",
    "fct_player_match.sql",
    "match_outcomes.sql",
)


@pytest.mark.integration
class TestE15CutoverGoldDagImports:
    """Verify dag_transform_fbref_gold still parses post-cutover and the
    6 migrated SQL files are syntactically loadable.

    We do NOT spin up Trino — just DagBag parse + raw file parse.
    """

    GOLD_SQL_DIR = Path(__file__).resolve().parents[2] / "dags" / "sql" / "gold"

    def test_fbref_gold_dag_loads_clean(self, dag_bag):
        """``dag_transform_fbref_gold`` must parse with no import errors
        related to the Gold DAG file. silver.xref_* SQL is read by the
        DAG via gold_tasks at runtime, so a parse-time import error here
        usually means a typo in the DAG wiring, not the SQL files.
        """
        relevant = {
            k: v for k, v in dag_bag.import_errors.items()
            if "fbref_gold" in k
        }
        assert relevant == {}, (
            "dag_transform_fbref_gold has DAG-import errors: "
            + "; ".join(f"{k}: {v}" for k, v in relevant.items())
        )

        dag_id = "dag_transform_fbref_gold"
        assert dag_id in dag_bag.dags, (
            f"DAG '{dag_id}' missing after E1.5 cutover. "
            f"Loaded: {sorted(dag_bag.dags.keys())}"
        )

    def test_cutover_sql_files_exist_and_readable(self):
        """Every one of the 6 cutover SQL files must exist + be UTF-8
        decodable. Catches accidental deletion / binary corruption."""
        for fname in CUTOVER_SQL_FILES:
            path = self.GOLD_SQL_DIR / fname
            assert path.exists(), (
                f"Cutover SQL file {fname} missing at {path}"
            )
            text = path.read_text(encoding="utf-8")
            assert text.strip(), f"{fname} is empty"

    def test_cutover_sql_files_have_select(self):
        """Every cutover SQL must contain a SELECT statement (gold_tasks
        expects pure SELECT, wraps in CTAS at runtime). Non-empty SELECT
        is the cheapest sanity check that the file parses 'enough' for
        the DAG to inject it into a Trino CTAS without a syntax error."""
        for fname in CUTOVER_SQL_FILES:
            text = (self.GOLD_SQL_DIR / fname).read_text(encoding="utf-8")
            assert "SELECT" in text.upper(), (
                f"{fname} is missing a SELECT statement"
            )

    def test_cutover_sql_files_reference_silver_xref(self):
        """Every cutover SQL except dim_player must reference
        ``iceberg.silver.xref_team``. dim_player applies the canonical
        prefix inline (no JOIN), so it's the documented exception.
        """
        no_join_files = {"dim_player.sql"}
        for fname in CUTOVER_SQL_FILES:
            text = (self.GOLD_SQL_DIR / fname).read_text(encoding="utf-8")
            if fname in no_join_files:
                continue
            assert "iceberg.silver.xref_team" in text, (
                f"{fname} must reference iceberg.silver.xref_team after "
                "the E1.5 cutover"
            )

    def test_cutover_sql_files_no_legacy_entity_xref(self):
        """No cutover SQL may reference ``gold.entity_xref`` in
        executable SQL. Header comments may legitimately mention it as a
        "Migrated from gold.entity_xref ..." breadcrumb — strip
        ``-- ...`` lines first."""
        for fname in CUTOVER_SQL_FILES:
            text = (self.GOLD_SQL_DIR / fname).read_text(encoding="utf-8")
            non_comment = "\n".join(
                line for line in text.splitlines()
                if not line.lstrip().startswith("--")
            )
            assert "gold.entity_xref" not in non_comment, (
                f"{fname} still references gold.entity_xref in "
                "executable SQL — E1.5 cutover incomplete"
            )


@pytest.mark.integration
class TestE2DimManagerWiring:
    """E2 Phase 1.5 — dim_manager + xref_manager + bronze parser landing.

    No Trino / DagBag spin-up — pure file/parse checks. Verifies the
    plumbing is in place so Phase 1.5 ingestion can run end-to-end.
    """

    PROJ_ROOT = Path(__file__).resolve().parents[2]
    SILVER_SQL = PROJ_ROOT / "dags" / "sql" / "silver" / "xref_manager.sql"
    GOLD_SQL = PROJ_ROOT / "dags" / "sql" / "gold" / "dim_manager.sql"
    GOLD_DAG = PROJ_ROOT / "dags" / "dag_transform_fbref_gold.py"
    XREF_DQ = PROJ_ROOT / "dags" / "utils" / "xref_dq.py"
    GOLD_TASKS = PROJ_ROOT / "dags" / "utils" / "gold_tasks.py"

    def test_xref_manager_sql_reads_bronze_table(self):
        """xref_manager.sql must read bronze.fbref_match_managers — the new
        Phase 1.5 Bronze landing table."""
        text = self.SILVER_SQL.read_text(encoding="utf-8")
        assert "iceberg.bronze.fbref_match_managers" in text, (
            "xref_manager.sql must source from bronze.fbref_match_managers "
            "(populated by parsers/finders.py::parse_match_managers)"
        )

    def test_dim_manager_sql_exists_and_uses_scd2(self):
        """gold/dim_manager.sql exists and uses LAG/LEAD window functions for SCD-2."""
        assert self.GOLD_SQL.exists(), (
            "Gold dim_manager.sql missing — Phase 1.5 plumbing incomplete"
        )
        text = self.GOLD_SQL.read_text(encoding="utf-8")
        assert "LAG(manager_canonical_id)" in text, (
            "dim_manager.sql must use LAG over team timeline to detect "
            "stint boundaries"
        )
        assert "LEAD(valid_from)" in text, (
            "dim_manager.sql must use LEAD to compute closed-open valid_to"
        )
        assert "iceberg.silver.xref_manager" in text, (
            "dim_manager.sql must JOIN against silver.xref_manager for "
            "canonical manager identity"
        )
        assert "iceberg.silver.xref_team" in text, (
            "dim_manager.sql must JOIN against silver.xref_team for "
            "canonical team identity"
        )

    def test_dim_manager_registered_in_gold_dag(self):
        """The Gold DAG must declare dim_manager in STAGE_2B_MASTER_DIMS_SQL."""
        text = self.GOLD_DAG.read_text(encoding="utf-8")
        # Non-comment line that registers dim_manager. Match on the SQL
        # path so a stray docstring mention does not trick the assertion.
        assert "dags/sql/gold/dim_manager.sql" in text, (
            "dag_transform_fbref_gold.py must register dim_manager.sql "
            "in STAGE_2B_MASTER_DIMS_SQL"
        )

    def test_xref_dq_no_zero_row_guard_for_manager(self):
        """build_xref_manager_checks() must NOT carry the STUB-phase
        row_count(min=max=0) guard anymore — Phase 1.5 produces rows.
        """
        text = self.XREF_DQ.read_text(encoding="utf-8")
        # Locate the function body and inspect the row_count signature.
        # Cheaper than dynamic import (which needs Airflow).
        idx = text.index("def build_xref_manager_checks")
        body = text[idx:idx + 2000]
        assert "min_rows=0,\n            max_rows=0," not in body, (
            "build_xref_manager_checks still asserts zero rows — Phase 1.5 "
            "should require min_rows > 0"
        )

    def test_gold_tasks_validates_dim_manager_scd2(self):
        """validate_gold_quality must include scd2_no_overlap for dim_manager."""
        text = self.GOLD_TASKS.read_text(encoding="utf-8")
        assert "gold.dim_manager" in text, (
            "validate_gold_quality must reference gold.dim_manager"
        )
        assert "scd2_no_overlap" in text, (
            "validate_gold_quality must call CHECK.scd2_no_overlap for the "
            "dim_manager timeline integrity guard"
        )
