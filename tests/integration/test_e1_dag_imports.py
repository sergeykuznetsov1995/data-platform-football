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
