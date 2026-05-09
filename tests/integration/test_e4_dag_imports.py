"""
Smoke integration tests for E4 (Narrow Facts) DAG wiring.

Mirror of ``test_e3_dag_imports.py``. Verifies ``dag_transform_e4`` parses,
exposes the documented TaskGroups (``silver_e4`` for 4 source-bridge silver
materializations + ``gold_e4`` for 5 narrow-fact passthrough materialisations
+ ``validate_e4``), and is wired into ``dag_master_pipeline`` after E3.

NOTE (2026-05-08): The E4.6 DAG is NOT yet shipped on this branch. The whole
test class is marked ``skip`` until ``dags/dag_transform_e4.py`` lands. Once
it does, drop the skip marker and (if needed) tighten task-id assertions
against the actual DAG topology.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_dags_folder() -> Path:
    env_folder = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER")
    if env_folder and Path(env_folder).is_dir():
        return Path(env_folder)
    candidate = PROJECT_ROOT / "dags"
    if candidate.is_dir():
        return candidate
    return Path("/opt/airflow/dags")


DAGS_FOLDER = _resolve_dags_folder()
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(DAGS_FOLDER))


_E4_DAG_PRESENT = (DAGS_FOLDER / "dag_transform_e4.py").exists()
_skip_until_dag_lands = pytest.mark.skipif(
    not _E4_DAG_PRESENT,
    reason="dag_transform_e4 not yet implemented (E4.6 deliverable)",
)


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
@_skip_until_dag_lands
class TestE4DagImports:
    """Verify the E4 narrow-facts DAG parses and is wired correctly."""

    DAG_ID = "dag_transform_e4"

    def _import_errors_for(self, dag_bag, *fragments):
        return {
            k: v for k, v in dag_bag.import_errors.items()
            if any(f in k for f in fragments)
        }

    def test_e4_dag_loads(self, dag_bag):
        relevant = self._import_errors_for(
            dag_bag, "transform_e4", "master_pipeline"
        )
        if relevant:
            pytest.fail(
                "DAG import errors: " + "; ".join(
                    f"{k}: {v}" for k, v in relevant.items()
                )
            )
        assert self.DAG_ID in dag_bag.dags, (
            f"DAG '{self.DAG_ID}' not found. "
            f"Loaded: {sorted(dag_bag.dags.keys())}"
        )

    def test_e4_dag_no_import_errors(self, dag_bag):
        relevant = self._import_errors_for(
            dag_bag, "transform_e4", "master_pipeline"
        )
        assert relevant == {}, (
            "E4 DAG import errors: "
            + "; ".join(f"{k}: {v}" for k, v in relevant.items())
        )

    def test_e4_dag_schedule_is_none(self, dag_bag):
        dag = dag_bag.dags[self.DAG_ID]
        assert dag.schedule_interval is None or str(dag.schedule_interval) in (
            "None", "NOTSET", "Timetable",
        )
        assert dag.catchup is False

    def test_e4_dag_uses_silver_args(self, dag_bag):
        dag = dag_bag.dags[self.DAG_ID]
        assert dag.max_active_runs == 1
        assert dag.max_active_tasks == 1, (
            "E4 tasks must run sequentially. Got "
            f"max_active_tasks={dag.max_active_tasks}"
        )

    def test_e4_dag_has_expected_tasks(self, dag_bag):
        """All documented top-level + group-prefixed tasks present."""
        dag = dag_bag.dags[self.DAG_ID]
        task_ids = {t.task_id for t in dag.tasks}

        expected_minima = {
            # silver_e4: 4 source bridges
            "silver_e4.match_cards",
            "silver_e4.match_substitutions",
            "silver_e4.matchhistory_match_odds",
            "silver_e4.sofascore_player_ratings",
            # gold_e4: 5 narrow facts
            "gold_e4.fct_goal",
            "gold_e4.fct_card",
            "gold_e4.fct_substitution",
            "gold_e4.fct_match_odds",
            "gold_e4.fct_match_rating",
        }
        # The DAG also has start/end markers + a validate task; assert that
        # the union of expected_minima + ≥1 validate-style task is present.
        missing = expected_minima - task_ids
        assert not missing, (
            f"E4 DAG missing tasks: {sorted(missing)}. "
            f"Tasks: {sorted(task_ids)}"
        )
        # Total must be ≥ 12 (4 silver + 5 gold + ≥3 markers/validate).
        assert len(dag.tasks) >= 12, (
            f"E4 DAG expected >=12 tasks, got {len(dag.tasks)}: "
            f"{sorted(task_ids)}"
        )

    def test_e4_dag_silver_runs_before_gold(self, dag_bag):
        """Earliest gold task must transitively depend on a silver task."""
        dag = dag_bag.dags[self.DAG_ID]

        # Pick ANY gold task and walk upstream until we find silver_e4.*.
        gold_task_ids = [
            t.task_id for t in dag.tasks if t.task_id.startswith("gold_e4.")
        ]
        assert gold_task_ids, "no gold_e4.* tasks present"
        gold_task = dag.get_task(gold_task_ids[0])

        seen = set()
        stack = list(gold_task.upstream_list)
        while stack:
            t = stack.pop()
            if t.task_id in seen:
                continue
            seen.add(t.task_id)
            stack.extend(t.upstream_list)

        silver_seen = {tid for tid in seen if tid.startswith("silver_e4.")}
        assert silver_seen, (
            f"{gold_task.task_id} has no silver_e4.* upstream. "
            f"Upstream chain: {sorted(seen)}"
        )

    def test_e4_dag_validate_runs_after_gold(self, dag_bag):
        dag = dag_bag.dags[self.DAG_ID]
        # Find a validate task — name may be 'validate_e4' or contain 'validate'.
        validate_candidates = [
            t for t in dag.tasks
            if "validate" in t.task_id.lower() and "e4" in t.task_id.lower()
        ]
        if not validate_candidates:
            pytest.skip("no validate_e4-like task — covered by other DAGs")
        validate = validate_candidates[0]

        seen = set()
        stack = list(validate.upstream_list)
        while stack:
            t = stack.pop()
            if t.task_id in seen:
                continue
            seen.add(t.task_id)
            stack.extend(t.upstream_list)

        gold_upstream = {tid for tid in seen if tid.startswith("gold_e4.")}
        assert gold_upstream, (
            f"{validate.task_id} has no gold_e4.* upstream. "
            f"Upstream: {sorted(seen)}"
        )

    def test_e4_dag_tags_include_medallion_e4(self, dag_bag):
        dag = dag_bag.dags[self.DAG_ID]
        tags = set(dag.tags)
        assert "medallion-e4" in tags
        assert "silver" in tags
        assert "gold" in tags

    def test_master_pipeline_triggers_e4(self, dag_bag):
        master = dag_bag.dags.get("dag_master_pipeline")
        if master is None:
            pytest.skip("dag_master_pipeline not loaded")
        task_ids = {t.task_id for t in master.tasks}
        candidates = [
            tid for tid in task_ids
            if "e4" in tid.lower() and "trigger" in tid.lower()
        ]
        assert candidates, (
            "Master pipeline missing an E4 trigger task. "
            f"Tasks: {sorted(task_ids)}"
        )
        for tid in candidates:
            t = master.get_task(tid)
            target = getattr(t, "trigger_dag_id", None)
            if target == "dag_transform_e4":
                return
        pytest.fail(
            "No trigger task in master_pipeline targets 'dag_transform_e4'. "
            f"Candidates inspected: {candidates}"
        )

    def test_master_pipeline_e4_runs_after_e3(self, dag_bag):
        """E4 trigger must depend (transitively) on the E3 trigger.

        Topology: master_pipeline ingest -> xref -> e3 -> e4. E4 narrow facts
        depend on Gold dim_match (E2) and silver xref_* (E1) being fresh,
        which is guaranteed only after E3 finishes.
        """
        master = dag_bag.dags.get("dag_master_pipeline")
        if master is None:
            pytest.skip("dag_master_pipeline not loaded")

        e4_trigger = None
        for t in master.tasks:
            if getattr(t, "trigger_dag_id", None) == "dag_transform_e4":
                e4_trigger = t
                break
        if e4_trigger is None:
            pytest.skip("no E4 trigger in master pipeline")

        seen = set()
        stack = list(e4_trigger.upstream_list)
        while stack:
            t = stack.pop()
            if t.task_id in seen:
                continue
            seen.add(t.task_id)
            stack.extend(t.upstream_list)

        upstream_targets = set()
        for tid in seen:
            try:
                tt = master.get_task(tid)
            except Exception:
                continue
            target = getattr(tt, "trigger_dag_id", None)
            if target:
                upstream_targets.add(target)

        assert "dag_transform_e3" in upstream_targets, (
            "E4 trigger must run after E3 trigger in master pipeline. "
            f"Upstream trigger targets: {sorted(upstream_targets)}"
        )
