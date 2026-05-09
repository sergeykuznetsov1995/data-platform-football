"""
Smoke integration tests for E3 (Core Match Facts) DAG wiring.

Goal: verify ``dag_transform_e3`` parses, exposes the expected task ids
inside the documented TaskGroups, and is wired into ``dag_master_pipeline``
after the xref TaskGroup.

We re-use the project-level DAG folder layout — no Airflow DB is needed,
DagBag parses the .py files directly. The fixture is local so the test
module is self-sufficient (mirrors ``test_e1_dag_imports.py`` /
``test_e5_dag_imports.py``).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_dags_folder() -> Path:
    """Pick the DAGs folder — env override (Airflow container) or repo root."""
    env_folder = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER")
    if env_folder and Path(env_folder).is_dir():
        return Path(env_folder)
    # Fallback: repo-root layout (e.g. host run + DagBag for repo tests).
    candidate = PROJECT_ROOT / "dags"
    if candidate.is_dir():
        return candidate
    # Last-ditch: /opt/airflow/dags (container default).
    return Path("/opt/airflow/dags")


DAGS_FOLDER = _resolve_dags_folder()
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
class TestE3DagImports:
    """Verify the E3 core-facts DAG parses and is wired correctly."""

    DAG_ID = "dag_transform_e3"

    def _import_errors_for(self, dag_bag, *fragments):
        return {
            k: v for k, v in dag_bag.import_errors.items()
            if any(f in k for f in fragments)
        }

    def test_e3_dag_loads(self, dag_bag):
        relevant = self._import_errors_for(
            dag_bag, "transform_e3", "master_pipeline"
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

    def test_e3_dag_no_import_errors(self, dag_bag):
        """E3 module + master_pipeline must import cleanly (no scrapers/* leak)."""
        relevant = self._import_errors_for(
            dag_bag, "transform_e3", "master_pipeline"
        )
        assert relevant == {}, (
            "E3 DAG import errors: "
            + "; ".join(f"{k}: {v}" for k, v in relevant.items())
        )

    def test_e3_dag_schedule_is_none(self, dag_bag):
        """Triggered by master_pipeline; not on schedule."""
        dag = dag_bag.dags[self.DAG_ID]
        assert dag.schedule_interval is None or str(dag.schedule_interval) in (
            "None", "NOTSET", "Timetable",
        ), (
            f"E3 DAG must be trigger-only, got "
            f"schedule_interval={dag.schedule_interval!r}"
        )
        assert dag.catchup is False

    def test_e3_dag_uses_silver_args(self, dag_bag):
        """Sequential execution + max_active_runs=1 — OOM-safety pattern."""
        dag = dag_bag.dags[self.DAG_ID]
        assert dag.max_active_runs == 1
        assert dag.max_active_tasks == 1, (
            "E3 tasks must run sequentially. Got "
            f"max_active_tasks={dag.max_active_tasks}"
        )

    def test_e3_dag_has_expected_tasks(self, dag_bag):
        """All 8 documented tasks are present (markers + 2 silver + 3 gold + validate)."""
        dag = dag_bag.dags[self.DAG_ID]
        task_ids = {t.task_id for t in dag.tasks}

        # Top-level markers + validate
        for tid in ("start_marker", "end_marker", "validate_e3"):
            assert tid in task_ids, (
                f"Top-level task {tid!r} missing. Tasks: {sorted(task_ids)}"
            )

        # TaskGroup-prefixed silver_e3
        for suffix in ("whoscored_events_spadl", "espn_lineup"):
            present = any(
                tid == f"silver_e3.{suffix}" or tid.endswith(f".{suffix}")
                for tid in task_ids
            )
            assert present, (
                f"silver_e3 task {suffix!r} missing. Tasks: {sorted(task_ids)}"
            )

        # TaskGroup-prefixed gold_e3
        for suffix in ("fct_event", "fct_shot", "fct_lineup"):
            present = any(
                tid == f"gold_e3.{suffix}" or tid.endswith(f".{suffix}")
                for tid in task_ids
            )
            assert present, (
                f"gold_e3 task {suffix!r} missing. Tasks: {sorted(task_ids)}"
            )

        # Total ≥ 8 (2 markers + 2 silver + 3 gold + 1 validate).
        assert len(dag.tasks) >= 8, (
            f"E3 DAG expected >=8 tasks, got {len(dag.tasks)}: {sorted(task_ids)}"
        )

    def test_e3_dag_taskgroup_topology(self, dag_bag):
        """Verify TaskGroup-prefixed task ids — silver_e3 and gold_e3."""
        dag = dag_bag.dags[self.DAG_ID]
        task_ids = {t.task_id for t in dag.tasks}

        for tid in (
            "silver_e3.whoscored_events_spadl",
            "silver_e3.espn_lineup",
            "gold_e3.fct_event",
            "gold_e3.fct_shot",
            "gold_e3.fct_lineup",
        ):
            assert tid in task_ids, (
                f"Missing TaskGroup-prefixed task {tid!r}. "
                f"Tasks: {sorted(task_ids)}"
            )

    def test_e3_dag_silver_runs_before_gold(self, dag_bag):
        """The first gold task must depend (transitively) on a silver task."""
        dag = dag_bag.dags[self.DAG_ID]

        gold_first = dag.get_task("gold_e3.fct_event")
        # Transitive upstream — walk the upstream tree to spot silver_e3 tasks.
        seen = set()
        stack = list(gold_first.upstream_list)
        while stack:
            t = stack.pop()
            if t.task_id in seen:
                continue
            seen.add(t.task_id)
            stack.extend(t.upstream_list)

        silver_seen = {tid for tid in seen if tid.startswith("silver_e3.")}
        assert silver_seen, (
            "gold_e3.fct_event has no silver_e3.* upstream — gold can fire "
            f"before silver. Upstream chain: {sorted(seen)}"
        )

    def test_e3_dag_validate_runs_after_gold(self, dag_bag):
        """validate_e3 must depend on the gold TaskGroup."""
        dag = dag_bag.dags[self.DAG_ID]
        validate = dag.get_task("validate_e3")

        # Walk transitive upstream for gold_e3.* presence.
        seen = set()
        stack = list(validate.upstream_list)
        while stack:
            t = stack.pop()
            if t.task_id in seen:
                continue
            seen.add(t.task_id)
            stack.extend(t.upstream_list)

        gold_upstream = {tid for tid in seen if tid.startswith("gold_e3.")}
        assert gold_upstream, (
            "validate_e3 has no gold_e3.* upstream — DQ can fire before "
            f"facts are materialised. Upstream: {sorted(seen)}"
        )

    def test_e3_dag_validate_trigger_rule_all_success(self, dag_bag):
        """validate_e3 should skip when any transform fails (trigger_rule=all_success)."""
        dag = dag_bag.dags[self.DAG_ID]
        validate = dag.get_task("validate_e3")
        # In Airflow 2+, trigger_rule is a TriggerRule enum or a str.
        rule = getattr(validate, "trigger_rule", None)
        rule_value = rule.value if hasattr(rule, "value") else str(rule)
        assert rule_value == "all_success", (
            f"validate_e3 trigger_rule must be 'all_success', got {rule_value!r}"
        )

    def test_e3_dag_silver_tasks_sequential_within_group(self, dag_bag):
        """Within silver_e3, espn_lineup must depend on whoscored_events_spadl."""
        dag = dag_bag.dags[self.DAG_ID]
        espn = dag.get_task("silver_e3.espn_lineup")
        upstream_ids = {t.task_id for t in espn.upstream_list}
        assert "silver_e3.whoscored_events_spadl" in upstream_ids, (
            "silver_e3 tasks must run sequentially: espn_lineup must depend on "
            "whoscored_events_spadl. Got upstream="
            f"{sorted(upstream_ids)}"
        )

    def test_e3_dag_gold_tasks_sequential_within_group(self, dag_bag):
        """Within gold_e3, the three facts run in registration order."""
        dag = dag_bag.dags[self.DAG_ID]
        fct_shot = dag.get_task("gold_e3.fct_shot")
        upstream_ids = {t.task_id for t in fct_shot.upstream_list}
        assert "gold_e3.fct_event" in upstream_ids, (
            "gold_e3.fct_shot must depend on gold_e3.fct_event for sequential "
            "execution. Got upstream={!r}".format(sorted(upstream_ids))
        )

        fct_lineup = dag.get_task("gold_e3.fct_lineup")
        upstream_ids = {t.task_id for t in fct_lineup.upstream_list}
        assert "gold_e3.fct_shot" in upstream_ids, (
            "gold_e3.fct_lineup must depend on gold_e3.fct_shot. "
            f"Got upstream={sorted(upstream_ids)}"
        )

    def test_e3_dag_tags_include_medallion_e3(self, dag_bag):
        """Tags drive Airflow UI filters; medallion-e3 is the iteration tag."""
        dag = dag_bag.dags[self.DAG_ID]
        tags = set(dag.tags)
        assert "medallion-e3" in tags
        assert "silver" in tags
        assert "gold" in tags

    def test_master_pipeline_triggers_e3(self, dag_bag):
        """``dag_master_pipeline`` must contain a trigger for the E3 DAG."""
        master = dag_bag.dags.get("dag_master_pipeline")
        if master is None:
            pytest.skip("dag_master_pipeline not loaded — skip integration check")
        task_ids = {t.task_id for t in master.tasks}
        candidates = [
            tid for tid in task_ids
            if "e3" in tid.lower() and "trigger" in tid.lower()
        ]
        assert candidates, (
            "Master pipeline missing an E3 trigger task. "
            f"Tasks: {sorted(task_ids)}"
        )
        # Also check that at least one trigger points at dag_transform_e3.
        for tid in candidates:
            t = master.get_task(tid)
            target = getattr(t, "trigger_dag_id", None)
            if target == "dag_transform_e3":
                return
        pytest.fail(
            "No trigger task in master_pipeline targets 'dag_transform_e3'. "
            f"Candidates inspected: {candidates}"
        )

    def test_master_pipeline_e3_runs_after_xref(self, dag_bag):
        """E3 trigger must depend (transitively) on the xref trigger.

        Topology: master_pipeline ingest -> xref -> e3. Re-running E3 against
        a stale xref spine breaks fct_event team_id resolution.
        """
        master = dag_bag.dags.get("dag_master_pipeline")
        if master is None:
            pytest.skip("dag_master_pipeline not loaded")

        # Find the E3 trigger task.
        e3_trigger = None
        for t in master.tasks:
            if (
                getattr(t, "trigger_dag_id", None) == "dag_transform_e3"
            ):
                e3_trigger = t
                break
        if e3_trigger is None:
            pytest.skip("No E3 trigger in master pipeline; covered by another test")

        # Walk transitive upstream looking for the xref trigger.
        seen = set()
        stack = list(e3_trigger.upstream_list)
        while stack:
            t = stack.pop()
            if t.task_id in seen:
                continue
            seen.add(t.task_id)
            stack.extend(t.upstream_list)

        upstream_trigger_targets = set()
        for tid in seen:
            try:
                tt = master.get_task(tid)
            except Exception:
                continue
            target = getattr(tt, "trigger_dag_id", None)
            if target:
                upstream_trigger_targets.add(target)

        assert "dag_transform_xref" in upstream_trigger_targets, (
            "E3 trigger must run after xref trigger in master pipeline. "
            f"Upstream trigger targets: {sorted(upstream_trigger_targets)}"
        )
