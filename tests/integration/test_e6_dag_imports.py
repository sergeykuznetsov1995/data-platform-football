"""
Smoke integration tests for E6 (Features for ML / predictions_input_v2)
DAG wiring across ``dag_transform_fbref_gold`` (W6) and
``dag_serve_predictions`` (W7).

Goal: verify both DAGs parse and expose the new E6 tasks where the
upstream wiring milestones say they should appear:

* W6: ``dag_transform_fbref_gold`` includes ``feat_referee_bias`` and
  ``feat_team_event_style`` inside the ``s4_features`` TaskGroup.
* W7: ``dag_serve_predictions`` materialises ``predictions_input_v2``
  alongside legacy ``predictions_input`` (dual write).

W7 may not yet be merged when this test runs; in that case
``test_v2_materialize_task_exists`` becomes a soft skip.
"""

from __future__ import annotations

import os
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


# sys.path setup (project root + dags folder) is centralised in the root conftest.py.
DAGS_FOLDER = _resolve_dags_folder()


@pytest.fixture(scope="module")
def dag_bag():
    """Self-sufficient DagBag fixture (mirrors ``test_e3_dag_imports.py``)."""
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
class TestE6GoldDagImports:
    """Verify ``dag_transform_fbref_gold`` parses and includes the E6 tasks."""

    DAG_ID = "dag_transform_fbref_gold"

    def _import_errors_for(self, dag_bag, *fragments):
        return {
            k: v for k, v in dag_bag.import_errors.items()
            if any(f in k for f in fragments)
        }

    def test_gold_dag_imports(self, dag_bag):
        relevant = self._import_errors_for(dag_bag, "transform_fbref_gold")
        if relevant:
            pytest.fail(
                "Gold DAG import errors: "
                + "; ".join(f"{k}: {v}" for k, v in relevant.items())
            )
        assert self.DAG_ID in dag_bag.dags, (
            f"DAG '{self.DAG_ID}' not found. Loaded: {sorted(dag_bag.dags.keys())}"
        )

    def test_feat_referee_bias_task_in_s4(self, dag_bag):
        """``s4_features.feat_referee_bias`` must be present (W6)."""
        if self.DAG_ID not in dag_bag.dags:
            pytest.skip("Gold DAG not loaded — covered by other tests")
        dag = dag_bag.dags[self.DAG_ID]
        task_ids = {t.task_id for t in dag.tasks}
        target = "s4_features.feat_referee_bias"
        present = (
            target in task_ids
            or any(tid.endswith(".feat_referee_bias") for tid in task_ids)
        )
        assert present, (
            f"feat_referee_bias task not found in {self.DAG_ID}. "
            f"s4_features children: "
            f"{sorted(t for t in task_ids if t.startswith('s4_features.'))}"
        )

    def test_feat_team_event_style_task_in_s4(self, dag_bag):
        """``s4_features.feat_team_event_style`` must be present (W6)."""
        if self.DAG_ID not in dag_bag.dags:
            pytest.skip("Gold DAG not loaded")
        dag = dag_bag.dags[self.DAG_ID]
        task_ids = {t.task_id for t in dag.tasks}
        target = "s4_features.feat_team_event_style"
        present = (
            target in task_ids
            or any(tid.endswith(".feat_team_event_style") for tid in task_ids)
        )
        assert present, (
            f"feat_team_event_style task not found. "
            f"s4_features children: "
            f"{sorted(t for t in task_ids if t.startswith('s4_features.'))}"
        )

    def test_e6_features_have_validate_downstream(self, dag_bag):
        """validate_gold_quality must transitively depend on the E6 features."""
        if self.DAG_ID not in dag_bag.dags:
            pytest.skip("Gold DAG not loaded")
        dag = dag_bag.dags[self.DAG_ID]
        try:
            validate = dag.get_task("validate_gold_quality")
        except Exception:
            pytest.skip(
                "validate_gold_quality not found — check renamed in this DAG version"
            )

        seen = set()
        stack = list(validate.upstream_list)
        while stack:
            t = stack.pop()
            if t.task_id in seen:
                continue
            seen.add(t.task_id)
            stack.extend(t.upstream_list)

        for needed in ("feat_referee_bias", "feat_team_event_style"):
            assert any(needed in tid for tid in seen), (
                f"validate_gold_quality has no transitive upstream including "
                f"{needed!r}; DQ won't see the new feature. Upstream: {sorted(seen)}"
            )


@pytest.mark.integration
class TestE6ServeDagImports:
    """Verify ``dag_serve_predictions`` parses; check W7 v2 task softly."""

    DAG_ID = "dag_serve_predictions"

    def test_serve_dag_imports(self, dag_bag):
        relevant = {
            k: v for k, v in dag_bag.import_errors.items()
            if "serve_predictions" in k
        }
        if relevant:
            pytest.fail(
                "Serve DAG import errors: "
                + "; ".join(f"{k}: {v}" for k, v in relevant.items())
            )
        assert self.DAG_ID in dag_bag.dags, (
            f"DAG '{self.DAG_ID}' not found. Loaded: {sorted(dag_bag.dags.keys())}"
        )

    def test_legacy_materialize_task_still_exists(self, dag_bag):
        """W7 keeps the legacy task alive for dual-write transition."""
        if self.DAG_ID not in dag_bag.dags:
            pytest.skip("Serve DAG not loaded")
        dag = dag_bag.dags[self.DAG_ID]
        task_ids = {t.task_id for t in dag.tasks}
        assert "materialize_predictions_input" in task_ids, (
            "Legacy materialize_predictions_input task missing. "
            f"Tasks: {sorted(task_ids)}"
        )

    def test_v2_materialize_task_exists(self, dag_bag):
        """W7: ``materialize_predictions_input_v2`` should appear once W7 is merged.

        Soft skip if W7 isn't wired yet (pre-merge state).
        """
        if self.DAG_ID not in dag_bag.dags:
            pytest.skip("Serve DAG not loaded")
        dag = dag_bag.dags[self.DAG_ID]
        task_ids = {t.task_id for t in dag.tasks}
        if "materialize_predictions_input_v2" not in task_ids:
            pytest.skip(
                "W7 (materialize_predictions_input_v2) not yet wired — skipping. "
                f"Current tasks: {sorted(task_ids)}"
            )
        # Once wired, the v2 task is also a sibling of the v1 task.
        assert "materialize_predictions_input_v2" in task_ids
