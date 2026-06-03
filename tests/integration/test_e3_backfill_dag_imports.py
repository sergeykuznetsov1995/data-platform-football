"""
Smoke integration tests for the E3.5 historical backfill DAG.

Goal: verify ``dag_e3_backfill`` parses cleanly, exposes the expected
parameter shape, task ids, sequential topology, and idempotency
contracts described in ``docs/research/E3.5_inventory.md`` §Wave 5.

Mirrors the structure of ``test_e3_dag_imports.py`` so any future
refactor of the DAG-import smoke pattern can update both files together.
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
class TestE3BackfillDagImports:
    """Verify the E3.5 backfill DAG parses and is wired correctly."""

    DAG_ID = "dag_e3_backfill"

    def _import_errors_for(self, dag_bag, *fragments):
        return {
            k: v for k, v in dag_bag.import_errors.items()
            if any(f in k for f in fragments)
        }

    def test_dag_loads_no_errors(self, dag_bag):
        relevant = self._import_errors_for(dag_bag, "e3_backfill")
        if relevant:
            pytest.fail(
                "DAG import errors: "
                + "; ".join(f"{k}: {v}" for k, v in relevant.items())
            )
        assert self.DAG_ID in dag_bag.dags, (
            f"DAG '{self.DAG_ID}' not found. "
            f"Loaded: {sorted(dag_bag.dags.keys())}"
        )

    def test_dag_schedule_is_none(self, dag_bag):
        """Manual-trigger only — schedule must be None."""
        dag = dag_bag.dags[self.DAG_ID]
        assert dag.schedule_interval is None or str(dag.schedule_interval) in (
            "None", "NOTSET", "Timetable",
        ), (
            f"Backfill DAG must be trigger-only, got "
            f"schedule_interval={dag.schedule_interval!r}"
        )
        assert dag.catchup is False

    def test_dag_paused_on_creation(self, dag_bag):
        """Defence-in-depth — do NOT auto-run when imported by the scheduler."""
        dag = dag_bag.dags[self.DAG_ID]
        assert dag.is_paused_upon_creation is True, (
            "Backfill DAG must be paused on creation to prevent accidental runs"
        )

    def test_dag_max_active_tasks_one(self, dag_bag):
        """Sequential execution — OOM-safety pattern."""
        dag = dag_bag.dags[self.DAG_ID]
        assert dag.max_active_runs == 1
        assert dag.max_active_tasks == 1, (
            f"Backfill tasks must run sequentially. Got "
            f"max_active_tasks={dag.max_active_tasks}"
        )

    def test_dag_has_required_params(self, dag_bag):
        """params dict carries season / league / dry_run with correct defaults."""
        dag = dag_bag.dags[self.DAG_ID]
        params = dag.params
        # Param keys present
        for key in ('season', 'league', 'dry_run'):
            assert key in params, (
                f"Backfill DAG must expose param {key!r}. "
                f"Got: {sorted(params.keys())}"
            )
        # Defaults
        assert params.get('league') == 'ENG-Premier League', (
            f"league default must be 'ENG-Premier League', got {params.get('league')!r}"
        )
        assert params.get('dry_run') is False, (
            f"dry_run default must be False, got {params.get('dry_run')!r}"
        )
        # season has no default (required)
        assert params.get('season') in (None, ''), (
            f"season default should be None (required), got {params.get('season')!r}"
        )

    def test_dag_has_expected_tasks(self, dag_bag):
        """All documented tasks are present (markers + 2 pre-checks + gate
        + 2 silver + 3 gold + validate)."""
        dag = dag_bag.dags[self.DAG_ID]
        task_ids = {t.task_id for t in dag.tasks}

        # Top-level singletons
        for tid in (
            "start_marker",
            "end_marker",
            "pre_check_bronze",
            "taxonomy_diff_check",
            "dry_run_gate",
            "validate_backfill",
        ):
            assert tid in task_ids, (
                f"Top-level task {tid!r} missing. Tasks: {sorted(task_ids)}"
            )

        # silver_backfill TaskGroup tasks
        for suffix in (
            "silver_whoscored_events_spadl_partition",
            "silver_espn_lineup_partition",
        ):
            present = any(
                tid == f"silver_backfill.{suffix}" or tid.endswith(f".{suffix}")
                for tid in task_ids
            )
            assert present, (
                f"silver_backfill task {suffix!r} missing. "
                f"Tasks: {sorted(task_ids)}"
            )

        # gold_backfill TaskGroup tasks
        for suffix in (
            "gold_fct_event_partition",
            "gold_fct_shot_partition",
            "gold_fct_lineup_partition",
        ):
            present = any(
                tid == f"gold_backfill.{suffix}" or tid.endswith(f".{suffix}")
                for tid in task_ids
            )
            assert present, (
                f"gold_backfill task {suffix!r} missing. "
                f"Tasks: {sorted(task_ids)}"
            )

        # Total ≥ 11 tasks (2 markers + 2 pre-checks + 1 gate +
        # 2 silver + 3 gold + 1 validate = 11)
        assert len(dag.tasks) >= 11, (
            f"Backfill DAG expected >=11 tasks, got {len(dag.tasks)}: "
            f"{sorted(task_ids)}"
        )

    def test_taxonomy_check_after_pre_check(self, dag_bag):
        """taxonomy_diff_check must depend on pre_check_bronze."""
        dag = dag_bag.dags[self.DAG_ID]
        tax = dag.get_task("taxonomy_diff_check")
        upstream = {t.task_id for t in tax.upstream_list}
        assert "pre_check_bronze" in upstream, (
            "taxonomy_diff_check must depend on pre_check_bronze. "
            f"Got upstream={sorted(upstream)}"
        )

    def test_dry_run_gate_after_pre_checks(self, dag_bag):
        """dry_run_gate is the short-circuit point — must run AFTER both pre-checks."""
        dag = dag_bag.dags[self.DAG_ID]
        gate = dag.get_task("dry_run_gate")
        seen = set()
        stack = list(gate.upstream_list)
        while stack:
            t = stack.pop()
            if t.task_id in seen:
                continue
            seen.add(t.task_id)
            stack.extend(t.upstream_list)
        for required in ("pre_check_bronze", "taxonomy_diff_check"):
            assert required in seen, (
                f"dry_run_gate must run after {required}. "
                f"Got upstream chain: {sorted(seen)}"
            )

    def test_silver_runs_after_dry_run_gate(self, dag_bag):
        """First Silver task must depend (transitively) on dry_run_gate."""
        dag = dag_bag.dags[self.DAG_ID]
        silver_first = dag.get_task("silver_backfill.silver_whoscored_events_spadl_partition")
        seen = set()
        stack = list(silver_first.upstream_list)
        while stack:
            t = stack.pop()
            if t.task_id in seen:
                continue
            seen.add(t.task_id)
            stack.extend(t.upstream_list)
        assert "dry_run_gate" in seen, (
            "silver tasks must run after dry_run_gate (so dry_run=True skips them). "
            f"Got upstream: {sorted(seen)}"
        )

    def test_silver_sequential_within_group(self, dag_bag):
        """Within silver_backfill, espn_lineup must depend on whoscored_events_spadl."""
        dag = dag_bag.dags[self.DAG_ID]
        espn = dag.get_task("silver_backfill.silver_espn_lineup_partition")
        upstream = {t.task_id for t in espn.upstream_list}
        assert "silver_backfill.silver_whoscored_events_spadl_partition" in upstream, (
            "Silver tasks must run sequentially. Got upstream="
            f"{sorted(upstream)}"
        )

    def test_gold_runs_after_silver(self, dag_bag):
        """First Gold task depends transitively on silver_backfill."""
        dag = dag_bag.dags[self.DAG_ID]
        gold_first = dag.get_task("gold_backfill.gold_fct_event_partition")
        seen = set()
        stack = list(gold_first.upstream_list)
        while stack:
            t = stack.pop()
            if t.task_id in seen:
                continue
            seen.add(t.task_id)
            stack.extend(t.upstream_list)
        silver_seen = {tid for tid in seen if tid.startswith("silver_backfill.")}
        assert silver_seen, (
            "gold_backfill.gold_fct_event_partition has no silver_backfill.* "
            f"upstream — gold can fire before silver. Upstream: {sorted(seen)}"
        )

    def test_gold_sequential_within_group(self, dag_bag):
        """Within gold_backfill, the three facts run in registration order."""
        dag = dag_bag.dags[self.DAG_ID]
        fct_shot = dag.get_task("gold_backfill.gold_fct_shot_partition")
        upstream = {t.task_id for t in fct_shot.upstream_list}
        assert "gold_backfill.gold_fct_event_partition" in upstream, (
            "gold_fct_shot must depend on gold_fct_event for sequential execution. "
            f"Got upstream={sorted(upstream)}"
        )
        fct_lineup = dag.get_task("gold_backfill.gold_fct_lineup_partition")
        upstream = {t.task_id for t in fct_lineup.upstream_list}
        assert "gold_backfill.gold_fct_shot_partition" in upstream, (
            "gold_fct_lineup must depend on gold_fct_shot. "
            f"Got upstream={sorted(upstream)}"
        )

    def test_validate_after_gold(self, dag_bag):
        """validate_backfill depends transitively on gold_backfill."""
        dag = dag_bag.dags[self.DAG_ID]
        validate = dag.get_task("validate_backfill")
        seen = set()
        stack = list(validate.upstream_list)
        while stack:
            t = stack.pop()
            if t.task_id in seen:
                continue
            seen.add(t.task_id)
            stack.extend(t.upstream_list)
        gold_upstream = {tid for tid in seen if tid.startswith("gold_backfill.")}
        assert gold_upstream, (
            "validate_backfill has no gold_backfill.* upstream — DQ can fire "
            f"before facts are materialised. Upstream: {sorted(seen)}"
        )

    def test_validate_trigger_rule_all_success(self, dag_bag):
        """validate_backfill should skip when any transform fails."""
        dag = dag_bag.dags[self.DAG_ID]
        validate = dag.get_task("validate_backfill")
        rule = getattr(validate, "trigger_rule", None)
        rule_value = rule.value if hasattr(rule, "value") else str(rule)
        assert rule_value == "all_success", (
            f"validate_backfill trigger_rule must be 'all_success', got {rule_value!r}"
        )

    def test_tags_include_medallion_e35_and_backfill(self, dag_bag):
        """Tags drive Airflow UI filters; medallion-e3.5 + backfill identify
        this DAG distinctly from production E3."""
        dag = dag_bag.dags[self.DAG_ID]
        tags = set(dag.tags)
        assert "medallion-e3.5" in tags
        assert "backfill" in tags
        assert "silver" in tags
        assert "gold" in tags


@pytest.mark.integration
class TestE3BackfillHelpers:
    """Smoke-test the helper module API exposed for the backfill DAG."""

    def test_silver_partition_helper_exists(self):
        from utils import silver_tasks
        assert hasattr(silver_tasks, "run_silver_partition_insert"), (
            "utils.silver_tasks must expose run_silver_partition_insert"
        )

    def test_gold_partition_wrapped_helper_exists(self):
        from utils import gold_tasks
        assert hasattr(gold_tasks, "run_gold_partition_insert_wrapped"), (
            "utils.gold_tasks must expose run_gold_partition_insert_wrapped"
        )

    def test_e3_dq_per_season_helpers_exist(self):
        from utils import e3_dq
        assert hasattr(e3_dq, "WHOSCORED_KNOWN_TYPES_39"), (
            "utils.e3_dq must expose WHOSCORED_KNOWN_TYPES_39"
        )
        assert len(e3_dq.WHOSCORED_KNOWN_TYPES_39) == 39, (
            f"WHOSCORED_KNOWN_TYPES_39 must have 39 entries, "
            f"got {len(e3_dq.WHOSCORED_KNOWN_TYPES_39)}"
        )
        assert hasattr(e3_dq, "taxonomy_diff_check")
        assert hasattr(e3_dq, "parity_check_event_counts_per_season")
        assert hasattr(e3_dq, "build_per_season_e3_checks")

    def test_per_season_check_builder_returns_check_list(self):
        """build_per_season_e3_checks returns a list of Check instances
        (no Trino calls — pure builder)."""
        from utils.e3_dq import build_per_season_e3_checks
        from utils.data_quality import Check

        checks = build_per_season_e3_checks(season='2324', league='ENG-Premier League')
        assert isinstance(checks, list)
        assert len(checks) >= 5, (
            f"Expected ≥5 per-season checks, got {len(checks)}"
        )
        for c in checks:
            assert isinstance(c, Check), (
                f"Every entry must be a Check instance, got {type(c).__name__}"
            )

    def test_per_season_check_builder_rejects_unsafe_input(self):
        """SQL injection guard — values containing forbidden chars must raise."""
        from utils.e3_dq import build_per_season_e3_checks

        # Case 1: classic SQL-injection probe (semicolon + comment marker).
        # Either guard ('forbidden chars' or 'comment marker') is acceptable.
        with pytest.raises(ValueError, match="forbidden|comment marker"):
            build_per_season_e3_checks(season="2324'; DROP TABLE x --")

        # Case 2: pure comment marker (must trip the comment-marker guard).
        with pytest.raises(ValueError, match="comment marker"):
            build_per_season_e3_checks(season="2324--")
