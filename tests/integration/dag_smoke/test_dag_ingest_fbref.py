"""Production-image DagBag checks for all durable FBref DAGs."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DAGS_FOLDER = PROJECT_ROOT / "dags"


@pytest.fixture(scope="module")
def fbref_dags():
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

    bag = DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)
    expected = {
        "dag_ingest_fbref",
        "dag_bootstrap_fbref",
        "dag_backfill_fbref",
        "dag_replay_fbref",
    }
    missing = expected.difference(bag.dags)
    assert not missing, (
        f"Missing FBref DAGs {sorted(missing)}; import errors: "
        f"{bag.import_errors}"
    )
    return {dag_id: bag.dags[dag_id] for dag_id in expected}


def _states(values):
    return [str(value) for value in values]


@pytest.mark.integration
class TestFBrefDagBag:
    def test_current_is_daily_and_serial(self, fbref_dags):
        dag = fbref_dags["dag_ingest_fbref"]
        assert str(dag.schedule_interval) == "0 6 * * *"
        assert dag.max_active_runs == 1
        assert dag.max_active_tasks == 1
        assert "run_live_waves" in dag.task_dict
        assert not any(
            task_id.startswith(("fetch_wave_", "parse_wave_"))
            for task_id in dag.task_dict
        )

    def test_backfill_and_replay_are_manual(self, fbref_dags):
        backfill = fbref_dags["dag_backfill_fbref"]
        bootstrap = fbref_dags["dag_bootstrap_fbref"]
        replay = fbref_dags["dag_replay_fbref"]
        assert backfill.schedule_interval is None
        assert bootstrap.schedule_interval is None
        # #1324: a paid path without freshness/Silver gates is paused.
        assert bootstrap.is_paused_upon_creation is True
        assert "paused by default" in bootstrap.doc_md
        assert len(bootstrap.task_dict) == 11
        assert {
            "validate_current_scope_freshness",
            "export_publication_scope",
            "trigger_silver_transform",
        }.isdisjoint(bootstrap.task_dict)
        assert replay.schedule_interval is None
        assert "run_live_waves" in backfill.task_dict
        assert not any(
            task_id.startswith(("fetch_wave_", "parse_wave_"))
            for task_id in backfill.task_dict
        )
        assert not any(
            task_id.startswith(("fetch_wave_", "parse_wave_"))
            for task_id in replay.task_dict
        )
        assert "drain_replay" in replay.task_dict
        assert len(replay.task_dict) == 10


@pytest.mark.integration
class TestFBrefCurrentFailureEdges:
    def test_one_live_runner_owns_fetch_parse_batches(self, fbref_dags):
        dag = fbref_dags["dag_ingest_fbref"]
        assert dag.task_dict["seed_competition_index"].downstream_task_ids == {
            "capture_raw_baseline"
        }
        assert dag.task_dict["capture_raw_baseline"].downstream_task_ids == {
            "recover_raw_before_fetch"
        }
        assert dag.task_dict["recover_raw_before_fetch"].downstream_task_ids == {
            "run_live_waves"
        }
        live = dag.task_dict["run_live_waves"]
        assert live.python_callable.__name__ == "run_fbref_live_waves"
        assert live.op_kwargs["max_batches"] == 14
        assert "player" not in live.op_kwargs["page_kinds"]
        assert "matchlog" not in live.op_kwargs["page_kinds"]
        factory = sys.modules["utils.fbref_current_dag_factory"]
        assert (
            factory.CURRENT_PAGE_KINDS_POLICY
            == "fbref-current-page-kinds-no-players-v1"
        )
        assert live.downstream_task_ids == {"audit_raw_integrity"}
        assert dag.task_dict["audit_raw_integrity"].downstream_task_ids == {
            "choose_publication_path"
        }

    def test_validation_is_the_only_silver_parent(self, fbref_dags):
        dag = fbref_dags["dag_ingest_fbref"]
        validate = dag.task_dict["validate_run"]
        export = dag.task_dict["export_publication_scope"]
        trigger = dag.task_dict["trigger_silver_transform"]
        assert validate.trigger_rule == "all_success"
        assert trigger.trigger_rule == "all_success"
        release = dag.task_dict["release_publication_lock"]
        factory = sys.modules["utils.fbref_current_dag_factory"]
        assert (
            factory.CURRENT_PUBLICATION_ORDER_POLICY
            == "fbref-current-silver-after-lock-v1"
        )
        assert export.upstream_task_ids == {"validate_run"}
        # #1324: Silver after the lock, unawaited; the lock never waits.
        assert trigger.upstream_task_ids == {"release_publication_lock"}
        assert release.task_type == "BranchPythonOperator"
        assert release.upstream_task_ids == {
            "export_publication_scope",
            "release_canary_publication_lock",
        }
        assert trigger.downstream_task_ids == set()
        assert trigger.wait_for_completion is False
        assert trigger.execution_timeout.total_seconds() == 10 * 60


@pytest.mark.integration
class TestFBrefBoundedModes:
    def test_backfill_uses_supported_profile_and_one_live_runner(self, fbref_dags):
        dag = fbref_dags["dag_backfill_fbref"]
        initialize = dag.task_dict["initialize_run"]
        assert initialize.op_kwargs["run_type"] == "backfill"
        assert initialize.op_kwargs["request_limit"] == (
            "{{ dag_run.conf.get('request_limit', params.request_limit) }}"
        )
        assert dag.task_dict["capture_raw_baseline"].downstream_task_ids == {
            "recover_raw_before_fetch"
        }
        assert dag.task_dict["recover_raw_before_fetch"].downstream_task_ids == {
            "seed_historical_seasons"
        }
        assert dag.task_dict["seed_historical_seasons"].downstream_task_ids == {
            "run_live_waves"
        }

    def test_replay_has_no_network_task_and_zero_budget(self, fbref_dags):
        dag = fbref_dags["dag_replay_fbref"]
        assert not any(task_id.startswith("fetch") for task_id in dag.task_dict)
        assert not any(task_id.startswith("seed") for task_id in dag.task_dict)
        initialize = dag.task_dict["initialize_run"]
        assert initialize.op_kwargs["run_type"] == "replay"
        assert initialize.op_kwargs["request_limit"] == 0
        assert initialize.op_kwargs["byte_limit_mb"] == 0
        assert dag.task_dict["drain_replay"].op_kwargs[
            "source_control_run_id"
        ] == (
            "{{ dag_run.conf.get('source_control_run_id', "
            "params.source_control_run_id) }}"
        )

    def test_publishing_modes_validate_before_silver(self, fbref_dags):
        # Bootstrap has no publication tasks at all (see
        # test_backfill_and_replay_are_manual), so only the three publishing
        # DAGs carry this invariant.  Backfill routes validate_run through
        # choose_publication_path before the export.  Ingest and backfill
        # trigger Silver after the publication lock is released (#1324).
        expected_export_parent = {
            "dag_ingest_fbref": "validate_run",
            "dag_backfill_fbref": "choose_publication_path",
            "dag_replay_fbref": "validate_run",
        }
        for dag_id, parent in expected_export_parent.items():
            dag = fbref_dags[dag_id]
            validate = dag.task_dict["validate_run"]
            export = dag.task_dict["export_publication_scope"]
            trigger = dag.task_dict["trigger_silver_transform"]
            assert export.upstream_task_ids == {parent}
            if dag_id == "dag_replay_fbref":
                # Replay keeps export -> Silver (wait) -> lock; the shared
                # finalizer picks its verdict from this topology.
                assert trigger.upstream_task_ids == {export.task_id}
                assert dag.task_dict[
                    "release_publication_lock"
                ].upstream_task_ids == {"trigger_silver_transform"}
            else:
                assert trigger.upstream_task_ids == {
                    "release_publication_lock"
                }
            assert validate.trigger_rule == "all_success"
            assert trigger.trigger_rule == "all_success"
