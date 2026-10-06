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
    def test_current_has_four_utc_windows_and_is_serial(self, fbref_dags):
        dag = fbref_dags["dag_ingest_fbref"]
        assert str(dag.schedule_interval) == "0 0,6,12,18 * * *"
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
        # A paid path without freshness/publication gates is paused.
        assert bootstrap.is_paused_upon_creation is True
        assert "paused by default" in bootstrap.doc_md
        assert len(bootstrap.task_dict) == 11
        assert {
            "validate_current_scope_freshness",
            "export_publication_scope",
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
        assert len(replay.task_dict) == 9


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
        assert "fbref_current_profile" in live.op_kwargs["max_batches"]
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

    def test_validation_exports_scope_then_releases_lock(self, fbref_dags):
        dag = fbref_dags["dag_ingest_fbref"]
        validate = dag.task_dict["validate_run"]
        export = dag.task_dict["export_publication_scope"]
        assert validate.trigger_rule == "all_success"
        release = dag.task_dict["release_publication_lock"]
        assert export.upstream_task_ids == {"validate_run"}
        assert release.task_type == "PythonOperator"
        assert release.upstream_task_ids == {
            "export_publication_scope",
            "release_canary_publication_lock",
        }
        assert release.downstream_task_ids == set()


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

    def test_publishing_modes_validate_before_releasing_lock(self, fbref_dags):
        # Bootstrap has no publication tasks at all (see
        # test_backfill_and_replay_are_manual), so only the three publishing
        # DAGs carry this invariant. Backfill routes validate_run through
        # choose_publication_path before the export.
        expected_export_parent = {
            "dag_ingest_fbref": "validate_run",
            "dag_backfill_fbref": "choose_publication_path",
            "dag_replay_fbref": "validate_run",
        }
        for dag_id, parent in expected_export_parent.items():
            dag = fbref_dags[dag_id]
            validate = dag.task_dict["validate_run"]
            export = dag.task_dict["export_publication_scope"]
            assert export.upstream_task_ids == {parent}
            assert dag.task_dict[
                "release_publication_lock"
            ].upstream_task_ids >= {export.task_id}
            assert validate.trigger_rule == "all_success"


@pytest.mark.integration
@pytest.mark.parametrize("hour,batches,budget", [
    (0, 9, 10800), (6, 20, 16200), (12, 9, 10800), (18, 9, 10800),
])
def test_real_airflow_native_window_templates(fbref_dags, hour, batches, budget):
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace

    dag = fbref_dags["dag_ingest_fbref"]
    task = dag.get_task("run_live_waves")
    end = datetime(2026, 10, 1, hour, tzinfo=timezone.utc)
    rendered = task.render_template(
        {name: task.op_kwargs[name] for name in ("max_batches", "deadline_seconds")},
        {"data_interval_end": end, "data_interval_start": end - timedelta(hours=6),
         "logical_date": end - timedelta(hours=6),
         "dag_run": SimpleNamespace(conf={}), "params": dag.params},
        jinja_env=dag.get_template_env(),
    )
    assert rendered == {"max_batches": batches, "deadline_seconds": budget}
    assert all(type(value) is int for value in rendered.values())
