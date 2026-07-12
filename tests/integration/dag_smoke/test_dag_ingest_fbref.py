"""Production-image DagBag checks for all durable FBref DAGs."""

from __future__ import annotations

import os
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
        fetch_tasks = [
            task_id for task_id in dag.task_dict if task_id.startswith("fetch_wave_")
        ]
        assert len(fetch_tasks) == 8
        assert len(dag.task_dict) == 2 * len(fetch_tasks) + 4

    def test_backfill_and_replay_are_manual(self, fbref_dags):
        backfill = fbref_dags["dag_backfill_fbref"]
        replay = fbref_dags["dag_replay_fbref"]
        assert backfill.schedule_interval is None
        assert replay.schedule_interval is None
        backfill_waves = [
            task_id
            for task_id in backfill.task_dict
            if task_id.startswith("fetch_wave_")
        ]
        assert len(backfill_waves) == 25
        assert len(backfill.task_dict) == 2 * len(backfill_waves) + 4
        replay_waves = [
            task_id
            for task_id in replay.task_dict
            if task_id.startswith("parse_wave_")
        ]
        assert len(replay_waves) == 8
        assert len(replay.task_dict) == len(replay_waves) + 3


@pytest.mark.integration
class TestFBrefCurrentFailureEdges:
    def test_each_fetch_is_immediately_followed_by_parse(self, fbref_dags):
        dag = fbref_dags["dag_ingest_fbref"]
        assert dag.task_dict["seed_competition_index"].downstream_task_ids == {
            "fetch_wave_01"
        }
        wave_count = len(
            [
                task_id
                for task_id in dag.task_dict
                if task_id.startswith("fetch_wave_")
            ]
        )
        for number in range(1, wave_count + 1):
            fetch = dag.task_dict[f"fetch_wave_{number:02d}"]
            parse = dag.task_dict[f"parse_wave_{number:02d}"]
            assert fetch.downstream_task_ids == {parse.task_id}
            expected = (
                f"fetch_wave_{number + 1:02d}"
                if number < wave_count
                else "validate_run"
            )
            assert parse.downstream_task_ids == {expected}

    def test_validation_is_the_only_silver_parent(self, fbref_dags):
        dag = fbref_dags["dag_ingest_fbref"]
        validate = dag.task_dict["validate_run"]
        trigger = dag.task_dict["trigger_silver_transform"]
        assert validate.trigger_rule == "all_success"
        assert trigger.trigger_rule == "all_success"
        assert trigger.upstream_task_ids == {"validate_run"}
        assert trigger.wait_for_completion is True
        assert _states(trigger.allowed_states) == ["success"]
        assert _states(trigger.failed_states) == ["failed"]


@pytest.mark.integration
class TestFBrefBoundedModes:
    def test_backfill_has_25_request_cap(self, fbref_dags):
        dag = fbref_dags["dag_backfill_fbref"]
        initialize = dag.task_dict["initialize_run"]
        assert initialize.op_kwargs["run_type"] == "backfill"
        assert initialize.op_kwargs["request_limit"] == (
            "{{ params.request_limit }}"
        )
        assert dag.task_dict["seed_historical_seasons"].downstream_task_ids == {
            "fetch_wave_01"
        }

    def test_replay_has_no_network_task_and_zero_budget(self, fbref_dags):
        dag = fbref_dags["dag_replay_fbref"]
        assert not any(task_id.startswith("fetch") for task_id in dag.task_dict)
        assert not any(task_id.startswith("seed") for task_id in dag.task_dict)
        initialize = dag.task_dict["initialize_run"]
        assert initialize.op_kwargs["run_type"] == "replay"
        assert initialize.op_kwargs["request_limit"] == 0
        assert initialize.op_kwargs["byte_limit_mb"] == 0
        for task_id, task in dag.task_dict.items():
            if task_id.startswith("parse_wave_"):
                assert task.op_kwargs["source_control_run_id"] == (
                    "{{ params.source_control_run_id }}"
                )

    def test_all_three_modes_validate_before_silver(self, fbref_dags):
        for dag in fbref_dags.values():
            validate = dag.task_dict["validate_run"]
            trigger = dag.task_dict["trigger_silver_transform"]
            assert trigger.upstream_task_ids == {validate.task_id}
            assert validate.trigger_rule == "all_success"
            assert trigger.trigger_rule == "all_success"
