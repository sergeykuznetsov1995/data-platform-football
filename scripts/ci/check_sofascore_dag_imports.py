"""Validate the seven staged SofaScore DAGs and the players task set."""

from __future__ import annotations

import os

from airflow.models import DagBag


def main() -> None:
    dag_bag = DagBag(
        dag_folder=os.environ["AIRFLOW__CORE__DAGS_FOLDER"],
        include_examples=False,
        safe_mode=False,
    )
    if dag_bag.import_errors:
        details = "\n".join(
            f"{path}: {error}" for path, error in sorted(dag_bag.import_errors.items())
        )
        raise SystemExit(f"real Airflow DAG import errors:\n{details}")

    expected = {
        "dag_ingest_sofascore": 70,
        "dag_backfill_sofascore_all_mens": 6,
        # #1368: the seasonal fields renewal is the lane's eighth task.
        "dag_refresh_sofascore_all_mens": 8,
        "dag_players_sofascore_all_mens": 5,
        "dag_sofascore_manifest_maintenance": 1,
        "dag_sofascore_schedule_maintenance": 1,
        "dag_replay_sofascore_raw": 3,
    }
    actual = set(dag_bag.dag_ids)
    if actual != set(expected):
        raise SystemExit(
            "real Airflow loaded the wrong DAG set: "
            f"missing={sorted(set(expected) - actual)}, "
            f"unexpected={sorted(actual - set(expected))}"
        )
    for dag_id, expected_count in sorted(expected.items()):
        dag = dag_bag.dags[dag_id]
        task_count = len(dag.tasks)
        print(f"{dag_id}: tasks={task_count}")
        if task_count != expected_count:
            raise SystemExit(
                f"real Airflow DAG {dag_id} must expose exactly "
                f"{expected_count} tasks, got {task_count}"
            )
    expected_tasks = {
        "plan_players_batch",
        "run_players_scope",
        "validate_players_scope",
        "finalize_players_run",
        "propagate_players_status",
    }
    actual_tasks = set(dag_bag.dags["dag_players_sofascore_all_mens"].task_ids)
    if actual_tasks != expected_tasks:
        raise SystemExit(
            "real Airflow loaded the wrong players task set: "
            f"missing={sorted(expected_tasks - actual_tasks)}, "
            f"unexpected={sorted(actual_tasks - expected_tasks)}"
        )


if __name__ == "__main__":
    main()
