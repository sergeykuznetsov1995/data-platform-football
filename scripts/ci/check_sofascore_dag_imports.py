"""Fail CI if the staged SofaScore players DAG is missing or cannot import."""

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

    expected = {"dag_players_sofascore_all_mens"}
    actual = set(dag_bag.dag_ids)
    if actual != expected:
        raise SystemExit(
            "real Airflow loaded the wrong DAG set: "
            f"missing={sorted(expected - actual)}, "
            f"unexpected={sorted(actual - expected)}"
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
