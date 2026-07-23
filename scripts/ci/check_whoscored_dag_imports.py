"""Fail CI when the staged production WhoScored DAG set cannot be imported."""

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
        "dag_ingest_whoscored",
        "dag_backfill_whoscored",
        "dag_canary_whoscored_proxy",
        "dag_backup_whoscored_storage",
    }
    actual = set(dag_bag.dag_ids)
    if actual != expected:
        raise SystemExit(
            "real Airflow loaded the wrong DAG set: "
            f"missing={sorted(expected - actual)}, "
            f"unexpected={sorted(actual - expected)}"
        )


if __name__ == "__main__":
    main()
