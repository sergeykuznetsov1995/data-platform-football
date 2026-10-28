"""Manual, pooled catch-up portions; invoked by scripts/compact_bronze.py."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from airflow import DAG
from airflow.sensors.python import PythonSensor

from utils.default_args import SILVER_ARGS


def _catchup(**context):
    from airflow.exceptions import AirflowFailException
    from airflow.sensors.base import PokeReturnValue
    from scrapers.fbref.control import StateConflict
    from utils.bronze_compaction import nightly_deadline, compaction_step

    conf = context["dag_run"].conf
    if type(conf.get("apply")) is not bool:
        raise ValueError("Catch-up apply must be a JSON boolean")
    deadline = datetime.fromisoformat(conf["deadline"])
    # A delayed/rescheduled task retains the original absolute deadline.
    if datetime.now(timezone.utc) >= deadline:
        raise AirflowFailException("Catch-up deadline expired")
    stop_path = Path(conf["stop_file"])
    journal_path = Path(conf["journal"])
    if not stop_path.is_absolute() or not journal_path.is_absolute():
        raise ValueError("Catch-up journal and stop file must be absolute paths")

    class Stop:
        def is_set(self):
            return stop_path.exists()

    with journal_path.open("a", encoding="utf-8") as journal:

        def record(event):
            journal.write(json.dumps(event, default=str, ensure_ascii=False) + "\n")
            journal.flush()

        try:
            result = compaction_step(
                context=context,
                tables=conf["tables"],
                deadline=min(deadline, nightly_deadline()),
                max_portions=conf["portions"],
                max_input_bytes=conf["bytes"],
                dry_run=not conf["apply"],
                stop=Stop(),
                record=record,
            )
        except StateConflict as exc:
            if str(exc) == "FBref publication is locked by another control run":
                return False
            record({"error": str(exc)})
            raise
        except Exception as exc:
            record({"error": str(exc)})
            raise
        record(result)
    if result["stop_reason"] in {"yield", "busy"}:
        return False
    if result["failures"]:
        raise AirflowFailException(
            "Catch-up compaction had table failures; see journal"
        )
    return PokeReturnValue(is_done=True, xcom_value=result)


with DAG(
    dag_id="dag_iceberg_compaction_catchup",
    default_args=SILVER_ARGS,
    schedule=None,
    start_date=datetime(2026, 10, 28),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["maintenance", "iceberg"],
) as dag:
    PythonSensor(
        task_id="compact_bronze",
        python_callable=_catchup,
        pool="fbref_scraper_pool",
        mode="reschedule",
        poke_interval=5,
        timeout=3 * 60 * 60,
        execution_timeout=timedelta(hours=3),
        retries=0,
    )
