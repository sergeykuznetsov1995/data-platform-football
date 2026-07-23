"""Fail CI when the staged production WhoScored DAG set cannot be imported."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow import plugins_manager
from airflow.models import DagBag
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.timetables.base import TimeRestriction

from dags.scripts.whoscored_bootstrap import (
    BOOTSTRAP_SLOT_COUNT,
    BOOTSTRAP_WAVES,
    AcceleratedBootstrapTimetable,
    scheduled_run_id,
)


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

    plugins_manager.initialize_timetables_plugins()
    class_path = "dags.scripts.whoscored_bootstrap.AcceleratedBootstrapTimetable"
    if plugins_manager.timetable_classes.get(class_path) is not (
        AcceleratedBootstrapTimetable
    ):
        raise SystemExit("WhoScored bootstrap timetable plugin is not registered")

    ingest = dag_bag.get_dag("dag_ingest_whoscored")
    if ingest is None or not isinstance(
        ingest.timetable, AcceleratedBootstrapTimetable
    ):
        raise SystemExit(
            "WhoScored ingest did not load the protected bootstrap schedule"
        )
    if len(ingest.timetable.bootstrap_slots) != BOOTSTRAP_SLOT_COUNT:
        raise SystemExit("WhoScored ingest loaded the wrong bootstrap slot count")

    restored = SerializedDAG.from_dict(SerializedDAG.to_dict(ingest))
    if not isinstance(restored.timetable, AcceleratedBootstrapTimetable):
        raise SystemExit(
            "WhoScored bootstrap timetable did not round-trip serialization"
        )
    slots = restored.timetable.bootstrap_slots
    if [slot["wave_id"] for slot in slots] != list(BOOTSTRAP_WAVES):
        raise SystemExit("WhoScored bootstrap timetable loaded the wrong wave order")

    first = datetime.fromisoformat(slots[0]["logical_date"].replace("Z", "+00:00"))
    last_slot = datetime.fromisoformat(
        slots[-1]["logical_date"].replace("Z", "+00:00")
    )
    evaluation_now = last_slot + timedelta(days=2) - timedelta(minutes=45)
    replay = AcceleratedBootstrapTimetable(
        slots, now_factory=lambda: evaluation_now
    )
    restriction = TimeRestriction(
        earliest=first - timedelta(days=1), latest=None, catchup=False
    )
    failed_slot_replay = AcceleratedBootstrapTimetable(
        slots,
        now_factory=lambda: evaluation_now,
        pointer_ready=lambda run_id: run_id == slots[0]["run_id"],
    )
    failed_first = failed_slot_replay.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=restriction,
    )
    if failed_first is None or failed_slot_replay.next_dagrun_info(
        last_automated_data_interval=failed_first.data_interval,
        restriction=restriction,
    ) is not None:
        raise SystemExit(
            "WhoScored failed bootstrap slot advanced without next issuance"
        )

    last_interval = None
    observed_run_ids: list[str] = []
    for expected in slots:
        info = replay.next_dagrun_info(
            last_automated_data_interval=last_interval,
            restriction=restriction,
        )
        if info is None:
            raise SystemExit("WhoScored bootstrap timetable stopped before slot six")
        logical_date = info.data_interval.start.astimezone(timezone.utc)
        observed_run_ids.append(scheduled_run_id(logical_date))
        if (
            info.data_interval.end != info.data_interval.start
            or info.run_after != info.data_interval.start
        ):
            raise SystemExit("WhoScored bootstrap timetable emitted a non-exact slot")
        last_interval = info.data_interval
    if observed_run_ids != [slot["run_id"] for slot in slots]:
        raise SystemExit("WhoScored bootstrap timetable replay changed slot identities")

    daily = replay.next_dagrun_info(
        last_automated_data_interval=last_interval,
        restriction=restriction,
    )
    if (
        daily is None
        or daily.data_interval.start != last_slot + timedelta(days=1)
        or daily.data_interval.end != last_slot + timedelta(days=2)
        or daily.run_after != daily.data_interval.end
    ):
        raise SystemExit(
            "WhoScored bootstrap timetable did not transition to daily cadence"
        )


if __name__ == "__main__":
    main()
