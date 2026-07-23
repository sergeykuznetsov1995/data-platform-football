"""Create a deterministic-shape, backdated pointer for real Airflow CI imports."""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

from dags.scripts.whoscored_bootstrap import (
    ACCEPTANCE_MODE,
    BOOTSTRAP_POINTER_NAME,
    BOOTSTRAP_WAVES,
    canonical_json_bytes,
    scheduled_run_id,
)


def main() -> None:
    root = Path(os.environ["WHOSCORED_SCHEDULED_PAID_POINTER_ROOT"])
    # Production Airflow runs as uid 50000, gid 0.  Keep the scheduler-readable
    # projection root-owned while permitting only the root group to traverse it.
    root.mkdir(mode=0o750, parents=True, exist_ok=False)
    first = datetime.combine(
        (datetime.now(timezone.utc) - timedelta(days=7)).date(),
        time(10),
        tzinfo=timezone.utc,
    )
    slots = []
    for index, wave_id in enumerate(BOOTSTRAP_WAVES):
        logical_date = first + timedelta(days=index)
        slots.append(
            {
                "run_id": scheduled_run_id(logical_date),
                "logical_date": logical_date.isoformat().replace("+00:00", "Z"),
                "wave_id": wave_id,
            }
        )
    unsigned = {
        "schema_version": 1,
        "acceptance_mode": ACCEPTANCE_MODE,
        "bootstrap_slots": slots,
        "capacity_receipt_sha256": "a" * 64,
        "provider_order_cap_bytes": 1_000_000_000,
        "rollout_id": "ci-airflow-bootstrap",
        "runtime_sha256": "b" * 64,
        "provider_policy_sha256": "c" * 64,
    }
    pointer = {
        **unsigned,
        "authority_sha256": hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest(),
        "signature": "d" * 64,
    }
    path = root / BOOTSTRAP_POINTER_NAME
    path.write_bytes(canonical_json_bytes(pointer) + b"\n")
    path.chmod(0o440)
    for run_id in (
        *(slot["run_id"] for slot in slots),
        scheduled_run_id(first + timedelta(days=len(slots))),
    ):
        run_pointer = {
            "schema_version": 1,
            "dag_id": "dag_ingest_whoscored",
            "run_id": run_id,
            "approval_id": "wsdaily-approval-" + "1" * 32,
            "approval_sha256": "2" * 64,
        }
        run_path = root / (hashlib.sha256(run_id.encode("utf-8")).hexdigest() + ".json")
        run_path.write_bytes(canonical_json_bytes(run_pointer) + b"\n")
        # The real Airflow images run as uid 50000, gid 0. CI creates these
        # fixtures as root, so group-read is required without weakening the
        # production UID-50000/mode-0600 contract.
        run_path.chmod(0o640)


if __name__ == "__main__":
    main()
