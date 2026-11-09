#!/usr/bin/env python3
"""Run bounded Bronze catch-up directly or via Airflow; dry-run by default."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
import uuid

# Host checkouts run this script without Airflow; make dags/utils importable.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from utils.maintenance_window import nightly_deadline  # noqa: E402


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", action="append")
    parser.add_argument("--priority-table", action="append")
    parser.add_argument("--apply", action="store_true", help="default: dry-run")
    parser.add_argument("--direct", action="store_true")
    parser.add_argument("--state-file", type=Path)
    parser.add_argument("--seconds", type=int)
    parser.add_argument("--portions", type=int, default=120)
    parser.add_argument("--bytes", type=int, default=8 * 1024**3)
    parser.add_argument("--journal", type=Path)
    parser.add_argument("--stop-file", type=Path, required=True)
    parser.add_argument("--stop", action="store_true")
    args = parser.parse_args(argv)
    if not args.stop_file.is_absolute():
        parser.error("--stop-file must be absolute and shared with the Airflow worker")
    if args.stop:
        args.stop_file.touch()
        return 0
    if not args.table or not args.journal or not args.journal.is_absolute():
        parser.error("--table and absolute --journal are required")
    if args.direct and (not args.state_file or not args.state_file.is_absolute()):
        parser.error("--direct requires absolute --state-file")
    if args.priority_table and not args.direct:
        parser.error("--priority-table requires --direct")
    if args.priority_table:
        if len(set(args.priority_table)) != len(args.priority_table):
            parser.error("--priority-table values must be unique")
        if set(args.priority_table) - set(args.table):
            parser.error("--priority-table values must be listed in --table")
    if args.seconds is None:
        args.seconds = 10800 if args.direct else 1800
    if (
        not 1 <= args.seconds <= 3 * 60 * 60
        or not 1 <= args.portions <= 120
        or not 1 <= args.bytes <= 8 * 1024**3
    ):
        parser.error("limits exceed the approved profile")
    now = datetime.now(timezone.utc)
    try:
        window_end = nightly_deadline(now)
    except TimeoutError as exc:
        parser.error(f"catch-up: {exc}")
    deadline = min(window_end, now + timedelta(seconds=args.seconds))
    if args.direct:
        from utils.bronze_compaction_direct import run_direct

        return run_direct(args, deadline)
    conf = dict(
        tables=args.table,
        apply=args.apply,
        deadline=deadline.isoformat(),
        portions=args.portions,
        bytes=args.bytes,
        journal=str(args.journal),
        stop_file=str(args.stop_file),
    )
    run_id = "catchup__" + uuid.uuid4().hex
    # Airflow mode retains scheduler admission to fbref_scraper_pool.
    subprocess.run(
        [
            "airflow",
            "dags",
            "trigger",
            "dag_iceberg_compaction_catchup",
            "--run-id",
            run_id,
            "--conf",
            json.dumps(conf),
        ],
        check=True,
    )
    print(f"Queued {run_id}; inspect Airflow state and {args.journal} for results")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
