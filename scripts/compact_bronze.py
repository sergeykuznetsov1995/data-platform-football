#!/usr/bin/env python3
"""Queue a bounded catch-up pass in Airflow, or stop it using its shared marker."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import uuid


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", action="append")
    parser.add_argument("--apply", action="store_true", help="default: dry-run")
    parser.add_argument("--seconds", type=int, default=1800)
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
    if (
        not 1 <= args.seconds <= 3 * 60 * 60
        or not 1 <= args.portions <= 120
        or not 1 <= args.bytes <= 8 * 1024**3
    ):
        parser.error("limits exceed the approved profile")
    now = datetime.now(timezone.utc)
    if not 2 <= now.hour < 5:
        parser.error("catch-up is permitted only at 02:00–05:00 UTC")
    deadline = min(
        now.replace(hour=5, minute=0, second=0, microsecond=0),
        now + timedelta(seconds=args.seconds),
    )
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
    # The scheduler owns admission to fbref_scraper_pool; a local CLI may not
    # impersonate a pool slot or execute FBref rewrites directly.
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
