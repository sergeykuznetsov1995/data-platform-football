#!/usr/bin/env python3
"""Nightly ops-journal lifecycle (full optimize + expire 3d) from the host; dry-run by default."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import logging
import os
from pathlib import Path
import sys

# Host checkouts run this script without Airflow; make dags/utils importable.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from utils.maintenance_window import nightly_deadline  # noqa: E402


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--direct", action="store_true", required=True,
                        help="host mode; the Airflow path is dag_iceberg_maintenance_daily")
    parser.add_argument("--apply", action="store_true", help="default: dry-run")
    parser.add_argument("--table", action="append", help="limit to these ops tables")
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--stop-file", type=Path, required=True)
    # Default leaves the second half of the 00:00-02:00 UTC window to the bronze
    # compaction that follows in the same night (review of #2135).
    parser.add_argument("--seconds", type=int, default=45 * 60)
    args = parser.parse_args(argv)
    if not args.journal.is_absolute() or not args.stop_file.is_absolute():
        parser.error("--journal and --stop-file must be absolute")
    if not 1 <= args.seconds <= 2 * 60 * 60:
        parser.error("--seconds must be within 1..7200")
    now = datetime.now(timezone.utc)
    try:
        window_end = nightly_deadline(now)
    except TimeoutError as exc:
        parser.error(f"ops maintenance: {exc}")
    deadline = min(window_end, now + timedelta(seconds=args.seconds))
    for key in ("TRINO_USER", "TRINO_PASSWORD", "FBREF_CONTROL_DB_URI"):
        if not os.environ.get(key):
            parser.error(f"missing required environment variable {key}")

    from utils import bronze_compaction_direct as direct
    from utils import maintenance_tasks as maintenance
    from utils.ops_maintenance import maintain_ops_tables

    old_logging = logging.root.manager.disable
    # Dependency exceptions can contain URLs, passwords or server diagnostics.
    logging.disable(logging.CRITICAL)
    try:
        try:
            # Same admission as direct compaction: no maintenance DAG may run.
            ready = direct._maintenance_ready(os.environ["FBREF_CONTROL_DB_URI"], now)
        except Exception:
            ready = None
        if ready is not True:
            reason = "maintenance_dag_running" if ready is False else "admission_error"
            with args.journal.open("a") as out:
                out.write(f'{{"event": "summary", "stop_reason": "{reason}", "exit_code": 1}}\n')
            return 1
        result = maintain_ops_tables(
            lambda: maintenance._get_trino_connection(host="127.0.0.1", port=8082),
            only=args.table,
            deadline=deadline,
            stop_file=args.stop_file,
            journal=args.journal,
            dry_run=not args.apply,
        )
        return result["exit_code"]
    except Exception:
        # Even an unavailable journal must not expose credentials in a traceback.
        return 1
    finally:
        logging.disable(old_logging)


if __name__ == "__main__":
    raise SystemExit(main())
