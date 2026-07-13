"""Run exactly one FBref fetch wave in a clean, unforked process.

Airflow's LocalExecutor forks the multi-threaded scheduler to run a task, and
Playwright's sync API deadlocks in such a process: Camoufox launches, but the
navigation never opens a socket and no timeout fires. The repository's own
convention (see CLAUDE.md) is therefore that browser scrapers run in their own
subprocess, never inside a PythonOperator.

This runner is that subprocess. It takes the wave parameters, reconstructs the
pipeline from the environment, runs one bounded fetch wave, and prints the wave
result as a single JSON document on stdout. It performs no seeding, no parsing,
and no Silver trigger, so the control plane keeps every budget, lease, and
validation gate it would have applied in-process.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from scrapers.fbref.pipeline import FBrefPipeline, PipelineSettings
from scrapers.fbref.settings import MIB


RESULT_PREFIX = "FBREF_FETCH_WAVE_RESULT:"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-run-id", required=True)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--page-kinds", required=True, help="comma separated")
    parser.add_argument("--run-type", required=True)
    parser.add_argument("--request-limit", type=int, required=True)
    parser.add_argument("--byte-limit-mb", type=int, required=True)
    parser.add_argument("--shard-size", type=int, required=True)
    parser.add_argument("--reservation-mb", type=int, required=True)
    parser.add_argument("--domain-interval-seconds", type=float, required=True)
    parser.add_argument("--proxy-file", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    # Progress goes to stderr (stdout carries the single result document), so a
    # wave that stalls says why: without this the process is silent and the
    # Airflow task can only report that it was killed.
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = build_parser().parse_args(argv)
    page_kinds = [kind for kind in args.page_kinds.split(",") if kind]
    if not page_kinds:
        raise SystemExit("at least one page kind is required")

    settings = PipelineSettings(
        run_type=args.run_type,
        request_limit=args.request_limit,
        byte_limit=args.byte_limit_mb * MIB,
        shard_size=args.shard_size,
        request_reservation_bytes=args.reservation_mb * MIB,
        domain_interval_seconds=args.domain_interval_seconds,
        proxy_file=args.proxy_file,
    )
    result = FBrefPipeline.from_env().fetch_wave(
        args.control_run_id,
        worker_id=args.worker_id,
        page_kinds=page_kinds,
        settings=settings,
    ).as_dict()
    print(f"{RESULT_PREFIX}{json.dumps(result, sort_keys=True)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
