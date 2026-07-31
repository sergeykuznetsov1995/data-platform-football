#!/usr/bin/env python3
"""CLI boundary for signed ESPN Native Raw/Bronze execution."""

from __future__ import annotations

import argparse
from datetime import date
import json
import logging
import sys

from scrapers.espn.runner import (
    ExecutionOptions,
    RunnerConfigurationError,
    execute,
)


LOGGER = logging.getLogger(__name__)


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected ISO date YYYY-MM-DD") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one immutable ESPN Native Bronze plan selection"
    )
    parser.add_argument("mode", choices=("daily", "repair", "backfill", "replay"))
    parser.add_argument(
        "--scope",
        action="append",
        default=[],
        help="Exact <espn_id>:<source_year> scope; repeatable (default: signed plan)",
    )
    parser.add_argument("--as-of", required=True, type=_iso_date)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--attempt", required=True, type=int)
    parser.add_argument("--plan-uri", required=True)
    parser.add_argument("--raw-manifest-uri", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--raw-store-uri", required=True)
    parser.add_argument("--max-events", type=int, default=100)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = build_parser().parse_args(argv)
    try:
        options = ExecutionOptions(
            mode=args.mode,
            scopes=tuple(args.scope),
            as_of=args.as_of,
            run_id=args.run_id,
            attempt=args.attempt,
            plan_uri=args.plan_uri,
            raw_manifest_uri=args.raw_manifest_uri,
            output_uri=args.output,
            raw_store_uri=args.raw_store_uri,
            max_events=args.max_events,
        )
        result = execute(options)
    except (RunnerConfigurationError, ValueError, TypeError) as exc:
        LOGGER.error("ESPN runner rejected configuration: %s", exc)
        return 2
    except Exception as exc:  # repository/transport terminal failure
        LOGGER.exception("ESPN runner failed: %s", exc)
        return 1
    print(json.dumps(result.payload, sort_keys=True, separators=(",", ":")))
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
