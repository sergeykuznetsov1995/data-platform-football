#!/usr/bin/env python3
"""Append a provider-metered SofaScore canary run to the versioned artifact."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.proxy_filter.budget import anonymize_proxy_exit, append_canary_sample


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--proxy-exit",
        required=True,
        help="Exit identifier/IP; only its SHA-256 is persisted",
    )
    parser.add_argument(
        "--provider-report",
        required=True,
        help="JSON with endpoint_provider_bytes and total_provider_bytes",
    )
    parser.add_argument("--cohort", default="25_matches_50_players")
    parser.add_argument("--mode", required=True)
    eligibility = parser.add_mutually_exclusive_group(required=True)
    eligibility.add_argument("--budget-eligible", action="store_true")
    eligibility.add_argument("--benchmark-only", action="store_true")
    args = parser.parse_args()

    report = json.loads(Path(args.provider_report).read_text(encoding="utf-8"))
    metric_names = (
        "browser_sessions",
        "navigations",
        "request_count",
        "completed_matches",
        "completed_players",
        "matches_per_second",
        "players_per_second",
        "p50_duration_ms",
        "p95_duration_ms",
        "cache_hit_rate",
        "replay_hit_rate",
        "endpoint_completeness",
    )
    append_canary_sample(
        args.artifact,
        {
            "run_id": args.run_id,
            "proxy_exit_hash": anonymize_proxy_exit(args.proxy_exit),
            "cohort": args.cohort,
            "mode": args.mode,
            "budget_eligible": bool(args.budget_eligible),
            "total_provider_bytes": report["total_provider_bytes"],
            "endpoint_provider_bytes": report["endpoint_provider_bytes"],
            "endpoint_request_provider_bytes": report[
                "endpoint_request_provider_bytes"
            ],
            "metrics": {
                name: report[name]
                for name in metric_names
                if name in report
            },
        },
    )


if __name__ == "__main__":
    main()
