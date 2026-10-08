#!/usr/bin/env python3
"""Reproduce #1361 on frozen September TI observations, without any network."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from deploy.sofascore.pipeline_watchdog import LANES, step  # noqa: E402


def snapshots(path):
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    for line in path.read_text().splitlines():
        day, successful, failed, *_ = line.split("\t")
        day = "2026-" + day
        now = datetime.fromisoformat(day).replace(tzinfo=timezone.utc) + timedelta(days=1, hours=5)
        yield {
            "observed_at": now.isoformat(), "day": day,
            "red_share": {LANES['history']: [int(failed), int(successful) + int(failed)]},
            "evidence": [f"{path.name}:sha256={digest}"],
        }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=ROOT / "tests/fixtures/sofascore_watchdog/history_scope_daily.tsv")
    parser.add_argument("--snapshots-output", type=Path)
    args = parser.parse_args(argv)
    records = list(snapshots(args.input))
    if args.snapshots_output:
        args.snapshots_output.write_text("".join(json.dumps(row) + "\n" for row in records))
    state = {"version": 1}
    alerts = {}
    for snapshot in records:
        result = step(snapshot, [], datetime.fromisoformat(snapshot["observed_at"]), state)
        alerts[snapshot["day"]] = result["rules"]["history:red_share"]["verdict"]
        print(json.dumps(result, ensure_ascii=False))
    expected = [f"2026-09-{day}" for day in range(14, 18)]
    if not all(alerts.get(day) == "active" for day in expected):
        raise SystemExit("14–17 September degradation not detected")
    print(json.dumps({"acceptance": "offline degradation detected", "incident_count": len(state["incidents"]),
                      "source_requests": 0, "publication_calls": 0,
                      "unknown_historical_metrics": ["coverage", "pool_wait", "closed_matches", "paid_requests"]}))


if __name__ == "__main__":
    main()
