#!/usr/bin/env python3
"""Offline #1339 report from paid-proxy-v2 JSONL and saved phase boundaries.

Boundaries JSON: {start, end, lease_ids, warmup: [{lease_id, start, end}]}.
Times are timezone-aware ISO 8601; intervals are [start, end). Include every
lease (also failures and tails) of the selected contour. Missing warmup is
unknown, whereas an empty list explicitly means no warmup in this period.
No historical percentages or rounded MB counters are used.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path


def _time(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("phase boundaries require timezone-aware timestamps")
    return result


def _interval(start, end):
    start, end = _time(start), _time(end)
    if start >= end:
        raise ValueError("phase interval must have start < end")
    return start, end


def _share(label, numerator, denominator):
    return {
        "label": label,
        "cloudflare_bytes": numerator,
        "total_bytes": denominator,
        "percent": 100 * numerator / denominator if denominator else None,
        "status": "available" if denominator else "no_data",
    }


def summarize(events, boundaries):
    start, end = _interval(boundaries["start"], boundaries["end"])
    leases = set(boundaries["lease_ids"])
    if not leases:
        raise ValueError("selected contour requires explicit lease_ids")
    warmup = {}
    for phase in boundaries.get("warmup", []):
        lease = phase["lease_id"]
        low, high = _interval(phase["start"], phase["end"])
        if lease not in leases or low < start or high > end:
            raise ValueError("warmup must belong to the selected contour")
        intervals = warmup.setdefault(lease, [])
        if any(low < previous_end and high > previous_start
               for previous_start, previous_end in intervals):
            raise ValueError("overlapping warmup intervals")
        intervals.append((low, high))
    total = cf = warm_total = warm_cf = 0
    for event in events:
        if (event.get("event_type") != "bytes"
                or event.get("source") != "fbref"
                or event.get("lease_id") not in leases):
            continue
        timestamp = _time(event["occurred_at"])
        if not start <= timestamp < end:
            continue
        if event.get("event_version") != "paid-proxy-v2":
            raise ValueError("expected paid-proxy-v2 byte ledger")
        count = event["bytes"]
        if type(count) is not int or count < 0:
            raise ValueError("ledger bytes must be a nonnegative integer")
        host = event["host"].lower().rstrip(".")
        is_cf = host == "cloudflare.com" or host.endswith(".cloudflare.com")
        total += count
        cf += count if is_cf else 0
        if any(low <= timestamp < high
               for low, high in warmup.get(event["lease_id"], [])):
            warm_total += count
            warm_cf += count if is_cf else 0
    return {
        "period": {"start": start.isoformat(), "end": end.isoformat()},
        "lease_ids": sorted(leases),
        "contour": _share("CF / весь трафик выбранного контура", cf, total),
        "warmup": _share(
            "CF в прогреве / весь прогрев",
            warm_cf if "warmup" in boundaries else None,
            warm_total if "warmup" in boundaries else None,
        ),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--boundaries", type=Path, required=True)
    args = parser.parse_args()
    boundaries = json.loads(args.boundaries.read_text())
    with args.ledger.open() as stream:
        report = summarize((json.loads(line) for line in stream if line.strip()), boundaries)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
