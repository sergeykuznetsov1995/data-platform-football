#!/usr/bin/env python3
"""Offline report of saved SofaScore history groups. No collection or writes."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scrapers.sofascore.history_groups_report import build_report  # noqa: E402


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--release-root", required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--controller-sha256", required=True)
    parser.add_argument("--checked-at")
    parser.add_argument("--max-age-seconds", type=float, default=86400)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    result = build_report(checkpoint_path=args.checkpoint, campaign_id=args.campaign_id,
                          release_root=args.release_root, release_id=args.release_id,
                          controller_sha256=args.controller_sha256, checked_at=args.checked_at,
                          max_age_seconds=args.max_age_seconds)
    print(json.dumps(result, ensure_ascii=False, indent=2) if args.json else result["summary"])
    return 2 if result["status"] == "UNKNOWN" else 0


if __name__ == "__main__":
    raise SystemExit(main())
