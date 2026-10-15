#!/usr/bin/env python3
"""Evaluate a saved refresh evidence bundle without network access."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scrapers.sofascore.refresh_evidence import (
    assess_nights, assess_third_day, planning_snapshot, write_immutable,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--capture", action="store_true", help="Save the full existing planning read before slicing")
    args = parser.parse_args()
    source = json.loads(args.input.read_text())
    if args.capture:
        result = planning_snapshot(**source)
        write_immutable(args.output, result)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    result = assess_nights(source["expected_runs"], source["bundles"], source.get("expected_revision"))
    if "third_day" in source:
        result["third_day"] = assess_third_day(source["third_day"], source["expected_revision"])
    result.update(source_requests=0, production_changes=0, acceptance_started=False)
    write_immutable(args.output, result)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
