#!/usr/bin/env python3
"""Assemble a saved seven-tournament tail proof; no HTTP, SQL or scheduler calls."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scrapers.sofascore.tail_evidence import assemble_tail
from scrapers.sofascore.refresh_evidence import write_immutable


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = assemble_tail(args.input)
    write_immutable(args.output, result)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
