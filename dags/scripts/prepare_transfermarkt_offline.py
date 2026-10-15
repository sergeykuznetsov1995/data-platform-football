#!/usr/bin/env python3
"""Pin existing full registry/source exports or qualify two offline parity days.

No HTTP, SQL connection, runtime activation, budget reset or acceptance mutation.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "dags") not in sys.path:
    sys.path.append(str(ROOT / "dags"))


def main(argv=None):
    from scrapers.transfermarkt.offline_qualification import export_raw_store, pin_registry, qualify_offline

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    export = commands.add_parser("export-raw", help="verify/decode an existing local RawResponseStore, preserve original receipts")
    export.add_argument("--raw-store", required=True, type=Path)
    export.add_argument("--output", required=True, type=Path)
    pin = commands.add_parser("pin", help="freeze full exported promoted registry and original source mapping")
    for name in ("registry", "denominator", "inventory", "mappings", "cohort"):
        pin.add_argument(f"--{name}", required=True, type=Path)
    qualification = commands.add_parser("qualify", help="reparse exported raw and apply existing two-day gates")
    for name in ("pin-directory", "day1", "day2"):
        qualification.add_argument(f"--{name}", required=True, type=Path)
    for command in (pin, qualification):
        command.add_argument("--capture-index", required=True, type=Path)
        command.add_argument("--output", required=True, type=Path)
        command.add_argument("--evidence-kind", required=True, choices=("fixture", "exported"))
    args = vars(parser.parse_args(argv))
    command = args.pop("command")
    result = {"pin": pin_registry, "qualify": qualify_offline, "export-raw": export_raw_store}[command](**args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
