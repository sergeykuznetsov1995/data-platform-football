#!/usr/bin/env python3
"""Atomically review and activate SofaScore registry tournaments."""

from __future__ import annotations

import argparse
import json
import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.sofascore.catalog import SofaScoreCatalog, registry_path  # noqa: E402
from scrapers.sofascore.discovery import write_registry_atomic  # noqa: E402
from scrapers.sofascore.registry import (  # noqa: E402
    approve_tournament,
    reject_tournament,
    set_activation,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Review or activate a SofaScore tournament atomically",
    )
    parser.add_argument("--registry", default=str(registry_path()))
    parser.add_argument("tournament_id", type=int)
    commands = parser.add_subparsers(dest="command", required=True)

    approve = commands.add_parser("approve")
    approve.add_argument("--canonical-id", required=True)
    approve.add_argument("--reviewed-by", required=True)
    approve.add_argument("--reviewed-at")
    approve.add_argument("--evidence", action="append", required=True)
    approve.add_argument("--notes")

    reject = commands.add_parser("reject")
    reject.add_argument("--reviewed-by", required=True)
    reject.add_argument("--reviewed-at")
    reject.add_argument("--evidence", action="append", required=True)
    reject.add_argument("--notes")

    commands.add_parser("enable")
    commands.add_parser("disable")
    return parser


def _read(path: Path) -> Mapping[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        document = json.load(handle)
    SofaScoreCatalog.from_mapping(document)
    return document


def _reviewed_at(value: Optional[str]) -> str:
    return value or datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _references(values: Sequence[str]) -> list[dict[str, str]]:
    return [
        {"type": "operator_reference", "reference": value.strip()}
        for value in values
        if value.strip()
    ]


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    path = Path(args.registry).resolve()
    current = _read(path)
    updated = deepcopy(dict(current))
    matches = [
        (index, item)
        for index, item in enumerate(updated["tournaments"])
        if int(item["unique_tournament_id"]) == args.tournament_id
    ]
    if len(matches) != 1:
        raise ValueError(
            f"registry must contain tournament {args.tournament_id} exactly once"
        )
    index, tournament = matches[0]
    if args.command == "approve":
        replacement = approve_tournament(
            tournament,
            canonical_id=args.canonical_id,
            reviewed_by=args.reviewed_by,
            reviewed_at=_reviewed_at(args.reviewed_at),
            evidence=_references(args.evidence),
            notes=args.notes,
        )
    elif args.command == "reject":
        replacement = reject_tournament(
            tournament,
            reviewed_by=args.reviewed_by,
            reviewed_at=_reviewed_at(args.reviewed_at),
            evidence=_references(args.evidence),
            notes=args.notes,
        )
    else:
        replacement = set_activation(
            tournament, enabled=args.command == "enable"
        )
    updated["tournaments"][index] = replacement
    SofaScoreCatalog.from_mapping(updated)
    changed = write_registry_atomic(path, updated, expected_current=current)
    print(json.dumps({
        "status": "success",
        "tournament_id": args.tournament_id,
        "command": args.command,
        "changed": changed,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
