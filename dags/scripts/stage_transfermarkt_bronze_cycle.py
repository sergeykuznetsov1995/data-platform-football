"""Publish a Transfermarkt scope registry, stage a cycle plan and seal its collected attempts."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

from pyarrow import fs

if __name__ == "__main__":
    project = Path(__file__).resolve().parents[2]
    sys.path[:0] = [str(project), str(project / "dags")]

from dags.scripts import run_transfermarkt_bronze_cycle as cycle
from scrapers.transfermarkt.raw_store import RawResponseStore
from scrapers.transfermarkt.write_journal import _write_once


def _json_bytes(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def stage_registry(root: Path, inventory: Path) -> Path:
    """Publish the original registry bytes under their SHA-256 for later admission."""
    data = inventory.read_bytes()
    document = cycle.read_input_json(data)
    if not isinstance(document, dict) or document.keys() != {"valid_from", "valid_until", "scopes"}:
        raise ValueError("Invalid Transfermarkt registry fields")
    try:
        first, last = (datetime.fromisoformat(document[key]) for key in ("valid_from", "valid_until"))
    except (TypeError, ValueError) as error:
        raise ValueError("Invalid Transfermarkt registry validity window") from error
    if first.utcoffset() is None or last.utcoffset() is None or first >= last:
        raise ValueError("Registry validity requires an ordered, timezone-aware window")
    digest = hashlib.sha256(data).hexdigest()
    validation_cycle = "registry-validation"
    cycle.validate_inventory({"cycle_id": validation_cycle, "registry_snapshot_id": digest,
                              "scopes": document["scopes"]}, validation_cycle)
    target = root / "registry" / f"{digest}.json"
    _write_once(target, data, "A different registry is already published under this digest")
    return target


def stage_plan(root: Path, cycle_id: str, inventory: Path) -> Path:
    """Publish the plan in the same canonical JSON format as the scheduled planner."""
    target = cycle.cycle_directory(root, cycle_id) / "plan.json"
    document = cycle.read_input_json(inventory.read_bytes())
    cycle.validate_inventory(document, cycle_id)
    _write_once(target, _json_bytes(document), "A different plan is already published for this cycle")
    return target


def stage_captures(root: Path, cycle_id: str, journals: list[Path], *, store: RawResponseStore,
                   started_at: datetime, traffic: Path) -> Path:
    """Validate all supplied attempts and dry-run the complete published plan before sealing it."""
    if not isinstance(started_at, datetime) or started_at.utcoffset() is None:
        raise ValueError("The Airflow start time must include a timezone")
    directory = cycle.cycle_directory(root, cycle_id)
    plan = (directory / "plan.json").read_bytes()
    universe, scopes = cycle.validate_inventory(cycle.read_input_json(plan), cycle_id)
    if any(traffic.samefile(journal) for journal in journals):
        raise ValueError("Traffic must come from the collector's separate request counter")
    traffic_bytes = traffic.read_bytes()
    originals = {}
    for journal in journals:
        data = journal.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        if digest in originals:
            raise ValueError("Repeated raw attempt journal")
        originals[digest] = data
    envelopes = cycle.verify_attempt_journals(
        store, cycle_id, (scope.scope_id for scope in scopes),
        [cycle.read_input_json(data) for data in originals.values()], cycle.read_input_json(traffic_bytes),
    )
    for scope in scopes:
        capture = cycle.CapturedScope(scope, tuple(envelopes[scope.scope_id]))
        # Use the consumer's complete replay and data-quality checks without opening a Bronze writer.
        cycle._parse(capture, store, universe=universe, started_at=started_at, write_options={})
    target = directory / "captures.json"
    for digest, data in originals.items():
        _write_once(directory / "evidence" / f"{digest}.json", data, "Original attempt journal differs")
    _write_once(directory / "traffic.json", traffic_bytes, "Bounded traffic receipt differs for this cycle")
    index = {"cycle_id": cycle_id, "plan_sha256": hashlib.sha256(plan).hexdigest(),
             "envelopes_by_scope": envelopes, "attempt_journals": sorted(originals),
             "traffic_sha256": hashlib.sha256(traffic_bytes).hexdigest()}
    _write_once(target, _json_bytes(index), "A different capture index is already published for this cycle")
    return target


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    registry = commands.add_parser("registry", help="publish a dated scope registry under its original SHA-256")
    plan = commands.add_parser("plan", help="publish the existing registry-bound scope inventory")
    captures = commands.add_parser("captures", help="verify the complete bounded attempt journal against the plan")
    for command in (registry, plan, captures):
        command.add_argument("--root", type=Path, required=True, help="Transfermarkt core state directory")
    for command in (plan, captures):
        command.add_argument("--cycle-id", required=True, help="exact Airflow run_id")
    registry.add_argument("--inventory", type=Path, required=True, help="registry JSON with valid_from, valid_until, scopes")
    plan.add_argument("--inventory", type=Path, required=True, help="original JSON with cycle_id, registry_snapshot_id, scopes")
    captures.add_argument("--started-at", type=datetime.fromisoformat, required=True, help="Airflow run start with timezone")
    captures.add_argument("--attempts", type=Path, nargs="+", required=True,
                          help="JSON files containing original attempt records, their IDs, or a raw_attempts journal")
    captures.add_argument("--traffic", type=Path, required=True,
                          help="collector's independent traffic.json for the whole bounded run (request_attempts)")
    captures.add_argument("--raw-store-uri", help="original raw-store URI (defaults to TRANSFERMARKT_RAW_STORE_URI)")
    captures.add_argument("--raw-mirror", type=Path,
                          help="read a local copy of that original store using its unchanged URI identity")
    args = parser.parse_args(argv)
    try:
        if args.command == "registry":
            path = stage_registry(args.root, args.inventory)
        elif args.command == "plan":
            path = stage_plan(args.root, args.cycle_id, args.inventory)
        else:
            if args.raw_mirror is not None:
                if not args.raw_store_uri:
                    raise ValueError("--raw-mirror requires the original --raw-store-uri")
                store = RawResponseStore(fs.LocalFileSystem(), str(args.raw_mirror.resolve()), uri_prefix=args.raw_store_uri)
            else:
                store = RawResponseStore.from_uri(args.raw_store_uri) if args.raw_store_uri else RawResponseStore.from_env()
            path = stage_captures(args.root, args.cycle_id, args.attempts, store=store,
                                  started_at=args.started_at, traffic=args.traffic)
    except (OSError, KeyError, TypeError, ValueError, RuntimeError) as error:
        parser.exit(1, f"{parser.prog}: {error}\n")
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
