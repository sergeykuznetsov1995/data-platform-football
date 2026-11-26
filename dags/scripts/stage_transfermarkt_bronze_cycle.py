"""Publish a Transfermarkt scope registry, stage a cycle plan and seal its collected attempts."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from dataclasses import asdict
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


def _attempt_entries(document):
    if isinstance(document, dict) and "raw_attempts" in document:
        entries = document["raw_attempts"]
        if not isinstance(entries, list):
            raise ValueError("Invalid raw_attempts journal")
        if any(document.get(field) for field in (
            "error", "errors", "failures", "failed_attempts", "failure_kind", "failure_stage", "check_failures", "cache_sources",
        )) or ("status" in document and document["status"] not in ("complete", "completed", "success", "ok")):
            raise ValueError("Bounded collection reported a failure or used prior-cycle evidence")
        if "network_fetches" in document and (
            type(document["network_fetches"]) is not int or document["network_fetches"] != len(entries)
        ):
            raise ValueError("Journal does not account for every reported network attempt")
    else:
        entries = document if isinstance(document, list) else [document]
    if not entries:
        raise ValueError("Empty bounded attempt journal")
    return entries


def _traffic_attempts(path: Path, cycle_id: str) -> int:
    receipt = cycle.read_input_json(path.read_bytes())
    if not isinstance(receipt, dict) or any(receipt.get(key, cycle_id) != cycle_id for key in ("cycle_id", "run_id")):
        raise ValueError("Invalid traffic receipt or traffic belongs to another Airflow run")
    count = receipt.get("request_attempts")
    if type(count) is not int or count <= 0:
        raise ValueError("Traffic receipt requires a positive integer request_attempts")
    return count


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
    expected_count = _traffic_attempts(traffic, cycle_id)
    by_scope = {scope.scope_id: set() for scope in scopes}
    ordinals = Counter()
    for journal in journals:
        for entry in _attempt_entries(cycle.read_input_json(journal.read_bytes())):
            envelope_id = entry.get("envelope_id") if isinstance(entry, dict) else entry
            if not isinstance(envelope_id, str):
                raise ValueError("Expected a raw attempt record or envelope ID")
            envelope = store.load_attempt_envelope(envelope_id)
            if isinstance(entry, dict) and _json_bytes(entry) != _json_bytes(asdict(envelope)):
                raise ValueError("Journal record differs from its immutable raw envelope")
            if envelope.cycle_id != cycle_id or envelope.scope_id not in by_scope:
                raise ValueError("Original attempt belongs to another cycle or an unplanned scope")
            if envelope.outcome_kind != "response":
                raise ValueError(f"Original attempt recorded a transport error: {envelope.error_kind}")
            ids = by_scope[envelope.scope_id]
            if envelope_id in ids:
                raise ValueError("Repeated raw attempt in the submitted journal")
            ids.add(envelope_id)
            ordinals[envelope.attempt] += 1
    if sum(ordinals.values()) != expected_count:
        raise ValueError("Raw journal does not account for every request_attempts entry in the traffic receipt")
    # Concurrent clients may each start at one. Every later ordinal needs a distinct predecessor.
    if any(i < 1 or (i > 1 and count > ordinals[i - 1]) for i, count in ordinals.items()):
        raise ValueError("Missing earlier raw attempt ordinal")
    envelopes = {key: sorted(ids) for key, ids in by_scope.items()}
    if not all(envelopes.values()):
        raise ValueError("Missing originals for a planned scope")
    for scope in scopes:
        capture = cycle.CapturedScope(scope, tuple(envelopes[scope.scope_id]))
        # Use the consumer's complete replay and data-quality checks without opening a Bronze writer.
        cycle._parse(capture, store, universe=universe, started_at=started_at, write_options={})
    target = directory / "captures.json"
    index = {"cycle_id": cycle_id, "plan_sha256": hashlib.sha256(plan).hexdigest(), "envelopes_by_scope": envelopes}
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
