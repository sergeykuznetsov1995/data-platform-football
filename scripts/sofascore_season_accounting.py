#!/usr/bin/env python3
"""Produce local season/day JSON from saved inputs; no source/SQL collection."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import sys
import tempfile
from pathlib import Path

if __package__ in (None, ""):
    repository = Path(__file__).resolve().parents[1]
    for location in (repository, repository / "dags"):
        sys.path.insert(0, str(location))

from scrapers.sofascore.season_accounting import (
    apply_observation, digest, summarize_day, summarize_series,
)


def _atomic(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(value, stream, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="saved observations and optional complete UTC days")
    parser.add_argument("--journal", type=Path, required=True, help="local durable journal (never runtime state.json)")
    parser.add_argument("--results", type=Path, required=True, help="local copies of receipts, reports and checkpoints")
    parser.add_argument("--output", type=Path, required=True, help="local produced JSON")
    args = parser.parse_args(argv)
    # Never let an output alias replace a saved input or the durable journal.
    paths = [p.resolve() for p in (args.input, args.journal, args.output)]
    lock_path = args.journal.with_name(args.journal.name + ".lock")
    if len(set(paths + [lock_path.resolve()])) != len(paths) + 1:
        parser.error("input, journal and output must be distinct paths")
    try:
        document = json.loads(args.input.read_text())
        args.journal.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            journal = None
            if args.journal.exists():
                envelope = json.loads(args.journal.read_text())
                journal = envelope["journal"]
                if envelope.get("journal_digest") != digest(journal):
                    raise ValueError("durable journal digest mismatch; preserve and investigate")
            for observation in document.get("observations", []):
                journal = apply_observation(journal, observation)
            if journal is None:
                raise ValueError("at least one initial saved inventory observation required")
            days = [summarize_day(journal, day, args.results) for day in document.get("days", [])]
            output = {"schema_version": 1, "journal_digest": digest(journal),
                      "identity": journal["identity"], "days": days,
                      "series": summarize_series(days), "observations": len(journal["observations"]),
                      "season_denominator": len(journal["roster"]),
                      "acceptance_started_at": None, "source_requests": 0, "production_changes": 0}
            _atomic(args.journal, {"journal": journal, "journal_digest": digest(journal)})
            _atomic(args.output, output)
        print(json.dumps({"output": str(args.output), "days": len(days), "series": output["series"]["verdict"]}))
        return 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(json.dumps({"status": "error", "error": str(error)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
