#!/usr/bin/env python3
"""Build a fail-closed machine-readable ESPN Top-5 repair queue."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile

from scrapers.espn.repair import audit_top5


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(
                payload,
                handle,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        document = json.loads(args.input.read_text(encoding="utf-8"))
        result = audit_top5(document)
        _atomic_json(args.output, result)
        print(
            json.dumps(
                result, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
        )
        return 0
    except Exception as exc:  # CLI boundary must always leave a machine report.
        failure = {
            "schema_version": "espn-top5-repair-queue-v1",
            "status": "failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        try:
            _atomic_json(args.output, failure)
        except OSError:
            pass
        print(json.dumps(failure, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
