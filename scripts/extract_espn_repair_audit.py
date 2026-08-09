#!/usr/bin/env python3
"""Extract snapshot-bound ESPN Top-5 repair evidence from Trino/Iceberg."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile

from scrapers.espn.repair import Top5SnapshotExtractor
from scrapers.espn.repository import EspnBronzeRepository


def _repository_from_env() -> EspnBronzeRepository:
    return EspnBronzeRepository.from_env()


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


def main(
    argv: list[str] | None = None, *, repository_factory=None
) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        repository_factory = repository_factory or _repository_from_env
        evidence = Top5SnapshotExtractor(repository_factory()).extract()
        _atomic_json(args.output, evidence)
        print(
            json.dumps(
                evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
        )
        return 0
    except Exception as exc:
        failure = {
            "schema_version": "espn-top5-audit-extraction-failure-v1",
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
