#!/usr/bin/env python3
"""Plan/apply one ESPN Native v2 scope promotion or append-only rollback.

Dry-run is the default.  ``--apply`` is the only mutating switch.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
from typing import Callable

from scrapers.espn.migration import (
    MIGRATION_VERSION,
    MigrationError,
    ProductionLeaseStore,
    RepositoryMigrationBackend,
    apply_promotion,
    apply_rollback,
    build_promotion_plan,
    build_rollback_plan,
    load_promotion_evidence,
)
from scrapers.espn.operations import PostgresEspnControlStore
from scrapers.espn.repository import EspnBronzeRepository, canonical_sha256
from scrapers.espn.layout import INTERNAL_SCHEMA, require_layout_mode


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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    promote = subparsers.add_parser("promote", help="plan/apply one scope cutover")
    promote.add_argument("--evidence", required=True, type=Path)
    promote.add_argument("--output", required=True, type=Path)
    promote.add_argument("--catalog", default="iceberg")
    promote.add_argument("--schema", default="bronze")
    promote.add_argument("--internal-schema", default=INTERNAL_SCHEMA)
    promote.add_argument("--apply", action="store_true")

    rollback = subparsers.add_parser(
        "rollback", help="plan/apply an append-only legacy successor"
    )
    rollback.add_argument("--promotion-report", required=True, type=Path)
    rollback.add_argument("--reason", required=True)
    rollback.add_argument("--output", required=True, type=Path)
    rollback.add_argument("--apply", action="store_true")
    return parser


def _live_backend() -> RepositoryMigrationBackend:
    return RepositoryMigrationBackend(EspnBronzeRepository.from_env())


def _live_lease() -> ProductionLeaseStore:
    return ProductionLeaseStore(PostgresEspnControlStore.from_env())


def main(
    argv: list[str] | None = None,
    *,
    backend_factory: Callable[[], object] | None = None,
    lease_factory: Callable[[], object] | None = None,
) -> int:
    args = _parser().parse_args(argv)
    backend_factory = backend_factory or _live_backend
    lease_factory = lease_factory or _live_lease
    try:
        layout_mode = require_layout_mode()
        if args.command == "promote":
            evidence = load_promotion_evidence(args.evidence)
            if args.apply:
                backend = backend_factory()
                lease = lease_factory()
                current_time = getattr(lease, "current_time", None)
                if not callable(current_time):
                    raise MigrationError("production lease adapter has no DB clock")
                result = apply_promotion(
                    evidence,
                    backend=backend,
                    lease_store=lease,
                    now=current_time(),
                )
                result["rollback"] = build_promotion_plan(
                    evidence,
                    output_path=args.output,
                    catalog=args.catalog,
                    schema=args.schema,
                    internal_schema=args.internal_schema,
                    layout_mode=layout_mode,
                )["rollback"]
                result.pop("result_sha256", None)
                result["result_sha256"] = canonical_sha256(result)
            else:
                result = build_promotion_plan(
                    evidence,
                    output_path=args.output,
                    catalog=args.catalog,
                    schema=args.schema,
                    internal_schema=args.internal_schema,
                    layout_mode=layout_mode,
                )
        else:
            promotion = json.loads(args.promotion_report.read_text(encoding="utf-8"))
            plan = build_rollback_plan(
                promotion,
                reason=args.reason,
                output_path=args.output,
            )
            if args.apply:
                backend = backend_factory()
                lease = lease_factory()
                current_time = getattr(lease, "current_time", None)
                if not callable(current_time):
                    raise MigrationError("production lease adapter has no DB clock")
                result = apply_rollback(
                    plan,
                    backend=backend,
                    lease_store=lease,
                    now=current_time(),
                )
            else:
                result = plan
        _atomic_json(args.output, result)
        print(
            json.dumps(
                result, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
        )
        return 0
    except Exception as exc:  # CLI boundary must always leave a machine report.
        failure = {
            "schema_version": MIGRATION_VERSION,
            "status": "failed",
            "mutates": bool(getattr(args, "apply", False)),
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
