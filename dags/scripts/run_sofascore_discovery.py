#!/usr/bin/env python3
"""Refresh the versioned SofaScore registry through direct JSON only."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

# Keep the source-metadata CLI runnable from any working directory. Airflow
# normally injects /opt/airflow into PYTHONPATH, while a checkout does not.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.sofascore.catalog import (  # noqa: E402
    SofaScoreCatalog,
    registry_path,
)
from scrapers.sofascore.discovery import (  # noqa: E402
    DirectSofaScoreClient,
    discover_registry,
    write_registry_atomic,
)


logger = logging.getLogger(__name__)
REPORT_SCHEMA_VERSION = 1
DEFAULT_REPORT_PATH = "/tmp/sofascore_discovery_result.json"
ZERO_TRAFFIC = {
    "requests": 0,
    "direct_response_bytes": 0,
    "paid_proxy_bytes": 0,
    "browser_sessions": 0,
    "browser_navigations": 0,
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover SofaScore tournaments without browser or proxy",
    )
    parser.add_argument(
        "--registry",
        default=str(registry_path()),
        help="Versioned SofaScore registry JSON",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_REPORT_PATH,
        help="Atomic JSON run report",
    )
    parser.add_argument(
        "--scope",
        choices=("full", "active-reviewed"),
        default="full",
        help=(
            "full weekly catalog scan, or a lightweight refresh of active "
            "and already-reviewed tournaments"
        ),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover and validate without writing the registry",
    )
    mode.add_argument(
        "--check",
        action="store_true",
        help="Do not write; return 2 when discovery would change the registry",
    )
    return parser


def _read_registry(path: Path) -> Mapping[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read registry {path}: {exc}") from exc
    SofaScoreCatalog.from_mapping(document)
    return document


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    rendered = (
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    ).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _mode(args: argparse.Namespace) -> str:
    if args.check:
        return "check"
    if args.dry_run:
        return "dry_run"
    return "write"


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    registry = Path(args.registry).resolve()
    report_path = Path(args.output).resolve()
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": "running",
        "mode": _mode(args),
        "registry": str(registry),
        "changed": False,
        "written": False,
        "traffic": dict(ZERO_TRAFFIC),
        "errors": [],
    }
    client: Optional[DirectSofaScoreClient] = None
    exit_code = 1

    try:
        if registry == report_path:
            raise ValueError("--output must differ from --registry")
        existing = _read_registry(registry)
        client = DirectSofaScoreClient()
        if args.scope == "full":
            # Preserve the long-standing two-argument embedding contract.
            discovered, discovery_report = discover_registry(existing, client)
        else:
            discovered, discovery_report = discover_registry(
                existing, client, scope=args.scope
            )
        changed = discovered != existing
        report.update(discovery_report)
        report["changed"] = changed
        report["traffic"] = client.stats

        if args.check:
            report["status"] = "changes_detected" if changed else "success"
            exit_code = 2 if changed else 0
        elif args.dry_run:
            report["status"] = "success"
            exit_code = 0
        else:
            report["written"] = write_registry_atomic(
                registry,
                discovered,
                expected_current=existing,
            )
            report["status"] = "success"
            exit_code = 0
    except Exception as exc:
        logger.error("SofaScore discovery failed: %s", exc, exc_info=True)
        report["status"] = "failed"
        report["errors"] = [str(exc)]
        if client is not None:
            report["traffic"] = client.stats
        exit_code = 1
    finally:
        if client is not None:
            client.close()
        # The invariant belongs in every report, including dependency and
        # transport failures before the first response arrives.
        traffic = report.setdefault("traffic", dict(ZERO_TRAFFIC))
        traffic["paid_proxy_bytes"] = 0
        traffic["browser_sessions"] = 0
        traffic["browser_navigations"] = 0
        if report_path != registry:
            try:
                _write_json_atomic(report_path, report)
            except Exception as exc:
                logger.error(
                    "Cannot write discovery report %s: %s", report_path, exc
                )
                exit_code = 1

    # One-shot containers are removed after the command. Keep the durable JSON
    # report contract and also surface the exact same result to their stdout.
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
