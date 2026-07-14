#!/usr/bin/env python3
"""Refresh the versioned SofaScore registry.

The default transport is direct JSON: no browser, no proxy, zero paid bytes.
A metered residential transport exists for the catalog fan-out that SofaScore's
edge refuses from a datacentre egress, and it is opt-in only: it requires both
``--transport lease-proxy`` and an explicit ``--budget-cap-bytes`` ceiling.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
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
    DISCOVERY_LEASE_MAX_BYTES,
    DISCOVERY_LEASE_TTL_SECONDS,
    DirectSofaScoreClient,
    LeaseProxySofaScoreClient,
    discover_registry,
    write_registry_atomic,
)


logger = logging.getLogger(__name__)
REPORT_SCHEMA_VERSION = 2
DEFAULT_REPORT_PATH = "/tmp/sofascore_discovery_result.json"
DIRECT_TRANSPORT = "direct"
LEASE_TRANSPORT = "lease-proxy"
ZERO_TRAFFIC = {
    "requests": 0,
    "direct_response_bytes": 0,
    "paid_proxy_bytes": 0,
    "browser_sessions": 0,
    "browser_navigations": 0,
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Discover SofaScore tournaments without a browser; direct JSON by "
            "default, metered lease proxy only on explicit opt-in"
        ),
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
        choices=("full", "active-reviewed", "targeted"),
        default="full",
        help=(
            "full weekly catalog scan, a lightweight refresh of active and "
            "already-reviewed tournaments, or a detail pass over the "
            "tournaments named by --tournament-id"
        ),
    )
    parser.add_argument(
        "--tournament-id",
        action="append",
        type=int,
        default=None,
        dest="tournament_ids",
        metavar="ID",
        help=(
            "SofaScore unique tournament id to refresh; repeatable and "
            "required by --scope targeted"
        ),
    )
    parser.add_argument(
        "--transport",
        choices=(DIRECT_TRANSPORT, LEASE_TRANSPORT),
        default=DIRECT_TRANSPORT,
        help=(
            "direct (default) spends zero paid bytes; lease-proxy meters every "
            "byte through the proxy filter and requires --budget-cap-bytes"
        ),
    )
    parser.add_argument(
        "--budget-cap-bytes",
        type=int,
        default=None,
        help="hard paid-byte ceiling for one lease-proxy run (no default)",
    )
    parser.add_argument(
        "--per-lease-max-bytes",
        type=int,
        default=DISCOVERY_LEASE_MAX_BYTES,
        help="paid bytes per lease before the scan rotates to a fresh exit",
    )
    parser.add_argument(
        "--lease-ttl-seconds",
        type=int,
        default=DISCOVERY_LEASE_TTL_SECONDS,
        help="lease lifetime requested from the proxy filter",
    )
    parser.add_argument(
        "--control-url",
        default=os.environ.get("SOFASCORE_PROXY_CONTROL_URL", ""),
        help="proxy-filter lease control URL (lease-proxy transport only)",
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


def _redact(value: Any) -> str:
    """Strip proxy credentials and lease tokens from a diagnostic string.

    A metered lease proxy URL is ``http://lease:<token>@host:port``; a TLS
    transport can fold it into an error, so every string bound for the run
    report or the log is redacted first (the control plane and canary bench
    already do this).  Imported lazily so the default direct transport does not
    pull the control-plane dependency tree.
    """

    from scrapers.sofascore.lease_client import redact_sensitive

    return redact_sensitive(value)


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


def _validate_arguments(args: argparse.Namespace) -> None:
    """Fail before any transport exists when the run is not authorized."""

    if args.scope == "targeted" and not args.tournament_ids:
        raise ValueError("--scope targeted requires at least one --tournament-id")
    if args.scope != "targeted" and args.tournament_ids:
        raise ValueError("--tournament-id requires --scope targeted")
    if args.transport == DIRECT_TRANSPORT:
        if args.budget_cap_bytes is not None:
            raise ValueError(
                "--budget-cap-bytes is only meaningful for --transport lease-proxy"
            )
        return
    if args.budget_cap_bytes is None or args.budget_cap_bytes <= 0:
        raise ValueError(
            "--transport lease-proxy requires a positive --budget-cap-bytes: "
            "metered discovery never runs on an implicit budget"
        )
    if not str(args.control_url).strip():
        raise ValueError(
            "--transport lease-proxy requires --control-url or "
            "SOFASCORE_PROXY_CONTROL_URL"
        )


def _build_client(args: argparse.Namespace) -> Any:
    if args.transport == DIRECT_TRANSPORT:
        return DirectSofaScoreClient()
    return LeaseProxySofaScoreClient(
        control_url=str(args.control_url).strip(),
        budget_cap_bytes=int(args.budget_cap_bytes),
        per_lease_max_bytes=int(args.per_lease_max_bytes),
        lease_ttl_seconds=int(args.lease_ttl_seconds),
        run_id=datetime.now(timezone.utc).strftime("discovery__%Y%m%dT%H%M%SZ"),
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    registry = Path(args.registry).resolve()
    report_path = Path(args.output).resolve()
    budget_cap_bytes = int(args.budget_cap_bytes or 0)
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": "running",
        "mode": _mode(args),
        "transport": args.transport,
        "budget_cap_bytes": budget_cap_bytes,
        "registry": str(registry),
        "changed": False,
        "written": False,
        "traffic": dict(ZERO_TRAFFIC),
        "errors": [],
    }
    client: Optional[Any] = None
    exit_code = 1

    try:
        if registry == report_path:
            raise ValueError("--output must differ from --registry")
        _validate_arguments(args)
        existing = _read_registry(registry)
        client = _build_client(args)
        if args.scope == "full":
            # Preserve the long-standing two-argument embedding contract.
            discovered, discovery_report = discover_registry(existing, client)
        elif args.scope == "targeted":
            discovered, discovery_report = discover_registry(
                existing,
                client,
                scope=args.scope,
                target_tournament_ids=list(args.tournament_ids or ()),
            )
        else:
            discovered, discovery_report = discover_registry(
                existing, client, scope=args.scope
            )
        changed = discovered != existing
        report.update(discovery_report)
        report["changed"] = changed

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
        safe_error = _redact(exc)
        logger.error("SofaScore discovery failed: %s", safe_error)
        report["status"] = "failed"
        report["errors"] = [safe_error]
        exit_code = 1
    finally:
        if client is not None:
            # Closing the metered transport is what bills the open lease, so
            # traffic is only final afterwards.
            try:
                client.close()
            except Exception as exc:  # noqa: BLE001 - teardown must not mask
                safe_error = _redact(exc)
                logger.error(
                    "SofaScore discovery transport close failed: %s", safe_error
                )
                report["status"] = "failed"
                report["errors"] = [*report["errors"], safe_error]
                exit_code = 1
            report["traffic"] = dict(client.stats)
        traffic = report.setdefault("traffic", dict(ZERO_TRAFFIC))
        # The zero-paid-byte invariant belongs in every direct report, including
        # dependency and transport failures before the first response arrives.
        # The metered transport reports what the proxy filter actually billed.
        if args.transport == DIRECT_TRANSPORT:
            traffic["paid_proxy_bytes"] = 0
        else:
            paid = int(traffic.get("paid_proxy_bytes", 0))
            traffic.setdefault("lease_count", 0)
            traffic.setdefault("upstream_repins", 0)
            if paid > budget_cap_bytes:
                report["status"] = "failed"
                report["errors"] = [
                    *report["errors"],
                    f"paid proxy bytes {paid} exceeded the "
                    f"{budget_cap_bytes}-byte cap",
                ]
                exit_code = 1
        # No discovery transport ever opens a browser.
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
