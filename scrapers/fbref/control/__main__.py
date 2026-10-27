"""Small administration entrypoint for the FBref control schema."""

from __future__ import annotations

import argparse
import json

from scrapers.fbref.control.store import ControlStore


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage FBref control state")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "migrate",
        help="apply versioned PostgreSQL migrations under an advisory lock",
    )
    backfill = subparsers.add_parser("backfill-page-scopes", help="commit one resumable ownership batch")
    backfill.add_argument("--batch-size", type=int, default=100)
    subparsers.add_parser("refresh-page-scopes", help="repair registry invalidations without provenance")
    arguments = parser.parse_args()
    if arguments.command == "backfill-page-scopes":
        print(json.dumps(ControlStore.from_env().backfill_page_scopes(batch_size=arguments.batch_size)))
        return 0
    if arguments.command == "refresh-page-scopes":
        print(json.dumps({"refreshed": ControlStore.from_env().refresh_page_scopes()}))
        return 0
    if arguments.command == "migrate":
        versions = ControlStore.from_env().migrate()
        print(json.dumps({"applied_migrations": list(versions)}))
        return 0
    parser.error(f"Unknown command: {arguments.command}")
    return 2


if __name__ == "__main__":  # pragma: no cover - exercised by container init
    raise SystemExit(main())
