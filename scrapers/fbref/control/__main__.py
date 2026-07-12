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
    arguments = parser.parse_args()
    if arguments.command == "migrate":
        versions = ControlStore.from_env().migrate()
        print(json.dumps({"applied_migrations": list(versions)}))
        return 0
    parser.error(f"Unknown command: {arguments.command}")
    return 2


if __name__ == "__main__":  # pragma: no cover - exercised by container init
    raise SystemExit(main())
