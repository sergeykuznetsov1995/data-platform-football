#!/usr/bin/env python3
"""Bootstrap OpenMetadata classifications + tags referenced by descriptions/*.yaml.

Idempotent: GET each classification/tag by FQN; POST only if 404.
Run BEFORE `apply_descriptions.py`, otherwise PATCH-with-tags returns 404
("tag instance for Tier.Gold not found"). See issue #68.

ENV:
    OPENMETADATA_HOST       (default http://openmetadata-server:8585)
    OPENMETADATA_JWT_TOKEN  (required; admin bot JWT — see README.md)
                            Falls back to OM_JWT_TOKEN, which the
                            openmetadata-ingestion container already exports
                            from compose.yaml / .env.

Usage:
    python bootstrap_classifications.py [--dry-run]
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

try:
    import requests
except ImportError:
    print("ERROR: `requests` not installed. pip install requests", file=sys.stderr)
    sys.exit(1)


DEFAULT_HOST = os.environ.get("OPENMETADATA_HOST", "http://openmetadata-server:8585")

# Source of truth: every tagFQN referenced by configs/openmetadata/descriptions/*.yaml.
# Expand here when a new tag appears in a description YAML.
CLASSIFICATIONS: list[dict[str, Any]] = [
    {
        "name": "Tier",
        "description": "Data lifecycle tier (Bronze raw / Silver cleaned / Gold curated).",
        "tags": [
            {"name": "Bronze", "description": "Raw ingested data, source-shaped."},
            {"name": "Silver", "description": "Cleaned, deduplicated, conformed."},
            {"name": "Gold", "description": "Business-ready facts and dimensions."},
        ],
    },
    {
        "name": "Domain",
        "description": "Business domain.",
        "tags": [
            {"name": "Football", "description": "Football match / player / team data."},
        ],
    },
    {
        "name": "PII",
        "description": "Personally Identifiable Information sensitivity.",
        "tags": [
            {"name": "None", "description": "No PII."},
            {"name": "Low", "description": "Low-sensitivity PII (e.g. public names)."},
        ],
    },
    {
        "name": "UseCase",
        "description": "Downstream use-case tag.",
        "tags": [
            {"name": "ML", "description": "Used as ML feature/training input."},
        ],
    },
]


def ensure_classification(host: str, headers: dict[str, str], cls: dict[str, Any], counter: dict[str, int]) -> bool:
    """Create classification if missing. Returns True if subsequent tag creates can proceed."""
    name = cls["name"]
    r = requests.get(f"{host}/api/v1/classifications/name/{name}", headers=headers, timeout=15)
    if r.status_code == 401:
        raise SystemExit("ERROR: HTTP 401 — bad/missing OPENMETADATA_JWT_TOKEN. See README.md.")
    if r.status_code == 200:
        print(f"OK   classification {name}: already exists")
        counter["skipped"] += 1
        return True
    if r.status_code != 404:
        print(f"FAIL classification {name}: GET HTTP {r.status_code}: {r.text[:200]}")
        counter["failed"] += 1
        return False

    body = {"name": name, "description": cls["description"], "mutuallyExclusive": False}
    cr = requests.post(f"{host}/api/v1/classifications", headers=headers, json=body, timeout=15)
    if cr.status_code in (200, 201):
        print(f"OK   classification {name}: created")
        counter["created"] += 1
        return True
    print(f"FAIL classification {name}: POST HTTP {cr.status_code}: {cr.text[:200]}")
    counter["failed"] += 1
    return False


def ensure_tag(host: str, headers: dict[str, str], classification_name: str, tag: dict[str, Any], counter: dict[str, int]) -> None:
    fqn = f"{classification_name}.{tag['name']}"
    r = requests.get(f"{host}/api/v1/tags/name/{fqn}", headers=headers, timeout=15)
    if r.status_code == 401:
        raise SystemExit("ERROR: HTTP 401 — bad/missing OPENMETADATA_JWT_TOKEN. See README.md.")
    if r.status_code == 200:
        print(f"OK   tag {fqn}: already exists")
        counter["skipped"] += 1
        return
    if r.status_code != 404:
        print(f"FAIL tag {fqn}: GET HTTP {r.status_code}: {r.text[:200]}")
        counter["failed"] += 1
        return

    body = {"name": tag["name"], "description": tag["description"], "classification": classification_name}
    cr = requests.post(f"{host}/api/v1/tags", headers=headers, json=body, timeout=15)
    if cr.status_code in (200, 201):
        print(f"OK   tag {fqn}: created")
        counter["created"] += 1
        return
    print(f"FAIL tag {fqn}: POST HTTP {cr.status_code}: {cr.text[:200]}")
    counter["failed"] += 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="render POST bodies without HTTP")
    ap.add_argument("--host", default=DEFAULT_HOST, help=f"OpenMetadata host (default {DEFAULT_HOST})")
    args = ap.parse_args()

    token = os.environ.get("OPENMETADATA_JWT_TOKEN") or os.environ.get("OM_JWT_TOKEN", "")
    if not args.dry_run and not token:
        print("ERROR: OPENMETADATA_JWT_TOKEN not set. See configs/openmetadata/README.md.", file=sys.stderr)
        return 2
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    counter = {"created": 0, "skipped": 0, "failed": 0}
    for cls in CLASSIFICATIONS:
        if args.dry_run:
            print(f"[DRY] POST /api/v1/classifications  name={cls['name']}")
            counter["created"] += 1
            for tag in cls["tags"]:
                print(f"[DRY] POST /api/v1/tags             {cls['name']}.{tag['name']}")
                counter["created"] += 1
            continue

        if not ensure_classification(args.host, headers, cls, counter):
            # Classification GET/POST failed → skip its tags (POST would 404 on classification).
            continue
        for tag in cls["tags"]:
            ensure_tag(args.host, headers, cls["name"], tag, counter)

    print(f"\nDone: created={counter['created']} skipped={counter['skipped']} failed={counter['failed']}")
    return 1 if counter["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
