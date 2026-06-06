#!/usr/bin/env python3
"""Apply YAML table/column descriptions to OpenMetadata via REST API.

Idempotent: GET each table by FQN, build JSON-Patch (RFC 6902), PATCH.
404 => WARN (not yet ingested). 401 => fail. Other 4xx on patch => WARN.

Relationships are best-effort POSTed to /api/v1/lineage as addLineageEdge.

ENV:
    OPENMETADATA_HOST       (default http://openmetadata-server:8585)
    OPENMETADATA_JWT_TOKEN  (required; admin bot JWT — see README.md)
                            Falls back to OM_JWT_TOKEN, which the
                            openmetadata-ingestion container already provides.

Usage:
    python apply_descriptions.py [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import yaml

try:
    import requests
except ImportError:
    print("ERROR: `requests` not installed. pip install requests", file=sys.stderr)
    sys.exit(1)


DESCRIPTIONS_DIR = Path(__file__).resolve().parent / "descriptions"
DEFAULT_HOST = os.environ.get("OPENMETADATA_HOST", "http://openmetadata-server:8585")


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def build_patch(spec: dict[str, Any], current: dict[str, Any]) -> list[dict[str, Any]]:
    """Build RFC 6902 JSON-Patch ops for table description, columns, tags."""
    ops: list[dict[str, Any]] = []
    table = spec.get("table") or {}

    if "description" in table:
        op = "replace" if current.get("description") else "add"
        ops.append({"op": op, "path": "/description", "value": table["description"]})

    tags = table.get("tags") or []
    if tags:
        tag_objs = [{"tagFQN": t, "labelType": "Manual", "state": "Confirmed", "source": "Classification"} for t in tags]
        op = "replace" if current.get("tags") else "add"
        ops.append({"op": op, "path": "/tags", "value": tag_objs})

    # Per-column descriptions: only patch columns the YAML knows about.
    yaml_cols = {c["name"]: c.get("description", "") for c in (spec.get("columns") or [])}
    current_cols = current.get("columns") or []
    for idx, col in enumerate(current_cols):
        name = col.get("name")
        if name in yaml_cols and yaml_cols[name]:
            existing = col.get("description") or ""
            new = yaml_cols[name]
            if existing.strip() != new.strip():
                op = "replace" if existing else "add"
                ops.append({"op": op, "path": f"/columns/{idx}/description", "value": new})
    return ops


def apply_lineage(host: str, headers: dict[str, str], rels: list[dict[str, Any]], from_fqn: str, dry_run: bool, counter: dict[str, int]) -> None:
    """Best-effort lineage edges. Non-fatal on errors."""
    for rel in rels:
        rel_type = (rel.get("type") or "").upper()
        if rel_type != "FOREIGN_KEY":
            continue
        to_fqn = rel.get("to")
        if not to_fqn:
            continue
        edge = {
            "edge": {
                "fromEntity": {"id": from_fqn, "type": "table"},
                "toEntity": {"id": to_fqn, "type": "table"},
                "description": rel.get("description") or f"FK: {from_fqn} -> {to_fqn}",
            }
        }
        if dry_run:
            print(f"[DRY] PUT /api/v1/lineage  {from_fqn} -> {to_fqn}")
            counter["lineage_dry"] = counter.get("lineage_dry", 0) + 1
            continue
        # TODO: Lineage API expects entity IDs, not FQNs — Phase 1.5 will resolve
        # FQN -> id via GET /api/v1/tables/name/{fqn}. For now best-effort PUT.
        try:
            r = requests.put(f"{host}/api/v1/lineage", headers=headers, json=edge, timeout=15)
            if r.status_code in (200, 201):
                counter["lineage_ok"] = counter.get("lineage_ok", 0) + 1
            else:
                print(f"  WARN lineage {from_fqn} -> {to_fqn}: HTTP {r.status_code}")
                counter["lineage_warn"] = counter.get("lineage_warn", 0) + 1
        except requests.RequestException as exc:
            print(f"  WARN lineage {from_fqn} -> {to_fqn}: {exc}")
            counter["lineage_warn"] = counter.get("lineage_warn", 0) + 1


def process_file(path: Path, host: str, headers: dict[str, str], dry_run: bool, counter: dict[str, int]) -> None:
    spec = load_yaml(path)
    table = spec.get("table") or {}
    fqn = table.get("fullyQualifiedName")
    if not fqn:
        print(f"SKIP {path.name}: missing table.fullyQualifiedName")
        counter["skipped"] += 1
        return

    if dry_run:
        # Render patch without GET; assume current state is empty.
        ops = build_patch(spec, {"columns": []})
        print(f"--- {path.name}  ({fqn})")
        for op in ops:
            print(f"  {json.dumps(op, ensure_ascii=False)}")
        rels = spec.get("relationships") or []
        if rels:
            apply_lineage(host, headers, rels, fqn, dry_run=True, counter=counter)
        counter["applied"] += 1
        return

    r = requests.get(f"{host}/api/v1/tables/name/{fqn}", headers=headers, timeout=15)
    if r.status_code == 401:
        raise SystemExit("ERROR: HTTP 401 — bad/missing OPENMETADATA_JWT_TOKEN. See README.md.")
    if r.status_code == 404:
        print(f"WARN {path.name}: table not ingested yet ({fqn})")
        counter["skipped"] += 1
        return
    if r.status_code != 200:
        print(f"FAIL {path.name}: GET HTTP {r.status_code}")
        counter["failed"] += 1
        return

    current = r.json()
    table_id = current.get("id")
    ops = build_patch(spec, current)
    if not ops:
        print(f"OK   {path.name}: nothing to patch")
        counter["applied"] += 1
    else:
        patch_headers = {**headers, "Content-Type": "application/json-patch+json"}
        pr = requests.patch(f"{host}/api/v1/tables/{table_id}", headers=patch_headers, json=ops, timeout=15)
        if pr.status_code in (200, 201):
            print(f"OK   {path.name}: {len(ops)} ops applied")
            counter["applied"] += 1
        else:
            print(f"FAIL {path.name}: PATCH HTTP {pr.status_code}: {pr.text[:200]}")
            counter["failed"] += 1
            return

    rels = spec.get("relationships") or []
    if rels:
        apply_lineage(host, headers, rels, fqn, dry_run=False, counter=counter)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="render patches without HTTP")
    ap.add_argument("--host", default=DEFAULT_HOST, help=f"OpenMetadata host (default {DEFAULT_HOST})")
    args = ap.parse_args()

    token = os.environ.get("OPENMETADATA_JWT_TOKEN") or os.environ.get("OM_JWT_TOKEN", "")
    if not args.dry_run and not token:
        print("ERROR: OPENMETADATA_JWT_TOKEN not set. See configs/openmetadata/README.md.", file=sys.stderr)
        return 2
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    if not DESCRIPTIONS_DIR.is_dir():
        print(f"ERROR: descriptions dir not found: {DESCRIPTIONS_DIR}", file=sys.stderr)
        return 2

    files = sorted(p for p in DESCRIPTIONS_DIR.glob("*.yaml"))
    if not files:
        print("WARN: no YAML files in descriptions/")
        return 0

    counter = {"applied": 0, "skipped": 0, "failed": 0}
    for path in files:
        process_file(path, args.host, headers, args.dry_run, counter)

    print(f"\nDone: applied={counter['applied']} skipped={counter['skipped']} failed={counter['failed']}")
    return 1 if counter["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
