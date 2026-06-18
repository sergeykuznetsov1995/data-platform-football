#!/usr/bin/env python3
"""Hard-delete OpenMetadata table entities for tables dropped in epic #478.

When a table disappears from Trino, ``om-ingest-trino`` (``markDeletedTables:
true``) only **soft-deletes** the entity. In practice that leaves its lineage
edges in place — in particular the edges added manually by
``apply_descriptions.py`` (``PUT /api/v1/lineage`` from the ``relationships:``
blocks), which ingestion's mark-deleted handling does not touch. So stale edges
to/from the dropped tables linger in the catalog (issue #529, surfaced when the
derived gold tier was dropped in epic #478).

A **hard delete** with ``recursive=true`` removes the table entity together with
its relationship rows — clearing those lineage edges. This script does exactly
that for the dropped tables.

Idempotent: a table already absent from OpenMetadata (HTTP 404) is reported as
"absent" and skipped. Safe to re-run.

By default the script is a DRY RUN — it prints what it would delete and makes no
delete calls. Pass ``--apply`` to actually delete.

ENV:
    OPENMETADATA_HOST       (default http://openmetadata-server:8585)
    OPENMETADATA_JWT_TOKEN  (required for --apply; admin bot JWT — see README.md)
                            Falls back to OM_JWT_TOKEN, which the
                            openmetadata-ingestion container already provides.

Usage:
    python cleanup_lineage.py                       # dry-run the curated #478 list
    python cleanup_lineage.py --apply               # hard-delete the curated #478 list
"""

from __future__ import annotations

import argparse
import os
import sys

try:
    import requests
except ImportError:
    print("ERROR: `requests` not installed. pip install requests", file=sys.stderr)
    sys.exit(1)


DEFAULT_HOST = os.environ.get("OPENMETADATA_HOST", "http://openmetadata-server:8585")
SERVICE = "trino_iceberg"

# Tables dropped in epic #478 (derived gold tier). Their OM entities + lineage
# edges (manual FK edges from apply_descriptions.py, or CTAS edges from
# om-lineage-trino) linger after the drop because markDeletedTables only
# soft-deletes. Source: the 19 deleted dags/sql/gold/*.sql in PR #490.
# entity_xref is intentionally EXCLUDED — it was never part of epic #478. Its own
# drop is the separate followup #211 (entity_xref is already absent from live gold
# per the Trino inventory in #475), so it is not one of these #478 soft-deletes.
DROPPED_TABLES = [
    "fct_match",
    "fct_match_train",
    "fct_match_test",
    "feat_player_form",
    "feat_team_form",
    "feat_team_h2h",
    "feat_team_xg_form",
    "feat_team_event_style",
    "feat_referee_bias",
    "mart_event_heatmap",
    "mart_referee_dashboard",
    "mart_scouting_radar",
    "match_outcomes",
    "predictions_input",
    "predictions_input_v2",
    "understat_team_season",
    "fotmob_team_season",
    "sofascore_team_season",
    "whoscored_team_season",
]
CURATED_FQNS = [f"{SERVICE}.iceberg.gold.{t}" for t in DROPPED_TABLES]


def resolve_table_id(host: str, headers: dict[str, str], fqn: str) -> str | None:
    """FQN -> table UUID (incl. soft-deleted via include=all); None on 404/error."""
    try:
        r = requests.get(
            f"{host}/api/v1/tables/name/{fqn}",
            headers=headers,
            params={"include": "all"},
            timeout=15,
        )
    except requests.RequestException as exc:
        print(f"  WARN resolve {fqn}: {exc}")
        return None
    if r.status_code == 401:
        raise SystemExit("ERROR: HTTP 401 — bad/missing OPENMETADATA_JWT_TOKEN. See README.md.")
    return r.json().get("id") if r.status_code == 200 else None


def hard_delete(host: str, headers: dict[str, str], fqn: str, dry_run: bool, counter: dict[str, int]) -> None:
    """Hard-delete one table by FQN (recursive → cascades to its lineage edges)."""
    if dry_run:
        print(f"[DRY] DELETE table {fqn}  (hardDelete=true, recursive=true → entity + lineage edges)")
        counter["dry"] += 1
        return

    table_id = resolve_table_id(host, headers, fqn)
    if not table_id:
        print(f"ABSENT  {fqn}: not in catalog (nothing to delete)")
        counter["absent"] += 1
        return

    try:
        r = requests.delete(
            f"{host}/api/v1/tables/{table_id}",
            headers=headers,
            params={"hardDelete": "true", "recursive": "true"},
            timeout=30,
        )
    except requests.RequestException as exc:
        print(f"  WARN delete {fqn}: {exc}")
        counter["warn"] += 1
        return
    if r.status_code in (200, 204):
        print(f"DELETED {fqn}  (entity + lineage edges)")
        counter["deleted"] += 1
    else:
        print(f"  WARN delete {fqn}: HTTP {r.status_code}: {r.text[:200]}")
        counter["warn"] += 1


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--apply", action="store_true", help="actually delete (default: dry-run preview)")
    ap.add_argument(
        "--dry-run", action="store_true",
        help="explicit no-op (dry-run is the default; --apply overrides it)",
    )
    ap.add_argument("--host", default=DEFAULT_HOST, help=f"OpenMetadata host (default {DEFAULT_HOST})")
    args = ap.parse_args()

    dry_run = not args.apply

    token = os.environ.get("OPENMETADATA_JWT_TOKEN") or os.environ.get("OM_JWT_TOKEN", "")
    if not dry_run and not token:
        print("ERROR: OPENMETADATA_JWT_TOKEN not set. See configs/openmetadata/README.md.", file=sys.stderr)
        return 2
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    fqns = list(CURATED_FQNS)

    mode = "DRY-RUN (no deletes; pass --apply to execute)" if dry_run else "APPLY (hard-delete)"
    print(f"cleanup_lineage: {mode} — {len(fqns)} target table(s) on {args.host}\n")

    counter = {"deleted": 0, "absent": 0, "warn": 0, "dry": 0}
    for fqn in fqns:
        hard_delete(args.host, headers, fqn, dry_run, counter)

    print(
        f"\nDone: deleted={counter['deleted']} absent={counter['absent']} "
        f"warn={counter['warn']} dry={counter['dry']}"
    )
    return 1 if counter["warn"] else 0


if __name__ == "__main__":
    sys.exit(main())
