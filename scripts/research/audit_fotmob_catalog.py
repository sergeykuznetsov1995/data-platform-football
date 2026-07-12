#!/usr/bin/env python3
"""No-write live audit of FotMob's dynamic competition catalog."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scrapers.fotmob.catalog import (  # noqa: E402
    classify_competition,
    discover_competitions,
    parse_seasons,
)
from scrapers.fotmob.domain import ScopeRef  # noqa: E402
from scrapers.fotmob.field_map import classify_paths  # noqa: E402
from scrapers.fotmob.parsers import (  # noqa: E402
    inventory_json_paths,
    parse_season_bundle,
)
from scrapers.fotmob.planner import MANDATORY_COMPETITION_IDS  # noqa: E402
from scrapers.fotmob.raw_store import FotMobRawStore  # noqa: E402
from scrapers.fotmob.transport import FotMobTransport  # noqa: E402


def atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def build_report(payload: Mapping[str, Any], *, fetch: Any) -> dict[str, Any]:
    discovery = discover_competitions(payload)
    classifications = tuple(
        classify_competition(item) for item in discovery.competitions
    )
    ids = {item.competition_id for item in discovery.competitions}
    paths = inventory_json_paths(payload)
    coverage = classify_paths("all_leagues", paths)
    keyword_rows = [
        {
            "competition_id": item.competition_id,
            "name": item.name,
            "country_code": item.country_code,
        }
        for item in discovery.competitions
        if any(
            token in item.name.lower()
            for token in (
                "champions league",
                "nations league",
                "premier league",
                "africa cup of nations",
            )
        )
    ]
    decision_counts = Counter(item.decision.value for item in classifications)
    stats = fetch
    complete = (
        not discovery.conflicts
        and not coverage.unknown
        and MANDATORY_COMPETITION_IDS <= ids
        and int(stats.proxy_bytes) == 0
    )
    return {
        "schema_version": "fotmob.catalog-audit.v1",
        "complete": complete,
        "catalog": {
            "occurrence_paths": sum(
                len(item.source_paths) for item in discovery.competitions
            ),
            "unique_competitions": len(discovery.competitions),
            "conflicts": [asdict(item) for item in discovery.conflicts],
            "issues": [asdict(item) for item in discovery.issues],
            "decision_counts": dict(sorted(decision_counts.items())),
            "mandatory_ids": sorted(MANDATORY_COMPETITION_IDS),
            "missing_mandatory_ids": sorted(MANDATORY_COMPETITION_IDS - ids),
            "keyword_matches": keyword_rows,
        },
        "fields": {
            "observed_paths": len(paths),
            "typed": len(coverage.typed),
            "raw_only": len(coverage.raw_only),
            "excluded": len(coverage.excluded),
            "unknown": list(coverage.unknown),
        },
        "transport": {
            "status": stats.status,
            "attempts": stats.attempts,
            "retries": stats.retries,
            "encoded_direct_bytes": stats.direct_bytes,
            "decoded_bytes": stats.decoded_bytes,
            "proxy_bytes": stats.proxy_bytes,
            "content_hash": stats.content_hash,
            "raw_uri": stats.raw_uri,
        },
    }


def audit_mandatory_seasons(
    transport: FotMobTransport,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Validate exact season discovery/parser shape for acceptance sentinels."""

    rows: list[dict[str, Any]] = []
    failures: list[str] = []
    for competition_id in sorted(MANDATORY_COMPETITION_IDS):
        fetch = transport.fetch_json("leagues", {"id": competition_id})
        item: dict[str, Any] = {
            "competition_id": competition_id,
            "status": fetch.status,
            "attempts": fetch.attempts,
            "direct_bytes": fetch.direct_bytes,
            "proxy_bytes": fetch.proxy_bytes,
            "raw_uri": fetch.raw_uri,
            "content_hash": fetch.content_hash,
        }
        if not fetch.ok:
            item["error"] = fetch.error
            failures.append(f"{competition_id}:{fetch.status}")
            rows.append(item)
            continue
        try:
            seasons = parse_seasons(fetch.data, competition_id)
            selected = next((season for season in seasons if season.is_selected), None)
            if selected is None:
                raise ValueError("no exact selected season in discovered season list")
            bundle = parse_season_bundle(fetch.data, ScopeRef.from_season(selected))
            coverage = classify_paths("league_season", bundle.json_paths)
            if coverage.unknown:
                raise ValueError(f"unclassified paths: {list(coverage.unknown)}")
            item.update(
                {
                    "selected_season": selected.source_season_key,
                    "season_count": len(seasons),
                    "season_keys": [season.source_season_key for season in seasons],
                    "matches": len(bundle.matches),
                    "standings": len(bundle.standings),
                    "stages": len(bundle.stages),
                    "playoffs": len(bundle.playoffs),
                    "teams": len(bundle.teams),
                    "leaderboard_categories": (
                        len(bundle.player_categories) + len(bundle.team_categories)
                    ),
                    "parse_issues": [asdict(issue) for issue in bundle.issues],
                    "json_paths": len(bundle.json_paths),
                    "unknown_paths": [],
                }
            )
            if bundle.issues:
                failures.append(f"{competition_id}:parse_issues")
        except Exception as exc:
            item["error"] = f"{type(exc).__name__}: {exc}"
            failures.append(f"{competition_id}:parse")
        rows.append(item)
    return rows, failures


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--raw-store-uri",
        default="file:///tmp/fotmob-catalog-audit/raw",
    )
    parser.add_argument(
        "--output", default="/tmp/fotmob-catalog-audit/report.json"
    )
    parser.add_argument(
        "--audit-mandatory-seasons",
        action="store_true",
        help="Also fetch/parse every mandatory competition root and season list",
    )
    args = parser.parse_args(argv)
    store = FotMobRawStore.from_uri(args.raw_store_uri)
    transport = FotMobTransport(store, max_attempts=4)
    fetch = transport.fetch_json("allLeagues")
    if not fetch.ok:
        report = {
            "schema_version": "fotmob.catalog-audit.v1",
            "complete": False,
            "error": fetch.error,
            "transport": asdict(fetch),
        }
    else:
        report = build_report(fetch.data, fetch=fetch)
        if args.audit_mandatory_seasons:
            rows, failures = audit_mandatory_seasons(transport)
            report["mandatory_seasons"] = rows
            report["mandatory_season_failures"] = failures
            report["complete"] = bool(report["complete"] and not failures)
            stats = transport.snapshot_stats()
            report["transport_totals"] = asdict(stats)
    output = Path(args.output)
    atomic_write_json(output, report)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report.get("complete") else 2


if __name__ == "__main__":
    raise SystemExit(main())
