#!/usr/bin/env python3
"""SELECT-only daily report; offline snapshot input or existing host readers."""

import argparse
import csv
from datetime import date, datetime, timedelta, timezone
import io
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scrapers.fbref.daily_report import (  # noqa: E402
    build_daily_report, instant, milestone_streak, render_markdown, summary_line,
)
from scrapers.fbref.daily_report_reader import collect_snapshot  # noqa: E402


def host_postgres(query: str):
    result = subprocess.run([
        "docker", "exec", "postgres", "psql", "-U", "airflow", "-d", "airflow", "-qAt",
        "-v", "ON_ERROR_STOP=1", "-c",
        "BEGIN READ ONLY; SET LOCAL statement_timeout='30s'; " + query + "; COMMIT;",
    ], capture_output=True, text=True, timeout=40, check=True)
    return json.loads(result.stdout)


def host_trino(query: str):
    result = subprocess.run(["/root/.claude/bin/trino-ro.sh", query.strip()],
                            capture_output=True, text=True, timeout=90, check=True)
    rows = list(csv.reader(io.StringIO(result.stdout)))
    # The established host wrapper emits CSV without a header.
    columns = {
        "fbref_schedule": ["source_competition_id", "source_season_id", "date", "time", "home", "away", "score", "notes", "match_url"],
        "fotmob_ingest_manifest": ["competition_id", "season_key", "fetched_at"],
        "fotmob_matches_current": ["competition_id", "season_key", "match_id", "utc_time", "timezone", "finished", "cancelled", "postponed", "awarded"],
    }
    selected = next(fields for table, fields in columns.items() if table in query)
    output = []
    for row in rows:
        if len(row) != len(selected):
            raise ValueError("Malformed Trino result")
        parsed = {key: None if value == "NULL" else value for key, value in zip(selected, row)}
        for key in ("finished", "cancelled", "awarded"):
            if key in parsed:
                parsed[key] = {"true": True, "false": False, None: None}.get(parsed[key])
        output.append(parsed)
    return output


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sources = parser.add_mutually_exclusive_group(required=True)
    sources.add_argument("--input", type=Path, help="Saved read-only snapshot JSON")
    sources.add_argument("--host-read-only", action="store_true")
    parser.add_argument("--as-of", type=instant, default=None)
    parser.add_argument("--date", type=date.fromisoformat, default=None)
    parser.add_argument("--lookback", type=int, default=14)
    parser.add_argument("--map", type=Path, default=ROOT / "configs/fbref/fotmob-report-map.json")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--require-final", action="store_true")
    args = parser.parse_args(argv)
    if not 1 <= args.lookback <= 31:
        parser.error("lookback must be between 1 and 31")
    now = args.as_of or datetime.now(timezone.utc)
    day = args.date or (now.date() - timedelta(days=1))
    if day >= now.date():
        parser.error("date must be a completed calendar day")
    mapping = json.loads(args.map.read_text())["competitions"]
    snapshot = json.loads(args.input.read_text()) if args.input else collect_snapshot(
        host_postgres, host_trino, day=day, as_of=now, mapping=mapping, lookback=args.lookback)
    reports = [build_daily_report(snapshot, day=day - timedelta(days=i), as_of=now, mapping=mapping)
               for i in range(args.lookback)]
    current = reports[0]
    final = next((r for r in reports if r["final"]), None)
    bundle = {"report": current, "latest_final": final, "days": reports,
              "milestone_streak": milestone_streak(reports)}
    args.output_dir.mkdir(parents=True, exist_ok=True)
    label = f"{day.isoformat()}-{now.strftime('%Y%m%dT%H%M%SZ')}"
    (args.output_dir / f"{label}.json").write_text(json.dumps(bundle, ensure_ascii=False, indent=2) + "\n")
    (args.output_dir / f"{label}.md").write_text(render_markdown(current) + "\n" + (
        "Последний окончательный срез: " + summary_line(final) if final else "Окончательных срезов пока нет."
    ) + f"\nСерия ≥99 %: {bundle['milestone_streak']} суток.\n")
    (args.output_dir / f"{label}-snapshot.json").write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n")
    print(summary_line(current))
    print("Последний окончательный: " + summary_line(final) if final else "Окончательных срезов пока нет.")
    print(f"Серия ≥99 %: {bundle['milestone_streak']} суток.")
    return 2 if args.require_final and not current["final"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
