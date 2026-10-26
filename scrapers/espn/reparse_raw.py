"""Reparse a frozen summary ledger without creating an ESPN client.

Run with --dry-run first. Writes require exclusive ownership of the selected
matches: the four child-to-match commits are not a cross-table transaction.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import asdict, replace
from datetime import datetime, timezone
import csv
import hashlib
import io
import json
import os
import re
from pathlib import Path
import subprocess

from .bronze_rows import BatchStamp, MatchPayload, RawRef, batch_rows
from .bronze_schema import MATCH_TABLE, NATURAL_KEYS, TABLES
from .bronze_writer import TournamentBatch, delete_filter, write_tournament_batch
from .denominator import load_denominator
from .editions_store import load as load_editions
from .history import duplicate_owners, mark_duplicates
from .parser_contracts import PARSER_VERSION, SummaryDisposition
from .parser_common import EspnParseError
from .raw_store import EspnRawStore, RawStoreError
from .recheck import DOWNGRADE_REJECTED, compare_parts, summary_parts
from .schedule_parser import schedule_row_from_header
from .summary_parser import parse_summary
from .wave import BronzeMatch, _MATCH_COLUMNS, build_competition

PARTS = (
    "lineup",
    "player_stats",
    "formations",
    "key_events",
    "commentary",
    "referee",
    "linescores",
)


def raw_parts(body):
    """Presence in each raw section, even when a neighbouring one is malformed."""

    def mapping(value):
        return value if isinstance(value, dict) else {}

    def objects(value):
        return (
            [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []
        )

    data = mapping(json.loads(body))
    rosters = objects(data.get("rosters"))
    competitions = objects(mapping(data.get("header")).get("competitions"))
    sides = objects(competitions[0].get("competitors")) if competitions else []
    officials = objects(mapping(data.get("gameInfo")).get("officials"))
    referee = False
    for official in officials:
        position = mapping(official.get("position"))
        label = position.get("name", position.get("displayName"))
        labelled = isinstance(label, str) and label.upper() in {
            "REFEREE",
            "MATCH REFEREE",
        }
        if official.get("order") == 1 or labelled:
            referee |= bool(official.get("fullName") or official.get("displayName"))
    return {
        "lineup": any(objects(r.get("roster")) for r in rosters),
        "player_stats": any(
            p.get("statistics") or p.get("stats")
            for r in rosters
            for p in objects(r.get("roster"))
        ),
        "formations": any(r.get("formation") for r in rosters),
        "key_events": bool(objects(data.get("keyEvents"))),
        "commentary": bool(objects(data.get("commentary"))),
        "referee": referee,
        "linescores": any(s.get("linescores") for s in sides),
    }


def parsed_parts(summary):
    return {
        "lineup": bool(summary.lineup),
        "player_stats": bool(summary_parts(summary)["player_stats"]),
        "formations": any(r.formation for r in summary.matchsheet),
        "key_events": any(r.kind == "key_event" for r in summary.events),
        "commentary": any(r.kind == "commentary" for r in summary.events),
        "referee": any(r.referee for r in summary.matchsheet),
        "linescores": any(
            any(getattr(r, c) is not None for c in ("score_h1", "score_h2", "score_et"))
            for r in summary.matchsheet
        ),
    }


def _read_tables(trino, slug, year, event_id):
    scope = delete_filter(slug, year, [event_id])
    return {
        table: [
            dict(zip(schema.names, row))
            for row in trino.execute_query(
                f"SELECT {', '.join(schema.names)} FROM iceberg.bronze.{table} WHERE {scope}"
            )
        ]
        for table, schema in TABLES.items()
    }


def _loses_rows(before, after):
    """Check actual Bronze cells, including both event kinds and match facts.

    Counts alone miss losses within one team/athlete or event kind. A present
    value may change, but may not disappear from its natural key.
    """
    ignored = {"reason", "anomalies", "duplicate_of"}
    for table, old_rows in before.items():
        keys = NATURAL_KEYS[table]
        indexed = {tuple(r[k] for k in keys): r for r in after[table]}
        if len(indexed) != len(after[table]):
            return True
        for old in old_rows:
            new = indexed.get(tuple(old[k] for k in keys))
            if new is None:
                return True
            for name, value in old.items():
                if (
                    name.startswith("_")
                    or name in ignored
                    or name in {"raw_uri", "raw_sha256", "parser_version"}
                ):
                    continue
                if value is not None and new.get(name) is None:
                    return True
                if name == "athlete_ids" and value is not None:
                    if not set(json.loads(value)).issubset(json.loads(new[name])):
                        return True
    return False


def _prepare_write(payload, competition, edition, store, trino):
    event = payload.schedule
    before = _read_tables(
        trino, event.competition_slug, event.source_season_year, event.event_id
    )
    matches = before[MATCH_TABLE]
    if len(matches) > 1:
        raise ValueError("duplicate Bronze match")
    if matches:
        row = matches[0]
        known = BronzeMatch.from_row([row[name] for name in _MATCH_COLUMNS])
        schedule = known.schedule_row(competition, edition)
        # Revalidate the selected body against the current schedule identity.
        body = store.load_exact(payload.raw.raw_uri, payload.raw.raw_sha256)
        summary = parse_summary(
            body, competition=competition, edition=edition, event=schedule
        )
        if summary.disposition is SummaryDisposition.SOURCE_MALFORMED:
            raise ValueError("candidate incompatible with current schedule")
        payload = MatchPayload(
            schedule,
            summary,
            payload.raw,
            first_published_at=known.first_published_at,
            status_checked_at=known.status_checked_at,
            rechecked_at=known.rechecked_at,
        )
        if known.summary_captured:
            old_body = store.load_exact(known.raw_uri, known.raw_sha256)
            old = parse_summary(
                old_body, competition=competition, edition=edition, event=schedule
            )
            if old.disposition is SummaryDisposition.SOURCE_MALFORMED:
                raise ValueError("cannot verify previous summary")
            if compare_parts(old, summary) == DOWNGRADE_REJECTED:
                raise ValueError("downgrade_rejected: summary parts")
    after = batch_rows(
        [payload], stamp=BatchStamp("preview", datetime.now(timezone.utc))
    )
    if _loses_rows(before, after):
        raise ValueError("downgrade_rejected: actual Bronze cells")
    return payload


def _aggregate(records):
    by_event = defaultdict(list)
    for record in records:
        if record["outcome"] != "outside_registry":
            by_event[record["event_id"]].append(record)
    count = len(by_event)
    parts = {}
    outcomes = {
        outcome: sum(r["outcome"] == outcome for r in records)
        for outcome in sorted({r["outcome"] for r in records})
    }
    for part in PARTS:
        raw = sum(any(r["raw_any"][part] for r in group) for group in by_event.values())
        selected = sum(
            any(r.get("raw", {}).get(part, False) for r in group)
            for group in by_event.values()
        )
        parsed = sum(
            any(r.get("parsed", {}).get(part, False) for r in group)
            for group in by_event.values()
        )
        parts[part] = {
            "raw": raw,
            "raw_selected": selected,
            "parsed": parsed,
            "raw_share": raw / count if count else None,
            "parsed_share": parsed / count if count else None,
        }
    return {
        "matches": len({r["event_id"] for r in records}),
        "comparison_matches": count,
        "selected_matches": sum(
            any("selected_sha256" in r for r in group) for group in by_event.values()
        ),
        "parts": parts,
        "outcomes": outcomes,
        "errors": sum(len(r.get("errors", [])) for r in records),
        "anomalous_matches": sum(
            any(r.get("anomalies") for r in group) for group in by_event.values()
        ),
        "raw_lineup_event_ids": sorted(
            event_id
            for event_id, group in by_event.items()
            if any(r["raw_any"]["lineup"] for r in group)
        ),
    }


def _code_sha(value):
    value = (
        value
        or os.environ.get("ESPN_DEPLOY_GIT_SHA")
        or os.environ.get("RELEASE_COMMIT")
    )
    if not value:
        try:
            value = subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                text=True,
                stderr=subprocess.DEVNULL,
                cwd=Path(__file__).resolve().parents[2],
                timeout=5,
            ).strip()
        except (OSError, subprocess.SubprocessError) as exc:
            raise ValueError(
                "code SHA required: --code-sha or ESPN_DEPLOY_GIT_SHA"
            ) from exc
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ValueError("code SHA must contain 40 lowercase hexadecimal characters")
    return value


def run(
    manifest,
    editions,
    *,
    store,
    trino=None,
    batch_size=100,
    competition=None,
    code_sha=None,
):
    """Only the injected raw reader and (in write mode) Bronze manager do IO."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    manifest_bytes = Path(manifest).read_bytes()
    edition_bytes = Path(editions).read_bytes()
    snapshot = load_editions(Path(editions))
    if snapshot is None:
        raise ValueError("invalid editions snapshot")
    registry = load_denominator()
    target_rows = {
        row.espn_id: row
        for row in registry.rows.values()
        if row.in_target and row.espn_id is not None
    }
    contexts = {}
    for state in snapshot.editions:
        row = registry.row(state.competition_slug)
        if row is not None and row.in_target and row.espn_id is not None:
            key = (row.espn_id, state.year)
            if key in contexts:
                raise ValueError("ambiguous competition/edition")
            contexts[key] = build_competition(row, state)
    grouped = defaultdict(dict)
    rows = list(csv.reader(io.StringIO(manifest_bytes.decode("utf-8"))))
    for fields in rows:
        if len(fields) != 9:
            raise ValueError("manifest must contain nine ledger columns")
        scope, cid, year, event_id, uri, sha, fetched, _, _ = fields
        cid, year, event_id = int(cid), int(year), int(event_id)
        if scope != f"{cid}:{year}" or event_id < 1:
            raise ValueError("invalid manifest scope")
        timestamp = datetime.fromisoformat(fetched.replace("Z", "+00:00"))
        # Trino ledger timestamps are UTC without a timezone suffix.
        timestamp = (
            timestamp.replace(tzinfo=timezone.utc)
            if timestamp.tzinfo is None
            else timestamp.astimezone(timezone.utc)
        )
        versions = grouped[(cid, year, event_id)]
        previous = versions.get(sha)
        candidate = (timestamp, sha, uri)
        if previous is None or candidate > previous:
            versions[sha] = candidate
    report = {
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "editions_sha256": hashlib.sha256(edition_bytes).hexdigest(),
        "parser_version": PARSER_VERSION,
        "code_sha": _code_sha(code_sha),
        "module_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "ledger_rows": len(rows),
        "unique_blobs": len({r[5] for r in rows}),
        "unique_matches": len({key[2] for key in grouped}),
        "mode": "write" if trino is not None else "dry-run",
        "matches": [],
        "receipts": [],
    }
    batches = defaultdict(list)

    def flush(key):
        pending = batches[key]
        if not pending:
            return
        slug, year = key
        owners, retake = duplicate_owners(
            trino, slug, year, [p.schedule.event_id for p, _ in pending]
        )
        for (other_slug, other_year), ids in sorted(retake.items()):
            mark_duplicates(trino, f"{slug}:{year}", other_slug, other_year, ids)
        payloads = [
            replace(p, schedule=replace(p.schedule, duplicate_of=owners[eid]))
            if (eid := p.schedule.event_id) in owners
            else p
            for p, _ in pending
        ]
        receipt = write_tournament_batch(
            TournamentBatch(*key, payloads), trino=trino
        )
        report["receipts"].append(asdict(receipt))
        for _, record in pending:
            record["outcome"] = "written"
        pending.clear()

    for (cid, year, event_id), variants in sorted(grouped.items()):
        context = contexts.get((cid, year))
        slug = target_rows[cid].slug if cid in target_rows else None
        if competition is not None and slug != competition:
            continue
        record = {
            "event_id": event_id,
            "competition_id": cid,
            "season_year": year,
            "competition": slug,
            "variants": len(variants),
            "errors": [],
            "raw_any": dict.fromkeys(PARTS, False),
        }
        report["matches"].append(record)
        if slug is None:
            record["outcome"] = "outside_registry"
            continue
        if context is not None:
            comp, edition = context
        chosen = None
        for fetched, sha, uri in sorted(variants.values(), reverse=True):
            try:
                body = store.load_exact(uri, sha)
                try:
                    presence = raw_parts(body)
                    for part in PARTS:
                        record["raw_any"][part] |= presence[part]
                except (ValueError, TypeError, KeyError, IndexError, AttributeError):
                    presence = dict.fromkeys(PARTS, False)
                if context is None:
                    continue
                event = schedule_row_from_header(
                    body, competition=comp, edition=edition
                )
                if event.event_id != event_id:
                    raise ValueError("manifest event identity mismatch")
                parsed = parse_summary(
                    body, competition=comp, edition=edition, event=event
                )
                if parsed.disposition is SummaryDisposition.SOURCE_MALFORMED:
                    record["errors"].append(
                        {
                            "sha256": sha,
                            "reason": parsed.reason,
                            "disposition": parsed.disposition.value,
                        }
                    )
                    continue
                if chosen is None:
                    chosen = MatchPayload(event, parsed, RawRef(uri, sha, fetched))
                    record.update(
                        selected_sha256=sha,
                        raw=presence,
                        parsed=parsed_parts(parsed),
                        anomalies=list(parsed.anomalies),
                        disposition=parsed.disposition.value,
                    )
            except (RawStoreError, OSError, EspnParseError, ValueError) as exc:
                record["errors"].append({"sha256": sha, "reason": str(exc)})
        record["other_variants"] = len(variants) - (chosen is not None)
        if context is None:
            record["outcome"] = "missing_edition"
        elif chosen is None:
            record["outcome"] = "no_valid_summary"
        elif trino is None:
            record["outcome"] = "parsed"
        else:
            try:
                chosen = _prepare_write(chosen, comp, edition, store, trino)
            except Exception as exc:
                record.update(
                    outcome="skipped", write_error=f"{type(exc).__name__}: {exc}"
                )
                continue
            key = (slug, year)
            batches[key].append((chosen, record))
            if len(batches[key]) >= batch_size:
                flush(key)
    for key in sorted(batches):
        flush(key)
    report["corpus"] = _aggregate(report["matches"])
    report["competitions"] = {
        slug: _aggregate([r for r in report["matches"] if r["competition"] == slug])
        for slug in sorted(
            {r["competition"] for r in report["matches"] if r["competition"]}
        )
    }
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--editions", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--write", action="store_true")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--competition")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--code-sha", help="release commit in containers without .git")
    args = parser.parse_args(argv)
    trino = None
    if args.write:
        from .trino_manager import EspnTrinoTableManager

        trino = EspnTrinoTableManager()
    report = run(
        args.manifest,
        args.editions,
        store=EspnRawStore.from_env(),
        trino=trino,
        batch_size=args.batch_size,
        competition=args.competition,
        code_sha=args.code_sha,
    )
    output = json.dumps(report, ensure_ascii=False, sort_keys=True, default=str)
    if args.report:
        args.report.write_text(output + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
