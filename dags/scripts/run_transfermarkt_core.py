"""Run one Transfermarkt core scope from fixture captures or live transport."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date
import gzip
import json
from pathlib import Path
import sys
from urllib.parse import urlsplit


if __name__ == "__main__" and not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    offline = modes.add_parser("offline", help="Replay a fixture manifest")
    offline.add_argument("--fixtures-root", type=Path, required=True)
    live = modes.add_parser("live", help="Fetch using the configured transport")
    live.add_argument("--club", action="append", required=True, metavar="ID:SLUG[:NAME]")
    live.add_argument("--player", action="append", default=[], metavar="ID")
    live.add_argument("--coach", action="append", default=[], metavar="ID:SLUG")
    live.add_argument("--i-have-transport-permission", action="store_true")
    for mode in (offline, live):
        for field in ("league", "season"):
            mode.add_argument(f"--{field}", required=True)
        for field in ("scope-id", "cycle-id"):
            mode.add_argument(f"--{field}", required=mode is live)
        mode.add_argument("--edition-id")
        mode.add_argument("--report-out", type=Path)
        mode.add_argument("--write", action="store_true")
        mode.add_argument("--trino-host")
        mode.add_argument("--trino-port", type=int)
        mode.add_argument("--trino-user")
    return parser


def _load_records(root):
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    records = {}
    for fixture in manifest["fixtures"]:
        capture_id = fixture["capture_id"]
        path = root / "captures" / fixture["endpoint"] / f"{capture_id}.json"
        record = json.loads(path.read_text(encoding="utf-8"))
        if record["capture_id"] != capture_id:
            raise ValueError(f"Capture ID mismatch in {path}")
        blob = root / fixture["blob_file"]
        url = record["url"]
        if url in records:
            raise ValueError(f"Multiple fixture captures for {url}")
        records[url] = (record, blob)
    return records


def _fixture_fetch(records):
    from scrapers.transfermarkt.models import FetchOutcome, FetchStatus

    def fetch(url, *, as_json, label, context=None, validator=None):
        entry = records.get(url)
        if entry is None:
            return FetchOutcome(status=FetchStatus.RETRY_EXHAUSTED, error="fixture_missing")
        record, blob = entry
        if record["status_code"] != 200:
            return FetchOutcome(
                status=FetchStatus.RETRY_EXHAUSTED, error=f"http_{record['status_code']}",
                status_code=record["status_code"],
            )
        text = gzip.decompress(blob.read_bytes()).decode("utf-8")
        if as_json:
            try:
                value = json.loads(text)
            except ValueError:
                return FetchOutcome(
                    status=FetchStatus.SCHEMA_ERROR, error="invalid_json",
                    status_code=record["status_code"],
                )
        else:
            value = text
        return FetchOutcome(
            status=FetchStatus.OK,
            value=value,
            raw_capture_id=record["capture_id"],
            raw_body_hash=record["content_hash"],
            raw_fetched_at=record["fetched_at"],
            cache_hit=False,
            status_code=record["status_code"],
        )

    return fetch


def _fixture_raw_loader(records):
    by_capture = {record["capture_id"]: entry for entry in records.values()
                  for record in (entry[0],)}

    def load(capture_id):
        record, blob = by_capture[capture_id]
        return gzip.decompress(blob.read_bytes()), _RawRecord(record)

    return load


class _RawRecord:
    def __init__(self, record):
        self.content_hash = record["content_hash"]
        self.url = record["url"]
        self.fetched_at = record["fetched_at"]


def _report(report, plan, path):
    if path:
        summary = asdict(plan)
        path.write_text(
            json.dumps({**asdict(report), "plan": summary}, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    else:
        print("entity\tstatus\trows")
        for item in report.outcomes:
            print(item["entity"], item["status"], item["rows"], sep="\t")
        print(f"DQ passed: {report.dq['passed']}")


def _season_year(season):
    """The saison year of a slug, with the century window of season.py."""
    if len(season) != 4 or not season.isdecimal():
        raise ValueError(f"Cannot infer a saison year from {season!r}; supply --edition-id")
    first, second = int(season[:2]), int(season[2:])
    if second == (first + 1) % 100:
        candidate = 2000 + first
        return candidate if candidate <= date.today().year + 1 else 1900 + first
    return int(season) - 1


def _squad_season_matches(url, season):
    """A squad fixture belongs to the run only when its saison year matches."""
    parts = urlsplit(url).path.strip("/").split("/")
    if len(parts) < 6 or parts[1] != "kader" or parts[4] != "saison_id" or not parts[5].isdecimal():
        raise ValueError(f"Unrecognised squad fixture URL {url!r}")
    return int(parts[5]) == _season_year(season)


def _club_parts(value):
    parts = value.split(":", 2)
    if len(parts) < 2 or not all(parts[:2]):
        raise ValueError(f"Expected club ID:SLUG[:NAME], got {value!r}")
    return parts[0], parts[1], parts[2] if len(parts) == 3 else ""


def _coach_parts(value):
    parts = value.split(":")
    if len(parts) != 2 or not all(parts):
        raise ValueError(f"Expected coach ID:SLUG, got {value!r}")
    return tuple(parts)


def _live_plan(args):
    from scrapers.transfermarkt.orchestrator import (
        ClubTarget, CoreScopePlan, coach_history_url, squad_url,
    )

    year = int(args.edition_id) if args.edition_id else _season_year(args.season)
    clubs = tuple(
        ClubTarget(club_id, slug, name, squad_url(slug, club_id, year),
                   coach_history_url(slug, club_id))
        for club_id, slug, name in map(_club_parts, args.club)
    )
    return CoreScopePlan(
        league=args.league, season=args.season, scope_id=args.scope_id, cycle_id=args.cycle_id,
        edition_id=args.edition_id or str(year), clubs=clubs,
        market_value_player_ids=tuple(args.player),
        transfer_player_ids=tuple(args.player),
        coach_profile_ids=tuple(map(_coach_parts, args.coach)),
    )


def _offline_plan(args, records):
    from scrapers.transfermarkt.orchestrator import ClubTarget, CoreScopePlan

    if not records:
        raise ValueError("The fixture manifest contains no captures")
    first = next(iter(records.values()))[0]
    season = args.season
    clubs = {}
    mv_players = []
    transfer_players = []
    coaches = {}
    squad_year = None
    core_endpoints = {"squad", "coach_history", "market_value_points", "transfer_events", "coach_profile"}
    for url, (record, _blob) in records.items():
        if record["endpoint"] not in core_endpoints:
            continue
        parts = urlsplit(url).path.strip("/").split("/")
        match parts:
            case [slug, "kader", "verein", club_id, "saison_id", year, "plus", "1"]:
                # The sample mixes seasons; a run owns exactly one of them.
                if _squad_season_matches(url, season):
                    squad_year = int(year)
                    clubs.setdefault(club_id, {"club_slug": slug})["squad_url"] = url
            case [slug, "mitarbeiterhistorie", "verein", club_id, "plus", "1"]:
                clubs.setdefault(club_id, {"club_slug": slug})["coach_history_url"] = url
            case ["ceapi", "marketValueDevelopment", "graph", player_id]:
                mv_players.append(player_id)
            case ["ceapi", "transferHistory", "list", player_id]:
                transfer_players.append(player_id)
            case [slug, "profil", "trainer", coach_id]:
                coaches[coach_id] = slug
            case _:
                raise ValueError(f"Unrecognised {record['endpoint']} fixture URL {url!r}")

    edition = args.edition_id or first.get("edition_id") or squad_year
    year = squad_year or (int(edition) if edition else _season_year(season))
    return CoreScopePlan(
        league=args.league, season=season,
        scope_id=args.scope_id or first["scope_id"], cycle_id=args.cycle_id or first["cycle_id"],
        edition_id=str(edition or year),
        clubs=tuple(ClubTarget(club_id=key, **value) for key, value in clubs.items()),
        market_value_player_ids=tuple(sorted(set(mv_players))),
        transfer_player_ids=tuple(sorted(set(transfer_players))),
        coach_profile_ids=tuple(sorted(coaches.items())),
    )


def _write_options(args):
    import os

    from trino.auth import BasicAuthentication
    from trino.dbapi import connect

    from scrapers.transfermarkt.native_writer import CommittingConnection
    from scrapers.transfermarkt.writer import TransfermarktIcebergWriter

    host = args.trino_host or os.getenv("TRINO_HOST", "trino")
    port = args.trino_port or int(os.getenv("TRINO_PORT", "8080"))
    user = args.trino_user or os.getenv("TRINO_USER", "airflow")
    password = os.getenv("TRINO_PASSWORD")

    def connection_factory():
        connection = connect(
            host=host, port=port, user=user, catalog="iceberg", schema="bronze",
            http_scheme="https" if password else "http",
            auth=BasicAuthentication(user, password) if password else None,
        )
        return CommittingConnection(connection)

    return {"connection_factory": connection_factory,
            "writer": TransfermarktIcebergWriter(trino_host=host, trino_port=port)}


def main(argv=None):
    parser = _parser()
    args = parser.parse_args(argv)
    if args.mode == "live":
        print("Paid Transfermarkt traffic requires the owner's transport permission.", file=sys.stderr)
        if not args.i_have_transport_permission:
            parser.error("live mode requires --i-have-transport-permission")

    from scrapers.transfermarkt.orchestrator import CoreRun, CoreRunError, planned_urls

    try:
        if args.mode == "offline":
            records = _load_records(args.fixtures_root)
            plan = _offline_plan(args, records)
            missing = [url for url in planned_urls(plan) if url not in records]
            if missing:
                raise ValueError("Missing planned fixture: " + ", ".join(missing))
            fetch = _fixture_fetch(records)
            raw_loader = _fixture_raw_loader(records)
        else:
            from scrapers.transfermarkt.client import TransfermarktHttpClient

            plan = _live_plan(args)
            client = TransfermarktHttpClient()
            fetch = client.fetch
            raw_loader = getattr(client, "_raw_store", None)
            raw_loader = raw_loader.load_capture if raw_loader is not None else None
        write_args = _write_options(args) if args.write else {}
        report = CoreRun(fetch, raw_loader=raw_loader).run(plan, **write_args)
        _report(report, plan, args.report_out)
    except (ValueError, KeyError, OSError, CoreRunError) as exc:
        parser.exit(2, f"{parser.prog}: {exc}\n")
    verified = bool(report.receipts) and all(r.get("verified") is True for r in report.receipts)
    return 0 if report.dq["passed"] is True and (not args.write or verified) else 1


if __name__ == "__main__":
    sys.exit(main())
