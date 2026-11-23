"""Run one Transfermarkt core scope from fixture captures or live transport."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import gzip
import hashlib
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
    hashes_by_capture = {}
    for fixture in manifest["fixtures"]:
        capture_id = fixture["capture_id"]
        path = root / "captures" / fixture["endpoint"] / f"{capture_id}.json"
        record = json.loads(path.read_text(encoding="utf-8"))
        if record["capture_id"] != capture_id:
            raise ValueError(f"Capture ID mismatch in {path}")
        if record["endpoint"] != fixture["endpoint"]:
            raise ValueError(f"Fixture endpoint mismatch in {path}")
        body_hash = record["content_hash"]
        if hashes_by_capture.setdefault(capture_id, body_hash) != body_hash:
            raise ValueError("capture_evidence_conflict")
        blob = root / fixture["blob_file"]
        url = record["url"]
        if url in records:
            raise ValueError(f"Multiple fixture captures for {url}")
        records[url] = (record, blob)
    return records


def _fixture_fetch(records):
    from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
    from scrapers.transfermarkt.sections import parse_json_integer

    requested = set()
    reached = set()

    def fetch(url, *, as_json, label, context=None, validator=None):
        requested.add(url)
        entry = records.get(url)
        if entry is None:
            return FetchOutcome(status=FetchStatus.RETRY_EXHAUSTED, error="fixture_missing")
        record, blob = entry
        if record["endpoint"] != label:
            raise ValueError(f"{label} raw capture endpoint differs")
        reached.add(url)
        if record["status_code"] != 200:
            return FetchOutcome(
                status=FetchStatus.RETRY_EXHAUSTED, error=f"http_{record['status_code']}",
                status_code=record["status_code"],
            )
        body = gzip.decompress(blob.read_bytes())
        if hashlib.sha256(body).hexdigest() != record["content_hash"]:
            raise ValueError("body_hash_mismatch")
        evidence = {
            "raw_capture_id": record["capture_id"], "raw_body_hash": record["content_hash"],
            "raw_fetched_at": record["fetched_at"], "status_code": record["status_code"],
        }
        text = body.decode("utf-8")
        if as_json:
            try:
                value = json.loads(text, parse_int=parse_json_integer)
            except ValueError:
                return FetchOutcome(
                    status=FetchStatus.SCHEMA_ERROR, error="invalid_json", **evidence,
                )
        else:
            value = text
        return FetchOutcome(status=FetchStatus.OK, value=value, **evidence)

    fetch.requested = requested
    fetch.reached = reached
    return fetch


def _fixture_raw_loader(records):
    def load(capture_id, url):
        record, blob = records[url]
        if record["capture_id"] != capture_id:
            raise ValueError(f"Capture ID mismatch for {url}")
        return gzip.decompress(blob.read_bytes()), _RawRecord(record)

    return load


class _RawRecord:
    def __init__(self, record):
        self.endpoint = record["endpoint"]
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
    from scrapers.transfermarkt import core

    return core.saison_year(season)


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

    year = _season_year(args.season)
    clubs = tuple(
        ClubTarget(club_id, slug, name, squad_url(slug, club_id, year),
                   coach_history_url(slug, club_id))
        for club_id, slug, name in map(_club_parts, args.club)
    )
    return CoreScopePlan(
        league=args.league, season=args.season, scope_id=args.scope_id, cycle_id=args.cycle_id,
        edition_id=args.edition_id or str(year), edition_inferred=args.edition_id is None, clubs=clubs,
        market_value_player_ids=tuple(args.player),
        transfer_player_ids=tuple(args.player),
        coach_profile_ids=tuple(map(_coach_parts, args.coach)),
    )


# Endpoints of the stage-B core lanes. Their records of another season are on
# file by design (the sample mixes seasons): a run owns exactly one of them.
_CORE_ENDPOINTS = frozenset({"squad", "coach_history", "market_value_points", "transfer_events", "coach_profile"})


def _offline_attendance_targets(urls):
    from scrapers.transfermarkt import attendance, core, sections
    from scrapers.transfermarkt.orchestrator import AttendanceEditionTarget

    starts = {}
    for url in urls:
        parts = urlsplit(url)
        match = attendance._ATTENDANCE_PATH_RE.fullmatch(parts.path)
        if match is None or not sections._attendance_source_valid(url):
            raise ValueError(f"Fixture endpoint 'attendance' does not match its URL {url!r}")
        competition, edition = match.groups()
        attendance._validate_attendance_url(
            url, host=core.TRANSFERMARKT_HOST, path=parts.path,
            competition=competition, edition=edition)
        key = (competition, edition)
        starts.setdefault(key, None)
        if sections._attendance_page_number(url) == 1:
            if starts[key] is not None:
                raise ValueError("attendance_duplicate_first_page")
            starts[key] = AttendanceEditionTarget(
                competition, int(edition), parts.path.split("/")[1], start_url=url)
    if None in starts.values():
        raise ValueError("attendance_first_page_missing")
    return tuple(starts.values())


def _offline_plan(args, records):
    from scrapers.transfermarkt import core
    from scrapers.transfermarkt.orchestrator import ClubTarget, CoreScopePlan

    if not records:
        raise ValueError("The fixture manifest contains no captures")
    first = next(iter(records.values()))[0]
    season = args.season
    clubs = {}
    mv_players = []
    transfer_players = []
    coaches = {}
    attendance_urls = []
    squad_year = None
    for url, (record, _blob) in records.items():
        endpoint = record["endpoint"]
        if endpoint == "attendance":
            attendance_urls.append(url)
            continue
        if endpoint not in _CORE_ENDPOINTS:
            if endpoint in core._ENDPOINTS:
                raise ValueError(f"The offline replay plan cannot represent {endpoint!r} fixtures yet")
            continue
        parts = urlsplit(url).path.strip("/").split("/")
        match endpoint, parts:
            case "squad", [slug, "kader", "verein", club_id, "saison_id", year, "plus", "1"]:
                # The sample mixes seasons; a run owns exactly one of them.
                if _squad_season_matches(url, season):
                    squad_year = int(year)
                    club = clubs.setdefault(club_id, {"club_slug": slug})
                    if "squad_url" in club:
                        raise ValueError(f"Conflicting squad fixture URLs for club {club_id}: "
                                         f"{club['squad_url']}, {url}")
                    club["squad_url"] = url
            case "coach_history", [slug, "mitarbeiterhistorie", "verein", club_id, "plus", "1"]:
                clubs.setdefault(club_id, {"club_slug": slug})["coach_history_url"] = url
            case "market_value_points", ["ceapi", "marketValueDevelopment", "graph", player_id]:
                mv_players.append(player_id)
            case "transfer_events", ["ceapi", "transferHistory", "list", player_id]:
                transfer_players.append(player_id)
            case "coach_profile", [slug, "profil", "trainer", coach_id]:
                coaches[coach_id] = slug
            case _:
                raise ValueError(f"Fixture endpoint {endpoint!r} does not match its URL {url!r}")

    edition = args.edition_id or squad_year or first.get("edition_id") or _season_year(season)
    return CoreScopePlan(
        league=args.league, season=season,
        scope_id=args.scope_id or first["scope_id"], cycle_id=args.cycle_id or first["cycle_id"],
        edition_id=str(edition), edition_inferred=args.edition_id is None,
        clubs=tuple(ClubTarget(club_id=key, **value) for key, value in clubs.items()),
        market_value_player_ids=tuple(sorted(set(mv_players))),
        transfer_player_ids=tuple(sorted(set(transfer_players))),
        coach_profile_ids=tuple(sorted(coaches.items())),
        attendance_editions=_offline_attendance_targets(attendance_urls),
    )


def _write_options(args):
    from inspect import signature
    import os

    from trino.auth import BasicAuthentication
    from trino.dbapi import Connection, connect

    from scrapers.transfermarkt.native_writer import CommittingConnection
    from scrapers.transfermarkt.writer import TransfermarktIcebergWriter

    host = args.trino_host or os.getenv("TRINO_HOST", "trino")
    port = args.trino_port or int(os.getenv("TRINO_PORT", "8080"))
    user = args.trino_user or os.getenv("TRINO_USER", "airflow")
    password = os.getenv("TRINO_PASSWORD")
    transport = {"max_attempts": 1}
    if "legacy_prepared_statements" in signature(Connection).parameters:
        transport["legacy_prepared_statements"] = False

    def connection_factory():
        connection = connect(
            host=host, port=port, user=user, catalog="iceberg", schema="bronze",
            http_scheme="https" if password else "http",
            auth=BasicAuthentication(user, password) if password else None,
            **transport,
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
        if args.edition_id == "":
            raise ValueError("season_edition_mismatch")
        records = _load_records(args.fixtures_root) if args.mode == "offline" else None
        plan = _offline_plan(args, records) if records is not None else _live_plan(args)
        urls = planned_urls(plan)
        if records is not None:
            missing = [url for url in urls if url not in records]
            if missing:
                raise ValueError("Missing planned fixture: " + ", ".join(missing))
            fetch = _fixture_fetch(records)
            run = CoreRun(fetch, request_raw_loader=_fixture_raw_loader(records))
        else:
            from scrapers.transfermarkt.client import TransfermarktHttpClient

            client = TransfermarktHttpClient()
            raw_loader = getattr(client, "_raw_store", None)
            raw_loader = raw_loader.load_capture if raw_loader is not None else None
            run = CoreRun(client.fetch, raw_loader=raw_loader)
        write_args = _write_options(args) if args.write else {}
        report = run.run(plan, **write_args)
        _report(report, plan, args.report_out)
        if args.mode == "offline":
            # A record of a section the core replay plans for that the run
            # never reached is a plan that skipped it in silence: nothing may
            # report success over it. Records of other seasons and of donors
            # the replay does not plan for are on file by design.
            from scrapers.transfermarkt import core

            eligible = {
                url for url, (record, _) in records.items()
                if record["endpoint"] in core._ENDPOINTS
                and (record["endpoint"] != "squad" or _squad_season_matches(url, plan.season))
            }
            missing = fetch.requested - fetch.reached
            unreached = eligible - fetch.requested
            if missing:
                print("Missing fixture responses:", *sorted(missing), sep="\n", file=sys.stderr)
            if unreached:
                print("Unreached fixture records:", *sorted(unreached), sep="\n", file=sys.stderr)
            if missing or unreached:
                return 2
    except (ValueError, KeyError, OSError, CoreRunError) as exc:
        parser.exit(2, f"{parser.prog}: {exc}\n")
    verified = bool(report.receipts) and all(r.get("verified") is True for r in report.receipts)
    return 0 if report.dq["passed"] is True and (not args.write or verified) else 1


if __name__ == "__main__":
    sys.exit(main())
