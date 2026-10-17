"""Offline, source-backed intake. This module never connects to a service.

Pins bind an exported *whole* promoted query to its independently exported
inventory. Neither an operator receipt nor a matching hash proves authenticity;
runtime admission and actual source provenance remain separate gates.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

from dags.utils.transfermarkt_daily_measure import daily_denominator, utc
from dags.utils.transfermarkt_scope_planner import build_promoted_registry_query
from scrapers.transfermarkt.denominator import load_denominator
from scrapers.transfermarkt.signal_qualification import build_qualification

VERSION = "tm-offline-intake-v1"


class IntakeError(ValueError):
    """The local export cannot establish the claimed contract."""


def require(condition, message):
    if not condition:
        raise IntakeError(message)


def sha(body):
    return hashlib.sha256(body).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def write_json(path, value):
    Path(path).write_bytes(canonical(value) + b"\n")


def local_path(root, relative):
    require(isinstance(relative, str) and not Path(relative).is_absolute(), "evidence path must be relative")
    path = (root / relative).resolve()
    require(path.is_relative_to(root.resolve()), "evidence path escapes export")
    return path


def export_raw_store(raw_store, output):
    """Decode an existing local RawResponseStore without modifying its manifest.

    Retained captures can name their original container mount in raw_uri. A
    local Arrow filesystem verifies those URIs against the retained prefix;
    it never opens that URI or constructs an S3/network filesystem.
    """
    from pyarrow import fs
    from scrapers.transfermarkt.raw_store import RawResponseStore

    root, output = Path(raw_store).resolve(), Path(output).resolve()
    require(root.is_dir() and not output.is_relative_to(root), "export must be outside original raw store")
    paths = sorted((root / "captures" / "sha256").glob("*/*.json"))
    require(paths, "local RawResponseStore has no captures")
    verified, failed = [], []
    for path in paths:
        receipt = json.loads(path.read_bytes())
        blob = receipt["blob_key"]
        suffix = "/" + blob
        require(receipt["raw_uri"].endswith(suffix), "retained raw URI does not identify its blob")
        prefix = receipt["raw_uri"][:-len(suffix)]
        store = RawResponseStore(fs.LocalFileSystem(), str(root), uri_prefix=prefix)
        require(path == root / store.capture_manifest_key(receipt["capture_id"]), "capture path identity differs")
        body, record = store.load_capture(receipt["capture_id"])
        verified.append((path, body, record))
        if record.status_code != 200:
            failed.append({"capture_id": record.capture_id, "url": record.url, "status_code": record.status_code,
                           "original_attempt": record.attempt, "fetched_at": record.fetched_at})
    output.mkdir(parents=True, exist_ok=False)
    entries = []
    for original, body, record in verified:
        body_name, receipt_name = record.capture_id + ".body", record.capture_id + ".json"
        (output / body_name).write_bytes(body)
        shutil.copyfile(original, output / receipt_name)
        entries.append({"body_path": body_name, "receipt_path": receipt_name})
    write_json(output / "captures.json", entries)
    summary = {"version": VERSION, "status": "exported_not_qualified", "capture_count": len(verified),
               "successful_captures": len(verified) - len(failed), "failed_captures": failed,
               "original_manifest_bytes_preserved": True, "original_capture_ids_and_clocks_preserved": True,
               "source_authenticity": "not_authenticated_offline", "runtime_admission": False, "launch_authority": False,
               "blockers": ["promoted_registry_inventory_and_current_source_mapping_required",
                            "fixed_cohort_before_day1_required", "two_consecutive_parity_days_and_real_change_required"]}
    if failed:
        summary["blockers"].append("failed_captures_retained_no_retry_or_exhaustion_reset")
    write_json(output / "export.json", summary)
    return summary


class CaptureExport:
    """Read exact existing body/receipt pairs; never repair or re-fetch a capture."""

    def __init__(self, index):
        self.root = Path(index).resolve().parent
        entries = json.loads(Path(index).read_bytes())
        require(isinstance(entries, list), "capture index must be a list")
        self.records = {}
        self.paths = {}
        for entry in entries:
            body_path = local_path(self.root, entry["body_path"])
            receipt_path = local_path(self.root, entry["receipt_path"])
            body, receipt = body_path.read_bytes(), json.loads(receipt_path.read_bytes())
            identity = receipt.get("capture_id")
            require(isinstance(identity, str) and re.fullmatch(r"[a-f0-9]{64}", identity), "capture identity is invalid")
            require(identity not in self.records, "duplicate capture identity")
            require(receipt.get("source") == "transfermarkt" and receipt.get("hash_algorithm") == "sha256"
                    and receipt.get("content_hash") == sha(body) and receipt.get("decoded_bytes") == len(body),
                    "capture body/receipt hash or size differs")
            utc(receipt.get("fetched_at"))
            parsed = urlsplit(receipt.get("url", ""))
            require(parsed.scheme == "https" and parsed.netloc in {
                "www.transfermarkt.com", "www.transfermarkt.de", "tmapi.transfermarkt.technology"
            } and not parsed.fragment, "capture URL is not an official source")
            self.records[identity] = (body, receipt)
            self.paths[identity] = (body_path, receipt_path)

    def load_capture(self, identity):
        require(identity in self.records, "source capture is absent from local export")
        body, receipt = self.records[identity]
        require(receipt.get("status_code") == 200, "failed/exhausted capture cannot prove source content")
        return body, SimpleNamespace(**receipt)

    def proof(self, proof):
        body, receipt = self.load_capture(proof["capture_id"])
        require(proof.get("url") == receipt.url and proof.get("body_sha256") == receipt.content_hash
                and proof.get("fetched_at") == receipt.fetched_at, "report source identity/hash/original clock differs")
        return body, receipt


def current_mapping(registry_rows, mapping_requests, captures):
    """Read the selected competition edition, never a club's selected season."""
    from bs4 import BeautifulSoup
    from scrapers.transfermarkt.discovery import _label_season_format, _profile_identity, _selector_options

    current = {}
    for row in registry_rows:
        if row.get("is_current") is True:
            current.setdefault(row["competition_id"], set()).add(row["edition_id"])
    proven, blockers, seen = [], {}, set()
    for request in mapping_requests:
        competition = request["competition_id"]
        require(competition not in seen, "duplicate competition mapping")
        seen.add(competition)
        require(competition in current, "mapping competition absent from pinned current registry")
        body, receipt = captures.load_capture(request["capture_id"])
        identity = _profile_identity(receipt.url)
        parsed = urlsplit(receipt.url)
        require(identity is not None and identity[0] == competition and not parsed.query
                and "/saison_id/" not in parsed.path, "mapping needs the unforced competition profile")
        options = _selector_options(BeautifulSoup(body.decode("utf-8"), "html.parser"), profile_url=receipt.url)
        selected = [edition for edition, _label, chosen, _attrs in options if chosen]
        selected_formats = {_label_season_format(label, receipt.url).value for _edition, label, chosen, _attrs in options if chosen}
        registry_formats = {row.get("edition_season_format") for row in registry_rows
                            if row.get("competition_id") == competition and row.get("is_current") is True}
        if set(selected) != current[competition] or selected_formats != registry_formats:
            blockers[competition] = "source current edition differs from pinned promoted registry"
        else:
            soup = BeautifulSoup(body.decode("utf-8"), "html.parser")
            clubs = sorted({match.group(1) for anchor in soup.select('table.items a[href]')
                            if (match := re.search(r"/verein/([1-9][0-9]*)(?:/|$)", anchor["href"]))}, key=int)
            proven.append({"competition_id": competition, "edition_id": selected[0],
                           "capture_id": receipt.capture_id, "body_sha256": receipt.content_hash,
                           "url": receipt.url, "fetched_at": receipt.fetched_at, "club_ids": clubs})
    for competition in sorted(current.keys() - seen):
        blockers[competition] = "source current edition proof missing"
    return {"proven": proven, "blockers": blockers}


def cohort_frame(value):
    clubs = value.get("clubs")
    require(isinstance(clubs, list) and clubs, "fixed cohort clubs are missing")
    ids = value.get("player_ids")
    require(isinstance(ids, list) and all(isinstance(player, str) and re.fullmatch(r"[1-9][0-9]*", player) for player in ids)
            and len(ids) >= 1000 and len(ids) == len(set(ids)), "fixed cohort needs >=1000 distinct player IDs")
    assigned = [player for club in clubs for player in club["player_ids"]]
    require(len(assigned) == len(set(assigned)) and set(assigned) == set(ids), "cohort club assignments differ from fixed player IDs")
    scopes = sorted({club["scope"] for club in clubs})
    require(len(scopes) >= 20, "fixed cohort needs >=20 competition editions")
    groups = sorted({group for club in clubs for group in club["groups"]})
    from scrapers.transfermarkt.signal_qualification import GROUPS
    require(GROUPS <= set(groups), "fixed cohort lacks sample groups")
    utc(value.get("created_at"))
    return {"player_ids": sorted(ids, key=int), "scopes": scopes, "groups": groups}


def pin_registry(registry, denominator, inventory, mappings, cohort, capture_index, output, *, evidence_kind):
    """Freeze supplied bytes only after matching the full snapshot inventory.

    Inventory must come from an independent export of the same snapshot's full
    join, including unclassified/inactive/current/noncurrent rows. Its identity
    and clock are retained, but cannot be authenticated offline.
    """
    require(evidence_kind in {"fixture", "exported"}, "evidence kind must be explicit")
    registry, denominator, inventory = map(Path, (registry, denominator, inventory))
    raw = registry.read_bytes()
    rows = json.loads(raw)
    census = json.loads(inventory.read_bytes())
    require(isinstance(rows, list) and rows, "full registry export is empty")
    snapshot = census.get("registry_snapshot_id")
    require(isinstance(snapshot, str) and snapshot.strip() and all(row.get("registry_snapshot_id") == snapshot for row in rows),
            "registry export mixes or lacks snapshots")
    require(census.get("status") == "promoted" and census.get("full_registry") is True
            and type(census.get("promotion_revision")) is int and census["promotion_revision"] > 0,
            "promoted snapshot inventory/version is required")
    utc(census.get("exported_at"))
    query = build_promoted_registry_query(registry_snapshot_id=snapshot, current_only=False)
    require(census.get("query_sha256") == sha(query.encode()) and bool(census.get("query_id")), "export query identity differs")
    keys = sorted([[row.get("competition_id"), row.get("edition_id")] for row in rows])
    current_keys = sorted([[row["competition_id"], row["edition_id"]] for row in rows if row.get("is_current") is True])
    require(len({tuple(key) for key in keys}) == len(rows), "registry has duplicate scope keys")
    require(census.get("row_count") == len(rows) and census.get("all_keys") == keys
            and census.get("current_keys") == current_keys, "full registry inventory/current subset differs; rows must not be dropped")
    require(census.get("registry_sha256") == sha(raw)
            and census.get("snapshot_fingerprint") == sha(canonical(rows)), "registry snapshot bytes/fingerprint differ")
    captures = CaptureExport(capture_index)
    mapping = current_mapping(rows, json.loads(Path(mappings).read_bytes()), captures)
    frozen_cohort = json.loads(Path(cohort).read_bytes())
    frame = cohort_frame(frozen_cohort)
    denom = daily_denominator(load_denominator(denominator).rows.values(), rows,
                              denominator_version=sha(denominator.read_bytes()))
    result = {"version": VERSION, "evidence_kind": evidence_kind, "registry_snapshot_id": snapshot,
              "registry_sha256": sha(raw), "snapshot_fingerprint": census["snapshot_fingerprint"],
              "inventory_sha256": sha(inventory.read_bytes()), "denominator_version": sha(denominator.read_bytes()),
              "cohort_sha256": sha(Path(cohort).read_bytes()), "cohort_frame": frame,
              "pin_written_at": datetime.now(timezone.utc).isoformat(),
              "denominator": asdict(denom), "mapping": mapping,
              "source_authenticity": "not_authenticated_offline", "runtime_admission": False,
              "launch_authority": False, "acceptance_clocks_changed": False}
    output = Path(output)
    # Existing output is an immutable pin; even an identical second call fails.
    output.mkdir(parents=True, exist_ok=False)
    try:
        (output / "registry.json").write_bytes(raw)
        (output / "denominator.tsv").write_bytes(denominator.read_bytes())
        (output / "inventory.json").write_bytes(inventory.read_bytes())
        (output / "cohort.json").write_bytes(Path(cohort).read_bytes())
        write_json(output / "mapping-requests.json", json.loads(Path(mappings).read_bytes()))
        (output / "captures").mkdir()
        entries = []
        for identity, (body_path, receipt_path) in captures.paths.items():
            body_name, receipt_name = f"captures/{identity}.body", f"captures/{identity}.json"
            shutil.copyfile(body_path, output / body_name)
            shutil.copyfile(receipt_path, output / receipt_name)
            entries.append({"body_path": body_name, "receipt_path": receipt_name})
        write_json(output / "captures.json", entries)
        write_json(output / "pin.json", result)
    except Exception:
        # Leave a failed, visibly incomplete directory; never rewrite a pin.
        raise
    return result


def read_pin(directory):
    root = Path(directory)
    pin = json.loads((root / "pin.json").read_bytes())
    rows = json.loads((root / "registry.json").read_bytes())
    require(pin.get("version") == VERSION and sha((root / "registry.json").read_bytes()) == pin.get("registry_sha256")
            and sha((root / "denominator.tsv").read_bytes()) == pin.get("denominator_version")
            and sha((root / "inventory.json").read_bytes()) == pin.get("inventory_sha256")
            and sha((root / "cohort.json").read_bytes()) == pin.get("cohort_sha256")
            and sha(canonical(rows)) == pin.get("snapshot_fingerprint"), "pinned export changed")
    mapping = current_mapping(rows, json.loads((root / "mapping-requests.json").read_bytes()), CaptureExport(root / "captures.json"))
    require(mapping == pin.get("mapping"), "pinned source mapping changed")
    recomputed = asdict(daily_denominator(load_denominator(root / "denominator.tsv").rows.values(), rows,
                                         denominator_version=pin["denominator_version"]))
    require(canonical(recomputed) == canonical(pin.get("denominator")), "pinned denominator changed")
    require(cohort_frame(json.loads((root / "cohort.json").read_bytes())) == pin.get("cohort_frame"), "pinned cohort changed")
    return pin, rows


def qualify_offline(pin_directory, day1, day2, capture_index, output, *, evidence_kind):
    """Reparse all supplied source bytes before existing two-day qualification.

    A fixture can exercise the entire path but never becomes runtime proof.
    Existing exhausted/failed task outcomes cannot be removed by this importer.
    """
    from dags.scripts.bench_transfermarkt_signals import parity_report

    pin, _rows = read_pin(pin_directory)
    require(evidence_kind == pin["evidence_kind"], "fixture/exported provenance must not be relabelled")
    captures = CaptureExport(capture_index)
    reports = [json.loads(Path(path).read_bytes()) for path in (day1, day2)]
    mapping = {f'{row["competition_id"]}/{row["edition_id"]}': row for row in pin["mapping"]["proven"]}
    eligible = {f"{row[0]}/{row[2]}" for row in pin["denominator"]["rows"] if row[1] is not None}
    registry = {f'{row["competition_id"]}/{row["edition_id"]}': row for row in _rows}
    denominator = load_denominator(Path(pin_directory) / "denominator.tsv")
    frozen_cohort = json.loads((Path(pin_directory) / "cohort.json").read_bytes())
    fixed_clubs = {(club["club_id"], player): club["scope"] for club in frozen_cohort["clubs"] for player in club["player_ids"]}
    export_at = utc(json.loads((Path(pin_directory) / "inventory.json").read_bytes())["exported_at"])
    for report_number, report in enumerate(reports):
        require(report.get("mode") == "parity" and report.get("complete") is True and report.get("task_failures") == {},
                "incomplete/failed/exhausted report cannot qualify")
        cohort = report["cohort"]
        require(all(cohort.get(key) == value for key, value in pin["cohort_frame"].items()), "report differs from pinned fixed cohort")
        require(set(cohort["scopes"]) <= mapping.keys() and set(cohort["scopes"]) <= eligible,
                "cohort lacks eligible source-proven current competition editions")
        observation = report["qualification_observation"]
        if report_number == 0:
            require(utc(frozen_cohort["created_at"]) <= utc(observation["observed_from"]), "fixed cohort was created after day1")
            require(evidence_kind == "fixture" or utc(pin["pin_written_at"]) <= utc(observation["observed_from"]),
                    "exported registry/cohort must be pinned before day1; retrospective pinning cannot qualify")
            require(all(utc(observation["observed_from"]) - utc(mapping[scope]["fetched_at"]) <= timedelta(hours=24)
                        for scope in cohort["scopes"]) and utc(observation["observed_from"]) - export_at <= timedelta(hours=24),
                    "day1 registry/source mapping export is stale")
        require(all(utc(mapping[scope]["fetched_at"]) <= utc(observation["observed_from"]) for scope in cohort["scopes"]),
                "source mapping must be pinned before day1")
        require(export_at <= utc(observation["observed_from"]), "registry export must precede day1")
        tasks, observed_scopes = {}, set()
        for row in observation["players"]:
            for kind, proof in row["raw"].items():
                captures.proof(proof)
                key = (kind, proof["capture_id"])
                if key not in tasks:
                    tasks[key] = {"key": str(len(tasks)), "kind": {"tmapi": "players", "plus1": "squad"}.get(kind, kind),
                                  "url": proof["url"], "json": kind != "plus1", "ids": [], "capture_id": proof["capture_id"]}
                task = tasks[key]
                task["ids"].append(row["player_id"])
                if kind == "plus1":
                    scopes = [scope for scope in cohort["scopes"] if row["plus1"]["club_id"] in mapping[scope]["club_ids"]]
                    require(scopes, "sample club has no source-proven current competition membership")
                    if report_number == 0:
                        fixed_scope = fixed_clubs.get((row["plus1"]["club_id"], row["player_id"]))
                        require(fixed_scope in scopes, "sample club/player differs from pinned cohort assignment")
                        scopes = [fixed_scope]
                    # The fixed IDs can transfer on day2. Their fresh roster
                    # must prove the new club inside the same pinned eligible
                    # scope set; every original scope still needs actual samples.
                    observed_scopes.update(scopes)
                    task.update(club_id=row["plus1"]["club_id"], scope=scopes[0])
                elif kind == "tmapi":
                    task["ids"] = parse_qs(urlsplit(proof["url"]).query).get("ids[]", [])
                else:
                    task["player_id"] = row["player_id"]
        require(observed_scopes == set(cohort["scopes"]), "cohort includes scopes without sampled source membership")
        groups = set()
        for scope in observed_scopes:
            row = registry[scope]
            tier = denominator.rows[row["competition_id"]].tier
            if row["competition_type"] == "domestic_league" and tier.isdigit():
                groups.add("top_league" if int(tier) == 1 else "lower_league")
            if row["edition_season_format"] == "single_year":
                groups.add("calendar")
            if row["competition_type"] in {"domestic_cup", "continental_club"}:
                groups.add("cup")
        require(set(cohort["groups"]) <= groups, "sample groups are not backed by pinned registry/denominator")
        rebuilt = parity_report(list(tasks.values()), {t["key"]: {"raw_capture_id": t["capture_id"]} for t in tasks.values()},
                                captures, cohort["player_ids"], set(cohort["groups"]))
        require(rebuilt["qualification_observation"] == observation, "reported comparisons differ from parsed source bodies")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    for offset, path in enumerate((day1, day2), start=1):
        shutil.copyfile(path, output / f"day{offset}.json")
    # The existing experimental format is accepted by runtime independently of
    # result.json. Never retain that format for fixtures or unauthenticated
    # exports: use it only inside a private directory and clean it on failure too.
    with TemporaryDirectory(prefix=".offline-validation-", dir=output) as private:
        validated = build_qualification(output / "day1.json", output / "day2.json", Path(private) / "parity-evidence.json",
                                        editions_proven_by_pin=True)
    result = {"version": VERSION, "status": "fixture_validated" if evidence_kind == "fixture" else "offline_export_validated",
              "evidence_kind": evidence_kind, "pin_sha256": sha((Path(pin_directory) / "pin.json").read_bytes()),
              "source_raw_export": {"index_path": str(Path(capture_index).resolve()),
                                    "index_sha256": sha(Path(capture_index).read_bytes())},
              "players_compared": validated["players_compared"], "changed_observations": validated["changed_observations"],
              "source_current_editions_proven": True, "source_authenticity": "not_authenticated_offline",
              "runtime_admission": False, "launch_authority": False, "D2_decision_required": True,
              "genuine_runtime_evidence_required": True, "acceptance_clocks_changed": False}
    write_json(output / "result.json", result)
    return result
