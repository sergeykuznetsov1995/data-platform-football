"""SYNTHETIC offline source exports, never genuine/runtime acceptance proof."""
from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from dags.scripts import prepare_transfermarkt_offline as cli
from dags.scripts.bench_transfermarkt_signals import parity_report
from dags.utils.transfermarkt_daily_measure import build_daily_measure_sql, daily_denominator
from dags.utils.transfermarkt_scope_planner import build_promoted_registry_query
from scrapers.transfermarkt import offline_qualification as intake
from scrapers.transfermarkt.denominator import COLUMNS, DEFAULT_DENOMINATOR_PATH, load_denominator
from scrapers.transfermarkt.registry import CompetitionType, SeasonFormat
from scrapers.transfermarkt.signal_qualification import GROUPS, QualificationError, validate_qualification
from scrapers.transfermarkt.tmapi import players_url
from tests.unit.dags.test_transfermarkt_scope_planner import _competition, _edition, _joined_row
from tests.unit.scripts.test_run_transfermarkt_current import listing, packet, squad


def create_fixture(root, *, day2_transfer_to=None):
    """Generate actual parser-readable bytes and original receipts for a demo."""
    root = Path(root)
    root.mkdir(parents=True)
    entries, mapping_entries, requests, rows = [], [], [], []

    def capture(label, body, url, stamp, *, status=200):
        body = body.encode() if isinstance(body, str) else intake.canonical(body)
        identity = intake.sha(label.encode())
        receipt = {"capture_id": identity, "content_hash": intake.sha(body), "decoded_bytes": len(body),
                   "source": "transfermarkt", "hash_algorithm": "sha256", "status_code": status,
                   "url": url, "fetched_at": stamp}
        body_name, receipt_name = label.replace("/", "-") + ".body", label.replace("/", "-") + ".json"
        (root / body_name).write_bytes(body)
        intake.write_json(root / receipt_name, receipt)
        entries.append({"body_path": body_name, "receipt_path": receipt_name})
        return identity

    clubs = {str(100 + offset): [str(player) for player in range(1 + offset * 50, 51 + offset * 50)] for offset in range(20)}
    for offset, club in enumerate(clubs):
        competition = f"TEST{offset}"
        season_format = SeasonFormat.SINGLE_YEAR if offset == 2 else SeasonFormat.SPLIT_YEAR
        record = _competition(competition, season_format=season_format)
        if offset == 3:
            record = replace(record, competition_type=CompetitionType.DOMESTIC_CUP,
                             evidence=tuple(replace(item, competition_type=CompetitionType.DOMESTIC_CUP) for item in record.evidence))
        row = _joined_row(record, _edition(competition, "2026", season_format=season_format))
        rows.append(row)
        html = '<select name="saison_id"><option value="2026" selected>26/27</option></select>' + listing((club,))
        if offset == 2:
            html = html.replace("26/27", "2027")
        identity = capture(f"mapping-{offset}", html, f"https://www.transfermarkt.com/test/startseite/wettbewerb/{competition}", "2026-10-09T10:01:00+00:00")
        requests.append({"competition_id": competition, "capture_id": identity})
        mapping_entries.append(entries[-1])
    # A current unknown classification stays in the denominator as a blocker.
    rows.append(_joined_row(_competition("UNKNOWN", status="unknown"), _edition("UNKNOWN", "2026")))
    intake.write_json(root / "registry-input.json", rows)
    template = DEFAULT_DENOMINATOR_PATH.read_text().splitlines()[1].split("\t")
    denom_lines = ["\t".join(COLUMNS)]
    for competition in [f"TEST{i}" for i in range(20)] + ["UNKNOWN", "MISSING"]:
        values = dict(zip(COLUMNS, template))
        values.update(id=competition, name=competition, tier="1", **{"class": "core_club"}, live="1", current_saison_id="2026", reason="rule:synthetic_fixture")
        if competition == "TEST1":
            values["tier"] = "2"
        denom_lines.append("\t".join(values[key] for key in COLUMNS))
    (root / "denominator-input.tsv").write_text("\n".join(denom_lines) + "\n")
    query = build_promoted_registry_query(registry_snapshot_id="registry-1", current_only=False)
    inventory = {"registry_snapshot_id": "registry-1", "status": "promoted", "promotion_revision": 1,
                 "full_registry": True, "query_id": "SYNTHETIC-query-1", "query_sha256": intake.sha(query.encode()),
                 "exported_at": "2026-10-09T10:10:00+00:00", "row_count": len(rows),
                 "all_keys": sorted([[row["competition_id"], row["edition_id"]] for row in rows]),
                 "current_keys": sorted([[row["competition_id"], row["edition_id"]] for row in rows if row["is_current"]]),
                 "registry_sha256": intake.sha((root / "registry-input.json").read_bytes()), "snapshot_fingerprint": intake.sha(intake.canonical(rows))}
    intake.write_json(root / "inventory-input.json", inventory)
    intake.write_json(root / "mappings-input.json", requests)
    intake.write_json(root / "mapping-captures.json", mapping_entries)
    ids = [str(player) for player in range(1, 1001)]
    intake.write_json(root / "cohort-input.json", {"created_at": "2026-10-09T10:15:00+00:00", "player_ids": ids,
                      "clubs": [{"club_id": club, "player_ids": selected, "scope": f"TEST{offset}/2026", "groups": sorted(GROUPS)}
                                for offset, (club, selected) in enumerate(clubs.items())]})
    for day in (10, 11):
        tasks, results = [], {}
        stamp = f"2026-10-{day}T10:00:01+00:00"

        def task(kind, body, url, **extra):
            label = f"d{day}-{len(tasks)}"
            identity = capture(label, body, url, stamp)
            item = {"key": label, "kind": kind, "url": url, "json": kind != "squad", **extra}
            tasks.append(item)
            results[label] = {"raw_capture_id": identity}

        for start in range(0, len(ids), 100):
            selected = ids[start:start + 100]
            body = packet(selected)
            for player in body["data"]:
                player["clubAssignments"][0]["clubId"] = str(100 + (int(player["id"]) - 1) // 50)
                if day == 11 and int(player["id"]) <= 50:
                    player["attributes"]["contractUntil"] = "2029-06-30"
                if day == 11 and player["id"] == "1" and day2_transfer_to is not None:
                    player["clubAssignments"][0]["clubId"] = day2_transfer_to
                    player["attributes"]["contractUntil"] = "2028-06-30"
            task("players", body, players_url(selected), ids=selected)
        rosters = {club: list(selected) for club, selected in clubs.items()}
        if day == 11 and day2_transfer_to is not None:
            rosters["100"].remove("1")
            rosters.setdefault(day2_transfer_to, []).append("1")
        for offset, (club, selected) in enumerate(rosters.items()):
            html = squad(club, selected)
            if day == 11 and club == "100":
                html = html.replace("Jun 30, 2028", "Jun 30, 2029")
            task("squad", html, f"https://www.transfermarkt.com/test/kader/verein/{club}/plus/1", ids=selected,
                 club_id=club, scope=f"TEST{offset if club in clubs else 1}/2026")
        for player in ids:
            task("mv", {"list": []}, f"https://www.transfermarkt.com/ceapi/marketValueDevelopment/graph/{player}", player_id=player)
            task("transfers", {"transfers": []}, f"https://www.transfermarkt.com/ceapi/transferHistory/list/{player}", player_id=player)
        intake.write_json(root / "captures.json", entries)
        report = parity_report(tasks, results, intake.CaptureExport(root / "captures.json"), ids, GROUPS)
        report.update(mode="parity", complete=True, task_failures={})
        intake.write_json(root / f"day{day - 9}.json", report)
    intake.write_json(root / "captures.json", entries)
    return root


@pytest.fixture(scope="module")
def exports(tmp_path_factory):
    return create_fixture(tmp_path_factory.mktemp("source-exports") / "fixture")


def pin_args(exports, output):
    return dict(registry=exports / "registry-input.json", denominator=exports / "denominator-input.tsv",
                inventory=exports / "inventory-input.json", mappings=exports / "mappings-input.json",
                cohort=exports / "cohort-input.json",
                capture_index=exports / "mapping-captures.json", output=output, evidence_kind="fixture")


@pytest.mark.parametrize("evidence_kind", ["fixture", "exported"])
def test_cli_pin_and_qualify_reparse_all_raw_and_keep_unknown_denominator(exports, tmp_path, capsys, monkeypatch, evidence_kind):
    def forbidden(*args, **kwargs):
        raise AssertionError("offline intake attempted a network call")
    monkeypatch.setattr("socket.create_connection", forbidden)
    monkeypatch.setattr("requests.sessions.Session.request", forbidden)
    class OriginalPinClock:
        @staticmethod
        def now(tz):
            return datetime(2026, 10, 9, 11, tzinfo=timezone.utc)
    # Both cases contain synthetic bytes. The exported label alone must never
    # create an artifact accepted by the existing runtime validator.
    monkeypatch.setattr(intake, "datetime", OriginalPinClock)
    args = pin_args(exports, tmp_path / "pin")
    args["evidence_kind"] = evidence_kind
    argv = ["pin"]
    for key, value in args.items():
        argv.extend(["--" + key.replace("_", "-"), str(value)])
    assert cli.main(argv) == 0
    pin = json.loads(capsys.readouterr().out)
    assert len(pin["denominator"]["rows"]) == 22
    assert dict(pin["denominator"]["blocked_competitions"]).keys() == {"UNKNOWN", "MISSING"}
    assert pin["runtime_admission"] is False and pin["launch_authority"] is False
    frozen = tmp_path / "pin"
    denom = daily_denominator(load_denominator(frozen / "denominator.tsv").rows.values(),
                              json.loads((frozen / "registry.json").read_bytes()), denominator_version=pin["denominator_version"])
    sql, params = build_daily_measure_sql(denom, "2026-10-10", as_of="2026-10-12T21:00:00Z")
    assert "WITH" in sql and "UNKNOWN" in params and "MISSING" in params
    assert cli.main(["qualify", "--pin-directory", str(frozen), "--day1", str(exports / "day1.json"),
                     "--day2", str(exports / "day2.json"), "--capture-index", str(exports / "captures.json"),
                     "--output", str(tmp_path / "qualified"), "--evidence-kind", evidence_kind]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == ("fixture_validated" if evidence_kind == "fixture" else "offline_export_validated")
    assert result["players_compared"] == 1000
    assert len(result["changed_observations"]) == 50
    assert result["genuine_runtime_evidence_required"] and result["D2_decision_required"]
    assert result["runtime_admission"] is False and result["acceptance_clocks_changed"] is False
    for path in (tmp_path / "qualified").rglob("*.json"):
        with pytest.raises(QualificationError):
            validate_qualification({"evidence_path": str(path), "evidence_sha256": intake.sha(path.read_bytes())})
    assert not list((tmp_path / "qualified").glob(".offline-validation-*"))


@pytest.mark.parametrize("field,value", [("row_count", 20), ("current_keys", []), ("query_sha256", "bad"),
                                        ("snapshot_fingerprint", "bad"), ("promotion_revision", 0)])
def test_pin_rejects_incomplete_or_unversioned_full_export(exports, tmp_path, field, value):
    census = json.loads((exports / "inventory-input.json").read_bytes())
    census[field] = value
    path = tmp_path / "inventory.json"
    intake.write_json(path, census)
    args = pin_args(exports, tmp_path / "pin")
    args["inventory"] = path
    with pytest.raises(intake.IntakeError):
        intake.pin_registry(**args)
    assert not args["output"].exists()


def test_pin_cannot_overwrite_and_readback_detects_changed_bytes(exports, tmp_path):
    args = pin_args(exports, tmp_path / "pin")
    intake.pin_registry(**args)
    with pytest.raises(FileExistsError):
        intake.pin_registry(**args)
    (args["output"] / "registry.json").write_text("[]")
    with pytest.raises(intake.IntakeError, match="changed"):
        intake.read_pin(args["output"])


def test_missing_mapping_is_retained_not_substituted(exports, tmp_path):
    args = pin_args(exports, tmp_path / "pin")
    path = tmp_path / "mappings.json"
    intake.write_json(path, [])
    args["mappings"] = path
    result = intake.pin_registry(**args)
    assert len(result["mapping"]["blockers"]) == 21
    with pytest.raises(intake.IntakeError, match="source-proven"):
        intake.qualify_offline(args["output"], exports / "day1.json", exports / "day2.json",
                               exports / "captures.json", tmp_path / "q", evidence_kind="fixture")


@pytest.mark.parametrize("change,match", [
    (lambda r: r.update(task_failures={"club-26645": "exhausted405 tries2/2"}), "exhausted"),
    (lambda r: r["qualification_observation"]["players"][0]["tmapi"].update(contract="2030-06-30"), "parsed source"),
    (lambda r: r["qualification_observation"]["players"][0]["raw"]["plus1"].update(fetched_at="2026-10-10T09:00:00Z"), "original clock"),
    (lambda r: r["qualification_observation"].update(unknown_ids=["1"]), "parsed source"),
    (lambda r: r["cohort"].update(scopes=["UNKNOWN/2026"]), "pinned fixed cohort"),
])
def test_qualification_rejects_exhausted_unknown_or_self_reported_values(exports, tmp_path, change, match):
    args = pin_args(exports, tmp_path / "pin")
    intake.pin_registry(**args)
    report = json.loads((exports / "day1.json").read_bytes())
    change(report)
    path = tmp_path / "day1.json"
    intake.write_json(path, report)
    with pytest.raises(intake.IntakeError, match=match):
        intake.qualify_offline(args["output"], path, exports / "day2.json", exports / "captures.json",
                               tmp_path / "q", evidence_kind="fixture")


def test_fixture_cannot_be_promoted_to_exported_evidence(exports, tmp_path):
    args = pin_args(exports, tmp_path / "pin")
    intake.pin_registry(**args)
    with pytest.raises(intake.IntakeError, match="relabelled"):
        intake.qualify_offline(args["output"], exports / "day1.json", exports / "day2.json",
                               exports / "captures.json", tmp_path / "q", evidence_kind="exported")


def test_source_proof_rejects_club_profile_and_failed_capture(exports, tmp_path):
    captures = intake.CaptureExport(exports / "mapping-captures.json")
    identity = next(iter(captures.records))
    body, receipt = captures.records[identity]
    for url, status in [("https://www.transfermarkt.com/test/kader/verein/100/plus/1", 200),
                        (receipt["url"], 405), (receipt["url"] + "/saison_id/2026", 200)]:
        captures.records[identity] = body, {**receipt, "url": url, "status_code": status}
        with pytest.raises(intake.IntakeError):
            intake.current_mapping(json.loads((exports / "registry-input.json").read_bytes()),
                                   [{"competition_id": "TEST0", "capture_id": identity}], captures)


def test_source_selected_edition_mismatch_blocks_without_rewriting_registry(exports):
    captures = intake.CaptureExport(exports / "mapping-captures.json")
    identity = next(iter(captures.records))
    body, receipt = captures.records[identity]
    captures.records[identity] = body.replace(b'value="2026"', b'value="2025"'), receipt
    result = intake.current_mapping(json.loads((exports / "registry-input.json").read_bytes()),
                                    [{"competition_id": "TEST0", "capture_id": identity}], captures)
    assert result["blockers"]["TEST0"] == "source current edition differs from pinned promoted registry"
    assert result["proven"] == []


def test_capture_body_hash_and_path_traversal_are_rejected(exports, tmp_path):
    for entry in [{"body_path": "../escape", "receipt_path": "x"},
                  {"body_path": "wrong.body", "receipt_path": "wrong.json"}]:
        intake.write_json(tmp_path / "index.json", [entry])
        (tmp_path / "wrong.body").write_bytes(b"tampered")
        identity = next(iter(intake.CaptureExport(exports / "mapping-captures.json").records))
        receipt = intake.CaptureExport(exports / "mapping-captures.json").records[identity][1]
        intake.write_json(tmp_path / "wrong.json", receipt)
        with pytest.raises(intake.IntakeError):
            intake.CaptureExport(tmp_path / "index.json")


def test_real_raw_store_adapter_preserves_receipt_identity_and_failed405(tmp_path):
    from pyarrow import fs
    from scrapers.transfermarkt.raw_store import RawResponseStore
    from tests.unit.scrapers.test_transfermarkt_raw_store import _capture

    root = tmp_path / "raw"
    store = RawResponseStore(fs.LocalFileSystem(), str(root), uri_prefix="file:///original/container/raw")
    good = _capture(store)
    failed = _capture(store, body=b"retained WAF", status_code=405, attempt=2)
    originals = {path.relative_to(root): intake.sha(path.read_bytes()) for path in root.rglob("*") if path.is_file()}
    summary = intake.export_raw_store(root, tmp_path / "export")
    assert summary["capture_count"] == 2 and summary["successful_captures"] == 1
    assert summary["failed_captures"][0]["capture_id"] == failed.capture_id
    assert summary["failed_captures"][0]["original_attempt"] == 2
    assert summary["status"] == "exported_not_qualified" and summary["runtime_admission"] is False
    assert (tmp_path / "export" / (good.capture_id + ".json")).read_bytes() == (root / store.capture_manifest_key(good.capture_id)).read_bytes()
    exported = intake.CaptureExport(tmp_path / "export" / "captures.json")
    assert exported.load_capture(good.capture_id)[0] == b"<html>exact</html>\n"
    with pytest.raises(intake.IntakeError, match="exhausted"):
        exported.load_capture(failed.capture_id)
    assert originals == {path.relative_to(root): intake.sha(path.read_bytes()) for path in root.rglob("*") if path.is_file()}


def test_fixed_cohort_id_removal_cannot_be_repaired_by_matching_reports(exports, tmp_path):
    args = pin_args(exports, tmp_path / "pin")
    intake.pin_registry(**args)
    report = json.loads((exports / "day1.json").read_bytes())
    report["cohort"]["player_ids"].pop()
    path = tmp_path / "day1.json"
    intake.write_json(path, report)
    with pytest.raises(intake.IntakeError, match="pinned fixed cohort"):
        intake.qualify_offline(args["output"], path, exports / "day2.json", exports / "captures.json",
                               tmp_path / "q", evidence_kind="fixture")


def test_private_runtime_format_is_cleaned_even_when_validation_raises(exports, tmp_path, monkeypatch):
    args = pin_args(exports, tmp_path / "pin")
    intake.pin_registry(**args)
    original = intake.build_qualification
    private_paths = []

    def fail_after_build(day1, day2, output, **kwargs):
        assert Path(output).parent.stat().st_mode & 0o777 == 0o700
        original(day1, day2, output, **kwargs)
        private_paths.append(Path(output))
        raise RuntimeError("injected failure after validation")

    monkeypatch.setattr(intake, "build_qualification", fail_after_build)
    with pytest.raises(RuntimeError, match="injected failure"):
        intake.qualify_offline(args["output"], exports / "day1.json", exports / "day2.json", exports / "captures.json",
                               tmp_path / "q", evidence_kind="fixture")
    assert private_paths and all(not path.exists() for path in private_paths)
    for path in (tmp_path / "q").rglob("*.json"):
        with pytest.raises(QualificationError):
            validate_qualification({"evidence_path": str(path), "evidence_sha256": intake.sha(path.read_bytes())})


@pytest.mark.parametrize("destination", ["101", "999"])
def test_day2_transfer_reparses_fresh_roster_without_rewriting_initial_cohort(tmp_path, destination):
    exports = create_fixture(tmp_path / "transfer-exports", day2_transfer_to=destination)
    args = pin_args(exports, tmp_path / "pin")
    pin = intake.pin_registry(**args)
    original_cohort = (args["output"] / "cohort.json").read_bytes()
    # Establish the existing contract accepts the actual raw-derived reports.
    # Keep its production-format intermediate private, including on test error.
    from tempfile import TemporaryDirectory
    with TemporaryDirectory(dir=tmp_path) as private:
        baseline = intake.build_qualification(exports / "day1.json", exports / "day2.json", Path(private) / "evidence.json",
                                              editions_proven_by_pin=True)
    assert any(change["player_id"] == "1" and change["field"] == "clubs"
               and change["before"] == ["100"] and change["after"] == [destination]
               for change in baseline["changed_observations"])
    kwargs = dict(pin_directory=args["output"], day1=exports / "day1.json", day2=exports / "day2.json",
                  capture_index=exports / "captures.json", output=tmp_path / "q", evidence_kind="fixture")
    if destination == "999":
        with pytest.raises(intake.IntakeError, match="source-proven current competition membership"):
            intake.qualify_offline(**kwargs)
    else:
        result = intake.qualify_offline(**kwargs)
        assert result["status"] == "fixture_validated" and result["players_compared"] == 1000
        assert any(change["player_id"] == "1" and change["field"] == "clubs"
                   for change in result["changed_observations"])
        assert result["runtime_admission"] is False
        for path in kwargs["output"].rglob("*.json"):
            with pytest.raises(QualificationError):
                validate_qualification({"evidence_path": str(path), "evidence_sha256": intake.sha(path.read_bytes())})
    assert (args["output"] / "cohort.json").read_bytes() == original_cohort
    assert len(pin["cohort_frame"]["player_ids"]) == 1000 and len(pin["cohort_frame"]["scopes"]) == 20
    assert pin["cohort_frame"]["groups"] == sorted(GROUPS)
