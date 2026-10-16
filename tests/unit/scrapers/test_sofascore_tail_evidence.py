"""Physical receipts, not green trigger leaves, qualify tail publication."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from scrapers.sofascore import tail_evidence as tail
from tests.unit.scrapers.test_sofascore_refresh_evidence import complete_tail, original_day_data
from scrapers.sofascore.refresh_evidence import assess_third_day


def test_exact_seven_existing_runner_publications_pass():
    inputs = complete_tail()
    result = tail.assess_tail(inputs, "a" * 40, "2026-10-15")
    assert result["status"] == "pass", result
    assert len(result["leagues"]) == 7
    assert result["expected_leagues"] == sorted(tail.configured_tournaments())
    assert all(row["phases"] == {"season": "pass", "match": "pass"} for row in result["leagues"])
    assert result["source_requests"] == 0 and result["acceptance_started"] is False


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "crossrun", "crossrelease", "native",
    "season", "failed", "receipt_missing", "receipt_partial", "receipt_duplicate", "table_wrong",
    "counter_differs", "manifest_missing", "incomplete", "players", "player_unknown", "fotmob",
    "fotmob_stale", "task_missing", "task_crossrun", "task_retry", "parent_wrong", "child_failed",
    "seven_foreign", "night_wrong", "noop", "boolean_only"])
def test_missing_failed_duplicate_crossrun_or_crossrelease_never_pass(mutation):
    data = complete_tail()
    report = data["reports"][1]
    if mutation == "missing":
        data["reports"].pop()
    elif mutation == "duplicate":
        data["reports"].append(copy.deepcopy(report))
    elif mutation == "crossrun":
        report["evidence_identity"]["run_id"] = "tail__other"
    elif mutation == "crossrelease":
        report["evidence_identity"]["code_revision"] = "b" * 40
    elif mutation == "native":
        report["evidence_identity"]["source_tournament_id"] = 999
    elif mutation == "season":
        report["evidence_identity"]["source_season_id"] += 1
    elif mutation == "failed":
        report["errors"] = ["writer rejected rows"]
    elif mutation == "receipt_missing":
        report.pop("publication_receipts")
    elif mutation == "receipt_partial":
        report.update(rows=12)
    elif mutation == "receipt_duplicate":
        report["publication_receipts"] *= 2
    elif mutation == "table_wrong":
        report["publication_receipts"][0]["location"] = "iceberg.bronze.other"
    elif mutation == "counter_differs":
        report["capture_status_rows"] += 1
    elif mutation == "manifest_missing":
        report.pop("manifest_committed")
    elif mutation == "incomplete":
        report["endpoint_completeness"] = .99
    elif mutation == "players":
        data["child"]["conf"]["run_players"] = True
    elif mutation == "player_unknown":
        data["task_instances"] = [t for t in data["task_instances"] if "player" not in t["task_id"]]
    elif mutation == "fotmob":
        data["import_errors"]["errors"] = ["could not import"]
    elif mutation == "fotmob_stale":
        data["import_errors"]["checked_at"] = "2026-10-14T00:00:00Z"
    elif mutation == "task_missing":
        data["task_instances"].pop(0)
    elif mutation == "task_crossrun":
        data["task_instances"][0]["run_id"] = "other"
    elif mutation == "task_retry":
        data["task_instances"][0]["try_number"] += 1
    elif mutation == "parent_wrong":
        data["parent"]["child_run_id"] = "other"
    elif mutation == "child_failed":
        data["child"]["state"] = "failed"
    elif mutation == "seven_foreign":
        data["child"]["conf"]["competition_ids"] = ["foreign" + str(n) for n in range(7)]
    elif mutation == "night_wrong":
        data["child"]["start_date"] = "2026-10-14T00:00:00Z"
    elif mutation == "noop":
        report.update(capture_status_rows=0, matches_complete=0, matches_skipped_existing=5,
                      publication_receipts=[], tables=[])
    else:
        data["reports"] = [dict(capture_complete=True, status="success")] * 7
    result = tail.assess_tail(data, "a" * 40, "2026-10-15")
    assert result["status"] != "pass", result
    if mutation in {"failed", "fotmob", "child_failed"}:
        assert result["status"] == "fail"


def test_physical_night_is_independent_of_parent_logical_date():
    data = complete_tail()
    data["parent"]["run_id"] = "scheduled__2026-10-14T00:20:00Z"
    assert tail.assess_tail(data, "a" * 40, "2026-10-15")["status"] == "pass"


def test_failed_publication_is_retained_when_another_league_missing():
    data = complete_tail()
    data["reports"][0]["errors"] = ["merge failed"]
    data["reports"].pop()
    result = tail.assess_tail(data, "a" * 40)
    assert result["status"] == "fail"
    assert any("missing/duplicate" in reason for reason in result["reasons"])


def test_actual_calendar_negative_and_empty_daily_remain_separate():
    data = original_day_data()
    data["lanes"] = {"SS": dict(in24=50, total=640), "daily": dict(in24=0, total=0)}
    result = assess_third_day(data, "a" * 40)
    assert result["status"] == "unknown"
    assert result["lanes"]["SS"]["status"] == "fail"
    assert result["lanes"]["SS"]["closed"] == 50
    assert result["lanes"]["daily"]["retained"] == dict(in24=0, total=0)
    assert result["measured_at"] == "2026-09-30T05:04:49+00:00"
    data["lanes"]["daily"] = dict(in24=9, total=10)
    assert assess_third_day(data, "a" * 40)["status"] == "fail"


@pytest.mark.parametrize("field", ["day", "original_delivery", "measured_at"])
def test_missing_historical_fact_does_not_pass(field):
    data = original_day_data()
    data.pop(field)
    assert assess_third_day(data, "a" * 40)["status"] == "unknown"


def test_recorder_revision_does_not_qualify_as_original_delivery():
    data = original_day_data()
    data["original_delivery"]["code_revision"] = "65e92608821dd6fa0cc923f36d61f41d807333a6"
    assert assess_third_day(data, "a" * 40)["status"] == "unknown"


def saved_manifest(tmp_path, data):
    spec = dict(expected_revision="a" * 40, expected_night="2026-10-15", reports=[])
    for key in ("parent", "child", "task_instances", "import_errors"):
        name = key + ".json"
        (tmp_path / name).write_text(json.dumps(data[key]))
        spec[key] = name
    for n, report in enumerate(data["reports"]):
        name = f"report-{n}.json"
        (tmp_path / name).write_text(json.dumps(report))
        spec["reports"].append(name)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(spec))
    return manifest


def test_cli_assembles_real_shapes_with_sockets_and_processes_forbidden(tmp_path):
    manifest = saved_manifest(tmp_path, complete_tail())
    output = tmp_path / "assembled.json"
    script = Path(__file__).resolve().parents[3] / "scripts/sofascore_tail_evidence.py"
    code = """import runpy,sys
def audit(event,args):
    if event.startswith(('socket.','subprocess.')): raise RuntimeError('network/process forbidden')
sys.addaudithook(audit)
sys.argv=sys.argv[1:]
runpy.run_path(sys.argv[0],run_name='__main__')
"""
    proc = subprocess.run([sys.executable, "-B", "-c", code, str(script), "--input", str(manifest),
                           "--output", str(output)], capture_output=True,
                          env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
    assert proc.returncode == 0, proc.stderr.decode()
    result = json.loads(output.read_text())
    assert result["status"] == "pass" and len(result["source_files"]) == 18
    assert result["evidence"]["reports"][0]["publication_receipts"]


def test_assembler_keeps_missing_file_and_does_not_create_success(tmp_path):
    manifest = saved_manifest(tmp_path, complete_tail())
    (tmp_path / "report-0.json").unlink()
    result = tail.assemble_tail(manifest)
    assert result["status"] == "unknown"
    assert result["assembly_gaps"]


def test_known_failed_report_survives_missing_child_receipt():
    data = complete_tail()
    data["child"] = None
    data["reports"][0]["errors"] = ["merge refused"]
    assert tail.assess_tail(data, "a" * 40)["status"] == "fail"


@pytest.mark.parametrize("third_status", ["unknown", "fail"])
def test_refresh_cli_retains_third_day_unknown_or_fail_with_separate_historical_pin(tmp_path, monkeypatch, third_status):
    from scripts import sofascore_refresh_evidence as cli
    from tests.unit.scrapers.test_sofascore_refresh_evidence import night_bundle
    rows = dict(night_bundle(day) for day in (15, 16, 17))
    third = original_day_data()
    third["code_revision"] = "b" * 40
    third["lanes"]["SS"] = dict(in24=50, total=640)
    if third_status == "unknown":
        third["lanes"]["daily"] = dict(in24=0, total=0)
    source = tmp_path / "source.json"
    output = tmp_path / "result.json"
    source.write_text(json.dumps(dict(expected_revision="a" * 40, expected_runs=list(rows), bundles=rows,
                                      third_day=third, third_day_expected_revision="b" * 40)))
    monkeypatch.setattr(sys, "argv", ["cli", "--input", str(source), "--output", str(output)])
    assert cli.main() == 2
    result = json.loads(output.read_text())
    assert result["night_status"] == "pass"
    assert result["status"] == third_status
    assert result["third_day"]["lanes"]["SS"]["status"] == "fail"


def test_latest_retry_without_qualified_history_is_unknown_even_when_claimed_complete():
    data = complete_tail()
    report = data["reports"][1]
    report["evidence_identity"]["try_number"] = 2
    task_id = tail._task("scrape_match_capture", report["evidence_identity"]["league"])
    next(task for task in data["task_instances"] if task["task_id"] == task_id)["try_number"] = 2
    data["physical_history_complete"] = True  # A self-declaration is not a retained history.
    report["physical_history_complete"] = True
    result = tail.assess_tail(data, "a" * 40)
    assert result["status"] == "unknown", result


def test_positive_receipt_cannot_disagree_with_zero_original_counter():
    data = complete_tail()
    report = data["reports"][1]
    report["rows"] = 0
    report["publication_receipts"].append(dict(table="sofascore_player_ratings", rows=12,
                                             location="iceberg.bronze.sofascore_player_ratings"))
    report["tables"].append("iceberg.bronze.sofascore_player_ratings")
    result = tail.assess_tail(data, "a" * 40)
    assert result["status"] == "unknown", result


@pytest.mark.parametrize("earlier_status", ["failed", "partial"])
def test_assembler_retains_earlier_physical_retry_records(tmp_path, earlier_status):
    data = complete_tail()
    latest = data["reports"][1]
    earlier = copy.deepcopy(latest)
    latest["evidence_identity"]["try_number"] = 2
    task_id = tail._task("scrape_match_capture", latest["evidence_identity"]["league"])
    next(task for task in data["task_instances"] if task["task_id"] == task_id)["try_number"] = 2
    if earlier_status == "failed":
        earlier["errors"] = ["first physical merge failed"]
    else:
        earlier["partial"] = dict(remaining_matches=2, stop_reason="wall_deadline")
    data["reports"].insert(1, earlier)
    manifest = saved_manifest(tmp_path, data)
    result = tail.assemble_tail(manifest)
    assert result["status"] == "fail"
    assert result["evidence"]["reports"] == data["reports"]
    assert len(result["evidence"]["reports"]) == 15


def test_matching_zero_receipt_remains_consistent_with_counter():
    data = complete_tail()
    report = data["reports"][1]
    report["rows"] = 0
    report["publication_receipts"].append(dict(table="sofascore_player_ratings", rows=0,
                                             location="iceberg.bronze.sofascore_player_ratings"))
    report["tables"].append("iceberg.bronze.sofascore_player_ratings")
    assert tail.assess_tail(data, "a" * 40)["status"] == "pass"
