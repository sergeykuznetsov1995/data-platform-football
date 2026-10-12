"""Transfer reports must prove every competition, budget and durable completion."""

import copy
import importlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scrapers.fotmob.catalog_contract import build_catalog_contract
from scrapers.fotmob.transfer_contract import validate_transfer_report
from scrapers.fotmob.transfers import (
    TRANSFER_POLICY, TRANSFER_PROFILE, TransferState, read_transfer_status,
    transfer_journal_signature,
)
from scripts.fotmob_catalog_acceptance import validate_report

NOW = datetime(2026, 10, 9, 1, tzinfo=timezone.utc)


def report_fixture(tmp_path):
    path = tmp_path / "transfers.sqlite3"
    state = TransferState(path)
    state.record_catalog([47, 48], NOW, complete=True)
    for competition_id in [47, 48]:
        state.record_completion(competition_id, NOW)
    status = read_transfer_status(NOW, path=path)
    contract = build_catalog_contract(
        catalog_batch_id="catalog-1296", catalog_content_hash="a" * 64,
        classifier_version="fotmob-men-v1", included_ids=[47, 48], scopes=[],
        entities=["transfers"], entity_policy={"transfer_policy": TRANSFER_POLICY},
    ).as_dict()
    return {
        "run_id": "transfers-test", "mode": "transfers", "status": "success",
        "complete": True, "completed_at": NOW.isoformat(), "errors": [],
        "operations": [{"entity": "commit_flush", "status": "success", "succeeded": 1}],
        "transport": {"attempts": 0, "direct_bytes": 0, "proxy_bytes": 0},
        "budget": {"requests": 0, "direct_bytes": 0, "proxy_bytes": 0,
                   "max_requests": 6000, "max_direct_bytes": 512 * 1024**2,
                   "max_proxy_bytes": 0},
        "daily_budget": status["daily_budget"], "transfer_status": status,
        "family_summary": status["family_summary"],
        "selection": {
            "profile": TRANSFER_PROFILE, "entities": ["transfers"],
            "scope_lane": "current", "catalog_contract": contract,
            "scope_plan_signature": contract["plan_signature"],
            "transfer_plan_signature": contract["plan_signature"],
            "journal_transfer_plan_signature": transfer_journal_signature(),
            "explicit_scopes": [], "planned_scopes": [], "scope_attempts": [],
            "competition_limit": 0, "season_limit": 0, "deferrals": [],
            "flush_succeeded": True, "catalog_complete": True,
            "completed_transfer_competition_ids": [47, 48], "catalog_ids": [47, 48],
            "catalog_decisions": [{
                "competition_id": i, "catalog_name": "Men's League",
                "profile_name": "Men's League", "source_gender": "male",
                "source_age_group": "adult", "source_type": "league",
                "probe_status": "success", "decision": "included",
                "reason": "structurally confirmed adult men's competition",
                "policy_rule": "include_structural_male_adult",
                "classifier_version": "fotmob-men-v1",
                "profile_target_key": f"leagues?id={i}", "profile_content_hash": "b" * 64,
            } for i in [47, 48]],
        },
    }


def test_exact_transfer_report_passes_both_contracts(tmp_path):
    report = report_fixture(tmp_path)
    assert validate_transfer_report(report) == []
    result = validate_report(report, now=NOW)
    assert result.ok, result.errors


@pytest.mark.parametrize("mutation, expected", [
    (lambda r: r["selection"].update(entities=["matches", "transfers"]), "profile"),
    (lambda r: r["selection"].update(profile="unreviewed"), "profile"),
    (lambda r: r["selection"].update(completed_transfer_competition_ids=[47]), "completed IDs"),
    (lambda r: r["daily_budget"].update(requests=6001), "requests"),
    (lambda r: r["daily_budget"].update(reserved=True), "reservation"),
    (lambda r: r["daily_budget"].update(day="2026-10-08"), "date"),
    (lambda r: r["transfer_status"].update(daily_complete=False), "daily completion"),
    (lambda r: r["transfer_status"]["completion_timestamps"].update(
        {"48": (NOW - timedelta(hours=49)).isoformat()}), "freshness"),
    (lambda r: r["transfer_status"]["completion_timestamps"].update(
        {"48": (NOW + timedelta(seconds=1)).isoformat()}), "future"),
    (lambda r: r["transfer_status"].update(included_ids=[47]), "cohort"),
    (lambda r: r["selection"].update(flush_succeeded=False), "flush"),
    (lambda r: r["selection"].update(catalog_complete=False), "catalog"),
    (lambda r: r.pop("transfer_status"), "missing"),
])
def test_transfer_report_rejects_false_evidence(tmp_path, mutation, expected):
    report = copy.deepcopy(report_fixture(tmp_path))
    mutation(report)
    assert any(expected in error for error in validate_transfer_report(report))


def test_ingest_validates_transfer_profile_and_exports_family_summary(tmp_path):
    from utils import medallion_config

    medallion_config.CONFIG_DIR = Path(__file__).resolve().parents[3] / "configs/medallion"
    medallion_config.reset_cache()
    dag = importlib.import_module("dag_ingest_fotmob")
    report = report_fixture(tmp_path)
    output = tmp_path / "result.json"
    output.write_text(json.dumps(report))
    summary = dag.validate_data(str(output))
    assert summary["family_summary"]["fresh_count"] == 2
    assert summary["selection"]["profile"] == TRANSFER_PROFILE
    report["daily_budget"]["requests"] = 6001
    output.write_text(json.dumps(report))
    with pytest.raises(Exception, match="daily requests budget"):
        dag.validate_data(str(output))
