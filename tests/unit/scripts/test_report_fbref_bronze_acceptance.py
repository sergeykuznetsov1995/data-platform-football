from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from scripts import report_fbref_bronze_acceptance as report


RUN_ID = "8ca16a99-4039-44a6-a47d-206037f11e70"
SOURCE_RUN_ID = "a323dc70-ded1-405e-9d32-d2650775da59"
COHORT_HASH = "c" * 64


def _live_run(*, scope: str = "current", marker: bool = True) -> dict:
    metadata = {
        "acceptance_profile": True,
        "publication_eligible": False,
        "acceptance_scope": scope,
        "acceptance_cohort": {
            "scope": scope,
            "cohort_sha256": COHORT_HASH,
        },
    }
    if marker:
        metadata["bronze_acceptance"] = {
            "schema_version": "fbref-bronze-acceptance-v1",
            "status": "passed",
            "processing_control_run_id": RUN_ID,
            "scope": scope,
            "cohort_sha256": COHORT_HASH,
            "strict_gates": {"all": True},
        }
    return {
        "status": "succeeded",
        "run_type": "current" if scope == "current" else "backfill",
        "started_at": "2026-11-26T10:00:00+00:00",
        "request_limit": 100,
        "byte_limit": 50 * 1024 * 1024,
        "requests_used": 16,
        "bytes_used": 1024,
        "metadata": metadata,
    }


def _replay_run() -> dict:
    return {
        "status": "succeeded",
        "run_type": "replay",
        "request_limit": 0,
        "byte_limit": 0,
        "requests_used": 0,
        "bytes_used": 0,
        "metadata": {
            "acceptance_replay": True,
            "bronze_acceptance_replay": {
                "schema_version": "fbref-bronze-acceptance-replay-v1",
                "status": "passed",
                "processing_control_run_id": RUN_ID,
                "source_control_run_id": SOURCE_RUN_ID,
                "strict_gates": {"zero_network": True},
            },
        },
    }


def _manager() -> MagicMock:
    manager = MagicMock()
    manager.table_exists.return_value = True
    manager.execute_query.side_effect = [
        [(2, 3, 4, 0)],
        [(3,)],
        [(4,)],
        *([[(1,)]] * len(report._typed_tables())),
        *[[(dataset, "PASS", 1, 1, 0, None)] for dataset in sorted(report.PAGE_DATASET_KEYS)],
    ]
    return manager


def _control():
    control = MagicMock()
    control._transaction.return_value.__enter__.return_value.fetchone.return_value = {
        "control_page_receipts_json": {"control_run_id": RUN_ID, "receipts": []}
    }
    return control


def test_ordinary_cohort_can_pass_while_unclaimed_route_reviews_are_due(monkeypatch):
    monkeypatch.setattr(report, "SEASON_ROUTE_REVIEW_DATES", {"passing": date(2026, 7, 14)})
    control = _control()
    control.get_run.return_value = _live_run()
    control.get_run_summary.return_value = {"target_counts": {"succeeded": 2}}

    evidence = report.build_evidence(
        control=control,
        manager=_manager(),
        control_run_id=RUN_ID,
        scope="current",
        git_sha="a" * 40,
        image_digest="sha256:" + "b" * 64,
    )

    assert evidence["verdict"] == "GO"
    assert evidence["verdict_scope"] == "frozen_cohort"
    assert evidence["run_date_utc"] == "2026-11-26"
    assert evidence["season_route_reviews"][0]["status"] == "review_required"
    assert not evidence["season_route_reviews"][0]["claimed"]
    assert evidence["generic_bronze"]["status"] == "passed"
    assert all(item["batch_rows"] == 1 for item in evidence["typed_bronze"])
    assert {item["dataset"] for item in evidence["typed_page_readback"]} == set(report.PAGE_DATASET_KEYS)


def test_succeeded_run_without_strict_marker_is_no_go():
    control = _control()
    control.get_run.return_value = _live_run(marker=False)
    control.get_run_summary.return_value = {}

    evidence = report.build_evidence(
        control=control,
        manager=_manager(),
        control_run_id=RUN_ID,
        scope="current",
        git_sha="a" * 40,
        image_digest="sha256:" + "b" * 64,
    )

    assert evidence["verdict"] == "NO-GO"


def test_replay_report_uses_replay_marker_without_trino_reads():
    control = MagicMock()
    control.get_run.return_value = _replay_run()
    control.get_run_summary.return_value = {
        "requests_used": 0,
        "bytes_used": 0,
        "traffic_totals": {"network_attempts": 0},
    }
    manager = MagicMock()

    evidence = report.build_evidence(
        control=control,
        manager=manager,
        control_run_id=RUN_ID,
        scope="replay",
        git_sha="a" * 40,
        image_digest="sha256:" + "b" * 64,
    )

    assert evidence["verdict"] == "GO"
    assert evidence["generic_bronze"]["status"] == "not_applicable"
    assert evidence["typed_bronze"] == []
    manager.execute_query.assert_not_called()


def test_report_scope_must_match_durable_acceptance_scope():
    control = _control()
    control.get_run.return_value = _live_run(scope="current")
    control.get_run_summary.return_value = {"target_counts": {"succeeded": 2}}

    evidence = report.build_evidence(
        control=control,
        manager=_manager(),
        control_run_id=RUN_ID,
        scope="history",
        git_sha="a" * 40,
        image_digest="sha256:" + "b" * 64,
    )

    assert evidence["verdict"] == "NO-GO"
    assert "live_run_type_mismatch" in evidence["gate_failures"]
    assert "live_acceptance_profile_mismatch" in evidence["gate_failures"]


def test_generic_count_mismatch_is_visible():
    manager = MagicMock()
    manager.table_exists.return_value = False
    manager.execute_query.side_effect = [[(2, 3, 4, 0)], [(2,)], [(4,)]]

    evidence = report._generic_evidence(manager, RUN_ID)

    assert evidence["status"] == "failed"
    assert evidence["declared_tables"] == 3
    assert evidence["inventory_rows"] == 2


def test_cli_requires_release_identity_before_connecting(monkeypatch):
    control = MagicMock()
    manager = MagicMock()
    monkeypatch.setattr(report.ControlStore, "from_env", control)
    monkeypatch.setattr(report, "TrinoTableManager", manager)

    with pytest.raises(SystemExit, match="git-sha"):
        report.main(
            [
                "--control-run-id",
                RUN_ID,
                "--scope",
                "current",
                "--image-digest",
                "sha256:" + "b" * 64,
            ]
        )

    control.assert_not_called()
    manager.assert_not_called()


def test_cli_rejects_nonimmutable_release_identity_before_connecting(monkeypatch):
    control = MagicMock()
    manager = MagicMock()
    monkeypatch.setattr(report.ControlStore, "from_env", control)
    monkeypatch.setattr(report, "TrinoTableManager", manager)

    with pytest.raises(SystemExit, match="invalid release identity"):
        report.main(
            [
                "--control-run-id",
                RUN_ID,
                "--scope",
                "current",
                "--git-sha",
                "abc123",
                "--image-digest",
                "latest",
            ]
        )

    control.assert_not_called()
    manager.assert_not_called()


def test_policy_exempt_routes_are_reported_not_required():
    typed = report._typed_tables()

    assert typed["player_stats"][1] == "supported"
    assert typed["keeper_keeper"][1] == "supported"
    assert typed["player_passing"][1] == "policy_exempt"
    assert typed["keeper_keeper_adv"][1] == "policy_exempt"
    assert typed["squad_roster"] == ("fbref_squad_roster", "supported")


def test_expired_route_and_wrong_page_are_reported_even_with_passed_run(monkeypatch):
    monkeypatch.setattr(report, "SEASON_ROUTE_REVIEW_DATES", {"passing": date(2000, 1, 1)})
    control = _control()
    control.get_run.return_value = _live_run()
    control.get_run.return_value["metadata"]["acceptance_cohort"]["required_routes"] = ["passing"]
    control.get_run_summary.return_value = {}
    manager = _manager()
    manager.execute_query.side_effect = [
        [(2, 3, 4, 0)], [(3,)], [(4,)], *([[(1,)]] * len(report._typed_tables())),
        [("match_meta", "FAIL", 1, 1, 2, ["correct", "wrong"])],
        [("squad_roster", "PASS", 0, 0, 0, None)], [("standings", "PASS", 1, 1, 0, None)],
    ]
    evidence = report.build_evidence(control=control, manager=manager, control_run_id=RUN_ID,
                                     scope="current", git_sha="a" * 40, image_digest="sha256:" + "b" * 64)
    assert evidence["verdict"] == "NO-GO"
    assert set(evidence["gate_failures"]) == {"season_route_review_due", "typed_page_readback_failed"}
    assert "review required" in report.render_markdown(evidence)
    assert report._route_reviews(date(2000, 1, 30), {"passing"})[0]["current"]
    assert not report._route_reviews(date(2000, 1, 31), {"passing"})[0]["current"]


def test_historical_route_claim_uses_run_start_date_in_utc(monkeypatch):
    monkeypatch.setattr(report, "SEASON_ROUTE_REVIEW_DATES", {"passing": date(2000, 1, 1)})
    control = _control()
    run = _live_run(scope="history")
    run["started_at"] = "2000-01-31T00:30:00+03:00"
    run["metadata"]["acceptance_cohort"]["required_routes"] = ["passing"]
    control.get_run.return_value = run
    control.get_run_summary.return_value = {}
    evidence = report.build_evidence(control=control, manager=_manager(), control_run_id=RUN_ID,
                                     scope="history", git_sha="a" * 40, image_digest="sha256:" + "b" * 64)
    assert evidence["verdict"] == "GO"
    assert evidence["run_date_utc"] == "2000-01-30"
    assert evidence["season_route_reviews"][0]["claimed"] and evidence["season_route_reviews"][0]["current"]


@pytest.fixture
def readback_db():
    duckdb = pytest.importorskip("duckdb")
    sqlglot = pytest.importorskip("sqlglot")
    with duckdb.connect() as db:
        db.execute("ATTACH ':memory:' AS iceberg")
        db.execute("CREATE SCHEMA iceberg.bronze")
        db.execute("CREATE TABLE iceberg.bronze.fbref_page_manifest(target_id varchar, content_hash varchar, "
                   "parser_version varchar, parse_status varchar DEFAULT 'success', persist_status varchar DEFAULT "
                   "'success', validation_status varchar DEFAULT 'success', run_id varchar)")
        for dataset in report.PAGE_DATASET_KEYS:
            key = "match_id" if dataset == "match_meta" else "source_target_id"
            db.execute(f"CREATE TABLE iceberg.bronze.fbref_{dataset} ({key} varchar, source_competition_id varchar, "
                       "source_season_id varchar, _batch_id varchar)")

        def read(dataset, receipts, *, table_exists=True, run_id=RUN_ID):
            control = _control()
            control._transaction.return_value.__enter__.return_value.fetchone.return_value = {
                "control_page_receipts_json": {
                    "control_run_id": run_id, "generic_parser_version": "fbref-page-document-v6",
                    "typed_parser_version": "fbref-typed-bronze-v8", "receipts": receipts,
                }
            }
            manager = MagicMock()
            manager.execute_query.side_effect = lambda sql, params: db.execute(
                sqlglot.transpile(sql, read="trino", write="duckdb")[0], params).fetchall()
            return report._page_readback(control, manager, RUN_ID, [
                {"dataset": dataset, "table": "fbref_" + dataset, "table_exists": table_exists}])[0]

        yield db, read


@pytest.mark.parametrize(("dataset", "target", "physical_id", "competition", "season"), [
    ("match_meta", "fbref:match:aaa", "aaa", "9", "2025-2026"),
    ("standings", "fbref:season:9:2025-2026", "fbref:season:9:2025-2026", "9", "2025-2026"),
    ("squad_roster", "fbref:squad:abc", "fbref:squad:abc", None, None),
])
def test_readback_validates_exact_target_scope_run_and_page_generation(
    readback_db, dataset, target, physical_id, competition, season
):
    db, read = readback_db
    table = f"iceberg.bronze.fbref_{dataset}"
    db.execute(f"INSERT INTO {table} VALUES (?, ?, ?, ?)", [physical_id, competition, season, RUN_ID])
    db.execute("INSERT INTO iceberg.bronze.fbref_page_manifest (target_id, content_hash, parser_version, run_id) "
               "VALUES (?, 'A', 'fbref-page-document-v6', ?)", [target, RUN_ID])
    receipt = {"dataset": dataset, "target_id": target, "content_hash": "A", "competition_id": competition,
               "season_id": season, "expected_rows": 1, "ready": True}
    assert read(dataset, [receipt])["verdict"] == "PASS"
    assert read(dataset, [receipt], run_id=SOURCE_RUN_ID)["verdict"] == "FAIL"
    for changed in ({"content_hash": "B"}, {"target_id": target + "-other"},
                    {"expected_rows": 2}, {"ready": False}):
        assert read(dataset, [{**receipt, **changed}])["verdict"] == "FAIL"
    if dataset != "squad_roster":
        assert read(dataset, [{**receipt, "competition_id": "other"}])["verdict"] == "FAIL"
        assert read(dataset, [{**receipt, "season_id": "other"}])["verdict"] == "FAIL"
    db.execute(f"UPDATE {table} SET _batch_id = ?", [SOURCE_RUN_ID])
    assert read(dataset, [receipt])["verdict"] == "FAIL"
    assert read(dataset, [{**receipt, "expected_rows": 0}], table_exists=False)["verdict"] == "PASS"
    assert read(dataset, [receipt], table_exists=False)["verdict"] == "FAIL"
    db.execute("UPDATE iceberg.bronze.fbref_page_manifest SET parser_version = 'fbref-page-document-v5'")
    assert read(dataset, [{**receipt, "expected_rows": 0}], table_exists=False)["verdict"] == "FAIL"
