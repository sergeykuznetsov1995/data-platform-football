from __future__ import annotations

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
    ]
    return manager


def test_build_evidence_requires_strict_acceptance_marker():
    control = MagicMock()
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
    assert evidence["generic_bronze"]["status"] == "passed"
    assert all(item["batch_rows"] == 1 for item in evidence["typed_bronze"])


def test_succeeded_run_without_strict_marker_is_no_go():
    control = MagicMock()
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
    control = MagicMock()
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
