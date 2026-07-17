from __future__ import annotations

import json
import uuid
from unittest.mock import MagicMock

import pytest

from scrapers.fbref.raw_audit import (
    capture_and_write_raw_inventory,
    load_inventory_baseline,
    raw_baseline_anchor,
    successful_attempt_snapshot,
)
from scrapers.fbref.raw_store import RawPageStore
from scripts import audit_fbref_production


RUN_ID = "8ca16a99-4039-44a6-a47d-206037f11e70"


def test_production_cli_has_no_missing_baseline_bypass():
    with pytest.raises(SystemExit):
        audit_fbref_production.build_parser().parse_args(
            ["--allow-missing-baseline"]
        )


def test_baseline_capture_requires_control_run_before_clients(monkeypatch):
    raw = MagicMock()
    control = MagicMock()
    monkeypatch.setattr(
        audit_fbref_production.RawPageStore, "from_env", raw
    )
    monkeypatch.setattr(
        audit_fbref_production.ControlStore, "from_env", control
    )

    with pytest.raises(SystemExit, match="--control-run-id"):
        audit_fbref_production.main(["--capture-baseline", "/tmp/x"])

    raw.assert_not_called()
    control.assert_not_called()


def test_cli_capture_is_create_once_and_control_anchored(
    monkeypatch, tmp_path, capsys
):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    store._write_bytes("immutable/one.bin", b"one")
    control = MagicMock()
    control.record_raw_baseline.return_value = {"idempotent": False}
    monkeypatch.setattr(
        audit_fbref_production.ControlStore,
        "from_env",
        MagicMock(return_value=control),
    )
    monkeypatch.setattr(
        audit_fbref_production.RawPageStore,
        "from_env",
        MagicMock(return_value=store),
    )
    destination = tmp_path / "acceptance" / "baseline.json"

    assert (
        audit_fbref_production.main(
            [
                "--control-run-id",
                RUN_ID,
                "--capture-baseline",
                str(destination),
            ]
        )
        == 0
    )

    inventory, digest = load_inventory_baseline(destination)
    control.record_raw_baseline.assert_called_once_with(
        RUN_ID, raw_baseline_anchor(inventory, digest)
    )
    output = json.loads(capsys.readouterr().out)
    assert output["control_anchored"] is True
    assert output["baseline_sha256"] == digest


def test_cli_audit_checks_anchor_and_sealed_attempt_snapshot(
    monkeypatch, tmp_path
):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    baseline_path, baseline, _ = capture_and_write_raw_inventory(
        store, tmp_path / "baseline.json"
    )
    attempt_id = str(uuid.uuid4())
    attempts = [{"attempt_id": attempt_id}]
    snapshot = successful_attempt_snapshot(attempts)
    control = MagicMock()
    control.get_raw_baseline.return_value = raw_baseline_anchor(
        baseline.summary, baseline.baseline_sha256
    )
    control.seal_raw_fetch_attempts.side_effect = [
        {**snapshot, "idempotent": False},
        {**snapshot, "idempotent": True},
    ]
    monkeypatch.setattr(
        audit_fbref_production.ControlStore,
        "from_env",
        MagicMock(return_value=control),
    )
    monkeypatch.setattr(
        audit_fbref_production.RawPageStore,
        "from_env",
        MagicMock(return_value=store),
    )
    monkeypatch.setattr(
        audit_fbref_production,
        "load_successful_run_attempts",
        MagicMock(return_value=attempts),
    )
    audit = MagicMock(
        return_value={
            "control_run_id": RUN_ID,
            "status": "passed",
            "successful_attempt_count": 1,
            "audited_attempt_count": 1,
            "failures": [],
            "attempts": [],
        }
    )
    monkeypatch.setattr(audit_fbref_production, "audit_raw_fetches", audit)

    assert (
        audit_fbref_production.main(
            [
                "--control-run-id",
                RUN_ID,
                "--baseline",
                str(baseline_path),
                "--output-root",
                str(tmp_path / "artifacts"),
            ]
        )
        == 0
    )

    assert control.seal_raw_fetch_attempts.call_count == 2
    assert audit.call_args.kwargs["require_baseline"] is True
    assert audit.call_args.kwargs["metadata"][
        "raw_attempt_snapshot_sha256"
    ] == snapshot["successful_attempt_ids_sha256"]
