from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

import pytest

from dags.utils.transfermarkt_approval import ApprovalJournal


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "prepare_transfermarkt_registry_approval.py"


def _load():
    spec = importlib.util.spec_from_file_location("tm_registry_approval_cli", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _plan(module, tmp_path: Path):
    approvals = tmp_path / "approvals"
    state = tmp_path / "state"
    return module.build_plan(
        "manual__tm_registry_prod_20260711T180000Z",
        approval_root=approvals,
        state_root=state,
        output_root=state / "manifests",
        cache_path=state / "cache" / "http.json",
        journal_path=approvals / "journal.json",
    )


def test_plan_is_deterministic_and_side_effect_free(tmp_path):
    module = _load()
    first = _plan(module, tmp_path)
    second = _plan(module, tmp_path)

    assert first["paid_packet_hash"] == second["paid_packet_hash"]
    assert first["bronze_packet_hash"] == second["bronze_packet_hash"]
    assert first["cycle_id"] == "tm-registry-77baf81bfcf39e40e44de0ad"
    assert not (tmp_path / "approvals").exists()
    assert first["packets"]["paid"].affected_tables == ()
    assert set(first["packets"]["bronze"].affected_tables) == {
        "iceberg.bronze.transfermarkt_competitions",
        "iceberg.bronze.transfermarkt_competition_editions",
    }
    assert first["packets"]["paid"].byte_cap_bytes == 15_728_640
    assert first["packets"]["paid"].request_limit == 1024
    assert first["packets"]["paid"].retry_limit == 12
    assert first["packets"]["paid"].concurrency == 1


def test_apply_requires_exact_hashes_and_approves_both(tmp_path):
    module = _load()
    plan = _plan(module, tmp_path)
    with pytest.raises(ValueError, match="paid packet hash drift"):
        module.apply_plan(
            plan,
            presented_paid_hash="0" * 64,
            presented_bronze_hash=plan["bronze_packet_hash"],
        )
    assert not Path(plan["paid_packet_path"]).exists()

    result = module.apply_plan(
        plan,
        presented_paid_hash=plan["paid_packet_hash"],
        presented_bronze_hash=plan["bronze_packet_hash"],
        now=datetime(2026, 7, 11, 18, 0, tzinfo=timezone.utc),
    )
    assert result["status"] == "approved"
    for key in ("paid_packet_path", "bronze_packet_path"):
        assert json.loads(Path(plan[key]).read_text())["argv"] == plan["internal_argv"]
    journal = ApprovalJournal(plan["journal_path"])
    assert journal.get(plan["paid_packet_hash"]).status == "approved"
    assert journal.get(plan["bronze_packet_hash"]).status == "approved"


def test_apply_rejects_naive_operator_clock(tmp_path):
    module = _load()
    plan = _plan(module, tmp_path)
    with pytest.raises(ValueError, match="timezone-aware"):
        module.apply_plan(
            plan,
            presented_paid_hash=plan["paid_packet_hash"],
            presented_bronze_hash=plan["bronze_packet_hash"],
            now=datetime(2026, 7, 11, 18, 0),
        )
    assert not Path(plan["paid_packet_path"]).exists()


def test_apply_is_no_clobber(tmp_path):
    module = _load()
    plan = _plan(module, tmp_path)
    module.apply_plan(
        plan,
        presented_paid_hash=plan["paid_packet_hash"],
        presented_bronze_hash=plan["bronze_packet_hash"],
    )
    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        module.apply_plan(
            plan,
            presented_paid_hash=plan["paid_packet_hash"],
            presented_bronze_hash=plan["bronze_packet_hash"],
        )
