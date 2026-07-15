from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

import pytest

from dags.utils.transfermarkt_approval import ApprovalJournal
from dags.utils import transfermarkt_registry_publish as registry_publish


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "prepare_transfermarkt_registry_promotion.py"
RUN_ID = "manual__tm_registry_prod_20260711T180000Z"
CYCLE_ID = "tm-registry-77baf81bfcf39e40e44de0ad"
SNAPSHOT_ID = "tm-discovery-0123456789abcdef01234567"


def _load():
    spec = importlib.util.spec_from_file_location("tm_registry_promotion_cli", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _manifest(status: str = "success") -> tuple[dict, str]:
    scopes = [
        {"competition_id": "GB1", "edition_id": "2025", "scope_id": "tm-scope-gb1"}
    ]
    value = {
        "status": status,
        "dry_run": False,
        "cycle_id": CYCLE_ID,
        "snapshot_id": SNAPSHOT_ID,
        "snapshot_hash": "1" * 64,
        "page_count": 2,
        "source_body_hashes": ["2" * 64, "3" * 64],
        "rows": {"competitions": 2, "competition_editions": 3},
        "hashes": {
            "competitions": "4" * 64,
            "competition_editions": "5" * 64,
            "crawl_scopes": registry_publish.stable_hash(scopes),
        },
        "classification_counts": {"eligible": 1, "excluded": 1},
        "blocked_competition_ids": [],
        "promotable": True,
        "crawl_scope_count": len(scopes),
        "crawl_scopes": scopes,
        "writes": [
            {"table": "iceberg.bronze.transfermarkt_competitions", "rows": 2},
            {"table": "iceberg.bronze.transfermarkt_competition_editions", "rows": 3},
        ],
    }
    return value, registry_publish.stable_hash(value)


def _write_manifest(output_root: Path, status: str = "success") -> Path:
    manifest, manifest_hash = _manifest(status)
    path = output_root / CYCLE_ID / f"transfermarkt-discovery-{manifest_hash}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"manifest": manifest, "manifest_hash": manifest_hash}),
        encoding="utf-8",
    )
    return path


def _plan(module, tmp_path: Path):
    return module.build_plan(
        RUN_ID,
        expected_revision=0,
        approval_root=tmp_path / "approvals",
        output_root=tmp_path / "manifests",
        journal_path=tmp_path / "approvals" / "journal.json",
    )


def test_plan_binds_the_exact_manifest_and_authorizes_zero_proxy_io(tmp_path):
    module = _load()
    _write_manifest(tmp_path / "manifests")

    first = _plan(module, tmp_path)
    second = _plan(module, tmp_path)
    packet = first["packet"]

    assert first["promotion_packet_hash"] == second["promotion_packet_hash"]
    assert first["cycle_id"] == CYCLE_ID
    assert not (tmp_path / "approvals").exists()
    assert packet.action == "production_write"
    assert packet.byte_cap_bytes == 0
    assert packet.request_limit == 0
    assert packet.retry_limit == 0
    assert packet.concurrency == 1
    assert first["discovery_manifest_hash"] in packet.argv
    assert first["registry_manifest_hash"] in packet.argv


def test_plan_refuses_a_discovery_cycle_that_did_not_succeed(tmp_path):
    module = _load()
    _write_manifest(tmp_path / "manifests", status="failed")

    with pytest.raises(ValueError, match="only a successful crawl"):
        _plan(module, tmp_path)


def test_apply_installs_one_approved_packet_and_refuses_reuse(tmp_path):
    module = _load()
    _write_manifest(tmp_path / "manifests")
    plan = _plan(module, tmp_path)
    now = datetime(2026, 7, 11, 18, 0, tzinfo=timezone.utc)

    result = module.apply_plan(
        plan,
        presented_hash=plan["promotion_packet_hash"],
        now=now,
    )

    assert result["status"] == "approved"
    packet_path = Path(result["promotion_packet_path"])
    assert packet_path.exists()
    record = ApprovalJournal(plan["journal_path"], clock=lambda: now).get(
        plan["promotion_packet_hash"]
    )
    assert record.status == "approved"

    with pytest.raises(FileExistsError):
        module.apply_plan(
            plan,
            presented_hash=plan["promotion_packet_hash"],
            now=now,
        )


def test_apply_rejects_a_presented_hash_that_drifted(tmp_path):
    module = _load()
    _write_manifest(tmp_path / "manifests")
    plan = _plan(module, tmp_path)

    with pytest.raises(ValueError, match="hash drift"):
        module.apply_plan(plan, presented_hash="b" * 64)

    assert not Path(plan["promotion_packet_path"]).exists()
