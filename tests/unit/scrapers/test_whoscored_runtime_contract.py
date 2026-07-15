from __future__ import annotations

import json
from pathlib import Path

import pytest

from scrapers.whoscored.runtime_contract import (
    RUNTIME_CONTRACT_PATH,
    RuntimeContractError,
    validate_runtime_contract,
)


ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.unit
def test_checked_in_whoscored_runtime_contract_matches_release_tree():
    result = validate_runtime_contract()

    assert result["status"] == "success"
    assert result["parser_version"] == "whoscored-parser-v8"
    assert result["report_schema_version"] == 3
    assert result["business_dataset_count"] == 25
    assert result["file_count"] == 9
    assert len(result["code_tree_sha256"]) == 64


@pytest.mark.unit
def test_runtime_contract_fails_closed_on_one_mixed_file(tmp_path):
    contract = json.loads(RUNTIME_CONTRACT_PATH.read_text(encoding="utf-8"))
    for relative in contract["files"]:
        source = ROOT / relative
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
    mixed = tmp_path / "scrapers" / "base" / "iceberg_writer.py"
    mixed.write_bytes(mixed.read_bytes() + b"\n# stale deployment\n")
    contract_path = tmp_path / "runtime_contract.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")

    with pytest.raises(RuntimeContractError, match="file hash mismatch.*iceberg_writer"):
        validate_runtime_contract(
            contract_path=contract_path,
            runtime_root=tmp_path,
        )


@pytest.mark.unit
def test_airflow_source_pool_contract_rejects_name_and_size_drift(monkeypatch):
    from scrapers.whoscored import runtime_contract

    monkeypatch.setenv("WHOSCORED_SOURCE_POOL_SLOTS", "4")
    monkeypatch.setattr(runtime_contract, "_airflow_pool_slots", lambda _pool: 4)
    assert runtime_contract.validate_airflow_source_pool(
        direct_pool="whoscored_direct_pool",
        backfill_pool="whoscored_direct_pool",
    )["actual_slots"] == 4

    with pytest.raises(RuntimeContractError, match="must share one Airflow source"):
        runtime_contract.validate_airflow_source_pool(
            direct_pool="whoscored_direct_pool",
            backfill_pool="separate_pool",
        )

    monkeypatch.setattr(runtime_contract, "_airflow_pool_slots", lambda _pool: 3)
    with pytest.raises(RuntimeContractError, match="pool size mismatch"):
        runtime_contract.validate_airflow_source_pool(
            direct_pool="whoscored_direct_pool",
            backfill_pool="whoscored_direct_pool",
        )
