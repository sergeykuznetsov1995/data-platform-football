"""Understat OpenMetadata must describe the complete seven-table contract."""

from pathlib import Path

import yaml

from scrapers.understat.contracts import TABLE_CONTRACTS


ROOT = Path(__file__).resolve().parents[3]
DESCRIPTIONS = ROOT / "configs" / "openmetadata" / "descriptions"
BRONZE_METADATA_COLUMNS = {
    "_source",
    "_entity_type",
    "_ingested_at",
    "_batch_id",
}


def test_all_understat_contract_columns_are_documented() -> None:
    for contract in TABLE_CONTRACTS:
        path = DESCRIPTIONS / f"bronze_{contract.table_name}.yaml"
        spec = yaml.safe_load(path.read_text(encoding="utf-8"))
        documented = {column["name"] for column in spec["columns"]}
        expected = set(contract.required_columns) | BRONZE_METADATA_COLUMNS

        assert documented == expected, (
            f"{path.name} differs from the physical contract: "
            f"missing={sorted(expected - documented)}, "
            f"extra={sorted(documented - expected)}"
        )


def test_optional_enrichment_contracts_are_documented_separately():
    from scrapers.understat.enrichment import ENRICHMENT_CONTRACTS

    assert len(TABLE_CONTRACTS) == 7
    for contract in ENRICHMENT_CONTRACTS:
        spec = yaml.safe_load((DESCRIPTIONS / f"bronze_{contract.table_name}.yaml").read_text())
        assert {c["name"] for c in spec["columns"]} == set(contract.required_columns) | BRONZE_METADATA_COLUMNS
