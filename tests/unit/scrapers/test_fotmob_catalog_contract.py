import hashlib

import pytest

from scrapers.fotmob.catalog_contract import (
    CATALOG_CONTRACT_SCHEMA,
    build_catalog_contract,
    catalog_contract_from_dict,
)


def _contract(**overrides):
    values = {
        "catalog_batch_id": "batch-1",
        "catalog_content_hash": "a" * 64,
        "classifier_version": "fotmob-men-v1",
        "parser_version": "fotmob-native-v2",
        "entities": ["matches", "season"],
        "entity_policy": {"match_policy": "finished_only"},
        "included_ids": [48, 47],
        "scopes": [(48, "2025 Apertura"), (47, "2025/2026")],
    }
    values.update(overrides)
    return build_catalog_contract(**values)


def test_contract_is_order_independent_and_classifier_bound():
    left = _contract()
    right = _contract(
        entities=["season", "matches"],
        included_ids=[47, 48],
        scopes=[(47, "2025/2026"), (48, "2025 Apertura")],
    )

    assert left == right
    assert left.schema == CATALOG_CONTRACT_SCHEMA == "fotmob-catalog-v1"
    assert left.included_ids == (47, 48)
    assert left.scopes == ("47=2025/2026", "48=2025 Apertura")
    assert left.included_ids_sha256 == hashlib.sha256(b"47\n48\n").hexdigest()
    assert left.scope_sha256 == hashlib.sha256(
        b"47=2025/2026\n48=2025 Apertura\n"
    ).hexdigest()
    assert left.plan_signature.startswith("fmplan1-")
    assert left.plan_signature != _contract(
        classifier_version="fotmob-men-v2"
    ).plan_signature


def test_contract_binds_catalog_parser_policy_ids_and_exact_scopes():
    baseline = _contract()

    assert baseline.plan_signature != _contract(
        parser_version="fotmob-native-v3"
    ).plan_signature
    assert baseline.plan_signature != _contract(
        entity_policy={"match_policy": "all"}
    ).plan_signature
    assert baseline.plan_signature != _contract(
        included_ids=[47], scopes=[(47, "2025/2026")]
    ).plan_signature
    assert baseline.plan_signature != _contract(
        scopes=[(47, "2025/2026")]
    ).plan_signature
    assert baseline != _contract(catalog_batch_id="batch-2")
    assert baseline != _contract(catalog_content_hash="b" * 64)


def test_contract_round_trip_recomputes_all_counts_hashes_and_signature():
    contract = _contract()
    payload = contract.as_dict()

    assert catalog_contract_from_dict(payload) == contract
    for field in (
        "included_count",
        "included_ids_sha256",
        "scope_count",
        "scope_sha256",
        "plan_signature",
    ):
        changed = dict(payload)
        changed[field] = 999 if field.endswith("count") else "0" * 64
        with pytest.raises(ValueError, match=field):
            catalog_contract_from_dict(changed)


def test_contract_rejects_duplicate_or_noncanonical_scope_evidence():
    with pytest.raises(ValueError, match="duplicate"):
        _contract(scopes=[(47, "2025/2026"), (47, "2025/2026")])
    payload = _contract().as_dict()
    payload["scopes"] = list(reversed(payload["scopes"]))
    with pytest.raises(ValueError, match="canonical"):
        catalog_contract_from_dict(payload)
    with pytest.raises(ValueError, match="included ID"):
        _contract(included_ids=[47], scopes=[(48, "2025 Apertura")])
