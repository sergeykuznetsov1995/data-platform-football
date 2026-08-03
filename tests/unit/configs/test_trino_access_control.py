"""Security contracts for Trino's file-based access control."""

from __future__ import annotations

import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[3]
RULES_PATH = ROOT / "configs" / "trino" / "rules.json"


def _table_rules() -> list[dict[str, object]]:
    rules = json.loads(RULES_PATH.read_text(encoding="utf-8"))
    return rules["tables"]


def test_trusted_view_owners_can_delegate_select() -> None:
    """SECURITY DEFINER views require GRANT_SELECT on their inputs."""
    table_rules = _table_rules()

    assert table_rules[0]["user"] == "airflow"
    assert table_rules[1]["user"] == "superset|openmetadata"
    assert table_rules[2]["group"] == "platform-admins"
    for rule in (table_rules[0], table_rules[2]):
        assert "GRANT_SELECT" in rule["privileges"]
    assert "GRANT_SELECT" not in table_rules[1]["privileges"]


def test_analyst_and_fallback_rules_remain_read_only_or_denied() -> None:
    """The view-owner fix must not grant new Bronze access to analysts."""
    table_rules = _table_rules()

    assert table_rules[3] == {
        "catalog": "iceberg",
        "schema": "silver|gold",
        "privileges": ["SELECT"],
    }
    assert table_rules[4] == {"privileges": []}


def test_completed_query_history_cannot_fill_the_coordinator_heap() -> None:
    """Busy production traffic must expire completed query plans promptly."""
    config = (ROOT / "configs" / "trino" / "config.properties").read_text(
        encoding="utf-8"
    )

    assert "query.max-history=20" in config.splitlines()
    assert "query.min-expire-age=10s" in config.splitlines()


def test_trino_has_heap_and_native_headroom_for_integrity_views() -> None:
    """One integrity view must fit beside normal platform write traffic."""
    config_lines = (ROOT / "configs" / "trino" / "config.properties").read_text(
        encoding="utf-8"
    ).splitlines()
    jvm_lines = (ROOT / "configs" / "trino" / "jvm.config").read_text(
        encoding="utf-8"
    ).splitlines()
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))

    assert "-Xmx8g" in jvm_lines
    assert "query.max-memory=4GB" in config_lines
    assert "query.max-memory-per-node=3GB" in config_lines
    assert "memory.heap-headroom-per-node=3GB" in config_lines
    assert (
        compose["services"]["trino"]["deploy"]["resources"]["limits"]["memory"]
        == "20G"
    )
    assert compose["services"]["trino"]["memswap_limit"] == "30G"
