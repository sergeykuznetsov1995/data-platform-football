"""Security contracts for Trino's file-based access control."""

from __future__ import annotations

import json
from pathlib import Path
import re

import yaml


ROOT = Path(__file__).resolve().parents[3]
RULES_PATH = ROOT / "configs" / "trino" / "rules.json"


def _table_rules() -> list[dict[str, object]]:
    rules = json.loads(RULES_PATH.read_text(encoding="utf-8"))
    return rules["tables"]


def _schema_rules() -> list[dict[str, object]]:
    rules = json.loads(RULES_PATH.read_text(encoding="utf-8"))
    return rules["schemas"]


def _effective_schema_owner(
    *,
    user: str,
    groups: tuple[str, ...] = (),
    catalog: str = "iceberg",
    schema: str,
) -> bool:
    for rule in _schema_rules():
        if "user" in rule and re.fullmatch(str(rule["user"]), user) is None:
            continue
        if "group" in rule and not any(
            re.fullmatch(str(rule["group"]), group) for group in groups
        ):
            continue
        if "catalog" in rule and re.fullmatch(
            str(rule["catalog"]), catalog
        ) is None:
            continue
        if "schema" in rule and re.fullmatch(str(rule["schema"]), schema) is None:
            continue
        return bool(rule["owner"])
    raise AssertionError("schema ACL has no matching fallback rule")


def _effective_table_privileges(
    *,
    user: str,
    groups: tuple[str, ...] = (),
    catalog: str = "iceberg",
    schema: str,
) -> set[str]:
    """Evaluate the first matching table rule like Trino file ACLs do."""

    for rule in _table_rules():
        if "user" in rule and re.fullmatch(str(rule["user"]), user) is None:
            continue
        if "group" in rule and not any(
            re.fullmatch(str(rule["group"]), group) for group in groups
        ):
            continue
        if "catalog" in rule and re.fullmatch(
            str(rule["catalog"]), catalog
        ) is None:
            continue
        if "schema" in rule and re.fullmatch(str(rule["schema"]), schema) is None:
            continue
        return {str(privilege) for privilege in rule["privileges"]}
    raise AssertionError("table ACL has no matching fallback rule")


def test_espn_internal_is_private_to_airflow_and_platform_admins() -> None:
    for user in ("superset", "openmetadata"):
        assert _effective_schema_owner(
            user=user,
            schema="espn_internal",
        ) is False
        assert _effective_table_privileges(
            user=user,
            schema="espn_internal",
        ) == set()

    assert _effective_schema_owner(
        user="airflow",
        schema="espn_internal",
    ) is True
    assert "SELECT" in _effective_table_privileges(
        user="airflow",
        schema="espn_internal",
    )
    assert _effective_schema_owner(
        user="operator",
        groups=("platform-admins",),
        schema="espn_internal",
    ) is True
    assert "SELECT" in _effective_table_privileges(
        user="operator",
        groups=("platform-admins",),
        schema="espn_internal",
    )


def test_trusted_view_owners_can_delegate_select() -> None:
    """SECURITY DEFINER views require GRANT_SELECT on their inputs."""
    table_rules = _table_rules()

    assert table_rules[0]["user"] == "airflow"
    assert table_rules[1] == {
        "user": "superset|openmetadata",
        "catalog": "iceberg",
        "schema": "espn_internal",
        "privileges": [],
    }
    assert table_rules[2]["user"] == "superset|openmetadata"
    assert table_rules[3]["group"] == "platform-admins"
    for rule in (table_rules[0], table_rules[3]):
        assert "GRANT_SELECT" in rule["privileges"]
    assert "GRANT_SELECT" not in table_rules[2]["privileges"]


def test_analyst_and_fallback_rules_remain_read_only_or_denied() -> None:
    """The view-owner fix must not grant new Bronze access to analysts."""
    table_rules = _table_rules()

    assert table_rules[4] == {
        "catalog": "iceberg",
        "schema": "silver|gold",
        "privileges": ["SELECT"],
    }
    assert table_rules[5] == {"privileges": []}


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
