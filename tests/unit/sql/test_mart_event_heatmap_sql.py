"""
Unit tests for ``dags/sql/gold/mart_event_heatmap.sql`` (E7 / T2).

Pure-text/regex inspection. The mart bins SPADL (x, y) ∈ [0, 100] into a
12×8 grid, drops `unknown` action_canonical (no spatial meaning), and keys
on (team_id, season, league, zone_x, zone_y, action_canonical).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "mart_event_heatmap.sql"


pytestmark = pytest.mark.unit


@pytest.fixture(scope="module")
def sql_text() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def test_renders_valid_sql(sql_text: str) -> None:
    assert SQL_PATH.is_file(), f"missing {SQL_PATH}"
    assert sql_text.strip(), "mart_event_heatmap.sql is empty"
    assert re.search(r"\bSELECT\b", sql_text, re.IGNORECASE)
    assert re.search(r"\bFROM\b", sql_text, re.IGNORECASE)
    stripped = re.sub(r"--.*?$", "", sql_text, flags=re.MULTILINE)
    stripped = re.sub(r"'[^']*'", "''", stripped)
    assert stripped.count("(") == stripped.count(")"), (
        f"unbalanced parens: {stripped.count('(')} vs {stripped.count(')')}"
    )


def test_zone_binning(sql_text: str) -> None:
    """12 column bins (x), 8 row bins (y), both clamped to valid index range."""
    zone_x = re.search(
        r"LEAST\s*\(\s*11\s*,\s*GREATEST\s*\(\s*0\s*,\s*"
        r"CAST\s*\(\s*FLOOR\s*\(\s*x\s*/\s*\(\s*100\.0\s*/\s*12\s*\)\s*\)\s+"
        r"AS\s+integer\s*\)\s*\)\s*\)\s+AS\s+zone_x",
        sql_text,
        re.IGNORECASE | re.DOTALL,
    )
    zone_y = re.search(
        r"LEAST\s*\(\s*7\s*,\s*GREATEST\s*\(\s*0\s*,\s*"
        r"CAST\s*\(\s*FLOOR\s*\(\s*y\s*/\s*\(\s*100\.0\s*/\s*8\s*\)\s*\)\s+"
        r"AS\s+integer\s*\)\s*\)\s*\)\s+AS\s+zone_y",
        sql_text,
        re.IGNORECASE | re.DOTALL,
    )
    assert zone_x, "Expected zone_x = LEAST(11, GREATEST(0, CAST(FLOOR(x/(100.0/12)) AS integer)))"
    assert zone_y, "Expected zone_y = LEAST(7,  GREATEST(0, CAST(FLOOR(y/(100.0/8))  AS integer)))"


def test_filter_unknown_action(sql_text: str) -> None:
    """`unknown` SPADL action has no spatial meaning — must be filtered."""
    pattern = re.compile(
        r"action_canonical\s*<>\s*'unknown'",
        re.IGNORECASE,
    )
    assert pattern.search(sql_text), "Expected `action_canonical <> 'unknown'` filter"


def test_pk_groupby(sql_text: str) -> None:
    """Final GROUP BY enforces PK: (team_id, season, league, zone_x, zone_y, action_canonical)."""
    pattern = re.compile(
        r"GROUP\s+BY\s+"
        r"\s*b\.team_id\s*,\s*"
        r"dt\.team_name\s*,\s*"
        r"b\.season\s*,\s*"
        r"b\.league\s*,\s*"
        r"b\.zone_x\s*,\s*"
        r"b\.zone_y\s*,\s*"
        r"b\.action_canonical",
        re.IGNORECASE | re.DOTALL,
    )
    assert pattern.search(sql_text), (
        "Expected `GROUP BY b.team_id, dt.team_name, b.season, b.league, "
        "b.zone_x, b.zone_y, b.action_canonical` to enforce PK uniqueness"
    )
