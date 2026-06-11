"""
Unit tests for ``dags/sql/gold/mart_scouting_radar.sql`` (E7 / T2).

Pure-text/regex inspection — no DuckDB bridge needed for this mart, since
the assertions cover render integrity, PK ROW_NUMBER window, point-in-time
mask, and minutes filter. These pin the contract without seeding fixtures.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "mart_scouting_radar.sql"


pytestmark = pytest.mark.unit


@pytest.fixture(scope="module")
def sql_text() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def test_renders_valid_sql(sql_text: str) -> None:
    """File exists, non-empty, contains SELECT/FROM and balanced parentheses."""
    assert SQL_PATH.is_file(), f"missing {SQL_PATH}"
    assert sql_text.strip(), "mart_scouting_radar.sql is empty"
    assert re.search(r"\bSELECT\b", sql_text, re.IGNORECASE), "no SELECT keyword"
    assert re.search(r"\bFROM\b", sql_text, re.IGNORECASE), "no FROM keyword"
    # Strip string literals + comments before paren-balance check.
    stripped = re.sub(r"--.*?$", "", sql_text, flags=re.MULTILINE)
    stripped = re.sub(r"'[^']*'", "''", stripped)
    assert stripped.count("(") == stripped.count(")"), (
        f"unbalanced parens: {stripped.count('(')} ( vs {stripped.count(')')} )"
    )


def test_pk_uniqueness_logic(sql_text: str) -> None:
    """Per-(player, season) appearance ranking via ROW_NUMBER over
    (match_date, match_id) — #425: dim_match renamed date -> match_date."""
    pattern = re.compile(
        r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(\s*"
        r"PARTITION\s+BY\s+pm\.player_id\s*,\s*pm\.season\s+"
        r"ORDER\s+BY\s+dm\.match_date\s*,\s*pm\.match_id",
        re.IGNORECASE | re.DOTALL,
    )
    assert pattern.search(sql_text), (
        "Expected ROW_NUMBER() OVER (PARTITION BY pm.player_id, pm.season "
        "ORDER BY dm.match_date, pm.match_id) — appearance_rn drives PK + L5 mask"
    )


def test_xg_l5_point_in_time_mask(sql_text: str) -> None:
    """xg_l5 must be NULL for the first 5 appearances — leakage guard."""
    pattern = re.compile(
        r"CASE\s+WHEN\s+appearance_rn\s*>\s*5\s+THEN\s+xg_l5_raw\s+END\s+AS\s+xg_l5",
        re.IGNORECASE,
    )
    assert pattern.search(sql_text), (
        "Expected `CASE WHEN appearance_rn > 5 THEN xg_l5_raw END AS xg_l5` "
        "— point-in-time strict mask is missing"
    )


def test_filter_minutes_threshold(sql_text: str) -> None:
    """Drop dressing-room subs / unused substitutes via minutes >= 10."""
    pattern = re.compile(r"WHERE\s+pm\.minutes\s*>=\s*10", re.IGNORECASE)
    assert pattern.search(sql_text), "Expected `WHERE pm.minutes >= 10` filter"
