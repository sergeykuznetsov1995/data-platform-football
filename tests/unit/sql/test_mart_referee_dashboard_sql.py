"""
Unit tests for ``dags/sql/gold/mart_referee_dashboard.sql`` (E7 / T2).

Pure-text/regex assertions: file renders, referee_id derived via the same
inline xxhash64 expression as feat_referee_bias (parity with dim_referee),
PK grouping covers (referee_id, name, season, league), and card_type enum
distinguishes yellow from red/second_yellow.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "mart_referee_dashboard.sql"


pytestmark = pytest.mark.unit


@pytest.fixture(scope="module")
def sql_text() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def test_renders_valid_sql(sql_text: str) -> None:
    assert SQL_PATH.is_file(), f"missing {SQL_PATH}"
    assert sql_text.strip(), "mart_referee_dashboard.sql is empty"
    assert re.search(r"\bSELECT\b", sql_text, re.IGNORECASE)
    assert re.search(r"\bFROM\b", sql_text, re.IGNORECASE)
    stripped = re.sub(r"--.*?$", "", sql_text, flags=re.MULTILINE)
    stripped = re.sub(r"'[^']*'", "''", stripped)
    assert stripped.count("(") == stripped.count(")"), (
        f"unbalanced parens: {stripped.count('(')} vs {stripped.count(')')}"
    )


def test_referee_id_from_dim_match(sql_text: str) -> None:
    """#425: referee_id is read verbatim from dim_match.referee_id (the
    canonical silver.xref_referee id) — the legacy inline hash must be gone."""
    assert re.search(r"m\.referee_id", sql_text, re.IGNORECASE), (
        "Expected the spine to select m.referee_id from gold.dim_match"
    )
    assert "xxhash64" not in sql_text.lower(), (
        "Inline referee-name hashing must NOT be used anymore (#425) — "
        "the canonical id comes from dim_match.referee_id"
    )


def test_null_referee_rows_filtered(sql_text: str) -> None:
    """Matches without a resolved referee are excluded from the mart spine."""
    assert re.search(r"m\.referee_id\s+IS\s+NOT\s+NULL", sql_text,
                     re.IGNORECASE), (
        "Expected `WHERE m.referee_id IS NOT NULL` in the spine CTE"
    )


def test_pk_grouping(sql_text: str) -> None:
    """Final GROUP BY must cover the PK + denorm referee_name."""
    pattern = re.compile(
        r"GROUP\s+BY\s+"
        r"\s*pm\.referee_id\s*,\s*"
        r"dr\.referee_name\s*,\s*"
        r"pm\.season\s*,\s*"
        r"pm\.league",
        re.IGNORECASE | re.DOTALL,
    )
    assert pattern.search(sql_text), (
        "Expected `GROUP BY pm.referee_id, dr.referee_name, pm.season, pm.league` "
        "to enforce PK uniqueness (#425: dim_referee exposes referee_name)"
    )


def test_card_type_enum(sql_text: str) -> None:
    """Yellow + red/second_yellow buckets must be split."""
    yellow = re.search(
        r"CASE\s+WHEN\s+card_type\s*=\s*'yellow'\s+THEN\s+1\s+ELSE\s+0\s+END",
        sql_text,
        re.IGNORECASE,
    )
    red_or_second = re.search(
        r"CASE\s+WHEN\s+card_type\s+IN\s*\(\s*'red'\s*,\s*'second_yellow'\s*\)",
        sql_text,
        re.IGNORECASE,
    )
    assert yellow, "Expected explicit `card_type = 'yellow'` aggregator"
    assert red_or_second, (
        "Expected `card_type IN ('red', 'second_yellow')` aggregator — "
        "second_yellow must NOT be double-counted as a yellow"
    )
