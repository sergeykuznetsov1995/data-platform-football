"""Render-smoke for the FBref match-officials SQL (issue #613).

Locks the contract of the Silver unpivot and the Gold fact (source tables,
dedup, two-array UNNEST, xref join with the season predicate, output columns,
pure-SELECT) so a refactor can't silently break it. Pure text/regex checks —
no DuckDB execution, so the Trino-only two-array UNNEST is not a concern here
(its behaviour is covered by the live container verification).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_SQL = PROJECT_ROOT / "dags" / "sql" / "silver" / "fbref_match_officials.sql"
GOLD_SQL = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_match_officials.sql"
GOLD_EMPTY_SQL = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_match_officials_empty.sql"


pytestmark = pytest.mark.unit


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )


class TestSilverOfficialsSql:
    def _sql(self) -> str:
        return SILVER_SQL.read_text(encoding="utf-8")

    def test_reads_bronze_officials(self):
        assert "iceberg.bronze.fbref_match_officials" in _strip_comments(self._sql())

    def test_dedup_via_row_number_on_match_grain(self):
        sql = self._sql()
        assert re.search(
            r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(\s*PARTITION\s+BY\s+match_id",
            sql, re.IGNORECASE,
        )
        assert re.search(r"ORDER\s+BY\s+_ingested_at\s+DESC", sql, re.IGNORECASE)
        assert re.search(r"\brn\s*=\s*1\b", sql)

    def test_unpivot_lists_all_five_roles(self):
        sql = _strip_comments(self._sql())
        assert "UNNEST" in sql.upper(), "Silver must unpivot wide→long via UNNEST"
        for role in ("'referee'", "'ar1'", "'ar2'", "'fourth_official'", "'var'"):
            assert role in sql, f"Silver officials must emit role label {role}"
        for col in ("o.referee", "o.ar1", "o.ar2", "o.fourth_official", "o.var"):
            assert col in sql, f"Silver officials must read bronze column {col}"

    def test_season_normalised_to_slug(self):
        sql = _strip_comments(self._sql())
        assert "MOD(" in sql and "LPAD(" in sql, "season must be normalised to slug"

    def test_drops_empty_official_names(self):
        sql = _strip_comments(self._sql())
        assert re.search(r"official_name\s+IS\s+NOT\s+NULL", sql, re.IGNORECASE)

    def test_outputs_required_columns(self):
        sql = self._sql()
        for col in ("match_id", "league", "season", "role",
                    "official_name", "_bronze_ingested_at"):
            assert re.search(rf"\b{col}\b", sql), f"Silver must project {col}"

    def test_pure_select(self):
        assert "CREATE TABLE" not in _strip_comments(self._sql()).upper()


class TestGoldOfficialsSql:
    def _sql(self) -> str:
        return GOLD_SQL.read_text(encoding="utf-8")

    def test_reads_silver_officials(self):
        assert "iceberg.silver.fbref_match_officials" in _strip_comments(self._sql())

    def test_resolves_referee_via_xref_with_season_predicate(self):
        """referee_id resolved through xref_referee WITH (league, season) — the
        season predicate is mandatory or the join fans out N× (footgun)."""
        sql = _strip_comments(self._sql())
        assert "iceberg.silver.xref_referee" in sql
        assert re.search(r"source\s*=\s*'fbref'", sql)
        assert re.search(r"\.league\s*=\s*o\.league", sql), "missing league predicate"
        assert re.search(r"\.season\s*=\s*o\.season", sql), "missing season predicate"

    def test_left_join_keeps_unresolved_officials(self):
        assert re.search(r"LEFT\s+JOIN", _strip_comments(self._sql()), re.IGNORECASE)

    def test_outputs_required_columns(self):
        sql = self._sql()
        for col in ("match_id", "role", "referee_id",
                    "official_name", "league", "season"):
            assert re.search(rf"\b{col}\b", sql), f"Gold must project {col}"

    def test_pure_select(self):
        assert "CREATE TABLE" not in _strip_comments(self._sql()).upper()


class TestGoldOfficialsEmptyFallback:
    def test_mirrors_schema_from_dim_match_spine(self):
        sql = GOLD_EMPTY_SQL.read_text(encoding="utf-8")
        for col in ("match_id", "role", "referee_id",
                    "official_name", "league", "season"):
            assert re.search(rf"\b{col}\b", sql), f"empty fallback must project {col}"
        assert "iceberg.gold.dim_match" in sql
        assert re.sub(r"\s+", "", sql).find("WHERE1=0") != -1, "must be a 0-row spine"
