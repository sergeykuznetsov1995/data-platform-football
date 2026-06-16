"""Render-smoke for ``dags/sql/silver/transfermarkt_coaches.sql`` (issue #434).

Head-coach snapshot feeding gold.dim_manager nationality/dob enrichment. This
file freezes the SQL contract — bronze source, name-normalize canonical_id
(must match xref_manager so TM coaches glue to the spine), dedup, columns.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "transfermarkt_coaches.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestTransfermarktCoachesSql:

    def test_reads_bronze_coaches(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.bronze.transfermarkt_coaches" in sql

    def test_canonical_id_name_normalize_idiom(self):
        """Must use the SAME diacritic-stripping idiom as xref_manager.sql so a
        TM head coach lands on the spine's canonical_id."""
        sql = _strip_comments(_read_sql())
        assert "NORMALIZE(b.name, NFD)" in sql, "must NORMALIZE(NFD) before slugging"
        assert r"\p{Mn}+" in sql, "must strip combining marks (\\p{Mn})"
        assert "canonical_id" in sql

    def test_dedup_via_row_number(self):
        sql = _read_sql()
        assert re.search(
            r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(\s*PARTITION\s+BY\s+coach_id",
            sql, re.IGNORECASE,
        ), "must dedup via ROW_NUMBER OVER (PARTITION BY coach_id, league, season)"

    def test_outputs_required_columns(self):
        sql = _read_sql()
        for col in (
            "coach_id", "canonical_id", "name", "role", "dob", "nationality",
            "current_club_id", "current_club_name", "_bronze_ingested_at",
            "league", "season",
        ):
            assert re.search(rf"\b{col}\b", sql), (
                f"transfermarkt_coaches.sql must project `{col}`"
            )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "must remain a pure SELECT (CTAS-wrapping is run_silver_transform's job)"
        )
