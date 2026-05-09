"""
Unit tests for ``dags/sql/silver/xref_manager.sql`` — STUB validation (T5/E1).

xref_manager is intentionally an empty placeholder until the R0.2c FotMob
``coachId`` endpoint hardening or the R0.2a FBref match-page parser lands.
The CTAS materialises a zero-row table with the correct column shape so:

  1. T4 DAG-task does not branch on "table exists?"
  2. T5 schema-drift tests can validate the column set today.
  3. Downstream JOINs in Gold never panic with "relation not found".

We assert:
  * ``WHERE 1 = 0`` (or equivalent always-false predicate) → row_count = 0.
  * Schema columns match the union schema (xref_team / xref_referee / xref_match).
  * Header explicitly documents the STUB / Phase-1.5 deferral.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "xref_manager.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


pytestmark = pytest.mark.unit


class TestXrefManagerStub:
    """The STUB must materialise zero rows with the right schema."""

    def test_always_false_predicate(self):
        """Stub guarantees zero rows via WHERE 1=0 or equivalent."""
        sql_lower = _read_sql().lower()
        # Any always-false predicate is fine — accept the documented forms.
        ok = (
            "where 1=0" in sql_lower
            or "where 1 = 0" in sql_lower
            or "where false" in sql_lower
        )
        assert ok, (
            "xref_manager.sql must use WHERE 1=0 / WHERE 1 = 0 / WHERE FALSE "
            "to guarantee zero rows in the STUB CTAS"
        )

    def test_schema_columns_present(self):
        """All 8 documented schema columns appear as `AS <col>` aliases."""
        sql = _read_sql()
        expected_cols = [
            "canonical_id",
            "source",
            "source_id",
            "display_name",
            "league",
            "season",
            "confidence",
            "match_score",
        ]
        for col in expected_cols:
            pattern = re.compile(
                r"AS\s+" + re.escape(col) + r"\b",
                re.IGNORECASE,
            )
            assert pattern.search(sql), (
                f"schema column {col!r} missing as `AS {col}` alias in "
                "xref_manager.sql STUB — Gold dim_manager will JOIN against "
                "this column set"
            )

    def test_column_types_match_union_schema(self):
        """All NULL casts use the documented column types (varchar / double)."""
        sql = _read_sql()
        # 7 varchar columns + 1 double column (match_score)
        assert "CAST(NULL AS varchar)" in sql or "CAST(NULL AS VARCHAR)" in sql, (
            "expected at least one CAST(NULL AS varchar) for the textual "
            "columns (canonical_id, source, source_id, display_name, league, "
            "season, confidence)"
        )
        assert "CAST(NULL AS double)" in sql or "CAST(NULL AS DOUBLE)" in sql, (
            "expected CAST(NULL AS double) for the match_score column — "
            "schema must match xref_team / xref_referee"
        )

    def test_documented_as_stub(self):
        """Header comment must explicitly tag this as a STUB / deferred work."""
        sql_lower = _read_sql().lower()
        # Accept any of the documented anchors.
        anchors = ["stub", "phase 1.5", "r0.2c", "r0.2a"]
        assert any(a in sql_lower for a in anchors), (
            "xref_manager.sql header must explicitly mark itself as STUB / "
            "Phase 1.5 / R0.2c / R0.2a so future readers know why the table "
            "is empty by design"
        )

    def test_pure_select_no_create_table(self):
        """File stays a pure SELECT — silver_tasks wraps in CTAS.

        Strip ``-- ...`` comments first; the header references
        ``CREATE TABLE iceberg.silver.xref_manager`` in a documentation note.
        """
        non_comment = "\n".join(
            line for line in _read_sql().splitlines()
            if not line.lstrip().startswith("--")
        )
        assert "CREATE TABLE" not in non_comment.upper(), (
            "xref_manager.sql must stay pure SELECT in executable SQL"
        )

    def test_no_bronze_table_references(self):
        """STUB must not query any Bronze table — ``WHERE 1=0`` prunes the read.

        Sanity: leaving a real FROM iceberg.bronze.* clause would charge the
        planner with a Trino split it can't avoid; the STUB pattern is to
        SELECT literal NULLs without a FROM clause at all.
        """
        sql_lower = _read_sql().lower()
        assert "iceberg.bronze." not in sql_lower, (
            "xref_manager STUB must not reference Bronze tables — use literal "
            "NULL casts only so Phase 1.5 can swap in real reads cleanly"
        )

    def test_match_count_eight_columns(self):
        """SELECT list has exactly 8 columns to match xref_team/referee schema."""
        sql = _read_sql()
        # Count `AS <col>` aliases — the canonical way to count SELECT outputs.
        as_aliases = re.findall(
            r"AS\s+(canonical_id|source|source_id|display_name|league|"
            r"season|confidence|match_score)\b",
            sql,
            re.IGNORECASE,
        )
        unique_aliases = set(a.lower() for a in as_aliases)
        assert len(unique_aliases) == 8, (
            f"expected 8 distinct AS-aliases (xref union schema), got "
            f"{len(unique_aliases)}: {sorted(unique_aliases)}"
        )
