"""Render-smoke for ``dags/sql/silver/capology_transfer_window.sql``.

Issue #603: promote write-only bronze.capology_transfer_window to Silver
(net transfer balance, team-grain). Фиксирует source, dedup, xref JOIN,
pure SELECT и обязательное двойное-кавычение зарезервированного `"foreign"`.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "capology_transfer_window.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestCapologyTransferWindowSql:

    def test_reads_bronze_transfer_window(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.bronze.capology_transfer_window" in sql, (
            "capology_transfer_window.sql must read bronze.capology_transfer_window"
        )

    def test_single_source_no_other_bronze(self):
        sql = _strip_comments(_read_sql())
        bronze_refs = set(re.findall(r"iceberg\.bronze\.(\w+)", sql))
        assert bronze_refs == {"capology_transfer_window"}, (
            f"expected only capology_transfer_window bronze, got {bronze_refs}"
        )

    def test_dedup_via_row_number_on_club_slug(self):
        sql = _read_sql()
        assert re.search(
            r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(\s*PARTITION\s+BY\s+club_slug",
            sql, re.IGNORECASE,
        ), "must dedup via ROW_NUMBER OVER (PARTITION BY club_slug, league, season)"

    def test_xref_team_join_on_club_name_with_league_season(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_team" in sql, "must enrich via silver.xref_team"
        assert re.search(r"source_id\s*=\s*b\.club_name", sql), (
            "xref_team JOIN must be on club_name"
        )
        assert re.search(r"\.league\s*=\s*b\.league", sql), "JOIN must carry league predicate"
        assert re.search(r"\.season\s*=\s*b\.season", sql), "JOIN must carry season predicate"

    def test_foreign_is_double_quoted(self):
        """`foreign` — зарезервированное слово Trino → без кавычек = parse error."""
        sql = _strip_comments(_read_sql())
        assert '"foreign"' in sql, (
            'reserved word `foreign` must be double-quoted as "foreign"'
        )

    def test_outputs_required_columns(self):
        sql = _read_sql()
        for col in (
            "club_slug",
            "canonical_id",
            "club_name",
            "players",
            "age",
            "income_gbp",
            "expense_gbp",
            "balance_gbp",
            "_bronze_ingested_at",
            "league",
            "season",
        ):
            assert re.search(rf"\b{col}\b", sql), (
                f"capology_transfer_window.sql must project `{col}`"
            )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "must remain a pure SELECT (CTAS done by run_silver_transform)"
        )
