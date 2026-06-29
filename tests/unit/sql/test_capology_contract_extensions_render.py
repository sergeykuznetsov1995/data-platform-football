"""Render-smoke for ``dags/sql/silver/capology_contract_extensions.sql``.

Issue #603: promote write-only bronze.capology_contract_extensions to Silver
(player contract snapshot). Фиксирует source, dedup (последний контракт по
signed), xref_player JOIN, дату-CAST и pure SELECT.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "capology_contract_extensions.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestCapologyContractExtensionsSql:

    def test_reads_bronze_contract_extensions(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.bronze.capology_contract_extensions" in sql, (
            "must read bronze.capology_contract_extensions"
        )

    def test_single_source_no_other_bronze(self):
        sql = _strip_comments(_read_sql())
        bronze_refs = set(re.findall(r"iceberg\.bronze\.(\w+)", sql))
        assert bronze_refs == {"capology_contract_extensions"}, (
            f"expected only capology_contract_extensions bronze, got {bronze_refs}"
        )

    def test_dedup_via_row_number_on_player_slug(self):
        """9 живых дублей (player_slug, league, season) — две подписи за сезон;
        снимок берёт последний контракт (ORDER BY signed DESC)."""
        sql = _read_sql()
        assert re.search(
            r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(\s*PARTITION\s+BY\s+player_slug",
            sql, re.IGNORECASE,
        ), "must dedup via ROW_NUMBER OVER (PARTITION BY player_slug, league, season)"
        assert re.search(r"ORDER\s+BY[^)]*signed", sql, re.IGNORECASE), (
            "dedup must ORDER BY signed (latest contract wins)"
        )

    def test_xref_player_join_on_player_slug_with_league_season(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_player" in sql, "must enrich via silver.xref_player"
        assert re.search(r"source_id\s*=\s*b\.player_slug", sql), (
            "xref_player JOIN must be on player_slug"
        )
        assert re.search(r"\.league\s*=\s*b\.league", sql), "JOIN must carry league predicate"
        assert re.search(r"\.season\s*=\s*b\.season", sql), "JOIN must carry season predicate"

    def test_dates_cast_to_date(self):
        """signed/expiration — varchar ISO в Bronze; conform к DATE через TRY(CAST)."""
        sql = _strip_comments(_read_sql())
        assert re.search(r"CAST\s*\(\s*b\.signed\s+AS\s+DATE\s*\)", sql, re.IGNORECASE), (
            "signed must be CAST AS DATE"
        )
        assert re.search(r"CAST\s*\(\s*b\.expiration\s+AS\s+DATE\s*\)", sql, re.IGNORECASE), (
            "expiration must be CAST AS DATE"
        )

    def test_outputs_required_columns(self):
        sql = _read_sql()
        for col in (
            "player_slug",
            "canonical_id",
            "player_name",
            "signed",
            "expiration",
            "years",
            "annual_gross_gbp",
            "contract_total_gross_gbp",
            "contract_total_net_gbp",
            "_bronze_ingested_at",
            "league",
            "season",
        ):
            assert re.search(rf"\b{col}\b", sql), (
                f"capology_contract_extensions.sql must project `{col}`"
            )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "must remain a pure SELECT (CTAS done by run_silver_transform)"
        )
