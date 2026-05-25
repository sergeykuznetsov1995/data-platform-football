"""
Unit tests for ``dags/sql/gold/dim_player.sql`` after E1.5 cutover (2026-05-09).

T2 aligned dim_player with the silver.xref_player canonical convention:
FBref-source players carry ``canonical_id = 'fb_' || raw player_id``.
Rather than introducing a JOIN (which would be redundant — the canonical
id is a deterministic function of source + raw id), the prefix is
applied inline: ``'fb_' || player_id AS player_id``.

This file pins down that contract so a future refactor can't silently
drop the prefix.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_player.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestDimPlayerCutoverStructure:
    """Regex sanity over ``dim_player.sql`` post-E1.5 cutover."""

    def test_fb_prefix_applied_inline(self):
        """``'fb_' || player_id AS player_id`` (or CONCAT equivalent)."""
        sql = _read_sql()
        # Trino accepts both `||` and CONCAT(...) — we accept either.
        pipes = re.search(
            r"'fb_'\s*\|\|\s*player_id\s+AS\s+player_id",
            sql, re.IGNORECASE,
        )
        concat = re.search(
            r"CONCAT\s*\(\s*'fb_'\s*,\s*player_id\s*\)\s+AS\s+player_id",
            sql, re.IGNORECASE,
        )
        assert pipes or concat, (
            "dim_player.sql must apply the silver.xref_player FBref "
            "canonical-id prefix inline: `'fb_' || player_id AS "
            "player_id` (or CONCAT equivalent)"
        )

    def test_no_legacy_entity_xref_reference(self):
        """gold.entity_xref must not appear in executable SQL."""
        sql = _strip_comments(_read_sql())
        assert "gold.entity_xref" not in sql, (
            "dim_player.sql must NOT reference gold.entity_xref"
        )

    def test_pk_contract_group_by_includes_player_id(self):
        """PK = (player_id, season). GROUP BY must include player_id
        so the prefixed id remains unique per season."""
        sql = _read_sql()
        m = re.search(r"GROUP\s+BY\s+([^\n;]+)", sql, re.IGNORECASE)
        assert m, "dim_player.sql is missing a GROUP BY clause"
        group_by = m.group(1).lower()
        # The natural-key player_id is what's grouped — the 'fb_' prefix
        # is applied in the SELECT projection only. We assert the bare
        # player_id is in the GROUP BY because grouping by the prefixed
        # alias would error in Trino (alias not visible to GROUP BY).
        assert "player_id" in group_by, (
            "dim_player.sql GROUP BY must include `player_id` to honour "
            f"the (player_id, season) PK contract; got: {group_by!r}"
        )
        assert "season" in group_by, (
            "dim_player.sql GROUP BY must include `season`; got: "
            f"{group_by!r}"
        )

    def test_reads_from_silver_player_season_profile(self):
        """Source is iceberg.silver.fbref_player_season_profile (FBref-only
        dimension; cross-source enrichment is deferred)."""
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.fbref_player_season_profile" in sql, (
            "dim_player.sql must read iceberg.silver.fbref_player_season_profile"
        )

    def test_migration_breadcrumb_in_header(self):
        """The E1.5 cutover note must be discoverable in the header."""
        sql = _read_sql()
        # dim_player's breadcrumb mentions `silver.xref_player` rather than
        # `silver.xref_team` (the canonical convention not the JOIN target).
        assert "E1.5" in sql and "silver.xref_player" in sql, (
            "dim_player.sql must keep an E1.5 breadcrumb pointing to "
            "silver.xref_player canonical convention"
        )

    def test_filters_null_player_id_and_season(self):
        """Existing WHERE clause must still filter out NULL player_id /
        season (otherwise 'fb_' || NULL → NULL violates the PK)."""
        sql = _read_sql()
        assert re.search(
            r"player_id\s+IS\s+NOT\s+NULL", sql, re.IGNORECASE
        ), (
            "dim_player.sql must keep `player_id IS NOT NULL` filter — "
            "'fb_' || NULL => NULL would corrupt the PK"
        )
        assert re.search(
            r"season\s+IS\s+NOT\s+NULL", sql, re.IGNORECASE
        ), "dim_player.sql must keep `season IS NOT NULL` filter"

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "dim_player.sql must remain a pure SELECT"
        )
