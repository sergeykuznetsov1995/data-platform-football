"""
Unit tests for ``dags/sql/gold/dim_player.sql`` — star-schema grain (#425).

dim_player is one row per PLAYER (no season in the grain): spine =
silver.xref_player (source='fbref', canonical 'fb_<id>'), attributes
COALESCE'd from FotMob / SofaScore / Transfermarkt / SoFIFA.

These are regex sanity checks over the SQL text — the executable contract
is exercised by the integration smoke + validate_gold_quality (PK
uniqueness on player_id).
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


class TestDimPlayerStarStructure:
    """Regex sanity over ``dim_player.sql`` post-#425 redesign."""

    def test_spine_is_xref_player_fbref(self):
        """The row spine comes from silver.xref_player (FBref, non-orphan)."""
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_player" in sql
        assert re.search(r"source\s*=\s*'fbref'", sql)
        assert re.search(r"confidence\s*<>\s*'orphan'", sql)

    def test_pk_is_canonical_id_no_season(self):
        """PK player_id = xref canonical; season is NOT a grain component."""
        sql = _strip_comments(_read_sql())
        assert re.search(r"canonical_id\s+AS\s+player_id", sql, re.IGNORECASE)
        # season must not appear in the final SELECT output — it is only a
        # MAX_BY ordering key inside the per-source CTEs.
        final = sql[sql.rindex("FROM xref_fbref"):]
        assert "season" not in final.lower(), (
            "season must not leak into the final dim_player projection"
        )

    def test_design_columns_present(self):
        """All 6 design attribute columns are emitted."""
        sql = _strip_comments(_read_sql())
        for col in ("player_name", "dob", "nationality", "height_cm",
                    "preferred_foot", "primary_position"):
            assert re.search(rf"AS\s+{col}\b", sql, re.IGNORECASE), (
                f"dim_player.sql must emit {col!r}"
            )

    def test_multi_source_enrichment(self):
        """All four enrichment sources are joined."""
        sql = _strip_comments(_read_sql())
        for src in ("fotmob_player_profile", "sofascore_player_profile",
                    "transfermarkt_players", "sofifa_player_profile"):
            assert f"iceberg.silver.{src}" in sql, (
                f"dim_player.sql must enrich from silver.{src}"
            )

    def test_height_priority_transfermarkt_first(self):
        """TM is the primary height source (official club profile)."""
        sql = _strip_comments(_read_sql())
        m = re.search(
            r"COALESCE\(\s*tm\.height_cm\s*,", sql, re.IGNORECASE
        )
        assert m, "height_cm COALESCE must start with Transfermarkt (tm.)"

    def test_no_legacy_entity_xref_reference(self):
        """gold.entity_xref must not appear in executable SQL."""
        sql = _strip_comments(_read_sql())
        assert "gold.entity_xref" not in sql, (
            "dim_player.sql must NOT reference gold.entity_xref"
        )

    def test_pure_select_no_create_table(self):
        """File stays a pure SELECT — CTAS wrapping is the runner's job."""
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper()
        assert "INSERT INTO" not in sql.upper()

    def test_fbref_profile_dedup_cte(self):
        """#463: silver profile grain = (player_id, squad, league, season) —
        MAX_BY(x, season) над multi-squad сезоном недетерминирован (pos может
        отличаться между клубами). fbref_latest must read a pre-deduped CTE
        (max-minutes club, tiebreaker squad)."""
        sql = _strip_comments(_read_sql())
        assert re.search(r"\bfbref_profile_dedup\s+AS\s*\(", sql, re.IGNORECASE), (
            "missing fbref_profile_dedup CTE — MAX_BY over multi-squad season "
            "is nondeterministic (#463)"
        )
        assert re.search(
            r"PARTITION\s+BY\s+player_id\s*,\s*season\s+"
            r"ORDER\s+BY\s+minutes\s+DESC\s+NULLS\s+LAST\s*,\s*squad",
            sql, re.IGNORECASE,
        ), "fbref_profile_dedup must pick max-minutes club per (player, season)"
        assert re.search(r"FROM\s+fbref_profile_dedup", sql, re.IGNORECASE), (
            "fbref_latest must read fbref_profile_dedup, not the raw silver table"
        )
