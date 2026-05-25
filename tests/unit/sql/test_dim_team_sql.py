"""
Unit tests for ``dags/sql/gold/dim_team.sql`` after E1.5 cutover (2026-05-09).

T2 migrated dim_team from ``gold.entity_xref`` to ``silver.xref_team`` —
this file pins down the structural invariants of that refactor without
spinning up a Trino/DuckDB engine. We use the same pattern as
``test_xref_team_sql.py``: regex/keyword sanity over the raw SQL text.

E1.5 cutover invariants we exercise:
  * Source is iceberg.silver.xref_team (NOT iceberg.gold.entity_xref).
  * source = 'fbref' filter retained (FBref-only subset of dim_team).
  * GROUP BY mirrors the documented PK contract (canonical_id /
    display_name / league / season).
  * Migration breadcrumb in header so future readers know why the
    rewrite happened.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_team.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    """Remove ``-- ...`` lines for assertions about executable SQL only."""
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestDimTeamCutoverStructure:
    """Regex/keyword sanity over ``dim_team.sql`` post-E1.5 cutover."""

    def test_reads_from_silver_xref_team(self):
        """Source must be ``iceberg.silver.xref_team``."""
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_team" in sql, (
            "dim_team.sql must read from iceberg.silver.xref_team after "
            "the E1.5 cutover; got:\n" + sql
        )

    def test_no_legacy_entity_xref_in_executable_sql(self):
        """``iceberg.gold.entity_xref`` must NOT appear outside comments.

        The header may legitimately reference the old table in a
        "Migrated from gold.entity_xref ..." breadcrumb — we strip
        comments before asserting.
        """
        sql = _strip_comments(_read_sql())
        assert "iceberg.gold.entity_xref" not in sql, (
            "dim_team.sql must NOT reference iceberg.gold.entity_xref in "
            "executable SQL after the E1.5 cutover"
        )
        assert "gold.entity_xref" not in sql, (
            "dim_team.sql must NOT reference gold.entity_xref in "
            "executable SQL after the E1.5 cutover"
        )

    def test_fbref_source_filter(self):
        """The FBref-only subset filter must remain — dim_team is
        documented as ``FBref-only subset retained here``."""
        sql = _read_sql()
        assert re.search(r"source\s*=\s*'fbref'", sql, re.IGNORECASE), (
            "dim_team.sql must keep the `source = 'fbref'` predicate"
        )

    def test_select_canonical_columns(self):
        """canonical_id / display_name carry through from xref_team."""
        sql = _read_sql()
        assert re.search(r"canonical_id\s+AS\s+team_id", sql, re.IGNORECASE), (
            "dim_team.sql must SELECT `canonical_id AS team_id`"
        )
        assert re.search(
            r"display_name\s+AS\s+team_name", sql, re.IGNORECASE
        ), "dim_team.sql must SELECT `display_name AS team_name`"

    def test_group_by_pk_columns(self):
        """GROUP BY must include canonical_id / display_name / league / season
        so the PK = (team_id, league, season) contract is enforced."""
        sql = _read_sql()
        # Regex tolerates any whitespace and trailing CAST(... AS bigint)
        # variant on season.
        m = re.search(r"GROUP\s+BY\s+([^\n;]+)", sql, re.IGNORECASE)
        assert m, "dim_team.sql is missing a GROUP BY clause"
        group_by = m.group(1).lower()
        for col in ["canonical_id", "display_name", "league", "season"]:
            assert col in group_by, (
                f"GROUP BY must include {col!r}; got: {group_by!r}"
            )

    def test_season_cast_to_bigint(self):
        """silver.xref_team.season is varchar — dim_team must CAST back to
        bigint to preserve the legacy dim_team.season=bigint contract."""
        sql = _read_sql()
        assert (
            "CAST(season AS bigint)" in sql
            or "CAST(season as bigint)" in sql
            or "CAST(season AS BIGINT)" in sql
        ), (
            "dim_team.sql must CAST(season AS bigint) — silver.xref_team "
            "stores season as varchar"
        )

    def test_migration_breadcrumb_in_header(self):
        """Header must carry the cutover breadcrumb so future readers
        know this file moved off gold.entity_xref in E1.5."""
        sql = _read_sql()
        assert "Migrated from gold.entity_xref to silver.xref_team in E1.5" in sql, (
            "dim_team.sql must keep the E1.5 migration breadcrumb in the "
            "header so the cutover is discoverable"
        )

    def test_pure_select_no_create_table(self):
        """File must remain a pure SELECT (gold_tasks wraps in CTAS)."""
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper(), (
            "dim_team.sql must remain a pure SELECT — gold_tasks "
            "wraps it in CREATE TABLE AS at run time"
        )
