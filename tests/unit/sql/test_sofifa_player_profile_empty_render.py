"""Render-smoke for ``dags/sql/silver/sofifa_player_profile_empty.sql``.

issue #180: when ``bronze.sofifa_player_ratings`` is absent (SoFIFA ingest
frozen by Cloudflare Turnstile), ``dag_transform_sofifa_silver`` builds this
empty-but-typed fallback so ``silver.sofifa_player_profile`` stays materialized
and ``gold.dim_player_attributes``' ``LEFT JOIN sofifa_latest`` keeps resolving.

The fallback's output column list MUST stay identical (names + order) to the
real ``sofifa_player_profile.sql`` SELECT — otherwise the silver table schema
drifts between the two paths and downstream consumers break. These tests guard
against that drift.
"""

from __future__ import annotations

from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
REAL_SQL = PROJECT_ROOT / "dags" / "sql" / "silver" / "sofifa_player_profile.sql"
EMPTY_SQL = PROJECT_ROOT / "dags" / "sql" / "silver" / "sofifa_player_profile_empty.sql"


pytestmark = pytest.mark.unit


def _strip_comments(sql: str) -> str:
    out = []
    for line in sql.splitlines():
        # Drop full-line and inline `--` comments.
        code = line.split("--", 1)[0]
        out.append(code)
    return "\n".join(out)


def _final_select_columns(sql: str) -> list[str]:
    """Ordered output column names of the final top-level SELECT.

    Finds the last bare ``SELECT`` line (column 0) and reads projection lines
    up to the next ``FROM`` (column 0). Each column name = alias after the last
    `` AS `` if present, else the token after the last dot (``j.player_id`` ->
    ``player_id``), else the bare token.
    """
    lines = _strip_comments(sql).splitlines()
    select_idx = max(
        i for i, ln in enumerate(lines) if ln.strip() == "SELECT"
    )
    cols: list[str] = []
    for ln in lines[select_idx + 1:]:
        stripped = ln.strip()
        if not stripped:
            continue
        if stripped.upper().startswith("FROM"):
            break
        token = stripped.rstrip(",").strip()
        if " AS " in token:
            name = token.rsplit(" AS ", 1)[-1].strip()
        elif "." in token:
            name = token.rsplit(".", 1)[-1].strip()
        else:
            name = token
        cols.append(name)
    return cols


class TestSofifaPlayerProfileEmptyRender:

    def test_column_list_matches_real_sql_exactly(self):
        """Empty fallback projects the same columns in the same order as the
        real SQL (``_silver_created_at`` is appended by the CTAS wrapper, so it
        is absent from both)."""
        real_cols = _final_select_columns(REAL_SQL.read_text(encoding="utf-8"))
        empty_cols = _final_select_columns(EMPTY_SQL.read_text(encoding="utf-8"))
        assert empty_cols == real_cols, (
            "sofifa_player_profile_empty.sql column list/order must match "
            f"sofifa_player_profile.sql.\n  real : {real_cols}\n  empty: {empty_cols}"
        )

    def test_ends_with_partition_keys(self):
        """Partition keys (league, season) must be the last two columns to
        match the writer's partitioning=ARRAY['league','season']."""
        empty_cols = _final_select_columns(EMPTY_SQL.read_text(encoding="utf-8"))
        assert empty_cols[-2:] == ["league", "season"], (
            f"empty fallback must end with league, season; got {empty_cols[-2:]}"
        )

    def test_no_silver_created_at(self):
        """`_silver_created_at` is added by the CTAS wrapper — must NOT be in
        the SQL body (else DUPLICATE_COLUMN_NAME)."""
        empty_cols = _final_select_columns(EMPTY_SQL.read_text(encoding="utf-8"))
        assert "_silver_created_at" not in empty_cols

    def test_spine_is_existing_table_and_empty(self):
        """Spine must be an always-present table filtered to 0 rows."""
        sql = _strip_comments(EMPTY_SQL.read_text(encoding="utf-8"))
        assert "iceberg.silver.xref_player" in sql, (
            "empty fallback spine must be silver.xref_player (always exists)"
        )
        assert "WHERE 1 = 0" in sql, (
            "empty fallback must filter to 0 rows (WHERE 1 = 0)"
        )

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(EMPTY_SQL.read_text(encoding="utf-8"))
        assert "CREATE TABLE" not in sql.upper(), (
            "empty fallback must remain a pure SELECT (CTAS wrapping is done by "
            "silver_tasks.run_silver_transform)"
        )
