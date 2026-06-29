"""Structural smoke for ``dags/sql/silver/fotmob_player_match_aggregate.sql``.

Issue #691: silver player×match aggregate parsed from
``bronze.fotmob_match_details.player_stats_json``. Full column-parity mirror of
``sofascore_player_match_aggregate`` so the 5-source Gold ``fct_player_match``
COALESCE compares identically-named columns.

JSON-heavy Trino parsing (``map_entries`` / two-level ``UNNEST`` /
``json_extract``) is impractical to run in DuckDB, so this is a structural smoke
(grain, dedup, season-slug idiom, parity columns, pure SELECT). End-to-end
parsing is covered by the container EXPLAIN VALIDATE + live materialisation in
the issue verification.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = (
    PROJECT_ROOT / "dags" / "sql" / "silver" / "fotmob_player_match_aggregate.sql"
)
SS_PATH = (
    PROJECT_ROOT / "dags" / "sql" / "silver" / "sofascore_player_match_aggregate.sql"
)

pytestmark = pytest.mark.unit


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )


def _output_aliases(sql: str) -> set:
    """Collect ``... AS <alias>`` names (lowercased) from the SQL."""
    return {m.lower() for m in re.findall(r"\bAS\s+([a-z_][a-z0-9_]*)", sql, re.I)}


class TestFotmobPlayerMatchAggregateSql:

    def test_reads_bronze_match_details(self):
        sql = _strip_comments(_read(SQL_PATH))
        assert "iceberg.bronze.fotmob_match_details" in sql
        assert "player_stats_json" in sql

    def test_grain_columns_projected(self):
        """PK grain (match_id, player_id, league, season) must be projected."""
        sql = _read(SQL_PATH)
        for col in ("match_id", "player_id", "league", "season"):
            assert re.search(rf"\b{col}\b", sql), f"grain column `{col}` missing"

    def test_dedup_row_number_on_match(self):
        """Defensive bronze dedup on (match_id, league, season)."""
        sql = _read(SQL_PATH)
        assert re.search(r"ROW_NUMBER\s*\(\s*\)\s*OVER", sql, re.I), (
            "expected ROW_NUMBER dedup of bronze rows"
        )
        assert re.search(
            r"PARTITION\s+BY\s+match_id\s*,\s*league\s*,\s*season", sql, re.I
        )

    def test_two_level_json_flatten(self):
        """player_stats_json is flattened: map_entries over players + UNNEST of
        the stat-group array + map_entries over each group's stats."""
        sql = _read(SQL_PATH)
        assert sql.count("map_entries") >= 2, "expected player + stat map_entries"
        assert "json_extract" in sql
        assert "'$.stat.value'" in sql, "stat value path must be $.stat.value"

    def test_season_emitted_as_slug(self):
        """Bronze season = bigint year-start (2025) → emit slug '2526' via
        LPAD(MOD(...)) (matches xref_player + other Silver tables)."""
        sql = _read(SQL_PATH)
        assert re.search(r"MOD\s*\(\s*season", sql, re.I), (
            "season must be converted bigint year-start → slug via MOD(season,100)"
        )

    def test_modeled_xg_xa_zero_filled(self):
        """xG/xA are additive (no shots = 0), 0-filled via COALESCE so the
        table is dense like Understat. rating is NOT 0-filled (unrated ≠ 0)."""
        sql = _read(SQL_PATH)
        assert re.search(
            r"COALESCE\(\s*MAX\(IF\(stat_name = 'Expected goals \(xG\)'", sql
        ), "xg must be 0-filled"
        assert re.search(
            r"COALESCE\(\s*MAX\(IF\(stat_name = 'Expected assists \(xA\)'", sql
        ), "xa must be 0-filled"

    def test_column_parity_with_sofascore(self):
        """Full parity: every SofaScore aggregate output column exists here
        (same name), so Gold COALESCE compares like-named columns. FotMob may
        leave some NULL, but the schema must match."""
        fm_cols = _output_aliases(_read(SQL_PATH))
        ss_cols = _output_aliases(_read(SS_PATH))
        # league / season are projected verbatim (no AS alias) in both files.
        missing = {c for c in (ss_cols - fm_cols) if c not in ("league", "season")}
        assert not missing, f"FotMob silver missing SofaScore parity cols: {missing}"

    def test_pure_select_no_create_table(self):
        """CTAS wrap is done by run_silver_transform — file is a pure SELECT."""
        sql = _strip_comments(_read(SQL_PATH)).upper()
        assert "CREATE TABLE" not in sql
        assert "INSERT INTO" not in sql
        assert not re.search(r"WITH\s*\(\s*PARTITIONING\s*=", sql)
