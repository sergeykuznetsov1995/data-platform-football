"""Structural smoke for ``dags/sql/silver/fotmob_player_match_aggregate.sql``.

Issue #691: silver player×match aggregate parsed from
``player_stats_json``; #930 cutover: source is native
``bronze.fotmob_match_payloads_current`` (+ ``fotmob_matches_current`` for team
names, ``league_map`` for the legacy 14-league scope) instead of legacy
``bronze.fotmob_match_details``. Full column-parity mirror of
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

    def test_reads_native_payloads_with_matches_join(self):
        """#930: stats come from native match_payloads_current; team names live
        only in matches_current (payloads carry ids only) → both must be read;
        legacy bronze.fotmob_match_details must be gone."""
        sql = _strip_comments(_read(SQL_PATH))
        assert "iceberg.bronze.fotmob_match_payloads_current" in sql
        assert "iceberg.bronze.fotmob_matches_current" in sql
        assert "player_stats_json" in sql
        assert "bronze.fotmob_match_details" not in sql

    def test_league_reconstructed_via_league_map(self):
        """Native has no `league` string — it is rebuilt from competition_id
        via the static league_map CTE; the INNER JOIN scopes output to the
        same 14 legacy leagues. payloads.competition_id is varchar vs
        matches.competition_id bigint → explicit CAST required."""
        sql = _strip_comments(_read(SQL_PATH))
        assert re.search(r"league_map\s*\(\s*competition_id\s*,\s*league\s*\)", sql), (
            "expected static league_map CTE (competition_id → legacy league string)"
        )
        assert "'ENG-Premier League'" in sql
        assert re.search(
            r"CAST\s*\(\s*p\.competition_id\s+AS\s+bigint\s*\)", sql, re.I
        ), "payloads.competition_id (varchar) must be CAST to bigint for the matches JOIN"

    def test_grain_columns_projected(self):
        """PK grain (match_id, player_id, league, season) must be projected."""
        sql = _read(SQL_PATH)
        for col in ("match_id", "player_id", "league", "season"):
            assert re.search(rf"\b{col}\b", sql), f"grain column `{col}` missing"

    def test_dedup_row_number_on_match(self):
        """Defensive dedup on (match_id, league, season). `*_current` already
        yields one row per match, but the guard stays; native has no _batch_id
        → tiebreak must be _observed_at + _target_batch_id."""
        sql = _read(SQL_PATH)
        assert re.search(r"ROW_NUMBER\s*\(\s*\)\s*OVER", sql, re.I), (
            "expected defensive ROW_NUMBER dedup"
        )
        assert re.search(
            r"PARTITION\s+BY\s+match_id\s*,\s*league\s*,\s*season", sql, re.I
        )
        assert re.search(
            r"ORDER\s+BY\s+_observed_at\s+DESC\s*,\s*_target_batch_id\s+DESC",
            sql,
            re.I,
        ), "dedup tiebreak must use _target_batch_id (native has no _batch_id)"

    def test_two_level_json_flatten(self):
        """player_stats_json is flattened: map_entries over players + UNNEST of
        the stat-group array + map_entries over each group's stats."""
        sql = _read(SQL_PATH)
        assert sql.count("map_entries") >= 2, "expected player + stat map_entries"
        assert "json_extract" in sql
        assert "'$.stat.value'" in sql, "stat value path must be $.stat.value"

    def test_season_emitted_as_slug(self):
        """Native season = varchar source_season_key ('2025/2026' / '2026') →
        year-start via substr(key, 1, 4), then emit slug '2526' via
        LPAD(MOD(...)) (matches xref_player + other Silver tables). The slug
        must NOT be derived from the key's shape (single-year AFCON keys would
        diverge from legacy)."""
        sql = _read(SQL_PATH)
        assert re.search(
            r"substr\s*\(\s*p\.source_season_key\s*,\s*1\s*,\s*4\s*\)", sql, re.I
        ), "season year-start must come from substr(source_season_key, 1, 4)"
        assert re.search(r"MOD\s*\(\s*season", sql, re.I), (
            "season must be converted year-start → slug via MOD(season,100)"
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
