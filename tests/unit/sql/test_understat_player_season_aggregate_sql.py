"""
Unit tests for ``dags/sql/silver/understat_player_season_aggregate.sql``.

Pure regex / keyword sanity — no Trino engine. Verifies the (canonical_id,
league, season) grain, mandatory ``(league, season)`` JOIN predicate against
``silver.xref_player`` (CLAUDE.md fan-out rule), and projection of HARD_FACT
+ UNIQUE_UNDERSTAT columns consumed by Gold layer.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = (
    PROJECT_ROOT / "dags" / "sql" / "silver" / "understat_player_season_aggregate.sql"
)


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


def test_sql_file_exists():
    assert SQL_PATH.exists(), f"missing SQL file: {SQL_PATH}"


def test_reads_bronze_understat_and_xref_player():
    """Reads bronze.understat_players (season-grain) + silver.xref_player."""
    body = _strip_comments(_read_sql())
    assert "iceberg.bronze.understat_players" in body
    assert "iceberg.silver.xref_player" in body
    # No other silver sources — this aggregate is a thin bridge
    assert "fbref_player_season_profile" not in body
    assert "fotmob_player_season_profile" not in body
    assert "whoscored_player_season_aggregate" not in body


def test_xref_filtered_to_understat_source():
    body = _strip_comments(_read_sql())
    assert re.search(r"WHERE\s+source\s*=\s*'understat'", body), (
        "xp CTE must filter xref_player to source='understat'"
    )


def test_xref_excludes_orphan_confidence():
    """Orphan-confidence understat rows cannot bridge to FBref-spine in Gold
    layer anyway — exclude at Silver to avoid downstream noise."""
    body = _strip_comments(_read_sql())
    assert re.search(r"confidence\s*<>\s*'orphan'", body, re.IGNORECASE) or \
           re.search(r"confidence\s*!=\s*'orphan'", body, re.IGNORECASE), (
        "xp CTE must exclude confidence='orphan' rows"
    )


def test_xref_join_has_league_season_predicate():
    """CLAUDE.md hard rule: every xref JOIN MUST include (league, season)
    predicate or you get 1.5–4× fan-out from per-(source, season) rows."""
    body = _strip_comments(_read_sql())
    assert re.search(r"b\.league\s*=\s*xp\.league", body), (
        "xref_player JOIN missing league predicate — fan-out risk"
    )
    assert re.search(r"b\.season\s*=\s*xp\.season", body), (
        "xref_player JOIN missing season predicate — fan-out risk"
    )


def test_source_id_cast_to_varchar():
    """bronze.understat_players.player_id is BIGINT but xref_player.source_id
    is varchar — JOIN must explicitly CAST or it silently returns 0 rows."""
    body = _strip_comments(_read_sql())
    assert re.search(
        r"CAST\s*\(\s*b\.player_id\s+AS\s+varchar\s*\)\s*=\s*xp\.source_id",
        body, re.IGNORECASE,
    ), "player_id JOIN must CAST bronze BIGINT to varchar for xref"


def test_hard_fact_columns_projected():
    """HARD_FACT columns Gold layer COALESCEs in fct_player_season_stats.sql."""
    body = _strip_comments(_read_sql())
    for col in ['games_played', 'minutes_played', 'goals', 'assists',
                'yellow_cards', 'red_cards']:
        assert re.search(rf"\bAS\s+{col}\b", body, re.IGNORECASE), (
            f"HARD_FACT column `{col}` must be projected"
        )


def test_unique_understat_columns_projected():
    """xG / xA / build-up — Understat-exclusive."""
    body = _strip_comments(_read_sql())
    for col in ['expected_goals', 'expected_assists', 'non_penalty_goals',
                'non_penalty_xg', 'xg_chain', 'xg_buildup',
                'key_passes', 'shots']:
        assert re.search(rf"\bAS\s+{col}\b", body, re.IGNORECASE), (
            f"UNIQUE_UNDERSTAT column `{col}` must be projected"
        )


def test_grain_emits_canonical_id_and_partition_keys():
    """Output grain = (canonical_id, league, season). league/season as last
    columns for Iceberg partitioning convention."""
    body = _strip_comments(_read_sql())
    assert "canonical_id" in body
    assert re.search(r"\bleague,\s*\n\s*b?\.?season\s*\n", body), (
        "partition columns league/season must be emitted as last columns"
    )


def test_pure_select_no_create_table():
    body = _strip_comments(_read_sql())
    assert "CREATE TABLE" not in body.upper(), (
        "SQL must remain pure SELECT — CTAS wrapping is done by run_silver_transform"
    )
