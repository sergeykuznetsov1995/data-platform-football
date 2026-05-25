"""
Unit tests for ``dags/sql/silver/whoscored_player_season_aggregate.sql`` (B1).

Pure regex / keyword sanity — no Trino engine. Verifies the SPADL action
enum coverage, mandatory ``(league, season)`` JOIN predicate against
``silver.xref_player`` (CLAUDE.md fan-out rule), and the GROUP BY
contract that defines the table's PK.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = (
    PROJECT_ROOT / "dags" / "sql" / "silver" / "whoscored_player_season_aggregate.sql"
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


def test_reads_canonical_silver_sources():
    """Reads silver.whoscored_events_spadl + silver.xref_player only."""
    body = _strip_comments(_read_sql())
    assert "iceberg.silver.whoscored_events_spadl" in body
    assert "iceberg.silver.xref_player" in body
    # No bronze reads — silver-only consumer
    assert "iceberg.bronze." not in body, (
        "Aggregate must consume Silver SPADL, not Bronze events"
    )


def test_xref_join_has_league_season_predicate():
    """CLAUDE.md hard rule: every xref JOIN MUST include (league, season)
    predicate or you get 1.5–4× fan-out from per-(source, season) rows."""
    body = _strip_comments(_read_sql())
    # The xp CTE filters to source='whoscored' and the join clause must
    # carry league + season equality.
    assert re.search(r"xp\.league\s*=\s*e\.league", body), (
        "xref_player JOIN missing league predicate — fan-out risk"
    )
    assert re.search(r"xp\.season\s*=\s*e\.season", body), (
        "xref_player JOIN missing season predicate — fan-out risk"
    )


def test_xref_filtered_to_whoscored_source():
    body = _strip_comments(_read_sql())
    assert re.search(r"WHERE\s+source\s*=\s*'whoscored'", body), (
        "xp CTE must filter xref_player to source='whoscored'"
    )


def test_pk_group_by_canonical_league_season():
    """Table PK = (canonical_id, league, season) — enforced by GROUP BY."""
    body = _strip_comments(_read_sql())
    gb = re.search(r"GROUP\s+BY\s+([^\n;]+)", body, flags=re.IGNORECASE)
    assert gb, "missing GROUP BY clause"
    cols = {c.strip() for c in gb.group(1).split(",")}
    assert "canonical_id" in cols
    assert "league" in cols
    assert "season" in cols


@pytest.mark.parametrize("action", [
    # Core actions B1 reports as columns; SPADL spec freeze.
    "pass", "take_on", "dribble", "bad_touch", "shot",
    "tackle", "interception", "ball_recovery", "clearance", "foul",
    "keeper_save", "keeper_pick_up", "keeper_claim",
])
def test_spadl_action_referenced(action):
    """Every metric column must be backed by a literal action_canonical
    predicate — protects against silent typos that turn a counter into 0."""
    body = _strip_comments(_read_sql())
    assert f"'{action}'" in body, (
        f"SPADL action '{action}' not referenced in COUNT_IF predicates"
    )


def test_spatial_avg_excludes_defensive_actions():
    """avg_x/avg_y must come from on-ball offensive actions only — defensive
    recoveries pull a winger's average back to his own half."""
    body = _strip_comments(_read_sql())
    # The CASE WHEN must include only attacking on-ball actions.
    assert re.search(
        r"action_canonical\s+IN\s*\(\s*'pass'\s*,\s*'take_on'\s*,\s*'shot'\s*,\s*'dribble'\s*\)\s+THEN\s+x",
        body,
    ), "avg_x CASE filter not restricted to (pass, take_on, shot, dribble)"


def test_touches_in_box_uses_spadl_box_polygon():
    """SPADL coords: opposition box ≈ x>=83 AND y∈[21,79]. Catches the
    classic mistake of writing y BETWEEN 30 AND 70."""
    body = _strip_comments(_read_sql())
    assert "x >= 83" in body, "touches_in_box: x>=83 threshold missing"
    assert "y BETWEEN 21 AND 79" in body, (
        "touches_in_box: y BETWEEN 21 AND 79 threshold missing"
    )


def test_partition_columns_are_last():
    """The CTAS layer (run_silver_transform) injects
    ``WITH (partitioning = ARRAY['league','season'])`` — by Iceberg
    convention partition keys must appear as the trailing columns of
    the SELECT list."""
    body = _strip_comments(_read_sql())
    # Find the final SELECT (top-level GROUP BY query).
    # Just ensure the last two emitted columns are league + season.
    # We grep for the literal block `league,\n    season` which the file uses.
    assert re.search(r"\bleague,\s*\n\s*season\s*\n", body), (
        "partition columns league/season must be emitted as last columns"
    )
