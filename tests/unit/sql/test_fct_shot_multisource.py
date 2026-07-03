"""
Unit tests for the SofaScore match-level fallback in ``dags/sql/gold/fct_shot.sql`` (#699).

Design
------
fct_shot is Understat-primary + SofaScore-fallback, merged at the MATCH level:
the two feeds share no shot key (Understat shot_id != SofaScore shot_id for the
same physical shot), so per-shot COALESCE is impossible. Instead each canonical
match keeps ALL shots from exactly ONE source — the lowest source_priority
present (Understat=1, SofaScore=2). SofaScore only fills matches Understat is
missing or failed to bridge.

silver.sofascore_shots is already canonicalised to fct_shot's match/team/player
IDs and enum domains (#602), so the SofaScore branch reads it directly.

Strategy
--------
Same DuckDB bridge as ``test_fct_shot_assist.py``: seed single-namespace fixture
tables, rewrite ``iceberg.<schema>.`` refs into DuckDB spelling, execute the
final SELECT, assert on the merged rows. ``MIN``/``GROUP BY``/``UNION ALL``/
``NOT LIKE`` are all DuckDB-native.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_shot.sql"


pytestmark = pytest.mark.unit


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )


def _translate_trino_to_duckdb(sql: str) -> str:
    return sql.replace("iceberg.bronze.", "bronze_").replace("iceberg.silver.", "silver_")


# ---------------------------------------------------------------------------
# Fixture-table schemas (only the columns the SELECT touches)
# ---------------------------------------------------------------------------

_TABLES: Dict[str, str] = {
    # #704: the Understat shot conform now lives in silver.understat_shots — the
    # fct_shot Understat branch reads it (already canonicalised), no longer
    # bronze.understat_shots / bronze.understat_players.
    "silver_understat_shots": """
        shot_id             VARCHAR,
        understat_game_id   BIGINT,
        team_id             VARCHAR,
        player_id           VARCHAR,
        assist_player_id    VARCHAR,
        minute              INTEGER,
        x                   DOUBLE,
        y                   DOUBLE,
        body_part           VARCHAR,
        situation           VARCHAR,
        xg                  DOUBLE,
        result              VARCHAR,
        is_goal             BOOLEAN,
        shot_source         VARCHAR,
        _bronze_ingested_at TIMESTAMP,
        league              VARCHAR,
        season              VARCHAR
    """,
    "bronze_understat_schedule": """
        game_id      BIGINT,
        date         TIMESTAMP,
        home_team    VARCHAR,
        away_team    VARCHAR,
        league       VARCHAR,
        season       VARCHAR,
        _ingested_at TIMESTAMP
    """,
    "silver_fbref_match_enriched": """
        match_id VARCHAR,
        date     DATE,
        home     VARCHAR,
        away     VARCHAR
    """,
    "silver_xref_team": """
        source       VARCHAR,
        source_id    VARCHAR,
        league       VARCHAR,
        canonical_id VARCHAR,
        confidence   VARCHAR
    """,
    # SofaScore fallback source — already canonicalised to fct_shot IDs (#602).
    # Only the columns the SofaScore branch of fct_shot.sql reads.
    "silver_sofascore_shots": """
        shot_id     VARCHAR,
        match_id    VARCHAR,
        team_id     VARCHAR,
        player_id   VARCHAR,
        minute      INTEGER,
        x           DOUBLE,
        y           DOUBLE,
        body_part   VARCHAR,
        situation   VARCHAR,
        xg          DOUBLE,
        xgot        DOUBLE,
        result      VARCHAR,
        is_goal     BOOLEAN,
        is_sot      BOOLEAN,
        shot_source VARCHAR,
        league      VARCHAR,
        season      VARCHAR
    """,
}


@pytest.fixture(scope="session")
def duck_conn():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    yield con
    con.close()


def _seed(con, table: str, rows: List[Dict[str, Any]]) -> None:
    cols = [c.strip().split()[0] for c in _TABLES[table].strip().split(",") if c.strip()]
    con.execute(f"DROP TABLE IF EXISTS {table}")
    con.execute(f"CREATE TABLE {table} ({_TABLES[table]})")
    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
    for row in rows:
        con.execute(insert_sql, [row.get(c) for c in cols])


# ---------------------------------------------------------------------------
# Base fixtures: one bridgeable Understat match (Liverpool vs Bournemouth,
# understat game_id 900 -> fbref match_id 'm_livbou').
# ---------------------------------------------------------------------------

_LEAGUE = "ENG-Premier League"
_SEASON = "2526"
_ING = "2025-09-01 00:00:00"


def _base_fixtures() -> Dict[str, List[Dict[str, Any]]]:
    return {
        "bronze_understat_schedule": [
            {"game_id": 900, "date": "2025-08-15 19:00:00", "home_team": "Liverpool",
             "away_team": "Bournemouth", "league": _LEAGUE, "season": _SEASON,
             "_ingested_at": _ING},
        ],
        "silver_fbref_match_enriched": [
            {"match_id": "m_livbou", "date": "2025-08-15", "home": "Liverpool",
             "away": "Bournemouth"},
        ],
        "silver_xref_team": [
            {"source": "understat", "source_id": "Liverpool", "league": _LEAGUE,
             "canonical_id": "t_liv", "confidence": "exact"},
            {"source": "understat", "source_id": "Bournemouth", "league": _LEAGUE,
             "canonical_id": "t_bou", "confidence": "exact"},
            {"source": "fbref", "source_id": "Liverpool", "league": _LEAGUE,
             "canonical_id": "t_liv", "confidence": "exact"},
            {"source": "fbref", "source_id": "Bournemouth", "league": _LEAGUE,
             "canonical_id": "t_bou", "confidence": "exact"},
        ],
    }


def _us_shot(**over: Any) -> Dict[str, Any]:
    """An Understat shot in the bridgeable Liverpool–Bournemouth match, as a
    silver.understat_shots row (already conformed/canonicalised, #704)."""
    row = {
        "shot_id": "1", "understat_game_id": 900,
        "team_id": "t_liv", "player_id": "fb_salah", "assist_player_id": None,
        "minute": 10, "x": 0.9, "y": 0.5,
        "body_part": "foot", "situation": "open_play",
        "xg": 0.3, "result": "goal", "is_goal": True,
        "shot_source": "understat_v1", "_bronze_ingested_at": _ING,
        "league": _LEAGUE, "season": _SEASON,
    }
    row.update(over)
    return row


def _ss_shot(**over: Any) -> Dict[str, Any]:
    """A SofaScore shot, already canonicalised (defaults onto m_livbou)."""
    row = {
        "shot_id": "s1", "match_id": "m_livbou", "team_id": "t_liv",
        "player_id": "fb_salah", "minute": 20, "x": 0.88, "y": 0.52,
        "body_part": "foot", "situation": "open_play", "xg": 0.25, "xgot": 0.4,
        "result": "goal", "is_goal": True, "shot_source": "sofascore_v1",
        "league": _LEAGUE, "season": _SEASON,
    }
    row.update(over)
    return row


def _run(con, understat_shots: List[Dict[str, Any]],
         sofascore_shots: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    fx = _base_fixtures()
    fx["silver_understat_shots"] = understat_shots
    fx["silver_sofascore_shots"] = sofascore_shots
    for table in _TABLES:
        _seed(con, table, fx.get(table, []))
    cur = con.execute(_translate_trino_to_duckdb(_read_sql()))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Match-level fallback behaviour
# ---------------------------------------------------------------------------


class TestMatchLevelFallback:

    def test_both_sources_understat_wins(self, duck_conn):
        """A match covered by BOTH feeds keeps only Understat shots (priority 1)."""
        rows = _run(
            duck_conn,
            understat_shots=[_us_shot(shot_id=1), _us_shot(shot_id=2, minute=30)],
            sofascore_shots=[_ss_shot(shot_id="s1"), _ss_shot(shot_id="s2"),
                             _ss_shot(shot_id="s3")],
        )
        livbou = [r for r in rows if r["match_id"] == "m_livbou"]
        assert len(livbou) == 2
        assert {r["shot_source"] for r in livbou} == {"understat_v1"}

    def test_sofascore_only_match_filled(self, duck_conn):
        """A match Understat does not cover is filled from SofaScore: shot_source
        'sofascore_v1', assist_player_id NULL, psxg sourced from xgot."""
        rows = _run(
            duck_conn,
            understat_shots=[_us_shot(shot_id=1)],            # m_livbou only
            sofascore_shots=[
                _ss_shot(shot_id="s9", match_id="m_arsche", xgot=0.37),
                _ss_shot(shot_id="s10", match_id="m_arsche", xgot=0.05,
                         result="saved", is_goal=False),
            ],
        )
        arsche = [r for r in rows if r["match_id"] == "m_arsche"]
        assert len(arsche) == 2
        assert {r["shot_source"] for r in arsche} == {"sofascore_v1"}
        assert all(r["assist_player_id"] is None for r in arsche)
        s9 = next(r for r in arsche if r["shot_id"] == "s9")
        assert s9["psxg"] == pytest.approx(0.37)

    def test_sofascore_orphan_match_excluded(self, duck_conn):
        """Non-fbref-spine match_ids ('sofascore_<raw>' and 'ss_<id>') are dropped."""
        rows = _run(
            duck_conn,
            understat_shots=[],
            sofascore_shots=[
                _ss_shot(shot_id="o1", match_id="sofascore_555"),
                _ss_shot(shot_id="o2", match_id="ss_777"),
            ],
        )
        assert rows == []

    def test_no_double_count(self, duck_conn):
        """Every match resolves to exactly one shot_source — no cross-source mix."""
        rows = _run(
            duck_conn,
            understat_shots=[_us_shot(shot_id=1), _us_shot(shot_id=2, minute=30)],
            sofascore_shots=[_ss_shot(shot_id="s1"),                       # m_livbou
                             _ss_shot(shot_id="s2", match_id="m_arsche")],  # ss-only
        )
        by_match: Dict[str, set] = {}
        for r in rows:
            by_match.setdefault(r["match_id"], set()).add(r["shot_source"])
        assert by_match  # non-empty
        assert all(len(sources) == 1 for sources in by_match.values())

    def test_sofascore_null_xg_dropped(self, duck_conn):
        """SofaScore shots with NULL xg are dropped (fct_shot.xg is NOT NULL)."""
        rows = _run(
            duck_conn,
            understat_shots=[],
            sofascore_shots=[
                _ss_shot(shot_id="n1", match_id="m_xnull", xg=None),
                _ss_shot(shot_id="n2", match_id="m_xnull", xg=0.2),
            ],
        )
        xnull = [r for r in rows if r["match_id"] == "m_xnull"]
        assert [r["shot_id"] for r in xnull] == ["n2"]

    def test_psxg_provenance(self, duck_conn):
        """psxg is NULL for Understat rows, carries xgot for SofaScore rows."""
        rows = _run(
            duck_conn,
            understat_shots=[_us_shot(shot_id=1)],
            sofascore_shots=[_ss_shot(shot_id="s5", match_id="m_arsche", xgot=0.6)],
        )
        us = next(r for r in rows if r["shot_source"] == "understat_v1")
        ss = next(r for r in rows if r["shot_source"] == "sofascore_v1")
        assert us["psxg"] is None
        assert ss["psxg"] == pytest.approx(0.6)


# ---------------------------------------------------------------------------
# Structural guardrails over the SQL text
# ---------------------------------------------------------------------------


class TestFctShotMultiSourceStructure:

    def test_reads_sofascore_silver(self):
        assert "iceberg.silver.sofascore_shots" in _strip_comments(_read_sql())

    def test_reads_understat_silver_not_bronze(self):
        """#704 one-hop: the Understat branch reads silver.understat_shots; the
        direct bronze.understat_shots / bronze.understat_players reads are gone.
        bronze.understat_schedule (the *_schedule match bridge) is kept."""
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.understat_shots" in sql
        assert "iceberg.bronze.understat_shots" not in sql
        assert "iceberg.bronze.understat_players" not in sql
        assert "iceberg.bronze.understat_schedule" in sql

    def test_has_match_winner_cte(self):
        sql = _strip_comments(_read_sql()).lower()
        assert "match_winner" in sql
        assert "min(source_priority)" in sql

    def test_drops_orphan_sofascore_matches(self):
        sql = _strip_comments(_read_sql())
        assert "NOT LIKE 'sofascore_%'" in sql
        assert "NOT LIKE 'ss_%'" in sql
