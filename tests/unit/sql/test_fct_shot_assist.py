"""
Unit tests for assist resolution in ``dags/sql/gold/fct_shot.sql`` (#444).

Bug
---
Understat exposes the assister only by NAME (``player_assisted``); it has no
per-shot numeric assist id. soccerdata 1.8.8 nonetheless emits an
``assist_player_id`` built from the roster-ROW id (``player["id"]``, range
414509…793112) instead of the true player id — so ``bronze.understat_shots.
assist_player_id`` never matches ``silver.xref_player.source_id`` (true understat
player ids) and ``gold.fct_shot.assist_player_id`` was 100% NULL.

Fix
---
Resolve the assister by NAME: ``bronze.understat_shots.assist_player`` →
``bronze.understat_players`` (authoritative understat name→player_id) → existing
``xref_player``. The bogus ``assist_player_id`` column is ignored entirely.

Strategy
--------
Same DuckDB-bridge as the sibling SQL tests (e.g.
``test_whoscored_team_match_sql.py``): seed single-namespace fixture tables,
rewrite the ``iceberg.<schema>.`` table refs into DuckDB spelling, execute the
final SELECT and assert on the resolved rows. ``ARBITRARY`` / window functions /
``CAST`` / ``LOWER`` are DuckDB-native.
"""

from __future__ import annotations

import re
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
    """Adapt the Gold SQL for execution on DuckDB.

    Only the namespaced table refs differ from DuckDB spelling; everything the
    statement uses (ARBITRARY, ROW_NUMBER OVER, CAST, LOWER) is DuckDB-native.
    """
    return sql.replace("iceberg.bronze.", "bronze_").replace("iceberg.silver.", "silver_")


# ---------------------------------------------------------------------------
# Fixture-table schemas (only the columns the SELECT touches)
# ---------------------------------------------------------------------------

_TABLES: Dict[str, str] = {
    "bronze_understat_shots": """
        shot_id          BIGINT,
        game_id          BIGINT,
        team             VARCHAR,
        player           VARCHAR,
        player_id        BIGINT,
        assist_player    VARCHAR,
        assist_player_id BIGINT,
        minute           BIGINT,
        location_x       DOUBLE,
        location_y       DOUBLE,
        xg               DOUBLE,
        body_part        VARCHAR,
        situation        VARCHAR,
        result           VARCHAR,
        league           VARCHAR,
        season           VARCHAR,
        _ingested_at     TIMESTAMP
    """,
    "bronze_understat_players": """
        player     VARCHAR,
        player_id  BIGINT,
        league     VARCHAR,
        season     VARCHAR
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
    "silver_xref_player": """
        source       VARCHAR,
        source_id    VARCHAR,
        canonical_id VARCHAR
    """,
    # SofaScore fallback source (#699). Seeded empty here — these tests exercise
    # the Understat assist path, and an empty SofaScore branch lets Understat win
    # every match. Multi-source merge behaviour is in test_fct_shot_multisource.
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
# A single resolvable match: Liverpool vs Bournemouth
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
        "bronze_understat_players": [
            {"player": "Mohamed Salah", "player_id": 11, "league": _LEAGUE, "season": _SEASON},
            {"player": "Cody Gakpo", "player_id": 12, "league": _LEAGUE, "season": _SEASON},
        ],
        "silver_xref_player": [
            {"source": "understat", "source_id": "11", "canonical_id": "fb_salah"},
            {"source": "understat", "source_id": "12", "canonical_id": "fb_gakpo"},
        ],
    }


def _shot(**over: Any) -> Dict[str, Any]:
    row = {
        "shot_id": 1, "game_id": 900, "team": "Liverpool",
        "player": "Mohamed Salah", "player_id": 11,
        "assist_player": None, "assist_player_id": None,
        "minute": 10, "location_x": 0.9, "location_y": 0.5, "xg": 0.3,
        "body_part": "Right Foot", "situation": "Open Play", "result": "Goal",
        "league": _LEAGUE, "season": _SEASON, "_ingested_at": _ING,
    }
    row.update(over)
    return row


def _run(con, shots: List[Dict[str, Any]]) -> Dict[Any, Dict[str, Any]]:
    fx = _base_fixtures()
    fx["bronze_understat_shots"] = shots
    for table in _TABLES:
        _seed(con, table, fx.get(table, []))
    sql = _translate_trino_to_duckdb(_read_sql())
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    out = [dict(zip(cols, r)) for r in cur.fetchall()]
    return {r["shot_id"]: r for r in out}


# ---------------------------------------------------------------------------
# Core: assist resolves by name, ignoring the bogus bronze assist_player_id
# ---------------------------------------------------------------------------


class TestAssistResolvesByName:

    def test_assist_resolved_via_name(self, duck_conn):
        """Salah's goal assisted by 'Cody Gakpo' → assist_player_id = fb_gakpo,
        even though bronze.assist_player_id is a bogus roster id (#444)."""
        rows = _run(duck_conn, [
            _shot(shot_id="1", assist_player="Cody Gakpo", assist_player_id=500001),
        ])
        assert rows["1"]["assist_player_id"] == "fb_gakpo"
        # shooter resolution is unchanged
        assert rows["1"]["player_id"] == "fb_salah"

    def test_no_assist_is_null(self, duck_conn):
        """A shot with no assister stays NULL (assist_player IS NULL)."""
        rows = _run(duck_conn, [
            _shot(shot_id="2", player="Cody Gakpo", player_id=12,
                  assist_player=None, assist_player_id=None),
        ])
        assert rows["2"]["assist_player_id"] is None

    def test_ignores_bronze_assist_player_id(self, duck_conn):
        """Strong repro: bronze.assist_player_id is set to a value that DOES
        exist in xref_player but points at the WRONG player (Salah's id 11).
        Name-based resolution must still return the assister (Gakpo)."""
        rows = _run(duck_conn, [
            _shot(shot_id="3", assist_player="Cody Gakpo", assist_player_id=11),
        ])
        assert rows["3"]["assist_player_id"] == "fb_gakpo"
        assert rows["3"]["assist_player_id"] != "fb_salah"

    def test_unknown_assister_name_is_null(self, duck_conn):
        """An assister not present in understat_players → NULL (orphan-tolerant),
        never a wrong canonical."""
        rows = _run(duck_conn, [
            _shot(shot_id="4", assist_player="Ghost Player", assist_player_id=999999),
        ])
        assert rows["4"]["assist_player_id"] is None

    def test_assist_name_case_insensitive(self, duck_conn):
        """Resolution normalises case (LOWER) — same source, minor case drift."""
        rows = _run(duck_conn, [
            _shot(shot_id="5", assist_player="cody gakpo", assist_player_id=500001),
        ])
        assert rows["5"]["assist_player_id"] == "fb_gakpo"


# ---------------------------------------------------------------------------
# Structural guardrails over the SQL text
# ---------------------------------------------------------------------------


class TestFctShotAssistStructure:

    def test_uses_understat_players_dictionary(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.bronze.understat_players" in sql

    def test_resolves_assist_by_name(self):
        sql = _strip_comments(_read_sql()).lower()
        assert "assist_player" in sql
        # name is lower-cased for the join
        assert re.search(r"lower\s*\(\s*s\.assist_player\s*\)", sql)

    def test_no_longer_joins_xref_on_bronze_assist_id(self):
        """The defective (source_id = bronze assist_player_id) join is gone."""
        sql = _strip_comments(_read_sql())
        assert "understat_assist_player_source_id" not in sql
