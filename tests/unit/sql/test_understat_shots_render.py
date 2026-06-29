"""
Unit tests for ``dags/sql/silver/understat_shots.sql`` (#704).

The Understat shot conform + canonical resolution used to live inside
``gold.fct_shot`` (which read ``bronze.understat_shots`` +
``bronze.understat_players`` directly). #704 lifted it into Silver so Gold reads
``silver.understat_shots`` one-hop. These tests cover what moved:

  * enum normalisation (body_part / situation / result / is_goal),
  * re-scrape dedup (latest ``_ingested_at`` per shot_id),
  * team xref (orphan-EXCLUDED) and player xref (orphan-tolerant),
  * assist-by-NAME resolution (#444 — the assister has no per-shot numeric id),
  * the mandatory (league, season) xref predicate (anti-fan-out).

Strategy
--------
Same DuckDB bridge as the sibling SQL tests: seed single-namespace fixture
tables, rewrite ``iceberg.<schema>.`` refs into DuckDB spelling, execute the
SELECT and assert on the rows. ROW_NUMBER / CAST / LOWER / CASE are all
DuckDB-native.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "understat_shots.sql"


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
        player       VARCHAR,
        player_id    BIGINT,
        league       VARCHAR,
        season       VARCHAR,
        _ingested_at TIMESTAMP
    """,
    "silver_xref_team": """
        source       VARCHAR,
        source_id    VARCHAR,
        league       VARCHAR,
        season       VARCHAR,
        canonical_id VARCHAR,
        confidence   VARCHAR
    """,
    "silver_xref_player": """
        source       VARCHAR,
        source_id    VARCHAR,
        league       VARCHAR,
        season       VARCHAR,
        canonical_id VARCHAR
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
# Base fixtures: Liverpool match; Salah (id 11) + Gakpo (id 12) resolvable.
# ---------------------------------------------------------------------------

_LEAGUE = "ENG-Premier League"
_SEASON = "2526"
_ING = "2025-09-01 00:00:00"


def _base_fixtures() -> Dict[str, List[Dict[str, Any]]]:
    return {
        "silver_xref_team": [
            {"source": "understat", "source_id": "Liverpool", "league": _LEAGUE,
             "season": _SEASON, "canonical_id": "t_liv", "confidence": "exact"},
        ],
        "bronze_understat_players": [
            {"player": "Mohamed Salah", "player_id": 11, "league": _LEAGUE,
             "season": _SEASON, "_ingested_at": _ING},
            {"player": "Cody Gakpo", "player_id": 12, "league": _LEAGUE,
             "season": _SEASON, "_ingested_at": _ING},
        ],
        "silver_xref_player": [
            {"source": "understat", "source_id": "11", "league": _LEAGUE,
             "season": _SEASON, "canonical_id": "fb_salah"},
            {"source": "understat", "source_id": "12", "league": _LEAGUE,
             "season": _SEASON, "canonical_id": "fb_gakpo"},
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


def _run(con, shots: List[Dict[str, Any]],
         overrides: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> Dict[Any, Dict[str, Any]]:
    fx = _base_fixtures()
    fx.update(overrides or {})
    fx["bronze_understat_shots"] = shots
    for table in _TABLES:
        _seed(con, table, fx.get(table, []))
    cur = con.execute(_translate_trino_to_duckdb(_read_sql()))
    cols = [d[0] for d in cur.description]
    out = [dict(zip(cols, r)) for r in cur.fetchall()]
    return {r["shot_id"]: r for r in out}


# ---------------------------------------------------------------------------
# Conform: enum normalisation
# ---------------------------------------------------------------------------


class TestEnumNormalisation:

    @pytest.mark.parametrize("raw,expected", [
        ("Right Foot", "foot"), ("Left Foot", "foot"), ("Head", "head"),
        ("Other Body Part", "other"), (None, None),
    ])
    def test_body_part(self, duck_conn, raw, expected):
        rows = _run(duck_conn, [_shot(shot_id="1", body_part=raw)])
        assert rows["1"]["body_part"] == expected

    @pytest.mark.parametrize("raw,expected", [
        ("Open Play", "open_play"), ("From Corner", "corner"),
        ("Direct Freekick", "free_kick"), ("Set Piece", "set_piece"),
        ("Penalty", "penalty"), ("Mystery", None),
    ])
    def test_situation(self, duck_conn, raw, expected):
        rows = _run(duck_conn, [_shot(shot_id="1", situation=raw)])
        assert rows["1"]["situation"] == expected

    @pytest.mark.parametrize("raw,expected,is_goal", [
        ("Goal", "goal", True), ("Saved Shot", "saved", False),
        ("Blocked Shot", "blocked", False), ("Missed Shot", "off_target", False),
        ("Shot On Post", "post", False), ("Own Goal", "own_goal", True),
    ])
    def test_result_and_is_goal(self, duck_conn, raw, expected, is_goal):
        rows = _run(duck_conn, [_shot(shot_id="1", result=raw)])
        assert rows["1"]["result"] == expected
        assert rows["1"]["is_goal"] is is_goal

    def test_shot_source_literal(self, duck_conn):
        rows = _run(duck_conn, [_shot(shot_id="1")])
        assert rows["1"]["shot_source"] == "understat_v1"

    def test_understat_game_id_carried(self, duck_conn):
        """Gold needs understat_game_id raw for its match bridge."""
        rows = _run(duck_conn, [_shot(shot_id="1", game_id=900)])
        assert rows["1"]["understat_game_id"] == 900


# ---------------------------------------------------------------------------
# Dedup: re-scrape protection
# ---------------------------------------------------------------------------


class TestDedup:

    def test_latest_ingested_wins(self, duck_conn):
        rows = _run(duck_conn, [
            _shot(shot_id="1", xg=0.1, _ingested_at="2025-09-01 00:00:00"),
            _shot(shot_id="1", xg=0.9, _ingested_at="2025-09-02 00:00:00"),
        ])
        assert len(rows) == 1
        assert rows["1"]["xg"] == pytest.approx(0.9)


# ---------------------------------------------------------------------------
# xref resolution
# ---------------------------------------------------------------------------


class TestXrefResolution:

    def test_team_resolved(self, duck_conn):
        rows = _run(duck_conn, [_shot(shot_id="1")])
        assert rows["1"]["team_id"] == "t_liv"

    def test_team_orphan_excluded(self, duck_conn):
        """An orphan xref_team row ('us_<slug>') must NOT leak as team_id (#506)."""
        rows = _run(duck_conn, [_shot(shot_id="1", team="Orphan FC")], overrides={
            "silver_xref_team": [
                {"source": "understat", "source_id": "Orphan FC", "league": _LEAGUE,
                 "season": _SEASON, "canonical_id": "us_orphan_fc", "confidence": "orphan"},
            ],
        })
        assert rows["1"]["team_id"] is None

    def test_player_resolved(self, duck_conn):
        rows = _run(duck_conn, [_shot(shot_id="1", player_id=11)])
        assert rows["1"]["player_id"] == "fb_salah"

    def test_player_unresolved_is_null(self, duck_conn):
        """A player absent from xref_player → NULL (orphan-tolerant LEFT JOIN)."""
        rows = _run(duck_conn, [_shot(shot_id="1", player_id=999)])
        assert rows["1"]["player_id"] is None

    def test_season_predicate_prevents_fanout(self, duck_conn):
        """xref rows for a DIFFERENT season must not match or duplicate (the
        mandatory (league, season) predicate). Player 11 has two xref rows."""
        rows = _run(duck_conn, [_shot(shot_id="1", player_id=11, season=_SEASON)], overrides={
            "silver_xref_player": [
                {"source": "understat", "source_id": "11", "league": _LEAGUE,
                 "season": _SEASON, "canonical_id": "fb_salah"},
                {"source": "understat", "source_id": "11", "league": _LEAGUE,
                 "season": "2425", "canonical_id": "fb_salah_old"},
            ],
        })
        assert len(rows) == 1
        assert rows["1"]["player_id"] == "fb_salah"


# ---------------------------------------------------------------------------
# Assist-by-name resolution (#444) — moved here from the old fct_shot test
# ---------------------------------------------------------------------------


class TestAssistResolvesByName:

    def test_assist_resolved_via_name(self, duck_conn):
        rows = _run(duck_conn, [
            _shot(shot_id="1", assist_player="Cody Gakpo", assist_player_id=500001),
        ])
        assert rows["1"]["assist_player_id"] == "fb_gakpo"
        assert rows["1"]["player_id"] == "fb_salah"

    def test_no_assist_is_null(self, duck_conn):
        rows = _run(duck_conn, [_shot(shot_id="2", assist_player=None)])
        assert rows["2"]["assist_player_id"] is None

    def test_ignores_bronze_assist_player_id(self, duck_conn):
        """bronze.assist_player_id is never read; name resolution returns Gakpo
        even when the bogus id points at Salah (#444)."""
        rows = _run(duck_conn, [
            _shot(shot_id="3", assist_player="Cody Gakpo", assist_player_id=11),
        ])
        assert rows["3"]["assist_player_id"] == "fb_gakpo"

    def test_unknown_assister_name_is_null(self, duck_conn):
        rows = _run(duck_conn, [
            _shot(shot_id="4", assist_player="Ghost Player", assist_player_id=999999),
        ])
        assert rows["4"]["assist_player_id"] is None

    def test_assist_name_case_insensitive(self, duck_conn):
        rows = _run(duck_conn, [
            _shot(shot_id="5", assist_player="cody gakpo"),
        ])
        assert rows["5"]["assist_player_id"] == "fb_gakpo"


# ---------------------------------------------------------------------------
# Structural guardrails over the SQL text (Silver charter)
# ---------------------------------------------------------------------------


class TestUnderstatShotsStructure:

    def test_reads_bronze_understat_sources(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.bronze.understat_shots" in sql
        assert "iceberg.bronze.understat_players" in sql

    def test_every_xref_join_carries_season(self):
        """Charter R3 / fan-out footgun: each xref_* join must carry (league,
        season). Assert league+season predicate per xref alias used in a join."""
        sql = _strip_comments(_read_sql()).lower()
        # three xref aliases joined: xt (team), xp (player), xa (assist)
        for alias in ("xt", "xp", "xa"):
            assert re.search(rf"{alias}\.league\s*=", sql), f"{alias} missing league predicate"
            assert re.search(rf"{alias}\.season\s*=", sql), f"{alias} missing season predicate"

    def test_team_join_excludes_orphans(self):
        sql = _strip_comments(_read_sql())
        assert "xt.confidence <> 'orphan'" in sql

    def test_pure_select_no_ddl(self):
        sql = _strip_comments(_read_sql()).lower()
        assert "create table" not in sql
        assert "insert into" not in sql

    def test_partition_keys_last(self):
        """league then season must be the final output columns (charter §4)."""
        sql = _strip_comments(_read_sql()).lower()
        assert sql.rfind("sn.league") < sql.rfind("sn.season")
