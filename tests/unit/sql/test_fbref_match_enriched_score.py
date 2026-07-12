"""
Unit tests for score parsing in ``silver.fbref_match_enriched`` (#898).

FBref's ``score`` column carries the OFFICIAL result:

  * an awarded match shows the forfeit score ('0–3') and explains it in
    ``notes`` ("Match awarded to Pescara") — the on-pitch result (2–1) only
    survives in ``bronze.fbref_match_events``;
  * a shoot-out renders as '(4) 1–1 (5)' — the penalty counts must be stripped
    before the 90-minute score is split, otherwise the leading '(5)' of
    '(5) 0–3 (6)' is read as the home score.

We execute ``dags/sql/silver/fbref_match_enriched.sql`` against an in-memory
DuckDB after a small translation pass.

CAVEAT: DuckDB's ``regexp_replace`` replaces only the FIRST match unless the
'g' flag is passed; Trino's replaces all. ``_translate`` adds the flag, and
``test_translation_patched_regexp_replace`` fails loudly if the SQL is
reformatted so the substitution silently stops matching.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "fbref_match_enriched.sql"
OVERRIDES_PATH = PROJECT_ROOT / "configs" / "medallion" / "match_result_overrides.yaml"

pytestmark = pytest.mark.unit

EN_DASH = chr(8211)


# ---------------------------------------------------------------------------
# Trino → DuckDB translation
# ---------------------------------------------------------------------------

_ICEBERG_TO_LOCAL = {
    "iceberg.bronze.fbref_schedule":         "bronze_fbref_schedule",
    "iceberg.bronze.fbref_match_team_stats": "bronze_fbref_match_team_stats",
    "iceberg.bronze.fbref_match_events":     "bronze_fbref_match_events",
    "iceberg.bronze.fbref_lineups":          "bronze_fbref_lineups",
    "iceberg.bronze.fbref_dataset_availability":
        "bronze_fbref_dataset_availability",
}

# Trino replaces every occurrence; DuckDB needs the explicit 'g' flag.
_TRINO_REGEXP_REPLACE = r"REGEXP_REPLACE(sch.score, '\(\d+\)\s*', '')"
_DUCK_REGEXP_REPLACE = r"REGEXP_REPLACE(sch.score, '\(\d+\)\s*', '', 'g')"


def _translate(sql: str) -> str:
    for k, v in _ICEBERG_TO_LOCAL.items():
        sql = sql.replace(k, v)
    sql = sql.replace(_TRINO_REGEXP_REPLACE, _DUCK_REGEXP_REPLACE)
    sql = sql.replace("REGEXP_LIKE(", "REGEXP_MATCHES(")
    return sql


def _render_overrides(sql: str) -> str:
    payload = yaml.safe_load(OVERRIDES_PATH.read_text(encoding="utf-8"))
    rows = []

    def quote(value: Any) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    for item in payload["overrides"]:
        rows.append(
            "(" + ", ".join([
                quote(item["match_id"]), quote(item["league"]), quote(item["season"]),
                str(item["official_home_score"]), str(item["official_away_score"]),
                quote(item["authority"]), quote(item["reference"]), quote(item["reason"]),
            ]) + ")"
        )
    typed_empty = (
        "(CAST(NULL AS varchar), CAST(NULL AS varchar), CAST(NULL AS varchar),\n"
        "         CAST(NULL AS integer), CAST(NULL AS integer), CAST(NULL AS varchar),\n"
        "         CAST(NULL AS varchar), CAST(NULL AS varchar))"
    )
    assert sql.count(typed_empty) == 1
    return sql.replace(typed_empty, ",\n        ".join(rows))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def duck_conn():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    # Trino built-ins DuckDB lacks. XXHASH64 only has to be deterministic here —
    # the synthetic 'fut_' id is not under test.
    con.execute("CREATE MACRO to_utf8(x)  AS (x)")
    con.execute("CREATE MACRO xxhash64(x) AS (hash(x))")
    con.execute("CREATE MACRO to_hex(x)   AS (printf('%x', x))")
    yield con
    con.close()


@pytest.fixture(autouse=True)
def _reset_schemas(duck_conn):
    for tbl in _ICEBERG_TO_LOCAL.values():
        duck_conn.execute(f"DROP TABLE IF EXISTS {tbl}")

    duck_conn.execute(
        """
        CREATE TABLE bronze_fbref_schedule (
            wk           VARCHAR,
            round        VARCHAR,  -- #913 Phase 3: WC stage carrier (NULL for clubs)
            day          VARCHAR,
            date         VARCHAR,
            time         VARCHAR,
            home         VARCHAR,
            score        VARCHAR,
            away         VARCHAR,
            attendance   VARCHAR,
            venue        VARCHAR,
            referee      VARCHAR,
            notes        VARCHAR,
            match_url    VARCHAR,
            league       VARCHAR,
            season       BIGINT,
            source_season_id VARCHAR,
            _ingested_at TIMESTAMP,
            _batch_id    VARCHAR
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE bronze_fbref_dataset_availability (
            match_id VARCHAR, dataset VARCHAR, availability VARCHAR,
            reason VARCHAR, _ingested_at TIMESTAMP, _batch_id VARCHAR
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE bronze_fbref_match_team_stats (
            match_id          VARCHAR,
            home_possession   VARCHAR,
            away_possession   VARCHAR,
            home_shots        VARCHAR,
            away_shots        VARCHAR,
            home_sot          VARCHAR,
            away_sot          VARCHAR,
            home_saves        VARCHAR,
            away_saves        VARCHAR,
            home_yellow_cards VARCHAR,
            away_yellow_cards VARCHAR,
            home_red_cards    VARCHAR,
            away_red_cards    VARCHAR,
            _ingested_at      TIMESTAMP,
            _batch_id         VARCHAR
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE bronze_fbref_match_events (
            match_id     VARCHAR,
            minute       VARCHAR,
            event_type   VARCHAR,
            player       VARCHAR,
            player_id    VARCHAR,
            team_side    VARCHAR,
            _ingested_at TIMESTAMP,
            _batch_id    VARCHAR
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE bronze_fbref_lineups (
            match_id     VARCHAR,
            player       VARCHAR,
            player_id    VARCHAR,
            team         VARCHAR,
            is_starter   BOOLEAN,
            _ingested_at TIMESTAMP,
            _batch_id    VARCHAR
        )
        """
    )


# match_id is derived from match_url; keep the real Sassuolo–Pescara slug.
SCHEDULE_ROWS = [
    # (wk, date, home, score, away, notes, match_url)
    ("1.0", "2016-08-27", "Roma", f"3{EN_DASH}4", "Sassuolo", None,
     "/en/matches/aaaaaaaa/Roma-Sassuolo-August-27-2016-Serie-A"),
    ("2.0", "2016-08-28", "Sassuolo", f"0{EN_DASH}3", "Pescara", "Match awarded to Pescara",
     "/en/matches/ed6efcb0/Sassuolo-Pescara-August-28-2016-Serie-A"),
    ("3.0", "2016-09-01", "Düsseldorf", f"(5) 0{EN_DASH}3 (6)", "Bochum", None,
     "/en/matches/bbbbbbbb/Dusseldorf-Bochum-September-1-2016-Serie-A"),
    ("4.0", "2016-09-02", "Saint-Étienne", f"(4) 1{EN_DASH}1 (5)", "Auxerre", None,
     "/en/matches/cccccccc/Saint-Etienne-Auxerre-September-2-2016-Serie-A"),
    ("5.0", "2016-09-03", "Genoa", None, "Crotone", "Match Cancelled",
     "/en/matches/dddddddd/Genoa-Crotone-September-3-2016-Serie-A"),
]

# Sassuolo won 2–1 on the pitch before the result was overturned.
EVENT_ROWS = [
    ("ed6efcb0", "38", "goal", "Grégoire Defrel", "p1", "home"),
    ("ed6efcb0", "67", "goal", "Domenico Berardi", "p2", "home"),
    ("ed6efcb0", "81", "goal", "Rey Manaj", "p3", "away"),
    # Düsseldorf-Bochum: three on-field away goals plus shoot-out attempts.
    # Empty-minute penalties must not inflate the additive on_field_* score.
    ("bbbbbbbb", "18", "goal", "Philipp Hofmann", "p4", "away"),
    ("bbbbbbbb", "66", "goal", "Philipp Hofmann", "p4", "away"),
    ("bbbbbbbb", "70", "penalty", "Kevin Stöger", "p5", "away"),
    ("bbbbbbbb", "", "penalty", "Shootout Home", "p6", "home"),
    ("bbbbbbbb", "", "penalty", "Shootout Away", "p7", "away"),
]


def _seed(duck_conn) -> None:
    duck_conn.executemany(
        """
        INSERT INTO bronze_fbref_schedule
            (wk, day, date, time, home, score, away, attendance, venue, referee,
             notes, match_url, league, season, _ingested_at, _batch_id)
        VALUES (?, 'Sun', ?, '20:45', ?, ?, ?, '9071.0', 'Stadio', 'Tagliavento',
                ?, ?, 'ITA-Serie A', 2016, TIMESTAMP '2026-07-07 15:55:08', 'b1')
        """,
        SCHEDULE_ROWS,
    )
    duck_conn.executemany(
        """
        INSERT INTO bronze_fbref_match_events
            (match_id, minute, event_type, player, player_id, team_side,
             _ingested_at, _batch_id)
        VALUES (?, ?, ?, ?, ?, ?, TIMESTAMP '2026-07-07 15:55:08', 'b1')
        """,
        EVENT_ROWS,
    )


def _run_silver(duck_conn) -> Dict[str, Dict[str, Any]]:
    sql = _translate(_render_overrides(SILVER_PATH.read_text(encoding="utf-8")))
    cur = duck_conn.execute(sql)
    cols = [d[0] for d in cur.description]
    rows: List[Dict[str, Any]] = [dict(zip(cols, r)) for r in cur.fetchall()]
    return {r["match_id"]: r for r in rows}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_translation_patched_regexp_replace() -> None:
    """The 'g'-flag patch must still apply — otherwise DuckDB silently keeps
    the trailing shoot-out count and every assertion below becomes vacuous."""
    raw = SILVER_PATH.read_text(encoding="utf-8")
    assert raw.count(_TRINO_REGEXP_REPLACE) == 6, "score parser was reformatted"
    assert _TRINO_REGEXP_REPLACE not in _translate(raw)


class TestScoreParsing:

    def test_source_single_year_season_is_preserved(self, duck_conn):
        _seed(duck_conn)
        duck_conn.execute(
            "UPDATE bronze_fbref_schedule SET source_season_id = '2016' "
            "WHERE match_url LIKE '%aaaaaaaa%'"
        )
        assert _run_silver(duck_conn)["aaaaaaaa"]["season"] == "2016"

    def test_plain_score(self, duck_conn):
        _seed(duck_conn)
        row = _run_silver(duck_conn)["aaaaaaaa"]
        assert (row["home_score"], row["away_score"]) == (3, 4)
        assert (row["source_home_score"], row["source_away_score"]) == (3, 4)
        assert (row["official_home_score"], row["official_away_score"]) == (3, 4)
        assert row["official_score_provenance"] == "fbref_schedule"
        assert row["result_status"] == "played"
        assert row["is_awarded"] is False
        assert row["notes"] is None

    def test_shootout_counts_are_stripped(self, duck_conn):
        """#898: '(5) 0–3 (6)' used to parse as 5–0 — a flipped result."""
        _seed(duck_conn)
        out = _run_silver(duck_conn)
        assert (out["bbbbbbbb"]["home_score"], out["bbbbbbbb"]["away_score"]) == (0, 3)
        assert (out["cccccccc"]["home_score"], out["cccccccc"]["away_score"]) == (1, 1)
        assert (out["bbbbbbbb"]["home_shootout_score"],
                out["bbbbbbbb"]["away_shootout_score"]) == (5, 6)
        assert (out["cccccccc"]["home_shootout_score"],
                out["cccccccc"]["away_shootout_score"]) == (4, 5)
        assert (out["bbbbbbbb"]["on_field_home_score"],
                out["bbbbbbbb"]["on_field_away_score"]) == (0, 3)
        # Legacy counters stay byte-compatible and therefore still include the
        # empty-minute shootout attempts; new consumers use on_field_*.
        assert (out["bbbbbbbb"]["home_goals_events"],
                out["bbbbbbbb"]["away_goals_events"]) == (1, 4)

    def test_awarded_match_keeps_official_score_and_is_flagged(self, duck_conn):
        """Sassuolo won 2–1 on the pitch; the official result is 0–3."""
        _seed(duck_conn)
        row = _run_silver(duck_conn)["ed6efcb0"]
        assert (row["home_score"], row["away_score"]) == (0, 3)
        assert row["is_awarded"] is True
        assert row["notes"] == "Match awarded to Pescara"
        assert (row["source_home_score"], row["source_away_score"]) == (0, 3)
        assert (row["official_home_score"], row["official_away_score"]) == (0, 3)
        assert row["official_score_provenance"] == "medallion_override"
        assert row["official_score_authority"] == "Lega Serie A, Giudice Sportivo"
        assert row["official_score_reference"].endswith("/cu24.pdf")
        assert row["result_status"] == "awarded"
        # the on-pitch result stays reachable via the event counters
        assert (row["home_goals_events"], row["away_goals_events"]) == (2, 1)
        assert (row["on_field_home_score"], row["on_field_away_score"]) == (2, 1)

    def test_cancelled_match_has_no_parsed_score(self, duck_conn):
        _seed(duck_conn)
        row = _run_silver(duck_conn)["dddddddd"]
        assert row["home_score"] is None and row["away_score"] is None
        assert row["is_awarded"] is False
        assert row["notes"] == "Match Cancelled"
        assert row["result_status"] == "cancelled"

    def test_score_roundtrips_to_the_bronze_string(self, duck_conn):
        """Mirrors the DQ gate score_roundtrip[silver.fbref_match_enriched]."""
        _seed(duck_conn)
        for row in _run_silver(duck_conn).values():
            if not row["score"]:
                continue
            stripped = re.sub(r"\(\d+\)\s*", "", row["score"]).strip()
            assert stripped == f"{row['home_score']}{EN_DASH}{row['away_score']}"
