"""
Unit tests for ``dags/sql/gold/feat_team_event_style.sql`` (E6 / W2b).

Strategy
--------
DuckDB-bridge: load the SQL file, replace ``iceberg.<schema>.<table>``
with single-namespace local tables, seed deterministic per-team event
streams, execute, then assert window mask + share semantics.

The empty-fallback parity test is pure-Python regex inspection of the
SELECT projection.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "feat_team_event_style.sql"
EMPTY_SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "feat_team_event_style_empty.sql"

_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Trino → DuckDB translation
# ---------------------------------------------------------------------------

_ICEBERG_TO_LOCAL = {
    "iceberg.bronze.whoscored_schedule": "bronze_whoscored_schedule",
    "iceberg.gold.fct_event":          "gold_fct_event",
    "iceberg.gold.fct_shot":           "gold_fct_shot",
    "iceberg.gold.fct_team_match":     "gold_fct_team_match",
    "iceberg.gold.dim_match":          "gold_dim_match",
}


def _translate(sql: str) -> str:
    for k, v in _ICEBERG_TO_LOCAL.items():
        sql = sql.replace(k, v)
    return sql


def _read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# DuckDB schema seed
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def duck_conn():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    yield con
    con.close()


@pytest.fixture(autouse=True)
def _reset_schemas(duck_conn):
    for tbl in (
        "bronze_whoscored_schedule",
        "gold_fct_event", "gold_fct_shot",
        "gold_fct_team_match", "gold_dim_match",
    ):
        duck_conn.execute(f"DROP TABLE IF EXISTS {tbl}")

    duck_conn.execute(
        """
        CREATE TABLE bronze_whoscored_schedule (
            game_id     BIGINT,
            start_time  VARCHAR,
            league      VARCHAR,
            season      VARCHAR
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE gold_fct_event (
            match_id_canonical VARCHAR,
            team_id_canonical  VARCHAR,
            league             VARCHAR,
            season             VARCHAR,
            action_canonical   VARCHAR,
            outcome_success    BOOLEAN
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE gold_fct_shot (
            match_id_canonical    VARCHAR,
            team_id_canonical     VARCHAR,
            situation_canonical   VARCHAR,
            body_part_canonical   VARCHAR
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE gold_fct_team_match (
            match_id   VARCHAR,
            team_id    VARCHAR,
            date       DATE,
            season     VARCHAR,
            league     VARCHAR
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE gold_dim_match (
            match_id      VARCHAR,
            date          DATE,
            league        VARCHAR,
            season        BIGINT,
            home_team_id  VARCHAR,
            away_team_id  VARCHAR
        )
        """
    )
    yield


# ---------------------------------------------------------------------------
# Fixture builder — produce a clean sample for one team across N matches
# ---------------------------------------------------------------------------

def _seed_team_matches(
    con,
    team_id: str,
    n_matches: int,
    *,
    season: str = "2425",
    league: str = "ENG-Premier League",
    base_match: int = 1,
    base_date: str = "2024-08-01",
    pass_per_match: int = 50,
    other_per_match: int = 50,   # action_canonical='dribble' for variety
    unknown_per_match: int = 0,  # action_canonical='unknown' (filtered out)
    success_rate: float = 0.5,   # fraction of pass+dribble events successful
    shots_per_match: int = 10,
    set_piece_per_match: int = 3,
    open_play_per_match: int = 7,
    header_per_match: int = 2,
) -> List[str]:
    """Insert n_matches games for ``team_id``; returns ws_game_id list.

    Each match produces:
      - 1 row in dim_match (the team is "home" with a fake away_team)
      - 1 row in fct_team_match
      - bronze.whoscored_schedule: 1 marker row with start_time = match_date
      - fct_event: pass + dribble + unknown (per counts)
      - fct_shot: shots split by situation/body_part
    """
    import datetime as dt
    base = dt.date.fromisoformat(base_date)
    # Mirror the SQL season-normalisation CASE: compact '2425' → bigint 2024.
    # dim_match.season is stored as the bigint year-of-start (see SQL bridge).
    season_year = (
        int(season)
        if (len(season) == 4 and 2000 <= int(season) <= 2100)
        else 2000 + int(season[:2])
    )
    ws_ids: List[str] = []
    away = team_id + "_opp"
    for i in range(n_matches):
        gid = base_match + i
        ws_id = str(gid)
        ws_ids.append(ws_id)
        d = (base + dt.timedelta(days=i * 7)).isoformat()
        match_id = f"m{gid:05d}_{team_id}"

        con.execute(
            f"INSERT INTO gold_dim_match VALUES "
            f"('{match_id}', DATE '{d}', '{league}', {season_year}, "
            f"'{team_id}', '{away}')"
        )
        con.execute(
            f"INSERT INTO gold_fct_team_match VALUES "
            f"('{match_id}', '{team_id}', DATE '{d}', '{season}', '{league}')"
        )
        con.execute(
            f"INSERT INTO bronze_whoscored_schedule VALUES "
            f"({gid}, '{d} 12:00:00', '{league}', '{season}')"
        )

        # Spread events
        succ_count = int((pass_per_match + other_per_match) * success_rate)
        succ_so_far = 0
        for k in range(pass_per_match):
            ok = "TRUE" if succ_so_far < succ_count else "FALSE"
            succ_so_far += 1 if ok == "TRUE" else 0
            con.execute(
                f"INSERT INTO gold_fct_event VALUES "
                f"('{match_id}', '{team_id}', '{league}', '{season}', "
                f"'pass', {ok})"
            )
        for k in range(other_per_match):
            ok = "TRUE" if succ_so_far < succ_count else "FALSE"
            succ_so_far += 1 if ok == "TRUE" else 0
            con.execute(
                f"INSERT INTO gold_fct_event VALUES "
                f"('{match_id}', '{team_id}', '{league}', '{season}', "
                f"'dribble', {ok})"
            )
        for k in range(unknown_per_match):
            con.execute(
                f"INSERT INTO gold_fct_event VALUES "
                f"('{match_id}', '{team_id}', '{league}', '{season}', "
                f"'unknown', FALSE)"
            )

        # Shots: situation_canonical mix + body_part
        for s in range(set_piece_per_match):
            bp = "head" if s < header_per_match else "foot"
            con.execute(
                f"INSERT INTO gold_fct_shot VALUES "
                f"('{match_id}', '{team_id}', 'set_piece', '{bp}')"
            )
        remaining_headers = max(0, header_per_match - set_piece_per_match)
        for s in range(open_play_per_match):
            bp = "head" if s < remaining_headers else "foot"
            con.execute(
                f"INSERT INTO gold_fct_shot VALUES "
                f"('{match_id}', '{team_id}', 'open_play', '{bp}')"
            )
        # Rest filler if shots_per_match > set+open
        rest = shots_per_match - set_piece_per_match - open_play_per_match
        for s in range(max(0, rest)):
            con.execute(
                f"INSERT INTO gold_fct_shot VALUES "
                f"('{match_id}', '{team_id}', 'open_play', 'foot')"
            )
    return ws_ids


def _run_feat_sql(con) -> List[Dict[str, Any]]:
    sql = _translate(_read_sql(SQL_PATH))
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows


# ---------------------------------------------------------------------------
# Tests — share bounds
# ---------------------------------------------------------------------------

class TestActionShareBounds:
    """Every *_share_l5_avg ∈ [0, 1]. Out-of-bound = numerator/denominator drift."""

    SHARE_COLS = [
        "pass_share_l5_avg",
        "dribble_share_l5_avg",
        "tackle_share_l5_avg",
        "interception_share_l5_avg",
        "cross_share_l5_avg",
        "shot_share_l5_avg",
        "success_rate_l5_avg",
        "set_piece_share_l5_avg",
        "open_play_share_l5_avg",
        "header_share_l5_avg",
    ]

    def test_action_shares_in_unit_range(self, duck_conn):
        _seed_team_matches(
            duck_conn, "TeamA", 8, base_match=100,
            pass_per_match=50, other_per_match=50,
            shots_per_match=10, set_piece_per_match=3, open_play_per_match=7,
            header_per_match=2,
        )
        _seed_team_matches(
            duck_conn, "TeamB", 8, base_match=200,
            pass_per_match=70, other_per_match=30,
            shots_per_match=8, set_piece_per_match=2, open_play_per_match=6,
            header_per_match=1,
        )
        rows = _run_feat_sql(duck_conn)
        assert rows, "No rows produced from seeded fixtures"
        for r in rows:
            for col in self.SHARE_COLS:
                v = r.get(col)
                if v is None:
                    continue
                assert 0.0 - 1e-9 <= v <= 1.0 + 1e-9, (
                    f"{col} out of [0,1] range: {v} on "
                    f"({r['match_id']}, {r['team_id']})"
                )


# ---------------------------------------------------------------------------
# Pass-share correctness on a fully-known fixture
# ---------------------------------------------------------------------------

class TestPassShareCorrectness:
    """Team with 50% pass / 50% dribble × 5 matches → pass_share_l5_avg ≈ 0.5."""

    def test_pass_share_correctness_after_5_matches(self, duck_conn):
        _seed_team_matches(
            duck_conn, "TeamPS", 8, base_match=300,
            pass_per_match=50, other_per_match=50, unknown_per_match=0,
        )
        rows = _run_feat_sql(duck_conn)
        rows = [r for r in rows if r["team_id"] == "TeamPS"]
        rows.sort(key=lambda r: (r["date"], r["match_id"]))
        assert len(rows) >= 6
        sixth = rows[5]
        # 5 prior matches, each 50/100 pass share. Window AVG = 0.5.
        assert sixth["pass_share_l5_avg"] is not None
        assert abs(sixth["pass_share_l5_avg"] - 0.5) < 1e-9


# ---------------------------------------------------------------------------
# First 5 rows masked to NULL
# ---------------------------------------------------------------------------

class TestFirstFiveRowsNull:
    """match_rn ∈ {1..5} per (team_id, season) → all 10 share cols NULL."""

    SHARE_COLS = [
        "pass_share_l5_avg",
        "dribble_share_l5_avg",
        "tackle_share_l5_avg",
        "interception_share_l5_avg",
        "cross_share_l5_avg",
        "shot_share_l5_avg",
        "success_rate_l5_avg",
        "set_piece_share_l5_avg",
        "open_play_share_l5_avg",
        "header_share_l5_avg",
    ]

    def test_first_5_rows_null(self, duck_conn):
        _seed_team_matches(duck_conn, "TFirst", 10, base_match=400)
        rows = _run_feat_sql(duck_conn)
        rows = [r for r in rows if r["team_id"] == "TFirst"]
        rows.sort(key=lambda r: (r["date"], r["match_id"]))
        assert len(rows) >= 5
        for r in rows[:5]:
            for col in self.SHARE_COLS:
                assert r[col] is None, (
                    f"{col}={r[col]} on early row {r['match_id']} — should be NULL"
                )


# ---------------------------------------------------------------------------
# 'unknown' excluded from denominator (R3.D5)
# ---------------------------------------------------------------------------

class TestUnknownExcluded:
    """50% events 'unknown' + 50% 'pass' → pass_share = 1.0 (not 0.5)."""

    def test_unknown_action_excluded_from_denominator(self, duck_conn):
        # 100 pass + 100 unknown per match, 0 other actions.
        _seed_team_matches(
            duck_conn, "TUnk", 8, base_match=500,
            pass_per_match=100, other_per_match=0, unknown_per_match=100,
        )
        rows = _run_feat_sql(duck_conn)
        rows = [r for r in rows if r["team_id"] == "TUnk"]
        rows.sort(key=lambda r: (r["date"], r["match_id"]))
        assert len(rows) >= 6
        sixth = rows[5]
        # All 5 prior matches: pass share should be 1.0 (after unknown excl).
        assert sixth["pass_share_l5_avg"] is not None
        assert abs(sixth["pass_share_l5_avg"] - 1.0) < 1e-9, (
            f"unknown action leaked into denominator: pass_share="
            f"{sixth['pass_share_l5_avg']}"
        )


# ---------------------------------------------------------------------------
# Empty-fallback schema parity (pure-Python regex)
# ---------------------------------------------------------------------------

# Reuse the same SELECT-body extractor structure as feat_referee_bias test.
_SELECT_BODY_RE = re.compile(
    r"\bSELECT\b(.*?)(?:\bFROM\b)",
    re.IGNORECASE | re.DOTALL,
)


def _extract_select_columns(sql: str) -> List[str]:
    sql_no_comments = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    sql_no_comments = re.sub(r"/\*.*?\*/", "", sql_no_comments, flags=re.DOTALL)

    matches = list(_SELECT_BODY_RE.finditer(sql_no_comments))
    if not matches:
        raise ValueError("No SELECT body found")
    body = matches[-1].group(1)

    cols: List[str] = []
    depth = 0
    cur = []
    for ch in body:
        if ch == "(":
            depth += 1; cur.append(ch)
        elif ch == ")":
            depth -= 1; cur.append(ch)
        elif ch == "," and depth == 0:
            cols.append("".join(cur).strip()); cur = []
        else:
            cur.append(ch)
    if cur and "".join(cur).strip():
        cols.append("".join(cur).strip())

    aliases: List[str] = []
    for col in cols:
        m = re.search(r"\bAS\s+([A-Za-z_][A-Za-z_0-9]*)\s*$", col, re.IGNORECASE)
        if m:
            aliases.append(m.group(1))
        else:
            tok = re.findall(r"[A-Za-z_][A-Za-z_0-9]*", col)
            if tok:
                aliases.append(tok[-1])
    return aliases


def _extract_empty_select_types(sql: str) -> Dict[str, str]:
    sql_no_comments = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    out: Dict[str, str] = {}
    for m in re.finditer(
        r"CAST\s*\(\s*NULL\s+AS\s+([A-Za-z_]+)\s*\)\s+AS\s+([A-Za-z_][A-Za-z_0-9]*)",
        sql_no_comments,
        re.IGNORECASE,
    ):
        dtype, name = m.group(1).lower(), m.group(2)
        out[name] = dtype
    return out


class TestEmptyFallbackSchemaParity:
    """Empty file MUST mirror the main file's output column names + order."""

    def test_column_names_match(self):
        main_cols = _extract_select_columns(_read_sql(SQL_PATH))
        empty_cols = _extract_select_columns(_read_sql(EMPTY_SQL_PATH))
        assert main_cols == empty_cols, (
            f"Column-name parity FAIL.\n"
            f"  main : {main_cols}\n"
            f"  empty: {empty_cols}"
        )

    def test_ten_share_cols_present_in_both(self):
        share = [
            "pass_share_l5_avg",
            "dribble_share_l5_avg",
            "tackle_share_l5_avg",
            "interception_share_l5_avg",
            "cross_share_l5_avg",
            "shot_share_l5_avg",
            "success_rate_l5_avg",
            "set_piece_share_l5_avg",
            "open_play_share_l5_avg",
            "header_share_l5_avg",
        ]
        main_cols = _extract_select_columns(_read_sql(SQL_PATH))
        empty_cols = _extract_select_columns(_read_sql(EMPTY_SQL_PATH))
        for col in share:
            assert col in main_cols, f"main missing {col}"
            assert col in empty_cols, f"empty missing {col}"

    def test_empty_share_cols_are_double(self):
        types = _extract_empty_select_types(_read_sql(EMPTY_SQL_PATH))
        share = [
            "pass_share_l5_avg",
            "dribble_share_l5_avg",
            "tackle_share_l5_avg",
            "interception_share_l5_avg",
            "cross_share_l5_avg",
            "shot_share_l5_avg",
            "success_rate_l5_avg",
            "set_piece_share_l5_avg",
            "open_play_share_l5_avg",
            "header_share_l5_avg",
        ]
        for col in share:
            assert types.get(col) == "double", (
                f"empty fallback {col!r} should be CAST AS DOUBLE, "
                f"got {types.get(col)!r}"
            )
