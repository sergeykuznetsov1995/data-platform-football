"""
Unit tests for ``dags/sql/gold/feat_referee_bias.sql`` (E6 / W2a).

Strategy
--------
DuckDB-bridge: load the SQL file, apply Trino → DuckDB substitutions
(``xxhash64 → md5``, ``to_hex``/``to_utf8`` collapse, ``iceberg.gold.* →
local table``), seed deterministic dim_match / fct_card / fct_goal rows,
execute, then assert on the rolling-window mask + averages.

The empty-fallback parity check is pure-Python: parse SELECT lists from
both .sql files via regex and assert names+CAST types align.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "feat_referee_bias.sql"
EMPTY_SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "feat_referee_bias_empty.sql"

_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Trino → DuckDB translation
# ---------------------------------------------------------------------------

def _collapse_call(sql: str, fn_name: str) -> str:
    """Drop a wrapper function call (paren-balanced)."""
    out = []
    i = 0
    n = len(sql)
    while i < n:
        if sql[i:i + len(fn_name)].lower() == fn_name.lower():
            j = i + len(fn_name)
            while j < n and sql[j] in " \t\n\r":
                j += 1
            if j < n and sql[j] == "(":
                depth = 1
                j += 1
                inner_start = j
                while j < n and depth > 0:
                    if sql[j] == "(":
                        depth += 1
                    elif sql[j] == ")":
                        depth -= 1
                    if depth == 0:
                        break
                    j += 1
                out.append(sql[inner_start:j])
                i = j + 1
                continue
        out.append(sql[i])
        i += 1
    return "".join(out)


_ICEBERG_TO_LOCAL = {
    "iceberg.gold.dim_match": "gold_dim_match",
    "iceberg.gold.fct_card":  "gold_fct_card",
    "iceberg.gold.fct_goal":  "gold_fct_goal",
}


def _translate(sql: str) -> str:
    for k, v in _ICEBERG_TO_LOCAL.items():
        sql = sql.replace(k, v)
    # Trino's NORMALIZE(x, NFD) diacritic-fold (issue #228) has no DuckDB
    # equivalent — rewrite to strip_accents(x); the following `\p{Mn}+`
    # REGEXP_REPLACE then matches nothing (harmless no-op).
    sql = re.sub(r"normalize\((.*?),\s*NFD\)", r"strip_accents(\1)",
                 sql, flags=re.IGNORECASE)
    sql = _collapse_call(sql, "to_utf8")
    sql = _collapse_call(sql, "to_hex")
    sql = re.sub(r"\bxxhash64\b", "md5", sql, flags=re.IGNORECASE)
    return sql


def _read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# DuckDB schema seed + helpers
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def duck_conn():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    yield con
    con.close()


@pytest.fixture(autouse=True)
def _reset_schemas(duck_conn):
    for tbl in ("gold_dim_match", "gold_fct_card", "gold_fct_goal"):
        duck_conn.execute(f"DROP TABLE IF EXISTS {tbl}")

    duck_conn.execute(
        """
        CREATE TABLE gold_dim_match (
            match_id     VARCHAR,
            referee      VARCHAR,
            date         DATE,
            season       VARCHAR,
            league       VARCHAR,
            result_1x2   VARCHAR,
            home_score   INTEGER,
            away_score   INTEGER
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE gold_fct_card (
            match_id_canonical  VARCHAR,
            card_type           VARCHAR
        )
        """
    )
    duck_conn.execute(
        """
        CREATE TABLE gold_fct_goal (
            match_id_canonical  VARCHAR,
            is_penalty          BOOLEAN
        )
        """
    )
    yield


def _seed_referee_matches(
    con,
    referee_name: str,
    n: int,
    *,
    season: str = "2425",
    league: str = "ENG-Premier League",
    base_match: int = 1,
    yellow_per_match: int = 2,
    red_per_match: int = 0,
    total_goals: int = 1,
    pen_per_match: int = 0,
    home_win_each: bool = False,
    base_date: str = "2024-08-01",
) -> List[str]:
    """Insert N matches officiated by ``referee_name`` with deterministic stats.

    Returns the list of inserted match_ids (sorted by date).
    """
    import datetime as dt
    base = dt.date.fromisoformat(base_date)
    match_ids: List[str] = []
    for i in range(n):
        mid = f"m_{referee_name.replace(' ', '')}_{base_match + i:04d}"
        match_ids.append(mid)
        d = (base + dt.timedelta(days=i * 7)).isoformat()
        result = "H" if home_win_each else "D"
        con.execute(
            f"INSERT INTO gold_dim_match VALUES "
            f"('{mid}', '{referee_name}', DATE '{d}', "
            f"'{season}', '{league}', '{result}', 1, 1)"
        )
        for _ in range(yellow_per_match):
            con.execute(
                f"INSERT INTO gold_fct_card VALUES ('{mid}', 'yellow')"
            )
        for _ in range(red_per_match):
            con.execute(
                f"INSERT INTO gold_fct_card VALUES ('{mid}', 'red')"
            )
        for k in range(total_goals):
            is_pen = "TRUE" if k < pen_per_match else "FALSE"
            con.execute(
                f"INSERT INTO gold_fct_goal VALUES ('{mid}', {is_pen})"
            )
    return match_ids


def _run_feat_sql(con) -> List[Dict[str, Any]]:
    sql = _translate(_read_sql(SQL_PATH))
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Tests — rolling-window correctness
# ---------------------------------------------------------------------------

class TestFeatRefereeBiasRolling:
    """Window: ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING per (referee, season)."""

    ROLLING_COLS = [
        "ref_yellow_per_match_l10",
        "ref_red_per_match_l10",
        "ref_cards_per_match_l10",
        "ref_goals_per_match_l10",
        "ref_home_win_rate_l10",
        "ref_pen_per_match_l10",
    ]

    def test_first_5_rows_per_referee_are_null(self, duck_conn):
        """match_rn ∈ {1..5} → all 6 rolling cols are NULL (skip_first_n=5)."""
        for ref_name, base in [
            ("RefAlpha", 1000),
            ("RefBeta",  2000),
            ("RefGamma", 3000),
        ]:
            _seed_referee_matches(duck_conn, ref_name, 12, base_match=base)

        rows = _run_feat_sql(duck_conn)
        # Group by (referee_id, season) and inspect the first 5 by date.
        from collections import defaultdict
        groups = defaultdict(list)
        for r in rows:
            groups[(r["referee_id"], r["season"])].append(r)
        for key, grp in groups.items():
            grp.sort(key=lambda r: (r["date"], r["match_id"]))
            for col in self.ROLLING_COLS:
                first_5_vals = [g[col] for g in grp[:5]]
                assert all(v is None for v in first_5_vals), (
                    f"NULL mask violated for {key}, col={col}, "
                    f"got first-5={first_5_vals}"
                )

    def test_6th_row_avg_correctness(self, duck_conn):
        """6th row's ref_yellow_per_match_l10 = AVG of 5 prior matches.

        Window L10 ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING; on row 6
        exactly 5 rows fall into the window (rows 1-5), each contributing
        2 yellows. Expected = 2.0 (within fp tolerance).
        """
        _seed_referee_matches(
            duck_conn, "SoloRef", 12,
            base_match=5000,
            yellow_per_match=2,
            red_per_match=0,
            total_goals=1,
            pen_per_match=0,
        )
        rows = _run_feat_sql(duck_conn)
        rows.sort(key=lambda r: (r["date"], r["match_id"]))
        # 6th row = index 5
        sixth = rows[5]
        assert sixth["ref_yellow_per_match_l10"] is not None
        assert abs(sixth["ref_yellow_per_match_l10"] - 2.0) < 1e-9

    def test_6th_row_zero_when_zero_input(self, duck_conn):
        """Sanity: 0 prior yellows → 0.0 avg (not NULL — COALESCE keeps 0)."""
        _seed_referee_matches(
            duck_conn, "ZeroRef", 8,
            base_match=6000,
            yellow_per_match=0,
            red_per_match=0,
            total_goals=0,
        )
        rows = _run_feat_sql(duck_conn)
        rows.sort(key=lambda r: (r["date"], r["match_id"]))
        sixth = rows[5]
        assert sixth["ref_yellow_per_match_l10"] == 0.0
        assert sixth["ref_cards_per_match_l10"] == 0.0
        assert sixth["ref_goals_per_match_l10"] == 0.0


# ---------------------------------------------------------------------------
# Tests — PK uniqueness
# ---------------------------------------------------------------------------

class TestFeatRefereeBiasPK:
    """PK: (referee_id, match_id) must be unique."""

    def test_pk_uniqueness(self, duck_conn):
        for ref_name, base in [
            ("PK_A", 7000),
            ("PK_B", 7100),
        ]:
            _seed_referee_matches(duck_conn, ref_name, 8, base_match=base)
        rows = _run_feat_sql(duck_conn)
        keys = [(r["referee_id"], r["match_id"]) for r in rows]
        assert len(keys) == len(set(keys)), (
            f"PK collision in {len(keys) - len(set(keys))} pairs; "
            "(referee_id, match_id) is not unique"
        )

    def test_no_referee_id_null(self, duck_conn):
        """match_with_ref filters NULL/blank referees → no NULL referee_id."""
        _seed_referee_matches(duck_conn, "Solid Ref", 7, base_match=8000)
        rows = _run_feat_sql(duck_conn)
        assert all(r["referee_id"] is not None for r in rows)


# ---------------------------------------------------------------------------
# Tests — referee_id format
# ---------------------------------------------------------------------------

class TestRefereeIdFormat:
    """``referee_id = 'ref_' || lower(to_hex(xxhash64(...)))``.

    DuckDB substitutes md5 for xxhash64 — md5 is 32 hex chars vs Trino's
    16 chars. We assert format ``ref_[a-f0-9]+`` (length tolerance) so
    the test is portable across the two engines.
    """

    def test_referee_id_starts_with_ref_prefix(self, duck_conn):
        _seed_referee_matches(duck_conn, "Format Ref", 6, base_match=9000)
        rows = _run_feat_sql(duck_conn)
        assert rows
        for r in rows:
            assert r["referee_id"].startswith("ref_"), (
                f"referee_id missing 'ref_' prefix: {r['referee_id']!r}"
            )

    def test_referee_id_hex_body(self, duck_conn):
        _seed_referee_matches(duck_conn, "HexRef", 6, base_match=10000)
        rows = _run_feat_sql(duck_conn)
        assert rows
        hex_re = re.compile(r"^ref_[a-f0-9]+$")
        for r in rows:
            assert hex_re.match(r["referee_id"]), (
                f"referee_id not hex: {r['referee_id']!r}"
            )

    def test_referee_id_stable_across_matches(self, duck_conn):
        """Same referee → identical id across all of their matches."""
        _seed_referee_matches(duck_conn, "Stable Ref", 7, base_match=11000)
        rows = _run_feat_sql(duck_conn)
        ids = {r["referee_id"] for r in rows}
        assert len(ids) == 1, (
            f"Same referee should yield 1 referee_id, got {ids}"
        )


# ---------------------------------------------------------------------------
# Empty-fallback schema parity (pure-Python, regex-driven)
# ---------------------------------------------------------------------------

# Capture top-level SELECT body (between ``SELECT`` and ``FROM``/end-of-file).
# Both files have a single SELECT body that defines the output column list.
_SELECT_BODY_RE = re.compile(
    r"\bSELECT\b(.*?)(?:\bFROM\b|\bWHERE\b\s+1\s*=\s*0\s*$)",
    re.IGNORECASE | re.DOTALL,
)


def _extract_select_columns(sql: str) -> List[str]:
    """Return the list of output column names from the LAST top-level
    SELECT statement.

    The empty fallback's SELECT is followed by ``WHERE 1=0`` (no FROM); the
    main file ends with ``FROM rolled``. We match both via the regex above.
    The function returns column aliases (``AS <name>``), falling back to the
    bare token if no AS is present.

    NB: comments and CASE blocks are stripped before splitting on commas
    (case-when commas would otherwise split mid-clause).
    """
    # Strip line + block comments first.
    sql_no_comments = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    sql_no_comments = re.sub(r"/\*.*?\*/", "", sql_no_comments, flags=re.DOTALL)

    # Find the LAST top-level SELECT (after the WITH / CTE chain).
    # We scan for "SELECT ... FROM rolled" at end-of-file or "SELECT ...
    # WHERE 1 = 0" for the empty file.
    matches = list(_SELECT_BODY_RE.finditer(sql_no_comments))
    if not matches:
        raise ValueError("No SELECT body found")
    body = matches[-1].group(1)

    # Walk paren-aware, splitting on top-level commas.
    cols: List[str] = []
    depth = 0
    cur = []
    for ch in body:
        if ch == "(":
            depth += 1
            cur.append(ch)
        elif ch == ")":
            depth -= 1
            cur.append(ch)
        elif ch == "," and depth == 0:
            cols.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if cur and "".join(cur).strip():
        cols.append("".join(cur).strip())

    # Extract alias (last word after AS, or last bare token).
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
    """Empty-fallback: each col is ``CAST(NULL AS <type>) AS <name>``.

    Returns ``{name: type_lower}`` for each column. For the non-empty
    feat_referee_bias.sql we don't have explicit CASTs on every column,
    so the parity check focuses on the empty-fallback shape.
    """
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
    """Empty file MUST mirror the main file's output column names."""

    def test_column_names_match(self):
        main_cols = _extract_select_columns(_read_sql(SQL_PATH))
        empty_cols = _extract_select_columns(_read_sql(EMPTY_SQL_PATH))

        # Both must list the exact same names in the same order.
        assert main_cols == empty_cols, (
            f"Column-name parity FAIL.\n"
            f"  main : {main_cols}\n"
            f"  empty: {empty_cols}"
        )

    def test_six_rolling_cols_present_in_both(self):
        rolling = [
            "ref_yellow_per_match_l10",
            "ref_red_per_match_l10",
            "ref_cards_per_match_l10",
            "ref_goals_per_match_l10",
            "ref_home_win_rate_l10",
            "ref_pen_per_match_l10",
        ]
        main_cols = _extract_select_columns(_read_sql(SQL_PATH))
        empty_cols = _extract_select_columns(_read_sql(EMPTY_SQL_PATH))
        for col in rolling:
            assert col in main_cols, f"main missing {col}"
            assert col in empty_cols, f"empty missing {col}"

    def test_empty_rolling_cols_are_double(self):
        """In feat_referee_bias_empty.sql the 6 rolling cols are CAST AS double."""
        types = _extract_empty_select_types(_read_sql(EMPTY_SQL_PATH))
        rolling = [
            "ref_yellow_per_match_l10",
            "ref_red_per_match_l10",
            "ref_cards_per_match_l10",
            "ref_goals_per_match_l10",
            "ref_home_win_rate_l10",
            "ref_pen_per_match_l10",
        ]
        for col in rolling:
            assert types.get(col) == "double", (
                f"empty fallback {col!r} should be CAST AS double, "
                f"got {types.get(col)!r}"
            )

    def test_empty_business_keys_have_expected_types(self):
        """match_id+referee_id+league = varchar; date = date; season = bigint
        (matches dim_match.season — year-of-start integer)."""
        types = _extract_empty_select_types(_read_sql(EMPTY_SQL_PATH))
        assert types.get("referee_id") == "varchar"
        assert types.get("match_id") == "varchar"
        assert types.get("date") == "date"
        assert types.get("league") == "varchar"
        assert types.get("season") == "varchar"  # #404: slug, matches dim_match
