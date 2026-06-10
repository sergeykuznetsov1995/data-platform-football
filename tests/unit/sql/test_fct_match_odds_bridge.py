"""
Unit tests for the matchhistory → fct_match_odds bridge pipeline (E4.5).

Pipeline under test:
  bronze.matchhistory_results → silver.matchhistory_match_odds (inline
                              team_aliases CTE → INNER JOIN gold.dim_match)
                            → gold.fct_match_odds (passthrough + canonical-trio)

We exercise:
  * The 63-pair team_aliases lookup (raw_name → canonical_id) on common
    naming variations (Wolves/Wolverhampton, Spurs/Tottenham, Manchester
    Utd/Manchester United, Nott'm Forest/Nottingham Forest, Newcastle Utd/
    Newcastle).
  * INNER JOIN coverage = 100% (every seeded match bridges to dim_match).
  * Tall-format unfold: 30 (bookmaker × market × closing_flag) rows per match.
  * closing_flag detection on the PSCH/PSCD/PSCA closing 1x2 columns.
  * Numeric typing — DECIMAL(6,3) odds parse without overflow.

#307: source switched from frozen bronze.matchhistory_games (raw football-data
column names) to live bronze.matchhistory_results. COLUMN_MAPPING renames ~50
cols (date→match_date, hometeam→home_team, B365H→odds_home_b365, …) and leaves
the rest raw (iw*, *closing, ah*, OU "b365>2.5"). This simulated bronze table
mirrors that mixed naming. DuckDB accepts ASCII operators inside double-quoted
identifiers identically to Trino. The 30-row tall format is 6 1x2-open + 6
1x2-closing + 4 ah-open + 4 ah-closing + 4 ou-open + 4 ou-closing — but the
WHERE drops all-NULL bookmaker rows; we seed numeric odds only for B365 / PS /
WH / VC / IW / BW (the 6 1x2 books) so the assertion is on the populated subset.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "matchhistory_match_odds.sql"
GOLD_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "fct_match_odds.sql"

_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Translation
# ---------------------------------------------------------------------------


def _collapse_call(sql: str, fn_name: str) -> str:
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
    "iceberg.bronze.matchhistory_results":    "bronze_matchhistory_results",
    "iceberg.gold.dim_match":                 "gold_dim_match",
    "iceberg.silver.matchhistory_match_odds": "silver_matchhistory_match_odds",
}


def _translate(sql: str) -> str:
    for k, v in _ICEBERG_TO_LOCAL.items():
        sql = sql.replace(k, v)
    sql = _collapse_call(sql, "to_utf8")
    sql = _collapse_call(sql, "to_hex")
    sql = re.sub(r"\bxxhash64\b", "md5", sql, flags=re.IGNORECASE)
    # Trino date_parse(x, fmt) → DuckDB strptime(x, fmt) (same %d/%m/%Y codes).
    sql = re.sub(r"\bdate_parse\b", "strptime", sql, flags=re.IGNORECASE)
    sql = sql.replace("timestamp(6)", "timestamp")
    return sql


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def duck_conn():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    yield con
    con.close()


# MatchHistory bronze odds columns observed in the SELECT, using the
# matchhistory_results physical names (#307): the 1x2-open books B365/BW/PS/WH/VC
# are renamed via COLUMN_MAPPING; IW + all closing/AH/OU stay raw.
_MH_COLUMNS = [
    "match_date", "home_team", "away_team", "league", "season", "_ingested_at",
    "odds_home_b365", "odds_draw_b365", "odds_away_b365",
    "odds_home_bw",   "odds_draw_bw",   "odds_away_bw",
    "iwh",   "iwd",   "iwa",
    "odds_home_ps",   "odds_draw_ps",   "odds_away_ps",
    "odds_home_wh",   "odds_draw_wh",   "odds_away_wh",
    "odds_home_vc",   "odds_draw_vc",   "odds_away_vc",
    "b365ch", "b365cd", "b365ca",
    "bwch",   "bwcd",   "bwca",
    "iwch",   "iwcd",   "iwca",
    "psch",   "pscd",   "psca",
    "whch",   "whcd",   "whca",
    "vcch",   "vccd",   "vcca",
    "ahh",
    "b365ahh", "b365aha",
    "pahh", "paha",
    "maxahh", "maxaha",
    "avgahh", "avgaha",
    "ahch",
    "b365cahh", "b365caha",
    "pcahh", "pcaha",
    "maxcahh", "maxcaha",
    "avgcahh", "avgcaha",
    "b365>2.5", "b365<2.5",
    "p>2.5", "p<2.5",
    "max>2.5", "max<2.5",
    "avg>2.5", "avg<2.5",
    "b365c>2.5", "b365c<2.5",
    "pc>2.5", "pc<2.5",
    "maxc>2.5", "maxc<2.5",
    "avgc>2.5", "avgc<2.5",
]


def _create_mh_table(con) -> None:
    cols_ddl = []
    for c in _MH_COLUMNS:
        # match_date is raw 'DD/MM/YYYY' varchar in matchhistory_results (#307),
        # parsed via date_parse in the silver SQL — keep it VARCHAR here.
        if c in ("match_date", "home_team", "away_team", "league"):
            cols_ddl.append(f'"{c}" VARCHAR')
        elif c == "season":
            cols_ddl.append(f'"{c}" BIGINT')
        elif c == "_ingested_at":
            cols_ddl.append(f'"{c}" TIMESTAMP')
        else:
            cols_ddl.append(f'"{c}" DOUBLE')
    con.execute(f"CREATE TABLE bronze_matchhistory_results ({', '.join(cols_ddl)})")


@pytest.fixture(autouse=True)
def _reset_schemas(duck_conn):
    for tbl in (
        "bronze_matchhistory_results",
        "gold_dim_match",
        "silver_matchhistory_match_odds",
    ):
        duck_conn.execute(f"DROP TABLE IF EXISTS {tbl}")

    _create_mh_table(duck_conn)
    duck_conn.execute(
        """
        CREATE TABLE gold_dim_match (
            match_id      VARCHAR,
            date          DATE,
            league        VARCHAR,
            season        VARCHAR,
            home_team_id  VARCHAR,
            away_team_id  VARCHAR
        )
        """
    )
    yield


# ---------------------------------------------------------------------------
# Seeding helpers
# ---------------------------------------------------------------------------


def _mh_row(
    *,
    date: str,
    home: str,
    away: str,
    league: str = "ENG-Premier League",
    season: int = 2024,
    b365h: float = 2.10, b365d: float = 3.30, b365a: float = 3.50,
    bwh: float = 2.05,  bwd: float = 3.40,  bwa: float = 3.60,
    iwh: float = 2.10,  iwd: float = 3.30,  iwa: float = 3.40,
    psh: float = 2.15,  psd: float = 3.20,  psa: float = 3.55,
    whh: float = 2.05,  whd: float = 3.40,  wha: float = 3.50,
    vch: float = 2.10,  vcd: float = 3.30,  vca: float = 3.45,
    psch: float = 2.20, pscd: float = 3.25, psca: float = 3.40,
) -> List[Any]:
    """Build a row matching _MH_COLUMNS order. Most odds default to NULL.

    Seed kwargs keep the short football-data names (b365h, …) for readability;
    they map to the renamed matchhistory_results physical columns below.
    """
    values = [None] * len(_MH_COLUMNS)
    idx = {c: i for i, c in enumerate(_MH_COLUMNS)}
    values[idx["match_date"]] = date
    values[idx["home_team"]] = home
    values[idx["away_team"]] = away
    values[idx["league"]] = league
    values[idx["season"]] = season
    values[idx["_ingested_at"]] = "2026-05-08 12:00:00"
    values[idx["odds_home_b365"]] = b365h
    values[idx["odds_draw_b365"]] = b365d
    values[idx["odds_away_b365"]] = b365a
    values[idx["odds_home_bw"]] = bwh
    values[idx["odds_draw_bw"]] = bwd
    values[idx["odds_away_bw"]] = bwa
    values[idx["iwh"]] = iwh
    values[idx["iwd"]] = iwd
    values[idx["iwa"]] = iwa
    values[idx["odds_home_ps"]] = psh
    values[idx["odds_draw_ps"]] = psd
    values[idx["odds_away_ps"]] = psa
    values[idx["odds_home_wh"]] = whh
    values[idx["odds_draw_wh"]] = whd
    values[idx["odds_away_wh"]] = wha
    values[idx["odds_home_vc"]] = vch
    values[idx["odds_draw_vc"]] = vcd
    values[idx["odds_away_vc"]] = vca
    values[idx["psch"]] = psch
    values[idx["pscd"]] = pscd
    values[idx["psca"]] = psca
    return values


def _seed_corpus(duck_conn) -> None:
    """5 matches with naming variations + matching dim_match rows."""
    placeholders = ", ".join(["?"] * len(_MH_COLUMNS))
    cols_quoted = ", ".join(f'"{c}"' for c in _MH_COLUMNS)
    insert_sql = f'INSERT INTO bronze_matchhistory_results ({cols_quoted}) VALUES ({placeholders})'

    # match_date seeded as raw football-data 'DD/MM/YYYY' (matches dim_match ISO dates after parse).
    rows = [
        _mh_row(date="15/08/2024", home="Wolves",          away="Tottenham"),
        _mh_row(date="01/09/2024", home="Wolverhampton",   away="Spurs"),
        _mh_row(date="05/10/2024", home="Manchester Utd",  away="Nott'm Forest"),
        _mh_row(date="10/11/2024", home="Manchester United", away="Nottingham Forest"),
        _mh_row(date="15/12/2024", home="Newcastle Utd",   away="West Ham"),
    ]
    for r in rows:
        duck_conn.execute(insert_sql, r)

    # gold.dim_match — slugs as observed in production
    duck_conn.execute(
        """
        INSERT INTO gold_dim_match VALUES
          ('M_WV_TT', DATE '2024-08-15', 'ENG-Premier League', '2425',
           'wolves', 'tottenham_hotspur'),
          ('M_WV_TT2', DATE '2024-09-01', 'ENG-Premier League', '2425',
           'wolves', 'tottenham_hotspur'),
          ('M_MU_NF', DATE '2024-10-05', 'ENG-Premier League', '2425',
           'manchester_utd', 'nottingham_forest'),
          ('M_MU_NF2', DATE '2024-11-10', 'ENG-Premier League', '2425',
           'manchester_utd', 'nottingham_forest'),
          ('M_NU_WH', DATE '2024-12-15', 'ENG-Premier League', '2425',
           'newcastle_united', 'west_ham_united')
        """
    )


def _materialize_silver(con) -> None:
    sql = _translate(SILVER_PATH.read_text(encoding="utf-8"))
    con.execute("DROP TABLE IF EXISTS silver_matchhistory_match_odds")
    con.execute(f"CREATE TABLE silver_matchhistory_match_odds AS {sql}")


def _run_gold(con) -> List[Dict[str, Any]]:
    sql = _translate(GOLD_PATH.read_text(encoding="utf-8"))
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFctMatchOddsBridge:

    def test_bridge_coverage_100_percent(self, duck_conn):
        """All 5 matches resolve to a dim_match → 5 distinct match_id_canonical."""
        _seed_corpus(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        match_ids = {r["match_id_canonical"] for r in out}
        assert match_ids == {"M_WV_TT", "M_WV_TT2", "M_MU_NF", "M_MU_NF2", "M_NU_WH"}, (
            f"bridge coverage incomplete: {sorted(match_ids)}"
        )

    def test_alias_canonicalisation_wolves_and_spurs(self, duck_conn):
        """Wolves/Wolverhampton → wolves; Spurs/Tottenham → tottenham_hotspur."""
        _seed_corpus(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        # Both M_WV_TT (Wolves vs Tottenham) and M_WV_TT2 (Wolverhampton vs Spurs)
        # must show up — proves both alias-pairs resolve.
        ids_present = {r["match_id_canonical"] for r in out}
        assert "M_WV_TT" in ids_present
        assert "M_WV_TT2" in ids_present

    def test_alias_canonicalisation_man_utd_variants(self, duck_conn):
        """Manchester Utd / Manchester United / Man Utd all → manchester_utd."""
        _seed_corpus(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        ids_present = {r["match_id_canonical"] for r in out}
        assert "M_MU_NF" in ids_present, (
            "Manchester Utd / Nott'm Forest alias-pair did not resolve"
        )
        assert "M_MU_NF2" in ids_present, (
            "Manchester United / Nottingham Forest alias-pair did not resolve"
        )

    def test_tall_format_unfold(self, duck_conn):
        """Each match unfolds to ~6 1x2-open + 6 1x2-closing rows for the
        seeded books (B365/BW/IW/PS/WH/VC for open; PS for closing). All-NULL
        bookmaker rows are dropped by the WHERE filter (AVG/MAX AH/OU + AH/OU
        open/closing — not seeded → empty → dropped).
        """
        _seed_corpus(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        # Expect ≥ 30 surviving rows across 5 matches × ≥6 books per match.
        assert len(out) >= 30, f"too few unfolded rows: {len(out)}"

    def test_closing_flag_detected_for_psc(self, duck_conn):
        """PSCH/PSCD/PSCA seeded → closing_flag=TRUE for PS-1x2-closing rows."""
        _seed_corpus(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        psc_rows = [
            r for r in out
            if r["bookmaker_code"] == "PS"
            and r["market"] == "1x2"
            and r["closing_flag"] is True
        ]
        assert psc_rows, "no PS closing 1x2 rows surfaced"
        for r in psc_rows:
            assert r["odds_h"] is not None
            assert r["odds_d"] is not None
            assert r["odds_a"] is not None

    def test_open_rows_have_closing_flag_false(self, duck_conn):
        """Non-closing books (B365/BW/IW/WH/VC open) → closing_flag=FALSE."""
        _seed_corpus(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        open_rows = [
            r for r in out
            if r["bookmaker_code"] == "B365"
            and r["market"] == "1x2"
            and r["closing_flag"] is False
        ]
        assert open_rows, "no B365 open 1x2 rows surfaced"

    def test_odds_decimal_typing_valid(self, duck_conn):
        """odds_h/d/a typed DECIMAL(6,3) — no parse failure."""
        from decimal import Decimal
        _seed_corpus(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        for r in out:
            for k in ("odds_h", "odds_d", "odds_a"):
                v = r.get(k)
                if v is not None:
                    assert isinstance(v, Decimal), (
                        f"{k} not typed as Decimal: {type(v)} {v!r}"
                    )
                    # Sanity-check 1.0–999.0 (decimal(6,3) supports this).
                    assert 1.0 <= float(v) <= 999.0, r

    def test_canonical_trio_populated(self, duck_conn):
        _seed_corpus(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        for r in out:
            assert r["odds_canonical"], r
            assert r["odds_source"] == "matchhistory", r
            assert r["odds_version"] == "v1", r

    def test_pk_uniqueness_per_match_book_market_closing(self, duck_conn):
        """PK = (match, bookmaker, market, closing_flag) — odds_canonical unique."""
        _seed_corpus(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        pks = [r["odds_canonical"] for r in out]
        assert len(pks) == len(set(pks)), (
            f"odds_canonical PK collision detected. Rows={len(pks)}, "
            f"distinct={len(set(pks))}"
        )
