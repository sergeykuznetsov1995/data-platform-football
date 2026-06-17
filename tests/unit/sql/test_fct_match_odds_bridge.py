"""
Unit tests for the matchhistory → fct_match_odds bridge pipeline (E4.5).

Pipeline under test (#477 — bridge now via silver.xref_match, not gold.dim_match):
  bronze.matchhistory_results → silver.matchhistory_match_odds (synthesise the
                              'mh_<hash>' source_id → INNER JOIN silver.xref_match
                              WHERE confidence='date_team_match')
                            → gold.fct_match_odds (passthrough + canonical-trio)

Team-name canonicalisation moved UPSTREAM into xref_match (via xref_team) — this
file no longer carries an inline team_aliases CTE. The bridge is now a hash join
on the synthetic source_id, so the test seeds silver.xref_match rows whose
source_id is computed with the SAME 'mh_<md5>' formula (xxhash64→md5 under the
DuckDB translation) keyed on the raw bronze natural key.

We exercise:
  * Bridge coverage = 100% via xref_match (every seeded match resolves through
    the synthetic source_id hash on naming variations Wolves/Wolverhampton,
    Spurs/Tottenham, Manchester Utd/United, Nott'm/Nottingham Forest, …).
  * INNER-JOIN + confidence='date_team_match' semantics: orphan matches dropped.
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
    "iceberg.silver.xref_match":              "silver_xref_match",
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
    "_batch_id",
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
        elif c == "_batch_id":
            cols_ddl.append(f'"{c}" VARCHAR')
        else:
            cols_ddl.append(f'"{c}" DOUBLE')
    con.execute(f"CREATE TABLE bronze_matchhistory_results ({', '.join(cols_ddl)})")


@pytest.fixture(autouse=True)
def _reset_schemas(duck_conn):
    for tbl in (
        "bronze_matchhistory_results",
        "silver_xref_match",
        "silver_matchhistory_match_odds",
    ):
        duck_conn.execute(f"DROP TABLE IF EXISTS {tbl}")

    _create_mh_table(duck_conn)
    # silver.xref_match — frozen E1.5 schema (see silver/xref_match.sql L22-32).
    duck_conn.execute(
        """
        CREATE TABLE silver_xref_match (
            canonical_id  VARCHAR,
            source        VARCHAR,
            source_id     VARCHAR,
            display_name  VARCHAR,
            league        VARCHAR,
            season        VARCHAR,
            confidence    VARCHAR,
            match_score   DOUBLE
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
    values[idx["_batch_id"]] = "batch-1"
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

    # silver.xref_match — canonical bridge rows for matchhistory. source_id is
    # computed with the SAME 'mh_<md5>' formula the silver SQL uses (the DuckDB
    # translation rewrites xxhash64→md5), keyed on the raw bronze natural key
    # (parsed ISO date | lower(home) | lower(away) | league | raw year-start
    # season). All seeded matches bridge → confidence='date_team_match'.
    duck_conn.execute(
        """
        INSERT INTO silver_xref_match
          (canonical_id, source, source_id, display_name, league, season, confidence, match_score)
        SELECT
            canonical_id,
            'matchhistory',
            'mh_' || lower(md5(
                CAST(strptime(raw_date, '%d/%m/%Y') AS DATE)::varchar
                || '|' || lower(home) || '|' || lower(away)
                || '|' || league || '|' || CAST(season_year AS varchar)
            )),
            home || ' vs ' || away,
            league,
            lpad(CAST((season_year % 100) AS varchar), 2, '0')
              || lpad(CAST(((season_year + 1) % 100) AS varchar), 2, '0'),
            'date_team_match',
            NULL
        FROM (VALUES
            ('15/08/2024', 'Wolves',            'Tottenham',         'ENG-Premier League', 2024, 'M_WV_TT'),
            ('01/09/2024', 'Wolverhampton',     'Spurs',             'ENG-Premier League', 2024, 'M_WV_TT2'),
            ('05/10/2024', 'Manchester Utd',    'Nott''m Forest',    'ENG-Premier League', 2024, 'M_MU_NF'),
            ('10/11/2024', 'Manchester United', 'Nottingham Forest', 'ENG-Premier League', 2024, 'M_MU_NF2'),
            ('15/12/2024', 'Newcastle Utd',     'West Ham',          'ENG-Premier League', 2024, 'M_NU_WH')
        ) AS t(raw_date, home, away, league, season_year, canonical_id)
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
        """All 5 matches resolve to a dim_match → 5 distinct match_id."""
        _seed_corpus(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        match_ids = {r["match_id"] for r in out}
        assert match_ids == {"M_WV_TT", "M_WV_TT2", "M_MU_NF", "M_MU_NF2", "M_NU_WH"}, (
            f"bridge coverage incomplete: {sorted(match_ids)}"
        )

    def test_bridge_resolves_wolves_and_spurs_name_variants(self, duck_conn):
        """Both name-variant fixtures bridge via xref_match (#477): the synthetic
        source_id hash is over the RAW bronze name, and xref_match is seeded from
        the same raw names, so 'Wolves vs Tottenham' and 'Wolverhampton vs Spurs'
        each resolve to their canonical match_id."""
        _seed_corpus(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        ids_present = {r["match_id"] for r in out}
        assert "M_WV_TT" in ids_present
        assert "M_WV_TT2" in ids_present

    def test_bridge_resolves_man_utd_name_variants(self, duck_conn):
        """'Manchester Utd' and 'Manchester United' fixtures both bridge via
        xref_match (the hash join is name-agnostic — canonicalisation is xref's
        job, here we only verify each distinct match resolves)."""
        _seed_corpus(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        ids_present = {r["match_id"] for r in out}
        assert "M_MU_NF" in ids_present, (
            "Manchester Utd / Nott'm Forest fixture did not bridge"
        )
        assert "M_MU_NF2" in ids_present, (
            "Manchester United / Nottingham Forest fixture did not bridge"
        )

    def test_orphan_match_dropped_by_confidence_filter(self, duck_conn):
        """A match whose only xref_match row is confidence='orphan' is dropped —
        the INNER JOIN + confidence='date_team_match' filter preserves the old
        INNER-JOIN-to-dim_match semantics (#477)."""
        _seed_corpus(duck_conn)
        # 6th bronze match (Brentford vs Fulham) with an ORPHAN xref_match row.
        cols_quoted = ", ".join(f'"{c}"' for c in _MH_COLUMNS)
        placeholders = ", ".join(["?"] * len(_MH_COLUMNS))
        duck_conn.execute(
            f'INSERT INTO bronze_matchhistory_results ({cols_quoted}) VALUES ({placeholders})',
            _mh_row(date="20/01/2025", home="Brentford", away="Fulham"),
        )
        duck_conn.execute(
            """
            INSERT INTO silver_xref_match
              (canonical_id, source, source_id, display_name, league, season, confidence, match_score)
            SELECT
                'mh_orphanhash', 'matchhistory',
                'mh_' || lower(md5(
                    CAST(strptime('20/01/2025', '%d/%m/%Y') AS DATE)::varchar
                    || '|' || lower('Brentford') || '|' || lower('Fulham')
                    || '|' || 'ENG-Premier League' || '|' || CAST(2024 AS varchar)
                )),
                'Brentford vs Fulham', 'ENG-Premier League', '2425', 'orphan', NULL
            """
        )
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        ids = {r["match_id"] for r in out}
        assert "mh_orphanhash" not in ids, "orphan match leaked past confidence filter"
        assert ids == {"M_WV_TT", "M_WV_TT2", "M_MU_NF", "M_MU_NF2", "M_NU_WH"}

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
        """PSCH/PSCD/PSCA seeded → is_closing=TRUE for PS-1x2-closing rows."""
        _seed_corpus(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        psc_rows = [
            r for r in out
            if r["bookmaker"] == "PS"
            and r["market"] == "1x2"
            and r["is_closing"] is True
        ]
        assert psc_rows, "no PS closing 1x2 rows surfaced"
        for r in psc_rows:
            assert r["odds_home"] is not None
            assert r["odds_draw"] is not None
            assert r["odds_away"] is not None

    def test_open_rows_have_closing_flag_false(self, duck_conn):
        """Non-closing books (B365/BW/IW/WH/VC open) → is_closing=FALSE."""
        _seed_corpus(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        open_rows = [
            r for r in out
            if r["bookmaker"] == "B365"
            and r["market"] == "1x2"
            and r["is_closing"] is False
        ]
        assert open_rows, "no B365 open 1x2 rows surfaced"

    def test_odds_decimal_typing_valid(self, duck_conn):
        """odds_home/draw/away typed DECIMAL(6,3) — no parse failure."""
        from decimal import Decimal
        _seed_corpus(duck_conn)
        _materialize_silver(duck_conn)
        out = _run_gold(duck_conn)
        for r in out:
            for k in ("odds_home", "odds_draw", "odds_away"):
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
