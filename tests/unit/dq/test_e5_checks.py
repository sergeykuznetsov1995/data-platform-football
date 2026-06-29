"""
Unit tests for E5 (Player Availability) DQ checks.

Strategy
--------
``utils.data_quality.run_checks`` runs SQL via Trino. For unit tests we
monkeypatch ``_get_conn`` to return an in-memory DuckDB connection seeded with
the same iceberg.gold tables the real checks query. DuckDB is wire-compatible
enough for the SQL produced by the runners (COUNT, JOIN, DATE_DIFF,
ROW_NUMBER) — we exercise the *real* check runners, not a re-implementation.

What we cover
-------------
1. no_duplicates — PK violation on (match_id, team_id, player_id_canonical)
2. no_nulls — NULL on PK column -> ERROR
3. ref_integrity — orphan match_id absent in dim_match -> ERROR
4. value_range — season outside (2021..2030) WARNING-severity
5. row_count coverage check — >200 NULL-team_id rows triggers WARNING
6. point_in_time — leakage in unavailable_count_l5 (NOT NULL inside the masked
   prefix) -> ERROR; well-masked data -> PASS

The DuckDB connection wraps a tiny adapter so the ``cursor.fetchone()`` used by
``_fetchone`` returns tuples (matches the Trino DBAPI contract).
"""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest

# data_quality.py lives under dags/utils — make sure dags/ is on sys.path
REPO_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = REPO_ROOT / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from utils import data_quality as dq  # noqa: E402


# ---------------------------------------------------------------------------
# DuckDB connection bridge
# ---------------------------------------------------------------------------

class _DuckCursor:
    """Trino-DBAPI-shaped cursor wrapper around DuckDB."""

    def __init__(self, con):
        self._con = con
        self._result = None

    def execute(self, sql: str):
        self._result = self._con.execute(sql)
        return self

    def fetchone(self):
        return self._result.fetchone() if self._result else None

    def fetchall(self):
        return self._result.fetchall() if self._result else []

    def close(self):
        pass


class _DuckConn:
    """Trino-DBAPI-shaped connection over duckdb."""

    def __init__(self, con):
        self._con = con

    def cursor(self):
        return _DuckCursor(self._con)

    def close(self):
        # do not close the underlying duckdb conn — tests own lifecycle
        pass


def _build_conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB pre-loaded with `iceberg.gold` schema + E5 tables."""
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS iceberg")
    con.execute("CREATE SCHEMA IF NOT EXISTS iceberg.gold")

    con.execute(
        """
        CREATE TABLE iceberg.gold.fct_player_unavailable (
            match_id VARCHAR,
            match_date DATE,
            team_id VARCHAR,
            team_name_raw VARCHAR,
            player_id_canonical VARCHAR,
            ws_player_id VARCHAR,
            player_name VARCHAR,
            reason VARCHAR,
            _silver_ingested_at TIMESTAMP,
            league VARCHAR,
            season INTEGER
        )
        """
    )
    con.execute(
        """
        CREATE TABLE iceberg.gold.dim_match (
            match_id VARCHAR,
            date DATE,
            home_team_id VARCHAR,
            away_team_id VARCHAR,
            league VARCHAR,
            season INTEGER
        )
        """
    )
    con.execute(
        """
        CREATE TABLE iceberg.gold.feat_team_form (
            match_id VARCHAR,
            team_id VARCHAR,
            season INTEGER,
            date DATE,
            unavailable_count_l5 DOUBLE
        )
        """
    )
    return con


@pytest.fixture
def duck_conn(monkeypatch):
    """Patch ``data_quality._get_conn`` to return a DuckDB-backed connection."""
    raw = _build_conn()
    bridge = _DuckConn(raw)

    def _fake_get_conn(catalog: str = "iceberg"):
        return bridge

    monkeypatch.setattr(dq, "_get_conn", _fake_get_conn)
    yield raw  # tests interact with the underlying duckdb connection
    raw.close()


# ---------------------------------------------------------------------------
# Helpers — seed sample matches + dim_match parents
# ---------------------------------------------------------------------------

def _seed_dim_match(con, match_ids):
    """Insert N parent matches into dim_match to satisfy ref_integrity."""
    if not match_ids:
        return
    rows = ", ".join(
        f"('{mid}', DATE '2025-01-01', 'arsenal','liverpool','ENG-PL', 2024)"
        for mid in match_ids
    )
    con.execute(f"INSERT INTO iceberg.gold.dim_match VALUES {rows}")


def _seed_fct_player_unavailable(con, rows):
    """rows = list of dict with keys matching fct_player_unavailable cols."""
    if not rows:
        return
    vals = []
    for r in rows:
        match_id = r.get("match_id", "m1")
        match_date = r.get("match_date", date(2025, 1, 1))
        team_id = r.get("team_id")
        team_id_sql = "NULL" if team_id is None else f"'{team_id}'"
        player_canon = r.get("player_id_canonical")
        canon_sql = "NULL" if player_canon is None else f"'{player_canon}'"
        ws_pid = r.get("ws_player_id", "ws1")
        season = r.get("season", 2024)
        vals.append(
            f"('{match_id}', DATE '{match_date.isoformat()}', {team_id_sql}, "
            f"'TeamName', {canon_sql}, '{ws_pid}','PlayerName','Injury', "
            f" TIMESTAMP '2025-01-01 00:00:00','ENG-PL', {season})"
        )
    con.execute(
        "INSERT INTO iceberg.gold.fct_player_unavailable VALUES " + ", ".join(vals)
    )


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestE5DQChecks:
    """End-to-end DQ check tests — exercise real check runners over DuckDB."""

    def test_no_duplicates_pk_violation(self, duck_conn):
        """Duplicate (match_id, team_id, player_id_canonical) -> ERROR."""
        _seed_dim_match(duck_conn, ["m1"])
        _seed_fct_player_unavailable(duck_conn, [
            {"match_id": "m1", "team_id": "arsenal",
             "player_id_canonical": "p1", "ws_player_id": "ws1"},
            # exact duplicate by PK
            {"match_id": "m1", "team_id": "arsenal",
             "player_id_canonical": "p1", "ws_player_id": "ws1"},
        ])

        check = dq.CHECK.no_duplicates(
            "gold.fct_player_unavailable",
            pk=["match_id", "team_id", "player_id_canonical"],
        )
        report = dq.run_checks([check], raise_on_error=False)

        assert len(report.errors) == 1
        err = report.errors[0]
        assert err.severity == "ERROR"
        assert err.passed is False
        assert err.value == 1  # one duplicate row

    def test_no_nulls_on_pk(self, duck_conn):
        """NULL on player_id_canonical PK col -> ERROR."""
        _seed_dim_match(duck_conn, ["m1"])
        _seed_fct_player_unavailable(duck_conn, [
            {"match_id": "m1", "team_id": "arsenal",
             "player_id_canonical": None, "ws_player_id": "ws1"},
        ])

        check = dq.CHECK.no_nulls(
            "gold.fct_player_unavailable",
            cols=["match_id", "match_date", "player_id_canonical"],
        )
        report = dq.run_checks([check], raise_on_error=False)

        assert len(report.errors) == 1
        err = report.errors[0]
        assert err.severity == "ERROR"
        assert err.value >= 1

    def test_ref_integrity_orphan_match_id(self, duck_conn):
        """fct.match_id missing from dim_match -> ERROR."""
        # Only m1 is a real match
        _seed_dim_match(duck_conn, ["m1"])
        _seed_fct_player_unavailable(duck_conn, [
            {"match_id": "m1", "team_id": "arsenal",
             "player_id_canonical": "p1"},
            # m_orphan is NOT in dim_match
            {"match_id": "m_orphan", "team_id": "arsenal",
             "player_id_canonical": "p2"},
        ])

        check = dq.CHECK.ref_integrity(
            child="gold.fct_player_unavailable",
            parent="gold.dim_match",
            key="match_id",
        )
        report = dq.run_checks([check], raise_on_error=False)

        assert len(report.errors) == 1
        err = report.errors[0]
        assert err.severity == "ERROR"
        assert err.value == 1  # one orphan distinct match_id

    def test_value_range_season_outside_window(self, duck_conn):
        """season=2050 outside [2021,2030] -> WARNING-severity violation."""
        _seed_dim_match(duck_conn, ["m1", "m2"])
        _seed_fct_player_unavailable(duck_conn, [
            {"match_id": "m1", "team_id": "arsenal",
             "player_id_canonical": "p1", "season": 2024},
            {"match_id": "m2", "team_id": "arsenal",
             "player_id_canonical": "p2", "season": 2050},
        ])

        check = dq.CHECK.value_range(
            "gold.fct_player_unavailable",
            "season",
            min_val=2021, max_val=2030,
            severity="WARNING",
        )
        report = dq.run_checks([check], raise_on_error=False)

        # No ERROR-severity failures (severity is WARNING)
        assert report.errors == []
        assert len(report.warnings) == 1
        warn = report.warnings[0]
        assert warn.severity == "WARNING"
        assert warn.passed is False
        assert warn.value == 1  # one row outside range

    def test_coverage_team_id_null_threshold(self, duck_conn):
        """>200 rows with NULL team_id -> coverage row_count WARNING."""
        # need parent matches covering the dummy match_ids we'll insert
        match_ids = [f"m{i}" for i in range(250)]
        _seed_dim_match(duck_conn, match_ids)

        # Insert 201 rows with team_id NULL — exceeds max_rows=200 in the check
        rows = [
            {"match_id": f"m{i}", "team_id": None,
             "player_id_canonical": f"p{i}"}
            for i in range(201)
        ]
        _seed_fct_player_unavailable(duck_conn, rows)

        check = dq.CHECK.row_count(
            "gold.fct_player_unavailable",
            min_rows=0, max_rows=200,
            where="team_id IS NULL",
            severity="WARNING",
            name="coverage[fct_player_unavailable.team_id non-NULL]",
        )
        report = dq.run_checks([check], raise_on_error=False)

        assert report.errors == []
        assert len(report.warnings) == 1
        assert report.warnings[0].value == 201

    def test_point_in_time_unavailable_l5_no_leakage_then_leak(self, duck_conn):
        """First N rows per partition must have NULL feature.

        Phase 1: well-masked data (NULL for match_rn<=5) — check passes.
        Phase 2: insert a leak row (NOT NULL for match_rn=1) — check fails.
        """
        # Phase 1 — 6 rows for one (team_id, season); first 5 have NULL feat
        feat_rows = []
        for i in range(1, 7):
            val = "NULL" if i <= 5 else "3.0"
            feat_rows.append(
                f"('m{i:02d}','arsenal', 2024, DATE '2025-01-{i:02d}', {val})"
            )
        duck_conn.execute(
            "INSERT INTO iceberg.gold.feat_team_form VALUES " + ", ".join(feat_rows)
        )

        check = dq.CHECK.point_in_time(
            "gold.feat_team_form",
            feature_col="unavailable_count_l5",
            partition_by=["team_id", "season"],
            order_by="date",
            skip_first_n=5,
        )
        report = dq.run_checks([check], raise_on_error=False)

        # Phase 1: no leakage — check passes
        assert report.errors == []
        assert report.warnings == []
        assert len(report.passed) == 1
        assert report.passed[0].value == 0

        # Phase 2: introduce a leak row — match_rn=1 (earliest date) has NOT NULL feat
        # Replace m01 row with a leak (drop+reinsert).
        duck_conn.execute(
            "DELETE FROM iceberg.gold.feat_team_form WHERE match_id='m01'"
        )
        duck_conn.execute(
            "INSERT INTO iceberg.gold.feat_team_form VALUES "
            "('m01','arsenal', 2024, DATE '2025-01-01', 99.0)"
        )

        report2 = dq.run_checks([check], raise_on_error=False)
        assert len(report2.errors) == 1
        err = report2.errors[0]
        assert err.severity == "ERROR"
        assert err.value == 1  # exactly one leak row

    def test_run_checks_raises_when_raise_on_error(self, duck_conn):
        """Sanity: ERRORs raise when ``raise_on_error=True``."""
        _seed_dim_match(duck_conn, [])  # no parents
        _seed_fct_player_unavailable(duck_conn, [
            {"match_id": "m_orphan", "team_id": "arsenal",
             "player_id_canonical": "p1"},
        ])

        check = dq.CHECK.ref_integrity(
            child="gold.fct_player_unavailable",
            parent="gold.dim_match",
            key="match_id",
        )
        with pytest.raises((RuntimeError, Exception)) as excinfo:
            dq.run_checks([check], raise_on_error=True)
        # Either AirflowException (if airflow installed) or RuntimeError
        assert "DQ failed" in str(excinfo.value)
