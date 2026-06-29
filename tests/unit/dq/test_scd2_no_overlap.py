"""
Unit tests for ``utils.data_quality.CHECK.scd2_no_overlap`` (Phase C.3).

The check asserts the SCD-2 timeline-integrity contract:

    For every business key (``pk_cols``) the validity intervals
    ``[valid_from, valid_to)`` MUST NOT overlap. ``NULL`` ``valid_to`` is
    interpreted as the open-ended (current) row.

Strategy
--------
Mirrors ``test_e5_checks.py``: we patch ``_get_conn`` to return a DuckDB
connection wrapped in a tiny Trino-DBAPI-shaped bridge so the *real*
runner SQL is executed. DuckDB supports COUNT, JOIN, COALESCE and DATE
literals — everything the runner emits.

Scenarios
---------
1. Adjacent stints (``[A, B)`` + ``[B, NULL)``) → 0 overlaps, PASS
2. Overlapping stints (``[A, C)`` + ``[B, NULL)`` with B<C) → FAIL
3. Single open-ended stint (NULL valid_to) → 0 overlaps, PASS
4. Distinct partition keys with date-overlap → independent timelines,
   0 overlaps, PASS
5. Three consecutive adjacent stints (``[A, B), [B, C), [C, NULL)``) → PASS
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import duckdb
import pytest

# data_quality.py lives under dags/utils — make sure dags/ is on sys.path
REPO_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = REPO_ROOT / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from utils import data_quality as dq  # noqa: E402


# ---------------------------------------------------------------------------
# DuckDB connection bridge (mirrors test_e5_checks.py)
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
    """In-memory DuckDB pre-loaded with iceberg.gold.dim_manager (SCD-2)."""
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS iceberg")
    con.execute("CREATE SCHEMA IF NOT EXISTS iceberg.gold")
    con.execute(
        """
        CREATE TABLE iceberg.gold.dim_manager (
            team_id VARCHAR,
            manager_name VARCHAR,
            valid_from DATE,
            valid_to DATE  -- NULL => open-ended (current)
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
# Helpers
# ---------------------------------------------------------------------------


def _ins(con, team_id: str, name: str, vf: date, vt):
    """Insert one SCD-2 row. ``vt=None`` => NULL valid_to (open-ended)."""
    vt_sql = "NULL" if vt is None else f"DATE '{vt.isoformat()}'"
    con.execute(
        f"INSERT INTO iceberg.gold.dim_manager VALUES "
        f"('{team_id}', '{name}', DATE '{vf.isoformat()}', {vt_sql})"
    )


def _make_check(severity: str = "ERROR"):
    return dq.CHECK.scd2_no_overlap(
        table="gold.dim_manager",
        pk_cols=["team_id"],
        valid_from_col="valid_from",
        valid_to_col="valid_to",
        severity=severity,
    )


# ---------------------------------------------------------------------------
# Factory tests (light — most behaviour covered by runner tests)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestScd2NoOverlapFactory:
    """``CHECK.scd2_no_overlap`` builds Check dataclass instances."""

    def test_returns_check_with_correct_kind(self):
        chk = dq.CHECK.scd2_no_overlap(
            table="gold.dim_manager",
            pk_cols=["team_id"],
        )
        assert chk.kind == "scd2_no_overlap"
        assert chk.params["table"] == "gold.dim_manager"
        assert chk.params["pk_cols"] == ["team_id"]
        assert chk.params["valid_from_col"] == "valid_from"
        assert chk.params["valid_to_col"] == "valid_to"
        assert chk.severity == "ERROR"

    def test_default_name_includes_table_and_pk(self):
        chk = dq.CHECK.scd2_no_overlap(
            table="gold.dim_manager",
            pk_cols=["team_id"],
        )
        assert "gold.dim_manager" in chk.name
        assert "team_id" in chk.name

    def test_severity_overridable(self):
        chk = dq.CHECK.scd2_no_overlap(
            table="gold.dim_manager",
            pk_cols=["team_id"],
            severity="WARNING",
        )
        assert chk.severity == "WARNING"

    def test_empty_pk_cols_raises(self):
        with pytest.raises(ValueError):
            dq.CHECK.scd2_no_overlap(
                table="gold.dim_manager",
                pk_cols=[],
            )

    def test_custom_valid_cols_propagate(self):
        chk = dq.CHECK.scd2_no_overlap(
            table="gold.dim_player_contract",
            pk_cols=["player_id"],
            valid_from_col="effective_from",
            valid_to_col="effective_to",
        )
        assert chk.params["valid_from_col"] == "effective_from"
        assert chk.params["valid_to_col"] == "effective_to"


# ---------------------------------------------------------------------------
# Runner tests — over real DuckDB SQL execution
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestScd2NoOverlapRunner:
    """End-to-end runner tests — exercise the real check SQL via DuckDB."""

    def test_adjacent_stints_pass(self, duck_conn):
        """``[2020-01-01, 2021-01-01)`` + ``[2021-01-01, NULL)`` — adjacent OK.

        Closed-open semantics: shared endpoint is NOT an overlap.
        """
        _ins(duck_conn, "arsenal", "Mgr A",
             date(2020, 1, 1), date(2021, 1, 1))
        _ins(duck_conn, "arsenal", "Mgr B",
             date(2021, 1, 1), None)

        report = dq.run_checks([_make_check()], raise_on_error=False)

        assert report.errors == []
        assert report.warnings == []
        assert len(report.passed) == 1
        assert report.passed[0].value == 0

    def test_overlapping_stints_fail(self, duck_conn):
        """``[2020-01-01, 2021-06-01)`` overlaps with ``[2021-01-01, NULL)``."""
        _ins(duck_conn, "arsenal", "Mgr A",
             date(2020, 1, 1), date(2021, 6, 1))
        _ins(duck_conn, "arsenal", "Mgr B",
             date(2021, 1, 1), None)  # starts before A ends

        report = dq.run_checks([_make_check()], raise_on_error=False)

        assert len(report.errors) == 1
        err = report.errors[0]
        assert err.severity == "ERROR"
        assert err.passed is False
        assert err.value >= 1, f"expected >=1 overlap, got value={err.value}"
        # Sanity on details — must mention the partition columns
        assert "team_id" in err.details

    def test_single_open_stint_passes(self, duck_conn):
        """One row with NULL valid_to — no other rows to overlap with."""
        _ins(duck_conn, "arsenal", "Mgr A",
             date(2020, 1, 1), None)

        report = dq.run_checks([_make_check()], raise_on_error=False)

        assert report.errors == []
        assert len(report.passed) == 1
        assert report.passed[0].value == 0

    def test_different_partitions_independent_timelines(self, duck_conn):
        """Two teams whose date ranges overlap — but different team_id => OK.

        Date ranges intersect (2020 → 2022 vs 2021 → 2023) but the partition
        key differs, so each timeline is independent and the check passes.
        """
        _ins(duck_conn, "arsenal", "A1",
             date(2020, 1, 1), date(2022, 1, 1))
        _ins(duck_conn, "liverpool", "L1",
             date(2021, 1, 1), date(2023, 1, 1))

        report = dq.run_checks([_make_check()], raise_on_error=False)

        assert report.errors == []
        assert report.warnings == []
        assert len(report.passed) == 1
        assert report.passed[0].value == 0

    def test_three_consecutive_adjacent_stints_pass(self, duck_conn):
        """Three adjacent stints sharing endpoints — ``[A,B), [B,C), [C,NULL)``."""
        _ins(duck_conn, "arsenal", "Mgr 1",
             date(2018, 1, 1), date(2020, 1, 1))
        _ins(duck_conn, "arsenal", "Mgr 2",
             date(2020, 1, 1), date(2022, 1, 1))
        _ins(duck_conn, "arsenal", "Mgr 3",
             date(2022, 1, 1), None)

        report = dq.run_checks([_make_check()], raise_on_error=False)

        assert report.errors == []
        assert report.warnings == []
        assert len(report.passed) == 1
        assert report.passed[0].value == 0
