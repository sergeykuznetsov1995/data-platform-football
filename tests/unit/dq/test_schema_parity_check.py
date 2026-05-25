"""Unit tests for ``CHECK.schema_parity`` factory + ``_run_schema_parity``
runner + ``_fetch_schema`` helper (E6 / W5b).

Strategy
--------
Pure-Python — mock ``_fetch_schema`` to return canned ``[(col, dtype), ...]``
lists per table. The runner consumes these via the ``conn`` argument and
formats a result dict; we never touch a real Trino connection.

Coverage
--------
* Identical sets across N≥3 tables → PASS.
* Missing column on one table → FAIL with column name in message.
* Type mismatch on a shared column → FAIL with both types reported.
* ``ignore_cols`` removes target columns from comparison.
* Factory rejects bad ``tables`` shapes (not 'schema.table', <2 entries).
* Factory accepts ``ignore_cols=None`` (normalises to empty list).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = REPO_ROOT / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from utils import data_quality as dq  # noqa: E402

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _StubConn:
    """A no-op connection. The runner only forwards it to ``_fetch_schema``,
    which we patch to ignore it; we never call ``cursor()`` here."""

    def cursor(self):
        raise AssertionError("Tests should patch _fetch_schema; cursor() unreachable")

    def close(self):
        pass


def _patch_fetch_schema(monkeypatch, table_to_schema):
    """Replace ``data_quality._fetch_schema`` with a lookup over
    ``table_to_schema: {table: [(col, dtype), ...]}``.
    """
    def fake_fetch(conn, table):
        if table not in table_to_schema:
            return []
        return list(table_to_schema[table])
    monkeypatch.setattr(dq, "_fetch_schema", fake_fetch)


# ---------------------------------------------------------------------------
# Runner-level tests
# ---------------------------------------------------------------------------

def test_three_identical_tables_pass(monkeypatch):
    """3 tables with identical column-name + type sets → PASS."""
    schemas = [
        ("match_id", "varchar"),
        ("season",   "varchar"),
        ("xg",       "double"),
    ]
    _patch_fetch_schema(monkeypatch, {
        "gold.fct_match_train":      schemas,
        "gold.fct_match_test":       schemas,
        "gold.predictions_input_v2": schemas,
    })
    chk = dq.CHECK.schema_parity(tables=[
        "gold.fct_match_train",
        "gold.fct_match_test",
        "gold.predictions_input_v2",
    ])
    out = dq._run_schema_parity(_StubConn(), chk)
    assert out["passed"] is True, out["details"]
    assert out["value"] == 0
    assert "OK" in out["details"] or "parity" in out["details"]


def test_missing_column_fails_with_detail(monkeypatch):
    """Table 2 missing 'extra_col' from ref → FAIL, message names the column."""
    ref_schema = [
        ("match_id",  "varchar"),
        ("extra_col", "double"),
    ]
    other_schema = [("match_id", "varchar")]
    _patch_fetch_schema(monkeypatch, {
        "gold.t1": ref_schema,
        "gold.t2": other_schema,
    })
    chk = dq.CHECK.schema_parity(tables=["gold.t1", "gold.t2"])
    out = dq._run_schema_parity(_StubConn(), chk)
    assert out["passed"] is False
    assert "extra_col" in out["details"]
    assert "MISSING" in out["details"]


def test_extra_column_fails_with_detail(monkeypatch):
    """Symmetric — ``other`` has a column the reference doesn't."""
    ref_schema = [("match_id", "varchar")]
    other_schema = [
        ("match_id", "varchar"),
        ("rogue_col", "varchar"),
    ]
    _patch_fetch_schema(monkeypatch, {
        "gold.t1": ref_schema,
        "gold.t2": other_schema,
    })
    chk = dq.CHECK.schema_parity(tables=["gold.t1", "gold.t2"])
    out = dq._run_schema_parity(_StubConn(), chk)
    assert out["passed"] is False
    assert "rogue_col" in out["details"]
    assert "EXTRA" in out["details"]


def test_data_type_mismatch_fails(monkeypatch):
    """Same column-name, divergent type → FAIL, message reports both types."""
    _patch_fetch_schema(monkeypatch, {
        "gold.t1": [("season", "varchar")],
        "gold.t2": [("season", "bigint")],
    })
    chk = dq.CHECK.schema_parity(tables=["gold.t1", "gold.t2"])
    out = dq._run_schema_parity(_StubConn(), chk)
    assert out["passed"] is False
    assert "type mismatch" in out["details"].lower()
    assert "season" in out["details"]
    assert "varchar" in out["details"]
    assert "bigint" in out["details"]


def test_ignore_cols_exclude_targets(monkeypatch):
    """``ignore_cols`` drops listed cols from BOTH sides → parity OK."""
    _patch_fetch_schema(monkeypatch, {
        "gold.t1": [
            ("match_id", "varchar"),
            ("over_2_5", "double"),     # only on t1; dropped via ignore
        ],
        "gold.t2": [
            ("match_id", "varchar"),
        ],
    })
    chk = dq.CHECK.schema_parity(
        tables=["gold.t1", "gold.t2"],
        ignore_cols=["over_2_5"],
    )
    out = dq._run_schema_parity(_StubConn(), chk)
    assert out["passed"] is True, out["details"]


def test_missing_table_surfaces_zero_cols(monkeypatch):
    """A table absent from the catalog returns 0 cols → flagged as missing."""
    _patch_fetch_schema(monkeypatch, {
        "gold.t1": [("match_id", "varchar")],
        "gold.absent": [],   # simulates missing table
    })
    chk = dq.CHECK.schema_parity(tables=["gold.t1", "gold.absent"])
    out = dq._run_schema_parity(_StubConn(), chk)
    assert out["passed"] is False
    assert "0 columns" in out["details"] or "missing" in out["details"].lower()


# ---------------------------------------------------------------------------
# Factory validation tests
# ---------------------------------------------------------------------------

def test_factory_validates_table_format():
    """Bad table reference (no dot / uppercase) raises ValueError at build time."""
    with pytest.raises(ValueError, match=r"schema_parity table must"):
        dq.CHECK.schema_parity(tables=["bad_format", "gold.t2"])

    with pytest.raises(ValueError, match=r"schema_parity table must"):
        dq.CHECK.schema_parity(tables=["GOLD.T1", "gold.t2"])


def test_factory_validates_dot_pattern():
    """Multiple dots / suffixed dots also rejected."""
    with pytest.raises(ValueError):
        dq.CHECK.schema_parity(tables=["gold.t1.t2", "gold.t3"])
    with pytest.raises(ValueError):
        dq.CHECK.schema_parity(tables=["gold.", "gold.t2"])


def test_single_table_factory_raises():
    """Parity with <2 tables is meaningless."""
    with pytest.raises(ValueError, match="at least 2 tables"):
        dq.CHECK.schema_parity(tables=["gold.t1"])
    with pytest.raises(ValueError, match="at least 2 tables"):
        dq.CHECK.schema_parity(tables=[])


def test_empty_ignore_cols_normalize():
    """``ignore_cols=None`` → params['ignore_cols'] is an empty list."""
    chk = dq.CHECK.schema_parity(
        tables=["gold.t1", "gold.t2"],
        ignore_cols=None,
    )
    assert chk.params["ignore_cols"] == []
    # And when omitted entirely:
    chk2 = dq.CHECK.schema_parity(tables=["gold.t1", "gold.t2"])
    assert chk2.params["ignore_cols"] == []


def test_factory_default_severity_error():
    chk = dq.CHECK.schema_parity(tables=["gold.t1", "gold.t2"])
    assert chk.severity == "ERROR"


def test_factory_severity_override():
    chk = dq.CHECK.schema_parity(
        tables=["gold.t1", "gold.t2"],
        severity="WARNING",
    )
    assert chk.severity == "WARNING"


def test_factory_kind_is_schema_parity():
    chk = dq.CHECK.schema_parity(tables=["gold.t1", "gold.t2"])
    assert chk.kind == "schema_parity"


def test_factory_default_name_includes_tables():
    chk = dq.CHECK.schema_parity(tables=["gold.t1", "gold.t2"])
    assert "gold.t1" in chk.name
    assert "gold.t2" in chk.name


# ---------------------------------------------------------------------------
# Runner integration with _RUNNERS dispatch
# ---------------------------------------------------------------------------

def test_runner_registered_in_runners_dict():
    """``_RUNNERS['schema_parity']`` resolves to ``_run_schema_parity``."""
    assert "schema_parity" in dq._RUNNERS
    assert dq._RUNNERS["schema_parity"] is dq._run_schema_parity
