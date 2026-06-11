"""
Unit tests for ``utils.data_quality.CHECK.coverage``.

The check measures the share of rows in a table satisfying a predicate
relative to a (filtered) total. Two-tier severity:

    coverage < error_threshold (default 50%)  -> ERROR
    error_threshold <= cov < warn_threshold   -> WARNING
    cov >= warn_threshold                     -> passed

The runner overrides ``CheckResult.severity`` based on the observed ratio,
so the same Check instance can produce ERROR or WARNING depending on
runtime data. Tests verify both the SQL shape and severity escalation.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


def _make_conn(total: int, covered: int) -> MagicMock:
    cursor = MagicMock()
    cursor.fetchone.return_value = (total, covered)
    cursor.fetchall.return_value = [(total, covered)]
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.close.return_value = None
    return conn


def _import_dq():
    from utils import data_quality
    return data_quality


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestCoverageFactory:
    def test_column_shortcut_builds_is_not_null_predicate(self):
        dq = _import_dq()
        chk = dq.CHECK.coverage(table='gold.fct_match', column='home_xg')
        assert chk.kind == 'coverage'
        # Either a 'column' or a 'condition' field — implementation detail,
        # but the SQL must end up with IS NOT NULL on the column.
        assert chk.params.get('column') == 'home_xg' or 'home_xg' in chk.params.get('condition', '')

    def test_explicit_condition_overrides_column(self):
        dq = _import_dq()
        chk = dq.CHECK.coverage(
            table='gold.fct_standings',
            condition="team_id_source = 'fbref_canonical'",
        )
        assert "fbref_canonical" in chk.params.get('condition', '')

    def test_requires_column_or_condition(self):
        dq = _import_dq()
        with pytest.raises(ValueError):
            dq.CHECK.coverage(table='gold.fct_match')

    def test_thresholds_default(self):
        dq = _import_dq()
        chk = dq.CHECK.coverage(table='gold.fct_match', column='home_xg')
        assert chk.params['warn_threshold'] == pytest.approx(0.80)
        assert chk.params['error_threshold'] == pytest.approx(0.50)

    def test_unsafe_condition_rejected(self):
        dq = _import_dq()
        with pytest.raises(ValueError):
            dq.CHECK.coverage(
                table='gold.fct_match',
                condition="x = 1; DROP TABLE foo",
            )


# ---------------------------------------------------------------------------
# Runner — two-tier severity
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestCoverageTwoTier:
    def test_above_warn_threshold_is_ok(self):
        dq = _import_dq()
        conn = _make_conn(total=1000, covered=850)  # 85% >= 80%
        with patch.object(dq, '_get_conn', return_value=conn):
            chk = dq.CHECK.coverage(table='gold.fct_match', column='home_xg')
            report = dq.run_checks([chk], raise_on_error=True)
        r = report.results[0]
        assert r.passed is True
        assert r.value == pytest.approx(0.85)
        assert '85.0%' in r.details

    def test_between_thresholds_is_warning(self):
        dq = _import_dq()
        conn = _make_conn(total=1000, covered=600)  # 60% — middle tier
        with patch.object(dq, '_get_conn', return_value=conn):
            chk = dq.CHECK.coverage(
                table='gold.fct_match', column='home_xg',
                severity='ERROR',  # original — runner must downgrade
            )
            report = dq.run_checks([chk], raise_on_error=True)
        r = report.results[0]
        assert r.passed is False
        # Runner downgraded to WARNING — so raise_on_error didn't trigger.
        assert r.severity == 'WARNING'
        assert len(report.errors) == 0
        assert len(report.warnings) == 1

    def test_below_error_threshold_raises(self):
        dq = _import_dq()
        conn = _make_conn(total=1000, covered=300)  # 30% < 50%
        try:
            from airflow.exceptions import AirflowException
            expected_exc = AirflowException
        except ImportError:
            expected_exc = RuntimeError
        with patch.object(dq, '_get_conn', return_value=conn):
            chk = dq.CHECK.coverage(
                table='gold.fct_match', column='home_xg',
                severity='WARNING',  # original — runner must escalate
            )
            with pytest.raises(expected_exc):
                dq.run_checks([chk], raise_on_error=True)

    def test_empty_table_zero_coverage(self):
        dq = _import_dq()
        conn = _make_conn(total=0, covered=0)
        with patch.object(dq, '_get_conn', return_value=conn):
            chk = dq.CHECK.coverage(
                table='gold.fct_match', column='home_xg',
                severity='WARNING',
            )
            report = dq.run_checks([chk], raise_on_error=False)
        r = report.results[0]
        assert r.passed is False
        # Empty table: 0% coverage. Don't raise (test uses raise_on_error=False)
        # but the result must be marked failed.
        assert r.value == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# SQL shape
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestCoverageSql:
    def test_count_if_predicate(self):
        dq = _import_dq()
        conn = _make_conn(total=100, covered=90)
        with patch.object(dq, '_get_conn', return_value=conn):
            chk = dq.CHECK.coverage(table='gold.fct_match', column='home_xg')
            dq.run_checks([chk], raise_on_error=False)
        executed_sql = conn.cursor.return_value.execute.call_args[0][0]
        assert 'iceberg.gold.fct_match' in executed_sql
        assert 'COUNT(*)' in executed_sql.upper()
        assert 'COUNT_IF' in executed_sql.upper()
        assert 'home_xg IS NOT NULL' in executed_sql

    def test_where_clause_applied(self):
        dq = _import_dq()
        conn = _make_conn(total=100, covered=90)
        with patch.object(dq, '_get_conn', return_value=conn):
            chk = dq.CHECK.coverage(
                table='gold.fct_match', column='home_xg',
                where="season=2425",
            )
            dq.run_checks([chk], raise_on_error=False)
        executed_sql = conn.cursor.return_value.execute.call_args[0][0]
        assert 'WHERE' in executed_sql.upper()
        assert 'season=2425' in executed_sql
