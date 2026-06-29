"""
Unit tests for ``utils.data_quality.CHECK.canonical_completeness`` (E2 — 2026-05).

The check asserts the R0.4 schema-versioning contract:

    every row with a non-NULL ``<base>_canonical`` MUST also carry
    a non-NULL ``<base>_source`` AND non-NULL ``<base>_version``.

The factory ``CHECK.canonical_completeness(table, canonical_col)`` validates
that ``canonical_col`` ends with ``_canonical`` (raises ValueError if not).
The runner ``_run_canonical_completeness`` derives the source/version
column names by stripping the suffix and runs a single COUNT(*) over the
offending predicate. Offender count > 0 → check fails.

Trino is mocked at the connection layer — ``_get_conn`` is monkeypatched
to return a fake connection whose cursor.fetchone() returns the offender
count.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ``utils.data_quality`` lives under ``dags/utils/`` — mirror the path
# bootstrap that ``tests/unit/dags/conftest.py`` does for DAG tests.
PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_fake_conn(offenders: int) -> MagicMock:
    """Build a fake Trino connection that returns (offenders,) for fetchone()."""
    cursor = MagicMock()
    cursor.fetchone.return_value = (offenders,)
    cursor.fetchall.return_value = [(offenders,)]

    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.close.return_value = None
    return conn


def _import_dq():
    from utils import data_quality
    return data_quality


# ---------------------------------------------------------------------------
# Factory tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestCanonicalCompletenessFactory:
    """``CHECK.canonical_completeness`` builds Check dataclass instances."""

    def test_returns_check_with_correct_kind(self):
        dq = _import_dq()
        chk = dq.CHECK.canonical_completeness(
            table="gold.dim_venue",
            canonical_col="venue_canonical",
        )
        assert chk.kind == "canonical_completeness"
        assert chk.params["table"] == "gold.dim_venue"
        assert chk.params["canonical_col"] == "venue_canonical"
        assert chk.severity == "ERROR"  # default

    def test_severity_overridable(self):
        dq = _import_dq()
        chk = dq.CHECK.canonical_completeness(
            table="gold.dim_referee",
            canonical_col="referee_canonical",
            severity="WARNING",
        )
        assert chk.severity == "WARNING"

    def test_default_name_includes_table_and_col(self):
        dq = _import_dq()
        chk = dq.CHECK.canonical_completeness(
            table="gold.dim_venue",
            canonical_col="venue_canonical",
        )
        # Auto-generated name should be informative
        assert "gold.dim_venue" in chk.name
        assert "venue_canonical" in chk.name

    def test_custom_name_honoured(self):
        dq = _import_dq()
        chk = dq.CHECK.canonical_completeness(
            table="gold.dim_venue",
            canonical_col="venue_canonical",
            name="my_custom_check",
        )
        assert chk.name == "my_custom_check"

    def test_invalid_canonical_col_raises_value_error(self):
        """The factory rejects column names that don't end with '_canonical'."""
        dq = _import_dq()
        with pytest.raises(ValueError) as exc:
            dq.CHECK.canonical_completeness(
                table="gold.dim_venue",
                canonical_col="no_suffix",
            )
        # Error message should mention the offending column and the
        # required suffix so on-call sees what's wrong without crawling
        # source.
        msg = str(exc.value)
        assert "_canonical" in msg
        assert "no_suffix" in msg


# ---------------------------------------------------------------------------
# Runner tests — happy-path
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestCanonicalCompletenessRunnerOK:
    """When offenders=0 the check passes."""

    def test_zero_offenders_is_ok(self):
        dq = _import_dq()
        fake_conn = _make_fake_conn(offenders=0)
        with patch.object(dq, "_get_conn", return_value=fake_conn):
            chk = dq.CHECK.canonical_completeness(
                table="gold.dim_venue",
                canonical_col="venue_canonical",
            )
            report = dq.run_checks([chk], raise_on_error=True)

        assert len(report.results) == 1
        result = report.results[0]
        assert result.passed is True
        assert result.severity == "ERROR"  # original severity preserved
        assert result.value == 0
        assert len(report.errors) == 0
        assert len(report.warnings) == 0


# ---------------------------------------------------------------------------
# Runner tests — failure path
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestCanonicalCompletenessRunnerFailure:
    """When offenders>0 the check fails; ERROR severity raises."""

    def test_offenders_with_default_severity_raises(self):
        dq = _import_dq()
        fake_conn = _make_fake_conn(offenders=5)
        # AirflowException is stubbed by tests/unit/dags/conftest.py if airflow
        # is missing on the host — but THIS test file lives under tests/unit/dq
        # which has no airflow stubs. The runner falls back to RuntimeError.
        try:
            from airflow.exceptions import AirflowException
            expected_exc = AirflowException
        except ImportError:
            expected_exc = RuntimeError

        with patch.object(dq, "_get_conn", return_value=fake_conn):
            chk = dq.CHECK.canonical_completeness(
                table="gold.dim_venue",
                canonical_col="venue_canonical",
            )
            with pytest.raises(expected_exc):
                dq.run_checks([chk], raise_on_error=True)

    def test_offenders_actual_count_reported(self):
        """``CheckResult.value`` carries the offender count."""
        dq = _import_dq()
        fake_conn = _make_fake_conn(offenders=5)
        with patch.object(dq, "_get_conn", return_value=fake_conn):
            chk = dq.CHECK.canonical_completeness(
                table="gold.dim_venue",
                canonical_col="venue_canonical",
            )
            # raise_on_error=False to inspect the report instead of raising
            report = dq.run_checks([chk], raise_on_error=False)

        assert len(report.results) == 1
        result = report.results[0]
        assert result.passed is False
        assert result.value == 5, (
            f"expected value=5 (offender count), got {result.value!r}"
        )
        assert result.severity == "ERROR"
        assert "5" in result.details, (
            f"details should mention the offender count: {result.details!r}"
        )

    def test_warning_severity_does_not_raise(self):
        """severity=WARNING — failure shows up as a warning, no raise."""
        dq = _import_dq()
        fake_conn = _make_fake_conn(offenders=3)
        with patch.object(dq, "_get_conn", return_value=fake_conn):
            chk = dq.CHECK.canonical_completeness(
                table="gold.dim_venue",
                canonical_col="venue_canonical",
                severity="WARNING",
            )
            # raise_on_error=True would still NOT raise for WARNING checks.
            report = dq.run_checks([chk], raise_on_error=True)

        assert report.results[0].passed is False
        assert report.results[0].severity == "WARNING"
        assert len(report.errors) == 0
        assert len(report.warnings) == 1


# ---------------------------------------------------------------------------
# SQL construction — verify _safe_ident is invoked for identifiers
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestCanonicalCompletenessSql:
    """Verify the SQL the runner sends to Trino:

    * is fully qualified (catalog.schema.table)
    * derives ``<base>_source`` / ``<base>_version`` from the canonical col
    * includes both ``IS NULL`` predicates joined with ``OR``
    * uses validated identifiers (rejects invalid names via _safe_ident)
    """

    def test_runner_executes_expected_sql_shape(self):
        dq = _import_dq()
        fake_conn = _make_fake_conn(offenders=0)
        with patch.object(dq, "_get_conn", return_value=fake_conn):
            chk = dq.CHECK.canonical_completeness(
                table="gold.dim_venue",
                canonical_col="venue_canonical",
            )
            dq.run_checks([chk], raise_on_error=False)

        # Inspect the SQL passed to cursor.execute
        cursor = fake_conn.cursor.return_value
        assert cursor.execute.called, "execute must be called at least once"
        executed_sql = cursor.execute.call_args[0][0]

        # Fully-qualified table
        assert "iceberg.gold.dim_venue" in executed_sql, (
            f"SQL should reference the fully-qualified table: {executed_sql!r}"
        )
        # All three derived columns appear
        assert "venue_canonical" in executed_sql
        assert "venue_source" in executed_sql
        assert "venue_version" in executed_sql
        # Predicate shape: canonical IS NOT NULL AND (source IS NULL OR version IS NULL)
        assert "IS NOT NULL" in executed_sql
        assert "IS NULL" in executed_sql
        # COUNT(*) form
        assert "COUNT(" in executed_sql.upper() or "count(" in executed_sql

    def test_invalid_table_identifier_rejected(self):
        """``_safe_ident`` must reject SQL-injection-shaped table names.

        We surface this here even though the factory doesn't validate the
        table name — the runner does, so an invalid table flows to the
        runner and surfaces as a recorded error (not a raise via
        raise_on_error since the per-check try/except converts to
        ``CheckResult.error``).
        """
        dq = _import_dq()
        fake_conn = _make_fake_conn(offenders=0)
        with patch.object(dq, "_get_conn", return_value=fake_conn):
            chk = dq.CHECK.canonical_completeness(
                # Invalid: contains semicolon -> identifier validator must reject
                table="gold.dim_venue; DROP TABLE foo",
                canonical_col="venue_canonical",
            )
            report = dq.run_checks([chk], raise_on_error=False)

        assert len(report.results) == 1
        result = report.results[0]
        assert result.passed is False
        assert result.error is not None, (
            f"expected an error string from _safe_ident, got: {result}"
        )

    def test_runner_validates_canonical_suffix_at_boundary(self):
        """The runner re-validates the suffix at the boundary even if the
        factory bypass (e.g. a direct Check() construction) is attempted.

        We exercise this by building a Check by hand (skipping the factory)
        with an invalid canonical_col name.
        """
        dq = _import_dq()
        # Build a Check directly with a bad column name (factory would reject)
        bad_check = dq.Check(
            name="bypass",
            kind="canonical_completeness",
            params={"table": "gold.dim_venue", "canonical_col": "no_suffix"},
            severity="ERROR",
        )
        fake_conn = _make_fake_conn(offenders=0)
        with patch.object(dq, "_get_conn", return_value=fake_conn):
            report = dq.run_checks([bad_check], raise_on_error=False)

        # Runner caught the ValueError from _run_canonical_completeness
        assert len(report.results) == 1
        result = report.results[0]
        assert result.passed is False
        assert result.error is not None
        assert "_canonical" in result.error or "no_suffix" in result.error
