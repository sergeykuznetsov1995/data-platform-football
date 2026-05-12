"""
Unit tests for ``utils.data_quality.CHECK.ref_integrity``.

Regression test for the bug surfaced in E4 postmortem (2026-05-09):

    6× ref_integrity[gold.fct_*.match_id_canonical -> gold.dim_match] failed
    with TrinoUserError COLUMN_NOT_FOUND because the call site did not pass
    parent_key='match_id' and the default parent_key=key='match_id_canonical'
    is not a column on dim_match.

After the fix, a missing parent_key column must surface as a
human-readable CheckResult (passed=False, details=...) instead of an opaque
Trino exception. The runner does a pre-flight ``information_schema.columns``
lookup; if either child.key or parent.parent_key is absent, the check fails
gracefully with a message naming the offending column.

Trino is mocked at the connection layer — ``_get_conn`` returns a fake
connection whose cursor.fetchone()/fetchall() are scripted per-query.
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_scripted_conn(*, columns_present: dict, orphan_count: int = 0) -> MagicMock:
    """Fake Trino connection whose cursor responds based on which SQL
    is being executed.

    ``columns_present`` maps a fully-qualified ``catalog.schema.table`` (or
    just ``schema.table``) to the set of column names that
    ``information_schema.columns`` should report.

    The scripted cursor:
      * For SQL containing ``information_schema.columns``: returns rows that
        match the requested table+column predicates.
      * For any other SQL (the orphan-count query): returns ``orphan_count``.
    """
    cursor = MagicMock()

    def execute(sql, *args, **kwargs):
        cursor._last_sql = sql
        sql_lower = sql.lower()

        if "information_schema.columns" in sql_lower:
            # The runner uses _fetch_schema which selects (column_name,
            # data_type) — return both fields so the [(str(r[0]), str(r[1]))]
            # coercion inside _fetch_schema doesn't IndexError.
            requested = []
            for table_key, cols in columns_present.items():
                if table_key.split(".")[-1] in sql_lower:
                    requested.extend((c, 'varchar') for c in cols)
            cursor.fetchall.return_value = requested
            cursor.fetchone.return_value = requested[0] if requested else None
        else:
            # Orphan-count query
            cursor.fetchone.return_value = (orphan_count,)
            cursor.fetchall.return_value = [(orphan_count,)]

    cursor.execute.side_effect = execute

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
class TestRefIntegrityFactory:
    def test_default_parent_key_is_key(self):
        dq = _import_dq()
        chk = dq.CHECK.ref_integrity(
            child='gold.fct_match', parent='gold.dim_match', key='match_id',
        )
        assert chk.params['parent_key'] == 'match_id'

    def test_explicit_parent_key_overrides_default(self):
        dq = _import_dq()
        chk = dq.CHECK.ref_integrity(
            child='gold.fct_goal', parent='gold.dim_match',
            key='match_id_canonical', parent_key='match_id',
        )
        assert chk.params['key'] == 'match_id_canonical'
        assert chk.params['parent_key'] == 'match_id'

    def test_severity_default_error(self):
        dq = _import_dq()
        chk = dq.CHECK.ref_integrity(
            child='gold.fct_match', parent='gold.dim_match', key='match_id',
        )
        assert chk.severity == 'ERROR'


# ---------------------------------------------------------------------------
# Runner — happy path
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestRefIntegrityHappyPath:
    def test_no_orphans_passes(self):
        dq = _import_dq()
        conn = _make_scripted_conn(
            columns_present={
                'gold.fct_match': ['match_id'],
                'gold.dim_match': ['match_id'],
            },
            orphan_count=0,
        )
        with patch.object(dq, '_get_conn', return_value=conn):
            chk = dq.CHECK.ref_integrity(
                child='gold.fct_match', parent='gold.dim_match', key='match_id',
            )
            report = dq.run_checks([chk], raise_on_error=True)

        assert report.results[0].passed is True
        assert report.results[0].value == 0

    def test_orphans_fail(self):
        dq = _import_dq()
        conn = _make_scripted_conn(
            columns_present={
                'gold.fct_match': ['match_id'],
                'gold.dim_match': ['match_id'],
            },
            orphan_count=7,
        )
        with patch.object(dq, '_get_conn', return_value=conn):
            chk = dq.CHECK.ref_integrity(
                child='gold.fct_match', parent='gold.dim_match', key='match_id',
                severity='WARNING',
            )
            report = dq.run_checks([chk], raise_on_error=True)

        r = report.results[0]
        assert r.passed is False
        assert r.value == 7
        assert '7 orphan' in r.details


# ---------------------------------------------------------------------------
# Runner — regression: missing parent_key column
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestRefIntegrityMissingColumn:
    """E4 postmortem regression.

    When the call site forgets ``parent_key`` and the default key
    ('match_id_canonical') is not a column in the parent ('match_id'),
    the runner must NOT raise a Trino exception. It must return a
    structured CheckResult with details naming the missing column.
    """

    def test_missing_parent_key_returns_actionable_failure(self):
        dq = _import_dq()
        conn = _make_scripted_conn(
            columns_present={
                # parent has 'match_id' but NOT 'match_id_canonical'
                'gold.fct_goal': ['match_id_canonical'],
                'gold.dim_match': ['match_id'],
            },
            orphan_count=0,  # irrelevant — should never run the orphan SQL
        )
        with patch.object(dq, '_get_conn', return_value=conn):
            chk = dq.CHECK.ref_integrity(
                child='gold.fct_goal', parent='gold.dim_match',
                key='match_id_canonical',  # default parent_key=match_id_canonical
                severity='WARNING',
            )
            report = dq.run_checks([chk], raise_on_error=False)

        r = report.results[0]
        assert r.passed is False
        # Details must name the missing column AND its owner table so the
        # operator can fix the call site without crawling logs.
        assert 'match_id_canonical' in (r.details or ''), r.details
        assert 'dim_match' in (r.details or ''), r.details
        # Severity preserved
        assert r.severity == 'WARNING'

    def test_missing_child_key_returns_actionable_failure(self):
        dq = _import_dq()
        conn = _make_scripted_conn(
            columns_present={
                # child table exists but lacks the requested key column
                'gold.fct_goal': ['canonical_id', 'event_id'],
                'gold.dim_match': ['match_id'],
            },
            orphan_count=0,
        )
        with patch.object(dq, '_get_conn', return_value=conn):
            chk = dq.CHECK.ref_integrity(
                child='gold.fct_goal', parent='gold.dim_match',
                key='match_id', parent_key='match_id',
            )
            report = dq.run_checks([chk], raise_on_error=False)

        r = report.results[0]
        assert r.passed is False
        assert 'match_id' in (r.details or ''), r.details
        assert 'fct_goal' in (r.details or ''), r.details


# ---------------------------------------------------------------------------
# SQL shape (sanity check)
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestRefIntegritySql:
    def test_left_join_pattern(self):
        dq = _import_dq()
        conn = _make_scripted_conn(
            columns_present={
                'gold.fct_match': ['match_id'],
                'gold.dim_match': ['match_id'],
            },
            orphan_count=0,
        )
        with patch.object(dq, '_get_conn', return_value=conn):
            chk = dq.CHECK.ref_integrity(
                child='gold.fct_match', parent='gold.dim_match', key='match_id',
            )
            dq.run_checks([chk], raise_on_error=False)

        executed = conn.cursor.return_value._last_sql
        # The orphan SQL is the LAST one executed (after info_schema lookups)
        assert 'LEFT JOIN' in executed.upper()
        assert 'iceberg.gold.dim_match' in executed
        assert 'iceberg.gold.fct_match' in executed
