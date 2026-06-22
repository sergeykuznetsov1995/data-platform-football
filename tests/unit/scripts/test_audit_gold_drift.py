"""
Unit tests for scripts/audit_gold_drift.py (#765).

Strategy
--------
``audit_gold_drift`` is a top-level script (not a package), loaded via
``importlib.util`` from its absolute path. The Trino client is imported lazily
inside ``main()`` / the render helpers, so importing the module is side-effect
free and its pure functions test without Trino or network.

What we cover
-------------
- ``diff``      — MISSING / EXTRA columns are ERROR; type mismatch is WARN only
                  under ``--types``; a clean schema yields no findings.
- ``_base_type``— parameterised types compare on their base ('varchar(10)' == 'varchar').
- ``gold_sql_files`` — ``*_empty.sql`` fallbacks are excluded; real tables kept.
- ``expected_schema`` — when the timestamp-appended probe trips
  DUPLICATE_COLUMN_NAME (a template already projecting _silver_created_at), it
  retries without the wrap and reads the schema off ``cursor.description``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / 'scripts' / 'audit_gold_drift.py'


def _load_module():
    spec = importlib.util.spec_from_file_location('audit_gold_drift', _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None, f'cannot load {_SCRIPT_PATH}'
    mod = importlib.util.module_from_spec(spec)
    sys.modules['audit_gold_drift'] = mod
    spec.loader.exec_module(mod)
    return mod


mod = _load_module()


class TestDiff:
    def test_missing_column_is_error(self):
        expected = {'venue_id': 'varchar', 'latitude': 'double'}
        actual = {'venue_id': 'varchar'}
        findings = mod.diff(expected, actual, check_types=False)
        assert findings == [('ERROR', 'MISSING', 'latitude (double) — in SQL, absent in live table')]

    def test_extra_column_is_error(self):
        expected = {'venue_id': 'varchar'}
        actual = {'venue_id': 'varchar', 'legacy_col': 'bigint'}
        findings = mod.diff(expected, actual, check_types=False)
        assert findings == [('ERROR', 'EXTRA', 'legacy_col (bigint) — in live table, absent in SQL')]

    def test_matching_schema_yields_no_findings(self):
        cols = {'venue_id': 'varchar', 'capacity': 'integer'}
        assert mod.diff(cols, dict(cols), check_types=False) == []

    def test_type_mismatch_is_warn_only_with_types_flag(self):
        expected = {'capacity': 'integer'}
        actual = {'capacity': 'bigint'}
        assert mod.diff(expected, actual, check_types=False) == []
        warns = mod.diff(expected, actual, check_types=True)
        assert len(warns) == 1
        assert warns[0][0] == 'WARN' and warns[0][1] == 'TYPE'

    def test_parameterised_type_does_not_flag(self):
        # live 'varchar(64)' vs SQL 'varchar' must NOT be a mismatch.
        expected = {'name': 'varchar'}
        actual = {'name': 'varchar(64)'}
        assert mod.diff(expected, actual, check_types=True) == []


class TestBaseType:
    @pytest.mark.parametrize('raw,base', [
        ('varchar', 'varchar'),
        ('varchar(10)', 'varchar'),
        ('decimal(5,2)', 'decimal'),
        ('timestamp(6) with time zone', 'timestamp'),
        ('INTEGER', 'integer'),
    ])
    def test_base_type(self, raw, base):
        assert mod._base_type(raw) == base


class TestGoldSqlFiles:
    def test_excludes_empty_fallbacks(self):
        stems = {mod._stem(f) for f in mod.gold_sql_files()}
        # The empty fallback must not surface as a phantom table...
        assert 'fct_player_unavailable_empty' not in stems
        # ...while the real table it materializes is still audited.
        assert 'fct_player_unavailable' in stems
        # No `*_empty` stem leaks through the filter.
        assert not any(s.endswith(mod.FALLBACK_SUFFIX) for s in stems)

    def test_dim_venue_is_audited(self):
        stems = {mod._stem(f) for f in mod.gold_sql_files()}
        assert 'dim_venue' in stems


class _FakeCursor:
    """Replays a scripted sequence of execute() outcomes.

    Each element of ``script`` is either an Exception to raise, or a list of
    column descriptions ``[(name, type), ...]`` to expose via ``description``.
    """

    def __init__(self, script):
        self._script = list(script)
        self.description = None

    def execute(self, sql):
        step = self._script.pop(0)
        if isinstance(step, Exception):
            raise step
        self.description = step

    def fetchall(self):
        return []


class TestExpectedSchemaDuplicateColumnRetry:
    def test_retries_without_timestamp_wrap_on_duplicate_column(self, monkeypatch, tmp_path):
        # render_gold_sql is file/Trino-render coupled; stub it out.
        monkeypatch.setattr(
            mod, 'render_gold_sql',
            lambda path: 'SELECT 1 AS x, CURRENT_TIMESTAMP AS _silver_created_at',
        )
        dup = Exception('TrinoUserError: DUPLICATE_COLUMN_NAME: Column _silver_created_at')
        ok_desc = [('x', 'integer'), ('_silver_created_at', 'timestamp(6) with time zone')]
        cur = _FakeCursor([dup, ok_desc])  # first (wrapped) fails, retry (unwrapped) succeeds

        schema = mod.expected_schema(cur, tmp_path / 'fct_x.sql')

        assert schema == {'x': 'integer', '_silver_created_at': 'timestamp(6) with time zone'}

    def test_non_duplicate_error_propagates(self, monkeypatch, tmp_path):
        monkeypatch.setattr(mod, 'render_gold_sql', lambda path: 'SELECT 1 AS x')
        boom = Exception('TABLE_NOT_FOUND: iceberg.silver.missing')
        cur = _FakeCursor([boom])
        with pytest.raises(Exception, match='TABLE_NOT_FOUND'):
            mod.expected_schema(cur, tmp_path / 'fct_x.sql')
