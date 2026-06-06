"""
Unit tests for scripts/drop_fotmob_dead_columns.py — CTAS-rebuild drop logic.

Strategy
--------
``drop_fotmob_dead_columns`` is a top-level script (not a package). Its only
non-stdlib import (``trino``) lives lazily inside ``get_conn``, so the module
loads on the host with no stubbing. We exercise ``rebuild_table`` against a
``FakeCursor`` that records executed SQL and answers the two read queries the
script issues (``information_schema.columns`` and ``SELECT COUNT(*)``). No Trino,
no network — pure keep/drop + guard logic.

What we cover
-------------
- dry-run: computes keep = live − dead, keeps the partition cols, emits NO DDL.
- happy path: CREATE staging / DROP / RENAME are emitted, the CTAS SELECT lists
  keep cols and none of the dead cols, final leftover is empty.
- already-clean table (no dead cols present) -> early return, no DDL.
- row-count mismatch (staging != before) -> SystemExit(3), RENAME never emitted.
- partition guard: a dead-set that swallows ``league`` -> SystemExit(2).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / 'scripts' / 'drop_fotmob_dead_columns.py'


def _load_module():
    spec = importlib.util.spec_from_file_location('drop_fotmob_dead_columns', _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None, f'cannot load {_SCRIPT_PATH}'
    mod = importlib.util.module_from_spec(spec)
    sys.modules['drop_fotmob_dead_columns'] = mod
    spec.loader.exec_module(mod)
    return mod


# Live schema of fotmob_team_stats before the drop: 5 dead cols + partition keys.
_TEAM_DEAD = {'table_type', 'short_name', 'xg', 'xg_conceded', 'qualification'}
_TEAM_COLS_BEFORE = [
    'team_id', 'team_name', 'table_type', 'short_name', 'xg', 'xg_conceded',
    'qualification', 'position', 'points', 'league', 'season',
    '_source', '_ingested_at', 'form',
]


class FakeCursor:
    """Records SQL and answers the script's two read queries.

    The ``information_schema.columns`` query is issued twice — once before the
    rebuild (full schema) and once after the RENAME (must reflect the drop), so
    the answer flips to ``cols_after`` once a RENAME has executed.
    """

    def __init__(self, cols_before, cols_after, count_table, count_staging):
        self.cols_before = cols_before
        self.cols_after = cols_after
        self.count_table = count_table
        self.count_staging = count_staging
        self.sql_log: list[str] = []
        self._renamed = False
        self._last = ''

    def execute(self, sql):
        self.sql_log.append(sql)
        self._last = sql

    def fetchall(self):
        s = self._last.lower()
        if 'information_schema.columns' in s:
            cols = self.cols_after if self._renamed else self.cols_before
            return [(c,) for c in cols]
        if 'count(*)' in s:
            return [(self.count_staging,)] if '_drop_staging' in s else [(self.count_table,)]
        if 'rename to' in s:
            self._renamed = True
        return []

    # convenience filters --------------------------------------------------
    def _emitted(self, needle: str) -> list[str]:
        return [q for q in self.sql_log if needle in q.lower()]

    @property
    def ddl_emitted(self) -> bool:
        return bool(
            self._emitted('create table ')
            or self._emitted('drop table iceberg')
            or self._emitted('rename to')
        )


def _team_cursor(count_table=40, count_staging=40):
    keep = [c for c in _TEAM_COLS_BEFORE if c not in _TEAM_DEAD]
    return FakeCursor(_TEAM_COLS_BEFORE, keep, count_table, count_staging)


def test_dry_run_computes_keep_and_emits_no_ddl():
    mod = _load_module()
    c = _team_cursor()

    mod.rebuild_table(c, 'fotmob_team_stats', _TEAM_DEAD, dry_run=True)

    # Only the schema read happened — no CREATE/DROP/RENAME, no COUNT(*).
    assert not c.ddl_emitted
    assert not c._emitted('count(*)')


def test_happy_path_emits_ctas_drop_rename_without_dead_cols():
    mod = _load_module()
    c = _team_cursor(count_table=40, count_staging=40)

    mod.rebuild_table(c, 'fotmob_team_stats', _TEAM_DEAD, dry_run=False)

    ctas = c._emitted('create table ')
    assert len(ctas) == 1
    # Partition keys survive, dead cols are gone from the CTAS projection.
    assert '"league"' in ctas[0] and '"season"' in ctas[0]
    for dead in _TEAM_DEAD:
        assert f'"{dead}"' not in ctas[0]
    # Old table dropped, staging renamed into place.
    assert c._emitted('drop table iceberg.bronze.fotmob_team_stats')
    assert c._emitted('rename to')


def test_already_clean_table_is_skipped():
    mod = _load_module()
    clean = [c for c in _TEAM_COLS_BEFORE if c not in _TEAM_DEAD]
    c = FakeCursor(clean, clean, 40, 40)

    mod.rebuild_table(c, 'fotmob_team_stats', _TEAM_DEAD, dry_run=False)

    assert not c.ddl_emitted


def test_row_count_mismatch_aborts_before_rename():
    mod = _load_module()
    c = _team_cursor(count_table=40, count_staging=39)

    with pytest.raises(SystemExit) as exc:
        mod.rebuild_table(c, 'fotmob_team_stats', _TEAM_DEAD, dry_run=False)

    assert exc.value.code == 3
    # Staging was built, but the swap (DROP old + RENAME) must NOT happen.
    assert not c._emitted('rename to')
    assert not c._emitted('drop table iceberg.bronze.fotmob_team_stats')


def test_partition_column_in_dead_set_aborts():
    mod = _load_module()
    # A dead-set that swallows the partition key must refuse to rebuild.
    bad_dead = {'league'}
    c = _team_cursor()

    with pytest.raises(SystemExit) as exc:
        mod.rebuild_table(c, 'fotmob_team_stats', bad_dead, dry_run=False)

    assert exc.value.code == 2
    assert not c.ddl_emitted
