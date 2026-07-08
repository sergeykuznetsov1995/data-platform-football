"""
Unit tests for scripts/purge_fbref_probe_seasons.py — EPL 2010/2015 purge (#892).

Same host-loadable-script pattern as ``test_drop_fotmob_dead_columns.py``:
``purge_table`` runs against a ``FakeCursor``, no Trino, no network.

What we cover
-------------
- dry-run: counts, emits no DELETE.
- happy path: one DELETE per table, snapshot_id logged first (rollback path),
  no table rebuild.
- already-clean table -> early return, no DELETE and no snapshot read.
- rows surviving the delete -> SystemExit(2).
- the predicate scopes both seasons and pins the league.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / 'scripts' / 'purge_fbref_probe_seasons.py'


def _load_module():
    spec = importlib.util.spec_from_file_location('purge_fbref_probe_seasons', _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None, f'cannot load {_SCRIPT_PATH}'
    mod = importlib.util.module_from_spec(spec)
    sys.modules['purge_fbref_probe_seasons'] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    """Answers COUNT and $snapshots reads; records every statement."""

    def __init__(self, before: int, after: int = 0):
        self.before = before
        self.after = after
        self.sql_log: list[str] = []
        self._last = ''
        self._deleted = False

    def execute(self, sql):
        self.sql_log.append(sql)
        self._last = sql
        if sql.lstrip().lower().startswith('delete'):
            self._deleted = True

    def fetchall(self):
        s = self._last.lower()
        if 'snapshot_id' in s:
            return [(7246610019590143131,)]
        if 'count(*)' in s:
            return [(self.after if self._deleted else self.before,)]
        return []

    def _emitted(self, needle: str) -> list[str]:
        return [q for q in self.sql_log if needle in q.lower()]


def test_dry_run_counts_but_emits_no_delete():
    mod = _load_module()
    c = FakeCursor(before=852)

    assert mod.purge_table(c, 'fbref_schedule', dry_run=True) == 852
    assert not c._emitted('delete from')
    assert not c._emitted('snapshot_id')


def test_happy_path_logs_snapshot_then_deletes():
    mod = _load_module()
    c = FakeCursor(before=852, after=0)

    assert mod.purge_table(c, 'fbref_schedule', dry_run=False) == 852

    deletes = c._emitted('delete from')
    assert len(deletes) == 1
    assert 'iceberg.bronze.fbref_schedule' in deletes[0]
    # Rollback anchor must be captured before the destructive statement.
    snapshot_idx = c.sql_log.index(c._emitted('snapshot_id')[0])
    assert snapshot_idx < c.sql_log.index(deletes[0])
    # Row-level delete only — never a rebuild.
    assert not c._emitted('drop table')
    assert not c._emitted('create table')


def test_already_clean_table_is_a_noop():
    mod = _load_module()
    c = FakeCursor(before=0)

    assert mod.purge_table(c, 'fbref_match_officials', dry_run=False) == 0
    assert not c._emitted('delete from')
    assert not c._emitted('snapshot_id')


def test_surviving_rows_abort():
    mod = _load_module()
    c = FakeCursor(before=852, after=3)

    with pytest.raises(SystemExit) as exc:
        mod.purge_table(c, 'fbref_schedule', dry_run=False)

    assert exc.value.code == 2


def test_predicate_pins_league_and_both_seasons():
    mod = _load_module()

    assert "league = 'ENG-Premier League'" in mod.PREDICATE
    assert '2010' in mod.PREDICATE and '2015' in mod.PREDICATE
    # 8 match-level + 9 season-level. The season-level ones matter: without them
    # the next Silver run resurrects a phantom '1516' season in the profiles.
    assert len(mod.TABLES) == 17
    assert 'fbref_player_stats' in mod.TABLES
    assert 'fbref_keeper_keeper' in mod.TABLES
