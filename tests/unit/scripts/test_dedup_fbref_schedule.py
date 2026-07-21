"""
Unit tests for scripts/dedup_fbref_schedule.py — all-NULL row purge (#892).

Strategy
--------
The script is a top-level module whose only non-stdlib import (``trino``) lives lazily inside
``get_conn``, so it loads on the host with no stubbing. ``purge_null_url_rows``
runs against a ``FakeCursor`` that records SQL and answers the two COUNT reads.

What we cover
-------------
- happy path: emits exactly one DELETE, never a DROP TABLE (the old CTAS +
  DROP + RENAME silently destroyed columns added after the script was written,
  e.g. ``round`` — that is the regression this file guards).
- already-clean table (no NULL match_url) -> early return, no DELETE.
- dry-run: reports, emits no DELETE.
- duplicate match_urls present -> SystemExit(3), no DELETE (choosing a survivor
  needs a score-aware rule, see the script docstring).
- empty table -> SystemExit(1).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / 'scripts' / 'dedup_fbref_schedule.py'


def _load_module():
    spec = importlib.util.spec_from_file_location('dedup_fbref_schedule', _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None, f'cannot load {_SCRIPT_PATH}'
    mod = importlib.util.module_from_spec(spec)
    sys.modules['dedup_fbref_schedule'] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    """Answers the script's COUNT reads; records every statement.

    The first read is ``COUNT(*), COUNT(match_url), COUNT(DISTINCT match_url)``.
    A later bare ``COUNT(*)`` (post-delete verification) returns the row count
    that survives the purge.
    """

    def __init__(self, total: int, with_url: int, uniq: int):
        self.total = total
        self.with_url = with_url
        self.uniq = uniq
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
        if 'count(distinct match_url)' in s:
            return [(self.total, self.with_url, self.uniq)]
        if 'count(*)' in s:
            return [(self.with_url if self._deleted else self.total,)]
        return []

    def _emitted(self, needle: str) -> list[str]:
        return [q for q in self.sql_log if needle in q.lower()]


# Live shape on 2026-07-08: 2 146 all-NULL rows, zero duplicate urls.
def _live_cursor():
    return FakeCursor(total=20981, with_url=18835, uniq=18835)


def test_happy_path_deletes_null_url_rows_without_dropping_the_table():
    mod = _load_module()
    c = _live_cursor()

    mod.purge_null_url_rows(c, dry_run=False)

    deletes = c._emitted('delete from')
    assert len(deletes) == 1
    assert 'match_url is null' in deletes[0].lower()
    # The regression this test exists for: no table rebuild, so no column can
    # be lost to an out-of-date SELECT list.
    assert not c._emitted('drop table')
    assert not c._emitted('create table')
    assert not c._emitted('rename to')


def test_already_clean_table_is_a_noop():
    mod = _load_module()
    c = FakeCursor(total=18835, with_url=18835, uniq=18835)

    mod.purge_null_url_rows(c, dry_run=False)

    assert not c._emitted('delete from')


def test_dry_run_emits_no_delete():
    mod = _load_module()
    c = _live_cursor()

    mod.purge_null_url_rows(c, dry_run=True)

    assert not c._emitted('delete from')


def test_duplicate_match_urls_abort_without_deleting():
    mod = _load_module()
    # 100 rows share a url with another row -> dedup, not our job.
    c = FakeCursor(total=20981, with_url=18835, uniq=18735)

    with pytest.raises(SystemExit) as exc:
        mod.purge_null_url_rows(c, dry_run=False)

    assert exc.value.code == 3
    assert not c._emitted('delete from')


def test_empty_table_aborts():
    mod = _load_module()
    c = FakeCursor(total=0, with_url=0, uniq=0)

    with pytest.raises(SystemExit) as exc:
        mod.purge_null_url_rows(c, dry_run=False)

    assert exc.value.code == 1
    assert not c._emitted('delete from')
