"""Unit tests for scripts/clubelo_freeze_archive.py (#1460) — no Trino, no network."""

from __future__ import annotations

import ast
import datetime as dt
import importlib.util
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / 'scripts' / 'clubelo_freeze_archive.py'
INGESTED = dt.datetime(2026, 9, 24, 19, 0, 0, 123456)


def _load_module():
    spec = importlib.util.spec_from_file_location('clubelo_freeze_archive', _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules['clubelo_freeze_archive'] = mod
    spec.loader.exec_module(mod)
    return mod


CSV = (
    'Rank,Club,Country,Level,Elo,From,To\n'
    '1,Liverpool,ENG,1,1993.43103027,2025-05-29,2025-08-15\n'
    "None,Nott'm Forest,ENG,2,1602.5,2025-06-01,\n"
    ',Leganes,ESP,2,1599.01965332,2025-07-06,2026-12-31\n'
)


@pytest.fixture
def csv_dir(tmp_path):
    (tmp_path / '2025-07-13.csv').write_text(CSV)
    (tmp_path / '2025-07-20.csv').write_text(CSV)
    return tmp_path


def test_parse_csv_rows(csv_dir):
    mod = _load_module()
    files, rows = mod.parse_csv_dir(csv_dir, INGESTED)
    assert [f.name for f in files] == ['2025-07-13.csv', '2025-07-20.csv']
    assert len(rows) == 6
    r = dict(zip(mod.SNAPSHOT_COLUMNS, rows[0]))
    assert r['rank'] == 1 and r['club'] == 'Liverpool' and r['level'] == 1
    assert r['elo'] == pytest.approx(1993.43103027)
    assert r['valid_from'] == dt.date(2025, 5, 29) and r['valid_to'] == dt.date(2025, 8, 15)
    assert r['rating_date'] == '2025-07-13'
    assert r['source_file'] == '2025-07-13.csv'
    assert r['model_version'] == 'api-legacy'
    assert r['_source'] == 'clubelo_api_cache'
    assert r['_batch_id'] == 'clubelo-archive-20260924'
    assert r['_ingested_at'] == INGESTED
    # "None" and empty Rank -> NULL; empty To -> NULL; 2026-12-31 placeholder kept.
    r2 = dict(zip(mod.SNAPSHOT_COLUMNS, rows[1]))
    r3 = dict(zip(mod.SNAPSHOT_COLUMNS, rows[2]))
    assert r2['rank'] is None and r2['valid_to'] is None
    assert r3['rank'] is None and r3['valid_to'] == dt.date(2026, 12, 31)
    assert rows[3][mod.SNAPSHOT_COLUMNS.index('rating_date')] == '2025-07-20'


def test_parse_rejects_unexpected_header(tmp_path):
    mod = _load_module()
    (tmp_path / '2025-07-13.csv').write_text('Club,Elo\nX,1\n')
    with pytest.raises(ValueError):
        mod.parse_csv_dir(tmp_path, INGESTED)


def test_insert_statements_literals_and_chunks(csv_dir):
    mod = _load_module()
    _, rows = mod.parse_csv_dir(csv_dir, INGESTED)
    stmts = mod.insert_statements(rows * 200)  # 1200 rows -> 500/500/200
    assert len(stmts) == 3
    first = stmts[0]
    assert first.startswith('INSERT INTO iceberg.bronze.clubelo_api_snapshot_archive (')
    assert "'Nott''m Forest'" in first
    assert "DATE '2025-05-29'" in first and '1993.43103027E0' in first
    assert "TIMESTAMP '2026-09-24 19:00:00.123456'" in first
    assert 'NULL' in first


def test_copy_dry_run_prints_three_ctas_without_connecting(capsys, monkeypatch):
    mod = _load_module()
    monkeypatch.setattr(mod, '_connect', lambda: pytest.fail('dry-run must not connect'))
    assert mod.main(['copy', '--dry-run']) == 0
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        f'CREATE TABLE IF NOT EXISTS iceberg.bronze.{t}_archive_20260924 '
        f'AS SELECT * FROM iceberg.bronze.{t};'
        for t in ('clubelo_ratings', 'clubelo_ratings_historical', 'clubelo_team_history')
    ]


def test_load_csv_dry_run_does_not_connect(csv_dir, capsys, monkeypatch):
    mod = _load_module()
    monkeypatch.setattr(mod, '_connect', lambda: pytest.fail('dry-run must not connect'))
    assert mod.main(['load-csv', '--dir', str(csv_dir), '--dry-run']) == 0
    out = capsys.readouterr().out
    assert 'files=2 rows=6' in out
    assert 'CREATE TABLE IF NOT EXISTS iceberg.bronze.clubelo_api_snapshot_archive' in out


def test_source_has_no_destructive_sql_and_inserts_only_into_snapshot():
    text = _SCRIPT_PATH.read_text()
    assert not re.search(r'\b(DROP|DELETE|ALTER|UPDATE|TRUNCATE|MERGE)\b', text, re.I)
    targets = re.findall(r'INSERT\s+INTO\s+(\S+)', text, re.I)
    assert targets and set(targets) == {'{SNAPSHOT_TABLE}'}
    mod = _load_module()
    assert mod.SNAPSHOT_TABLE == 'iceberg.bronze.clubelo_api_snapshot_archive'
    doc_end = ast.parse(text).body[0].end_lineno  # skip the module docstring
    code = '\n'.join(text.splitlines()[doc_end:])
    creates = re.findall(r'CREATE TABLE IF NOT EXISTS (\S+)', code)
    assert set(creates) == {'{SCHEMA}.{t}{COPY_SUFFIX}', '{SNAPSHOT_TABLE}'}
    assert 'CREATE TABLE ' not in text.replace('CREATE TABLE IF NOT EXISTS', '')


class _Cur:
    def __init__(self, existing):
        self.existing = existing
        self.sql = []
        self._last = None

    def execute(self, sql):
        self.sql.append(sql)
        self._last = sql

    def fetchall(self):
        if 'count(DISTINCT rating_date)' in self._last:
            return [(6, 2)]
        if 'SELECT count(*)' in self._last:
            return [(self.existing,)]
        return []


def test_load_refuses_non_empty_target(csv_dir):
    mod = _load_module()
    files, rows = mod.parse_csv_dir(csv_dir, INGESTED)
    cur = _Cur(existing=5)
    assert mod.load_csv(cur, rows, len(files)) == 1
    assert not [s for s in cur.sql if s.startswith('INSERT')]


def test_load_inserts_into_empty_target(csv_dir):
    mod = _load_module()
    files, rows = mod.parse_csv_dir(csv_dir, INGESTED)
    cur = _Cur(existing=0)
    assert mod.load_csv(cur, rows, len(files)) == 0
    assert len([s for s in cur.sql if s.startswith('INSERT')]) == 1
