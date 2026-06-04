"""
Unit tests for scripts/audit_bronze_columns.py — contract-diff + EXPECTED_ABSENT.

Strategy
--------
``audit_bronze_columns`` is a top-level script (not a package) that imports
``utils.silver_tasks._get_trino_connection`` at module load. That import only
resolves inside the airflow container, so we inject a stub ``utils.silver_tasks``
into ``sys.modules`` before loading the script via ``importlib.util``. No network.

What we cover
-------------
``diff_contract`` routing (#276):
  - EXPECTED_ABSENT table that is absent  → ``expected_absent`` (not a failure)
  - EXPECTED_ABSENT table present-but-empty → ``expected_absent``
  - a normal absent contract table         → ``missing_tables``
  - a contract column missing from DESCRIBE → ``missing_columns``
  - an ALL_NULL finding from audit_table    → ``all_null_columns``
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / 'scripts' / 'audit_bronze_columns.py'


def _load_module():
    # Stub the container-only import so exec_module succeeds on the host.
    stub = types.ModuleType('utils.silver_tasks')
    stub._get_trino_connection = lambda *a, **k: None  # noqa: E731 — never called here
    sys.modules.setdefault('utils', types.ModuleType('utils'))
    sys.modules['utils.silver_tasks'] = stub

    spec = importlib.util.spec_from_file_location('audit_bronze_columns', _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None, f'cannot load {_SCRIPT_PATH}'
    mod = importlib.util.module_from_spec(spec)
    sys.modules['audit_bronze_columns'] = mod
    spec.loader.exec_module(mod)
    return mod


mod = _load_module()


class _FakeCursor:
    """Minimal cursor: DESCRIBE <table> -> the column list seeded per table."""

    def __init__(self, describe_rows: dict[str, list[tuple[str, str]]]):
        self._describe_rows = describe_rows
        self._last_table: str | None = None

    def execute(self, sql: str):
        # diff_contract only uses the cursor via describe() -> "DESCRIBE iceberg.bronze.<t>"
        self._last_table = sql.rsplit('.', 1)[-1].strip()

    def fetchall(self):
        return self._describe_rows.get(self._last_table, [])


@pytest.fixture
def patched_contract(monkeypatch):
    """A synthetic 'testsrc' contract so tests don't couple to the live fbref one."""
    monkeypatch.setitem(
        mod.EXPECTED_TABLES,
        'testsrc',
        {
            'testsrc_present': {'league', 'season', 'value'},
            'testsrc_gone': {'league', 'season'},
            'testsrc_restricted': {'league', 'season'},
        },
    )
    monkeypatch.setitem(mod.EXPECTED_ABSENT, 'testsrc', {'testsrc_restricted'})


def test_expected_absent_table_absent_is_not_a_failure(patched_contract):
    cur = _FakeCursor({'testsrc_present': [('league', 'varchar'), ('season', 'varchar'),
                                           ('value', 'bigint')]})
    live = {'testsrc_present'}  # both _gone and _restricted absent
    per_table = {'testsrc_present': (10, [])}

    diff = mod.diff_contract(cur, 'testsrc', live, per_table)

    assert ('testsrc_restricted', 'absent — expected (upstream restriction)') \
        in diff['expected_absent']
    assert ('testsrc_gone', 'absent from bronze') in diff['missing_tables']
    # restricted must NOT leak into missing_tables
    assert all(t != 'testsrc_restricted' for t, _ in diff['missing_tables'])


def test_expected_absent_present_but_empty_is_ok(patched_contract):
    cur = _FakeCursor({'testsrc_present': [('league', 'varchar'), ('season', 'varchar'),
                                           ('value', 'bigint')]})
    live = {'testsrc_present', 'testsrc_restricted', 'testsrc_gone'}
    per_table = {
        'testsrc_present': (10, []),
        'testsrc_restricted': (0, []),  # materialised but empty
        'testsrc_gone': (0, []),        # empty + NOT in EXPECTED_ABSENT
    }

    diff = mod.diff_contract(cur, 'testsrc', live, per_table)

    assert ('testsrc_restricted', 'present but empty — expected') in diff['expected_absent']
    assert ('testsrc_gone', 'present but empty (0 rows)') in diff['missing_tables']


def test_missing_column_and_all_null_passthrough(patched_contract):
    # 'value' is dropped from the live DESCRIBE -> missing column.
    cur = _FakeCursor({'testsrc_present': [('league', 'varchar'), ('season', 'varchar')]})
    live = {'testsrc_present'}
    per_table = {
        'testsrc_present': (
            10,
            [{'table': 'testsrc_present', 'col': 'value', 'sev': 'ERROR',
              'detail': 'ALL_NULL — 0 of 10 non-NULL (bigint)'}],
        ),
    }

    diff = mod.diff_contract(cur, 'testsrc', live, per_table)

    assert ('testsrc_present', 'value') in diff['missing_columns']
    assert ('testsrc_present', 'value',
            'ALL_NULL — 0 of 10 non-NULL (bigint)') in diff['all_null_columns']


# --- Understat contract presence guard (#277) ------------------------------
# Regression guard: the 5 Understat bronze tables must stay in the contract so
# the --source understat audit keeps verifying full coverage.
@pytest.mark.parametrize('table', [
    'understat_schedule',
    'understat_shots',
    'understat_players',
    'understat_team_match_stats',
    'understat_player_match_stats',
])
def test_understat_contract_lists_all_five_tables(table):
    assert table in mod.EXPECTED_TABLES['understat']


# --- WhoScored contract presence guard (#278) ------------------------------
# Regression guard: the 4 WhoScored bronze tables must stay in the contract so
# the --source whoscored audit keeps verifying full coverage. whoscored_season_stages
# is gated by EXPECTED_ABSENT (top-5 leagues have no cup stages) but stays in the
# contract for completeness.
@pytest.mark.parametrize('table', [
    'whoscored_schedule',
    'whoscored_events',
    'whoscored_missing_players',
    'whoscored_season_stages',
])
def test_whoscored_contract_lists_all_four_tables(table):
    assert table in mod.EXPECTED_TABLES['whoscored']


# --- ESPN contract presence guard (#279, #298) ------------------------------
# Regression guard: the 3 ESPN bronze tables must stay in the contract so the
# --source espn audit keeps verifying full coverage. espn_standings is NOT in
# the contract — soccerdata's ESPN reader has no read_standings (scraper.py:112
# returns None), so the table never materialises (would be a permanent
# false-positive). espn_matchsheet was legacy/ad-hoc but its write-path is now
# formalized via scripts/backfill_espn_e3_5.py (soccerdata read_matchsheet, #298).
@pytest.mark.parametrize('table', [
    'espn_schedule',
    'espn_lineup',
    'espn_matchsheet',
])
def test_espn_contract_lists_all_tables(table):
    assert table in mod.EXPECTED_TABLES['espn']


def test_espn_standings_excluded_from_contract():
    assert 'espn_standings' not in mod.EXPECTED_TABLES['espn']


# --- SofaScore contract presence guard (#280) ------------------------------
# Regression guard: the 8 SofaScore bronze tables must stay in the contract so
# the --source sofascore audit keeps verifying full coverage. All 8 materialise
# and are non-empty live (verified 2026-06-04, #280): the 2 soccerdata tables
# (schedule, league_table) + 6 cherry-pick JSON-API tables.
@pytest.mark.parametrize('table', [
    'sofascore_schedule',
    'sofascore_league_table',
    'sofascore_player_ratings',
    'sofascore_player_season_stats',
    'sofascore_player_profile',
    'sofascore_event_shotmap',
    'sofascore_event_player_stats',
    'sofascore_match_stats',
])
def test_sofascore_contract_lists_all_eight_tables(table):
    assert table in mod.EXPECTED_TABLES['sofascore']
