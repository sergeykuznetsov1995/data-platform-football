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
    # The stub must NOT leak into sys.modules past load — otherwise later tests
    # that import the REAL utils.silver_tasks (e.g. test_silver_tasks,
    # test_dag_transform_sofifa_silver_fallback) pick up this stub and fail with
    # ImportError. Restore sys.modules in a finally so loading is side-effect-free.
    stub = types.ModuleType('utils.silver_tasks')
    stub._get_trino_connection = lambda *a, **k: None  # noqa: E731 — never called here
    _sentinel = object()
    _prev_utils = sys.modules.get('utils', _sentinel)
    _prev_silver = sys.modules.get('utils.silver_tasks', _sentinel)
    sys.modules.setdefault('utils', types.ModuleType('utils'))
    sys.modules['utils.silver_tasks'] = stub

    try:
        spec = importlib.util.spec_from_file_location('audit_bronze_columns', _SCRIPT_PATH)
        assert spec is not None and spec.loader is not None, f'cannot load {_SCRIPT_PATH}'
        mod = importlib.util.module_from_spec(spec)
        sys.modules['audit_bronze_columns'] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        for _name, _prev in (('utils.silver_tasks', _prev_silver), ('utils', _prev_utils)):
            if _prev is _sentinel:
                sys.modules.pop(_name, None)
            else:
                sys.modules[_name] = _prev


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


# --- FotMob contract presence guard (#281) ---------------------------------
# Regression guard: the 9 FotMob bronze tables must stay in the contract so the
# --source fotmob audit keeps verifying full coverage. All 9 materialise and are
# non-empty live (verified 2026-06-04, #281): schedule 760, team_stats 40,
# player_stats 20227, team_profile 20, team_squad 607, team_leaderboards 574,
# transfers 100, match_details 380, player_details 607 rows. 14 columns are
# 100% NULL (10 dead-legacy drift -> followup #304, 4 upstream-missing) and live
# in EXPECTED_NULL so the contract audit stays green.
@pytest.mark.parametrize('table', [
    'fotmob_schedule',
    'fotmob_team_stats',
    'fotmob_player_stats',
    'fotmob_team_profile',
    'fotmob_team_squad',
    'fotmob_team_leaderboards',
    'fotmob_transfers',
    'fotmob_match_details',
    'fotmob_player_details',
])
def test_fotmob_contract_lists_all_nine_tables(table):
    assert table in mod.EXPECTED_TABLES['fotmob']


# --- ClubElo contract presence guard (#283) --------------------------------
# Regression guard: all 3 ClubElo bronze tables must stay in the contract so the
# --source clubelo audit keeps verifying full coverage. All 3 materialise + are
# non-empty live (verified 2026-06-04, #283): ratings 2068, team_history 105600,
# and ratings_historical (~52 weekly snapshots) now produced by the weekly
# dag_ingest_clubelo_full. The two heavy tables use replace_partitions
# (ratings_historical=['rating_date'], team_history=['team']) + weekly cadence,
# neutralizing the daily-APPEND HDFS-overflow footgun (2026-05-04 incident).
@pytest.mark.parametrize('table', [
    'clubelo_ratings',
    'clubelo_ratings_historical',
    'clubelo_team_history',
])
def test_clubelo_contract_lists_all_three_tables(table):
    assert table in mod.EXPECTED_TABLES['clubelo']


# --- MatchHistory contract presence guard (#282) ---------------------------
# Regression guard: единственная MatchHistory bronze-таблица должна оставаться в
# контракте, чтобы --source matchhistory продолжал проверять покрытие. matchhistory_results
# материализуется и не пустая live (verified 2026-06-04, #282): 1439 строк, сезон 2025.
# 0 all-NULL колонок. Известный дрейф: silver читает legacy matchhistory_games (followup).
@pytest.mark.parametrize('table', [
    'matchhistory_results',
])
def test_matchhistory_contract_lists_all_tables(table):
    assert table in mod.EXPECTED_TABLES['matchhistory']


# --- SoFIFA contract presence guard (#284) ---------------------------------
# Regression guard: all 6 SoFIFA bronze tables must stay in the contract so the
# --source sofifa audit keeps verifying full coverage. FlareSolverr v3.4.6
# (Chromium 142) clears the sofifa.com Turnstile — ingest works (the earlier
# #180 CF freeze is resolved). All 6 materialise + non-empty (verified live
# 2026-06-05): FC 26, ENG-Premier League — player_ratings 546, players 546,
# team_ratings 20, teams 20, versions 852, leagues 1. 0 all-NULL outside
# allowlist; 15 sofifa_team_ratings cols (build_up/chance_creation/defence/...)
# are 100% NULL and live in EXPECTED_NULL.
@pytest.mark.parametrize('table', [
    'sofifa_players',
    'sofifa_teams',
    'sofifa_player_ratings',
    'sofifa_team_ratings',
    'sofifa_versions',
    'sofifa_leagues',
])
def test_sofifa_contract_lists_all_six_tables(table):
    assert table in mod.EXPECTED_TABLES['sofifa']


# --- Transfermarkt contract presence guard (#285) --------------------------
# Regression guard: all 3 Transfermarkt bronze tables must stay in the contract
# so the --source transfermarkt audit keeps verifying full coverage. MVP scope =
# ENG-Premier League only (TM_LEAGUE_MAP). All 3 materialise + non-empty live
# (verified 2026-06-05, #285): players 555, market_value_history 2121,
# transfers 750 rows on ('ENG-Premier League','2526'). 0 all-NULL columns ->
# no EXPECTED_NULL entry. Sparse-by-design (NOT drift): transfers.fee_eur 176/750
# (free transfers), transfers.market_value_eur 435/750.
@pytest.mark.parametrize('table', [
    'transfermarkt_players',
    'transfermarkt_market_value_history',
    'transfermarkt_transfers',
])
def test_transfermarkt_contract_lists_all_three_tables(table):
    assert table in mod.EXPECTED_TABLES['transfermarkt']


# --- Capology contract presence guard (#286) -------------------------------
# Regression guard: the 1 Capology bronze table must stay in the contract so
# the --source capology audit keeps verifying full coverage. MVP scope =
# ENG-Premier League only (CAPOLOGY_LEAGUE_MAP), currency='GBP'. All 3 salary
# currencies (gbp/eur/usd) arrive inline in one scraper JS block, so the GBP
# partition carries the full symmetric 30-salary set (10 money bases x 3 cur).
# Verified live 2026-06-05 (#286): 730 rows on ('ENG-Premier League','2526').
# 24/30 salary cols carry data; the 6 `adjusted_total_*` cols are 100% NULL
# because the only materialised partition is the in-progress season ('2526'),
# which Capology has no adjusted totals for yet (completed seasons like '2425'
# do) -> EXPECTED_NULL, followup #319.
@pytest.mark.parametrize('table', [
    'capology_player_salaries',
])
def test_capology_contract_lists_all_tables(table):
    assert table in mod.EXPECTED_TABLES['capology']
