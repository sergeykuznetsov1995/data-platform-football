"""
Unit tests for scripts/audit_bronze_columns.py — contract-diff + EXPECTED_ABSENT.

Strategy
--------
``audit_bronze_columns`` is a top-level script (not a package), so we load it
via ``importlib.util``.  The root ``conftest.py`` exposes ``dags/`` and the test
runtime provides Trino, allowing its real ``utils.silver_tasks`` dependency to
load without opening a connection. No network.

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
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT_PATH = REPO_ROOT / "scripts" / "audit_bronze_columns.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("audit_bronze_columns", _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None, f"cannot load {_SCRIPT_PATH}"
    mod = importlib.util.module_from_spec(spec)
    sys.modules["audit_bronze_columns"] = mod
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
        self._last_table = sql.rsplit(".", 1)[-1].strip()

    def fetchall(self):
        return self._describe_rows.get(self._last_table, [])


@pytest.fixture
def patched_contract(monkeypatch):
    """A synthetic 'testsrc' contract so tests don't couple to the live fbref one."""
    monkeypatch.setitem(
        mod.EXPECTED_TABLES,
        "testsrc",
        {
            "testsrc_present": {"league", "season", "value"},
            "testsrc_gone": {"league", "season"},
            "testsrc_restricted": {"league", "season"},
        },
    )
    monkeypatch.setitem(mod.EXPECTED_ABSENT, "testsrc", {"testsrc_restricted"})
    monkeypatch.setitem(mod.SOURCE_PREFIXES, "testsrc", "testsrc_")


def test_expected_absent_table_absent_is_not_a_failure(patched_contract):
    cur = _FakeCursor(
        {
            "testsrc_present": [
                ("league", "varchar"),
                ("season", "varchar"),
                ("value", "bigint"),
            ]
        }
    )
    live = {"testsrc_present"}  # both _gone and _restricted absent
    per_table = {"testsrc_present": (10, [])}

    diff = mod.diff_contract(cur, "testsrc", live, per_table)

    assert ("testsrc_restricted", "absent — expected (upstream restriction)") in diff[
        "expected_absent"
    ]
    assert ("testsrc_gone", "absent from bronze") in diff["missing_tables"]
    # restricted must NOT leak into missing_tables
    assert all(t != "testsrc_restricted" for t, _ in diff["missing_tables"])


def test_expected_absent_present_but_empty_is_ok(patched_contract):
    cur = _FakeCursor(
        {
            "testsrc_present": [
                ("league", "varchar"),
                ("season", "varchar"),
                ("value", "bigint"),
            ]
        }
    )
    live = {"testsrc_present", "testsrc_restricted", "testsrc_gone"}
    per_table = {
        "testsrc_present": (10, []),
        "testsrc_restricted": (0, []),  # materialised but empty
        "testsrc_gone": (0, []),  # empty + NOT in EXPECTED_ABSENT
    }

    diff = mod.diff_contract(cur, "testsrc", live, per_table)

    assert ("testsrc_restricted", "present but empty — expected") in diff[
        "expected_absent"
    ]
    assert ("testsrc_gone", "present but empty (0 rows)") in diff["missing_tables"]


def test_missing_column_and_all_null_passthrough(patched_contract):
    # 'value' is dropped from the live DESCRIBE -> missing column.
    cur = _FakeCursor(
        {"testsrc_present": [("league", "varchar"), ("season", "varchar")]}
    )
    live = {"testsrc_present"}
    per_table = {
        "testsrc_present": (
            10,
            [
                {
                    "table": "testsrc_present",
                    "col": "value",
                    "sev": "ERROR",
                    "detail": "ALL_NULL — 0 of 10 non-NULL (bigint)",
                }
            ],
        ),
    }

    diff = mod.diff_contract(cur, "testsrc", live, per_table)

    assert ("testsrc_present", "value") in diff["missing_columns"]
    assert ("testsrc_present", "value", "ALL_NULL — 0 of 10 non-NULL (bigint)") in diff[
        "all_null_columns"
    ]


class _MainCursor(_FakeCursor):
    def __init__(self, tables, describe_rows):
        super().__init__(describe_rows)
        self._tables = tables
        self._show_tables = False

    def execute(self, sql: str):
        self._show_tables = sql == "SHOW TABLES FROM iceberg.bronze"
        if not self._show_tables:
            super().execute(sql)

    def fetchall(self):
        if self._show_tables:
            return [(table,) for table in self._tables]
        return super().fetchall()


class _MainConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_main_returns_nonzero_for_machine_readable_contract_failure(
    patched_contract, monkeypatch, tmp_path
):
    cursor = _MainCursor([], {})
    monkeypatch.setattr(mod, "_get_trino_connection", lambda: _MainConnection(cursor))
    monkeypatch.setattr(mod, "audit_table", lambda *_args: (1, []))

    status = mod.main(["--source", "testsrc", "--output", str(tmp_path / "failed.md")])

    assert status != 0


def test_main_returns_zero_only_for_clean_contract(
    patched_contract, monkeypatch, tmp_path
):
    describe_rows = {
        "testsrc_present": [
            ("league", "varchar"),
            ("season", "varchar"),
            ("value", "bigint"),
        ],
        "testsrc_gone": [("league", "varchar"), ("season", "varchar")],
        "testsrc_restricted": [("league", "varchar"), ("season", "varchar")],
    }
    cursor = _MainCursor(sorted(describe_rows), describe_rows)
    monkeypatch.setattr(mod, "_get_trino_connection", lambda: _MainConnection(cursor))
    monkeypatch.setattr(mod, "audit_table", lambda *_args: (1, []))

    status = mod.main(["--source", "testsrc", "--output", str(tmp_path / "clean.md")])

    assert status == 0


# --- Understat contract presence guard (#277) ------------------------------
# Regression guard: all 7 native Understat Bronze tables must stay in the contract so
# the --source understat audit keeps verifying full coverage.
@pytest.mark.parametrize(
    "table",
    [
        "understat_schedule",
        "understat_shots",
        "understat_players",
        "understat_team_match_stats",
        "understat_player_match_stats",
        "understat_player_team_season_stats",
        "understat_team_season_breakdowns",
    ],
)
def test_understat_contract_lists_all_seven_tables(table):
    assert table in mod.EXPECTED_TABLES["understat"]


def test_understat_audit_contract_is_loaded_from_native_registry():
    expected = {
        contract.table_name: set(contract.required_columns) | mod.META_COLS
        for contract in mod.UNDERSTAT_CONTRACT.TABLE_CONTRACTS
    }

    assert mod.EXPECTED_TABLES["understat"] == expected


# --- WhoScored contract presence guard (#278) ------------------------------
# The audit consumes the same dependency-free inventory as migration/cleanup:
# 25 source datasets and five logical-commit manifests.  Deprecated parser-v2
# tables must not remain as an "expected absent" escape hatch.
def test_whoscored_contract_lists_complete_v2_inventory():
    expected = set(mod.WHOSCORED_CONTRACT.BUSINESS_TABLES) | set(
        mod.WHOSCORED_CONTRACT.MANIFEST_TABLES
    )

    assert set(mod.EXPECTED_TABLES["whoscored"]) == expected
    assert len(mod.WHOSCORED_CONTRACT.BUSINESS_TABLES) == 25
    assert "whoscored_season_stages" not in expected
    assert "whoscored_player_profile" not in expected


def test_whoscored_contract_requires_logical_commit_column_for_every_dataset():
    contract = mod.EXPECTED_TABLES["whoscored"]

    for table, batch_column in mod.WHOSCORED_CONTRACT.BATCH_COLUMN_BY_TABLE.items():
        assert batch_column in contract[table]
        assert mod.META_COLS <= contract[table]


# --- ESPN native-v2 contract guard -------------------------------------------
def test_espn_contract_is_loaded_from_dependency_free_v2_inventory():
    assert mod.EXPECTED_TABLES["espn"] == {
        table: set(columns)
        for table, columns in mod.ESPN_CONTRACT.REQUIRED_COLUMNS.items()
    }


def test_espn_contract_lists_legacy_native_current_and_control_objects():
    assert set(mod.EXPECTED_TABLES["espn"]) == (
        set(mod.ESPN_CONTRACT.LEGACY_TABLES)
        | set(mod.ESPN_CONTRACT.GENERATION_TABLES)
        | set(mod.ESPN_CONTRACT.CURRENT_VIEWS)
        | set(mod.ESPN_CONTRACT.CONTROL_TABLES)
    )


def test_espn_standings_excluded_from_contract():
    assert "espn_standings" not in mod.EXPECTED_TABLES["espn"]


def test_capability_gated_empty_espn_entity_is_structurally_audited(monkeypatch):
    table = "espn_lineup_generation_v2"
    required = mod.EXPECTED_TABLES["espn"][table]
    cursor = _FakeCursor({table: [(column, "varchar") for column in required]})

    diff = mod.diff_contract(cursor, "espn", {table}, {table: (0, [])})

    assert (table, "present but empty — capability-gated") in diff["expected_empty"]
    assert all(item[0] != table for item in diff["missing_tables"])
    assert all(item[0] != table for item in diff["missing_columns"])


# --- SofaScore contract presence guard (#280) ------------------------------
# Regression guard: the 8 SofaScore bronze tables must stay in the contract so
# the --source sofascore audit keeps verifying full coverage. All 8 materialise
# and are non-empty live (verified 2026-06-04, #280): the 2 soccerdata tables
# (schedule, league_table) + 6 cherry-pick JSON-API tables.
@pytest.mark.parametrize(
    "table",
    [
        "sofascore_schedule",
        "sofascore_league_table",
        "sofascore_player_ratings",
        "sofascore_player_season_stats",
        "sofascore_player_profile",
        "sofascore_event_shotmap",
        "sofascore_event_player_stats",
        "sofascore_match_stats",
    ],
)
def test_sofascore_contract_lists_all_eight_tables(table):
    assert table in mod.EXPECTED_TABLES["sofascore"]


# --- FotMob contract presence guard (#281) ---------------------------------
# Regression guard: the 9 FotMob bronze tables must stay in the contract so the
# --source fotmob audit keeps verifying full coverage. All 9 materialise and are
# non-empty live (verified 2026-06-04, #281): schedule 760, team_stats 40,
# player_stats 20227, team_profile 20, team_squad 607, team_leaderboards 574,
# transfers 100, match_details 380, player_details 607 rows. 14 columns are
# 100% NULL (10 dead-legacy drift -> followup #304, 4 upstream-missing) and live
# in EXPECTED_NULL so the contract audit stays green.
@pytest.mark.parametrize(
    "table",
    [
        "fotmob_schedule",
        "fotmob_team_stats",
        "fotmob_player_stats",
        "fotmob_team_profile",
        "fotmob_team_squad",
        "fotmob_team_leaderboards",
        "fotmob_transfers",
        "fotmob_match_details",
        "fotmob_player_details",
    ],
)
def test_fotmob_contract_lists_all_nine_tables(table):
    assert table in mod.EXPECTED_TABLES["fotmob"]


# --- ClubElo contract presence guard (#283) --------------------------------
# Regression guard: both ClubElo bronze tables must stay in the contract so the
# --source clubelo audit keeps verifying full coverage. Both materialise + are
# non-empty live (verified 2026-06-04, #283): ratings 2068 and
# ratings_historical (weekly snapshots) produced by the gated weekly branch of
# dag_ingest_clubelo (folded in from dag_ingest_clubelo_full, #716) with
# replace_partitions=['rating_date'] + weekly cadence, neutralizing the
# daily-APPEND HDFS-overflow footgun (2026-05-04 incident). clubelo_team_history
# was dropped in #604 (write-only, never read).
@pytest.mark.parametrize(
    "table",
    [
        "clubelo_ratings",
        "clubelo_ratings_historical",
    ],
)
def test_clubelo_contract_lists_both_tables(table):
    assert table in mod.EXPECTED_TABLES["clubelo"]


# --- MatchHistory contract presence guard (#282) ---------------------------
# Regression guard: единственная MatchHistory bronze-таблица должна оставаться в
# контракте, чтобы --source matchhistory продолжал проверять покрытие. matchhistory_results
# материализуется и не пустая live (verified 2026-06-04, #282): сезоны 2021-2025, ~380/сезон.
# 0 all-NULL колонок. #307 RESOLVED: silver мигрирован на matchhistory_results, games дропнут.
@pytest.mark.parametrize(
    "table",
    [
        "matchhistory_results",
    ],
)
def test_matchhistory_contract_lists_all_tables(table):
    assert table in mod.EXPECTED_TABLES["matchhistory"]


# --- SoFIFA contract presence guard (#284) ---------------------------------
# Regression guard: all 6 SoFIFA bronze tables must stay in the contract so the
# --source sofifa audit keeps verifying full coverage. FlareSolverr v3.4.6
# (Chromium 142) clears the sofifa.com Turnstile — ingest works (the earlier
# #180 CF freeze is resolved). All 6 materialise + non-empty (verified live
# 2026-06-05): FC 26, ENG-Premier League — player_ratings 546, players 546,
# team_ratings 20, teams 20, versions 852, leagues 1. The 15 dead FC-26
# sofifa_team_ratings cols (build_up/chance_creation/defence/...) were removed
# from the parser + Bronze (#601), so there is no longer an EXPECTED_NULL entry.
@pytest.mark.parametrize(
    "table",
    [
        "sofifa_players",
        "sofifa_teams",
        "sofifa_player_ratings",
        "sofifa_team_ratings",
        "sofifa_versions",
        "sofifa_leagues",
    ],
)
def test_sofifa_contract_lists_all_six_tables(table):
    assert table in mod.EXPECTED_TABLES["sofifa"]


# --- Transfermarkt native-v2 + transition contract presence guard ----------
# Four legacy tables stay audited for the dual-write rollback window; six
# native tables encode honest season/global grains.
@pytest.mark.parametrize(
    "table",
    [
        "transfermarkt_players",
        "transfermarkt_market_value_history",
        "transfermarkt_transfers",
        "transfermarkt_coaches",
        "transfermarkt_squad_memberships",
        "transfermarkt_player_attribute_observations",
        "transfermarkt_player_contract_observations",
        "transfermarkt_market_value_points",
        "transfermarkt_transfer_events",
        "transfermarkt_coach_profiles",
        "transfermarkt_coach_stints",
        "transfermarkt_competitions",
        "transfermarkt_competition_editions",
    ],
)
def test_transfermarkt_contract_lists_transition_and_native_tables(table):
    assert table in mod.EXPECTED_TABLES["transfermarkt"]


# --- Capology contract presence guard (#321) -------------------------------
# Regression guard: all 4 Capology APL data products must stay in the contract
# so the --source capology audit keeps verifying full coverage. MVP scope =
# ENG-Premier League only (CAPOLOGY_LEAGUE_MAP). Verified live 2026-06-05
# across seasons 2324/2425/2526: salaries 2038, contract_extensions 258,
# team_payrolls 60, transfer_window 60 rows; 0 all-NULL columns (completed-
# season backfill populates adjusted_total_*, resolving #319) -> no
# EXPECTED_NULL entry. Positional payroll split d/f/k/m is Capology-Pro-locked
# upstream and intentionally not ingested.
@pytest.mark.parametrize(
    "table",
    [
        "capology_player_salaries",
        "capology_team_payrolls",
        "capology_contract_extensions",
        "capology_transfer_window",
    ],
)
def test_capology_contract_lists_all_four_tables(table):
    assert table in mod.EXPECTED_TABLES["capology"]
