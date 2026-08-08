"""Contract tests for the ESPN native-v2 Silver DQ builder."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit


def test_standard_checks_pin_keys_floors_and_never_add_freshness():
    from utils.espn_silver_dq import SILVER_MIN_ROWS, build_espn_silver_checks

    assert SILVER_MIN_ROWS == {
        "espn_match": 20_000,
        "espn_team_match": 14_000,
        "espn_player_match_aggregate": 300_000,
        "espn_match_events": 50_000,
        "espn_substitutions": 65_000,
        "espn_venue": 1_000,
    }
    checks = build_espn_silver_checks()
    assert all(check.kind != "freshness" for check in checks)
    by_name = {check.name: check for check in checks}
    assert by_name["pk[silver.espn_match]"].params["pk"] == ["event_id"]
    assert by_name["pk[silver.espn_player_match_aggregate]"].params["pk"] == [
        "event_id", "team_id", "athlete_id"
    ]
    assert by_name["required[silver.espn_match]"].params["cols"] == [
        "event_id", "league", "season", "kickoff", "status", "home_team_id", "away_team_id"
    ]
    assert by_name["score_present_for_played"].params["where"] == "is_played = true"
    assert by_name["row_floor[silver.espn_substitutions]"].params["min_rows"] == 65_000


def test_standard_checks_cover_child_references_played_constraint_and_ranges():
    from utils.espn_silver_dq import build_espn_silver_checks

    checks = build_espn_silver_checks()
    refs = [check for check in checks if check.kind == "ref_integrity"]
    assert {(check.params["child"], check.params["parent"], check.params["key"])
            for check in refs} == {
        ("silver.espn_team_match", "silver.espn_match", "event_id"),
        ("silver.espn_player_match_aggregate", "silver.espn_match", "event_id"),
        ("silver.espn_match_events", "silver.espn_match", "event_id"),
        ("silver.espn_substitutions", "silver.espn_match", "event_id"),
    }
    assert {check.name for check in checks if check.kind == "value_range"} == {
        "range[team possession]", "range[event minute]", "range[event plus_minute]"
    }


def test_custom_sql_pins_allowlist_thresholds_and_warning_only_severities():
    from utils.espn_silver_dq import (
        LINEUP_ZERO_COVERAGE_ALLOWLIST,
        build_espn_silver_custom_checks,
    )

    assert LINEUP_ZERO_COVERAGE_ALLOWLIST == frozenset({
        "arg.3", "caf.championship_qual", "fifa.conmebol.olympicsq",
        "ned.3.promotion.relegation", "global.gulf_cup", "caf.nations_qual",
        "slv.1", "sco.tennents_qual", "hon.1", "rus.1.promotion.relegation",
        "chi.super_cup", "bol.ply.rel", "arg.trofeo_de_la_campeones",
    })
    checks = {check.name: check for check in build_espn_silver_custom_checks()}
    assert checks["coverage[played lineup >=80%]"].severity == "WARNING"
    assert checks["coverage[played team stats >=85%]"].severity == "WARNING"
    assert checks["coverage[played events >=85%]"].severity == "WARNING"
    assert checks["coverage[played referee >=15%]"].severity == "WARNING"
    assert checks["scoreboard totalGoals parity"].severity == "WARNING"
    assert checks["children are played"].severity == "ERROR"
    assert checks["manifest played-lineup disposition"].severity == "ERROR"
    assert checks["winner parity"].severity == "ERROR"
    assert checks["coverage[played lineup >=80%]"].threshold == 0.80
    assert checks["coverage[played team stats >=85%]"].threshold == 0.85
    assert checks["played 0:0 share <=15%"].threshold == 0.15
    assert "STATUS_FINAL_PEN" in checks["goals events <= score + 2"].sql
    assert "totalGoals" in checks["scoreboard totalGoals parity"].sql
    assert "dispositions_json" in checks["manifest played-lineup disposition"].sql


def test_custom_results_parse_safe_rows_and_preserve_warning_severity(monkeypatch):
    from utils import espn_silver_dq as dq

    class Cursor:
        def execute(self, _sql):
            pass

        def fetchone(self):
            return ("2", "10")

        def close(self):
            pass

    class Conn:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    monkeypatch.setattr(dq, "_get_conn", lambda: Conn())
    result = dq.run_espn_silver_custom_check(
        dq.CustomCheck("warn", "SELECT 1", "WARNING", lambda row: int(row[0]) == 0)
    )
    assert result.passed is False
    assert result.severity == "WARNING"
    assert result.value == (2, 10)
