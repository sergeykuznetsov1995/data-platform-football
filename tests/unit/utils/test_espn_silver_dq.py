"""Contract tests for the ESPN native-v2 Silver DQ builder."""

from __future__ import annotations

import pytest
from sqlglot import exp, parse_one

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


def _checks_by_name():
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
    return {check.name: check for check in build_espn_silver_custom_checks()}


def test_custom_checks_pin_exact_severity_map_and_thresholds():
    checks = _checks_by_name()
    assert {name: check.severity for name, check in checks.items()} == {
        "children are played": "ERROR",
        "coverage[played lineup >=80%]": "WARNING",
        "coverage[played team stats >=85%]": "WARNING",
        "coverage[played events >=85%]": "WARNING",
        "lineup zero coverage outside allowlist": "WARNING",
        "0:0 with goal event": "WARNING",
        "played 0:0 share <=15%": "WARNING",
        "goals events <= score + 2": "WARNING",
        "coverage[played referee >=15%]": "WARNING",
        "manifest played-lineup disposition": "WARNING",
        "winner parity": "WARNING",
        "scoreboard totalGoals parity": "WARNING",
    }
    assert checks["coverage[played lineup >=80%]"].threshold == 0.80
    assert checks["coverage[played team stats >=85%]"].threshold == 0.85
    assert checks["coverage[played events >=85%]"].threshold == 0.85
    assert checks["played 0:0 share <=15%"].threshold == 0.15


@pytest.mark.parametrize(
    ("name", "passing", "failing", "requires_distinct_played_match_denominator"),
    [
        ("coverage[played lineup >=80%]", (8, 10), (7, 10), True),
        ("coverage[played team stats >=85%]", (17, 20), (16, 20), True),
        ("coverage[played events >=85%]", (17, 20), (16, 20), True),
        ("played 0:0 share <=15%", (3, 20), (4, 20), False),
        ("coverage[played referee >=15%]", (3, 20), (2, 20), False),
    ],
)
def test_ratio_predicates_use_covered_and_distinct_played_match_denominators(
    name, passing, failing, requires_distinct_played_match_denominator
):
    check = _checks_by_name()[name]

    assert check.passed(passing) is True
    assert check.passed(failing) is False
    assert check.passed((0, 0)) is False

    if requires_distinct_played_match_denominator:
        tree = parse_one(check.sql, read="trino")
        counts = list(tree.find_all(exp.Count))
        denominator = next(
            count for count in counts
            if isinstance(count.this, exp.Distinct)
            and {column.name for column in count.this.find_all(exp.Column)} == {"event_id"}
            and any(column.table == "m" for column in count.this.find_all(exp.Column))
        )
        assert isinstance(denominator.this, exp.Distinct)


def test_every_custom_query_parses_as_trino_and_latest_ctes_precede_unnest():
    checks = _checks_by_name()

    for check in checks.values():
        assert parse_one(check.sql, read="trino") is not None

    for name in ("manifest played-lineup disposition", "winner parity", "scoreboard totalGoals parity"):
        tree = parse_one(checks[name].sql, read="trino")
        ctes = {cte.alias_or_name: cte for cte in tree.find_all(exp.CTE)}
        source_name = "bronze_src_manifest" if name.startswith("manifest") else "bronze_src_schedule"
        assert source_name in ctes
        assert any("dedup" in cte_name for cte_name in ctes)
        windows = list(tree.find_all(exp.Window))
        assert windows
        assert any(
            isinstance(window.this, exp.RowNumber)
            and any(column.name == "event_id" or column.name == "scope_id"
                    for column in window.find_all(exp.Column))
            for window in windows
        )
        assert list(tree.find_all(exp.Unnest)) or name == "winner parity"

    manifest = parse_one(checks["manifest played-lineup disposition"].sql, read="trino")
    manifest_ctes = {cte.alias_or_name: cte for cte in manifest.find_all(exp.CTE)}
    assert {table.name for table in manifest_ctes["lineup_dispositions"].find_all(exp.Table)} == {
        "manifest_dedup"
    }


def test_winner_and_scoreboard_queries_preserve_the_independent_latest_generation_contract():
    checks = _checks_by_name()

    winner = parse_one(checks["winner parity"].sql, read="trino")
    winner_ctes = {cte.alias_or_name: cte for cte in winner.find_all(exp.CTE)}
    assert {"schedule_dedup", "winner_flags"} <= set(winner_ctes)
    cases = list(winner_ctes["winner_flags"].find_all(exp.Case))
    assert len(cases) >= 2  # raw winner flags plus the independent score model
    literals = {literal.this for literal in winner.find_all(exp.Literal)}
    assert "STATUS_FINAL_PEN" not in literals
    json_paths = {
        extract.expression.sql(dialect="trino").strip("'")
        for extract in winner.find_all(exp.JSONExtractScalar)
    }
    assert {
        "$.sides.home.competitor.winner",
        "$.sides.away.competitor.winner",
        "$.sides.home.competitor.shootoutScore",
        "$.sides.away.competitor.shootoutScore",
    } <= json_paths
    assert {"home_score", "away_score", "extra_json"} <= {
        column.name for column in winner_ctes["winner_flags"].find_all(exp.Column)
    }

    scoreboard = parse_one(checks["scoreboard totalGoals parity"].sql, read="trino")
    scoreboard_ctes = {cte.alias_or_name: cte for cte in scoreboard.find_all(exp.CTE)}
    assert {"schedule_dedup", "schedule_sides", "scoreboard_total_goals"} <= set(scoreboard_ctes)
    group = next(scoreboard_ctes["scoreboard_total_goals"].find_all(exp.Group))
    assert {column.name for column in group.find_all(exp.Column)} == {"event_id", "team_id"}
    assert {table.name for table in scoreboard_ctes["schedule_sides"].find_all(exp.Table)} == {
        "schedule_dedup"
    }
    assert {table.name for table in scoreboard_ctes["scoreboard_total_goals"].find_all(exp.Table)} == {
        "schedule_sides"
    }


def test_goal_events_parity_sums_player_aggregate_and_excludes_penalty_finals():
    check = _checks_by_name()["goals events <= score + 2"]
    tree = parse_one(check.sql, read="trino")

    tables = {(table.name, table.alias_or_name) for table in tree.find_all(exp.Table)}
    assert ("espn_player_match_aggregate", "p") in tables
    assert not any(table.name == "espn_match_events" for table in tree.find_all(exp.Table))

    player_goal_sum = next(tree.find_all(exp.Sum))
    assert isinstance(player_goal_sum.this, exp.Coalesce)
    assert (player_goal_sum.this.this.table, player_goal_sum.this.this.name) == (
        "p", "goals_events"
    )
    assert player_goal_sum.this.expressions[0].this == "0"

    grouped = next(tree.find_all(exp.Group))
    assert {(column.table, column.name) for column in grouped.find_all(exp.Column)} == {
        ("m", "event_id"), ("m", "home_score"), ("m", "away_score"),
    }
    penalty_final_filter = next(
        condition for condition in tree.find_all(exp.NEQ)
        if (condition.this.table, condition.this.name) == ("m", "status")
    )
    assert penalty_final_filter.expression.this == "STATUS_FINAL_PEN"
    assert check.severity == "WARNING"
    assert check.passed((0,)) is True
    assert check.passed((1,)) is False


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
