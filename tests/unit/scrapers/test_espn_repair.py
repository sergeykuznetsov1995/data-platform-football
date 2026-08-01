from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path

import duckdb
import pytest

from scrapers.espn.repair import (
    REPAIR_SEED_PATH,
    TOP5_COMPETITIONS,
    TOP5_SEASONS,
    RepairAuditError,
    Top5SnapshotExtractor,
    audit_top5,
    classify_trust,
    expected_top5_scopes,
    load_repair_seed,
    render_top5_audit_sql,
    seal_top5_audit_input,
)
from scripts.audit_espn_repair import main
from scripts.extract_espn_repair_audit import main as extract_main


def _green_records():
    return [
        {
            "scope_id": scope.scope_id,
            "legacy_league": scope.legacy_league,
            "legacy_season": scope.legacy_season,
            "source_season_year": scope.source_season_year,
            "trust_label": "trusted_candidate",
            "event_count": 380,
            "observed_min_date": scope.start_date.isoformat(),
            "observed_max_date": scope.end_date.isoformat(),
            "out_of_window_events": 0,
            "null_schedule_game_ids": 0,
            "duplicate_event_ids": 0,
            "null_lineup_keys": 0,
            "duplicate_lineup_keys": 0,
            "null_matchsheet_keys": 0,
            "duplicate_matchsheet_keys": 0,
            "matchsheet_two_side_failures": 0,
            "final_events": 300,
            "unresolved_final_scores": 0,
            "summary_required_events": 300,
            "summary_covered_events": 300,
        }
        for scope in expected_top5_scopes()
    ]


def _audit_input(records):
    return seal_top5_audit_input(
        records,
        snapshot_ids={
            "espn_schedule": 101,
            "espn_lineup": 102,
            "espn_matchsheet": 103,
        },
        as_of=datetime(2026, 8, 1, 9, tzinfo=timezone.utc),
    )


def test_seed_manifest_contains_exact_five_named_repairs_and_trust_policy():
    seed = load_repair_seed(REPAIR_SEED_PATH)

    assert [(item.legacy_league, item.legacy_season) for item in seed.scopes] == [
        ("ITA-Serie A", "2021"),
        ("ESP-La Liga", "2021"),
        ("ESP-La Liga", "2324"),
        ("INT-World Cup", "2022"),
        ("FRA-Ligue 1", "1920"),
    ]
    assert len(seed.scopes) == 5
    assert seed.pre_2016_trust_label == "legacy_untrusted"
    assert all(
        item.checks == ("date", "duplicate", "final_score", "summary_coverage")
        for item in seed.scopes
    )
    assert all(item.registry_promotion_required is True for item in seed.scopes)


def test_top5_audit_inventory_is_every_league_and_1617_through_2526():
    scopes = expected_top5_scopes()

    assert len(TOP5_COMPETITIONS) == 5
    assert TOP5_SEASONS == (
        "1617",
        "1718",
        "1819",
        "1920",
        "2021",
        "2122",
        "2223",
        "2324",
        "2425",
        "2526",
    )
    assert len(scopes) == 50
    assert len({item.scope_id for item in scopes}) == 50
    assert {(item.legacy_league, item.legacy_season) for item in scopes} == {
        (league, season) for league in TOP5_COMPETITIONS for season in TOP5_SEASONS
    }


def test_audit_queues_each_failed_check_and_keeps_green_scopes_out():
    records = _green_records()
    failures = {
        "700:2016": ("out_of_window_events", 1),
        "740:2017": ("duplicate_event_ids", 2),
        "730:2018": ("unresolved_final_scores", 3),
        "710:2019": ("summary_covered_events", 299),
    }
    for record in records:
        change = failures.get(record["scope_id"])
        if change:
            record[change[0]] = change[1]

    result = audit_top5(_audit_input(records))

    queued = {item["scope_id"]: item["reasons"] for item in result["queue"]}
    assert queued == {
        "700:2016": ["date"],
        "740:2017": ["duplicate"],
        "730:2018": ["final_score"],
        "710:2019": ["summary_coverage"],
    }
    assert result["audited_scope_count"] == 50
    assert result["queue_count"] == 4
    assert result["status"] == "repairs_required"


def test_audit_queues_missing_top5_scope_instead_of_silently_passing():
    records = _green_records()[:-1]
    result = audit_top5(_audit_input(records))
    assert result["queue"][-1]["reasons"] == ["missing_scope"]
    assert result["queue"][-1]["registry_promotion_required"] is True


def test_empty_observed_scope_with_null_date_bounds_is_queued_for_date_repair():
    records = _green_records()
    records[0].update(
        event_count=0,
        observed_min_date=None,
        observed_max_date=None,
    )

    result = audit_top5(_audit_input(records))

    assert result["queue"][0]["reasons"] == ["date"]


@pytest.mark.parametrize(
    "field",
    [
        "null_schedule_game_ids",
        "duplicate_lineup_keys",
        "null_lineup_keys",
        "duplicate_matchsheet_keys",
        "null_matchsheet_keys",
    ],
)
def test_every_natural_key_defect_queues_duplicate_repair(field):
    records = _green_records()
    records[0][field] = 1
    result = audit_top5(_audit_input(records))
    assert result["queue"][0]["reasons"] == ["duplicate"]


def test_matchsheet_two_side_failure_is_summary_coverage_failure():
    records = _green_records()
    records[0]["matchsheet_two_side_failures"] = 1
    result = audit_top5(_audit_input(records))
    assert result["queue"][0]["reasons"] == ["summary_coverage"]


def test_pre_2016_is_explicitly_untrusted_and_never_auto_queued():
    records = _green_records()
    records.append(
        {
            **deepcopy(records[0]),
            "scope_id": "700:2015",
            "legacy_season": "1516",
            "source_season_year": 2015,
            "trust_label": "legacy_untrusted",
            "duplicate_event_ids": 10,
        }
    )
    result = audit_top5(_audit_input(records))
    assert result["excluded"] == [
        {
            "scope_id": "700:2015",
            "legacy_league": "ENG-Premier League",
            "legacy_season": "1516",
            "trust_label": "legacy_untrusted",
            "reason": "pre_2016_not_trusted_for_automatic_cutover",
        }
    ]
    assert all(item["scope_id"] != "700:2015" for item in result["queue"])
    assert classify_trust(2015) == "legacy_untrusted"
    assert classify_trust(2016) == "trusted_candidate"


def test_audit_rejects_a_forged_trust_label():
    records = _green_records()
    records[0]["trust_label"] = "legacy_untrusted"
    with pytest.raises(RepairAuditError, match="trust_label"):
        audit_top5(_audit_input(records))


def test_repair_audit_cli_is_executable_and_writes_a_sealed_queue(tmp_path):
    input_path = tmp_path / "audit-input.json"
    output_path = tmp_path / "repair-queue.json"
    input_path.write_text(
        json.dumps(_audit_input(_green_records())),
        encoding="utf-8",
    )

    assert main(["--input", str(input_path), "--output", str(output_path)]) == 0
    report = json.loads(output_path.read_text(encoding="utf-8"))
    assert report["status"] == "passed"
    assert report["queue_count"] == 0
    assert len(report["result_sha256"]) == 64
    script = Path(__file__).resolve().parents[3] / "scripts/audit_espn_repair.py"
    assert os.access(script, os.X_OK)


def test_snapshot_sql_is_read_only_and_time_travels_all_three_tables():
    sql = render_top5_audit_sql(
        {
            "espn_schedule": 101,
            "espn_lineup": 102,
            "espn_matchsheet": 103,
        }
    )

    assert "espn_schedule FOR VERSION AS OF 101" in sql
    assert "espn_lineup FOR VERSION AS OF 102" in sql
    assert "espn_matchsheet FOR VERSION AS OF 103" in sql
    assert all(word not in sql.upper() for word in ("INSERT ", "UPDATE ", "DELETE "))
    assert "home_goals" in sql and "matchsheet_two_side_failures" in sql
    matchsheet_key_sql = sql.split("), matchsheet_stats AS (", 1)[1].split(
        "), lineup_games AS (", 1
    )[0]
    assert "concat(m.game, chr(31), m.team)" in matchsheet_key_sql
    assert "m.is_home IS NULL" not in matchsheet_key_sql


def test_summary_coverage_requires_lineups_from_both_final_match_teams():
    sql = render_top5_audit_sql(
        {
            "espn_schedule": 101,
            "espn_lineup": 102,
            "espn_matchsheet": 103,
        }
    )
    lineup_games = sql.split("), lineup_games AS (", 1)[1].split(
        "), matchsheet_games AS (", 1
    )[0]
    coverage = sql.split("), coverage AS (", 1)[1].split(")\nSELECT", 1)[0]

    assert "COUNT(DISTINCT l.team) AS team_count" in lineup_games
    assert (
        "COUNT(DISTINCT IF(l.team IN (s.home_team, s.away_team), l.team, NULL)) "
        "AS required_team_count" in lineup_games
    )
    assert "l.team_count = 2" in coverage
    assert "l.required_team_count = 2" in coverage
    assert "l.game IS NOT NULL" not in coverage


def test_summary_coverage_sql_executes_with_exact_final_teams():
    scope = expected_top5_scopes()[0]
    connection = duckdb.connect(":memory:")
    connection.execute(
        "CREATE TABLE espn_schedule (league VARCHAR, season VARCHAR, game VARCHAR, "
        "game_id VARCHAR, match_date DATE, status VARCHAR, home_team VARCHAR, "
        "away_team VARCHAR, home_goals INTEGER, away_goals INTEGER)"
    )
    connection.execute(
        "CREATE TABLE espn_lineup (league VARCHAR, season VARCHAR, game VARCHAR, "
        "team VARCHAR, player VARCHAR)"
    )
    connection.execute(
        "CREATE TABLE espn_matchsheet (league VARCHAR, season VARCHAR, game VARCHAR, "
        "team VARCHAR, is_home BOOLEAN)"
    )
    games = ("wrong-teams", "one-team", "correct-teams")
    connection.executemany(
        "INSERT INTO espn_schedule VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                scope.legacy_league,
                scope.legacy_season,
                game,
                str(index),
                scope.start_date,
                "STATUS_FULL_TIME",
                "Home",
                "Away",
                1,
                0,
            )
            for index, game in enumerate(games, start=1)
        ],
    )
    connection.executemany(
        "INSERT INTO espn_lineup VALUES (?, ?, ?, ?, ?)",
        [
            (scope.legacy_league, scope.legacy_season, "wrong-teams", team, player)
            for team, player in (("Other A", "p1"), ("Other B", "p2"))
        ]
        + [(scope.legacy_league, scope.legacy_season, "one-team", "Home", "p3")]
        + [
            (scope.legacy_league, scope.legacy_season, "correct-teams", team, player)
            for team, player in (("Home", "p4"), ("Away", "p5"))
        ],
    )
    connection.executemany(
        "INSERT INTO espn_matchsheet VALUES (?, ?, ?, ?, ?)",
        [
            (scope.legacy_league, scope.legacy_season, game, team, is_home)
            for game in games
            for team, is_home in (("Home", True), ("Away", False))
        ],
    )
    sql = render_top5_audit_sql(
        {"espn_schedule": 101, "espn_lineup": 102, "espn_matchsheet": 103}
    )
    executable = (
        sql.replace(
            "iceberg.bronze.espn_schedule FOR VERSION AS OF 101", "espn_schedule"
        )
        .replace("iceberg.bronze.espn_lineup FOR VERSION AS OF 102", "espn_lineup")
        .replace(
            "iceberg.bronze.espn_matchsheet FOR VERSION AS OF 103", "espn_matchsheet"
        )
    )

    rows = connection.execute(executable).fetchall()
    observed = next(item for item in rows if item[0] == scope.scope_id)

    assert observed[16] == 3  # final_events
    assert observed[18] == 3  # summary_required_events
    assert observed[19] == 1  # summary_covered_events


def test_validator_rejects_operator_rows_without_snapshot_provenance():
    with pytest.raises(RepairAuditError, match="schema mismatch"):
        audit_top5(
            {
                "schema_version": "espn-top5-audit-input-v1",
                "as_of": "2026-08-01T09:00:00+00:00",
                "records": _green_records(),
            }
        )

    evidence = _audit_input(_green_records())
    evidence["records"][0]["event_count"] = 1
    with pytest.raises(RepairAuditError, match="record hash"):
        audit_top5(evidence)


def test_live_extractor_resolves_main_refs_then_seals_query_rows(tmp_path):
    records = _green_records()
    order = (
        "scope_id",
        "legacy_league",
        "legacy_season",
        "source_season_year",
        "trust_label",
        "event_count",
        "observed_min_date",
        "observed_max_date",
        "out_of_window_events",
        "null_schedule_game_ids",
        "duplicate_event_ids",
        "null_lineup_keys",
        "duplicate_lineup_keys",
        "null_matchsheet_keys",
        "duplicate_matchsheet_keys",
        "matchsheet_two_side_failures",
        "final_events",
        "unresolved_final_scores",
        "summary_required_events",
        "summary_covered_events",
    )

    class Repository:
        catalog = "iceberg"
        schema = "bronze"

        def __init__(self):
            self.statements = []

        def _execute(self, sql, params=()):
            self.statements.append(sql)
            if "$refs" in sql:
                table = next(
                    name
                    for name in ("espn_schedule", "espn_lineup", "espn_matchsheet")
                    if name in sql
                )
                return [
                    (
                        {
                            "espn_schedule": 101,
                            "espn_lineup": 102,
                            "espn_matchsheet": 103,
                        }[table],
                    )
                ]
            if sql == "SELECT current_timestamp":
                return [(datetime(2026, 8, 1, 9, tzinfo=timezone.utc),)]
            return [tuple(record[name] for name in order) for record in records]

    repository = Repository()
    evidence = Top5SnapshotExtractor(repository).extract()

    assert evidence["schema_version"] == "espn-top5-audit-input-v2"
    assert evidence["snapshot_evidence"]["tables"]["espn_schedule"] == 101
    assert audit_top5(evidence)["status"] == "passed"
    assert any("FOR VERSION AS OF" in sql for sql in repository.statements)

    output = tmp_path / "extracted.json"
    assert extract_main(["--output", str(output)], repository_factory=Repository) == 0
    assert json.loads(output.read_text())["schema_version"] == (
        "espn-top5-audit-input-v2"
    )
    script = (
        Path(__file__).resolve().parents[3] / "scripts/extract_espn_repair_audit.py"
    )
    assert os.access(script, os.X_OK)
