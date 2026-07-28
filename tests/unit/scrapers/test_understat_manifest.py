"""Hermetic scope-DQ and publication-fence tests for Understat."""

from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pytest

from scrapers.understat.manifest import (
    CONTRACT_VERSION,
    MANIFEST_COLUMNS,
    UNDERSTAT_ENTITIES,
    ManifestStatus,
    ScopeAttempt,
    ScopeKey,
    UnderstatManifestRepository,
    validate_scope_attempt_result,
)
from scrapers.understat.quality import (
    REQUIRED_COLUMNS,
    UnderstatQualityError,
    build_failure_attempt,
    build_scope_attempt,
    validate_understat_scope,
)
from scrapers.understat.coverage import coverage_exceptions_for_scope
from scrapers.understat.parsers import TEAM_BREAKDOWN_DIMENSIONS


SCOPE = ScopeKey(
    "ENG-Premier League",
    "2526",
    source_league="EPL",
    source_season_id="2025",
)
BATCH = "batch-2526"
RFPL_EMPTY_MATCH_GAME_IDS = {"11214", "11222", "11249", "11260"}


def _value(column: str):
    values = {
        "league": SCOPE.league,
        "season": SCOPE.season,
        "source_season_id": 2025,
        "game_id": 100,
        "shot_id": 1000,
        "player_id": 10,
        "team_id": 20,
        "home_team_id": 20,
        "away_team_id": 21,
        "date": "2026-01-10T15:00:00Z",
        "is_result": True,
        "has_data": True,
        "forecast_home_win": 0.5,
        "forecast_draw": 0.25,
        "forecast_away_win": 0.25,
        "xg": 0.2,
        "last_action": "Pass",
        "situation": "Open Play",
        "assist_player_id": 10,
        "player": "Player Ten",
        "source_team_title": "Club",
        "is_multi_team": False,
        "matches": 1,
        "minutes": 90,
        "home_ppda_att": 10,
        "home_ppda_def": 5,
        "home_ppda_allowed_att": 11,
        "home_ppda_allowed_def": 4,
        "away_ppda_att": 11,
        "away_ppda_def": 4,
        "away_ppda_allowed_att": 10,
        "away_ppda_allowed_def": 5,
        "team_side": "h",
        "roster_entry_ids": "501",
        "roster_in": 0,
        "roster_out": 0,
        "dimension": "situation",
        "category": "OpenPlay",
    }
    return values.get(column, 1)


def _frames(*, batch_id: str | None = BATCH) -> dict[str, pd.DataFrame]:
    result = {}
    for entity in UNDERSTAT_ENTITIES:
        row = {column: _value(column) for column in REQUIRED_COLUMNS[entity]}
        if batch_id is not None:
            row["_batch_id"] = batch_id
        result[entity] = pd.DataFrame([row])
    breakdown_template = result["understat_team_season_breakdowns"].iloc[0]
    breakdown_rows = []
    for team_id in (20, 21):
        for dimension in sorted(TEAM_BREAKDOWN_DIMENSIONS):
            row = breakdown_template.copy()
            row["team_id"] = team_id
            row["dimension"] = dimension
            breakdown_rows.append(row)
    result["understat_team_season_breakdowns"] = pd.DataFrame(breakdown_rows)
    for entity in (
        "understat_player_match_stats",
        "understat_player_team_season_stats",
    ):
        away_row = result[entity].iloc[0].copy()
        away_row["team_id"] = 21
        if entity == "understat_player_match_stats":
            away_row["team_side"] = "a"
        result[entity] = pd.concat(
            [result[entity], away_row.to_frame().T], ignore_index=True
        )
    return result


def _two_game_frames() -> dict[str, pd.DataFrame]:
    frames = _frames()
    schedule_2 = frames["understat_schedule"].iloc[0].copy()
    schedule_2["game_id"] = 101
    frames["understat_schedule"] = pd.concat(
        [frames["understat_schedule"], schedule_2.to_frame().T],
        ignore_index=True,
    )
    for entity in ("understat_shots", "understat_team_match_stats"):
        second = frames[entity].iloc[0].copy()
        second["game_id"] = 101
        if entity == "understat_shots":
            second["shot_id"] = 1001
        frames[entity] = pd.concat(
            [frames[entity], second.to_frame().T], ignore_index=True
        )
    second_roster = frames["understat_player_match_stats"].copy()
    second_roster.loc[:, "game_id"] = 101
    frames["understat_player_match_stats"] = pd.concat(
        [frames["understat_player_match_stats"], second_roster],
        ignore_index=True,
    )
    return frames


def _complete_report(frames=None):
    return validate_understat_scope(
        frames or _frames(),
        scope=SCOPE,
        active=False,
        batch_id=BATCH,
    )


def _complete_attempt(**overrides) -> ScopeAttempt:
    values = {
        "report": _complete_report(),
        "batch_id": BATCH,
        "run_id": "run-1",
        "attempt_id": "attempt-1",
        "mode": "history",
        "parser_version": "native-v1",
    }
    values.update(overrides)
    return build_scope_attempt(**values)


def test_complete_scope_has_exact_seven_entity_contract_and_shared_batch():
    report = _complete_report()

    assert report.status is ManifestStatus.COMPLETE
    assert report.publishable
    assert report.passed
    assert set(report.row_counts) == set(UNDERSTAT_ENTITIES)
    assert report.row_counts["understat_team_season_breakdowns"] == 14
    assert report.row_counts["understat_player_match_stats"] == 2
    assert report.row_counts["understat_player_team_season_stats"] == 2
    assert all(
        count == 1
        for entity, count in report.row_counts.items()
        if entity
        not in {
            "understat_team_season_breakdowns",
            "understat_player_match_stats",
            "understat_player_team_season_stats",
        }
    )
    assert report.row_counts == report.natural_key_counts
    assert all(len(value) == 64 for value in report.payload_hashes.values())

    attempt = _complete_attempt()
    assert attempt.published
    assert attempt.batch_id == BATCH
    assert all(
        status is ManifestStatus.COMPLETE
        for status in attempt.entity_statuses.values()
    )


def test_partial_batch_cannot_be_constructed_as_complete():
    attempt = _complete_attempt()
    partial_statuses = dict(attempt.entity_statuses)
    partial_statuses.pop("understat_shots")

    with pytest.raises(ValueError, match="exactly the seven"):
        replace(attempt, entity_statuses=partial_statuses)

    broken_counts = dict(attempt.natural_key_counts)
    broken_counts["understat_shots"] = 0
    with pytest.raises(ValueError, match="seven complete"):
        replace(attempt, natural_key_counts=broken_counts)


def test_duplicate_natural_key_is_contract_failure():
    frames = _frames()
    frames["understat_shots"] = pd.concat(
        [frames["understat_shots"], frames["understat_shots"]],
        ignore_index=True,
    )

    report = _complete_report(frames)

    assert report.status is ManifestStatus.CONTRACT_FAILURE
    assert not report.passed
    assert any(issue.code == "duplicate_natural_key" for issue in report.issues)
    with pytest.raises(UnderstatQualityError, match="duplicate"):
        report.raise_for_failure()


def test_missing_contract_column_is_schema_drift():
    frames = _frames()
    frames["understat_schedule"] = frames["understat_schedule"].drop(
        columns=["source_season_id"]
    )

    report = _complete_report(frames)

    assert report.status is ManifestStatus.SCHEMA_DRIFT
    assert any(issue.code == "missing_columns" for issue in report.issues)


def test_rows_outside_exact_scope_fail_even_if_aggregate_counts_are_healthy():
    frames = _frames()
    frames["understat_players"].loc[0, "season"] = "2425"

    report = _complete_report(frames)

    assert report.status is ManifestStatus.CONTRACT_FAILURE
    assert any(issue.code == "scope_mismatch" for issue in report.issues)


def test_source_season_id_is_part_of_the_exact_scope():
    frames = _frames()
    frames["understat_players"].loc[0, "source_season_id"] = 2024

    report = _complete_report(frames)

    assert report.status is ManifestStatus.CONTRACT_FAILURE
    mismatch = next(issue for issue in report.issues if issue.code == "scope_mismatch")
    assert mismatch.details["source_season_ids"] == ["2024"]


def test_source_league_id_is_part_of_the_exact_scope():
    frames = _frames()
    frames["understat_players"].loc[0, "league_id"] = 4

    report = _complete_report(frames)

    assert report.status is ManifestStatus.CONTRACT_FAILURE
    mismatch = next(
        issue for issue in report.issues if issue.code == "league_id_mismatch"
    )
    assert mismatch.details["expected_league_id"] == "1"
    assert mismatch.details["league_ids"] == ["4"]


def test_null_scope_or_batch_values_cannot_hide_behind_valid_rows():
    frames = _two_game_frames()
    frames["understat_schedule"].loc[1, "source_season_id"] = pd.NA
    frames["understat_shots"].loc[1, "_batch_id"] = pd.NA

    report = _complete_report(frames)

    assert report.status is ManifestStatus.CONTRACT_FAILURE
    assert {issue.code for issue in report.issues} >= {
        "scope_mismatch",
        "batch_mismatch",
    }


def test_xg_and_forecast_ranges_are_fail_closed():
    frames = _frames()
    frames["understat_shots"].loc[0, "xg"] = 1.2
    frames["understat_schedule"].loc[0, "forecast_home_win"] = 0.9

    report = _complete_report(frames)

    assert report.status is ManifestStatus.CONTRACT_FAILURE
    assert {issue.code for issue in report.issues} >= {
        "invalid_xg",
        "invalid_forecast",
    }


def test_cross_table_missing_game_fails_unless_explicitly_allowlisted():
    frames = _two_game_frames()
    frames["understat_shots"] = frames["understat_shots"].query(
        "game_id == 100"
    )

    failed = _complete_report(frames)
    allowed = validate_understat_scope(
        frames,
        scope=SCOPE,
        active=False,
        batch_id=BATCH,
        coverage_exceptions={
            "understat_shots": {"missing": {"101": "known upstream gap"}}
        },
    )

    assert failed.status is ManifestStatus.CONTRACT_FAILURE
    assert any(issue.code == "missing_completed_games" for issue in failed.issues)
    assert allowed.status is ManifestStatus.COMPLETE
    assert any(
        issue.code == "allowlisted_game_coverage" for issue in allowed.issues
    )


def test_completed_match_roster_requires_both_schedule_teams():
    frames = _frames()
    frames["understat_player_match_stats"] = frames[
        "understat_player_match_stats"
    ].query("team_id == 20")

    report = _complete_report(frames)

    assert report.status is ManifestStatus.CONTRACT_FAILURE
    issue = next(
        issue
        for issue in report.issues
        if issue.code == "player_match_team_coverage_mismatch"
    )
    assert issue.details["games"] == {"100": ["21"]}


def test_completed_match_roster_side_must_match_its_schedule_team():
    frames = _frames()
    player_match = frames["understat_player_match_stats"]
    player_match.loc[player_match["team_side"] == "h", "team_id"] = 21
    player_match.loc[player_match["team_side"] == "a", "team_id"] = 20

    report = _complete_report(frames)

    assert report.status is ManifestStatus.CONTRACT_FAILURE
    issue = next(
        issue
        for issue in report.issues
        if issue.code == "player_match_side_team_mismatch"
    )
    assert issue.details["games"]["100"] == {
        "expected": ["a:21", "h:20"],
        "actual": ["a:20", "h:21"],
    }


@pytest.mark.parametrize("column", ["home_xg", "away_deep_allowed"])
def test_team_match_requires_non_null_core_metrics_for_both_sides(column):
    frames = _frames()
    frames["understat_team_match_stats"].loc[0, column] = pd.NA

    report = _complete_report(frames)

    assert report.status is ManifestStatus.CONTRACT_FAILURE
    issue = next(
        issue
        for issue in report.issues
        if issue.code == "team_match_side_metrics_incomplete"
    )
    assert column in issue.details["games"]["100"]


def test_team_breakdowns_require_every_source_team_and_all_seven_dimensions():
    missing_team = _frames()
    missing_team["understat_team_season_breakdowns"] = missing_team[
        "understat_team_season_breakdowns"
    ].query("team_id == 20")

    team_report = _complete_report(missing_team)
    assert team_report.status is ManifestStatus.CONTRACT_FAILURE
    assert any(
        issue.code == "team_breakdown_coverage_mismatch"
        for issue in team_report.issues
    )

    missing_dimension = _frames()
    missing_dimension["understat_team_season_breakdowns"] = missing_dimension[
        "understat_team_season_breakdowns"
    ].query("not (team_id == 20 and dimension == 'timing')")

    dimension_report = _complete_report(missing_dimension)
    assert dimension_report.status is ManifestStatus.CONTRACT_FAILURE
    issue = next(
        issue
        for issue in dimension_report.issues
        if issue.code == "team_breakdown_dimension_mismatch"
    )
    assert issue.details["teams"]["20"]["missing"] == ["timing"]


def test_reviewed_coverage_exceptions_are_exact_and_scope_specific():
    fra = coverage_exceptions_for_scope(
        ScopeKey("FRA-Ligue 1", "1617", source_season_id="2016")
    )
    ger = coverage_exceptions_for_scope(
        ScopeKey("GER-Bundesliga", "2425", source_season_id="2024")
    )
    rfpl = coverage_exceptions_for_scope(
        ScopeKey("RUS-Premier League", "1920", source_season_id="2019")
    )

    assert set(fra) == {"understat_shots", "understat_player_match_stats"}
    assert set(fra["understat_shots"]["missing"]) == {"4238"}
    assert set(ger["understat_player_match_stats"]["missing"]) == {"27930"}
    assert set(rfpl) == {"understat_shots", "understat_player_match_stats"}
    assert set(rfpl["understat_shots"]["missing"]) == RFPL_EMPTY_MATCH_GAME_IDS
    assert (
        set(rfpl["understat_player_match_stats"]["missing"])
        == RFPL_EMPTY_MATCH_GAME_IDS
    )
    assert coverage_exceptions_for_scope(SCOPE) == {}

    # The helper must not expose the reviewed module constant to mutation.
    fra["understat_shots"]["missing"].clear()
    fresh = coverage_exceptions_for_scope(
        ScopeKey("FRA-Ligue 1", "1617", source_season_id="2016")
    )
    assert set(fresh["understat_shots"]["missing"]) == {"4238"}

    rfpl["understat_shots"]["missing"].clear()
    fresh_rfpl = coverage_exceptions_for_scope(
        ScopeKey("RUS-Premier League", "1920", source_season_id="2019")
    )
    assert (
        set(fresh_rfpl["understat_shots"]["missing"])
        == RFPL_EMPTY_MATCH_GAME_IDS
    )


@pytest.mark.parametrize(
    ("league", "season", "game_id", "expected_game_ids"),
    [
        ("FRA-Ligue 1", "1617", "4238", {"4238"}),
        ("GER-Bundesliga", "2425", "27930", {"27930"}),
        ("RUS-Premier League", "1920", "11214", RFPL_EMPTY_MATCH_GAME_IDS),
        ("RUS-Premier League", "1920", "11222", RFPL_EMPTY_MATCH_GAME_IDS),
        ("RUS-Premier League", "1920", "11249", RFPL_EMPTY_MATCH_GAME_IDS),
        ("RUS-Premier League", "1920", "11260", RFPL_EMPTY_MATCH_GAME_IDS),
    ],
)
def test_reviewed_empty_match_payload_exceptions_are_exact(
    league, season, game_id, expected_game_ids
):
    scope = ScopeKey(league, season)
    exceptions = coverage_exceptions_for_scope(scope)

    assert set(exceptions) == {
        "understat_shots",
        "understat_player_match_stats",
    }
    assert game_id in expected_game_ids
    assert set(exceptions["understat_shots"]["missing"]) == expected_game_ids
    assert (
        set(exceptions["understat_player_match_stats"]["missing"])
        == expected_game_ids
    )
    assert coverage_exceptions_for_scope(SCOPE) == {}


def test_active_schedule_only_is_upstream_pending_and_never_published():
    frames = _frames()
    frames["understat_schedule"].loc[:, "is_result"] = False
    frames["understat_schedule"].loc[:, "has_data"] = False
    for entity in UNDERSTAT_ENTITIES[1:]:
        frames[entity] = pd.DataFrame()

    report = validate_understat_scope(
        frames,
        scope=SCOPE,
        active=True,
        batch_id=BATCH,
    )
    attempt = build_scope_attempt(
        report,
        batch_id=BATCH,
        run_id="run-pending",
        mode="current",
        parser_version="native-v1",
    )

    assert report.status is ManifestStatus.UPSTREAM_PENDING
    assert report.passed and not report.publishable
    assert attempt.status is ManifestStatus.UPSTREAM_PENDING
    assert not attempt.published
    assert attempt.row_counts["understat_schedule"] == 1
    assert all(
        attempt.row_counts[entity] == 0 for entity in UNDERSTAT_ENTITIES[1:]
    )


def test_future_rfpl_schedule_with_source_teams_is_upstream_pending():
    scope = ScopeKey(
        "RUS-Premier League",
        "2627",
        source_league="RFPL",
        source_season_id="2026",
    )
    frames = _frames()
    schedule = frames["understat_schedule"].copy()
    schedule.loc[:, "league"] = scope.league
    schedule.loc[:, "league_id"] = 6
    schedule.loc[:, "season"] = scope.season
    schedule.loc[:, "season_id"] = 2026
    schedule.loc[:, "source_season_id"] = 2026
    schedule.loc[:, "is_result"] = False
    schedule.loc[:, "has_data"] = False
    frames["understat_schedule"] = schedule
    for entity in UNDERSTAT_ENTITIES[1:]:
        frames[entity] = pd.DataFrame()

    report = validate_understat_scope(
        frames,
        scope=scope,
        active=True,
        batch_id=BATCH,
    )

    assert report.status is ManifestStatus.UPSTREAM_PENDING
    assert report.passed and not report.publishable


def test_all_empty_new_scope_is_not_published():
    report = validate_understat_scope(
        {entity: pd.DataFrame() for entity in UNDERSTAT_ENTITIES},
        scope=SCOPE,
        active=True,
    )

    assert report.status is ManifestStatus.NOT_PUBLISHED
    assert report.passed and not report.publishable


def test_all_empty_closed_scope_is_contract_failure_not_expected_absence():
    report = validate_understat_scope(
        {entity: pd.DataFrame() for entity in UNDERSTAT_ENTITIES},
        scope=SCOPE,
        active=False,
    )

    assert report.status is ManifestStatus.CONTRACT_FAILURE
    assert not report.passed
    assert any(
        issue.code == "closed_scope_not_published" for issue in report.issues
    )


def test_empty_previously_populated_entity_is_hard_failure():
    report = validate_understat_scope(
        {entity: pd.DataFrame() for entity in UNDERSTAT_ENTITIES},
        scope=SCOPE,
        active=True,
        previous_row_counts={"understat_shots": 50},
    )

    assert report.status is ManifestStatus.CONTRACT_FAILURE
    assert any(
        issue.code == "previously_populated_entity_empty"
        for issue in report.issues
    )


@pytest.mark.parametrize(
    "status",
    [
        ManifestStatus.RETRYABLE_FAILURE,
        ManifestStatus.CONTRACT_FAILURE,
        ManifestStatus.SCHEMA_DRIFT,
    ],
)
def test_failure_builder_always_describes_all_seven_entities(status):
    attempt = build_failure_attempt(
        scope=SCOPE,
        status=status,
        batch_id="failed-batch",
        run_id="failed-run",
        mode="history",
        parser_version="native-v1",
        attempt_no=3,
        error_message="injected failure",
    )

    assert attempt.status is status
    assert attempt.attempt_no == 3
    assert set(attempt.entity_statuses) == set(UNDERSTAT_ENTITIES)
    assert set(attempt.entity_statuses.values()) == {status}
    assert not attempt.published


class _FakeWriter:
    def __init__(self):
        self.calls = []

    def write_dataframe(self, frame, **kwargs):
        self.calls.append((frame.copy(), kwargs))
        return "iceberg.ops.understat_ingest_manifest_v1"


class _FakeQuery:
    def __init__(self, handler=None):
        self.calls = []
        self.handler = handler or (lambda _sql, _params: [])

    def execute_query(self, sql, params=None):
        self.calls.append((sql, params))
        return self.handler(sql, params)


def test_repository_append_is_one_append_only_row_and_is_fully_injectable():
    writer = _FakeWriter()
    query = _FakeQuery()
    repository = UnderstatManifestRepository(writer=writer, query=query)

    path = repository.append_attempt(_complete_attempt())

    assert path == "iceberg.ops.understat_ingest_manifest_v1"
    assert len(writer.calls) == 1
    frame, kwargs = writer.calls[0]
    assert tuple(frame.columns) == MANIFEST_COLUMNS
    assert frame.loc[0, "batch_id"] == BATCH
    assert kwargs["mode"] == "append"
    assert kwargs["add_metadata"] is False
    assert kwargs["database"] == "ops"
    assert len(query.calls) == 2
    assert query.calls[0][0].startswith("CREATE SCHEMA IF NOT EXISTS")
    assert "CREATE TABLE IF NOT EXISTS" in query.calls[1][0]


def _physical_rows(attempt: ScopeAttempt, *, wrong_entity: str | None = None):
    rows = []
    for entity in UNDERSTAT_ENTITIES:
        count = attempt.row_counts[entity]
        batch_rows = 0 if entity == wrong_entity else count
        rows.append((entity, count, batch_rows, 1))
    return rows


def test_latest_attempt_and_physical_batch_are_both_required_for_publication():
    attempt = _complete_attempt()

    def healthy(sql, _params):
        return _physical_rows(attempt) if "UNION ALL" in sql else [attempt.to_row()]

    repository = UnderstatManifestRepository(
        writer=_FakeWriter(), query=_FakeQuery(healthy), ensure_table_on_write=False
    )
    assert repository.is_scope_complete(SCOPE)

    def mixed(sql, _params):
        return (
            _physical_rows(attempt, wrong_entity="understat_shots")
            if "UNION ALL" in sql
            else [attempt.to_row()]
        )

    mixed_repository = UnderstatManifestRepository(
        writer=_FakeWriter(), query=_FakeQuery(mixed), ensure_table_on_write=False
    )
    assert not mixed_repository.is_scope_complete(SCOPE)


def test_newer_failure_invalidates_older_complete_without_physical_query():
    complete = _complete_attempt()
    failure = ScopeAttempt(
        scope=SCOPE,
        status=ManifestStatus.CONTRACT_FAILURE,
        batch_id="failed-batch",
        run_id="run-2",
        attempt_id="attempt-2",
        mode="history",
        parser_version="native-v1",
        entity_statuses={
            entity: ManifestStatus.CONTRACT_FAILURE
            for entity in UNDERSTAT_ENTITIES
        },
        row_counts={entity: 0 for entity in UNDERSTAT_ENTITIES},
        natural_key_counts={entity: 0 for entity in UNDERSTAT_ENTITIES},
        payload_hashes={entity: "" for entity in UNDERSTAT_ENTITIES},
    )

    query = _FakeQuery(lambda sql, _params: [failure.to_row()])
    repository = UnderstatManifestRepository(
        writer=_FakeWriter(), query=query, ensure_table_on_write=False
    )

    assert complete.published  # proves an older valid generation existed
    assert not repository.is_scope_complete(SCOPE)
    assert not any("UNION ALL" in sql for sql, _params in query.calls)


def test_latest_complete_query_remains_available_for_audit_and_previous_counts():
    attempt = _complete_attempt()
    query = _FakeQuery(lambda _sql, _params: [attempt.to_row()])
    repository = UnderstatManifestRepository(
        writer=_FakeWriter(), query=query, ensure_table_on_write=False
    )

    observed = repository.latest_complete(SCOPE)

    assert observed == attempt
    sql, params = query.calls[0]
    assert '"status" = ?' in sql
    assert params == (
        SCOPE.league,
        SCOPE.season,
        CONTRACT_VERSION,
        ManifestStatus.COMPLETE.value,
    )


def test_latest_data_attempt_remembers_complete_or_upstream_pending_counts():
    attempt = _complete_attempt()
    query = _FakeQuery(lambda _sql, _params: [attempt.to_row()])
    repository = UnderstatManifestRepository(
        writer=_FakeWriter(), query=query, ensure_table_on_write=False
    )

    observed = repository.latest_data_attempt(SCOPE)

    assert observed == attempt
    sql, params = query.calls[0]
    assert '"status" IN (?, ?)' in sql
    assert params == (
        SCOPE.league,
        SCOPE.season,
        CONTRACT_VERSION,
        ManifestStatus.COMPLETE.value,
        ManifestStatus.UPSTREAM_PENDING.value,
    )


def test_physical_scope_row_count_is_exact_and_unfenced_for_cutover_preflight():
    query = _FakeQuery(lambda _sql, _params: [(17,)])
    repository = UnderstatManifestRepository(
        writer=_FakeWriter(), query=query, ensure_table_on_write=False
    )

    assert repository.physical_scope_row_count("understat_schedule", SCOPE) == 17
    sql, params = query.calls[0]
    assert "iceberg.bronze.understat_schedule" in sql
    assert '"league" = ? AND "season" = ?' in sql
    assert params == (SCOPE.league, SCOPE.season)

    with pytest.raises(ValueError, match="unknown Understat entity"):
        repository.physical_scope_row_count("understat_unknown", SCOPE)


def test_runner_result_validation_is_serialization_friendly(tmp_path):
    attempt = _complete_attempt()
    result = {"scope_attempt": attempt.to_dict()}

    observed = validate_scope_attempt_result(
        result,
        expected_scope=SCOPE,
    )

    assert observed == attempt
    wrong_scope = ScopeKey("ESP-La Liga", "2526")
    with pytest.raises(ValueError, match="does not match"):
        validate_scope_attempt_result(result, expected_scope=wrong_scope)
