"""Offline SofaScore raw/manifest/Iceberg publication gates."""

from __future__ import annotations

import json
import re
from collections import deque
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from utils.sofascore_dq import (
    ActiveRegistryPartition,
    CaptureExpectation,
    SofaScoreContractError,
    SofaScoreDQViolation,
    active_registry_partitions,
    build_partition_dq_queries,
    compare_schema_fingerprints,
    load_coverage_contract,
    manifest_state_for_response,
    raw_payload_sha256,
    run_active_registry_committed_dq,
    run_committed_partition_dq,
    schema_fingerprint,
    validate_bronze_compatibility_columns,
    validate_coverage_contract,
    validate_event_participants,
    validate_lineup_semantics,
    validate_manifest_completeness,
    validate_offline_replay,
    validate_partition_replacement,
    validate_player_coverage,
    validate_raw_payload,
    validate_schedule_rows,
    validate_season_alignment,
    validate_table_rows,
)


pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests/fixtures"


class _ScalarCursor:
    def __init__(self, connection):
        self.connection = connection
        self.value = None
        self.closed = False

    def execute(self, sql):
        self.connection.executed.append(sql)
        if self.connection.fail_on and self.connection.fail_on in sql:
            raise RuntimeError("injected Trino failure")
        override = next(
            (
                value
                for token, value in self.connection.overrides.items()
                if token in sql
            ),
            None,
        )
        if override is not None:
            self.value = override
        elif self.connection.values:
            self.value = self.connection.values.popleft()
        elif "CASE WHEN COUNT(*) = 0" in sql:
            self.value = self.connection.coverage
        elif (
            "SELECT COUNT(DISTINCT player_id)" in sql
            or (
                "SELECT COUNT(*) FROM "
                "iceberg.silver.sofascore_player_match_aggregate WHERE league ="
                in sql
                and "minutes_played > 0" in sql
            )
            or (
                "SELECT COUNT(*) FROM "
                "iceberg.silver.sofascore_player_match_aggregate ss WHERE ss.league ="
                in sql
            )
        ):
            self.value = 1
        else:
            self.value = 0

    def fetchone(self):
        return (self.value,)

    def close(self):
        self.closed = True
        self.connection.closed_cursors += 1


class _ScalarConnection:
    def __init__(
        self,
        *,
        coverage=1.0,
        values=(),
        fail_on=None,
        overrides=None,
    ):
        self.coverage = coverage
        self.values = deque(values)
        self.fail_on = fail_on
        self.overrides = overrides or {}
        self.executed = []
        self.closed_cursors = 0
        self.closed = False

    def cursor(self):
        return _ScalarCursor(self)

    def close(self):
        self.closed = True


def _fixture(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_http_failures_can_never_be_success_or_empty():
    for status in (403, 429, 500, 502, 503, 504):
        assert (
            manifest_state_for_response(status, parsed=True, row_count=0)
            == "retryable_failure"
        )
    assert (
        manifest_state_for_response(200, parsed=False, row_count=None) == "schema_error"
    )
    assert (
        manifest_state_for_response(200, parsed=True, row_count=0) == "legitimate_empty"
    )
    assert manifest_state_for_response(200, parsed=True, row_count=3) == "success"
    assert (
        manifest_state_for_response(404, parsed=True, row_count=0, supported=False)
        == "not_supported"
    )
    assert (
        manifest_state_for_response(404, parsed=True, row_count=0, supported=True)
        == "retryable_failure"
    )


def test_schedule_skeleton_gate_allows_upcoming_match_without_score():
    good = {
        "game_id": 1,
        "home_team_id": 10,
        "away_team_id": 20,
        "start_timestamp": 1784307600,
        "home_score": None,
        "away_score": None,
    }
    assert validate_schedule_rows([good]).passed
    report = validate_schedule_rows(
        [
            good,
            {"league": "ENG-Premier League", "season": "2526"},
            {**good, "game_id": 2, "away_team_id": 10},
        ]
    )
    assert not report.passed
    assert report.metrics["schedule.skeleton_rows"] == 2
    with pytest.raises(SofaScoreDQViolation):
        report.require()


def test_table_contract_rejects_duplicates_missing_fields_and_bad_enum():
    base = {
        "match_id": "1",
        "player_id": "p1",
        "team_side": "home",
        "source_tournament_id": 17,
        "source_season_id": 76986,
        "is_starter": True,
        "is_bench": False,
        "is_unused_substitute": False,
        "participation_status": "starter",
        "league": "ENG-Premier League",
        "season": "2526",
        "raw_content_hash": "a" * 64,
        "raw_blob_key": "sofascore/17/76986/lineups/1.json.gz",
        "_ingested_at": "2026-07-11T00:00:00Z",
    }
    report = validate_table_rows(
        "bronze.sofascore_lineups",
        [base, dict(base), {**base, "player_id": "p2", "team_side": "neutral"}],
    )
    codes = {finding.code for finding in report.findings}
    assert "duplicate_natural_key" in codes
    assert "invalid_enum_value" in codes

    missing = dict(base)
    del missing["raw_blob_key"]
    missing["player_id"] = None
    codes = {
        finding.code
        for finding in validate_table_rows(
            "bronze.sofascore_lineups", [missing]
        ).findings
    }
    assert {"required_field_loss", "null_natural_key"} <= codes


def test_lineup_semantics_preserve_unused_bench():
    good = [
        {
            "is_starter": True,
            "is_bench": False,
            "is_unused_substitute": False,
            "participation_status": "starter",
        },
        {
            "is_starter": False,
            "is_bench": True,
            "is_unused_substitute": True,
            "participation_status": "unused_substitute",
        },
    ]
    report = validate_lineup_semantics(good)
    assert report.passed
    assert report.metrics["lineup.unused_substitutes"] == 1
    bad = validate_lineup_semantics(
        [
            {
                "is_starter": True,
                "is_bench": True,
                "is_unused_substitute": True,
                "participation_status": "substitute_used",
            }
        ]
    )
    assert not bad.passed


def test_event_requires_exactly_two_distinct_sides():
    good = [
        {"match_id": "m1", "team_id": "1", "team_side": "home"},
        {"match_id": "m1", "team_id": "2", "team_side": "away"},
    ]
    assert validate_event_participants(good).passed
    assert not validate_event_participants(good[:1]).passed
    assert not validate_event_participants(
        [{**good[0]}, {**good[1], "team_id": "1"}]
    ).passed


def test_season_gate_distinguishes_source_id_from_canonical_season():
    split = {"source_season_id": 76986, "canonical_season": "2526"}
    calendar = {"source_season_id": 58210, "canonical_season": "2026"}
    assert validate_season_alignment(
        [split], expected_source_season_id=76986, expected_canonical_season="2526"
    ).passed
    assert validate_season_alignment(
        [calendar], expected_source_season_id=58210, expected_canonical_season="2026"
    ).passed
    assert not validate_season_alignment(
        [split], expected_source_season_id=76986, expected_canonical_season="2026"
    ).passed
    assert not validate_season_alignment(
        [{**split, "season_id": 99999}],
        expected_source_season_id=76986,
        expected_canonical_season="2526",
    ).passed
    assert not validate_season_alignment(
        [{**split, "season": "2026"}],
        expected_source_season_id=76986,
        expected_canonical_season="2526",
    ).passed


def test_partition_shrink_is_blocked_before_commit():
    old = [("m1",), ("m2",), ("m3",)]
    assert validate_partition_replacement(old, old + [("m4",)]).passed
    report = validate_partition_replacement(old, [("m1",), ("m3",)])
    assert not report.passed
    assert report.metrics["partition.lost_keys"] == 1
    assert validate_partition_replacement(
        old, [("m1",), ("m3",)], allow_removed_keys=[("m2",)]
    ).passed


def test_required_manifest_completeness_rejects_failure_and_schema_error():
    expected = [
        CaptureExpectation("lineups", "event", "m1", 17, 76986, "final"),
        CaptureExpectation("shotmap", "event", "m1", 17, 76986, "final"),
    ]
    common = {
        "source_tournament_id": 17,
        "source_season_id": 76986,
        "target_type": "event",
        "target_id": "m1",
        "freshness_key": "final",
        "attempt": 1,
    }
    observations = [
        {**common, "endpoint": "lineups", "state": "success"},
        {**common, "endpoint": "shotmap", "state": "retryable_failure"},
    ]
    report = validate_manifest_completeness(expected, observations)
    assert not report.passed
    assert report.metrics["manifest.completeness"] == 0.5
    observations[-1]["state"] = "legitimate_empty"
    assert validate_manifest_completeness(expected, observations).passed
    observations[-1]["state"] = "schema_error"
    assert not validate_manifest_completeness(expected, observations).passed


def test_optional_endpoint_accepts_not_supported_but_required_does_not():
    expected = [CaptureExpectation("standings_total", "season", "76986")]
    observed = [
        {
            "endpoint": "standings_total",
            "target_type": "season",
            "target_id": "76986",
            "state": "not_supported",
        }
    ]
    assert validate_manifest_completeness(expected, observed).passed

    required = [CaptureExpectation("lineups", "event", "m1")]
    observed[0].update(endpoint="lineups", target_type="event", target_id="m1")
    assert not validate_manifest_completeness(required, observed).passed


def test_latest_manifest_attempt_wins():
    expected = [CaptureExpectation("lineups", "event", "m1")]
    observed = [
        {
            "endpoint": "lineups",
            "target_type": "event",
            "target_id": "m1",
            "state": "retryable_failure",
            "attempt": 1,
        },
        {
            "endpoint": "lineups",
            "target_type": "event",
            "target_id": "m1",
            "state": "success",
            "attempt": 2,
        },
    ]
    assert validate_manifest_completeness(expected, observed).passed


def test_profile_rating_and_attach_all_gate_at_95_percent():
    player_ids = {f"p{i}" for i in range(100)}
    player_events = {f"m1:p{i}" for i in range(100)}
    passing = validate_player_coverage(
        squad_player_ids=player_ids,
        lineup_player_ids={"unused-bench"},
        statistics_player_ids=set(),
        incident_player_ids=set(),
        profile_player_ids=player_ids | {"unused-bench"},
        rated_player_event_ids=set(list(player_events)[:95]),
        appeared_player_event_ids=player_events,
        silver_player_event_ids=player_events,
        gold_player_event_ids=set(list(player_events)[:95]),
    )
    assert passing.passed
    failing = validate_player_coverage(
        squad_player_ids=player_ids,
        lineup_player_ids={"unused-bench"},
        statistics_player_ids=set(),
        incident_player_ids=set(),
        profile_player_ids=set(list(player_ids)[:94]),
        rated_player_event_ids=set(list(player_events)[:94]),
        appeared_player_event_ids=player_events,
        silver_player_event_ids=player_events,
        gold_player_event_ids=set(list(player_events)[:94]),
    )
    assert {finding.code for finding in failing.findings} == {
        "player_profile_coverage",
        "player_rating_coverage",
        "silver_gold_attach_coverage",
    }


def test_player_coverage_zero_denominators_fail_closed():
    report = validate_player_coverage(
        squad_player_ids=(),
        lineup_player_ids=(),
        statistics_player_ids=(),
        incident_player_ids=(),
        profile_player_ids=(),
        rated_player_event_ids=(),
        appeared_player_event_ids=(),
        silver_player_event_ids=(),
        gold_player_event_ids=(),
    )

    assert report.metrics == {
        "player_profile.coverage": 0.0,
        "player_profile.expected": 0,
        "player_profile.covered": 0,
        "player_rating.coverage": 0.0,
        "player_rating.expected": 0,
        "player_rating.covered": 0,
        "silver_gold_attach.coverage": 0.0,
        "silver_gold_attach.expected": 0,
        "silver_gold_attach.covered": 0,
        "player_universe.size": 0,
    }
    assert {finding.code for finding in report.findings} == {
        "player_profile_coverage",
        "player_rating_coverage",
        "silver_gold_attach_coverage",
    }


def test_saved_json_fixtures_preserve_arrays_and_exact_cardinality():
    lineups = _fixture("sofascore_event_14023925_lineups.json")
    assert validate_raw_payload("lineups", lineups, normalized_row_count=4).passed
    incidents = _fixture("sofascore_event_14023925_incidents.json")
    assert validate_raw_payload("incidents", incidents, normalized_row_count=4).passed
    shots = _fixture("sofascore_event_14023925_shotmap.json")
    assert validate_raw_payload("shotmap", shots, normalized_row_count=2).passed

    cardinality_loss = validate_raw_payload("lineups", lineups, normalized_row_count=3)
    assert "normalized_cardinality_drift" in {
        finding.code for finding in cardinality_loss.findings
    }


def test_raw_array_and_required_field_schema_drift_fail_closed():
    payload = _fixture("sofascore_event_14023925_lineups.json")
    broken = deepcopy(payload)
    broken["away"]["players"] = {"not": "an array"}
    report = validate_raw_payload("lineups", broken)
    assert "raw_array_loss" in {finding.code for finding in report.findings}

    no_home = deepcopy(payload)
    del no_home["home"]["players"]
    report = validate_raw_payload("lineups", no_home)
    assert {finding.code for finding in report.findings} >= {
        "schema_drift",
        "raw_array_loss",
    }


def test_declared_legitimate_empty_parent_does_not_fake_nested_array_loss():
    report = validate_raw_payload(
        "statistics", {"statistics": []}, normalized_row_count=0
    )
    assert report.passed
    assert report.metrics["statistics.raw_cardinality"] == 0


def test_schema_fingerprint_allows_addition_but_rejects_removal_and_type_change():
    payload = _fixture("sofascore_event_14023925.json")
    baseline = schema_fingerprint(payload)
    additive = deepcopy(payload)
    additive["event"]["newSourceField"] = "ok"
    assert compare_schema_fingerprints(baseline, schema_fingerprint(additive)).passed

    removed = deepcopy(payload)
    del removed["event"]["homeTeam"]
    assert not compare_schema_fingerprints(baseline, schema_fingerprint(removed)).passed

    changed = deepcopy(payload)
    changed["event"]["id"] = {"unexpected": 14023925}
    assert not compare_schema_fingerprints(baseline, schema_fingerprint(changed)).passed


def test_offline_replay_uses_exact_saved_bytes():
    raw = (FIXTURES / "sofascore_event_14023925_incidents.json").read_bytes()
    parsed = json.loads(raw)
    report = validate_offline_replay(raw, parsed)
    assert report.passed
    assert len(raw_payload_sha256(raw)) == 64
    parsed["incidents"].pop()
    assert not validate_offline_replay(raw, parsed).passed


def test_fresh_bootstrap_compatibility_superset_is_enforced():
    contract = load_coverage_contract()
    complete = {
        table: {column for group in groups for column in group}
        for table, groups in contract["bronze_compatibility_columns"].items()
    }
    assert validate_bronze_compatibility_columns(complete).passed
    complete["sofascore_schedule"].remove("home_team")
    report = validate_bronze_compatibility_columns(complete)
    assert not report.passed
    assert report.findings[0].examples == (("sofascore_schedule", "home_team"),)


def test_partition_sql_contract_contains_all_hard_gates_and_is_injection_safe():
    queries = build_partition_dq_queries("ENG-Premier League", "2526", 17, 76986)
    names = {query.name for query in queries}
    assert "skeleton_schedule_rows" in names
    assert "schedule_season_mismatches" in names
    assert "event_season_mismatches" in names
    assert {
        "player_ratings_season_mismatches",
        "event_player_stats_season_mismatches",
        "match_stats_season_mismatches",
        "event_shotmap_season_mismatches",
    } <= names
    assert "required_endpoint_completeness" in names
    assert "player_profile_coverage" in names
    assert "player_rating_coverage" in names
    assert "silver_gold_attach_rate" in names
    assert "player_profile_expected_universe_nonempty" in names
    assert "player_rating_expected_appearances_nonempty" in names
    assert "silver_gold_expected_candidates_nonempty" in names
    assert any(name.startswith("duplicate_natural_key[") for name in names)
    assert any(name.startswith("null_natural_key[") for name in names)
    ops_manifest = next(
        query
        for query in queries
        if query.name
        == "duplicate_natural_key[ops.sofascore_capture_manifest]"
    )
    assert ops_manifest.expected_value == 0
    assert "FROM iceberg.ops.sofascore_capture_manifest" in ops_manifest.sql
    assert "freshness_key" in ops_manifest.sql
    assert "HAVING COUNT(*) > 1" in ops_manifest.sql
    profile_query = next(
        query for query in queries if query.name == "player_profile_coverage"
    )
    assert "bronze.sofascore_player_universe" in profile_query.sql
    schedule_season_query = next(
        query for query in queries if query.name == "schedule_season_mismatches"
    )
    event_season_query = next(
        query for query in queries if query.name == "event_season_mismatches"
    )
    assert "TRY_CAST(season_id AS double)" in schedule_season_query.sql
    assert "CAST(76986 AS double)" in schedule_season_query.sql
    assert "CAST(season_id AS varchar)" not in schedule_season_query.sql
    assert "TRY_CAST(source_season_id AS double)" in event_season_query.sql
    assert "CAST(source_season_id AS varchar)" not in event_season_query.sql
    assert "TRY_CAST(season_id AS double)" in event_season_query.sql
    assert {
        query.name: query.expected_value
        for query in queries
        if query.name
        in {
            "player_profile_coverage",
            "player_rating_coverage",
            "silver_gold_attach_rate",
        }
    } == {
        "player_profile_coverage": 0.95,
        "player_rating_coverage": 0.95,
        "silver_gold_attach_rate": 0.95,
    }
    assert {
        query.name: (query.expected_value, query.comparator)
        for query in queries
        if query.name.endswith("_nonempty")
    } == {
        "player_profile_expected_universe_nonempty": (1, "gte"),
        "player_rating_expected_appearances_nonempty": (1, "gte"),
        "silver_gold_expected_candidates_nonempty": (1, "gte"),
    }
    assert all(
        "THEN 0e0" in query.sql
        for query in queries
        if query.name
        in {
            "player_profile_coverage",
            "player_rating_coverage",
            "silver_gold_attach_rate",
        }
    )
    with pytest.raises(ValueError):
        build_partition_dq_queries("ENG'; DROP TABLE x; --", "2526", 17, 76986)


def test_schedule_skeleton_sql_supports_new_and_legacy_columns():
    query = next(
        query
        for query in build_partition_dq_queries(
            "ENG-Premier League", "2526", 17, 76986
        )
        if query.name == "skeleton_schedule_rows"
    )

    assert (
        "COALESCE(NULLIF(TRIM(CAST(home_team_name AS varchar)), ''), "
        "NULLIF(TRIM(CAST(home_team AS varchar)), '')) IS NULL"
    ) in query.sql
    assert (
        "COALESCE(NULLIF(TRIM(CAST(away_team_name AS varchar)), ''), "
        "NULLIF(TRIM(CAST(away_team AS varchar)), '')) IS NULL"
    ) in query.sql
    assert "(start_timestamp IS NULL AND date IS NULL)" in query.sql
    assert "start_timestamp IS NULL OR date IS NULL" not in query.sql
    assert (
        "NULLIF(TRIM(CAST(home_team AS varchar)), '')) = COALESCE("
    ) in query.sql


@pytest.mark.parametrize(
    ("check_name", "table_name"),
    (
        ("player_ratings_season_mismatches", "sofascore_player_ratings"),
        ("event_player_stats_season_mismatches", "sofascore_event_player_stats"),
        ("match_stats_season_mismatches", "sofascore_match_stats"),
        ("event_shotmap_season_mismatches", "sofascore_event_shotmap"),
    ),
)
def test_match_child_season_sql_uses_schedule_spine_without_hiding_mismatch(
    check_name, table_name
):
    query = next(
        query
        for query in build_partition_dq_queries(
            "ENG-Premier League", "2526", 17, 76986
        )
        if query.name == check_name
    )

    assert f"FROM iceberg.bronze.{table_name} c" in query.sql
    assert "LEFT JOIN iceberg.bronze.sofascore_schedule s" in query.sql
    assert (
        "CAST(s.game_id AS varchar) = CAST(c.match_id AS varchar)"
    ) in query.sql
    assert "c.league = 'ENG-Premier League' AND c.season = '2526'" in query.sql
    assert "s.game_id IS NULL" in query.sql
    assert "CAST(s.season AS varchar) IS DISTINCT FROM CAST(c.season AS varchar)" in query.sql
    assert "s.league IS DISTINCT FROM c.league" in query.sql
    # Joining on the partition fields would hide the very mismatch this query
    # exists to detect.
    join_clause = query.sql.split(" WHERE ", 1)[0]
    assert "s.league = c.league" not in join_clause
    assert "s.season = c.season" not in join_clause
    assert "c.source_season_id" not in query.sql
    assert "c.season_id" not in query.sql


def test_partition_sql_references_only_materialized_contract_tables():
    contract = load_coverage_contract()
    queries = build_partition_dq_queries(
        "ENG-Premier League", "2526", 17, 76986, contract=contract
    )
    table_refs = set(
        re.findall(
            r"iceberg\.((?:bronze|silver|gold|ops)\.[a-z0-9_]+)",
            "\n".join(query.sql for query in queries),
        )
    )
    allowed = set(contract["tables"]) | {
        "ops.sofascore_capture_manifest",
        "silver.xref_match",
        "silver.xref_player",
        "gold.fct_lineup",
    }
    assert table_refs <= allowed
    assert {
        "silver.sofascore_squad",
        "silver.sofascore_lineup",
        "silver.sofascore_incident",
        "gold.fct_player_match",
    }.isdisjoint(table_refs)

    completeness = next(
        query for query in queries if query.name == "required_endpoint_completeness"
    )
    assert "iceberg.ops.sofascore_capture_manifest" in completeness.sql
    assert "source_tournament_id = '17'" in completeness.sql
    assert "source_season_id = '76986'" in completeness.sql
    for endpoint in contract["committed_state_dq"]["required_event_endpoints"]:
        assert f"'{endpoint}'" in completeness.sql


def test_active_registry_partitions_are_capture_allowed_and_sorted(monkeypatch):
    from scrapers.sofascore.catalog import SofaScoreCatalog
    import utils.medallion_config as medallion_config

    active_seasons = (
        SimpleNamespace(
            activatable=True,
            canonical_season="2526",
            season_id=76986,
        ),
        SimpleNamespace(
            activatable=False,
            canonical_season=None,
            season_id=99999,
        ),
        SimpleNamespace(
            activatable=True,
            canonical_season="2425",
            season_id=62400,
        ),
    )
    catalog = SimpleNamespace(
        tournaments=(
            SimpleNamespace(
                capture_allowed=True,
                canonical_id="ENG-Premier League",
                unique_tournament_id=17,
                seasons=active_seasons,
            ),
            SimpleNamespace(
                capture_allowed=True,
                canonical_id="INT-World Cup",
                unique_tournament_id=16,
                seasons=(
                    SimpleNamespace(
                        activatable=True,
                        canonical_season="2026",
                        season_id=58210,
                    ),
                ),
            ),
            SimpleNamespace(
                capture_allowed=False,
                canonical_id="ENG-WSL",
                unique_tournament_id=44,
                seasons=active_seasons,
            ),
        )
    )
    season_by_key = {
        (17, "2526"): active_seasons[0],
        (16, "2026"): catalog.tournaments[1].seasons[0],
    }
    catalog.resolve_source_season = lambda tournament_id, token: season_by_key.get(
        (tournament_id, token)
    )
    monkeypatch.setattr(SofaScoreCatalog, "load", lambda path=None: catalog)
    monkeypatch.setattr(
        medallion_config,
        "get_active_season",
        lambda league: {
            "ENG-Premier League": 2025,
            "INT-World Cup": 2026,
        }.get(league),
    )
    monkeypatch.setattr(
        medallion_config,
        "is_single_year_competition",
        lambda league: league == "INT-World Cup",
    )

    assert active_registry_partitions("ignored.json") == (
        ActiveRegistryPartition("ENG-Premier League", "2526", 17, 76986),
        ActiveRegistryPartition("INT-World Cup", "2026", 16, 58210),
    )


def test_committed_partition_dq_executes_every_gate_and_closes_cursors():
    partition = ActiveRegistryPartition("ENG-Premier League", "2526", 17, 76986)
    connection = _ScalarConnection()
    result = run_committed_partition_dq(partition, connection)

    assert result["checks"] == len(
        build_partition_dq_queries("ENG-Premier League", "2526", 17, 76986)
    )
    assert len(connection.executed) == result["checks"]
    assert connection.closed_cursors == result["checks"]
    assert all(check["passed"] for check in result["results"])


def test_committed_partition_dq_enforces_rating_threshold():
    connection = _ScalarConnection(
        overrides={
            "END FROM iceberg.silver.sofascore_player_match_aggregate ": 0.949,
        }
    )
    with pytest.raises(SofaScoreDQViolation, match="player_rating_coverage"):
        run_committed_partition_dq(
            ActiveRegistryPartition("ENG-Premier League", "2526", 17, 76986),
            connection,
        )


def test_committed_partition_dq_rejects_empty_expected_universe():
    connection = _ScalarConnection(
        overrides={
            "SELECT COUNT(DISTINCT player_id)": 0,
            "SELECT COUNT(*) FROM iceberg.silver.sofascore_player_match_aggregate WHERE": 0,
            "SELECT COUNT(*) FROM iceberg.silver.sofascore_player_match_aggregate ss WHERE": 0,
        }
    )

    with pytest.raises(SofaScoreDQViolation, match="expected_.*_nonempty"):
        run_committed_partition_dq(
            ActiveRegistryPartition("ENG-Premier League", "2526", 17, 76986),
            connection,
        )


def test_committed_partition_dq_fails_closed_on_query_error():
    connection = _ScalarConnection(fail_on="sofascore_schedule")
    with pytest.raises(SofaScoreDQViolation, match="query failed"):
        run_committed_partition_dq(
            ActiveRegistryPartition("ENG-Premier League", "2526", 17, 76986),
            connection,
        )
    queries = build_partition_dq_queries("ENG-Premier League", "2526", 17, 76986)
    failing_index = next(
        index for index, query in enumerate(queries) if "sofascore_schedule" in query.sql
    )
    assert connection.closed_cursors == failing_index + 1


def test_active_registry_dq_runs_every_partition_without_closing_injected_conn(
    monkeypatch,
):
    import utils.sofascore_dq as dq

    partitions = (
        ActiveRegistryPartition("ENG-Premier League", "2526", 17, 76986),
        ActiveRegistryPartition("INT-World Cup", "2026", 16, 58210),
    )
    monkeypatch.setattr(dq, "active_registry_partitions", lambda path=None: partitions)
    connection = _ScalarConnection()
    result = run_active_registry_committed_dq(connection=connection)

    per_partition = len(
        build_partition_dq_queries("ENG-Premier League", "2526", 17, 76986)
    )
    assert result["status"] == "success"
    assert result["partitions"] == 2
    assert result["checks"] == 2 * per_partition
    assert not connection.closed


def test_committed_dq_matrix_rejects_unknown_or_duplicate_tables():
    contract = deepcopy(load_coverage_contract())
    contract["committed_state_dq"]["duplicate_tables"].append(
        "silver.sofascore_does_not_exist"
    )
    with pytest.raises(SofaScoreContractError, match="unknown tables"):
        validate_coverage_contract(contract)

    contract = deepcopy(load_coverage_contract())
    table = contract["committed_state_dq"]["duplicate_tables"][0]
    contract["committed_state_dq"]["duplicate_tables"].append(table)
    with pytest.raises(SofaScoreContractError, match="contains duplicates"):
        validate_coverage_contract(contract)


def test_invalid_matrix_cannot_add_a_fifth_status():
    contract = deepcopy(load_coverage_contract())
    contract["coverage_statuses"].append("ignored")
    with pytest.raises(SofaScoreContractError):
        validate_coverage_contract(contract)


def test_matrix_summary_and_materializer_claims_are_fail_closed():
    contract = deepcopy(load_coverage_contract())
    contract["coverage_summary"]["normalized"] += 1
    with pytest.raises(SofaScoreContractError, match="coverage_summary drift"):
        validate_coverage_contract(contract)

    contract = deepcopy(load_coverage_contract())
    contract["tables"]["bronze.sofascore_events"]["materialized_by"] = (
        "dags/does_not_exist.py#writer"
    )
    with pytest.raises(SofaScoreContractError, match="path does not exist"):
        validate_coverage_contract(contract)


def test_uncaptured_endpoint_cannot_pretend_raw_or_normalized_coverage():
    contract = deepcopy(load_coverage_contract())
    graph = contract["endpoints"]["event_graph"]
    graph["status"] = "raw-only"
    contract["coverage_summary"]["raw-only"] = 1
    contract["coverage_summary"]["intentionally-excluded"] -= 1
    with pytest.raises(SofaScoreContractError, match="must declare preserved arrays"):
        validate_coverage_contract(contract)

    contract = deepcopy(load_coverage_contract())
    rounds = contract["endpoints"]["rounds"]
    rounds["destination"] = ["silver.sofascore_round"]
    with pytest.raises(SofaScoreContractError, match="cannot claim a destination"):
        validate_coverage_contract(contract)
