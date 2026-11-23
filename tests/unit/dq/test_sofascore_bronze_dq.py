"""Bronze-only SQL gates and scope accounting; no database is required."""

import re
from collections.abc import Mapping
from copy import deepcopy

import pytest

from dags.utils import sofascore_dq as dq
from dags.utils.sofascore_dq import ActiveRegistryPartition


SCOPE = {
    "capture_key": "SS-17",
    "canonical_season": "2526",
    "tournament_id": 17,
    "source_season_id": 76986,
}
CHILDREN = (
    "sofascore_player_ratings",
    "sofascore_event_player_stats",
    "sofascore_match_stats",
    "sofascore_event_shotmap",
)


def _name(query):
    return query["name"] if isinstance(query, Mapping) else query.name


def _sql(query):
    return query["sql"] if isinstance(query, Mapping) else query.sql


def _queries(require_complete=True):
    return dq.build_bronze_partition_dq_queries(
        "SS-17", "2526", 17, 76986, require_complete=require_complete
    )


def _named(queries, name):
    matches = [query for query in queries if name in _name(query)]
    assert len(matches) == 1, (name, [_name(query) for query in queries])
    return matches[0]


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.closed = False
        self.value = 0

    def execute(self, sql, parameters=None):
        self.connection.executed.append(sql)
        if self.connection.exception_substring and self.connection.exception_substring in sql:
            raise RuntimeError("synthetic Bronze query failure")
        self.value = 1 if sql in self.connection.nonempty_sql else 0
        for substring, value in self.connection.overrides.items():
            if substring in sql:
                self.value = value
        return self

    def fetchone(self):
        return (self.value,)

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class FakeConnection:
    def __init__(self, queries, *, overrides=None, exception_substring=None):
        self.overrides = overrides or {}
        self.exception_substring = exception_substring
        self.nonempty_sql = {
            _sql(query)
            for query in queries
            if "expected" in _name(query).lower()
            and ("nonempty" in _name(query).lower().replace("_", "")
                 or "universe" in _name(query).lower())
        }
        assert self.nonempty_sql, "The expected Bronze universe needs a nonempty gate"
        self.executed = []
        self.cursors = []
        self.closed = False

    def cursor(self):
        cursor = FakeCursor(self)
        self.cursors.append(cursor)
        return cursor

    def close(self):
        self.closed = True


def _assert_identity(report):
    assert report["league"] == "SS-17"
    assert report["season"] == "2526"
    assert str(report["source_tournament_id"]) == "17"
    assert str(report["source_season_id"]) == "76986"
    assert {"status", "checks", "violations", "errors"} <= report.keys()


def test_builder_covers_bronze_correctness_without_silver_or_gold():
    queries = _queries()
    names = " ".join(_name(query).lower() for query in queries)
    sql = "\n".join(_sql(query).lower() for query in queries)
    assert "iceberg.silver" not in sql
    assert "iceberg.gold" not in sql
    assert "bronze." in sql
    assert "duplicate" in names
    assert "null" in names
    assert "group by" in sql and "having" in sql
    for name in (
        "skeleton_schedule_rows",
        "adult_men_gender_tripwire",
        "required_endpoint_completeness",
    ):
        _named(queries, name)
    source_checks = [
        query for query in queries
        if "source_season" in _sql(query).lower()
        and "76986" in _sql(query)
    ]
    assert any("schedule" in _sql(query).lower() for query in source_checks)
    assert any("event" in _sql(query).lower() for query in source_checks)


@pytest.mark.parametrize("table", CHILDREN)
def test_child_schedule_join_uses_match_identity_across_seasons(table):
    candidates = [
        query for query in _queries()
        if table in _sql(query).lower()
        and "join" in _sql(query).lower()
        and "s.game_id is null" in " ".join(_sql(query).lower().split())
    ]
    assert candidates, f"Missing orphan/season gate for {table}"
    sql = " ".join(_sql(candidates[0]).lower().split())
    assert re.search(r"cast\s*\(\s*s\.game_id\s+as\s+varchar\s*\)", sql)
    assert (
        "cast(s.game_id as varchar) = cast(c.match_id as varchar)" in sql
        or "cast(s.game_id as varchar) = c.match_id" in sql
    )
    join = sql.split(" join ", 1)[1].split(" where ", 1)[0]
    assert not re.search(r"\b\w+\.season\s*=\s*\w+\.season\b", join)
    assert not re.search(r"\b\w+\.season\s*=\s*'2526'", join)
    where = sql.split(" where ", 1)[1]
    assert "s.game_id is null" in where
    assert "s.league is distinct from c.league" in where
    assert "cast(s.season as varchar) is distinct from cast(c.season as varchar)" in where


def test_schedule_gate_detects_game_ids_shared_by_distinct_seasons():
    candidates = [
        query for query in _queries()
        if "schedule" in _sql(query).lower()
        and re.search(r"count\s*\(\s*distinct\s+(?:\w+\.)?season\s*\)", _sql(query), re.I)
    ]
    assert candidates, "A game_id spanning schedule seasons must fail DQ"
    sql = " ".join(_sql(candidates[0]).lower().split())
    assert "game_id" in sql and "group by" in sql
    assert re.search(r"count\s*\(\s*distinct\s+(?:\w+\.)?season\s*\)\s*>\s*1", sql)
    # Filtering the rows being counted to 2526 makes this gate tautologically zero.
    distinct = re.search(r"count\s*\(\s*distinct\s+(?:\w+\.)?season\s*\)", sql)
    select_start = sql.rfind("select", 0, distinct.start())
    assert select_start >= 0, sql
    # COUNT(DISTINCT season) may be in SELECT or HAVING. Inspect the FROM /
    # WHERE of that aggregation in either form, not an unrelated outer scope.
    group_by = sql.find("group by", select_start)
    assert group_by >= 0, sql
    aggregation = sql[select_start:max(group_by, distinct.end())]
    assert not re.search(r"(?:\w+\.)?season\s*=\s*'2526'", aggregation)
    assert not re.search(r"\b\w+\.season\s*=\s*\w+\.season\b", aggregation)


def test_partial_mode_removes_only_required_endpoint_completeness():
    complete = {_name(query): _sql(query) for query in _queries(True)}
    partial = {_name(query): _sql(query) for query in _queries(False)}
    completeness = _name(_named(_queries(True), "required_endpoint_completeness"))
    assert partial == {name: sql for name, sql in complete.items() if name != completeness}


def test_clean_partition_executes_every_gate_and_keeps_injected_connection_open():
    queries = _queries()
    connection = FakeConnection(queries)
    report = dq.run_bronze_partition_dq(
        ActiveRegistryPartition("SS-17", "2526", 17, 76986), connection
    )
    _assert_identity(report)
    assert report["status"] == "success"
    assert report["checks"] == len(queries)
    assert not report["violations"] and not report["errors"]
    assert connection.executed == [_sql(query) for query in queries]
    assert connection.cursors and all(cursor.closed for cursor in connection.cursors)
    assert not connection.closed


@pytest.mark.parametrize("failure", ["mismatch", "exception", "null"])
def test_bad_scalar_or_sql_failure_fails_closed_and_closes_cursors(failure):
    queries = _queries()
    target = next(
        query for query in queries
        if CHILDREN[0] in _sql(query) and "join" in _sql(query).lower()
    )
    sql = _sql(target)
    connection = FakeConnection(
        queries,
        overrides={sql: None if failure == "null" else 1},
        exception_substring=sql if failure == "exception" else None,
    )
    report = dq.run_bronze_partition_dq(
        ActiveRegistryPartition("SS-17", "2526", 17, 76986), connection
    )
    _assert_identity(report)
    assert report["status"] == "failed"
    details = repr((report["violations"], report["errors"]))
    assert report["violations"] or report["errors"]
    assert _name(target) in details or CHILDREN[0] in details
    if failure == "exception":
        assert "synthetic Bronze query failure" in details
    assert all(cursor.closed for cursor in connection.cursors)
    assert not connection.closed


def test_required_completeness_correlates_quarantine_with_endpoint_scope():
    query = _named(_queries(), "required_endpoint_completeness")
    sql = " ".join(_sql(query).lower().split())
    assert "bronze.sofascore_rejected_rows" in sql
    assert "schema_error" in sql
    assert "exists" in sql or " join " in sql
    assert "row_json" in sql
    assert "json_extract_scalar" in sql
    assert re.search(r"(?:match_id|target_id)\s*=\s*(?:match_id|target_id)", sql) or "json_extract_scalar" in sql
    for identity in ("league", "season"):
        assert re.search(rf"\b\w+\.{identity}\s*=\s*(?:\w+\.{identity}|'(?:SS-17|2526)')", sql, re.I)
    # A bare schema_error is not an ordinary terminal endpoint state.
    for state_list in re.findall(r"\bin\s*\(([^()]*)\)", sql):
        if "'schema_error'" in state_list:
            assert not re.search(r"'(?:success|ok|empty|not_found|no_data|completed)'", state_list)
    assert re.search(r"manifest|expected|planned|required_endpoint", sql)
    assert re.search(r"not exists|is null|left join|except", sql)


def test_missing_required_endpoint_evidence_is_a_violation():
    queries = _queries()
    query = _named(queries, "required_endpoint_completeness")
    connection = FakeConnection(queries, overrides={_sql(query): 1})
    report = dq.run_bronze_partition_dq(
        ActiveRegistryPartition("SS-17", "2526", 17, 76986), connection
    )
    assert report["status"] == "failed"
    assert _name(query) in repr(report["violations"])


@pytest.fixture
def scope_runner(monkeypatch):
    calls = []

    def run(partition, connection, *, contract=None, require_complete=True,
            require_player_universe=True):
        calls.append((
            partition, connection, contract, require_complete, require_player_universe
        ))
        return {
            "status": "success", "league": "SS-17", "season": "2526",
            "source_tournament_id": 17, "source_season_id": 76986,
            "checks": 1, "violations": [], "errors": [],
        }

    monkeypatch.setattr(dq, "run_bronze_partition_dq", run)
    return calls


def _phase(planned=5, terminal=4, rejected=1):
    return {
        "phase": "matches", "status": "success", "errors": [],
        "planned_endpoints": planned, "terminal_endpoints": terminal,
        "rejected_endpoints": rejected, "rejected_rows": {"schema_error": 2},
    }


def test_scope_maps_identity_and_aggregates_rejected_separately(scope_runner):
    connection, contract = object(), object()
    phases = [_phase(), {**_phase(2, 2, 0), "phase": "events", "rejected_rows": {}}]
    report = dq.run_scope_bronze_dq(
        SCOPE, phases, require_complete=True, connection=connection, contract=contract
    )
    _assert_identity(report)
    assert report["status"] == "success"
    assert scope_runner == [
        (ActiveRegistryPartition("SS-17", "2526", 17, 76986), connection, contract, True, False)
    ]
    assert report["planned_endpoints"] == 7
    assert report["terminal_endpoints"] == 6
    assert report["rejected_endpoints"] == 1
    assert report["rejected_rows"] == {"schema_error": 2}


@pytest.mark.parametrize("counts", [(5, 4, 0), (5, 5, 1), (5, 6, 0), (5, 4, -1)])
def test_complete_scope_fails_missing_or_inconsistent_accounting(scope_runner, counts):
    report = dq.run_scope_bronze_dq(
        SCOPE, [_phase(*counts)], require_complete=True, connection=object()
    )
    assert report["status"] == "failed"
    assert report["violations"] or report["errors"]


def test_partial_scope_forwards_incomplete_mode_and_retains_counters(scope_runner):
    report = dq.run_scope_bronze_dq(
        SCOPE, [_phase(5, 4, 0)], require_complete=False, connection=object()
    )
    assert scope_runner[0][3] is False
    assert report["status"] == "success"
    assert report["planned_endpoints"] == 5
    assert report["terminal_endpoints"] == 4
    assert report["rejected_rows"] == {"schema_error": 2}


def test_history_scope_defers_player_universe_gate_to_players_lane(scope_runner):
    connection, contract = object(), object()
    report = dq.run_scope_bronze_dq(
        SCOPE, [_phase()], require_complete=True, connection=connection, contract=contract
    )
    assert scope_runner[0][4] is False
    assert report["status"] == "success"


def test_players_scope_keeps_player_universe_gate(scope_runner):
    connection, contract = object(), object()
    report = dq.run_scope_bronze_dq(
        SCOPE, [{**_phase(), "phase": "players"}], require_complete=True,
        connection=connection, contract=contract,
    )
    assert scope_runner[0][4] is True
    assert report["status"] == "success"


def test_query_builder_respects_player_universe_scope():
    names = {query.name for query in dq.build_bronze_partition_dq_queries(
        "SS-17", "2526", 17, 76986, require_player_universe=False
    )}
    assert "player_profile_expected_universe_nonempty" not in names
    names = {query.name for query in dq.build_bronze_partition_dq_queries(
        "SS-17", "2526", 17, 76986, require_player_universe=True
    )}
    assert "player_profile_expected_universe_nonempty" in names


def test_league_table_natural_key_uses_source_id_with_legacy_fallback():
    contract = deepcopy(dq.load_coverage_contract())
    assert contract["bronze_compatibility_columns"]["sofascore_league_table"] == [
        ["team_id", "team"]
    ]
    duplicate = _named(_queries(), "duplicate_natural_key[bronze.sofascore_league_table]")
    assert '"group", COALESCE("team_id", "team")' in _sql(duplicate)
    null = _named(_queries(), "null_natural_key[bronze.sofascore_league_table]")
    assert 'COALESCE("team_id", "team") IS NULL' in _sql(null)


@pytest.mark.parametrize("missing_section", [False, True], ids=["entry", "section"])
def test_league_table_legacy_contract_without_fallback_uses_declared_key(missing_section):
    contract = deepcopy(dq.load_coverage_contract())
    if missing_section:
        contract.pop("bronze_compatibility_columns")
    else:
        contract["bronze_compatibility_columns"].pop("sofascore_league_table")
    duplicate = _named(
        dq.build_partition_dq_queries("SS-17", "2526", 17, 76986, contract=contract),
        "duplicate_natural_key[bronze.sofascore_league_table]",
    )
    assert "COALESCE" not in _sql(duplicate)
    assert '"team_id"' in _sql(duplicate)


def test_duplicate_key_uses_declared_compatibility_for_any_bronze_table():
    contract = deepcopy(dq.load_coverage_contract())
    table_name = "bronze.sofascore_example"
    contract["tables"][table_name] = {
        "natural_key": ["league", "season", "entity_id"],
        "partition_columns": ["league", "season"],
    }
    contract["committed_state_dq"]["duplicate_tables"].append(table_name)
    contract["bronze_compatibility_columns"]["sofascore_example"] = [
        ["entity_id", "entity_name"]
    ]
    duplicate = _named(
        dq.build_partition_dq_queries("SS-17", "2526", 17, 76986, contract=contract),
        f"duplicate_natural_key[{table_name}]",
    )
    assert 'COALESCE("entity_id", "entity_name")' in _sql(duplicate)


def test_natural_key_rejects_multiple_compatibility_fallbacks():
    contract = deepcopy(dq.load_coverage_contract())
    contract["bronze_compatibility_columns"]["sofascore_league_table"].append(
        ["team_id", "team_name"]
    )
    with pytest.raises(dq.SofaScoreContractError, match="multiple compatibility fallbacks"):
        dq.build_partition_dq_queries("SS-17", "2526", 17, 76986, contract=contract)
