import hashlib
import json
from dataclasses import replace

import duckdb
import pytest

from dags.utils.transfermarkt_native_replay import QueryPair, compare_many, compare_query_pair


@pytest.fixture
def cur():
    conn = duckdb.connect(":memory:")
    try:
        yield conn
    finally:
        conn.close()


def values_pair(legacy: str, native: str) -> QueryPair:
    def sql(rows: str) -> str:
        return f"SELECT * FROM (VALUES {rows}) AS v(id, label)"

    return QueryPair("players", sql(legacy), sql(native), ("id",))


def test_equal_multisets_with_nulls(cur):
    pair = values_pair("(NULL, 'A'), (1, NULL), (1, NULL)", "(1, NULL), (NULL, 'A'), (1, NULL)")
    result = compare_query_pair(cur, pair)
    assert result["passed"]
    assert result["legacy_sql_sha256"] == hashlib.sha256(pair.legacy_sql.encode()).hexdigest()
    assert result["native_sql_sha256"] == hashlib.sha256(pair.native_sql.encode()).hexdigest()
    assert result["legacy_row_count"] == result["native_row_count"] == 3
    assert result["multiset_diff"] == result["keyed_diff"] == 0
    assert result["sample_keys"] == []


def test_duplicate_native_copy(cur):
    report = compare_query_pair(cur, values_pair("(1, 'A')", "(1, 'A'), (1, 'A')"))
    assert not report["passed"]
    assert (report["legacy_row_count"], report["native_row_count"]) == (1, 2)
    assert report["keyed_diff"] == report["multiset_diff"] == 1
    assert report["native_excess_copies"] == report["native_missing_or_value_mismatch_keys"] == 1
    assert report["legacy_excess_copies"] == 0
    assert report["sample_keys"] == [{"id": 1}]


def test_empty_results(cur):
    sql = "SELECT 1 AS id WHERE FALSE"
    result = compare_query_pair(cur, QueryPair("empty", sql, sql, ("id",)))
    assert result["passed"]
    assert result["legacy_row_count"] == result["native_row_count"] == 0
    assert result["sample_keys"] == []


def test_null_value_changes_at_same_key_and_count(cur):
    pair = values_pair("(NULL, 'A'), (2, NULL)", "(NULL, NULL), (2, 'A')")
    result = compare_query_pair(cur, pair, sample_limit=1)
    assert not result["passed"]
    assert result["keyed_diff"] == 0
    assert result["multiset_diff"] == 4
    assert result["legacy_missing_or_value_mismatch_keys"] == 2
    assert result["native_missing_or_value_mismatch_keys"] == 2
    assert len(result["sample_keys"]) == 1
    assert set(result["sample_keys"][0]) == {"id"}
    assert compare_query_pair(cur, replace(pair, compare_columns=("id",)))["passed"]


def test_missing_key_on_each_side(cur):
    result = compare_query_pair(cur, values_pair("(NULL, 'A'), (2, 'B')", "(3, 'A'), (2, 'B')"))
    assert not result["passed"]
    assert result["keyed_diff"] == 2
    assert {row["id"] for row in result["sample_keys"]} == {None, 3}
    assert result["legacy_excess_copies"] == result["native_excess_copies"] == 1


def test_value_multiplicities_differ_with_equal_key_counts(cur):
    pair = values_pair("(1, 'A'), (1, 'A'), (1, 'B')", "(1, 'A'), (1, 'B'), (1, 'B')")
    result = compare_query_pair(cur, pair, sample_limit=0)
    assert not result["passed"]
    assert result["keyed_diff"] == 0
    assert result["multiset_diff"] == 2
    assert result["legacy_excess_copies"] == result["native_excess_copies"] == 1
    assert result["sample_keys"] == []


def test_composite_key_and_reordered_projection(cur):
    def sql(label: str) -> str:
        return f"""
            SELECT * FROM (VALUES (NULL, 2, '{label}', 99), (3, NULL, 'B', 42))
            AS v(player, season, label, copies)
        """

    pair = QueryPair(
        "player_seasons", sql("A"), sql("C"), ("player", "season"),
        ("season", "copies", "player", "label"),
    )
    result = compare_query_pair(cur, pair)
    assert not result["passed"]
    assert result["keyed_diff"] == 0 and result["multiset_diff"] == 2
    assert result["sample_keys"] == [{"player": None, "season": 2}]


def test_replay_registration_join_and_downstream_aggregation(cur):
    # The third registration moved between clubs. Each club still matches and
    # the league total is unchanged, while the two players' join counts change.
    cur.execute("CREATE TABLE squads AS SELECT * FROM (VALUES (10, 1), (20, 2)) s(player, club)")
    for table, extra_club in (("legacy", 1), ("native", 2)):
        cur.execute(f"""
            CREATE TABLE {table}_registrations AS
            SELECT * FROM (VALUES (1, 1), (2, 2), (3, {extra_club})) r(registration, club)
        """)

    def consumer(name: str, select: str, suffix: str = "") -> QueryPair:
        def query(side: str) -> str:
            return f"SELECT {select} FROM squads JOIN {side}_registrations USING (club) {suffix}"

        return QueryPair(name, query("legacy"), query("native"), ("player",))

    joined = consumer("registered_players", "player, club")
    totals = consumer("registrations_per_player", "player, COUNT(*) AS registrations", "GROUP BY player")
    same = QueryPair("squad_players", "SELECT * FROM squads", "SELECT * FROM squads", ("player",))
    report = compare_many(cur, [joined, totals, same])
    assert not report["passed"]
    assert [r["name"] for r in report["queries"]] == [joined.name, totals.name, same.name]
    join_result, total_result, unchanged = report["queries"]
    assert not join_result["passed"] and not total_result["passed"]
    assert unchanged["passed"]
    assert join_result["legacy_row_count"] == join_result["native_row_count"] == 3
    assert join_result["keyed_diff"] == join_result["multiset_diff"] == 2
    assert total_result["legacy_row_count"] == total_result["native_row_count"] == 2
    assert total_result["keyed_diff"] == 0
    assert total_result["multiset_diff"] == 4


class ProbeCursor:
    def __init__(self, cur):
        self.cur = cur
        self.calls = 0

    def execute(self, sql):
        assert sql.startswith("SELECT * FROM (") and sql.endswith("AS schema_probe LIMIT 0")
        self.calls += 1
        self.cur.execute(sql)
        self.description = self.cur.description


@pytest.mark.parametrize("native", [
    "SELECT 2 AS label, 1 AS id",
    "SELECT 1 AS id, CAST(2 AS BIGINT) AS label",
])
def test_schema_mismatch_only_executes_probes(cur, native):
    probes = ProbeCursor(cur)
    result = compare_query_pair(probes, QueryPair("schema", "SELECT 1 AS id, 2 AS label", native, ("id",)))
    assert not result["passed"] and "reason" in result
    assert probes.calls == 2


def test_absent_compare_column_only_executes_probes(cur):
    probes = ProbeCursor(cur)
    pair = replace(values_pair("(1, 'A')", "(1, 'A')"), compare_columns=("id", "absent"))
    assert not compare_query_pair(probes, pair)["passed"]
    assert probes.calls == 2


def test_comparison_error_is_reported_and_next_query_runs(cur):
    good = values_pair("(1, 'A')", "(1, 'A')")
    bad = replace(good, name="missing_relation", native_sql="SELECT * FROM absent_players")
    result = compare_many(cur, [bad, good])
    assert not result["passed"]
    failed, passed = result["queries"]
    assert failed["name"] == bad.name and not failed["passed"] and failed["reason"]
    assert passed["name"] == good.name and passed["passed"]


class NoExecute:
    def execute(self, sql):
        pytest.fail("invalid replay reached the cursor")


@pytest.mark.parametrize("bad", [
    "PRAGMA version", "CREATE TABLE x(i INT)", "VALUES (1); SELECT 2",
    "SELECT 1; -- comment\nDELETE FROM players", "SELECT (SELECT 1; SELECT 2)",
    "WITH removed AS (DELETE FROM players RETURNING id) SELECT * FROM removed",
    "WITH x AS (VALUES (1)) INSERT INTO players SELECT * FROM x",
    "SELECT 1) AS src", "SELECT 'unterminated", "SELECT 1 /* unterminated",
    """SELECT * FROM system."query" /* nested SQL */ ('DELETE FROM players')""",
])
def test_rejects_unsafe_query_before_execute(bad):
    with pytest.raises(ValueError):
        compare_query_pair(NoExecute(), QueryPair("bad", "SELECT 1 AS id", bad, ("id",)))


@pytest.mark.parametrize("fields", [
    {"name": "  "}, {"key_columns": ()}, {"key_columns": "id"},
    {"key_columns": ('id" OR 1=1',)}, {"compare_columns": ("id", "x;drop")},
    {"compare_columns": ("label",)}, {"compare_columns": ("id", "ID")},
])
def test_invalid_metadata_before_execute(fields):
    with pytest.raises(ValueError):
        compare_query_pair(NoExecute(), replace(values_pair("(1, 'A')", "(2, 'B')"), **fields))


def test_empty_or_duplicate_pair_names_before_execute():
    pair = values_pair("(1, 'A')", "(1, 'A')")
    for pairs in ([], [pair, replace(pair, name=" players ")]):
        with pytest.raises(ValueError):
            compare_many(NoExecute(), pairs)


@pytest.mark.parametrize("limit", [-1, 101, True, "1"])
def test_invalid_sample_limit_before_execute(limit):
    with pytest.raises(ValueError):
        compare_query_pair(NoExecute(), values_pair("(1, 'A')", "(1, 'A')"), sample_limit=limit)


def test_standard_sql_quotes_and_terminal_separator(cur):
    sql = """WITH v AS (VALUES ('it''s; DELETE')) SELECT * FROM v AS q("id"); -- end"""
    assert compare_query_pair(cur, QueryPair("quoted", sql, sql, ("id",)))["passed"]


def test_cli_input_requires_snapshot_and_watermark_metadata(tmp_path):
    from scripts import replay_transfermarkt_native_v2 as replay

    spec = {
        "queries": [{
            "name": "players",
            "legacy_sql": "SELECT 1 AS id",
            "native_sql": "SELECT 1 AS id",
            "key_columns": ["id"],
        }],
    }
    path = tmp_path / "replay.json"
    path.write_text(json.dumps(spec), encoding="utf-8")
    with pytest.raises(ValueError, match="snapshot ids"):
        replay._read_pairs(path)

    spec["metadata"] = {
        "legacy_snapshot_ids": {"players": 10},
        "native_snapshot_ids": {"memberships": 11},
        "common_watermark": "2026-11-21T00:00:00Z",
    }
    path.write_text(json.dumps(spec), encoding="utf-8")
    pairs, metadata = replay._read_pairs(path)
    assert len(pairs) == 1
    assert metadata["common_watermark"] == "2026-11-21T00:00:00Z"
