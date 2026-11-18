"""Offline behavioral tests for partition-batched staging and metadata swap."""

from collections import Counter
from copy import deepcopy
import re

import pytest
import sqlglot
from sqlglot import exp

from scripts import migrate_sofascore_partition_keys as migration
from scripts import migrate_sofascore_partition_keys_batched as batched
from tests.unit.scripts.test_migrate_sofascore_partition_keys import (
    COLUMNS,
    SPEC,
    FakeManager,
    canonical_rows,
    sample_rows,
    winners,
    multiset,
)


def sql_pairs(sql):
    """Read exact pair predicates, never independent league/season IN lists."""
    pairs = re.findall(r"= '(SS-[1-9][0-9]*)' AND \"season\" = '((?:''|[^'])*)'", sql)
    assert pairs, sql
    return {(league, season.replace("''", "'")) for league, season in pairs}


def selected_rows(rows, pairs):
    return [row for row in rows if (row["league"], row["season"]) in pairs]


def partition_inventory_sql(sql, stage):
    query = re.sub(r"^/\*.*?\*/\s*", "", sql).strip()
    return bool(re.fullmatch(
        r'SELECT "?league"?,\s*"?season"?,\s*count\(\*\)(?: AS "?\w+"?)? FROM '
        + re.escape(migration.qualified_name(stage))
        + r' GROUP BY "?league"?,\s*"?season"?(?: ORDER BY "?league"?,\s*"?season"?)?',
        query, flags=re.I,
    ))


class BatchedFakeManager(FakeManager):
    def __init__(self, rows=None, *, columns=None, fail_on_rename=False, damage_after_swap=False):
        super().__init__(rows, columns=columns)
        self.archive = migration.archive_name(SPEC, "r2-synthetic", "archive")
        self.stage = batched.stage_name(SPEC, "r2-synthetic", "archive")
        self.legacy = batched.legacy_name(SPEC, "r2-synthetic", "archive")
        self.tables[self.archive] = deepcopy(self.tables[SPEC.name])
        self.stage_ddl = None
        self.fail_on_rename = fail_on_rename
        self.damage_after_swap = damage_after_swap
        self.failed_once = False

    def _rename(self, sql):
        match = re.fullmatch(
            r'ALTER TABLE "([^"]+)"\."([^"]+)" RENAME TO "([^"]+)"\."([^"]+)"', sql
        )
        assert match, sql
        source = match.group(1) + "." + match.group(2)
        target = match.group(3) + "." + match.group(4)
        if target == SPEC.name and self.fail_on_rename and not self.failed_once:
            self.failed_once = True
            raise RuntimeError("injected rename failure")
        self.tables[target] = self.tables.pop(source)
        if target == SPEC.name and self.damage_after_swap and not self.failed_once:
            self.failed_once = True
            self.tables[target].pop()
        if source == self.stage:
            self.stage_ddl = None

    def _execute(self, sql, fetch=False):
        if partition_inventory_sql(sql, self.stage):
            self.statements.append(sql)
            assert fetch
            counts = Counter((row["league"], row["season"]) for row in self.tables[self.stage])
            return [(league, season, count) for (league, season), count in counts.items()]
        if sql.startswith("SHOW CREATE TABLE"):
            self.statements.append(sql)
            if sql.endswith('"' + self.stage.split(".")[-1] + '"') and self.stage_ddl:
                return [(self.stage_ddl,)]
            return [(self.ddl_text or "CREATE TABLE t WITH (format='PARQUET', partitioning=ARRAY['league','season'])",)]
        if sql.startswith("CREATE TABLE " + migration.qualified_name(self.stage)):
            self.statements.append(sql)
            assert not fetch
            self.tables[self.stage] = []
            self.stage_ddl = sql.split(") AS WITH canonical", 1)[0] + ")"
            return None
        if sql.startswith("INSERT INTO"):
            self.statements.append(sql)
            assert not fetch
            assert sql.startswith("INSERT INTO " + migration.qualified_name(self.stage))
            expected = selected_rows(winners(self.tables[self.archive]), sql_pairs(sql))
            self.tables[self.stage].extend(deepcopy(expected))
            return None
        if sql.startswith("ALTER TABLE"):
            self.statements.append(sql)
            self._rename(sql)
            return None
        if sql.startswith("SELECT DISTINCT"):
            self.statements.append(sql)
            rows = canonical_rows(self.tables[self.archive])
            pairs = sorted({(row["league"], row["season"]) for row in rows})
            return [list(pair) for pair in pairs]
        if sql.startswith("/* pk:batch_rows */"):
            self.statements.append(sql)
            assert fetch
            if "ROW_NUMBER" in sql:
                rows = selected_rows(winners(self.tables[self.archive]), sql_pairs(sql))
            else:
                rows = selected_rows(self.tables[self.stage], sql_pairs(sql))
            return [(len(rows),)]
        if sql.startswith("/* pk:batch_payload_difference */"):
            self.statements.append(sql)
            assert fetch
            assert sql.count("EXCEPT ALL") == 2 and "UNION ALL" in sql
            expected = selected_rows(winners(self.tables[self.archive]), sql_pairs(sql))
            actual = selected_rows(self.tables[self.stage], sql_pairs(sql))
            left = multiset(expected, self.columns)
            right = multiset(actual, self.columns)
            value = sum((left - right).values()) + sum((right - left).values())
            return [(value,)]
        target = None
        match = re.search(r'actual AS \(SELECT DISTINCT .* FROM "([^"]+)"\."([^"]+)"\)', sql)
        if match:
            target = match.group(1) + "." + match.group(2)
        elif sql.startswith("/* pk:payload_mismatches"):
            target = self.stage if self.stage in self.tables else SPEC.name
        if target is not None:
            original = self.tables[SPEC.name]
            self.tables[SPEC.name] = self.tables[target]
            try:
                return super()._execute(sql, fetch=fetch)
            finally:
                self.tables[SPEC.name] = original
        return super()._execute(sql, fetch=fetch)


def execute(manager, **kwargs):
    return batched.run_batched_rewrite(
        manager,
        SPEC,
        manager.archive,
        {"ENG": "SS-17"},
        "r2-synthetic",
        "archive",
        **kwargs,
    )


def create_stage(manager):
    manager._execute(batched.create_stage_sql(
        batched.stage_spec(SPEC, manager.stage), manager.archive,
        manager.columns, {"ENG": "SS-17"},
        "CREATE TABLE t WITH (format='PARQUET', partitioning=ARRAY['league','season'])",
    ))


def populate(manager, **kwargs):
    return batched.populate_stage(
        manager, SPEC, manager.stage, manager.archive, manager.columns,
        {"ENG": "SS-17"}, **kwargs,
    )


def many_seasons(count=40, *, two_leagues=False):
    rows = []
    for index in range(count):
        season_rows = sample_rows()
        for row in season_rows:
            # Reuse IDs across seasons: the whole declared natural key matters.
            row["season"] = f"season-{index:03d}-O'Brien"
        rows.extend(season_rows)
        if two_leagues:
            other = deepcopy(season_rows)
            for row in other:
                row["league"] = "SS-18"
            rows.extend(other)
    return rows


def inserts(manager):
    return [sql for sql in manager.statements if sql.startswith("INSERT INTO")]


def assert_preserved(manager, original):
    assert manager.tables[SPEC.name] == original
    assert manager.tables[manager.archive] == original
    assert not any(sql.startswith("ALTER") for sql in manager.statements)


def assert_reports(result, archive, group_size):
    assert isinstance(result, dict) and type(result["complete"]) is bool
    seen = set()
    for item in result["batches"]:
        assert isinstance(item["partitions"], list)
        assert 1 <= len(item["partitions"]) <= group_size
        assert all(isinstance(pair, list) and len(pair) == 2 for pair in item["partitions"])
        pairs = {tuple(pair) for pair in item["partitions"]}
        assert len(pairs) == len(item["partitions"])
        assert not seen & pairs
        seen.update(pairs)
        expected = selected_rows(winners(archive), pairs)
        assert item["expected_rows"] == item["after_rows"] == len(expected)
        assert ("inserted_rows" in item) != ("noop_rows" in item)
        if "inserted_rows" in item:
            assert item["before_rows"] == 0
            assert item["inserted_rows"] == len(expected)
        else:
            assert item["before_rows"] == item["noop_rows"] == len(expected)
    return seen


def test_stage_sql_is_empty_and_preserves_non_location_properties():
    original = (
        "CREATE TABLE t COMMENT 'grain text' WITH (format='PARQUET', format_version=2, "
        "location='s3://bucket/original', partitioning=ARRAY['league','season'])"
    )
    sql = batched.create_stage_sql(SPEC, "archive.original", COLUMNS, {"ENG": "SS-17"}, original)
    assert "s3://bucket/original" not in sql
    assert "COMMENT 'grain text'" in sql
    assert "format_version=2" in sql
    assert "partitioning=ARRAY['league','season']" in sql
    assert "WHERE 1 = 0" in sql
    sqlglot.parse_one(sql, read="trino")
    filtered = batched.filtered_winner_select(
        SPEC, "archive.original", COLUMNS, {"ENG": "SS-17"}, '"league" = \'SS-17\''
    )
    assert filtered.index("WHERE") < filtered.index("ROW_NUMBER")
    sqlglot.parse_one(filtered, read="trino")


def test_full_batched_swap_preserves_archive_legacy_and_match_ids():
    rows = sample_rows()
    rows[0]["season"] = rows[1]["season"] = "2025-26"
    manager = BatchedFakeManager(rows)
    result = execute(manager)
    assert result["success"] is True, result
    assert result["status"] == "applied_verified"
    assert len(result["batches"]) == 1
    assert result["batches"][0]["partitions"] == [["SS-17", "2025-26"], ["SS-17", "2026-27"]]
    assert {row["match_id"] for row in manager.tables[SPEC.name]} == {101, 102, 103}
    assert all(row["league"] == "SS-17" for row in manager.tables[SPEC.name])
    assert len(manager.tables[manager.archive]) == 4
    assert len(manager.tables[manager.legacy]) == 4
    assert result["reconciliation"]["payload_mismatches"] == 0
    assert result["reconciliation"]["duplicate_keys"] == 0
    assert not any(re.search(r"\b(?:DELETE|DROP|TRUNCATE)\b", sql, re.I) for sql in manager.statements)


def test_second_rename_and_repeat_are_noops():
    manager = BatchedFakeManager()
    assert execute(manager)["success"] is True
    manager.statements.clear()
    result = execute(manager)
    assert result["success"] is True and result["status"] == "noop_verified"
    assert not any(sql.startswith(("CREATE", "ALTER", "INSERT")) for sql in manager.statements)
    assert len(manager.tables[manager.archive]) == 4
    assert len(manager.tables[manager.legacy]) == 4


def test_completed_batch_is_noop_and_mismatched_stage_fails_closed():
    manager = BatchedFakeManager()
    manager._execute(
        batched.create_stage_sql(
            batched.stage_spec(SPEC, manager.stage),
            manager.archive,
            COLUMNS,
            {"ENG": "SS-17"},
            "CREATE TABLE t WITH (format='PARQUET', partitioning=ARRAY['league','season'])",
        )
    )
    batched.populate_stage(manager, SPEC, manager.stage, manager.archive, COLUMNS, {"ENG": "SS-17"})
    manager.statements.clear()
    repeated = batched.populate_stage(manager, SPEC, manager.stage, manager.archive, COLUMNS, {"ENG": "SS-17"})
    assert repeated["complete"] is True
    assert all(item.get("noop_rows") == item["expected_rows"] for item in repeated["batches"])
    assert not any(sql.startswith("INSERT INTO") for sql in manager.statements)
    manager.tables[manager.stage].append(manager.tables[manager.stage][0])
    with pytest.raises(ValueError):
        batched.populate_stage(manager, SPEC, manager.stage, manager.archive, COLUMNS, {"ENG": "SS-17"})


def test_existing_group_payload_difference_nests_winner_select_for_trino():
    manager = BatchedFakeManager()
    manager._execute(
        batched.create_stage_sql(
            batched.stage_spec(SPEC, manager.stage),
            manager.archive,
            COLUMNS,
            {"ENG": "SS-17"},
            "CREATE TABLE t WITH (format='PARQUET', partitioning=ARRAY['league','season'])",
        )
    )
    batched.populate_stage(manager, SPEC, manager.stage, manager.archive, COLUMNS, {"ENG": "SS-17"})
    manager.statements.clear()

    batched.populate_stage(manager, SPEC, manager.stage, manager.archive, COLUMNS, {"ENG": "SS-17"})

    differences = [
        sql for sql in manager.statements
        if sql.startswith("/* pk:batch_payload_difference */")
    ]
    assert differences
    for sql in differences:
        assert "FROM ((SELECT * FROM (WITH canonical AS" in sql


def test_second_rename_failure_rolls_back_without_row_loss():
    manager = BatchedFakeManager(fail_on_rename=True)
    original = deepcopy(manager.tables[SPEC.name])
    result = execute(manager)
    assert result["success"] is False
    assert result["rollback_error"] is None
    assert manager.tables[SPEC.name] == original
    assert len(manager.tables[manager.archive]) == 4


def test_postcheck_failure_rolls_back_without_row_loss():
    manager = BatchedFakeManager(damage_after_swap=True)
    original = deepcopy(manager.tables[SPEC.name])
    result = execute(manager)
    assert result["success"] is False
    assert result["rollback_error"] is None
    assert manager.tables[SPEC.name] == original
    assert len(manager.tables[manager.archive]) == 4


def test_unmapped_or_null_partition_identity_fails_before_mutation():
    manager = BatchedFakeManager()
    manager.tables[manager.archive][0]["league"] = None
    result = execute(manager)
    assert result["success"] is False and result["mutates"] is False
    assert manager.tables[SPEC.name] == sample_rows()


def test_locale_column_survives_swap():
    locale = "tournament_field_translations_name_translation_es-la"
    columns = {**COLUMNS, locale: "varchar"}
    rows = sample_rows()
    for row in rows:
        row[locale] = "value-" + str(row["match_id"])
    manager = BatchedFakeManager(rows, columns=columns)
    result = execute(manager)
    assert result["success"] is True
    assert {row[locale] for row in manager.tables[SPEC.name]} == {"value-101", "value-102", "value-103"}


def test_missing_archive_is_created_before_staging():
    manager = BatchedFakeManager()
    original = deepcopy(manager.tables[manager.archive])
    del manager.tables[manager.archive]
    result = execute(manager)
    assert result["success"] is True
    assert result["mutates"] is True
    assert manager.tables[manager.archive] == original


def assert_group_sql(sql):
    """Validate SQL structure separately from the relational fake's dispatch."""
    tree = sqlglot.parse_one(sql, read="trino")
    checked = 0
    canonical_pairs = set()
    actual_pairs = set()

    def disjuncts(node):
        if isinstance(node, exp.Paren):
            return disjuncts(node.this)
        if isinstance(node, exp.Or):
            return disjuncts(node.this) + disjuncts(node.expression)
        return [node]

    for where in tree.find_all(exp.Where):
        if not any(column.name == "season" for column in where.find_all(exp.Column)):
            continue
        leaves = disjuncts(where.this)
        for leaf in leaves:
            assert isinstance(leaf, exp.And), leaf
            season = leaf.expression
            assert isinstance(season, exp.EQ) and isinstance(season.this, exp.Column)
            assert season.this.name == "season" and isinstance(season.expression, exp.Literal)
            league = leaf.this
            assert isinstance(league, exp.EQ) and isinstance(league.expression, exp.Literal)
            assert migration.SS_KEY.fullmatch(league.expression.this)
            if isinstance(league.this, exp.Case):
                expected = sqlglot.parse_one(migration.league_expression({"ENG": "SS-17"}), read="trino")
                assert league.this == expected
                canonical_pairs.add((league.expression.this, season.expression.this))
            else:
                assert isinstance(league.this, exp.Column) and league.this.name == "league"
                actual_pairs.add((league.expression.this, season.expression.this))
            checked += 1
    assert checked > 0, sql
    if "ROW_NUMBER" in sql:
        assert canonical_pairs == sql_pairs(sql)
    if "EXCEPT ALL" in sql:
        assert actual_pairs == canonical_pairs
    elif "ROW_NUMBER" not in sql:
        assert actual_pairs == sql_pairs(sql) and not canonical_pairs
    for window in tree.find_all(exp.Window):
        assert [column.name for column in window.args["partition_by"]] == list(SPEC.natural_key)
        order = window.args["order"]
        assert [column.name for column in order.find_all(exp.Column)] == ["_ingested_at", "_batch_id"]
        assert "raw_content_hash" not in order.sql() and "payload" not in order.sql()
        assert all(item.args.get("desc") is True for item in order.expressions)
        # The surrounding SELECT still carries every original column.
        projected = {item.name for item in window.parent.parent.expressions if isinstance(item, exp.Column)}
        assert set(COLUMNS) <= projected


def test_forty_seasons_use_five_grouped_inserts_and_keep_every_winner():
    original = many_seasons()
    manager = BatchedFakeManager(original)
    create_stage(manager)
    manager.statements.clear()
    result = populate(manager, partition_group_size=8)
    assert result["complete"] is True
    assert len(result["batches"]) == len(inserts(manager)) == 5
    assert len(assert_reports(result, original, 8)) == 40
    assert multiset(manager.tables[manager.stage], COLUMNS) == multiset(winners(original), COLUMNS)
    assert len(manager.tables[manager.stage]) == 120
    assert {row["match_id"] for row in manager.tables[manager.stage]} == {101, 102, 103}
    assert_preserved(manager, original)
    seen = set()
    for sql in inserts(manager):
        pairs = sql_pairs(sql)
        assert len(pairs) == 8 and not seen & pairs
        seen.update(pairs)
        assert_group_sql(sql)
    assert seen == {(row["league"], row["season"]) for row in winners(original)}
    counts = [sql for sql in manager.statements if sql.startswith("/* pk:batch_rows */")]
    expected_counts = [sql for sql in counts if "ROW_NUMBER" in sql]
    actual_counts = [sql for sql in counts if "ROW_NUMBER" not in sql]
    assert len(expected_counts) == 5
    # One after count, optionally a before count; never another final count.
    assert 5 <= len(actual_counts) <= 10
    for index, sql in enumerate(manager.statements):
        if not sql.startswith("INSERT INTO"):
            continue
        end = next((j for j in range(index + 1, len(manager.statements))
                    if manager.statements[j].startswith("INSERT INTO")), len(manager.statements))
        after = [query for query in manager.statements[index + 1:end]
                 if query in actual_counts and sql_pairs(query) == sql_pairs(sql)]
        assert len(after) == 1
    assert sum(partition_inventory_sql(sql, manager.stage) for sql in manager.statements) <= 1
    for sql in counts:
        assert len(sql_pairs(sql)) == 8
        assert_group_sql(sql)


def test_exact_pair_predicates_do_not_form_league_season_cross_products():
    manager = BatchedFakeManager(many_seasons(5, two_leagues=True))
    create_stage(manager)
    result = populate(manager, partition_group_size=3)
    assert result["complete"] is True
    assert len(inserts(manager)) == 4
    seen = set()
    for sql in inserts(manager):
        assert_group_sql(sql)
        pairs = sql_pairs(sql)
        assert not pairs & seen
        seen.update(pairs)
    assert len(seen) == 10
    assert multiset(manager.tables[manager.stage], COLUMNS) == multiset(winners(manager.tables[manager.archive]), COLUMNS)


def test_single_partition_stage_resumes_in_groups_without_reinserting_existing_pairs():
    original = many_seasons(12)
    manager = BatchedFakeManager(original)
    create_stage(manager)
    first = populate(manager, partition_group_size=1, should_stop=lambda: len(inserts(manager)) >= 3)
    assert first["complete"] is False
    assert len(first["batches"]) == 3
    existing = assert_reports(first, original, 1)
    saved = deepcopy(manager.tables[manager.stage])
    manager.statements.clear()
    result = populate(manager, partition_group_size=8)
    assert result["complete"] is True
    assert len(assert_reports(result, original, 8)) == 12
    noop_pairs = {tuple(pair) for item in result["batches"] if "noop_rows" in item for pair in item["partitions"]}
    assert noop_pairs == existing
    for sql in inserts(manager):
        assert not sql_pairs(sql) & existing
    differences = [sql for sql in manager.statements if sql.startswith("/* pk:batch_payload_difference */")]
    assert differences
    assert set().union(*(sql_pairs(sql) for sql in differences)) == existing
    for sql in differences:
        assert_group_sql(sql)
    assert multiset(selected_rows(manager.tables[manager.stage], existing), COLUMNS) == multiset(saved, COLUMNS)
    assert multiset(manager.tables[manager.stage], COLUMNS) == multiset(winners(original), COLUMNS)
    assert_preserved(manager, original)


def test_stop_callback_limits_groups_and_resume_verifies_before_swap(monkeypatch):
    original = many_seasons()
    manager = BatchedFakeManager(original)
    create_stage(manager)
    calls = []

    def stop():
        calls.append(len(inserts(manager)))
        return len(calls) > 2

    result = populate(manager, partition_group_size=8, should_stop=stop)
    assert calls == [0, 1, 2]
    assert result["complete"] is False
    assert len(result["batches"]) == len(inserts(manager)) == 2
    existing = assert_reports(result, original, 8)
    assert len(existing) == 16
    assert multiset(manager.tables[manager.stage], COLUMNS) == multiset(selected_rows(winners(original), existing), COLUMNS)
    assert_preserved(manager, original)
    manager.statements.clear()
    verified = []
    verify_stage = batched.verify_stage

    def verify(*args, **kwargs):
        assert not any(sql.startswith("ALTER") for sql in manager.statements)
        evidence = verify_stage(*args, **kwargs)
        verified.append(evidence)
        return evidence

    monkeypatch.setattr(batched, "verify_stage", verify)
    resumed = execute(manager)
    assert resumed["success"] is True and resumed["status"] == "applied_verified"
    assert len(verified) == 1 and verified[0]["reconciliation"]["payload_mismatches"] == 0
    assert all(not sql_pairs(sql) & existing for sql in inserts(manager))
    assert multiset(manager.tables[SPEC.name], COLUMNS) == multiset(winners(original), COLUMNS)
    assert manager.tables[manager.archive] == manager.tables[manager.legacy] == original
    assert not any(re.search(r"\b(?:DELETE|DROP|TRUNCATE)\b", sql, re.I) for sql in manager.statements)


def test_resume_result_skips_verified_prefix_and_preserves_full_verify():
    original = many_seasons(24)
    manager = BatchedFakeManager(original)
    create_stage(manager)
    pairs = sorted({(row["league"], row["season"]) for row in winners(original)})
    existing = set(pairs[:8])
    manager.tables[manager.stage] = deepcopy(selected_rows(winners(original), existing))
    manager.statements.clear()
    answers = iter([False, True])
    first = populate(manager, partition_group_size=8, should_stop=lambda: next(answers))
    assert first["complete"] is False
    assert len(first["batches"]) == 1
    assert first["batches"][0]["noop_rows"] == len(selected_rows(winners(original), existing))
    previous = {
        "migration_id": "r2-synthetic",
        "table": SPEC.name,
        "archive": manager.archive,
        "stage": manager.stage,
        "legacy": manager.legacy,
        "status": "stage_incomplete",
        "success": False,
        "ready": False,
        "resume_required": True,
        "stage_complete": False,
        "mutates": False,
        "batches": first["batches"],
    }
    manager.statements.clear()

    resumed = execute(manager, previous_result=previous)

    assert resumed["success"] is True and resumed["status"] == "applied_verified"
    assert resumed["resumed_groups"] == 1
    assert resumed["batches"][0] == first["batches"][0]
    assert len(resumed["batches"]) == 3
    assert len(inserts(manager)) == 2
    assert all(not sql_pairs(sql) & existing for sql in inserts(manager))
    assert not any(sql.startswith("/* pk:batch_payload_difference */") for sql in manager.statements)
    assert resumed["reconciliation"]["payload_mismatches"] == 0
    assert multiset(manager.tables[SPEC.name], COLUMNS) == multiset(winners(original), COLUMNS)
    assert manager.tables[manager.archive] == manager.tables[manager.legacy] == original


def test_resume_result_must_match_current_work_group_prefix():
    original = many_seasons(8)
    manager = BatchedFakeManager(original)
    create_stage(manager)
    previous = {
        "migration_id": "r2-synthetic",
        "table": SPEC.name,
        "archive": manager.archive,
        "stage": manager.stage,
        "legacy": manager.legacy,
        "status": "stage_incomplete",
        "success": False,
        "ready": False,
        "resume_required": True,
        "stage_complete": False,
        "mutates": False,
        "batches": [{
            "partitions": [["SS-999", "unknown"]],
            "expected_rows": 1,
            "before_rows": 1,
            "after_rows": 1,
            "noop_rows": 1,
        }],
    }
    manager.statements.clear()

    result = execute(manager, previous_result=previous)

    assert result["success"] is False and result["mutates"] is False
    assert "work-group prefix" in result["error"]
    assert not inserts(manager)
    assert_preserved(manager, original)


def test_resume_checkpoint_row_count_change_fails_before_writes():
    original = many_seasons(24)
    manager = BatchedFakeManager(original)
    create_stage(manager)
    pairs = sorted({(row["league"], row["season"]) for row in winners(original)})
    manager.tables[manager.stage] = deepcopy(selected_rows(winners(original), set(pairs[:8])))
    answers = iter([False, True])
    first = populate(manager, partition_group_size=8, should_stop=lambda: next(answers))
    manager.tables[manager.stage].pop()
    saved = deepcopy(manager.tables[manager.stage])
    manager.statements.clear()

    with pytest.raises(ValueError, match="checkpoint row count"):
        populate(manager, partition_group_size=8, resume_batches=first["batches"])

    assert manager.tables[manager.stage] == saved
    assert not inserts(manager)
    assert_preserved(manager, original)


def assert_incomplete(result):
    assert result["success"] is False
    assert result["status"] == "stage_incomplete"
    assert result["ready"] is False and result["resume_required"] is True
    assert result["mutates"] is True
    assert "stage_verification" not in result


@pytest.mark.parametrize("missing_archive", [False, True])
def test_equal_budget_and_reserve_stop_before_first_group_then_resume(monkeypatch, missing_archive):
    original = many_seasons()
    manager = BatchedFakeManager(original)
    if missing_archive:
        del manager.tables[manager.archive]
    with monkeypatch.context() as patch:
        patch.setattr(batched, "verify_stage", lambda *a, **k: pytest.fail("incomplete stage verified"))
        result = execute(manager, max_duration_seconds=1200, verification_reserve_seconds=1200, clock=lambda: 700.0)
    assert_incomplete(result)
    assert result["stage_complete"] is False and result["batches"] == []
    assert not inserts(manager)
    assert_preserved(manager, original)
    result = execute(manager, max_duration_seconds=6000, verification_reserve_seconds=1200, clock=lambda: 900.0)
    assert result["success"] is True and result["status"] == "applied_verified"
    assert multiset(manager.tables[SPEC.name], COLUMNS) == multiset(winners(original), COLUMNS)
    assert manager.tables[manager.archive] == manager.tables[manager.legacy] == original
    assert execute(manager)["status"] == "noop_verified"


def test_deadline_stops_resumed_stage_after_completed_groups(monkeypatch):
    original = many_seasons()
    manager = BatchedFakeManager(original)
    create_stage(manager)  # No DDL in this run: INSERT still requires mutates=true.
    manager.statements.clear()
    monkeypatch.setattr(batched, "verify_stage", lambda *a, **k: pytest.fail("incomplete stage verified"))
    result = execute(
        manager, max_duration_seconds=6000, verification_reserve_seconds=1200,
        clock=lambda: 100.0 + 2400.0 * len(inserts(manager)),
    )
    assert_incomplete(result)
    assert result["stage_complete"] is False
    assert len(result["batches"]) == len(inserts(manager)) == 2
    assert len(assert_reports({"batches": result["batches"], "complete": False}, original, 8)) == 16
    assert_preserved(manager, original)


def test_reserve_reached_after_last_insert_prevents_verification_and_swap(monkeypatch):
    manager = BatchedFakeManager()
    original = deepcopy(manager.tables[SPEC.name])
    monkeypatch.setattr(batched, "verify_stage", lambda *a, **k: pytest.fail("verification started after reserve"))
    result = execute(
        manager, max_duration_seconds=6000, verification_reserve_seconds=1200,
        clock=lambda: 100.0 + (4800.0 if inserts(manager) else 0.0),
    )
    assert_incomplete(result)
    assert len(result["batches"]) == len(inserts(manager)) == 1
    assert multiset(manager.tables[manager.stage], COLUMNS) == multiset(winners(original), COLUMNS)
    assert_preserved(manager, original)


@pytest.mark.parametrize("size", [0, -1, True, False, 1.0, 1.5, "8", None, float("nan"), float("inf")])
def test_invalid_partition_group_sizes_are_rejected_without_writes(size):
    manager = BatchedFakeManager()
    create_stage(manager)
    manager.statements.clear()
    with pytest.raises(ValueError):
        populate(manager, partition_group_size=size)
    assert manager.tables[manager.stage] == []
    assert not inserts(manager)
    assert_preserved(manager, sample_rows())


@pytest.mark.parametrize("field", ["max_duration_seconds", "verification_reserve_seconds"])
@pytest.mark.parametrize("value", [-1, True, False, float("nan"), float("inf"), -float("inf"), "6000", None])
def test_invalid_time_budgets_are_rejected_before_mutation(field, value):
    manager = BatchedFakeManager()
    original = deepcopy(manager.tables)
    kwargs = dict(max_duration_seconds=6000, verification_reserve_seconds=1200, clock=lambda: 100.0)
    kwargs[field] = value
    try:
        result = execute(manager, **kwargs)
    except ValueError:
        pass
    else:
        assert result["success"] is False and result["mutates"] is False
        assert result.get("error")
    assert manager.tables == original
    assert not any(sql.startswith(("CREATE", "INSERT", "ALTER")) for sql in manager.statements)


def test_reserve_greater_than_duration_is_rejected_before_mutation():
    manager = BatchedFakeManager()
    try:
        result = execute(manager, max_duration_seconds=1, verification_reserve_seconds=2, clock=lambda: 0.0)
    except ValueError:
        pass
    else:
        assert result["success"] is False and result["mutates"] is False
        assert result.get("error")
    assert not any(sql.startswith(("CREATE", "INSERT", "ALTER")) for sql in manager.statements)


@pytest.mark.parametrize("duration,reserve", [(0, 0), (0.5, 0.5), (1.5, 0)])
def test_finite_nonnegative_fractional_and_zero_budgets_are_supported(duration, reserve):
    manager = BatchedFakeManager()
    result = execute(manager, max_duration_seconds=duration, verification_reserve_seconds=reserve, clock=lambda: 100.0)
    if duration == reserve:
        assert_incomplete(result)
        assert result["batches"] == [] and not inserts(manager)
    else:
        assert result["success"] is True and result["status"] == "applied_verified"


@pytest.mark.parametrize("identity", [
    ("SS-999", "2026-27"), ("SS-17", "unknown-season"),
    (None, "2026-27"), ("ENG", "2026-27"), ("SS-0", "2026-27"),
    ("SS-17\n", "2026-27"), ("SS-17", None), ("SS-17", "  "),
    ("SS-17", 2026), (17, "2026-27"),
])
def test_unknown_or_malformed_existing_stage_identity_fails_closed(identity):
    manager = BatchedFakeManager()
    create_stage(manager)
    bad = deepcopy(winners(sample_rows())[0])
    bad["league"], bad["season"] = identity
    manager.tables[manager.stage] = [bad]
    saved = deepcopy(manager.tables)
    manager.statements.clear()
    with pytest.raises(ValueError):
        populate(manager, partition_group_size=8)
    assert manager.tables == saved
    assert not inserts(manager)
    assert_preserved(manager, sample_rows())


@pytest.mark.parametrize("damage", ["payload", "missing", "duplicate", "natural_key"])
def test_existing_partition_mismatch_is_never_repaired_by_inserting(damage):
    original = many_seasons(8)
    manager = BatchedFakeManager(original)
    create_stage(manager)
    first_pair = {("SS-17", "season-000-O'Brien")}
    manager.tables[manager.stage] = deepcopy(selected_rows(winners(original), first_pair))
    if damage == "payload":
        manager.tables[manager.stage][0]["payload"] = "synthetic-corruption"
    elif damage == "missing":
        manager.tables[manager.stage].pop()
    elif damage == "duplicate":
        manager.tables[manager.stage].append(deepcopy(manager.tables[manager.stage][0]))
    else:
        manager.tables[manager.stage][0]["match_id"] = 999
    saved = deepcopy(manager.tables[manager.stage])
    manager.statements.clear()
    with pytest.raises(ValueError):
        populate(manager, partition_group_size=8)
    assert selected_rows(manager.tables[manager.stage], first_pair) == saved
    assert all(not sql_pairs(sql) & first_pair for sql in inserts(manager))
    assert_preserved(manager, original)


def test_after_insert_count_is_observed_not_assumed():
    class LostRowManager(BatchedFakeManager):
        def _execute(self, sql, fetch=False):
            result = super()._execute(sql, fetch=fetch)
            if sql.startswith("INSERT INTO"):
                self.tables[self.stage].pop()
            return result

    manager = LostRowManager(many_seasons(8))
    create_stage(manager)
    with pytest.raises(ValueError):
        populate(manager, partition_group_size=8)
    assert len(inserts(manager)) == 1
    assert not any(sql.startswith("ALTER") for sql in manager.statements)


def test_verified_existing_group_is_counted_once_without_final_recount():
    manager = BatchedFakeManager(many_seasons(8))
    create_stage(manager)
    manager.tables[manager.stage] = deepcopy(winners(manager.tables[manager.archive]))
    manager.statements.clear()
    result = populate(manager, partition_group_size=8)
    assert result["complete"] is True
    assert len(result["batches"]) == 1
    assert_reports(result, manager.tables[manager.archive], 8)
    assert not inserts(manager)
    actual_counts = [sql for sql in manager.statements
                     if sql.startswith("/* pk:batch_rows */") and "ROW_NUMBER" not in sql]
    inventory = [sql for sql in manager.statements if partition_inventory_sql(sql, manager.stage)]
    assert len(inventory) <= 1
    assert len(actual_counts) <= 1
    assert inventory or actual_counts
    differences = [sql for sql in manager.statements if sql.startswith("/* pk:batch_payload_difference */")]
    assert len(differences) == 1
    assert len(sql_pairs(differences[0])) == 8
    assert_group_sql(differences[0])


def test_wide_projection_keeps_payload_columns_out_of_window_order():
    columns = {**COLUMNS, **{f"scalar_{index}": "varchar" for index in range(200)}}
    sql = batched.filtered_winner_select(
        SPEC, "archive.synthetic", columns, {"ENG": "SS-17"},
        "(" + migration.league_expression({"ENG": "SS-17"})
        + " = 'SS-17' AND \"season\" = '2026-27')",
    )
    assert_group_sql(sql)
    tree = sqlglot.parse_one(sql, read="trino")
    window = next(tree.find_all(exp.Window))
    order_columns = {column.name for column in window.args["order"].find_all(exp.Column)}
    assert order_columns == {"_ingested_at", "_batch_id"}
    projection = {item.name for item in window.parent.parent.expressions if isinstance(item, exp.Column)}
    assert projection == set(columns)
    # The general migration helper retains its independent full ordering contract.
    full = sqlglot.parse_one(migration.winner_select(SPEC, "archive.synthetic", columns, {"ENG": "SS-17"}), read="trino")
    full_window = next(full.find_all(exp.Window))
    assert {column.name for column in full_window.args["order"].find_all(exp.Column)} == set(columns)


def test_stop_before_first_group_preserves_preexisting_stage():
    original = many_seasons(8)
    manager = BatchedFakeManager(original)
    create_stage(manager)
    manager.tables[manager.stage] = deepcopy(selected_rows(winners(original), {("SS-17", "season-000-O'Brien")}))
    saved = deepcopy(manager.tables[manager.stage])
    manager.statements.clear()
    result = populate(manager, partition_group_size=8, should_stop=lambda: True)
    assert result == {"batches": [], "complete": False}
    assert manager.tables[manager.stage] == saved
    assert not inserts(manager)
    assert_preserved(manager, original)


def test_default_group_size_stays_below_known_open_partition_limit():
    original = many_seasons()
    manager = BatchedFakeManager(original)
    create_stage(manager)
    result = populate(manager)
    assert result["complete"] is True
    assert len(inserts(manager)) == 5
    assert all(len(sql_pairs(sql)) <= 8 for sql in inserts(manager))
    assert multiset(manager.tables[manager.stage], COLUMNS) == multiset(winners(original), COLUMNS)


def test_stop_between_existing_and_missing_subgroups_prevents_late_insert():
    original = many_seasons(12)
    manager = BatchedFakeManager(original)
    create_stage(manager)
    first_pair = ("SS-17", "season-000-O'Brien")
    manager.tables[manager.stage] = deepcopy(selected_rows(winners(original), {first_pair}))
    saved = deepcopy(manager.tables[manager.stage])
    manager.statements.clear()
    answers = iter([False, True])
    result = populate(manager, partition_group_size=8, should_stop=lambda: next(answers))
    assert result["complete"] is False
    assert len(result["batches"]) == 1
    assert result["batches"][0]["partitions"] == [list(first_pair)]
    assert result["batches"][0]["noop_rows"] == 3
    assert not inserts(manager)
    assert manager.tables[manager.stage] == saved
    assert_preserved(manager, original)


def test_equal_metadata_conflicting_payload_blocks_before_stage():
    rows = sample_rows()[:2]
    rows[1].update(
        _ingested_at=rows[0]["_ingested_at"],
        _batch_id=rows[0]["_batch_id"],
        payload="conflict",
        raw_content_hash="hash-9",
    )
    manager = BatchedFakeManager(rows)
    original = deepcopy(manager.tables)
    result = execute(manager)
    assert result["success"] is False
    assert result["mutates"] is False
    assert manager.tables == original
    assert not any(sql.startswith(("CREATE", "INSERT", "ALTER")) for sql in manager.statements)
