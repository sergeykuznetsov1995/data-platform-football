"""Offline behavioral tests for partition-batched staging and metadata swap."""

from copy import deepcopy
import re

import pytest
import sqlglot

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
            values = re.findall(r"'((?:''|[^'])*)'", sql)
            league, season = values[-2:]
            expected = [
                row for row in winners(self.tables[self.archive])
                if row["league"] == league and row["season"] == season
            ]
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
            values = re.findall(r"'((?:''|[^'])*)'", sql)
            league, season = values[-2:]
            if "ROW_NUMBER" in sql:
                rows = [
                    row for row in winners(self.tables[self.archive])
                    if row["league"] == league and row["season"] == season
                ]
            else:
                rows = [
                    row for row in self.tables[self.stage]
                    if row["league"] == league and row["season"] == season
                ]
            return [(len(rows),)]
        if sql.startswith("/* pk:batch_payload_difference */"):
            self.statements.append(sql)
            values = re.findall(r"'((?:''|[^'])*)'", sql)
            league, season = values[-2:]
            expected = [
                row for row in winners(self.tables[self.archive])
                if row["league"] == league and row["season"] == season
            ]
            actual = [
                row for row in self.tables[self.stage]
                if row["league"] == league and row["season"] == season
            ]
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


def execute(manager):
    return batched.run_batched_rewrite(
        manager,
        SPEC,
        manager.archive,
        {"ENG": "SS-17"},
        "r2-synthetic",
        "archive",
    )


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
    assert len(result["batches"]) == 2
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
    assert all(item.get("noop_rows") == item["expected_rows"] for item in repeated)
    assert not any(sql.startswith("INSERT INTO") for sql in manager.statements)
    manager.tables[manager.stage].append(manager.tables[manager.stage][0])
    with pytest.raises(ValueError):
        batched.populate_stage(manager, SPEC, manager.stage, manager.archive, COLUMNS, {"ENG": "SS-17"})


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
