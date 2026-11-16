"""Synthetic state/count tests; no real catalog, network or query engine."""

from collections import Counter, defaultdict
from copy import deepcopy
import re

import pytest
import sqlglot
from sqlglot import exp

from scripts import migrate_sofascore_partition_keys as migration


SPEC = migration.TableSpec("bronze.synthetic_matches", ("league", "season", "match_id"), ("league", "season"))
REGISTRY = {"ENG": 17}
COLUMNS = {
    "league": "varchar", "season": "varchar", "match_id": "bigint",
    "_ingested_at": "timestamp(6)", "_batch_id": "varchar",
    "raw_content_hash": "varchar", "payload": "varchar",
}


def row(league, match_id, *, stamp="2026-11-15 01:00:00", batch="batch-1", payload="old", content_hash="hash-1"):
    return {
        "league": league, "season": "2026-27", "match_id": match_id,
        "_ingested_at": stamp, "_batch_id": batch,
        "raw_content_hash": content_hash, "payload": payload,
    }


def sample_rows():
    return [
        row("ENG", 101),
        row("SS-17", 101, stamp="2026-11-16 01:00:00", batch="batch-2", payload="new", content_hash="hash-2"),
        row("ENG", 102),
        row("SS-17", 103),
    ]


def canonical_rows(rows):
    result = deepcopy(rows)
    for item in result:
        value = item["league"]
        if value in REGISTRY:
            item["league"] = "SS-" + str(REGISTRY[value])
        elif not isinstance(value, str) or not re.fullmatch(r"SS-[1-9][0-9]*", value):
            item["league"] = None
    return result


def groups(rows):
    result = defaultdict(list)
    for item in rows:
        result[tuple(item[key] for key in SPEC.natural_key)].append(item)
    return result


def metadata_rank(item):
    return tuple((item.get(key) is not None, item.get(key) or "") for key in ("_ingested_at", "_batch_id") if key in item)


def winners(rows):
    result = []
    for members in groups(canonical_rows(rows)).values():
        highest = max(map(metadata_rank, members))
        top = [item for item in members if metadata_rank(item) == highest]
        result.append(min(top, key=lambda item: repr(tuple(item.items()))))
    return result


def multiset(rows, columns):
    return Counter(tuple(item[key] for key in columns) for item in rows)


class FakeManager:
    """Execute migration phases against in-memory rowsets, not canned counts.

    Query labels dispatch independently computed relational operations. SQL
    syntax/AST checks below complement this fake; this is not a Trino emulator.
    """

    def __init__(self, rows=None, *, columns=None, damage=None, damage_archive=False, ddl_text=None, drop_ddl_properties=False):
        self.columns = dict(COLUMNS if columns is None else columns)
        self.tables = {SPEC.name: deepcopy(sample_rows() if rows is None else rows)}
        self.statements = []
        self.closed = False
        self.damage = damage
        self.damage_archive = damage_archive
        self.ddl_partition = "ARRAY['league','season']"
        self.ddl_text = ddl_text
        self.drop_ddl_properties = drop_ddl_properties
        self.archive = None

    def table_exists(self, schema, table):
        return schema + "." + table in self.tables

    def get_table_columns(self, schema, table):
        assert schema + "." + table in self.tables
        return dict(self.columns)

    def close(self):
        self.closed = True

    @staticmethod
    def referenced_table(sql):
        match = re.search(r'FROM "([A-Za-z_][A-Za-z0-9_]*)"\."([A-Za-z_][A-Za-z0-9_]*)"', sql)
        assert match, sql
        return match.group(1) + "." + match.group(2)

    def _execute(self, sql, fetch=False):
        self.statements.append(sql)
        if sql.startswith("SHOW CREATE TABLE"):
            assert fetch
            ddl = self.ddl_text or "CREATE TABLE t (league varchar) WITH (format='PARQUET', partitioning=" + self.ddl_partition + ")"
            return [(ddl,)]
        if sql.startswith("CREATE TABLE"):
            assert not fetch
            match = re.match(r'CREATE TABLE "([^"]+)"\."([^"]+)" AS SELECT \* FROM', sql)
            assert match
            archive = match.group(1) + "." + match.group(2)
            assert archive not in self.tables
            self.archive = archive
            self.tables[archive] = deepcopy(self.tables[self.referenced_table(sql)])
            if self.damage_archive:
                self.tables[archive].pop()
            return None
        if sql.startswith("CREATE OR REPLACE TABLE"):
            assert not fetch and self.archive is not None
            assert migration.qualified_name(self.archive) in sql
            self.tables[SPEC.name] = winners(self.tables[self.archive])
            if self.drop_ddl_properties:
                self.ddl_text = None
            if self.damage:
                self.damage(self.tables[SPEC.name])
            return None
        assert fetch and sql.startswith("/* pk:"), sql
        label = re.match(r"/\* pk:([a-z_]+) \*/", sql).group(1)
        current = self.tables[SPEC.name]
        if label in {"missing_natural_keys", "extra_natural_keys"}:
            expected_keys = set(groups(canonical_rows(self.tables[self.archive])))
            actual_keys = set(groups(current))
            value = len(expected_keys - actual_keys if label == "missing_natural_keys" else actual_keys - expected_keys)
        elif label in {"source_archive_difference", "payload_mismatches"}:
            expected = self.tables[self.archive]
            if label == "payload_mismatches":
                expected = winners(expected)
            left, right = multiset(expected, self.columns), multiset(current, self.columns)
            value = sum((left - right).values()) + sum((right - left).values())
        else:
            rows = self.tables[self.referenced_table(sql)]
            canonical = canonical_rows(rows)
            if label in {"rows", "archive_rows", "archive_rows_post"}:
                value = len(rows)
            elif label == "null_natural_keys":
                value = sum(any(item[key] is None or str(item[key]).strip() == "" for key in SPEC.natural_key) for item in rows)
            elif label == "non_ss_keys":
                value = sum(not isinstance(item["league"], str) or not re.fullmatch(r"SS-[1-9][0-9]*", item["league"]) for item in rows)
            elif label == "unmapped_rows":
                value = sum(item["league"] is None for item in canonical)
            elif label == "duplicate_groups_before":
                value = sum(len(members) > 1 for members in groups(rows).values())
            elif label == "duplicate_groups_after":
                value = sum(len(members) > 1 for members in groups(canonical).values())
            elif label == "distinct_canonical_keys":
                value = len(groups(canonical))
            elif label == "unsafe_winner_groups":
                value = 0
                for members in groups(canonical).values():
                    highest = max(map(metadata_rank, members))
                    top = [item for item in members if metadata_rank(item) == highest]
                    value += len(multiset(top, self.columns)) > 1
            else:
                raise AssertionError(label)
        return [(value,)]


def execute(manager, mode="apply", **kwargs):
    return migration.run_migration(
        migration_id="r2-synthetic", mode=mode, specs=(SPEC,),
        tournament_by_canonical=REGISTRY, manager_factory=lambda: manager,
        writers_paused=True, **kwargs,
    )


def mutations(manager):
    return [sql for sql in manager.statements if sql.startswith("CREATE")]


def test_contract_loading_preserves_declared_order_and_filters(tmp_path):
    contract = tmp_path / "coverage.yaml"
    contract.write_text(
        "tables:\n"
        "  bronze.z_table:\n"
        "    natural_key: [season, league, event_id]\n"
        "    partition_columns: [league, season]\n"
        "  bronze.a_table:\n"
        "    natural_key: [league, season, match_id]\n"
        "    partition_columns: [league, season]\n"
        "  gold.ignored: {}\n",
        encoding="utf-8",
    )
    specs = migration.load_table_specs(contract, {"z_table", "bronze.a_table"})
    assert tuple(spec.name for spec in specs) == ("bronze.a_table", "bronze.z_table")
    assert specs[1].natural_key == ("season", "league", "event_id")
    assert specs[1].partition_columns == ("league", "season")
    with pytest.raises(ValueError, match="absent"):
        migration.load_table_specs(contract, {"missing_table"})


@pytest.mark.parametrize("entry", [
    "{}", "{natural_key: [league, season, match_id]}",
    "{natural_key: [], partition_columns: [league, season]}",
    "{natural_key: [league, season, 'match;id'], partition_columns: [league, season]}",
    "{natural_key: [league, season], partition_columns: [season, league]}",
    "{natural_key: [league, league, season], partition_columns: [league, season]}",
    "{natural_key: [match_id], partition_columns: [league, season]}",
])
def test_bad_contract_entries_are_rejected(tmp_path, entry):
    contract = tmp_path / "bad.yaml"
    contract.write_text("tables:\n  bronze.synthetic_matches: " + entry, encoding="utf-8")
    with pytest.raises(ValueError):
        migration.load_table_specs(contract, {"synthetic_matches"})


@pytest.mark.parametrize("name", ["silver.matches", "bronze.matches;bad", "bronze.a.b", 'bronze.a"b'])
def test_bad_table_names(name):
    with pytest.raises(ValueError):
        migration.TableSpec(name, ("league", "season", "match_id"), ("league", "season"))


def test_dry_run_is_deterministic_and_never_connects():
    def forbidden_factory():
        pytest.fail("dry-run connected")

    arguments = dict(migration_id="r2-synthetic", specs=(SPEC,), tournament_by_canonical=REGISTRY, manager_factory=forbidden_factory)
    first = migration.run_migration(**arguments)
    assert first == migration.run_migration(**arguments)
    assert first["mode"] == "dry_run" and first["mutates"] is False
    assert first["success"] and not first["ready"]
    assert first["tables"][0]["plan"]["rewrite_is_template"]
    assert first["migration_id"] == "r2-synthetic"


@pytest.mark.parametrize("migration_id", ["", "a/b", "x'; bad", "a.b", "a\n", "я", "x" * 49])
def test_unsafe_id_rejected_before_connection(migration_id):
    with pytest.raises(ValueError, match="migration_id"):
        migration.run_migration(migration_id=migration_id, mode="preflight", manager_factory=lambda: pytest.fail("connected"))


def test_cli_requires_identity_and_exclusive_modes():
    with pytest.raises(SystemExit) as missing:
        migration.main([])
    assert missing.value.code == 2
    with pytest.raises(SystemExit) as exclusive:
        migration.main(["--migration-id", "r2", "--apply", "--preflight"])
    assert exclusive.value.code == 2


def test_cli_invalid_id_is_json_failure_before_connection(capsys):
    assert migration.main(["--migration-id", "unsafe/id"], manager_factory=lambda: pytest.fail("connected")) == 2
    import json

    result = json.loads(capsys.readouterr().out)
    assert not result["success"] and not result["mutates"]


def test_explicit_writer_pause_required_before_connection():
    with pytest.raises(ValueError, match="writer pause"):
        migration.run_migration(migration_id="r2", mode="apply", manager_factory=lambda: pytest.fail("connected"))


def test_identifier_and_literal_quoting_and_archive_identity():
    assert migration.quote_literal("O'Brien") == "'O''Brien'"
    with pytest.raises(ValueError):
        migration.quote_literal("ENG\x00")
    with pytest.raises(ValueError):
        migration.quote_identifier("league;SELECT")
    assert migration.archive_name(SPEC, "a-b", "archives") != migration.archive_name(SPEC, "a_b", "archives")


def test_registry_conflict_fails_before_connection():
    with pytest.raises(ValueError, match="ambiguous"):
        migration.run_migration(migration_id="r2", mode="preflight", specs=(SPEC,), tournament_by_canonical={"SS-17": 18}, manager_factory=lambda: pytest.fail("connected"))


def test_merge_keeps_all_match_ids_and_archives_every_original_row():
    original = sample_rows()
    manager = FakeManager(original)
    result = execute(manager)
    assert result["success"] and result["mutates"]
    item = result["tables"][0]
    assert item["status"] == "applied_verified"
    assert manager.tables[manager.archive] == original
    assert {item["match_id"] for item in manager.tables[SPEC.name]} == {101, 102, 103}
    assert len(manager.tables[SPEC.name]) == 3
    assert {item["league"] for item in manager.tables[SPEC.name]} == {"SS-17"}
    assert next(item for item in manager.tables[SPEC.name] if item["match_id"] == 101)["payload"] == "new"
    counts = item["reconciliation"]
    assert counts["before_rows"] == counts["archive_rows"] == 4
    assert counts["after_rows"] == counts["before_distinct_canonical_keys"] == 3
    assert counts["collapsed_duplicate_excess"] == 1
    assert item["before"]["counts"]["duplicate_groups_before"] == 0
    assert item["before"]["counts"]["duplicate_groups_after"] == 1
    migration.validate_reconciliation(**counts)
    assert manager.closed


def valid_reconciliation():
    return dict(
        before_rows=4, before_distinct_canonical_keys=3, after_rows=3,
        archive_rows=4, collapsed_duplicate_excess=1, missing_natural_keys=0,
        extra_natural_keys=0, unmapped_rows=0, null_natural_keys=0,
        duplicate_keys=0, non_ss_keys=0, payload_mismatches=0,
    )


@pytest.mark.parametrize("field", list(valid_reconciliation()))
def test_reconciliation_rejects_every_broken_invariant(field):
    values = valid_reconciliation()
    migration.validate_reconciliation(**values)
    values[field] += 1
    with pytest.raises(ValueError, match="reconciliation"):
        migration.validate_reconciliation(**values)


@pytest.mark.parametrize("bad_count", [-1, None, 1.0, True, "0"])
def test_reconciliation_rejects_non_counts(bad_count):
    values = valid_reconciliation()
    values["missing_natural_keys"] = bad_count
    with pytest.raises(ValueError):
        migration.validate_reconciliation(**values)


def test_reconciliation_requires_all_evidence():
    values = valid_reconciliation()
    del values["missing_natural_keys"]
    with pytest.raises(TypeError):
        migration.validate_reconciliation(**values)
    empty = {name: 0 for name in valid_reconciliation()}
    migration.validate_reconciliation(**empty)


def test_equal_metadata_conflicting_payload_blocks_before_any_mutation():
    # A different hash/lexical payload does not prove a fresher winner.
    manager = FakeManager([row("ENG", 101), row("SS-17", 101, payload="conflict", content_hash="hash-9")])
    result = execute(manager)
    assert not result["success"] and not result["mutates"]
    assert result["tables"][0]["before"]["counts"]["unsafe_winner_groups"] == 1
    assert not mutations(manager)
    assert manager.tables[SPEC.name][0]["league"] == "ENG"


def test_identical_tied_rows_can_collapse_without_payload_loss():
    manager = FakeManager([row("ENG", 101), row("SS-17", 101)])
    result = execute(manager)
    assert result["success"]
    assert len(manager.tables[manager.archive]) == 2
    assert len(manager.tables[SPEC.name]) == 1


def test_missing_winner_metadata_does_not_allow_lexical_winner():
    rows = [row("ENG", 101), row("SS-17", 101, payload="conflict")]
    columns = {key: value for key, value in COLUMNS.items() if key not in {"_ingested_at", "_batch_id"}}
    rows = [{key: value for key, value in item.items() if key in columns} for item in rows]
    manager = FakeManager(rows, columns=columns)
    assert not execute(manager)["success"]
    assert not mutations(manager)


@pytest.mark.parametrize("bad_row", [row("UNKNOWN", 101), row("SS-17\n", 101), row(None, 101), row("ENG", None), {**row("ENG", 101), "season": "  "}])
def test_unknown_mapping_and_null_or_blank_keys_block_apply(bad_row):
    manager = FakeManager([bad_row])
    result = execute(manager)
    assert not result["ready"] and not result["success"]
    assert not mutations(manager)


def test_preflight_only_reads_and_reports_duplicates():
    manager = FakeManager()
    result = execute(manager, mode="preflight")
    assert result["success"] and result["ready"] and not result["mutates"]
    assert result["tables"][0]["before"]["counts"]["duplicate_groups_after"] == 1
    assert not mutations(manager)
    for sql in manager.statements:
        parsed = sqlglot.parse_one(sql, read="trino")
        assert not any(isinstance(node, (exp.Create, exp.Alter, exp.Update, exp.Merge, exp.Delete, exp.Drop)) for node in parsed.walk())


def test_archive_verify_rewrite_postcheck_order_and_sql_parse():
    manager = FakeManager()
    result = execute(manager)
    assert result["success"]
    statements = manager.statements
    archive = next(i for i, sql in enumerate(statements) if sql.startswith("CREATE TABLE"))
    verified = next(i for i, sql in enumerate(statements) if sql.startswith("/* pk:archive_rows */"))
    rewrite = next(i for i, sql in enumerate(statements) if sql.startswith("CREATE OR REPLACE TABLE"))
    postcheck = next(i for i, sql in enumerate(statements) if sql.startswith("/* pk:missing_natural_keys */"))
    assert archive < verified < rewrite < postcheck
    assert any(sql.startswith("/* pk:unsafe_winner_groups */") for sql in statements[:archive])
    for sql in statements:
        assert not re.search(r"\b(?:DELETE|DROP|TRUNCATE)\b", sql, re.I)
        sqlglot.parse_one(sql, read="trino")
    plan = result["tables"][0]["plan"]
    assert not plan["rewrite_is_template"]
    archive_ast = sqlglot.parse_one(plan["archive_sql"], read="trino")
    rewrite_ast = sqlglot.parse_one(plan["rewrite_sql"], read="trino")
    assert isinstance(archive_ast, exp.Create) and not archive_ast.args.get("replace")
    assert isinstance(rewrite_ast, exp.Create) and rewrite_ast.args.get("replace")
    assert len(mutations(manager)) == 2


def test_rewrite_preserves_original_properties_and_comment():
    ddl = (
        "CREATE TABLE t (league varchar) COMMENT 'grain' "
        "WITH (format='PARQUET', partitioning=ARRAY['league','season'], compression='ZSTD')"
    )
    manager = FakeManager(ddl_text=ddl)
    result = execute(manager)
    assert result["success"]
    rewrite = result["tables"][0]["plan"]["rewrite_sql"]
    assert "COMMENT 'grain'" in rewrite
    assert "compression='ZSTD'" in rewrite
    assert migration.ddl_properties(ddl) in rewrite
    sqlglot.parse_one(rewrite, read="trino")


def test_postcheck_rejects_lost_properties_and_comment():
    ddl = (
        "CREATE TABLE t (league varchar) COMMENT 'grain' "
        "WITH (format='PARQUET', partitioning=ARRAY['league','season'], compression='ZSTD')"
    )
    manager = FakeManager(ddl_text=ddl, drop_ddl_properties=True)
    result = execute(manager)
    assert not result["success"]
    assert result["tables"][0]["phase"] == "postcheck"
    assert "properties or comment changed" in result["error"]


@pytest.mark.parametrize("damage, expected", [
    (lambda rows: rows.pop(), "missing_natural_keys"),
    (lambda rows: rows.append(row("SS-17", 999)), "extra_natural_keys"),
    (lambda rows: rows.append(deepcopy(rows[0])), "duplicate_keys"),
    (lambda rows: rows[0].update(league="ENG"), "non_ss_keys"),
    (lambda rows: rows[0].update(league="UNKNOWN"), "unmapped_rows"),
    (lambda rows: rows[0].update(match_id=None), "null_natural_keys"),
    (lambda rows: rows[0].update(payload="unexpected"), "payload_mismatches"),
])
def test_actual_postwrite_failure_is_never_success(damage, expected):
    manager = FakeManager(damage=damage)
    original = deepcopy(manager.tables[SPEC.name])
    result = execute(manager)
    assert not result["success"] and result["mutates"]
    item = result["tables"][0]
    assert item["status"] == "failed" and item["phase"] == "postcheck"
    assert item["reconciliation"][expected] > 0
    assert manager.tables[manager.archive] == original


def test_archive_count_mismatch_stops_before_rewrite():
    manager = FakeManager(damage_archive=True)
    result = execute(manager)
    assert not result["success"]
    assert len(mutations(manager)) == 1
    assert manager.tables[SPEC.name] == sample_rows()


def test_same_id_repeat_verifies_state_and_is_noop():
    manager = FakeManager()
    first = execute(manager)
    assert first["success"]
    archive_before = deepcopy(manager.tables[manager.archive])
    manager.statements.clear()
    repeated = execute(manager)
    assert repeated["success"] and not repeated["mutates"]
    assert repeated["tables"][0]["status"] == "noop_verified"
    assert repeated["tables"][0]["reconciliation"]["before_rows"] == 4
    assert manager.tables[manager.archive] == archive_before
    assert not mutations(manager)


def test_existing_archive_with_unclean_source_never_authorizes_retry_write():
    manager = FakeManager(damage=lambda rows: rows.pop())
    assert not execute(manager)["success"]
    manager.statements.clear()
    manager.damage = None
    repeated = execute(manager)
    assert not repeated["success"] and not repeated["mutates"]
    assert not mutations(manager)


@pytest.mark.parametrize("problem", ["partition", "column", "complex_type", "metadata_type", "reserved", "missing_table"])
def test_schema_and_partition_fail_closed(problem):
    manager = FakeManager()
    if problem == "partition":
        manager.ddl_partition = "ARRAY['season','league']"
    elif problem == "column":
        del manager.columns["match_id"]
    elif problem == "complex_type":
        manager.columns["payload"] = "array(varchar)"
    elif problem == "metadata_type":
        manager.columns["_ingested_at"] = "varchar"
    elif problem == "reserved":
        manager.columns[migration.RANK_COLUMN] = "bigint"
    else:
        manager.tables.clear()
    result = execute(manager)
    assert not result["success"] and not mutations(manager)


def test_no_later_table_is_mutated_after_failure():
    manager = FakeManager(damage=lambda rows: rows.pop())
    later = migration.TableSpec("bronze.z_later", SPEC.natural_key, SPEC.partition_columns)
    manager.tables[later.name] = sample_rows()
    result = migration.run_migration(
        migration_id="r2-synthetic", mode="apply", specs=(SPEC, later),
        tournament_by_canonical=REGISTRY, manager_factory=lambda: manager, writers_paused=True,
    )
    assert not result["success"]
    assert len(mutations(manager)) == 2
    assert manager.tables[later.name] == sample_rows()


def test_default_manager_factory_is_not_called_by_dry_run(monkeypatch):
    monkeypatch.setattr(migration, "default_manager_factory", lambda: pytest.fail("default connection"))
    result = migration.run_migration(migration_id="r2", specs=(SPEC,), tournament_by_canonical=REGISTRY)
    assert result["success"] and not result["mutates"]


def test_default_manager_factory_uses_existing_trino_manager():
    from scrapers.base.trino_manager import TrinoTableManager

    manager = migration.default_manager_factory()
    assert isinstance(manager, TrinoTableManager)
    assert manager._conn is None


def test_same_count_source_drift_stops_before_rewrite():
    class DriftingManager(FakeManager):
        def _execute(self, sql, fetch=False):
            result = super()._execute(sql, fetch=fetch)
            if sql.startswith("CREATE TABLE"):
                self.tables[SPEC.name][0]["payload"] = "concurrent-update"
            return result

    manager = DriftingManager()
    result = execute(manager)
    assert not result["success"]
    assert result["tables"][0]["source_archive_difference"] == 2
    assert len(mutations(manager)) == 1
    assert manager.tables[manager.archive] == sample_rows()


def test_driver_failure_does_not_leak_row_payloads_or_claim_success():
    class FailingManager(FakeManager):
        def _execute(self, sql, fetch=False):
            if sql.startswith("CREATE OR REPLACE TABLE"):
                raise RuntimeError("synthetic-private-payload-from-driver")
            return super()._execute(sql, fetch=fetch)

    manager = FailingManager()
    result = execute(manager)
    assert not result["success"] and result["mutates"]
    assert "synthetic-private-payload" not in str(result)
    assert result["tables"][0]["phase"] == "rewrite"
    assert manager.tables[manager.archive] == sample_rows()
