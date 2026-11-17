"""Partition-batched canonical staging and rollback-safe Iceberg table swap."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any

from scripts import migrate_sofascore_partition_keys as migration

manager_call = migration.manager_call


def stage_spec(spec, name: str):
    return SimpleNamespace(
        name=name,
        natural_key=spec.natural_key,
        partition_columns=spec.partition_columns,
    )


def _digest(spec, migration_id: str, kind: str) -> str:
    migration.validate_migration_id(migration_id)
    return hashlib.sha256((spec.name + "\0" + kind + "\0" + migration_id).encode()).hexdigest()


def stage_name(spec, migration_id: str, archive_schema: str) -> str:
    name = spec.name.split(".")[1][:40] + "_stage_" + _digest(spec, migration_id, "stage")
    migration.qualified_name(archive_schema + "." + name)
    return archive_schema + "." + name


def legacy_name(spec, migration_id: str, archive_schema: str) -> str:
    name = spec.name.split(".")[1][:40] + "_legacy_" + _digest(spec, migration_id, "legacy")
    migration.qualified_name(archive_schema + "." + name)
    return archive_schema + "." + name


def _without_location(properties: str) -> str:
    cleaned = re.sub(
        r"(?:^|,)\s*location\s*=\s*'(?:''|[^'])*'", "", properties, flags=re.I
    )
    return " ".join(cleaned.replace("\n", " ").split()).strip(" ,")


def _rename_sql(source: str, target: str) -> str:
    return f"ALTER TABLE {migration.qualified_name(source)} RENAME TO {migration.qualified_name(target)}"


def _batch_predicate(mapping: Mapping[str, str], league: str, season: str) -> str:
    return (
        f"{migration.league_expression(mapping)} = {migration.quote_literal(league)} "
        f"AND \"season\" = {migration.quote_literal(season)}"
    )


def filtered_winner_select(
    spec,
    archive: str,
    columns: Mapping[str, str],
    mapping: Mapping[str, str],
    predicate: str | None = None,
) -> str:
    columns = migration.checked_columns(columns, spec)
    names = ", ".join(map(migration.quote_identifier, columns))
    keys = ", ".join(map(migration.quote_identifier, spec.natural_key))
    where = f" WHERE {predicate}" if predicate else ""
    return (
        f"WITH canonical AS (SELECT {migration.projection(columns, mapping)} "
        f"FROM {migration.qualified_name(archive)}{where}) "
        f"SELECT {names} FROM (SELECT {names}, ROW_NUMBER() OVER "
        f"(PARTITION BY {keys} ORDER BY {migration.ordering(columns)}) AS "
        f"{migration.quote_identifier(migration.RANK_COLUMN)} FROM canonical) AS ranked "
        f"WHERE {migration.quote_identifier(migration.RANK_COLUMN)} = 1"
    )


def create_stage_sql(
    spec,
    archive: str,
    columns: Mapping[str, str],
    mapping: Mapping[str, str],
    source_ddl: str,
) -> str:
    properties = _without_location(migration.ddl_properties(source_ddl))
    if not properties:
        raise ValueError("staging requires properties besides location")
    comment = migration.ddl_comment(source_ddl)
    return (
        f"CREATE TABLE {migration.qualified_name(spec.name)} "
        + (f"COMMENT {comment} " if comment else "")
        + f"WITH ({properties}) AS "
        + filtered_winner_select(spec, archive, columns, mapping, "1 = 0")
    )


def partition_batches(manager: Any, archive: str, mapping: Mapping[str, str]) -> list[tuple[str, str]]:
    expression = migration.league_expression(mapping)
    rows = migration.manager_call(
        manager,
        "_execute",
        f"SELECT DISTINCT {expression} AS canonical_league, \"season\" "
        f"FROM {migration.qualified_name(archive)} ORDER BY canonical_league, \"season\"",
        fetch=True,
    )
    batches = set()
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != 2:
            raise ValueError("invalid partition batch row")
        league, season = row
        if not isinstance(league, str) or not migration.SS_KEY.fullmatch(league):
            raise ValueError("invalid canonical league in partition batches")
        if not isinstance(season, str) or not season.strip():
            raise ValueError("invalid season in partition batches")
        batches.add((league, season))
    return sorted(batches)


def _batch_count(manager: Any, query: str) -> int:
    return migration.count_query(manager, "batch_rows", "SELECT count(*) FROM (" + query + ") counted")


def populate_stage(
    manager: Any,
    spec,
    stage: str,
    archive: str,
    columns: Mapping[str, str],
    mapping: Mapping[str, str],
) -> list[dict]:
    batches = []
    for league, season in partition_batches(manager, archive, mapping):
        predicate = _batch_predicate(mapping, league, season)
        expected = filtered_winner_select(spec, archive, columns, mapping, predicate)
        expected_count = _batch_count(manager, expected)
        actual_filter = f"\"league\" = {migration.quote_literal(league)} AND \"season\" = {migration.quote_literal(season)}"
        actual = f"SELECT * FROM {migration.qualified_name(stage)} WHERE {actual_filter}"
        actual_count = _batch_count(manager, actual)
        item = {"league": league, "season": season, "expected_rows": expected_count, "before_rows": actual_count}
        if actual_count == 0:
            manager_call(manager, "_execute", f"INSERT INTO {migration.qualified_name(stage)} {expected}")
            after_count = _batch_count(manager, actual)
            if after_count != expected_count:
                raise ValueError("inserted partition row count mismatch")
            item["inserted_rows"] = expected_count
        elif actual_count == expected_count:
            difference = migration.rowset_difference(manager, expected, actual, "batch_payload_difference")
            if difference:
                raise ValueError("existing partition payload mismatch")
            item["noop_rows"] = actual_count
        else:
            raise ValueError("unexpected rows in existing partition")
        item["after_rows"] = _batch_count(manager, actual)
        batches.append(item)
    return batches


def _verify_payload(
    manager: Any,
    spec,
    archive: str,
    archived: dict,
    mapping: Mapping[str, str],
    original_ddl: str,
) -> tuple[dict, dict]:
    after = migration.profile_table(manager, spec, mapping)
    if list(after["columns"].items()) != list(archived["columns"].items()):
        raise ValueError("ordered result schema differs")
    if _without_location(migration.ddl_properties(after["ddl"])) != _without_location(migration.ddl_properties(original_ddl)):
        raise ValueError("non-location table properties changed")
    if migration.ddl_comment(after["ddl"]) != migration.ddl_comment(original_ddl):
        raise ValueError("table comment changed")
    keys = ", ".join(map(migration.quote_identifier, spec.natural_key))
    canonical_keys = ", ".join(
        migration.league_expression(mapping) + ' AS "league"' if key == "league" else migration.quote_identifier(key)
        for key in spec.natural_key
    )
    common = (
        f"WITH expected AS (SELECT DISTINCT {canonical_keys} FROM {migration.qualified_name(archive)}), "
        f"actual AS (SELECT DISTINCT {keys} FROM {migration.qualified_name(spec.name)}) "
    )
    equality = " AND ".join(f"e.{migration.quote_identifier(k)} = a.{migration.quote_identifier(k)}" for k in spec.natural_key)
    missing = migration.count_query(manager, "missing_natural_keys", common + f"SELECT count(*) FROM expected e WHERE NOT EXISTS (SELECT 1 FROM actual a WHERE {equality})")
    extra = migration.count_query(manager, "extra_natural_keys", common + f"SELECT count(*) FROM actual a WHERE NOT EXISTS (SELECT 1 FROM expected e WHERE {equality})")
    names = ", ".join(map(migration.quote_identifier, archived["columns"]))
    expected_rows = filtered_winner_select(spec, archive, archived["columns"], mapping)
    mismatches = migration.rowset_difference(
        manager,
        "SELECT * FROM (" + expected_rows + ") expected_rows",
        f"SELECT {names} FROM {migration.qualified_name(spec.name)}",
        "payload_mismatches",
    )
    before, current = archived["counts"], after["counts"]
    reconciliation = {
        "before_rows": before["rows"],
        "before_distinct_canonical_keys": before["distinct_canonical_keys"],
        "after_rows": current["rows"],
        "archive_rows": migration.count_query(manager, "archive_rows_post", "SELECT count(*) FROM " + migration.qualified_name(archive)),
        "collapsed_duplicate_excess": before["collapsed_duplicate_excess"],
        "missing_natural_keys": missing,
        "extra_natural_keys": extra,
        "unmapped_rows": current["unmapped_rows"],
        "null_natural_keys": current["null_natural_keys"],
        "duplicate_keys": current["duplicate_groups_before"],
        "non_ss_keys": current["non_ss_keys"],
        "payload_mismatches": mismatches,
    }
    migration.validate_reconciliation(**reconciliation)
    return after, reconciliation


def verify_stage(manager: Any, spec, stage: str, archive: str, archived: dict, mapping: Mapping[str, str], original_ddl: str) -> dict:
    stage_contract = stage_spec(spec, stage)
    after, reconciliation = _verify_payload(manager, stage_contract, archive, archived, mapping, original_ddl)
    return {"after": after, "reconciliation": reconciliation}


def _rollback(manager: Any, spec, stage: str, legacy: str, source_moved: bool, stage_moved: bool) -> str | None:
    try:
        if stage_moved:
            manager_call(manager, "_execute", _rename_sql(spec.name, stage))
        if source_moved:
            manager_call(manager, "_execute", _rename_sql(legacy, spec.name))
        return None
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"


def swap_stage(manager: Any, spec, stage: str, legacy: str, archive: str, archived: dict, mapping: Mapping[str, str], original_ddl: str) -> dict:
    source_moved = stage_moved = False
    try:
        manager_call(manager, "_execute", _rename_sql(spec.name, legacy))
        source_moved = True
        manager_call(manager, "_execute", _rename_sql(stage, spec.name))
        stage_moved = True
        after, reconciliation = _verify_payload(manager, spec, archive, archived, mapping, original_ddl)
        return {"success": True, "status": "applied_verified", "after": after, "reconciliation": reconciliation}
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        rollback_error = _rollback(manager, spec, stage, legacy, source_moved, stage_moved)
        return {"success": False, "status": "failed", "error": error, "rollback_error": rollback_error}


def run_batched_rewrite(
    manager: Any,
    spec,
    archive: str,
    mapping: Mapping[str, str],
    migration_id: str,
    archive_schema: str,
) -> dict:
    migration.validate_migration_id(migration_id)
    stage = stage_name(spec, migration_id, archive_schema)
    legacy = legacy_name(spec, migration_id, archive_schema)
    report = {"migration_id": migration_id, "table": spec.name, "archive": archive, "stage": stage, "legacy": legacy, "success": False, "mutates": False}
    try:
        before = migration.profile_table(manager, spec, mapping)
        if not manager_call(manager, "table_exists", *archive.split(".")):
            report["mutates"] = True
            manager_call(manager, "_execute", migration.archive_sql(spec, archive))
        archived = migration.profile_table(manager, spec, mapping, source=archive, require_partition=False)
        if not before["ready"] or not archived["ready"]:
            raise ValueError("source/archive preflight blocked")
        source_is_canonical = before["counts"]["non_ss_keys"] == 0 and before["counts"]["duplicate_groups_after"] == 0
        if source_is_canonical and manager_call(manager, "table_exists", *legacy.split(".")):
            after, reconciliation = _verify_payload(manager, spec, archive, archived, mapping, before["ddl"])
            report.update(success=True, status="noop_verified", after=after, reconciliation=reconciliation)
            return report
        if manager_call(manager, "table_exists", *legacy.split(".")):
            raise ValueError("legacy backup exists but source is not verified canonical")
        if before["counts"] != archived["counts"]:
            raise ValueError("source and archive profiles differ")
        difference = migration.rowset_difference(
            manager,
            "SELECT * FROM " + migration.qualified_name(spec.name),
            "SELECT * FROM " + migration.qualified_name(archive),
            "source_archive_difference",
        )
        if difference:
            raise ValueError("source differs from full archive")
        if not manager_call(manager, "table_exists", *stage.split(".")):
            report["mutates"] = True
            manager_call(manager, "_execute", create_stage_sql(
                stage_spec(spec, stage),
                archive,
                before["columns"],
                mapping,
                before["ddl"],
            ))
        report["batches"] = populate_stage(manager, spec, stage, archive, before["columns"], mapping)
        verified = verify_stage(manager, spec, stage, archive, archived, mapping, before["ddl"])
        report["stage_verification"] = verified
        swapped = swap_stage(manager, spec, stage, legacy, archive, archived, mapping, before["ddl"])
        report.update(swapped)
        report["mutates"] = True
        return report
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        return report
