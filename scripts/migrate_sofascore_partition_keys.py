"""Prepare and, only in a separately approved window, migrate Bronze keys.

No connection is opened by the default dry run. Apply requires an externally
enforced writer pause for its entire duration; this module does not manage it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

from scrapers.sofascore.iceberg_writer import PARTITION_SCOPED_TABLES
from scrapers.sofascore.partition_keys import canonical_league


DEFAULT_CONTRACT = Path(__file__).resolve().parents[1] / "configs/sofascore/endpoint_coverage.yaml"
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
MIGRATION_ID = re.compile(r"[A-Za-z0-9_-]{1,48}\Z")
SS_KEY = re.compile(r"SS-[1-9][0-9]*\Z")
SCALAR_TYPE = re.compile(
    r"(?:boolean|tinyint|smallint|integer|int|bigint|real|double|date|uuid|varbinary|"
    r"decimal\(\d+\s*,\s*\d+\)|(?:varchar|char)(?:\(\d+\))?|"
    r"(?:timestamp|time)(?:\(\d+\))?(?: (?:with|without) time zone)?)\Z"
)
RANK_COLUMN = "__pk_migration_rank"


def quote_identifier(value: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValueError("unsafe SQL identifier")
    return '"' + value + '"'


def quote_literal(value: str) -> str:
    if not isinstance(value, str) or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ValueError("unsafe SQL string literal")
    return "'" + value.replace("'", "''") + "'"


def qualified_name(value: str) -> str:
    parts = value.split(".") if isinstance(value, str) else []
    if len(parts) != 2:
        raise ValueError("expected schema.table")
    return ".".join(quote_identifier(p) for p in parts)


def validate_migration_id(value: str) -> str:
    if not isinstance(value, str) or not MIGRATION_ID.fullmatch(value):
        raise ValueError("migration_id must contain 1–48 ASCII letters, digits, '_' or '-'")
    return value


@dataclass(frozen=True)
class TableSpec:
    name: str
    natural_key: tuple[str, ...]
    partition_columns: tuple[str, ...]

    def __post_init__(self) -> None:
        qualified_name(self.name)
        if self.name.split(".")[0] != "bronze":
            raise ValueError("only bronze tables are eligible")
        if not isinstance(self.natural_key, tuple) or not self.natural_key:
            raise ValueError("natural_key must be a nonempty tuple")
        for column in self.natural_key:
            quote_identifier(column)
        if len(set(self.natural_key)) != len(self.natural_key):
            raise ValueError("repeated natural-key column")
        if self.partition_columns != ("league", "season"):
            raise ValueError("partition_columns must be exactly (league, season)")
        if not {"league", "season"}.issubset(self.natural_key):
            raise ValueError("natural key must include league and season")


def load_table_specs(
    contract_path: str | Path,
    eligible_tables: Sequence[str] = PARTITION_SCOPED_TABLES,
) -> tuple[TableSpec, ...]:
    expected = set()
    for table in eligible_tables:
        name = table if "." in table else "bronze." + table
        qualified_name(name)
        if not name.startswith("bronze."):
            raise ValueError("eligible table outside bronze")
        expected.add(name)
    if not expected:
        raise ValueError("empty eligible table set")
    with Path(contract_path).open(encoding="utf-8") as stream:
        document = yaml.safe_load(stream)
    tables = document.get("tables") if isinstance(document, dict) else None
    if not isinstance(tables, dict):
        raise ValueError("contract must contain a tables mapping")
    missing = expected - tables.keys()
    if missing:
        raise ValueError("eligible tables absent from contract: " + ", ".join(sorted(missing)))
    result = []
    for name in sorted(expected):
        entry = tables[name]
        if not isinstance(entry, dict):
            raise ValueError("invalid contract entry: " + name)
        key, partition = entry.get("natural_key"), entry.get("partition_columns")
        if not isinstance(key, list) or not isinstance(partition, list):
            raise ValueError("missing natural_key/partition_columns: " + name)
        result.append(TableSpec(name, tuple(key), tuple(partition)))
    return tuple(result)


def canonical_mapping(registry: Mapping[str, int]) -> dict[str, str]:
    """Freeze only registry-proven aliases; never guess an unlisted alias."""
    result = {}
    for legacy, tournament_id in registry.items():
        quote_literal(legacy)
        if type(tournament_id) is not int or tournament_id <= 0:
            raise ValueError("invalid registry tournament ID")
        canonical = canonical_league(legacy, registry)
        if not isinstance(canonical, str) or not SS_KEY.fullmatch(canonical):
            raise ValueError("registry returned a noncanonical key")
        if canonical != "SS-" + str(tournament_id):
            raise ValueError("ambiguous registry mapping")
        if SS_KEY.fullmatch(legacy) and legacy != canonical:
            raise ValueError("conflicting canonical registry key")
        result[legacy] = canonical
    return dict(sorted(result.items()))


def league_expression(mapping: Mapping[str, str]) -> str:
    cases = []
    for legacy, target in sorted(mapping.items()):
        if not SS_KEY.fullmatch(target):
            raise ValueError("invalid mapping target")
        cases.append(f"WHEN {quote_identifier('league')} = {quote_literal(legacy)} THEN {quote_literal(target)}")
    return (
        "CASE " + " ".join(cases)
        + " WHEN regexp_like(\"league\", '^SS-[1-9][0-9]*\\z') THEN \"league\" ELSE NULL END"
    )


def archive_name(spec: TableSpec, migration_id: str, archive_schema: str) -> str:
    validate_migration_id(migration_id)
    quote_identifier(archive_schema)
    # Include the full source identity in the digest; '-' and '_' never alias.
    digest = hashlib.sha256((spec.name + "\0" + migration_id).encode()).hexdigest()
    table = spec.name.split(".")[1][:40] + "_pk_" + digest
    name = archive_schema + "." + table
    qualified_name(name)
    return name


def checked_columns(columns: Mapping[str, Any], spec: TableSpec) -> dict[str, str]:
    if not isinstance(columns, Mapping) or not columns:
        raise ValueError("missing ordered source schema")
    checked = {}
    for name, kind in columns.items():
        quote_identifier(name)
        kind = " ".join(str(kind).lower().split())
        if not SCALAR_TYPE.fullmatch(kind):
            raise ValueError("unsupported non-scalar/unknown column type: " + name)
        checked[name] = kind
    if RANK_COLUMN in checked:
        raise ValueError("reserved migration column already exists")
    if not set(spec.natural_key + spec.partition_columns).issubset(checked):
        raise ValueError("source is missing contract columns")
    if not checked["league"].startswith(("varchar", "char")):
        raise ValueError("league must be textual")
    if "_ingested_at" in checked and not checked["_ingested_at"].startswith("timestamp"):
        raise ValueError("_ingested_at must have a timestamp type to prove temporal ordering")
    return checked


def projection(columns: Mapping[str, str], mapping: Mapping[str, str]) -> str:
    return ", ".join(
        league_expression(mapping) + ' AS "league"' if c == "league" else quote_identifier(c)
        for c in columns
    )


def winner_metadata(columns: Mapping[str, str]) -> list[str]:
    # A hash/lexical fallback proves reproducibility, not a fresher payload.
    return [c for c in ("_ingested_at", "_batch_id") if c in columns]


def ordering(columns: Mapping[str, str], metadata_only: bool = False) -> str:
    metadata = winner_metadata(columns)
    parts = [quote_identifier(c) + " DESC NULLS LAST" for c in metadata]
    if not metadata_only:
        fallback = (["raw_content_hash"] if "raw_content_hash" in columns else [])
        fallback += [c for c in columns if c not in metadata and c != "raw_content_hash"]
        parts.extend(quote_identifier(c) + " ASC NULLS LAST" for c in fallback)
    return ", ".join(parts) or "1"


def winner_select(spec: TableSpec, source: str, columns: Mapping[str, str], mapping: Mapping[str, str]) -> str:
    columns = checked_columns(columns, spec)
    names = ", ".join(map(quote_identifier, columns))
    keys = ", ".join(map(quote_identifier, spec.natural_key))
    return (
        f"WITH canonical AS (SELECT {projection(columns, mapping)} FROM {qualified_name(source)}) "
        f"SELECT {names} FROM (SELECT {names}, ROW_NUMBER() OVER "
        f"(PARTITION BY {keys} ORDER BY {ordering(columns)}) AS {quote_identifier(RANK_COLUMN)} "
        f"FROM canonical) AS ranked WHERE {quote_identifier(RANK_COLUMN)} = 1"
    )


def archive_sql(spec: TableSpec, archive: str) -> str:
    return f"CREATE TABLE {qualified_name(archive)} AS SELECT * FROM {qualified_name(spec.name)}"


def rewrite_sql(spec: TableSpec, archive: str, columns: Mapping[str, str], mapping: Mapping[str, str]) -> str:
    return (
        f"CREATE OR REPLACE TABLE {qualified_name(spec.name)} "
        "WITH (format='PARQUET', partitioning=ARRAY['league','season']) AS "
        + winner_select(spec, archive, columns, mapping)
    )


def preview(spec: TableSpec, archive: str, mapping: Mapping[str, str], columns: Mapping[str, str] | None = None) -> dict:
    if columns is not None:
        rewrite = rewrite_sql(spec, archive, columns, mapping)
    else:
        # A disconnected run cannot invent the current ordered source schema.
        keys = ", ".join(map(quote_identifier, spec.natural_key))
        rewrite = (
            f"CREATE OR REPLACE TABLE {qualified_name(spec.name)} "
            "WITH (format='PARQUET', partitioning=ARRAY['league','season']) AS "
            "WITH canonical AS (SELECT {{ordered_columns_with_canonical_league}} FROM "
            + qualified_name(archive) + ") SELECT {{ordered_source_columns}} FROM "
            "(SELECT {{ordered_source_columns}}, ROW_NUMBER() OVER (PARTITION BY "
            + keys + " ORDER BY {{winner_metadata_then_scalar_columns}}) AS "
            + quote_identifier(RANK_COLUMN) + " FROM canonical) AS ranked WHERE "
            + quote_identifier(RANK_COLUMN) + " = 1"
        )
    return {
        "archive": archive,
        "archive_sql": archive_sql(spec, archive),
        "rewrite_sql": rewrite,
        "rewrite_is_template": columns is None,
        "canonical_league_expression": league_expression(mapping),
        "requires": ["approved external writer lock", "successful preflight", "verified full archive"],
    }


def manager_call(manager: Any, method: str, *args: Any, **kwargs: Any) -> Any:
    try:
        return getattr(manager, method)(*args, **kwargs)
    except Exception as exc:
        # Driver errors may embed a source value. Keep row payloads out of JSON.
        raise RuntimeError(f"manager {method} failed ({type(exc).__name__})") from None


def scalar(manager: Any, sql: str) -> int:
    rows = manager_call(manager, "_execute", sql, fetch=True)
    if not isinstance(rows, (list, tuple)) or len(rows) != 1:
        raise ValueError("scalar query did not return exactly one row")
    row = rows[0]
    if not isinstance(row, (list, tuple)) or len(row) != 1 or type(row[0]) is not int or row[0] < 0:
        raise ValueError("scalar query did not return a nonnegative integer")
    return row[0]


def count_query(manager: Any, label: str, query: str) -> int:
    # Fixed labels make evidence and synthetic manager traces unambiguous.
    return scalar(manager, f"/* pk:{label} */ {query}")


def ddl_evidence(manager: Any, name: str, require_partition: bool) -> str:
    rows = manager_call(manager, "_execute", "SHOW CREATE TABLE " + qualified_name(name), fetch=True)
    if not isinstance(rows, (list, tuple)) or len(rows) != 1 or len(rows[0]) != 1 or not isinstance(rows[0][0], str):
        raise ValueError("missing SHOW CREATE TABLE evidence")
    ddl = rows[0][0]
    if require_partition:
        matches = re.findall(r"\bpartitioning\s*=\s*ARRAY\s*\[([^\]]*)\]", ddl, flags=re.I)
        if len(matches) != 1 or not re.fullmatch(r"\s*'league'\s*,\s*'season'\s*", matches[0]):
            raise ValueError("source partition declaration is not exactly league, season")
    return ddl


def profile_table(manager: Any, spec: TableSpec, mapping: Mapping[str, str], *, source: str | None = None, require_partition: bool = True) -> dict:
    name = source or spec.name
    schema, table = name.split(".")
    qualified_name(name)
    if not manager_call(manager, "table_exists", schema, table):
        raise ValueError("required table is absent: " + name)
    columns = checked_columns(manager_call(manager, "get_table_columns", schema, table), spec)
    ddl = ddl_evidence(manager, name, require_partition)
    table_sql = qualified_name(name)
    key_sql = ", ".join(map(quote_identifier, spec.natural_key))
    canonical_cte = f"WITH canonical AS (SELECT {projection(columns, mapping)} FROM {table_sql}) "
    invalid = " OR ".join(
        f"{quote_identifier(k)} IS NULL OR trim(CAST({quote_identifier(k)} AS VARCHAR)) = ''"
        for k in spec.natural_key
    )
    queries = {
        "rows": f"SELECT count(*) FROM {table_sql}",
        "null_natural_keys": f"SELECT count(*) FROM {table_sql} WHERE {invalid}",
        "non_ss_keys": f"SELECT count(*) FROM {table_sql} WHERE \"league\" IS NULL OR NOT regexp_like(\"league\", '^SS-[1-9][0-9]*\\z')",
        "unmapped_rows": f"SELECT count(*) FROM {table_sql} WHERE ({league_expression(mapping)}) IS NULL",
        "duplicate_groups_before": f"SELECT count(*) FROM (SELECT {key_sql} FROM {table_sql} GROUP BY {key_sql} HAVING count(*) > 1) AS groups_before",
        "distinct_canonical_keys": canonical_cte + f"SELECT count(*) FROM (SELECT {key_sql} FROM canonical GROUP BY {key_sql}) AS distinct_keys",
        "duplicate_groups_after": canonical_cte + f"SELECT count(*) FROM (SELECT {key_sql} FROM canonical GROUP BY {key_sql} HAVING count(*) > 1) AS groups_after",
    }
    names = ", ".join(map(quote_identifier, columns))
    queries["unsafe_winner_groups"] = (
        canonical_cte + f", ranked AS (SELECT {names}, DENSE_RANK() OVER "
        f"(PARTITION BY {key_sql} ORDER BY {ordering(columns, metadata_only=True)}) AS {quote_identifier(RANK_COLUMN)} FROM canonical), "
        f"distinct_winners AS (SELECT DISTINCT {names} FROM ranked WHERE {quote_identifier(RANK_COLUMN)} = 1) "
        f"SELECT count(*) FROM (SELECT {key_sql} FROM distinct_winners GROUP BY {key_sql} HAVING count(*) > 1) AS conflicts"
    )
    counts = {label: count_query(manager, label, query) for label, query in queries.items()}
    if counts["distinct_canonical_keys"] > counts["rows"]:
        raise ValueError("inconsistent preflight counts; source may be changing")
    counts["collapsed_duplicate_excess"] = counts["rows"] - counts["distinct_canonical_keys"]
    blockers = [key for key in ("null_natural_keys", "unmapped_rows", "unsafe_winner_groups") if counts[key]]
    return {"name": name, "columns": columns, "ddl": ddl, "counts": counts, "ready": not blockers, "blockers": blockers}


def validate_reconciliation(
    *, before_rows: int, before_distinct_canonical_keys: int, after_rows: int,
    archive_rows: int, collapsed_duplicate_excess: int, missing_natural_keys: int,
    extra_natural_keys: int, unmapped_rows: int, null_natural_keys: int,
    duplicate_keys: int, non_ss_keys: int, payload_mismatches: int,
) -> None:
    values = locals()
    if any(type(value) is not int or value < 0 for value in values.values()):
        raise ValueError("reconciliation requires nonnegative integer counts")
    zero = ("missing_natural_keys", "extra_natural_keys", "unmapped_rows", "null_natural_keys", "duplicate_keys", "non_ss_keys", "payload_mismatches")
    failed = [key for key in zero if values[key] != 0]
    if after_rows != before_distinct_canonical_keys:
        failed.append("after_rows != before_distinct_canonical_keys")
    if before_rows != after_rows + collapsed_duplicate_excess:
        failed.append("before_rows != after_rows + collapsed_duplicate_excess")
    if archive_rows != before_rows:
        failed.append("archive_rows != before_rows")
    if failed:
        raise ValueError("reconciliation failed: " + ", ".join(failed))


def rowset_difference(manager: Any, left: str, right: str, label: str) -> int:
    return count_query(
        manager, label,
        "SELECT count(*) FROM ((" + left + " EXCEPT ALL " + right
        + ") UNION ALL (" + right + " EXCEPT ALL " + left + ")) AS differences",
    )


def postcheck(manager: Any, spec: TableSpec, archive: str, archived: dict, mapping: Mapping[str, str]) -> tuple[dict, dict]:
    after = profile_table(manager, spec, mapping)
    if after["columns"] != archived["columns"] or list(after["columns"]) != list(archived["columns"]):
        raise ValueError("ordered source schema changed")
    keys = ", ".join(map(quote_identifier, spec.natural_key))
    canonical_keys = ", ".join(
        league_expression(mapping) + ' AS "league"' if key == "league" else quote_identifier(key)
        for key in spec.natural_key
    )
    common = (
        f"WITH expected AS (SELECT DISTINCT {canonical_keys} FROM {qualified_name(archive)}), "
        f"actual AS (SELECT DISTINCT {keys} FROM {qualified_name(spec.name)}) "
    )
    equality = " AND ".join(f"e.{quote_identifier(k)} = a.{quote_identifier(k)}" for k in spec.natural_key)
    missing = count_query(manager, "missing_natural_keys", common + f"SELECT count(*) FROM expected e WHERE NOT EXISTS (SELECT 1 FROM actual a WHERE {equality})")
    extra = count_query(manager, "extra_natural_keys", common + f"SELECT count(*) FROM actual a WHERE NOT EXISTS (SELECT 1 FROM expected e WHERE {equality})")
    names = ", ".join(map(quote_identifier, archived["columns"]))
    # Nest the winner query so EXCEPT ALL has ordinary SELECT operands.
    expected_rows = "SELECT * FROM (" + winner_select(spec, archive, archived["columns"], mapping) + ") AS expected_rows"
    payload_mismatches = rowset_difference(manager, expected_rows, f"SELECT {names} FROM {qualified_name(spec.name)}", "payload_mismatches")
    before, current = archived["counts"], after["counts"]
    reconciliation = {
        "before_rows": before["rows"],
        "before_distinct_canonical_keys": before["distinct_canonical_keys"],
        "after_rows": current["rows"],
        "archive_rows": count_query(manager, "archive_rows_post", "SELECT count(*) FROM " + qualified_name(archive)),
        "collapsed_duplicate_excess": before["collapsed_duplicate_excess"],
        "missing_natural_keys": missing,
        "extra_natural_keys": extra,
        "unmapped_rows": current["unmapped_rows"],
        "null_natural_keys": current["null_natural_keys"],
        "duplicate_keys": current["duplicate_groups_before"],
        "non_ss_keys": current["non_ss_keys"],
        "payload_mismatches": payload_mismatches,
    }
    return after, reconciliation


def default_manager_factory() -> Any:
    """Construct the existing Trino manager only for a live mode."""
    from scrapers.base.trino_manager import TrinoTableManager

    return TrinoTableManager()


def run_migration(
    *, migration_id: str, mode: str = "dry_run", contract_path: str | Path = DEFAULT_CONTRACT,
    specs: Sequence[TableSpec] | None = None, tournament_by_canonical: Mapping[str, int] | None = None,
    manager_factory: Callable[[], Any] | None = None,
    archive_schema: str = "bronze_migration_archive", writers_paused: bool = False,
) -> dict:
    validate_migration_id(migration_id)
    quote_identifier(archive_schema)
    if mode not in {"dry_run", "preflight", "apply"}:
        raise ValueError("unknown mode")
    if mode == "apply" and not writers_paused:
        raise ValueError("apply requires an externally enforced writer pause and writers_paused=True")
    selected = tuple(sorted(specs if specs is not None else load_table_specs(contract_path), key=lambda s: s.name))
    if not selected or len({spec.name for spec in selected}) != len(selected):
        raise ValueError("empty or repeated table selection")
    if tournament_by_canonical is None:
        from scrapers.sofascore.catalog import SofaScoreCatalog

        tournament_by_canonical = SofaScoreCatalog.load().tournament_map(enabled_only=False)
    mapping = canonical_mapping(tournament_by_canonical)
    report = {
        "mode": mode, "mutates": False, "migration_id": migration_id,
        "archive_schema": archive_schema, "success": False, "ready": False,
        "tables": [
            {"spec": asdict(spec), "status": "prepared", "plan": preview(spec, archive_name(spec, migration_id, archive_schema), mapping)}
            for spec in selected
        ],
    }
    if mode == "dry_run":
        report["success"] = True
        report["readiness"] = "not_checked_without_connection"
        return report
    manager = None
    active = None
    try:
        try:
            manager = (manager_factory or default_manager_factory)()
        except Exception as exc:
            raise RuntimeError(f"manager construction failed ({type(exc).__name__}); verify the live adapter") from None
        # All tables must pass read-only preflight before the first mutation.
        for spec, item in zip(selected, report["tables"]):
            active = item
            item["phase"] = "preflight"
            before = profile_table(manager, spec, mapping)
            item["before"] = before
            item["plan"] = preview(spec, item["plan"]["archive"], mapping, before["columns"])
            item["status"] = "ready" if before["ready"] else "blocked"
            if not before["ready"]:
                report["error"] = "preflight blocked: " + ", ".join(before["blockers"])
                return report
        report["ready"] = True
        if mode == "preflight":
            report["success"] = True
            return report
        for spec, item in zip(selected, report["tables"]):
            active = item
            archive = item["plan"]["archive"]
            schema, table = archive.split(".")
            existing = manager_call(manager, "table_exists", schema, table)
            item["phase"] = "verify_existing_archive" if existing else "archive"
            if not existing:
                # Mark attempts as potentially mutating even if the server loses
                # its response. Never retry DDL automatically after an exception.
                report["mutates"] = True
                manager_call(manager, "_execute", item["plan"]["archive_sql"])
            item["archive_rows"] = count_query(manager, "archive_rows", "SELECT count(*) FROM " + qualified_name(archive))
            if not existing and item["archive_rows"] != item["before"]["counts"]["rows"]:
                raise ValueError("archive row count differs from before_rows")
            archived = profile_table(manager, spec, mapping, source=archive, require_partition=False)
            item["archive_evidence"] = archived
            if not archived["ready"] or archived["counts"]["rows"] != item["archive_rows"]:
                raise ValueError("archive is not safe or its counts changed")
            if list(archived["columns"].items()) != list(item["before"]["columns"].items()):
                raise ValueError("archive schema differs from ordered source schema")
            if not existing:
                if archived["counts"] != item["before"]["counts"]:
                    raise ValueError("archive profile differs from preflight")
                item["phase"] = "verify_source_unchanged"
                drift = rowset_difference(manager, "SELECT * FROM " + qualified_name(spec.name), "SELECT * FROM " + qualified_name(archive), "source_archive_difference")
                item["source_archive_difference"] = drift
                if drift:
                    raise ValueError("source differs from full archive; writer pause is not established")
                item["phase"] = "rewrite"
                manager_call(manager, "_execute", item["plan"]["rewrite_sql"])
            # An existing archive is never overwritten and never authorizes a
            # second rewrite. Only a verified canonical result is a valid repeat.
            item["phase"] = "postcheck"
            after, reconciliation = postcheck(manager, spec, archive, archived, mapping)
            item["after"] = after
            item["reconciliation"] = reconciliation
            validate_reconciliation(**reconciliation)
            item["status"] = "noop_verified" if existing else "applied_verified"
            item["phase"] = "complete"
        report["success"] = True
        return report
    except Exception as exc:
        report["success"] = False
        report["ready"] = False
        report["error"] = f"{type(exc).__name__}: {exc}"
        if active is not None:
            active["status"] = "failed"
        return report
    finally:
        close = getattr(manager, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:
                report["success"] = False
                report["close_error"] = type(exc).__name__


def main(argv: Sequence[str] | None = None, *, manager_factory: Callable[[], Any] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--dry-run", action="store_true")
    modes.add_argument("--preflight", action="store_true")
    modes.add_argument("--apply", action="store_true")
    parser.add_argument("--migration-id", required=True)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--archive-schema", default="bronze_migration_archive")
    parser.add_argument("--writers-paused", action="store_true", help="attest an approved external writer lock for the entire apply window")
    args = parser.parse_args(argv)
    mode = "apply" if args.apply else "preflight" if args.preflight else "dry_run"
    try:
        result = run_migration(
            mode=mode, migration_id=args.migration_id, contract_path=args.contract,
            archive_schema=args.archive_schema, writers_paused=args.writers_paused,
            manager_factory=manager_factory,
        )
    except Exception as exc:
        result = {"mode": mode, "migration_id": args.migration_id, "mutates": False, "success": False, "error": f"{type(exc).__name__}: {exc}"}
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if result["success"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
