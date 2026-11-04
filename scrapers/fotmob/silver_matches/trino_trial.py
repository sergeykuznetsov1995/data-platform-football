"""Bounded first-release runner: typed Iceberg tables in a new isolated namespace.

No production schema, replacement, retries of uncertain writes or scheduling.
Python decodes source carriers; Trino executes typed SELECTs and Iceberg writes.
"""

from __future__ import annotations

import collections
from datetime import datetime, timezone
import json
import re
from typing import Any

TRIAL_PREFIX = "silver_fotmob_match_trial_"
SCHEMA_RE = re.compile(r"silver_fotmob_match_trial_[a-z0-9_]{1,48}\Z")
IDENTIFIER_RE = re.compile(r"[a-z_][a-z0-9_]*\Z")
SCALAR_TYPES = {"VARCHAR", "BIGINT", "DOUBLE", "BOOLEAN"}


def identifier(value: str) -> str:
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError("Invalid SQL identifier")
    return '"' + value + '"'


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def physical_type(logical: str) -> str:
    if logical == "JSON":
        return "VARCHAR"
    if logical == "ARRAY(JSON)":
        return "ARRAY(VARCHAR)"
    if logical in SCALAR_TYPES | {
        "ARRAY(BIGINT)",
        "ARRAY(VARCHAR)",
        "MAP(VARCHAR,VARCHAR)",
        "TIMESTAMP(6) WITH TIME ZONE",
    }:
        return logical
    raise ValueError(f"Unsupported logical type: {logical}")


def expression(field: str, logical: str) -> str:
    identifier(field)
    value = f"json_extract(doc, '$.{field}')"
    scalar = f"json_extract_scalar(doc, '$.{field}')"
    if logical == "JSON":
        return f"nullif(json_format({value}), 'null')"
    if logical == "ARRAY(JSON)":
        return f"transform(CAST({value} AS ARRAY(JSON)), x -> json_format(x))"
    if logical == "TIMESTAMP(6) WITH TIME ZONE":
        return f"CAST(from_iso8601_timestamp_nanos({scalar}) AS TIMESTAMP(6) WITH TIME ZONE)"
    physical_type(logical)
    if logical.startswith(("ARRAY(", "MAP(")):
        return f"CAST({value} AS {logical})"
    return f"CAST({scalar} AS {logical})"


def select_rows(fields: dict, encoded_rows: list[str], input_set_id: str) -> str:
    columns = [
        f"{expression(name, spec['type'])} AS {identifier(name)}"
        for name, spec in fields.items()
    ]
    columns += [
        f'{literal(input_set_id)} AS "_input_set_id"',
        'CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE) AS "_silver_created_at"',
    ]
    values = encoded_rows or ["{}"]
    source = ",\n".join("(" + literal(v) + ")" for v in values)
    return (
        "SELECT\n"
        + ",\n".join(columns)
        + "\nFROM (SELECT json_parse(raw) AS doc FROM (VALUES\n"
        + source
        + "\n) AS v(raw))"
        + (" WHERE false" if not encoded_rows else "")
    )


def _encoded_chunks(rows: list[dict], *, byte_budget: int = 200_000):
    chunk, size = [], 0
    for row in rows:
        encoded = json.dumps(
            row, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        )
        n = len(literal(encoded).encode("utf-8")) + 4
        if n > byte_budget:
            raise ValueError("One row exceeds bounded SQL budget")
        if chunk and size + n > byte_budget:
            yield chunk
            chunk, size = [], 0
        chunk.append(encoded)
        size += n
    if chunk:
        yield chunk


def _comparable(value: Any, logical: str, *, stored: bool = False):
    if value is None:
        return None
    if logical == "TIMESTAMP(6) WITH TIME ZONE":
        dt = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(value.replace("Z", "+00:00"))
        )
        if dt.tzinfo is None:
            raise ValueError("Timezone is required for Silver timestamp")
        return dt.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if logical == "JSON" and stored:
        return json.loads(value)
    if logical == "ARRAY(JSON)" and stored:
        return [json.loads(v) if v is not None else None for v in value]
    if logical == "DOUBLE":
        return float(value)
    return value


def row_token(row: dict, fields: dict, *, stored: bool = False) -> str:
    normalized = {
        name: _comparable(row.get(name), spec["type"], stored=stored)
        for name, spec in fields.items()
    }
    return json.dumps(
        normalized,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def write_trial(connection, *, schema_name: str, result, input_set_id: str) -> dict:
    """Create a fresh trial, compare every stored field, then write success receipt.

    Failure leaves unaccepted diagnostic tables. Existing namespace is never
    reused or dropped; no destructive cleanup or production destination exists.
    """
    if not SCHEMA_RE.fullmatch(schema_name):
        raise ValueError("Only a new silver_fotmob_match_trial_* namespace is allowed")
    if not re.fullmatch(r"[0-9a-f]{64}", input_set_id):
        raise ValueError("Input set must have a SHA256 fingerprint")
    tables = result.schema["tables"]
    if set(result.rows) != set(tables):
        raise ValueError("Incomplete table set")
    if len(result.rows["matches"]) != 8:
        raise ValueError("First trial requires exactly eight selected matches")
    publication_ids = {r["publication_id"] for rs in result.rows.values() for r in rs}
    if len(publication_ids) != 1:
        raise ValueError("Mixed publications")
    # Prevalidate serialization/types and query budgets before creating anything.
    for table, spec in tables.items():
        identifier(table)
        for name, field in spec["fields"].items():
            identifier(name)
            physical_type(field["type"])
        list(_encoded_chunks(result.rows[table]))
        for row in result.rows[table]:
            row_token(row, spec["fields"])
    cursor = connection.cursor()

    def execute(sql):
        cursor.execute(sql)
        return cursor.fetchall()

    namespace = "iceberg." + identifier(schema_name)
    execute("CREATE SCHEMA " + namespace)
    table_results = {}
    for table, spec in tables.items():
        fields = spec["fields"]
        name = namespace + "." + identifier(table)
        execute(
            "CREATE TABLE " + name + " AS\n" + select_rows(fields, [], input_set_id)
        )
        columns = list(fields) + ["_input_set_id", "_silver_created_at"]
        for chunk in _encoded_chunks(result.rows[table]):
            execute(
                "INSERT INTO "
                + name
                + " ("
                + ",".join(identifier(c) for c in columns)
                + ")\n"
                + select_rows(fields, chunk, input_set_id)
            )
        observed = execute(
            "SELECT " + ",".join(identifier(c) for c in fields) + " FROM " + name
        )
        expected = collections.Counter(row_token(r, fields) for r in result.rows[table])
        actual = collections.Counter(
            row_token(dict(zip(fields, values)), fields, stored=True)
            for values in observed
        )
        if actual != expected:
            raise ValueError(f"Iceberg field readback differs for {table}")
        lineage = execute(
            "SELECT count(*) FROM "
            + name
            + ' WHERE "_input_set_id" <> '
            + literal(input_set_id)
            + ' OR "_input_set_id" IS NULL'
        )
        if lineage != [[0]] and lineage != [(0,)]:
            raise ValueError(f"Input lineage mismatch for {table}")
        table_results[table] = {"rows": len(observed), "all_fields_equal": True}
    receipt = {
        "schema": schema_name,
        "publication_id": next(iter(publication_ids)),
        "input_set_id": input_set_id,
        "tables": table_results,
        "complete": True,
    }
    # Commit marker only after all table and value checks; consumer must pin it.
    execute(
        "CREATE TABLE "
        + namespace
        + '."trial_receipt" AS SELECT '
        + literal(json.dumps(receipt, sort_keys=True))
        + ' AS "receipt_json"'
    )
    observed_receipt = execute(
        'SELECT "receipt_json" FROM ' + namespace + '."trial_receipt"'
    )
    if json.loads(observed_receipt[0][0]) != receipt:
        raise ValueError("Receipt readback mismatch")
    return receipt
