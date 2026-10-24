from datetime import datetime, timezone
from types import SimpleNamespace
import json
import pytest
from scrapers.fotmob.silver_matches.trino_trial import (
    expression,
    physical_type,
    select_rows,
    row_token,
    write_trial,
    _encoded_chunks,
)


def test_production_destination_rejected_before_connection():
    with pytest.raises(ValueError, match="namespace"):
        write_trial(None, schema_name="silver", result=None, input_set_id="a" * 64)


def test_sql_quotes_source_strings_without_code_execution():
    sql = select_rows(
        {"name": {"type": "VARCHAR"}},
        [json.dumps({"name": "O'Brien; DROP TABLE x"})],
        "a" * 64,
    )
    assert "O''Brien; DROP TABLE x" in sql
    assert sql.startswith("SELECT") and "json_parse(raw)" in sql


def test_iceberg_json_has_explicit_string_encoding():
    assert physical_type("JSON") == "VARCHAR"
    assert physical_type("ARRAY(JSON)") == "ARRAY(VARCHAR)"
    assert "json_format" in expression("raw_value", "JSON")
    with pytest.raises(ValueError):
        physical_type("UNKNOWN")


def test_readback_semantics_json_map_array_bigint_and_time():
    fields = {
        k: {"type": t}
        for k, t in [
            ("raw", "JSON"),
            ("items", "ARRAY(JSON)"),
            ("id", "BIGINT"),
            ("time", "TIMESTAMP(6) WITH TIME ZONE"),
        ]
    }
    source = {
        "raw": {"k": None},
        "items": [{"id": 1}, None],
        "id": 9007199254740993,
        "time": "2026-10-24T10:11:12.123456Z",
    }
    target = {
        "raw": '{"k":null}',
        "items": ['{"id":1}', "null"],
        "id": 9007199254740993,
        "time": datetime(2026, 10, 24, 10, 11, 12, 123456, tzinfo=timezone.utc),
    }
    assert row_token(source, fields) == row_token(target, fields, stored=True)
    target["id"] -= 1
    assert row_token(source, fields) != row_token(target, fields, stored=True)


def test_chunks_bound_escaped_sql_bytes():
    rows = [{"text": "'" * 10} for _ in range(6)]
    chunks = list(_encoded_chunks(rows, byte_budget=75))
    assert len(chunks) > 1 and sum(map(len, chunks)) == 6
    with pytest.raises(ValueError, match="exceeds"):
        list(_encoded_chunks([{"text": "x" * 100}], byte_budget=50))


def test_empty_select_retains_schema_but_emits_no_row():
    assert select_rows({"ids": {"type": "ARRAY(BIGINT)"}}, [], "a" * 64).endswith(
        "WHERE false"
    )
    assert "ARRAY(BIGINT)" in select_rows(
        {"ids": {"type": "ARRAY(BIGINT)"}}, [], "a" * 64
    )


def test_bounded_input_required_before_any_db_call():
    result = SimpleNamespace(
        schema={"tables": {"matches": {"fields": {}}}}, rows={"matches": []}
    )
    with pytest.raises(ValueError, match="eight"):
        write_trial(
            None,
            schema_name="silver_fotmob_match_trial_test",
            result=result,
            input_set_id="a" * 64,
        )


def test_metadata_column_identifiers_are_supported():
    from scrapers.fotmob.silver_matches.trino_trial import identifier

    assert identifier("_input_set_id") == '"_input_set_id"'
    assert identifier("_silver_created_at") == '"_silver_created_at"'
    with pytest.raises(ValueError):
        identifier("x;DROP TABLE matches")
