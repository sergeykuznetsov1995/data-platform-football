from dataclasses import replace
from unittest.mock import MagicMock

import pytest

from scrapers.fbref.bronze import (
    FBrefGenericBronzeWriter,
    GenericPersistenceError,
    PAGE_MANIFEST_TABLE,
    _token,
)
from scrapers.fbref.page_document import parse_page_document


def _manager():
    manager = MagicMock()
    manager.catalog = "iceberg"
    staged_rows = {}

    def insert(_schema, table, frame):
        staged_rows[table] = len(frame)
        return len(frame)

    def execute(sql, fetch=False):
        if fetch and sql.startswith("SELECT count(*)"):
            table = sql.split("FROM iceberg.bronze.", 1)[1]
            return [(staged_rows[table],)]
        return None

    manager.insert_dataframe.side_effect = insert
    manager._execute.side_effect = execute
    return manager


def _page():
    return parse_page_document(
        """
        <table id="stats_standard"><tr><th data-stat="player">Player</th></tr>
        <tr><td data-stat="player"><a href="/en/players/1234abcd/P">P</a></td></tr>
        </table>
        """,
        target_id="fbref:season:9:2025",
        page_kind="season",
    )


def test_generic_writer_merges_by_identity_and_commits_page_manifest_last():
    manager = _manager()
    writer = FBrefGenericBronzeWriter(manager)

    counts = writer.persist_page(
        _page(),
        canonical_url="https://fbref.com/en/comps/9/2025/source",
        run_id="scheduled__2026-07-11",
        staging_identity="dag_task_0_1",
    )

    assert counts == {"cells": 1, "tables": 1, "manifest": 1}
    assert manager.create_iceberg_table.call_count == 3
    merge_sql = [
        call.args[0]
        for call in manager._execute.call_args_list
        if call.args and call.args[0].startswith("MERGE INTO")
    ]
    assert len(merge_sql) == 3
    assert f"iceberg.bronze.{PAGE_MANIFEST_TABLE}" in merge_sql[-1]
    assert all("WHEN MATCHED THEN UPDATE" in sql for sql in merge_sql)
    stages = [sql.split(" USING ", 1)[1].split(" s ON", 1)[0] for sql in merge_sql]
    assert len(stages) == len(set(stages))


def test_parser_error_is_persisted_as_error_marker_and_fails_task():
    manager = _manager()
    writer = FBrefGenericBronzeWriter(manager)
    page = replace(_page(), errors=("bad table",))

    with pytest.raises(GenericPersistenceError, match="bad table"):
        writer.persist_page(
            page,
            canonical_url="https://fbref.com/test",
            run_id="run",
        )

    manifest_frames = [
        call.args[2]
        for call in manager.insert_dataframe.call_args_list
        if call.args[1].startswith(PAGE_MANIFEST_TABLE)
    ]
    assert manifest_frames[-1].iloc[0]["parse_status"] == "error"
    assert manifest_frames[-1].iloc[0]["validation_status"] == "error"


def test_stage_identity_is_deterministic_and_exposes_logical_refresh_owner():
    logical_refresh_id = "cb02b6ce-aab7-4c9a-85d0-1292a49e03a2"

    first = _token(logical_refresh_id)
    second = _token(logical_refresh_id)

    assert first == second == "lr_cb02b6ceaab74c9a85d01292a49e03a2"


def test_non_uuid_stage_identity_is_stable_and_identifier_safe():
    token = _token("scheduled__2026-07-15 / secret-looking input")

    assert token == _token("scheduled__2026-07-15 / secret-looking input")
    assert token.startswith("id_")
    assert len(token) == 35
    assert token.replace("_", "").isalnum()
