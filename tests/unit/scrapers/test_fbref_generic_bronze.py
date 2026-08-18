from dataclasses import replace
import re
from unittest.mock import MagicMock

import pytest

from scrapers.fbref.bronze import (
    FBrefGenericBronzeWriter,
    GenericBronzeBatchUnsupported,
    GenericPagePersistItem,
    GenericPersistenceError,
    PAGE_MANIFEST_TABLE,
    TABLE_CELLS_TABLE,
    TABLE_INVENTORY_TABLE,
    _batch_token,
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


def _page(target_id="fbref:season:9:2025"):
    return parse_page_document(
        """
        <table id="stats_standard"><tr><th data-stat="player">Player</th></tr>
        <tr><td data-stat="player"><a href="/en/players/1234abcd/P">P</a></td></tr>
        </table>
        """,
        target_id=target_id,
        page_kind="season",
    )


def _page_item(identity: str, *, page=None):
    return GenericPagePersistItem(
        page=page or _page(f"fbref:season:9:{identity}"),
        canonical_url=f"https://fbref.com/en/comps/9/{identity}/source",
        run_id=f"run-{identity}",
        staging_identity=identity,
    )


def _merge_sql(manager):
    return [
        call.args[0]
        for call in manager._execute.call_args_list
        if call.args and call.args[0].startswith("MERGE INTO")
    ]


def test_persist_pages_merges_each_generic_table_once_in_commit_order():
    manager = _manager()

    counts = FBrefGenericBronzeWriter(manager).persist_pages(
        [_page_item("a"), _page_item("b")]
    )

    assert counts == [
        {"cells": 1, "tables": 1, "manifest": 1},
        {"cells": 1, "tables": 1, "manifest": 1},
    ]
    sql = _merge_sql(manager)
    assert len(sql) == 3
    assert [
        next(
            table
            for table in (
                TABLE_CELLS_TABLE,
                TABLE_INVENTORY_TABLE,
                PAGE_MANIFEST_TABLE,
            )
            if f"iceberg.bronze.{table}" in statement
        )
        for statement in sql
    ] == [TABLE_CELLS_TABLE, TABLE_INVENTORY_TABLE, PAGE_MANIFEST_TABLE]
    assert all(
        re.search(r"__stg_batch_[0-9a-f]{16}_[ctm]", statement)
        for statement in sql
    )


def test_persist_pages_returns_aligned_zero_counts_for_empty_valid_page():
    manager = _manager()
    empty = parse_page_document(
        "<html><body></body></html>",
        target_id="fbref:season:9:empty",
        page_kind="season",
    )

    counts = FBrefGenericBronzeWriter(manager).persist_pages(
        [_page_item("normal"), _page_item("empty", page=empty)]
    )

    assert counts == [
        {"cells": 1, "tables": 1, "manifest": 1},
        {"cells": 0, "tables": 0, "manifest": 1},
    ]
    assert len(_merge_sql(manager)) == 3


def test_persist_pages_rejects_duplicate_natural_keys_before_first_ddl():
    manager = _manager()
    item = _page_item("duplicate")

    with pytest.raises(GenericBronzeBatchUnsupported, match="duplicate"):
        FBrefGenericBronzeWriter(manager).persist_pages([item, item])

    manager.create_iceberg_table.assert_not_called()
    manager._execute.assert_not_called()
    manager.insert_dataframe.assert_not_called()


def test_persist_pages_rejects_parser_errors_for_sequential_fallback():
    manager = _manager()
    error_page = replace(_page("fbref:season:9:error"), errors=("bad table",))

    with pytest.raises(GenericBronzeBatchUnsupported, match="parser errors"):
        FBrefGenericBronzeWriter(manager).persist_pages(
            [_page_item("valid"), _page_item("error", page=error_page)]
        )

    manager.create_iceberg_table.assert_not_called()
    manager._execute.assert_not_called()
    manager.insert_dataframe.assert_not_called()


def test_batch_stage_token_uses_complete_sorted_delimiter_safe_identity_set():
    assert _batch_token(["b", "a"]) == _batch_token(["a", "b"])
    assert _batch_token(["a", "b"]).startswith("batch_")
    assert len(_batch_token(["a", "b"])) == len("batch_") + 16
    assert _batch_token(["a", "b\x1fc"]) != _batch_token(["a\x1fb", "c"])


def test_persist_page_delegates_valid_pages_to_single_item_batch(monkeypatch):
    writer = FBrefGenericBronzeWriter(_manager())
    captured = []

    def persist_pages(items):
        captured.extend(items)
        return [{"cells": 1, "tables": 1, "manifest": 1}]

    monkeypatch.setattr(writer, "persist_pages", persist_pages)

    counts = writer.persist_page(
        _page(),
        canonical_url="https://fbref.com/test",
        run_id="run",
        staging_identity="identity",
    )

    assert counts == {"cells": 1, "tables": 1, "manifest": 1}
    assert len(captured) == 1
    assert captured[0].staging_identity == "identity"


def test_single_page_keeps_exact_logical_refresh_stage_owner():
    manager = _manager()
    logical_refresh_id = "cb02b6ce-aab7-4c9a-85d0-1292a49e03a2"

    FBrefGenericBronzeWriter(manager).persist_page(
        _page(),
        canonical_url="https://fbref.com/test",
        run_id="run",
        staging_identity=logical_refresh_id,
    )

    assert all(
        "__stg_lr_cb02b6ceaab74c9a85d01292a49e03a2_" in statement
        for statement in _merge_sql(manager)
    )


def test_persist_pages_materializes_every_frame_before_table_preflight(
    monkeypatch,
):
    manager = _manager()
    writer = FBrefGenericBronzeWriter(manager)
    original_decorate = writer._decorate
    calls = 0

    def fail_late_decorate(records, run_id, persisted_at):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise TypeError("injected late concat failure")
        return original_decorate(records, run_id, persisted_at)

    monkeypatch.setattr(writer, "_decorate", fail_late_decorate)

    with pytest.raises(TypeError, match="late concat failure"):
        writer.persist_pages([_page_item("materialize-first")])

    manager.create_iceberg_table.assert_not_called()
    manager._execute.assert_not_called()
    manager.insert_dataframe.assert_not_called()


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

    writer.persist_page(
        _page(),
        canonical_url="https://fbref.com/en/comps/9/2025/source",
        run_id="scheduled__2026-07-11-retry",
        staging_identity="dag_task_0_2",
    )
    assert manager.create_iceberg_table.call_count == 3


def test_generic_writer_retries_table_preflight_after_partial_failure():
    manager = _manager()
    writer = FBrefGenericBronzeWriter(manager)
    manager.create_iceberg_table.side_effect = [
        RuntimeError("injected CREATE failure"),
        None,
        None,
        None,
    ]

    with pytest.raises(RuntimeError, match="CREATE failure"):
        writer.persist_page(
            _page(),
            canonical_url="https://fbref.com/test",
            run_id="failed-preflight",
        )

    writer.persist_page(
        _page(),
        canonical_url="https://fbref.com/test",
        run_id="preflight-retry",
    )
    assert manager.create_iceberg_table.call_count == 4


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


def _statement_count(manager) -> int:
    """Manager operations, which is not the same as Trino statements.

    ``insert_dataframe`` fans out into as many INSERTs as the row bytes need
    (SQL_BYTE_BUDGET, trino_manager.py), so the cell payload keeps scaling with
    the data whatever the cohort. What cohorting removes is the fixed cost
    around it -- stage create/drop, count, merge -- and the two small tables
    that carry one or two rows per page.
    """

    return (
        len(manager._execute.call_args_list)
        + len(manager.insert_dataframe.call_args_list)
        + len(manager.drop_table.call_args_list)
        + len(manager.create_iceberg_table.call_args_list)
    )


@pytest.mark.parametrize("cohort_size", [2, 8, 15])
def test_a_cohort_costs_one_pages_worth_of_statements(cohort_size):
    """The whole point of cohorting: statements stop scaling with pages.

    A page written alone costs six statements per generic table -- drop stage,
    create stage, insert, count, merge, drop stage -- and it has three tables,
    two of which carry a single row.  Live evidence for what that costs:
    history_20260818T015855Z persisted 15 pages and spent 3485 HTTP round
    trips on Trino while fetching for about a minute.
    """

    items = [_page_item(str(index)) for index in range(cohort_size)]

    one_by_one = _manager()
    writer = FBrefGenericBronzeWriter(one_by_one)
    for item in items:
        writer.persist_pages([item])
    alone = _statement_count(one_by_one)

    cohorted = _manager()
    FBrefGenericBronzeWriter(cohorted).persist_pages(items)
    together = _statement_count(cohorted)

    # Six statements per generic table -- drop stage, create stage, insert,
    # count, merge, drop stage -- and three tables. The table preflight is
    # cached per writer, so it is paid once either way.
    per_page = 6 * 3
    preflight = 3
    assert together == per_page + preflight
    assert alone == per_page * cohort_size + preflight
    assert len(_merge_sql(cohorted)) == 3
    assert len(_merge_sql(one_by_one)) == 3 * cohort_size
