from unittest.mock import patch

import pandas as pd
import pytest

from scrapers.base.trino_manager import TrinoTableManager
from scrapers.sofascore.iceberg_writer import (
    PARTITION_SCOPED_TABLES,
    SofaScoreIcebergWriter,
)


@pytest.mark.unit
@pytest.mark.parametrize("table", sorted(PARTITION_SCOPED_TABLES))
def test_scopes_each_bronze_merge(table):
    manager = SofaScoreIcebergWriter()._get_trino_manager()
    frame = pd.DataFrame({"league": ["A"], "season": ["2526"], "id": [1]})
    keys = ["league", "season", "id"]
    with patch.object(TrinoTableManager, "insert_dataframe_atomic", return_value=1) as insert:
        assert manager.insert_dataframe_atomic("bronze", table, frame, merge_keys=keys) == 1
    assert insert.call_args.args == ("bronze", table, frame)
    assert insert.call_args.kwargs == {
        "merge_keys": keys,
        "merge_partition_cols": ["league", "season"],
    }


@pytest.mark.unit
@pytest.mark.parametrize("schema,table,keys", [
    ("bronze", "sofascore_rejected_rows", ["table_name", "natural_key", "run_id"]),
    ("ops", "sofascore_capture_manifest", ["source_tournament_id", "source_season_id"]),
    ("bronze", "sofascore_schedule", None),
    ("silver", "sofascore_schedule", ["league", "season", "game_id"]),
    ("bronze", "other_source", ["league", "season", "game_id"]),
])
def test_preserves_unscoped_write_contract(schema, table, keys):
    manager = SofaScoreIcebergWriter()._get_trino_manager()
    frame = pd.DataFrame({"id": [1]})
    kwargs = {"merge_keys": keys, "delete_filter": None}
    with patch.object(TrinoTableManager, "insert_dataframe_atomic", return_value=1) as insert:
        manager.insert_dataframe_atomic(schema, table, frame, **kwargs)
    assert insert.call_args.kwargs == kwargs
