"""#1472: match tables via PyIceberg, commit retry, failure batching, janitor."""

from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

import scrapers.whoscored.repository as repository_module
from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.whoscored.repository import (
    MATCH_COMMIT_RETRIES,
    WHOSCORED_BUSINESS_COLUMN_CONTRACTS,
    ManifestFailure,
    WhoScoredRepository,
)

_ICEBERG_TYPES = {
    "BIGINT": LongType(),
    "BOOLEAN": BooleanType(),
    "DOUBLE": DoubleType(),
    "TIMESTAMP(6)": TimestampType(),
    "VARCHAR": StringType(),
}


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(repository_module.time, "sleep", lambda _seconds: None)


def _repository():
    return WhoScoredRepository(writer=MagicMock(), trino=MagicMock())


# --- commit retry ---------------------------------------------------------


@pytest.mark.unit
def test_match_frame_retries_commit_conflict_then_succeeds():
    repository = _repository()
    repository.writer.write_dataframe.side_effect = [
        CommitFailedException("conflict"),
        CommitFailedException("conflict"),
        "iceberg.bronze.whoscored_events",
    ]

    repository._write_match_frame(
        "whoscored_events", pd.DataFrame([{"x": 1}]), league="L", season="2526"
    )

    assert repository.writer.write_dataframe.call_count == 3
    for call in repository.writer.write_dataframe.call_args_list:
        assert call.kwargs["bulk_arrow"] is True
        assert call.kwargs["table"] == "whoscored_events"


@pytest.mark.unit
def test_match_frame_does_not_retry_unknown_outcome_errors():
    repository = _repository()
    repository.writer.write_dataframe.side_effect = RuntimeError("s3 timeout")

    with pytest.raises(RuntimeError, match="s3 timeout"):
        repository._write_match_frame(
            "whoscored_events", pd.DataFrame([{"x": 1}]), league="L", season="2526"
        )

    assert repository.writer.write_dataframe.call_count == 1


@pytest.mark.unit
def test_match_frame_gives_up_after_bounded_commit_conflicts():
    repository = _repository()
    repository.writer.write_dataframe.side_effect = CommitFailedException("conflict")

    with pytest.raises(CommitFailedException):
        repository._write_match_frame(
            "whoscored_events", pd.DataFrame([{"x": 1}]), league="L", season="2526"
        )

    assert repository.writer.write_dataframe.call_count == MATCH_COMMIT_RETRIES == 5


# --- Arrow alignment of whoscored_events ----------------------------------


class _Target:
    def __init__(self, schema: Schema):
        self._schema = schema

    def schema(self) -> Schema:
        return self._schema


def _events_iceberg_schema(*, legacy_required: bool) -> Schema:
    contract = WHOSCORED_BUSINESS_COLUMN_CONTRACTS["whoscored_events"]
    fields = [
        NestedField(index + 1, name, _ICEBERG_TYPES[data_type], required=False)
        for index, (name, data_type) in enumerate(contract.items())
    ]
    # A column that only exists in the legacy physical table.
    fields.append(
        NestedField(
            len(fields) + 1, "legacy_second", DoubleType(), required=legacy_required
        )
    )
    return Schema(*fields)


def _events_arrow() -> pa.Table:
    rows = [
        {
            "league": "L",
            "season": "2526",
            "game": "Home-Away",
            "game_id": 1,
            "source_event_id": 10,
            "minute": 3,
            "second": None,
            "x": 50,
            "y": None,
            "is_touch": True,
            "qualifiers": "[]",
            "_game_batch_id": "ws2-v3-b",
            "_entity_type": "events",
        },
        {
            "league": "L",
            "season": "2526",
            "game": "Home-Away",
            "game_id": 1,
            "source_event_id": None,
            "minute": 4,
            "second": 12.0,
            "x": 1.5,
            "y": 2,
            "is_touch": None,
            "qualifiers": "[]",
            "_game_batch_id": "ws2-v3-b",
            "_entity_type": "events",
        },
    ]
    frame = WhoScoredRepository._normalise_frame_types(
        pd.DataFrame(rows), table="whoscored_events"
    )
    writer = IcebergWriter.__new__(IcebergWriter)
    return writer._pandas_to_arrow(writer._add_metadata_columns(frame, "whoscored"))


@pytest.mark.unit
def test_events_frame_aligns_to_real_iceberg_schema_with_optional_legacy_column():
    schema = _events_iceberg_schema(legacy_required=False)

    aligned = IcebergWriter._align_pyiceberg_arrow_table(
        _events_arrow(), target=_Target(schema)
    )

    assert aligned.schema == schema_to_pyarrow(schema)
    assert aligned.schema.field("second").type == pa.float64()
    assert aligned.schema.field("minute").type == pa.int64()
    assert aligned["legacy_second"].null_count == aligned.num_rows == 2
    assert aligned["second"].to_pylist() == [None, 12.0]
    assert aligned["source_event_id"].to_pylist() == [10, None]
    buffer = io.BytesIO()
    pq.write_table(aligned, buffer)


@pytest.mark.unit
def test_required_legacy_column_fails_loudly_before_commit():
    """Documents the risk: a NOT NULL legacy column is filled with nulls by the
    shared aligner and the Parquet write (inside PyIceberg append, before any
    snapshot commit) rejects it with an error naming the column."""

    schema = _events_iceberg_schema(legacy_required=True)
    aligned = IcebergWriter._align_pyiceberg_arrow_table(
        _events_arrow(), target=_Target(schema)
    )

    with pytest.raises(pa.ArrowInvalid, match="legacy_second.*non-nullable"):
        pq.write_table(aligned, io.BytesIO())


# --- failure manifests in one write ---------------------------------------


def _failure(game_id: int) -> ManifestFailure:
    return ManifestFailure(
        game_id=game_id,
        league="L",
        season="2526",
        state="terminal",
        failure_code="http",
        error="boom",
        retry_after=None,
        attempt_no=1,
    )


@pytest.mark.unit
def test_failure_rows_are_built_without_writing_and_written_once():
    repository = _repository()

    built = [repository.build_failure_row(_failure(game_id)) for game_id in (1, 2, 3)]
    repository.writer.write_dataframe.assert_not_called()
    repository.write_failure_rows([row for row, _batch_id in built])

    repository.writer.write_dataframe.assert_called_once()
    frame = repository.writer.write_dataframe.call_args.args[0]
    assert frame["game_id"].tolist() == [1, 2, 3]
    assert repository.writer.write_dataframe.call_args.kwargs["table"] == (
        "whoscored_match_ingest_manifest"
    )


@pytest.mark.unit
def test_record_failure_keeps_single_row_contract():
    repository = _repository()

    assert repository.record_failure(_failure(1)) is None

    repository.writer.write_dataframe.assert_called_once()


# --- stale stage janitor --------------------------------------------------


@pytest.mark.unit
def test_stale_stage_tables_older_than_cutoff_are_dropped():
    repository = _repository()
    now = datetime.now(timezone.utc)
    snapshots = {
        "whoscored_events__stg_old": now - timedelta(hours=7),
        "whoscored_lineups__stg_new": now - timedelta(hours=1),
        "whoscored_formations__stg_naive": (now - timedelta(hours=30)).replace(
            tzinfo=None
        ),
        "whoscored_matches__stg_empty": None,
    }

    def execute_query(sql):
        if "information_schema.tables" in sql:
            return [(name,) for name in snapshots]
        for name, committed_at in snapshots.items():
            if f'"{name}$snapshots"' in sql:
                return [(committed_at,)]
        raise AssertionError(sql)

    repository.trino.execute_query.side_effect = execute_query

    dropped = repository.drop_stale_stage_tables(max_age_hours=6)

    assert dropped == [
        "whoscored_events__stg_old",
        "whoscored_formations__stg_naive",
    ]
    assert [call.args for call in repository.trino.drop_table.call_args_list] == [
        ("bronze", "whoscored_events__stg_old"),
        ("bronze", "whoscored_formations__stg_naive"),
    ]
    listing = repository.trino.execute_query.call_args_list[0].args[0]
    assert "table_schema = 'bronze'" in listing
    assert r"LIKE 'whoscored\_%\_\_stg\_%' ESCAPE '\'" in listing
