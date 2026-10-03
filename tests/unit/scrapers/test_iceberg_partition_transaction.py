"""Real PyIceberg writes against a temporary SQLite catalog and local Parquet."""

from datetime import date
from unittest.mock import patch

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.snapshots import Operation
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, LongType, NestedField, StringType

from scrapers.base.iceberg_writer import IcebergWriter


@pytest.fixture(params=["rating_date_string", "rating_date_date", "population_sha256"])
def partition_table(request, tmp_path, monkeypatch):
    """Cover ClubElo's varchar/date alignment and WhoScored frozen DQ keys."""
    is_hash = request.param == "population_sha256"
    is_date = request.param == "rating_date_date"
    column = "population_sha256" if is_hash else "rating_date"
    value, neighbour = ("a" * 64, "b" * 64) if is_hash else ("2026-10-02", "2026-10-01")
    catalog = SqlCatalog(
        "partition_regression",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=(tmp_path / "warehouse").as_uri(),
    )
    catalog.create_namespace("bronze")
    target = catalog.create_table(
        "bronze.partition_regression",
        schema=Schema(
            NestedField(1, column, DateType() if is_date else StringType()),
            NestedField(2, "value", LongType()),
        ),
        partition_spec=PartitionSpec(
            PartitionField(1, 1000, IdentityTransform(), column)
        ),
    )
    stored_values = (
        [date.fromisoformat(v) for v in (value, neighbour)]
        if is_date
        else [value, neighbour]
    )
    target.append(pa.table({column: stored_values, "value": [1, 9]}))
    writer = IcebergWriter()
    monkeypatch.setattr(writer, "_load_pyiceberg_table", lambda **_kwargs: target)

    def load():
        # A fresh table object must resolve committed metadata, not staged updates.
        return catalog.load_table(target.name())

    def rows():
        return sorted(
            (str(row[column]), row["value"])
            for row in load().scan().to_arrow().to_pylist()
        )

    def batch(*values):
        return pa.table({column: [value] * len(values), "value": list(values)})

    def replace(batches):
        return writer.replace_identity_partition_arrow_batches(
            batches,
            database="bronze",
            table="partition_regression",
            partition_column=column,
            partition_value=value,
        )

    yield catalog, target, column, value, neighbour, load, rows, batch, replace
    catalog.engine.dispose()


def test_replace_publishes_complete_partition_in_one_catalog_commit(partition_table):
    catalog, target, column, value, neighbour, load, rows, batch, replace = (
        partition_table
    )
    old_rows = rows()
    old_metadata = load().metadata_location
    old_snapshot = target.current_snapshot().snapshot_id
    original_commit = catalog.commit_table
    observed = []

    def still_old(stage):
        assert load().metadata_location == old_metadata
        assert rows() == old_rows
        observed.append(stage)

    def commit(*args, **kwargs):
        still_old("before commit")
        return original_commit(*args, **kwargs)

    def batches():
        still_old("after delete")
        yield batch(2)
        still_old("after first append")
        yield batch(3, 4)
        still_old("after second append")

    with patch.object(catalog, "commit_table", side_effect=commit) as publish:
        assert replace(batches()) == 3
    assert publish.call_count == 1
    assert observed == [
        "after delete",
        "after first append",
        "after second append",
        "before commit",
    ]
    assert rows() == sorted([(value, 2), (value, 3), (value, 4), (neighbour, 9)])
    committed = load()
    assert committed.metadata_location != old_metadata
    assert committed.current_snapshot().snapshot_id != old_snapshot
    # The old committed snapshot remains readable after replacement.
    assert (
        sorted(
            (str(row[column]), row["value"])
            for row in committed.scan(snapshot_id=old_snapshot).to_arrow().to_pylist()
        )
        == old_rows
    )
    appends = [
        s
        for s in committed.snapshots()
        if s.snapshot_id != old_snapshot and s.summary.operation == Operation.APPEND
    ]
    assert len(appends) == 2
    for snapshot in appends:
        summary = snapshot.summary.model_dump()
        assert summary["operation"] == "append"
        assert summary["dpf.operation"] == "replace-identity-partition-batch"
        assert summary["partition-column"] == column
        assert summary["partition-value"] == value


@pytest.mark.parametrize(
    "failure", ["producer", "append", "commit", "wrong_partition", "empty"]
)
def test_failed_replace_keeps_committed_metadata_and_all_partitions(
    partition_table, failure
):
    catalog, target, column, _value, neighbour, load, rows, batch, replace = (
        partition_table
    )
    old_rows = rows()
    old_metadata = load().metadata_location
    old_snapshots = load().snapshots()
    original_write = target.io.new_output

    def batches():
        if failure == "empty":
            return
        yield batch(2)
        assert rows() == old_rows
        if failure == "producer":
            raise RuntimeError("batch producer failed")
        if failure == "wrong_partition":
            yield pa.table({column: [neighbour], "value": [3]})
        else:
            yield batch(3)

    def write(path):
        # Real first append succeeds; the second append cannot create its file.
        if failure == "append" and str(path).endswith(".parquet"):
            raise OSError("Parquet write failed")
        return original_write(path)

    def batches_with_write_failure():
        yield batch(2)
        assert rows() == old_rows
        with patch.object(target.io, "new_output", side_effect=write):
            yield batch(3)

    error, match = {
        "producer": (RuntimeError, "batch producer failed"),
        "append": (OSError, "Parquet write failed"),
        "commit": (CommitFailedException, "catalog rejected commit"),
        "wrong_partition": (ValueError, "outside the replacement partition"),
        "empty": (ValueError, "has no batches"),
    }[failure]
    commit_effect = (
        CommitFailedException("catalog rejected commit")
        if failure == "commit"
        else catalog.commit_table
    )
    with patch.object(catalog, "commit_table", side_effect=commit_effect) as publish:
        with pytest.raises(error, match=match):
            replace(batches_with_write_failure() if failure == "append" else batches())
    assert publish.call_count == (1 if failure == "commit" else 0)
    assert load().metadata_location == old_metadata
    assert load().snapshots() == old_snapshots
    assert rows() == old_rows
