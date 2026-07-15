"""
Tests for IcebergWriter.

Tests the new Trino-based IcebergWriter that writes data directly
to Iceberg tables via Trino INSERT statements.
"""

import pytest
import pandas as pd
import pyarrow as pa
import yaml
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[3]


class TestIcebergWriterInit:
    """Tests for IcebergWriter initialization."""

    def test_init_default_values(self):
        """Test IcebergWriter default initialization values."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            with patch.dict("os.environ", {}, clear=False):
                from scrapers.base.iceberg_writer import IcebergWriter

                writer = IcebergWriter()
                assert writer.trino_host == "trino"
                assert (
                    writer.trino_port is None
                )  # TrinoTableManager decides port based on auth mode
                assert writer.catalog == "iceberg"

    def test_init_custom_values(self):
        """Test IcebergWriter custom initialization values."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter(
                trino_host="custom-trino",
                trino_port=9090,
                catalog="custom_catalog",
            )
            assert writer.trino_host == "custom-trino"
            assert writer.trino_port == 9090
            assert writer.catalog == "custom_catalog"

    def test_init_from_environment(self):
        """Test IcebergWriter reads from environment variables."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            with patch.dict(
                "os.environ", {"TRINO_HOST": "env-trino", "TRINO_PORT": "8888"}
            ):
                from scrapers.base.iceberg_writer import IcebergWriter

                writer = IcebergWriter()
                assert writer.trino_host == "env-trino"
                assert (
                    writer.trino_port is None
                )  # TRINO_PORT resolved by TrinoTableManager, not IcebergWriter


class TestIcebergWriterMetadata:
    """Tests for metadata column handling."""

    def test_add_metadata_columns(self):
        """Test adding metadata columns to DataFrame."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            df = pd.DataFrame({"col1": [1, 2, 3]})
            result = writer._add_metadata_columns(df, source="test_source")

            assert "_source" in result.columns
            assert "_ingested_at" in result.columns
            assert "_batch_id" in result.columns
            assert result["_source"].iloc[0] == "test_source"
            assert isinstance(result["_ingested_at"].iloc[0], datetime)

    def test_add_metadata_columns_custom_batch_id(self):
        """Test adding metadata columns with custom batch_id."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            df = pd.DataFrame({"col1": [1, 2]})
            result = writer._add_metadata_columns(
                df, source="test", batch_id="custom-batch-123"
            )

            assert result["_batch_id"].iloc[0] == "custom-batch-123"


class TestIcebergWriterTrinoManager:
    """Tests for TrinoTableManager integration."""

    def test_get_trino_manager_creates_instance(self):
        """Test _get_trino_manager creates TrinoTableManager."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            # Mock the TrinoTableManager import inside _get_trino_manager
            with patch("scrapers.base.trino_manager.TrinoTableManager") as MockTrino:
                mock_manager = MagicMock()
                MockTrino.return_value = mock_manager

                result = writer._get_trino_manager()
                assert result is not None

    def test_namespace_exists_delegates_to_trino(self):
        """Test namespace_exists delegates to TrinoTableManager."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.schema_exists.return_value = True
            writer._trino_manager = mock_trino

            result = writer.namespace_exists("bronze")

            assert result is True
            mock_trino.schema_exists.assert_called_once_with("bronze")

    def test_table_exists_delegates_to_trino(self):
        """Test table_exists delegates to TrinoTableManager."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = True
            writer._trino_manager = mock_trino

            result = writer.table_exists("bronze", "test_table")

            assert result is True
            mock_trino.table_exists.assert_called_once_with("bronze", "test_table")


class TestIcebergWriterWriteDataFrame:
    """Tests for write_dataframe operation."""

    def test_write_dataframe_empty(self):
        """Test writing empty DataFrame returns table name without writing."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            df = pd.DataFrame()
            result = writer.write_dataframe(df, "bronze", "test_table")

            assert result == "iceberg.bronze.test_table"

    def test_write_dataframe_calls_write_to_iceberg(self):
        """Test write_dataframe calls _write_to_iceberg."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            df = pd.DataFrame({"col1": [1, 2, 3]})

            with patch.object(
                writer, "_write_to_iceberg", return_value="iceberg.bronze.test"
            ) as mock_write:
                writer.write_dataframe(df, "bronze", "test_table")
                mock_write.assert_called_once()

    def test_write_dataframe_adds_metadata(self):
        """Test write_dataframe adds metadata columns when requested."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            df = pd.DataFrame({"col1": [1, 2]})

            with patch.object(writer, "_write_to_iceberg") as mock_write:
                writer.write_dataframe(
                    df, "bronze", "test", add_metadata=True, source="my_source"
                )

                # Check that the DataFrame passed to _write_to_iceberg has metadata
                call_args = mock_write.call_args
                written_df = call_args[0][0]
                assert "_source" in written_df.columns
                assert "_ingested_at" in written_df.columns
                assert "_batch_id" in written_df.columns

    def test_write_dataframe_skips_metadata_when_disabled(self):
        """Test write_dataframe skips metadata when add_metadata=False."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            df = pd.DataFrame({"col1": [1, 2]})

            with patch.object(writer, "_write_to_iceberg") as mock_write:
                writer.write_dataframe(df, "bronze", "test", add_metadata=False)

                call_args = mock_write.call_args
                written_df = call_args[0][0]
                assert "_source" not in written_df.columns

    def test_real_public_writer_interface_accepts_and_forwards_bulk_arrow(self):
        """The deployed writer must match the WhoScored repository call site."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()
            frame = pd.DataFrame({"entity_key": ["one"]})

            with patch.object(
                writer,
                "_write_to_iceberg",
                return_value="iceberg.bronze.whoscored_player_stage_stats",
            ) as write:
                result = writer.write_dataframe(
                    frame,
                    database="bronze",
                    table="whoscored_player_stage_stats",
                    add_metadata=False,
                    source="whoscored",
                    bulk_arrow=True,
                )

            assert result == "iceberg.bronze.whoscored_player_stage_stats"
            assert write.call_args.kwargs["bulk_arrow"] is True
            assert write.call_args.kwargs["mode"] == "append"
            assert write.call_args.kwargs["delete_filter"] is None
            assert write.call_args.kwargs["merge_keys"] is None


class TestIcebergWriterWriteToIceberg:
    """Tests for _write_to_iceberg internal method."""

    def test_write_to_iceberg_creates_table(self):
        """Test _write_to_iceberg creates table if not exists."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = False
            mock_trino.arrow_schema_to_trino.return_value = {"col1": "BIGINT"}
            mock_trino.insert_dataframe_atomic.return_value = 3
            writer._trino_manager = mock_trino

            df = pd.DataFrame({"col1": [1, 2, 3]})
            result = writer._write_to_iceberg(df, "bronze", "test_table", None)

            mock_trino.create_iceberg_table.assert_called_once()
            mock_trino.insert_dataframe_atomic.assert_called_once()
            assert result == "iceberg.bronze.test_table"

    def test_write_to_iceberg_skips_table_creation_if_exists(self):
        """Test _write_to_iceberg skips table creation if exists."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = True
            mock_trino.insert_dataframe_atomic.return_value = 3
            writer._trino_manager = mock_trino

            df = pd.DataFrame({"col1": [1, 2, 3]})
            writer._write_to_iceberg(df, "bronze", "test_table", None)

            mock_trino.create_iceberg_table.assert_not_called()
            mock_trino.insert_dataframe_atomic.assert_called_once()

    def test_write_to_iceberg_overwrite_mode(self):
        """Full overwrite is staged and never performs an eager delete."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = True
            mock_trino.insert_dataframe_atomic.return_value = 2
            writer._trino_manager = mock_trino

            df = pd.DataFrame({"col1": [1, 2]})
            writer._write_to_iceberg(df, "bronze", "test", None, mode="overwrite")

            mock_trino._execute.assert_not_called()
            mock_trino.insert_dataframe_atomic.assert_called_once()
            assert (
                mock_trino.insert_dataframe_atomic.call_args.kwargs["delete_filter"]
                == "TRUE"
            )

    def test_write_to_iceberg_overwrite_failure_is_not_downgraded(self):
        """A failed staged replacement propagates instead of appending."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()
            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = True
            mock_trino.insert_dataframe_atomic.side_effect = RuntimeError("swap failed")
            writer._trino_manager = mock_trino

            df = pd.DataFrame({"col1": [1, 2]})
            with pytest.raises(RuntimeError, match="swap failed"):
                writer._write_to_iceberg(df, "bronze", "test", None, mode="overwrite")

            mock_trino._execute.assert_not_called()

    def test_write_to_iceberg_with_partitions(self):
        """Test _write_to_iceberg creates table with partitions."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = False
            mock_trino.arrow_schema_to_trino.return_value = {
                "col1": "BIGINT",
                "league": "VARCHAR",
            }
            mock_trino.insert_dataframe_atomic.return_value = 1
            writer._trino_manager = mock_trino

            df = pd.DataFrame({"col1": [1], "league": ["EPL"]})
            partition_spec = [("league", "identity")]

            writer._write_to_iceberg(df, "bronze", "test", partition_spec)

            # Check partition columns were passed
            call_args = mock_trino.create_iceberg_table.call_args
            assert call_args.kwargs.get("partition_columns") == ["league"]

    def test_write_to_iceberg_routes_delete_filter_into_atomic(self):
        """replace_partitions (#314): the writer hands delete_filter to
        insert_dataframe_atomic instead of issuing its own DELETE — so the swap
        is atomic (stage-first) and a failed INSERT can't leave the table empty."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = True
            mock_trino.insert_dataframe_atomic.return_value = 2
            writer._trino_manager = mock_trino

            df = pd.DataFrame({"col1": [1, 2], "league": ["EPL", "EPL"]})
            writer._write_to_iceberg(
                df,
                "bronze",
                "test",
                None,
                delete_filter="league = 'EPL'",
            )

            # delete_filter is forwarded to the atomic writer...
            assert (
                mock_trino.insert_dataframe_atomic.call_args.kwargs.get("delete_filter")
                == "league = 'EPL'"
            )
            # ...and the writer no longer issues a standalone partition DELETE.
            delete_calls = [
                c for c in mock_trino._execute.call_args_list if "DELETE FROM" in str(c)
            ]
            assert delete_calls == []

    def test_write_to_iceberg_routes_bounded_bulk_frame_to_pyiceberg(self):
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()
            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = True
            writer._trino_manager = mock_trino
            frame = pd.DataFrame({"col1": [1, 2, 3]})

            with patch.object(
                writer, "_append_dataframe_pyiceberg", return_value=3
            ) as append:
                writer._write_to_iceberg(frame, "bronze", "test", None, bulk_arrow=True)

            append.assert_called_once()
            assert append.call_args.kwargs == {
                "database": "bronze",
                "table": "test",
            }
            mock_trino.insert_dataframe_atomic.assert_not_called()

    def test_bulk_arrow_rejects_delete_or_merge_semantics(self):
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()
            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = True
            writer._trino_manager = mock_trino

            with pytest.raises(ValueError, match="does not support replace or merge"):
                writer._write_to_iceberg(
                    pd.DataFrame({"col1": [1]}),
                    "bronze",
                    "test",
                    None,
                    delete_filter="col1 = 1",
                    bulk_arrow=True,
                )

    def test_bulk_arrow_uses_rendered_seaweed_endpoint_and_rest_warehouse(self):
        from scrapers.base.iceberg_writer import (
            IcebergWriter,
            _pyiceberg_s3_properties,
        )

        compose = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
        compose_env = compose["x-airflow-common"]["environment"]
        runtime_env = {
            "S3_ENDPOINT": compose_env["S3_ENDPOINT"],
            "S3_SCHEME": compose_env["S3_SCHEME"],
            "S3_ACCESS_KEY": "test-access",
            "S3_SECRET_KEY": "test-secret",
            "ICEBERG_REST_WAREHOUSE": "football",
        }
        properties = _pyiceberg_s3_properties(runtime_env)

        assert properties["s3.endpoint"] == "http://seaweedfs:8333"
        assert compose_env["ICEBERG_REST_WAREHOUSE"] == (
            "${ICEBERG_REST_WAREHOUSE:-football}"
        )

        writer = IcebergWriter()
        target = MagicMock()
        catalog = MagicMock()
        catalog.load_table.return_value = target
        file_io = MagicMock()
        schema = pa.schema([pa.field("col1", pa.int64())])
        with (
            patch.dict("os.environ", runtime_env, clear=False),
            patch("pyiceberg.catalog.load_catalog", return_value=catalog) as load,
            patch("pyiceberg.io.pyarrow.PyArrowFileIO", return_value=file_io) as io,
            patch("pyiceberg.io.pyarrow.schema_to_pyarrow", return_value=schema),
        ):
            inserted = writer._append_dataframe_pyiceberg(
                pa.table({"col1": [1, 2]}),
                database="bronze",
                table="whoscored_player_stage_stats",
            )

        assert inserted == 2
        assert load.call_args.kwargs["warehouse"] == "football"
        assert io.call_args.args[0]["s3.endpoint"] == "http://seaweedfs:8333"
        assert target.io is file_io
        target.append.assert_called_once()


class TestIcebergWriterArrowConversion:
    """Tests for Arrow conversion."""

    def test_pandas_to_arrow(self):
        """Test pandas to arrow conversion."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            df = pd.DataFrame(
                {
                    "int_col": [1, 2, 3],
                    "str_col": ["a", "b", "c"],
                    "float_col": [1.1, 2.2, 3.3],
                }
            )

            arrow_table = writer._pandas_to_arrow(df)

            assert arrow_table.num_rows == 3
            assert len(arrow_table.schema) == 3

    def test_pandas_to_arrow_handles_timestamps(self):
        """Test pandas to arrow conversion handles timestamps."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            df = pd.DataFrame(
                {
                    "ts_col": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                }
            )

            arrow_table = writer._pandas_to_arrow(df)

            assert arrow_table.num_rows == 2
            # Should be converted to microseconds
            assert "timestamp" in str(arrow_table.schema[0].type).lower()

    def test_identity_partition_contract_rejects_unpartitioned_table(self, monkeypatch):
        from types import SimpleNamespace

        from scrapers.base.iceberg_writer import IcebergWriter

        target = MagicMock()
        target.spec.return_value = SimpleNamespace(fields=[])
        writer = IcebergWriter()
        monkeypatch.setattr(writer, "_load_pyiceberg_table", lambda **_kwargs: target)

        with pytest.raises(RuntimeError, match="must be partitioned only"):
            writer.require_exact_identity_partition(
                database="bronze",
                table="dq_keys",
                partition_column="population_sha256",
            )

    def test_batched_partition_replace_uses_one_metadata_transaction(self, monkeypatch):
        from types import SimpleNamespace

        from scrapers.base.iceberg_writer import IcebergWriter

        calls = []

        class Transaction:
            def __enter__(self):
                calls.append("enter")
                return self

            def __exit__(self, exc_type, _exc, _tb):
                calls.append(("exit", exc_type))

            def delete(self, expression):
                calls.append(("delete", str(expression)))

            def append(self, table, **_kwargs):
                calls.append(("append", table.num_rows))

        target = MagicMock()
        target.spec.return_value = SimpleNamespace(
            fields=[SimpleNamespace(name="population_sha256", transform="identity")]
        )
        target.transaction.return_value = Transaction()
        writer = IcebergWriter()
        monkeypatch.setattr(writer, "_load_pyiceberg_table", lambda **_kwargs: target)
        monkeypatch.setattr(
            writer,
            "_align_pyiceberg_arrow_table",
            lambda table, **_kwargs: table,
        )
        value = "a" * 64
        batches = (
            pa.table({"population_sha256": [value], "value": [1]}),
            pa.table({"population_sha256": [value, value], "value": [2, 3]}),
        )

        rows = writer.replace_identity_partition_arrow_batches(
            iter(batches),
            database="bronze",
            table="dq_keys",
            partition_column="population_sha256",
            partition_value=value,
        )

        assert rows == 3
        target.transaction.assert_called_once_with()
        assert calls[0] == "enter"
        assert [
            call for call in calls if isinstance(call, tuple) and call[0] == "append"
        ] == [
            ("append", 1),
            ("append", 2),
        ]
        assert calls[-1] == ("exit", None)

    def test_current_snapshot_id_uses_main_ref_not_timestamp_order(self, monkeypatch):
        from types import SimpleNamespace

        from scrapers.base.iceberg_writer import IcebergWriter

        target = MagicMock()
        # These simulate same-commit timestamps with random numeric IDs; the
        # main ref deliberately points at neither the largest nor first ID.
        target.snapshots.return_value = [
            SimpleNamespace(snapshot_id=999, timestamp_ms=123),
            SimpleNamespace(snapshot_id=17, timestamp_ms=123),
            SimpleNamespace(snapshot_id=500, timestamp_ms=123),
        ]
        target.current_snapshot.return_value = SimpleNamespace(snapshot_id=17)
        writer = IcebergWriter()
        monkeypatch.setattr(writer, "_load_pyiceberg_table", lambda **_kwargs: target)

        assert writer.current_snapshot_id(database="bronze", table="dq_keys") == 17
        target.current_snapshot.assert_called_once_with()
        target.snapshots.assert_not_called()


class TestIcebergWriterTableOperations:
    """Tests for table operation methods."""

    def test_create_table_if_not_exists_creates(self):
        """Test create_table_if_not_exists creates table when not exists."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            import pyarrow as pa
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = False
            mock_trino.arrow_schema_to_trino.return_value = {"col1": "BIGINT"}
            writer._trino_manager = mock_trino

            schema = pa.schema([("col1", pa.int64())])
            writer.create_table_if_not_exists("bronze", "test", schema)

            mock_trino.create_iceberg_table.assert_called_once()

    def test_create_table_if_not_exists_skips_if_exists(self):
        """Test create_table_if_not_exists skips if table exists."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            import pyarrow as pa
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = True
            writer._trino_manager = mock_trino

            schema = pa.schema([("col1", pa.int64())])
            writer.create_table_if_not_exists("bronze", "test", schema)

            mock_trino.create_iceberg_table.assert_not_called()

    def test_expire_snapshots(self):
        """Test expire_snapshots calls expire_snapshots."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            mock_trino = MagicMock()
            writer._trino_manager = mock_trino

            writer.expire_snapshots("bronze", "test", retention_days=14)

            mock_trino._execute.assert_called_once()
            sql = mock_trino._execute.call_args[0][0]
            assert "expire_snapshots" in sql
            assert "14d" in sql


class TestIcebergWriterMetadataRecovery:
    """Metadata corruption must never trigger destructive auto-recovery."""

    def test_evolve_schema_fails_closed_without_drop(self):
        """Invalid metadata stops the write and preserves the existing table."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import (
                IcebergMetadataCorruptionError,
                IcebergWriter,
            )
            from scrapers.base.trino_manager import TrinoError

            writer = IcebergWriter()

            # Simulate: table_exists=True, but get_table_columns fails with ICEBERG_INVALID_METADATA
            cause = Exception("Error accessing metadata file")
            cause.error_name = "ICEBERG_INVALID_METADATA"
            metadata_error = TrinoError("SQL execution failed")
            metadata_error.__cause__ = cause

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = True
            mock_trino.get_table_columns.side_effect = metadata_error
            mock_trino.arrow_schema_to_trino.return_value = {"col1": "BIGINT"}
            mock_trino.insert_dataframe_atomic.return_value = 3
            writer._trino_manager = mock_trino

            df = pd.DataFrame({"col1": [1, 2, 3]})

            with pytest.raises(
                IcebergMetadataCorruptionError, match="automatic DROP/CREATE"
            ):
                writer._write_to_iceberg(df, "bronze", "test_table", None)

            mock_trino.drop_table.assert_not_called()
            mock_trino.create_iceberg_table.assert_not_called()
            mock_trino.insert_dataframe_atomic.assert_not_called()

    def test_reraises_non_metadata_errors(self):
        """Regular TrinoError in _evolve_schema → drop NOT called, error re-raised."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter
            from scrapers.base.trino_manager import TrinoError

            writer = IcebergWriter()

            regular_error = TrinoError("Connection refused")

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = True
            mock_trino.get_table_columns.side_effect = regular_error
            mock_trino.arrow_schema_to_trino.return_value = {"col1": "BIGINT"}
            writer._trino_manager = mock_trino

            df = pd.DataFrame({"col1": [1, 2, 3]})

            with pytest.raises(TrinoError, match="Connection refused"):
                writer._write_to_iceberg(df, "bronze", "test_table", None)

            mock_trino.drop_table.assert_not_called()

    def test_fail_closed_error_names_table_and_restore_action(self):
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import (
                IcebergMetadataCorruptionError,
                IcebergWriter,
            )

            writer = IcebergWriter()
            with pytest.raises(IcebergMetadataCorruptionError) as exc_info:
                writer._recover_corrupted_table(
                    MagicMock(),
                    "bronze",
                    "whoscored_events",
                    {},
                    None,
                )

            message = str(exc_info.value)
            assert "iceberg.bronze.whoscored_events" in message
            assert "verified backup" in message


class TestIcebergWriterReadOperations:
    """Tests for read operation methods."""

    def test_read_table(self):
        """Test read_table executes SELECT query."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.execute_query.return_value = [(1, "a"), (2, "b")]
            mock_cursor = MagicMock()
            mock_cursor.description = [("col1",), ("col2",)]
            mock_trino.connection.cursor.return_value = mock_cursor
            writer._trino_manager = mock_trino

            writer.read_table("bronze", "test")

            assert mock_trino.execute_query.called
            sql = mock_trino.execute_query.call_args[0][0]
            assert "SELECT * FROM iceberg.bronze.test" in sql

    def test_read_table_with_columns(self):
        """Test read_table with specific columns."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.execute_query.return_value = []
            writer._trino_manager = mock_trino

            writer.read_table("bronze", "test", columns=["col1", "col2"])

            sql = mock_trino.execute_query.call_args[0][0]
            assert '"col1", "col2"' in sql

    def test_read_table_with_filter(self):
        """Test read_table with filter expression."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.execute_query.return_value = []
            writer._trino_manager = mock_trino

            writer.read_table("bronze", "test", filter_expr="rating > 1500")

            sql = mock_trino.execute_query.call_args[0][0]
            assert "WHERE rating > 1500" in sql

    def test_get_table_history(self):
        """Test get_table_history queries snapshots table."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.execute_query.return_value = []
            writer._trino_manager = mock_trino

            writer.get_table_history("bronze", "test")

            sql = mock_trino.execute_query.call_args[0][0]
            assert "$snapshots" in sql

    def test_read_snapshot(self):
        """Test read_snapshot uses VERSION AS OF."""
        with patch.dict(
            "sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}
        ):
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.execute_query.return_value = []
            writer._trino_manager = mock_trino

            writer.read_snapshot("bronze", "test", snapshot_id=12345)

            sql = mock_trino.execute_query.call_args[0][0]
            assert "FOR VERSION AS OF 12345" in sql
