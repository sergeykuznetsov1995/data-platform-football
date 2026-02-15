"""
Tests for IcebergWriter.

Tests the new Trino-based IcebergWriter that writes data directly
to Iceberg tables via Trino INSERT statements.
"""

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import MagicMock, patch


class TestIcebergWriterInit:
    """Tests for IcebergWriter initialization."""

    def test_init_default_values(self):
        """Test IcebergWriter default initialization values."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()
            assert writer.trino_host == 'trino'
            assert writer.trino_port == 8080
            assert writer.catalog == 'iceberg'

    def test_init_custom_values(self):
        """Test IcebergWriter custom initialization values."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter(
                trino_host='custom-trino',
                trino_port=9090,
                catalog='custom_catalog',
            )
            assert writer.trino_host == 'custom-trino'
            assert writer.trino_port == 9090
            assert writer.catalog == 'custom_catalog'

    def test_init_from_environment(self):
        """Test IcebergWriter reads from environment variables."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            with patch.dict('os.environ', {'TRINO_HOST': 'env-trino', 'TRINO_PORT': '8888'}):
                from scrapers.base.iceberg_writer import IcebergWriter
                writer = IcebergWriter()
                assert writer.trino_host == 'env-trino'
                assert writer.trino_port == 8888


class TestIcebergWriterMetadata:
    """Tests for metadata column handling."""

    def test_add_metadata_columns(self):
        """Test adding metadata columns to DataFrame."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            df = pd.DataFrame({'col1': [1, 2, 3]})
            result = writer._add_metadata_columns(df, source='test_source')

            assert '_source' in result.columns
            assert '_ingested_at' in result.columns
            assert '_batch_id' in result.columns
            assert result['_source'].iloc[0] == 'test_source'
            assert isinstance(result['_ingested_at'].iloc[0], datetime)

    def test_add_metadata_columns_custom_batch_id(self):
        """Test adding metadata columns with custom batch_id."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            df = pd.DataFrame({'col1': [1, 2]})
            result = writer._add_metadata_columns(df, source='test', batch_id='custom-batch-123')

            assert result['_batch_id'].iloc[0] == 'custom-batch-123'


class TestIcebergWriterTrinoManager:
    """Tests for TrinoTableManager integration."""

    def test_get_trino_manager_creates_instance(self):
        """Test _get_trino_manager creates TrinoTableManager."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            # Mock the TrinoTableManager import inside _get_trino_manager
            with patch('scrapers.base.trino_manager.TrinoTableManager') as MockTrino:
                mock_manager = MagicMock()
                MockTrino.return_value = mock_manager

                result = writer._get_trino_manager()
                assert result is not None

    def test_namespace_exists_delegates_to_trino(self):
        """Test namespace_exists delegates to TrinoTableManager."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.schema_exists.return_value = True
            writer._trino_manager = mock_trino

            result = writer.namespace_exists('bronze')

            assert result is True
            mock_trino.schema_exists.assert_called_once_with('bronze')

    def test_table_exists_delegates_to_trino(self):
        """Test table_exists delegates to TrinoTableManager."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = True
            writer._trino_manager = mock_trino

            result = writer.table_exists('bronze', 'test_table')

            assert result is True
            mock_trino.table_exists.assert_called_once_with('bronze', 'test_table')


class TestIcebergWriterWriteDataFrame:
    """Tests for write_dataframe operation."""

    def test_write_dataframe_empty(self):
        """Test writing empty DataFrame returns table name without writing."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            df = pd.DataFrame()
            result = writer.write_dataframe(df, 'bronze', 'test_table')

            assert result == 'iceberg.bronze.test_table'

    def test_write_dataframe_calls_write_to_iceberg(self):
        """Test write_dataframe calls _write_to_iceberg."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            df = pd.DataFrame({'col1': [1, 2, 3]})

            with patch.object(writer, '_write_to_iceberg', return_value='iceberg.bronze.test') as mock_write:
                writer.write_dataframe(df, 'bronze', 'test_table')
                mock_write.assert_called_once()

    def test_write_dataframe_adds_metadata(self):
        """Test write_dataframe adds metadata columns when requested."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            df = pd.DataFrame({'col1': [1, 2]})

            with patch.object(writer, '_write_to_iceberg') as mock_write:
                writer.write_dataframe(df, 'bronze', 'test', add_metadata=True, source='my_source')

                # Check that the DataFrame passed to _write_to_iceberg has metadata
                call_args = mock_write.call_args
                written_df = call_args[0][0]
                assert '_source' in written_df.columns
                assert '_ingested_at' in written_df.columns
                assert '_batch_id' in written_df.columns

    def test_write_dataframe_skips_metadata_when_disabled(self):
        """Test write_dataframe skips metadata when add_metadata=False."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            df = pd.DataFrame({'col1': [1, 2]})

            with patch.object(writer, '_write_to_iceberg') as mock_write:
                writer.write_dataframe(df, 'bronze', 'test', add_metadata=False)

                call_args = mock_write.call_args
                written_df = call_args[0][0]
                assert '_source' not in written_df.columns


class TestIcebergWriterWriteToIceberg:
    """Tests for _write_to_iceberg internal method."""

    def test_write_to_iceberg_creates_table(self):
        """Test _write_to_iceberg creates table if not exists."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = False
            mock_trino.arrow_schema_to_trino.return_value = {'col1': 'BIGINT'}
            mock_trino.insert_dataframe.return_value = 3
            writer._trino_manager = mock_trino

            df = pd.DataFrame({'col1': [1, 2, 3]})
            result = writer._write_to_iceberg(df, 'bronze', 'test_table', None)

            mock_trino.create_iceberg_table.assert_called_once()
            mock_trino.insert_dataframe.assert_called_once()
            assert result == 'iceberg.bronze.test_table'

    def test_write_to_iceberg_skips_table_creation_if_exists(self):
        """Test _write_to_iceberg skips table creation if exists."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = True
            mock_trino.insert_dataframe.return_value = 3
            writer._trino_manager = mock_trino

            df = pd.DataFrame({'col1': [1, 2, 3]})
            writer._write_to_iceberg(df, 'bronze', 'test_table', None)

            mock_trino.create_iceberg_table.assert_not_called()
            mock_trino.insert_dataframe.assert_called_once()

    def test_write_to_iceberg_overwrite_mode(self):
        """Test _write_to_iceberg deletes data in overwrite mode."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = True
            mock_trino.insert_dataframe.return_value = 2
            writer._trino_manager = mock_trino

            df = pd.DataFrame({'col1': [1, 2]})
            writer._write_to_iceberg(df, 'bronze', 'test', None, mode='overwrite')

            # Should call DELETE before INSERT
            mock_trino._execute.assert_called()
            delete_call = mock_trino._execute.call_args[0][0]
            assert 'DELETE FROM' in delete_call

    def test_write_to_iceberg_with_partitions(self):
        """Test _write_to_iceberg creates table with partitions."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = False
            mock_trino.arrow_schema_to_trino.return_value = {
                'col1': 'BIGINT',
                'league': 'VARCHAR',
            }
            mock_trino.insert_dataframe.return_value = 1
            writer._trino_manager = mock_trino

            df = pd.DataFrame({'col1': [1], 'league': ['EPL']})
            partition_spec = [('league', 'identity')]

            writer._write_to_iceberg(df, 'bronze', 'test', partition_spec)

            # Check partition columns were passed
            call_args = mock_trino.create_iceberg_table.call_args
            assert call_args.kwargs.get('partition_columns') == ['league']


class TestIcebergWriterArrowConversion:
    """Tests for Arrow conversion."""

    def test_pandas_to_arrow(self):
        """Test pandas to arrow conversion."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            df = pd.DataFrame({
                'int_col': [1, 2, 3],
                'str_col': ['a', 'b', 'c'],
                'float_col': [1.1, 2.2, 3.3],
            })

            arrow_table = writer._pandas_to_arrow(df)

            assert arrow_table.num_rows == 3
            assert len(arrow_table.schema) == 3

    def test_pandas_to_arrow_handles_timestamps(self):
        """Test pandas to arrow conversion handles timestamps."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            df = pd.DataFrame({
                'ts_col': pd.to_datetime(['2024-01-01', '2024-01-02']),
            })

            arrow_table = writer._pandas_to_arrow(df)

            assert arrow_table.num_rows == 2
            # Should be converted to microseconds
            assert 'timestamp' in str(arrow_table.schema[0].type).lower()


class TestIcebergWriterTableOperations:
    """Tests for table operation methods."""

    def test_create_table_if_not_exists_creates(self):
        """Test create_table_if_not_exists creates table when not exists."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            import pyarrow as pa
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = False
            mock_trino.arrow_schema_to_trino.return_value = {'col1': 'BIGINT'}
            writer._trino_manager = mock_trino

            schema = pa.schema([('col1', pa.int64())])
            writer.create_table_if_not_exists('bronze', 'test', schema)

            mock_trino.create_iceberg_table.assert_called_once()

    def test_create_table_if_not_exists_skips_if_exists(self):
        """Test create_table_if_not_exists skips if table exists."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            import pyarrow as pa
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.table_exists.return_value = True
            writer._trino_manager = mock_trino

            schema = pa.schema([('col1', pa.int64())])
            writer.create_table_if_not_exists('bronze', 'test', schema)

            mock_trino.create_iceberg_table.assert_not_called()

    def test_compact_table(self):
        """Test compact_table calls optimize."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            mock_trino = MagicMock()
            writer._trino_manager = mock_trino

            writer.compact_table('bronze', 'test')

            mock_trino._execute.assert_called_once()
            sql = mock_trino._execute.call_args[0][0]
            assert 'EXECUTE optimize' in sql

    def test_expire_snapshots(self):
        """Test expire_snapshots calls expire_snapshots."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            mock_trino = MagicMock()
            writer._trino_manager = mock_trino

            writer.expire_snapshots('bronze', 'test', retention_days=14)

            mock_trino._execute.assert_called_once()
            sql = mock_trino._execute.call_args[0][0]
            assert 'expire_snapshots' in sql
            assert '14d' in sql


class TestIcebergWriterReadOperations:
    """Tests for read operation methods."""

    def test_read_table(self):
        """Test read_table executes SELECT query."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.execute_query.return_value = [(1, 'a'), (2, 'b')]
            mock_cursor = MagicMock()
            mock_cursor.description = [('col1',), ('col2',)]
            mock_trino.connection.cursor.return_value = mock_cursor
            writer._trino_manager = mock_trino

            result = writer.read_table('bronze', 'test')

            assert mock_trino.execute_query.called
            sql = mock_trino.execute_query.call_args[0][0]
            assert 'SELECT * FROM iceberg.bronze.test' in sql

    def test_read_table_with_columns(self):
        """Test read_table with specific columns."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.execute_query.return_value = []
            writer._trino_manager = mock_trino

            writer.read_table('bronze', 'test', columns=['col1', 'col2'])

            sql = mock_trino.execute_query.call_args[0][0]
            assert '"col1", "col2"' in sql

    def test_read_table_with_filter(self):
        """Test read_table with filter expression."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.execute_query.return_value = []
            writer._trino_manager = mock_trino

            writer.read_table('bronze', 'test', filter_expr="rating > 1500")

            sql = mock_trino.execute_query.call_args[0][0]
            assert 'WHERE rating > 1500' in sql

    def test_get_table_history(self):
        """Test get_table_history queries snapshots table."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.execute_query.return_value = []
            writer._trino_manager = mock_trino

            writer.get_table_history('bronze', 'test')

            sql = mock_trino.execute_query.call_args[0][0]
            assert '$snapshots' in sql

    def test_read_snapshot(self):
        """Test read_snapshot uses VERSION AS OF."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.iceberg_writer import IcebergWriter
            writer = IcebergWriter()

            mock_trino = MagicMock()
            mock_trino.execute_query.return_value = []
            writer._trino_manager = mock_trino

            writer.read_snapshot('bronze', 'test', snapshot_id=12345)

            sql = mock_trino.execute_query.call_args[0][0]
            assert 'FOR VERSION AS OF 12345' in sql
