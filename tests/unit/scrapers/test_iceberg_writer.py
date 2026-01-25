"""
Tests for IcebergWriter.
"""

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import MagicMock, patch, PropertyMock


class TestIcebergWriter:
    """Tests for IcebergWriter."""

    @pytest.fixture
    def mock_catalog(self):
        """Mock Iceberg catalog."""
        with patch('scrapers.base.iceberg_writer.load_catalog') as mock:
            catalog = MagicMock()
            catalog.list_namespaces.return_value = [('bronze',), ('silver',)]
            mock.return_value = catalog
            yield catalog

    @pytest.fixture
    def mock_spark(self):
        """Mock SparkSession."""
        with patch('scrapers.base.iceberg_writer.IcebergWriter._get_spark') as mock:
            spark = MagicMock()
            mock.return_value = spark
            yield spark

    @pytest.fixture
    def writer(self, mock_catalog):
        """Create IcebergWriter instance."""
        from scrapers.base.iceberg_writer import IcebergWriter
        return IcebergWriter()

    def test_init(self, writer):
        """Test IcebergWriter initialization."""
        assert writer.catalog_name == 'iceberg'
        assert 'hive-metastore' in writer.metastore_uri

    def test_add_metadata_columns(self, writer):
        """Test adding metadata columns to DataFrame."""
        df = pd.DataFrame({'col1': [1, 2, 3]})

        result = writer._add_metadata_columns(df, source='test')

        assert '_source' in result.columns
        assert '_ingested_at' in result.columns
        assert '_batch_id' in result.columns
        assert result['_source'].iloc[0] == 'test'

    def test_namespace_exists_true(self, writer, mock_catalog):
        """Test namespace existence check - exists."""
        mock_catalog.list_namespaces.return_value = [('bronze',)]

        with patch.object(writer, '_catalog', mock_catalog):
            result = writer.namespace_exists('bronze')
            assert result is True

    def test_namespace_exists_false(self, writer, mock_catalog):
        """Test namespace existence check - doesn't exist."""
        mock_catalog.list_namespaces.return_value = [('bronze',)]

        with patch.object(writer, '_catalog', mock_catalog):
            result = writer.namespace_exists('gold')
            assert result is False

    def test_table_exists_true(self, writer, mock_catalog):
        """Test table existence check - exists."""
        mock_catalog.load_table.return_value = MagicMock()

        with patch.object(writer, '_catalog', mock_catalog):
            result = writer.table_exists('bronze', 'test_table')
            assert result is True

    def test_table_exists_false(self, writer, mock_catalog):
        """Test table existence check - doesn't exist."""
        from scrapers.base.iceberg_writer import PYICEBERG_AVAILABLE
        if PYICEBERG_AVAILABLE:
            from pyiceberg.exceptions import NoSuchTableError
            mock_catalog.load_table.side_effect = NoSuchTableError("test")

            with patch.object(writer, '_catalog', mock_catalog):
                result = writer.table_exists('bronze', 'test_table')
                assert result is False

    def test_write_dataframe_empty(self, writer):
        """Test writing empty DataFrame."""
        df = pd.DataFrame()

        result = writer.write_dataframe(df, 'bronze', 'test_table')
        assert 'iceberg.bronze.test_table' in result

    def test_pandas_to_arrow(self, writer):
        """Test pandas to arrow conversion."""
        df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'str_col': ['a', 'b', 'c'],
            'float_col': [1.1, 2.2, 3.3],
        })

        arrow_table = writer._pandas_to_arrow(df)
        assert arrow_table.num_rows == 3
        assert len(arrow_table.schema) == 3

    def test_arrow_to_spark_type_int(self, writer):
        """Test arrow to spark type conversion - integers."""
        import pyarrow as pa

        assert writer._arrow_to_spark_type(pa.int32()) == 'INT'
        assert writer._arrow_to_spark_type(pa.int64()) == 'BIGINT'

    def test_arrow_to_spark_type_float(self, writer):
        """Test arrow to spark type conversion - floats."""
        import pyarrow as pa

        assert writer._arrow_to_spark_type(pa.float32()) == 'FLOAT'
        assert writer._arrow_to_spark_type(pa.float64()) == 'DOUBLE'

    def test_arrow_to_spark_type_string(self, writer):
        """Test arrow to spark type conversion - string."""
        import pyarrow as pa

        assert writer._arrow_to_spark_type(pa.string()) == 'STRING'

    def test_arrow_to_spark_type_bool(self, writer):
        """Test arrow to spark type conversion - boolean."""
        import pyarrow as pa

        assert writer._arrow_to_spark_type(pa.bool_()) == 'BOOLEAN'

    def test_arrow_to_spark_type_date(self, writer):
        """Test arrow to spark type conversion - date."""
        import pyarrow as pa

        assert writer._arrow_to_spark_type(pa.date32()) == 'DATE'

    def test_arrow_to_spark_type_unknown(self, writer):
        """Test arrow to spark type conversion - unknown type."""
        import pyarrow as pa

        # Unknown type defaults to STRING
        result = writer._arrow_to_spark_type(pa.binary())
        assert result == 'STRING'


pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")


class TestIcebergWriterWithSpark:
    """Tests for IcebergWriter using Spark fallback."""

    @pytest.fixture
    def mock_spark_session(self):
        """Mock SparkSession."""
        with patch('pyspark.sql.SparkSession') as mock:
            session = MagicMock()
            mock.builder.appName.return_value.config.return_value.config.return_value.config.return_value.config.return_value.getOrCreate.return_value = session
            yield session

    def test_write_with_spark_creates_table(self, mock_spark_session):
        """Test writing with Spark creates table if not exists."""
        from scrapers.base.iceberg_writer import IcebergWriter

        with patch.object(IcebergWriter, '_get_spark', return_value=mock_spark_session):
            with patch.object(IcebergWriter, 'table_exists', return_value=False):
                with patch.object(IcebergWriter, 'create_namespace'):
                    with patch('scrapers.base.iceberg_writer.PYICEBERG_AVAILABLE', False):
                        writer = IcebergWriter()

                        df = pd.DataFrame({'col1': [1, 2, 3]})
                        spark_df = MagicMock()
                        mock_spark_session.createDataFrame.return_value = spark_df

                        writer_mock = MagicMock()
                        spark_df.writeTo.return_value = writer_mock

                        result = writer._write_with_spark(
                            df, 'bronze', 'test_table', None, 'append'
                        )

                        assert 'iceberg.bronze.test_table' in result
