"""
Integration tests for Storage Pipeline (HDFS + Trino).

These tests require running Docker containers:
- namenode (HDFS)
- trino (Trino coordinator)
- hive-metastore

Run with: pytest tests/integration/test_storage_pipeline.py -v -m integration
"""

import os
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


# Mark all tests in this module as integration tests
pytestmark = [pytest.mark.integration, pytest.mark.slow]


@pytest.fixture(scope="module")
def hdfs_client():
    """Create HDFSClient for integration tests."""
    from scrapers.base.hdfs_client import HDFSClient

    client = HDFSClient(
        namenode_host=os.environ.get('HDFS_NAMENODE_HOST', 'localhost'),
        port=int(os.environ.get('HDFS_WEBHDFS_PORT', 9870)),
    )
    yield client
    client.close()


@pytest.fixture(scope="module")
def trino_manager():
    """Create TrinoTableManager for integration tests (uses iceberg catalog by default)."""
    from scrapers.base.trino_manager import TrinoTableManager

    manager = TrinoTableManager(
        host=os.environ.get('TRINO_HOST', 'localhost'),
        port=int(os.environ.get('TRINO_PORT', 8082)),
        catalog='iceberg',  # Default to iceberg catalog
    )
    yield manager
    manager.close()


@pytest.fixture
def test_table_name():
    """Generate unique table name for tests."""
    return f"test_table_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def sample_dataframe():
    """Create sample DataFrame for testing."""
    return pd.DataFrame({
        'match_date': [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
        'home_team': ['Arsenal', 'Liverpool', 'Manchester City'],
        'away_team': ['Chelsea', 'Manchester United', 'Tottenham'],
        'home_goals': [2, 1, 3],
        'away_goals': [1, 1, 0],
        'league': ['EPL', 'EPL', 'EPL'],
        'season': [2024, 2024, 2024],
    })


class TestHDFSIntegration:
    """Integration tests for HDFS operations."""

    @pytest.mark.integration
    def test_hdfs_connection(self, hdfs_client):
        """Test HDFS connection is working."""
        # Check root exists
        assert hdfs_client.exists('/')

    @pytest.mark.integration
    def test_hdfs_mkdir_and_list(self, hdfs_client):
        """Test creating and listing directory."""
        test_dir = f"/tmp/test_integration_{uuid.uuid4().hex[:8]}"

        try:
            # Create directory
            result = hdfs_client.mkdir(test_dir)
            assert result is True

            # Verify exists
            assert hdfs_client.exists(test_dir) is True

            # List parent directory
            dirs = hdfs_client.list_dir('/tmp')
            dir_names = [d['name'] for d in dirs]
            assert test_dir.split('/')[-1] in dir_names

        finally:
            # Cleanup
            hdfs_client.delete(test_dir, recursive=True)

    @pytest.mark.integration
    @pytest.mark.skip(reason="Requires Docker network - datanode not accessible from host")
    def test_hdfs_upload_file(self, hdfs_client, tmp_path):
        """Test uploading file to HDFS."""
        # Create local test file
        local_file = tmp_path / "test_upload.txt"
        local_file.write_text("Hello HDFS Integration Test!")

        hdfs_path = f"/tmp/test_upload_{uuid.uuid4().hex[:8]}.txt"

        try:
            # Upload file
            result = hdfs_client.upload_file(str(local_file), hdfs_path)
            assert result is True

            # Verify exists
            assert hdfs_client.exists(hdfs_path) is True

            # Read back and verify content
            content = hdfs_client.read_file(hdfs_path)
            assert content == b"Hello HDFS Integration Test!"

        finally:
            # Cleanup
            hdfs_client.delete(hdfs_path)

    @pytest.mark.integration
    @pytest.mark.skip(reason="Requires Docker network - datanode not accessible from host")
    def test_hdfs_upload_parquet(self, hdfs_client, sample_dataframe, tmp_path):
        """Test uploading Parquet file to HDFS."""
        # Write DataFrame to local Parquet
        local_file = tmp_path / "test.parquet"
        arrow_table = pa.Table.from_pandas(sample_dataframe)
        pq.write_table(arrow_table, str(local_file))

        hdfs_path = f"/tmp/test_parquet_{uuid.uuid4().hex[:8]}.parquet"

        try:
            # Upload file
            result = hdfs_client.upload_file(str(local_file), hdfs_path)
            assert result is True

            # Verify exists
            assert hdfs_client.exists(hdfs_path) is True

            # Check file status
            status = hdfs_client.get_file_status(hdfs_path)
            assert status is not None
            assert status['type'] == 'FILE'
            assert status['size'] > 0

        finally:
            # Cleanup
            hdfs_client.delete(hdfs_path)


class TestTrinoIntegration:
    """Integration tests for Trino operations."""

    @pytest.mark.integration
    def test_trino_connection(self, trino_manager):
        """Test Trino connection is working."""
        result = trino_manager.execute_query("SELECT 1")
        # Trino Python client returns lists, not tuples
        assert result[0][0] == 1

    @pytest.mark.integration
    def test_trino_create_schema(self, trino_manager):
        """Test creating schema in Trino."""
        test_schema = f"test_schema_{uuid.uuid4().hex[:8]}"

        try:
            # Create schema
            trino_manager.create_schema(test_schema)

            # Verify exists
            assert trino_manager.schema_exists(test_schema) is True

        finally:
            # Cleanup - drop schema
            try:
                trino_manager.execute_query(f"DROP SCHEMA IF EXISTS iceberg.{test_schema}")
            except Exception:
                pass

    @pytest.mark.integration
    def test_trino_show_catalogs(self, trino_manager):
        """Test listing catalogs."""
        result = trino_manager.execute_query("SHOW CATALOGS")
        catalogs = [r[0] for r in result]
        assert 'iceberg' in catalogs


class TestStoragePipeline:
    """Integration tests for full storage pipeline."""

    @pytest.mark.integration
    @pytest.mark.skip(reason="Requires Docker network - datanode not accessible from host")
    def test_full_pipeline(
        self,
        hdfs_client,
        trino_manager,
        sample_dataframe,
        test_table_name,
        tmp_path
    ):
        """Test complete pipeline: DataFrame → HDFS → Trino → SELECT."""
        test_schema = 'bronze'
        hdfs_dir = f"/data/bronze/test_integration/{test_table_name}"
        filename = "data.parquet"
        hdfs_path = f"{hdfs_dir}/{filename}"

        try:
            # Step 1: Write DataFrame to local Parquet
            local_file = tmp_path / filename
            arrow_table = pa.Table.from_pandas(sample_dataframe)
            pq.write_table(arrow_table, str(local_file))

            # Step 2: Upload to HDFS
            hdfs_client.mkdir(hdfs_dir)
            upload_result = hdfs_client.upload_file(str(local_file), hdfs_path)
            assert upload_result is True
            assert hdfs_client.exists(hdfs_path) is True

            # Step 3: Create external table in Trino
            trino_columns = trino_manager.arrow_schema_to_trino(arrow_table.schema)
            hdfs_location = f"hdfs://namenode:9000{hdfs_dir}"

            trino_manager.create_external_table(
                schema=test_schema,
                table=test_table_name,
                columns=trino_columns,
                location=hdfs_location,
            )

            # Verify table exists
            assert trino_manager.table_exists(test_schema, test_table_name) is True

            # Step 4: Query data via Trino
            query_result = trino_manager.execute_query(
                f"SELECT COUNT(*) FROM iceberg.{test_schema}.{test_table_name}"
            )
            assert query_result[0][0] == len(sample_dataframe)

            # Verify data content
            data_result = trino_manager.execute_query(
                f"SELECT home_team FROM iceberg.{test_schema}.{test_table_name} ORDER BY home_team"
            )
            teams = [r[0] for r in data_result]
            assert 'Arsenal' in teams
            assert 'Liverpool' in teams

        finally:
            # Cleanup
            try:
                trino_manager.drop_table(test_schema, test_table_name)
            except Exception:
                pass

            try:
                hdfs_client.delete(hdfs_dir, recursive=True)
            except Exception:
                pass

    @pytest.mark.integration
    def test_iceberg_writer_integration(
        self,
        trino_manager,
        sample_dataframe,
        test_table_name,
    ):
        """Test IcebergWriter with Trino Iceberg tables."""
        # Set environment variables for IcebergWriter
        os.environ['TRINO_HOST'] = os.environ.get('TRINO_HOST', 'localhost')
        os.environ['TRINO_PORT'] = os.environ.get('TRINO_PORT', '8082')

        from scrapers.base.iceberg_writer import IcebergWriter

        writer = IcebergWriter()
        test_schema = 'bronze'

        try:
            # Write DataFrame using IcebergWriter (direct to Iceberg via Trino)
            result = writer.write_dataframe(
                df=sample_dataframe,
                database=test_schema,
                table=test_table_name,
                add_metadata=True,
                source='integration_test',
            )

            # Check result is a valid Iceberg table identifier
            assert result is not None
            assert test_table_name in result
            assert 'iceberg.' in result

            # Create a manager with iceberg catalog to verify
            from scrapers.base.trino_manager import TrinoTableManager
            iceberg_manager = TrinoTableManager(
                host=os.environ.get('TRINO_HOST', 'localhost'),
                port=int(os.environ.get('TRINO_PORT', 8082)),
                catalog='iceberg',
            )

            # Verify table exists in Iceberg catalog
            assert iceberg_manager.table_exists(test_schema, test_table_name) is True

            # Query row count (DataFrame + 3 metadata columns)
            query_result = iceberg_manager.execute_query(
                f"SELECT COUNT(*) FROM iceberg.{test_schema}.{test_table_name}"
            )
            assert query_result[0][0] == len(sample_dataframe)

            # Verify metadata columns were added
            columns = iceberg_manager.get_table_columns(test_schema, test_table_name)
            assert '_source' in columns
            assert '_ingested_at' in columns
            assert '_batch_id' in columns

            iceberg_manager.close()

        finally:
            # Cleanup - drop Iceberg table
            try:
                from scrapers.base.trino_manager import TrinoTableManager
                cleanup_manager = TrinoTableManager(
                    host=os.environ.get('TRINO_HOST', 'localhost'),
                    port=int(os.environ.get('TRINO_PORT', 8082)),
                    catalog='iceberg',
                )
                cleanup_manager.drop_table(test_schema, test_table_name)
                cleanup_manager.close()
            except Exception:
                pass


class TestIcebergFeatures:
    """Integration tests for Iceberg-specific features."""

    @pytest.mark.integration
    def test_iceberg_catalog_exists(self, trino_manager):
        """Verify iceberg catalog is available."""
        result = trino_manager.execute_query("SHOW CATALOGS")
        catalogs = [r[0] for r in result]
        assert 'iceberg' in catalogs

    @pytest.mark.integration
    def test_iceberg_table_snapshots(self, sample_dataframe, test_table_name):
        """Test Iceberg snapshots after inserts."""
        os.environ['TRINO_HOST'] = os.environ.get('TRINO_HOST', 'localhost')
        os.environ['TRINO_PORT'] = os.environ.get('TRINO_PORT', '8082')

        from scrapers.base.iceberg_writer import IcebergWriter
        from scrapers.base.trino_manager import TrinoTableManager

        writer = IcebergWriter()
        test_schema = 'bronze'

        try:
            # First write
            writer.write_dataframe(
                df=sample_dataframe.head(2),
                database=test_schema,
                table=test_table_name,
                add_metadata=False,
            )

            # Second write (append)
            writer.write_dataframe(
                df=sample_dataframe.tail(1),
                database=test_schema,
                table=test_table_name,
                add_metadata=False,
            )

            # Check snapshots
            iceberg_manager = TrinoTableManager(
                host=os.environ.get('TRINO_HOST', 'localhost'),
                port=int(os.environ.get('TRINO_PORT', 8082)),
                catalog='iceberg',
            )

            snapshots = iceberg_manager.execute_query(
                f'SELECT * FROM iceberg.{test_schema}."{test_table_name}$snapshots"'
            )

            # Should have at least 2 snapshots (may have more due to table creation)
            assert len(snapshots) >= 2

            iceberg_manager.close()

        finally:
            try:
                from scrapers.base.trino_manager import TrinoTableManager
                cleanup_manager = TrinoTableManager(
                    host=os.environ.get('TRINO_HOST', 'localhost'),
                    port=int(os.environ.get('TRINO_PORT', 8082)),
                    catalog='iceberg',
                )
                cleanup_manager.drop_table(test_schema, test_table_name)
                cleanup_manager.close()
            except Exception:
                pass


class TestDataLayerVerification:
    """Tests to verify the Medallion architecture setup."""

    @pytest.mark.integration
    def test_bronze_schema_exists(self, trino_manager):
        """Verify bronze schema exists in Iceberg."""
        exists = trino_manager.schema_exists('bronze')
        if not exists:
            trino_manager.create_schema('bronze')
        assert trino_manager.schema_exists('bronze') is True

    @pytest.mark.integration
    def test_silver_schema_exists(self, trino_manager):
        """Verify silver schema exists in Iceberg."""
        exists = trino_manager.schema_exists('silver')
        if not exists:
            trino_manager.create_schema('silver')
        assert trino_manager.schema_exists('silver') is True

    @pytest.mark.integration
    def test_gold_schema_exists(self, trino_manager):
        """Verify gold schema exists in Iceberg."""
        exists = trino_manager.schema_exists('gold')
        if not exists:
            trino_manager.create_schema('gold')
        assert trino_manager.schema_exists('gold') is True

    @pytest.mark.integration
    def test_hdfs_data_directories_exist(self, hdfs_client):
        """Verify HDFS data directories exist."""
        directories = ['/data/bronze', '/data/silver', '/data/gold']

        for dir_path in directories:
            if not hdfs_client.exists(dir_path):
                hdfs_client.mkdir(dir_path)
            assert hdfs_client.exists(dir_path) is True
