"""
Integration tests for the storage pipeline (Iceberg REST catalog + S3).

These tests require the running stack:
- trino       (Trino coordinator, host-mapped on :8082)
- lakekeeper  (Iceberg REST catalog, :8181)
- seaweedfs   (S3 object storage, :8333)

The Medallion schemas must already exist (run `make bootstrap-catalog` then
`make init-storage`).

Run with: pytest tests/integration/test_storage_pipeline.py -v -m integration
"""

import os
import uuid
from datetime import date

import pandas as pd
import pytest

# sys.path setup (project root + dags folder) is centralised in the root conftest.py.

# Mark all tests in this module as integration tests
pytestmark = [pytest.mark.integration, pytest.mark.slow]


@pytest.fixture(scope="module")
def trino_manager():
    """TrinoTableManager bound to the iceberg (REST) catalog."""
    from scrapers.base.trino_manager import TrinoTableManager

    manager = TrinoTableManager(
        host=os.environ.get('TRINO_HOST', 'localhost'),
        port=int(os.environ.get('TRINO_PORT', 8082)),
        catalog='iceberg',
    )
    yield manager
    manager.close()


@pytest.fixture(scope="module")
def iceberg_writer():
    """IcebergWriter writing through Trino INSERT into the REST catalog."""
    from scrapers.base.iceberg_writer import IcebergWriter

    yield IcebergWriter(
        trino_host=os.environ.get('TRINO_HOST', 'localhost'),
        trino_port=int(os.environ.get('TRINO_PORT', 8082)),
    )


@pytest.fixture
def test_table_name():
    """Generate a unique table name for tests."""
    return f"test_table_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def sample_dataframe():
    """Create a small synthetic DataFrame for testing."""
    return pd.DataFrame({
        'match_date': [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
        'home_team': ['Arsenal', 'Liverpool', 'Manchester City'],
        'away_team': ['Chelsea', 'Manchester United', 'Tottenham'],
        'home_goals': [2, 1, 3],
        'away_goals': [1, 1, 0],
        'league': ['EPL', 'EPL', 'EPL'],
        'season': [2024, 2024, 2024],
    })


class TestIcebergRestCatalog:
    """End-to-end checks against Lakekeeper (REST) + SeaweedFS (S3)."""

    def test_medallion_schemas_exist(self, trino_manager):
        """bronze/silver/gold namespaces exist in the REST catalog."""
        for schema in ('bronze', 'silver', 'gold'):
            assert trino_manager.schema_exists(schema), (
                f"iceberg.{schema} missing — run `make bootstrap-catalog` then "
                f"`make init-storage`"
            )

    def test_write_read_drop_roundtrip(
        self, iceberg_writer, trino_manager, test_table_name, sample_dataframe
    ):
        """Full path: INSERT via Trino → data lands on S3 → SELECT back → DROP."""
        iceberg_writer.write_dataframe(
            df=sample_dataframe,
            database='bronze',
            table=test_table_name,
            partition_spec=[('league', 'identity'), ('season', 'identity')],
            mode='overwrite',
            source='integration_test',
        )
        try:
            assert trino_manager.table_exists('bronze', test_table_name)

            df = iceberg_writer.read_table('bronze', test_table_name)
            assert len(df) == len(sample_dataframe)
        finally:
            trino_manager.drop_table('bronze', test_table_name, if_exists=True)

        assert not trino_manager.table_exists('bronze', test_table_name)
