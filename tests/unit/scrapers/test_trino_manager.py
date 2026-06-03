"""
Unit tests for TrinoTableManager.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
import pyarrow as pa


class TestTrinoTableManagerInit:
    """Tests for TrinoTableManager initialization."""

    def test_init_default_values(self):
        """Test default initialization values."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            with patch.dict('os.environ', {}, clear=False):
                from scrapers.base.trino_manager import TrinoTableManager
                manager = TrinoTableManager()
                assert manager.host == 'trino'
                assert manager.port == 8443
                assert manager.user == 'airflow'
                assert manager.catalog == 'iceberg'

    def test_init_custom_values(self):
        """Test custom initialization values."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager
            manager = TrinoTableManager(
                host='test-trino',
                port=8080,
                user='test-user',
                catalog='hive',
            )
            assert manager.host == 'test-trino'
            assert manager.user == 'test-user'


class TestTrinoTableManagerCreateSchema:
    """Tests for create_schema operation."""

    def test_create_schema_success(self):
        """Test successful schema creation."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            # Create mock connection and cursor
            mock_cursor = MagicMock()
            mock_cursor.execute.return_value = None
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            manager.create_schema('bronze')

            mock_cursor.execute.assert_called_once()
            call_sql = mock_cursor.execute.call_args[0][0]
            assert 'CREATE SCHEMA IF NOT EXISTS' in call_sql
            assert 'iceberg.bronze' in call_sql


class TestTrinoTableManagerSchemaExists:
    """Tests for schema_exists operation."""

    def test_schema_exists_true(self):
        """Test schema_exists returns True when schema exists."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [('bronze',)]
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            result = manager.schema_exists('bronze')

            assert result is True

    def test_schema_exists_false(self):
        """Test schema_exists returns False when schema doesn't exist."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = []
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            result = manager.schema_exists('nonexistent')

            assert result is False


class TestTrinoTableManagerTableExists:
    """Tests for table_exists operation."""

    def test_table_exists_true(self):
        """Test table_exists returns True when table exists."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [('fbref_schedule',)]
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            result = manager.table_exists('bronze', 'fbref_schedule')

            assert result is True

    def test_table_exists_false(self):
        """Test table_exists returns False when table doesn't exist."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = []
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            result = manager.table_exists('bronze', 'nonexistent')

            assert result is False

    def test_table_exists_error(self):
        """Test table_exists returns False on error."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = Exception("Query failed")
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            result = manager.table_exists('bronze', 'test')

            assert result is False


class TestTrinoTableManagerCreateExternalTable:
    """Tests for create_external_table operation."""

    def test_create_external_table_basic(self):
        """Test basic external table creation."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.execute.return_value = None
            mock_cursor.fetchall.return_value = []
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            columns = {
                'date': 'DATE',
                'home_team': 'VARCHAR',
                'away_team': 'VARCHAR',
                'home_goals': 'INTEGER',
            }

            manager.create_external_table(
                schema='bronze',
                table='test_matches',
                columns=columns,
                location='hdfs://namenode:9000/data/bronze/test',
            )

            # Check that execute was called (for schema creation and table creation)
            assert mock_cursor.execute.call_count >= 1

    def test_create_external_table_with_partitions(self):
        """Test external table creation with partition columns."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.execute.return_value = None
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            columns = {
                'date': 'DATE',
                'team': 'VARCHAR',
                'goals': 'INTEGER',
            }

            partition_columns = {
                'league': 'VARCHAR',
                'season': 'INTEGER',
            }

            manager.create_external_table(
                schema='bronze',
                table='test_stats',
                columns=columns,
                location='hdfs://namenode:9000/data/bronze/stats',
                partition_columns=partition_columns,
            )

            # Find the CREATE TABLE call (last call with CREATE TABLE in it)
            for call in mock_cursor.execute.call_args_list:
                call_sql = call[0][0]
                if 'CREATE TABLE' in call_sql:
                    assert 'league' in call_sql
                    assert 'season' in call_sql
                    break


class TestTrinoTableManagerCreateIcebergTable:
    """Tests for create_iceberg_table operation."""

    def test_create_iceberg_table_basic(self):
        """Test basic Iceberg table creation."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.execute.return_value = None
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            columns = {
                'date': 'DATE',
                'team': 'VARCHAR',
                'rating': 'DOUBLE',
            }

            manager.create_iceberg_table(
                schema='bronze',
                table='test_ratings',
                columns=columns,
            )

            # Check that CREATE TABLE was called
            calls = mock_cursor.execute.call_args_list
            table_call = None
            for call in calls:
                if 'CREATE TABLE' in call[0][0] and 'test_ratings' in call[0][0]:
                    table_call = call[0][0]
                    break

            assert table_call is not None
            assert 'CREATE TABLE IF NOT EXISTS' in table_call
            assert 'iceberg.bronze.test_ratings' in table_call
            assert '"date" DATE' in table_call
            assert '"team" VARCHAR' in table_call
            assert '"rating" DOUBLE' in table_call

    def test_create_iceberg_table_with_partitions(self):
        """Test Iceberg table creation with partitioning."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.execute.return_value = None
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            columns = {
                'date': 'DATE',
                'team': 'VARCHAR',
                'league': 'VARCHAR',
                'season': 'INTEGER',
            }

            manager.create_iceberg_table(
                schema='bronze',
                table='test_stats',
                columns=columns,
                partition_columns=['league', 'season'],
            )

            # Check that partitioning clause was included
            calls = mock_cursor.execute.call_args_list
            for call in calls:
                sql = call[0][0]
                if 'CREATE TABLE' in sql and 'test_stats' in sql:
                    assert "WITH (partitioning = ARRAY['league', 'season'])" in sql
                    break


class TestTrinoTableManagerInsertDataFrame:
    """Tests for insert_dataframe operation."""

    def test_insert_dataframe_basic(self):
        """Test basic DataFrame insertion."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.execute.return_value = None
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            df = pd.DataFrame({
                'team': ['Arsenal', 'Liverpool'],
                'goals': [2, 3],
            })

            result = manager.insert_dataframe('bronze', 'test_table', df)

            assert result == 2
            # 1 DESCRIBE (get_table_columns) + 1 INSERT = 2 calls
            assert mock_cursor.execute.call_count == 2
            call_sql = mock_cursor.execute.call_args[0][0]
            assert 'INSERT INTO iceberg.bronze.test_table' in call_sql
            assert 'Arsenal' in call_sql
            assert 'Liverpool' in call_sql

    def test_insert_dataframe_empty(self):
        """Test empty DataFrame insertion returns 0."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            df = pd.DataFrame()

            result = manager.insert_dataframe('bronze', 'test_table', df)

            assert result == 0

    def test_insert_dataframe_with_null(self):
        """Test DataFrame insertion with NULL values."""
        import pandas as pd
        import numpy as np
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.execute.return_value = None
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            df = pd.DataFrame({
                'team': ['Arsenal', None],
                'goals': [2, np.nan],
            })

            manager.insert_dataframe('bronze', 'test_table', df)

            call_sql = mock_cursor.execute.call_args[0][0]
            assert 'NULL' in call_sql

    def test_insert_dataframe_escapes_quotes(self):
        """Test DataFrame insertion escapes single quotes."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.execute.return_value = None
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            df = pd.DataFrame({
                'name': ["O'Brien"],
            })

            manager.insert_dataframe('bronze', 'test_table', df)

            call_sql = mock_cursor.execute.call_args[0][0]
            # Single quote should be escaped as two single quotes
            assert "O''Brien" in call_sql

    def test_insert_dataframe_batch_size(self):
        """Test DataFrame insertion respects batch_size."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.execute.return_value = None
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            # Create DataFrame with 5 rows
            df = pd.DataFrame({
                'team': ['A', 'B', 'C', 'D', 'E'],
                'goals': [1, 2, 3, 4, 5],
            })

            result = manager.insert_dataframe('bronze', 'test_table', df, batch_size=2)

            assert result == 5
            # 1 DESCRIBE (get_table_columns) + 3 INSERT batches: [A,B], [C,D], [E] = 4 calls
            assert mock_cursor.execute.call_count == 4


class TestTrinoTableManagerInsertDataFrameAtomic:
    """Tests for insert_dataframe_atomic (stage+merge → one target snapshot, #269)."""

    def test_insert_dataframe_atomic_stages_and_merges(self):
        """Staged batches collapse into a single INSERT...SELECT on the target."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            df = pd.DataFrame({'team': ['A', 'B', 'C'], 'goals': [1, 2, 3]})

            with patch.object(manager, '_execute') as mock_execute, \
                 patch.object(manager, 'insert_dataframe', return_value=3) as mock_insert, \
                 patch.object(manager, 'drop_table') as mock_drop:
                result = manager.insert_dataframe_atomic('bronze', 'test_table', df)

            assert result == 3

            # Batches are staged on the throwaway table, not the target.
            mock_insert.assert_called_once()
            assert mock_insert.call_args[0][1] == 'test_table__stg'

            # Stage is dropped before (crash leftovers) and after (cleanup).
            assert mock_drop.call_count == 2
            for call in mock_drop.call_args_list:
                assert call[0][1] == 'test_table__stg'

            executed = [c[0][0] for c in mock_execute.call_args_list]
            # Empty-schema copy of the target.
            assert any(
                'CREATE TABLE iceberg.bronze.test_table__stg AS SELECT * FROM '
                'iceberg.bronze.test_table WHERE false' in sql for sql in executed
            )
            # Exactly ONE INSERT into the target — the merge from stage.
            target_inserts = [
                sql for sql in executed
                if 'INSERT INTO iceberg.bronze.test_table ' in sql
            ]
            assert len(target_inserts) == 1
            assert 'SELECT' in target_inserts[0]
            assert 'FROM iceberg.bronze.test_table__stg' in target_inserts[0]

    def test_insert_dataframe_atomic_empty(self):
        """Empty DataFrame is a no-op (no staging)."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            with patch.object(manager, '_execute') as mock_execute, \
                 patch.object(manager, 'drop_table') as mock_drop:
                result = manager.insert_dataframe_atomic('bronze', 'test_table', pd.DataFrame())

            assert result == 0
            mock_execute.assert_not_called()
            mock_drop.assert_not_called()


class TestTrinoTableManagerDropTable:
    """Tests for drop_table operation."""

    def test_drop_table_success(self):
        """Test successful table drop."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.execute.return_value = None
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            manager.drop_table('bronze', 'test_table')

            call_sql = mock_cursor.execute.call_args[0][0]
            assert 'DROP TABLE IF EXISTS' in call_sql
            assert 'iceberg.bronze.test_table' in call_sql


class TestTrinoTableManagerGetTableColumns:
    """Tests for get_table_columns operation."""

    def test_get_table_columns_success(self):
        """Test successful column retrieval."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [
                ('date', 'date', '', ''),
                ('home_team', 'varchar', '', ''),
                ('goals', 'integer', '', ''),
            ]
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            result = manager.get_table_columns('bronze', 'test_table')

            assert 'date' in result
            assert result['date'] == 'date'
            assert result['home_team'] == 'varchar'
            assert result['goals'] == 'integer'


class TestTrinoTableManagerArrowSchemaConversion:
    """Tests for Arrow schema to Trino type conversion."""

    @pytest.fixture
    def manager(self):
        """Create TrinoTableManager for schema conversion tests."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager
            return TrinoTableManager()

    def test_convert_basic_types(self, manager):
        """Test conversion of basic Arrow types."""
        schema = pa.schema([
            ('int_col', pa.int32()),
            ('bigint_col', pa.int64()),
            ('float_col', pa.float32()),
            ('double_col', pa.float64()),
            ('string_col', pa.string()),
            ('bool_col', pa.bool_()),
            ('date_col', pa.date32()),
        ])

        result = manager.arrow_schema_to_trino(schema)

        assert result['int_col'] == 'INTEGER'
        assert result['bigint_col'] == 'BIGINT'
        assert result['float_col'] == 'REAL'
        assert result['double_col'] == 'DOUBLE'
        assert result['string_col'] == 'VARCHAR'
        assert result['bool_col'] == 'BOOLEAN'
        assert result['date_col'] == 'DATE'

    def test_convert_timestamp_types(self, manager):
        """Test conversion of timestamp types."""
        schema = pa.schema([
            ('ts', pa.timestamp('us')),
            ('ts_tz', pa.timestamp('us', tz='UTC')),
        ])

        result = manager.arrow_schema_to_trino(schema)

        assert result['ts'] == 'TIMESTAMP'
        assert result['ts_tz'] == 'TIMESTAMP WITH TIME ZONE'

    def test_convert_decimal_type(self, manager):
        """Test conversion of decimal type."""
        schema = pa.schema([
            ('price', pa.decimal128(10, 2)),
        ])

        result = manager.arrow_schema_to_trino(schema)

        assert result['price'] == 'DECIMAL(10, 2)'

    def test_convert_list_type(self, manager):
        """Test conversion of list/array type."""
        schema = pa.schema([
            ('tags', pa.list_(pa.string())),
        ])

        result = manager.arrow_schema_to_trino(schema)

        assert result['tags'] == 'ARRAY(VARCHAR)'

    def test_convert_unsigned_integers(self, manager):
        """Test conversion of unsigned integers (Trino doesn't have unsigned)."""
        schema = pa.schema([
            ('uint8_col', pa.uint8()),
            ('uint16_col', pa.uint16()),
            ('uint32_col', pa.uint32()),
            ('uint64_col', pa.uint64()),
        ])

        result = manager.arrow_schema_to_trino(schema)

        # Unsigned types are promoted to signed types
        assert result['uint8_col'] == 'SMALLINT'
        assert result['uint16_col'] == 'INTEGER'
        assert result['uint32_col'] == 'BIGINT'
        assert result['uint64_col'] == 'BIGINT'


class TestTrinoTableManagerExecuteQuery:
    """Tests for execute_query operation."""

    def test_execute_query_success(self):
        """Test successful query execution."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [
                (1, 'Arsenal', 2),
                (2, 'Liverpool', 3),
            ]
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            result = manager.execute_query("SELECT * FROM test")

            assert len(result) == 2
            assert result[0] == (1, 'Arsenal', 2)


class TestTrinoTableManagerContextManager:
    """Tests for context manager support."""

    def test_context_manager(self):
        """Test context manager usage."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager
            with TrinoTableManager() as manager:
                assert manager is not None

    def test_close(self):
        """Test close method."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            mock_conn = MagicMock()
            manager._conn = mock_conn

            manager.close()

            mock_conn.close.assert_called_once()
            assert manager._conn is None


class TestTrinoError:
    """Tests for TrinoError exception."""

    def test_trino_error_message(self):
        """Test TrinoError exception message."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoError
            error = TrinoError("Test error message")
            assert str(error) == "Test error message"


class TestIsIcebergInvalidMetadata:
    """Tests for _is_iceberg_invalid_metadata helper."""

    def test_detects_iceberg_invalid_metadata(self):
        """TrinoError with TrinoExternalError(ICEBERG_INVALID_METADATA) cause → True."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoError, _is_iceberg_invalid_metadata

            # Simulate TrinoExternalError with error_name attribute
            cause = Exception("Error accessing metadata file")
            cause.error_name = 'ICEBERG_INVALID_METADATA'

            error = TrinoError("SQL execution failed")
            error.__cause__ = cause

            assert _is_iceberg_invalid_metadata(error) is True

    def test_detects_iceberg_missing_metadata(self):
        """TrinoError with TrinoExternalError(ICEBERG_MISSING_METADATA) cause → True."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoError, _is_iceberg_invalid_metadata

            cause = Exception("Metadata not found in metadata location")
            cause.error_name = 'ICEBERG_MISSING_METADATA'

            error = TrinoError("SQL execution failed")
            error.__cause__ = cause

            assert _is_iceberg_invalid_metadata(error) is True

    def test_returns_false_for_other_errors(self):
        """TrinoError with different error_name → False."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoError, _is_iceberg_invalid_metadata

            cause = Exception("Some other error")
            cause.error_name = 'GENERIC_INTERNAL_ERROR'

            error = TrinoError("SQL execution failed")
            error.__cause__ = cause

            assert _is_iceberg_invalid_metadata(error) is False

    def test_returns_false_for_plain_exception(self):
        """Plain ValueError without error_name → False."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import _is_iceberg_invalid_metadata

            error = ValueError("something went wrong")
            assert _is_iceberg_invalid_metadata(error) is False


class TestTrinoTableManagerConnectionRetry:
    """Tests for connection retry logic."""

    def test_connect_with_retry_success_first_attempt(self):
        """Test successful connection on first attempt."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = [(1,)]
            mock_conn.cursor.return_value = mock_cursor

            with patch.object(manager, '_create_connection', return_value=mock_conn):
                manager._connect_with_retry()

            assert manager._conn is mock_conn
            mock_cursor.execute.assert_called_once_with('SELECT 1')

    def test_connect_with_retry_fails_then_succeeds(self):
        """Test connection fails first attempt, succeeds on second."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            manager._CONNECT_BACKOFF = (0, 0, 0)

            mock_conn_good = MagicMock()
            mock_cursor_good = MagicMock()
            mock_cursor_good.fetchall.return_value = [(1,)]
            mock_conn_good.cursor.return_value = mock_cursor_good

            with patch.object(
                manager, '_create_connection',
                side_effect=[Exception("Connection refused"), mock_conn_good]
            ):
                manager._connect_with_retry()

            assert manager._conn is mock_conn_good

    def test_connect_with_retry_all_attempts_fail(self):
        """Test all connection attempts fail → TrinoError raised."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager, TrinoError

            manager = TrinoTableManager()
            manager._CONNECT_BACKOFF = (0, 0, 0)

            with patch.object(
                manager, '_create_connection',
                side_effect=Exception("Connection refused")
            ):
                with pytest.raises(TrinoError, match="Failed to connect to Trino after"):
                    manager._connect_with_retry()

    def test_reset_connection(self):
        """Test _reset_connection closes and clears connection."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager
            manager = TrinoTableManager()
            mock_conn = MagicMock()
            manager._conn = mock_conn

            manager._reset_connection()

            mock_conn.close.assert_called_once()
            assert manager._conn is None

    def test_execute_retries_on_connection_error(self):
        """Test _execute resets connection and retries on Connection refused."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            # First call: raise Connection refused
            mock_cursor_bad = MagicMock()
            mock_cursor_bad.execute.side_effect = Exception("Connection refused")

            # Second call: succeed
            mock_cursor_good = MagicMock()
            mock_cursor_good.fetchall.return_value = [(1,)]

            mock_conn1 = MagicMock()
            mock_conn1.cursor.return_value = mock_cursor_bad

            mock_conn2 = MagicMock()
            mock_conn2.cursor.return_value = mock_cursor_good

            manager._conn = mock_conn1

            # After reset, _connect_with_retry should set new conn
            with patch.object(manager, '_connect_with_retry') as mock_retry:
                def set_good_conn():
                    manager._conn = mock_conn2
                mock_retry.side_effect = set_good_conn

                result = manager._execute('SELECT 1', fetch=True)

                assert result == [(1,)]
                mock_retry.assert_called_once()
