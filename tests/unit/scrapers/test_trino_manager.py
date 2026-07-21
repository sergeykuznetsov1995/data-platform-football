"""
Unit tests for TrinoTableManager.
"""

import pytest
from unittest.mock import MagicMock, call, patch
import pandas as pd
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

    def test_verified_column_types_skip_stage_describe(self):
        """A caller-owned schema removes only the redundant metadata query."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            mock_cursor = MagicMock()
            mock_cursor.execute.return_value = None
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            result = manager.insert_dataframe(
                'bronze',
                'typed_stage',
                pd.DataFrame({'match_id': ['m1'], 'goals': [2]}),
                column_types={
                    'match_id': 'VARCHAR',
                    'goals': 'BIGINT',
                },
            )

            assert result == 1
            assert mock_cursor.execute.call_count == 1
            sql = mock_cursor.execute.call_args.args[0]
            assert 'INSERT INTO iceberg.bronze.typed_stage' in sql
            assert 'CAST(2 AS BIGINT)' in sql

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
    """Tests for insert_dataframe_atomic (stage+merge → one target snapshot, #269;
    stage-first → atomic swap for replace_partitions, #314)."""

    @staticmethod
    def _exec_ok(staged_count):
        """_execute side_effect: return a row count for the count(*) probe,
        None for every other (DDL/DML) statement."""
        def _exec(sql, fetch=False):
            if fetch:
                return [[staged_count]]
            return None
        return _exec

    def test_insert_dataframe_atomic_stages_and_merges(self):
        """Staged batches collapse into a single INSERT...SELECT on the target."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            df = pd.DataFrame({'team': ['A', 'B', 'C'], 'goals': [1, 2, 3]})

            with patch.object(manager, '_execute', side_effect=self._exec_ok(3)) as mock_execute, \
                 patch.object(manager, 'insert_dataframe', return_value=3) as mock_insert, \
                 patch.object(manager, 'drop_table') as mock_drop:
                result = manager.insert_dataframe_atomic('bronze', 'test_table', df)

            assert result == 3

            # Batches are staged on the throwaway table, not the target.
            mock_insert.assert_called_once()
            stage = mock_insert.call_args[0][1]
            assert stage.startswith('test_table__stg_')
            assert len(stage) == len('test_table__stg_') + 12

            # A unique stage needs only the post-success cleanup.
            mock_drop.assert_called_once_with('bronze', stage, if_exists=True)

            executed = [c[0][0] for c in mock_execute.call_args_list]
            # Empty-schema copy of the target.
            assert any(
                f'CREATE TABLE iceberg.bronze.{stage} AS SELECT * FROM '
                'iceberg.bronze.test_table WHERE false' in sql for sql in executed
            )
            # Exactly ONE INSERT into the target — the merge from stage.
            target_inserts = [
                sql for sql in executed
                if 'INSERT INTO iceberg.bronze.test_table ' in sql
            ]
            assert len(target_inserts) == 1
            assert 'SELECT' in target_inserts[0]
            assert f'FROM iceberg.bronze.{stage}' in target_inserts[0]
            # Plain append → no DELETE issued.
            assert not any('DELETE FROM' in sql for sql in executed)

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

    def test_insert_dataframe_atomic_empty_replacement_deletes_scope(self):
        """An explicit zero-row replacement clears only its exact scope."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            with patch.object(manager, '_execute') as mock_execute, \
                 patch.object(manager, 'drop_table') as mock_drop:
                result = manager.insert_dataframe_atomic(
                    'bronze',
                    'test_table',
                    pd.DataFrame(),
                    delete_filter="match_id = 'abc123'",
                )

            assert result == 0
            mock_execute.assert_called_once_with(
                "DELETE FROM iceberg.bronze.test_table "
                "WHERE match_id = 'abc123'"
            )
            mock_drop.assert_not_called()

    def test_insert_dataframe_atomic_empty_replacement_failure_propagates(self):
        """A failed zero-row replacement is never reported as success."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoError, TrinoTableManager

            manager = TrinoTableManager()
            with patch.object(
                manager,
                '_execute',
                side_effect=TrinoError("delete failed"),
            ):
                with pytest.raises(TrinoError, match="delete failed"):
                    manager.insert_dataframe_atomic(
                        'bronze',
                        'test_table',
                        pd.DataFrame(),
                        delete_filter="match_id = 'abc123'",
                    )

    def test_replace_partitions_stages_before_delete(self):
        """delete_filter path (#314): the partition DELETE runs only AFTER the
        stage is populated, and the swap is DELETE then INSERT...SELECT on the
        target. The stage is cleaned up on success."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            df = pd.DataFrame({'team': ['A', 'B'], 'goals': [1, 2]})

            with patch.object(manager, '_execute', side_effect=self._exec_ok(2)) as mock_execute, \
                 patch.object(manager, 'insert_dataframe', return_value=2) as mock_insert, \
                 patch.object(manager, 'drop_table') as mock_drop:
                result = manager.insert_dataframe_atomic(
                    'bronze', 'test_table', df, delete_filter="team = 'A' OR team = 'B'",
                )

            assert result == 2
            executed = [c[0][0] for c in mock_execute.call_args_list]

            # Staging happens before any DELETE: insert_dataframe targets the stage.
            stage = mock_insert.call_args[0][1]
            assert stage.startswith('test_table__stg_')

            delete_idx = next(i for i, s in enumerate(executed) if 'DELETE FROM iceberg.bronze.test_table ' in s)
            insert_idx = next(i for i, s in enumerate(executed) if 'INSERT INTO iceberg.bronze.test_table ' in s)
            # DELETE is the partition replace, immediately before the merge INSERT.
            assert "WHERE team = 'A' OR team = 'B'" in executed[delete_idx]
            assert delete_idx < insert_idx
            # Success → unique stage dropped once.
            mock_drop.assert_called_once_with('bronze', stage, if_exists=True)

    def test_single_statement_replace_uses_tombstone_merge_without_delete(self):
        """FBref typed partitions publish in one Iceberg snapshot.

        The live target is never exposed between a committed DELETE and a
        later INSERT: old scoped rows become tombstones in the stage and one
        MERGE both removes them and inserts the replacement rows.
        """
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            df = pd.DataFrame({'match_id': ['m1'], 'goals': [2]})

            with patch.object(manager, '_execute', side_effect=self._exec_ok(1)) as execute, \
                 patch.object(manager, 'insert_dataframe', return_value=1) as insert, \
                 patch.object(manager, 'drop_table') as drop:
                assert manager.insert_dataframe_atomic(
                    'bronze',
                    'fbref_match_team_stats',
                    df,
                    delete_filter="match_id = 'm1'",
                    single_statement_replace=True,
                ) == 1

            statements = [call.args[0] for call in execute.call_args_list]
            assert not any(sql.startswith('DELETE FROM') for sql in statements)
            assert any(
                sql.startswith('INSERT INTO iceberg.bronze.')
                and "'delete' FROM iceberg.bronze.fbref_match_team_stats"
                in sql
                for sql in statements
            )
            merge = next(sql for sql in statements if sql.startswith('MERGE INTO'))
            assert 'WHEN MATCHED THEN DELETE' in merge
            assert "WHEN NOT MATCHED AND s.\"__dpf_replace_op\" = 'insert'" in merge
            staged_frame = insert.call_args.args[2]
            assert staged_frame['__dpf_replace_op'].tolist() == ['insert']
            drop.assert_called_once()

    def test_atomic_stage_reuses_verified_target_column_types(self):
        """A unique stage needs no DESCRIBE when its target was just checked."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            df = pd.DataFrame({'match_id': ['m1'], 'goals': [2]})
            with patch.object(
                manager, '_execute', side_effect=self._exec_ok(1)
            ), patch.object(
                manager, 'insert_dataframe', return_value=1
            ) as insert, patch.object(manager, 'drop_table'):
                manager.insert_dataframe_atomic(
                    'bronze',
                    'fbref_match_team_stats',
                    df,
                    delete_filter="match_id = 'm1'",
                    single_statement_replace=True,
                    target_column_types={
                        'match_id': 'VARCHAR',
                        'goals': 'BIGINT',
                    },
                )

            assert insert.call_args.kwargs['column_types'] == {
                'match_id': 'VARCHAR',
                'goals': 'BIGINT',
                '__dpf_replace_op': 'VARCHAR',
            }

    def test_deterministic_stage_is_cleared_before_retry_and_after_success(self):
        """A retained phase-two stage cannot make an Airflow retry collide."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoError, TrinoTableManager

            manager = TrinoTableManager()
            df = pd.DataFrame({'control_run_id': ['run-1'], 'rows': [2]})
            merge_attempts = 0

            def execute(sql, fetch=False):
                nonlocal merge_attempts
                if fetch:
                    return [[1]]
                if sql.startswith('MERGE INTO'):
                    merge_attempts += 1
                    if merge_attempts == 1:
                        raise TrinoError('transient commit failure')
                return None

            with patch.object(manager, '_execute', side_effect=execute), \
                 patch.object(manager, 'insert_dataframe', return_value=1), \
                 patch.object(manager, 'drop_table') as drop:
                with pytest.raises(TrinoError, match='transient commit failure'):
                    manager.insert_dataframe_atomic(
                        'bronze',
                        'fbref_target_scope',
                        df,
                        delete_filter="control_run_id = 'run-1'",
                        staging_id='scope_retry_safe',
                        single_statement_replace=True,
                    )

                assert manager.insert_dataframe_atomic(
                    'bronze',
                    'fbref_target_scope',
                    df,
                    delete_filter="control_run_id = 'run-1'",
                    staging_id='scope_retry_safe',
                    single_statement_replace=True,
                ) == 1

            stage = 'fbref_target_scope__stg_scope_retry_safe'
            assert drop.call_args_list == [
                call('bronze', stage, if_exists=True),
                call('bronze', stage, if_exists=True),
                call('bronze', stage, if_exists=True),
            ]

    def test_replace_partitions_retains_stage_when_swap_insert_fails(self):
        """The #283/#314 wipe scenario: DELETE commits, the merge INSERT then
        fails. The stage MUST be retained (not dropped) so the rows survive, and
        the error MUST propagate (no silent empty table)."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager, TrinoError

            manager = TrinoTableManager()
            df = pd.DataFrame({'team': ['A', 'B'], 'goals': [1, 2]})

            def _exec(sql, fetch=False):
                if fetch:
                    return [[2]]
                if sql.lstrip().startswith('INSERT INTO'):
                    raise TrinoError("transient SSL error during merge")
                return None

            with patch.object(manager, '_execute', side_effect=_exec) as mock_execute, \
                 patch.object(manager, 'insert_dataframe', return_value=2), \
                 patch.object(manager, 'drop_table') as mock_drop:
                with pytest.raises(TrinoError):
                    manager.insert_dataframe_atomic(
                        'bronze', 'test_table', df, delete_filter="team = 'A'",
                    )

            executed = [c[0][0] for c in mock_execute.call_args_list]
            # DELETE was issued (the dangerous window opened)...
            assert any('DELETE FROM iceberg.bronze.test_table ' in s for s in executed)
            # ...and the unique recovery stage is retained.
            mock_drop.assert_not_called()

    def test_replace_partitions_drops_stage_when_staging_fails(self):
        """If staging fails before the swap, the target is untouched: no DELETE is
        issued and the half-built stage is cleaned up."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager, TrinoError

            manager = TrinoTableManager()
            df = pd.DataFrame({'team': ['A', 'B'], 'goals': [1, 2]})

            with patch.object(manager, '_execute', side_effect=self._exec_ok(2)) as mock_execute, \
                 patch.object(manager, 'insert_dataframe', side_effect=TrinoError("staging boom")), \
                 patch.object(manager, 'drop_table') as mock_drop:
                with pytest.raises(TrinoError):
                    manager.insert_dataframe_atomic(
                        'bronze', 'test_table', df, delete_filter="team = 'A'",
                    )

            executed = [c[0][0] for c in mock_execute.call_args_list]
            # Target never touched.
            assert not any('DELETE FROM iceberg.bronze.test_table ' in s for s in executed)
            assert not any('INSERT INTO iceberg.bronze.test_table ' in s for s in executed)
            # The unique half-built stage is cleaned up once.
            assert mock_drop.call_count == 1

    def test_replace_partitions_aborts_on_stage_count_mismatch(self):
        """If the staged row count != len(df), abort BEFORE the swap — no DELETE,
        stage dropped, error raised."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager, TrinoError

            manager = TrinoTableManager()
            df = pd.DataFrame({'team': ['A', 'B'], 'goals': [1, 2]})

            # Stage reports only 1 row though df has 2.
            with patch.object(manager, '_execute', side_effect=self._exec_ok(1)) as mock_execute, \
                 patch.object(manager, 'insert_dataframe', return_value=2), \
                 patch.object(manager, 'drop_table') as mock_drop:
                with pytest.raises(TrinoError):
                    manager.insert_dataframe_atomic(
                        'bronze', 'test_table', df, delete_filter="team = 'A'",
                    )

            executed = [c[0][0] for c in mock_execute.call_args_list]
            assert not any('DELETE FROM iceberg.bronze.test_table ' in s for s in executed)
            assert mock_drop.call_count == 1

    def test_merge_keys_emit_incremental_upsert(self):
        """Natural keys use MERGE and never delete/rewrite a partition.

        Two rows on purpose: a single-row frame takes the #951 fast path, and
        this test must keep pinning the STAGED merge shape deterministically.
        """
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            df = pd.DataFrame({
                'league': ['ENG-Premier League', 'ENG-Premier League'],
                'season': ['2526', '2526'],
                'match_id': ['123', '124'],
                'rating': [7.4, 6.9],
            })
            with patch.object(manager, '_execute', side_effect=self._exec_ok(2)) as execute, \
                 patch.object(manager, 'insert_dataframe', return_value=2), \
                 patch.object(manager, 'drop_table'):
                assert manager.insert_dataframe_atomic(
                    'bronze', 'sofascore_player_ratings', df,
                    merge_keys=['league', 'season', 'match_id'],
                ) == 2

            statements = [call.args[0] for call in execute.call_args_list]
            merge = next(sql for sql in statements if sql.startswith('MERGE INTO'))
            assert 't."league" = s."league"' in merge
            assert 't."season" = s."season"' in merge
            assert 't."match_id" = s."match_id"' in merge
            assert 'WHEN MATCHED THEN UPDATE SET' in merge
            assert 'WHEN NOT MATCHED THEN INSERT' in merge
            assert not any(sql.startswith('DELETE FROM') for sql in statements)
            # Regression (#951): a Trino MERGE forbids qualifying the SET target
            # column with the target alias. `SET t."col" = ...` raised
            # SYNTAX_ERROR ("mismatched input '.'. Expecting: '='") against the
            # live warehouse and blocked every SofaScore manifest upsert. The ON
            # clause legally qualifies with `t."`; the SET clause must not.
            set_clause = merge.split(
                'WHEN MATCHED THEN UPDATE SET ', 1
            )[1].split(' WHEN NOT MATCHED', 1)[0]
            assert '"rating" = s."rating"' in set_clause
            assert 't."' not in set_clause

    # ------------------------------------------------------------------
    # Single-row MERGE fast path (#951): one statement, no staging table.
    # ------------------------------------------------------------------

    _MANIFEST_DESCRIBE = [
        ('league', 'varchar', '', ''),
        ('season', 'varchar', '', ''),
        ('match_id', 'varchar', '', ''),
        ('rating', 'bigint', '', ''),
    ]

    @staticmethod
    def _exec_fast(describe_rows, merge_error=None, staged_count=1):
        """_execute side_effect for fast-path tests: answers DESCRIBE with
        real 4-tuples, optionally raises on the fast-path MERGE, and returns
        a row count for any other fetch (staged count probe)."""
        def _exec(sql, fetch=False, params=None):
            if sql.startswith('DESCRIBE'):
                return describe_rows
            if (merge_error is not None and sql.startswith('MERGE INTO')
                    and 'USING (SELECT' in sql):
                raise merge_error
            if fetch:
                return [[staged_count]]
            return None
        return _exec

    def _single_row_frame(self):
        import pandas as pd
        return pd.DataFrame({
            'league': ['ENG-Premier League'],
            'season': ['2526'],
            'match_id': ['123'],
            'rating': [7],
        })

    def test_single_row_merge_keys_uses_one_statement_fast_path(self):
        """One-row upsert issues DESCRIBE + a single inline MERGE — no stage."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            with patch.object(
                    manager, '_execute',
                    side_effect=self._exec_fast(self._MANIFEST_DESCRIBE)) as execute, \
                 patch.object(manager, 'insert_dataframe') as insert, \
                 patch.object(manager, 'drop_table') as drop:
                assert manager.insert_dataframe_atomic(
                    'ops', 'sofascore_capture_manifest', self._single_row_frame(),
                    merge_keys=['league', 'season', 'match_id'],
                ) == 1

            insert.assert_not_called()
            drop.assert_not_called()
            statements = [call.args[0] for call in execute.call_args_list]
            assert [s.split(None, 1)[0] for s in statements] == ['DESCRIBE', 'MERGE']
            merge = statements[1]
            assert 'USING (SELECT' in merge
            assert '__stg_' not in merge
            assert 't."league" = s."league"' in merge
            assert 't."season" = s."season"' in merge
            assert 't."match_id" = s."match_id"' in merge
            assert ('WHEN NOT MATCHED THEN INSERT '
                    '("league", "season", "match_id", "rating") '
                    'VALUES (s."league", s."season", s."match_id", s."rating")') in merge
            # Same #951 SET-qualification regression pin as the staged shape.
            set_clause = merge.split(
                'WHEN MATCHED THEN UPDATE SET ', 1
            )[1].split(' WHEN NOT MATCHED', 1)[0]
            assert '"rating" = s."rating"' in set_clause
            assert 't."' not in set_clause

    def test_single_row_merge_renders_quotes_none_and_int_types(self):
        """Literal rendering: quote doubling, typed NULL, exact int CASTs."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            describe = [
                ('name', 'varchar', '', ''),
                ('note', 'varchar', '', ''),
                ('attempts', 'integer', '', ''),
                ('row_count', 'bigint', '', ''),
            ]
            df = pd.DataFrame([{
                'name': "O'Brien\nLtd",
                'note': None,
                'attempts': 3,
                'row_count': 40000000000,
            }])
            with patch.object(
                    manager, '_execute',
                    side_effect=self._exec_fast(describe)) as execute, \
                 patch.object(manager, 'insert_dataframe'), \
                 patch.object(manager, 'drop_table'):
                manager.insert_dataframe_atomic(
                    'ops', 'scratch', df, merge_keys=['name'],
                )

            merge = next(
                call.args[0] for call in execute.call_args_list
                if call.args[0].startswith('MERGE INTO')
            )
            assert "O''Brien" in merge
            assert 'CAST(NULL AS varchar)' in merge
            assert 'CAST(3 AS INTEGER)' in merge
            assert 'CAST(40000000000 AS BIGINT)' in merge

    def test_multi_row_merge_keys_still_uses_staged_path(self):
        """Two rows keep the staged CREATE/INSERT/MERGE/DROP cycle."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            df = pd.DataFrame({'k': ['a', 'b'], 'v': [1, 2]})
            with patch.object(manager, '_execute', side_effect=self._exec_ok(2)) as execute, \
                 patch.object(manager, 'insert_dataframe', return_value=2) as insert, \
                 patch.object(manager, 'drop_table') as drop:
                assert manager.insert_dataframe_atomic(
                    'bronze', 'test_table', df, merge_keys=['k'],
                ) == 2

            insert.assert_called_once()
            stage = insert.call_args[0][1]
            assert stage.startswith('test_table__stg_')
            drop.assert_called_once_with('bronze', stage, if_exists=True)
            statements = [call.args[0] for call in execute.call_args_list]
            assert any('CREATE TABLE' in s and 'WHERE false' in s for s in statements)
            assert any(
                s.startswith('MERGE INTO') and f'USING iceberg.bronze.{stage}' in s
                for s in statements
            )

    def test_single_row_fast_path_falls_back_on_unexpected_error(self):
        """A non-conflict MERGE error degrades to the staged path and
        invalidates the type cache (next upsert re-DESCRIBEs)."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager, TrinoError

            manager = TrinoTableManager()
            error = TrinoError('TYPE_MISMATCH: cannot cast')
            with patch.object(
                    manager, '_execute',
                    side_effect=self._exec_fast(
                        self._MANIFEST_DESCRIBE, merge_error=error)) as execute, \
                 patch.object(manager, 'insert_dataframe', return_value=1) as insert, \
                 patch.object(manager, 'drop_table') as drop:
                assert manager.insert_dataframe_atomic(
                    'ops', 'sofascore_capture_manifest', self._single_row_frame(),
                    merge_keys=['league', 'season', 'match_id'],
                ) == 1

            statements = [call.args[0] for call in execute.call_args_list]
            fast = [s for s in statements
                    if s.startswith('MERGE INTO') and 'USING (SELECT' in s]
            staged = [s for s in statements
                      if s.startswith('MERGE INTO') and '__stg_' in s]
            assert len(fast) == 1 and len(staged) == 1
            insert.assert_called_once()
            drop.assert_called_once()
            assert ('ops', 'sofascore_capture_manifest') not in manager._merge_type_cache

    def test_single_row_fast_path_propagates_commit_conflict(self):
        """Commit-conflict exhaustion re-raises — the staged MERGE would face
        the same contention. Routed through _execute_committing's retry loop."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager, TrinoError

            manager = TrinoTableManager()
            conflict = TrinoError(
                'Failed to commit the transaction during write: conflicting files'
            )
            with patch.object(
                    manager, '_execute',
                    side_effect=self._exec_fast(
                        self._MANIFEST_DESCRIBE, merge_error=conflict)) as execute, \
                 patch.object(manager, 'insert_dataframe') as insert, \
                 patch.object(manager, 'drop_table'), \
                 patch('scrapers.base.trino_manager.time.sleep'):
                with pytest.raises(TrinoError):
                    manager.insert_dataframe_atomic(
                        'ops', 'sofascore_capture_manifest',
                        self._single_row_frame(),
                        merge_keys=['league', 'season', 'match_id'],
                    )

            insert.assert_not_called()
            merges = [call.args[0] for call in execute.call_args_list
                      if call.args[0].startswith('MERGE INTO')]
            assert len(merges) == manager._COMMIT_RETRIES

    def test_single_row_fast_path_holds_commit_lock(self):
        """The inline MERGE still runs under _COMMIT_LOCK via _execute_committing."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base import trino_manager as tm
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            locked_during_merge = []

            def _exec(sql, fetch=False, params=None):
                if sql.startswith('DESCRIBE'):
                    return self._MANIFEST_DESCRIBE
                if sql.startswith('MERGE INTO'):
                    locked_during_merge.append(tm._COMMIT_LOCK.locked())
                return [[1]] if fetch else None

            with patch.object(manager, '_execute', side_effect=_exec), \
                 patch.object(manager, 'insert_dataframe'), \
                 patch.object(manager, 'drop_table'):
                manager.insert_dataframe_atomic(
                    'ops', 'sofascore_capture_manifest', self._single_row_frame(),
                    merge_keys=['league', 'season', 'match_id'],
                )

            assert locked_during_merge == [True]

    def test_single_row_fast_path_skips_unsupported_types(self):
        """A non-scalar target column forces the staged path."""
        import pandas as pd
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            describe = [('k', 'varchar', '', ''), ('payload', 'array(varchar)', '', '')]
            df = pd.DataFrame([{'k': 'a', 'payload': 'x'}])

            def _exec(sql, fetch=False, params=None):
                if sql.startswith('DESCRIBE'):
                    return describe
                return [[1]] if fetch else None

            with patch.object(manager, '_execute', side_effect=_exec) as execute, \
                 patch.object(manager, 'insert_dataframe', return_value=1) as insert, \
                 patch.object(manager, 'drop_table'):
                assert manager.insert_dataframe_atomic(
                    'bronze', 'test_table', df, merge_keys=['k'],
                ) == 1

            insert.assert_called_once()
            assert not any(
                'USING (SELECT' in call.args[0] for call in execute.call_args_list
            )

    def test_single_row_fast_path_respects_staging_id(self):
        """A caller-owned staging_id keeps retry-stable staged semantics."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            with patch.object(manager, '_execute', side_effect=self._exec_ok(1)) as execute, \
                 patch.object(manager, 'insert_dataframe', return_value=1) as insert, \
                 patch.object(manager, 'drop_table'):
                assert manager.insert_dataframe_atomic(
                    'ops', 'sofascore_capture_manifest', self._single_row_frame(),
                    merge_keys=['league', 'season', 'match_id'],
                    staging_id='run1_task1_try1',
                ) == 1

            insert.assert_called_once()
            assert insert.call_args[0][1] == (
                'sofascore_capture_manifest__stg_run1_task1_try1'
            )
            assert not any(
                'USING (SELECT' in call.args[0] for call in execute.call_args_list
            )

    def test_single_row_fast_path_caches_describe_across_calls(self):
        """Two upserts issue exactly one DESCRIBE and two inline MERGEs."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()
            with patch.object(
                    manager, '_execute',
                    side_effect=self._exec_fast(self._MANIFEST_DESCRIBE)) as execute, \
                 patch.object(manager, 'insert_dataframe'), \
                 patch.object(manager, 'drop_table'):
                for _ in range(2):
                    manager.insert_dataframe_atomic(
                        'ops', 'sofascore_capture_manifest',
                        self._single_row_frame(),
                        merge_keys=['league', 'season', 'match_id'],
                    )

            heads = [call.args[0].split(None, 1)[0] for call in execute.call_args_list]
            assert heads == ['DESCRIBE', 'MERGE', 'MERGE']

    @pytest.mark.parametrize(
        ('frame', 'keys', 'message'),
        [
            (pd.DataFrame({'id': [1, 1]}), ['id'], 'duplicate rows'),
            (pd.DataFrame({'id': [None]}), ['id'], 'contains null'),
            (pd.DataFrame({'id': [1]}), ['missing'], 'absent from DataFrame'),
        ],
    )
    def test_merge_keys_fail_closed(self, frame, keys, message):
        from scrapers.base.trino_manager import TrinoTableManager

        manager = TrinoTableManager()
        with pytest.raises(ValueError, match=message):
            manager.insert_dataframe_atomic(
                'bronze', 'target', frame, merge_keys=keys,
            )

    def test_merge_keys_cannot_mix_with_partition_delete(self):
        import pandas as pd
        from scrapers.base.trino_manager import TrinoTableManager

        manager = TrinoTableManager()
        with pytest.raises(ValueError, match='mutually exclusive'):
            manager.insert_dataframe_atomic(
                'bronze', 'target', pd.DataFrame({'id': [1]}),
                delete_filter='id = 1', merge_keys=['id'],
            )


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


class TestExecuteDdlErrorPropagation:
    """Tests for _execute(fetch=False) error propagation (#456).

    Trino surfaces runtime failures (ICEBERG_COMMIT_ERROR, OOM, dead worker)
    while polling results — i.e. inside cursor.fetchall(). Swallowing them
    turns a failed DELETE/INSERT into a silent success and lets
    insert_dataframe_atomic drop the only copy of the data."""

    def test_execute_raises_when_dml_poll_fails(self):
        """A Trino error raised during the consuming fetchall() must propagate
        as TrinoError, not be swallowed into a silent success."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager, TrinoError

            manager = TrinoTableManager()
            mock_cursor = MagicMock()
            mock_cursor.fetchall.side_effect = Exception(
                "ICEBERG_COMMIT_ERROR: Failed to commit during write"
            )
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            with pytest.raises(TrinoError, match="ICEBERG_COMMIT_ERROR"):
                manager._execute(
                    "DELETE FROM iceberg.bronze.test_table WHERE league = 'X'"
                )

    def test_execute_preserves_cause_chain_on_poll_fail(self):
        """The original error stays on __cause__, so the iceberg_writer
        metadata-recovery path also sees poll-phase metadata errors."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import (
                TrinoTableManager, TrinoError, _is_iceberg_invalid_metadata,
            )

            poll_error = Exception("Error accessing metadata file")
            poll_error.error_name = 'ICEBERG_INVALID_METADATA'

            manager = TrinoTableManager()
            mock_cursor = MagicMock()
            mock_cursor.fetchall.side_effect = poll_error
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            manager._conn = mock_conn

            with pytest.raises(TrinoError) as exc_info:
                manager._execute("DROP TABLE iceberg.bronze.test_table")

            assert exc_info.value.__cause__ is poll_error
            assert _is_iceberg_invalid_metadata(exc_info.value) is True

    def test_execute_retries_on_connection_error_during_fetch(self):
        """A connection error during the consuming fetchall() goes through the
        existing reset-and-retry-once path instead of being swallowed."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager

            manager = TrinoTableManager()

            mock_cursor_bad = MagicMock()
            mock_cursor_bad.fetchall.side_effect = Exception("Connection reset")
            mock_conn1 = MagicMock()
            mock_conn1.cursor.return_value = mock_cursor_bad

            mock_cursor_good = MagicMock()
            mock_cursor_good.fetchall.return_value = []
            mock_conn2 = MagicMock()
            mock_conn2.cursor.return_value = mock_cursor_good

            manager._conn = mock_conn1

            with patch.object(manager, '_connect_with_retry') as mock_retry:
                def set_good_conn():
                    manager._conn = mock_conn2
                mock_retry.side_effect = set_good_conn

                result = manager._execute("DELETE FROM iceberg.bronze.test_table")

            assert result is None
            mock_retry.assert_called_once()
            mock_cursor_good.fetchall.assert_called_once()


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


class TestFormatSqlValueHardening:
    """_format_sql_value: BOOLEAN string parsing + DATE/TIMESTAMP quote escaping (#470 bug 4)."""

    @staticmethod
    def _manager():
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base.trino_manager import TrinoTableManager
            return TrinoTableManager()

    def test_boolean_string_false_renders_false(self):
        """String 'False' (a known SofaScore format) must NOT become TRUE via
        Python truthiness — that was silent data corruption."""
        m = self._manager()
        assert m._format_sql_value("False", "BOOLEAN") == "FALSE"
        assert m._format_sql_value("false", "BOOLEAN") == "FALSE"
        assert m._format_sql_value("0", "BOOLEAN") == "FALSE"

    def test_boolean_string_true_renders_true(self):
        m = self._manager()
        assert m._format_sql_value("True", "BOOLEAN") == "TRUE"
        assert m._format_sql_value("true", "BOOLEAN") == "TRUE"
        assert m._format_sql_value("1", "BOOLEAN") == "TRUE"

    def test_boolean_native_bool_unchanged(self):
        m = self._manager()
        assert m._format_sql_value(True, "BOOLEAN") == "TRUE"
        assert m._format_sql_value(False, "BOOLEAN") == "FALSE"

    def test_boolean_unrecognized_string_is_null(self):
        """Unrecognized values must not be silently coerced to a boolean."""
        m = self._manager()
        assert m._format_sql_value("maybe", "BOOLEAN") == "NULL"

    def test_date_string_with_quote_is_escaped(self):
        """A stray single quote in a DATE value must be doubled, not break SQL."""
        m = self._manager()
        val = "2024-01-01' OR '1'='1"
        assert m._format_sql_value(val, "DATE") == "DATE '" + val.replace("'", "''") + "'"

    def test_timestamp_string_with_quote_is_escaped(self):
        m = self._manager()
        val = "2024-01-01 00:00:00' x"
        assert m._format_sql_value(val, "TIMESTAMP") == "TIMESTAMP '" + val.replace("'", "''") + "'"

    def test_timestamp_iso_utc_string_is_normalized_for_trino(self):
        m = self._manager()
        assert m._format_sql_value(
            "2025-08-22T18:30:00Z", "TIMESTAMP(6)"
        ) == "TIMESTAMP '2025-08-22 18:30:00.000000'"

    def test_timestamp_offset_is_normalized_to_utc(self):
        m = self._manager()
        assert m._format_sql_value(
            "2025-08-22T20:30:00+02:00", "TIMESTAMP(6)"
        ) == "TIMESTAMP '2025-08-22 18:30:00.000000'"

    def test_timestamp_with_time_zone_preserves_instant(self):
        m = self._manager()
        assert m._format_sql_value(
            "2025-08-22T20:30:00+02:00", "TIMESTAMP(6) WITH TIME ZONE"
        ) == "TIMESTAMP '2025-08-22 18:30:00.000000 UTC'"

    def test_naive_timestamp_with_time_zone_is_explicitly_utc(self):
        m = self._manager()
        assert m._format_sql_value(
            "2025-08-22 18:30:00", "TIMESTAMP WITH TIME ZONE"
        ) == "TIMESTAMP '2025-08-22 18:30:00.000000 UTC'"


class TestTrinoManagerRestartResilience:
    """#847: a Trino container restart takes ~30-60s (SERVER STARTED ~13s +
    authenticator warm-up), but the old 3-attempt/18s connect window gave up
    mid-restart — a backfill that hit the window lost its bronze write (APL
    16/17, #842). The connect window must outlast a full restart, and the
    warm-up error ("authenticators were not loaded") must reset the
    connection exactly like a network-level connection error."""

    def _manager(self):
        from scrapers.base.trino_manager import TrinoTableManager
        TrinoTableManager._trino_unreachable = False  # isolate class-level cache
        return TrinoTableManager()

    def test_connect_retry_window_survives_restart(self):
        """Cumulative backoff across connect attempts covers >=90s."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            from scrapers.base import trino_manager as tm_mod

            # Arrange — every attempt but the last dies on a refused socket.
            manager = self._manager()
            failures = manager._CONNECT_RETRIES - 1
            attempts = {'n': 0}

            def _flaky():
                attempts['n'] += 1
                if attempts['n'] <= failures:
                    raise ConnectionError('Connection refused')
                return MagicMock()

            sleeps = []
            # Act
            with patch.object(manager, '_create_connection', side_effect=_flaky), \
                 patch.object(tm_mod.time, 'sleep', side_effect=sleeps.append):
                manager._connect_with_retry()

            # Assert — connection recovered and total wait outlasts a restart.
            assert manager._conn is not None
            assert sum(sleeps) >= 90

    def test_authenticators_not_loaded_resets_connection(self):
        """The Trino warm-up 500 resets the connection and _execute retries."""
        with patch.dict('sys.modules', {'trino': MagicMock(), 'trino.dbapi': MagicMock()}):
            # Arrange — first cursor hits the warm-up 500, reconnect succeeds.
            manager = self._manager()

            warm_cursor = MagicMock()
            warm_cursor.execute.side_effect = Exception(
                'TrinoQueryError: error 500: authenticators were not loaded')
            warm_conn = MagicMock()
            warm_conn.cursor.return_value = warm_cursor

            ok_cursor = MagicMock()
            ok_cursor.fetchall.return_value = []
            ok_conn = MagicMock()
            ok_conn.cursor.return_value = ok_cursor

            manager._conn = warm_conn
            reconnect = MagicMock(
                side_effect=lambda: setattr(manager, '_conn', ok_conn))

            # Act
            with patch.object(manager, '_connect_with_retry', reconnect):
                manager._execute('SELECT 1')

            # Assert — reconnected once, statement re-ran on the new connection.
            reconnect.assert_called_once()
            ok_cursor.execute.assert_called_once_with('SELECT 1')
