"""
Unit tests for Silver Transformation Tasks (dags/utils/silver_tasks.py).
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# dags/utils/__init__.py uses "from utils.config import ..." which only works
# inside the container where dags/ is on PYTHONPATH.  Add it here for host tests.
_dags_dir = str(Path(__file__).resolve().parents[3] / 'dags')
if _dags_dir not in sys.path:
    sys.path.insert(0, _dags_dir)


def _import_silver_tasks():
    """Import silver_tasks module."""
    import utils.silver_tasks as mod
    return mod


class TestResolveSqlPath:
    """Tests for _resolve_sql_path helper."""

    def test_absolute_path_exists(self, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1")

        mod = _import_silver_tasks()
        result = mod._resolve_sql_path(str(sql_file))
        assert result == sql_file

    def test_relative_path_resolves(self, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1")

        mod = _import_silver_tasks()
        result = mod._resolve_sql_path(str(sql_file))
        assert result == sql_file

    def test_path_not_found_raises(self):
        mod = _import_silver_tasks()
        with pytest.raises(FileNotFoundError, match="SQL file not found"):
            mod._resolve_sql_path("/nonexistent/path/to/file.sql")

    def test_relative_path_not_found_raises(self):
        mod = _import_silver_tasks()
        with pytest.raises(FileNotFoundError, match="SQL file not found"):
            mod._resolve_sql_path("nonexistent_dir/nonexistent_file.sql")


class TestRunSilverTransform:
    """Tests for run_silver_transform function."""

    def _run_transform(self, mock_conn, sql_file, **kwargs):
        """Helper: import module and call run_silver_transform with mocked Trino connection."""
        mod = _import_silver_tasks()
        with patch.object(mod, '_get_trino_connection', return_value=mock_conn):
            return mod.run_silver_transform(sql_file=str(sql_file), **kwargs)

    def _make_conn(self, fetchall_side_effect=None):
        """Create mock connection with cursor support."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        if fetchall_side_effect is not None:
            mock_cursor.fetchall.side_effect = fetchall_side_effect
        return mock_conn, mock_cursor

    def test_success(self, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT * FROM iceberg.bronze.test_table")

        # Atomic CREATE OR REPLACE (#265): SCHEMA + CREATE OR REPLACE consume
        # calls + 1 COUNT fetch
        mock_conn, mock_cursor = self._make_conn(
            fetchall_side_effect=[[], [], [[42]]]
        )

        result = self._run_transform(
            mock_conn, sql_file,
            table_name='test_table', schema='silver',
            trino_host='localhost', trino_port=8443,
        )

        assert result['status'] == 'success'
        assert result['rows'] == 42
        assert result['table'] == 'iceberg.silver.test_table'
        assert result['error'] is None
        # SCHEMA + CREATE OR REPLACE + COUNT
        assert mock_cursor.execute.call_count == 3
        mock_conn.close.assert_called_once()

    def test_ctas_sql_contains_partition(self, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1 AS league, 2 AS season")

        mock_conn, mock_cursor = self._make_conn(
            fetchall_side_effect=[[], [], [[1]]]
        )

        self._run_transform(mock_conn, sql_file, table_name='t', trino_host='localhost')

        # 2nd execute call is CREATE OR REPLACE (after CREATE SCHEMA)
        ctas_sql = mock_cursor.execute.call_args_list[1][0][0]
        assert "CREATE OR REPLACE TABLE iceberg.silver.t" in ctas_sql
        assert "partitioning = ARRAY['league', 'season']" in ctas_sql
        assert "SELECT 1 AS league, 2 AS season" in ctas_sql

    def test_empty_sql_raises(self, tmp_path):
        sql_file = tmp_path / "empty.sql"
        sql_file.write_text("")

        mock_conn, _ = self._make_conn()

        with pytest.raises(ValueError, match="SQL file is empty"):
            self._run_transform(mock_conn, sql_file, table_name='t', trino_host='localhost')

    def test_trino_failure_raises_runtime(self, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1")

        mock_conn, mock_cursor = self._make_conn()
        # First execute (CREATE SCHEMA) succeeds, second (CREATE OR REPLACE) fails
        mock_cursor.execute.side_effect = [None, Exception("Trino connection failed")]

        with pytest.raises(RuntimeError, match="Silver transform failed"):
            self._run_transform(mock_conn, sql_file, table_name='t', trino_host='localhost')

        mock_conn.close.assert_called_once()

    def test_trailing_semicolon_stripped(self, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1;")

        mock_conn, mock_cursor = self._make_conn(
            fetchall_side_effect=[[], [], [[1]]]
        )

        self._run_transform(mock_conn, sql_file, table_name='t', trino_host='localhost')

        # 2nd execute call is CREATE OR REPLACE
        ctas_sql = mock_cursor.execute.call_args_list[1][0][0]
        assert not ctas_sql.rstrip().endswith(';')

    def test_sql_file_not_found(self):
        mod = _import_silver_tasks()
        with pytest.raises(FileNotFoundError):
            mod.run_silver_transform(
                sql_file='/nonexistent/test.sql',
                table_name='t',
                trino_host='localhost',
            )

    def test_add_timestamp_true_injects_silver_created_at(self, tmp_path):
        """Default behaviour: CTAS wraps SELECT and injects _silver_created_at."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1 AS x")

        mock_conn, mock_cursor = self._make_conn(
            fetchall_side_effect=[[], [], [[1]]]
        )

        self._run_transform(
            mock_conn, sql_file,
            table_name='t', trino_host='localhost',
            add_timestamp=True,
        )

        ctas_sql = mock_cursor.execute.call_args_list[1][0][0]
        assert "CURRENT_TIMESTAMP AS _silver_created_at" in ctas_sql
        # Wrap-style: outer SELECT around the user SELECT
        assert "FROM (" in ctas_sql

    def test_add_timestamp_false_omits_silver_created_at(self, tmp_path):
        """Re-selecting a table that already carries _silver_created_at
        (SELECT m.*) must NOT inject the timestamp — it would raise
        DUPLICATE_COLUMN_NAME. Verifies the wrapper is bypassed entirely."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT m.* FROM iceberg.gold.fct_team_match m")

        mock_conn, mock_cursor = self._make_conn(
            fetchall_side_effect=[[], [], [[1]]]
        )

        self._run_transform(
            mock_conn, sql_file,
            table_name='t', trino_host='localhost',
            add_timestamp=False,
        )

        ctas_sql = mock_cursor.execute.call_args_list[1][0][0]
        assert "_silver_created_at" not in ctas_sql
        assert "CURRENT_TIMESTAMP" not in ctas_sql
        # The user SELECT is inlined directly (no FROM (...) wrapping)
        assert "SELECT m.* FROM iceberg.gold.fct_team_match m" in ctas_sql

    def test_add_timestamp_default_is_true(self, tmp_path):
        """Backward-compat: omitting add_timestamp behaves like add_timestamp=True."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1 AS x")

        mock_conn, mock_cursor = self._make_conn(
            fetchall_side_effect=[[], [], [[1]]]
        )

        # No add_timestamp kwarg → default
        self._run_transform(mock_conn, sql_file, table_name='t', trino_host='localhost')

        ctas_sql = mock_cursor.execute.call_args_list[1][0][0]
        assert "_silver_created_at" in ctas_sql

    def test_create_or_replace_failure_leaves_table_untouched(self, tmp_path):
        """#265: if the CREATE OR REPLACE fails, the live table must NOT be
        dropped — CREATE OR REPLACE is atomic, so there is no staging to clean
        up and no DROP is ever issued."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1")

        mock_conn, mock_cursor = self._make_conn()
        # SCHEMA(ok) → CREATE OR REPLACE(FAIL)
        mock_cursor.execute.side_effect = [None, Exception("SELECT exploded")]

        with pytest.raises(RuntimeError, match="Silver transform failed"):
            self._run_transform(mock_conn, sql_file, table_name='t', trino_host='localhost')

        executed = [c[0][0] for c in mock_cursor.execute.call_args_list]
        # No DROP is ever issued — neither the live table nor any staging table.
        assert not any(s.startswith("DROP TABLE") for s in executed)
        mock_conn.close.assert_called_once()

    def test_create_or_replace_single_statement(self, tmp_path):
        """#265: the rebuild emits a single CREATE OR REPLACE TABLE — no DROP,
        no RENAME, no staging table."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1 AS x")

        mock_conn, mock_cursor = self._make_conn(
            fetchall_side_effect=[[], [], [[1]]]
        )

        self._run_transform(mock_conn, sql_file, table_name='t', trino_host='localhost')

        executed = [c[0][0] for c in mock_cursor.execute.call_args_list]
        cor = [s for s in executed if "CREATE OR REPLACE TABLE iceberg.silver.t" in s]
        assert len(cor) == 1
        assert not any("RENAME TO" in s for s in executed)
        assert not any(s.startswith("DROP TABLE") for s in executed)
        assert not any("_new" in s for s in executed)

    def test_positional_schema_drift_triggers_drop_and_retry(self, tmp_path):
        """#741: when CREATE OR REPLACE trips the positional ICEBERG_COMMIT_ERROR
        (the SQL added/reordered columns vs the stale physical table), the runner
        auto-heals: DROP TABLE IF EXISTS, then re-run the same CTAS on empty
        ground (no positional match). Order must be CTAS(fail) → DROP → CTAS(ok)."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1 AS x")

        # fetchall consumed for: SCHEMA, DROP, retry-CTAS, then COUNT fetch.
        # (The failed CTAS raises in execute() before reaching fetchall.)
        mock_conn, mock_cursor = self._make_conn(
            fetchall_side_effect=[[], [], [], [[42]]]
        )
        drift = Exception(
            "TrinoQueryError(type=EXTERNAL, name=ICEBERG_COMMIT_ERROR, "
            "message=\"The following columns have types incompatible with the "
            "existing columns in their respective positions: foo, bar\")"
        )
        # SCHEMA(ok) → CREATE OR REPLACE(drift) → DROP(ok) → CREATE OR REPLACE(ok) → COUNT(ok)
        mock_cursor.execute.side_effect = [None, drift, None, None, None]

        result = self._run_transform(
            mock_conn, sql_file, table_name='t', trino_host='localhost',
        )

        assert result['status'] == 'success'
        assert result['rows'] == 42

        executed = [c[0][0] for c in mock_cursor.execute.call_args_list]
        cor_idx = [
            i for i, s in enumerate(executed)
            if "CREATE OR REPLACE TABLE iceberg.silver.t" in s
        ]
        drop_idx = [
            i for i, s in enumerate(executed)
            if s.startswith("DROP TABLE IF EXISTS iceberg.silver.t")
        ]
        # Two CREATE OR REPLACE (original + retry) with exactly one DROP between them.
        assert len(cor_idx) == 2, executed
        assert len(drop_idx) == 1, executed
        assert cor_idx[0] < drop_idx[0] < cor_idx[1]

    def test_generic_commit_error_does_not_drop(self, tmp_path):
        """#741 guard: a NON-positional ICEBERG_COMMIT_ERROR (e.g. a concurrent
        commit conflict) must NOT trigger the DROP+retry heal — dropping a live
        table on a transient conflict could destroy a table another writer needs.
        Only the 'respective positions' signature heals."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1 AS x")

        mock_conn, mock_cursor = self._make_conn()
        # SCHEMA(ok) → CREATE OR REPLACE(transient commit conflict, no positional sig)
        mock_cursor.execute.side_effect = [
            None,
            Exception("ICEBERG_COMMIT_ERROR: Failed to commit during write: "
                      "conflicting concurrent update"),
        ]

        with pytest.raises(RuntimeError, match="Silver transform failed"):
            self._run_transform(
                mock_conn, sql_file, table_name='t', trino_host='localhost',
            )

        executed = [c[0][0] for c in mock_cursor.execute.call_args_list]
        assert not any(s.startswith("DROP TABLE") for s in executed)


class TestValidateSilverTables:
    """Tests for validate_silver_tables function."""

    def _validate(self, mock_conn, tables, **kwargs):
        """Helper: import module and call validate_silver_tables with mocked Trino connection."""
        mod = _import_silver_tasks()
        with patch.object(mod, '_get_trino_connection', return_value=mock_conn):
            return mod.validate_silver_tables(tables, **kwargs)

    def _make_conn(self, fetchall_side_effect=None, execute_side_effect=None):
        """Create mock connection with cursor support."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        if fetchall_side_effect is not None:
            mock_cursor.fetchall.side_effect = fetchall_side_effect
        if execute_side_effect is not None:
            mock_cursor.execute.side_effect = execute_side_effect
        return mock_conn, mock_cursor

    def test_all_tables_pass(self):
        mock_conn, _ = self._make_conn(fetchall_side_effect=[[[100]], [[50]]])

        result = self._validate(mock_conn, {
            'fbref_player_season_profile': 10,
            'fbref_keeper_profile': 5,
        })

        assert result['status'] == 'success'
        assert result['warnings'] == []
        assert result['details']['fbref_player_season_profile'] == 100
        assert result['details']['fbref_keeper_profile'] == 50
        assert result['total_rows'] == 150
        mock_conn.close.assert_called_once()

    def test_below_threshold_warns(self):
        mock_conn, _ = self._make_conn(fetchall_side_effect=[[[100]], [[0]]])

        result = self._validate(mock_conn, {
            'fbref_player_season_profile': 10,
            'fbref_keeper_profile': 5,
        })

        assert result['status'] == 'partial_success'
        assert len(result['warnings']) == 1
        assert 'fbref_keeper_profile' in result['warnings'][0]
        assert result['total_rows'] == 100

    def test_query_failure_handled(self):
        # First query succeeds, second fails at execute
        mock_conn, _ = self._make_conn(
            fetchall_side_effect=[[[100]]],
            execute_side_effect=[None, Exception("Table not found")],
        )

        result = self._validate(mock_conn, {
            'fbref_player_season_profile': 10,
            'fbref_keeper_profile': 5,
        })

        assert result['status'] == 'partial_success'
        assert result['details']['fbref_keeper_profile'] == -1
        assert any('query failed' in w for w in result['warnings'])


class TestCheckBronzeTableExists:
    """Tests for check_bronze_table_exists function."""

    def _make_conn(self, fetchall_return=None):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        if fetchall_return is not None:
            mock_cursor.fetchall.return_value = fetchall_return
        return mock_conn, mock_cursor

    def test_table_exists(self):
        mod = _import_silver_tasks()
        mock_conn, _ = self._make_conn(fetchall_return=[['fbref_shot_events']])

        with patch.object(mod, '_get_trino_connection', return_value=mock_conn):
            result = mod.check_bronze_table_exists('fbref_shot_events')

        assert result is True
        mock_conn.close.assert_called_once()

    def test_table_not_exists(self):
        mod = _import_silver_tasks()
        mock_conn, _ = self._make_conn(fetchall_return=[])

        with patch.object(mod, '_get_trino_connection', return_value=mock_conn):
            result = mod.check_bronze_table_exists('fbref_shot_events')

        assert result is False
        mock_conn.close.assert_called_once()

    def test_connection_failure_returns_false(self):
        mod = _import_silver_tasks()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Connection refused")

        with patch.object(mod, '_get_trino_connection', return_value=mock_conn):
            result = mod.check_bronze_table_exists('fbref_shot_events')

        assert result is False
