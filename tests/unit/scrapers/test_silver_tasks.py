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

        # 3 DDL consume calls + 1 COUNT fetch
        mock_conn, mock_cursor = self._make_conn(
            fetchall_side_effect=[[], [], [], [[42]]]
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
        assert mock_cursor.execute.call_count == 4  # CREATE SCHEMA + DROP + CTAS + COUNT
        mock_conn.close.assert_called_once()

    def test_ctas_sql_contains_partition(self, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1 AS league, 2 AS season")

        mock_conn, mock_cursor = self._make_conn(
            fetchall_side_effect=[[], [], [], [[1]]]
        )

        self._run_transform(mock_conn, sql_file, table_name='t', trino_host='localhost')

        # 3rd execute call is CTAS (after CREATE SCHEMA and DROP TABLE)
        ctas_sql = mock_cursor.execute.call_args_list[2][0][0]
        assert "CREATE TABLE iceberg.silver.t" in ctas_sql
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
        # First execute (CREATE SCHEMA) succeeds, second (DROP TABLE) fails
        mock_cursor.execute.side_effect = [None, Exception("Trino connection failed")]

        with pytest.raises(RuntimeError, match="Silver transform failed"):
            self._run_transform(mock_conn, sql_file, table_name='t', trino_host='localhost')

        mock_conn.close.assert_called_once()

    def test_trailing_semicolon_stripped(self, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1;")

        mock_conn, mock_cursor = self._make_conn(
            fetchall_side_effect=[[], [], [], [[1]]]
        )

        self._run_transform(mock_conn, sql_file, table_name='t', trino_host='localhost')

        # 3rd execute call is CTAS
        ctas_sql = mock_cursor.execute.call_args_list[2][0][0]
        assert not ctas_sql.rstrip().endswith(';')

    def test_sql_file_not_found(self):
        mod = _import_silver_tasks()
        with pytest.raises(FileNotFoundError):
            mod.run_silver_transform(
                sql_file='/nonexistent/test.sql',
                table_name='t',
                trino_host='localhost',
            )


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


class TestValidateSilverQuality:
    """Tests for validate_silver_quality function."""

    def _make_conn(self, fetchall_side_effect=None):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        if fetchall_side_effect is not None:
            mock_cursor.fetchall.side_effect = fetchall_side_effect
        return mock_conn, mock_cursor

    def test_null_rate_passes(self):
        mod = _import_silver_tasks()
        # total=100, nulls=2 → 2% NULL, threshold 5% → pass
        mock_conn, _ = self._make_conn(fetchall_side_effect=[[[100, 2]]])

        checks = [
            {'type': 'null_rate', 'table': 'fbref_player_season_profile', 'column': 'mp', 'max_null_pct': 5.0},
        ]
        with patch.object(mod, '_get_trino_connection', return_value=mock_conn):
            result = mod.validate_silver_quality(checks=checks)

        assert result['passed'] == 1
        assert result['warnings'] == []

    def test_null_rate_warns(self):
        mod = _import_silver_tasks()
        # total=100, nulls=10 → 10% NULL, threshold 5% → warning
        mock_conn, _ = self._make_conn(fetchall_side_effect=[[[100, 10]]])

        checks = [
            {'type': 'null_rate', 'table': 'fbref_player_season_profile', 'column': 'mp', 'max_null_pct': 5.0},
        ]
        with patch.object(mod, '_get_trino_connection', return_value=mock_conn):
            result = mod.validate_silver_quality(checks=checks)

        assert result['passed'] == 0
        assert len(result['warnings']) == 1
        assert '10.0% NULL' in result['warnings'][0]

    def test_ref_integrity_passes(self):
        mod = _import_silver_tasks()
        # 0 orphan keys → pass
        mock_conn, _ = self._make_conn(fetchall_side_effect=[[[0]]])

        checks = [
            {'type': 'ref_integrity', 'child': 'fbref_player_match_stats', 'parent': 'fbref_match_enriched', 'key': 'match_id'},
        ]
        with patch.object(mod, '_get_trino_connection', return_value=mock_conn):
            result = mod.validate_silver_quality(checks=checks)

        assert result['passed'] == 1
        assert result['warnings'] == []

    def test_ref_integrity_warns(self):
        mod = _import_silver_tasks()
        # 5 orphan keys → warning
        mock_conn, _ = self._make_conn(fetchall_side_effect=[[[5]]])

        checks = [
            {'type': 'ref_integrity', 'child': 'fbref_player_match_stats', 'parent': 'fbref_match_enriched', 'key': 'match_id'},
        ]
        with patch.object(mod, '_get_trino_connection', return_value=mock_conn):
            result = mod.validate_silver_quality(checks=checks)

        assert result['passed'] == 0
        assert len(result['warnings']) == 1
        assert '5 orphan' in result['warnings'][0]

    def test_range_passes(self):
        mod = _import_silver_tasks()
        # 0 violations → pass
        mock_conn, _ = self._make_conn(fetchall_side_effect=[[[0]]])

        checks = [
            {'type': 'range', 'table': 'fbref_player_season_profile', 'column': 'goals', 'min': 0},
        ]
        with patch.object(mod, '_get_trino_connection', return_value=mock_conn):
            result = mod.validate_silver_quality(checks=checks)

        assert result['passed'] == 1
        assert result['warnings'] == []

    def test_range_warns(self):
        mod = _import_silver_tasks()
        # 3 violations → warning
        mock_conn, _ = self._make_conn(fetchall_side_effect=[[[3]]])

        checks = [
            {'type': 'range', 'table': 'fbref_keeper_profile', 'column': 'save_pct', 'min': 0, 'max': 100},
        ]
        with patch.object(mod, '_get_trino_connection', return_value=mock_conn):
            result = mod.validate_silver_quality(checks=checks)

        assert result['passed'] == 0
        assert len(result['warnings']) == 1
        assert '3 row(s)' in result['warnings'][0]

    def test_query_error_handled(self):
        mod = _import_silver_tasks()
        mock_conn, mock_cursor = self._make_conn()
        mock_cursor.execute.side_effect = Exception("Table not found")

        checks = [
            {'type': 'null_rate', 'table': 'fbref_player_season_profile', 'column': 'mp', 'max_null_pct': 5.0},
        ]
        with patch.object(mod, '_get_trino_connection', return_value=mock_conn):
            result = mod.validate_silver_quality(checks=checks)

        assert result['passed'] == 0
        assert len(result['errors']) == 1
        mock_conn.close.assert_called_once()

    def test_empty_table_warns(self):
        mod = _import_silver_tasks()
        # total=0, nulls=0 → empty table → warning
        mock_conn, _ = self._make_conn(fetchall_side_effect=[[[0, 0]]])

        checks = [
            {'type': 'null_rate', 'table': 'fbref_player_season_profile', 'column': 'mp', 'max_null_pct': 5.0},
        ]
        with patch.object(mod, '_get_trino_connection', return_value=mock_conn):
            result = mod.validate_silver_quality(checks=checks)

        assert len(result['warnings']) == 1
        assert 'empty' in result['warnings'][0]
