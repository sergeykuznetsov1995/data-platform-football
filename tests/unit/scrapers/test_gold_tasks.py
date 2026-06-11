"""
Unit tests for Gold Transformation Tasks (dags/utils/gold_tasks.py).

Covers:
  * run_gold_transform fallback / require_silver graceful-degrade flow.
  * validate_gold_quality module imports cleanly without a live Trino.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# dags/utils/__init__.py uses "from utils.config import ..." which only works
# inside the container where dags/ is on PYTHONPATH. Add it here for host tests.
_dags_dir = str(Path(__file__).resolve().parents[3] / 'dags')
if _dags_dir not in sys.path:
    sys.path.insert(0, _dags_dir)


def _import_gold_tasks():
    import utils.gold_tasks as mod
    return mod


class TestModuleImports:
    """gold_tasks must import without a live Trino."""

    def test_validate_gold_quality_importable(self):
        """validate_gold_quality is callable + the symbol exists. Don't run
        it (needs Trino) — just assert no ImportError on the module."""
        mod = _import_gold_tasks()
        assert callable(mod.validate_gold_quality)
        assert callable(mod.run_gold_transform)


class TestRunGoldTransformFallback:
    """fallback_sql_file + require_silver: graceful degrade when an optional
    Silver dependency (e.g. whoscored_player_unavailable) is missing."""

    def test_no_fallback_when_all_silver_present(self, tmp_path):
        mod = _import_gold_tasks()
        sql_file = tmp_path / "main.sql"
        sql_file.write_text("SELECT 1 AS x")
        fb_file = tmp_path / "fb.sql"
        fb_file.write_text("SELECT 2 AS x")

        with patch.object(mod, 'check_bronze_table_exists', return_value=True) as mock_exists, \
             patch.object(mod, 'run_silver_transform',
                          return_value={'status': 'success', 'rows': 10}) as mock_run:
            result = mod.run_gold_transform(
                sql_file=str(sql_file),
                table_name='fct_player_unavailable',
                fallback_sql_file=str(fb_file),
                require_silver=['whoscored_player_unavailable'],
            )

        # All required tables present → main sql_file used, no fallback flag
        assert 'fallback' not in result
        mock_run.assert_called_once()
        assert mock_run.call_args.kwargs['sql_file'] == str(sql_file)
        assert mock_run.call_args.kwargs['schema'] == 'gold'
        mock_exists.assert_called_once_with(
            table_name='whoscored_player_unavailable', schema='silver',
        )

    def test_fallback_used_when_silver_missing(self, tmp_path):
        mod = _import_gold_tasks()
        sql_file = tmp_path / "main.sql"
        sql_file.write_text("SELECT 1 AS x")
        fb_file = tmp_path / "fb.sql"
        fb_file.write_text("SELECT NULL AS x")

        # check_bronze_table_exists returns False → fallback path
        with patch.object(mod, 'check_bronze_table_exists', return_value=False), \
             patch.object(mod, 'run_silver_transform',
                          return_value={'status': 'success', 'rows': 0}) as mock_run:
            result = mod.run_gold_transform(
                sql_file=str(sql_file),
                table_name='fct_player_unavailable',
                fallback_sql_file=str(fb_file),
                require_silver=['whoscored_player_unavailable'],
            )

        # Fallback fired → result carries fallback=True + reason
        assert result.get('fallback') is True
        assert 'whoscored_player_unavailable' in result.get('fallback_reason', '')
        # run_silver_transform was called with the FALLBACK SQL, schema=gold
        mock_run.assert_called_once()
        assert mock_run.call_args.kwargs['sql_file'] == str(fb_file)
        assert mock_run.call_args.kwargs['schema'] == 'gold'

    def test_no_check_when_fallback_args_omitted(self, tmp_path):
        """fallback_sql_file=None → backward-compat path: no Silver check."""
        mod = _import_gold_tasks()
        sql_file = tmp_path / "main.sql"
        sql_file.write_text("SELECT 1 AS x")

        with patch.object(mod, 'check_bronze_table_exists') as mock_exists, \
             patch.object(mod, 'run_silver_transform',
                          return_value={'status': 'success', 'rows': 5}) as mock_run:
            result = mod.run_gold_transform(
                sql_file=str(sql_file),
                table_name='dim_team',
            )

        # Existence check must NOT be called when fallback isn't configured
        mock_exists.assert_not_called()
        assert 'fallback' not in result
        mock_run.assert_called_once()
        assert mock_run.call_args.kwargs['schema'] == 'gold'

    def test_add_timestamp_propagated_to_silver_transform(self, tmp_path):
        """add_timestamp must be forwarded — callers re-selecting a table
        that already carries _silver_created_at pass False."""
        mod = _import_gold_tasks()
        sql_file = tmp_path / "main.sql"
        sql_file.write_text("SELECT m.* FROM iceberg.silver.some_table m")

        with patch.object(mod, 'run_silver_transform',
                          return_value={'status': 'success', 'rows': 1}) as mock_run:
            mod.run_gold_transform(
                sql_file=str(sql_file),
                table_name='some_table_copy',
                add_timestamp=False,
            )

        assert mock_run.call_args.kwargs['add_timestamp'] is False
