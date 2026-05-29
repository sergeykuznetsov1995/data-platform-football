"""
Unit tests for ``dags/dag_ingest_sofifa.py`` ``validate_data`` hardening.

Background: 2026-05-13 audit (``docs/bronze_audit_2026-05-13.md`` §E) found
the validator silently passed when 0 rows were scraped. After CF blocks /
``tab crashed`` events SoFIFA frequently returns empty Bronze, and the
old logic only emitted a warning. The DAG appeared green while Bronze
was stale for days.

These tests pin the new contract:
  - players_rows == 0 AND teams_rows == 0  → raises AirflowException
  - players_rows > 0, partial: only warnings, never raises
"""

from __future__ import annotations

import importlib
import json
import sys

import pytest


def _reload_dag_module():
    """Force a fresh import of the SoFIFA DAG module."""
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()

    sys.modules.pop("dag_ingest_sofifa", None)
    sys.modules.pop("dags.dag_ingest_sofifa", None)

    return importlib.import_module("dag_ingest_sofifa")


@pytest.fixture
def dag_module():
    """Reload and return the dag_ingest_sofifa module."""
    return _reload_dag_module()


@pytest.fixture
def write_result(tmp_path, monkeypatch):
    """Factory: write a fake /tmp/sofifa_result.json with given contents."""
    def _write(payload: dict):
        result_path = tmp_path / "sofifa_result.json"
        result_path.write_text(json.dumps(payload))
        # Patch the hardcoded /tmp path the DAG opens.
        original_open = open

        def _patched_open(file, *a, **kw):
            if file == "/tmp/sofifa_result.json":
                return original_open(result_path, *a, **kw)
            return original_open(file, *a, **kw)

        monkeypatch.setattr("builtins.open", _patched_open)
        return result_path

    return _write


class TestSofifaValidateData:
    @pytest.mark.unit
    def test_zero_rows_raises_airflow_exception(self, dag_module, write_result):
        """Empty Bronze must FAIL the validate_data task, not warn."""
        from airflow.exceptions import AirflowException

        write_result({
            "tables": [],
            "table_keys": [],
            "errors": [],
            "players_rows": 0,
            "teams_rows": 0,
        })

        with pytest.raises(AirflowException, match="Zero rows"):
            dag_module.validate_data()

    @pytest.mark.unit
    def test_partial_rows_warns_but_succeeds(self, dag_module, write_result):
        """545 players (PL-only scrape) is below the 1000 advisory threshold
        but is legitimate partial success — must NOT raise."""
        write_result({
            "tables": ["iceberg.bronze.sofifa_players"],
            "table_keys": ["versions", "teams", "players"],
            "errors": [],
            "players_rows": 545,
            "teams_rows": 96,
        })

        # Must not raise — partial success is valid.
        result = dag_module.validate_data()
        assert result["status"] == "success"
        # The low-count warning is still useful for ops visibility.
        assert any("Low player count" in w for w in result["warnings"])

    @pytest.mark.unit
    def test_full_rows_clean_success(self, dag_module, write_result):
        """Healthy scrape — no warnings, status success."""
        write_result({
            "tables": ["iceberg.bronze.sofifa_players", "iceberg.bronze.sofifa_teams"],
            "table_keys": ["versions", "teams", "players"],
            "errors": [],
            "players_rows": 2500,
            "teams_rows": 200,
            "player_ratings_rows": 545,
        })

        result = dag_module.validate_data()
        assert result["status"] == "success"
        assert result["warnings"] == []
