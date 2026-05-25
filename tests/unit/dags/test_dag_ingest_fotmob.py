"""
Unit tests for ``dags/dag_ingest_fotmob.py``.

Verifies the regression fix that adds ``append_env=True`` to the
``scrape_fotmob_data`` BashOperator. Without it, the operator passes ONLY
the explicit ``env`` dict to the subprocess, so anything the Airflow
container relies on (PYTHONPATH extensions, TRINO_PASSWORD, etc.) is
stripped — and the scraper aborts during import or Trino auth.

These tests run on the host where Airflow is NOT installed, so they
rely on the lightweight ``airflow.*`` stubs installed by
``tests/unit/dags/conftest.py`` (DAG, BashOperator, PythonOperator,
@dag/@task decorators).
"""

from __future__ import annotations

import importlib
import sys

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _reload_dag_module():
    """Force a fresh import of the FotMob DAG module so each test sees
    a clean ``BashOperator._instances`` list (the stubbed BashOperator
    registers every constructed instance globally for inspection)."""
    from airflow.operators.bash import BashOperator  # stub

    BashOperator._instances.clear()

    # Drop cached module so DAG body re-executes (and re-creates operators)
    sys.modules.pop("dag_ingest_fotmob", None)
    sys.modules.pop("dags.dag_ingest_fotmob", None)

    # The DAG file lives in /root/data_platform/dags/ which is on sys.path
    # via the conftest.
    return importlib.import_module("dag_ingest_fotmob")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestFotmobBashOperatorEnv:
    """Regression coverage for the append_env=True fix."""

    @pytest.mark.unit
    def test_dag_loads_without_errors(self):
        """The DAG module should import cleanly under the stubbed Airflow."""
        mod = _reload_dag_module()

        # validate_data is a plain function — it should still be exported
        assert hasattr(mod, "validate_data")
        assert callable(mod.validate_data)

    @pytest.mark.unit
    def test_scrape_task_has_append_env_true(self):
        """``scrape_fotmob_data`` BashOperator MUST set append_env=True.

        Without append_env, the explicit ``env`` dict completely replaces
        the parent process environment, dropping TRINO_PASSWORD,
        FLARESOLVERR_URL, etc. and breaking the scraper.
        """
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        scrape_tasks = [
            op for op in BashOperator._instances
            if op.task_id == "scrape_fotmob_data"
        ]
        assert len(scrape_tasks) == 1, (
            f"Expected exactly one scrape_fotmob_data BashOperator, "
            f"found {len(scrape_tasks)}: "
            f"{[op.task_id for op in BashOperator._instances]}"
        )
        task = scrape_tasks[0]

        assert task.append_env is True, (
            "scrape_fotmob_data BashOperator must set append_env=True so "
            "the parent Airflow env (TRINO_PASSWORD, PYTHONPATH, ...) "
            "leaks through to the subprocess."
        )

    @pytest.mark.unit
    def test_scrape_task_env_still_overrides_pythonpath(self):
        """append_env=True doesn't mean we lose explicit overrides — the
        ``env`` dict should still contain the expected PYTHONPATH/PATH/HOME
        keys, otherwise the subprocess won't find scrapers/."""
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        task = next(
            op for op in BashOperator._instances
            if op.task_id == "scrape_fotmob_data"
        )

        assert task.env is not None, "explicit env dict must be set"
        assert "PYTHONPATH" in task.env
        assert "/opt/airflow" in task.env["PYTHONPATH"]
