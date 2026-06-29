"""
Unit tests for ``dags/dag_ingest_whoscored.py`` validation wrappers.

The fail-closed validator itself was extracted to
``dags/utils/bronze_validation.py`` (issue #466) and is covered by
``test_bronze_validation.py``. What's left to pin here is the wrapper
contract: each validate task must call ``validate_table`` with the right
``(table_name, threshold_key)`` pair — a fat-fingered key would otherwise
surface only at DAG runtime as a missing-key AirflowException.

These tests run on the host using the lightweight Airflow stubs from
``tests/unit/dags/conftest.py``.
"""

from __future__ import annotations

import importlib
import sys

import pytest


def _load_dag_module():
    sys.modules.pop("dag_ingest_whoscored", None)
    sys.modules.pop("dags.dag_ingest_whoscored", None)
    return importlib.import_module("dag_ingest_whoscored")


class TestValidateWrappers:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "wrapper, expected_table",
        [
            ("validate_schedule", "whoscored_schedule"),
            ("validate_events", "whoscored_events"),
        ],
    )
    def test_wrapper_passes_table_and_threshold_key(
        self, monkeypatch, wrapper, expected_table
    ):
        mod = _load_dag_module()
        calls = []

        monkeypatch.setattr(
            mod,
            "validate_table",
            lambda table, key: calls.append((table, key)) or {"table": table},
        )

        getattr(mod, wrapper)()

        assert calls == [(expected_table, expected_table)]
