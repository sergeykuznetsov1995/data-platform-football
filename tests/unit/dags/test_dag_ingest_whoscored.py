"""
Unit tests for ``dags/dag_ingest_whoscored.py`` — fail-closed validator.

Regression coverage for issue #110: ``_validate_table`` previously called
``MIN_ROW_THRESHOLDS.get(threshold_key, 0)`` which silently passed any
empty Bronze table when the key was missing (root cause amplifier of #102).
The function must now raise ``AirflowException`` on a missing threshold key
instead of falling back to 0.

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


class TestValidateTableFailClosed:
    @pytest.mark.unit
    def test_known_key_passes_when_rows_above_threshold(self, monkeypatch):
        mod = _load_dag_module()
        monkeypatch.setattr(mod, "_bronze_count", lambda _t: 400)

        result = mod._validate_table("whoscored_schedule", "whoscored_schedule")

        assert result["rows"] == 400
        assert result["threshold"] == mod.MIN_ROW_THRESHOLDS["whoscored_schedule"]

    @pytest.mark.unit
    def test_missing_key_raises_instead_of_silent_pass(self, monkeypatch):
        """Regression: missing MIN_ROW_THRESHOLDS key must NOT silently
        fall back to 0. This is the hidden-enabler from issue #102 that
        let an empty ``bronze.whoscored_schedule`` scrape pass as success.
        """
        mod = _load_dag_module()
        from airflow.exceptions import AirflowException

        # _bronze_count must not be reached — the missing-key guard fires first
        def _should_not_be_called(_t):
            raise AssertionError("_bronze_count must not be called when key missing")

        monkeypatch.setattr(mod, "_bronze_count", _should_not_be_called)
        monkeypatch.setattr(
            mod,
            "MIN_ROW_THRESHOLDS",
            {k: v for k, v in mod.MIN_ROW_THRESHOLDS.items() if k != "whoscored_schedule"},
        )

        with pytest.raises(AirflowException, match="missing key 'whoscored_schedule'"):
            mod._validate_table("whoscored_schedule", "whoscored_schedule")
