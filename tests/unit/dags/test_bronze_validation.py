"""
Unit tests for ``dags/utils/bronze_validation.py`` — fail-closed validator.

Moved from ``test_dag_ingest_whoscored.py`` when the validator was extracted
to a shared util (issue #466). Regression coverage for issue #110:
``validate_table`` previously called ``MIN_ROW_THRESHOLDS.get(threshold_key,
0)`` which silently passed any empty Bronze table when the key was missing
(root cause amplifier of #102). The function must raise ``AirflowException``
on a missing threshold key instead of falling back to 0.

These tests run on the host using the lightweight Airflow stubs from
``tests/unit/dags/conftest.py``.
"""

from __future__ import annotations

import importlib
import sys

import pytest


def _load_module():
    sys.modules.pop("utils.bronze_validation", None)
    return importlib.import_module("utils.bronze_validation")


class TestValidateTableFailClosed:
    @pytest.mark.unit
    def test_known_key_passes_when_rows_above_threshold(self, monkeypatch):
        mod = _load_module()
        # Derive the passing row count from the configured threshold so the
        # test survives threshold re-scaling (e.g. × len(WHOSCORED_LEAGUES)).
        threshold = mod.MIN_ROW_THRESHOLDS["whoscored_schedule"]
        rows = threshold + 60
        monkeypatch.setattr(mod, "bronze_count", lambda _t: rows)

        result = mod.validate_table("whoscored_schedule", "whoscored_schedule")

        assert result["rows"] == rows
        assert result["threshold"] == threshold

    @pytest.mark.unit
    def test_missing_key_raises_instead_of_silent_pass(self, monkeypatch):
        """Regression: missing MIN_ROW_THRESHOLDS key must NOT silently
        fall back to 0. This is the hidden-enabler from issue #102 that
        let an empty ``bronze.whoscored_schedule`` scrape pass as success.
        """
        mod = _load_module()
        from airflow.exceptions import AirflowException

        # bronze_count must not be reached — the missing-key guard fires first
        def _should_not_be_called(_t):
            raise AssertionError("bronze_count must not be called when key missing")

        monkeypatch.setattr(mod, "bronze_count", _should_not_be_called)
        monkeypatch.setattr(
            mod,
            "MIN_ROW_THRESHOLDS",
            {k: v for k, v in mod.MIN_ROW_THRESHOLDS.items() if k != "whoscored_schedule"},
        )

        with pytest.raises(AirflowException, match="missing key 'whoscored_schedule'"):
            mod.validate_table("whoscored_schedule", "whoscored_schedule")

    @pytest.mark.unit
    def test_rows_below_threshold_raises(self, monkeypatch):
        """An empty/wiped Bronze table must hard-fail the validation task."""
        mod = _load_module()
        from airflow.exceptions import AirflowException

        monkeypatch.setattr(mod, "bronze_count", lambda _t: 0)

        with pytest.raises(AirflowException, match="0 rows < threshold"):
            mod.validate_table("whoscored_schedule", "whoscored_schedule")
