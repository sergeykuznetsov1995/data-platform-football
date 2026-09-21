"""#1312: ошибки DQ silver не красят волну сбора, пока silver заморожен."""

from __future__ import annotations

import importlib
import logging
import sys

import pytest

pytestmark = pytest.mark.unit


def _reload():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_transform_fotmob_silver", None)
    sys.modules.pop("dags.dag_transform_fotmob_silver", None)
    return importlib.import_module("dag_transform_fotmob_silver")


def _one_error_report(monkeypatch):
    from utils.data_quality import CheckResult, RunReport

    report = RunReport(
        results=[
            CheckResult(
                name="no_duplicates[silver.fotmob_lineup]",
                kind="no_duplicates",
                severity="ERROR",
                passed=False,
                details="2 duplicate rows",
            ),
            CheckResult(
                name="row_count[silver.fotmob_team_match]",
                kind="row_count",
                severity="ERROR",
                passed=True,
            ),
        ]
    )
    monkeypatch.setattr(
        "utils.data_quality.run_checks",
        lambda checks, raise_on_error: report,
    )
    monkeypatch.setattr(
        "utils.alerts.telegram_dq_summary",
        lambda report, header: None,
    )
    return report


def test_dq_errors_do_not_redden_the_wave_while_silver_is_frozen(monkeypatch, caplog):
    module = _reload()
    _one_error_report(monkeypatch)
    monkeypatch.setattr(module, "SILVER_DQ_BLOCKING", False)

    with caplog.at_level(logging.WARNING):
        result = module._validate_silver_quality_unfenced()

    assert result["errors"] == ["no_duplicates[silver.fotmob_lineup]"]
    assert result["blocking"] is False
    assert any("non-blocking" in record.message for record in caplog.records)


def test_dq_errors_still_fail_the_task_when_blocking_is_restored(monkeypatch):
    from airflow.exceptions import AirflowException

    module = _reload()
    _one_error_report(monkeypatch)
    monkeypatch.setattr(module, "SILVER_DQ_BLOCKING", True)

    with pytest.raises(AirflowException, match="FotMob Silver DQ failed: 1 error"):
        module._validate_silver_quality_unfenced()


def test_frozen_silver_flag_is_off_by_default():
    from utils.fotmob_publication import SILVER_DQ_BLOCKING

    assert SILVER_DQ_BLOCKING is False
