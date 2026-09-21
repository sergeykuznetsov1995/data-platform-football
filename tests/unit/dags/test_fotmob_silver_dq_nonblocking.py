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


def test_dq_error_carries_through_the_silver_chain_and_keeps_ingest_green(monkeypatch):
    """Полная цепочка волны: ошибка DQ → кандидат → зелёный silver → зелёный ingest."""

    import ast
    from pathlib import Path
    from unittest.mock import MagicMock

    from utils import fotmob_publication

    module = _reload()
    _one_error_report(monkeypatch)
    monkeypatch.setattr(module, "SILVER_DQ_BLOCKING", False)
    monkeypatch.setattr(fotmob_publication, "SILVER_DQ_BLOCKING", False)

    # Шаг 1: DQ отдаёт XCom с ошибкой и не роняет задачу.
    quality_gate = module._validate_silver_quality_unfenced()
    assert quality_gate["errors"] and quality_gate["blocking"] is False

    # Шаг 2: кандидат записывается той же волной (ceremony отключена — запись в
    # ControlStore пропускается, гейты evidence остаются).
    monkeypatch.setattr(fotmob_publication, "fotmob_ceremony_configured", lambda: False)
    values = {
        "silver_transforms.a": {"status": "success", "rows": 10},
        "validate_silver": {"status": "success", "warnings": []},
        "validate_silver_quality": quality_gate,
    }
    task_instance = MagicMock()
    task_instance.xcom_pull.side_effect = lambda task_ids: values[task_ids]

    candidate = fotmob_publication.record_fotmob_silver_candidate(
        transform_task_ids=["silver_transforms.a"],
        ti=task_instance,
    )
    assert candidate["quality_gate"]["errors"] == quality_gate["errors"]

    # Шаг 3: ingest считает волну красной только по failed-состоянию silver,
    # а silver при неблокирующей ошибке DQ заканчивается success.
    tree = ast.parse(
        (Path(__file__).resolve().parents[3] / "dags" / "dag_ingest_fotmob.py")
        .read_text(encoding="utf-8")
    )
    trigger_kwargs = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
        task_id = kwargs.get("task_id")
        if isinstance(task_id, ast.Constant) and task_id.value == "trigger_silver_transform":
            trigger_kwargs = kwargs
            break

    assert trigger_kwargs, "task trigger_silver_transform not found"
    assert ast.literal_eval(trigger_kwargs["allowed_states"]) == ["success"]
    assert ast.literal_eval(trigger_kwargs["failed_states"]) == ["failed"]
    assert ast.literal_eval(trigger_kwargs["wait_for_completion"]) is True
