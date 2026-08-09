"""Topology and callable contracts for the trigger-only ESPN Silver DAG."""

from __future__ import annotations

import importlib
import sys

import pytest

pytestmark = pytest.mark.unit


def _reload():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop("dag_transform_espn_silver", None)
    sys.modules.pop("dags.dag_transform_espn_silver", None)
    return importlib.import_module("dag_transform_espn_silver")


def _tasks():
    from airflow.operators.python import PythonOperator

    return {task.task_id: task for task in PythonOperator._instances}


def test_trigger_only_dag_materializes_all_six_before_validation():
    module = _reload()
    tasks = _tasks()

    assert module.dag.schedule is None
    assert module.dag._dag_kwargs["catchup"] is False
    assert module.dag._dag_kwargs["max_active_runs"] == 1
    assert module.dag._dag_kwargs["max_active_tasks"] == 1
    assert module.SILVER_TRANSFORMS == [
        ("match", "dags/sql/silver/espn_match.sql", "espn_match"),
        ("team_match", "dags/sql/silver/espn_team_match.sql", "espn_team_match"),
        ("player_match_aggregate", "dags/sql/silver/espn_player_match_aggregate.sql", "espn_player_match_aggregate"),
        ("match_events", "dags/sql/silver/espn_match_events.sql", "espn_match_events"),
        ("substitutions", "dags/sql/silver/espn_substitutions.sql", "espn_substitutions"),
        ("venue", "dags/sql/silver/espn_venue.sql", "espn_venue"),
    ]
    transform_ids = {f"silver_transforms.{name}" for name, _, _ in module.SILVER_TRANSFORMS}
    assert transform_ids == set(tasks) - {
        "validate_source_layout", "validate_silver", "validate_silver_quality"
    }
    assert tasks["validate_source_layout"].downstream_task_ids == transform_ids
    for transform_id in transform_ids:
        assert tasks[transform_id].upstream_task_ids == {"validate_source_layout"}
    assert tasks["validate_silver"].upstream_task_ids == transform_ids
    assert tasks["validate_silver"].downstream_task_ids == {"validate_silver_quality"}
    assert tasks["validate_silver_quality"].upstream_task_ids == {"validate_silver"}
    assert tasks["validate_silver_quality"].python_callable is module._validate_silver_quality


def _inventory(layout_module, mode):
    if mode == layout_module.LEGACY14:
        return [
            (layout_module.BRONZE_SCHEMA, name, kind)
            for name, kind in layout_module.LEGACY14_PUBLIC_OBJECTS.items()
        ]
    return [
        *(
            (layout_module.BRONZE_SCHEMA, name, kind)
            for name, kind in layout_module.COMPACT6_PUBLIC_OBJECTS.items()
        ),
        *(
            (layout_module.INTERNAL_SCHEMA, name, kind)
            for name, kind in layout_module.COMPACT6_INTERNAL_REQUIRED_OBJECTS.items()
        ),
    ]


def test_source_layout_preflight_queries_and_proves_exact_compact6(monkeypatch):
    module = _reload()
    from scrapers.espn import layout

    monkeypatch.setenv(layout.LAYOUT_MODE_ENV, layout.COMPACT6)
    calls = []

    class Manager:
        def __enter__(self):
            calls.append("enter")
            return self

        def __exit__(self, exc_type, exc, traceback):
            calls.append("exit")

        def execute_query(self, sql, params=()):
            calls.append((sql, params))
            return _inventory(layout, layout.COMPACT6)

    monkeypatch.setattr("scrapers.base.trino_manager.TrinoTableManager", Manager)

    result = module._validate_source_layout()

    assert result == {
        "layout_mode": "compact6",
        "public_object_count": 6,
        "internal_object_count": len(layout.COMPACT6_INTERNAL_REQUIRED_OBJECTS),
    }
    assert calls[0] == "enter" and calls[-1] == "exit"
    assert calls[1] == (layout.catalog_inventory_sql(catalog="iceberg"), ())


@pytest.mark.parametrize("inventory_kind", ["missing", "legacy", "mixed"])
def test_source_layout_preflight_rejects_nonexact_catalog_before_transforms(
    monkeypatch, inventory_kind
):
    module = _reload()
    from scrapers.espn import layout

    monkeypatch.setenv(layout.LAYOUT_MODE_ENV, layout.COMPACT6)
    rows = _inventory(layout, layout.COMPACT6)
    if inventory_kind == "missing":
        rows = rows[1:]
    elif inventory_kind == "legacy":
        rows = _inventory(layout, layout.LEGACY14)
    else:
        rows.append(("bronze", "espn_schedule_generation_v2", "BASE TABLE"))

    class Manager:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return None

        def execute_query(self, sql, params=()):
            return rows

    monkeypatch.setattr("scrapers.base.trino_manager.TrinoTableManager", Manager)

    with pytest.raises(layout.LayoutError):
        module._validate_source_layout()


@pytest.mark.parametrize("layout_mode", [None, "legacy14"])
def test_runtime_callables_reject_noncompact_layout_before_side_effects(
    monkeypatch, layout_mode
):
    module = _reload()
    from scrapers.espn import layout
    from utils.data_quality import RunReport

    if layout_mode is None:
        monkeypatch.delenv(layout.LAYOUT_MODE_ENV, raising=False)
    else:
        monkeypatch.setenv(layout.LAYOUT_MODE_ENV, layout_mode)
    calls = []
    monkeypatch.setattr(
        "utils.silver_tasks.run_silver_transform",
        lambda **kwargs: calls.append(("transform", kwargs)),
    )
    monkeypatch.setattr(
        "utils.silver_tasks.validate_silver_tables",
        lambda **kwargs: calls.append(("validate", kwargs)),
    )
    monkeypatch.setattr(
        "utils.data_quality.run_checks",
        lambda *args, **kwargs: calls.append(("checks", args, kwargs)) or RunReport([]),
    )
    monkeypatch.setattr(
        "utils.espn_silver_dq.run_espn_silver_custom_checks",
        lambda: calls.append(("custom",)),
    )
    monkeypatch.setattr(
        "utils.alerts.telegram_dq_summary",
        lambda *args, **kwargs: calls.append(("alert", args, kwargs)),
    )

    for callable_, kwargs in (
        (module._run_transform, {"sql_file": "x.sql", "table_name": "x"}),
        (module._validate_silver, {}),
        (module._validate_silver_quality, {}),
    ):
        with pytest.raises(layout.LayoutError):
            callable_(**kwargs)

    assert calls == []


def test_transform_and_minimum_validation_use_compact6_runtime_fence(monkeypatch):
    module = _reload()
    from scrapers.espn import layout

    monkeypatch.setenv(layout.LAYOUT_MODE_ENV, layout.COMPACT6)
    calls = []
    monkeypatch.setattr(
        module,
        "_validate_source_layout",
        lambda: calls.append(("layout",)) or {"layout_mode": "compact6"},
    )
    monkeypatch.setattr(
        "utils.silver_tasks.run_silver_transform",
        lambda **kwargs: calls.append(("transform", kwargs)) or {"ok": True},
    )
    monkeypatch.setattr(
        "utils.silver_tasks.validate_silver_tables",
        lambda **kwargs: calls.append(("validate", kwargs)) or {"warnings": []},
    )

    assert module._run_transform("x.sql", "x") == {"ok": True}
    assert module._validate_silver() == {"warnings": []}
    assert calls == [
        ("layout",),
        (
            "transform",
            {"sql_file": "x.sql", "table_name": "x", "schema": "silver"},
        ),
        ("layout",),
        (
            "validate",
            {"tables": module.SILVER_MIN_ROWS, "min_rows": 0},
        ),
    ]


def test_transform_retry_rechecks_catalog_and_never_calls_writer_on_drift(monkeypatch):
    module = _reload()
    from scrapers.espn import layout

    monkeypatch.setenv(layout.LAYOUT_MODE_ENV, layout.COMPACT6)
    rows = _inventory(layout, layout.COMPACT6)
    rows.append(("bronze", "espn_schedule_generation_v2", "BASE TABLE"))
    calls = []

    class Manager:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return None

        def execute_query(self, sql, params=()):
            calls.append(("inventory", sql, params))
            return rows

    monkeypatch.setattr("scrapers.base.trino_manager.TrinoTableManager", Manager)
    monkeypatch.setattr(
        "utils.silver_tasks.run_silver_transform",
        lambda **kwargs: calls.append(("writer", kwargs)),
    )

    with pytest.raises(layout.LayoutError, match="unexpected public ESPN objects"):
        module._run_transform("x.sql", "x")

    assert [call[0] for call in calls] == ["inventory"]


def test_validation_retries_recheck_catalog_before_checks_or_alerts(monkeypatch):
    module = _reload()
    from scrapers.espn import layout
    from utils.data_quality import RunReport

    monkeypatch.setenv(layout.LAYOUT_MODE_ENV, layout.COMPACT6)
    rows = _inventory(layout, layout.COMPACT6)
    rows.pop(0)
    calls = []

    class Manager:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return None

        def execute_query(self, sql, params=()):
            calls.append(("inventory", sql, params))
            return rows

    monkeypatch.setattr("scrapers.base.trino_manager.TrinoTableManager", Manager)
    monkeypatch.setattr(
        "utils.silver_tasks.validate_silver_tables",
        lambda **kwargs: calls.append(("validate", kwargs)),
    )
    monkeypatch.setattr(
        "utils.data_quality.run_checks",
        lambda *args, **kwargs: calls.append(("checks", args, kwargs)) or RunReport([]),
    )
    monkeypatch.setattr(
        "utils.espn_silver_dq.run_espn_silver_custom_checks",
        lambda: calls.append(("custom",)),
    )
    monkeypatch.setattr(
        "utils.alerts.telegram_dq_summary",
        lambda *args, **kwargs: calls.append(("alert", args, kwargs)),
    )

    for callable_ in (module._validate_silver, module._validate_silver_quality):
        with pytest.raises(layout.LayoutError, match="missing public objects"):
            callable_()

    assert [call[0] for call in calls] == ["inventory", "inventory"]


def test_quality_merges_warning_custom_results_and_sends_one_summary(monkeypatch):
    module = _reload()
    monkeypatch.setattr(module, "_validate_source_layout", lambda: None)

    from utils.data_quality import CheckResult, RunReport

    report = RunReport(results=[])
    sent = []
    monkeypatch.setattr("utils.data_quality.run_checks", lambda checks, raise_on_error: report)
    monkeypatch.setattr(
        "utils.espn_silver_dq.run_espn_silver_custom_checks",
        lambda: [CheckResult("soft", "custom", "WARNING", False)],
    )
    monkeypatch.setattr("utils.alerts.telegram_dq_summary", lambda value, header: sent.append((value, header)))

    result = module._validate_silver_quality()

    assert result["warnings"] == ["soft"]
    assert report.results[-1].name == "soft"
    assert sent == [(report, "ESPN Silver DQ")]


def test_quality_raises_only_for_merged_custom_errors_and_still_sends_summary(monkeypatch):
    module = _reload()
    monkeypatch.setattr(module, "_validate_source_layout", lambda: None)

    from airflow.exceptions import AirflowException
    from utils.data_quality import CheckResult, RunReport

    report = RunReport(results=[])
    sent = []
    monkeypatch.setattr("utils.data_quality.run_checks", lambda checks, raise_on_error: report)
    monkeypatch.setattr(
        "utils.espn_silver_dq.run_espn_silver_custom_checks",
        lambda: [CheckResult("hard", "custom", "ERROR", False, details="broken")],
    )
    monkeypatch.setattr("utils.alerts.telegram_dq_summary", lambda value, header: sent.append((value, header)))

    with pytest.raises(AirflowException, match="hard: broken"):
        module._validate_silver_quality()

    assert sent == [(report, "ESPN Silver DQ")]
