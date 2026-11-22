"""Offline contracts for the raw replay DAG; Airflow stubs come from conftest."""

import importlib
import json
import re
import sys
from datetime import timedelta
from pathlib import PurePosixPath
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, sentinel

import pytest


@pytest.fixture
def runtime(monkeypatch):
    """Replace lazy worker/pipeline imports before any planner invocation."""
    source = Mock(side_effect=AssertionError("source traffic is forbidden"))
    capture = SimpleNamespace(manifest_store=sentinel.manifest, source_client=source)
    build = Mock(return_value=capture)
    manager = Mock(return_value=sentinel.manager)
    scopes = Mock(return_value=[])
    pipeline = ModuleType("scrapers.sofascore.pipeline")
    pipeline.build_capture_runtime = build
    worker = ModuleType("dags.scripts.replay_sofascore_raw")
    worker._replay_manager = manager
    import scrapers.sofascore.raw_replay as raw_replay_lib
    monkeypatch.setattr(raw_replay_lib, "list_replay_scope_keys", scopes)
    monkeypatch.setitem(sys.modules, pipeline.__name__, pipeline)
    monkeypatch.setitem(sys.modules, worker.__name__, worker)
    yield SimpleNamespace(build=build, manager=manager, scopes=scopes)
    assert source.mock_calls == []


@pytest.fixture
def replay(runtime):
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    BashOperator._instances.clear()
    for name in ("dags.dag_replay_sofascore_raw", "dag_replay_sofascore_raw"):
        sys.modules.pop(name, None)
    module = importlib.import_module("dags.dag_replay_sofascore_raw")
    runtime.build.assert_not_called()
    runtime.manager.assert_not_called()
    runtime.scopes.assert_not_called()
    return module


def test_dag_identity_topology_and_worker_bounds(replay):
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    dag = replay.dag
    assert dag.dag_id == "dag_replay_sofascore_raw"
    config = dag._dag_kwargs
    assert config["schedule"] is None
    assert config["is_paused_upon_creation"] is True
    assert config["catchup"] is False
    assert config["max_active_runs"] == 1
    assert 1 <= config["max_active_tasks"] <= 2
    assert timedelta(0) < config["dagrun_timeout"] <= timedelta(hours=2)
    assert config["default_args"]["retries"] == 0
    operators = [*PythonOperator._instances, *BashOperator._instances]
    tasks = {task.task_id: task for task in operators}
    assert len(operators) == len(tasks) == 3
    assert set(tasks) == {"planner", "replay_scope", "validate_summary"}
    planner, worker, summary = (
        tasks[name] for name in ("planner", "replay_scope", "validate_summary")
    )
    assert planner.upstream_task_ids == set()
    assert planner.downstream_task_ids == {"replay_scope"}
    assert worker.upstream_task_ids == {"planner"}
    assert worker.downstream_task_ids == {"validate_summary"}
    assert summary.upstream_task_ids == {"replay_scope"}
    assert summary.downstream_task_ids == set()
    # A failed worker blocks the summary; no exception-swallowing simulation.
    assert summary._init_kwargs["trigger_rule"] == "all_success"
    assert planner.python_callable is replay.plan_replay_scopes
    assert summary.python_callable is replay.validate_replay_summary
    assert worker.is_mapped is True
    assert worker._expand_kwargs["env"].operator is planner
    partial = worker._init_kwargs
    assert partial["pool"] == replay.INGEST_SCRAPER_POOL
    assert partial["pool"]
    assert partial.get("pool_slots", 1) == 1
    assert partial["append_env"] is True
    assert partial["cwd"] == "/opt/airflow"
    assert partial["bash_command"] == replay.RUN_REPLAY_SCOPE_COMMAND
    for timeout in (
        planner._init_kwargs["execution_timeout"], partial["execution_timeout"],
        summary._init_kwargs["execution_timeout"]
    ):
        assert timedelta(0) < timeout <= timedelta(minutes=30)


def _assert_planner_call(scopes, **expected):
    """Assert the planner's list call(s); the catalog is compared by type.

    An empty fields plan also triggers the blocked-candidates probe with
    ``include_blocked=True`` (a skipped scope must never read as drained).
    """
    from scrapers.sofascore.catalog import SofaScoreCatalog

    expected_calls = 2 if expected.get("expect_drained_probe") else 1
    assert scopes.call_count == expected_calls
    first = scopes.call_args_list[0]
    assert first.args == (sentinel.manager,)
    kwargs = first.kwargs
    assert kwargs == {
        "mode": expected["mode"],
        "exact_scope": expected["exact_scope"],
        "limit": expected["limit"],
        "catalog": kwargs["catalog"],
        "include_blocked": False,
        "snapshot_document": kwargs.get("snapshot_document"),
    }
    assert isinstance(kwargs["catalog"], SofaScoreCatalog)
    if expected_calls == 2:
        probe = scopes.call_args_list[1]
        assert probe.kwargs["blocked_only"] is True
        assert probe.kwargs.get("include_blocked", False) is False


def test_params_and_default_planner_arguments(replay, runtime):
    params = replay.PARAMS
    assert set(params) == {
        "mode", "scope_cap", "target_cap", "source_tournament_id",
        "source_season_id", "include_blocked",
    }
    assert params["mode"].value == "fields"
    assert params["mode"]._kw["enum"] == ["fields", "drain"]
    assert params["mode"]._kw.get("type", "string") == "string"
    for name, default in (("scope_cap", 3), ("target_cap", 25)):
        assert params[name].value == default
        assert params[name]._kw["type"] == "integer"
        assert params[name]._kw["minimum"] == 1
    for name in ("source_tournament_id", "source_season_id"):
        assert params[name].value is None
        assert set(params[name]._kw["type"]) == {"null", "string"}
    assert params["include_blocked"].value is False
    assert params["include_blocked"]._kw["type"] == "boolean"
    runtime.scopes.return_value = [
        SimpleNamespace(source_tournament_id="7", source_season_id="42", freshness_key="v1")
    ]
    environments = replay.plan_replay_scopes()
    assert len(environments) == 1
    assert environments[0]["SOFASCORE_REPLAY_MODE"] == "fields"
    assert environments[0]["SOFASCORE_REPLAY_TARGET_CAP"] == "25"
    runtime.build.assert_called_once_with(run_id="manual", task_id="planner")
    runtime.manager.assert_called_once_with(sentinel.manifest)
    _assert_planner_call(
        runtime.scopes,
        mode="fields",
        exact_scope=None,
        limit=3,
    )


@pytest.mark.parametrize("mode", ["fields", "drain"])
def test_planner_deterministic_exact_scopes_and_safe_unique_paths(replay, runtime, mode):
    keys = [
        SimpleNamespace(source_tournament_id=tournament, source_season_id="42", freshness_key=freshness)
        for tournament, freshness in (("cup/a", "v1"), ("cup?a", "v1"), ("cup/a", "v2"))
    ]
    runtime.scopes.return_value = [keys[0], keys[0], *keys[1:]]
    params = {"mode": mode, "scope_cap": 4, "target_cap": 9}
    run_id = "../manual/2026:11?synthetic"
    environments = replay.plan_replay_scopes(params=params, run_id=run_id)
    runtime.build.assert_called_once_with(run_id=run_id, task_id="planner")
    runtime.manager.assert_called_once_with(sentinel.manifest)
    _assert_planner_call(
        runtime.scopes,
        mode=mode,
        exact_scope=None,
        limit=4,
    )
    assert len(environments) == 3
    outputs = set()
    for environment, key in zip(environments, (keys[0], keys[1], keys[2])):
        assert set(environment) == {
            "SOFASCORE_REPLAY_MODE", "SOFASCORE_REPLAY_SCOPE_JSON",
            "SOFASCORE_REPLAY_TARGET_CAP", "SOFASCORE_REPLAY_OUTPUT",
        }
        assert all(isinstance(value, str) for value in environment.values())
        assert environment["SOFASCORE_REPLAY_MODE"] == mode
        assert environment["SOFASCORE_REPLAY_TARGET_CAP"] == "9"
        assert json.loads(environment["SOFASCORE_REPLAY_SCOPE_JSON"]) == vars(key)
        assert len(vars(key)) == 3 and all(isinstance(value, str) for value in vars(key).values())
        output = environment["SOFASCORE_REPLAY_OUTPUT"]
        relative = PurePosixPath(output).relative_to(replay.REPLAY_OUTPUT_ROOT)
        assert len(relative.parts) == 2
        assert all(re.fullmatch(r"[A-Za-z0-9_.-]+", part) for part in relative.parts)
        assert all(part not in (".", "..") for part in relative.parts)
        assert relative.suffix == ".json"
        outputs.add(output)
    assert len(outputs) == 3  # Sanitization collisions and different freshness remain distinct.
    assert replay.plan_replay_scopes(params=params, run_id=run_id) == environments
    other = replay.plan_replay_scopes(params=params, run_id="another-manual-run")
    assert outputs.isdisjoint(env["SOFASCORE_REPLAY_OUTPUT"] for env in other)


@pytest.mark.parametrize("mode", ["fields", "drain"])
def test_exact_tournament_season_pair_is_trimmed(replay, runtime, mode):
    assert replay.plan_replay_scopes(params={
        "mode": mode, "source_tournament_id": " 7 ", "source_season_id": " 42 "
    }) == []
    _assert_planner_call(
        runtime.scopes,
        mode=mode,
        exact_scope=("7", "42"),
        limit=3,
        expect_drained_probe=(mode == "fields"),
    )


@pytest.mark.parametrize("params", [
    *({"mode": value} for value in ("live", "", None, 1)),
    *({name: value} for name in ("scope_cap", "target_cap")
      for value in (0, -1, True, False, 1.5, "3", None)),
    *({"include_blocked": value} for value in (0, 1, "true", None)),
    *({"source_tournament_id": tournament, "source_season_id": season}
      for tournament, season in (("7", None), (None, "42"), ("", "42"),
                                 ("7", " "), (7, "42"), ("7", 42))),
])
def test_invalid_params_fail_before_runtime(replay, runtime, params):
    with pytest.raises(ValueError):
        replay.plan_replay_scopes(params=params)
    runtime.build.assert_not_called()
    runtime.manager.assert_not_called()
    runtime.scopes.assert_not_called()


def test_cli_quotes_environment_and_creates_parent_before_exec(replay):
    command = replay.RUN_REPLAY_SCOPE_COMMAND
    flags = {
        "mode": "MODE", "scope-json": "SCOPE_JSON",
        "target-cap": "TARGET_CAP", "output": "OUTPUT",
    }
    assert set(re.findall(r"--[a-z][a-z-]*", command)) == {f"--{flag}" for flag in flags}
    for flag, suffix in flags.items():
        assert f'--{flag} "${{SOFASCORE_REPLAY_{suffix}}}"' in command
    mkdir = 'mkdir -p "$(dirname "${SOFASCORE_REPLAY_OUTPUT}")"'
    execute = f"exec /opt/legacy-scraper-venv/bin/python {replay.REPLAY_WORKER_SCRIPT}"
    assert "set -euo pipefail" in command
    assert command.index(mkdir) < command.index(execute)


def _summarize(replay, environments):
    pull = Mock(return_value=environments)
    try:
        return replay.validate_replay_summary(ti=SimpleNamespace(xcom_pull=pull))
    finally:
        pull.assert_called_once_with(task_ids="planner")


def _report(rows=5):
    location = "iceberg.bronze.sofascore_rounds"
    return {
        "errors": [], "source_request_count": 0,
        "status_counts": {"deferred": 0, "schema_error": 0},
        "phases": {"fields": {"ran": True, "exit_code": 0}},
        "tables": [location], "rows": rows,
        "publication_receipts": [{"location": location, "table": "sofascore_rounds", "rows": rows}],
    }


def _write_report(tmp_path, report, name="result.json"):
    output = tmp_path / name
    output.write_text(json.dumps(report), encoding="utf-8")
    return {"SOFASCORE_REPLAY_OUTPUT": str(output)}


def test_empty_plan_is_zero_scope_success_without_report_reads(replay, runtime, monkeypatch):
    environments = replay.plan_replay_scopes(run_id="empty")
    assert environments == []
    load = Mock(side_effect=AssertionError("empty plan must not read reports"))
    monkeypatch.setattr(replay, "_load_replay_report", load)
    assert _summarize(replay, environments) == {
        "scopes": 0, "tables": 0, "rows": 0, "source_request_count": 0
    }
    load.assert_not_called()


@pytest.mark.parametrize("rows", [(5,), (5, 8)])
def test_summary_sums_scopes_tables_and_rows(replay, tmp_path, rows):
    environments = [
        _write_report(tmp_path, _report(count), f"scope-{index}.json")
        for index, count in enumerate(rows)
    ]
    assert _summarize(replay, environments) == {
        "scopes": len(rows), "tables": len(rows), "rows": sum(rows), "source_request_count": 0
    }


@pytest.mark.parametrize("contents", [None, "{broken", "[]", "null"])
def test_summary_rejects_missing_malformed_or_nonobject_json(replay, tmp_path, contents):
    output = tmp_path / "result.json"
    if contents is not None:
        output.write_text(contents, encoding="utf-8")
    with pytest.raises(replay.AirflowException, match="cannot read replay output|not a JSON object"):
        _summarize(replay, [{"SOFASCORE_REPLAY_OUTPUT": str(output)}])


_MISSING = object()


@pytest.mark.parametrize("path,value,message", [
    (("errors",), ["failed"], "errors must be empty"),
    (("errors",), _MISSING, "errors must be empty"),
    *(( ("source_request_count",), value, "source_request_count must be 0")
      for value in (1, -1, True, "0", _MISSING)),
    (("status_counts",), _MISSING, "status_counts"),
    (("status_counts", "deferred"), 1, "status_counts.deferred"),
    (("status_counts", "schema_error"), 1, "status_counts.schema_error"),
    (("status_counts", "deferred"), True, "status_counts.deferred"),
    (("status_counts", "schema_error"), _MISSING, "status_counts.schema_error"),
    (("phases",), _MISSING, "phases"),
    (("phases", "fields"), None, "must be an object"),
    (("phases", "fields", "exit_code"), 2, "exited"),
    (("phases", "fields", "exit_code"), True, "exited"),
    (("tables",), _MISSING, "tables"),
    (("tables",), [""], "nonempty string"),
    (("publication_receipts",), _MISSING, "publication_receipts"),
    (("publication_receipts",), [], "has no publication receipt"),
    (("publication_receipts",), [None], "has no publication receipt"),
    (("publication_receipts", 0, "location"), "iceberg.bronze.other", "has no publication receipt"),
    (("publication_receipts", 0, "table"), "other", "does not match"),
    (("publication_receipts", 0, "rows"), -1, "receipt rows"),
    (("publication_receipts", 0, "rows"), True, "receipt rows"),
    (("rows",), -1, "nonnegative integer"),
    (("rows",), True, "nonnegative integer"),
    (("rows",), _MISSING, "nonnegative integer"),
])
def test_summary_fails_closed_on_invalid_worker_report(replay, tmp_path, path, value, message):
    report = _report()
    parent = report
    for key in path[:-1]:
        parent = parent[key]
    if value is _MISSING:
        del parent[path[-1]]
    else:
        parent[path[-1]] = value
    # A preceding successful scope cannot hide a later failed report.
    environments = [
        _write_report(tmp_path, _report(), "good.json"),
        _write_report(tmp_path, report, "bad.json"),
    ]
    with pytest.raises(replay.AirflowException, match=message):
        _summarize(replay, environments)


@pytest.mark.parametrize("environment", [None, {}, {"SOFASCORE_REPLAY_OUTPUT": ""},
                                          {"SOFASCORE_REPLAY_OUTPUT": 1}])
def test_summary_rejects_invalid_plan_entries(replay, environment):
    with pytest.raises(replay.AirflowException, match="misses a SOFASCORE_REPLAY_OUTPUT string"):
        _summarize(replay, [environment])


# --- #1367 planner resolvability/terminality wiring -------------------------


def test_planner_lazy_imports_resolve_against_real_modules():
    """PR #2418 lesson: a fixture that fakes the worker module hides a wrong
    import until the DAG runs in production.  Import every lazily imported
    name of the planner against the REAL modules."""
    from dags.scripts import replay_sofascore_raw as worker
    from scrapers.sofascore import pipeline
    from scrapers.sofascore import raw_replay

    assert callable(worker._replay_manager)
    assert callable(raw_replay.list_replay_scope_keys)
    assert callable(raw_replay.replay_eligible_scope_pairs)
    assert callable(raw_replay.validate_replay_scope_pair)
    assert callable(pipeline.build_capture_runtime)
    from scrapers.sofascore.catalog import SofaScoreCatalog

    assert callable(SofaScoreCatalog.load)


def test_planner_empty_fields_plan_distinguishes_blocked_from_drained(
    replay, runtime, caplog
):
    import logging

    blocked = SimpleNamespace(
        source_tournament_id="16", source_season_id="58210", freshness_key="2026"
    )
    runtime.scopes.side_effect = [[], [blocked]]
    with caplog.at_level(logging.WARNING):
        assert replay.plan_replay_scopes() == []
    assert runtime.scopes.call_count == 2
    # Review r1 F3: the probe must query exactly the blocked triples, not
    # every candidate an unrestricted second query could sweep in.
    assert runtime.scopes.call_args.kwargs["blocked_only"] is True
    assert runtime.scopes.call_args.kwargs.get("include_blocked", False) is False
    assert any("schema_error season blockers" in message for message in caplog.messages)


def test_planner_empty_drained_plan_runs_no_second_query(replay, runtime):
    runtime.scopes.return_value = []
    assert replay.plan_replay_scopes(params={"mode": "drain"}) == []
    assert runtime.scopes.call_count == 1


def test_planner_passes_include_blocked_through(replay, runtime):
    runtime.scopes.return_value = []
    assert (
        replay.plan_replay_scopes(params={"mode": "fields", "include_blocked": True})
        == []
    )
    assert runtime.scopes.call_count == 1
    assert runtime.scopes.call_args.kwargs["include_blocked"] is True
