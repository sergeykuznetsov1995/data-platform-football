from __future__ import annotations

import inspect
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dags.utils import fbref_pipeline_tasks


@pytest.mark.unit
def test_validate_records_control_budget_in_proxy_traffic_rollup(monkeypatch):
    summary = {
        "requests_used": 7,
        "bytes_used": 3 * 1024 * 1024,
        "target_counts": {"succeeded": 4},
        "dataset_validation_counts": {"succeeded": 12},
    }
    pipeline = MagicMock()
    pipeline.validate_and_finish.return_value = summary
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_pipeline", MagicMock(return_value=pipeline)
    )
    telemetry = SimpleNamespace(
        log_traffic_summary=MagicMock(),
        record_traffic_run=MagicMock(return_value=True),
    )
    monkeypatch.setitem(sys.modules, "utils.proxy_traffic", telemetry)

    result = fbref_pipeline_tasks.validate_fbref_run(
        airflow_run_id="scheduled__2026-07-11T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
    )

    assert result is summary
    traffic = telemetry.record_traffic_run.call_args.args[0]
    assert traffic["source"] == "fbref"
    assert traffic["total_mb"] == pytest.approx(3.0)
    assert telemetry.record_traffic_run.call_args.kwargs["dag_run_id"] == (
        "scheduled__2026-07-11T06:00:00+00:00"
    )
    assert telemetry.record_traffic_run.call_args.kwargs["replace_existing"] is True


@pytest.mark.unit
def test_control_traffic_bridge_is_non_fatal(monkeypatch):
    telemetry = SimpleNamespace(
        log_traffic_summary=MagicMock(side_effect=RuntimeError("trino unavailable")),
        record_traffic_run=MagicMock(),
    )
    monkeypatch.setitem(sys.modules, "utils.proxy_traffic", telemetry)

    fbref_pipeline_tasks._record_control_traffic(
        {"bytes_used": 1024}, airflow_run_id="manual__canary"
    )

    telemetry.record_traffic_run.assert_not_called()


@pytest.mark.unit
def test_replay_parse_rejects_import_safe_none_source_param(monkeypatch):
    pipeline = MagicMock(side_effect=AssertionError("must fail before pipeline"))
    monkeypatch.setattr(fbref_pipeline_tasks, "_pipeline", pipeline)

    with pytest.raises(ValueError, match="requires source_control_run_id"):
        fbref_pipeline_tasks.parse_fbref_wave(
            airflow_run_id="manual__missing-source",
            dag_id="dag_replay_fbref",
            page_kinds=["match"],
            run_type="replay",
            source_control_run_id=None,
            request_limit=0,
            byte_limit_mb=0,
            shard_size=1,
        )

    pipeline.assert_not_called()


@pytest.mark.unit
def test_replay_parse_propagates_source_run_preflight_failure(monkeypatch):
    pipeline = MagicMock()
    pipeline.parse_wave.side_effect = RuntimeError(
        "replay_source_run_not_found"
    )
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_pipeline", MagicMock(return_value=pipeline)
    )
    source_run_id = "00000000-0000-4000-8000-000000000099"

    with pytest.raises(RuntimeError, match="replay_source_run_not_found"):
        fbref_pipeline_tasks.parse_fbref_wave(
            airflow_run_id="manual__missing-source",
            dag_id="dag_replay_fbref",
            page_kinds=["match"],
            run_type="replay",
            source_control_run_id=source_run_id,
            request_limit=0,
            byte_limit_mb=0,
            shard_size=1,
        )

    assert pipeline.parse_wave.call_args.kwargs["source_run_id"] == source_run_id


@pytest.mark.unit
def test_replay_validation_propagates_source_run_preflight_failure(monkeypatch):
    pipeline = MagicMock()
    pipeline.validate_and_finish.side_effect = RuntimeError(
        "replay_source_run_not_terminal=running"
    )
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_pipeline", MagicMock(return_value=pipeline)
    )
    source_run_id = "00000000-0000-4000-8000-000000000099"

    with pytest.raises(RuntimeError, match="replay_source_run_not_terminal"):
        fbref_pipeline_tasks.validate_fbref_run(
            airflow_run_id="manual__live-source",
            dag_id="dag_replay_fbref",
            source_control_run_id=source_run_id,
        )

    assert (
        pipeline.validate_and_finish.call_args.kwargs["replay_source_run_id"]
        == source_run_id
    )


@pytest.mark.unit
def test_abort_callable_uses_control_store_without_constructing_pipeline(
    monkeypatch,
):
    control = MagicMock()
    control.abort_run.return_value = {
        "status": "failed",
        "targets_released": 2,
        "reservations_settled": 1,
    }
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "_control_store",
        MagicMock(return_value=control),
    )
    pipeline_factory = MagicMock(side_effect=AssertionError("must not construct"))
    monkeypatch.setattr(fbref_pipeline_tasks, "_pipeline", pipeline_factory)

    result = fbref_pipeline_tasks.abort_fbref_run(
        airflow_run_id="scheduled__2026-07-11T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
    )

    assert result["status"] == "failed"
    control.abort_run.assert_called_once_with(
        fbref_pipeline_tasks._control_run_id(
            airflow_run_id="scheduled__2026-07-11T06:00:00+00:00",
            dag_id="dag_ingest_fbref",
        ),
        error_class="AirflowDagFailure",
        error_message="Airflow DAG reached a terminal failure",
    )
    pipeline_factory.assert_not_called()


@pytest.mark.unit
def test_dag_failure_callback_aborts_without_telegram(monkeypatch):
    abort = MagicMock()
    monkeypatch.setattr(fbref_pipeline_tasks, "abort_fbref_run", abort)
    context = {
        "dag_run": SimpleNamespace(
            run_id="manual__failed", dag_id="dag_backfill_fbref"
        ),
        "task_instance": SimpleNamespace(task_id="fetch_wave_02"),
    }

    fbref_pipeline_tasks.fbref_dag_failure_callback(context)

    abort.assert_called_once_with(
        airflow_run_id="manual__failed",
        dag_id="dag_backfill_fbref",
        error_class="AirflowDagFailure",
        error_message="Airflow DAG failed after task fetch_wave_02",
    )
    callback_source = inspect.getsource(
        fbref_pipeline_tasks.fbref_dag_failure_callback
    ).casefold()
    assert "telegram" not in callback_source


@pytest.mark.unit
def test_dag_failure_callback_is_best_effort(monkeypatch):
    monkeypatch.setattr(
        fbref_pipeline_tasks,
        "abort_fbref_run",
        MagicMock(side_effect=RuntimeError("postgres unavailable")),
    )

    fbref_pipeline_tasks.fbref_dag_failure_callback({
        "dag_run": SimpleNamespace(
            run_id="manual__failed", dag_id="dag_replay_fbref"
        )
    })


@pytest.mark.unit
def test_fetch_wave_runs_in_an_unforked_subprocess(monkeypatch):
    """Playwright's sync API deadlocks in a process forked from the scheduler,
    so the browser wave must never be driven inside the Airflow task itself."""
    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return SimpleNamespace(
            returncode=0,
            stdout=(
                "camoufox noise\n"
                'FBREF_FETCH_WAVE_RESULT:{"claimed": 2, "fetched": 2}\n'
            ),
            stderr="",
        )

    monkeypatch.setattr(fbref_pipeline_tasks.subprocess, "run", fake_run)
    pipeline = MagicMock()
    monkeypatch.setattr(
        fbref_pipeline_tasks, "_pipeline", MagicMock(return_value=pipeline)
    )

    result = fbref_pipeline_tasks.fetch_fbref_wave(
        airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
        worker_id="current-wave-01",
        page_kinds=["competition", "season"],
        run_type="current",
        request_limit=200,
        byte_limit_mb=100,
        shard_size=25,
    )

    assert result == {"claimed": 2, "fetched": 2}
    pipeline.fetch_wave.assert_not_called()
    command = captured["command"]
    assert command[1] == fbref_pipeline_tasks.FETCH_WAVE_RUNNER
    assert "--page-kinds" in command
    assert command[command.index("--page-kinds") + 1] == "competition,season"
    assert command[command.index("--request-limit") + 1] == "200"
    assert command[command.index("--proxy-file") + 1] == (
        fbref_pipeline_tasks.DEFAULT_PROXY_FILE
    )


@pytest.mark.unit
def test_fetch_wave_fails_closed_when_the_subprocess_fails(monkeypatch):
    monkeypatch.setattr(
        fbref_pipeline_tasks.subprocess,
        "run",
        lambda command, **kwargs: SimpleNamespace(
            returncode=1, stdout="", stderr="clearance failed"
        ),
    )

    with pytest.raises(RuntimeError, match="exit code 1"):
        fbref_pipeline_tasks.fetch_fbref_wave(
            airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
            dag_id="dag_ingest_fbref",
            worker_id="current-wave-01",
            page_kinds=["competition"],
            run_type="current",
        )


@pytest.mark.unit
def test_fetch_wave_fails_closed_without_a_result_document(monkeypatch):
    monkeypatch.setattr(
        fbref_pipeline_tasks.subprocess,
        "run",
        lambda command, **kwargs: SimpleNamespace(
            returncode=0, stdout="browser noise only\n", stderr=""
        ),
    )

    with pytest.raises(RuntimeError, match="no result document"):
        fbref_pipeline_tasks.fetch_fbref_wave(
            airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
            dag_id="dag_ingest_fbref",
            worker_id="current-wave-01",
            page_kinds=["competition"],
            run_type="current",
        )


@pytest.mark.unit
def test_fetch_wave_fails_closed_when_the_subprocess_hangs(monkeypatch):
    """A hung clearance must not hold this wave's leases until they expire."""

    def fake_run(command, **kwargs):
        assert kwargs["timeout"] == fbref_pipeline_tasks.FETCH_WAVE_TIMEOUT_SECONDS
        raise fbref_pipeline_tasks.subprocess.TimeoutExpired(
            cmd=command, timeout=kwargs["timeout"]
        )

    monkeypatch.setattr(fbref_pipeline_tasks.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="exceeded 1800s"):
        fbref_pipeline_tasks.fetch_fbref_wave(
            airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
            dag_id="dag_ingest_fbref",
            worker_id="current-wave-01",
            page_kinds=["competition"],
            run_type="current",
        )
