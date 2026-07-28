"""Production orchestration contracts for ``dag_ingest_understat``."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest


def _reload_dag_module():
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    sys.modules.pop("dag_ingest_understat", None)
    sys.modules.pop("dags.dag_ingest_understat", None)
    return importlib.import_module("dag_ingest_understat")


@pytest.fixture
def dag_module():
    return _reload_dag_module()


def _bash(task_id: str):
    from airflow.operators.bash import BashOperator

    return next(task for task in BashOperator._instances if task.task_id == task_id)


def _python(task_id: str):
    from airflow.operators.python import PythonOperator

    return next(task for task in PythonOperator._instances if task.task_id == task_id)


def _scope(
    league="ENG-Premier League",
    season="2526",
    source_id=2025,
    *,
    discovered=True,
):
    return SimpleNamespace(
        league=league,
        season=season,
        source_season_id=source_id,
        is_closed=True,
        discovered=discovered,
    )


def test_daily_dag_is_the_single_0900_owner(dag_module):
    kwargs = dag_module.dag._dag_kwargs
    assert kwargs["schedule"] == "0 9 * * *"
    assert kwargs["max_active_runs"] == 1
    assert kwargs["max_active_tasks"] == 1
    assert kwargs["catchup"] is False
    assert kwargs.get("params", {}) == {}


def test_runner_is_dynamically_mapped_one_scope_per_subprocess(dag_module):
    plan = _python("plan_current_scopes")
    runner = _bash("run_current_scope")
    validator = _python("validate_current_scope")

    assert runner.is_mapped is True
    assert runner._expand_kwargs["env"].operator is plan
    assert validator.is_mapped is True
    assert validator._expand_kwargs["op_kwargs"].operator is plan
    assert plan._init_kwargs["pool"] == "ingest_scraper_pool"
    assert plan._init_kwargs["priority_weight"] == dag_module.CURRENT_PRIORITY
    assert runner._init_kwargs["pool"] == "ingest_scraper_pool"
    assert runner._init_kwargs["priority_weight"] == dag_module.CURRENT_PRIORITY
    assert "--mode \"${UNDERSTAT_MODE}\"" in runner.bash_command
    assert "--league \"${UNDERSTAT_LEAGUE}\"" in runner.bash_command
    assert "--season-slug \"${UNDERSTAT_SEASON_SLUG}\"" in runner.bash_command
    assert "--source-season-id \"${UNDERSTAT_SOURCE_SEASON_ID}\"" in runner.bash_command
    assert "--source-discovered \"${UNDERSTAT_SOURCE_DISCOVERED}\"" in runner.bash_command
    assert "--output \"${UNDERSTAT_RESULT_PATH}\"" in runner.bash_command
    assert "/tmp/understat_result.json" not in runner.bash_command


def test_scope_environment_uses_canonical_slug_and_unique_result(dag_module):
    first = dag_module.scope_environment(
        _scope(), mode="current", run_id="scheduled__2026-07-27T09:00:00+00:00"
    )
    second = dag_module.scope_environment(
        _scope(league="ESP-La Liga"),
        mode="current",
        run_id="scheduled__2026-07-27T09:00:00+00:00",
    )

    assert first["UNDERSTAT_SEASON_SLUG"] == "2526"
    assert first["UNDERSTAT_SOURCE_SEASON_ID"] == "2025"
    assert first["UNDERSTAT_SOURCE_DISCOVERED"] == "true"
    assert first["UNDERSTAT_RESULT_PATH"] != second["UNDERSTAT_RESULT_PATH"]
    assert first["UNDERSTAT_RESULT_PATH"].startswith("/tmp/understat_current_")
    assert "UNDERSTAT_REPARSE" not in second
    assert first["PYTHONPATH"] == "/opt/airflow:/opt/airflow/dags"


@pytest.mark.parametrize("bad_slug", ["2025", "25", "2025/2026", "abcd"])
def test_scope_environment_rejects_ambiguous_or_invalid_seasons(dag_module, bad_slug):
    from airflow.exceptions import AirflowException

    with pytest.raises(AirflowException, match="canonical four-digit slug"):
        dag_module.scope_environment(
            _scope(season=bad_slug), mode="current", run_id="manual__one"
        )


def test_discovery_conflict_fails_closed(dag_module):
    from airflow.exceptions import AirflowException

    scopes = [_scope(source_id=2025), _scope(source_id=2026)]
    with pytest.raises(AirflowException, match="conflicting source season ids"):
        dag_module._deduplicate_scopes(scopes)


def test_current_planner_uses_runtime_rolling_catalog_and_config_scope(
    dag_module, monkeypatch
):
    import scrapers.understat as understat

    calls = []

    class Client:
        pass

    class Catalog:
        def __init__(self, client):
            assert isinstance(client, Client)

        def rolling_scopes(self, **kwargs):
            calls.append(kwargs)
            return [
                _scope(),
                _scope(league="UNKNOWN-League"),
            ]

    monkeypatch.setattr(understat, "UnderstatClient", Client)
    monkeypatch.setattr(understat, "UnderstatCatalog", Catalog)

    plan = dag_module.plan_current_scopes(run_id="scheduled__one")

    assert calls == [{"window": 2, "probe_next": True}]
    assert len(plan) == 1
    assert plan[0]["UNDERSTAT_LEAGUE"] == "ENG-Premier League"
    assert "UNDERSTAT_REPARSE" not in plan[0]


def _result(
    path: Path,
    *,
    status="complete",
    top_overrides=None,
    attempt_overrides=None,
):
    from scrapers.understat.manifest import (
        CONTRACT_VERSION,
        UNDERSTAT_ENTITIES,
        ScopeAttempt,
        ScopeKey,
    )

    entity_status = status
    row_count = 1 if status == "complete" else 0
    attempt = ScopeAttempt(
        scope=ScopeKey(
            league="ENG-Premier League",
            season="2526",
            source_league="EPL",
            source_season_id="2025",
        ),
        status=status,
        batch_id="batch-1",
        run_id="scheduled__one",
        attempt_id="attempt-1",
        mode="current",
        parser_version="understat-native-v1",
        contract_version=CONTRACT_VERSION,
        entity_statuses={key: entity_status for key in UNDERSTAT_ENTITIES},
        row_counts={key: row_count for key in UNDERSTAT_ENTITIES},
        natural_key_counts={key: row_count for key in UNDERSTAT_ENTITIES},
        payload_hashes={
            key: (f"sha256:{key}" if row_count else "")
            for key in UNDERSTAT_ENTITIES
        },
    ).to_dict()
    attempt.update(attempt_overrides or {})
    payload = {
        "status": status,
        "league": "ENG-Premier League",
        "season": "2526",
        "source_season_id": 2025,
        "batch_id": "batch-1",
        "entity_statuses": attempt["entity_statuses"],
        "row_counts": attempt["row_counts"],
        "errors": [],
        "scope_attempt": attempt,
        **(top_overrides or {}),
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return payload


def _validation_context(path: Path, *, mode="current"):
    return {
        "UNDERSTAT_RESULT_PATH": str(path),
        "UNDERSTAT_MODE": mode,
        "UNDERSTAT_LEAGUE": "ENG-Premier League",
        "UNDERSTAT_SEASON_SLUG": "2526",
        "UNDERSTAT_SOURCE_SEASON_ID": "2025",
    }


def test_exact_scope_validation_accepts_complete_publication(dag_module, tmp_path):
    path = tmp_path / "result.json"
    expected = _result(path)
    assert dag_module.validate_scope_result(**_validation_context(path)) == expected


def test_current_validation_accepts_explicit_not_published_probe(dag_module, tmp_path):
    path = tmp_path / "result.json"
    _result(
        path,
        status="not_published",
    )
    assert dag_module.validate_scope_result(**_validation_context(path))["status"] == (
        "not_published"
    )


def test_backfill_validation_rejects_not_published_scope(dag_module, tmp_path):
    from airflow.exceptions import AirflowException

    path = tmp_path / "result.json"
    _result(path, status="not_published")
    with pytest.raises(AirflowException, match="terminal state"):
        dag_module.validate_scope_result(
            **_validation_context(path, mode="backfill")
        )


def test_scope_validation_rejects_stale_artifact_identity(dag_module, tmp_path):
    from airflow.exceptions import AirflowException

    path = tmp_path / "result.json"
    _result(path, top_overrides={"league": "ESP-La Liga"})
    with pytest.raises(AirflowException, match="scope mismatch"):
        dag_module.validate_scope_result(**_validation_context(path))


def test_complete_scope_requires_publication_evidence(dag_module, tmp_path):
    from airflow.exceptions import AirflowException

    path = tmp_path / "result.json"
    _result(path, attempt_overrides={"batch_id": ""})
    with pytest.raises(AirflowException, match="publication evidence"):
        dag_module.validate_scope_result(**_validation_context(path))
