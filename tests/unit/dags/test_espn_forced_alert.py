from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest


def test_forced_schema_alert_is_persisted_before_the_health_task_fails(monkeypatch):
    from airflow.exceptions import AirflowException
    from dags.utils import espn_native_tasks

    verdict_ref = {"uri": "file:///immutable/verdict.json", "sha256": "a" * 64}
    verdict = {
        "kind": "espn-terminal-verdict-v1",
        "schema_version": 1,
        "dag_id": "dag_ingest_espn",
        "run_id": "forced-alert-run",
        "attempt": 1,
        "status": "failed",
        "failures": ["registry schema drift was forced by this test"],
        "scope_metrics": {},
    }
    written = {}

    monkeypatch.setattr(
        espn_native_tasks,
        "_read_ref",
        lambda ref, **_kwargs: verdict if ref == verdict_ref else None,
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_ref_for_uri",
        lambda _uri: (_ for _ in ()).throw(FileNotFoundError("no plan index")),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_optional_payload", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        espn_native_tasks, "_artifact_root", lambda: "file:///artifacts"
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, payload, **_kwargs: (
            written.update(uri=uri, payload=payload) or {"uri": uri, "sha256": "b" * 64}
        ),
    )

    class Store:
        def current_time(self):
            return datetime(2026, 7, 31, 12, tzinfo=timezone.utc)

    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: Store()),
    )
    context = {
        "dag": SimpleNamespace(dag_id="dag_ingest_espn"),
        "run_id": "forced-alert-run",
        "logical_date": datetime(2026, 7, 31, 10, tzinfo=timezone.utc),
        "params": {"attempt": 1},
    }

    with pytest.raises(AirflowException, match="hard alerts"):
        espn_native_tasks.record_health_metrics(
            verdict_ref=verdict_ref,
            **context,
        )

    assert written["uri"].endswith("/health.json")
    health = written["payload"]
    assert health["kind"] == "espn-health-result-v1"
    schema_alert = next(
        item for item in health["alerts"] if item["code"] == "schema_drift"
    )
    assert schema_alert["severity"] == "hard"
    assert schema_alert["identity_kind"] == "pre-admission-run"
    assert len(schema_alert["alert_sha256"]) == 64
