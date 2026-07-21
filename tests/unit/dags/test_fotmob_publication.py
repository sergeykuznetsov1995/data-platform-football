"""Generation binding and cross-stack FotMob publication fence tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib
import json
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from utils import fotmob_publication as publication


pytestmark = pytest.mark.unit
GIT_SHA = "a" * 40
OTHER_SHA = "b" * 40
START = datetime(2026, 7, 20, 14, tzinfo=timezone.utc)
END = START + timedelta(days=1)


def _issue_930_scope_file() -> Path:
    return (
        Path(__file__).resolve().parents[3]
        / "configs"
        / "fotmob"
        / "issue-930-scopes.txt"
    )


def _isolated_runtime_evidence(tmp_path: Path):
    roots = {
        prefix: tmp_path / prefix.replace("/", "-")
        for prefix in publication.FOTMOB_ISOLATED_RUNTIME_ROOTS
    }
    for root in roots.values():
        root.mkdir(parents=True)
    for relative_path in publication.FOTMOB_ISOLATED_REQUIRED_RUNTIME_PATHS:
        prefix = next(
            candidate
            for candidate in sorted(roots, key=len, reverse=True)
            if relative_path.startswith(candidate + "/")
        )
        path = roots[prefix] / relative_path.removeprefix(prefix + "/")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(relative_path, encoding="utf-8")
    manifest = publication.isolated_runtime_manifest(roots=roots)
    report_path = (tmp_path / "deployment.json").resolve()
    deployment_id = "f" * 32
    container_id = "1" * 64
    report = {
        "schema_version": "fotmob-deploy-v2",
        "passed": True,
        "activation_state": "active",
        "kept_paused": False,
        "paused": [],
        "unpaused": sorted(publication.FOTMOB_EXPECTED_ISOLATED_DAGS),
        "container_report_path": str(report_path),
        "generated_at": "2026-07-21T10:00:00Z",
        "deployment_id": deployment_id,
        "git_sha": GIT_SHA,
        "scheduler_container_id": container_id,
        "resolved_image_id": "sha256:" + "2" * 64,
        "isolated_runtime_sha256": manifest,
    }
    report_path.write_text(json.dumps(report), encoding="utf-8")
    environment = {
        publication.FOTMOB_ISOLATED_STACK_ENV: "1",
        publication.FOTMOB_DEPLOYMENT_ID_ENV: deployment_id,
        publication.FOTMOB_RUNTIME_FINGERPRINT_ENV: GIT_SHA,
        publication.FOTMOB_DEPLOYMENT_REPORT_PATH_ENV: str(report_path),
        "TELEGRAM_BOT_TOKEN": "must-not-appear",
    }
    dag_run = SimpleNamespace(
        dag_id=publication.FOTMOB_ISOLATED_DAILY_DAG_ID,
        run_type="scheduled",
    )
    return roots, report_path, report, environment, dag_run


def test_scheduled_runtime_attestation_binds_report_container_and_manifest(tmp_path):
    roots, _path, _report, environment, dag_run = _isolated_runtime_evidence(
        tmp_path
    )

    result = publication.attest_fotmob_isolated_runtime(
        environ=environment,
        hostname="1" * 12,
        roots=roots,
        dag_run=dag_run,
    )

    assert result["deployment_id"] == "f" * 32
    assert result["scheduler_container_id"] == "1" * 64
    assert result["runtime_file_count"] == len(
        publication.FOTMOB_ISOLATED_REQUIRED_RUNTIME_PATHS
    )
    assert "must-not-appear" not in json.dumps(result)


def test_scheduled_runtime_attestation_rejects_drift_manual_and_pending(tmp_path):
    roots, report_path, report, environment, dag_run = _isolated_runtime_evidence(
        tmp_path
    )
    service = roots["scrapers"] / "fotmob/service.py"
    service.write_text("drift", encoding="utf-8")
    with pytest.raises(Exception, match="runtime bytes differ"):
        publication.attest_fotmob_isolated_runtime(
            environ=environment,
            hostname="1" * 12,
            roots=roots,
            dag_run=dag_run,
        )

    service.write_text("scrapers/fotmob/service.py", encoding="utf-8")
    with pytest.raises(Exception, match="exact scheduled DagRun"):
        publication.attest_fotmob_isolated_runtime(
            environ=environment,
            hostname="1" * 12,
            roots=roots,
            dag_run=SimpleNamespace(
                dag_id=publication.FOTMOB_ISOLATED_DAILY_DAG_ID,
                run_type="manual",
            ),
        )

    report["activation_state"] = "committed_pending_trigger"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(Exception, match="no completed active"):
        publication.attest_fotmob_isolated_runtime(
            environ=environment,
            hostname="1" * 12,
            roots=roots,
            dag_run=dag_run,
        )


def test_kept_paused_attestation_allows_only_exact_issue930_bronze_and_silver(
    tmp_path,
):
    roots, report_path, report, environment, _dag_run = _isolated_runtime_evidence(
        tmp_path
    )
    report.update(
        activation_state="kept_paused",
        kept_paused=True,
        paused=sorted(publication.FOTMOB_EXPECTED_ISOLATED_DAGS),
        unpaused=[],
    )
    start = datetime(2026, 7, 22, 10, 0, 1, tzinfo=timezone.utc)
    binding = publication.make_publication_binding(
        owner="isolated",
        data_interval_start=start,
        data_interval_end=start + timedelta(seconds=1),
        fingerprint=GIT_SHA,
    )
    lifecycle_publication = {
        "generation_id": publication.make_generation_id(binding),
        "binding": binding,
    }
    report_path.write_text(json.dumps(report), encoding="utf-8")
    scopes = _issue_930_scope_file().read_text(encoding="utf-8").splitlines()
    bronze = publication.attest_fotmob_isolated_runtime(
        environ=environment,
        hostname="1" * 12,
        roots=roots,
        require_scheduled_owner=False,
        allow_kept_paused_writer=True,
        writer_identity={
            "component": "bronze_runner",
            "mode": "replay",
            "scopes": scopes,
            "entities": sorted(publication.FOTMOB_ISSUE930_WRITER_ENTITIES),
            "competition_limit": 0,
            "season_limit": 0,
            "publication": lifecycle_publication,
        },
    )
    assert bronze["issue930_lifecycle"] == {
        "mode": "replay",
        "attempt": 1,
        "generation_id": lifecycle_publication["generation_id"],
    }

    silver_run = SimpleNamespace(
        dag_id="dag_transform_fotmob_silver",
        run_id="fotmob_silver__" + lifecycle_publication["generation_id"],
        conf={"fotmob_publication": lifecycle_publication},
    )
    silver = publication.attest_fotmob_isolated_runtime(
        environ=environment,
        hostname="1" * 12,
        roots=roots,
        require_scheduled_owner=False,
        allow_kept_paused_writer=True,
        dag_run=silver_run,
    )
    assert silver["issue930_lifecycle"]["mode"] == "replay"

    with pytest.raises(Exception, match="mode/attempt identity differs"):
        publication.attest_fotmob_isolated_runtime(
            environ=environment,
            hostname="1" * 12,
            roots=roots,
            require_scheduled_owner=False,
            allow_kept_paused_writer=True,
            writer_identity={
                "component": "bronze_runner",
                "mode": "daily",
                "publication": lifecycle_publication,
            },
        )


def test_isolated_owner_missing_role_env_never_skips_attestation(tmp_path):
    roots, _path, _report, environment, dag_run = _isolated_runtime_evidence(
        tmp_path
    )
    environment.pop(publication.FOTMOB_ISOLATED_STACK_ENV)
    with pytest.raises(Exception, match="runtime identity differs"):
        publication.attest_fotmob_isolated_runtime(
            environ=environment,
            hostname="1" * 12,
            roots=roots,
            dag_run=dag_run,
        )


def test_daily_contract_derives_exact_competitions_from_immutable_scope_bytes():
    contract = publication.load_fotmob_daily_competition_contract(
        _issue_930_scope_file(),
        scope_sha256=publication.FOTMOB_DAILY_SCOPE_SHA256,
        competition_ids_sha256=(
            publication.FOTMOB_DAILY_COMPETITION_IDS_SHA256
        ),
    )

    assert contract["scope_count"] == 158
    assert contract["competition_count"] == 21
    assert contract["competition_ids"] == list(
        publication.FOTMOB_DAILY_COMPETITION_IDS
    )
    assert contract["competition_ids_sha256"] == (
        "664f972d5d86002131293bcc8da8382f6b7378cd43a8bd37a247c321decf689a"
    )


def test_daily_contract_rejects_same_count_identity_substitution(tmp_path):
    raw = _issue_930_scope_file().read_text(encoding="utf-8")
    mutated = tmp_path / "scopes.txt"
    mutated.write_text(raw.replace("42=2010/2011", "41=2010/2011"), encoding="utf-8")

    with pytest.raises(ValueError, match="artifact bytes differ"):
        publication.load_fotmob_daily_competition_contract(
            mutated,
            scope_sha256=publication.FOTMOB_DAILY_SCOPE_SHA256,
            competition_ids_sha256=(
                publication.FOTMOB_DAILY_COMPETITION_IDS_SHA256
            ),
        )


def test_daily_trigger_conf_is_exact_all_entity_profile():
    assert publication.fotmob_daily_trigger_conf() == {
        "mode": "daily",
        "scope": "",
        "daily_contract": "fotmob-daily-v1",
        "competition_scope_file": publication.FOTMOB_DAILY_SCOPE_FILE,
        "competition_scope_sha256": publication.FOTMOB_DAILY_SCOPE_SHA256,
        "competition_ids_sha256": (
            publication.FOTMOB_DAILY_COMPETITION_IDS_SHA256
        ),
        "entities": "season,leaderboards,matches,teams,players,transfers",
        "max_requests": 10_000,
        "max_direct_mib": 512,
        "competition_limit": 0,
        "season_limit": 0,
        "requests_per_minute": 60,
    }


def _binding(owner: str = "isolated", fingerprint: str = GIT_SHA):
    return publication.make_publication_binding(
        owner=owner,
        data_interval_start=START,
        data_interval_end=END,
        fingerprint=fingerprint,
    )


def _context(binding=None, generation_id=None, **states):
    binding = binding or _binding()
    generation_id = generation_id or publication.make_generation_id(binding)
    dag_run = SimpleNamespace(
        dag_id="dag_master_pipeline",
        run_id="scheduled__2026-07-20T14:00:00+00:00",
        conf={
            "fotmob_publication": {
                "generation_id": generation_id,
                "binding": binding,
            }
        },
        data_interval_start=START,
        data_interval_end=END,
        get_task_instances=lambda: [
            SimpleNamespace(task_id=task_id, state=state)
            for task_id, state in states.items()
        ],
    )
    return {
        "dag_run": dag_run,
        "data_interval_start": START,
        "data_interval_end": END,
        "ti": MagicMock(),
    }


def test_generation_id_binds_exact_interval_runtime_and_owner(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    binding = _binding()

    assert publication.make_generation_id(binding) == publication.make_generation_id(
        dict(binding)
    )
    assert publication.make_generation_id(binding) != publication.make_generation_id(
        _binding(owner="shared")
    )
    assert publication.make_generation_id(binding) != publication.make_generation_id(
        _binding(fingerprint=OTHER_SHA)
    )
    shifted = dict(binding)
    shifted["data_interval_start"] = (
        START + timedelta(days=1)
    ).isoformat(timespec="microseconds")
    shifted["data_interval_end"] = (
        END + timedelta(days=1)
    ).isoformat(timespec="microseconds")
    assert publication.make_generation_id(binding) != publication.make_generation_id(
        shifted
    )


def test_child_rejects_stale_runtime_and_wrong_generation(monkeypatch):
    binding = _binding()
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, OTHER_SHA)
    with pytest.raises(Exception, match="runtime fingerprint mismatch"):
        publication.publication_from_context(_context(binding=binding))

    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    with pytest.raises(Exception, match="does not match its binding"):
        publication.publication_from_context(
            _context(binding=binding, generation_id=str("1" * 32))
        )


def test_child_rejects_noncanonical_binding_even_with_matching_id(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    binding = _binding()
    generation_id = publication.make_generation_id(binding)
    binding["source"] = "not-fotmob"

    with pytest.raises(Exception, match="binding is not canonical"):
        publication.publication_from_context(
            _context(binding=binding, generation_id=generation_id)
        )


def test_master_waits_for_writing_and_claims_only_exact_ready(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    context = _context()
    binding = _binding()
    generation_id = publication.make_generation_id(binding)
    store = SimpleNamespace(
        get_publication_generation=MagicMock(
            return_value={
                "generation_id": generation_id,
                "binding": binding,
                "phase": "writing",
                "status": "running",
                "active": True,
            }
        ),
        claim_publication_generation=MagicMock(),
    )
    monkeypatch.setattr(publication, "_control_store", lambda: store)

    assert publication.wait_and_claim_fotmob_publication(
        publication_owner="isolated", **context
    ) is False
    store.claim_publication_generation.assert_not_called()

    store.get_publication_generation.return_value.update(
        phase="ready", status="succeeded"
    )
    store.claim_publication_generation.return_value = {"phase": "consuming"}
    assert publication.wait_and_claim_fotmob_publication(
        publication_owner="isolated", **context
    ) is True
    kwargs = store.claim_publication_generation.call_args.kwargs
    assert kwargs["binding"] == binding
    assert kwargs["consumer"] == {
        "dag_id": "dag_master_pipeline",
        "run_id": "scheduled__2026-07-20T14:00:00+00:00",
    }
    assert kwargs["ttl_seconds"] == 14 * 24 * 60 * 60


def test_initializer_acquires_exact_generation_before_return(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    monkeypatch.setenv(publication.FOTMOB_ISOLATED_STACK_ENV, "1")
    attestation = MagicMock(return_value={"runtime_manifest_sha256": "c" * 64})
    monkeypatch.setattr(
        publication, "attest_fotmob_isolated_runtime", attestation
    )
    initialize = MagicMock(return_value={"phase": "writing", "active": True})
    store = SimpleNamespace(initialize_publication_generation=initialize)
    monkeypatch.setattr(publication, "_control_store", lambda: store)
    context = _context()
    context["dag_run"].dag_id = "dag_trigger_fotmob_daily"

    result = publication.initialize_fotmob_publication(
        publication_owner="isolated", **context
    )

    attestation.assert_called_once()
    assert result["generation_id"] == publication.make_generation_id(_binding())
    args, kwargs = initialize.call_args
    assert args == (result["generation_id"],)
    assert kwargs["dag_id"] == "dag_trigger_fotmob_daily"
    assert kwargs["binding"] == _binding()
    assert kwargs["source"] == "fotmob"
    assert kwargs["ttl_seconds"] == 14 * 24 * 60 * 60


def test_writer_preflight_holds_exact_phase_guard(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    monkeypatch.setenv(publication.FOTMOB_ISOLATED_STACK_ENV, "1")
    attestation = MagicMock(return_value={"runtime_manifest_sha256": "c" * 64})
    monkeypatch.setattr(
        publication, "attest_fotmob_isolated_runtime", attestation
    )
    events = []

    @contextmanager
    def guard(run_id, *, source):
        events.append(("enter", run_id, source))
        yield {"phase": "writing"}
        events.append(("exit", run_id, source))

    monkeypatch.setattr(
        publication,
        "_control_store",
        lambda: SimpleNamespace(guard_publication_writer=guard),
    )
    expected_id = publication.make_generation_id(_binding())

    assert publication.validate_fotmob_writer_fence(**_context())[
        "generation_id"
    ] == expected_id
    assert events == [
        ("enter", expected_id, "fotmob"),
        ("exit", expected_id, "fotmob"),
    ]
    assert attestation.call_args.kwargs["require_scheduled_owner"] is False
    assert attestation.call_args.kwargs["allow_kept_paused_writer"] is True


def test_isolated_writer_missing_role_cannot_skip_runtime_attestation(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    monkeypatch.delenv(publication.FOTMOB_ISOLATED_STACK_ENV, raising=False)
    store = MagicMock()
    monkeypatch.setattr(publication, "_control_store", store)

    with pytest.raises(Exception, match="report path is not exactly configured"):
        publication.validate_fotmob_writer_fence(**_context())

    store.assert_not_called()


def test_candidate_is_exact_digested_and_seal_renews_full_lease(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    record = MagicMock(return_value={"phase": "writing"})
    seal = MagicMock(return_value={"phase": "ready"})
    monkeypatch.setattr(
        publication,
        "_control_store",
        lambda: SimpleNamespace(
            record_publication_candidate=record,
            seal_publication_generation=seal,
        ),
    )
    values = {
        "silver_transforms.a": {
            "status": "success",
            "table": "iceberg.silver.a",
            "rows": 10,
        },
        "silver_transforms.b": {
            "status": "success",
            "table": "iceberg.silver.b",
            "rows": 20,
        },
        "validate_silver": {"status": "success", "warnings": []},
        "validate_silver_quality": {"passed": 2, "errors": [], "warnings": []},
    }
    context = _context()
    context["ti"].xcom_pull.side_effect = lambda task_ids: values[task_ids]

    candidate = publication.record_fotmob_silver_candidate(
        transform_task_ids=["silver_transforms.b", "silver_transforms.a"],
        **context,
    )
    assert candidate["transform_task_ids"] == [
        "silver_transforms.a",
        "silver_transforms.b",
    ]
    assert len(candidate["digest"]) == 64
    assert record.call_args.args[1] == candidate

    assert publication.seal_fotmob_publication(**context)["phase"] == "ready"
    assert seal.call_args.kwargs["ttl_seconds"] == 14 * 24 * 60 * 60


def test_xref_consumer_preflight_requires_full_active_claim(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    context = _context()
    context["dag_run"].conf.update(
        publication_owner="dag_sofascore_pipeline",
        master_run_id="scheduled__2026-07-20T14:00:00+00:00",
    )
    consumer = {
        "dag_id": "dag_sofascore_pipeline",
        "run_id": "scheduled__2026-07-20T14:00:00+00:00",
    }
    store = SimpleNamespace(
        get_publication_generation=MagicMock(
            return_value={
                "binding": _binding(),
                "phase": "consuming",
                "status": "succeeded",
                "active": True,
                "consumer": consumer,
            }
        )
    )
    monkeypatch.setattr(publication, "_control_store", lambda: store)

    result = publication.validate_fotmob_consumer_fence(**context)
    assert result["consumer"] == consumer
    assert result["phase"] == "consuming"

    store.get_publication_generation.return_value["consumer"] = {
        "dag_id": "dag_master_pipeline",
        "run_id": consumer["run_id"],
    }
    with pytest.raises(Exception, match="consumer identity mismatch"):
        publication.validate_fotmob_consumer_fence(**context)


def test_raw_manual_xref_has_no_publication_authority(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    context = _context()
    context["dag_run"].conf = {}

    with pytest.raises(Exception, match="DagRun conf requires fotmob_publication"):
        publication.validate_fotmob_consumer_fence(**context)


@pytest.mark.parametrize("phase,status", [("failed", "failed"), ("published", "succeeded")])
def test_master_rejects_failed_or_already_published_generation(
    monkeypatch, phase, status
):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    binding = _binding()
    store = SimpleNamespace(
        get_publication_generation=lambda *_args, **_kwargs: {
            "binding": binding,
            "phase": phase,
            "status": status,
            "active": phase != "published",
        }
    )
    monkeypatch.setattr(publication, "_control_store", lambda: store)

    with pytest.raises(Exception, match="terminal or invalid"):
        publication.wait_and_claim_fotmob_publication(
            publication_owner="isolated", **_context(binding=binding)
        )


def test_lost_child_response_never_releases_failed_writer(monkeypatch):
    """A failed parent trigger may leave the exact Silver child still writing."""

    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    fail = MagicMock(return_value={"phase": "failed", "released": False})
    monkeypatch.setattr(
        publication,
        "_control_store",
        lambda: SimpleNamespace(fail_publication_generation=fail),
    )
    context = _context(
        trigger_silver_transform="failed",
        seal_fotmob_publication_ready="upstream_failed",
    )

    with pytest.raises(Exception, match="lock retained"):
        publication.fail_unsealed_fotmob_publication(
            success_task_id="seal_fotmob_publication_ready",
            writer_task_ids=["trigger_silver_transform"],
            **context,
        )
    assert fail.call_args.kwargs["safe_to_release"] is False


def test_master_publishes_and_releases_only_after_sensor_and_report(monkeypatch):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    complete = MagicMock(return_value={"phase": "published", "released": True})
    store = SimpleNamespace(
        get_publication_generation=lambda *_args, **_kwargs: {
            "binding": _binding(),
            "phase": "consuming",
            "status": "succeeded",
        },
        complete_publication_generation=complete,
    )
    monkeypatch.setattr(publication, "_control_store", lambda: store)
    context = _context(
        wait_for_fotmob_publication="success",
        generate_pipeline_report="success",
    )

    result = publication.finalize_fotmob_publication_consumer(
        publication_owner="isolated",
        report_task_id="generate_pipeline_report",
        sensor_task_id="wait_for_fotmob_publication",
        **context,
    )
    assert result == {"phase": "published", "released": True}
    assert complete.call_args.kwargs["published"] is True
    assert complete.call_args.kwargs["consumer"] == {
        "dag_id": "dag_master_pipeline",
        "run_id": "scheduled__2026-07-20T14:00:00+00:00",
    }


@pytest.mark.parametrize("phase", ["ready", "consuming"])
def test_sofascore_failure_retains_ready_or_consuming_lock(monkeypatch, phase):
    monkeypatch.setenv(publication.FOTMOB_RUNTIME_FINGERPRINT_ENV, GIT_SHA)
    complete = MagicMock()
    fail = MagicMock()
    store = SimpleNamespace(
        get_publication_generation=lambda *_args, **_kwargs: {
            "binding": _binding(),
            "phase": phase,
            "status": "succeeded",
            "active": True,
        },
        complete_publication_generation=complete,
        fail_publication_generation=fail,
    )
    monkeypatch.setattr(publication, "_control_store", lambda: store)
    context = _context(
        wait_for_fotmob_publication=(
            "failed" if phase == "ready" else "success"
        ),
        trigger_e4_transforms="failed",
    )

    with pytest.raises(Exception, match="lock retained"):
        publication.finalize_fotmob_publication_consumer(
            publication_owner="isolated",
            report_task_id="trigger_e4_transforms",
            sensor_task_id="wait_for_fotmob_publication",
            release_unclaimed_ready_on_failure=False,
            **context,
        )
    complete.assert_not_called()
    fail.assert_not_called()


def test_consumer_trigger_conf_is_complete_and_uses_exact_sensor_xcom():
    conf = publication.fotmob_consumer_trigger_conf("dag_master_pipeline")

    assert conf["publication_owner"] == "dag_master_pipeline"
    assert conf["master_run_id"] == "{{ run_id }}"
    payload = conf["fotmob_publication"]
    assert "wait_for_fotmob_publication" in payload["generation_id"]
    assert set(payload["binding"]) == set(
        publication.FOTMOB_PUBLICATION_BINDING_FIELDS
    )
    assert all(
        "wait_for_fotmob_publication" in value
        for value in payload["binding"].values()
    )


def test_isolated_owner_initializes_before_deterministic_exact_trigger(
    monkeypatch,
):
    from airflow.operators.python import PythonOperator

    monkeypatch.setenv("FOTMOB_ISOLATED_STACK", "1")
    PythonOperator._instances.clear()
    sys.modules.pop("dag_trigger_fotmob_daily", None)
    sys.modules.pop("dags.dag_trigger_fotmob_daily", None)
    module = importlib.import_module("dag_trigger_fotmob_daily")
    tasks = {task.task_id: task for task in PythonOperator._instances}
    initializer = tasks["initialize_fotmob_publication"]
    trigger = tasks["trigger_fotmob_ingest"]
    finalizer = tasks["finalize_fotmob_publication"]

    assert trigger.upstream_task_ids == {initializer.task_id}
    assert trigger._init_kwargs["trigger_run_id"].startswith("fotmob_ingest__")
    assert trigger._init_kwargs["logical_date"] == "{{ logical_date.isoformat() }}"
    assert trigger._init_kwargs["reset_dag_run"] is False
    assert trigger._init_kwargs["execution_timeout"].total_seconds() == 14 * 3600
    expected_daily = publication.fotmob_daily_trigger_conf()
    assert {
        key: trigger._init_kwargs["conf"][key] for key in expected_daily
    } == expected_daily
    assert "execution_date" not in trigger._init_kwargs
    assert finalizer.upstream_task_ids == {trigger.task_id}
    assert finalizer._init_kwargs["trigger_rule"] == "all_done"
    assert module.dag._dag_kwargs["schedule"] == "0 14 * * *"
