import copy
import json
import stat
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import fotmob_observe as mod


ACTIVATION_AT = "2026-08-07T13:35:00+00:00"
OWNER_RUN_ID = "scheduled__2026-08-07T14:00:00+00:00"
SOFA_RUN_ID = "scheduled__2026-08-06T14:00:00+00:00"


def _binding() -> dict[str, str]:
    return {
        "schema": "fotmob-publication-v1",
        "source": "fotmob",
        "owner": "isolated",
        "data_interval_start": "2026-08-06T14:00:00.000000+00:00",
        "data_interval_end": "2026-08-07T14:00:00.000000+00:00",
        "runtime_fingerprint": "b" * 40,
    }


def _generation_id(binding: dict[str, str] | None = None) -> str:
    value = binding or _binding()
    material = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"fotmob-publication:{material}"))


def _publication() -> dict:
    return {"generation_id": _generation_id(), "binding": _binding()}


def _context(tmp_path: Path) -> dict:
    evidence = tmp_path / "evidence"
    evidence.mkdir(exist_ok=True)
    deployment = evidence / "deployment.json"
    deployment.write_text("{}", encoding="utf-8")
    deployment.chmod(0o600)
    return {
        "activation_state": "active",
        "kept_paused": False,
        "deployment_id": "a" * 32,
        "git_sha": "b" * 40,
        "scheduler_container_id": "c" * 64,
        "metadb_container_id": "e" * 64,
        "resolved_image_id": "sha256:" + "f" * 64,
        "resolved_postgres_image_id": "sha256:" + "1" * 64,
        "evidence_dir": str(evidence.resolve()),
        "deployment_report": str(deployment.resolve()),
        "automatic_rollout_summary": {
            "phase": "active",
            "owner_at": ACTIVATION_AT,
            "passed": True,
        },
        "paused": sorted(mod.LEGACY_OWNER_DAGS),
        "unpaused": sorted(mod.ACTIVE_AUTOMATIC_DAGS),
        "shared_handoff_final": {
            "shared_scheduler_container": "d" * 64,
        },
    }


def _isolated_snapshot() -> dict:
    publication = _publication()
    decision_conf = {
        "mode": "daily",
        "scope": "",
        "catalog_contract": "fotmob-catalog-v1",
    }
    return {
        "schema_version": mod.ISOLATED_SNAPSHOT_SCHEMA,
        "deployment": {
            "deployment_id": "a" * 32,
            "git_sha": "b" * 40,
            "scheduler_container_id": "c" * 64,
        },
        "activation_at": ACTIVATION_AT,
        "daily_runs": [
            {
                "owner": {
                    "dag_id": mod.OWNER_DAG_ID,
                    "run_id": OWNER_RUN_ID,
                    "run_type": "scheduled",
                    "state": "success",
                    "logical_date": "2026-08-07T14:00:00+00:00",
                    "data_interval_start": "2026-08-07T13:55:00+00:00",
                    "data_interval_end": "2026-08-07T14:00:00+00:00",
                    "start_date": "2026-08-07T14:00:01+00:00",
                },
                "attest_task_states": ["success"],
                "attest_task_start_dates": ["2026-08-07T14:00:02+00:00"],
                "attestations": [
                    {
                        "deployment_id": "a" * 32,
                        "git_sha": "b" * 40,
                        "scheduler_container_id": "c" * 64,
                    }
                ],
                "decisions": [
                    {
                        "lane": "daily",
                        "selected_date": "2026-08-07",
                        "conf": decision_conf,
                    }
                ],
                "initializers": [{**publication, "state": {"phase": "writing"}}],
                "trigger_states": ["success"],
                "active_tasks": [],
                "ingest_runs": [
                    {
                        "dag_id": mod.INGEST_DAG_ID,
                        "run_id": f"fotmob_orchestrated__{_generation_id()}",
                        "run_type": "manual",
                        "state": "success",
                        "conf": {**decision_conf, "fotmob_publication": publication},
                    }
                ],
                "ingest_trigger_states": ["success"],
                "ingest_active_tasks": [],
                "silver_runs": [
                    {
                        "dag_id": mod.SILVER_DAG_ID,
                        "run_id": f"fotmob_silver__{_generation_id()}",
                        "run_type": "manual",
                        "state": "success",
                        "conf": {"fotmob_publication": publication},
                    }
                ],
                "silver_active_tasks": [],
                "publication": {
                    **publication,
                    "source": "fotmob",
                    "status": "succeeded",
                    "phase": "published",
                    "consumer": {
                        "dag_id": mod.SOFA_DAG_ID,
                        "run_id": SOFA_RUN_ID,
                    },
                    "lock_active": False,
                    "active": False,
                },
            }
        ],
    }


def _shared_snapshot() -> dict:
    publication = _publication()
    return {
        "schema_version": mod.SHARED_SNAPSHOT_SCHEMA,
        "expected": publication,
        "runs": [
            {
                "dag_id": mod.SOFA_DAG_ID,
                "run_id": SOFA_RUN_ID,
                "run_type": "scheduled",
                "state": "success",
                "logical_date": _binding()["data_interval_start"],
                "data_interval_start": _binding()["data_interval_start"],
                "data_interval_end": _binding()["data_interval_end"],
                "finalizer_states": ["success"],
                "finalizer_xcoms": [
                    {
                        **publication,
                        "source": "fotmob",
                        "status": "succeeded",
                        "phase": "published",
                        "active": False,
                        "published": True,
                        "released": True,
                    }
                ],
                "active_tasks": [],
            }
        ],
    }


def _later_complete_snapshots() -> tuple[dict, dict]:
    """Build the next day's exact Silver-backed publication lineage."""

    isolated = _isolated_snapshot()
    item = isolated["daily_runs"][0]
    old_generation = item["initializers"][0]["generation_id"]
    binding = {
        **_binding(),
        "data_interval_start": "2026-08-07T14:00:00.000000+00:00",
        "data_interval_end": "2026-08-08T14:00:00.000000+00:00",
    }
    publication = {
        "generation_id": _generation_id(binding),
        "binding": binding,
    }
    generation_id = publication["generation_id"]
    owner_run_id = "scheduled__2026-08-08T14:00:00+00:00"
    sofa_run_id = "scheduled__2026-08-07T14:00:00+00:00"

    item["owner"].update(
        run_id=owner_run_id,
        logical_date="2026-08-08T14:00:00+00:00",
        data_interval_start="2026-08-08T13:55:00+00:00",
        data_interval_end="2026-08-08T14:00:00+00:00",
        start_date="2026-08-08T14:00:01+00:00",
    )
    item["attest_task_start_dates"] = ["2026-08-08T14:00:02+00:00"]
    item["decisions"][0]["selected_date"] = "2026-08-08"
    item["initializers"] = [{**publication, "state": {"phase": "writing"}}]
    item["ingest_runs"][0].update(
        run_id=f"fotmob_orchestrated__{generation_id}",
        conf={
            **item["decisions"][0]["conf"],
            "fotmob_publication": publication,
        },
    )
    item["silver_runs"][0].update(
        run_id=f"fotmob_silver__{generation_id}",
        conf={"fotmob_publication": publication},
    )
    item["publication"].update(
        **publication,
        consumer={"dag_id": mod.SOFA_DAG_ID, "run_id": sofa_run_id},
    )
    assert generation_id != old_generation

    shared = _shared_snapshot()
    shared["expected"] = publication
    shared["runs"][0].update(
        run_id=sofa_run_id,
        logical_date=binding["data_interval_start"],
        data_interval_start=binding["data_interval_start"],
        data_interval_end=binding["data_interval_end"],
        finalizer_xcoms=[
            {
                **publication,
                "source": "fotmob",
                "status": "succeeded",
                "phase": "published",
                "active": False,
                "published": True,
                "released": True,
            }
        ],
    )
    return isolated, shared


def test_validate_observation_emits_exact_purge_schema(tmp_path):
    report = mod.validate_observation(
        _context(tmp_path), _isolated_snapshot(), _shared_snapshot()
    )

    assert report == {
        "schema_version": "fotmob-scheduled-observation-v1",
        "passed": True,
        "deployment": {
            "deployment_id": "a" * 32,
            "git_sha": "b" * 40,
            "scheduler_container_id": "c" * 64,
        },
        "runs": {
            "owner": {
                "dag_id": mod.OWNER_DAG_ID,
                "run_id": OWNER_RUN_ID,
                "run_type": "scheduled",
                "generation_id": _generation_id(),
                "state": "success",
            },
            "ingest": {
                "dag_id": mod.INGEST_DAG_ID,
                "run_id": f"fotmob_orchestrated__{_generation_id()}",
                "owner_run_id": OWNER_RUN_ID,
                "generation_id": _generation_id(),
                "state": "success",
            },
            "silver": {
                "dag_id": mod.SILVER_DAG_ID,
                "run_id": f"fotmob_silver__{_generation_id()}",
                "ingest_run_id": f"fotmob_orchestrated__{_generation_id()}",
                "generation_id": _generation_id(),
                "state": "success",
            },
            "sofascore": {
                "dag_id": mod.SOFA_DAG_ID,
                "run_id": SOFA_RUN_ID,
                "generation_id": _generation_id(),
                "state": "success",
            },
            "finalizer": {
                "dag_id": mod.SOFA_DAG_ID,
                "run_id": SOFA_RUN_ID,
                "task_id": mod.SOFA_FINALIZER_TASK_ID,
                "generation_id": _generation_id(),
                "state": "success",
            },
        },
        "publication": {
            **_publication(),
            "status": "succeeded",
            "phase": "published",
            "active": False,
            "published": True,
            "released": True,
        },
    }


def test_noop_refresh_and_backfill_runs_are_ignored(tmp_path):
    isolated = _isolated_snapshot()
    selected = isolated["daily_runs"][0]
    for index, lane in enumerate((None, "refresh", "backfill")):
        ignored = copy.deepcopy(selected)
        ignored["owner"]["run_id"] = f"ignored-{index}"
        ignored["owner"]["start_date"] = f"2026-08-07T13:4{index}:00+00:00"
        ignored["decisions"] = [False if lane is None else {"lane": lane}]
        isolated["daily_runs"].insert(index, ignored)

    report = mod.validate_observation(_context(tmp_path), isolated, _shared_snapshot())

    assert report["runs"]["owner"]["run_id"] == OWNER_RUN_ID


@pytest.mark.parametrize(
    "mutation, message",
    (
        (
            lambda value: value["daily_runs"][0]["owner"].update(run_type="manual"),
            "manual",
        ),
        (
            lambda value: value["daily_runs"][0]["active_tasks"].append(
                {"task_id": "late", "state": "up_for_retry"}
            ),
            "active task",
        ),
        (
            lambda value: value["daily_runs"][0]["initializers"].append(
                copy.deepcopy(value["daily_runs"][0]["initializers"][0])
            ),
            "ambiguous",
        ),
        (
            lambda value: value["daily_runs"][0]["initializers"][0]["binding"].update(
                data_interval_end="2026-08-07T15:00:00.000000+00:00"
            ),
            "14:00 UTC daily",
        ),
    ),
)
def test_isolated_observation_rejects_manual_active_ambiguous_or_wrong_interval(
    tmp_path, mutation, message
):
    isolated = _isolated_snapshot()
    mutation(isolated)

    with pytest.raises(mod.ObservationError, match=message):
        mod.validate_observation(_context(tmp_path), isolated, _shared_snapshot())


def test_earliest_admitted_daily_owner_is_selected(tmp_path):
    isolated = _isolated_snapshot()
    later = copy.deepcopy(isolated["daily_runs"][0])
    later["owner"]["run_id"] = "scheduled__2026-08-08T14:00:00+00:00"
    later["owner"]["logical_date"] = "2026-08-08T14:00:00+00:00"
    later["owner"]["start_date"] = "2026-08-08T14:00:01+00:00"
    isolated["daily_runs"].insert(0, later)

    report = mod.validate_observation(_context(tmp_path), isolated, _shared_snapshot())

    assert report["runs"]["owner"]["run_id"] == OWNER_RUN_ID


def test_bronze_only_first_daily_does_not_block_later_silver_observation(tmp_path):
    isolated = _isolated_snapshot()
    bronze_only = isolated["daily_runs"][0]
    bronze_only["ingest_trigger_states"] = ["skipped"]
    bronze_only["silver_runs"] = []
    bronze_only["silver_active_tasks"] = []
    later, shared = _later_complete_snapshots()
    isolated["daily_runs"].extend(later["daily_runs"])

    report = mod.validate_observation(_context(tmp_path), isolated, shared)

    assert report["runs"]["owner"]["run_id"] == (
        "scheduled__2026-08-08T14:00:00+00:00"
    )


def test_malformed_missing_silver_first_daily_still_fails_closed(tmp_path):
    isolated = _isolated_snapshot()
    isolated["daily_runs"][0]["silver_runs"] = []
    later, shared = _later_complete_snapshots()
    isolated["daily_runs"].extend(later["daily_runs"])

    with pytest.raises(mod.ObservationError, match="Silver child"):
        mod.validate_observation(_context(tmp_path), isolated, shared)


def test_owner_dagrun_may_start_before_owner_readback_when_attestation_is_after(
    tmp_path,
):
    isolated = _isolated_snapshot()
    isolated["daily_runs"][0]["owner"]["start_date"] = "2026-08-07T13:34:59+00:00"

    report = mod.validate_observation(_context(tmp_path), isolated, _shared_snapshot())

    assert report["runs"]["owner"]["run_id"] == OWNER_RUN_ID


def test_pre_activation_attestation_is_not_an_admitted_daily_owner(tmp_path):
    isolated = _isolated_snapshot()
    isolated["daily_runs"][0]["attest_task_start_dates"] = ["2026-08-07T13:34:59+00:00"]

    with pytest.raises(mod.ObservationError, match="after activation"):
        mod.validate_observation(_context(tmp_path), isolated, _shared_snapshot())


@pytest.mark.parametrize("missing", (False, True))
def test_stale_pre_activation_failed_or_missing_attestation_is_ignored(
    tmp_path, missing
):
    isolated = _isolated_snapshot()
    stale = copy.deepcopy(isolated["daily_runs"][0])
    stale["owner"].update(
        run_id="scheduled__2026-08-07T13:20:00+00:00",
        logical_date="2026-08-07T13:20:00+00:00",
        start_date="2026-08-07T13:25:00+00:00",
    )
    stale["attest_task_states"] = [] if missing else ["failed"]
    stale["attest_task_start_dates"] = [] if missing else ["2026-08-07T13:25:01+00:00"]
    stale["attestations"] = []
    isolated["daily_runs"].insert(0, stale)

    report = mod.validate_observation(_context(tmp_path), isolated, _shared_snapshot())

    assert report["runs"]["owner"]["run_id"] == OWNER_RUN_ID


@pytest.mark.parametrize(
    "mutation, message",
    (
        (
            lambda value: value["runs"][0].update(run_type="manual"),
            "scheduled",
        ),
        (
            lambda value: value["runs"].append(copy.deepcopy(value["runs"][0])),
            "ambiguous",
        ),
        (
            lambda value: value["runs"][0]["active_tasks"].append(
                {"task_id": "finalize_fotmob_publication", "state": "running"}
            ),
            "active task",
        ),
        (
            lambda value: value["runs"][0]["finalizer_xcoms"][0].update(released=False),
            "finalizer XCom",
        ),
    ),
)
def test_shared_observation_rejects_manual_ambiguity_active_task_or_bad_xcom(
    tmp_path, mutation, message
):
    shared = _shared_snapshot()
    mutation(shared)

    with pytest.raises(mod.ObservationError, match=message):
        mod.validate_observation(_context(tmp_path), _isolated_snapshot(), shared)


def test_output_must_be_canonical_inside_evidence_and_not_deployment(tmp_path):
    context = _context(tmp_path)
    evidence = Path(context["evidence_dir"])
    outside = tmp_path / "outside.json"
    symlink = evidence / "linked.json"
    symlink.symlink_to(outside)

    for output, message in (
        (Path(context["deployment_report"]), "deployment"),
        (outside, "inside"),
        (symlink, "symlink"),
        (Path("relative.json"), "absolute"),
    ):
        with pytest.raises(mod.ObservationError, match=message):
            mod.validate_output_path(context, output)


def test_atomic_output_is_mode_0600(tmp_path):
    context = _context(tmp_path)
    output = Path(context["evidence_dir"]) / "first-scheduled-observation.json"
    report = mod.validate_observation(context, _isolated_snapshot(), _shared_snapshot())

    mod.write_protected_observation(context, output, report)

    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert json.loads(output.read_text(encoding="utf-8")) == report


def test_protected_writer_is_idempotent_but_never_overwrites_other_evidence(
    tmp_path,
):
    context = _context(tmp_path)
    output = Path(context["evidence_dir"]) / "first-scheduled-observation.json"
    report = mod.validate_observation(context, _isolated_snapshot(), _shared_snapshot())
    mod.write_protected_observation(context, output, report)
    original_stat = output.stat()

    mod.write_protected_observation(context, output, report)

    assert output.stat().st_ino == original_stat.st_ino
    unrelated = copy.deepcopy(report)
    unrelated["runs"]["owner"]["run_id"] = "unrelated"
    with pytest.raises(mod.ObservationError, match="unrelated protected evidence"):
        mod.write_protected_observation(context, output, unrelated)


def test_atomic_install_never_clobbers_a_concurrent_evidence_creator(
    tmp_path, monkeypatch
):
    context = _context(tmp_path)
    output = Path(context["evidence_dir"]) / "first-scheduled-observation.json"
    report = mod.validate_observation(context, _isolated_snapshot(), _shared_snapshot())
    unrelated = b'{"schema_version":"unrelated"}\n'

    def concurrent_link(_source, target):
        Path(target).write_bytes(unrelated)
        Path(target).chmod(0o600)
        raise FileExistsError(target)

    monkeypatch.setattr(mod.os, "link", concurrent_link)

    with pytest.raises(mod.ObservationError, match="concurrently acquired"):
        mod.write_protected_observation(context, output, report)

    assert output.read_bytes() == unrelated
    assert stat.S_IMODE(output.stat().st_mode) == 0o600


def test_collected_report_is_accepted_by_purge_validator(tmp_path):
    from scripts import purge_fotmob_competitions as purge
    from tests.unit.scripts.test_purge_fotmob_competitions import (
        _scheduled_observation_payload,
    )

    context = _context(tmp_path)
    report = mod.validate_observation(context, _isolated_snapshot(), _shared_snapshot())
    evidence = Path(context["evidence_dir"])
    deployment, _example = _scheduled_observation_payload(evidence_dir=evidence)
    deployment_path = Path(context["deployment_report"])
    deployment_path.write_text(json.dumps(deployment), encoding="utf-8")
    deployment_path.chmod(0o600)
    output = evidence / "first-scheduled-observation.json"
    mod.write_protected_observation(context, output, report)

    accepted = purge._scheduled_observation(output, deployment_report=deployment_path)

    assert accepted.generation_id == _generation_id()
    assert accepted.runs["sofascore"]["run_id"] == SOFA_RUN_ID


def test_collector_rechecks_both_runtimes_and_rejects_snapshot_drift(
    tmp_path, monkeypatch
):
    context = _context(tmp_path)
    args = SimpleNamespace(
        project="fotmob-airflow",
        compose_file=tmp_path / "compose.yaml",
        env_file=tmp_path / "fotmob.env",
        deployment_report=Path(context["deployment_report"]),
        output=Path(context["evidence_dir"]) / "observation.json",
    )
    contexts = iter([(copy.deepcopy(context), "e" * 64)] * 2)
    live_calls = []
    monkeypatch.setattr(mod, "load_active_deployment", lambda _args: next(contexts))
    monkeypatch.setattr(
        mod,
        "validate_live_runtimes",
        lambda *_args, **_kwargs: (
            live_calls.append("live")
            or {"isolated": {"passed": True}, "shared": {"passed": True}}
        ),
    )
    isolated_values = iter([_isolated_snapshot(), _isolated_snapshot()])
    monkeypatch.setattr(
        mod,
        "collect_isolated_snapshot",
        lambda *_args, **_kwargs: next(isolated_values),
    )
    first_shared = _shared_snapshot()
    second_shared = _shared_snapshot()
    second_shared["runs"][0]["finalizer_xcoms"][0]["released"] = False
    shared_values = iter([first_shared, second_shared])
    monkeypatch.setattr(
        mod,
        "collect_shared_snapshot",
        lambda *_args, **_kwargs: next(shared_values),
    )

    with pytest.raises(mod.ObservationError, match="drifted"):
        mod.collect_scheduled_observation(args)

    assert live_calls == ["live", "live"]
    assert not args.output.exists()


def test_generated_collectors_are_read_only_python():
    isolated = mod.isolated_snapshot_code(
        {
            "deployment_id": "a" * 32,
            "git_sha": "b" * 40,
            "scheduler_container_id": "c" * 64,
            "automatic_rollout_summary": {"owner_at": ACTIVATION_AT},
        }
    )
    shared = mod.shared_snapshot_code(_generation_id(), _binding())

    compile(isolated, "<fotmob-isolated-observer>", "exec")
    compile(shared, "<fotmob-shared-observer>", "exec")
    combined = isolated + shared
    for mutation in (".delete(", ".update(", ".commit(", "Variable.set("):
        assert mutation not in combined
    assert "DagRun.logical_date." not in combined
    assert "DagRun.logical_date ==" not in combined
    assert "DagRun.execution_date.asc()" in isolated
    assert "DagRun.execution_date == start" in shared
    assert "DagRun.start_date >=" not in isolated
    assert "DagRun.execution_date >= query_boundary" in isolated


def test_main_writes_only_a_successful_stable_observation(tmp_path, monkeypatch):
    context = _context(tmp_path)
    output = Path(context["evidence_dir"]) / "observation.json"
    report = mod.validate_observation(context, _isolated_snapshot(), _shared_snapshot())
    monkeypatch.setattr(mod, "collect_scheduled_observation", lambda *_a, **_k: report)
    monkeypatch.setattr(
        mod,
        "load_active_deployment",
        lambda _args: (copy.deepcopy(context), "e" * 64),
    )

    result = mod.main(
        [
            "--env-file",
            str(tmp_path / "fotmob.env"),
            "--deployment-report",
            context["deployment_report"],
            "--output",
            str(output),
        ]
    )

    assert result == 0
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert json.loads(output.read_text(encoding="utf-8")) == report
