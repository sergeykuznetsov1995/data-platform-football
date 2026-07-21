from pathlib import Path

from scripts import fotmob_runtime


def next_schedule_boundary() -> dict[str, str]:
    return {
        "logical_date": "2026-07-20T14:00:00.000000+00:00",
        "data_interval_start": "2026-07-20T14:00:00.000000+00:00",
        "data_interval_end": "2026-07-21T14:00:00.000000+00:00",
        "run_after": "2026-07-21T14:00:00.000000+00:00",
    }


def schedule_boundary_proof() -> dict:
    boundary = next_schedule_boundary()
    return {
        "shared_dag_id": fotmob_runtime.SHARED_CONSUMER_DAG_ID,
        "isolated_dag_id": fotmob_runtime.ISOLATED_DAILY_DAG_ID,
        "shared_initial": dict(boundary),
        "shared_final": dict(boundary),
        "isolated_initial": dict(boundary),
        "isolated_final": dict(boundary),
        "exact_match": True,
    }


def materialize_shared_runtime(release_root: Path) -> dict[str, str]:
    runtime_paths = {
        *fotmob_runtime.SHARED_REQUIRED_RUNTIME_PATHS,
        "configs/medallion/competitions.yaml",
        "scripts/runtime_marker.py",
    }
    for relative_path in runtime_paths:
        path = release_root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            if relative_path in {
                fotmob_runtime.APPROVED_SCOPE_PATH,
                fotmob_runtime.PLAYER_SOURCE_REFRESH_PATH,
            }:
                project_root = Path(__file__).resolve().parents[3]
                path.write_bytes((project_root / relative_path).read_bytes())
            else:
                path.write_text(relative_path, encoding="utf-8")
    airflowignore = release_root / "deploy/fotmob/.airflowignore"
    airflowignore.parent.mkdir(parents=True, exist_ok=True)
    if not airflowignore.exists():
        airflowignore.write_text("^utils/\n^sql/\n^scripts/\n", encoding="utf-8")
    return fotmob_runtime.shared_runtime_manifest(release_root)


def isolated_runtime_proof(release_root: Path) -> dict[str, str]:
    shared = materialize_shared_runtime(release_root)
    return fotmob_runtime.expected_isolated_runtime_manifest(release_root, shared)


def _downstream_proof(dag_id: str, first_task: str, *, has_start: bool) -> dict:
    terminal = f"{dag_id}.terminal"
    task_ids = [
        "validate_fotmob_publication_consumer",
        first_task,
        terminal,
    ]
    if has_start:
        task_ids.insert(0, "start_marker")
    return {
        "present": True,
        "fileloc": f"/opt/airflow/dags/{dag_id}.py",
        "task_ids": task_ids,
        "start_present": has_start,
        "start_downstream": (
            ["validate_fotmob_publication_consumer"] if has_start else []
        ),
        "preflight_present": True,
        "preflight_upstream": ["start_marker"] if has_start else [],
        "preflight_downstream": [first_task],
        "preflight_descendants": [first_task, terminal],
        "preflight_trigger_rule": "all_success",
        "direct_downstream_trigger_rules": {first_task: "all_success"},
    }


def shared_handoff_proof(
    control_database: dict,
    *,
    release_root: Path,
    git_sha: str = "a" * 40,
    shared_container: str = "3" * 64,
) -> dict:
    xref_writers = [
        "xref_transforms.xref_team",
        "xref_transforms.xref_referee",
        "xref_transforms.xref_match",
        "xref_transforms.xref_manager",
        "xref_player",
    ]
    xref_tail = ["validate_xref", "end_marker"]
    hashes = materialize_shared_runtime(release_root)
    return {
        "shared_scheduler_container": shared_container,
        "shared_admission_mount": {
            "type": "bind",
            "source": str((release_root / "evidence").resolve()),
            "destination": str(fotmob_runtime.SHARED_CONTAINER_EVIDENCE_ROOT),
            "read_only": True,
            "report_path": str(
                fotmob_runtime.SHARED_CONTAINER_EVIDENCE_ROOT / "deployment.json"
            ),
        },
        "master_dag_sha256": hashes[fotmob_runtime.MASTER_RUNTIME_PATH],
        "remote_master_dag_sha256": hashes[fotmob_runtime.MASTER_RUNTIME_PATH],
        "runtime_code_sha256": hashes,
        "runtime_git_sha": git_sha,
        "serialized_master": {
            "present": True,
            "fileloc": "/opt/airflow/dags/dag_master_pipeline.py",
            "gate_present": True,
            "trigger_upstream": ["ingestion_triggers.fotmob_shared_schedule_owner"],
        },
        "serialized_sofascore": {
            "present": True,
            "fileloc": "/opt/airflow/dags/dag_sofascore_pipeline.py",
            "sensor_present": True,
            "xref_present": True,
            "e4_present": True,
            "finalizer_present": True,
            "sensor_downstream": ["trigger_xref_transforms"],
            "xref_upstream": ["wait_for_fotmob_publication"],
            "e4_downstream": ["finalize_fotmob_publication"],
            "finalizer_upstream": [
                "trigger_e4_transforms",
                "wait_for_fotmob_publication",
            ],
            "finalizer_trigger_rule": "all_done",
        },
        "serialized_xref": {
            "present": True,
            "fileloc": "/opt/airflow/dags/dag_transform_xref.py",
            "task_ids": [
                "start_marker",
                "validate_fotmob_publication_consumer",
                *xref_writers,
                *xref_tail,
            ],
            "start_present": True,
            "preflight_present": True,
            "start_downstream": ["validate_fotmob_publication_consumer"],
            "preflight_upstream": ["start_marker"],
            "preflight_descendants": [*xref_writers, *xref_tail],
            "preflight_trigger_rule": "all_success",
            "task_trigger_rules": {task_id: "all_success" for task_id in xref_writers},
        },
        "serialized_downstream": {
            "dag_transform_e3": _downstream_proof(
                "dag_transform_e3",
                "silver_e3.whoscored_events_spadl",
                has_start=True,
            ),
            "dag_transform_e4": _downstream_proof(
                "dag_transform_e4",
                "silver_e4.matchhistory_match_odds",
                has_start=True,
            ),
            "dag_transform_fbref_gold": _downstream_proof(
                "dag_transform_fbref_gold",
                "transfermarkt_reader_precondition",
                has_start=False,
            ),
        },
        "next_scheduled_interval": next_schedule_boundary(),
        "orchestration_state": {
            "pause_states": dict(fotmob_runtime.EXPECTED_SHARED_PAUSE_STATES),
            "expected_pause_states": dict(fotmob_runtime.EXPECTED_SHARED_PAUSE_STATES),
            "active_runs": [],
            "atomic_metadata_snapshot": True,
            "shared_daily_trigger": {
                "isolated_stack_env": None,
                "serialized_present": False,
                "serialized_fileloc": None,
                "dag_model_present": False,
                "dag_model_paused": None,
            },
        },
        "schedule_owner": "isolated",
        "active_run_checks": {
            dag_id: {"running": [], "queued": []}
            for dag_id in fotmob_runtime.SHARED_STATE_DAGS
        },
        "control_database": control_database,
        "passed": True,
    }
