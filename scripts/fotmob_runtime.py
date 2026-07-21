"""Deployment-bound runtime identity for FotMob operational commands.

The isolated scheduler executes code from host bind mounts.  Container IDs and
an environment Git SHA therefore are not sufficient evidence by themselves:
the checkout and generated DagBag must still contain the admitted bytes, and
the Compose service names must still resolve to the reported containers.

This module contains no acceptance/cleanup imports so all operational entry
points can share the same fail-closed binding without circular dependencies.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping


TRINO_ENV_KEYS = (
    "TRINO_HOST",
    "TRINO_PORT",
    "TRINO_USER",
    "TRINO_PASSWORD",
    "TRINO_HTTP_SCHEME",
    "TRINO_TLS_VERIFY",
)
PROJECTION_SOURCES = {
    "dag_ingest_fotmob.py": "dags/dag_ingest_fotmob.py",
    "dag_transform_fotmob_silver.py": "dags/dag_transform_fotmob_silver.py",
    "dag_trigger_fotmob_daily.py": "dags/dag_trigger_fotmob_daily.py",
    ".airflowignore": "deploy/fotmob/.airflowignore",
}
PROJECTION_DIRECTORIES = {"utils", "sql", "scripts"}
CONTAINER_EVIDENCE_ROOT = Path("/opt/airflow/logs/fotmob")
SHARED_CONTAINER_EVIDENCE_ROOT = Path("/opt/airflow/fotmob-admission")
EXPECTED_DAGS = {
    "dag_ingest_fotmob",
    "dag_transform_fotmob_silver",
    "dag_trigger_fotmob_daily",
}
SHARED_RUNTIME_ROOTS = {
    "dags": "/opt/airflow/dags",
    "scrapers": "/opt/airflow/scrapers",
    "scripts": "/opt/airflow/scripts",
    "configs/medallion": "/opt/airflow/configs/medallion",
    "configs/fotmob": "/opt/airflow/configs/fotmob",
}
SHARED_RUNTIME_SUFFIXES = (
    ".py",
    ".pyi",
    ".sql",
    ".j2",
    ".json",
    ".yaml",
    ".yml",
    ".lock",
    ".sh",
    ".txt",
)
ISOLATED_DAG_ROOT_PATHS = {
    "dags/dag_ingest_fotmob.py",
    "dags/dag_transform_fotmob_silver.py",
    "dags/dag_trigger_fotmob_daily.py",
}
ISOLATED_DAG_PREFIXES = (
    "dags/scripts/",
    "dags/sql/",
    "dags/utils/",
)
ISOLATED_AIRFLOWIGNORE_PATH = "dags/.airflowignore"
SHARED_REQUIRED_RUNTIME_PATHS = {
    "configs/fotmob/competitions.json",
    "configs/fotmob/issue-930-scopes.txt",
    "dags/.airflowignore",
    "dags/dag_ingest_fotmob.py",
    "dags/dag_master_pipeline.py",
    "dags/dag_sofascore_pipeline.py",
    "dags/dag_trigger_fotmob_daily.py",
    "dags/dag_transform_e3.py",
    "dags/dag_transform_e4.py",
    "dags/dag_transform_fbref_gold.py",
    "dags/dag_transform_fotmob_silver.py",
    "dags/dag_transform_xref.py",
    "dags/scripts/run_fotmob_scraper.py",
    "dags/sql/silver/fotmob_keeper_profile.sql",
    "dags/sql/silver/fotmob_manager_profile.sql",
    "dags/sql/silver/fotmob_player_profile.sql",
    "dags/sql/silver/fotmob_player_season_profile.sql",
    "dags/sql/silver/xref_manager.sql.j2",
    "dags/utils/fotmob_publication.py",
    "dags/utils/maintenance_tasks.py",
    "dags/utils/silver_tasks.py",
    "dags/utils/xref_player_resolver.py",
    "scrapers/base/iceberg_writer.py",
    "scrapers/base/trino_manager.py",
    "scrapers/fbref/control/store.py",
    "scrapers/fotmob/constants.py",
    "scrapers/fotmob/raw_store.py",
    "scrapers/fotmob/repository.py",
    "scrapers/fotmob/service.py",
    "scrapers/fotmob/transport.py",
}
MASTER_RUNTIME_PATH = "dags/dag_master_pipeline.py"
APPROVED_SCOPE_PATH = "configs/fotmob/issue-930-scopes.txt"
APPROVED_SCOPE_SHA256 = (
    "f1d95f916c78ed80e5784e2cd5bda7263cece37d9fde6d52fb2a1a4d9e97cb58"
)
SHARED_STATE_DAGS = {
    "dag_master_pipeline",
    "dag_sofascore_pipeline",
    "dag_ingest_fotmob",
    "dag_transform_fotmob_silver",
    "dag_transform_xref",
    "dag_transform_e3",
    "dag_transform_e4",
    "dag_transform_fbref_gold",
    "dag_trigger_fotmob_daily",
}
EXPECTED_SHARED_PAUSE_STATES = {
    "dag_master_pipeline": True,
    "dag_sofascore_pipeline": False,
    "dag_ingest_fotmob": True,
    "dag_transform_fotmob_silver": True,
}


class RuntimeBindingError(RuntimeError):
    pass


def _is_generated_bytecode_path(value: str) -> bool:
    path = Path(value)
    return "__pycache__" in path.parts and path.suffix in {".pyc", ".pyo"}


def shared_runtime_manifest(release_root: Path) -> dict[str, str]:
    """Hash the exact regular-file inventory visible through shared bind mounts."""

    manifest: dict[str, str] = {}
    for relative_root in SHARED_RUNTIME_ROOTS:
        root = release_root / relative_root
        if not root.is_dir():
            raise RuntimeBindingError(f"shared runtime root is absent: {relative_root}")
        for path in sorted(root.rglob("*")):
            if path.is_symlink():
                raise RuntimeBindingError(
                    f"shared runtime manifest rejects symlink: {path}"
                )
            if (
                path.is_file()
                and "__pycache__" not in path.parts
                and (
                    path.name == ".airflowignore"
                    or path.name.endswith(SHARED_RUNTIME_SUFFIXES)
                )
            ):
                relative_path = path.relative_to(release_root).as_posix()
                manifest[relative_path] = hashlib.sha256(path.read_bytes()).hexdigest()
    missing = SHARED_REQUIRED_RUNTIME_PATHS - set(manifest)
    if missing:
        raise RuntimeBindingError(
            f"shared runtime manifest misses required files: {sorted(missing)!r}"
        )
    if manifest[APPROVED_SCOPE_PATH] != APPROVED_SCOPE_SHA256:
        raise RuntimeBindingError(
            "issue-930 scope artifact differs from approved SHA-256"
        )
    return manifest


def expected_isolated_runtime_manifest(
    release_root: Path, shared_manifest: Mapping[str, str]
) -> dict[str, str]:
    """Derive the exact effective isolated inventory from admitted sources."""

    manifest = {
        path: str(digest)
        for path, digest in shared_manifest.items()
        if not path.startswith("dags/")
        or path in ISOLATED_DAG_ROOT_PATHS
        or path.startswith(ISOLATED_DAG_PREFIXES)
    }
    missing = ISOLATED_DAG_ROOT_PATHS - set(manifest)
    if missing:
        raise RuntimeBindingError(
            f"isolated runtime manifest misses root DAGs: {sorted(missing)!r}"
        )
    airflowignore = release_root / PROJECTION_SOURCES[".airflowignore"]
    if not airflowignore.is_file() or airflowignore.is_symlink():
        raise RuntimeBindingError("isolated release misses .airflowignore")
    manifest[ISOLATED_AIRFLOWIGNORE_PATH] = hashlib.sha256(
        airflowignore.read_bytes()
    ).hexdigest()
    return dict(sorted(manifest.items()))


def _validate_fenced_downstream_proof(
    proof: Any,
    *,
    dag_id: str,
    fileloc: str,
    first_tasks: set[str],
    has_start: bool,
) -> None:
    if not isinstance(proof, Mapping):
        raise RuntimeBindingError(f"deployment report misses serialized {dag_id}")
    task_ids = set(proof.get("task_ids") or ())
    descendants = set(proof.get("preflight_descendants") or ())
    preflight_id = "validate_fotmob_publication_consumer"
    excluded = {preflight_id}
    expected_upstream: set[str] = set()
    if has_start:
        excluded.add("start_marker")
        expected_upstream.add("start_marker")
    direct_rules = proof.get("direct_downstream_trigger_rules")
    if (
        proof.get("present") is not True
        or proof.get("fileloc") != fileloc
        or proof.get("preflight_present") is not True
        or proof.get("preflight_trigger_rule") != "all_success"
        or set(proof.get("preflight_upstream") or ()) != expected_upstream
        or set(proof.get("preflight_downstream") or ()) != first_tasks
        or task_ids - excluded != descendants
        or not isinstance(direct_rules, Mapping)
        or any(direct_rules.get(task_id) != "all_success" for task_id in first_tasks)
        or (
            has_start
            and (
                proof.get("start_present") is not True
                or set(proof.get("start_downstream") or ()) != {preflight_id}
            )
        )
        or (not has_start and proof.get("start_present") is True)
    ):
        raise RuntimeBindingError(
            f"deployment report has unsafe serialized {dag_id} topology"
        )


def _validate_shared_handoff_report(
    handoff: Any,
    *,
    git_sha: str,
    control_database: Mapping[str, Any],
    expected_runtime_manifest: Mapping[str, str],
    expected_admission_mount: Mapping[str, Any],
) -> None:
    if (
        not isinstance(handoff, Mapping)
        or handoff.get("passed") is not True
        or re.fullmatch(
            r"[0-9a-f]{64}", str(handoff.get("shared_scheduler_container", ""))
        )
        is None
        or handoff.get("runtime_git_sha") != git_sha
        or handoff.get("schedule_owner") != "isolated"
        or handoff.get("control_database") != control_database
    ):
        raise RuntimeBindingError("deployment report has no valid shared runtime proof")

    admission_mount = handoff.get("shared_admission_mount")
    if not isinstance(admission_mount, Mapping) or dict(admission_mount) != dict(
        expected_admission_mount
    ):
        raise RuntimeBindingError(
            "deployment report has no exact shared admission mount"
        )

    hashes = handoff.get("runtime_code_sha256")
    if (
        not isinstance(hashes, Mapping)
        or hashes != expected_runtime_manifest
        or any(
            re.fullmatch(r"[0-9a-f]{64}", str(value)) is None
            for value in hashes.values()
        )
        or handoff.get("master_dag_sha256") != hashes.get(MASTER_RUNTIME_PATH)
        or handoff.get("remote_master_dag_sha256") != hashes.get(MASTER_RUNTIME_PATH)
    ):
        raise RuntimeBindingError("deployment report has no exact shared code hashes")

    master = handoff.get("serialized_master")
    master_gate = "ingestion_triggers.fotmob_shared_schedule_owner"
    if (
        not isinstance(master, Mapping)
        or master.get("present") is not True
        or master.get("fileloc") != "/opt/airflow/dags/dag_master_pipeline.py"
        or master.get("gate_present") is not True
        or master_gate not in set(master.get("trigger_upstream") or ())
    ):
        raise RuntimeBindingError("deployment report has unsafe serialized master DAG")

    sofa = handoff.get("serialized_sofascore")
    if (
        not isinstance(sofa, Mapping)
        or sofa.get("present") is not True
        or sofa.get("fileloc") != "/opt/airflow/dags/dag_sofascore_pipeline.py"
        or any(
            sofa.get(key) is not True
            for key in (
                "sensor_present",
                "xref_present",
                "e4_present",
                "finalizer_present",
            )
        )
        or "wait_for_fotmob_publication" not in set(sofa.get("xref_upstream") or ())
        or "trigger_xref_transforms" not in set(sofa.get("sensor_downstream") or ())
        or set(sofa.get("finalizer_upstream") or ())
        != {"wait_for_fotmob_publication", "trigger_e4_transforms"}
        or "finalize_fotmob_publication" not in set(sofa.get("e4_downstream") or ())
        or sofa.get("finalizer_trigger_rule") != "all_done"
    ):
        raise RuntimeBindingError("deployment report has unsafe serialized Sofa DAG")

    xref = handoff.get("serialized_xref")
    xref_writers = {
        "xref_transforms.xref_team",
        "xref_transforms.xref_referee",
        "xref_transforms.xref_match",
        "xref_transforms.xref_manager",
        "xref_player",
    }
    if not isinstance(xref, Mapping):
        raise RuntimeBindingError("deployment report misses serialized xref DAG")
    xref_task_ids = set(xref.get("task_ids") or ())
    xref_descendants = set(xref.get("preflight_descendants") or ())
    xref_rules = xref.get("task_trigger_rules")
    if (
        xref.get("present") is not True
        or xref.get("fileloc") != "/opt/airflow/dags/dag_transform_xref.py"
        or xref.get("start_present") is not True
        or xref.get("preflight_present") is not True
        or set(xref.get("start_downstream") or ())
        != {"validate_fotmob_publication_consumer"}
        or set(xref.get("preflight_upstream") or ()) != {"start_marker"}
        or xref.get("preflight_trigger_rule") != "all_success"
        or not xref_writers.issubset(xref_task_ids)
        or xref_task_ids - {"start_marker", "validate_fotmob_publication_consumer"}
        != xref_descendants
        or not isinstance(xref_rules, Mapping)
        or any(xref_rules.get(task_id) != "all_success" for task_id in xref_writers)
    ):
        raise RuntimeBindingError("deployment report has unsafe serialized xref DAG")

    downstream = handoff.get("serialized_downstream")
    if not isinstance(downstream, Mapping) or set(downstream) != {
        "dag_transform_e3",
        "dag_transform_e4",
        "dag_transform_fbref_gold",
    }:
        raise RuntimeBindingError("deployment report misses downstream fence proofs")
    _validate_fenced_downstream_proof(
        downstream["dag_transform_e3"],
        dag_id="dag_transform_e3",
        fileloc="/opt/airflow/dags/dag_transform_e3.py",
        first_tasks={"silver_e3.whoscored_events_spadl"},
        has_start=True,
    )
    _validate_fenced_downstream_proof(
        downstream["dag_transform_e4"],
        dag_id="dag_transform_e4",
        fileloc="/opt/airflow/dags/dag_transform_e4.py",
        first_tasks={"silver_e4.matchhistory_match_odds"},
        has_start=True,
    )
    _validate_fenced_downstream_proof(
        downstream["dag_transform_fbref_gold"],
        dag_id="dag_transform_fbref_gold",
        fileloc="/opt/airflow/dags/dag_transform_fbref_gold.py",
        first_tasks={"transfermarkt_reader_precondition"},
        has_start=False,
    )

    orchestration = handoff.get("orchestration_state")
    if (
        not isinstance(orchestration, Mapping)
        or orchestration.get("pause_states") != EXPECTED_SHARED_PAUSE_STATES
        or orchestration.get("expected_pause_states") != EXPECTED_SHARED_PAUSE_STATES
        or orchestration.get("active_runs") != []
        or orchestration.get("atomic_metadata_snapshot") is not True
    ):
        raise RuntimeBindingError(
            "deployment report has no atomic shared quiescence proof"
        )
    shared_daily = orchestration.get("shared_daily_trigger")
    if (
        not isinstance(shared_daily, Mapping)
        or shared_daily.get("isolated_stack_env") not in {None, ""}
        or (
            shared_daily.get("serialized_present") is True
            and shared_daily.get("serialized_fileloc")
            != "/opt/airflow/dags/dag_trigger_fotmob_daily.py"
        )
        or (
            shared_daily.get("serialized_present") is True
            and shared_daily.get("dag_model_present") is not True
        )
        or (
            shared_daily.get("dag_model_present") is True
            and shared_daily.get("dag_model_paused") is not True
        )
    ):
        raise RuntimeBindingError(
            "deployment report has unsafe shared isolated daily trigger"
        )
    run_checks = handoff.get("active_run_checks")
    if (
        not isinstance(run_checks, Mapping)
        or set(run_checks) != SHARED_STATE_DAGS
        or any(check != {"running": [], "queued": []} for check in run_checks.values())
    ):
        raise RuntimeBindingError(
            "deployment report has incomplete shared active-run proof"
        )


def _timestamp(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise RuntimeBindingError(f"invalid deployment timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise RuntimeBindingError("deployment timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def load_deployment_context(
    deployment_report: Path,
    *,
    project: str,
    compose_file: Path,
) -> dict[str, Any]:
    try:
        payload = json.loads(deployment_report.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeBindingError(f"invalid deployment report: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("passed") is not True:
        raise RuntimeBindingError("deployment report is not green")
    if payload.get("schema_version") != "fotmob-deploy-v2":
        raise RuntimeBindingError("unsupported deployment report schema")
    activation_state = payload.get("activation_state")
    if activation_state == "committed_pending_trigger":
        raise RuntimeBindingError(
            "deployment trigger activation is incomplete; rerun deploy"
        )
    if activation_state not in {"active", "kept_paused"}:
        raise RuntimeBindingError("deployment report has no completed activation state")
    paused = payload.get("paused")
    unpaused = payload.get("unpaused")
    if activation_state == "active":
        if (
            payload.get("kept_paused") is not False
            or paused != []
            or not isinstance(unpaused, list)
            or set(unpaused) != EXPECTED_DAGS
        ):
            raise RuntimeBindingError("active deployment pause state is inconsistent")
    elif (
        payload.get("kept_paused") is not True
        or not isinstance(paused, list)
        or set(paused) != EXPECTED_DAGS
        or unpaused != []
    ):
        raise RuntimeBindingError("kept-paused deployment state is inconsistent")
    required = (
        "project",
        "compose_file",
        "release_root",
        "evidence_dir",
        "container_report_path",
        "shared_container_report_path",
        "dagbag_root",
        "git_sha",
        "image",
        "postgres_image",
        "resolved_image_id",
        "resolved_postgres_image_id",
        "deployment_id",
        "scheduler_container_id",
        "metadb_container_id",
        "data_plane_marker",
        "delivery_credentials",
        "isolated_runtime_sha256",
        "control_database",
        "shared_handoff_initial",
        "shared_handoff_final",
        "generated_at",
    )
    missing = [key for key in required if not str(payload.get(key, "")).strip()]
    if missing:
        raise RuntimeBindingError(
            f"deployment report misses runtime context: {missing!r}"
        )
    if payload["project"] != project:
        raise RuntimeBindingError("deployment project does not match --project")
    if Path(str(payload["compose_file"])).resolve() != compose_file.resolve():
        raise RuntimeBindingError(
            "deployment compose file does not match --compose-file"
        )
    for key in ("release_root", "evidence_dir", "dagbag_root"):
        if not Path(str(payload[key])).is_absolute():
            raise RuntimeBindingError(f"deployment {key} is not absolute")
    container_report_path = Path(str(payload["container_report_path"]))
    try:
        container_report_relative = container_report_path.relative_to(
            CONTAINER_EVIDENCE_ROOT
        )
    except ValueError as exc:
        raise RuntimeBindingError(
            "deployment report is not mounted below the container evidence root"
        ) from exc
    if not container_report_relative.parts or ".." in container_report_relative.parts:
        raise RuntimeBindingError("deployment container report path is invalid")
    shared_container_report_path = Path(str(payload["shared_container_report_path"]))
    try:
        shared_container_report_relative = shared_container_report_path.relative_to(
            SHARED_CONTAINER_EVIDENCE_ROOT
        )
    except ValueError as exc:
        raise RuntimeBindingError(
            "deployment shared report is not mounted below its evidence root"
        ) from exc
    if shared_container_report_relative != container_report_relative:
        raise RuntimeBindingError(
            "deployment isolated/shared report paths identify different files"
        )
    if not re.fullmatch(r"[0-9a-f]{40}", str(payload["git_sha"])):
        raise RuntimeBindingError("deployment report has an invalid Git SHA")
    for key in ("image", "postgres_image"):
        if not re.fullmatch(r"[^\s@]+@sha256:[0-9a-fA-F]{64}", str(payload[key])):
            raise RuntimeBindingError(f"deployment {key} is not digest-pinned")
    for key in ("resolved_image_id", "resolved_postgres_image_id"):
        if not re.fullmatch(r"sha256:[0-9a-fA-F]{64}", str(payload[key])):
            raise RuntimeBindingError(f"deployment {key} is not an immutable image ID")
    if not re.fullmatch(r"[0-9a-f]{32}", str(payload["deployment_id"])):
        raise RuntimeBindingError(
            "deployment report has an invalid deployment identity"
        )
    for key in ("scheduler_container_id", "metadb_container_id"):
        if not re.fullmatch(r"[0-9a-f]{64}", str(payload[key])):
            raise RuntimeBindingError(f"deployment {key} is not a full container ID")
    marker = payload.get("data_plane_marker")
    if not isinstance(marker, Mapping) or marker.get("table") != (
        "iceberg.bronze.fotmob_runtime_deployments"
    ):
        raise RuntimeBindingError("deployment report has an invalid data-plane marker")
    marker_expected = {
        "deployment_id": payload["deployment_id"],
        "git_sha": payload["git_sha"],
        "scheduler_container_id": payload["scheduler_container_id"],
        "scheduler_image_id": payload["resolved_image_id"],
    }
    if any(marker.get(key) != value for key, value in marker_expected.items()):
        raise RuntimeBindingError(
            "deployment data-plane marker identity is inconsistent"
        )
    if payload.get("delivery_credentials") != {
        "telegram_bot_token_configured": True,
        "telegram_chat_id_configured": True,
    }:
        raise RuntimeBindingError("deployment report has no delivery credential proof")
    control = payload.get("control_database")
    if (
        not isinstance(control, Mapping)
        or control.get("same_runtime_configuration") is not True
    ):
        raise RuntimeBindingError("deployment report has no shared control DB proof")
    for side in ("shared", "isolated"):
        proof = control.get(side)
        migrations = proof.get("migrations") if isinstance(proof, Mapping) else None
        if (
            not isinstance(migrations, Mapping)
            or migrations.get("status") != "passed"
            or migrations.get("checksum_verified") is not True
        ):
            raise RuntimeBindingError(
                f"deployment report has no valid {side} control migration proof"
            )
    initial_handoff = payload.get("shared_handoff_initial")
    final_handoff = payload.get("shared_handoff_final")
    expected_runtime_manifest = shared_runtime_manifest(
        Path(str(payload["release_root"]))
    )
    expected_admission_mount = {
        "type": "bind",
        "source": str(Path(str(payload["evidence_dir"])).resolve()),
        "destination": str(SHARED_CONTAINER_EVIDENCE_ROOT),
        "read_only": True,
        "report_path": str(shared_container_report_path),
    }
    _validate_shared_handoff_report(
        initial_handoff,
        git_sha=str(payload["git_sha"]),
        control_database=control["shared"],
        expected_runtime_manifest=expected_runtime_manifest,
        expected_admission_mount=expected_admission_mount,
    )
    _validate_shared_handoff_report(
        final_handoff,
        git_sha=str(payload["git_sha"]),
        control_database=control["shared"],
        expected_runtime_manifest=expected_runtime_manifest,
        expected_admission_mount=expected_admission_mount,
    )
    if (
        initial_handoff["shared_scheduler_container"]
        != final_handoff["shared_scheduler_container"]
        or initial_handoff["runtime_code_sha256"]
        != final_handoff["runtime_code_sha256"]
        or initial_handoff["shared_admission_mount"]
        != final_handoff["shared_admission_mount"]
    ):
        raise RuntimeBindingError("shared handoff identity changed during deployment")
    expected_isolated_manifest = expected_isolated_runtime_manifest(
        Path(str(payload["release_root"])),
        final_handoff["runtime_code_sha256"],
    )
    if payload.get("isolated_runtime_sha256") != expected_isolated_manifest:
        raise RuntimeBindingError(
            "deployment report isolated runtime manifest differs from admitted release"
        )
    _timestamp(payload["generated_at"])
    return payload


def compose_environment(context: Mapping[str, Any]) -> dict[str, str]:
    environment = dict(os.environ)
    environment.update(
        {
            "FOTMOB_RELEASE_ROOT": str(context["release_root"]),
            "FOTMOB_EVIDENCE_DIR": str(context["evidence_dir"]),
            "FOTMOB_DAGBAG_ROOT": str(context["dagbag_root"]),
            "FOTMOB_DEPLOY_GIT_SHA": str(context["git_sha"]),
            "FOTMOB_AIRFLOW_IMAGE": str(context["image"]),
            "FOTMOB_POSTGRES_IMAGE": str(context["postgres_image"]),
            "FOTMOB_DEPLOYMENT_ID": str(context["deployment_id"]),
            "FOTMOB_DEPLOYMENT_REPORT_PATH": str(context["container_report_path"]),
        }
    )
    return environment


def compose_base(
    *, project: str, compose_file: Path, env_file: Path
) -> tuple[str, ...]:
    if not compose_file.is_file():
        raise RuntimeBindingError("--compose-file does not exist")
    if not env_file.is_file():
        raise RuntimeBindingError("--env-file does not exist")
    return (
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(compose_file.resolve()),
        "--env-file",
        str(env_file.resolve()),
    )


def _inspect_container(
    container_id: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> Mapping[str, Any]:
    output = run(
        ("docker", "inspect", "--format", "{{json .}}", container_id),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as exc:
        raise RuntimeBindingError(
            "docker inspect did not return one JSON object"
        ) from exc
    if not isinstance(payload, Mapping):
        raise RuntimeBindingError("docker inspect payload is not an object")
    return payload


def _parsed_environment(container: Mapping[str, Any]) -> dict[str, str]:
    values = (container.get("Config") or {}).get("Env") or ()
    return {
        str(item).split("=", 1)[0]: str(item).split("=", 1)[1]
        for item in values
        if "=" in str(item)
    }


def _attest_release(
    context: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    release = Path(str(context["release_root"]))
    projection = Path(str(context["dagbag_root"]))
    if not release.is_dir():
        raise RuntimeBindingError("admitted release root is unavailable")
    observed_sha = run(
        ("git", "-C", str(release), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if observed_sha != context["git_sha"]:
        raise RuntimeBindingError("live release HEAD differs from deployment report")
    dirty = run(
        ("git", "-C", str(release), "status", "--porcelain"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if dirty:
        raise RuntimeBindingError("live release checkout is dirty")
    ignored_runtime_output = run(
        (
            "git",
            "-C",
            str(release),
            "ls-files",
            "--others",
            "--ignored",
            "--exclude-standard",
            "--",
            "dags",
            "scrapers",
            "scripts",
            "configs",
        ),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    unsafe_ignored_runtime = [
        line.strip()
        for line in ignored_runtime_output.splitlines()
        if line.strip() and not _is_generated_bytecode_path(line.strip())
    ]
    if unsafe_ignored_runtime:
        raise RuntimeBindingError("live runtime trees contain ignored/untracked files")
    if not projection.is_dir():
        raise RuntimeBindingError("admitted DagBag projection is unavailable")
    observed_files = {item.name for item in projection.iterdir() if item.is_file()}
    observed_dirs = {item.name for item in projection.iterdir() if item.is_dir()}
    if (
        observed_files != set(PROJECTION_SOURCES)
        or observed_dirs != PROJECTION_DIRECTORIES
    ):
        raise RuntimeBindingError("live DagBag projection has unexpected entries")
    hashes: dict[str, str] = {}
    for name, relative in PROJECTION_SOURCES.items():
        source = release / relative
        projected = projection / name
        if not source.is_file():
            raise RuntimeBindingError(
                f"live DagBag projection source is absent: {name}"
            )
        source_bytes = source.read_bytes()
        projected_bytes = projected.read_bytes()
        if projected_bytes != source_bytes:
            raise RuntimeBindingError(f"live DagBag projection drifted: {name}")
        hashes[name] = hashlib.sha256(projected_bytes).hexdigest()
    return {
        "git_sha": observed_sha,
        "checkout_clean": True,
        "dagbag_sha256": hashes,
    }


def _current_service_ids(
    context: Mapping[str, Any],
    *,
    project: str,
    compose_file: Path,
    env_file: Path,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    base = compose_base(project=project, compose_file=compose_file, env_file=env_file)
    environment = compose_environment(context)
    for service, key in (
        ("airflow-scheduler", "scheduler_container_id"),
        ("airflow-metadb", "metadb_container_id"),
    ):
        output = run(
            (*base, "ps", "--all", "--no-trunc", "-q", service),
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        ).stdout
        observed = [line.strip() for line in output.splitlines() if line.strip()]
        if observed != [str(context[key])]:
            raise RuntimeBindingError(
                f"current Compose {service} container differs from deployment report"
            )


def validate_live_deployment(
    context: Mapping[str, Any],
    *,
    project: str,
    compose_file: Path,
    env_file: Path,
    require_running: bool,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    _current_service_ids(
        context,
        project=project,
        compose_file=compose_file,
        env_file=env_file,
        run=run,
    )
    release_identity = _attest_release(context, run=run)
    scheduler = _inspect_container(str(context["scheduler_container_id"]), run=run)
    metadb = _inspect_container(str(context["metadb_container_id"]), run=run)
    if scheduler.get("Id") != context["scheduler_container_id"]:
        raise RuntimeBindingError(
            "live scheduler container differs from deployment report"
        )
    if metadb.get("Id") != context["metadb_container_id"]:
        raise RuntimeBindingError(
            "live metadata DB container differs from deployment report"
        )
    if scheduler.get("Image") != context["resolved_image_id"]:
        raise RuntimeBindingError("live scheduler image differs from deployment report")
    if metadb.get("Image") != context["resolved_postgres_image_id"]:
        raise RuntimeBindingError(
            "live metadata DB image differs from deployment report"
        )
    running = bool((scheduler.get("State") or {}).get("Running"))
    if require_running and not running:
        raise RuntimeBindingError("admitted scheduler container is not running")
    if not bool((metadb.get("State") or {}).get("Running")):
        raise RuntimeBindingError("admitted metadata DB container is not running")
    parsed_env = _parsed_environment(scheduler)
    if parsed_env.get("FOTMOB_DEPLOYMENT_ID") != context["deployment_id"]:
        raise RuntimeBindingError(
            "live scheduler deployment identity differs from report"
        )
    if (
        parsed_env.get("FOTMOB_DEPLOYMENT_REPORT_PATH")
        != context["container_report_path"]
    ):
        raise RuntimeBindingError(
            "live scheduler deployment report path differs from report"
        )
    if parsed_env.get("FOTMOB_DEPLOY_GIT_SHA") != context["git_sha"]:
        raise RuntimeBindingError(
            "live scheduler Git SHA differs from deployment report"
        )
    if parsed_env.get("FOTMOB_ISOLATED_STACK") != "1":
        raise RuntimeBindingError("live scheduler is not the explicit isolated stack")
    if any(
        not parsed_env.get(key, "").strip()
        for key in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID")
    ):
        raise RuntimeBindingError("live scheduler misses delivery credentials")
    control_uri = parsed_env.get("FBREF_CONTROL_DB_URI", "")
    if not control_uri or "airflow-metadb" in control_uri.lower():
        raise RuntimeBindingError(
            "live scheduler does not use the shared production control DB"
        )
    missing_trino = [key for key in TRINO_ENV_KEYS if key not in parsed_env]
    if (
        missing_trino
        or not parsed_env.get("TRINO_HOST")
        or not parsed_env.get("TRINO_PASSWORD")
    ):
        raise RuntimeBindingError(
            f"live scheduler misses admitted Trino configuration: {missing_trino!r}"
        )

    release = Path(str(context["release_root"]))
    expected_mounts = {
        "/opt/airflow/dags": (Path(str(context["dagbag_root"])), False),
        "/opt/airflow/dags/utils": (release / "dags/utils", False),
        "/opt/airflow/dags/sql": (release / "dags/sql", False),
        "/opt/airflow/dags/scripts": (release / "dags/scripts", False),
        "/opt/airflow/scrapers": (release / "scrapers", False),
        "/opt/airflow/scripts": (release / "scripts", False),
        "/opt/airflow/configs/medallion": (release / "configs/medallion", False),
        "/opt/airflow/configs/fotmob": (release / "configs/fotmob", False),
        "/opt/airflow/logs/fotmob": (Path(str(context["evidence_dir"])), True),
    }
    mounts = {
        str(item.get("Destination")): item
        for item in scheduler.get("Mounts") or ()
        if isinstance(item, Mapping)
    }
    if set(mounts) != set(expected_mounts):
        raise RuntimeBindingError(
            "live scheduler mount destinations differ from report"
        )
    for destination, (source, writable) in expected_mounts.items():
        mount = mounts[destination]
        if mount.get("Type") != "bind":
            raise RuntimeBindingError(f"live mount type differs for {destination}")
        if Path(str(mount.get("Source"))).resolve() != source.resolve():
            raise RuntimeBindingError(f"live mount source differs for {destination}")
        if bool(mount.get("RW")) is not writable:
            raise RuntimeBindingError(
                f"live mount access mode differs for {destination}"
            )
    metadb_mounts = [
        item
        for item in metadb.get("Mounts") or ()
        if isinstance(item, Mapping)
        and item.get("Destination") == "/var/lib/postgresql/data"
    ]
    if len(metadb_mounts) != 1 or metadb_mounts[0].get("Type") != "volume":
        raise RuntimeBindingError("live metadata DB does not use its admitted volume")
    _current_service_ids(
        context,
        project=project,
        compose_file=compose_file,
        env_file=env_file,
        run=run,
    )
    return {
        "scheduler_container_id": context["scheduler_container_id"],
        "metadb_container_id": context["metadb_container_id"],
        "deployment_id": context["deployment_id"],
        "scheduler_running": running,
        "scheduler_image_id": context["resolved_image_id"],
        "metadb_image_id": context["resolved_postgres_image_id"],
        "mounts_verified": True,
        "release": release_identity,
        "trino": {
            "host": parsed_env["TRINO_HOST"],
            "port": parsed_env["TRINO_PORT"],
            "user": parsed_env["TRINO_USER"],
            "http_scheme": parsed_env["TRINO_HTTP_SCHEME"],
            "tls_verify": parsed_env["TRINO_TLS_VERIFY"],
            "credential_bound": True,
        },
    }


def bind_admitted_trino(
    context: Mapping[str, Any],
    *,
    project: str,
    compose_file: Path,
    env_file: Path,
    require_running: bool,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    """Validate the live runtime before a marker-bound local Trino query."""

    evidence = validate_live_deployment(
        context,
        project=project,
        compose_file=compose_file,
        env_file=env_file,
        require_running=require_running,
        run=run,
    )
    return evidence


def assert_no_active_fotmob_publication(
    context: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    """Prove the shared control plane has no active FotMob generation.

    This is deliberately read-only.  Cleanup and rollback must not release a
    writer/consumer lease on an operator's behalf; they fail and wait for the
    exact generation to become terminal instead.  Both schedulers' runtime
    environment and the shared publication code bytes are re-attested so a
    query against a different control database cannot produce false safety.
    """

    handoff = context.get("shared_handoff_final")
    if not isinstance(handoff, Mapping):
        raise RuntimeBindingError("deployment report has no shared handoff proof")
    shared_container = str(handoff.get("shared_scheduler_container", "")).strip()
    if not shared_container:
        raise RuntimeBindingError("deployment report has no shared scheduler identity")

    isolated = _inspect_container(str(context["scheduler_container_id"]), run=run)
    shared = _inspect_container(shared_container, run=run)
    if isolated.get("Id") != context["scheduler_container_id"]:
        raise RuntimeBindingError("isolated scheduler identity drifted")
    if shared.get("Id") != shared_container:
        raise RuntimeBindingError("shared scheduler identity drifted")
    if not bool((shared.get("State") or {}).get("Running")):
        raise RuntimeBindingError("shared scheduler is not running")
    isolated_env = _parsed_environment(isolated)
    shared_env = _parsed_environment(shared)
    control_uri = isolated_env.get("FBREF_CONTROL_DB_URI", "")
    if (
        not control_uri
        or "airflow-metadb" in control_uri.lower()
        or shared_env.get("FBREF_CONTROL_DB_URI") != control_uri
    ):
        raise RuntimeBindingError(
            "shared and isolated schedulers do not use the same production control DB"
        )
    if shared_env.get("FOTMOB_DEPLOY_GIT_SHA") != context["git_sha"]:
        raise RuntimeBindingError(
            "shared scheduler Git SHA differs from deployment report"
        )
    if shared_env.get("FOTMOB_ISOLATED_STACK", ""):
        raise RuntimeBindingError(
            "shared scheduler opted into the isolated daily stack"
        )

    release = Path(str(context["release_root"]))
    code_hashes = shared_runtime_manifest(release)
    if handoff.get("runtime_code_sha256") != code_hashes:
        raise RuntimeBindingError("shared runtime differs from deployment manifest")
    manifest_code = (
        "import hashlib,json\n"
        "from pathlib import Path\n"
        f"roots={SHARED_RUNTIME_ROOTS!r}\n"
        f"suffixes={SHARED_RUNTIME_SUFFIXES!r}\n"
        "manifest={}\n"
        "for prefix, root_name in roots.items():\n"
        "    root=Path(root_name)\n"
        "    if not root.is_dir():\n"
        "        raise RuntimeError('shared runtime root is absent: '+prefix)\n"
        "    for path in sorted(root.rglob('*')):\n"
        "        if path.is_symlink():\n"
        "            raise RuntimeError('shared runtime symlink: '+str(path))\n"
        "        if (path.is_file() and '__pycache__' not in path.parts "
        "and (path.name == '.airflowignore' or "
        "path.name.endswith(suffixes))):\n"
        "            key=prefix+'/'+path.relative_to(root).as_posix()\n"
        "            manifest[key]=hashlib.sha256(path.read_bytes()).hexdigest()\n"
        "print('FOTMOB_SHARED_RUNTIME_MANIFEST_JSON='+"
        "json.dumps(manifest,sort_keys=True))\n"
    )
    manifest_output = run(
        ("docker", "exec", shared_container, "python", "-c", manifest_code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    remote_manifest: Any = None
    for line in reversed(manifest_output.splitlines()):
        if line.startswith("FOTMOB_SHARED_RUNTIME_MANIFEST_JSON="):
            try:
                remote_manifest = json.loads(line.split("=", 1)[1])
            except json.JSONDecodeError as exc:
                raise RuntimeBindingError(
                    "shared runtime manifest returned invalid evidence"
                ) from exc
            break
    if remote_manifest != code_hashes:
        raise RuntimeBindingError("shared scheduler bind-mounted runtime drifted")

    marker = "FOTMOB_PUBLICATION_QUIESCENCE_JSON="
    code = (
        "import json\n"
        "from airflow.models import DagModel,DagRun\n"
        "from airflow.settings import Session\n"
        "from scrapers.fbref.control import ControlStore\n"
        f"dag_ids={sorted(SHARED_STATE_DAGS)!r}\n"
        "checks={dag_id:{'running':[],'queued':[]} for dag_id in dag_ids}\n"
        "session=Session()\n"
        "rows=session.query(DagRun.dag_id,DagRun.run_id,DagRun.state).filter("
        "DagRun.dag_id.in_(dag_ids),"
        "DagRun.state.in_(('running','queued'))).all()\n"
        "daily=session.query(DagModel.dag_id,DagModel.is_paused).filter("
        "DagModel.dag_id=='dag_trigger_fotmob_daily').one_or_none()\n"
        "session.close()\n"
        "for dag_id,run_id,state in rows:\n"
        "    state=str(getattr(state,'value',state)).lower()\n"
        "    checks[str(dag_id)][state].append(str(run_id))\n"
        "result=dict(ControlStore.from_env()."
        "assert_no_active_publication_generation(source='fotmob'))\n"
        "result['active_run_checks']=checks\n"
        "result['shared_daily_trigger']={"
        "'dag_model_present':daily is not None,"
        "'dag_model_paused':bool(daily[1]) if daily is not None else None}\n"
        f"print('{marker}'+json.dumps(result,default=str,sort_keys=True))\n"
    )
    try:
        output = run(
            ("docker", "exec", shared_container, "python", "-c", code),
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except subprocess.CalledProcessError as exc:
        raise RuntimeBindingError(
            "FotMob publication generation is active or its control check failed"
        ) from exc
    payload: Any = None
    for line in reversed(output.splitlines()):
        if line.startswith(marker):
            try:
                payload = json.loads(line.removeprefix(marker))
            except json.JSONDecodeError as exc:
                raise RuntimeBindingError(
                    "shared publication check returned invalid evidence"
                ) from exc
            break
    if (
        not isinstance(payload, Mapping)
        or payload.get("source") != "fotmob"
        or payload.get("safe") is not True
        or payload.get("active") is not False
        or not isinstance(payload.get("active_run_checks"), Mapping)
        or set(payload["active_run_checks"]) != SHARED_STATE_DAGS
        or any(
            check != {"running": [], "queued": []}
            for check in payload["active_run_checks"].values()
        )
        or not isinstance(payload.get("shared_daily_trigger"), Mapping)
        or (
            payload["shared_daily_trigger"].get("dag_model_present") is True
            and payload["shared_daily_trigger"].get("dag_model_paused") is not True
        )
    ):
        raise RuntimeBindingError("shared publication check did not prove quiescence")
    return {
        "source": "fotmob",
        "safe": True,
        "active": False,
        "phase": payload.get("phase"),
        "shared_scheduler_container_id": shared.get("Id"),
        "runtime_git_sha": context["git_sha"],
        "runtime_code_sha256": code_hashes,
        "active_run_checks": dict(payload["active_run_checks"]),
        "shared_daily_trigger": dict(payload["shared_daily_trigger"]),
        "control_database_bound": True,
    }


def load_host_trino_environment(path: Path) -> None:
    """Load an explicit host-reachable Trino endpoint, overriding ambient env.

    The endpoint may differ from Docker DNS (for example ``127.0.0.1`` versus
    ``trino``). Same-data-plane identity is established separately by the
    unguessable deployment marker, not by comparing hostnames.
    """

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise RuntimeBindingError(f"cannot read host Trino env file: {exc}") from exc
    parsed: dict[str, str] = {}
    for line_number, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in TRINO_ENV_KEYS:
            continue
        value = value.strip()
        if value[:1] in {"'", '"'}:
            if len(value) < 2 or value[-1] != value[0]:
                raise RuntimeBindingError(
                    f"{path}:{line_number}: unterminated quoted {key} value"
                )
            value = value[1:-1]
        parsed[key] = value
    if not parsed.get("TRINO_HOST"):
        raise RuntimeBindingError("host Trino env file must define TRINO_HOST")
    for key in TRINO_ENV_KEYS:
        os.environ.pop(key, None)
    os.environ.update(parsed)


def validate_data_plane_marker(
    client: Any,
    context: Mapping[str, Any],
) -> dict[str, Any]:
    marker = context["data_plane_marker"]
    values = (
        str(marker["deployment_id"]),
        str(marker["git_sha"]),
        str(marker["scheduler_container_id"]),
        str(marker["scheduler_image_id"]),
    )
    patterns = (
        r"[0-9a-f]{32}",
        r"[0-9a-f]{40}",
        r"[0-9a-f]{64}",
        r"sha256:[0-9a-fA-F]{64}",
    )
    if any(
        not re.fullmatch(pattern, value) for pattern, value in zip(patterns, values)
    ):
        raise RuntimeBindingError("unsafe data-plane marker identity")
    rows = client.query(
        "-- runtime-binding:data-plane-marker\n"
        'SELECT COUNT(*) FROM "iceberg"."bronze".'
        '"fotmob_runtime_deployments"\n'
        f"WHERE deployment_id = '{values[0]}' AND git_sha = '{values[1]}'\n"
        f"  AND scheduler_container_id = '{values[2]}'\n"
        f"  AND scheduler_image_id = '{values[3]}'"
    )
    if len(rows) != 1 or len(rows[0]) != 1 or int(rows[0][0]) != 1:
        raise RuntimeBindingError(
            "queried Trino data plane does not contain the exact deployment marker"
        )
    return {
        "table": marker["table"],
        "deployment_id": values[0],
        "git_sha": values[1],
        "scheduler_container_id": values[2],
        "scheduler_image_id": values[3],
        "matched": True,
    }
