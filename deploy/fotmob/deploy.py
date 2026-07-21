#!/usr/bin/env python3
"""Deploy and admit the isolated FotMob Airflow stack.

Admission is deliberately fail-closed: the scheduler must be healthy, its
DagBag must contain exactly the three expected DAGs, and import errors must be
empty before any DAG is unpaused.  A JSON report is written for every attempt.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import shutil
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


EXPECTED_DAGS = frozenset(
    {
        "dag_ingest_fotmob",
        "dag_transform_fotmob_silver",
        "dag_trigger_fotmob_daily",
    }
)
EXPECTED_DAG_FILES = {
    "dag_ingest_fotmob": "/opt/airflow/dags/dag_ingest_fotmob.py",
    "dag_transform_fotmob_silver": (
        "/opt/airflow/dags/dag_transform_fotmob_silver.py"
    ),
    "dag_trigger_fotmob_daily": "/opt/airflow/dags/dag_trigger_fotmob_daily.py",
}
EXPECTED_SCHEDULES = {
    "dag_ingest_fotmob": "None",
    "dag_transform_fotmob_silver": "None",
    "dag_trigger_fotmob_daily": "0 14 * * *",
}
ACTIVE_STATES = ("running", "queued")
RUNTIME_MARKER_TABLE = "iceberg.bronze.fotmob_runtime_deployments"
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
CONTAINER_EVIDENCE_ROOT = Path("/opt/airflow/logs/fotmob")
SHARED_REQUIRED_RUNTIME_PATHS = {
    "configs/fotmob/competitions.json",
    "configs/fotmob/issue-930-scopes.txt",
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


class DeploymentError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
            temporary = Path(stream.name)
        temporary.replace(path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _commit_trigger_activation(
    report_path: Path,
    report: Mapping[str, Any],
    *,
    airflow: Callable[..., subprocess.CompletedProcess[str]],
    assert_paused: Callable[[set[str]], list[dict[str, Any]]],
) -> dict[str, Any]:
    """Durably commit the complete admission before making the schedule live.

    ``_atomic_json`` fsyncs both the file and parent directory.  Consequently
    every crash boundary is fail-safe: before it returns the trigger is still
    paused; after it returns any successful/ambiguous unpause is backed by the
    complete active report that the first scheduled task re-attests.
    """

    active = {
        **report,
        "generated_at": _now(),
        "activation_state": "active",
        "kept_paused": False,
        "paused": [],
        "unpaused": sorted(EXPECTED_DAGS),
    }
    _atomic_json(report_path, active)
    airflow("dags", "unpause", "dag_trigger_fotmob_daily")
    assert_paused(set())
    return active


def validate_image_reference(image: str, *, label: str = "image") -> None:
    value = image.strip()
    if not re.fullmatch(r"[^\s@]+@sha256:[0-9a-fA-F]{64}", value):
        raise DeploymentError(
            f"{label} must be pinned by a full sha256 digest"
        )


def validate_database_password(env_file: Path, environment: Mapping[str, str]) -> None:
    value = _configured_env_value(
        env_file, environment, "FOTMOB_AIRFLOW_DB_PASSWORD"
    )

    # This secret is interpolated into a SQLAlchemy URI. Requiring the
    # RFC-3986 unreserved alphabet prevents reserved characters from changing
    # URI structure; operators should use a generated base64url/hex secret.
    if not re.fullmatch(r"[A-Za-z0-9._~-]+", value):
        raise DeploymentError(
            "FOTMOB_AIRFLOW_DB_PASSWORD must use only URL-safe unreserved characters"
        )


def _configured_env_value(
    env_file: Path,
    environment: Mapping[str, str],
    key_name: str,
) -> str:
    value = str(environment.get(key_name, ""))
    if not value:
        try:
            lines = env_file.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            raise DeploymentError(f"cannot read --env-file: {exc}") from exc
        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, candidate = line.split("=", 1)
            if key.strip() != key_name:
                continue
            value = candidate.strip()
            if value[:1] in {"'", '"'} and value[-1:] == value[:1]:
                value = value[1:-1]
    return value


def validate_delivery_credentials(
    env_file: Path, environment: Mapping[str, str]
) -> None:
    missing = [
        key
        for key in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID")
        if not _configured_env_value(env_file, environment, key).strip()
    ]
    if missing:
        raise DeploymentError(
            f"required delivery credentials are absent: {sorted(missing)!r}"
        )


def release_sha(root: Path, run: Callable[..., subprocess.CompletedProcess[str]]) -> str:
    if not root.is_absolute() or not root.is_dir():
        raise DeploymentError("--release-root must be an existing absolute directory")
    result = run(
        ("git", "-C", str(root), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    )
    sha = result.stdout.strip()
    if len(sha) != 40 or any(character not in "0123456789abcdef" for character in sha):
        raise DeploymentError(f"release checkout returned invalid Git SHA: {sha!r}")
    dirty = run(
        ("git", "-C", str(root), "status", "--porcelain"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if dirty:
        raise DeploymentError("release checkout is dirty; deploy an immutable checkout")
    ignored_runtime = run(
        (
            "git",
            "-C",
            str(root),
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
    ).stdout.strip()
    if ignored_runtime:
        raise DeploymentError(
            "release runtime trees contain ignored/untracked files; use a pristine worktree"
        )
    return sha


def prepare_dagbag(release_root: Path, evidence_dir: Path, sha: str) -> Path:
    """Create/reuse an exact read-only projection that masks image-baked DAGs."""

    sources = {
        "dag_ingest_fotmob.py": release_root / "dags/dag_ingest_fotmob.py",
        "dag_transform_fotmob_silver.py": release_root
        / "dags/dag_transform_fotmob_silver.py",
        "dag_trigger_fotmob_daily.py": release_root
        / "dags/dag_trigger_fotmob_daily.py",
        ".airflowignore": release_root / "deploy/fotmob/.airflowignore",
    }
    missing = [str(path) for path in sources.values() if not path.is_file()]
    if missing:
        raise DeploymentError(f"release misses DagBag source files: {missing!r}")
    destination = evidence_dir / "runtime" / sha / "dags"

    def verify_existing() -> None:
        observed_files = {
            item.name for item in destination.iterdir() if item.is_file()
        }
        observed_dirs = {
            item.name for item in destination.iterdir() if item.is_dir()
        }
        if observed_files != set(sources) or observed_dirs != {"utils", "sql", "scripts"}:
            raise DeploymentError("existing DagBag projection has unexpected entries")
        for name, source in sources.items():
            if destination.joinpath(name).read_bytes() != source.read_bytes():
                raise DeploymentError(f"existing DagBag projection drifted: {name}")

    if destination.exists():
        if not destination.is_dir():
            raise DeploymentError("DagBag projection path is not a directory")
        verify_existing()
        return destination

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=".fotmob-dagbag-", dir=destination.parent)
    )
    try:
        for name, source in sources.items():
            shutil.copyfile(source, temporary / name)
            (temporary / name).chmod(0o444)
        for name in ("utils", "sql", "scripts"):
            (temporary / name).mkdir(mode=0o555)
        temporary.chmod(0o555)
        temporary.replace(destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    verify_existing()
    return destination


def parse_airflow_json(output: str) -> list[dict[str, Any]]:
    """Parse Airflow JSON while tolerating log prefixes emitted by some images."""

    decoder = json.JSONDecoder()
    for index, character in enumerate(output):
        if character != "[":
            continue
        try:
            payload, _ = decoder.raw_decode(output[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, list) and all(isinstance(row, dict) for row in payload):
            return payload
    raise DeploymentError("Airflow command did not emit a JSON array of objects")


def parse_marker_json(output: str, marker: str) -> Any:
    for line in reversed(output.splitlines()):
        if line.startswith(marker):
            try:
                return json.loads(line.removeprefix(marker))
            except json.JSONDecodeError as exc:
                raise DeploymentError(f"invalid {marker} payload") from exc
    raise DeploymentError(f"command did not emit required {marker} evidence")


def validate_delivery_runtime(
    container: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, bool]:
    marker = "FOTMOB_DELIVERY_ENV_JSON="
    code = (
        "import json,os; "
        "r={'telegram_bot_token_configured':"
        "bool(os.environ.get('TELEGRAM_BOT_TOKEN','').strip()),"
        "'telegram_chat_id_configured':"
        "bool(os.environ.get('TELEGRAM_CHAT_ID','').strip())}; "
        f"print('{marker}'+json.dumps(r,sort_keys=True))"
    )
    output = run(
        ("docker", "exec", container, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = parse_marker_json(output, marker)
    expected = {
        "telegram_bot_token_configured": True,
        "telegram_chat_id_configured": True,
    }
    if payload != expected:
        raise DeploymentError("admitted scheduler misses Telegram delivery credentials")
    return expected


def validate_control_database(
    container: str,
    expected_uri: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    """Prove a scheduler uses the shared, fully-migrated control database."""

    if not expected_uri or "airflow-metadb" in expected_uri.lower():
        raise DeploymentError(
            "FBREF_CONTROL_DB_URI must reference the shared production control DB"
        )
    observed_uri = run(
        ("docker", "exec", container, "printenv", "FBREF_CONTROL_DB_URI"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.rstrip("\n")
    if observed_uri != expected_uri:
        raise DeploymentError(
            "scheduler FBREF_CONTROL_DB_URI differs from the admitted shared value"
        )
    code = (
        "import json; from scrapers.fbref.control import ControlStore; "
        "r=ControlStore.from_env().validate_migrations(); "
        "print('FOTMOB_CONTROL_DB_JSON='+json.dumps(r,sort_keys=True))"
    )
    output = run(
        ("docker", "exec", container, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    payload = parse_marker_json(output, "FOTMOB_CONTROL_DB_JSON=")
    if (
        not isinstance(payload, Mapping)
        or payload.get("status") != "passed"
        or payload.get("checksum_verified") is not True
        or not isinstance(payload.get("versions"), list)
        or not payload["versions"]
    ):
        raise DeploymentError("shared control database migration preflight failed")
    return {
        "same_shared_database": True,
        "migrations": payload,
    }


def validate_dagbag(dag_rows: Sequence[Mapping[str, Any]], errors: Sequence[Any]) -> None:
    dag_ids = {str(row.get("dag_id")) for row in dag_rows}
    if dag_ids != EXPECTED_DAGS:
        raise DeploymentError(
            f"unexpected DagBag: expected={sorted(EXPECTED_DAGS)!r}, "
            f"observed={sorted(dag_ids)!r}"
        )
    if errors:
        raise DeploymentError(f"DagBag has {len(errors)} import error(s)")


def validate_fresh_dagbag(payload: Mapping[str, Any]) -> None:
    dags = payload.get("dags")
    errors = payload.get("import_errors")
    if not isinstance(dags, Mapping) or set(dags) != EXPECTED_DAGS:
        raise DeploymentError("fresh DagBag does not contain exactly the admitted DAGs")
    if not isinstance(errors, Mapping) or errors:
        raise DeploymentError("fresh DagBag contains import errors")
    for dag_id in EXPECTED_DAGS:
        row = dags.get(dag_id)
        if not isinstance(row, Mapping):
            raise DeploymentError(f"fresh DagBag misses metadata for {dag_id}")
        if row.get("fileloc") != EXPECTED_DAG_FILES[dag_id]:
            raise DeploymentError(
                f"{dag_id} loaded from unexpected file: {row.get('fileloc')!r}"
            )
        if row.get("schedule") != EXPECTED_SCHEDULES[dag_id]:
            raise DeploymentError(
                f"{dag_id} has unexpected schedule: {row.get('schedule')!r}"
            )


def _paused_ids(rows: Sequence[Mapping[str, Any]]) -> set[str]:
    return {
        str(row.get("dag_id"))
        for row in rows
        if row.get("is_paused") in (True, "True", "true", "1", 1)
    }


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def shared_runtime_manifest(release_root: Path) -> dict[str, str]:
    """Hash the exact regular-file inventory exposed by shared bind mounts."""

    manifest: dict[str, str] = {}
    for relative_root in SHARED_RUNTIME_ROOTS:
        root = release_root / relative_root
        if not root.is_dir():
            raise DeploymentError(f"shared runtime root is absent: {relative_root}")
        for path in sorted(root.rglob("*")):
            if path.is_symlink():
                raise DeploymentError(
                    f"shared runtime manifest rejects symlink: {path}"
                )
            if (
                path.is_file()
                and "__pycache__" not in path.parts
                and path.name.endswith(SHARED_RUNTIME_SUFFIXES)
            ):
                relative_path = path.relative_to(release_root).as_posix()
                manifest[relative_path] = _sha256(path)
    missing = SHARED_REQUIRED_RUNTIME_PATHS - set(manifest)
    if missing:
        raise DeploymentError(
            f"shared runtime manifest misses required files: {sorted(missing)!r}"
        )
    if manifest[APPROVED_SCOPE_PATH] != APPROVED_SCOPE_SHA256:
        raise DeploymentError("issue-930 scope artifact differs from approved SHA-256")
    return manifest


def expected_isolated_runtime_manifest(
    release_root: Path, dagbag_root: Path
) -> dict[str, str]:
    """Return the exact effective file inventory mounted in the isolated stack."""

    shared = shared_runtime_manifest(release_root)
    manifest = {
        path: digest
        for path, digest in shared.items()
        if not path.startswith("dags/")
        or path in ISOLATED_DAG_ROOT_PATHS
        or path.startswith(ISOLATED_DAG_PREFIXES)
    }
    missing = ISOLATED_DAG_ROOT_PATHS - set(manifest)
    if missing:
        raise DeploymentError(
            f"isolated runtime manifest misses root DAGs: {sorted(missing)!r}"
        )
    airflowignore = dagbag_root / ".airflowignore"
    if not airflowignore.is_file() or airflowignore.is_symlink():
        raise DeploymentError("isolated DagBag projection misses .airflowignore")
    manifest[ISOLATED_AIRFLOWIGNORE_PATH] = _sha256(airflowignore)
    return dict(sorted(manifest.items()))


def validate_isolated_runtime_manifest(
    container_id: str,
    expected: Mapping[str, str],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, str]:
    """Re-hash the effective bind mounts inside the admitted scheduler."""

    code = (
        "import json,sys; sys.path.insert(0,'/opt/airflow/dags'); "
        "from utils.fotmob_publication import isolated_runtime_manifest; "
        "print('FOTMOB_ISOLATED_RUNTIME_MANIFEST_JSON='+"
        "json.dumps(isolated_runtime_manifest(),sort_keys=True))"
    )
    output = run(
        ("docker", "exec", container_id, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    observed = parse_marker_json(
        output, "FOTMOB_ISOLATED_RUNTIME_MANIFEST_JSON="
    )
    if not isinstance(observed, Mapping) or any(
        not isinstance(path, str)
        or re.fullmatch(r"[0-9a-f]{64}", str(digest)) is None
        for path, digest in observed.items()
    ):
        raise DeploymentError("isolated runtime manifest evidence is invalid")
    normalized = {str(path): str(digest) for path, digest in observed.items()}
    if normalized != dict(expected):
        raise DeploymentError(
            "isolated scheduler bind-mounted runtime differs from release manifest"
        )
    return normalized


def validate_shared_handoff(
    release_root: Path,
    shared_container: str,
    expected_control_uri: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    """Prove the shared scheduler has surrendered FotMob schedule ownership."""

    shared_container_id = run(
        ("docker", "inspect", "--format", "{{.Id}}", shared_container),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if re.fullmatch(r"[0-9a-f]{64}", shared_container_id) is None:
        raise DeploymentError("cannot resolve full shared scheduler container ID")
    shared_container = shared_container_id
    control_database = validate_control_database(
        shared_container, expected_control_uri, run=run
    )
    shared_runtime_hashes = shared_runtime_manifest(release_root)
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
        "and path.name.endswith(suffixes)):\n"
        "            key=prefix+'/'+path.relative_to(root).as_posix()\n"
        "            manifest[key]=hashlib.sha256(path.read_bytes()).hexdigest()\n"
        "print('FOTMOB_SHARED_RUNTIME_MANIFEST_JSON='+"
        "json.dumps(manifest,sort_keys=True))\n"
    )
    remote_manifest_output = run(
        (
            "docker",
            "exec",
            shared_container,
            "python",
            "-c",
            manifest_code,
        ),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    remote_manifest = parse_marker_json(
        remote_manifest_output, "FOTMOB_SHARED_RUNTIME_MANIFEST_JSON="
    )
    if (
        not isinstance(remote_manifest, Mapping)
        or remote_manifest != shared_runtime_hashes
    ):
        raise DeploymentError(
            "shared scheduler bind-mounted runtime differs from the exact release manifest"
        )
    expected_hash = shared_runtime_hashes[MASTER_RUNTIME_PATH]
    remote_hash = str(remote_manifest[MASTER_RUNTIME_PATH])
    shared_runtime_sha = run(
        (
            "docker",
            "exec",
            shared_container,
            "printenv",
            "FOTMOB_DEPLOY_GIT_SHA",
        ),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.rstrip("\n")
    expected_runtime_sha = run(
        ("git", "-C", str(release_root), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", expected_runtime_sha):
        raise DeploymentError("release checkout returned invalid Git SHA")
    if shared_runtime_sha != expected_runtime_sha:
        raise DeploymentError(
            "shared scheduler FOTMOB_DEPLOY_GIT_SHA differs from release HEAD"
        )
    serialized_code = """
import json
import os
from airflow.models import DagModel, DagRun, Variable
from airflow.models.serialized_dag import SerializedDagModel
from airflow.settings import Session
from sqlalchemy import text

s = Session()
s.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
serialized_ids = (
    'dag_master_pipeline',
    'dag_sofascore_pipeline',
    'dag_transform_xref',
    'dag_transform_e3',
    'dag_transform_e4',
    'dag_transform_fbref_gold',
    'dag_trigger_fotmob_daily',
)
serialized_rows = s.query(SerializedDagModel).filter(
    SerializedDagModel.dag_id.in_(serialized_ids)
).all()
dags = {row.dag_id: row.dag for row in serialized_rows}
master = dags.get('dag_master_pipeline')
master_tasks = getattr(master, 'task_dict', {}) if master is not None else {}
master_gate = master_tasks.get('ingestion_triggers.fotmob_shared_schedule_owner')
master_trigger = master_tasks.get('ingestion_triggers.trigger_fotmob')
sofa = dags.get('dag_sofascore_pipeline')
sofa_tasks = getattr(sofa, 'task_dict', {}) if sofa is not None else {}
sensor = sofa_tasks.get('wait_for_fotmob_publication')
xref = sofa_tasks.get('trigger_xref_transforms')
e4 = sofa_tasks.get('trigger_e4_transforms')
finalizer = sofa_tasks.get('finalize_fotmob_publication')
xref_dag = dags.get('dag_transform_xref')
xref_tasks = getattr(xref_dag, 'task_dict', {}) if xref_dag is not None else {}
xref_start = xref_tasks.get('start_marker')
xref_preflight = xref_tasks.get('validate_fotmob_publication_consumer')
isolated_daily = dags.get('dag_trigger_fotmob_daily')

def descendants(task):
    pending = list(getattr(task, 'downstream_task_ids', [])) if task else []
    observed = set()
    while pending:
        task_id = pending.pop()
        if task_id in observed:
            continue
        observed.add(task_id)
        child = xref_tasks.get(task_id)
        pending.extend(getattr(child, 'downstream_task_ids', []))
    return sorted(observed)

def fenced_downstream(dag_id):
    downstream_dag = dags.get(dag_id)
    tasks = (
        getattr(downstream_dag, 'task_dict', {})
        if downstream_dag is not None else {}
    )
    start = tasks.get('start_marker')
    preflight = tasks.get('validate_fotmob_publication_consumer')
    pending = list(getattr(preflight, 'downstream_task_ids', [])) if preflight else []
    observed = set()
    while pending:
        task_id = pending.pop()
        if task_id in observed:
            continue
        observed.add(task_id)
        child = tasks.get(task_id)
        pending.extend(getattr(child, 'downstream_task_ids', []))
    direct_downstream = sorted(getattr(preflight, 'downstream_task_ids', []))
    return {
        'present': downstream_dag is not None,
        'fileloc': getattr(downstream_dag, 'fileloc', None),
        'task_ids': sorted(tasks),
        'start_present': start is not None,
        'start_downstream': sorted(getattr(start, 'downstream_task_ids', [])),
        'preflight_present': preflight is not None,
        'preflight_upstream': sorted(getattr(preflight, 'upstream_task_ids', [])),
        'preflight_downstream': direct_downstream,
        'preflight_descendants': sorted(observed),
        'preflight_trigger_rule': str(getattr(preflight, 'trigger_rule', '')),
        'direct_downstream_trigger_rules': {
            task_id: str(getattr(tasks.get(task_id), 'trigger_rule', ''))
            for task_id in direct_downstream
        },
    }

pause_ids = (
    'dag_master_pipeline',
    'dag_sofascore_pipeline',
    'dag_ingest_fotmob',
    'dag_transform_fotmob_silver',
)
active_ids = (
    *pause_ids,
    'dag_transform_xref',
    'dag_transform_e3',
    'dag_transform_e4',
    'dag_transform_fbref_gold',
    'dag_trigger_fotmob_daily',
)
pause_rows = s.query(DagModel.dag_id, DagModel.is_paused).filter(
    DagModel.dag_id.in_(pause_ids)
).all()
daily_model = s.query(DagModel.dag_id, DagModel.is_paused).filter(
    DagModel.dag_id == 'dag_trigger_fotmob_daily'
).one_or_none()
run_rows = s.query(DagRun.dag_id, DagRun.run_id, DagRun.state).filter(
    DagRun.dag_id.in_(active_ids), DagRun.state.in_(('running', 'queued'))
).all()
owner_row = s.query(Variable).filter(
    Variable.key == 'fotmob_schedule_owner'
).one_or_none()
payload = {
    'master': {
        'present': master is not None,
        'fileloc': getattr(master, 'fileloc', None),
        'gate_present': master_gate is not None,
        'trigger_upstream': sorted(getattr(master_trigger, 'upstream_task_ids', [])),
    },
    'sofascore': {
        'present': sofa is not None,
        'fileloc': getattr(sofa, 'fileloc', None),
        'sensor_present': sensor is not None,
        'xref_present': xref is not None,
        'e4_present': e4 is not None,
        'finalizer_present': finalizer is not None,
        'sensor_downstream': sorted(getattr(sensor, 'downstream_task_ids', [])),
        'xref_upstream': sorted(getattr(xref, 'upstream_task_ids', [])),
        'e4_downstream': sorted(getattr(e4, 'downstream_task_ids', [])),
        'finalizer_upstream': sorted(getattr(finalizer, 'upstream_task_ids', [])),
        'finalizer_trigger_rule': str(getattr(finalizer, 'trigger_rule', '')),
    },
    'xref': {
        'present': xref_dag is not None,
        'fileloc': getattr(xref_dag, 'fileloc', None),
        'task_ids': sorted(xref_tasks),
        'start_present': xref_start is not None,
        'preflight_present': xref_preflight is not None,
        'start_downstream': sorted(
            getattr(xref_start, 'downstream_task_ids', [])
        ),
        'preflight_upstream': sorted(
            getattr(xref_preflight, 'upstream_task_ids', [])
        ),
        'preflight_descendants': descendants(xref_preflight),
        'preflight_trigger_rule': str(
            getattr(xref_preflight, 'trigger_rule', '')
        ),
        'task_trigger_rules': {
            task_id: str(getattr(task, 'trigger_rule', ''))
            for task_id, task in xref_tasks.items()
        },
    },
    'fenced_downstream': {
        dag_id: fenced_downstream(dag_id)
        for dag_id in (
            'dag_transform_e3',
            'dag_transform_e4',
            'dag_transform_fbref_gold',
        )
    },
    'pause_states': {dag_id: bool(paused) for dag_id, paused in pause_rows},
    'schedule_owner': getattr(owner_row, 'val', None),
    'shared_daily_trigger': {
        'isolated_stack_env': os.environ.get('FOTMOB_ISOLATED_STACK'),
        'serialized_present': isolated_daily is not None,
        'serialized_fileloc': getattr(isolated_daily, 'fileloc', None),
        'dag_model_present': daily_model is not None,
        'dag_model_paused': bool(daily_model[1]) if daily_model is not None else None,
    },
    'active_runs': [
        {
            'dag_id': dag_id,
            'run_id': str(run_id),
            'state': str(getattr(state, 'value', state)).lower(),
        }
        for dag_id, run_id, state in run_rows
    ],
}
print('FOTMOB_SHARED_ORCHESTRATION_JSON=' + json.dumps(payload, sort_keys=True))
s.close()
"""
    serialized_output = run(
        (
            "docker",
            "exec",
            shared_container,
            "python",
            "-c",
            serialized_code,
        ),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    orchestration = parse_marker_json(
        serialized_output, "FOTMOB_SHARED_ORCHESTRATION_JSON="
    )
    gate_id = "ingestion_triggers.fotmob_shared_schedule_owner"
    if not isinstance(orchestration, Mapping):
        raise DeploymentError("shared orchestration evidence is not an object")
    serialized = orchestration.get("master")
    if not isinstance(serialized, Mapping) or not serialized.get("present"):
        raise DeploymentError("shared metadata has no serialized master DAG")
    if serialized.get("fileloc") != "/opt/airflow/dags/dag_master_pipeline.py":
        raise DeploymentError("shared serialized master DAG has unexpected file location")
    if serialized.get("gate_present") is not True or gate_id not in set(
        serialized.get("trigger_upstream") or ()
    ):
        raise DeploymentError(
            "shared serialized master DAG has not admitted the FotMob ownership gate"
        )
    serialized_sofa = orchestration.get("sofascore")
    if not isinstance(serialized_sofa, Mapping) or not serialized_sofa.get("present"):
        raise DeploymentError("shared metadata has no serialized SofaScore pipeline DAG")
    if serialized_sofa.get("fileloc") != (
        "/opt/airflow/dags/dag_sofascore_pipeline.py"
    ):
        raise DeploymentError(
            "shared serialized SofaScore pipeline has unexpected file location"
        )
    required_sofa_tasks = (
        "sensor_present",
        "xref_present",
        "e4_present",
        "finalizer_present",
    )
    if any(serialized_sofa.get(key) is not True for key in required_sofa_tasks):
        raise DeploymentError(
            "shared serialized SofaScore pipeline misses FotMob publication tasks"
        )
    sensor_id = "wait_for_fotmob_publication"
    xref_id = "trigger_xref_transforms"
    e4_id = "trigger_e4_transforms"
    finalizer_id = "finalize_fotmob_publication"
    if (
        sensor_id not in set(serialized_sofa.get("xref_upstream") or ())
        or xref_id not in set(serialized_sofa.get("sensor_downstream") or ())
        or set(serialized_sofa.get("finalizer_upstream") or ())
        != {sensor_id, e4_id}
        or finalizer_id not in set(serialized_sofa.get("e4_downstream") or ())
        or serialized_sofa.get("finalizer_trigger_rule") != "all_done"
    ):
        raise DeploymentError(
            "shared serialized SofaScore pipeline has unsafe FotMob publication edges"
        )
    serialized_xref = orchestration.get("xref")
    if not isinstance(serialized_xref, Mapping) or not serialized_xref.get("present"):
        raise DeploymentError("shared metadata has no serialized xref DAG")
    if serialized_xref.get("fileloc") != "/opt/airflow/dags/dag_transform_xref.py":
        raise DeploymentError("shared serialized xref DAG has unexpected file location")
    xref_start_id = "start_marker"
    xref_preflight_id = "validate_fotmob_publication_consumer"
    xref_writer_ids = {
        "xref_transforms.xref_team",
        "xref_transforms.xref_referee",
        "xref_transforms.xref_match",
        "xref_transforms.xref_manager",
        "xref_player",
    }
    xref_task_ids = set(serialized_xref.get("task_ids") or ())
    xref_descendants = set(serialized_xref.get("preflight_descendants") or ())
    trigger_rules = serialized_xref.get("task_trigger_rules")
    if (
        serialized_xref.get("start_present") is not True
        or serialized_xref.get("preflight_present") is not True
        or set(serialized_xref.get("start_downstream") or ())
        != {xref_preflight_id}
        or set(serialized_xref.get("preflight_upstream") or ()) != {xref_start_id}
        or serialized_xref.get("preflight_trigger_rule") != "all_success"
        or not xref_writer_ids.issubset(xref_task_ids)
        or not xref_writer_ids.issubset(xref_descendants)
        or xref_task_ids - {xref_start_id, xref_preflight_id} != xref_descendants
        or not isinstance(trigger_rules, Mapping)
        or any(trigger_rules.get(task_id) != "all_success" for task_id in xref_writer_ids)
    ):
        raise DeploymentError(
            "shared serialized xref DAG does not gate every writer behind "
            "the FotMob publication preflight"
        )
    fenced_downstream = orchestration.get("fenced_downstream")
    if not isinstance(fenced_downstream, Mapping):
        raise DeploymentError("shared metadata has no downstream fence evidence")
    downstream_contracts = {
        "dag_transform_e3": {
            "fileloc": "/opt/airflow/dags/dag_transform_e3.py",
            "start": True,
            "first": {"silver_e3.whoscored_events_spadl"},
        },
        "dag_transform_e4": {
            "fileloc": "/opt/airflow/dags/dag_transform_e4.py",
            "start": True,
            "first": {"silver_e4.matchhistory_match_odds"},
        },
        "dag_transform_fbref_gold": {
            "fileloc": "/opt/airflow/dags/dag_transform_fbref_gold.py",
            "start": False,
            "first": {"transfermarkt_reader_precondition"},
        },
    }
    for dag_id, contract in downstream_contracts.items():
        proof = fenced_downstream.get(dag_id)
        if not isinstance(proof, Mapping):
            raise DeploymentError(f"shared metadata has no serialized {dag_id} DAG")
        task_ids = set(proof.get("task_ids") or ())
        descendants = set(proof.get("preflight_descendants") or ())
        preflight_id = "validate_fotmob_publication_consumer"
        excluded = {preflight_id}
        expected_upstream: set[str] = set()
        if contract["start"]:
            excluded.add("start_marker")
            expected_upstream.add("start_marker")
        direct_rules = proof.get("direct_downstream_trigger_rules")
        if (
            proof.get("present") is not True
            or proof.get("fileloc") != contract["fileloc"]
            or proof.get("preflight_present") is not True
            or proof.get("preflight_trigger_rule") != "all_success"
            or set(proof.get("preflight_upstream") or ()) != expected_upstream
            or set(proof.get("preflight_downstream") or ()) != contract["first"]
            or task_ids - excluded != descendants
            or not isinstance(direct_rules, Mapping)
            or any(
                direct_rules.get(task_id) != "all_success"
                for task_id in contract["first"]
            )
            or (
                contract["start"]
                and (
                    proof.get("start_present") is not True
                    or set(proof.get("start_downstream") or ()) != {preflight_id}
                )
            )
            or (not contract["start"] and proof.get("start_present") is True)
        ):
            raise DeploymentError(
                f"shared serialized {dag_id} does not place the FotMob "
                "publication preflight before every downstream task"
            )
    pause_states = orchestration.get("pause_states")
    expected_pause_states = {
        "dag_master_pipeline": True,
        "dag_sofascore_pipeline": False,
        "dag_ingest_fotmob": True,
        "dag_transform_fotmob_silver": True,
    }
    if not isinstance(pause_states, Mapping) or {
        dag_id: pause_states.get(dag_id) for dag_id in expected_pause_states
    } != expected_pause_states:
        raise DeploymentError(
            "shared orchestration must have master/ingest/Silver paused and "
            "SofaScore pipeline unpaused"
        )
    active_rows = orchestration.get("active_runs")
    if not isinstance(active_rows, list) or any(
        not isinstance(row, Mapping) for row in active_rows
    ):
        raise DeploymentError("shared orchestration active-run evidence is invalid")
    if active_rows:
        raise DeploymentError(
            "shared scheduler still has active master/Sofa/FotMob/xref/E3/E4/Gold/"
            "isolated-daily runs: "
            f"{active_rows!r}"
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
        raise DeploymentError(
            "shared isolated daily trigger must be absent or a paused stale row"
        )
    owner = str(orchestration.get("schedule_owner", "")).strip().lower()
    if owner != "isolated":
        raise DeploymentError(
            "shared Airflow Variable fotmob_schedule_owner must equal 'isolated'"
        )
    checked_runs = {
        dag_id: {state: [] for state in ACTIVE_STATES}
        for dag_id in (
            "dag_master_pipeline",
            "dag_sofascore_pipeline",
            "dag_ingest_fotmob",
            "dag_transform_fotmob_silver",
            "dag_transform_xref",
            "dag_transform_e3",
            "dag_transform_e4",
            "dag_transform_fbref_gold",
            "dag_trigger_fotmob_daily",
        )
    }
    return {
        "shared_scheduler_container": shared_container_id,
        "master_dag_sha256": expected_hash,
        "remote_master_dag_sha256": remote_hash,
        "runtime_code_sha256": shared_runtime_hashes,
        "runtime_git_sha": shared_runtime_sha,
        "serialized_master": serialized,
        "serialized_sofascore": serialized_sofa,
        "serialized_xref": serialized_xref,
        "serialized_downstream": dict(fenced_downstream),
        "orchestration_state": {
            "pause_states": dict(pause_states),
            "expected_pause_states": expected_pause_states,
            "active_runs": [],
            "atomic_metadata_snapshot": True,
            "shared_daily_trigger": dict(shared_daily),
        },
        "schedule_owner": owner,
        "active_run_checks": checked_runs,
        "control_database": control_database,
        "passed": True,
    }


def validate_stable_shared_handoff(
    initial: Mapping[str, Any], final: Mapping[str, Any]
) -> None:
    if (
        initial.get("shared_scheduler_container")
        != final.get("shared_scheduler_container")
        or initial.get("runtime_code_sha256") != final.get("runtime_code_sha256")
    ):
        raise DeploymentError("shared handoff identity changed during admission")


def build_parser() -> argparse.ArgumentParser:
    default_compose = Path(__file__).resolve().with_name("airflow.compose.yaml")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-root", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument(
        "--postgres-image",
        required=True,
        help="PostgreSQL metadata image pinned by full sha256 digest",
    )
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--compose-file", type=Path, default=default_compose)
    parser.add_argument("--project", default="fotmob-airflow")
    parser.add_argument(
        "--shared-scheduler-container",
        default="airflow-scheduler",
        help="Shared scheduler container whose ownership handoff must be proven",
    )
    parser.add_argument(
        "--keep-paused",
        action="store_true",
        help="Admit the release but keep every DAG paused (required for rollback)",
    )
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--report", type=Path)
    return parser


def deploy(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    validate_image_reference(args.image, label="FOTMOB_AIRFLOW_IMAGE")
    validate_image_reference(args.postgres_image, label="FOTMOB_POSTGRES_IMAGE")
    release_root = args.release_root.resolve()
    evidence_dir = args.evidence_dir.resolve()
    compose_file = args.compose_file.resolve()
    configured_report = getattr(args, "report", None)
    report_path = (
        configured_report.resolve()
        if configured_report is not None
        else evidence_dir / "deployment.json"
    )
    try:
        report_relative_path = report_path.relative_to(evidence_dir)
    except ValueError as exc:
        raise DeploymentError(
            "--report must be inside --evidence-dir for scheduled runtime attestation"
        ) from exc
    if not report_relative_path.parts:
        raise DeploymentError("--report must name a file inside --evidence-dir")
    container_report_path = CONTAINER_EVIDENCE_ROOT / report_relative_path
    sha = release_sha(release_root, run)
    if not args.env_file.is_file():
        raise DeploymentError("--env-file does not exist")
    validate_database_password(args.env_file, os.environ)
    validate_delivery_credentials(args.env_file, os.environ)
    control_db_uri = _configured_env_value(
        args.env_file, os.environ, "FBREF_CONTROL_DB_URI"
    )
    if not control_db_uri:
        raise DeploymentError("FBREF_CONTROL_DB_URI is required")
    if not compose_file.is_file():
        raise DeploymentError("--compose-file does not exist")
    evidence_dir.mkdir(parents=True, exist_ok=True)
    dagbag_root = prepare_dagbag(release_root, evidence_dir, sha)
    initial_handoff = validate_shared_handoff(
        release_root,
        args.shared_scheduler_container,
        control_db_uri,
        run=run,
    )
    deployment_id = secrets.token_hex(16)
    environment = dict(os.environ)
    environment.update(
        {
            "FOTMOB_RELEASE_ROOT": str(release_root),
            "FOTMOB_AIRFLOW_IMAGE": args.image,
            "FOTMOB_POSTGRES_IMAGE": args.postgres_image,
            "FOTMOB_EVIDENCE_DIR": str(evidence_dir),
            "FOTMOB_DAGBAG_ROOT": str(dagbag_root),
            "FOTMOB_DEPLOY_GIT_SHA": sha,
            "FOTMOB_DEPLOYMENT_ID": deployment_id,
            "FOTMOB_DEPLOYMENT_REPORT_PATH": str(container_report_path),
        }
    )
    base = (
        "docker",
        "compose",
        "-p",
        args.project,
        "-f",
        str(compose_file),
        "--env-file",
        str(args.env_file.resolve()),
    )

    def command(
        *parts: str,
        capture: bool = False,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        return run(
            (*base, *parts),
            check=check,
            env=environment,
            capture_output=capture,
            text=True,
        )

    def airflow(*parts: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return command(
            "exec",
            "-T",
            "airflow-scheduler",
            "airflow",
            *parts,
            capture=True,
            check=check,
        )

    def dag_rows() -> list[dict[str, Any]]:
        return parse_airflow_json(
            airflow("dags", "list", "--output", "json").stdout
        )

    def active_runs() -> dict[str, dict[str, list[str]]]:
        active: dict[str, dict[str, list[str]]] = {}
        for dag_id in sorted(EXPECTED_DAGS):
            rows = parse_airflow_json(
                airflow(
                    "dags",
                    "list-runs",
                    "-d",
                    dag_id,
                    "--output",
                    "json",
                ).stdout
            )
            for state in ACTIVE_STATES:
                run_ids = [
                    str(row.get("run_id"))
                    for row in rows
                    if str(row.get("state", "")).lower() == state
                ]
                if run_ids:
                    active.setdefault(dag_id, {})[state] = [
                        *run_ids
                    ]
        return active

    def assert_paused(expected: set[str]) -> list[dict[str, Any]]:
        rows = dag_rows()
        validate_dagbag(rows, ())
        observed = _paused_ids(rows)
        if observed != expected:
            raise DeploymentError(
                f"unexpected pause state: expected={sorted(expected)!r}, "
                f"observed={sorted(observed)!r}"
            )
        return rows

    command("config", "--quiet")
    existing_scheduler = command(
        "ps", "--no-trunc", "-q", "airflow-scheduler", capture=True
    ).stdout.strip()
    if existing_scheduler:
        existing_active = active_runs()
        if existing_active:
            raise DeploymentError(
                f"isolated scheduler has active runs; redeploy aborted: {existing_active!r}"
            )
        for dag_id in sorted(EXPECTED_DAGS):
            airflow("dags", "pause", dag_id)
        assert_paused(set(EXPECTED_DAGS))
        post_pause_active = active_runs()
        if post_pause_active:
            raise DeploymentError(
                "isolated runs appeared during redeploy handoff; leave paused and wait: "
                f"{post_pause_active!r}"
            )
        command("stop", "airflow-scheduler")

    launch_attempted = False
    try:
        # ``docker compose up`` is not transactional: it may start the
        # scheduler and still return non-zero because another service failed.
        # Mark the attempt before invoking Compose so the exception path always
        # quiesces any partially-created scheduler.
        launch_attempted = True
        command("up", "-d", "airflow-metadb", "airflow-init", "airflow-scheduler")
        deadline = time.monotonic() + max(1, args.timeout_seconds)
        health_error: str | None = "scheduler health check not attempted"
        while time.monotonic() < deadline:
            try:
                airflow(
                    "jobs", "check", "--job-type", "SchedulerJob"
                )
                health_error = None
                break
            except subprocess.CalledProcessError as exc:
                health_error = str(exc)
                sleeper(2)
        if health_error is not None:
            raise DeploymentError(f"scheduler did not become healthy: {health_error}")

        cli_rows = dag_rows()
        import_errors = parse_airflow_json(
            airflow(
                "dags", "list-import-errors", "--output", "json"
            ).stdout
        )
        validate_dagbag(cli_rows, import_errors)

        inspection_code = (
            "import json; from airflow.models import DagBag; "
            "b=DagBag(dag_folder='/opt/airflow/dags', include_examples=False, "
            "safe_mode=False); "
            "p={'dags':{i:{'fileloc':d.fileloc,'schedule':str(d.schedule_interval)} "
            "for i,d in b.dags.items()},'import_errors':b.import_errors}; "
            "print('FOTMOB_DAGBAG_JSON='+json.dumps(p,default=str,sort_keys=True))"
        )
        fresh_output = command(
            "exec",
            "-T",
            "airflow-scheduler",
            "python",
            "-c",
            inspection_code,
            capture=True,
        ).stdout
        fresh_payload = parse_marker_json(fresh_output, "FOTMOB_DAGBAG_JSON=")
        if not isinstance(fresh_payload, Mapping):
            raise DeploymentError("fresh DagBag evidence is not an object")
        validate_fresh_dagbag(fresh_payload)

        # airflow-init pauses surviving DagModel rows; prove that fact before
        # any admission transition.
        assert_paused(set(EXPECTED_DAGS))
        if active_runs():
            raise DeploymentError("isolated stack gained active runs while paused")

        # Support modules/configs are host bind mounts rather than image
        # layers. Re-attest the checkout and copied DagBag at the admission
        # edge; a host-side edit during the health wait must fail closed and
        # drive the exception path below.
        if release_sha(release_root, run) != sha:
            raise DeploymentError("release Git SHA changed during admission")
        if prepare_dagbag(release_root, evidence_dir, sha) != dagbag_root:
            raise DeploymentError("DagBag projection changed during admission")

        container_id = command(
            "ps", "--all", "--no-trunc", "-q", "airflow-scheduler", capture=True
        ).stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{64}", container_id):
            raise DeploymentError("cannot resolve admitted scheduler container")
        image_id = run(
            ("docker", "inspect", "--format", "{{.Image}}", container_id),
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if not re.fullmatch(r"sha256:[0-9a-fA-F]{64}", image_id):
            raise DeploymentError("cannot resolve immutable scheduler image ID")
        metadb_container_id = command(
            "ps", "--all", "--no-trunc", "-q", "airflow-metadb", capture=True
        ).stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{64}", metadb_container_id):
            raise DeploymentError("cannot resolve admitted metadata DB container")
        postgres_image_id = run(
            (
                "docker",
                "inspect",
                "--format",
                "{{.Image}}",
                metadb_container_id,
            ),
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if not re.fullmatch(r"sha256:[0-9a-fA-F]{64}", postgres_image_id):
            raise DeploymentError("cannot resolve immutable PostgreSQL image ID")
        isolated_control_database = validate_control_database(
            container_id, control_db_uri, run=run
        )
        delivery_credentials = validate_delivery_runtime(container_id, run=run)
        marker_create_sql = f"""CREATE TABLE IF NOT EXISTS {RUNTIME_MARKER_TABLE} (
            deployment_id VARCHAR,
            git_sha VARCHAR,
            scheduler_container_id VARCHAR,
            scheduler_image_id VARCHAR,
            admitted_at TIMESTAMP(6) WITH TIME ZONE
        )"""
        marker_insert_sql = f"""INSERT INTO {RUNTIME_MARKER_TABLE}
            (deployment_id, git_sha, scheduler_container_id, scheduler_image_id, admitted_at)
        VALUES ('{deployment_id}', '{sha}', '{container_id}', '{image_id}', current_timestamp)"""
        marker_count_sql = f"""SELECT COUNT(*) FROM {RUNTIME_MARKER_TABLE}
        WHERE deployment_id = '{deployment_id}' AND git_sha = '{sha}'
          AND scheduler_container_id = '{container_id}'
          AND scheduler_image_id = '{image_id}'"""
        marker_code = (
            "import json,os; import trino; from trino.auth import BasicAuthentication; "
            "u=os.environ.get('TRINO_USER','airflow'); p=os.environ.get('TRINO_PASSWORD',''); "
            "k={'host':os.environ['TRINO_HOST'],"
            "'port':int(os.environ.get('TRINO_PORT','8443')),"
            "'user':u,'catalog':'iceberg','schema':'bronze',"
            "'http_scheme':os.environ.get('TRINO_HTTP_SCHEME','https'),"
            "'verify':os.environ.get('TRINO_TLS_VERIFY','true').lower() not in "
            "{'0','false','no'}}; "
            "k.update({'auth':BasicAuthentication(u,p)} if p else {}); "
            "c=trino.dbapi.connect(**k); q=c.cursor(); "
            f"q.execute({marker_create_sql!r}); q.fetchall(); "
            f"q.execute({marker_insert_sql!r}); q.fetchall(); "
            f"q.execute({marker_count_sql!r}); n=int(q.fetchone()[0]); "
            "q.close(); c.close(); "
            "print('FOTMOB_RUNTIME_MARKER_JSON='+json.dumps({'count':n}))"
        )
        marker_output = command(
            "exec",
            "-T",
            "airflow-scheduler",
            "python",
            "-c",
            marker_code,
            capture=True,
        ).stdout
        marker_result = parse_marker_json(
            marker_output, "FOTMOB_RUNTIME_MARKER_JSON="
        )
        if not isinstance(marker_result, Mapping) or marker_result.get("count") != 1:
            raise DeploymentError("durable Trino deployment marker was not admitted")
        data_plane_marker = {
            "table": RUNTIME_MARKER_TABLE,
            "deployment_id": deployment_id,
            "git_sha": sha,
            "scheduler_container_id": container_id,
            "scheduler_image_id": image_id,
        }
        final_handoff = initial_handoff
        if not args.keep_paused:
            for dag_id in (
                "dag_ingest_fotmob",
                "dag_transform_fotmob_silver",
            ):
                airflow("dags", "unpause", dag_id)
            assert_paused({"dag_trigger_fotmob_daily"})
            if active_runs():
                raise DeploymentError(
                    "isolated stack gained an active run before schedule admission"
                )
            # Re-prove both ownership and live bind bytes at the final schedule
            # boundary. No fallible admission work remains after trigger
            # unpause apart from the exact pause-state assertion.
            final_handoff = validate_shared_handoff(
                release_root,
                args.shared_scheduler_container,
                control_db_uri,
                run=run,
            )
            validate_stable_shared_handoff(initial_handoff, final_handoff)
            if release_sha(release_root, run) != sha:
                raise DeploymentError("release Git SHA changed before schedule admission")
            if prepare_dagbag(release_root, evidence_dir, sha) != dagbag_root:
                raise DeploymentError("DagBag projection changed before schedule admission")
        isolated_runtime_hashes = validate_isolated_runtime_manifest(
            container_id,
            expected_isolated_runtime_manifest(release_root, dagbag_root),
            run=run,
        )
        report = {
            "schema_version": "fotmob-deploy-v2",
            "generated_at": _now(),
            "passed": True,
            "project": args.project,
            "compose_file": str(compose_file),
            "release_root": str(release_root),
            "evidence_dir": str(evidence_dir),
            "container_report_path": str(container_report_path),
            "dagbag_root": str(dagbag_root),
            "git_sha": sha,
            "deployment_id": deployment_id,
            "image": args.image,
            "postgres_image": args.postgres_image,
            "resolved_image_id": image_id,
            "resolved_postgres_image_id": postgres_image_id,
            "scheduler_container_id": container_id,
            "metadb_container_id": metadb_container_id,
            "data_plane_marker": data_plane_marker,
            "delivery_credentials": delivery_credentials,
            "isolated_runtime_sha256": isolated_runtime_hashes,
            "control_database": {
                "shared": initial_handoff["control_database"],
                "isolated": isolated_control_database,
                "same_runtime_configuration": True,
            },
            "dags": sorted(EXPECTED_DAGS),
            "fresh_dagbag": fresh_payload,
            "import_errors": 0,
            "shared_handoff_initial": initial_handoff,
            "shared_handoff_final": final_handoff,
        }
        if args.keep_paused:
            kept_paused = {
                **report,
                "activation_state": "kept_paused",
                "kept_paused": True,
                "paused": sorted(EXPECTED_DAGS),
                "unpaused": [],
            }
            _atomic_json(report_path, kept_paused)
            return kept_paused
        return _commit_trigger_activation(
            report_path,
            report,
            airflow=airflow,
            assert_paused=assert_paused,
        )
    except Exception as exc:
        if launch_attempted:
            for dag_id in sorted(EXPECTED_DAGS):
                try:
                    airflow("dags", "pause", dag_id, check=False)
                except Exception:
                    pass
            try:
                # Best-effort confirmation is useful in command logs even
                # though the scheduler is stopped unconditionally below.
                assert_paused(set(EXPECTED_DAGS))
            except Exception:
                pass
            # An already-created LocalExecutor task can keep running after a
            # successful metadata pause. A failed admission never leaves the
            # scheduler alive, irrespective of pause command outcome.
            try:
                command("stop", "airflow-scheduler", check=False)
            except Exception:
                pass
        failure_report = {
            "schema_version": "fotmob-deploy-v2",
            "generated_at": _now(),
            "passed": False,
            "activation_state": "failed",
            "project": args.project,
            "compose_file": str(compose_file),
            "release_root": str(release_root),
            "evidence_dir": str(evidence_dir),
            "git_sha": sha,
            "deployment_id": deployment_id,
            "error": f"{type(exc).__name__}: {exc}",
        }
        try:
            _atomic_json(report_path, failure_report)
        except Exception:
            # Preserve the original admission failure.  main() makes a second
            # best-effort report write, while all schedulers are already safe.
            pass
        raise


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report_path = args.report or args.evidence_dir / "deployment.json"
    try:
        report = deploy(args)
    except Exception as exc:
        report = {
            "schema_version": "fotmob-deploy-v2",
            "generated_at": _now(),
            "passed": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
    _atomic_json(report_path, report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report.get("passed") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
