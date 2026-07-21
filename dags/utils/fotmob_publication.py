"""Durable cross-stack publication fence for the FotMob generation.

The isolated and shared Airflow stacks have different metadata databases, so
Airflow task state is not a publication contract.  This module binds one
FotMob generation to the exact data interval, deployed Git revision and
schedule owner, then stores its state in the shared PostgreSQL ControlStore.

Only ``ready`` generations may be claimed by the master.  Bronze and Silver
writers hold a transactional writer guard and are rejected as soon as the
generation leaves ``writing`` -- including a retry by the same lock owner.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import socket
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence


logger = logging.getLogger(__name__)

FOTMOB_PUBLICATION_SOURCE = "fotmob"
FOTMOB_PUBLICATION_SCHEMA = "fotmob-publication-v1"
FOTMOB_RUNTIME_FINGERPRINT_ENV = "FOTMOB_DEPLOY_GIT_SHA"
FOTMOB_PUBLICATION_LOCK_TTL_SECONDS = 14 * 24 * 60 * 60
FOTMOB_PUBLICATION_XCOM_KEY = "fotmob_publication"
FOTMOB_PUBLICATION_CONF_KEY = "fotmob_publication"
FOTMOB_PUBLICATION_OWNERS = frozenset({"shared", "isolated"})
FOTMOB_PUBLICATION_BINDING_FIELDS = (
    "schema",
    "source",
    "owner",
    "data_interval_start",
    "data_interval_end",
    "runtime_fingerprint",
)

# The scheduled native run is intentionally narrower than the complete
# historical FotMob catalog.  Issue #930 admitted an immutable 158-scope
# cutover artifact; the daily workload derives its competition cohort from
# those exact bytes, but discovers each competition's selected/latest season
# dynamically.  This avoids freezing season strings while preventing an
# accidental ~493-competition crawl from entering the 14:00 publication slot.
FOTMOB_DAILY_CONTRACT_SCHEMA = "fotmob-daily-v1"
FOTMOB_DAILY_SCOPE_FILE = "/opt/airflow/configs/fotmob/issue-930-scopes.txt"
FOTMOB_DAILY_SCOPE_SHA256 = (
    "f1d95f916c78ed80e5784e2cd5bda7263cece37d9fde6d52fb2a1a4d9e97cb58"
)
FOTMOB_DAILY_SCOPE_COUNT = 158
FOTMOB_DAILY_COMPETITION_COUNT = 21
FOTMOB_DAILY_COMPETITION_IDS = (
    42,
    47,
    53,
    54,
    55,
    63,
    77,
    87,
    289,
    9333,
    9806,
    9807,
    9808,
    9809,
    10557,
    10558,
    10608,
    10611,
    10717,
    10718,
    10719,
)
FOTMOB_DAILY_COMPETITION_IDS_SHA256 = (
    "664f972d5d86002131293bcc8da8382f6b7378cd43a8bd37a247c321decf689a"
)
FOTMOB_DAILY_ENTITIES = (
    "season",
    "leaderboards",
    "matches",
    "teams",
    "players",
    "transfers",
)
FOTMOB_DAILY_MAX_REQUESTS = 10_000
FOTMOB_DAILY_MAX_DIRECT_MIB = 512
FOTMOB_DAILY_REQUESTS_PER_MINUTE = 60

FOTMOB_ISOLATED_STACK_ENV = "FOTMOB_ISOLATED_STACK"
FOTMOB_DEPLOYMENT_ID_ENV = "FOTMOB_DEPLOYMENT_ID"
FOTMOB_DEPLOYMENT_REPORT_PATH_ENV = "FOTMOB_DEPLOYMENT_REPORT_PATH"
FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH_ENV = "FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH"
FOTMOB_SHARED_EVIDENCE_ROOT = "/opt/airflow/fotmob-admission"
FOTMOB_ISOLATED_DAILY_DAG_ID = "dag_trigger_fotmob_daily"
FOTMOB_SHARED_MASTER_DAG_ID = "dag_master_pipeline"
FOTMOB_EXPECTED_ISOLATED_DAGS = frozenset(
    {
        "dag_ingest_fotmob",
        "dag_transform_fotmob_silver",
        FOTMOB_ISOLATED_DAILY_DAG_ID,
    }
)
FOTMOB_ISOLATED_RUNTIME_ROOTS = {
    "dags": "/opt/airflow/dags",
    "scrapers": "/opt/airflow/scrapers",
    "scripts": "/opt/airflow/scripts",
    "configs/medallion": "/opt/airflow/configs/medallion",
    "configs/fotmob": "/opt/airflow/configs/fotmob",
}
FOTMOB_SHARED_RUNTIME_ROOTS = dict(FOTMOB_ISOLATED_RUNTIME_ROOTS)
FOTMOB_ISOLATED_RUNTIME_SUFFIXES = (
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
FOTMOB_ISOLATED_REQUIRED_RUNTIME_PATHS = frozenset(
    {
        "configs/fotmob/competitions.json",
        "configs/fotmob/issue-930-scopes.txt",
        "dags/.airflowignore",
        "dags/dag_ingest_fotmob.py",
        "dags/dag_transform_fotmob_silver.py",
        "dags/dag_trigger_fotmob_daily.py",
        "dags/scripts/run_fotmob_scraper.py",
        "dags/utils/fotmob_publication.py",
        "scrapers/fotmob/repository.py",
        "scrapers/fotmob/service.py",
    }
)
FOTMOB_SHARED_REQUIRED_RUNTIME_PATHS = frozenset(
    {
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
)
FOTMOB_ISSUE930_WRITER_ENTITIES = frozenset(
    {"season", "leaderboards", "matches", "teams", "players"}
)
FOTMOB_ISSUE930_MODE_PARITY = {"backfill": 0, "replay": 1}

_FULL_GIT_SHA_RE = re.compile(r"[0-9a-f]{40}")
_SCOPE_LINE_RE = re.compile(r"([1-9][0-9]*)=(\S+)")


def _competition_ids_digest(competition_ids: Sequence[int]) -> str:
    material = "".join(f"{int(value)}\n" for value in competition_ids).encode("ascii")
    return hashlib.sha256(material).hexdigest()


def load_fotmob_daily_competition_contract(
    scope_file: str | os.PathLike[str],
    *,
    scope_sha256: str,
    competition_ids_sha256: str,
) -> dict[str, Any]:
    """Verify the immutable #930 artifact and derive its daily ID cohort.

    The season strings are evidence for the one-time cutover only.  Returning
    just the competition IDs lets the daily planner follow source-selected
    seasons without expanding to the complete historical catalog.
    """

    path = Path(scope_file)
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"cannot read FotMob daily scope artifact: {path}") from exc
    observed_scope_sha256 = hashlib.sha256(raw).hexdigest()
    normalized_scope_sha256 = str(scope_sha256 or "").strip().casefold()
    if normalized_scope_sha256 != FOTMOB_DAILY_SCOPE_SHA256:
        raise ValueError("FotMob daily scope SHA is not the approved #930 SHA")
    if observed_scope_sha256 != normalized_scope_sha256:
        raise ValueError("FotMob daily scope artifact bytes differ from approved SHA")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("FotMob daily scope artifact must be UTF-8") from exc
    if not text.endswith("\n") or "\r" in text:
        raise ValueError("FotMob daily scope artifact is not canonical LF text")

    scopes: list[str] = []
    competition_ids: set[int] = set()
    for line_number, line in enumerate(text.splitlines(), start=1):
        match = _SCOPE_LINE_RE.fullmatch(line)
        if match is None:
            raise ValueError(
                f"invalid FotMob daily scope at line {line_number}: {line!r}"
            )
        scopes.append(line)
        competition_ids.add(int(match.group(1)))
    if len(scopes) != FOTMOB_DAILY_SCOPE_COUNT or len(set(scopes)) != len(scopes):
        raise ValueError(
            "FotMob daily scope artifact must contain exactly 158 unique scopes"
        )

    ordered_ids = tuple(sorted(competition_ids))
    normalized_ids_sha256 = str(competition_ids_sha256 or "").strip().casefold()
    observed_ids_sha256 = _competition_ids_digest(ordered_ids)
    if normalized_ids_sha256 != FOTMOB_DAILY_COMPETITION_IDS_SHA256:
        raise ValueError("FotMob daily competition ID SHA is not approved")
    if (
        len(ordered_ids) != FOTMOB_DAILY_COMPETITION_COUNT
        or ordered_ids != FOTMOB_DAILY_COMPETITION_IDS
        or observed_ids_sha256 != normalized_ids_sha256
    ):
        raise ValueError("FotMob daily competition cohort differs from issue #930")
    return {
        "schema": FOTMOB_DAILY_CONTRACT_SCHEMA,
        "scope_file": str(path),
        "scope_sha256": observed_scope_sha256,
        "scope_count": len(scopes),
        "competition_ids": list(ordered_ids),
        "competition_ids_sha256": observed_ids_sha256,
        "competition_count": len(ordered_ids),
    }


def fotmob_daily_trigger_conf() -> dict[str, Any]:
    """Return the one admitted all-entity production daily profile."""

    return {
        "mode": "daily",
        "scope": "",
        "daily_contract": FOTMOB_DAILY_CONTRACT_SCHEMA,
        "competition_scope_file": FOTMOB_DAILY_SCOPE_FILE,
        "competition_scope_sha256": FOTMOB_DAILY_SCOPE_SHA256,
        "competition_ids_sha256": FOTMOB_DAILY_COMPETITION_IDS_SHA256,
        "entities": ",".join(FOTMOB_DAILY_ENTITIES),
        "max_requests": FOTMOB_DAILY_MAX_REQUESTS,
        "max_direct_mib": FOTMOB_DAILY_MAX_DIRECT_MIB,
        "competition_limit": 0,
        "season_limit": 0,
        "requests_per_minute": FOTMOB_DAILY_REQUESTS_PER_MINUTE,
    }


def _airflow_exception(message: str) -> Exception:
    try:
        from airflow.exceptions import AirflowException
    except ImportError:  # pragma: no cover - CLI can run without Airflow
        return RuntimeError(message)
    return AirflowException(message)


def isolated_runtime_manifest(
    *,
    roots: Mapping[str, str | os.PathLike[str]] | None = None,
) -> dict[str, str]:
    """Hash the exact effective source/config inventory visible to a task."""

    selected_roots = roots or FOTMOB_ISOLATED_RUNTIME_ROOTS
    required = set(FOTMOB_ISOLATED_REQUIRED_RUNTIME_PATHS)
    manifest: dict[str, str] = {}
    for prefix, raw_root in selected_roots.items():
        root = Path(raw_root)
        if not root.is_dir() or root.is_symlink():
            raise _airflow_exception(
                f"FotMob isolated runtime root is invalid: {prefix}"
            )
        for path in sorted(root.rglob("*")):
            if path.is_symlink():
                raise _airflow_exception(
                    f"FotMob isolated runtime rejects symlink: {prefix}"
                )
            if (
                not path.is_file()
                or "__pycache__" in path.parts
                or (
                    path.name != ".airflowignore"
                    and not path.name.endswith(FOTMOB_ISOLATED_RUNTIME_SUFFIXES)
                )
            ):
                continue
            key = f"{prefix}/{path.relative_to(root).as_posix()}"
            manifest[key] = hashlib.sha256(path.read_bytes()).hexdigest()
    missing = required - set(manifest)
    if missing:
        raise _airflow_exception(
            f"FotMob isolated runtime misses required files: {sorted(missing)!r}"
        )
    return dict(sorted(manifest.items()))


def shared_runtime_manifest(
    *,
    roots: Mapping[str, str | os.PathLike[str]] | None = None,
) -> dict[str, str]:
    """Hash the exact source/config inventory mounted in the shared stack."""

    selected_roots = roots or FOTMOB_SHARED_RUNTIME_ROOTS
    manifest: dict[str, str] = {}
    for prefix, raw_root in selected_roots.items():
        root = Path(raw_root)
        if not root.is_dir() or root.is_symlink():
            raise _airflow_exception(f"FotMob shared runtime root is invalid: {prefix}")
        for path in sorted(root.rglob("*")):
            if path.is_symlink():
                raise _airflow_exception(
                    f"FotMob shared runtime rejects symlink: {prefix}"
                )
            if (
                path.is_file()
                and "__pycache__" not in path.parts
                and (
                    path.name == ".airflowignore"
                    or path.name.endswith(FOTMOB_ISOLATED_RUNTIME_SUFFIXES)
                )
            ):
                key = f"{prefix}/{path.relative_to(root).as_posix()}"
                manifest[key] = hashlib.sha256(path.read_bytes()).hexdigest()
    missing = set(FOTMOB_SHARED_REQUIRED_RUNTIME_PATHS) - set(manifest)
    if missing:
        raise _airflow_exception(
            f"FotMob shared runtime misses required files: {sorted(missing)!r}"
        )
    if manifest.get("configs/fotmob/issue-930-scopes.txt") != (
        FOTMOB_DAILY_SCOPE_SHA256
    ):
        raise _airflow_exception(
            "FotMob shared runtime has an unapproved issue-930 scope artifact"
        )
    return dict(sorted(manifest.items()))


def _runtime_manifest_digest(manifest: Mapping[str, str]) -> str:
    payload = json.dumps(dict(manifest), sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(payload).hexdigest()


def _issue930_writer_identity_from_context(
    context: Mapping[str, Any],
) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None)
    if not isinstance(conf, Mapping):
        conf = {}
    return {
        "component": "airflow_task",
        "dag_id": getattr(dag_run, "dag_id", None),
        "run_id": getattr(dag_run, "run_id", None),
        "mode": conf.get("mode"),
        "scopes": conf.get("scope"),
        "entities": conf.get("entities"),
        "competition_limit": conf.get("competition_limit"),
        "season_limit": conf.get("season_limit"),
        "publication": conf.get(FOTMOB_PUBLICATION_CONF_KEY),
    }


def _validate_issue930_kept_paused_writer(
    report: Mapping[str, Any], identity: Mapping[str, Any]
) -> dict[str, Any]:
    """Accept only the coordinator's exact replay/backfill writer namespace."""

    try:
        raw_publication = identity.get("publication")
        raw_binding = (
            raw_publication.get("binding")
            if isinstance(raw_publication, Mapping)
            else None
        )
        if not isinstance(raw_binding, Mapping):
            raise ValueError("missing publication binding")
        binding = make_publication_binding(
            owner=raw_binding.get("owner"),
            data_interval_start=raw_binding.get("data_interval_start"),
            data_interval_end=raw_binding.get("data_interval_end"),
            fingerprint=raw_binding.get("runtime_fingerprint"),
        )
        supplied_generation = str(uuid.UUID(str(raw_publication.get("generation_id"))))
        if (
            dict(raw_binding) != binding
            or supplied_generation != make_generation_id(binding)
            or binding["runtime_fingerprint"] != report.get("git_sha")
        ):
            raise ValueError("publication identity differs")
        publication = {
            "generation_id": supplied_generation,
            "binding": binding,
        }
        generated_at = datetime.fromisoformat(
            str(report.get("generated_at", "")).replace("Z", "+00:00")
        ).astimezone(timezone.utc)
        start = datetime.fromisoformat(
            publication["binding"]["data_interval_start"]
        ).astimezone(timezone.utc)
        end = datetime.fromisoformat(
            publication["binding"]["data_interval_end"]
        ).astimezone(timezone.utc)
    except (KeyError, TypeError, ValueError) as exc:
        raise _airflow_exception(
            "FotMob kept-paused writer has invalid lifecycle identity"
        ) from exc
    offset = (start - generated_at).total_seconds() - 86_400
    if end - start != timedelta(seconds=1) or offset < 0 or not offset.is_integer():
        raise _airflow_exception(
            "FotMob kept-paused writer is outside the coordinator namespace"
        )
    slot = int(offset)
    inferred_mode = "backfill" if slot % 2 == 0 else "replay"
    attempt = slot // 2 + 1
    mode = str(identity.get("mode") or inferred_mode).strip().casefold()
    if (
        mode not in FOTMOB_ISSUE930_MODE_PARITY
        or FOTMOB_ISSUE930_MODE_PARITY[mode] != slot % 2
        or attempt <= 0
    ):
        raise _airflow_exception(
            "FotMob kept-paused writer mode/attempt identity differs"
        )

    generation_id = publication["generation_id"]
    compact_generation = generation_id.replace("-", "")
    component = str(identity.get("component") or "")
    dag_id = str(identity.get("dag_id") or "")
    run_id = str(identity.get("run_id") or "")
    if component == "airflow_task" and dag_id == "dag_transform_fotmob_silver":
        if run_id != f"fotmob_silver__{generation_id}":
            raise _airflow_exception("FotMob kept-paused Silver run identity differs")
    elif (
        component == "airflow_task" and dag_id == "dag_ingest_fotmob"
    ) or component == "bronze_runner":
        if component == "airflow_task" and run_id != (
            f"issue930_{mode}_a{attempt}__{compact_generation}"
        ):
            raise _airflow_exception("FotMob kept-paused ingest run identity differs")
        raw_scopes = identity.get("scopes")
        raw_scope_items = (
            [item.strip() for item in raw_scopes.split(",")]
            if isinstance(raw_scopes, str)
            else list(raw_scopes or ())
        )
        scope_items = (
            raw_scope_items
            if all(isinstance(item, str) for item in raw_scope_items)
            else []
        )
        scope_bytes = ("\n".join(scope_items) + "\n").encode("utf-8")
        raw_entities = identity.get("entities")
        entities = {
            item.strip().casefold()
            for item in (
                raw_entities.split(",")
                if isinstance(raw_entities, str)
                else raw_entities or ()
            )
            if str(item).strip()
        }
        try:
            competition_limit = int(identity.get("competition_limit", -1))
            season_limit = int(identity.get("season_limit", -1))
        except (TypeError, ValueError):
            competition_limit = season_limit = -1
        if (
            len(scope_items) != FOTMOB_DAILY_SCOPE_COUNT
            or len(set(scope_items)) != FOTMOB_DAILY_SCOPE_COUNT
            or any(_SCOPE_LINE_RE.fullmatch(str(item)) is None for item in scope_items)
            or hashlib.sha256(scope_bytes).hexdigest() != FOTMOB_DAILY_SCOPE_SHA256
            or entities != FOTMOB_ISSUE930_WRITER_ENTITIES
            or competition_limit != 0
            or season_limit != 0
        ):
            raise _airflow_exception(
                "FotMob kept-paused ingest scope/entity contract differs"
            )
    else:
        raise _airflow_exception(
            "FotMob kept-paused writer component is not coordinator-owned"
        )
    return {"mode": mode, "attempt": attempt, "generation_id": generation_id}


def attest_fotmob_isolated_runtime(
    *,
    report_path: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
    hostname: str | None = None,
    roots: Mapping[str, str | os.PathLike[str]] | None = None,
    require_scheduled_owner: bool = True,
    allow_kept_paused_writer: bool = False,
    writer_identity: Mapping[str, Any] | None = None,
    **context: Any,
) -> dict[str, Any]:
    """Fail closed unless this task still runs the byte-exact admitted stack.

    Only identity and digests are returned.  Credential values are neither
    copied from the environment nor included in errors or task evidence.
    """

    runtime_env = os.environ if environ is None else environ
    configured_path = str(
        runtime_env.get(FOTMOB_DEPLOYMENT_REPORT_PATH_ENV, "")
    ).strip()
    selected_path = Path(report_path or configured_path)
    if (
        not configured_path
        or not selected_path.is_absolute()
        or str(selected_path) != configured_path
    ):
        raise _airflow_exception(
            "FotMob isolated deployment report path is not exactly configured"
        )
    try:
        report = json.loads(selected_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise _airflow_exception(
            "FotMob isolated deployment report is unavailable or invalid"
        ) from exc
    if not isinstance(report, Mapping):
        raise _airflow_exception("FotMob isolated deployment report is not an object")
    report_unpaused = report.get("unpaused")
    report_paused = report.get("paused")
    common_admission = (
        report.get("schema_version") == "fotmob-deploy-v2"
        and report.get("passed") is True
        and isinstance(report_unpaused, list)
        and isinstance(report_paused, list)
        and all(isinstance(dag_id, str) for dag_id in report_unpaused)
        and all(isinstance(dag_id, str) for dag_id in report_paused)
    )
    active_admission = (
        common_admission
        and report.get("activation_state") == "active"
        and report.get("kept_paused") is False
        and report_paused == []
        and set(report_unpaused) == FOTMOB_EXPECTED_ISOLATED_DAGS
    )
    kept_paused_admission = (
        common_admission
        and report.get("activation_state") == "kept_paused"
        and report.get("kept_paused") is True
        and set(report_paused) == FOTMOB_EXPECTED_ISOLATED_DAGS
        and report_unpaused == []
    )
    if not active_admission and not (
        kept_paused_admission and allow_kept_paused_writer
    ):
        raise _airflow_exception(
            "FotMob scheduled runtime has no completed active deployment admission"
        )
    if report.get("container_report_path") != configured_path:
        raise _airflow_exception("FotMob deployment report path identity differs")

    deployment_id = str(runtime_env.get(FOTMOB_DEPLOYMENT_ID_ENV, "")).strip()
    git_sha = str(runtime_env.get(FOTMOB_RUNTIME_FINGERPRINT_ENV, "")).strip()
    if (
        runtime_env.get(FOTMOB_ISOLATED_STACK_ENV) != "1"
        or re.fullmatch(r"[0-9a-f]{32}", deployment_id) is None
        or report.get("deployment_id") != deployment_id
        or report.get("git_sha") != runtime_fingerprint(git_sha)
    ):
        raise _airflow_exception("FotMob isolated runtime identity differs from report")

    container_id = str(report.get("scheduler_container_id", "")).strip()
    image_id = str(report.get("resolved_image_id", "")).strip()
    observed_hostname = str(hostname or socket.gethostname()).strip().casefold()
    if (
        re.fullmatch(r"[0-9a-f]{64}", container_id) is None
        or re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None
        or re.fullmatch(r"[0-9a-f]{12,64}", observed_hostname) is None
        or not container_id.startswith(observed_hostname)
    ):
        raise _airflow_exception(
            "FotMob task container identity differs from deployment report"
        )

    if require_scheduled_owner:
        dag_run = context.get("dag_run")
        dag_id = getattr(dag_run, "dag_id", None)
        run_type = getattr(dag_run, "run_type", None)
        normalized_run_type = str(getattr(run_type, "value", run_type) or "").casefold()
        if dag_id != FOTMOB_ISOLATED_DAILY_DAG_ID or normalized_run_type != "scheduled":
            raise _airflow_exception(
                "FotMob daily producer requires an exact scheduled DagRun"
            )
    lifecycle = None
    if kept_paused_admission:
        lifecycle = _validate_issue930_kept_paused_writer(
            report,
            writer_identity or _issue930_writer_identity_from_context(context),
        )

    expected_manifest = report.get("isolated_runtime_sha256")
    if not isinstance(expected_manifest, Mapping) or any(
        not isinstance(path, str) or re.fullmatch(r"[0-9a-f]{64}", str(digest)) is None
        for path, digest in expected_manifest.items()
    ):
        raise _airflow_exception(
            "FotMob deployment report has no valid isolated runtime manifest"
        )
    observed_manifest = isolated_runtime_manifest(roots=roots)
    if observed_manifest != dict(expected_manifest):
        raise _airflow_exception(
            "FotMob isolated runtime bytes differ from deployment report"
        )
    result = {
        "deployment_id": deployment_id,
        "git_sha": git_sha,
        "scheduler_container_id": container_id,
        "scheduler_image_id": image_id,
        "runtime_file_count": len(observed_manifest),
        "runtime_manifest_sha256": _runtime_manifest_digest(observed_manifest),
    }
    if lifecycle is not None:
        result["issue930_lifecycle"] = lifecycle
    return result


def attest_fotmob_shared_runtime(
    *,
    report_path: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
    hostname: str | None = None,
    roots: Mapping[str, str | os.PathLike[str]] | None = None,
    require_scheduled_owner: bool = True,
    **context: Any,
) -> dict[str, Any]:
    """Re-attest the shared host bind mounts against isolated admission.

    The isolated deploy report is also the trust certificate for the shared
    fallback: deploy generated it only after proving the exact shared
    scheduler container, Git SHA, ControlStore migration and full bind-mount
    manifest.  The shared compose mounts that evidence directory read-only.
    A fallback writer therefore cannot rely on the mutable SHA environment
    label alone.
    """

    runtime_env = os.environ if environ is None else environ
    configured_path = str(
        runtime_env.get(FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH_ENV, "")
    ).strip()
    selected_path = Path(report_path or configured_path)
    if (
        not configured_path
        or not selected_path.is_absolute()
        or str(selected_path) != configured_path
    ):
        raise _airflow_exception(
            "FotMob shared deployment report path is not exactly configured"
        )
    try:
        report = json.loads(selected_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise _airflow_exception(
            "FotMob shared deployment report is unavailable or invalid"
        ) from exc
    if not isinstance(report, Mapping):
        raise _airflow_exception("FotMob shared deployment report is not an object")

    activation_state = report.get("activation_state")
    paused = report.get("paused")
    unpaused = report.get("unpaused")
    kept_paused_admission = (
        activation_state == "kept_paused"
        and report.get("kept_paused") is True
        and isinstance(paused, list)
        and set(paused) == FOTMOB_EXPECTED_ISOLATED_DAGS
        and unpaused == []
    )
    deployment_id = str(report.get("deployment_id", "")).strip()
    git_sha = str(runtime_env.get(FOTMOB_RUNTIME_FINGERPRINT_ENV, "")).strip()
    if (
        report.get("schema_version") != "fotmob-deploy-v2"
        or report.get("passed") is not True
        or not kept_paused_admission
        or re.fullmatch(r"[0-9a-f]{32}", deployment_id) is None
        or report.get("git_sha") != runtime_fingerprint(git_sha)
        or runtime_env.get(FOTMOB_ISOLATED_STACK_ENV) not in {None, ""}
    ):
        raise _airflow_exception(
            "FotMob shared runtime has no completed deployment admission"
        )

    control = report.get("control_database")
    initial = report.get("shared_handoff_initial")
    final = report.get("shared_handoff_final")
    if (
        not isinstance(control, Mapping)
        or control.get("same_runtime_configuration") is not True
        or not isinstance(control.get("shared"), Mapping)
        or not isinstance(initial, Mapping)
        or not isinstance(final, Mapping)
    ):
        raise _airflow_exception(
            "FotMob shared deployment report has no control-plane admission"
        )
    container_id = str(final.get("shared_scheduler_container", "")).strip()
    expected_manifest = final.get("runtime_code_sha256")
    initial_mount = initial.get("shared_admission_mount")
    final_mount = final.get("shared_admission_mount")
    expected_mount = {
        "type": "bind",
        "source": (
            str(initial_mount.get("source", "")).strip()
            if isinstance(initial_mount, Mapping)
            else ""
        ),
        "destination": FOTMOB_SHARED_EVIDENCE_ROOT,
        "read_only": True,
        "report_path": configured_path,
    }
    if (
        initial.get("passed") is not True
        or final.get("passed") is not True
        or initial.get("schedule_owner") != "isolated"
        or final.get("schedule_owner") != "isolated"
        or initial.get("runtime_git_sha") != git_sha
        or final.get("runtime_git_sha") != git_sha
        or initial.get("shared_scheduler_container") != container_id
        or initial.get("runtime_code_sha256") != expected_manifest
        or initial.get("control_database") != control["shared"]
        or final.get("control_database") != control["shared"]
        or report.get("shared_container_report_path") != configured_path
        or not isinstance(initial_mount, Mapping)
        or dict(initial_mount) != expected_mount
        or not Path(expected_mount["source"]).is_absolute()
        or not isinstance(final_mount, Mapping)
        or dict(final_mount) != expected_mount
        or re.fullmatch(r"[0-9a-f]{64}", container_id) is None
        or not isinstance(expected_manifest, Mapping)
        or any(
            not isinstance(path, str)
            or re.fullmatch(r"[0-9a-f]{64}", str(digest)) is None
            for path, digest in expected_manifest.items()
        )
    ):
        raise _airflow_exception(
            "FotMob shared deployment report has no exact handoff identity"
        )
    observed_hostname = str(hostname or socket.gethostname()).strip().casefold()
    if re.fullmatch(
        r"[0-9a-f]{12,64}", observed_hostname
    ) is None or not container_id.startswith(observed_hostname):
        raise _airflow_exception(
            "FotMob shared task container differs from deployment report"
        )
    control_uri = str(runtime_env.get("FBREF_CONTROL_DB_URI", "")).strip()
    if not control_uri or "airflow-metadb" in control_uri.casefold():
        raise _airflow_exception(
            "FotMob shared runtime has no production ControlStore binding"
        )

    if require_scheduled_owner:
        dag_run = context.get("dag_run")
        dag_id = getattr(dag_run, "dag_id", None)
        run_type = getattr(dag_run, "run_type", None)
        normalized_run_type = str(getattr(run_type, "value", run_type) or "").casefold()
        if dag_id != FOTMOB_SHARED_MASTER_DAG_ID or normalized_run_type != (
            "scheduled"
        ):
            raise _airflow_exception(
                "FotMob shared producer requires an exact scheduled master DagRun"
            )

    observed_manifest = shared_runtime_manifest(roots=roots)
    if observed_manifest != dict(expected_manifest):
        raise _airflow_exception(
            "FotMob shared runtime bytes differ from deployment report"
        )
    return {
        "deployment_id": deployment_id,
        "git_sha": git_sha,
        "shared_scheduler_container_id": container_id,
        "runtime_file_count": len(observed_manifest),
        "runtime_manifest_sha256": _runtime_manifest_digest(observed_manifest),
        "control_database_bound": True,
    }


def runtime_fingerprint(value: Any = None) -> str:
    """Return the exact deployed Git SHA; abbreviations fail closed."""

    raw = os.environ.get(FOTMOB_RUNTIME_FINGERPRINT_ENV, "") if value is None else value
    normalized = str(raw or "").strip().casefold()
    if _FULL_GIT_SHA_RE.fullmatch(normalized) is None:
        raise _airflow_exception(
            f"{FOTMOB_RUNTIME_FINGERPRINT_ENV} must be an exact 40-hex Git SHA"
        )
    return normalized


def _canonical_instant(value: Any, *, field: str) -> str:
    if isinstance(value, str):
        candidate = value.strip()
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise _airflow_exception(f"{field} must be an ISO-8601 instant") from exc
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise _airflow_exception(f"{field} is required")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _airflow_exception(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")


def make_publication_binding(
    *,
    owner: Any,
    data_interval_start: Any,
    data_interval_end: Any,
    fingerprint: Any = None,
) -> dict[str, str]:
    """Normalize the complete immutable identity of one publication."""

    normalized_owner = str(owner or "").strip().casefold()
    if normalized_owner not in FOTMOB_PUBLICATION_OWNERS:
        raise _airflow_exception(
            "FotMob publication owner must be exactly 'shared' or 'isolated'"
        )
    start = _canonical_instant(data_interval_start, field="data_interval_start")
    end = _canonical_instant(data_interval_end, field="data_interval_end")
    if end <= start:
        raise _airflow_exception("FotMob data interval end must be after start")
    return {
        "schema": FOTMOB_PUBLICATION_SCHEMA,
        "source": FOTMOB_PUBLICATION_SOURCE,
        "owner": normalized_owner,
        "data_interval_start": start,
        "data_interval_end": end,
        "runtime_fingerprint": runtime_fingerprint(fingerprint),
    }


def make_generation_id(binding: Mapping[str, Any]) -> str:
    """Return a retry-safe UUID for an exact normalized binding."""

    normalized = make_publication_binding(
        owner=binding.get("owner"),
        data_interval_start=binding.get("data_interval_start"),
        data_interval_end=binding.get("data_interval_end"),
        fingerprint=binding.get("runtime_fingerprint"),
    )
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"fotmob-publication:{payload}"))


def _context_interval(context: Mapping[str, Any]) -> tuple[Any, Any]:
    start = context.get("data_interval_start")
    end = context.get("data_interval_end")
    dag_run = context.get("dag_run")
    if start is None and dag_run is not None:
        start = getattr(dag_run, "data_interval_start", None)
    if end is None and dag_run is not None:
        end = getattr(dag_run, "data_interval_end", None)
    if start is None or end is None:
        raise _airflow_exception("FotMob publication requires an exact data interval")
    return start, end


def expected_publication(owner: str, context: Mapping[str, Any]) -> dict[str, Any]:
    start, end = _context_interval(context)
    binding = make_publication_binding(
        owner=owner,
        data_interval_start=start,
        data_interval_end=end,
    )
    return {"generation_id": make_generation_id(binding), "binding": binding}


def _control_store():
    from scrapers.fbref.control import ControlStore

    return ControlStore.from_env()


def initialize_fotmob_publication(
    *, publication_owner: str, **context: Any
) -> dict[str, Any]:
    """Atomically create/acquire the exact generation before any write."""

    normalized_owner = str(publication_owner or "").strip().casefold()
    if normalized_owner == "isolated":
        attest_fotmob_isolated_runtime(**context)
    elif normalized_owner == "shared":
        attest_fotmob_shared_runtime(**context)
    publication = expected_publication(publication_owner, context)
    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, "dag_id", None) or context.get("dag_id")
    if dag_id is None:
        task = context.get("task")
        dag_id = getattr(getattr(task, "dag", None), "dag_id", None)
    if not dag_id:
        raise _airflow_exception("FotMob publication initializer has no dag_id")
    state = _control_store().initialize_publication_generation(
        publication["generation_id"],
        dag_id=str(dag_id),
        binding=publication["binding"],
        source=FOTMOB_PUBLICATION_SOURCE,
        ttl_seconds=FOTMOB_PUBLICATION_LOCK_TTL_SECONDS,
    )
    publication["state"] = state
    task_instance = context.get("ti")
    if task_instance is not None:
        task_instance.xcom_push(
            key=FOTMOB_PUBLICATION_XCOM_KEY,
            value=publication,
        )
    return publication


def _dag_run_conf(context: Mapping[str, Any]) -> Mapping[str, Any]:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) if dag_run is not None else None
    if not isinstance(conf, Mapping):
        conf = context.get("dag_run_conf")
    return conf if isinstance(conf, Mapping) else {}


def publication_from_payload(raw: Any) -> dict[str, Any]:
    """Validate and cryptographically re-derive one publication payload."""

    if not isinstance(raw, Mapping):
        raise _airflow_exception(
            f"FotMob publication payload requires {FOTMOB_PUBLICATION_CONF_KEY}"
        )
    raw_binding = raw.get("binding")
    if not isinstance(raw_binding, Mapping):
        raise _airflow_exception("FotMob publication binding is missing")
    binding = make_publication_binding(
        owner=raw_binding.get("owner"),
        data_interval_start=raw_binding.get("data_interval_start"),
        data_interval_end=raw_binding.get("data_interval_end"),
        fingerprint=raw_binding.get("runtime_fingerprint"),
    )
    if dict(raw_binding) != binding:
        raise _airflow_exception("FotMob publication binding is not canonical")
    # Every component must run exactly the same release as the owner.  This
    # prevents an old scheduler from accepting a new stack's generation.
    if binding["runtime_fingerprint"] != runtime_fingerprint():
        raise _airflow_exception("FotMob publication runtime fingerprint mismatch")
    expected_id = make_generation_id(binding)
    try:
        supplied_id = str(uuid.UUID(str(raw.get("generation_id"))))
    except (AttributeError, TypeError, ValueError) as exc:
        raise _airflow_exception("FotMob generation_id must be a UUID") from exc
    if supplied_id != expected_id:
        raise _airflow_exception("FotMob generation_id does not match its binding")
    return {"generation_id": supplied_id, "binding": binding}


def publication_from_context(context: Mapping[str, Any]) -> dict[str, Any]:
    """Read and cryptographically re-derive the parent-bound generation."""

    raw = _dag_run_conf(context).get(FOTMOB_PUBLICATION_CONF_KEY)
    if raw is None:
        raise _airflow_exception(f"DagRun conf requires {FOTMOB_PUBLICATION_CONF_KEY}")
    return publication_from_payload(raw)


def fotmob_consumer_trigger_conf(
    publication_owner: str,
    *,
    sensor_task_id: str = "wait_for_fotmob_publication",
) -> dict[str, Any]:
    """Build the complete templated binding required by every child reader."""

    normalized_owner = str(publication_owner or "").strip()
    normalized_sensor = str(sensor_task_id or "").strip()
    if not normalized_owner or not normalized_sensor:
        raise ValueError("FotMob consumer parent and sensor task ids are required")
    xcom = (
        "{{ ti.xcom_pull(task_ids='"
        + normalized_sensor
        + "', key='"
        + FOTMOB_PUBLICATION_XCOM_KEY
        + "')"
    )
    return {
        "publication_owner": normalized_owner,
        "master_run_id": "{{ run_id }}",
        FOTMOB_PUBLICATION_CONF_KEY: {
            "generation_id": xcom + "['generation_id'] }}",
            "binding": {
                field: xcom + "['binding']['" + field + "'] }}"
                for field in FOTMOB_PUBLICATION_BINDING_FIELDS
            },
        },
    }


def _attest_fotmob_writer_runtime(
    publication: Mapping[str, Any], context: Mapping[str, Any]
) -> dict[str, Any]:
    """Attest the exact owner runtime at one writer-guard boundary."""

    if publication["binding"]["owner"] == "isolated":
        return attest_fotmob_isolated_runtime(
            require_scheduled_owner=False,
            allow_kept_paused_writer=True,
            **dict(context),
        )
    return attest_fotmob_shared_runtime(
        require_scheduled_owner=False,
        **dict(context),
    )


@contextmanager
def fotmob_publication_writer(
    context: Mapping[str, Any],
) -> Iterator[dict[str, Any]]:
    """Hold the DB guard and attest before and after one Silver mutation."""

    publication = publication_from_context(context)
    _attest_fotmob_writer_runtime(publication, context)
    with _control_store().guard_publication_writer(
        publication["generation_id"], source=FOTMOB_PUBLICATION_SOURCE
    ):
        try:
            yield publication
        finally:
            # Bind-mount read-only mode does not prevent host-side replacement.
            # Re-hash while the generation guard is still held so drift makes
            # this task fail and the candidate can never be sealed/published.
            _attest_fotmob_writer_runtime(publication, context)


def validate_fotmob_writer_fence(**context: Any) -> dict[str, Any]:
    """Cheap preflight; the actual writer also holds the guard throughout."""

    with fotmob_publication_writer(context) as publication:
        return publication


def _normalize_candidate_value(value: Any) -> Any:
    try:
        json.dumps(value, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise _airflow_exception("FotMob candidate evidence is not JSON-safe") from exc
    return value


def record_fotmob_silver_candidate(
    *, transform_task_ids: Sequence[str], **context: Any
) -> dict[str, Any]:
    """Record an immutable candidate only after every transform and both DQs."""

    publication = publication_from_context(context)
    task_instance = context.get("ti")
    if task_instance is None:
        raise _airflow_exception("FotMob candidate task has no task instance")
    expected_ids = tuple(sorted(str(task_id) for task_id in transform_task_ids))
    if len(expected_ids) != len(set(expected_ids)) or not expected_ids:
        raise _airflow_exception("FotMob candidate transform set is invalid")
    transform_results: dict[str, Any] = {}
    for task_id in expected_ids:
        result = task_instance.xcom_pull(task_ids=task_id)
        if not isinstance(result, Mapping) or result.get("status") != "success":
            raise _airflow_exception(
                f"FotMob transform {task_id!r} has no successful result"
            )
        transform_results[task_id] = _normalize_candidate_value(dict(result))
    row_gate = task_instance.xcom_pull(task_ids="validate_silver")
    quality_gate = task_instance.xcom_pull(task_ids="validate_silver_quality")
    if not isinstance(row_gate, Mapping) or row_gate.get("warnings"):
        raise _airflow_exception("FotMob Silver row-count evidence is not clean")
    if not isinstance(quality_gate, Mapping) or quality_gate.get("errors"):
        raise _airflow_exception("FotMob Silver quality evidence is not clean")
    evidence = {
        "schema": FOTMOB_PUBLICATION_SCHEMA,
        "generation_id": publication["generation_id"],
        "transform_task_ids": list(expected_ids),
        "transform_results": transform_results,
        "row_count_gate": _normalize_candidate_value(dict(row_gate)),
        "quality_gate": _normalize_candidate_value(dict(quality_gate)),
    }
    evidence["digest"] = hashlib.sha256(
        json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    # ``record_publication_candidate`` locks and validates the same
    # generation row in its transaction; opening a second writer transaction
    # here would self-deadlock on PostgreSQL's row lock.
    _control_store().record_publication_candidate(
        publication["generation_id"],
        evidence,
        source=FOTMOB_PUBLICATION_SOURCE,
    )
    return evidence


def seal_fotmob_publication(**context: Any) -> dict[str, Any]:
    """Move ``writing`` to ``ready`` only after the Silver child succeeded."""

    publication = publication_from_context(context)
    return _control_store().seal_publication_generation(
        publication["generation_id"],
        source=FOTMOB_PUBLICATION_SOURCE,
        ttl_seconds=FOTMOB_PUBLICATION_LOCK_TTL_SECONDS,
    )


def _task_states(context: Mapping[str, Any]) -> dict[str, str]:
    dag_run = context.get("dag_run")
    instances = dag_run.get_task_instances() if dag_run is not None else []
    result: dict[str, str] = {}
    for instance in instances:
        state = getattr(instance, "state", None)
        state = getattr(state, "value", state)
        result[str(getattr(instance, "task_id", ""))] = (
            str(state or "missing").casefold().split(".")[-1]
        )
    return result


def fail_unsealed_fotmob_publication(
    *,
    success_task_id: str,
    writer_task_ids: Sequence[str] = (),
    publication_owner: str | None = None,
    **context: Any,
) -> dict[str, Any]:
    """Terminal producer cleanup without ever masking a failed DagRun."""

    publication = (
        publication_from_context(context)
        if publication_owner is None
        else expected_publication(publication_owner, context)
    )
    states = _task_states(context)
    success_state = states.get(success_task_id, "missing")
    if success_state == "success":
        return {"status": "ready", "generation_id": publication["generation_id"]}
    writer_states = {
        task_id: states.get(task_id, "missing") for task_id in writer_task_ids
    }
    # A terminal TriggerDagRunOperator state does not prove that its exact
    # child DagRun stopped writing (timeout/lost response is ambiguous).  Keep
    # every failed producer lock until explicit operator recovery or expiry.
    state = _control_store().fail_publication_generation(
        publication["generation_id"],
        safe_to_release=False,
        source=FOTMOB_PUBLICATION_SOURCE,
    )
    raise _airflow_exception(
        "FotMob generation did not reach ready; "
        "lock retained because child-writer state is ambiguous"
        f" (success_task={success_state}, writers={writer_states}, state={state})"
    )


def _consumer(context: Mapping[str, Any]) -> dict[str, str]:
    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, "dag_id", None) or context.get("dag_id")
    run_id = getattr(dag_run, "run_id", None) or context.get("run_id")
    if not dag_id or not run_id:
        raise _airflow_exception("FotMob publication consumer identity is missing")
    return {"dag_id": str(dag_id), "run_id": str(run_id)}


def wait_and_claim_fotmob_publication(
    *, publication_owner: str, **context: Any
) -> bool:
    """PythonSensor poke: wait for exact ``ready`` then claim atomically."""

    publication = expected_publication(publication_owner, context)
    state = _control_store().get_publication_generation(
        publication["generation_id"], source=FOTMOB_PUBLICATION_SOURCE
    )
    if state is None:
        logger.info(
            "FotMob generation %s is not created yet", publication["generation_id"]
        )
        return False
    if state.get("binding") != publication["binding"]:
        raise _airflow_exception("FotMob generation binding differs from master")
    phase = str(state.get("phase") or "").casefold()
    status = str(state.get("status") or "").casefold()
    if phase == "writing" and status in {"pending", "running"}:
        return False
    if phase not in {"ready", "consuming"}:
        raise _airflow_exception(
            f"FotMob generation is terminal or invalid: phase={phase!r}, status={status!r}"
        )
    consumer = _consumer(context)
    claimed = _control_store().claim_publication_generation(
        publication["generation_id"],
        consumer=consumer,
        binding=publication["binding"],
        source=FOTMOB_PUBLICATION_SOURCE,
        ttl_seconds=FOTMOB_PUBLICATION_LOCK_TTL_SECONDS,
    )
    publication["state"] = claimed
    task_instance = context.get("ti")
    if task_instance is not None:
        task_instance.xcom_push(
            key=FOTMOB_PUBLICATION_XCOM_KEY,
            value=publication,
        )
    return True


def validate_fotmob_consumer_fence(**context: Any) -> dict[str, Any]:
    """Fail unless this child belongs to the exact active claiming parent."""

    publication = publication_from_context(context)
    conf = _dag_run_conf(context)
    parent_dag_id = str(conf.get("publication_owner") or "").strip()
    parent_run_id = str(conf.get("master_run_id") or "").strip()
    if not parent_dag_id or not parent_run_id:
        raise _airflow_exception(
            "FotMob consumer requires publication_owner and master_run_id"
        )
    expected_consumer = {
        "dag_id": parent_dag_id,
        "run_id": parent_run_id,
    }
    state = _control_store().get_publication_generation(
        publication["generation_id"], source=FOTMOB_PUBLICATION_SOURCE
    )
    if state is None:
        raise _airflow_exception("FotMob consumer generation does not exist")
    violations = []
    if state.get("binding") != publication["binding"]:
        violations.append("binding mismatch")
    if str(state.get("status") or "").casefold() != "succeeded":
        violations.append(f"status={state.get('status')!r}")
    if str(state.get("phase") or "").casefold() != "consuming":
        violations.append(f"phase={state.get('phase')!r}")
    if not bool(state.get("active")):
        violations.append("publication lock is inactive")
    if state.get("consumer") != expected_consumer:
        violations.append("consumer identity mismatch")
    if violations:
        raise _airflow_exception(
            "FotMob consumer fence rejected child: " + "; ".join(violations)
        )
    return {
        "generation_id": publication["generation_id"],
        "binding": publication["binding"],
        "consumer": expected_consumer,
        "phase": "consuming",
        "active": True,
    }


def finalize_fotmob_publication_consumer(
    *,
    publication_owner: str,
    report_task_id: str,
    sensor_task_id: str,
    release_unclaimed_ready_on_failure: bool = True,
    **context: Any,
) -> dict[str, Any]:
    """Publish+release after the consumer, or fail without an unsafe unlock."""

    publication = expected_publication(publication_owner, context)
    store = _control_store()
    state = store.get_publication_generation(
        publication["generation_id"], source=FOTMOB_PUBLICATION_SOURCE
    )
    states = _task_states(context)
    report_state = states.get(report_task_id, "missing")
    sensor_state = states.get(sensor_task_id, "missing")
    if report_state == "success" and sensor_state == "success":
        return store.complete_publication_generation(
            publication["generation_id"],
            consumer=_consumer(context),
            published=True,
            source=FOTMOB_PUBLICATION_SOURCE,
        )

    phase = str((state or {}).get("phase") or "").casefold()
    if phase == "ready" and release_unclaimed_ready_on_failure:
        # No consumer was admitted, so this release cannot race a downstream
        # write.  The failed master verdict is still preserved by the raise.
        store.complete_publication_generation(
            publication["generation_id"],
            consumer=None,
            published=False,
            source=FOTMOB_PUBLICATION_SOURCE,
        )
        disposition = "unclaimed ready generation released"
    elif phase == "ready":
        disposition = "unclaimed ready generation lock retained"
    elif phase == "writing":
        store.fail_publication_generation(
            publication["generation_id"],
            safe_to_release=False,
            source=FOTMOB_PUBLICATION_SOURCE,
        )
        disposition = (
            "lock retained because a failed parent does not prove its child "
            "writer terminal"
        )
    else:
        # Once consuming begins, downstream tables may already contain part of
        # this generation.  Retaining the lock is the only fail-closed action.
        disposition = f"lock retained in phase {phase or 'missing'}"
    raise _airflow_exception(
        "FotMob consumer publication did not succeed; "
        f"{disposition} (sensor={sensor_state}, report={report_state})"
    )
