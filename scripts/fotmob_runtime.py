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
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))


TRINO_ENV_KEYS = (
    "TRINO_HOST",
    "TRINO_PORT",
    "TRINO_USER",
    "TRINO_PASSWORD",
    "TRINO_HTTP_SCHEME",
    "TRINO_TLS_VERIFY",
)
PURGE_RAW_ENV_KEYS = (
    "FOTMOB_RAW_STORE_URI",
    "FOTMOB_RAW_S3_ENDPOINT",
    "FOTMOB_RAW_S3_SCHEME",
    "FOTMOB_RAW_S3_REGION",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
)
PROJECTION_SOURCES = {
    "dag_orchestrate_fotmob.py": "dags/dag_orchestrate_fotmob.py",
    "dag_ingest_fotmob.py": "dags/dag_ingest_fotmob.py",
    "dag_transform_fotmob_silver.py": "dags/dag_transform_fotmob_silver.py",
    "dag_trigger_fotmob_daily.py": "dags/dag_trigger_fotmob_daily.py",
    "dag_refresh_fotmob.py": "dags/dag_refresh_fotmob.py",
    "dag_backfill_fotmob.py": "dags/dag_backfill_fotmob.py",
    ".airflowignore": "deploy/fotmob/.airflowignore",
}
PROJECTION_DIRECTORIES = {"utils", "sql", "scripts"}
CONTAINER_EVIDENCE_ROOT = Path("/opt/airflow/logs/fotmob")
SHARED_CONTAINER_EVIDENCE_ROOT = Path("/opt/airflow/fotmob-admission")
EXPECTED_DAGS = {
    "dag_orchestrate_fotmob",
    "dag_ingest_fotmob",
    "dag_transform_fotmob_silver",
    "dag_trigger_fotmob_daily",
    "dag_refresh_fotmob",
    "dag_backfill_fotmob",
}
SCHEDULE_BOUNDARY_FIELDS = (
    "logical_date",
    "data_interval_start",
    "data_interval_end",
    "run_after",
)
# Identity/ownership proof only: ``failed`` still means that the exact admitted
# scheduled interval exists and cannot be treated as safe to recreate.
EXACT_SCHEDULED_RUN_STATES = frozenset({"queued", "running", "success", "failed"})
SHARED_CONSUMER_DAG_ID = "dag_sofascore_pipeline"
ISOLATED_DAILY_DAG_ID = "dag_trigger_fotmob_daily"
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
    "dags/dag_orchestrate_fotmob.py",
    "dags/dag_ingest_fotmob.py",
    "dags/dag_transform_fotmob_silver.py",
    "dags/dag_trigger_fotmob_daily.py",
    "dags/dag_refresh_fotmob.py",
    "dags/dag_backfill_fotmob.py",
}
ISOLATED_DAG_PREFIXES = (
    "dags/scripts/",
    "dags/sql/",
    "dags/utils/",
)
ISOLATED_AIRFLOWIGNORE_PATH = "dags/.airflowignore"
SHARED_REQUIRED_RUNTIME_PATHS = {
    "configs/fotmob/competitions.json",
    "configs/fotmob/issue-930-player-source-refresh.json",
    "configs/fotmob/issue-930-scopes.txt",
    "dags/.airflowignore",
    "dags/dag_ingest_fotmob.py",
    "dags/dag_orchestrate_fotmob.py",
    "dags/dag_refresh_fotmob.py",
    "dags/dag_backfill_fotmob.py",
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
    "dags/utils/fotmob_orchestration.py",
    "dags/utils/maintenance_tasks.py",
    "dags/utils/silver_tasks.py",
    "dags/utils/xref_player_resolver.py",
    "scrapers/base/iceberg_writer.py",
    "scrapers/base/trino_manager.py",
    "scrapers/fbref/control/store.py",
    "scrapers/fotmob/constants.py",
    "scrapers/fotmob/catalog.py",
    "scrapers/fotmob/catalog_contract.py",
    "scrapers/fotmob/domain.py",
    "scrapers/fotmob/raw_store.py",
    "scrapers/fotmob/repository.py",
    "scrapers/fotmob/service.py",
    "scrapers/fotmob/scope_codec.py",
    "scrapers/fotmob/source_refresh.py",
    "scrapers/fotmob/transport.py",
}
MASTER_RUNTIME_PATH = "dags/dag_master_pipeline.py"
APPROVED_SCOPE_PATH = "configs/fotmob/issue-930-scopes.txt"
APPROVED_SCOPE_SHA256 = (
    "f1d95f916c78ed80e5784e2cd5bda7263cece37d9fde6d52fb2a1a4d9e97cb58"
)
PLAYER_SOURCE_REFRESH_PATH = "configs/fotmob/issue-930-player-source-refresh.json"
PLAYER_SOURCE_REFRESH_SHA256 = (
    "f6cb854c6d60463c899fd9077b61a71d8d0f817741c3a9d6423925b32949045b"
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
    "dag_refresh_fotmob",
    "dag_backfill_fotmob",
    "dag_orchestrate_fotmob",
}
EXPECTED_SHARED_PAUSE_STATES = {
    "dag_master_pipeline": True,
    "dag_sofascore_pipeline": True,
    "dag_ingest_fotmob": True,
    "dag_transform_fotmob_silver": True,
}
SHARED_MAINTENANCE_DAGS = {
    "dag_iceberg_maintenance",
    "dag_iceberg_maintenance_daily",
}
DESTRUCTIVE_SHARED_STATE_DAGS = SHARED_STATE_DAGS | SHARED_MAINTENANCE_DAGS
DESTRUCTIVE_SHARED_PAUSE_STATES = {
    **EXPECTED_SHARED_PAUSE_STATES,
    **{dag_id: True for dag_id in SHARED_MAINTENANCE_DAGS},
}

AUTOMATIC_ADMISSION_SCHEMA = "fotmob-automatic-admission-v1"
AUTOMATIC_ROLLOUT_SCHEMA = "fotmob-automatic-rollout-v1"
COORDINATOR_ROLLOUT_SCHEMA = "fotmob-coordinator-rollout-v1"
AUTOMATIC_CONTRACT_SCHEMA = "fotmob-catalog-v1"
AUTOMATIC_CLASSIFIER_VERSION = "fotmob-men-v1"
SCOPE_OBSERVATIONS_TABLE = "fotmob_competition_scope_observations"
SCOPE_OBSERVATIONS_CURRENT_VIEW = (
    "fotmob_competition_scope_observations_current"
)
LEGACY_OWNER_DAGS = frozenset(
    {
        "dag_trigger_fotmob_daily",
        "dag_refresh_fotmob",
        "dag_backfill_fotmob",
    }
)
AUTOMATIC_ACTIVE_DAGS = frozenset(
    {
        "dag_orchestrate_fotmob",
        "dag_ingest_fotmob",
        "dag_transform_fotmob_silver",
    }
)
AUTOMATIC_DAGBAG_DAGS = AUTOMATIC_ACTIVE_DAGS | LEGACY_OWNER_DAGS
AUTOMATIC_EXPECTED_SCHEDULES = {
    "dag_orchestrate_fotmob": "*/5 * * * *",
    "dag_ingest_fotmob": "None",
    "dag_transform_fotmob_silver": "None",
    "dag_trigger_fotmob_daily": "None",
    "dag_refresh_fotmob": "None",
    "dag_backfill_fotmob": "None",
}
AUTOMATIC_LANES = frozenset({"daily", "refresh", "backfill"})
AUTOMATIC_ADMISSION_MAX_AGE = timedelta(minutes=15)
AUTOMATIC_ADMISSION_FUTURE_TOLERANCE = timedelta(minutes=1)
AUTOMATIC_DAILY_ENTITIES = (
    "leaderboards",
    "matches",
    "players",
    "season",
    "teams",
    "transfers",
)
AUTOMATIC_DAILY_ENTITY_POLICY = {
    "match_policy": "finished_only",
    "leaderboard_policy": "all_advertised",
    "team_policy": "global_observed_snapshot",
    "player_policy": "global_observed_snapshot",
    "transfer_policy": {
        "window": "1year",
        "pagination": "unique_hits",
        "completion_scope": "included_ids",
        "completion_signature": "catalog_contract",
    },
}
AUTOMATIC_DAILY_MAX_REQUESTS = 10_000
AUTOMATIC_DAILY_MAX_DIRECT_BYTES = 512 * 1024 * 1024
AUTOMATIC_DAILY_REQUESTS_PER_MINUTE = 60


class RuntimeBindingError(RuntimeError):
    pass


def _automatic_id_digest(values: Any, *, label: str) -> tuple[int, str]:
    if not isinstance(values, list) or any(
        type(value) is not int or value <= 0 for value in values
    ):
        raise RuntimeBindingError(f"automatic {label} IDs are invalid")
    if values != sorted(set(values)):
        raise RuntimeBindingError(f"automatic {label} IDs are not canonical")
    raw = "".join(f"{value}\n" for value in values).encode("ascii")
    return len(values), hashlib.sha256(raw).hexdigest()


AUTOMATIC_DECISION_DIGEST_FIELDS = (
    "competition_id",
    "decision",
    "reason",
    "policy_rule",
    "classifier_version",
    "probe_status",
    "profile_content_hash",
)


def automatic_decision_digest(values: Any) -> tuple[int, str]:
    """Hash the durable decision fields shared by runner and current view."""

    if not isinstance(values, list) or any(not isinstance(item, Mapping) for item in values):
        raise RuntimeBindingError("automatic decision evidence is invalid")
    normalized = [
        {field: item.get(field) for field in AUTOMATIC_DECISION_DIGEST_FIELDS}
        for item in values
    ]
    if any(type(item["competition_id"]) is not int for item in normalized):
        raise RuntimeBindingError("automatic decision IDs are invalid")
    if [item["competition_id"] for item in normalized] != sorted(
        {item["competition_id"] for item in normalized}
    ):
        raise RuntimeBindingError("automatic decision evidence is not canonical")
    raw = json.dumps(
        normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return len(normalized), hashlib.sha256(raw).hexdigest()


def validate_automatic_catalog_admission(
    value: Mapping[str, Any], *, now: datetime | None = None
) -> dict[str, Any]:
    """Validate the durable gate used before enabling the automatic owner.

    The scope-observation view proves that structural decisions can be read
    after a restart.  Contract hashes come from the full runner report instead
    of per-competition fingerprints, which legitimately differ by ID.
    """

    if not isinstance(value, Mapping):
        raise RuntimeBindingError("automatic admission must be an object")
    if value.get("schema_version") != AUTOMATIC_ADMISSION_SCHEMA:
        raise RuntimeBindingError("automatic admission schema is unsupported")
    if value.get("classifier_version") != AUTOMATIC_CLASSIFIER_VERSION:
        raise RuntimeBindingError("automatic classifier version drifted")
    if value.get("contract_schema") != AUTOMATIC_CONTRACT_SCHEMA:
        raise RuntimeBindingError("automatic contract schema drifted")
    validated_at = _timestamp(value.get("validated_at"))
    checked_at = now or datetime.now(timezone.utc)
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise RuntimeBindingError("automatic admission check time has no timezone")
    checked_at = checked_at.astimezone(timezone.utc)
    if validated_at > checked_at + AUTOMATIC_ADMISSION_FUTURE_TOLERANCE:
        raise RuntimeBindingError("automatic admission evidence is from the future")
    if checked_at - validated_at > AUTOMATIC_ADMISSION_MAX_AGE:
        raise RuntimeBindingError("automatic admission evidence is stale")

    writer_snapshot = value.get("writer_snapshot")
    if not isinstance(writer_snapshot, Mapping):
        raise RuntimeBindingError("automatic atomic writer snapshot is missing")
    snapshot_at = _timestamp(writer_snapshot.get("observed_at"))
    if snapshot_at > validated_at or (
        validated_at - snapshot_at > AUTOMATIC_ADMISSION_FUTURE_TOLERANCE
    ):
        raise RuntimeBindingError("automatic writer snapshot is not the cutover edge")
    pause_states = writer_snapshot.get("pause_states")
    if (
        writer_snapshot.get("schema_version") != "fotmob-writer-snapshot-v1"
        or re.fullmatch(
            r"[0-9a-f]{32}", str(writer_snapshot.get("transaction_id") or "")
        )
        is None
        or not isinstance(pause_states, Mapping)
        or set(pause_states) != AUTOMATIC_DAGBAG_DAGS
        or any(pause_states.get(dag_id) is not True for dag_id in pause_states)
        or writer_snapshot.get("active_runs") != {}
    ):
        raise RuntimeBindingError(
            "automatic writer snapshot is not one atomic paused six-DAG view"
        )

    legacy = value.get("legacy_owners")
    if not isinstance(legacy, Mapping) or set(legacy) != LEGACY_OWNER_DAGS:
        raise RuntimeBindingError("automatic legacy owner evidence is incomplete")
    for dag_id in sorted(LEGACY_OWNER_DAGS):
        state = legacy.get(dag_id)
        if (
            not isinstance(state, Mapping)
            or state.get("schedule") is not None
            or state.get("is_paused") is not True
        ):
            raise RuntimeBindingError(
                f"automatic legacy owner {dag_id} is scheduled or unpaused"
            )

    lane_budgets = value.get("lane_budgets")
    if not isinstance(lane_budgets, Mapping) or set(lane_budgets) != AUTOMATIC_LANES:
        raise RuntimeBindingError("automatic lane budget evidence is incomplete")
    for lane in sorted(AUTOMATIC_LANES):
        budget = lane_budgets.get(lane)
        if (
            not isinstance(budget, Mapping)
            or type(budget.get("max_proxy_mib")) is not int
            or budget.get("max_proxy_mib") != 0
            or (
                "max_proxy_bytes" in budget
                and (
                    type(budget.get("max_proxy_bytes")) is not int
                    or budget.get("max_proxy_bytes") != 0
                )
            )
        ):
            raise RuntimeBindingError(
                f"automatic {lane} proxy budget must be exactly zero"
            )

    active_writers = value.get("active_writers")
    if not isinstance(active_writers, list) or any(
        not isinstance(item, Mapping) for item in active_writers
    ):
        raise RuntimeBindingError("automatic active writer evidence is invalid")
    if active_writers:
        raise RuntimeBindingError("automatic admission found an active writer")

    reports = value.get("current_run_reports")
    if (
        not isinstance(reports, list)
        or len(reports) != 1
        or any(not isinstance(item, Mapping) for item in reports)
    ):
        raise RuntimeBindingError("automatic admission requires exactly one canary report")
    contract_identities: set[tuple[str, str, str, str]] = set()
    for index, report in enumerate(reports):
        selection = report.get("selection")
        if not isinstance(selection, Mapping):
            raise RuntimeBindingError(
                f"automatic current-run report {index} has no selection"
            )
        if (
            "daily_contract" in selection
            or "competition_scope" in selection
            or selection.get("catalog_contract") is None
        ):
            raise RuntimeBindingError(
                "automatic admission rejects legacy daily contract evidence"
            )
        if report.get("mode") != "daily" or selection.get("scope_lane") != "current":
            raise RuntimeBindingError(
                "automatic admission requires one daily/current canary report"
            )
        completed_at = _timestamp(report.get("completed_at"))
        if completed_at > validated_at + AUTOMATIC_ADMISSION_FUTURE_TOLERANCE:
            raise RuntimeBindingError("automatic canary completed after validation")
        if validated_at - completed_at > AUTOMATIC_ADMISSION_MAX_AGE:
            raise RuntimeBindingError("automatic canary report is stale")
        try:
            from scrapers.fotmob.catalog_contract import catalog_contract_from_dict

            contract = catalog_contract_from_dict(selection["catalog_contract"])
        except (TypeError, ValueError) as exc:
            raise RuntimeBindingError(
                f"automatic current-run contract {index} is invalid: {exc}"
            ) from exc
        if (
            contract.schema != AUTOMATIC_CONTRACT_SCHEMA
            or contract.classifier_version != AUTOMATIC_CLASSIFIER_VERSION
        ):
            raise RuntimeBindingError(
                "automatic current-run classifier/contract version drifted"
            )
        if (
            contract.entities != AUTOMATIC_DAILY_ENTITIES
            or contract.entity_policy != AUTOMATIC_DAILY_ENTITY_POLICY
        ):
            raise RuntimeBindingError(
                "automatic daily contract has an unsafe entity profile"
            )
        transport = report.get("transport")
        budget = report.get("budget")
        if (
            not isinstance(transport, Mapping)
            or not isinstance(budget, Mapping)
            or type(transport.get("proxy_bytes")) is not int
            or transport.get("proxy_bytes") != 0
            or type(budget.get("proxy_bytes")) is not int
            or budget.get("proxy_bytes") != 0
            or type(budget.get("max_proxy_bytes")) is not int
            or budget.get("max_proxy_bytes") != 0
        ):
            raise RuntimeBindingError(
                "automatic current-run proxy evidence must be exactly zero"
            )
        if (
            budget.get("max_requests") != AUTOMATIC_DAILY_MAX_REQUESTS
            or budget.get("max_direct_bytes") != AUTOMATIC_DAILY_MAX_DIRECT_BYTES
            or selection.get("requests_per_minute")
            != AUTOMATIC_DAILY_REQUESTS_PER_MINUTE
            or selection.get("competition_limit") != 0
            or selection.get("season_limit") != 0
            or selection.get("explicit_scopes") != []
        ):
            raise RuntimeBindingError(
                "automatic daily profile or budget differs from the admitted caps"
            )
        try:
            from scripts.fotmob_catalog_acceptance import validate_report

            acceptance = validate_report(report, now=checked_at)
        except (AttributeError, KeyError, TypeError, ValueError) as exc:
            raise RuntimeBindingError(
                f"automatic current-run report {index} cannot be validated: {exc}"
            ) from exc
        if not acceptance.ok:
            raise RuntimeBindingError(
                "automatic current-run report failed dynamic acceptance: "
                + "; ".join(acceptance.errors)
            )
        contract_identities.add(
            (
                contract.catalog_content_hash,
                contract.included_ids_sha256,
                contract.scope_sha256,
                contract.plan_signature,
            )
        )
        catalog_count, catalog_ids_sha256 = _automatic_id_digest(
            selection.get("catalog_ids"), label="catalog"
        )
        decisions = selection.get("catalog_decisions")
        if not isinstance(decisions, list) or any(
            not isinstance(item, Mapping) for item in decisions
        ):
            raise RuntimeBindingError("automatic catalog decisions are invalid")
        decision_ids = [item.get("competition_id") for item in decisions]
        decision_count, decision_ids_sha256 = _automatic_id_digest(
            decision_ids, label="decision"
        )
        _decision_count, decision_evidence_sha256 = automatic_decision_digest(
            decisions
        )
        if decision_ids != selection.get("catalog_ids"):
            raise RuntimeBindingError(
                "automatic decisions do not cover the exact catalog IDs"
            )

        observation = value.get("scope_observations")
        expected_observation = {
            "table": SCOPE_OBSERVATIONS_TABLE,
            "current_view": SCOPE_OBSERVATIONS_CURRENT_VIEW,
            "table_exists": True,
            "current_view_exists": True,
            "snapshot_run_id": report.get("run_id"),
            "catalog_batch_id": contract.catalog_batch_id,
            "catalog_content_hash": contract.catalog_content_hash,
            "catalog_id_count": catalog_count,
            "catalog_ids_sha256": catalog_ids_sha256,
            "decision_count": decision_count,
            "decision_ids_sha256": decision_ids_sha256,
            "decision_evidence_sha256": decision_evidence_sha256,
            "duplicate_decision_count": 0,
            "classifier_version": AUTOMATIC_CLASSIFIER_VERSION,
            "included_id_count": contract.included_count,
            "included_ids_sha256": contract.included_ids_sha256,
        }
        if not isinstance(observation, Mapping) or any(
            observation.get(key) != expected
            for key, expected in expected_observation.items()
        ):
            raise RuntimeBindingError(
                "automatic durable scope-observation snapshot differs from canary"
            )
    if len(contract_identities) > 1:
        raise RuntimeBindingError(
            "automatic current-run evidence contains mixed catalog/selection hashes"
        )
    identity = next(iter(contract_identities), None)
    canary = value.get("canary")
    report = reports[0]
    final_publication = (
        canary.get("final_publication") if isinstance(canary, Mapping) else None
    )
    final_candidate = (
        final_publication.get("candidate")
        if isinstance(final_publication, Mapping)
        else None
    )
    transform_task_ids = (
        final_candidate.get("transform_task_ids")
        if isinstance(final_candidate, Mapping)
        else None
    )
    canary_publication = (
        canary.get("publication") if isinstance(canary, Mapping) else None
    )
    canary_binding = (
        canary_publication.get("binding")
        if isinstance(canary_publication, Mapping)
        else None
    )
    if (
        not isinstance(canary, Mapping)
        or canary.get("schema_version") != "fotmob-automatic-canary-v1"
        or re.fullmatch(r"[0-9a-f]{32}", str(canary.get("deployment_id") or ""))
        is None
        or re.fullmatch(r"[0-9a-f]{40}", str(canary.get("git_sha") or "")) is None
        or re.fullmatch(
            r"[0-9a-f]{64}", str(canary.get("scheduler_container_id") or "")
        )
        is None
        or str(canary.get("generation_id") or "") != str(report.get("run_id") or "")
        or canary.get("ingest_run_state") != "success"
        or canary.get("silver_run_state") != "success"
        or re.fullmatch(
            r"[0-9a-f]{64}", str(canary.get("candidate_digest") or "")
        )
        is None
        or not isinstance(canary_publication, Mapping)
        or canary_publication.get("generation_id") != canary.get("generation_id")
        or not isinstance(canary_binding, Mapping)
        or canary_binding.get("schema") != "fotmob-publication-v1"
        or canary_binding.get("source") != "fotmob"
        or canary_binding.get("owner") != "isolated"
        or canary_binding.get("runtime_fingerprint") != canary.get("git_sha")
        or re.fullmatch(
            r"[0-9a-f]{64}", str(canary.get("runner_report_sha256") or "")
        )
        is None
        or not isinstance(final_publication, Mapping)
        or final_publication.get("generation_id") != canary.get("generation_id")
        or final_publication.get("phase") != "abandoned"
        or final_publication.get("active") is not False
        or final_publication.get("released") is not True
        or final_publication.get("published") is not False
        or not isinstance(final_candidate, Mapping)
        or final_candidate.get("generation_id") != canary.get("generation_id")
        or final_candidate.get("digest") != canary.get("candidate_digest")
        or not isinstance(transform_task_ids, list)
        or not transform_task_ids
        or any(not isinstance(task_id, str) or not task_id for task_id in transform_task_ids)
        or transform_task_ids != sorted(set(transform_task_ids))
    ):
        raise RuntimeBindingError(
            "automatic canary is not bound to an abandoned exact publication"
        )
    return {
        "schema_version": AUTOMATIC_ADMISSION_SCHEMA,
        "classifier_version": AUTOMATIC_CLASSIFIER_VERSION,
        "contract_schema": AUTOMATIC_CONTRACT_SCHEMA,
        "validated_at": validated_at.isoformat(),
        "scope_observation_ready": True,
        "legacy_owners_paused": sorted(LEGACY_OWNER_DAGS),
        "active_writers": [],
        "proxy_budget_bytes": 0,
        "current_run_report_count": len(reports),
        "current_contract_identity": (
            None
            if identity is None
            else {
                "catalog_content_hash": identity[0],
                "included_ids_sha256": identity[1],
                "scope_sha256": identity[2],
                "plan_signature": identity[3],
            }
        ),
        "canary": {
            "deployment_id": canary["deployment_id"],
            "git_sha": canary["git_sha"],
            "scheduler_container_id": canary["scheduler_container_id"],
            "generation_id": canary["generation_id"],
            "runner_report_sha256": canary["runner_report_sha256"],
        },
        "passed": True,
    }


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
    if manifest[PLAYER_SOURCE_REFRESH_PATH] != PLAYER_SOURCE_REFRESH_SHA256:
        raise RuntimeBindingError(
            "issue-930 player source-refresh artifact differs from approved SHA-256"
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


def _normalize_schedule_boundary(raw: Any, *, label: str) -> dict[str, str]:
    if not isinstance(raw, Mapping) or set(raw) != set(SCHEDULE_BOUNDARY_FIELDS):
        raise RuntimeBindingError(f"{label} next scheduled interval is incomplete")
    parsed: dict[str, datetime] = {}
    for field in SCHEDULE_BOUNDARY_FIELDS:
        value = raw.get(field)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeBindingError(f"{label} {field} is missing")
        try:
            instant = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise RuntimeBindingError(
                f"{label} {field} is not an ISO-8601 instant"
            ) from exc
        if instant.tzinfo is None or instant.utcoffset() is None:
            raise RuntimeBindingError(f"{label} {field} has no timezone")
        parsed[field] = instant.astimezone(timezone.utc)
    if parsed["logical_date"] != parsed["data_interval_start"]:
        raise RuntimeBindingError(f"{label} logical date differs from interval start")
    if parsed["data_interval_start"] >= parsed["data_interval_end"]:
        raise RuntimeBindingError(f"{label} next scheduled interval is invalid")
    if parsed["run_after"] != parsed["data_interval_end"]:
        raise RuntimeBindingError(f"{label} run-after differs from interval end")
    return {
        field: parsed[field].isoformat(timespec="microseconds")
        for field in SCHEDULE_BOUNDARY_FIELDS
    }


def _scheduled_run_id(logical_date: Any) -> str:
    """Mirror Airflow 2.11 ``DagRunType.SCHEDULED.generate_run_id`` exactly."""

    # Airflow formats the timezone-aware logical datetime with default
    # ``datetime.isoformat()``: zero microseconds are omitted, non-zero values
    # are retained.  The admitted boundary is normalized to UTC first.
    return f"scheduled__{_timestamp(logical_date).isoformat()}"


def _validate_active_schedule_proof(
    payload: Mapping[str, Any], expected_boundary: Mapping[str, str]
) -> None:
    safety = payload.get("activation_safety_window")
    if (
        not isinstance(safety, Mapping)
        or safety.get("passed") is not True
        or not isinstance(safety.get("timeout_seconds"), int)
        or not isinstance(safety.get("required_seconds"), int)
        or not isinstance(safety.get("remaining_seconds"), int)
        or safety["required_seconds"] < max(15 * 60, safety["timeout_seconds"] + 5 * 60)
        or safety["remaining_seconds"] < safety["required_seconds"]
    ):
        raise RuntimeBindingError("active report has no valid schedule safety window")
    checked_at = _timestamp(safety.get("checked_at"))
    next_boundary = _timestamp(safety.get("next_boundary"))
    if next_boundary <= checked_at:
        raise RuntimeBindingError("active schedule safety window is inverted")

    activation = payload.get("scheduled_activation")
    if (
        not isinstance(activation, Mapping)
        or set(activation) != {"status", "producer", "consumer", "exact_identity_match"}
        or activation.get("status") != "proved"
        or activation.get("exact_identity_match") is not True
    ):
        raise RuntimeBindingError("active report has no exact scheduled handoff proof")
    normalized_runs: dict[str, dict[str, str]] = {}
    expected_dags = {
        "producer": ISOLATED_DAILY_DAG_ID,
        "consumer": SHARED_CONSUMER_DAG_ID,
    }
    expected_run_id = _scheduled_run_id(expected_boundary.get("logical_date"))
    for role, dag_id in expected_dags.items():
        row = activation.get(role)
        if not isinstance(row, Mapping) or set(row) != {
            "dag_id",
            "run_id",
            "run_type",
            "logical_date",
            "data_interval_start",
            "data_interval_end",
            "state",
        }:
            raise RuntimeBindingError(f"active {role} scheduled proof is incomplete")
        boundary = _normalize_schedule_boundary(
            {
                "logical_date": row.get("logical_date"),
                "data_interval_start": row.get("data_interval_start"),
                "data_interval_end": row.get("data_interval_end"),
                "run_after": row.get("data_interval_end"),
            },
            label=f"active {role}",
        )
        if (
            row.get("dag_id") != dag_id
            or row.get("run_type") != "scheduled"
            or row.get("run_id") != expected_run_id
            or str(row.get("state") or "").casefold() not in EXACT_SCHEDULED_RUN_STATES
            or boundary != dict(expected_boundary)
        ):
            raise RuntimeBindingError(f"active {role} does not match admitted interval")
        normalized_runs[role] = {
            "run_id": str(row["run_id"]),
            "run_type": str(row["run_type"]),
            **boundary,
        }
    identity_fields = (
        "run_id",
        "run_type",
        "logical_date",
        "data_interval_start",
        "data_interval_end",
    )
    if any(
        normalized_runs["producer"][field] != normalized_runs["consumer"][field]
        for field in identity_fields
    ):
        raise RuntimeBindingError(
            "active producer/consumer scheduled identities differ"
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

    _normalize_schedule_boundary(
        handoff.get("next_scheduled_interval"),
        label=f"shared {SHARED_CONSUMER_DAG_ID}",
    )

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


def validate_automatic_rollout_activation(
    payload: Mapping[str, Any],
    admission: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the ordered cutover ceremony carried by an active report."""

    rollout = payload.get("automatic_rollout")
    bootstrap = {
        "table": SCOPE_OBSERVATIONS_TABLE,
        "table_exists": True,
        "current_view": SCOPE_OBSERVATIONS_CURRENT_VIEW,
        "current_view_exists": True,
    }
    if (
        not isinstance(rollout, Mapping)
        or set(rollout)
        != {"schema_version", "phase", "scope_observation_bootstrap", "canary_report"}
        or rollout.get("schema_version") != AUTOMATIC_ROLLOUT_SCHEMA
        or rollout.get("phase") != "active"
        or rollout.get("scope_observation_bootstrap") != bootstrap
    ):
        raise RuntimeBindingError("active automatic rollout certificate is invalid")
    canary_path = Path(str(rollout.get("canary_report") or ""))
    evidence_dir = Path(str(payload.get("evidence_dir") or ""))
    try:
        canary_path.resolve().relative_to(evidence_dir.resolve())
    except (OSError, ValueError) as exc:
        raise RuntimeBindingError(
            "active automatic canary path is outside evidence directory"
        ) from exc

    activation = payload.get("automatic_activation")
    if not isinstance(activation, Mapping):
        raise RuntimeBindingError("active automatic cutover evidence is missing")
    required = {
        "fresh_shared_handoff",
        "daily_boundary_initial",
        "daily_boundary_commit",
        "quiescence_before",
        "live_canary",
        "children_transaction",
        "shared_consumer_unpaused",
        "shared_consumer_readback",
        "owner_unpaused_last",
    }
    if not required.issubset(activation):
        raise RuntimeBindingError("active automatic cutover evidence is incomplete")

    final_handoff = payload.get("shared_handoff_final")
    fresh_handoff = activation.get("fresh_shared_handoff")
    immutable_handoff_fields = {
        "shared_scheduler_container",
        "shared_admission_mount",
        "runtime_code_sha256",
        "runtime_git_sha",
        "control_database",
        "schedule_owner",
    }
    if (
        not isinstance(final_handoff, Mapping)
        or not isinstance(fresh_handoff, Mapping)
        or fresh_handoff.get("passed") is not True
        or any(
            fresh_handoff.get(field) != final_handoff.get(field)
            for field in immutable_handoff_fields
        )
    ):
        raise RuntimeBindingError("active automatic shared handoff differs")

    def daily_boundary(value: Any, *, label: str) -> dict[str, str]:
        expected_keys = {
            "schema_version",
            "checked_at",
            "selected_date",
            "state",
            "data_interval_start",
            "data_interval_end",
            "safe_start",
            "safe_cutoff",
            "passed",
        }
        if (
            not isinstance(value, Mapping)
            or set(value) != expected_keys
            or value.get("schema_version") != "fotmob-automatic-boundary-v1"
            or value.get("passed") is not True
            or value.get("state") not in {"future", "daily_window_open"}
        ):
            raise RuntimeBindingError(f"{label} automatic daily boundary is invalid")
        checked = _timestamp(value.get("checked_at"))
        start = _timestamp(value.get("data_interval_start"))
        end = _timestamp(value.get("data_interval_end"))
        cutoff = _timestamp(value.get("safe_cutoff"))
        safe_start = _timestamp(value.get("safe_start"))
        if (
            end - start != timedelta(days=1)
            or (end.hour, end.minute, end.second, end.microsecond) != (14, 0, 0, 0)
            or value.get("selected_date") != end.date().isoformat()
            or safe_start != end - timedelta(minutes=30)
            or cutoff != end + timedelta(minutes=45)
            or checked < safe_start
            or checked >= cutoff
        ):
            raise RuntimeBindingError(f"{label} automatic daily boundary differs")
        return {
            "checked_at": checked.isoformat(timespec="microseconds"),
            "data_interval_start": start.isoformat(timespec="microseconds"),
            "data_interval_end": end.isoformat(timespec="microseconds"),
            "safe_cutoff": cutoff.isoformat(timespec="microseconds"),
        }

    initial_boundary = daily_boundary(
        activation.get("daily_boundary_initial"), label="initial"
    )
    commit_boundary = daily_boundary(
        activation.get("daily_boundary_commit"), label="commit"
    )
    if (
        initial_boundary["data_interval_start"]
        != commit_boundary["data_interval_start"]
        or initial_boundary["data_interval_end"]
        != commit_boundary["data_interval_end"]
        or _timestamp(commit_boundary["checked_at"])
        < _timestamp(initial_boundary["checked_at"])
    ):
        raise RuntimeBindingError("automatic daily boundary changed during cutover")
    shared_boundary = _normalize_schedule_boundary(
        fresh_handoff.get("next_scheduled_interval"),
        label="automatic fresh shared handoff",
    )
    if (
        shared_boundary["logical_date"] != commit_boundary["data_interval_start"]
        or shared_boundary["data_interval_start"]
        != commit_boundary["data_interval_start"]
        or shared_boundary["data_interval_end"]
        != commit_boundary["data_interval_end"]
        or shared_boundary["run_after"] != commit_boundary["data_interval_end"]
    ):
        raise RuntimeBindingError(
            "automatic daily boundary differs from fresh shared interval"
        )

    all_paused = {dag_id: True for dag_id in AUTOMATIC_DAGBAG_DAGS}
    children_paused = {
        dag_id: dag_id
        not in {"dag_ingest_fotmob", "dag_transform_fotmob_silver"}
        for dag_id in AUTOMATIC_DAGBAG_DAGS
    }
    active_paused = {
        dag_id: dag_id in LEGACY_OWNER_DAGS for dag_id in AUTOMATIC_DAGBAG_DAGS
    }

    def isolated_tx(value: Any, *, phase: str, before: Mapping[str, bool], after: Mapping[str, bool]) -> datetime:
        scheduler_state = value.get("scheduler_state") if isinstance(value, Mapping) else None
        if (
            not isinstance(value, Mapping)
            or value.get("schema_version") != "fotmob-writer-snapshot-v1"
            or value.get("phase") != phase
            or value.get("pause_states") != dict(before)
            or value.get("pause_states_after") != dict(after)
            or value.get("active_runs") != {}
            or not isinstance(scheduler_state, Mapping)
            or set(scheduler_state)
            != {"next_background_lane", "daily_date", "generation", "updated_at"}
            or scheduler_state.get("next_background_lane") not in {"refresh", "backfill"}
            or scheduler_state.get("daily_date")
            == str((activation.get("daily_boundary_commit") or {}).get("selected_date"))
            or isinstance(scheduler_state.get("generation"), bool)
            or not isinstance(scheduler_state.get("generation"), int)
            or scheduler_state["generation"] < 0
            or re.fullmatch(r"[0-9a-f]{32}", str(value.get("transaction_id") or "")) is None
        ):
            raise RuntimeBindingError(f"automatic {phase} writer transaction differs")
        try:
            if scheduler_state.get("daily_date") is not None:
                parsed_daily = date.fromisoformat(str(scheduler_state["daily_date"]))
                if parsed_daily.isoformat() != scheduler_state["daily_date"]:
                    raise ValueError("non-canonical daily date")
            _timestamp(scheduler_state.get("updated_at"))
        except (TypeError, ValueError, RuntimeBindingError) as exc:
            raise RuntimeBindingError(
                f"automatic {phase} scheduler state is malformed"
            ) from exc
        return _timestamp(value.get("observed_at"))

    children_at = isolated_tx(
        activation.get("children_transaction"),
        phase="children",
        before=all_paused,
        after=children_paused,
    )
    shared_before = dict(EXPECTED_SHARED_PAUSE_STATES)
    shared_after = dict(shared_before)
    shared_after[SHARED_CONSUMER_DAG_ID] = False

    def shared_tx(value: Any, *, phase: str, expected_before: Mapping[str, bool]) -> datetime:
        if (
            not isinstance(value, Mapping)
            or value.get("schema_version")
            != "fotmob-shared-consumer-snapshot-v1"
            or value.get("phase") != phase
            or value.get("pause_states_before") != dict(expected_before)
            or value.get("pause_states_after") != shared_after
            or value.get("schedule_owner") != "isolated"
            or not isinstance(value.get("active_runs"), list)
            or re.fullmatch(r"[0-9a-f]{32}", str(value.get("transaction_id") or "")) is None
        ):
            raise RuntimeBindingError(f"automatic shared {phase} transaction differs")
        if phase == "unpause" and value.get("active_runs") != []:
            raise RuntimeBindingError("automatic shared cutover was not idle")
        return _timestamp(value.get("observed_at"))

    readback_at = shared_tx(
        activation.get("shared_consumer_readback"),
        phase="inspect_unpaused",
        expected_before=shared_after,
    )

    def validate_shared_recovery() -> datetime:
        shared_readback = activation.get("shared_consumer_readback")
        shared_recovery = activation.get("shared_recovery")
        if not isinstance(shared_readback, Mapping) or not isinstance(
            shared_recovery, Mapping
        ):
            raise RuntimeBindingError(
                "automatic recovered owner has no shared recovery proof"
            )
        expected_stored_boundary = {
            "logical_date": commit_boundary["data_interval_start"],
            "data_interval_start": commit_boundary["data_interval_start"],
            "data_interval_end": commit_boundary["data_interval_end"],
            "run_after": commit_boundary["data_interval_end"],
        }
        try:
            stored_recovery_boundary = _normalize_schedule_boundary(
                shared_recovery.get("stored_boundary"),
                label="automatic recovered stored shared interval",
            )
            live_recovery_boundary = _normalize_schedule_boundary(
                shared_recovery.get("live_boundary"),
                label="automatic recovered live shared interval",
            )
        except RuntimeBindingError:
            raise
        if (
            shared_recovery.get("schema_version")
            != "fotmob-shared-recovery-v1"
            or shared_recovery.get("passed") is not True
            or stored_recovery_boundary != expected_stored_boundary
        ):
            raise RuntimeBindingError(
                "automatic recovered shared interval differs from commit"
            )
        recovery_mode = shared_recovery.get("mode")
        if recovery_mode == "idle_before_scheduled_run":
            if (
                shared_readback.get("active_runs") != []
                or live_recovery_boundary != stored_recovery_boundary
                or "consumer_run" in shared_recovery
            ):
                raise RuntimeBindingError(
                    "automatic idle shared recovery proof differs"
                )
        elif recovery_mode in {
            "scheduled_wait_sensor",
            "terminal_wait_sensor_failed",
        }:
            expected_run_id = _scheduled_run_id(
                stored_recovery_boundary["logical_date"]
            )
            active_runs = shared_readback.get("active_runs")
            consumer = shared_recovery.get("consumer_run")
            task_states = consumer.get("task_states") if isinstance(consumer, Mapping) else None
            downstream = {
                "trigger_xref_transforms",
                "trigger_e3_transforms",
                "trigger_e4_transforms",
                "finalize_fotmob_publication",
            }
            expected_tasks = downstream | {"wait_for_fotmob_publication"}
            observed_consumer_runs = shared_readback.get("consumer_runs")
            expected_active = {
                "dag_id": SHARED_CONSUMER_DAG_ID,
                "run_id": expected_run_id,
            }
            shifted = {
                key: (
                    _timestamp(value) + timedelta(days=1)
                ).isoformat(timespec="microseconds")
                for key, value in stored_recovery_boundary.items()
            }
            terminal = recovery_mode == "terminal_wait_sensor_failed"
            expected_run_state = "failed" if terminal else {"queued", "running"}
            expected_wait_state = (
                {"failed"}
                if terminal
                else {
                    "queued",
                    "running",
                    "scheduled",
                    "up_for_reschedule",
                    "deferred",
                }
            )
            terminal_downstream = {
                "trigger_xref_transforms": "upstream_failed",
                "trigger_e3_transforms": "upstream_failed",
                "trigger_e4_transforms": "upstream_failed",
                "finalize_fotmob_publication": "failed",
            }
            if (
                not isinstance(active_runs, list)
                or (
                    terminal
                    and active_runs != []
                )
                or (
                    not terminal
                    and (
                        len(active_runs) != 1
                        or any(
                            active_runs[0].get(key) != value
                            for key, value in expected_active.items()
                        )
                        or active_runs[0].get("state") not in expected_run_state
                    )
                )
                or not isinstance(observed_consumer_runs, list)
                or observed_consumer_runs != [consumer]
                or not isinstance(consumer, Mapping)
                or any(
                    consumer.get(key) != value
                    for key, value in expected_active.items()
                )
                or consumer.get("run_type") != "scheduled"
                or (
                    consumer.get("state") != expected_run_state
                    if terminal
                    else consumer.get("state") not in expected_run_state
                )
                or consumer.get("logical_date")
                != stored_recovery_boundary["logical_date"]
                or consumer.get("data_interval_start")
                != stored_recovery_boundary["data_interval_start"]
                or consumer.get("data_interval_end")
                != stored_recovery_boundary["data_interval_end"]
                or not isinstance(task_states, Mapping)
                or set(task_states) != expected_tasks
                or task_states.get("wait_for_fotmob_publication")
                not in expected_wait_state
                or (
                    terminal
                    and any(
                        task_states.get(task_id) != state
                        for task_id, state in terminal_downstream.items()
                    )
                )
                or (
                    not terminal
                    and any(
                        task_states.get(task_id) is not None
                        for task_id in downstream
                    )
                )
                or live_recovery_boundary != shifted
                or (
                    terminal
                    and (
                        shared_recovery.get("roll_forward") is not True
                        or shared_recovery.get("next_scheduled_boundary")
                        != live_recovery_boundary
                    )
                )
            ):
                raise RuntimeBindingError(
                    "automatic scheduled shared recovery is not wait-only"
                )
        else:
            raise RuntimeBindingError("automatic shared recovery mode is invalid")
        recovery_control = activation.get("control_quiescence_at_recovery")
        if (
            not isinstance(recovery_control, Mapping)
            or recovery_control.get("source") != "fotmob"
            or recovery_control.get("safe") is not True
            or recovery_control.get("active") is not False
        ):
            raise RuntimeBindingError(
                "automatic recovered owner has no current ControlStore proof"
            )
        recovery_boundary = daily_boundary(
            activation.get("daily_boundary_recovery"), label="recovery"
        )
        recovered_at = _timestamp(activation.get("recovered_at"))
        if (
            recovery_boundary["data_interval_start"]
            != commit_boundary["data_interval_start"]
            or recovery_boundary["data_interval_end"]
            != commit_boundary["data_interval_end"]
            or _timestamp(recovery_boundary["checked_at"])
            < _timestamp(commit_boundary["checked_at"])
            or recovered_at < _timestamp(commit_boundary["checked_at"])
        ):
            raise RuntimeBindingError(
                "automatic recovered daily boundary differs from commit"
            )
        return readback_at

    owner_value = activation.get("owner_transaction")
    shared_transition_value = activation.get("shared_consumer_transaction")
    if owner_value is not None:
        shared_at = (
            shared_tx(
                shared_transition_value,
                phase="unpause",
                expected_before=shared_before,
            )
            if shared_transition_value is not None
            else validate_shared_recovery()
        )
        owner_at = isolated_tx(
            owner_value,
            phase="owner",
            before=children_paused,
            after=active_paused,
        )
    else:
        recovery = activation.get("owner_recovery_snapshot")
        if (
            not isinstance(recovery, Mapping)
            or recovery.get("pause_states") != active_paused
            or recovery.get("atomic_metadata_snapshot") is not True
            or recovery.get("active_runs") != []
        ):
            raise RuntimeBindingError("automatic owner commit proof is missing")
        shared_at = validate_shared_recovery()
        owner_at = readback_at
    if not (children_at <= shared_at <= readback_at <= owner_at):
        raise RuntimeBindingError("automatic activation transaction order differs")

    quiescence = activation.get("quiescence_before")
    control = (
        activation.get("control_quiescence_at_commit")
        if shared_transition_value is not None
        else activation.get("control_quiescence_at_recovery")
    )
    live_canary = activation.get("live_canary")
    canary = admission.get("canary")
    if (
        not isinstance(quiescence, Mapping)
        or quiescence.get("safe") is not True
        or quiescence.get("active") is not False
        or not isinstance(control, Mapping)
        or control.get("source") != "fotmob"
        or control.get("safe") is not True
        or control.get("active") is not False
        or not isinstance(live_canary, Mapping)
        or not isinstance(canary, Mapping)
        or live_canary.get("runner_sha256") != canary.get("runner_report_sha256")
        or not isinstance(live_canary.get("runner_bytes"), int)
        or live_canary["runner_bytes"] <= 0
        or activation.get("shared_consumer_unpaused") is not True
        or activation.get("owner_unpaused_last") is not True
    ):
        raise RuntimeBindingError("automatic activation final evidence differs")
    return {
        "schema_version": AUTOMATIC_ROLLOUT_SCHEMA,
        "phase": "active",
        "daily_interval": {
            "start": commit_boundary["data_interval_start"],
            "end": commit_boundary["data_interval_end"],
        },
        "children_at": children_at.isoformat(),
        "shared_at": shared_at.isoformat(),
        "owner_at": owner_at.isoformat(),
        "recovered": activation.get("shared_recovery") is not None,
        "passed": True,
    }


def validate_pending_automatic_shared_wait(
    payload: Mapping[str, Any],
    admission: Mapping[str, Any],
) -> dict[str, Any]:
    """Admit only a wait-only Sofa sensor during the atomic cutover gap.

    This certificate deliberately grants no claim, finalizer, producer, or
    writer authority.  The sensor may only keep poking until the same report
    is atomically replaced by a fully validated active certificate.
    """

    rollout = payload.get("automatic_rollout")
    bootstrap = {
        "table": SCOPE_OBSERVATIONS_TABLE,
        "table_exists": True,
        "current_view": SCOPE_OBSERVATIONS_CURRENT_VIEW,
        "current_view_exists": True,
    }
    if (
        not isinstance(rollout, Mapping)
        or set(rollout)
        != {"schema_version", "phase", "scope_observation_bootstrap", "canary_report"}
        or rollout.get("schema_version") != AUTOMATIC_ROLLOUT_SCHEMA
        or rollout.get("phase") != "pending_owner"
        or rollout.get("scope_observation_bootstrap") != bootstrap
    ):
        raise RuntimeBindingError("pending automatic wait rollout is invalid")
    evidence_dir = Path(str(payload.get("evidence_dir") or ""))
    canary_path = Path(str(rollout.get("canary_report") or ""))
    try:
        canary_path.resolve().relative_to(evidence_dir.resolve())
    except (OSError, ValueError) as exc:
        raise RuntimeBindingError(
            "pending automatic wait canary is outside evidence directory"
        ) from exc

    activation = payload.get("automatic_activation")
    expected_activation_keys = {
        "fresh_shared_handoff",
        "daily_boundary_initial",
        "daily_boundary_commit",
        "quiescence_before",
        "live_canary",
        "children_transaction",
        "shared_consumer_unpaused",
        "owner_unpaused_last",
    }
    if not isinstance(activation, Mapping) or set(activation) != expected_activation_keys:
        raise RuntimeBindingError("pending automatic wait ceremony is incomplete")
    if (
        activation.get("shared_consumer_unpaused") is not False
        or activation.get("owner_unpaused_last") is not False
    ):
        raise RuntimeBindingError("pending automatic wait claims a completed cutover")

    fresh_handoff = activation.get("fresh_shared_handoff")
    final_handoff = payload.get("shared_handoff_final")
    immutable_handoff_fields = {
        "shared_scheduler_container",
        "shared_admission_mount",
        "runtime_code_sha256",
        "runtime_git_sha",
        "control_database",
        "schedule_owner",
    }
    if (
        not isinstance(fresh_handoff, Mapping)
        or fresh_handoff.get("passed") is not True
        or not isinstance(final_handoff, Mapping)
        or any(
            fresh_handoff.get(field) != final_handoff.get(field)
            for field in immutable_handoff_fields
        )
    ):
        raise RuntimeBindingError("pending automatic shared handoff differs")

    def boundary(value: Any, *, label: str) -> tuple[datetime, datetime, datetime]:
        expected_keys = {
            "schema_version",
            "checked_at",
            "selected_date",
            "state",
            "data_interval_start",
            "data_interval_end",
            "safe_start",
            "safe_cutoff",
            "passed",
        }
        if (
            not isinstance(value, Mapping)
            or set(value) != expected_keys
            or value.get("schema_version") != "fotmob-automatic-boundary-v1"
            or value.get("passed") is not True
            or value.get("state") not in {"future", "daily_window_open"}
        ):
            raise RuntimeBindingError(f"pending {label} daily boundary is invalid")
        checked = _timestamp(value.get("checked_at"))
        start = _timestamp(value.get("data_interval_start"))
        end = _timestamp(value.get("data_interval_end"))
        safe_start = _timestamp(value.get("safe_start"))
        cutoff = _timestamp(value.get("safe_cutoff"))
        if (
            end - start != timedelta(days=1)
            or (end.hour, end.minute, end.second, end.microsecond) != (14, 0, 0, 0)
            or value.get("selected_date") != end.date().isoformat()
            or safe_start != end - timedelta(minutes=30)
            or cutoff != end + timedelta(minutes=45)
            or checked < safe_start
            or checked >= cutoff
        ):
            raise RuntimeBindingError(f"pending {label} daily boundary differs")
        return checked, start, end

    initial_checked, initial_start, initial_end = boundary(
        activation.get("daily_boundary_initial"), label="initial"
    )
    commit_checked, commit_start, commit_end = boundary(
        activation.get("daily_boundary_commit"), label="commit"
    )
    shared_boundary = _normalize_schedule_boundary(
        fresh_handoff.get("next_scheduled_interval"),
        label="pending automatic shared handoff",
    )
    if (
        (initial_start, initial_end) != (commit_start, commit_end)
        or commit_checked < initial_checked
        or shared_boundary["logical_date"]
        != commit_start.isoformat(timespec="microseconds")
        or shared_boundary["data_interval_start"]
        != commit_start.isoformat(timespec="microseconds")
        or shared_boundary["data_interval_end"]
        != commit_end.isoformat(timespec="microseconds")
        or shared_boundary["run_after"]
        != commit_end.isoformat(timespec="microseconds")
    ):
        raise RuntimeBindingError("pending automatic shared interval differs")

    all_paused = {dag_id: True for dag_id in AUTOMATIC_DAGBAG_DAGS}
    children_paused = {
        dag_id: dag_id
        not in {"dag_ingest_fotmob", "dag_transform_fotmob_silver"}
        for dag_id in AUTOMATIC_DAGBAG_DAGS
    }
    transaction = activation.get("children_transaction")
    scheduler_state = (
        transaction.get("scheduler_state") if isinstance(transaction, Mapping) else None
    )
    if (
        not isinstance(transaction, Mapping)
        or transaction.get("schema_version") != "fotmob-writer-snapshot-v1"
        or transaction.get("phase") != "children"
        or transaction.get("pause_states") != all_paused
        or transaction.get("pause_states_after") != children_paused
        or transaction.get("active_runs") != {}
        or re.fullmatch(
            r"[0-9a-f]{32}", str(transaction.get("transaction_id") or "")
        )
        is None
        or not isinstance(scheduler_state, Mapping)
        or set(scheduler_state)
        != {"next_background_lane", "daily_date", "generation", "updated_at"}
        or scheduler_state.get("next_background_lane") not in {"refresh", "backfill"}
        or scheduler_state.get("daily_date") == commit_end.date().isoformat()
        or isinstance(scheduler_state.get("generation"), bool)
        or not isinstance(scheduler_state.get("generation"), int)
        or scheduler_state["generation"] < 0
    ):
        raise RuntimeBindingError("pending automatic children transaction differs")
    try:
        if scheduler_state.get("daily_date") is not None:
            parsed_daily = date.fromisoformat(str(scheduler_state["daily_date"]))
            if parsed_daily.isoformat() != scheduler_state["daily_date"]:
                raise ValueError("non-canonical daily date")
        _timestamp(scheduler_state.get("updated_at"))
        transaction_at = _timestamp(transaction.get("observed_at"))
        generated_at = _timestamp(payload.get("generated_at"))
    except (TypeError, ValueError, RuntimeBindingError) as exc:
        raise RuntimeBindingError(
            "pending automatic children transaction is malformed"
        ) from exc
    if transaction_at < initial_checked or transaction_at > generated_at:
        raise RuntimeBindingError("pending automatic transaction ordering differs")

    quiescence = activation.get("quiescence_before")
    live_canary = activation.get("live_canary")
    canary = admission.get("canary")
    if (
        not isinstance(quiescence, Mapping)
        or quiescence.get("safe") is not True
        or quiescence.get("active") is not False
        or not isinstance(live_canary, Mapping)
        or not isinstance(canary, Mapping)
        or live_canary.get("runner_sha256") != canary.get("runner_report_sha256")
        or not isinstance(live_canary.get("runner_bytes"), int)
        or live_canary["runner_bytes"] <= 0
        or canary.get("deployment_id") != payload.get("deployment_id")
        or canary.get("git_sha") != payload.get("git_sha")
        or canary.get("scheduler_container_id")
        != payload.get("scheduler_container_id")
    ):
        raise RuntimeBindingError("pending automatic wait evidence differs")
    return {
        "schema_version": AUTOMATIC_ROLLOUT_SCHEMA,
        "phase": "pending_owner",
        "wait_only": True,
        "daily_interval": {
            "start": commit_start.isoformat(timespec="microseconds"),
            "end": commit_end.isoformat(timespec="microseconds"),
        },
        "passed": True,
    }


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
    if activation_state in {"committed_pending_trigger", "pending_consumer"}:
        raise RuntimeBindingError(
            "deployment consumer activation is incomplete; resume deploy"
        )
    if activation_state not in {"active", "kept_paused"}:
        raise RuntimeBindingError("deployment report has no completed activation state")
    paused = payload.get("paused")
    unpaused = payload.get("unpaused")
    raw_automatic_admission = payload.get("automatic_catalog_admission")
    raw_automatic_rollout = payload.get("automatic_rollout")
    raw_coordinator_rollout = payload.get("coordinator_rollout")
    deployment_generated_at = (
        _timestamp(payload.get("generated_at"))
        if raw_automatic_admission is not None
        else None
    )
    automatic_admission = (
        validate_automatic_catalog_admission(
            raw_automatic_admission, now=deployment_generated_at
        )
        if raw_automatic_admission is not None
        else None
    )
    automatic_bootstrap = (
        isinstance(raw_automatic_rollout, Mapping)
        and raw_automatic_rollout.get("schema_version") == AUTOMATIC_ROLLOUT_SCHEMA
        and raw_automatic_rollout.get("phase") == "awaiting_canary"
        and raw_automatic_rollout.get("scope_observation_bootstrap")
        == {
            "table": SCOPE_OBSERVATIONS_TABLE,
            "table_exists": True,
            "current_view": SCOPE_OBSERVATIONS_CURRENT_VIEW,
            "current_view_exists": True,
        }
    )
    if raw_automatic_rollout is not None and not automatic_bootstrap and (
        automatic_admission is None
    ):
        raise RuntimeBindingError("automatic rollout bootstrap evidence is invalid")
    automatic_profile = automatic_admission is not None or automatic_bootstrap
    coordinator_profile = (
        activation_state == "kept_paused"
        and isinstance(raw_coordinator_rollout, Mapping)
        and raw_coordinator_rollout
        == {
            "schema_version": COORDINATOR_ROLLOUT_SCHEMA,
            "phase": "kept_paused",
            "legacy_activation_retired": True,
        }
    )
    if automatic_admission is not None and activation_state == "active":
        if (
            automatic_admission["current_run_report_count"] != 1
            or automatic_admission["current_contract_identity"] is None
            or payload.get("kept_paused") is not False
            or not isinstance(paused, list)
            or set(paused) != LEGACY_OWNER_DAGS
            or not isinstance(unpaused, list)
            or set(unpaused) != AUTOMATIC_ACTIVE_DAGS
        ):
            raise RuntimeBindingError(
                "automatic active deployment pause state is inconsistent"
            )
        payload["automatic_rollout_summary"] = validate_automatic_rollout_activation(
            payload, automatic_admission
        )
    elif automatic_profile:
        if (
            payload.get("kept_paused") is not True
            or not isinstance(paused, list)
            or set(paused) != AUTOMATIC_DAGBAG_DAGS
            or unpaused != []
        ):
            raise RuntimeBindingError(
                "automatic kept-paused deployment state is inconsistent"
            )
    elif activation_state == "active":
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
    if not automatic_profile and not coordinator_profile:
        required = (*required, "schedule_boundary")
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
    if not automatic_profile and not coordinator_profile:
        schedule_boundary = payload.get("schedule_boundary")
        expected_boundary_keys = {
            "shared_dag_id",
            "isolated_dag_id",
            "shared_initial",
            "shared_final",
            "isolated_initial",
            "isolated_final",
            "exact_match",
        }
        if activation_state == "active":
            expected_boundary_keys.update({"shared_commit", "isolated_commit"})
        if (
            not isinstance(schedule_boundary, Mapping)
            or set(schedule_boundary) != expected_boundary_keys
            or schedule_boundary.get("shared_dag_id") != SHARED_CONSUMER_DAG_ID
            or schedule_boundary.get("isolated_dag_id") != ISOLATED_DAILY_DAG_ID
            or schedule_boundary.get("exact_match") is not True
        ):
            raise RuntimeBindingError(
                "deployment report has no exact producer/consumer schedule proof"
            )
        boundary_names = [
            "shared_initial",
            "shared_final",
            "isolated_initial",
            "isolated_final",
        ]
        if activation_state == "active":
            boundary_names.extend(["shared_commit", "isolated_commit"])
        normalized_boundaries = {
            key: _normalize_schedule_boundary(
                schedule_boundary.get(key), label=f"deployment {key}"
            )
            for key in boundary_names
        }
        expected_boundary = normalized_boundaries["shared_initial"]
        if (
            any(value != expected_boundary for value in normalized_boundaries.values())
            or _normalize_schedule_boundary(
                initial_handoff.get("next_scheduled_interval"),
                label="initial shared handoff",
            )
            != expected_boundary
            or _normalize_schedule_boundary(
                final_handoff.get("next_scheduled_interval"),
                label="final shared handoff",
            )
            != expected_boundary
        ):
            raise RuntimeBindingError(
                "deployment producer/consumer next scheduled intervals differ"
            )
        if activation_state == "active":
            _validate_active_schedule_proof(payload, expected_boundary)
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
    if automatic_admission is not None:
        canary_summary = automatic_admission.get("canary")
        if (
            not isinstance(canary_summary, Mapping)
            or canary_summary.get("deployment_id") != payload["deployment_id"]
            or canary_summary.get("git_sha") != payload["git_sha"]
            or canary_summary.get("scheduler_container_id")
            != payload["scheduler_container_id"]
        ):
            raise RuntimeBindingError(
                "automatic canary identity differs from deployment report"
            )
        payload["automatic_catalog_admission_summary"] = automatic_admission
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
        "control_database_fingerprint": hashlib.sha256(
            control_uri.encode("utf-8")
        ).hexdigest(),
    }


def validate_live_shared_runtime(
    context: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    """Re-attest the active shared container without assuming Sofa is paused."""

    handoff = context.get("shared_handoff_final")
    if not isinstance(handoff, Mapping):
        raise RuntimeBindingError("deployment report has no shared handoff proof")
    container_id = str(handoff.get("shared_scheduler_container") or "")
    shared = _inspect_container(container_id, run=run)
    isolated = _inspect_container(str(context.get("scheduler_container_id") or ""), run=run)
    if (
        shared.get("Id") != container_id
        or not bool((shared.get("State") or {}).get("Running"))
        or isolated.get("Id") != context.get("scheduler_container_id")
    ):
        raise RuntimeBindingError("shared/isolated scheduler identity drifted")
    shared_env = _parsed_environment(shared)
    isolated_env = _parsed_environment(isolated)
    control_uri = isolated_env.get("FBREF_CONTROL_DB_URI", "")
    if (
        not control_uri
        or "airflow-metadb" in control_uri.lower()
        or shared_env.get("FBREF_CONTROL_DB_URI") != control_uri
        or shared_env.get("FOTMOB_DEPLOY_GIT_SHA") != context.get("git_sha")
        or shared_env.get("FOTMOB_ISOLATED_STACK", "")
        or shared_env.get("FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH")
        != context.get("shared_container_report_path")
    ):
        raise RuntimeBindingError("active shared runtime environment drifted")
    expected_mount = {
        "Source": str(Path(str(context.get("evidence_dir") or "")).resolve()),
        "Destination": str(SHARED_CONTAINER_EVIDENCE_ROOT),
    }
    matching_mounts = [
        mount
        for mount in shared.get("Mounts") or ()
        if isinstance(mount, Mapping)
        and mount.get("Destination") == expected_mount["Destination"]
    ]
    if (
        len(matching_mounts) != 1
        or str(matching_mounts[0].get("Source") or "") != expected_mount["Source"]
        or matching_mounts[0].get("RW") is not False
    ):
        raise RuntimeBindingError("active shared admission mount drifted")
    release = Path(str(context.get("release_root") or ""))
    expected_manifest = shared_runtime_manifest(release)
    if handoff.get("runtime_code_sha256") != expected_manifest:
        raise RuntimeBindingError("active shared report manifest drifted")
    marker = "FOTMOB_ACTIVE_SHARED_MANIFEST_JSON="
    code = (
        "import hashlib,json\n"
        "from pathlib import Path\n"
        f"roots={SHARED_RUNTIME_ROOTS!r}\n"
        f"suffixes={SHARED_RUNTIME_SUFFIXES!r}\n"
        "manifest={}\n"
        "for prefix, root_name in roots.items():\n"
        "    root=Path(root_name)\n"
        "    for path in sorted(root.rglob('*')):\n"
        "        if path.is_symlink(): raise RuntimeError('shared runtime symlink')\n"
        "        if path.is_file() and '__pycache__' not in path.parts and "
        "(path.name == '.airflowignore' or path.name.endswith(suffixes)):\n"
        "            key=prefix+'/'+path.relative_to(root).as_posix()\n"
        "            manifest[key]=hashlib.sha256(path.read_bytes()).hexdigest()\n"
        f"print('{marker}'+json.dumps(manifest,sort_keys=True))\n"
    )
    output = run(
        ("docker", "exec", container_id, "python", "-c", code),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    remote_manifest = None
    for line in reversed(output.splitlines()):
        if line.startswith(marker):
            try:
                remote_manifest = json.loads(line.removeprefix(marker))
            except json.JSONDecodeError as exc:
                raise RuntimeBindingError(
                    "active shared runtime manifest is invalid"
                ) from exc
            break
    if remote_manifest != expected_manifest:
        raise RuntimeBindingError("active shared runtime bytes drifted")
    return {
        "shared_scheduler_container_id": container_id,
        "runtime_git_sha": context.get("git_sha"),
        "runtime_code_sha256": expected_manifest,
        "control_database_bound": True,
        "control_database_fingerprint": hashlib.sha256(
            control_uri.encode("utf-8")
        ).hexdigest(),
        "admission_mount_read_only": True,
        "passed": True,
    }


def validate_live_purge_data_bindings(
    context: Mapping[str, Any],
    *,
    raw_store_uri: str,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    """Bind destructive host raw access to the admitted isolated runtime."""

    isolated = _inspect_container(str(context.get("scheduler_container_id") or ""), run=run)
    if isolated.get("Id") != context.get("scheduler_container_id"):
        raise RuntimeBindingError("purge isolated scheduler identity drifted")
    admitted = _parsed_environment(isolated)
    host_raw = str(raw_store_uri).strip()
    if not host_raw or admitted.get("FOTMOB_RAW_STORE_URI") != host_raw:
        raise RuntimeBindingError("purge raw-store URI differs from admitted runtime")
    mismatches = [
        key
        for key in PURGE_RAW_ENV_KEYS[1:]
        if os.environ.get(key, "") != admitted.get(key, "")
    ]
    if mismatches:
        raise RuntimeBindingError(
            f"purge raw-store credentials/endpoint differ: {mismatches!r}"
        )
    host_trino = {key: os.environ.get(key, "") for key in TRINO_ENV_KEYS}
    # A host-reachable DNS name may differ from Docker DNS.  The immutable
    # deployment marker proves the data plane; every other endpoint/security
    # property and the credential must be the admitted value.
    trino_mismatches = [
        key
        for key in TRINO_ENV_KEYS
        if key != "TRINO_HOST" and host_trino[key] != admitted.get(key, "")
    ]
    if trino_mismatches:
        raise RuntimeBindingError(
            f"purge Trino security binding differs: {trino_mismatches!r}"
        )
    if not host_trino["TRINO_HOST"]:
        raise RuntimeBindingError("purge host Trino endpoint is missing")
    return {
        "scheduler_container_id": context.get("scheduler_container_id"),
        "raw_store_uri": host_raw,
        "raw_store_bound": True,
        "trino_host": host_trino["TRINO_HOST"],
        "trino_security_bound": True,
        "passed": True,
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
