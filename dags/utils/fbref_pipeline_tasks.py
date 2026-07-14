"""Airflow callables for the durable FBref pipeline.

The functions keep DAG files declarative and recompute the deterministic
control run UUID instead of passing scheduler-local files or mutable XCom
payloads between workers.
"""

from __future__ import annotations

import hashlib
import json
import logging
import subprocess
import sys
from typing import Mapping, Optional, Sequence

from scrapers.fbref.settings import (
    DEFAULT_BYTE_LIMIT,
    DEFAULT_REQUEST_LIMIT,
    DEFAULT_REQUEST_RESERVATION_BYTES,
    DEFAULT_SHARD_SIZE,
    MIB,
)


logger = logging.getLogger(__name__)

DEFAULT_PROXY_FILE = "/opt/airflow/proxys.txt"
FETCH_WAVE_RUNNER = "/opt/airflow/dags/scripts/run_fbref_fetch_wave.py"
FETCH_WAVE_RESULT_PREFIX = "FBREF_FETCH_WAVE_RESULT:"
# A full 25-page wave sleeps 3s per page and needs one clearance bootstrap, so
# it finishes in minutes.  Anything past this is a hung browser, not slow work.
FETCH_WAVE_TIMEOUT_SECONDS = 30 * 60

# Runtime limits are repeated here intentionally: the Airflow boundary must
# reject an unsafe dag_run.conf even when Param validation is bypassed.  The
# only supported live profiles are the measured production budget and the
# separately bounded canary budget.
FBREF_PRODUCTION_REQUEST_LIMIT = 200
FBREF_PRODUCTION_BYTE_LIMIT_MB = 100
FBREF_CANARY_REQUEST_LIMIT = 100
FBREF_CANARY_BYTE_LIMIT_MB = 50
FBREF_MAX_WARM_SESSION_TARGETS = 25
FBREF_PUBLICATION_SCOPE_TABLE = "fbref_target_scope"
FBREF_PUBLICATION_LOCK_TTL_SECONDS = 8 * 24 * 60 * 60
FBREF_LIVE_BUDGET_PROFILES = {
    (FBREF_PRODUCTION_REQUEST_LIMIT, FBREF_PRODUCTION_BYTE_LIMIT_MB): (
        "production"
    ),
    (FBREF_CANARY_REQUEST_LIMIT, FBREF_CANARY_BYTE_LIMIT_MB): "canary",
}

# Current-scope source SLAs.  ControlStore.get_run_summary owns the target
# selection and reports the measured state; this Airflow layer owns the policy
# and the fail-closed verdict.
FBREF_REGISTRY_SCHEDULE_FINAL_FRESHNESS_HOURS = 24
FBREF_SEASON_STATS_SQUAD_FRESHNESS_HOURS = 7 * 24
FBREF_PLAYER_MATCHLOG_FRESHNESS_HOURS = 30 * 24
FBREF_CURRENT_SCOPE_FRESHNESS_HOURS = {
    "competition_index": FBREF_REGISTRY_SCHEDULE_FINAL_FRESHNESS_HOURS,
    "registry": FBREF_REGISTRY_SCHEDULE_FINAL_FRESHNESS_HOURS,
    "schedule": FBREF_REGISTRY_SCHEDULE_FINAL_FRESHNESS_HOURS,
    "match": FBREF_REGISTRY_SCHEDULE_FINAL_FRESHNESS_HOURS,
    "final_match": FBREF_REGISTRY_SCHEDULE_FINAL_FRESHNESS_HOURS,
    "competition": FBREF_SEASON_STATS_SQUAD_FRESHNESS_HOURS,
    "season": FBREF_SEASON_STATS_SQUAD_FRESHNESS_HOURS,
    "season_stats": FBREF_SEASON_STATS_SQUAD_FRESHNESS_HOURS,
    "standings": FBREF_SEASON_STATS_SQUAD_FRESHNESS_HOURS,
    "squad": FBREF_SEASON_STATS_SQUAD_FRESHNESS_HOURS,
    "player": FBREF_PLAYER_MATCHLOG_FRESHNESS_HOURS,
    "matchlog": FBREF_PLAYER_MATCHLOG_FRESHNESS_HOURS,
}
FBREF_REQUIRED_CURRENT_PAGE_KINDS = frozenset(
    {
        "competition_index",
        "competition",
        "season",
        "season_stats",
        "schedule",
        "standings",
        "squad",
        "player",
        "matchlog",
        "match",
    }
)


def _pipeline():
    from scrapers.fbref.pipeline import FBrefPipeline

    return FBrefPipeline.from_env()


def _control_store():
    from scrapers.fbref.control import ControlStore

    return ControlStore.from_env()


def _record_control_traffic(summary: dict, *, airflow_run_id: str) -> None:
    """Bridge durable control metrics into the existing daily cost rollup.

    This is deliberately passive.  The control-plane budget and validation
    gates have already run; an unavailable telemetry table must not turn a
    successful, fully validated ingest into a producer failure.
    """

    try:
        from utils.proxy_traffic import log_traffic_summary, record_traffic_run

        total_mb = float(summary.get("bytes_used") or 0) / MIB
        traffic_summary = {
            "source": "fbref",
            "total_mb": round(total_mb, 6),
            # Control stores browser-vs-HTTP and page-kind breakdowns, not a
            # host attribution. Do not mislabel Turnstile asset bytes as
            # fbref.com traffic.
            "top_domains": [],
            "files_read": 0,
        }
        log_traffic_summary(traffic_summary)
        record_traffic_run(
            traffic_summary,
            dag_run_id=str(airflow_run_id),
            replace_existing=True,
        )
    except Exception as exc:  # noqa: BLE001 - telemetry is non-fatal
        logger.warning("FBref proxy-traffic telemetry skipped: %s", exc)


def _settings(
    *,
    run_type: str,
    request_limit=DEFAULT_REQUEST_LIMIT,
    byte_limit_mb=DEFAULT_BYTE_LIMIT // MIB,
    shard_size=DEFAULT_SHARD_SIZE,
    reservation_mb=DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
    domain_interval_seconds=3.0,
    proxy_file: Optional[str] = "/opt/airflow/proxys.txt",
) -> object:
    from scrapers.fbref.pipeline import PipelineSettings

    validate_fbref_runtime_limits(
        run_type=run_type,
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
    )

    return PipelineSettings(
        run_type=str(run_type),
        request_limit=int(request_limit),
        byte_limit=int(byte_limit_mb) * MIB,
        shard_size=int(shard_size),
        request_reservation_bytes=int(reservation_mb) * MIB,
        domain_interval_seconds=float(domain_interval_seconds),
        proxy_file=proxy_file,
    )


def validate_fbref_runtime_limits(
    *,
    run_type: str,
    request_limit,
    byte_limit_mb,
    shard_size,
) -> dict:
    """Validate settings rendered from Params or arbitrary DagRun conf."""

    normalized_run_type = str(run_type).strip().casefold()
    normalized_requests = int(request_limit)
    normalized_bytes = int(byte_limit_mb)
    normalized_shard = int(shard_size)
    if normalized_run_type not in {"current", "backfill", "replay"}:
        raise ValueError(f"Unknown FBref run_type: {run_type!r}")
    if not 1 <= normalized_shard <= FBREF_MAX_WARM_SESSION_TARGETS:
        raise ValueError(
            "FBref shard_size must be between 1 and "
            f"{FBREF_MAX_WARM_SESSION_TARGETS} targets"
        )
    if normalized_run_type == "replay":
        if normalized_requests != 0 or normalized_bytes != 0:
            raise ValueError("FBref replay requires zero request/byte budgets")
        profile = "replay"
    else:
        profile = FBREF_LIVE_BUDGET_PROFILES.get(
            (normalized_requests, normalized_bytes)
        )
        if profile is None:
            allowed = ", ".join(
                f"{requests} requests/{bytes_mb} MiB"
                for requests, bytes_mb in FBREF_LIVE_BUDGET_PROFILES
            )
            raise ValueError(
                "Unsupported FBref live budget; use one hard profile: "
                f"{allowed}"
            )
    return {
        "run_type": normalized_run_type,
        "profile": profile,
        "request_limit": normalized_requests,
        "byte_limit_mb": normalized_bytes,
        "shard_size": normalized_shard,
    }


def validate_fbref_production_readiness(
    *,
    run_type: str,
    request_limit,
    byte_limit_mb,
    shard_size,
) -> dict:
    """First-task gate for alert labelling and hard runtime limits."""

    from utils.alerts import validate_alert_environment

    alert = validate_alert_environment("prod")
    limits = validate_fbref_runtime_limits(
        run_type=run_type,
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
    )
    return {**alert, **limits}


def _boolean_parameter(value, *, name: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().casefold()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def choose_fbref_backfill_mode(*, dry_run) -> str:
    """Branch before any control-run mutation or paid-network task."""

    return (
        "plan_backfill"
        if _boolean_parameter(dry_run, name="dry_run")
        else "validate_production_readiness"
    )


def plan_fbref_backfill(
    *,
    request_limit=FBREF_PRODUCTION_REQUEST_LIMIT,
    byte_limit_mb=FBREF_PRODUCTION_BYTE_LIMIT_MB,
    shard_size=FBREF_MAX_WARM_SESSION_TARGETS,
) -> dict:
    """Return the exact next historical cohort without changing state."""

    limits = validate_fbref_runtime_limits(
        run_type="backfill",
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
    )
    settings = _settings(
        run_type="backfill",
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
    )
    from scrapers.fbref.pipeline import wave_target_capacity

    effective_limit = wave_target_capacity(settings)
    rows = _control_store().list_backfill_seasons(limit=effective_limit)
    candidates = [
        {
            "competition_id": str(row["competition_id"]),
            "season_id": str(row["season_id"]),
            "canonical_url": str(row["canonical_url"]),
            "competition_name": (
                None
                if row.get("competition_name") is None
                else str(row["competition_name"])
            ),
        }
        for row in rows
    ]
    result = {
        **limits,
        "dry_run": True,
        "network_requests": 0,
        "state_mutations": 0,
        "effective_cohort_limit": effective_limit,
        "next_cohort_count": len(candidates),
        "next_cohort": candidates,
    }
    logger.info("FBref backfill dry-run: %s", json.dumps(result, sort_keys=True))
    return result


def export_fbref_publication_scope(
    *, airflow_run_id: str, dag_id: str
) -> dict:
    """Atomically publish one immutable control-run scope generation."""

    from datetime import datetime, timezone

    import pandas as pd

    from scrapers.base.trino_manager import TrinoTableManager
    from scrapers.fbref.typed_bronze import (
        COMPATIBILITY_LEAGUE_BY_COMPETITION_ID,
        TypedSourceContext,
    )

    rows = _control_store().list_publication_scope(source="fbref")
    if not rows:
        raise RuntimeError("FBref publication scope is empty")
    exported_at = datetime.now(timezone.utc).replace(tzinfo=None)
    control_run_id = _control_run_id(
        airflow_run_id=airflow_run_id, dag_id=dag_id
    )
    columns = {
        "source": "VARCHAR",
        "source_competition_id": "VARCHAR",
        "source_season_id": "VARCHAR",
        "canonical_season_id": "VARCHAR",
        "scope_kind": "VARCHAR",
        "legacy_league": "VARCHAR",
        "legacy_season": "BIGINT",
        "competition_name": "VARCHAR",
        "gender": "VARCHAR",
        "competition_crawl_state": "VARCHAR",
        "competition_lifecycle_state": "VARCHAR",
        "competition_present": "BOOLEAN",
        "season_label": "VARCHAR",
        "season_is_current": "BOOLEAN",
        "season_lifecycle_state": "VARCHAR",
        "season_present": "BOOLEAN",
        "direct_match_only": "BOOLEAN",
        "eligible_male": "BOOLEAN",
        "control_run_id": "VARCHAR",
        "scope_hash": "VARCHAR",
        "exported_at": "TIMESTAMP(6)",
    }
    records = []
    for row in rows:
        competition_id = str(row.get("source_competition_id") or "")
        source_season_id = str(row.get("source_season_id") or "")
        legacy_league = COMPATIBILITY_LEAGUE_BY_COMPETITION_ID.get(
            competition_id
        )
        legacy_season = None
        if legacy_league and source_season_id:
            legacy_season = TypedSourceContext(
                source_competition_id=competition_id,
                source_season_id=source_season_id,
                season_label=source_season_id,
            ).season
        records.append(
            {
                **{key: row.get(key) for key in columns},
                "source": "fbref",
                "legacy_league": legacy_league,
                "legacy_season": legacy_season,
                "control_run_id": control_run_id,
            }
        )
    hash_columns = tuple(
        name for name in columns if name not in {"scope_hash", "exported_at"}
    )
    canonical_rows = sorted(
        json.dumps(
            {name: record.get(name) for name in hash_columns},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        for record in records
    )
    scope_hash = hashlib.sha256(
        "\n".join(canonical_rows).encode("utf-8")
    ).hexdigest()
    records = [
        {
            **record,
            "scope_hash": scope_hash,
            "exported_at": exported_at,
        }
        for record in records
    ]
    frame = pd.DataFrame(records, columns=list(columns))
    # Keep SQL-nullable string columns as Python objects.  Pandas otherwise
    # promotes a mixed string/None series to a floating NaN sentinel, which is
    # easy to mishandle outside Trino's literal formatter.
    frame["legacy_league"] = frame["legacy_league"].astype(object).where(
        frame["legacy_league"].notna(), None
    )
    frame["legacy_season"] = frame["legacy_season"].astype(object).where(
        frame["legacy_season"].notna(), None
    )
    manager = TrinoTableManager()
    manager.create_iceberg_table(
        "bronze", FBREF_PUBLICATION_SCOPE_TABLE, columns
    )
    existing_columns = {
        str(name).casefold()
        for name in manager.get_table_columns(
            "bronze", FBREF_PUBLICATION_SCOPE_TABLE
        )
    }
    for name, column_type in columns.items():
        if name.casefold() not in existing_columns:
            manager.add_column(
                "bronze", FBREF_PUBLICATION_SCOPE_TABLE, name, column_type
            )
    existing_generation = manager.execute_query(
        "SELECT count(*), count(DISTINCT scope_hash), min(scope_hash) "
        "FROM iceberg.bronze.fbref_target_scope "
        "WHERE source = ? AND control_run_id = ?",
        params=("fbref", control_run_id),
    )
    existing_count = (
        int(existing_generation[0][0]) if existing_generation else 0
    )
    eligible = int(frame["eligible_male"].fillna(False).sum())
    if eligible <= 0:
        raise RuntimeError("FBref publication scope has no eligible male rows")
    if existing_count:
        distinct_hashes = int(existing_generation[0][1] or 0)
        existing_hash = existing_generation[0][2]
        if (
            existing_count != len(frame)
            or distinct_hashes != 1
            or str(existing_hash or "") != scope_hash
        ):
            raise RuntimeError(
                "FBref publication scope generation is immutable and the "
                f"existing rows differ for control_run_id={control_run_id}"
            )
        result = {
            "table": f"iceberg.bronze.{FBREF_PUBLICATION_SCOPE_TABLE}",
            "rows": existing_count,
            "eligible_male_rows": eligible,
            "quarantined_rows": existing_count - eligible,
            "control_run_id": control_run_id,
            "scope_hash": scope_hash,
            "idempotent": True,
        }
        logger.info(
            "FBref publication scope already exists: %s",
            json.dumps(result, sort_keys=True),
        )
        return result
    inserted = manager.insert_dataframe_atomic(
        "bronze",
        FBREF_PUBLICATION_SCOPE_TABLE,
        frame,
        delete_filter=(
            "source = 'fbref' AND control_run_id = "
            f"'{control_run_id}'"
        ),
        staging_id=(
            "scope_" + control_run_id.replace("-", "")
        ),
        single_statement_replace=True,
    )
    if inserted != len(frame):
        raise RuntimeError(
            "FBref publication scope row-count mismatch: "
            f"inserted={inserted}, expected={len(frame)}"
        )
    result = {
        "table": f"iceberg.bronze.{FBREF_PUBLICATION_SCOPE_TABLE}",
        "rows": len(frame),
        "eligible_male_rows": eligible,
        "quarantined_rows": len(frame) - eligible,
        "control_run_id": control_run_id,
        "scope_hash": scope_hash,
        "idempotent": False,
    }
    logger.info("FBref publication scope: %s", json.dumps(result, sort_keys=True))
    return result


def validate_fbref_publication_scope(*, control_run_id: str) -> dict:
    """Prove a succeeded run owns one complete immutable scope generation."""

    import uuid

    from scrapers.base.trino_manager import TrinoTableManager

    normalized = str(uuid.UUID(str(control_run_id).strip()))
    run = _control_store().get_run(normalized)
    if run is None or str(run.get("status") or "").casefold() != "succeeded":
        raise RuntimeError(
            "FBref publication scope requires a succeeded control run"
        )
    rows = TrinoTableManager().execute_query(
        "SELECT count(*), count_if(eligible_male), "
        "count(DISTINCT scope_hash), min(scope_hash), "
        "count_if(scope_hash IS NULL) "
        "FROM iceberg.bronze.fbref_target_scope "
        "WHERE source = ? AND control_run_id = ?",
        params=("fbref", normalized),
    )
    if not rows:
        raise RuntimeError("FBref publication scope query returned no evidence")
    total, eligible, hash_count, scope_hash, null_hashes = rows[0]
    if (
        int(total or 0) <= 0
        or int(eligible or 0) <= 0
        or int(hash_count or 0) != 1
        or not str(scope_hash or "").strip()
        or int(null_hashes or 0) != 0
    ):
        raise RuntimeError(
            "FBref publication scope is absent, ineligible, or not immutable"
        )
    return {
        "control_run_id": normalized,
        "rows": int(total),
        "eligible_male_rows": int(eligible),
        "scope_hash": str(scope_hash),
        "status": "ready",
    }


def _non_negative_metric(payload: Mapping[str, object], key: str) -> int:
    try:
        value = int(payload.get(key, 0) or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid FBref freshness metric {key!r}") from exc
    if value < 0:
        raise ValueError(f"Invalid negative FBref freshness metric {key!r}")
    return value


def validate_fbref_current_scope_freshness(
    *, airflow_run_id: str, dag_id: str, run_type: str
) -> dict:
    """Fail closed when any active male/current target exceeds its source SLA.

    ``ControlStore.get_run_summary`` selects the authoritative current scope.
    This callable intentionally uses only that public API, and accepts either
    the detailed per-page-kind document or its aggregate for rolling upgrades.
    """

    normalized_run_type = str(run_type).strip().casefold()
    if normalized_run_type == "replay":
        return {"status": "not_applicable", "run_type": "replay"}
    if normalized_run_type not in {"current", "backfill"}:
        raise ValueError(f"Unknown FBref run_type: {run_type!r}")

    summary = _control_store().get_run_summary(
        _control_run_id(airflow_run_id=airflow_run_id, dag_id=dag_id)
    )
    if summary is None:
        raise RuntimeError("FBref freshness gate cannot find its control run")

    aggregate = summary.get("current_scope_freshness")
    by_kind = summary.get("freshness_by_page_kind")
    if not isinstance(aggregate, Mapping) and not isinstance(by_kind, Mapping):
        raise RuntimeError(
            "FBref control summary has no current-scope freshness evidence"
        )

    violations = []
    normalized_kinds = {}
    if isinstance(by_kind, Mapping):
        missing = sorted(FBREF_REQUIRED_CURRENT_PAGE_KINDS - set(by_kind))
        if missing:
            violations.append("missing_page_kinds=" + ",".join(missing))
        for raw_kind, raw_metrics in by_kind.items():
            kind = str(raw_kind)
            if kind not in FBREF_CURRENT_SCOPE_FRESHNESS_HOURS:
                continue
            if not isinstance(raw_metrics, Mapping):
                violations.append(f"{kind}:invalid_metrics")
                continue
            expected_seconds = (
                FBREF_CURRENT_SCOPE_FRESHNESS_HOURS[kind] * 60 * 60
            )
            try:
                reported_seconds = int(raw_metrics.get("sla_seconds", -1))
            except (TypeError, ValueError):
                reported_seconds = -1
            total = _non_negative_metric(raw_metrics, "total_targets")
            stale = _non_negative_metric(raw_metrics, "stale_targets")
            never = _non_negative_metric(raw_metrics, "never_fetched_targets")
            if reported_seconds <= 0 or reported_seconds > expected_seconds:
                violations.append(
                    f"{kind}:sla_seconds={reported_seconds}>"
                    f"{expected_seconds}"
                )
            if total == 0:
                violations.append(f"{kind}:total_targets=0")
            # ``never_fetched`` is diagnostic, not independently a breach:
            # a newly discovered final match may be queued for up to its 24h
            # SLA and is counted as fresh by the control-plane evaluator.
            if stale:
                violations.append(
                    f"{kind}:stale={stale},never_fetched={never}"
                )
            normalized_kinds[kind] = {
                "sla_seconds": reported_seconds,
                "total_targets": total,
                "stale_targets": stale,
                "never_fetched_targets": never,
            }

    normalized_aggregate = {}
    if isinstance(aggregate, Mapping):
        total = _non_negative_metric(aggregate, "total_targets")
        stale = _non_negative_metric(aggregate, "stale_targets")
        never = _non_negative_metric(aggregate, "never_fetched_targets")
        within_sla = aggregate.get("all_within_sla") is True
        if total == 0:
            violations.append("current_scope:total_targets=0")
        if stale or not within_sla:
            violations.append(
                "current_scope:"
                f"stale={stale},never_fetched={never},"
                f"all_within_sla={within_sla}"
            )
        normalized_aggregate = {
            "total_targets": total,
            "stale_targets": stale,
            "never_fetched_targets": never,
            "all_within_sla": within_sla,
        }

    if violations:
        from airflow.exceptions import AirflowException

        raise AirflowException(
            "FBref current-scope freshness failed: " + "; ".join(violations)
        )
    return {
        "status": "passed",
        "run_type": normalized_run_type,
        "current_scope_freshness": normalized_aggregate,
        "freshness_by_page_kind": normalized_kinds,
    }


def _control_run_id(*, airflow_run_id: str, dag_id: str) -> str:
    from scrapers.fbref.control import make_control_run_id

    return make_control_run_id(airflow_run_id, dag_id=dag_id)


def _json_safe_control_result(result: Mapping[str, object]) -> dict:
    """Keep control-task XComs JSON-safe across UUID/datetime drivers."""

    safe = {}
    for key, value in result.items():
        if value is None or isinstance(value, (bool, int, float, str)):
            safe[str(key)] = value
        elif hasattr(value, "isoformat"):
            safe[str(key)] = value.isoformat()
        else:
            safe[str(key)] = str(value)
    return safe


def initialize_fbref_run(
    *,
    airflow_run_id: str,
    dag_id: str,
    run_type: str,
    request_limit=DEFAULT_REQUEST_LIMIT,
    byte_limit_mb=DEFAULT_BYTE_LIMIT // MIB,
    shard_size=DEFAULT_SHARD_SIZE,
    reservation_mb=DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
    domain_interval_seconds=3.0,
) -> str:
    settings = _settings(
        run_type=run_type,
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
        reservation_mb=reservation_mb,
        domain_interval_seconds=domain_interval_seconds,
    )
    run_id = _pipeline().initialize_run(
        airflow_run_id=airflow_run_id,
        dag_id=dag_id,
        settings=settings,
    )
    logger.info("FBref control run initialized: %s (%s)", run_id, run_type)
    return run_id


def acquire_fbref_publication_lock(
    *, airflow_run_id: str, dag_id: str
) -> dict:
    """Fence every FBref Bronze/Silver writer until publication is terminal."""

    run_id = _control_run_id(
        airflow_run_id=airflow_run_id, dag_id=dag_id
    )
    result = _control_store().acquire_publication_lock(
        run_id,
        dag_id=dag_id,
        ttl_seconds=FBREF_PUBLICATION_LOCK_TTL_SECONDS,
    )
    logger.info(
        "FBref publication lock acquired: owner=%s idempotent=%s",
        run_id,
        result.get("idempotent"),
    )
    return _json_safe_control_result(result)


def release_fbref_publication_lock(
    *,
    airflow_run_id: Optional[str] = None,
    dag_id: Optional[str] = None,
    control_run_id: Optional[str] = None,
) -> dict:
    """Idempotently release one exact publication generation."""

    import uuid

    if control_run_id is not None and str(control_run_id).strip():
        run_id = str(uuid.UUID(str(control_run_id).strip()))
    else:
        if not airflow_run_id or not dag_id:
            raise ValueError(
                "release requires control_run_id or Airflow DAG/run identity"
            )
        run_id = _control_run_id(
            airflow_run_id=airflow_run_id, dag_id=dag_id
        )
    result = _control_store().release_publication_lock(run_id)
    logger.info(
        "FBref publication lock release: owner=%s released=%s idempotent=%s",
        run_id,
        result.get("released"),
        result.get("idempotent"),
    )
    return _json_safe_control_result(result)


def finalize_fbref_publication_lock(
    *, airflow_run_id: str, dag_id: str, **context
) -> dict:
    """Release after Silver success; otherwise retain the fence and fail."""

    from airflow.exceptions import AirflowException

    dag_run = context.get("dag_run")
    instances = (
        dag_run.get_task_instances() if dag_run is not None else []
    )
    states = {
        str(getattr(instance, "task_id", "")): str(
            getattr(instance, "state", "") or ""
        ).casefold()
        for instance in instances
    }
    acquire_state = states.get("acquire_publication_lock", "missing")
    plan_state = states.get("plan_backfill", "missing")
    if acquire_state == "skipped" and plan_state == "success":
        return {
            "released": False,
            "dry_run": True,
            "status": "not_acquired",
        }
    if acquire_state != "success":
        raise AirflowException(
            "FBref publication lock was not acquired; final source verdict "
            f"fails closed (state={acquire_state})"
        )
    silver_state = states.get("trigger_silver_transform", "missing")
    if silver_state != "success":
        if silver_state in {"skipped", "upstream_failed"}:
            release_fbref_publication_lock(
                airflow_run_id=airflow_run_id, dag_id=dag_id
            )
        raise AirflowException(
            "FBref Silver publication did not succeed; publication lock "
            + (
                "released because the child never started "
                if silver_state in {"skipped", "upstream_failed"}
                else "retained because child state is ambiguous "
            )
            + f"(state={silver_state})"
        )
    return release_fbref_publication_lock(
        airflow_run_id=airflow_run_id, dag_id=dag_id
    )


def seed_fbref_competition_index(
    *, airflow_run_id: str, dag_id: str
) -> str:
    run_id = _control_run_id(
        airflow_run_id=airflow_run_id, dag_id=dag_id
    )
    target_id = _pipeline().seed_competition_index()
    logger.info("FBref run %s seeded %s", run_id, target_id)
    return target_id


def seed_fbref_historical_seasons(
    *,
    airflow_run_id: str,
    dag_id: str,
    request_limit=DEFAULT_REQUEST_LIMIT,
    byte_limit_mb=DEFAULT_BYTE_LIMIT // MIB,
    shard_size=DEFAULT_SHARD_SIZE,
    reservation_mb=DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
) -> dict:
    run_id = _control_run_id(
        airflow_run_id=airflow_run_id, dag_id=dag_id
    )
    settings = _settings(
        run_type="backfill",
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
        reservation_mb=reservation_mb,
    )
    result = _pipeline().seed_historical_seasons(
        run_id=run_id, settings=settings, limit=int(shard_size)
    )
    logger.info("FBref backfill seed for %s: %s", run_id, result)
    return result


def run_recovery_wave(
    *,
    airflow_run_id: str,
    dag_id: str,
    page_kinds: Sequence[str],
    run_type: str,
    request_limit=DEFAULT_REQUEST_LIMIT,
    byte_limit_mb=DEFAULT_BYTE_LIMIT // MIB,
    shard_size=DEFAULT_SHARD_SIZE,
    reservation_mb=DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
) -> dict:
    """Parse reusable successful raw before any live transport is opened."""

    settings = _settings(
        run_type=run_type,
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
        reservation_mb=reservation_mb,
    )
    pipeline = _pipeline()
    control_run_id = _control_run_id(
        airflow_run_id=airflow_run_id, dag_id=dag_id
    )
    aggregate: dict[str, object] = {"batches": 0}
    while True:
        result = pipeline.recover_unprocessed_wave(
            control_run_id,
            page_kinds=list(page_kinds),
            settings=settings,
        ).as_dict()
        cohort_size = int(result.get("cohort_size") or 0)
        if cohort_size == 0:
            break
        parsed = int(result.get("parsed") or 0)
        aggregate["batches"] = int(aggregate["batches"]) + 1
        for key, value in result.items():
            if key == "failures":
                aggregate.setdefault(key, [])
                aggregate[key].extend(value)
            elif isinstance(value, bool):
                aggregate[key] = bool(aggregate.get(key, False)) or value
            elif isinstance(value, int):
                aggregate[key] = int(aggregate.get(key, 0)) + value
        if parsed == 0:
            raise RuntimeError(
                "FBref raw recovery made no progress with "
                f"{cohort_size} observation(s) still selected"
            )
    aggregate.setdefault("cohort_size", 0)
    aggregate.setdefault("parsed", 0)
    logger.info(
        "FBref raw recovery drain: %s",
        json.dumps(aggregate, sort_keys=True),
    )
    return aggregate


def fetch_fbref_wave(
    *,
    airflow_run_id: str,
    dag_id: str,
    worker_id: str,
    page_kinds: Sequence[str],
    run_type: str,
    request_limit=DEFAULT_REQUEST_LIMIT,
    byte_limit_mb=DEFAULT_BYTE_LIMIT // MIB,
    shard_size=DEFAULT_SHARD_SIZE,
    reservation_mb=DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
    domain_interval_seconds=3.0,
    proxy_file: Optional[str] = DEFAULT_PROXY_FILE,
) -> dict:
    """Run one bounded fetch wave in a dedicated, unforked subprocess.

    The wave is the only task that drives Camoufox, and Playwright's sync API
    deadlocks inside a process forked from the multi-threaded scheduler: the
    browser starts, the navigation never opens a socket, and no timeout fires.
    The wave therefore executes through ``run_fbref_fetch_wave.py`` and this
    callable only relays its bounded result, so every control-plane budget,
    lease, and validation gate stays exactly where it was.
    """

    validate_fbref_runtime_limits(
        run_type=run_type,
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
    )

    command = [
        sys.executable,
        FETCH_WAVE_RUNNER,
        "--control-run-id",
        _control_run_id(airflow_run_id=airflow_run_id, dag_id=dag_id),
        "--worker-id",
        worker_id,
        "--page-kinds",
        ",".join(page_kinds),
        "--run-type",
        run_type,
        "--request-limit",
        str(request_limit),
        "--byte-limit-mb",
        str(byte_limit_mb),
        "--shard-size",
        str(shard_size),
        "--reservation-mb",
        str(reservation_mb),
        "--domain-interval-seconds",
        str(domain_interval_seconds),
    ]
    if proxy_file:
        command += ["--proxy-file", proxy_file]

    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=FETCH_WAVE_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        # A hung clearance keeps the browser inside Playwright's event loop and
        # never returns, holding this wave's fenced leases until they expire.
        # Fail closed and synchronously abort the control run so retries cannot
        # inherit its fences. Log what the wave printed, or the hang is opaque.
        logger.warning(
            "FBref fetch wave timed out.\nstdout:\n%s\nstderr:\n%s",
            _decoded_stream(exc.stdout),
            _decoded_stream(exc.stderr),
        )
        _abort_failed_fetch_subprocess(
            airflow_run_id=airflow_run_id,
            dag_id=dag_id,
            error_class="FetchWaveSubprocessTimeout",
            error_message=(
                "FBref fetch wave subprocess exceeded "
                f"{FETCH_WAVE_TIMEOUT_SECONDS}s"
            ),
        )
        raise RuntimeError(
            "FBref fetch wave subprocess exceeded "
            f"{FETCH_WAVE_TIMEOUT_SECONDS}s and was killed"
        ) from exc
    if completed.stderr:
        logger.info("FBref fetch wave stderr:\n%s", completed.stderr.strip())
    if completed.returncode != 0:
        _abort_failed_fetch_subprocess(
            airflow_run_id=airflow_run_id,
            dag_id=dag_id,
            error_class="FetchWaveSubprocessFailure",
            error_message=(
                "FBref fetch wave subprocess failed with exit code "
                f"{completed.returncode}"
            ),
        )
        raise RuntimeError(
            "FBref fetch wave subprocess failed with exit code "
            f"{completed.returncode}"
        )

    result = _parse_fetch_wave_result(completed.stdout)
    logger.info("FBref fetch wave: %s", json.dumps(result, sort_keys=True))
    return result


def _abort_failed_fetch_subprocess(
    *,
    airflow_run_id: str,
    dag_id: str,
    error_class: str,
    error_message: str,
) -> None:
    """Synchronously release the failed wave's leases and reservations."""

    try:
        abort_fbref_run(
            airflow_run_id=airflow_run_id,
            dag_id=dag_id,
            error_class=error_class,
            error_message=error_message,
        )
    except Exception:  # noqa: BLE001 - preserve the subprocess root cause
        logger.exception(
            "Could not synchronously abort FBref control run after fetch "
            "subprocess failure"
        )


def _decoded_stream(stream) -> str:
    """Best-effort text of a killed subprocess stream (bytes or str, may be None)."""
    if stream is None:
        return "<empty>"
    if isinstance(stream, bytes):
        stream = stream.decode("utf-8", "replace")
    text = str(stream).strip()
    return text[-8000:] if text else "<empty>"


def _parse_fetch_wave_result(stdout: str) -> dict:
    """Return the wave result the runner printed, or fail closed."""

    for line in reversed(stdout.splitlines()):
        if line.startswith(FETCH_WAVE_RESULT_PREFIX):
            return json.loads(line[len(FETCH_WAVE_RESULT_PREFIX):])
    raise RuntimeError("FBref fetch wave subprocess emitted no result document")


def parse_fbref_wave(
    *,
    airflow_run_id: str,
    dag_id: str,
    page_kinds: Sequence[str],
    run_type: str,
    source_control_run_id: Optional[str] = None,
    request_limit=DEFAULT_REQUEST_LIMIT,
    byte_limit_mb=DEFAULT_BYTE_LIMIT // MIB,
    shard_size=DEFAULT_SHARD_SIZE,
    reservation_mb=DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
) -> dict:
    settings = _settings(
        run_type=run_type,
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
        reservation_mb=reservation_mb,
    )
    normalized_source_run_id = (
        None
        if source_control_run_id is None
        or not str(source_control_run_id).strip()
        else str(source_control_run_id).strip()
    )
    if settings.run_type == "replay" and normalized_source_run_id is None:
        raise ValueError("Replay requires source_control_run_id")
    result = _pipeline().parse_wave(
        _control_run_id(airflow_run_id=airflow_run_id, dag_id=dag_id),
        page_kinds=list(page_kinds),
        settings=settings,
        source_run_id=normalized_source_run_id,
    ).as_dict()
    logger.info("FBref offline parse wave: %s", json.dumps(result, sort_keys=True))
    return result


def validate_fbref_run(
    *,
    airflow_run_id: str,
    dag_id: str,
    source_control_run_id: Optional[str] = None,
) -> dict:
    summary = _pipeline().validate_and_finish(
        _control_run_id(airflow_run_id=airflow_run_id, dag_id=dag_id),
        replay_source_run_id=(
            None
            if source_control_run_id is None
            or not str(source_control_run_id).strip()
            else str(source_control_run_id).strip()
        ),
    )
    logger.info(
        "FBref validated run: requests=%s bytes=%s targets=%s datasets=%s",
        summary.get("requests_used"),
        summary.get("bytes_used"),
        summary.get("target_counts"),
        summary.get("dataset_validation_counts"),
    )
    logger.info("FBref control metrics: %s", json.dumps(summary, default=str))
    _record_control_traffic(summary, airflow_run_id=airflow_run_id)
    return summary


def abort_fbref_run(
    *,
    airflow_run_id: str,
    dag_id: str,
    error_class: str = "AirflowDagFailure",
    error_message: str = "Airflow DAG reached a terminal failure",
) -> dict:
    """Abort one deterministic control run without constructing data clients."""

    result = _control_store().abort_run(
        _control_run_id(airflow_run_id=airflow_run_id, dag_id=dag_id),
        error_class=error_class,
        error_message=error_message,
    )
    logger.warning(
        "FBref control run abort result: status=%s released=%s settled=%s",
        result.get("status"),
        result.get("targets_released"),
        result.get("reservations_settled"),
    )
    return result


def fbref_dag_failure_callback(context: dict) -> None:
    """Best-effort state cleanup; task callbacks retain alert ownership."""

    try:
        dag_run = context.get("dag_run")
        dag = context.get("dag")
        task_instance = context.get("task_instance") or context.get("ti")
        airflow_run_id = (
            getattr(dag_run, "run_id", None) or context.get("run_id")
        )
        dag_id = (
            getattr(dag_run, "dag_id", None)
            or getattr(dag, "dag_id", None)
        )
        if not airflow_run_id or not dag_id:
            logger.warning("FBref DAG failure callback lacks run identity")
            return
        task_id = getattr(task_instance, "task_id", None) or "unknown"
        abort_fbref_run(
            airflow_run_id=str(airflow_run_id),
            dag_id=str(dag_id),
            error_class="AirflowDagFailure",
            error_message=f"Airflow DAG failed after task {task_id}",
        )
        if task_id not in {
            "trigger_silver_transform",
            "release_publication_lock",
            "unknown",
        }:
            try:
                release_fbref_publication_lock(
                    airflow_run_id=str(airflow_run_id),
                    dag_id=str(dag_id),
                )
            except Exception:  # noqa: BLE001 - another owner stays fenced
                logger.exception("FBref publication lock cleanup failed")
        else:
            logger.warning(
                "Retaining FBref publication lock after ambiguous child "
                "or cleanup failure (task=%s)",
                task_id,
            )
    except Exception:  # noqa: BLE001 - callbacks must not mask DAG state
        logger.exception("FBref control-run abort callback failed")


__all__ = [
    "acquire_fbref_publication_lock",
    "abort_fbref_run",
    "choose_fbref_backfill_mode",
    "export_fbref_publication_scope",
    "finalize_fbref_publication_lock",
    "fetch_fbref_wave",
    "fbref_dag_failure_callback",
    "initialize_fbref_run",
    "parse_fbref_wave",
    "plan_fbref_backfill",
    "release_fbref_publication_lock",
    "run_recovery_wave",
    "seed_fbref_competition_index",
    "seed_fbref_historical_seasons",
    "validate_fbref_current_scope_freshness",
    "validate_fbref_production_readiness",
    "validate_fbref_publication_scope",
    "validate_fbref_runtime_limits",
    "validate_fbref_run",
]
