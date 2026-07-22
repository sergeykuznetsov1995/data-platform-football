"""Airflow callables for the durable FBref pipeline.

The functions keep DAG files declarative and recompute the deterministic
control run UUID instead of passing scheduler-local files or mutable XCom
payloads between workers.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Mapping, Optional, Sequence

from scrapers.fbref.policy import (
    PUBLICATION_FRESHNESS_PAGE_KINDS,
    PUBLICATION_REQUIRED_PAGE_KINDS,
)
from scrapers.fbref.settings import (
    DEFAULT_BYTE_LIMIT,
    DEFAULT_DOMAIN_INTERVAL_SECONDS,
    DEFAULT_REQUEST_LIMIT,
    DEFAULT_REQUEST_RESERVATION_BYTES,
    DEFAULT_SHARD_SIZE,
    MIB,
)


logger = logging.getLogger(__name__)

LEGACY_SCRAPER_PYTHON_ENV = "LEGACY_SCRAPER_PYTHON"
DEFAULT_LEGACY_SCRAPER_PYTHON = "/opt/legacy-scraper-venv/bin/python"
LIVE_WAVES_RUNNER = "/opt/airflow/dags/scripts/run_fbref_live_waves.py"
LIVE_WAVES_PYTHONPATH = "/opt/airflow"
LIVE_WAVES_RESULT_PREFIX = "FBREF_LIVE_WAVES_RESULT:"
LIVE_WAVES_TIMEOUT_SECONDS = 110 * 60
LIVE_WAVES_TERMINATION_GRACE_SECONDS = 10
LIVE_WAVES_KILL_GRACE_SECONDS = 10
FBREF_MAX_LIVE_BATCHES = 16
FBREF_ACCEPTANCE_OUTPUT_ROOT_ENV = "FBREF_ACCEPTANCE_OUTPUT_ROOT"
DEFAULT_FBREF_ACCEPTANCE_OUTPUT_ROOT = (
    "/opt/airflow/logs/fbref_acceptance"
)
FBREF_RAW_BASELINE_FILENAME = "raw_inventory_before.json"


class _LiveRunnerTermination(SystemExit):
    """Interrupt ``communicate`` so the detached child can be reaped first."""

    def __init__(self, signum: int) -> None:
        self.signum = int(signum)
        super().__init__(128 + self.signum)


class _LiveRunnerLifecycle:
    """Mutable ownership record shared across the protected spawn boundary."""

    def __init__(self) -> None:
        self.process = None
        self.process_group_id: Optional[int] = None
        self.handler_state: Optional[dict] = None
        self.previous_sigterm_handler = None
        self.handler_installed = False


def _install_live_runner_sigterm_handler() -> tuple[dict, object]:
    """Install a handler that defers cancellation until the PGID is saved."""

    previous = signal.getsignal(signal.SIGTERM)
    state = {"armed": False, "pending_signum": None}

    def handle(signum, _frame) -> None:
        if state["armed"]:
            # Raise only once.  Every later SIGTERM is latched while the
            # process group is being reaped, so cancellation cannot interrupt
            # the cleanup path and strand the paid browser tree.
            state["armed"] = False
            raise _LiveRunnerTermination(signum)
        state["pending_signum"] = int(signum)

    try:
        signal.signal(signal.SIGTERM, handle)
    except ValueError as exc:  # a non-main-thread task runner
        raise RuntimeError(
            "FBref live runner must be spawned from the main thread"
        ) from exc
    return state, previous

# Runtime limits are repeated here intentionally: the Airflow boundary must
# reject an unsafe dag_run.conf even when Param validation is bypassed.  The
# only supported live profiles are the measured production budget and the
# separately bounded canary budget.
FBREF_PRODUCTION_REQUEST_LIMIT = 200
FBREF_PRODUCTION_BYTE_LIMIT_MB = 100
FBREF_CANARY_REQUEST_LIMIT = 100
FBREF_CANARY_BYTE_LIMIT_MB = 50
FBREF_MAX_WARM_SESSION_TARGETS = 25
FBREF_BOOTSTRAP_DAG_ID = "dag_bootstrap_fbref"
FBREF_BOOTSTRAP_REQUIRED_TASK_IDS = (
    "validate_production_readiness",
    "initialize_run",
    "acquire_publication_lock",
    "seed_competition_index",
    "capture_raw_baseline",
    "recover_raw_before_fetch",
    "run_live_waves",
    "audit_raw_integrity",
    "validate_bootstrap_run",
    "release_bootstrap_publication_lock",
)
FBREF_PUBLICATION_SCOPE_TABLE = "fbref_target_scope"
FBREF_PUBLICATION_LOCK_TTL_SECONDS = 8 * 24 * 60 * 60
FBREF_LIVE_BUDGET_PROFILES = {
    (FBREF_PRODUCTION_REQUEST_LIMIT, FBREF_PRODUCTION_BYTE_LIMIT_MB): (
        "production"
    ),
    (FBREF_CANARY_REQUEST_LIMIT, FBREF_CANARY_BYTE_LIMIT_MB): "canary",
}


def _legacy_scraper_python() -> str:
    """Return the explicit isolated browser runner or fail before claiming work."""

    raw = os.environ.get(
        LEGACY_SCRAPER_PYTHON_ENV,
        DEFAULT_LEGACY_SCRAPER_PYTHON,
    )
    path = Path(str(raw))
    if not path.is_absolute():
        raise RuntimeError(f"{LEGACY_SCRAPER_PYTHON_ENV} must be an absolute path")
    try:
        metadata = path.stat()
    except OSError as exc:
        raise RuntimeError("isolated FBref browser interpreter is unavailable") from exc
    if not path.is_file() or not os.access(path, os.X_OK):
        raise RuntimeError("isolated FBref browser interpreter is not executable")
    if metadata.st_size <= 0:
        raise RuntimeError("isolated FBref browser interpreter is empty")
    # A venv's bin/python is normally a symlink. Resolving it would bypass the
    # venv and execute the system interpreter, so preserve the configured path.
    return str(path)


def _live_runner_environment() -> dict[str, str]:
    """Return inherited runtime settings with one fixed child import root."""

    environment = dict(os.environ)
    # The isolated legacy venv does not inherit the scheduler interpreter's
    # attested sys.path. Give only this child the read-only application root;
    # never trust a host/container PYTHONPATH or weaken safe-path handling.
    environment["PYTHONPATH"] = LIVE_WAVES_PYTHONPATH
    environment["PYTHONSAFEPATH"] = "1"
    return environment

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
    PUBLICATION_REQUIRED_PAGE_KINDS
)
FBREF_PUBLICATION_CURRENT_PAGE_KINDS = PUBLICATION_FRESHNESS_PAGE_KINDS


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
    domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
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
    bootstrap_only=False,
    dag_run_type=None,
) -> dict:
    """Fail closed on every production dependency before creating a run."""

    from utils.alerts import validate_alert_environment

    from scrapers.base.trino_manager import TrinoTableManager
    from scrapers.fbref.raw_store import RawPageStore
    from scrapers.fbref.readiness import (
        check_raw_store_roundtrip,
        check_trino_roundtrip,
        validate_camoufox_runtime,
        validate_fbref_proxy_meter,
        validate_raw_store_uri,
    )

    limits = validate_fbref_runtime_limits(
        run_type=run_type,
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
    )
    if limits["run_type"] == "current":
        execution = validate_fbref_current_execution_mode(
            bootstrap_only=bootstrap_only,
            dag_run_type=dag_run_type,
            request_limit=request_limit,
            byte_limit_mb=byte_limit_mb,
            shard_size=shard_size,
        )
    else:
        if _boolean_parameter(bootstrap_only, name="bootstrap_only"):
            raise ValueError(
                "FBref bootstrap_only is supported only for current runs"
            )
        execution = {
            "bootstrap_only": False,
            "execution_mode": limits["run_type"],
            "publication_eligible": True,
        }
    alert = validate_alert_environment("prod")
    raw_uri = validate_raw_store_uri(os.environ.get("FBREF_RAW_STORE_URI"))
    migrations = _control_store().validate_migrations()
    raw_store = RawPageStore.from_uri(raw_uri)
    raw_health = check_raw_store_roundtrip(raw_store)
    trino_health = check_trino_roundtrip(TrinoTableManager())
    proxy = {}
    browser = {}
    if limits["run_type"] != "replay":
        browser = validate_camoufox_runtime()
        proxy = validate_fbref_proxy_meter(
            os.environ.get("FBREF_PROXY_CONTROL_URL"),
            control_token=os.environ.get("FBREF_PROXY_CONTROL_TOKEN"),
            required_bytes=int(limits["byte_limit_mb"]) * MIB,
            minimum_configured_exits=(
                4 if limits["profile"] == "production" else 1
            ),
        )
    return {
        **alert,
        **limits,
        **execution,
        **proxy,
        "dependencies": {
            "control_migrations": migrations,
            "raw_store": raw_health,
            "trino": trino_health,
            "camoufox": browser or {"status": "not_required"},
            "proxy_meter": proxy or {"status": "not_required"},
        },
    }


def _boolean_parameter(value, *, name: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().casefold()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def validate_fbref_current_execution_mode(
    *,
    bootstrap_only=False,
    dag_run_type=None,
    request_limit=FBREF_PRODUCTION_REQUEST_LIMIT,
    byte_limit_mb=FBREF_PRODUCTION_BYTE_LIMIT_MB,
    shard_size=FBREF_MAX_WARM_SESSION_TARGETS,
) -> dict:
    """Resolve publishing, canary, or explicit manual bootstrap mode.

    ``bootstrap_only`` is deliberately stricter than the existing canary: it
    may run only from a manual DagRun and only with the exact measured
    production profile.  This check is repeated at every Airflow branch that
    could otherwise select a publishing task.
    """

    limits = validate_fbref_runtime_limits(
        run_type="current",
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
    )
    enabled = _boolean_parameter(bootstrap_only, name="bootstrap_only")
    normalized_dag_run_type = (
        None
        if dag_run_type is None
        else str(dag_run_type).strip().casefold()
    )
    if enabled:
        if normalized_dag_run_type != "manual":
            raise ValueError(
                "FBref bootstrap_only requires a manual DagRun"
            )
        if (
            limits["profile"] != "production"
            or limits["shard_size"] != FBREF_MAX_WARM_SESSION_TARGETS
        ):
            raise ValueError(
                "FBref bootstrap_only requires exactly 200 requests, "
                "100 MiB, and shard_size 25"
            )
        execution_mode = "bootstrap_only"
        publication_eligible = False
    elif limits["profile"] == "canary":
        execution_mode = "canary_nonpublishing"
        publication_eligible = False
    else:
        execution_mode = "publishing"
        publication_eligible = True
    return {
        **limits,
        "bootstrap_only": enabled,
        "dag_run_type": normalized_dag_run_type,
        "execution_mode": execution_mode,
        "publication_eligible": publication_eligible,
    }


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
    from scrapers.fbref.pipeline import backfill_season_cohort_capacity

    effective_limit = backfill_season_cohort_capacity(settings)
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


def _require_fbref_publication_mode(run: Mapping[str, object]) -> None:
    """Physically fence explicit non-publishing runs from publication."""

    metadata = run.get("metadata")
    if not isinstance(metadata, Mapping):
        # Runs created before execution-mode evidence was introduced have no
        # such marker. Their existing publication contract remains unchanged.
        return
    execution_mode = str(metadata.get("execution_mode") or "").casefold()
    explicitly_ineligible = (
        "publication_eligible" in metadata
        and metadata.get("publication_eligible") is not True
    )
    if (
        explicitly_ineligible
        or metadata.get("bootstrap_only") is True
        or execution_mode in {"bootstrap_only", "canary_nonpublishing"}
    ):
        raise RuntimeError(
            "FBref control run is explicitly non-publishing "
            f"(execution_mode={execution_mode or 'unknown'})"
        )


def export_fbref_publication_scope(
    *, airflow_run_id: str, dag_id: str
) -> dict:
    """Atomically publish one immutable control-run scope generation."""

    control_run_id = _control_run_id(
        airflow_run_id=airflow_run_id, dag_id=dag_id
    )
    control = _control_store()
    run = control.get_run(control_run_id)
    if run is None:
        raise RuntimeError("FBref publication control run is missing")
    _require_fbref_publication_mode(run)
    with control.guard_publication_lock(control_run_id, source="fbref"):
        return _export_fbref_publication_scope_under_guard(
            control=control,
            control_run_id=control_run_id,
        )


def _export_fbref_publication_scope_under_guard(
    *, control, control_run_id: str
) -> dict:
    """Write the scope while a PostgreSQL row lock fences its owner."""

    from datetime import datetime, timezone

    import pandas as pd

    from scrapers.base.trino_manager import TrinoTableManager
    from scrapers.fbref.typed_bronze import (
        COMPATIBILITY_LEAGUE_BY_COMPETITION_ID,
        TypedSourceContext,
    )

    rows = control.list_publication_scope(source="fbref")
    if not rows:
        raise RuntimeError("FBref publication scope is empty")
    exported_at = datetime.now(timezone.utc).replace(tzinfo=None)
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
    _require_fbref_publication_mode(run)
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
    *,
    airflow_run_id: str,
    dag_id: str,
    run_type: str,
    fail_fast: bool = True,
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
    _require_fbref_publication_mode(summary)

    aggregate = summary.get("publication_scope_freshness")
    if not isinstance(aggregate, Mapping):
        # Rolling upgrade compatibility for summaries written before the
        # publication-critical split.
        aggregate = summary.get("current_scope_freshness")
    by_kind = summary.get("freshness_by_page_kind")
    if not isinstance(aggregate, Mapping) and not isinstance(by_kind, Mapping):
        raise RuntimeError(
            "FBref control summary has no current-scope freshness evidence"
        )

    violations = []
    if (
        normalized_run_type == "backfill"
        and _non_negative_metric(
            summary, "promotion_pending_match_count"
        )
        != 0
    ):
        violations.append(
            "promotion_pending_match_count="
            f"{int(summary['promotion_pending_match_count'])}"
        )
    normalized_kinds = {}
    if isinstance(by_kind, Mapping):
        missing = sorted(FBREF_REQUIRED_CURRENT_PAGE_KINDS - set(by_kind))
        if missing:
            violations.append("missing_page_kinds=" + ",".join(missing))
        for raw_kind, raw_metrics in by_kind.items():
            kind = str(raw_kind)
            if kind not in FBREF_PUBLICATION_CURRENT_PAGE_KINDS:
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
        from airflow.exceptions import AirflowException, AirflowFailException

        # A verdict checked under the publication lock cannot change on retry.
        # The backfill preflight runs before that lock and remains retryable so
        # a concurrent current refresh may finish first.
        error_type = AirflowFailException if fail_fast else AirflowException
        raise error_type(
            "FBref current-scope freshness failed: " + "; ".join(violations)
        )
    return {
        "status": "passed",
        "run_type": normalized_run_type,
        "publication_scope_freshness": normalized_aggregate,
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


def _fbref_acceptance_output_root() -> Path:
    configured = os.environ.get(
        FBREF_ACCEPTANCE_OUTPUT_ROOT_ENV,
        DEFAULT_FBREF_ACCEPTANCE_OUTPUT_ROOT,
    ).strip()
    path = Path(configured)
    if not configured or not path.is_absolute():
        raise ValueError(
            f"{FBREF_ACCEPTANCE_OUTPUT_ROOT_ENV} must be an absolute path"
        )
    return path.resolve()


def _fbref_raw_baseline_path(control_run_id: object) -> Path:
    import uuid

    normalized = str(uuid.UUID(str(control_run_id)))
    return (
        _fbref_acceptance_output_root()
        / normalized
        / FBREF_RAW_BASELINE_FILENAME
    )


def capture_fbref_raw_baseline(
    *, airflow_run_id: str, dag_id: str
) -> dict:
    """Persist one immutable pre-source inventory outside the raw prefix."""

    from scrapers.fbref.raw_audit import (
        capture_and_write_raw_inventory,
        open_authenticated_raw_inventory_cache,
        raw_baseline_anchor,
    )
    from scrapers.fbref.raw_store import RawPageStore

    control_run_id = _control_run_id(
        airflow_run_id=airflow_run_id, dag_id=dag_id
    )
    path = _fbref_raw_baseline_path(control_run_id)
    control = _control_store()
    reuse_inventory = None
    if not (path.exists() or path.is_symlink()):
        reuse_inventory = open_authenticated_raw_inventory_cache(
            _fbref_acceptance_output_root(), control
        )
    path, inventory, idempotent = capture_and_write_raw_inventory(
        RawPageStore.from_env(optional=False),
        path,
        reuse_inventory=reuse_inventory,
    )
    anchor = raw_baseline_anchor(
        inventory.summary, inventory.baseline_sha256
    )
    anchored = control.record_raw_baseline(control_run_id, anchor)
    result = {
        "status": "captured",
        "control_run_id": control_run_id,
        "object_count": int(inventory.summary["object_count"]),
        "encoded_bytes": int(inventory.summary["encoded_bytes"]),
        "fingerprint_sha256": str(
            inventory.summary["fingerprint_sha256"]
        ),
        "baseline_sha256": inventory.baseline_sha256,
        "baseline_path": str(path),
        "idempotent": bool(idempotent or anchored.get("idempotent")),
        "control_anchored": True,
    }
    logger.info("FBref raw baseline: %s", json.dumps(result, sort_keys=True))
    return result


def audit_fbref_raw_integrity(
    *,
    airflow_run_id: str,
    dag_id: str,
    run_type: str,
    source_control_run_id: Optional[str] = None,
) -> dict:
    """Gate Airflow publication on content-hashed raw evidence."""

    from scrapers.fbref.raw_audit import (
        audit_raw_fetches,
        cleanup_anchored_raw_inventory_indexes,
        load_audit_artifact,
        load_inventory_baseline,
        load_successful_run_attempts,
        open_disk_backed_inventory,
        promote_raw_inventory_cache,
        raw_baseline_anchor,
        successful_attempt_snapshot,
        write_audit_artifact,
    )
    from scrapers.fbref.raw_store import RawPageStore

    normalized_run_type = str(run_type).strip().casefold()
    if normalized_run_type not in {"current", "backfill", "replay"}:
        raise ValueError(f"Unknown FBref run_type: {run_type!r}")
    processing_run_id = _control_run_id(
        airflow_run_id=airflow_run_id, dag_id=dag_id
    )
    if normalized_run_type == "replay":
        if not source_control_run_id:
            raise ValueError("FBref replay raw audit requires source_control_run_id")
        audited_run_id = str(uuid.UUID(str(source_control_run_id).strip()))
    else:
        if source_control_run_id not in (None, ""):
            raise ValueError(
                "source_control_run_id is valid only for FBref replay"
            )
        audited_run_id = processing_run_id

    control = _control_store()
    root = _fbref_acceptance_output_root()
    baseline_path = _fbref_raw_baseline_path(processing_run_id)
    expected_anchor = control.get_raw_baseline(processing_run_id)
    if expected_anchor is None:
        raise RuntimeError(
            "FBref raw baseline has no immutable control-plane anchor"
        )

    existing_audit = control.get_raw_audit(processing_run_id)
    if existing_audit is not None:
        baseline_summary, baseline_sha256 = load_inventory_baseline(
            baseline_path
        )
        actual_anchor = raw_baseline_anchor(
            baseline_summary, baseline_sha256
        )
        if dict(expected_anchor) != actual_anchor:
            raise RuntimeError(
                "FBref raw baseline does not match its immutable "
                "control-plane anchor"
            )
        expected_existing = {
            "status": "passed",
            "run_type": normalized_run_type,
            "audited_control_run_id": audited_run_id,
            "processing_control_run_id": processing_run_id,
            "zero_delta_required": normalized_run_type == "replay",
        }
        if any(
            existing_audit.get(key) != value
            for key, value in expected_existing.items()
        ):
            raise RuntimeError(
                "Existing FBref raw audit anchor has the wrong run contract"
            )
        sealed_snapshot = control.seal_raw_fetch_attempts(audited_run_id)
        if (
            int(sealed_snapshot["successful_attempt_count"])
            != int(existing_audit["successful_attempt_count"])
            or str(sealed_snapshot["successful_attempt_ids_sha256"])
            != str(existing_audit["attempt_snapshot_sha256"])
        ):
            raise RuntimeError(
                "Existing FBref raw audit anchor differs from sealed attempts"
            )
        artifact_path = Path(str(existing_audit["artifact"]))
        if not artifact_path.is_absolute():
            raise RuntimeError("Existing FBref raw audit path is not absolute")
        try:
            artifact_path.parent.resolve().relative_to(root)
        except ValueError as exc:
            raise RuntimeError(
                "Existing FBref raw audit path is outside acceptance storage"
            ) from exc
        artifact, digest_path = load_audit_artifact(
            artifact_path,
            expected_sha256=str(existing_audit["artifact_sha256"]),
        )
        artifact_metadata = artifact.get("metadata") or {}
        if (
            str(artifact.get("status") or "") != "passed"
            or str(artifact.get("control_run_id") or "") != audited_run_id
            or int(artifact.get("successful_attempt_count") or -1)
            != int(existing_audit["successful_attempt_count"])
            or int(artifact.get("audited_attempt_count") or -1)
            != int(existing_audit["audited_attempt_count"])
            or list(artifact.get("failures") or [])
            or not isinstance(artifact_metadata, Mapping)
            or str(
                artifact_metadata.get("processing_control_run_id") or ""
            )
            != processing_run_id
            or str(
                artifact_metadata.get("raw_attempt_snapshot_sha256") or ""
            )
            != str(existing_audit["attempt_snapshot_sha256"])
        ):
            raise RuntimeError(
                "Existing FBref raw audit artifact differs from its anchor"
            )
        summary = {
            "status": "passed",
            "control_run_id": audited_run_id,
            "processing_control_run_id": processing_run_id,
            "successful_attempt_count": int(
                existing_audit["successful_attempt_count"]
            ),
            "audited_attempt_count": int(
                existing_audit["audited_attempt_count"]
            ),
            "failure_count": 0,
            "zero_delta_required": normalized_run_type == "replay",
            "attempt_snapshot_sha256": str(
                existing_audit["attempt_snapshot_sha256"]
            ),
            "artifact": str(artifact_path),
            "artifact_sha256": str(existing_audit["artifact_sha256"]),
            "sha256_sidecar": str(digest_path),
            "control_anchored": True,
            "idempotent": True,
        }
        summary["inventory_retention"] = (
            cleanup_anchored_raw_inventory_indexes(
                root,
                control,
                protected_control_run_ids={processing_run_id},
            )
        )
        logger.info(
            "FBref raw integrity: %s", json.dumps(summary, sort_keys=True)
        )
        return summary

    baseline = open_disk_backed_inventory(baseline_path)
    actual_anchor = raw_baseline_anchor(
        baseline.summary, baseline.baseline_sha256
    )
    if dict(expected_anchor) != actual_anchor:
        raise RuntimeError(
            "FBref raw baseline does not match its immutable control-plane "
            "anchor"
        )

    sealed_snapshot = control.seal_raw_fetch_attempts(audited_run_id)
    attempts = load_successful_run_attempts(control, audited_run_id)
    loaded_snapshot = successful_attempt_snapshot(attempts)
    comparable_seal = {
        key: sealed_snapshot[key]
        for key in (
            "schema_version",
            "successful_attempt_count",
            "successful_attempt_ids_sha256",
        )
    }
    if loaded_snapshot != comparable_seal:
        raise RuntimeError(
            "FBref successful-attempt evidence differs from its sealed "
            "control-plane snapshot"
        )
    metadata = {
        "airflow_run_id": str(airflow_run_id),
        "dag_id": str(dag_id),
        "processing_control_run_id": processing_run_id,
        "run_type": normalized_run_type,
        "raw_attempt_snapshot_sha256": str(
            loaded_snapshot["successful_attempt_ids_sha256"]
        ),
    }
    git_sha = str(os.environ.get("GIT_SHA") or "").strip()
    if git_sha:
        metadata["git_sha"] = git_sha
    result = audit_raw_fetches(
        RawPageStore.from_env(optional=False),
        attempts,
        control_run_id=audited_run_id,
        baseline_inventory=baseline,
        require_baseline=True,
        require_nonempty=True,
        require_zero_delta=normalized_run_type == "replay",
        metadata=metadata,
    )
    resealed_snapshot = control.seal_raw_fetch_attempts(audited_run_id)
    if {
        key: resealed_snapshot[key]
        for key in comparable_seal
    } != comparable_seal:
        raise RuntimeError(
            "FBref successful-attempt evidence changed during raw audit"
        )
    artifact_path, digest_path = write_audit_artifact(
        result,
        _fbref_acceptance_output_root(),
        artifact_id=(
            processing_run_id if normalized_run_type == "replay" else None
        ),
    )
    artifact_sha256 = digest_path.read_text(encoding="ascii").split()[0]
    summary = {
        "status": str(result["status"]),
        "control_run_id": audited_run_id,
        "processing_control_run_id": processing_run_id,
        "successful_attempt_count": int(result["successful_attempt_count"]),
        "audited_attempt_count": int(result["audited_attempt_count"]),
        "failure_count": len(result["failures"]),
        "zero_delta_required": normalized_run_type == "replay",
        "attempt_snapshot_sha256": str(
            loaded_snapshot["successful_attempt_ids_sha256"]
        ),
        "artifact": str(artifact_path),
        "artifact_sha256": artifact_sha256,
        "sha256_sidecar": str(digest_path),
    }
    logger.info(
        "FBref raw integrity: %s", json.dumps(summary, sort_keys=True)
    )
    if result["status"] != "passed":
        raise RuntimeError(
            "FBref raw integrity failed; "
            f"failure_count={summary['failure_count']}; "
            f"artifact={artifact_path}"
        )
    anchored = control.record_raw_audit(
        processing_run_id,
        {
            "schema_version": "fbref-raw-audit-anchor-v1",
            "status": "passed",
            "run_type": normalized_run_type,
            "audited_control_run_id": audited_run_id,
            "processing_control_run_id": processing_run_id,
            "successful_attempt_count": summary[
                "successful_attempt_count"
            ],
            "audited_attempt_count": summary["audited_attempt_count"],
            "failure_count": summary["failure_count"],
            "zero_delta_required": summary["zero_delta_required"],
            "attempt_snapshot_sha256": summary[
                "attempt_snapshot_sha256"
            ],
            "artifact_sha256": artifact_sha256,
            "artifact": str(artifact_path),
        },
    )
    summary["inventory_cache"] = promote_raw_inventory_cache(
        root, processing_run_id, control
    )
    summary["inventory_retention"] = cleanup_anchored_raw_inventory_indexes(
        root,
        control,
        protected_control_run_ids={processing_run_id},
    )
    summary["control_anchored"] = True
    summary["idempotent"] = bool(anchored.get("idempotent"))
    return summary


def initialize_fbref_run(
    *,
    airflow_run_id: str,
    dag_id: str,
    run_type: str,
    request_limit=DEFAULT_REQUEST_LIMIT,
    byte_limit_mb=DEFAULT_BYTE_LIMIT // MIB,
    shard_size=DEFAULT_SHARD_SIZE,
    reservation_mb=DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
    domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
    bootstrap_only=False,
    dag_run_type=None,
) -> str:
    settings = _settings(
        run_type=run_type,
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
        reservation_mb=reservation_mb,
        domain_interval_seconds=domain_interval_seconds,
    )
    normalized_run_type = str(run_type).strip().casefold()
    if normalized_run_type == "current":
        execution = validate_fbref_current_execution_mode(
            bootstrap_only=bootstrap_only,
            dag_run_type=dag_run_type,
            request_limit=request_limit,
            byte_limit_mb=byte_limit_mb,
            shard_size=shard_size,
        )
    else:
        if _boolean_parameter(bootstrap_only, name="bootstrap_only"):
            raise ValueError(
                "FBref bootstrap_only is supported only for current runs"
            )
        execution = {
            "bootstrap_only": False,
            "dag_run_type": (
                None
                if dag_run_type is None
                else str(dag_run_type).strip().casefold()
            ),
            "execution_mode": normalized_run_type,
            "publication_eligible": True,
        }
    control_execution = {
        "bootstrap_only": execution["bootstrap_only"],
        "dag_run_type": execution.get("dag_run_type"),
        "execution_mode": execution["execution_mode"],
        "publication_eligible": execution["publication_eligible"],
        "runtime_profile": execution.get("profile", normalized_run_type),
    }
    run_id = _pipeline().initialize_run(
        airflow_run_id=airflow_run_id,
        dag_id=dag_id,
        settings=settings,
        execution_metadata=control_execution,
    )
    logger.info(
        "FBref control run initialized: %s (%s, mode=%s)",
        run_id,
        run_type,
        execution["execution_mode"],
    )
    return run_id


def acquire_fbref_publication_lock(
    *,
    airflow_run_id: str,
    dag_id: str,
    ttl_seconds=FBREF_PUBLICATION_LOCK_TTL_SECONDS,
) -> dict:
    """Fence every FBref Bronze/Silver writer until publication is terminal."""

    run_id = _control_run_id(
        airflow_run_id=airflow_run_id, dag_id=dag_id
    )
    result = _control_store().acquire_publication_lock(
        run_id,
        dag_id=dag_id,
        ttl_seconds=int(ttl_seconds),
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
    *,
    airflow_run_id: str,
    dag_id: str,
    bootstrap_only=False,
    **context,
) -> dict:
    """Release exact locks while preserving an honest terminal DAG verdict."""

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
    bootstrap_mode = _boolean_parameter(
        bootstrap_only, name="bootstrap_only"
    )
    if bootstrap_mode:
        if str(dag_id) != FBREF_BOOTSTRAP_DAG_ID:
            raise AirflowException(
                "FBref bootstrap finalizer is allowed only for "
                f"{FBREF_BOOTSTRAP_DAG_ID}"
            )
        released = release_fbref_publication_lock(
            airflow_run_id=airflow_run_id, dag_id=dag_id
        )
        failed_states = {
            task_id: states.get(task_id, "missing")
            for task_id in FBREF_BOOTSTRAP_REQUIRED_TASK_IDS
            if states.get(task_id, "missing") != "success"
        }
        if failed_states:
            raise AirflowException(
                "FBref bootstrap lock was released, but its source verdict "
                "is red: "
                + json.dumps(failed_states, sort_keys=True)
            )
        return {
            **released,
            "bootstrap_only": True,
            "status": "released_after_successful_bootstrap",
        }

    canary_release_state = states.get(
        "release_canary_publication_lock", "missing"
    )
    canary_validation_state = states.get("validate_canary_run", "missing")
    if (
        acquire_state == "success"
        and canary_validation_state in {"success", "failed"}
    ):
        if (
            canary_validation_state == "success"
            and canary_release_state == "success"
        ):
            return {
                "released": True,
                "canary": True,
                "status": "released_by_canary_path",
            }
        release_fbref_publication_lock(
            airflow_run_id=airflow_run_id, dag_id=dag_id
        )
        raise AirflowException(
            "FBref canary lock was released, but its source verdict is red "
            f"(validation={canary_validation_state}, "
            f"release={canary_release_state})"
        )
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


def _process_group_exists(process_group_id: int) -> bool:
    """Probe the group itself; an exited leader says nothing about children."""

    try:
        os.killpg(int(process_group_id), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wait_for_process_group_exit(
    process_group_id: int, *, deadline: float
) -> bool:
    """Wait only until the supplied monotonic deadline for the whole group."""

    while _process_group_exists(process_group_id):
        remaining = float(deadline) - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(0.1, remaining))
    return True


def _terminate_process_group(
    process, process_group_id: Optional[int] = None
) -> tuple[str, str]:
    """Terminate the runner tree without an unbounded final wait."""

    pgid = int(
        process_group_id
        if process_group_id is not None
        else process.pid
    )

    def signal_group(sig) -> bool:
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            return False
        except OSError as exc:
            raise RuntimeError(
                f"could not signal FBref live process group {pgid}"
            ) from exc
        return True

    def partial(exc: BaseException) -> tuple[str, str]:
        return (
            getattr(exc, "stdout", None)
            or getattr(exc, "output", None)
            or "",
            getattr(exc, "stderr", None) or "",
        )

    try:
        term_deadline = (
            time.monotonic() + LIVE_WAVES_TERMINATION_GRACE_SECONDS
        )
        term_sent = signal_group(signal.SIGTERM)
        term_output: tuple[str, str]
        try:
            term_output = process.communicate(
                timeout=LIVE_WAVES_TERMINATION_GRACE_SECONDS
            )
        except subprocess.TimeoutExpired as term_exc:
            term_output = partial(term_exc)
    except BaseException:
        # A second cancellation while TERM cleanup is running must not leave
        # the detached group alive. Do not mask it after issuing SIGKILL.
        kill_deadline = time.monotonic() + LIVE_WAVES_KILL_GRACE_SECONDS
        kill_sent = signal_group(signal.SIGKILL)
        try:
            process.communicate(timeout=LIVE_WAVES_KILL_GRACE_SECONDS)
        except BaseException:
            pass
        if kill_sent and not _wait_for_process_group_exit(
            pgid, deadline=kill_deadline
        ):
            logger.critical(
                "FBref live process group %s still exists after SIGKILL", pgid
            )
        raise

    if not term_sent or _wait_for_process_group_exit(
        pgid, deadline=term_deadline
    ):
        return term_output

    kill_deadline = time.monotonic() + LIVE_WAVES_KILL_GRACE_SECONDS
    kill_sent = signal_group(signal.SIGKILL)
    kill_output = ("", "")
    try:
        kill_output = process.communicate(timeout=LIVE_WAVES_KILL_GRACE_SECONDS)
    except subprocess.TimeoutExpired as kill_exc:
        kill_output = partial(kill_exc)
    if kill_sent and not _wait_for_process_group_exit(
        pgid, deadline=kill_deadline
    ):
        raise RuntimeError(
            f"FBref live process group {pgid} survived SIGKILL grace"
        )
    return kill_output if any(kill_output) else term_output


def _spawn_live_runner(
    command, lifecycle: _LiveRunnerLifecycle
) -> None:
    """Populate caller-owned state before arming cancellation exceptions."""

    handler_state, previous_handler = (
        _install_live_runner_sigterm_handler()
    )
    lifecycle.handler_state = handler_state
    lifecycle.previous_sigterm_handler = previous_handler
    lifecycle.handler_installed = True
    if handler_state["pending_signum"] is not None:
        pending_signum = int(handler_state["pending_signum"])
        handler_state["pending_signum"] = None
        raise _LiveRunnerTermination(pending_signum)
    lifecycle.process = subprocess.Popen(
        command,
        env=_live_runner_environment(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    # start_new_session makes the child's PID the durable PGID even if the
    # leader exits before cleanup begins. Keep it in caller-owned state so a
    # signal immediately after this helper returns can still reap the group.
    lifecycle.process_group_id = int(lifecycle.process.pid)
    handler_state["armed"] = True
    if handler_state["pending_signum"] is not None:
        pending_signum = int(handler_state["pending_signum"])
        handler_state["pending_signum"] = None
        handler_state["armed"] = False
        raise _LiveRunnerTermination(pending_signum)


def run_fbref_live_waves(
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
    domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
    max_batches: int = FBREF_MAX_LIVE_BATCHES,
) -> dict:
    """Run all bounded live batches in one warm, unforked subprocess."""

    validate_fbref_runtime_limits(
        run_type=run_type,
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
    )
    normalized_batches = int(max_batches)
    if not 1 <= normalized_batches <= FBREF_MAX_LIVE_BATCHES:
        raise ValueError(
            f"max_batches must be between 1 and {FBREF_MAX_LIVE_BATCHES}"
        )
    command = [
        _legacy_scraper_python(),
        LIVE_WAVES_RUNNER,
        "--control-run-id",
        _control_run_id(airflow_run_id=airflow_run_id, dag_id=dag_id),
        "--parent-pid",
        str(os.getpid()),
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
        "--max-batches",
        str(normalized_batches),
    ]
    lifecycle = _LiveRunnerLifecycle()
    stdout = ""
    stderr = ""
    process_group_terminated = False
    try:
        try:
            # The spawn and every instruction after it execute inside this
            # outer exception table. Lifecycle state is caller-owned, so a
            # signal on an inner TimeoutExpired-handler boundary still reaches
            # the one cleanup path below.
            _spawn_live_runner(command, lifecycle)
            process = lifecycle.process
            process_group_id = lifecycle.process_group_id
            if process is None or process_group_id is None:
                raise RuntimeError(
                    "FBref live runner spawn did not return a PGID"
                )
            stdout, stderr = process.communicate(
                timeout=LIVE_WAVES_TIMEOUT_SECONDS
            )
            if _process_group_exists(process_group_id):
                raise RuntimeError(
                    "FBref live runner exited while descendants remained in "
                    f"its process group {process_group_id}"
                )
        except subprocess.TimeoutExpired as exc:
            if lifecycle.handler_state is not None:
                lifecycle.handler_state["armed"] = False
            process = lifecycle.process
            process_group_id = lifecycle.process_group_id
            if process is None or process_group_id is None:
                raise RuntimeError(
                    "FBref live runner timed out before its PGID was recorded"
                ) from exc
            stdout, stderr = _terminate_process_group(
                process, process_group_id
            )
            process_group_terminated = True
            logger.warning(
                "FBref live runner timed out.\nstdout:\n%s\nstderr:\n%s",
                _decoded_stream(stdout),
                _decoded_stream(stderr),
            )
            message = (
                "FBref live runner exceeded "
                f"{LIVE_WAVES_TIMEOUT_SECONDS}s"
            )
            _abort_failed_live_subprocess(
                airflow_run_id=airflow_run_id,
                dag_id=dag_id,
                error_class="LiveWavesSubprocessTimeout",
                error_message=message,
            )
            raise RuntimeError(
                message + " and its process group was killed"
            ) from exc
    except BaseException:
        if lifecycle.handler_state is not None:
            lifecycle.handler_state["armed"] = False
        process = lifecycle.process
        process_group_id = lifecycle.process_group_id
        if process is not None and not process_group_terminated:
            if process_group_id is None:
                process_group_id = int(process.pid)
            stdout, stderr = _terminate_process_group(
                process, process_group_id
            )
            process_group_terminated = True
            logger.warning(
                "FBref live runner was externally interrupted; its process "
                "group was terminated.\nstdout:\n%s\nstderr:\n%s",
                _decoded_stream(stdout),
                _decoded_stream(stderr),
            )
        raise
    finally:
        if lifecycle.handler_installed:
            if lifecycle.handler_state is not None:
                lifecycle.handler_state["armed"] = False
            signal.signal(
                signal.SIGTERM, lifecycle.previous_sigterm_handler
            )
            pending_signum = (
                None
                if lifecycle.handler_state is None
                else lifecycle.handler_state["pending_signum"]
            )
            if pending_signum is not None and sys.exception() is None:
                lifecycle.handler_state["pending_signum"] = None
                raise _LiveRunnerTermination(int(pending_signum))

    if stderr:
        logger.info("FBref live runner stderr:\n%s", stderr.strip())
    if process.returncode != 0:
        message = (
            "FBref live runner failed with exit code "
            f"{process.returncode}"
        )
        _abort_failed_live_subprocess(
            airflow_run_id=airflow_run_id,
            dag_id=dag_id,
            error_class="LiveWavesSubprocessFailure",
            error_message=message,
        )
        raise RuntimeError(message)
    try:
        result = _parse_prefixed_result(stdout, LIVE_WAVES_RESULT_PREFIX)
    except Exception as exc:
        _abort_failed_live_subprocess(
            airflow_run_id=airflow_run_id,
            dag_id=dag_id,
            error_class="LiveWavesResultMissing",
            error_message=str(exc),
        )
        raise
    logger.info("FBref live waves: %s", json.dumps(result, sort_keys=True))
    return result


def _abort_failed_live_subprocess(
    *,
    airflow_run_id: str,
    dag_id: str,
    error_class: str,
    error_message: str,
) -> None:
    """Synchronously release the failed live run's leases and reservations."""

    try:
        abort_fbref_run(
            airflow_run_id=airflow_run_id,
            dag_id=dag_id,
            error_class=error_class,
            error_message=error_message,
        )
    except Exception:  # noqa: BLE001 - preserve the subprocess root cause
        logger.exception(
            "Could not synchronously abort FBref control run after live "
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


def _parse_prefixed_result(stdout: str, prefix: str) -> dict:
    """Return the last exact runner document, ignoring library noise."""

    for line in reversed(stdout.splitlines()):
        if line.startswith(prefix):
            return json.loads(line[len(prefix):])
    raise RuntimeError("FBref runner subprocess emitted no result document")


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
    acceptance_replay=False,
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
    normalized_acceptance_replay = _boolean_parameter(
        acceptance_replay, name="acceptance_replay"
    )
    result = _pipeline().parse_wave(
        _control_run_id(airflow_run_id=airflow_run_id, dag_id=dag_id),
        page_kinds=list(page_kinds),
        settings=settings,
        source_run_id=normalized_source_run_id,
        acceptance_replay=normalized_acceptance_replay,
    ).as_dict()
    logger.info("FBref offline parse wave: %s", json.dumps(result, sort_keys=True))
    return result


def validate_fbref_run(
    *,
    airflow_run_id: str,
    dag_id: str,
    source_control_run_id: Optional[str] = None,
    publication_eligible: bool = True,
) -> dict:
    summary = _pipeline().validate_and_finish(
        _control_run_id(airflow_run_id=airflow_run_id, dag_id=dag_id),
        replay_source_run_id=(
            None
            if source_control_run_id is None
            or not str(source_control_run_id).strip()
            else str(source_control_run_id).strip()
        ),
        publication_eligible=publication_eligible,
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


def validate_fbref_bootstrap_run(
    *,
    airflow_run_id: str,
    dag_id: str,
    bootstrap_only,
    dag_run_type,
    request_limit,
    byte_limit_mb,
    shard_size,
) -> dict:
    """Validate and finish a bootstrap run without making it publishable."""

    execution = validate_fbref_current_execution_mode(
        bootstrap_only=bootstrap_only,
        dag_run_type=dag_run_type,
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=shard_size,
    )
    if execution["execution_mode"] != "bootstrap_only":
        raise ValueError(
            "validate_fbref_bootstrap_run requires bootstrap_only=true"
        )
    control_run_id = _control_run_id(
        airflow_run_id=airflow_run_id, dag_id=dag_id
    )
    control = _control_store()
    control_run = control.get_run(control_run_id)
    _validate_fbref_bootstrap_control_evidence(
        control_run, expected_status="running"
    )
    summary = validate_fbref_run(
        airflow_run_id=airflow_run_id,
        dag_id=dag_id,
        publication_eligible=False,
    )
    control_run = control.get_run(control_run_id)
    _validate_fbref_bootstrap_control_evidence(
        control_run, expected_status="succeeded"
    )
    evidence = {
        "control_run_id": control_run_id,
        "control_status": "succeeded",
        "execution_mode": "bootstrap_only",
        "publication_eligible": False,
        "runtime_profile": "production_200_requests_100_mib_shard_25",
        "validation_summary": summary,
    }
    logger.info("FBref bootstrap evidence: %s", json.dumps(evidence, default=str))
    return evidence


def _validate_fbref_bootstrap_control_evidence(
    control_run: Optional[Mapping[str, object]], *, expected_status: str
) -> None:
    """Prove bootstrap mode before finish and prove its terminal result."""

    if control_run is None:
        raise RuntimeError("FBref bootstrap control run evidence is missing")
    metadata = control_run.get("metadata")
    if not isinstance(metadata, Mapping):
        raise RuntimeError("FBref bootstrap control metadata is missing")
    try:
        exact_profile = (
            str(control_run.get("run_type") or "").casefold() == "current"
            and int(control_run.get("request_limit"))
            == FBREF_PRODUCTION_REQUEST_LIMIT
            and int(control_run.get("byte_limit"))
            == FBREF_PRODUCTION_BYTE_LIMIT_MB * MIB
            and int(metadata.get("shard_size"))
            == FBREF_MAX_WARM_SESSION_TARGETS
        )
    except (TypeError, ValueError):
        exact_profile = False
    if (
        str(metadata.get("execution_mode") or "") != "bootstrap_only"
        or metadata.get("bootstrap_only") is not True
        or metadata.get("publication_eligible") is not False
        or str(metadata.get("dag_run_type") or "") != "manual"
        or str(metadata.get("runtime_profile") or "") != "production"
        or not exact_profile
        or str(control_run.get("status") or "").casefold()
        != str(expected_status).casefold()
    ):
        raise RuntimeError(
            "FBref bootstrap control evidence does not prove a "
            f"non-publishing {expected_status} run"
        )


def choose_fbref_publication_path(
    *, request_limit: int, byte_limit_mb: int
) -> str:
    """Route the bounded canary away from publication tasks."""

    profile = validate_fbref_runtime_limits(
        run_type="current",
        request_limit=request_limit,
        byte_limit_mb=byte_limit_mb,
        shard_size=FBREF_MAX_WARM_SESSION_TARGETS,
    )["profile"]
    return (
        "validate_canary_run"
        if profile == "canary"
        else "validate_current_scope_freshness"
    )


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
    "audit_fbref_raw_integrity",
    "abort_fbref_run",
    "capture_fbref_raw_baseline",
    "choose_fbref_backfill_mode",
    "choose_fbref_publication_path",
    "export_fbref_publication_scope",
    "finalize_fbref_publication_lock",
    "fbref_dag_failure_callback",
    "initialize_fbref_run",
    "parse_fbref_wave",
    "plan_fbref_backfill",
    "release_fbref_publication_lock",
    "run_fbref_live_waves",
    "run_recovery_wave",
    "seed_fbref_competition_index",
    "seed_fbref_historical_seasons",
    "validate_fbref_bootstrap_run",
    "validate_fbref_current_execution_mode",
    "validate_fbref_current_scope_freshness",
    "validate_fbref_production_readiness",
    "validate_fbref_publication_scope",
    "validate_fbref_runtime_limits",
    "validate_fbref_run",
]
