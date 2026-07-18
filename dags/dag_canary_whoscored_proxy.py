"""Manual, fail-closed 1 GB paid-proxy measurement for WhoScored.

The DAG never authorizes paid traffic from DagRun scalar parameters.  A
canonical HMAC approval fixes the exact decimal byte cap, hosts, path families,
runtime, classifier and separate full-history-discovery/representative-capture
allocations. Source work remains raw-cache/direct-first; the paid route is only
available after a real alert delivery succeeds and the filtering proxy revalidates
the signed allocation.
"""

# ruff: noqa: E402 -- the trust anchor must run before every non-built-in import

from __future__ import annotations

import sys as _whoscored_bootstrap_sys

_whoscored_source = __file__
if not _whoscored_source.startswith("/"):
    raise RuntimeError("WhoScored entrypoint requires an absolute source path")
_whoscored_production = _whoscored_source.startswith("/opt/airflow/")
_whoscored_root = (
    "/opt/airflow"
    if _whoscored_production
    else _whoscored_source.rsplit("/dags/", 1)[0]
)
if _whoscored_production:
    if (
        getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_schema", None)
        != 2
    ):
        raise RuntimeError("image-baked WhoScored startup anchor is required")
elif (
    getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_root", None)
    != _whoscored_root
):
    _whoscored_anchor_path = (
        _whoscored_root + "/docker/images/airflow/whoscored_runtime_startup.py"
    )
    _whoscored_anchor_globals = {
        "__builtins__": __builtins__,
        "sys": _whoscored_bootstrap_sys,
        "_WHOSCORED_RUNTIME_ROOT": _whoscored_root,
        "_WHOSCORED_REQUIRE_FULL_ATTESTATION": False,
    }
    with open(_whoscored_anchor_path, "rb") as _whoscored_anchor_handle:
        _whoscored_anchor_source = _whoscored_anchor_handle.read()
    exec(
        compile(_whoscored_anchor_source, _whoscored_anchor_path, "exec"),
        _whoscored_anchor_globals,
    )
_WHOSCORED_RUNTIME_CONTRACT = _whoscored_bootstrap_sys._load_whoscored_runtime_contract(
    _whoscored_root
)

import errno
import hashlib
import importlib
import json
import math
import os
import re
import signal
import stat
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlsplit

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from dags.scripts.whoscored_identity import stable_safe_token
from scripts.whoscored_proxy_campaign import (
    CANARY_ALLOCATION_ID,
    CANARY_ALLOWED_PATH_FAMILIES,
    CANARY_CAPTURE_CAP_BYTES,
    CANARY_DISCOVERY_ALLOCATION_ID,
    CANARY_DISCOVERY_CAP_BYTES,
    CANARY_DISCOVERY_PATH_FAMILIES,
    CANARY_DISCOVERY_WORK_ITEM_ID,
    CANARY_TASK_ID,
    CANARY_WORK_ITEM_ID,
    DEFAULT_APPROVAL_ROOT,
)
from scrapers.whoscored.proxy_campaign import (
    PROXY_CAMPAIGN_METER,
    WHOSCORED_CANARY_CAP_BYTES,
    WHOSCORED_CANARY_DAG_ID,
    WHOSCORED_CANARY_FIXED_SCOPES,
    WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE,
    WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE,
    ProxyCampaignApproval,
    canonical_json_bytes,
    deterministic_proxy_attempt_id,
    load_proxy_campaign_approval_structure,
    whoscored_canary_run_id,
)
from utils.config import DAG_TAGS
from utils.default_args import SCRAPER_ARGS


DAG_ID = WHOSCORED_CANARY_DAG_ID
CANARY_ALERT_TASK_ID = "deliver_whoscored_proxy_canary_alert"
SOURCE_POOL = os.environ.get("WHOSCORED_DIRECT_POOL", "whoscored_direct_pool")
EXPECTED_SOURCE_POOL_SLOTS = 2
EXPECTED_PAID_GATEWAY_URL = "http://whoscored_paid_gateway:8898"
PAID_GATEWAY_URL_ENV = "WHOSCORED_PAID_GATEWAY_URL"
PAID_GATEWAY_TOKEN_ENV = "WHOSCORED_PAID_GATEWAY_TOKEN"
RUNNER_FORBIDDEN_AUTHORITY_ENV_NAMES = (
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "PROXY_FILTER_CONTROL_TOKEN",
    "PROXY_FILTER_URL",
    "SOFASCORE_PROXY_CONTROL_TOKEN",
    "SOFASCORE_PROXY_CONTROL_URL",
    "TM_PROXY_CONTROL_TOKEN",
    "TM_PROXY_CONTROL_URL",
    "WHOSCORED_PAID_PROXY_URL",
    "WHOSCORED_PROXY_CONTROL_URL",
    "WHOSCORED_PROXY_CONTROL_TOKEN",
    "WHOSCORED_PROXY_CAMPAIGN_LEDGER_PATH",
    "PROXY_FILTER_LEDGER_PATH",
    "WHOSCORED_PROXY_APPROVAL_HMAC_SECRET",
    "WHOSCORED_PROXY_LEDGER_HMAC_SECRET",
    "WHOSCORED_PAID_ALERT_HMAC_SECRET",
    "WHOSCORED_PAID_ALERT_SECRET_PATH",
    "WHOSCORED_PAID_ALERT_BINDING_PATH",
    "WHOSCORED_PAID_ALERT_RECEIPT_ROOT",
)
RUN_ROOT = "/opt/airflow/logs/whoscored_proxy_canary"
PROVIDER_LEDGER_PATH = "/opt/airflow/state/whoscored-proxy-filter/paid_requests.jsonl"
MAX_JSON_BYTES = 256 * 1024 * 1024
MAX_LEDGER_EVENT_BYTES = 256 * 1024
SESSION_SUPERVISOR_QUIET_SECONDS = 95.0
SESSION_SUPERVISOR_DEADLINE_SECONDS = 125.0
SESSION_SUPERVISOR_POLL_SECONDS = 2.0
SESSION_SUPERVISOR_API_TIMEOUT_SECONDS = 3.0
SESSION_SUPERVISOR_CLEANUP_PATH = "/v1/whoscored/capacity-sessions/cleanup"
PROCESS_GROUP_TERMINATE_GRACE_SECONDS = 5.0
PROCESS_GROUP_KILL_CONFIRM_SECONDS = 5.0
MIN_APPROVAL_VALIDITY = timedelta(hours=8)
CANARY_EXECUTION_TIMEOUT = timedelta(hours=6)
CANARY_DAGRUN_TIMEOUT = timedelta(hours=8)
CANARY_DISCOVERY_TIMEOUT = timedelta(hours=2)
CANARY_CAPTURE_TIMEOUT = timedelta(hours=3, minutes=45)
CANARY_MAX_WORK_ITEMS = 100
CANARY_MAX_ESTIMATED_WORK_ITEMS = 90
CANARY_FIXED_SCOPES = WHOSCORED_CANARY_FIXED_SCOPES
CANARY_MAX_COHORT_SCOPES = len(CANARY_FIXED_SCOPES)
CANARY_MIN_COHORT_STAGES = 20
CANARY_MIN_COHORT_MATCHES = 100
CANARY_MIN_COHORT_PLAYERS = 100
CANARY_MIN_SAMPLES_PER_CLASS = 20
VERIFIED_REMAINING_TARGET_BASIS = "verified-raw-miss-inventory-v1"
FULL_TARGET_UPPER_BOUND_BASIS = "full-population-upper-bound-v1"
REQUIRED_MODEL_CLASSES = (
    "catalog_or_bootstrap",
    "match_live",
    "match_preview",
    "player_profile",
    "stage_data",
    "stage_feed",
    "statistics_feed",
)
CLASSIFIER_FILES = (
    "configs/medallion/competitions.yaml",
    "scrapers/whoscored/catalog.py",
    "scrapers/whoscored/domain.py",
)
_SHA256 = re.compile(r"\A[0-9a-f]{64}\Z")


def _runtime_helper() -> Any | None:
    """Load an optional shared helper without making DAG parsing depend on it."""

    try:
        return importlib.import_module("dags.scripts.whoscored_proxy_runtime")
    except ModuleNotFoundError as exc:
        if exc.name != "dags.scripts.whoscored_proxy_runtime":
            raise
        return None


def _context_params(context: Mapping[str, Any]) -> Mapping[str, Any]:
    params = context.get("params")
    return params if isinstance(params, Mapping) else {}


def _approval_pin(context: Mapping[str, Any]) -> tuple[Path, str, str]:
    params = _context_params(context)
    approval_id = str(params.get("paid_approval_id") or "").strip()
    approval_sha256 = str(params.get("paid_approval_sha256") or "").strip()
    if not approval_id or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", approval_id
    ):
        raise AirflowException("paid_approval_id is required and must be canonical")
    if _SHA256.fullmatch(approval_sha256) is None:
        raise AirflowException("paid_approval_sha256 must be 64 lowercase hex")

    root = Path(
        os.environ.get("WHOSCORED_PROXY_APPROVAL_ROOT", DEFAULT_APPROVAL_ROOT)
    ).resolve()
    explicit = str(os.environ.get("WHOSCORED_PROXY_APPROVAL_PATH", "")).strip()
    path = Path(explicit) if explicit else root / f"{approval_id}.json"
    try:
        if path.is_symlink():
            raise AirflowException("signed proxy approval must not be a symlink")
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
    except AirflowException:
        raise
    except (OSError, ValueError) as exc:
        raise AirflowException(
            "approval path is absent or outside its mounted root"
        ) from exc
    metadata = resolved.stat()
    if resolved.name != f"{approval_id}.json" or resolved.is_symlink():
        raise AirflowException("approval filename must equal <paid_approval_id>.json")
    if not stat.S_ISREG(metadata.st_mode):
        raise AirflowException("signed proxy approval must be a regular file")
    if metadata.st_uid != os.geteuid():
        raise AirflowException("signed proxy approval must be owned by Airflow")
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise AirflowException("signed proxy approval must have mode 0600")
    return resolved, approval_id, approval_sha256


def _assert_private_approval_path(path: Path, approval_id: str) -> Path:
    try:
        if path.is_symlink():
            raise AirflowException("signed proxy approval must not be a symlink")
        resolved = path.resolve(strict=True)
    except AirflowException:
        raise
    except OSError as exc:
        raise AirflowException("signed proxy approval is not mounted") from exc
    metadata = resolved.stat()
    if resolved.is_symlink() or resolved.name != f"{approval_id}.json":
        raise AirflowException("approval filename must equal <paid_approval_id>.json")
    if not stat.S_ISREG(metadata.st_mode):
        raise AirflowException("signed proxy approval must be a regular file")
    if metadata.st_uid != os.geteuid():
        raise AirflowException("signed proxy approval must be owned by Airflow")
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise AirflowException("signed proxy approval must have mode 0600")
    return resolved


def _load_canary_approval(
    context: Mapping[str, Any],
    *,
    now: datetime | None = None,
) -> tuple[ProxyCampaignApproval, Path]:
    helper = _runtime_helper()
    if helper is not None and callable(getattr(helper, "resolve_paid_runtime", None)):
        try:
            runtime = helper.resolve_paid_runtime(context)
        except Exception as exc:
            error_type = getattr(helper, "WhoScoredProxyRuntimeError", RuntimeError)
            if isinstance(exc, error_type):
                raise AirflowException(str(exc)) from exc
            raise
        if not runtime.is_paid or runtime.approval is None:
            raise AirflowException("canary requires transport_policy=direct_then_paid")
        approval = runtime.approval
        path = _assert_private_approval_path(
            Path(runtime.approval_path), approval.approval_id
        )
        if now is not None:
            approval = load_proxy_campaign_approval_structure(
                path,
                expected_approval_id=approval.approval_id,
                expected_approval_sha256=approval.approval_sha256,
                now=now,
            )
    else:
        path, approval_id, approval_sha256 = _approval_pin(context)
        approval = load_proxy_campaign_approval_structure(
            path,
            expected_approval_id=approval_id,
            expected_approval_sha256=approval_sha256,
            now=now,
        )
    if not approval.is_exact_canary:
        raise AirflowException(
            "approval must bind only dag_canary_whoscored_proxy to exactly "
            "1_000_000_000 provider-billed bytes"
        )
    run_id = str(context.get("run_id") or "")
    if run_id != whoscored_canary_run_id(approval.campaign_id):
        raise AirflowException(
            "exact canary approval requires run_id="
            f"{whoscored_canary_run_id(approval.campaign_id)!r}"
        )
    if (
        approval.caps.daily_provider_bytes != WHOSCORED_CANARY_CAP_BYTES
        or approval.caps.discovery_provider_bytes != CANARY_DISCOVERY_CAP_BYTES
        or approval.caps.capture_provider_bytes != CANARY_CAPTURE_CAP_BYTES
    ):
        raise AirflowException(
            "canary phase/daily byte caps differ from release policy"
        )
    if len(approval.allocations) != 2:
        raise AirflowException("measurement canary requires exactly two allocations")
    if approval.allowed_path_families != CANARY_ALLOWED_PATH_FAMILIES:
        raise AirflowException(
            "canary approval path families differ from release policy"
        )
    discovery = approval.allocation(CANARY_DISCOVERY_ALLOCATION_ID)
    allocation = approval.allocation(CANARY_ALLOCATION_ID)
    if (
        discovery.task_id != CANARY_TASK_ID
        or discovery.work_item_id != CANARY_DISCOVERY_WORK_ITEM_ID
        or discovery.workload_class != "catalog_discovery"
        or discovery.phase != "discovery"
        or discovery.budget_bytes != CANARY_DISCOVERY_CAP_BYTES
        or discovery.allowed_path_families != CANARY_DISCOVERY_PATH_FAMILIES
        or allocation.task_id != CANARY_TASK_ID
        or allocation.work_item_id != CANARY_WORK_ITEM_ID
        or allocation.workload_class != "representative_cohort"
        or allocation.phase != "capture"
        or allocation.budget_bytes != CANARY_CAPTURE_CAP_BYTES
        or allocation.allowed_path_families != CANARY_ALLOWED_PATH_FAMILIES
    ):
        raise AirflowException(
            "signed canary allocations do not match discovery/capture policy"
        )
    if (
        discovery.request_limit + allocation.request_limit > approval.limits.requests
        or discovery.lease_limit + allocation.lease_limit > approval.limits.leases
    ):
        raise AirflowException("signed canary allocation limits are invalid")
    if approval.limits.concurrency > EXPECTED_SOURCE_POOL_SLOTS:
        raise AirflowException(
            "signed proxy concurrency exceeds the shared source pool"
        )
    return approval, path


def classifier_sha256(root: Path | None = None) -> str:
    """Hash the exact senior-men catalog classifier implementation.

    Runtime callers never select this trust root from the environment. An
    alternate root is an explicit tooling/test input only.
    """

    runtime_root = Path(_whoscored_root)
    root = runtime_root if root is None else root
    hashes: dict[str, str] = {}
    for relative in CLASSIFIER_FILES:
        if root == runtime_root:
            try:
                hashes[relative] = (
                    _WHOSCORED_RUNTIME_CONTRACT.attested_runtime_file_sha256(
                        relative,
                        runtime_root=root,
                    )
                )
            except Exception as exc:
                raise AirflowException(
                    f"cannot attest classifier runtime file {relative}"
                ) from exc
            continue

        # Explicit non-runtime roots are supported only for side-effect-free
        # tooling and unit fixtures. Production always takes the branch above,
        # whose digest is bound to the fd-pinned startup snapshot.
        path = root / relative
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise AirflowException(
                f"cannot read classifier runtime file {relative}"
            ) from exc
        hashes[relative] = hashlib.sha256(payload).hexdigest()
    return hashlib.sha256(canonical_json_bytes(hashes)).hexdigest()


def _validate_canary_release_pins(approval: ProxyCampaignApproval) -> dict[str, Any]:
    """Recheck mutable bind-mounted code immediately before paid source work."""

    runtime = _WHOSCORED_RUNTIME_CONTRACT.validate_runtime_contract(
        report_schema_version=3
    )
    if runtime.get("code_tree_sha256") != approval.runtime_sha256:
        raise AirflowException("signed runtime SHA-256 differs from deployed code")
    actual_classifier = classifier_sha256()
    if actual_classifier != approval.classifier_sha256:
        raise AirflowException("signed classifier SHA-256 differs from deployed code")
    return {**runtime, "classifier_sha256": actual_classifier}


def _validate_paid_gateway_environment(environment: Mapping[str, str]) -> None:
    if str(environment.get(PAID_GATEWAY_URL_ENV, "")).strip() != (
        EXPECTED_PAID_GATEWAY_URL
    ):
        raise AirflowException("paid application gateway URL differs from admission")
    if len(str(environment.get(PAID_GATEWAY_TOKEN_ENV, "")).strip()) < 32:
        raise AirflowException(
            f"{PAID_GATEWAY_TOKEN_ENV} must contain at least 32 characters"
        )


def _paid_gateway_client() -> Any:
    _validate_paid_gateway_environment(os.environ)
    from scrapers.whoscored.transport import PaidGatewayClient

    return PaidGatewayClient(
        EXPECTED_PAID_GATEWAY_URL,
        token=str(os.environ[PAID_GATEWAY_TOKEN_ENV]).strip(),
    )


def _paid_campaign_gateway_call(
    approval: ProxyCampaignApproval,
    operation: str,
    **arguments: Any,
) -> Any:
    if operation not in {
        "snapshot",
        "complete_allocation",
        "assert_exact_accounting",
        "seal_for_reconciliation",
        "sealed_snapshot",
    }:
        raise AirflowException("unsupported paid campaign gateway operation")
    from scrapers.whoscored.transport import (
        PaidCampaignContext,
        PaidGatewayError,
    )

    client = _paid_gateway_client()
    try:
        method = getattr(client, operation)
        return method(
            context=PaidCampaignContext.from_approval(approval),
            **arguments,
        )
    except (PaidGatewayError, ValueError) as exc:
        raise AirflowException(
            f"paid gateway campaign operation failed: {operation}"
        ) from exc
    finally:
        client.close()


def validate_canary_approval(
    *,
    now: datetime | None = None,
    **context: Any,
) -> dict[str, object]:
    """Prove runtime identity, source pool, approval validity and active state."""

    _WHOSCORED_RUNTIME_CONTRACT.require_production_runtime_class(
        operation="WhoScored paid canary approval"
    )
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise AirflowException("canary validation time must be timezone-aware")
    current = current.astimezone(timezone.utc)
    approval, path = _load_canary_approval(context, now=current)
    expires = datetime.fromisoformat(approval.expires_at.replace("Z", "+00:00"))
    if expires - current < MIN_APPROVAL_VALIDITY:
        raise AirflowException(
            "approval must remain valid for the complete eight-hour canary window"
        )

    runtime = _validate_canary_release_pins(approval)
    source_pool = _WHOSCORED_RUNTIME_CONTRACT.validate_airflow_source_pool(
        direct_pool=SOURCE_POOL,
        backfill_pool=os.environ.get("WHOSCORED_BACKFILL_POOL", SOURCE_POOL),
    )
    if source_pool.get("actual_slots") != EXPECTED_SOURCE_POOL_SLOTS:
        raise AirflowException(
            "measurement canary requires exactly two source-pool slots"
        )
    actual_classifier = str(runtime["classifier_sha256"])
    _validate_paid_gateway_environment(os.environ)
    if not WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE:
        raise AirflowException(
            "paid canary is blocked: no provider-side decimal 1 GB invoice quota "
            "and authoritative usage reconciliation are implemented"
        )
    if not WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE:
        raise AirflowException(
            "paid canary is blocked: no authenticated isolated application "
            "gateway protects exact origins and FlareSolverr sessions"
        )

    snapshot = _paid_campaign_gateway_call(approval, "snapshot")
    if snapshot.get("status") != "active":
        raise AirflowException(
            f"proxy campaign is not active: {snapshot.get('status')!r}"
        )
    return {
        "status": "approved",
        "campaign_id": approval.campaign_id,
        "approval_id": approval.approval_id,
        "approval_sha256": approval.approval_sha256,
        "approval_path": str(path),
        "expires_at": approval.expires_at,
        "total_provider_bytes": approval.caps.total_provider_bytes,
        "meter": PROXY_CAMPAIGN_METER,
        "discovery_allocation_id": CANARY_DISCOVERY_ALLOCATION_ID,
        "capture_allocation_id": CANARY_ALLOCATION_ID,
        "runtime_sha256": approval.runtime_sha256,
        "classifier_sha256": actual_classifier,
        "source_pool": source_pool,
    }


def deliver_canary_alert(
    approval_metadata: Mapping[str, Any],
    **context: Any,
) -> dict[str, Any]:
    """Require one gateway-owned production alert before any paid work."""

    if approval_metadata.get("status") != "approved":
        raise AirflowException("canary approval preflight did not succeed")
    task_instance = context.get("ti") or context.get("task_instance")
    if str(getattr(task_instance, "task_id", "") or "") != CANARY_ALERT_TASK_ID:
        raise AirflowException("canary alert task identity differs")
    approval, path = _load_canary_approval(context)
    if (
        str(path) != str(approval_metadata.get("approval_path") or "")
        or approval.campaign_id != approval_metadata.get("campaign_id")
        or approval.approval_id != approval_metadata.get("approval_id")
        or approval.approval_sha256 != approval_metadata.get("approval_sha256")
    ):
        raise AirflowException("canary alert approval metadata differs")
    from scrapers.whoscored.transport import (
        PaidCampaignContext,
        PaidGatewayError,
    )

    client = _paid_gateway_client()
    try:
        result = client.preflight_alert(
            context=PaidCampaignContext.from_approval(approval)
        )
    except (PaidGatewayError, ValueError) as exc:
        raise AirflowException("paid gateway alert preflight failed") from exc
    finally:
        client.close()
    if (
        result.get("status") != "delivered"
        or result.get("campaign_id") != approval_metadata.get("campaign_id")
        or result.get("approval_id") != approval_metadata.get("approval_id")
        or result.get("approval_sha256") != approval_metadata.get("approval_sha256")
    ):
        raise AirflowException("real alert test was not delivered")
    return dict(result)


def _estimated_scope_work_items(matches: int, players: int) -> int:
    # One schedule freeze, 25-match chunks, one roster freeze and 200-profile
    # chunks. Preview work is executed inside its owning match chunk.
    return 2 + math.ceil(matches / 25) + math.ceil(players / 200)


def select_representative_cohort(
    catalog_seasons: Sequence[Any],
    population_rows: Sequence[Sequence[Any]],
    *,
    fixed_scopes: Sequence[str] = CANARY_FIXED_SCOPES,
) -> list[dict[str, Any]]:
    """Validate the release-bound representative scopes against live Bronze."""

    populations: dict[str, tuple[int, int]] = {}
    for row in population_rows:
        if len(row) != 4:
            raise AirflowException("invalid canary population aggregate")
        spec = f"{row[0]}={row[1]}"
        matches = int(row[2])
        players = int(row[3])
        if matches < 0 or players < 0 or spec in populations:
            raise AirflowException("invalid/duplicate canary population aggregate")
        populations[spec] = (matches, players)

    candidates: list[dict[str, Any]] = []
    for season in catalog_seasons:
        scope = getattr(season, "scope", None)
        spec = str(getattr(scope, "spec", ""))
        stage_ids = sorted({int(value) for value in getattr(season, "stage_ids", ())})
        matches, players = populations.get(spec, (0, 0))
        if not spec or not stage_ids or matches < 20 or players < 20:
            continue
        estimated = _estimated_scope_work_items(matches, players)
        if estimated > CANARY_MAX_ESTIMATED_WORK_ITEMS:
            continue
        candidates.append(
            {
                "scope": spec,
                "stage_ids": stage_ids,
                "stage_count": len(stage_ids),
                "completed_match_count": matches,
                "roster_player_count": players,
                "estimated_work_items": estimated,
            }
        )
    by_scope = {str(item["scope"]): item for item in candidates}
    normalized_fixed = tuple(str(scope) for scope in fixed_scopes)
    if (
        not normalized_fixed
        or len(set(normalized_fixed)) != len(normalized_fixed)
        or len(normalized_fixed) > CANARY_MAX_COHORT_SCOPES
    ):
        raise AirflowException("fixed canary scope policy is invalid")
    missing = sorted(set(normalized_fixed) - set(by_scope))
    if missing:
        raise AirflowException(
            "fixed canary scopes are absent/incomplete in catalog Bronze: "
            + ", ".join(missing)
        )
    selected = [by_scope[scope] for scope in normalized_fixed]
    totals = {
        "stages": sum(int(item["stage_count"]) for item in selected),
        "matches": sum(int(item["completed_match_count"]) for item in selected),
        "players": sum(int(item["roster_player_count"]) for item in selected),
        "work": sum(int(item["estimated_work_items"]) for item in selected),
    }

    if (
        totals["stages"] < CANARY_MIN_COHORT_STAGES
        or totals["matches"] < CANARY_MIN_COHORT_MATCHES
        or totals["players"] < CANARY_MIN_COHORT_PLAYERS
        or totals["work"] > CANARY_MAX_ESTIMATED_WORK_ITEMS
    ):
        raise AirflowException(
            "fixed representative canary cohort is not safely bounded by the immutable "
            f"catalog/frontier: {totals}"
        )
    return sorted(selected, key=lambda item: str(item["scope"]))


def freeze_measurement_cohort(**context: Any) -> dict[str, Any]:
    """Persist the capture cohort after discovery and before capture spend."""

    from scrapers.whoscored.repository import WhoScoredRepository

    repository = WhoScoredRepository()
    generation, catalog = repository.load_catalog_generation_snapshot()
    if generation.get("catalog_discovery_mode") != "full_history":
        raise AirflowException("canary cohort requires a full-history catalog snapshot")
    if catalog.quarantined:
        raise AirflowException("canary cohort cannot use a quarantined catalog")
    catalog_name = str(repository.catalog)
    schema_name = str(repository.schema)
    population_rows = repository.trino.execute_query(
        f"""
        WITH ranked_schedule AS (
            SELECT league, season, game_id, status, home_score, away_score, date,
                   ROW_NUMBER() OVER (
                       PARTITION BY league, season, game_id
                       ORDER BY _ingested_at DESC
                   ) AS rn
            FROM {catalog_name}.{schema_name}.whoscored_schedule_current
        ), match_counts AS (
            SELECT league, season, COUNT(*) AS completed_matches
            FROM ranked_schedule
            WHERE rn = 1 AND game_id IS NOT NULL
              AND (
                  status = 6
                  OR (
                      status = 1
                      AND home_score IS NOT NULL
                      AND away_score IS NOT NULL
                      AND date <= CAST(
                          CURRENT_TIMESTAMP - INTERVAL '3' HOUR AS TIMESTAMP
                      )
                  )
              )
            GROUP BY league, season
        ), player_counts AS (
            SELECT league, season, COUNT(DISTINCT player_id) AS roster_players
            FROM {catalog_name}.{schema_name}.whoscored_player_roster
            WHERE player_id IS NOT NULL
            GROUP BY league, season
        )
        SELECT matches.league, matches.season,
               CAST(matches.completed_matches AS BIGINT),
               CAST(COALESCE(players.roster_players, 0) AS BIGINT)
        FROM match_counts matches
        LEFT JOIN player_counts players
          ON players.league = matches.league
         AND players.season = matches.season
        ORDER BY matches.league, matches.season
        """
    )
    eligible_seasons = catalog.eligible_scopes(active_only=False)
    selected = select_representative_cohort(eligible_seasons, population_rows)
    totals = {
        "scope_count": len(selected),
        "stage_count": sum(int(item["stage_count"]) for item in selected),
        "completed_match_count": sum(
            int(item["completed_match_count"]) for item in selected
        ),
        "roster_player_count": sum(
            int(item["roster_player_count"]) for item in selected
        ),
        "estimated_work_items": sum(
            int(item["estimated_work_items"]) for item in selected
        ),
    }
    from dags.scripts.whoscored_ops_store import SCHEDULE_REQUEST_UNITS_PER_STAGE
    from scrapers.whoscored.stage_feeds import STAGE_TEAM_FEED_CATALOG

    eligible_specs = {season.scope.spec for season in eligible_seasons}
    full_stage_count = sum(len(set(season.stage_ids)) for season in eligible_seasons)
    full_match_count = sum(
        int(row[2]) for row in population_rows if f"{row[0]}={row[1]}" in eligible_specs
    )
    # Summing per-scope distinct rosters is deliberately conservative when a
    # player appears in multiple seasons. It can over-budget but never silently
    # drop a non-zero full-history population.
    full_player_count = sum(
        int(row[3]) for row in population_rows if f"{row[0]}={row[1]}" in eligible_specs
    )
    stage_feed_targets = len(STAGE_TEAM_FEED_CATALOG) * full_stage_count
    statistics_targets_per_stage = (
        SCHEDULE_REQUEST_UNITS_PER_STAGE - len(STAGE_TEAM_FEED_CATALOG) - 2
    )
    if statistics_targets_per_stage <= 0:
        raise AirflowException("schedule request-unit policy cannot price feed classes")
    remaining_targets = {
        "catalog_or_bootstrap": full_stage_count,
        "match_live": full_match_count,
        "match_preview": full_match_count,
        "player_profile": full_player_count,
        # The 70-unit immutable schedule envelope reserves one calendar-data
        # and one bootstrap target per stage, the exact eight stage-team feeds,
        # and assigns the conservative remainder to paginated statistics feeds.
        "stage_data": full_stage_count,
        "stage_feed": stage_feed_targets,
        "statistics_feed": statistics_targets_per_stage * full_stage_count,
    }
    if any(remaining_targets[name] <= 0 for name in REQUIRED_MODEL_CLASSES):
        raise AirflowException("full-history target population has an empty class")
    target_provenance = {
        "schema_version": 1,
        "policy": "full-history-catalog-bronze-upper-bound-v1",
        "target_basis": FULL_TARGET_UPPER_BOUND_BASIS,
        "catalog_batch_id": generation.get("catalog_batch_id"),
        "catalog_payload_sha256": generation.get("catalog_payload_sha256"),
        "eligible_scope_count": len(eligible_seasons),
        "full_stage_count": full_stage_count,
        "full_completed_match_count": full_match_count,
        "full_per_scope_distinct_roster_count": full_player_count,
        "schedule_request_units_per_stage": SCHEDULE_REQUEST_UNITS_PER_STAGE,
        "stage_team_feed_targets_per_stage": len(STAGE_TEAM_FEED_CATALOG),
        "statistics_feed_upper_bound_per_stage": statistics_targets_per_stage,
        "remaining_targets": remaining_targets,
        "remaining_target_basis": FULL_TARGET_UPPER_BOUND_BASIS,
    }
    target_provenance_sha256 = hashlib.sha256(
        canonical_json_bytes(target_provenance)
    ).hexdigest()
    cohort = {
        "schema_version": 1,
        "cohort_type": "whoscored_proxy_representative_measurement_v1",
        "catalog_generation": generation,
        "classifier_sha256": classifier_sha256(),
        "selection_policy": {
            "fixed_scopes": list(CANARY_FIXED_SCOPES),
            "minimum_stages": CANARY_MIN_COHORT_STAGES,
            "minimum_completed_matches": CANARY_MIN_COHORT_MATCHES,
            "minimum_roster_players": CANARY_MIN_COHORT_PLAYERS,
            "maximum_scopes": CANARY_MAX_COHORT_SCOPES,
            "maximum_estimated_work_items": CANARY_MAX_ESTIMATED_WORK_ITEMS,
        },
        "scopes": selected,
        "totals": totals,
        "remaining_target_provenance": target_provenance,
        "remaining_target_provenance_sha256": target_provenance_sha256,
    }
    cohort_sha256 = hashlib.sha256(canonical_json_bytes(cohort)).hexdigest()
    from dags.scripts.whoscored_ops_store import WhoScoredOpsStore

    ops_store = WhoScoredOpsStore.from_env(optional=False)
    if ops_store is None:  # pragma: no cover - optional=False is fail closed
        raise AirflowException("WhoScored operational store is required")
    artifact = ops_store.put_content_addressed_json("proxy-campaigns/cohorts", cohort)
    return {
        "status": "cohort_frozen",
        "cohort_sha256": cohort_sha256,
        "scopes": [str(item["scope"]) for item in selected],
        "totals": totals,
        "remaining_targets": remaining_targets,
        "remaining_target_basis": FULL_TARGET_UPPER_BOUND_BASIS,
        "remaining_target_provenance_sha256": target_provenance_sha256,
        "artifact": artifact,
    }


def _run_directory(context: Mapping[str, Any]) -> Path:
    run_id = str(context.get("run_id") or "unknown")
    return Path(RUN_ROOT) / stable_safe_token(run_id)


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        size = path.stat().st_size
        if size <= 0 or size > MAX_JSON_BYTES:
            raise AirflowException(f"invalid JSON artifact size: {path.name}")
        value = json.loads(path.read_text(encoding="utf-8"))
    except AirflowException:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AirflowException(f"cannot read JSON artifact: {path.name}") from exc
    if not isinstance(value, dict):
        raise AirflowException(f"JSON artifact must be an object: {path.name}")
    return value


def _canary_supervisor_owner(attempt_id: str, allocation_id: str) -> str:
    return hashlib.sha256(f"{attempt_id}\0{allocation_id}".encode("utf-8")).hexdigest()[
        :24
    ]


def _supervised_session_ids(path: Path, *, owner: str) -> set[str]:
    """Replay the child-fsynced remote-resource WAL and return open sessions."""

    if not path.exists():
        return set()
    try:
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size <= 0
            or metadata.st_size > 1024 * 1024
        ):
            raise AirflowException("invalid canary supervisor resource ledger")
        payload = path.read_bytes()
    except AirflowException:
        raise
    except OSError as exc:
        raise AirflowException("cannot read canary supervisor resource ledger") from exc
    if not payload.endswith(b"\n"):
        raise AirflowException("canary supervisor resource ledger is truncated")
    prefix = f"ws-cap-{owner}-"
    owned: set[str] = set()
    released: set[str] = set()
    expected_keys = {
        "schema_version",
        "event",
        "resource",
        "owner",
        "session_id",
        "dag_id",
        "run_id",
        "task_id",
        "try_number",
        "recorded_at",
    }
    for raw_line in payload.splitlines():
        try:
            event = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AirflowException(
                "canary supervisor resource ledger contains invalid JSON"
            ) from exc
        if not isinstance(event, dict) or set(event) != expected_keys:
            raise AirflowException(
                "canary supervisor resource ledger event schema is invalid"
            )
        session_id = event.get("session_id")
        lifecycle = event.get("event")
        if (
            event.get("schema_version") != 1
            or event.get("resource") != "flaresolverr_session"
            or event.get("owner") != owner
            or not isinstance(session_id, str)
            or not session_id.startswith(prefix)
            or len(session_id) > 120
            or lifecycle not in {"owned", "released"}
        ):
            raise AirflowException(
                "canary supervisor resource ledger identity is invalid"
            )
        if lifecycle == "owned":
            if session_id in owned:
                raise AirflowException("duplicate supervised session ownership")
            owned.add(session_id)
        else:
            if session_id not in owned or session_id in released:
                raise AirflowException("supervised session lifecycle is out of order")
            released.add(session_id)
    return owned - released


def _cleanup_supervised_sessions(
    path: Path,
    *,
    owner: str,
    flaresolverr_url: str,
) -> list[str]:
    _WHOSCORED_RUNTIME_CONTRACT.require_production_runtime_class(
        operation="WhoScored supervised browser cleanup"
    )
    ledger_error: AirflowException | None = None
    try:
        outstanding = _supervised_session_ids(path, owner=owner)
    except AirflowException as exc:
        # A killed writer may leave only its final JSONL record torn. Cleanup
        # must still enumerate the deterministic owner namespace, then surface
        # the corrupt evidence as a red task.
        ledger_error = exc
        outstanding = set()
    try:
        parsed = urlsplit(flaresolverr_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ValueError
        endpoint = f"{parsed.scheme}://{parsed.netloc}"
    except ValueError as exc:
        raise AirflowException("invalid supervised FlareSolverr endpoint") from exc

    from requests import Session
    from requests.adapters import HTTPAdapter

    try:
        extension_sha256 = _WHOSCORED_RUNTIME_CONTRACT.attested_runtime_file_sha256(
            "scripts/flaresolverr_extended.py",
            runtime_root=Path(_whoscored_root),
        )
    except Exception as exc:
        raise AirflowException("cannot attest FlareSolverr cleanup extension") from exc

    client = Session()
    client.trust_env = False
    adapter = HTTPAdapter(max_retries=0)
    client.mount("http://", adapter)
    client.mount("https://", adapter)
    deadline = time.monotonic() + SESSION_SUPERVISOR_DEADLINE_SECONDS
    zero_started_at: float | None = None
    final_zero_scans = 0
    sticky_error: BaseException | None = None
    verified = False
    expected_fields = {
        "status",
        "version",
        "extension_sha256",
        "active",
        "pending_create",
        "pending_destroy",
        "failed_create",
        "failed_destroy",
        "failure_generation",
        "cleanup_scheduled",
    }
    count_fields = (
        "active",
        "pending_create",
        "pending_destroy",
        "failed_create",
        "failed_destroy",
    )
    try:
        while time.monotonic() <= deadline:
            try:
                response = client.post(
                    f"{endpoint}{SESSION_SUPERVISOR_CLEANUP_PATH}",
                    json={"owner": owner},
                    timeout=SESSION_SUPERVISOR_API_TIMEOUT_SECONDS,
                    allow_redirects=False,
                )
                if response.status_code != 200:
                    raise RuntimeError("unexpected cleanup status")
                snapshot = response.json()
                if (
                    not isinstance(snapshot, Mapping)
                    or set(snapshot) != expected_fields
                    or snapshot.get("status") != "ok"
                    or snapshot.get("version") != "3.4.6"
                    or snapshot.get("extension_sha256") != extension_sha256
                    or snapshot.get("cleanup_scheduled") is not True
                ):
                    raise RuntimeError("invalid cleanup response identity")
                counts = []
                for field_name in (*count_fields, "failure_generation"):
                    value = snapshot.get(field_name)
                    if type(value) is not int or value < 0:
                        raise RuntimeError("invalid cleanup lifecycle counter")
                    if field_name in count_fields:
                        counts.append(value)
                if snapshot.get("failure_generation") != 0:
                    sticky_error = RuntimeError("supervised session lifecycle failed")
                if snapshot.get("failed_create") or snapshot.get("failed_destroy"):
                    sticky_error = RuntimeError("supervised session cleanup failed")
            except Exception as exc:
                sticky_error = exc
                zero_started_at = None
                final_zero_scans = 0
            else:
                now = time.monotonic()
                if any(counts):
                    zero_started_at = None
                    final_zero_scans = 0
                elif zero_started_at is None:
                    zero_started_at = now
                elif now - zero_started_at >= SESSION_SUPERVISOR_QUIET_SECONDS:
                    final_zero_scans += 1
                    if final_zero_scans >= 2:
                        verified = sticky_error is None
                        break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(SESSION_SUPERVISOR_POLL_SECONDS, remaining))
    finally:
        client.close()
    if not verified:
        raise AirflowException(
            "canary supervisor did not prove a quiet remote-session cleanup window"
        ) from sticky_error
    if ledger_error is not None:
        raise AirflowException(
            "canary supervisor cleaned sessions but resource evidence is invalid"
        ) from ledger_error
    return sorted(outstanding)


def _process_group_exists(process_group_id: int) -> bool:
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError as exc:
        raise AirflowException("cannot prove canary process-group ownership") from exc
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        raise AirflowException("cannot probe canary process group") from exc
    return True


def _terminate_process_group(process: subprocess.Popen[Any]) -> None:
    """Boundedly terminate every local descendant of one canary phase."""

    process_group_id = process.pid
    if not _process_group_exists(process_group_id):
        return
    try:
        os.killpg(process_group_id, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=PROCESS_GROUP_TERMINATE_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        pass
    deadline = time.monotonic() + PROCESS_GROUP_TERMINATE_GRACE_SECONDS
    while _process_group_exists(process_group_id) and time.monotonic() < deadline:
        time.sleep(0.05)
    if not _process_group_exists(process_group_id):
        return
    try:
        os.killpg(process_group_id, signal.SIGKILL)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + PROCESS_GROUP_KILL_CONFIRM_SECONDS
    while _process_group_exists(process_group_id) and time.monotonic() < deadline:
        time.sleep(0.05)
    if _process_group_exists(process_group_id):
        raise AirflowException("canary process group did not terminate")


def _run_supervised_process(
    command: Sequence[str],
    *,
    environment: Mapping[str, str],
    timeout_seconds: float,
) -> int:
    process = subprocess.Popen(
        command,
        env=dict(environment),
        cwd="/opt/airflow",
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        return int(process.wait(timeout=timeout_seconds))
    finally:
        _terminate_process_group(process)


def _execute_runner_phase(
    *,
    context: Mapping[str, Any],
    approval: ProxyCampaignApproval,
    approval_path: Path,
    allocation_id: str,
    runner_args: Sequence[str],
    report_name: str,
    request_ledger_name: str,
    timeout: timedelta,
    alert_metadata: Mapping[str, Any],
) -> dict[str, Any]:
    """Execute one signed discovery/capture subprocess under the shared task."""

    _WHOSCORED_RUNTIME_CONTRACT.require_production_runtime_class(
        operation="WhoScored paid canary runner phase"
    )
    run_dir = _run_directory(context)
    report_path = run_dir / report_name
    request_ledger_path = run_dir / request_ledger_name
    supervisor_ledger_path = run_dir / f"{report_name}.remote-resources.jsonl"
    if (
        report_path.exists()
        or request_ledger_path.exists()
        or supervisor_ledger_path.exists()
    ):
        raise AirflowException(
            "canary phase staging already exists; refusing ambiguous rerun"
        )
    allocation = approval.allocation(allocation_id)
    runner_python = str(
        os.environ.get("WHOSCORED_SCRAPER_PYTHON") or sys.executable
    ).strip()
    if not runner_python:
        raise AirflowException("WhoScored scraper Python is not configured")
    command = [
        runner_python,
        str(Path(__file__).resolve().parent / "scripts" / "run_whoscored_scraper.py"),
        *runner_args,
        "--transport-policy",
        "direct_then_paid",
        "--proxy-approval-path",
        str(approval_path),
        "--proxy-approval-id",
        approval.approval_id,
        "--proxy-approval-sha256",
        approval.approval_sha256,
        "--proxy-work-item-id",
        allocation.work_item_id,
        "--output",
        str(report_path),
    ]
    task_instance = context.get("ti") or context.get("task_instance")
    task_id = str(getattr(task_instance, "task_id", CANARY_TASK_ID))
    if task_id != CANARY_TASK_ID or allocation.task_id != CANARY_TASK_ID:
        raise AirflowException("canary source task identity differs from approval")
    expected_attempt_id = deterministic_proxy_attempt_id(
        dag_id=DAG_ID,
        run_id=str(context.get("run_id") or ""),
        task_id=CANARY_TASK_ID,
        map_index=int(getattr(task_instance, "map_index", -1)),
        try_number=int(getattr(task_instance, "try_number", 1)),
    )
    supervisor_owner = _canary_supervisor_owner(expected_attempt_id, allocation_id)
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    for name in RUNNER_FORBIDDEN_AUTHORITY_ENV_NAMES:
        environment.pop(name, None)
    _validate_paid_gateway_environment(environment)
    environment.update(
        {
            "AIRFLOW_CTX_DAG_ID": DAG_ID,
            "AIRFLOW_CTX_DAG_RUN_ID": str(context.get("run_id") or ""),
            "AIRFLOW_CTX_TASK_ID": CANARY_TASK_ID,
            "AIRFLOW_CTX_TRY_NUMBER": str(getattr(task_instance, "try_number", 1)),
            "AIRFLOW_CTX_MAP_INDEX": str(getattr(task_instance, "map_index", -1)),
            "WHOSCORED_REQUEST_LEDGER_PATH": str(request_ledger_path),
            "WHOSCORED_TRANSPORT_POLICY": "direct_then_paid",
            "WHOSCORED_PROXY_APPROVAL_PATH": str(approval_path),
            "WHOSCORED_PROXY_APPROVAL_ID": approval.approval_id,
            "WHOSCORED_PROXY_APPROVAL_SHA256": approval.approval_sha256,
            "WHOSCORED_SUPERVISOR_SESSION_OWNER": supervisor_owner,
            "WHOSCORED_SUPERVISOR_RESOURCE_LEDGER_PATH": str(supervisor_ledger_path),
        }
    )
    from dags.utils.alerts import paid_alert_receipt_environment

    environment.update(paid_alert_receipt_environment(alert_metadata))
    environment.pop("WHOSCORED_PROXY_ALLOCATION_ID", None)
    environment.pop("WHOSCORED_PROXY_ATTEMPT_ID", None)
    runner_error: BaseException | None = None
    returncode: int | None = None
    try:
        returncode = _run_supervised_process(
            command,
            environment=environment,
            timeout_seconds=timeout.total_seconds(),
        )
    except BaseException as exc:
        runner_error = exc
    cleanup_error: BaseException | None = None
    try:
        _cleanup_supervised_sessions(
            supervisor_ledger_path,
            owner=supervisor_owner,
            flaresolverr_url=str(
                environment.get("FLARESOLVERR_URL") or "http://flaresolverr:8191"
            ),
        )
    except AirflowException as exc:
        cleanup_error = exc
    if runner_error is not None or cleanup_error is not None:
        failure = cleanup_error or runner_error
        assert failure is not None
        if runner_error is not None and not isinstance(
            runner_error,
            (OSError, subprocess.TimeoutExpired, AirflowException),
        ):
            if cleanup_error is not None and hasattr(runner_error, "add_note"):
                runner_error.add_note(
                    f"remote cleanup also failed: {type(cleanup_error).__name__}"
                )
            raise runner_error
        raise AirflowException(
            "canary runner phase could not complete and clean up safely: "
            f"{type(failure).__name__}"
        ) from failure
    assert returncode is not None
    if returncode not in (0, 1, 2):
        raise AirflowException(f"canary runner phase exited unexpectedly: {returncode}")
    report = _read_json_object(report_path)
    airflow = report.get("airflow")
    if (
        not isinstance(airflow, Mapping)
        or airflow.get("dag_id") != DAG_ID
        or airflow.get("dag_run_id") != str(context.get("run_id") or "")
        or airflow.get("task_id") != CANARY_TASK_ID
        or report.get("transport_policy") != "direct_then_paid"
        or report.get("proxy_approval_id") != approval.approval_id
        or report.get("proxy_approval_sha256") != approval.approval_sha256
        or report.get("proxy_allocation_id") != allocation_id
        or report.get("proxy_work_item_id") != allocation.work_item_id
        or report.get("proxy_attempt_id") != expected_attempt_id
    ):
        raise AirflowException("canary runner phase identity is not exact")
    return {
        "status": str(report.get("status") or "unknown"),
        "returncode": returncode,
        "report_path": str(report_path),
        "request_ledger_path": str(request_ledger_path),
        "allocation_id": allocation_id,
        "attempt_id": expected_attempt_id,
        "report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
        "request_ledger_sha256": hashlib.sha256(
            request_ledger_path.read_bytes()
        ).hexdigest(),
    }


def execute_measurement_canary(
    approval_metadata: Mapping[str, Any],
    alert_metadata: Mapping[str, Any],
    **context: Any,
) -> dict[str, object]:
    """Discover, freeze and capture under two separately capped allocations."""

    _WHOSCORED_RUNTIME_CONTRACT.require_production_runtime_class(
        operation="WhoScored paid canary source execution"
    )
    # Recheck expiry, HMAC and durable revocation immediately before source
    # work. The filtering proxy repeats this check for every lease and delta.
    approval, approval_path = _load_canary_approval(context)
    _validate_canary_release_pins(approval)
    try:
        from dags.utils.alerts import validate_paid_alert_metadata

        validate_paid_alert_metadata(
            alert_metadata,
            campaign_id=approval.campaign_id,
            approval_id=approval.approval_id,
            approval_sha256=approval.approval_sha256,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        raise AirflowException(
            "paid work is blocked until the exact alert receipt passes"
        ) from exc
    snapshot = _paid_campaign_gateway_call(approval, "snapshot")
    if snapshot.get("status") != "active":
        raise AirflowException("proxy campaign was revoked or exhausted before spend")

    run_dir = _run_directory(context)
    run_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(run_dir, 0o700)
    discovery = _execute_runner_phase(
        context=context,
        approval=approval,
        approval_path=approval_path,
        allocation_id=CANARY_DISCOVERY_ALLOCATION_ID,
        runner_args=("discover", "--full-history"),
        report_name="discovery-result.json",
        request_ledger_name="discovery-requests.jsonl",
        timeout=CANARY_DISCOVERY_TIMEOUT,
        alert_metadata=alert_metadata,
    )
    if discovery["status"] != "success" or discovery["returncode"] != 0:
        raise AirflowException(
            "full-history discovery did not complete under its signed allocation"
        )
    _paid_campaign_gateway_call(
        approval,
        "complete_allocation",
        allocation_id=CANARY_DISCOVERY_ALLOCATION_ID,
        dag_id=DAG_ID,
        run_id=str(context.get("run_id") or ""),
        task_id=CANARY_TASK_ID,
        attempt_id=str(discovery["attempt_id"]),
        report_sha256=str(discovery["report_sha256"]),
        request_ledger_sha256=str(discovery["request_ledger_sha256"]),
    )

    # Freeze only after the signed discovery succeeds so the cohort and target
    # provenance bind the exact catalog generation the capture phase will use.
    cohort_metadata = freeze_measurement_cohort(**context)
    if cohort_metadata.get("status") != "cohort_frozen":
        raise AirflowException("representative measurement cohort is not frozen")
    raw_scopes = cohort_metadata.get("scopes")
    if (
        not isinstance(raw_scopes, list)
        or not raw_scopes
        or len(raw_scopes) > CANARY_MAX_COHORT_SCOPES
        or raw_scopes != sorted(set(str(item) for item in raw_scopes))
    ):
        raise AirflowException("frozen canary cohort has invalid scope identities")
    scopes = [str(item) for item in raw_scopes]
    if tuple(scopes) != tuple(sorted(CANARY_FIXED_SCOPES)):
        raise AirflowException("frozen canary cohort differs from the release policy")
    remaining_targets = cohort_metadata.get("remaining_targets")
    remaining_target_basis = str(cohort_metadata.get("remaining_target_basis") or "")
    target_provenance_sha256 = str(
        cohort_metadata.get("remaining_target_provenance_sha256") or ""
    )
    if (
        not isinstance(remaining_targets, Mapping)
        or remaining_target_basis != FULL_TARGET_UPPER_BOUND_BASIS
        or _SHA256.fullmatch(target_provenance_sha256) is None
        or any(
            isinstance(remaining_targets.get(name), bool)
            or not isinstance(remaining_targets.get(name), int)
            or int(remaining_targets[name]) <= 0
            for name in REQUIRED_MODEL_CLASSES
        )
    ):
        raise AirflowException("cohort has no immutable full-population provenance")

    # Discovery and capture have independent signed phase caps. Recheck the
    # durable campaign between them so exhaustion/revocation stops immediately.
    snapshot = _paid_campaign_gateway_call(approval, "snapshot")
    if snapshot.get("status") != "active":
        raise AirflowException("proxy campaign cannot enter capture phase")
    _validate_canary_release_pins(approval)

    campaign_hash = hashlib.sha256(approval.campaign_id.encode("utf-8")).hexdigest()
    queue_id = f"proxy-canary-{campaign_hash[:32]}"
    capture_args = [
        "backfill",
        "--queue-id",
        queue_id,
        "--max-work-items",
        str(CANARY_MAX_WORK_ITEMS),
    ]
    for scope in scopes:
        capture_args.extend(("--scope", scope))
    capture = _execute_runner_phase(
        context=context,
        approval=approval,
        approval_path=approval_path,
        allocation_id=CANARY_ALLOCATION_ID,
        runner_args=tuple(capture_args),
        report_name="capture-result.json",
        request_ledger_name="capture-requests.jsonl",
        timeout=CANARY_CAPTURE_TIMEOUT,
        alert_metadata=alert_metadata,
    )
    if capture["status"] != "success" or capture["returncode"] != 0:
        raise AirflowException(
            "representative capture did not complete under its signed allocation"
        )
    report = _read_json_object(Path(str(capture["report_path"])))
    reported_scopes = report.get("scopes")
    if (
        not isinstance(reported_scopes, list)
        or len(reported_scopes) != len(scopes)
        or not all(isinstance(item, Mapping) for item in reported_scopes)
        or sorted(str(item.get("scope")) for item in reported_scopes) != scopes
    ):
        raise AirflowException("runner did not execute the exact frozen cohort")
    _paid_campaign_gateway_call(
        approval,
        "complete_allocation",
        allocation_id=CANARY_ALLOCATION_ID,
        dag_id=DAG_ID,
        run_id=str(context.get("run_id") or ""),
        task_id=CANARY_TASK_ID,
        attempt_id=str(capture["attempt_id"]),
        report_sha256=str(capture["report_sha256"]),
        request_ledger_sha256=str(capture["request_ledger_sha256"]),
    )
    return {
        "status": capture["status"],
        "returncode": capture["returncode"],
        "phases": {
            "discovery": discovery,
            "capture": capture,
        },
        "queue_id": queue_id,
        "campaign_id": approval.campaign_id,
        "approval_id": approval.approval_id,
        "approval_sha256": approval.approval_sha256,
        "cohort_sha256": str(cohort_metadata.get("cohort_sha256") or ""),
        "cohort_artifact": cohort_metadata.get("artifact"),
        "remaining_targets": dict(remaining_targets),
        "remaining_target_basis": remaining_target_basis,
        "remaining_target_provenance_sha256": target_provenance_sha256,
    }


def _iter_json_lines(path: Path) -> Iterable[tuple[int, Mapping[str, Any]]]:
    try:
        stream = path.open("rb")
    except OSError as exc:
        raise AirflowException(f"cannot open ledger: {path}") from exc
    with stream:
        line_number = 0
        while True:
            raw = stream.readline(MAX_LEDGER_EVENT_BYTES + 1)
            if not raw:
                break
            line_number += 1
            if len(raw) > MAX_LEDGER_EVENT_BYTES:
                raise AirflowException(
                    f"oversized ledger event at {path}:{line_number}"
                )
            try:
                value = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise AirflowException(
                    f"invalid ledger event at {path}:{line_number}"
                ) from exc
            if not isinstance(value, Mapping):
                raise AirflowException(
                    f"ledger event is not an object at {path}:{line_number}"
                )
            yield line_number, value


def _url_class(value: object) -> str:
    parts = urlsplit(str(value or ""))
    host = (parts.hostname or "").casefold()
    path = parts.path.casefold()
    if host in {"challenges.cloudflare.com", "turnstile.cloudflare.com"}:
        return "cloudflare_challenge"
    if path.startswith("/statisticsfeed/"):
        return "statistics_feed"
    if path.startswith("/stagestatfeed/"):
        return "stage_feed"
    if path.startswith("/matches/"):
        return "match_preview" if path.endswith("/preview") else "match_live"
    if path.startswith("/players/"):
        return "player_profile"
    if path.startswith("/regions/"):
        return "catalog_or_bootstrap"
    if path.startswith("/tournaments/"):
        return "stage_data"
    return "other"


def _nearest_rank(values: Sequence[int], percentile: float) -> int:
    if not values:
        raise AirflowException("cannot calculate a percentile without observations")
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def _request_measurement(
    path: Path,
    approval: ProxyCampaignApproval,
    *,
    allocation_id: str = CANARY_ALLOCATION_ID,
    expected_attempt_id: str | None = None,
) -> tuple[
    int,
    dict[str, Any],
    dict[str, list[int]],
    dict[tuple[str, str], int],
]:
    paid_total = 0
    route_requests: dict[str, int] = defaultdict(int)
    route_wire_bytes: dict[str, int] = defaultdict(int)
    observations: dict[str, list[int]] = defaultdict(list)
    paid_by_lease_url: dict[tuple[str, str], int] = defaultdict(int)
    event_ids: set[str] = set()
    paid_lease_ids: set[str] = set()
    allocation = approval.allocation(allocation_id) if expected_attempt_id else None
    for line_number, event in _iter_json_lines(path):
        if event.get("event_version") != "whoscored-request-v1":
            raise AirflowException(f"unsupported request event at line {line_number}")
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or event_id in event_ids:
            raise AirflowException("request ledger has a missing/duplicate event ID")
        event_ids.add(event_id)
        for field in ("request_bytes", "response_bytes", "paid_proxy_bytes"):
            value = event.get(field, 0)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise AirflowException(f"request ledger has invalid {field}")
        route = str(event.get("route") or "unknown")
        route_requests[route] += 1
        route_wire_bytes[route] += int(event["request_bytes"]) + int(
            event["response_bytes"]
        )
        if event.get("status") == "accounted":
            paid = int(event["paid_proxy_bytes"])
            if (
                event.get("proxy_campaign_id") != approval.campaign_id
                or event.get("proxy_approval_id") != approval.approval_id
                or event.get("proxy_approval_sha256") != approval.approval_sha256
                or event.get("proxy_allocation_id") != allocation_id
                or event.get("transport_policy") != "direct_then_paid"
            ):
                raise AirflowException(
                    "paid request event has another campaign identity"
                )
            if expected_attempt_id is not None:
                map_index = event.get("map_index")
                try_number = event.get("try_number")
                if (
                    event.get("dag_id") != DAG_ID
                    or event.get("run_id") != approval.run_id
                    or event.get("task_id") != allocation.task_id
                    or event.get("proxy_attempt_id") != expected_attempt_id
                    or event.get("proxy_allocation") != allocation.to_dict()
                    or isinstance(map_index, bool)
                    or not isinstance(map_index, int)
                    or map_index < -1
                    or isinstance(try_number, bool)
                    or not isinstance(try_number, int)
                    or try_number < 0
                    or deterministic_proxy_attempt_id(
                        dag_id=DAG_ID,
                        run_id=approval.run_id,
                        task_id=allocation.task_id,
                        map_index=map_index,
                        try_number=try_number,
                    )
                    != expected_attempt_id
                ):
                    raise AirflowException(
                        "paid request event has another task attempt identity"
                    )
            lease_id = event.get("lease_id")
            canonical_url = event.get("url")
            if (
                not isinstance(lease_id, str)
                or not lease_id
                or lease_id in paid_lease_ids
                or not isinstance(canonical_url, str)
                or not canonical_url
            ):
                raise AirflowException(
                    "paid request event has an invalid or duplicate lease identity"
                )
            paid_lease_ids.add(lease_id)
            paid_total += paid
            paid_by_lease_url[(lease_id, canonical_url)] += paid
            observations[_url_class(event.get("url"))].append(paid)
            continue
        if int(event["paid_proxy_bytes"]):
            raise AirflowException("provider bytes appear outside an accounted event")
    traffic = {
        "request_event_count": len(event_ids),
        "route_requests": dict(sorted(route_requests.items())),
        "route_wire_bytes": dict(sorted(route_wire_bytes.items())),
    }
    return paid_total, traffic, observations, dict(paid_by_lease_url)


def _provider_ledger_bytes(
    path: Path,
    approval: ProxyCampaignApproval,
    *,
    expected_attempts: Mapping[str, str],
) -> tuple[dict[str, int], dict[str, dict[tuple[str, str], int]]]:
    totals: dict[str, int] = {allocation_id: 0 for allocation_id in expected_attempts}
    bytes_by_lease_url: dict[str, dict[tuple[str, str], int]] = {
        allocation_id: defaultdict(int) for allocation_id in expected_attempts
    }
    event_ids: set[str] = set()
    lease_identity: dict[str, tuple[str, str, str]] = {}
    lease_lifecycle: dict[str, dict[str, Any]] = {}
    for line_number, event in _iter_json_lines(path):
        if event.get("proxy_campaign_id") != approval.campaign_id:
            continue
        if (
            event.get("event_version") != "paid-proxy-v2"
            or event.get("proxy_approval_id") != approval.approval_id
            or event.get("proxy_approval_sha256") != approval.approval_sha256
            or event.get("provider_meter") != PROXY_CAMPAIGN_METER
            or event.get("dag_id") != DAG_ID
            or event.get("run_id") != approval.run_id
            or event.get("task_id") != CANARY_TASK_ID
        ):
            raise AirflowException(
                f"provider ledger campaign identity mismatch at line {line_number}"
            )
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or event_id in event_ids:
            raise AirflowException("provider ledger has a missing/duplicate event ID")
        event_ids.add(event_id)
        allocation_id = event.get("allocation_id")
        if not isinstance(allocation_id, str) or allocation_id not in expected_attempts:
            raise AirflowException("provider ledger has another allocation identity")
        allocation = approval.allocation(allocation_id)
        map_index = event.get("map_index")
        try_number = event.get("try_number")
        if (
            event.get("proxy_attempt_id") != expected_attempts[allocation_id]
            or event.get("proxy_work_item_id") != allocation.work_item_id
            or isinstance(map_index, bool)
            or not isinstance(map_index, int)
            or map_index < -1
            or isinstance(try_number, bool)
            or not isinstance(try_number, int)
            or try_number < 0
            or deterministic_proxy_attempt_id(
                dag_id=DAG_ID,
                run_id=approval.run_id,
                task_id=CANARY_TASK_ID,
                map_index=map_index,
                try_number=try_number,
            )
            != expected_attempts[allocation_id]
        ):
            raise AirflowException("provider ledger has another task attempt identity")
        lease_id = event.get("lease_id")
        canonical_url = event.get("canonical_url")
        if (
            not isinstance(lease_id, str)
            or not lease_id
            or not isinstance(canonical_url, str)
            or not canonical_url
        ):
            raise AirflowException("provider ledger has an invalid lease/URL identity")
        identity = (allocation_id, expected_attempts[allocation_id], canonical_url)
        previous_identity = lease_identity.setdefault(lease_id, identity)
        if previous_identity != identity:
            raise AirflowException("provider ledger reused a lease identity")
        lease_url_key = (lease_id, canonical_url)
        event_type = event.get("event_type")
        if event_type == "lease_created":
            max_bytes = event.get("max_bytes")
            if (
                lease_id in lease_lifecycle
                or isinstance(max_bytes, bool)
                or not isinstance(max_bytes, int)
                or max_bytes <= 0
            ):
                raise AirflowException(
                    "provider ledger has a duplicate/invalid lease creation"
                )
            lease_lifecycle[lease_id] = {"closed": False, "bytes": 0}
            bytes_by_lease_url[allocation_id][lease_url_key] = 0
            continue
        lifecycle = lease_lifecycle.get(lease_id)
        if lifecycle is None or lifecycle["closed"] is True:
            raise AirflowException(
                "provider ledger lease lifecycle is missing or out of order"
            )
        if event_type == "bytes":
            amount = event.get("bytes")
            if (
                event.get("direction") not in {"up", "down"}
                or isinstance(amount, bool)
                or not isinstance(amount, int)
                or amount <= 0
            ):
                raise AirflowException("provider ledger has an invalid byte delta")
            totals[allocation_id] += amount
            bytes_by_lease_url[allocation_id][lease_url_key] += amount
            lifecycle["bytes"] += amount
            continue
        if event_type == "lease_closed":
            total_bytes = event.get("total_bytes")
            if (
                isinstance(total_bytes, bool)
                or not isinstance(total_bytes, int)
                or total_bytes < 0
                or total_bytes != lifecycle["bytes"]
            ):
                raise AirflowException(
                    "provider ledger lease close differs from byte deltas"
                )
            lifecycle["closed"] = True
            continue
        raise AirflowException("provider ledger has an unknown lifecycle event")
    incomplete = sorted(
        lease_id
        for lease_id, lifecycle in lease_lifecycle.items()
        if lifecycle["closed"] is not True
    )
    if incomplete:
        raise AirflowException(
            "provider ledger has incomplete lease lifecycle: "
            + ", ".join(incomplete[:10])
        )
    return totals, {
        allocation_id: dict(values)
        for allocation_id, values in bytes_by_lease_url.items()
    }


def _reconcile_canary_allocation(
    *,
    phase: str,
    task_report_provider_bytes: int,
    request_ledger_provider_bytes: int,
    proxy_ledger_provider_bytes: int,
    campaign_allocation_provider_bytes: int,
    task_report_bytes_by_url: Mapping[str, int],
    request_bytes_by_lease_url: Mapping[tuple[str, str], int],
    proxy_bytes_by_lease_url: Mapping[tuple[str, str], int],
    campaign_attempts: object,
    expected_attempt_id: str,
) -> int:
    counters = (
        task_report_provider_bytes,
        request_ledger_provider_bytes,
        proxy_ledger_provider_bytes,
        campaign_allocation_provider_bytes,
    )
    if len(set(counters)) != 1:
        raise AirflowException(
            f"{phase} allocation report/request/proxy/campaign bytes differ"
        )
    if dict(request_bytes_by_lease_url) != dict(proxy_bytes_by_lease_url):
        raise AirflowException(
            f"{phase} allocation request/proxy lease and URL bytes differ"
        )
    request_bytes_by_url: dict[str, int] = defaultdict(int)
    for (_lease_id, canonical_url), amount in request_bytes_by_lease_url.items():
        request_bytes_by_url[canonical_url] += int(amount)
    if dict(task_report_bytes_by_url) != dict(request_bytes_by_url):
        raise AirflowException(
            f"{phase} allocation task report/request URL bytes differ"
        )
    if not isinstance(campaign_attempts, list):
        raise AirflowException(f"{phase} allocation campaign attempts are malformed")
    expected_attempt_hash = hashlib.sha256(
        expected_attempt_id.encode("utf-8")
    ).hexdigest()
    expected_by_lease_hash: dict[str, tuple[str, int]] = {}
    for (lease_id, canonical_url), amount in request_bytes_by_lease_url.items():
        lease_hash = hashlib.sha256(lease_id.encode("utf-8")).hexdigest()
        if lease_hash in expected_by_lease_hash:
            raise AirflowException(
                f"{phase} allocation reused one lease for multiple URLs"
            )
        expected_by_lease_hash[lease_hash] = (
            hashlib.sha256(canonical_url.encode("utf-8")).hexdigest(),
            int(amount),
        )
    actual_by_lease_hash: dict[str, tuple[str, int]] = {}
    for raw_attempt in campaign_attempts:
        if not isinstance(raw_attempt, Mapping):
            raise AirflowException(f"{phase} allocation campaign attempt is malformed")
        lease_hash = raw_attempt.get("lease_id_hash")
        canonical_url_hash = raw_attempt.get("canonical_url_sha256")
        amount = raw_attempt.get("provider_billed_bytes")
        unsettled = raw_attempt.get("unsettled_provider_reservation_bytes", 0)
        if (
            not isinstance(lease_hash, str)
            or _SHA256.fullmatch(lease_hash) is None
            or lease_hash in actual_by_lease_hash
            or not isinstance(canonical_url_hash, str)
            or _SHA256.fullmatch(canonical_url_hash) is None
            or raw_attempt.get("attempt_id_hash") != expected_attempt_hash
            or isinstance(amount, bool)
            or not isinstance(amount, int)
            or amount < 0
            or isinstance(unsettled, bool)
            or not isinstance(unsettled, int)
            or unsettled != 0
            or raw_attempt.get("expired") is not False
        ):
            raise AirflowException(
                f"{phase} allocation campaign attempt identity is invalid"
            )
        actual_by_lease_hash[lease_hash] = (canonical_url_hash, amount)
    if actual_by_lease_hash != expected_by_lease_hash:
        raise AirflowException(
            f"{phase} allocation campaign lease attempts differ from request ledger"
        )
    return task_report_provider_bytes


def _task_report_provider_bytes_by_url(
    report: Mapping[str, Any], *, expected_total: int
) -> dict[str, int]:
    traffic = report.get("traffic")
    raw = (
        traffic.get("paid_proxy_bytes_by_url") if isinstance(traffic, Mapping) else None
    )
    if not isinstance(raw, Mapping):
        raise AirflowException("runner report has no paid_proxy_bytes_by_url map")
    normalized: dict[str, int] = {}
    for raw_url, raw_amount in raw.items():
        if not isinstance(raw_url, str) or not raw_url:
            raise AirflowException("runner report has an invalid paid URL")
        parsed = urlsplit(raw_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname not in {"www.whoscored.com", "1xbet.whoscored.com"}
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port not in {None, 443}
            or parsed.fragment
        ):
            raise AirflowException("runner report has a non-canonical paid URL")
        if (
            isinstance(raw_amount, bool)
            or not isinstance(raw_amount, int)
            or raw_amount < 0
        ):
            raise AirflowException("runner report has invalid paid URL bytes")
        normalized[raw_url] = raw_amount
    if sum(normalized.values()) != expected_total:
        raise AirflowException("runner report paid URL bytes differ from its total")
    return normalized


def build_budget_model(
    observations: Mapping[str, Sequence[int]],
    remaining_targets: Mapping[str, int],
    *,
    target_basis: str = VERIFIED_REMAINING_TARGET_BASIS,
    minimum_samples: int = CANARY_MIN_SAMPLES_PER_CLASS,
    required_classes: Sequence[str] = REQUIRED_MODEL_CLASSES,
) -> dict[str, Any]:
    """Build p95/class evidence and the reviewed 25% full-budget formula."""

    classes: dict[str, dict[str, int]] = {}
    for name, raw_values in sorted(observations.items()):
        values = [int(value) for value in raw_values]
        if any(value < 0 for value in values):
            raise AirflowException("provider observations must be non-negative")
        if not values:
            continue
        classes[name] = {
            "samples": len(values),
            "provider_billed_bytes": sum(values),
            "p50_provider_billed_bytes": _nearest_rank(values, 0.50),
            "p95_provider_billed_bytes": _nearest_rank(values, 0.95),
            "max_provider_billed_bytes": max(values),
        }
    required = tuple(sorted(set(str(name) for name in required_classes)))
    missing_targets = sorted(
        name for name in required if int(remaining_targets.get(name, 0)) <= 0
    )
    modeled_classes = set(required) | {
        name for name, count in remaining_targets.items() if count > 0
    }
    undersampled = sorted(
        name
        for name in modeled_classes
        if name not in classes or classes[name]["samples"] < minimum_samples
    )
    missing = sorted(set((*missing_targets, *undersampled)))
    weighted = None
    cap = None
    status = "ready"
    if target_basis != VERIFIED_REMAINING_TARGET_BASIS:
        status = "unverified_remaining_targets"
    elif missing_targets:
        status = "incomplete_remaining_targets"
    elif undersampled:
        status = "insufficient_samples"
    elif status == "ready":
        weighted = sum(
            count * classes[name]["p95_provider_billed_bytes"]
            for name, count in remaining_targets.items()
            if count
        )
        # ceil(weighted * 1.25), expressed as exact integer arithmetic.
        cap = (weighted * 5 + 3) // 4
    return {
        "status": status,
        "target_basis": target_basis,
        "formula": "ceil(sum(remaining_targets[class] * p95_bytes[class]) * 1.25)",
        "safety_factor": {"numerator": 5, "denominator": 4},
        "minimum_samples_per_class": minimum_samples,
        "required_classes": list(required),
        "remaining_targets": dict(sorted(remaining_targets.items())),
        "missing_or_zero_target_classes": missing_targets,
        "undersampled_classes": undersampled,
        "missing_or_undersampled_classes": missing,
        "classes": classes,
        "weighted_p95_provider_bytes": weighted,
        "proposed_full_cap_provider_bytes": cap,
        "requires_separate_signed_approval": True,
    }


def persist_canary_measurement(
    approval_metadata: Mapping[str, Any],
    execution_metadata: Mapping[str, Any],
    **context: Any,
) -> dict[str, object]:
    """Require four-way byte equality, then publish immutable evidence."""

    approval, _path = _load_canary_approval(context)
    if (
        approval_metadata.get("campaign_id") != approval.campaign_id
        or execution_metadata.get("campaign_id") != approval.campaign_id
        or execution_metadata.get("approval_sha256") != approval.approval_sha256
    ):
        raise AirflowException("measurement inputs belong to another campaign")
    raw_phases = execution_metadata.get("phases")
    if not isinstance(raw_phases, Mapping) or set(raw_phases) != {
        "discovery",
        "capture",
    }:
        raise AirflowException("measurement has no exact discovery/capture phases")
    run_dir = _run_directory(context).resolve()
    phase_allocations = {
        "discovery": CANARY_DISCOVERY_ALLOCATION_ID,
        "capture": CANARY_ALLOCATION_ID,
    }
    reported = 0
    request_bytes = 0
    reported_by_allocation: dict[str, int] = {}
    request_bytes_by_allocation: dict[str, int] = {}
    request_lease_url_bytes: dict[str, dict[tuple[str, str], int]] = {}
    report_url_bytes: dict[str, dict[str, int]] = {}
    expected_attempts: dict[str, str] = {}
    reports: dict[str, dict[str, Any]] = {}
    report_payloads: dict[str, bytes] = {}
    request_payloads: dict[str, bytes] = {}
    observations: dict[str, list[int]] = defaultdict(list)
    route_requests: dict[str, int] = defaultdict(int)
    route_wire_bytes: dict[str, int] = defaultdict(int)
    request_event_count = 0
    for phase, allocation_id in phase_allocations.items():
        metadata = raw_phases.get(phase)
        if (
            not isinstance(metadata, Mapping)
            or metadata.get("allocation_id") != allocation_id
        ):
            raise AirflowException("measurement phase allocation identity is invalid")
        report_path = Path(str(metadata.get("report_path") or ""))
        request_path = Path(str(metadata.get("request_ledger_path") or ""))
        for path in (report_path, request_path):
            try:
                path.resolve(strict=True).relative_to(run_dir)
            except (OSError, ValueError) as exc:
                raise AirflowException(
                    "measurement staging path escaped its DagRun"
                ) from exc
        report = _read_json_object(report_path)
        allocation = approval.allocation(allocation_id)
        attempt_id = str(metadata.get("attempt_id") or "")
        phase_reported = report.get("paid_proxy_bytes")
        if (
            isinstance(phase_reported, bool)
            or not isinstance(phase_reported, int)
            or phase_reported < 0
        ):
            raise AirflowException("runner report has invalid paid_proxy_bytes")
        if (
            not attempt_id
            or report.get("transport_policy") != "direct_then_paid"
            or report.get("proxy_approval_id") != approval.approval_id
            or report.get("proxy_approval_sha256") != approval.approval_sha256
            or report.get("proxy_allocation_id") != allocation_id
            or report.get("proxy_work_item_id") != allocation.work_item_id
            or report.get("proxy_attempt_id") != attempt_id
        ):
            raise AirflowException("runner report has another signed work identity")
        report_url_bytes[allocation_id] = _task_report_provider_bytes_by_url(
            report, expected_total=phase_reported
        )
        reported += phase_reported
        reported_by_allocation[allocation_id] = phase_reported
        (
            phase_request_bytes,
            phase_traffic,
            phase_observations,
            phase_lease_url_bytes,
        ) = _request_measurement(
            request_path,
            approval,
            allocation_id=allocation_id,
            expected_attempt_id=attempt_id,
        )
        request_bytes += phase_request_bytes
        request_bytes_by_allocation[allocation_id] = phase_request_bytes
        request_lease_url_bytes[allocation_id] = phase_lease_url_bytes
        expected_attempts[allocation_id] = attempt_id
        request_event_count += int(phase_traffic["request_event_count"])
        for route, count in phase_traffic["route_requests"].items():
            route_requests[str(route)] += int(count)
        for route, count in phase_traffic["route_wire_bytes"].items():
            route_wire_bytes[str(route)] += int(count)
        for workload_class, values in phase_observations.items():
            observations[workload_class].extend(values)
        reports[phase] = report
        report_payloads[phase] = report_path.read_bytes()
        request_payloads[phase] = request_path.read_bytes()
        if (
            metadata.get("report_sha256")
            != hashlib.sha256(report_payloads[phase]).hexdigest()
            or metadata.get("request_ledger_sha256")
            != hashlib.sha256(request_payloads[phase]).hexdigest()
        ):
            raise AirflowException("measurement phase artifacts changed after sealing")
    traffic = {
        "request_event_count": request_event_count,
        "route_requests": dict(sorted(route_requests.items())),
        "route_wire_bytes": dict(sorted(route_wire_bytes.items())),
    }
    provider_path = Path(
        os.environ.get("PROXY_FILTER_LEDGER_PATH", PROVIDER_LEDGER_PATH)
    )
    provider_bytes_by_allocation, provider_lease_url_bytes = _provider_ledger_bytes(
        provider_path,
        approval,
        expected_attempts=expected_attempts,
    )
    provider_bytes = sum(provider_bytes_by_allocation.values())
    campaign_snapshot = _paid_campaign_gateway_call(approval, "snapshot")
    allocation_state = campaign_snapshot.get("allocations")
    if not isinstance(allocation_state, Mapping):
        raise AirflowException("campaign completion allocation state is malformed")
    reconciled_by_allocation: dict[str, int] = {}
    for phase, allocation_id in phase_allocations.items():
        sealed = allocation_state.get(allocation_id)
        metadata = raw_phases[phase]
        if (
            not isinstance(sealed, Mapping)
            or sealed.get("completed") is not True
            or sealed.get("report_sha256") != metadata.get("report_sha256")
            or sealed.get("request_ledger_sha256")
            != metadata.get("request_ledger_sha256")
        ):
            raise AirflowException(
                "campaign allocation completion does not bind phase artifacts"
            )
        campaign_allocation_bytes = sealed.get("spent_provider_bytes")
        if (
            isinstance(campaign_allocation_bytes, bool)
            or not isinstance(campaign_allocation_bytes, int)
            or campaign_allocation_bytes < 0
        ):
            raise AirflowException("campaign allocation has an invalid byte count")
        reconciled_by_allocation[allocation_id] = _reconcile_canary_allocation(
            phase=phase,
            task_report_provider_bytes=reported_by_allocation[allocation_id],
            request_ledger_provider_bytes=request_bytes_by_allocation[allocation_id],
            proxy_ledger_provider_bytes=provider_bytes_by_allocation[allocation_id],
            campaign_allocation_provider_bytes=campaign_allocation_bytes,
            task_report_bytes_by_url=report_url_bytes[allocation_id],
            request_bytes_by_lease_url=request_lease_url_bytes[allocation_id],
            proxy_bytes_by_lease_url=provider_lease_url_bytes[allocation_id],
            campaign_attempts=sealed.get("attempts"),
            expected_attempt_id=expected_attempts[allocation_id],
        )
    campaign_bytes = _paid_campaign_gateway_call(
        approval,
        "assert_exact_accounting",
        task_report_provider_bytes=reported,
        request_ledger_provider_bytes=request_bytes,
        proxy_ledger_provider_bytes=provider_bytes,
        require_complete=True,
    )
    if campaign_bytes != sum(reconciled_by_allocation.values()):
        raise AirflowException("campaign total differs from its reconciled allocations")
    if campaign_bytes > WHOSCORED_CANARY_CAP_BYTES:
        raise AirflowException("measurement exceeded the signed decimal 1 GB cap")
    raw_targets = execution_metadata.get("remaining_targets")
    remaining_target_basis = str(execution_metadata.get("remaining_target_basis") or "")
    target_provenance_sha256 = str(
        execution_metadata.get("remaining_target_provenance_sha256") or ""
    )
    if (
        not isinstance(raw_targets, Mapping)
        or not remaining_target_basis
        or _SHA256.fullmatch(target_provenance_sha256) is None
    ):
        raise AirflowException("measurement lost immutable target provenance")
    cohort_reference = execution_metadata.get("cohort_artifact")
    if not isinstance(cohort_reference, Mapping) or set(cohort_reference) != {
        "uri",
        "key",
        "sha256",
        "bytes",
    }:
        raise AirflowException("measurement has no immutable cohort reference")
    from dags.scripts.whoscored_ops_store import WhoScoredOpsStore

    ops_store = WhoScoredOpsStore.from_env(optional=False)
    if ops_store is None:  # pragma: no cover - optional=False is fail closed
        raise AirflowException("WhoScored operational store is required")
    cohort = ops_store.read_content_addressed_json(
        str(cohort_reference["key"]),
        expected_sha256=str(cohort_reference["sha256"]),
        expected_bytes=int(cohort_reference["bytes"]),
    )
    cohort_sha256 = str(execution_metadata.get("cohort_sha256") or "")
    target_provenance = cohort.get("remaining_target_provenance")
    if (
        _SHA256.fullmatch(cohort_sha256) is None
        or hashlib.sha256(canonical_json_bytes(cohort)).hexdigest() != cohort_sha256
        or not isinstance(target_provenance, Mapping)
        or hashlib.sha256(canonical_json_bytes(target_provenance)).hexdigest()
        != target_provenance_sha256
        or cohort.get("remaining_target_provenance_sha256") != target_provenance_sha256
        or target_provenance.get("remaining_targets") != dict(raw_targets)
        or target_provenance.get("target_basis") != remaining_target_basis
    ):
        raise AirflowException("immutable cohort/target provenance does not match")
    remaining_targets: dict[str, int] = {}
    for raw_name, raw_count in raw_targets.items():
        name = str(raw_name)
        if (
            not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", name)
            or isinstance(raw_count, bool)
            or not isinstance(raw_count, int)
            or raw_count <= 0
        ):
            raise AirflowException("immutable target population is malformed")
        remaining_targets[name] = raw_count
    model = build_budget_model(
        observations,
        remaining_targets,
        target_basis=remaining_target_basis,
    )
    evidence = {
        "schema_version": 1,
        "evidence_type": "whoscored_proxy_measurement_canary",
        "dag_id": DAG_ID,
        "run_id": str(context.get("run_id") or ""),
        "campaign_id": approval.campaign_id,
        "approval_id": approval.approval_id,
        "approval_sha256": approval.approval_sha256,
        "runtime_sha256": approval.runtime_sha256,
        "classifier_sha256": approval.classifier_sha256,
        "transport_policy": "direct_then_paid",
        "meter": PROXY_CAMPAIGN_METER,
        "signed_cap_provider_bytes": WHOSCORED_CANARY_CAP_BYTES,
        "provider_billed_bytes": campaign_bytes,
        "remaining_signed_provider_bytes": (
            WHOSCORED_CANARY_CAP_BYTES - campaign_bytes
        ),
        "exact_accounting": {
            "task_report_provider_bytes": reported,
            "request_ledger_provider_bytes": request_bytes,
            "proxy_ledger_provider_bytes": provider_bytes,
            "campaign_allocation_ledger_provider_bytes": campaign_bytes,
            "all_equal": True,
            "allocations": {
                allocation_id: {
                    "task_report_provider_bytes": reported_by_allocation[allocation_id],
                    "request_ledger_provider_bytes": request_bytes_by_allocation[
                        allocation_id
                    ],
                    "proxy_ledger_provider_bytes": provider_bytes_by_allocation[
                        allocation_id
                    ],
                    "campaign_allocation_ledger_provider_bytes": value,
                    "lease_url_maps_equal": True,
                }
                for allocation_id, value in sorted(reconciled_by_allocation.items())
            },
        },
        "runner_phases": {
            phase: {
                "status": reports[phase].get("status"),
                "returncode": raw_phases[phase].get("returncode"),
                "queue": reports[phase].get("queue"),
                "allocation_id": phase_allocations[phase],
                "report_sha256": hashlib.sha256(report_payloads[phase]).hexdigest(),
                "request_ledger_sha256": hashlib.sha256(
                    request_payloads[phase]
                ).hexdigest(),
            }
            for phase in ("discovery", "capture")
        },
        "cohort": {
            "sha256": execution_metadata.get("cohort_sha256"),
            "artifact": execution_metadata.get("cohort_artifact"),
            "remaining_target_provenance_sha256": target_provenance_sha256,
        },
        "traffic": traffic,
        "budget_model": model,
        "measured_at": reports["capture"].get("finished_at"),
    }
    prefix = (
        "proxy-campaigns/"
        f"{stable_safe_token(approval.campaign_id)}/canary/"
        f"{stable_safe_token(str(context.get('run_id') or 'unknown'))}"
    )
    artifact = ops_store.put_content_addressed_json(f"{prefix}/measurement", evidence)
    completion = {
        "schema_version": 1,
        "campaign_id": approval.campaign_id,
        "approval_id": approval.approval_id,
        "approval_sha256": approval.approval_sha256,
        "provider_billed_bytes": campaign_bytes,
        "signed_cap_provider_bytes": WHOSCORED_CANARY_CAP_BYTES,
        "budget_model_status": model["status"],
        "measurement_artifact": artifact,
    }
    completion_artifact = ops_store.put_json_immutable(
        f"{prefix}/completion.json", completion
    )
    return {
        "status": "measurement_persisted",
        "campaign_id": approval.campaign_id,
        "provider_billed_bytes": campaign_bytes,
        "signed_cap_provider_bytes": WHOSCORED_CANARY_CAP_BYTES,
        "budget_model_status": model["status"],
        "proposed_full_cap_provider_bytes": model["proposed_full_cap_provider_bytes"],
        "artifact_uri": artifact["uri"],
        "artifact_sha256": artifact["sha256"],
        "completion_uri": completion_artifact["uri"],
    }


def enforce_canary_gate(
    execution_metadata: Mapping[str, Any],
    measurement: Mapping[str, Any],
    **context: Any,
) -> dict[str, object]:
    """Make fatal runner/accounting failures visible at the terminal leaf."""

    dag_run = context.get("dag_run")
    if dag_run is None or not callable(getattr(dag_run, "get_task_instances", None)):
        raise AirflowException("canary final gate requires DagRun task-state context")
    required_upstreams = {
        "validate_whoscored_proxy_canary",
        "deliver_whoscored_proxy_canary_alert",
        CANARY_TASK_ID,
        "persist_whoscored_proxy_canary_measurement",
    }
    observed: dict[str, list[str]] = {task_id: [] for task_id in required_upstreams}
    for task_instance in dag_run.get_task_instances():
        if task_instance.task_id not in observed:
            continue
        observed[task_instance.task_id].append(
            str(task_instance.state or "none").lower().split(".")[-1]
        )
    failures = [
        f"{task_id}={states or ['missing']}"
        for task_id, states in sorted(observed.items())
        if not states or any(state != "success" for state in states)
    ]
    if failures:
        raise AirflowException(
            "canary final gate blocked by unsuccessful upstreams: "
            + ", ".join(failures)
        )

    if measurement.get("status") != "measurement_persisted":
        raise AirflowException("immutable canary measurement was not persisted")
    if execution_metadata.get("status") not in {"success", "retryable"}:
        raise AirflowException(
            f"canary workload failed: {execution_metadata.get('status')!r}"
        )
    spent = measurement.get("provider_billed_bytes")
    if isinstance(spent, bool) or not isinstance(spent, int):
        raise AirflowException("measurement returned an invalid byte count")
    if not 0 < spent <= WHOSCORED_CANARY_CAP_BYTES:
        raise AirflowException("measurement violated the signed byte cap")
    model_status = str(measurement.get("budget_model_status") or "")
    proposed = measurement.get("proposed_full_cap_provider_bytes")
    if model_status not in {
        "unverified_remaining_targets",
        "incomplete_remaining_targets",
        "insufficient_samples",
        "ready",
    }:
        raise AirflowException("measurement returned an unknown budget-model status")
    if model_status == "ready":
        if isinstance(proposed, bool) or not isinstance(proposed, int) or proposed <= 0:
            raise AirflowException(
                "canary did not produce a positive full-budget model"
            )
    elif proposed is not None:
        raise AirflowException(
            "non-authorizing measurement unexpectedly proposed a full budget"
        )
    if model_status != "ready" or execution_metadata.get("status") != "success":
        return {
            "status": "measurement_recorded_non_authorizing",
            "provider_billed_bytes": spent,
            "signed_cap_provider_bytes": WHOSCORED_CANARY_CAP_BYTES,
            "budget_model_status": model_status,
            "proposed_full_cap_provider_bytes": None,
            "full_approval_eligible": False,
            "requires_followup_measurement": True,
            "requires_separate_full_approval": True,
        }
    return {
        "status": "measurement_recorded",
        "provider_billed_bytes": spent,
        "signed_cap_provider_bytes": WHOSCORED_CANARY_CAP_BYTES,
        "budget_model_status": model_status,
        "proposed_full_cap_provider_bytes": proposed,
        "full_approval_eligible": True,
        "requires_followup_measurement": False,
        "requires_separate_full_approval": True,
    }


CANARY_ARGS = {
    **{
        key: value
        for key, value in SCRAPER_ARGS.items()
        if key not in {"pool", "retries", "retry_delay", "execution_timeout"}
    },
    "retries": 0,
}


with DAG(
    dag_id=DAG_ID,
    default_args=CANARY_ARGS,
    description="Paused 1 GB direct-first WhoScored paid-proxy measurement",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=CANARY_DAGRUN_TIMEOUT,
    is_paused_upon_creation=True,
    params={
        "transport_policy": "direct_only",
        "paid_approval_id": "",
        "paid_approval_sha256": "",
    },
    tags=DAG_TAGS.get("whoscored", ["scraping", "whoscored", "canary"]),
    doc_md="""
    Manual and paused by default. The DAG accepts only an approval ID/SHA pin;
    the mounted 0600 HMAC document fixes exactly 1,000,000,000 decimal provider
    bytes split between signed discovery and capture allocations. A real
    production alert must be delivered before full-history discovery; the
    fixed representative cohort is frozen from that exact generation before
    capture starts.
    """,
) as dag:
    validate = PythonOperator(
        task_id="validate_whoscored_proxy_canary",
        python_callable=validate_canary_approval,
        pool=SOURCE_POOL,
        pool_slots=EXPECTED_SOURCE_POOL_SLOTS,
        execution_timeout=timedelta(minutes=5),
    )
    alert = PythonOperator(
        task_id=CANARY_ALERT_TASK_ID,
        python_callable=deliver_canary_alert,
        op_kwargs={"approval_metadata": validate.output},
        execution_timeout=timedelta(minutes=2),
    )
    execute = PythonOperator(
        task_id=CANARY_TASK_ID,
        python_callable=execute_measurement_canary,
        op_kwargs={
            "approval_metadata": validate.output,
            "alert_metadata": alert.output,
        },
        pool=SOURCE_POOL,
        pool_slots=EXPECTED_SOURCE_POOL_SLOTS,
        retries=0,
        execution_timeout=CANARY_EXECUTION_TIMEOUT,
    )
    measurement = PythonOperator(
        task_id="persist_whoscored_proxy_canary_measurement",
        python_callable=persist_canary_measurement,
        op_kwargs={
            "approval_metadata": validate.output,
            "execution_metadata": execute.output,
        },
        pool=SOURCE_POOL,
        pool_slots=EXPECTED_SOURCE_POOL_SLOTS,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=20),
    )
    final_gate = PythonOperator(
        task_id="final_whoscored_proxy_canary_gate",
        python_callable=enforce_canary_gate,
        op_kwargs={
            "execution_metadata": execute.output,
            "measurement": measurement.output,
        },
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=5),
    )

    validate >> alert >> execute >> measurement >> final_gate
