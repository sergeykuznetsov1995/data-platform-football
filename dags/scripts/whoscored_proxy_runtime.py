"""Airflow-side authority projection for WhoScored paid proxy campaigns.

Scheduled and legacy DagRuns stay direct-only.  Paid transport is admitted
only when the DagRun configuration pins one signed approval by ID and SHA-256;
the approval path itself is deployment-owned and is never accepted from
DagRun input.
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

import hashlib
import json
import os
import re
import shlex
import stat
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

from scrapers.whoscored.proxy_campaign import (
    PROXY_ALLOCATION_ID_ENV,
    PROXY_APPROVAL_ID_ENV,
    PROXY_APPROVAL_PATH_ENV,
    PROXY_APPROVAL_SHA256_ENV,
    PROXY_ATTEMPT_ID_ENV,
    TRANSPORT_POLICY_DIRECT_THEN_PAID,
    WHOSCORED_CANARY_DAG_ID,
    WHOSCORED_FULL_PAID_CRAWL_AVAILABLE,
    WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE,
    WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE,
    ProxyCampaignApproval,
    ProxyCampaignError,
    ProxyCampaignValidationError,
    ProxyWorkAllocation,
    canonical_json_bytes,
    daily_ingest_paid_crawl_allowed,
    deterministic_proxy_attempt_id,
    load_proxy_campaign_approval_structure,
    strict_json_loads,
)


TRANSPORT_POLICY_DIRECT_ONLY = "direct_only"
TRANSPORT_POLICIES = frozenset(
    {TRANSPORT_POLICY_DIRECT_ONLY, TRANSPORT_POLICY_DIRECT_THEN_PAID}
)
PAID_APPROVAL_ID_CONF = "paid_approval_id"
PAID_APPROVAL_SHA256_CONF = "paid_approval_sha256"
PROXY_APPROVAL_ROOT_ENV = "WHOSCORED_PROXY_APPROVAL_ROOT"
# Deployment-owned directory of per-run pointers that let a *scheduled* daily
# ingest run reach the signed paid approval issued for it out of band, without
# ever accepting authority from DagRun input.  Absent env or file -> the run
# stays direct-only (fail-closed).
SCHEDULED_PAID_POINTER_ROOT_ENV = "WHOSCORED_SCHEDULED_PAID_POINTER_ROOT"
SCHEDULED_PAID_POINTER_SCHEMA_VERSION = 1
_MAX_POINTER_BYTES = 4096
_POINTER_FIELDS = frozenset(
    {"schema_version", "dag_id", "run_id", "approval_id", "approval_sha256"}
)
PAID_ALERT_PREFLIGHT_TASK_ID = "validate_whoscored_paid_alert_delivery"
PAID_GATEWAY_URL_ENV = "WHOSCORED_PAID_GATEWAY_URL"
PAID_GATEWAY_TOKEN_ENV = "WHOSCORED_PAID_GATEWAY_TOKEN"
EXPECTED_PAID_GATEWAY_URL = "http://whoscored_paid_gateway:8898"
_RUNNER_FORBIDDEN_AUTHORITY_ENV_NAMES = (
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
_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_CAMPAIGN_ENV_NAMES = (
    PROXY_APPROVAL_PATH_ENV,
    PROXY_APPROVAL_ID_ENV,
    PROXY_APPROVAL_SHA256_ENV,
    PROXY_ALLOCATION_ID_ENV,
    PROXY_ATTEMPT_ID_ENV,
)
_MINIMUM_APPROVAL_VALIDITY = {
    "dag_ingest_whoscored": timedelta(hours=6),
    "dag_backfill_whoscored": timedelta(hours=12),
    "dag_canary_whoscored_proxy": timedelta(hours=8),
}
CLASSIFIER_RUNTIME_FILES = (
    "configs/medallion/competitions.yaml",
    "scrapers/whoscored/catalog.py",
    "scrapers/whoscored/domain.py",
)


class WhoScoredProxyRuntimeError(RuntimeError):
    """Raised before source work when paid authority is incomplete or stale."""


def _dag_run_conf(context: Mapping[str, Any]) -> Mapping[str, Any]:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None)
    return conf if isinstance(conf, Mapping) else {}


def _dag_id(context: Mapping[str, Any]) -> str:
    dag = context.get("dag")
    value = getattr(dag, "dag_id", None) or context.get("dag_id")
    if not value:
        dag_run = context.get("dag_run")
        value = getattr(dag_run, "dag_id", None)
    return str(value or "")


def _run_id(context: Mapping[str, Any]) -> str:
    value = context.get("run_id")
    if not value:
        dag_run = context.get("dag_run")
        value = getattr(dag_run, "run_id", None)
    return str(value or "")


def _run_type(context: Mapping[str, Any]) -> str:
    dag_run = context.get("dag_run")
    return str(getattr(dag_run, "run_type", "") or "")


def _scheduled_pointer_path(raw_root: str, run_id: str) -> Path | None:
    """Resolve one deployment-owned pointer without following any link.

    Returns ``None`` when the pointer does not exist (the scheduled run then
    stays direct-only, fail-closed).  A present-but-unsafe pointer -- wrong
    owner, mode, a symlink, or one escaping its mounted root -- raises.
    """

    root_path = Path(raw_root)
    if not root_path.is_absolute():
        raise WhoScoredProxyRuntimeError(
            "scheduled paid pointer root must be absolute"
        )
    if root_path.is_symlink():
        raise WhoScoredProxyRuntimeError(
            "scheduled paid pointer root must not be a symlink"
        )
    name = hashlib.sha256(run_id.encode("utf-8")).hexdigest() + ".json"
    path = root_path / name
    try:
        metadata = path.stat(follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise WhoScoredProxyRuntimeError(
            "scheduled paid pointer is not mounted"
        ) from exc
    if stat.S_ISLNK(metadata.st_mode):
        raise WhoScoredProxyRuntimeError(
            "scheduled paid pointer must not be a symlink"
        )
    if not stat.S_ISREG(metadata.st_mode):
        raise WhoScoredProxyRuntimeError("scheduled paid pointer must be a file")
    if metadata.st_uid != os.geteuid():
        raise WhoScoredProxyRuntimeError(
            "scheduled paid pointer must be owned by the Airflow runtime UID"
        )
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise WhoScoredProxyRuntimeError(
            "scheduled paid pointer must have mode 0600"
        )
    try:
        root = root_path.resolve(strict=True)
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise WhoScoredProxyRuntimeError(
            "scheduled paid pointer escapes its mounted root"
        ) from exc
    return resolved


def _scheduled_paid_pins(context: Mapping[str, Any]) -> dict[str, str] | None:
    """Return signed-approval pins for an eligible scheduled paid run, if any.

    Only a ``scheduled`` DagRun of a DAG admitted to the daily paid path can be
    upgraded here.  The backfill DAG is never admitted (``daily_ingest_paid_
    crawl_allowed`` is False for it), so the historical crawl child-lock is
    untouched.  A missing env or pointer yields ``None`` (direct-only); a
    tampered or mismatched pointer raises.
    """

    if _run_type(context) != "scheduled":
        return None
    dag_id = _dag_id(context)
    if not daily_ingest_paid_crawl_allowed(dag_id):
        return None
    raw_root = str(os.environ.get(SCHEDULED_PAID_POINTER_ROOT_ENV) or "").strip()
    if not raw_root:
        return None
    run_id = _run_id(context)
    if not run_id:
        return None
    path = _scheduled_pointer_path(raw_root, run_id)
    if path is None:
        return None
    try:
        size = path.stat().st_size
        if size <= 0 or size > _MAX_POINTER_BYTES:
            raise WhoScoredProxyRuntimeError(
                "scheduled paid pointer has an invalid size"
            )
        data = strict_json_loads(path.read_bytes().decode("utf-8"))
    except WhoScoredProxyRuntimeError:
        raise
    except (OSError, UnicodeDecodeError, ProxyCampaignError, ValueError) as exc:
        raise WhoScoredProxyRuntimeError(
            "scheduled paid pointer is unreadable"
        ) from exc
    if not isinstance(data, Mapping) or frozenset(data) != _POINTER_FIELDS:
        raise WhoScoredProxyRuntimeError("scheduled paid pointer schema is invalid")
    if data.get("schema_version") != SCHEDULED_PAID_POINTER_SCHEMA_VERSION:
        raise WhoScoredProxyRuntimeError(
            "unsupported scheduled paid pointer schema"
        )
    if data.get("dag_id") != dag_id or data.get("run_id") != run_id:
        raise WhoScoredProxyRuntimeError(
            "scheduled paid pointer identity does not match this DagRun"
        )
    approval_id = data.get("approval_id")
    approval_sha256 = data.get("approval_sha256")
    if (
        not isinstance(approval_id, str)
        or not approval_id
        or not isinstance(approval_sha256, str)
        or _SHA256_RE.fullmatch(approval_sha256) is None
    ):
        raise WhoScoredProxyRuntimeError("scheduled paid pointer pins are malformed")
    return {"approval_id": approval_id, "approval_sha256": approval_sha256}


def _effective_transport_conf(context: Mapping[str, Any]) -> Mapping[str, Any]:
    """DagRun conf, or the signed pins a scheduled run's pointer provides.

    Explicit DagRun conf always wins, so the manual paid flow and any explicit
    ``direct_only`` request are never overridden.  The pointer only fills in the
    pins for an eligible scheduled run whose conf carries no transport intent.
    """

    conf = _dag_run_conf(context)
    if (
        conf.get("transport_policy")
        or conf.get(PAID_APPROVAL_ID_CONF)
        or conf.get(PAID_APPROVAL_SHA256_CONF)
    ):
        return conf
    pins = _scheduled_paid_pins(context)
    if pins is None:
        return conf
    effective = dict(conf)
    effective["transport_policy"] = TRANSPORT_POLICY_DIRECT_THEN_PAID
    effective[PAID_APPROVAL_ID_CONF] = pins["approval_id"]
    effective[PAID_APPROVAL_SHA256_CONF] = pins["approval_sha256"]
    return effective


def resolve_transport_policy(context: Mapping[str, Any]) -> str:
    """Resolve policy from DagRun conf or a scheduled run's signed pointer.

    Booleans can never enable paid; only a signed approval id + SHA-256 (from
    conf or a deployment-owned pointer) does.
    """

    conf = _effective_transport_conf(context)
    policy = str(conf.get("transport_policy") or TRANSPORT_POLICY_DIRECT_ONLY).strip()
    if policy not in TRANSPORT_POLICIES:
        raise WhoScoredProxyRuntimeError(
            f"unsupported WhoScored transport_policy: {policy!r}"
        )
    approval_id = str(conf.get(PAID_APPROVAL_ID_CONF) or "").strip()
    approval_sha256 = str(conf.get(PAID_APPROVAL_SHA256_CONF) or "").strip()
    if policy == TRANSPORT_POLICY_DIRECT_ONLY:
        if approval_id or approval_sha256:
            raise WhoScoredProxyRuntimeError(
                "paid approval pins require transport_policy=direct_then_paid"
            )
        return policy
    if not approval_id or _SHA256_RE.fullmatch(approval_sha256) is None:
        raise WhoScoredProxyRuntimeError(
            "direct_then_paid requires paid_approval_id and a lowercase "
            "paid_approval_sha256 in DagRun conf"
        )
    return policy


def classifier_code_sha256(runtime_root: Path | None = None) -> str:
    """Hash code and static registry data that decide catalog eligibility."""

    if runtime_root is None:
        files = {
            relative: _WHOSCORED_RUNTIME_CONTRACT.attested_runtime_file_sha256(
                relative,
                runtime_root=Path(_whoscored_root),
            )
            for relative in CLASSIFIER_RUNTIME_FILES
        }
        return hashlib.sha256(canonical_json_bytes(files)).hexdigest()

    root = (
        Path(runtime_root).resolve()
        if runtime_root is not None
        else Path(__file__).resolve().parents[2]
    )
    files: dict[str, str] = {}
    for relative in CLASSIFIER_RUNTIME_FILES:
        candidate = root / relative
        try:
            path = candidate.resolve(strict=True)
        except OSError as exc:
            raise WhoScoredProxyRuntimeError(
                f"cannot resolve WhoScored classifier file {relative}: {exc}"
            ) from exc
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise WhoScoredProxyRuntimeError(
                f"WhoScored classifier file escapes runtime root: {relative}"
            ) from exc
        digest = hashlib.sha256()
        try:
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
        except OSError as exc:
            raise WhoScoredProxyRuntimeError(
                f"cannot hash WhoScored classifier file {path}: {exc}"
            ) from exc
        files[relative] = digest.hexdigest()
    return hashlib.sha256(canonical_json_bytes(files)).hexdigest()


@dataclass(frozen=True)
class PaidRuntime:
    """Validated policy plus, for paid work, one exact signed allocation."""

    policy: str
    approval_path: str = ""
    approval: ProxyCampaignApproval | None = None
    allocation: ProxyWorkAllocation | None = None

    @property
    def is_paid(self) -> bool:
        return self.policy == TRANSPORT_POLICY_DIRECT_THEN_PAID

    @property
    def campaign_id(self) -> str:
        return self.approval.campaign_id if self.approval is not None else ""

    @property
    def approval_id(self) -> str:
        return self.approval.approval_id if self.approval is not None else ""

    @property
    def approval_sha256(self) -> str:
        return self.approval.approval_sha256 if self.approval is not None else ""

    @property
    def dagrun_limit_bytes(self) -> int:
        return self.approval.caps.total_provider_bytes if self.approval else 0

    def cli_args(self, *, work_item_id: str | None = None) -> str:
        if not self.is_paid:
            return "--transport-policy direct_only"
        if self.approval is None or self.allocation is None:
            raise WhoScoredProxyRuntimeError(
                "paid CLI projection requires one validated work allocation"
            )
        expected_item = self.allocation.work_item_id
        if work_item_id is not None and str(work_item_id) != expected_item:
            raise WhoScoredProxyRuntimeError(
                "paid CLI work item differs from the signed allocation"
            )
        values = (
            ("--transport-policy", self.policy),
            ("--proxy-approval-path", self.approval_path),
            ("--proxy-approval-id", self.approval.approval_id),
            ("--proxy-approval-sha256", self.approval.approval_sha256),
            ("--proxy-work-item-id", expected_item),
        )
        return " ".join(f"{name} {shlex.quote(str(value))}" for name, value in values)

    def for_allocation(
        self, *, task_id: str, work_item_id: str, missing_ok: bool = False
    ) -> "PaidRuntime":
        """Bind an already verified approval to one exact work allocation.

        ``missing_ok`` handles catalog drift within a paid DagRun: a scope that
        discovery opened *after* the standing approval was issued has no signed
        allocation.  Rather than failing the whole run, that one scope degrades
        to direct-only transport (it is normally Cloudflare-challenged, becomes
        retryable, and is covered by the next day's approval).  Two or more
        matches always remain a hard error, and direct binding never touches the
        paid path.  Discovery and the constant profile work item never pass
        ``missing_ok`` because their allocations are always present.
        """

        if not self.is_paid:
            return self
        _WHOSCORED_RUNTIME_CONTRACT.require_production_runtime_class(
            operation="WhoScored paid proxy allocation"
        )
        if self.approval is None:
            raise WhoScoredProxyRuntimeError("paid runtime has no verified approval")
        matches = tuple(
            item
            for item in self.approval.allocations
            if item.task_id == str(task_id) and item.work_item_id == str(work_item_id)
        )
        if len(matches) > 1:
            raise WhoScoredProxyRuntimeError(
                "approval must contain exactly one allocation for task_id/work_item_id"
            )
        if not matches:
            if missing_ok:
                return PaidRuntime(policy=TRANSPORT_POLICY_DIRECT_ONLY)
            raise WhoScoredProxyRuntimeError(
                "approval must contain exactly one allocation for task_id/work_item_id"
            )
        return PaidRuntime(
            policy=self.policy,
            approval_path=self.approval_path,
            approval=self.approval,
            allocation=matches[0],
        )


def _verify_release_pins(approval: ProxyCampaignApproval) -> None:
    runtime = _WHOSCORED_RUNTIME_CONTRACT.validate_runtime_contract()
    actual_runtime = str(runtime.get("code_tree_sha256") or "")
    if approval.runtime_sha256 != actual_runtime:
        raise WhoScoredProxyRuntimeError(
            "paid approval is pinned to another WhoScored runtime release"
        )
    actual_classifier = classifier_code_sha256()
    if approval.classifier_sha256 != actual_classifier:
        raise WhoScoredProxyRuntimeError(
            "paid approval is pinned to another WhoScored catalog classifier"
        )


def _verify_approval_window(approval: ProxyCampaignApproval, *, dag_id: str) -> None:
    minimum = _MINIMUM_APPROVAL_VALIDITY.get(dag_id)
    if minimum is None:
        raise WhoScoredProxyRuntimeError(
            "paid approval is not valid for this WhoScored DAG"
        )
    try:
        expires_at = datetime.fromisoformat(approval.expires_at.replace("Z", "+00:00"))
    except ValueError as exc:  # pragma: no cover - parsed by core approval first
        raise WhoScoredProxyRuntimeError(
            "paid approval has an invalid expiry timestamp"
        ) from exc
    if expires_at - datetime.now(timezone.utc) < minimum:
        raise WhoScoredProxyRuntimeError(
            "paid approval expires before the complete DagRun timeout window"
        )


def _private_approval_path(
    raw_path: str,
    *,
    approval_id: str,
    raw_root: str,
) -> Path:
    """Resolve one immutable deployment-owned approval without following links."""

    path = Path(raw_path)
    root_path = Path(raw_root)
    try:
        if not path.is_absolute() or not root_path.is_absolute():
            raise WhoScoredProxyRuntimeError(
                "paid approval path and root must be absolute"
            )
        if path.is_symlink() or root_path.is_symlink():
            raise WhoScoredProxyRuntimeError(
                "paid approval path/root must not be a symlink"
            )
        lexical_relative = path.relative_to(root_path)
        if any(component in {"", ".", ".."} for component in lexical_relative.parts):
            raise WhoScoredProxyRuntimeError(
                "paid approval path contains a non-canonical component"
            )
        cursor = root_path
        for component in lexical_relative.parts:
            cursor = cursor / component
            if stat.S_ISLNK(cursor.lstat().st_mode):
                raise WhoScoredProxyRuntimeError(
                    "paid approval path must not contain symlink components"
                )
        root = root_path.resolve(strict=True)
        metadata = path.stat(follow_symlinks=False)
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
    except WhoScoredProxyRuntimeError:
        raise
    except ValueError as exc:
        raise WhoScoredProxyRuntimeError(
            "paid approval artifact is outside its mounted root"
        ) from exc
    except OSError as exc:
        raise WhoScoredProxyRuntimeError(
            "paid approval artifact is not mounted"
        ) from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise WhoScoredProxyRuntimeError("paid approval artifact must be a file")
    if metadata.st_uid != os.geteuid():
        raise WhoScoredProxyRuntimeError(
            "paid approval artifact must be owned by the Airflow runtime UID"
        )
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise WhoScoredProxyRuntimeError("paid approval artifact must have mode 0600")
    if resolved.name != f"{approval_id}.json":
        raise WhoScoredProxyRuntimeError(
            "paid approval filename must equal <paid_approval_id>.json"
        )
    return resolved


def resolve_paid_runtime(
    context: Mapping[str, Any],
    *,
    task_id: str | None = None,
    work_item_id: str | None = None,
    missing_ok: bool = False,
) -> PaidRuntime:
    """Validate policy, approval, DAG/release pins and optionally allocation."""

    policy = resolve_transport_policy(context)
    if policy == TRANSPORT_POLICY_DIRECT_ONLY:
        return PaidRuntime(policy=policy)
    if bool(task_id) != bool(work_item_id):
        raise WhoScoredProxyRuntimeError(
            "paid allocation lookup requires both task_id and work_item_id"
        )
    conf = _effective_transport_conf(context)
    raw_approval_path = str(os.environ.get(PROXY_APPROVAL_PATH_ENV) or "").strip()
    if not raw_approval_path:
        raise WhoScoredProxyRuntimeError(
            f"paid transport requires deployment env {PROXY_APPROVAL_PATH_ENV}"
        )
    raw_approval_root = str(os.environ.get(PROXY_APPROVAL_ROOT_ENV) or "").strip()
    if not raw_approval_root:
        raise WhoScoredProxyRuntimeError(
            f"paid transport requires deployment env {PROXY_APPROVAL_ROOT_ENV}"
        )
    try:
        approval_path = _private_approval_path(
            raw_approval_path,
            approval_id=str(conf[PAID_APPROVAL_ID_CONF]),
            raw_root=raw_approval_root,
        )
        approval = load_proxy_campaign_approval_structure(
            approval_path,
            expected_approval_id=str(conf[PAID_APPROVAL_ID_CONF]),
            expected_approval_sha256=str(conf[PAID_APPROVAL_SHA256_CONF]),
        )
        dag_id = _dag_id(context)
        if not dag_id or dag_id not in approval.allowed_dag_ids:
            raise ProxyCampaignValidationError(
                "DAG is absent from the signed paid approval"
            )
        if dag_id == WHOSCORED_CANARY_DAG_ID:
            if not approval.is_exact_canary:
                raise ProxyCampaignValidationError(
                    "WhoScored canary approval must match the exact 1 GB contract"
                )
        elif not (
            daily_ingest_paid_crawl_allowed(dag_id)
            or WHOSCORED_FULL_PAID_CRAWL_AVAILABLE
        ):
            raise ProxyCampaignValidationError(
                "WhoScored full paid crawl is disabled pending exact reconciliation"
            )
        if not WHOSCORED_PROVIDER_INVOICE_HARD_CAP_AVAILABLE:
            raise ProxyCampaignValidationError(
                "WhoScored paid traffic has no provider-side invoice hard cap"
            )
        if not WHOSCORED_PAID_APPLICATION_GATEWAY_AVAILABLE:
            raise ProxyCampaignValidationError(
                "WhoScored paid traffic has no authenticated isolated "
                "application gateway"
            )
        if _run_id(context) != approval.run_id:
            raise ProxyCampaignValidationError(
                "DagRun run_id differs from the signed paid approval"
            )
        _verify_approval_window(approval, dag_id=dag_id)
        _verify_release_pins(approval)
    except (ProxyCampaignError, OSError, ValueError) as exc:
        raise WhoScoredProxyRuntimeError(str(exc)) from exc
    runtime = PaidRuntime(
        policy=policy,
        approval_path=str(approval_path),
        approval=approval,
        allocation=None,
    )
    if task_id is not None and work_item_id is not None:
        runtime = runtime.for_allocation(
            task_id=task_id, work_item_id=work_item_id, missing_ok=missing_ok
        )
    return runtime


def stable_scope_work_item(scope: str) -> str:
    return "scope-" + hashlib.sha256(str(scope).encode("utf-8")).hexdigest()


# The profile refresh binds to a single constant work item.  The catalog batch
# is content-derived *inside* the DagRun, so a pre-issued standing approval can
# never predict a batch-scoped work item id; a constant one lets the daily
# issuer sign exactly one profiles allocation.  Per-URL and allocation byte caps
# still bound the work.
PROFILES_DAILY_WORK_ITEM = "profiles-daily"


def stable_profiles_work_item() -> str:
    return PROFILES_DAILY_WORK_ITEM


def paid_campaign_gateway_call(
    approval: ProxyCampaignApproval,
    operation: str,
    **arguments: Any,
) -> Any:
    """Execute one bounded campaign operation without scheduler ledger authority."""

    if operation not in {
        "snapshot",
        "complete_allocation",
        "assert_exact_accounting",
        "seal_for_reconciliation",
        "sealed_snapshot",
    }:
        raise WhoScoredProxyRuntimeError("unsupported paid campaign gateway operation")
    gateway_url = str(os.environ.get(PAID_GATEWAY_URL_ENV) or "").strip()
    gateway_token = str(os.environ.get(PAID_GATEWAY_TOKEN_ENV) or "").strip()
    if gateway_url != EXPECTED_PAID_GATEWAY_URL or len(gateway_token) < 32:
        raise WhoScoredProxyRuntimeError(
            "paid campaign operation requires the admitted application gateway"
        )
    from scrapers.whoscored.transport import (
        PaidCampaignContext,
        PaidGatewayClient,
        PaidGatewayError,
    )

    client = PaidGatewayClient(gateway_url, token=gateway_token)
    try:
        method = getattr(client, operation)
        return method(
            context=PaidCampaignContext.from_approval(approval),
            **arguments,
        )
    except (PaidGatewayError, ValueError) as exc:
        raise WhoScoredProxyRuntimeError(
            f"paid application-gateway campaign operation failed: {operation}"
        ) from exc
    finally:
        client.close()


def validate_transport_alert_delivery(**context: Any) -> dict[str, Any]:
    """Ask the isolated gateway to deliver its idempotent paid alert."""

    runtime = resolve_paid_runtime(context)
    if not runtime.is_paid:
        return {"status": "not_required", "transport_policy": runtime.policy}
    ti = context.get("ti")
    actual_task_id = str(getattr(ti, "task_id", "") or "")
    if actual_task_id != PAID_ALERT_PREFLIGHT_TASK_ID:
        raise WhoScoredProxyRuntimeError(
            "paid alert preflight task identity is not exact"
        )
    if runtime.approval is None:
        raise WhoScoredProxyRuntimeError("paid alert requires one signed approval")
    gateway_url = str(os.environ.get(PAID_GATEWAY_URL_ENV) or "").strip()
    gateway_token = str(os.environ.get(PAID_GATEWAY_TOKEN_ENV) or "").strip()
    if gateway_url != EXPECTED_PAID_GATEWAY_URL or len(gateway_token) < 32:
        raise WhoScoredProxyRuntimeError(
            "paid alert requires the admitted application gateway"
        )
    from scrapers.whoscored.transport import (
        PaidCampaignContext,
        PaidGatewayClient,
        PaidGatewayError,
    )

    client = PaidGatewayClient(gateway_url, token=gateway_token)
    try:
        result = client.preflight_alert(
            context=PaidCampaignContext.from_approval(runtime.approval)
        )
    except (PaidGatewayError, ValueError) as exc:
        raise WhoScoredProxyRuntimeError(
            "paid application-gateway alert preflight failed"
        ) from exc
    finally:
        client.close()
    return {**result, "transport_policy": runtime.policy}


def validate_paid_alert_for_source(
    runtime: PaidRuntime,
    alert_metadata: Mapping[str, Any] | None,
    context: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    """Recheck one paid receipt immediately before in-process source work."""

    if not runtime.is_paid:
        if alert_metadata is not None and alert_metadata.get("status") not in {
            None,
            "not_required",
        }:
            raise WhoScoredProxyRuntimeError(
                "direct-only source received paid alert metadata"
            )
        return None
    if runtime.approval is None or not isinstance(alert_metadata, Mapping):
        raise WhoScoredProxyRuntimeError(
            "paid source requires exact alert receipt metadata"
        )
    from dags.utils.alerts import PaidAlertError, validate_paid_alert_metadata

    try:
        return validate_paid_alert_metadata(
            alert_metadata,
            campaign_id=runtime.campaign_id,
            approval_id=runtime.approval_id,
            approval_sha256=runtime.approval_sha256,
        )
    except PaidAlertError as exc:
        raise WhoScoredProxyRuntimeError(
            "paid source alert receipt is invalid"
        ) from exc


def paid_alert_source_guard_command(
    runtime: PaidRuntime,
    alert_metadata: Mapping[str, Any] | None,
    context: Mapping[str, Any],
) -> str:
    """Recheck one paid receipt inside the source Bash task before its runner."""

    validated = validate_paid_alert_for_source(runtime, alert_metadata, context)
    if validated is None:
        return ""
    from dags.utils.alerts import paid_alert_receipt_environment

    projected = paid_alert_receipt_environment(validated)
    return (
        "export "
        + " ".join(
            f"{name}={shlex.quote(value)}" for name, value in sorted(projected.items())
        )
        + " && unset "
        + " ".join(_RUNNER_FORBIDDEN_AUTHORITY_ENV_NAMES)
        + " && "
    )


@contextmanager
def projected_paid_alert_environment(
    metadata: Mapping[str, Any] | None,
) -> Iterator[None]:
    """Project a verified receipt only for an in-process paid source scope."""

    from dags.utils.alerts import PAID_ALERT_RECEIPT_ENV, paid_alert_receipt_environment

    if metadata is None:
        projected: dict[str, str] = {}
    else:
        projected = paid_alert_receipt_environment(metadata)
    previous = {name: os.environ.get(name) for name in PAID_ALERT_RECEIPT_ENV.values()}
    for name in PAID_ALERT_RECEIPT_ENV.values():
        os.environ.pop(name, None)
    os.environ.update(projected)
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _airflow_attempt_id(context: Mapping[str, Any], *, task_id: str) -> str:
    dag_id = _dag_id(context)
    dag_run = context.get("dag_run")
    run_id = context.get("run_id") or getattr(dag_run, "run_id", None)
    ti = context.get("ti")
    actual_task = getattr(ti, "task_id", None) or getattr(
        context.get("task"), "task_id", None
    )
    if actual_task and str(actual_task) != task_id:
        raise WhoScoredProxyRuntimeError(
            "paid environment task differs from the signed allocation task"
        )
    try:
        map_index = int(getattr(ti, "map_index", -1))
        try_number = int(getattr(ti, "try_number", 0))
    except (TypeError, ValueError) as exc:
        raise WhoScoredProxyRuntimeError(
            "invalid Airflow paid attempt identity"
        ) from exc
    if not dag_id or not run_id:
        raise WhoScoredProxyRuntimeError(
            "paid transport requires a complete Airflow DagRun identity"
        )
    try:
        return deterministic_proxy_attempt_id(
            dag_id=dag_id,
            run_id=str(run_id),
            task_id=task_id,
            map_index=map_index,
            try_number=try_number,
        )
    except ProxyCampaignError as exc:
        raise WhoScoredProxyRuntimeError(str(exc)) from exc


@contextmanager
def projected_transport_environment(
    runtime: PaidRuntime,
    context: Mapping[str, Any],
) -> Iterator[None]:
    """Temporarily project validated authority for in-process source work."""

    names = (
        *_CAMPAIGN_ENV_NAMES,
        "WHOSCORED_TRANSPORT_POLICY",
        PAID_GATEWAY_URL_ENV,
        PAID_GATEWAY_TOKEN_ENV,
        *_RUNNER_FORBIDDEN_AUTHORITY_ENV_NAMES,
    )
    previous = {name: os.environ.get(name) for name in names}
    try:
        os.environ["WHOSCORED_TRANSPORT_POLICY"] = runtime.policy
        for name in _CAMPAIGN_ENV_NAMES:
            os.environ.pop(name, None)
        for name in _RUNNER_FORBIDDEN_AUTHORITY_ENV_NAMES:
            os.environ.pop(name, None)
        if runtime.is_paid:
            if runtime.approval is None or runtime.allocation is None:
                raise WhoScoredProxyRuntimeError(
                    "paid environment requires one validated allocation"
                )
            if not os.environ.get(PAID_GATEWAY_URL_ENV, "").strip():
                raise WhoScoredProxyRuntimeError(
                    "paid environment requires the isolated application gateway URL"
                )
            if len(os.environ.get(PAID_GATEWAY_TOKEN_ENV, "").strip()) < 32:
                raise WhoScoredProxyRuntimeError(
                    f"{PAID_GATEWAY_TOKEN_ENV} must contain at least 32 characters"
                )
            attempt_id = _airflow_attempt_id(
                context, task_id=runtime.allocation.task_id
            )
            os.environ.update(
                {
                    PROXY_APPROVAL_PATH_ENV: runtime.approval_path,
                    PROXY_APPROVAL_ID_ENV: runtime.approval.approval_id,
                    PROXY_APPROVAL_SHA256_ENV: runtime.approval.approval_sha256,
                    PROXY_ALLOCATION_ID_ENV: runtime.allocation.allocation_id,
                    PROXY_ATTEMPT_ID_ENV: attempt_id,
                }
            )
        else:
            os.environ.pop(PAID_GATEWAY_URL_ENV, None)
            os.environ.pop(PAID_GATEWAY_TOKEN_ENV, None)
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
