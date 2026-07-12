"""One-lease, one-browser SofaScore production JSON transport.

The capture engine owns endpoint resume, rate limiting, retries, raw persistence
and the local verified-canary ledger.  This adapter owns only the paid data
plane: one authenticated proxy lease and one warmed Camoufox page for the whole
logical task.  Provider bytes are read from the proxy control plane immediately
before and after every exact JSON request and returned to the engine as the
authoritative ``HttpPayload.provider_bytes`` value.
"""

from __future__ import annotations

import hashlib
import ipaddress
import os
from contextlib import AbstractContextManager
from types import SimpleNamespace
from typing import Any, Callable, Mapping, Optional, Sequence
from urllib.parse import urlsplit

from scrapers.sofascore.capture_engine import (
    HttpPayload,
    ProviderBudgetToken,
    SofaScoreCaptureEngine,
    TransportError,
)
from scripts.proxy_filter.budget import (
    BudgetAccountingError,
    ProxyBudgetExceeded,
)
from scrapers.sofascore.manifest import ManifestStatus
from scrapers.sofascore.workload_plan import SignedDagRunPlan, WorkloadAllocation


_ALLOWED_SOURCE_HOSTS = frozenset({"api.sofascore.com", "www.sofascore.com"})


def _zero_traffic() -> dict[str, Any]:
    return {
        "paid_proxy_bytes": 0,
        "paid_proxy_mb": 0.0,
        "browser_sessions": 0,
        "browser_navigations": 0,
        "navigations": 0,
        "request_count": 0,
        "endpoint_request_count": 0,
        "source_request_count": 0,
        "cache_hit_rate": 1.0,
        "replay_hit_rate": 1.0,
        "endpoint_completeness": 1.0,
        "provider_total_bytes": 0,
        "endpoint_provider_bytes": {},
        "endpoint_request_provider_bytes": {},
        "proxy_exit_hash": None,
    }


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return parsed


def _boolean(value: object, label: str) -> bool:
    token = str(value).strip().lower()
    if token in {"1", "true", "yes"}:
        return True
    if token in {"0", "false", "no", ""}:
        return False
    raise ValueError(f"{label} must be a boolean")


def hash_proxy_exit(value: object) -> str:
    """Validate and irreversibly anonymize a canary's residential exit."""

    try:
        normalized = str(ipaddress.ip_address(value))
    except (TypeError, ValueError):
        raise ValueError("SofaScore canary exit probe returned no valid IP") from None
    return hashlib.sha256(normalized.encode("ascii")).hexdigest()


class LeaseBackedCamoufoxTransport(AbstractContextManager):
    """A synchronous single-page transport for sequential EndpointSpecs.

    Construction and ``__enter__`` do not start a browser.  The first engine-
    authorised ``request`` starts and warms Camoufox, so every provider byte is
    inside an endpoint reservation.  Subsequent requests reuse that page.  A
    challenge may re-navigate the same browser, but the adapter never opens a
    second cold session for the logical task.
    """

    # The page-level route observes navigation and every SofaScore data XHR.
    # The engine still reserves/finishes provider bytes per EndpointSpec, but
    # deliberately skips its logical limiter for this transport.
    paces_requests = True

    def __init__(
        self,
        engine: SofaScoreCaptureEngine,
        *,
        canonical_url: str,
        scope: str,
        entity: str,
        control_url: Optional[str] = None,
        lease_ttl_seconds: Optional[int] = None,
        mode: Optional[str] = None,
        exit_probe_enabled: Optional[bool] = None,
        workload_plan: Optional[SignedDagRunPlan | Mapping[str, Any]] = None,
        allocation_id: str = "",
        attempt_id: Optional[str] = None,
        lease_client_factory: Optional[Callable[..., Any]] = None,
        capture_factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        if engine.budget is None:
            raise ValueError("live SofaScore transport requires a verified budget")
        parsed = urlsplit(str(canonical_url))
        if parsed.scheme != "https" or parsed.hostname not in _ALLOWED_SOURCE_HOSTS:
            raise ValueError("canonical_url must be an official HTTPS SofaScore URL")
        self.engine = engine
        self.canonical_url = str(canonical_url)
        self.scope = str(scope)
        self.entity = str(entity)
        self.mode = str(
            mode or os.environ.get("SOFASCORE_PROXY_LEASE_MODE", "production")
        ).strip().lower()
        if self.mode not in {"production", "canary"}:
            raise ValueError("SofaScore proxy lease mode must be production or canary")
        if not _boolean(
            os.environ.get("SOFASCORE_BLOCK_RESOURCES", "1"),
            "SOFASCORE_BLOCK_RESOURCES",
        ):
            raise ValueError(
                "paid SofaScore transport requires resource blocking enabled"
            )
        self.exit_probe_enabled = (
            _boolean(
                os.environ.get("SOFASCORE_CANARY_EXIT_PROBE", "0"),
                "SOFASCORE_CANARY_EXIT_PROBE",
            )
            if exit_probe_enabled is None
            else bool(exit_probe_enabled)
        )
        if self.exit_probe_enabled and self.mode != "canary":
            raise ValueError("proxy exit probe is restricted to SofaScore canary leases")
        self.workload_plan: Optional[SignedDagRunPlan]
        self.allocation: Optional[WorkloadAllocation]
        if self.mode == "production":
            if isinstance(workload_plan, SignedDagRunPlan):
                self.workload_plan = workload_plan
            elif isinstance(workload_plan, Mapping):
                self.workload_plan = SignedDagRunPlan.from_dict(workload_plan)
            else:
                raise ValueError(
                    "production SofaScore transport requires a signed workload plan"
                )
            if self.workload_plan.run_id.count("::") != 1:
                raise ValueError(
                    "production workload run_id must end in "
                    "::season, ::targets or ::players"
                )
            self.base_run_id, self.workload_phase = (
                self.workload_plan.run_id.rsplit("::", 1)
            )
            if (
                not self.base_run_id
                or self.workload_phase not in {"season", "targets", "players"}
            ):
                raise ValueError(
                    "production workload run_id must end in "
                    "::season, ::targets or ::players"
                )
            try:
                self.allocation = next(
                    item
                    for item in self.workload_plan.allocations
                    if item.allocation_id == str(allocation_id).strip()
                )
            except StopIteration as exc:
                raise ValueError(
                    "allocation_id is absent from the signed workload plan"
                ) from exc
            self.attempt_id = str(
                attempt_id
                if attempt_id is not None
                else os.environ.get("AIRFLOW_CTX_TRY_NUMBER", "")
            ).strip()
            if not self.attempt_id:
                raise ValueError("production SofaScore transport requires attempt_id")
            if (
                self.engine.run_id != self.workload_plan.run_id
                or self.engine.task_id != self.allocation.task_id
            ):
                raise ValueError(
                    "capture engine run/task do not match signed workload allocation"
                )
        else:
            if workload_plan is not None or allocation_id or attempt_id is not None:
                raise ValueError("canary transport cannot carry production allocations")
            self.workload_plan = None
            self.allocation = None
            self.attempt_id = ""
            self.base_run_id = ""
            self.workload_phase = ""
        self.control_url = (
            control_url
            or os.environ.get("SOFASCORE_PROXY_CONTROL_URL", "")
        ).strip()
        if not self.control_url:
            raise ValueError("SOFASCORE_PROXY_CONTROL_URL is required for live capture")
        self.lease_ttl_seconds = _positive_int(
            lease_ttl_seconds
            if lease_ttl_seconds is not None
            else os.environ.get("SOFASCORE_PROXY_LEASE_TTL_SECONDS", "3600"),
            "SofaScore proxy lease TTL",
        )
        self._lease_client_factory = lease_client_factory
        self._capture_factory = capture_factory
        self._client = None
        self._lease = None
        self._capture_cm = None
        self._capture = None
        self._entered = False
        self._closed = False
        self._broken = False
        self._needs_rewarm = False
        self._accounted_provider_bytes = 0
        self._last_stats = None
        self._proxy_exit_hash: Optional[str] = None
        self._browser_started = False
        self._browser_reported = False
        self._upstream_fingerprint: Optional[str] = None
        self._endpoint_request_provider_bytes: dict[str, list[int]] = {}
        self._completed = False

    @property
    def hard_run_bytes(self) -> int:
        local_budget = _positive_int(
            self.engine.budget.policy.hard_run_bytes,
            "local SofaScore hard run budget",
        )
        if self.mode == "canary":
            experimental_cap = _positive_int(
                os.environ.get(
                    "PROXY_FILTER_SOFASCORE_CANARY_HARD_CAP_BYTES", "0"
                ),
                "SofaScore canary hard cap",
            )
            if experimental_cap != local_budget:
                raise BudgetAccountingError(
                    "local canary budget differs from proxy-filter hard cap"
                )
        elif self.workload_plan is not None and self.allocation is not None:
            local_artifact = str(
                getattr(
                    self.engine.budget.policy,
                    "parent_artifact_id",
                    getattr(self.engine.budget.policy, "artifact_id", ""),
                )
            )
            local_class = getattr(
                self.engine.budget.policy, "workload_class", None
            )
            if (
                local_artifact != self.workload_plan.artifact_id
                or local_budget != self.allocation.budget_bytes
                or (
                    local_class is not None
                    and str(local_class) != self.allocation.workload_class
                )
            ):
                raise BudgetAccountingError(
                    "local allocation budget differs from the signed workload plan"
                )
        return local_budget

    @property
    def signed_run_cap_bytes(self) -> int:
        if self.workload_plan is not None:
            return self.workload_plan.run_cap_bytes
        return self.hard_run_bytes

    def __enter__(self) -> "LeaseBackedCamoufoxTransport":
        if self._entered:
            raise RuntimeError("SofaScore live transport cannot be entered twice")
        if self._lease_client_factory is None:
            from scrapers.sofascore.lease_client import SofascoreLeaseClient

            client_factory = SofascoreLeaseClient
        else:
            client_factory = self._lease_client_factory
        self._client = client_factory(self.control_url)
        expected_dag_id = (
            "dag_canary_sofascore_proxy"
            if self.mode == "canary"
            else "dag_ingest_sofascore"
        )
        dag_id = os.environ.get("AIRFLOW_CTX_DAG_ID", expected_dag_id)
        if dag_id != expected_dag_id:
            raise ValueError(
                f"SofaScore {self.mode} transport requires dag_id={expected_dag_id}"
            )
        try:
            self._lease = self._client.acquire(
                max_bytes=self.hard_run_bytes,
                ttl_seconds=self.lease_ttl_seconds,
                dag_id=dag_id,
                run_id=(
                    self.workload_plan.run_id
                    if self.workload_plan is not None
                    else self.engine.run_id
                ),
                task_id=(
                    self.allocation.task_id
                    if self.allocation is not None
                    else self.engine.task_id
                ),
                scope=self.scope,
                entity=self.entity,
                canonical_url=self.canonical_url,
                source=(
                    "sofascore_canary" if self.mode == "canary" else "sofascore"
                ),
                workload_plan=self.workload_plan,
                allocation_id=(self.allocation.allocation_id if self.allocation else ""),
                attempt_id=self.attempt_id,
            )
            self._last_stats = self._validate_stats(
                self._client.stats(self._lease)
            )
            if int(self._last_stats.total_bytes) != 0:
                raise BudgetAccountingError(
                    "new SofaScore lease already has provider traffic"
                )
        except BaseException:
            if self._lease is not None:
                try:
                    self._client.close(self._lease)
                except Exception:
                    pass
            self._closed = True
            raise
        self._entered = True
        return self

    def _new_capture(self):
        if self._capture_factory is None:
            from scrapers.sofascore.camoufox_capture import SofascoreCamoufoxCapture

            capture_factory = SofascoreCamoufoxCapture
        else:
            capture_factory = self._capture_factory
        # Keep the bearer token out of URLs, exception text and browser logs.
        # The control client validates the unauthenticated proxy URL returned by
        # the filter and exposes Playwright's split credential representation.
        proxy = self._client.playwright_proxy(self._lease)
        return capture_factory(
            proxy=proxy,
            request_limiter=self.engine.rate_limiter.acquire,
        )

    def _safe_error(self, exc: BaseException) -> str:
        from scrapers.sofascore.lease_client import redact_sensitive

        token = str(getattr(self._lease, "token", "") or "")
        return redact_sensitive(exc, secrets=(token,))

    def _validate_stats(self, stats: Any) -> Any:
        """Bind every meter observation to this exact lease and policy."""

        if self._lease is None:
            raise BudgetAccountingError("SofaScore stats arrived without a lease")
        expected_source = (
            "sofascore_canary" if self.mode == "canary" else "sofascore"
        )
        expected_artifact = str(
            (
                self.workload_plan.artifact_id
                if self.workload_plan is not None
                else getattr(self.engine.budget.policy, "artifact_id", "")
            )
            or ""
        )
        expected_budget = self.signed_run_cap_bytes
        fingerprint = str(getattr(stats, "upstream_fingerprint", "") or "")
        if (
            getattr(self._lease, "source", None) != expected_source
            or getattr(stats, "source", None) != expected_source
            or str(getattr(stats, "budget_artifact_id", "") or "")
            != expected_artifact
            or int(getattr(stats, "dagrun_budget_bytes", -1)) != expected_budget
            or int(getattr(stats, "max_bytes", -1))
            != int(getattr(self._lease, "max_bytes", -2))
            or int(getattr(self._lease, "max_bytes", 0)) > expected_budget
            or not fingerprint
        ):
            raise BudgetAccountingError(
                "SofaScore lease stats do not match the local policy provenance"
            )
        if self.allocation is not None and (
            str(getattr(stats, "plan_digest", ""))
            != self.workload_plan.plan_digest
            or str(getattr(stats, "allocation_id", ""))
            != self.allocation.allocation_id
            or str(getattr(stats, "allocation_task_id", ""))
            != self.allocation.task_id
            or str(getattr(stats, "allocation_scope", ""))
            != self.allocation.scope
            or str(getattr(stats, "allocation_class", ""))
            != self.allocation.workload_class
            or int(getattr(stats, "allocation_batch_index", -1))
            != self.allocation.batch_index
            or tuple(getattr(stats, "allocation_units", ()))
            != self.allocation.units
            or int(getattr(stats, "allocation_budget_bytes", -1))
            != self.allocation.budget_bytes
            or str(getattr(stats, "base_run_id", "")) != self.base_run_id
            or str(getattr(stats, "workload_phase", ""))
            != self.workload_phase
            or str(getattr(stats, "phase_plan_digest", ""))
            != self.workload_plan.plan_digest
        ):
            raise BudgetAccountingError(
                "SofaScore lease stats do not match the signed allocation"
            )
        if (
            self._upstream_fingerprint is not None
            and fingerprint != self._upstream_fingerprint
        ):
            raise BudgetAccountingError(
                "SofaScore sticky lease changed residential upstream"
            )
        self._upstream_fingerprint = fingerprint
        return stats

    @staticmethod
    def _source_path(url: str) -> str:
        parsed = urlsplit(str(url))
        if (
            parsed.scheme != "https"
            or parsed.hostname not in _ALLOWED_SOURCE_HOSTS
            or not parsed.path.startswith("/api/v1/")
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("live transport accepts exact official SofaScore API URLs")
        return parsed.path

    def _ensure_capture(self) -> tuple[int, int]:
        """Return ``(new_sessions, new_navigations)`` after warm/re-warm."""

        if self._capture is None:
            if self._broken:
                raise TransportError(
                    "the logical task's only Camoufox session is unavailable",
                    provider_bytes=0,
                    retryable=False,
                )
            self._capture_cm = self._new_capture()
            try:
                self._capture = self._capture_cm.__enter__()
                self._browser_started = True
                self._capture.warm_exact_json(self.canonical_url)
                if self.exit_probe_enabled:
                    self._proxy_exit_hash = hash_proxy_exit(
                        self._capture.probe_proxy_exit()
                    )
            except BaseException:
                self._broken = True
                raise
            self._needs_rewarm = False
            sessions = int(self._browser_started and not self._browser_reported)
            self._browser_reported = self._browser_reported or bool(sessions)
            return sessions, 1
        if self._needs_rewarm:
            before = int(getattr(self._capture, "_navigation_count", 0) or 0)
            self._capture.warm_exact_json(self.canonical_url)
            self._needs_rewarm = False
            after = int(getattr(self._capture, "_navigation_count", 0) or 0)
            return 0, max(0, after - before)
        return 0, 0

    def request(
        self,
        url: str,
        *,
        provider_budget: Optional[ProviderBudgetToken],
    ) -> HttpPayload:
        if not self._entered or self._closed or self._lease is None:
            raise RuntimeError("SofaScore live transport is not open")
        if provider_budget is None:
            raise BudgetAccountingError(
                "paid SofaScore request reached transport without authorization"
            )
        if provider_budget.run_id != self.engine.run_id:
            raise BudgetAccountingError("SofaScore provider token run mismatch")
        path = self._source_path(url)
        before = self._validate_stats(self._client.stats(self._lease))
        before_total = int(before.total_bytes)
        if before_total < self._accounted_provider_bytes:
            raise BudgetAccountingError(
                "SofaScore lease provider counter moved backwards"
            )
        request_boundary: Optional[str] = None
        sessions = 0
        navigations = 0
        navigation_before = int(
            getattr(self._capture, "_navigation_count", 0) or 0
        )
        source_before = int(
            getattr(self._capture, "_source_request_count", 0) or 0
        )
        try:
            request_boundary = self._client.begin_endpoint(
                self._lease,
                provider_budget.endpoint,
            )
            sessions, navigations = self._ensure_capture()
            nav_before = int(getattr(self._capture, "_navigation_count", 0) or 0)
            record = self._capture.fetch_api_json(path)
            nav_after = int(getattr(self._capture, "_navigation_count", 0) or 0)
            navigations += max(0, nav_after - nav_before)
        except BaseException as exc:
            if self._browser_started and not self._browser_reported:
                sessions = 1
                self._browser_reported = True
            navigation_after = int(
                getattr(self._capture, "_navigation_count", navigation_before) or 0
            )
            # ``_ensure_capture`` normally returns the warm-up delta.  When the
            # warm itself fails, it cannot return, but the attempted navigation
            # and started browser still consumed provider traffic and must not
            # disappear from failure metrics.
            navigations = max(
                navigations,
                max(0, navigation_after - navigation_before),
            )
            source_after = int(
                getattr(self._capture, "_source_request_count", source_before) or 0
            )
            try:
                if request_boundary is not None:
                    after = self._validate_stats(
                        self._client.finish_endpoint(
                            self._lease,
                            request_boundary,
                        )
                    )
                    request_boundary = None
                else:
                    after = self._validate_stats(self._client.stats(self._lease))
                # Include bytes that arrived after the previous endpoint's
                # meter read.  They travelled through this logical run's lease
                # and therefore must be charged to an authorised endpoint.
                after_total = int(after.total_bytes)
                if (
                    after_total < before_total
                    or after_total < self._accounted_provider_bytes
                ):
                    raise BudgetAccountingError(
                        "SofaScore lease provider counter moved backwards"
                    )
                provider_bytes = after_total - self._accounted_provider_bytes
                self._last_stats = after
                self._accounted_provider_bytes += provider_bytes
                self._endpoint_request_provider_bytes.setdefault(
                    provider_budget.endpoint, []
                ).append(provider_bytes)
            except Exception:
                provider_bytes = None
            raise TransportError(
                f"warmed SofaScore request failed for {path}: {self._safe_error(exc)}",
                provider_bytes=provider_bytes,
                retryable=not self._broken,
                browser_sessions=sessions,
                navigations=navigations,
                source_requests=max(0, source_after - source_before),
            ) from None

        after = self._validate_stats(
            self._client.finish_endpoint(self._lease, request_boundary)
        )
        request_boundary = None
        after_total = int(after.total_bytes)
        if after_total < before_total or after_total < self._accounted_provider_bytes:
            raise BudgetAccountingError(
                "SofaScore lease provider counter moved backwards"
            )
        provider_bytes = after_total - self._accounted_provider_bytes
        self._last_stats = after
        self._accounted_provider_bytes += provider_bytes
        self._endpoint_request_provider_bytes.setdefault(
            provider_budget.endpoint, []
        ).append(provider_bytes)
        source_requests = max(
            0,
            int(getattr(self._capture, "_source_request_count", source_before) or 0)
            - source_before,
        )
        if bool(getattr(after, "budget_exceeded", False)):
            raise TransportError(
                "SofaScore proxy lease exhausted the logical DAG-run budget",
                provider_bytes=provider_bytes,
                retryable=False,
                browser_sessions=sessions,
                navigations=navigations,
                source_requests=source_requests,
            )
        if not isinstance(record, dict) or not isinstance(record.get("status"), int):
            raise TransportError(
                f"warmed SofaScore request returned no HTTP response for {path}",
                provider_bytes=provider_bytes,
                retryable=True,
                browser_sessions=sessions,
                navigations=navigations,
                source_requests=source_requests,
            )
        status = int(record["status"])
        self._needs_rewarm = status == 403 or bool(record.get("challenge"))
        body = record.get("body")
        if isinstance(body, str):
            body = body.encode("utf-8")
        if body is None:
            body = b""
        if not isinstance(body, bytes):
            raise TransportError(
                f"warmed SofaScore response body is not exact bytes for {path}",
                provider_bytes=provider_bytes,
                retryable=False,
                browser_sessions=sessions,
                navigations=navigations,
                source_requests=source_requests,
            )
        return HttpPayload(
            status_code=status,
            body=body,
            headers=dict(record.get("headers") or {}),
            provider_bytes=provider_bytes,
            browser_sessions=sessions,
            navigations=navigations,
            source_requests=source_requests,
        )

    def close(self, *, completed: bool = False) -> Any:
        if self._closed:
            return self._last_stats
        # Revoke the paid data plane first. The filter drains/counts and closes
        # every tunnel before returning final stats, so subsequent local browser
        # teardown cannot emit unowned TLS close traffic through the provider.
        final = None
        lease_error: Optional[BaseException] = None
        if self._client is not None and self._lease is not None:
            try:
                final = self._validate_stats(
                    self._client.close(
                        self._lease,
                        endpoint_request_provider_bytes=(
                            self._endpoint_request_provider_bytes
                        ),
                        completed=completed,
                        proxy_exit_hash=self._proxy_exit_hash,
                    )
                )
                self._last_stats = final
            except BaseException as exc:
                lease_error = exc
        capture_error = None
        if self._capture_cm is not None:
            try:
                self._capture_cm.__exit__(None, None, None)
            except Exception as exc:  # browser teardown must not hide accounting
                capture_error = exc
        self._capture = None
        self._capture_cm = None
        self._closed = True
        if lease_error is not None:
            raise lease_error
        if final is not None:
            total = int(final.total_bytes)
            if total != self._accounted_provider_bytes:
                raise BudgetAccountingError(
                    "unattributed SofaScore provider traffic after browser close: "
                    f"meter={total}, endpoints={self._accounted_provider_bytes}"
                )
            if bool(getattr(final, "budget_exceeded", False)):
                raise ProxyBudgetExceeded(
                    "SofaScore proxy lease exceeded the logical DAG-run budget"
                )
        if capture_error is not None:
            raise RuntimeError(
                "SofaScore browser teardown failed: "
                + self._safe_error(capture_error)
            ) from None
        return final

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        try:
            self.close(completed=exc_type is None)
        except Exception:
            if exc_type is None:
                raise
        return False

    def provider_snapshot(self) -> dict[str, int | bool | str | None]:
        stats = self._last_stats
        if stats is None:
            return {
                "provider_up_bytes": 0,
                "provider_down_bytes": 0,
                "provider_total_bytes": 0,
                "provider_budget_bytes": self.hard_run_bytes,
                "provider_run_cap_bytes": self.signed_run_cap_bytes,
                "parent_run_cap_bytes": self.signed_run_cap_bytes,
                "parent_run_spent_provider_bytes": 0,
                "provider_budget_exceeded": False,
                "proxy_exit_hash": self._proxy_exit_hash,
            }
        return {
            "provider_up_bytes": int(stats.up_bytes),
            "provider_down_bytes": int(stats.down_bytes),
            "provider_total_bytes": int(stats.total_bytes),
            "provider_budget_bytes": (
                self.allocation.budget_bytes
                if self.allocation is not None
                else int(stats.dagrun_budget_bytes)
            ),
            "provider_run_cap_bytes": int(stats.dagrun_budget_bytes),
            "parent_run_cap_bytes": int(
                getattr(stats, "parent_run_cap_bytes", stats.dagrun_budget_bytes)
            ),
            "parent_run_spent_provider_bytes": int(
                getattr(stats, "parent_run_spent_provider_bytes", stats.total_bytes)
            ),
            "provider_budget_exceeded": bool(stats.budget_exceeded),
            "proxy_exit_hash": self._proxy_exit_hash,
        }


class _AllocationBudgetView:
    """Give each signed allocation its own local SharedBudgetLedger run key."""

    def __init__(
        self,
        wrapped: Any,
        *,
        plan: SignedDagRunPlan,
        allocation: WorkloadAllocation,
    ) -> None:
        if wrapped is None:
            raise BudgetAccountingError("signed paid allocation has no local budget")
        parent_artifact_id = str(
            getattr(
                wrapped.policy,
                "parent_artifact_id",
                getattr(wrapped.policy, "artifact_id", ""),
            )
        )
        workload_class = getattr(wrapped.policy, "workload_class", None)
        if (
            parent_artifact_id != plan.artifact_id
            or int(getattr(wrapped.policy, "hard_run_bytes", 0))
            != allocation.budget_bytes
            or (
                workload_class is not None
                and str(workload_class) != allocation.workload_class
            )
        ):
            raise BudgetAccountingError(
                "local budget policy differs from signed allocation provenance"
            )
        self._wrapped = wrapped
        self._plan_run_id = plan.run_id
        self._allocation_run_id = f"{plan.run_id}::{allocation.allocation_id}"
        self.policy = SimpleNamespace(
            artifact_id=plan.artifact_id,
            hard_run_bytes=allocation.budget_bytes,
        )

    def _run(self, run_id: str) -> str:
        if str(run_id) != self._plan_run_id:
            raise BudgetAccountingError("capture engine run differs from signed plan")
        return self._allocation_run_id

    def reserve(self, run_id: str, endpoint: str):
        return self._wrapped.reserve(self._run(run_id), endpoint)

    def finish(self, run_id: str, token: str, **kwargs):
        return self._wrapped.finish(self._run(run_id), token, **kwargs)

    def cancel(self, run_id: str, token: str):
        return self._wrapped.cancel(self._run(run_id), token)


def _allocation_budget_view(
    engine: SofaScoreCaptureEngine,
    workload_plan: Optional[SignedDagRunPlan | Mapping[str, Any]],
    allocation_id: str,
) -> tuple[Any, Optional[SignedDagRunPlan], Optional[WorkloadAllocation]]:
    if workload_plan is None:
        return engine.budget, None, None
    if isinstance(workload_plan, SignedDagRunPlan):
        plan = workload_plan
    else:
        plan = SignedDagRunPlan.from_dict(workload_plan)
    try:
        allocation = next(
            item for item in plan.allocations if item.allocation_id == allocation_id
        )
    except StopIteration as exc:
        raise ValueError("allocation_id is absent from signed workload plan") from exc
    return (
        _AllocationBudgetView(engine.budget, plan=plan, allocation=allocation),
        plan,
        allocation,
    )


def capture_live_specs(
    runtime: Any,
    specs: Sequence[Any],
    *,
    canonical_url: str,
    scope: str,
    entity: str,
    workload_plan: Optional[SignedDagRunPlan | Mapping[str, Any]] = None,
    allocation_id: str = "",
    attempt_id: Optional[str] = None,
    transport_factory: Callable[..., LeaseBackedCamoufoxTransport] = (
        LeaseBackedCamoufoxTransport
    ),
) -> tuple[list[Any], dict[str, Any]]:
    """Capture a deterministic spec sequence through one lease/browser.

    An empty plan is an exact no-op and returns before the transport factory is
    touched.  A successful normalized payload is intentionally nonterminal until
    its Bronze MERGE (``DeferredMaterialization``); every other nonterminal result
    aborts the task before another paid endpoint is attempted.
    """

    if not specs:
        return [], _zero_traffic()
    engine = runtime.engine
    ordered_specs = list(specs)
    keys = [spec.key for spec in ordered_specs]
    if len(keys) != len(set(keys)):
        raise ValueError("SofaScore live capture plan has duplicate manifest keys")
    captured_by_key: dict[Any, Any] = {}
    network_specs: list[Any] = []
    for spec in ordered_specs:
        existing = engine.manifest_store.get(spec.key)
        replayable = bool(existing and existing.is_terminal) or not spec.supported
        if not replayable:
            try:
                _, raw = engine.raw_store.load_bytes(spec.raw_target)
            except Exception as exc:
                # Only the store's explicit miss is a network requirement;
                # corrupt/unreadable retained raw must fail rather than be paid
                # for again and silently overwrite evidence.
                from scrapers.sofascore.raw_store import RawPayloadNotFound

                if isinstance(exc, RawPayloadNotFound):
                    raw = None
                else:
                    raise
            replayable = bool(raw and 200 <= raw.http_status < 300 and raw.http_status != 204)
        if replayable:
            result = engine.capture(spec)
            _require_publishable(result)
            captured_by_key[spec.key] = result
        else:
            network_specs.append(spec)
    if not network_specs:
        return [captured_by_key[spec.key] for spec in ordered_specs], _zero_traffic()
    metrics_before = engine.metrics.snapshot()
    previous_budget = engine.budget
    previous_run_id = engine.run_id
    previous_task_id = engine.task_id
    allocation_budget, normalized_plan, normalized_allocation = _allocation_budget_view(
        engine,
        workload_plan,
        allocation_id,
    )
    previous_transport = engine.transport
    engine.budget = allocation_budget
    if normalized_plan is not None and normalized_allocation is not None:
        engine.run_id = normalized_plan.run_id
        engine.task_id = normalized_allocation.task_id
    try:
        transport = transport_factory(
            engine,
            canonical_url=canonical_url,
            scope=scope,
            entity=entity,
            workload_plan=normalized_plan,
            allocation_id=allocation_id,
            attempt_id=attempt_id,
        )
        with transport:
            engine.transport = transport
            for spec in network_specs:
                result = engine.capture(spec)
                _require_publishable(result)
                captured_by_key[spec.key] = result
    finally:
        engine.transport = previous_transport
        engine.budget = previous_budget
        engine.run_id = previous_run_id
        engine.task_id = previous_task_id
    traffic = _live_traffic(engine, metrics_before, transport)
    return [captured_by_key[spec.key] for spec in ordered_specs], traffic


def _require_publishable(result: Any) -> None:
    manifest = result.manifest
    deferred = (
        manifest.status.value == "retryable_failure"
        and manifest.error_type == "DeferredMaterialization"
    )
    if not manifest.is_terminal and not deferred:
        raise RuntimeError(
            "SofaScore endpoint did not reach a publishable state: "
            f"{manifest.key.stable_id()} status={manifest.status.value} "
            f"error={manifest.error_type}: {manifest.error_message}"
        )


def _live_traffic(
    engine: SofaScoreCaptureEngine,
    before: dict[str, Any],
    transport: LeaseBackedCamoufoxTransport,
) -> dict[str, Any]:
    """Return the exact provider breakdown for this one lease only."""

    current = engine.metrics.snapshot()
    traffic = dict(current)
    # One capture engine is intentionally reused by every signed allocation in
    # a logical run.  Its snapshot is cumulative, while this function's return
    # value is a single lease/batch report later summed by the runner.  Return
    # deltas for every additive counter or batches would be counted as
    # 1 + 2 + ... + N sessions/requests instead of exactly N.
    additive_fields = (
        "endpoints",
        "request_count",
        "endpoint_request_count",
        "source_request_count",
        "browser_sessions",
        "navigations",
        "cache_hits",
        "replay_hits",
        "row_count",
        "completed_matches",
        "completed_players",
    )
    for field in additive_fields:
        traffic[field] = max(
            0,
            int(current.get(field, 0) or 0) - int(before.get(field, 0) or 0),
        )
    traffic["elapsed_seconds"] = max(
        0.0,
        float(current.get("elapsed_seconds", 0.0) or 0.0)
        - float(before.get("elapsed_seconds", 0.0) or 0.0),
    )
    current_status = current.get("status_counts") or {}
    before_status = before.get("status_counts") or {}
    traffic["status_counts"] = {
        status: max(
            0,
            int(current_status.get(status, 0) or 0)
            - int(before_status.get(status, 0) or 0),
        )
        for status in current_status
    }
    completed = traffic["endpoints"] or 1
    terminal = sum(
        traffic["status_counts"].get(status.value, 0)
        for status in (
            ManifestStatus.SUCCESS,
            ManifestStatus.LEGITIMATE_EMPTY,
            ManifestStatus.NOT_SUPPORTED,
        )
    )
    traffic["cache_hit_rate"] = (
        traffic["cache_hits"] + traffic["replay_hits"]
    ) / completed
    traffic["replay_hit_rate"] = traffic["replay_hits"] / completed
    traffic["endpoint_completeness"] = terminal / completed
    elapsed = traffic["elapsed_seconds"]
    traffic["matches_per_second"] = (
        traffic["completed_matches"] / elapsed if elapsed else 0.0
    )
    traffic["players_per_second"] = (
        traffic["completed_players"] / elapsed if elapsed else 0.0
    )
    traffic.update(transport.provider_snapshot())
    provider_total = int(traffic["provider_total_bytes"])
    paid_delta = int(current["paid_proxy_bytes"]) - int(
        before["paid_proxy_bytes"]
    )
    if paid_delta != provider_total:
        raise BudgetAccountingError(
            "capture-engine and proxy-lease provider totals disagree: "
            f"engine_delta={paid_delta}, lease={provider_total}"
        )

    previous = before.get("endpoint_request_provider_bytes") or {}
    current: dict[str, list[int]] = {}
    for endpoint, observations in (
        traffic.get("endpoint_request_provider_bytes") or {}
    ).items():
        old = list(previous.get(endpoint) or [])
        values = list(observations)
        if values[: len(old)] != old:
            raise BudgetAccountingError(
                "SofaScore endpoint provider observations changed retroactively"
            )
        delta = values[len(old) :]
        if delta:
            current[endpoint] = delta
    endpoint_totals = {
        endpoint: sum(values) for endpoint, values in current.items()
    }
    if sum(endpoint_totals.values()) != provider_total:
        raise BudgetAccountingError(
            "SofaScore endpoint request bytes do not equal the lease meter"
        )
    traffic["paid_proxy_bytes"] = provider_total
    traffic["paid_proxy_mb"] = provider_total / 1_048_576
    traffic["endpoint_provider_bytes"] = endpoint_totals
    traffic["endpoint_request_provider_bytes"] = current
    traffic["browser_navigations"] = int(traffic.get("navigations", 0))
    return traffic


def capture_live_dynamic_specs(
    runtime: Any,
    planner: Callable[[], Any],
    *,
    canonical_url: str,
    scope: str,
    entity: str,
    workload_plan: Optional[SignedDagRunPlan | Mapping[str, Any]] = None,
    allocation_id: str = "",
    attempt_id: Optional[str] = None,
    transport_factory: Callable[..., LeaseBackedCamoufoxTransport] = (
        LeaseBackedCamoufoxTransport
    ),
) -> tuple[list[Any], Any, dict[str, Any]]:
    """Capture a locally expanding raw plan through one paid session.

    ``planner`` must be network-free and return an object with ``specs`` and
    ``missing_raw_keys``.  It is called after every captured batch, allowing a
    stored schedule page or participants payload to reveal the next pages,
    squads and other evidence-derived endpoints.  Repeated missing-key state is
    rejected instead of looping or opening another browser.
    """

    plan = planner()
    if not plan.missing_raw_keys:
        return [], plan, _zero_traffic()
    engine = runtime.engine
    metrics_before = engine.metrics.snapshot()
    previous_budget = engine.budget
    previous_run_id = engine.run_id
    previous_task_id = engine.task_id
    allocation_budget, normalized_plan, normalized_allocation = _allocation_budget_view(
        engine,
        workload_plan,
        allocation_id,
    )
    previous_transport = engine.transport
    captured: list[Any] = []
    seen_states: set[tuple[str, ...]] = set()
    engine.budget = allocation_budget
    if normalized_plan is not None and normalized_allocation is not None:
        engine.run_id = normalized_plan.run_id
        engine.task_id = normalized_allocation.task_id
    try:
        transport = transport_factory(
            engine,
            canonical_url=canonical_url,
            scope=scope,
            entity=entity,
            workload_plan=normalized_plan,
            allocation_id=allocation_id,
            attempt_id=attempt_id,
        )
        with transport:
            engine.transport = transport
            while plan.missing_raw_keys:
                missing = set(plan.missing_raw_keys)
                state = tuple(sorted(key.stable_id() for key in missing))
                if state in seen_states:
                    raise RuntimeError(
                        "SofaScore dynamic capture made no raw-plan progress: "
                        + ", ".join(state)
                    )
                seen_states.add(state)
                batch = [spec for spec in plan.specs if spec.key in missing]
                if {spec.key for spec in batch} != missing:
                    raise RuntimeError(
                        "SofaScore planner exposed missing raw keys without specs"
                    )
                for spec in batch:
                    result = engine.capture(spec)
                    captured.append(result)
                    _require_publishable(result)
                plan = planner()
    finally:
        engine.transport = previous_transport
        engine.budget = previous_budget
        engine.run_id = previous_run_id
        engine.task_id = previous_task_id
    traffic = _live_traffic(engine, metrics_before, transport)
    return captured, plan, traffic


__all__ = [
    "LeaseBackedCamoufoxTransport",
    "capture_live_dynamic_specs",
    "capture_live_specs",
    "hash_proxy_exit",
]
