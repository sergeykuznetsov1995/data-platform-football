"""Small fail-closed client for proxy-filter's SofaScore session leases.

Lease creation is stateful and intentionally has no hidden retry.  One lease is
owned by one logical Airflow task and one warmed browser session; endpoint
payloads share its exact provider-path byte counter.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional
from urllib.parse import quote, urlsplit, urlunsplit

from scrapers.sofascore.workload_plan import SignedDagRunPlan, WorkloadAllocation


_URL_CREDENTIALS_RE = re.compile(
    r"(?P<scheme>(?:https?|socks[45])://)(?P<credentials>[^/@\s]+)@",
    re.IGNORECASE,
)
_AUTH_VALUE_RE = re.compile(
    r"(?i)(authorization|proxy-authorization|token)(\s*[:=]\s*)([^\s,;}]+)"
)


def redact_sensitive(value: Any, *, secrets: tuple[str, ...] = ()) -> str:
    """Remove proxy credentials and known bearer values from diagnostics."""
    redacted = _URL_CREDENTIALS_RE.sub(r"\g<scheme>****:****@", str(value))
    redacted = _AUTH_VALUE_RE.sub(r"\1\2[REDACTED]", redacted)
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "[REDACTED]")
    return redacted


class SofascoreLeaseError(RuntimeError):
    """Base class for lease control failures."""


class SofascoreLeaseRejected(SofascoreLeaseError):
    """The control plane rejected a lease or its credentials."""

    def __init__(self, message: str, *, status_code: int, code: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code


class SofascoreLeaseProtocolError(SofascoreLeaseError):
    """The lease service returned an unusable or inconsistent response."""


def _phase_run_id(run_id: str) -> tuple[str, str]:
    value = str(run_id).strip()
    if value.count("::") != 1:
        raise ValueError(
            "production run_id must end in ::season, ::targets or ::players"
        )
    base_run_id, phase = value.rsplit("::", 1)
    if not base_run_id or phase not in {"season", "targets", "players"}:
        raise ValueError(
            "production run_id must end in ::season, ::targets or ::players"
        )
    return base_run_id, phase


@dataclass(frozen=True)
class SofascoreProxyLease:
    lease_id: str
    token: str = field(repr=False)
    proxy_url: str
    max_bytes: int
    expires_at: float
    source: str = "sofascore"
    artifact_id: str = ""
    plan_digest: str = ""
    allocation_id: str = ""
    allocation_task_id: str = ""
    allocation_scope: str = ""
    allocation_class: str = ""
    allocation_batch_index: int = -1
    allocation_units: tuple[str, ...] = ()
    allocation_budget_bytes: int = 0
    base_run_id: str = ""
    workload_phase: str = ""


@dataclass(frozen=True)
class SofascoreLeaseStats:
    lease_id: str
    up_bytes: int
    down_bytes: int
    total_bytes: int
    max_bytes: int
    dagrun_total_bytes: int
    dagrun_budget_bytes: int
    daily_total_bytes: int
    daily_budget_bytes: int
    active_tunnels: int
    reserved_bytes: int
    closed: bool
    expired: bool
    budget_exceeded: bool
    source: str
    upstream_fingerprint: str
    budget_artifact_id: str
    plan_digest: str
    allocation_id: str
    allocation_task_id: str
    allocation_scope: str
    allocation_class: str
    allocation_batch_index: int
    allocation_units: tuple[str, ...]
    allocation_budget_bytes: int
    allocation_spent_provider_bytes: int
    allocation_remaining_provider_bytes: int
    endpoint_request_provider_bytes: Mapping[str, tuple[int, ...]]
    base_run_id: str
    workload_phase: str
    phase_plan_digest: str
    parent_run_cap_bytes: int
    parent_run_spent_provider_bytes: int
    # Times the filter re-pinned the lease's residential exit before its first
    # provider byte (#946 dead-exit failover).  Defaults to 0 when the proxy
    # predates the field, which keeps any fingerprint drift fail-closed.
    upstream_repins: int = 0

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "SofascoreLeaseStats":
        def integer(name: str, *, positive: bool = False) -> int:
            value = payload.get(name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise SofascoreLeaseProtocolError(
                    f"proxy lease field {name!r} must be an integer"
                )
            if value < (1 if positive else 0):
                raise SofascoreLeaseProtocolError(
                    f"proxy lease field {name!r} is out of range"
                )
            return value

        def boolean(name: str) -> bool:
            value = payload.get(name)
            if not isinstance(value, bool):
                raise SofascoreLeaseProtocolError(
                    f"proxy lease field {name!r} must be a boolean"
                )
            return value

        lease_id = str(payload.get("id") or "").strip()
        if not lease_id:
            raise SofascoreLeaseProtocolError("proxy lease stats have no id")
        up_bytes = integer("up_bytes")
        down_bytes = integer("down_bytes")
        total_bytes = integer("total_bytes")
        max_bytes = integer("max_bytes", positive=True)
        dagrun_total_bytes = integer("dagrun_total_bytes")
        dagrun_budget_bytes = integer("dagrun_budget_bytes", positive=True)
        daily_total_bytes = integer("daily_total_bytes")
        daily_budget_bytes = integer("daily_budget_bytes", positive=True)
        if total_bytes != up_bytes + down_bytes:
            raise SofascoreLeaseProtocolError(
                "proxy lease directional counters do not equal total_bytes"
            )
        if total_bytes > max_bytes or dagrun_total_bytes < total_bytes:
            raise SofascoreLeaseProtocolError(
                "proxy lease counters exceed their bounds"
            )
        source = str(payload.get("source") or "")
        if source not in ("sofascore", "sofascore_canary"):
            raise SofascoreLeaseProtocolError(
                "proxy lease stats are not scoped to SofaScore"
            )
        upstream_fingerprint = str(payload.get("upstream_fingerprint") or "")
        budget_artifact_id = str(payload.get("budget_artifact_id") or "")
        if len(upstream_fingerprint) < 12 or len(budget_artifact_id) != 64:
            raise SofascoreLeaseProtocolError(
                "proxy lease stats lack pinned-upstream or canary provenance"
            )
        plan_digest = str(payload.get("plan_digest") or "")
        allocation_id = str(payload.get("allocation_id") or "")
        allocation_task_id = str(payload.get("allocation_task_id") or "")
        allocation_scope = str(payload.get("allocation_scope") or "")
        allocation_class = str(payload.get("allocation_class") or "")
        raw_units = payload.get("allocation_units")
        raw_endpoint_map = payload.get("endpoint_request_provider_bytes", {})
        base_run_id = str(payload.get("base_run_id") or "")
        workload_phase = str(payload.get("workload_phase") or "")
        phase_plan_digest = str(payload.get("phase_plan_digest") or "")
        if source == "sofascore":
            if (
                len(plan_digest) != 64
                or not allocation_id
                or not allocation_task_id
                or allocation_scope not in {"match", "player", "season"}
                or not allocation_class
                or not isinstance(raw_units, list)
                or not raw_units
                or not base_run_id
                or workload_phase not in {"season", "targets", "players"}
                or phase_plan_digest != plan_digest
            ):
                raise SofascoreLeaseProtocolError(
                    "production lease stats lack signed allocation provenance"
                )
        elif raw_units is None:
            raw_units = []
        if not isinstance(raw_units, list) or any(
            not isinstance(unit, str) or not unit for unit in raw_units
        ):
            raise SofascoreLeaseProtocolError("invalid allocation units in stats")
        if not isinstance(raw_endpoint_map, Mapping):
            raise SofascoreLeaseProtocolError("invalid endpoint provider map in stats")
        endpoint_map: dict[str, tuple[int, ...]] = {}
        for endpoint, observations in raw_endpoint_map.items():
            if (
                not isinstance(endpoint, str)
                or not endpoint
                or not isinstance(observations, list)
                or not observations
                or any(
                    isinstance(value, bool) or not isinstance(value, int) or value < 0
                    for value in observations
                )
            ):
                raise SofascoreLeaseProtocolError(
                    "invalid endpoint provider observations in stats"
                )
            endpoint_map[endpoint] = tuple(observations)
        raw_batch_index = payload.get("allocation_batch_index", -1)
        if (
            isinstance(raw_batch_index, bool)
            or not isinstance(raw_batch_index, int)
            or raw_batch_index < (-1 if source == "sofascore_canary" else 0)
        ):
            raise SofascoreLeaseProtocolError("invalid allocation batch index in stats")
        raw_upstream_repins = payload.get("upstream_repins", 0)
        if (
            isinstance(raw_upstream_repins, bool)
            or not isinstance(raw_upstream_repins, int)
            or raw_upstream_repins < 0
        ):
            raise SofascoreLeaseProtocolError("invalid upstream re-pin count in stats")
        parent_run_cap_bytes = integer("parent_run_cap_bytes")
        parent_run_spent_provider_bytes = integer("parent_run_spent_provider_bytes")
        if source == "sofascore" and (
            parent_run_cap_bytes <= 0
            or parent_run_spent_provider_bytes > parent_run_cap_bytes
        ):
            raise SofascoreLeaseProtocolError("invalid parent DagRun envelope stats")
        return cls(
            lease_id=lease_id,
            up_bytes=up_bytes,
            down_bytes=down_bytes,
            total_bytes=total_bytes,
            max_bytes=max_bytes,
            dagrun_total_bytes=dagrun_total_bytes,
            dagrun_budget_bytes=dagrun_budget_bytes,
            daily_total_bytes=daily_total_bytes,
            daily_budget_bytes=daily_budget_bytes,
            active_tunnels=integer("active_tunnels"),
            reserved_bytes=integer("reserved_bytes"),
            closed=boolean("closed"),
            expired=boolean("expired"),
            budget_exceeded=boolean("budget_exceeded"),
            source=source,
            upstream_fingerprint=upstream_fingerprint,
            budget_artifact_id=budget_artifact_id,
            plan_digest=plan_digest,
            allocation_id=allocation_id,
            allocation_task_id=allocation_task_id,
            allocation_scope=allocation_scope,
            allocation_class=allocation_class,
            allocation_batch_index=raw_batch_index,
            allocation_units=tuple(raw_units),
            allocation_budget_bytes=integer(
                "allocation_budget_bytes", positive=(source == "sofascore")
            ),
            allocation_spent_provider_bytes=integer("allocation_spent_provider_bytes"),
            allocation_remaining_provider_bytes=integer(
                "allocation_remaining_provider_bytes"
            ),
            endpoint_request_provider_bytes=endpoint_map,
            base_run_id=base_run_id,
            workload_phase=workload_phase,
            phase_plan_digest=phase_plan_digest,
            parent_run_cap_bytes=parent_run_cap_bytes,
            parent_run_spent_provider_bytes=parent_run_spent_provider_bytes,
            upstream_repins=raw_upstream_repins,
        )


class SofascoreLeaseClient:
    """Control-plane adapter for one sticky, metered warmed-browser lease."""

    def __init__(
        self,
        control_url: str,
        *,
        timeout_seconds: float = 5.0,
        session: Optional[Any] = None,
        control_token: Optional[str] = None,
    ) -> None:
        base = str(control_url).rstrip("/")
        parsed = urlsplit(base)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError("proxy lease control URL must be credential-free HTTP(S)")
        if timeout_seconds <= 0:
            raise ValueError("proxy lease timeout must be positive")
        resolved_token = str(
            control_token
            if control_token is not None
            else os.environ.get("SOFASCORE_PROXY_CONTROL_TOKEN", "")
        )
        if len(resolved_token) < 32:
            raise ValueError(
                "SOFASCORE_PROXY_CONTROL_TOKEN must contain at least 32 characters"
            )
        self.control_url = base
        self.timeout_seconds = float(timeout_seconds)
        self._control_token = resolved_token
        self._session = session
        if self._session is not None:
            self._session.trust_env = False

    def _http(self):
        if self._session is None:
            import requests

            self._session = requests.Session()
            self._session.trust_env = False
        return self._session

    def _request(
        self,
        method: str,
        path: str,
        *,
        token: str = "",
        payload: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        headers = {"X-Proxy-Control-Token": self._control_token}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            response = self._http().request(
                method,
                f"{self.control_url}{path}",
                json=dict(payload) if payload is not None else None,
                headers=headers,
                timeout=self.timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001 - requests is an adapter boundary
            message = redact_sensitive(exc, secrets=(token,))
            raise SofascoreLeaseProtocolError(
                f"proxy lease control request failed: {message}"
            ) from exc
        status = int(getattr(response, "status_code", 0) or 0)
        try:
            body = response.json()
        except Exception as exc:  # noqa: BLE001 - response adapter boundary
            raise SofascoreLeaseProtocolError(
                f"proxy lease API returned invalid JSON (HTTP {status})"
            ) from exc
        if not isinstance(body, dict):
            raise SofascoreLeaseProtocolError(
                f"proxy lease API returned a non-object (HTTP {status})"
            )
        if not 200 <= status < 300:
            code = str(body.get("code") or "")
            error = redact_sensitive(
                body.get("error") or "request rejected", secrets=(token,)
            )
            raise SofascoreLeaseRejected(
                f"proxy lease API rejected {method} {path} (HTTP {status}): {error}",
                status_code=status,
                code=code,
            )
        return body

    def acquire(
        self,
        *,
        max_bytes: int,
        ttl_seconds: int,
        dag_id: str,
        run_id: str,
        task_id: str,
        scope: str = "",
        entity: str = "",
        canonical_url: str = "https://www.sofascore.com/",
        map_index: int = -1,
        try_number: int = 0,
        source: str = "sofascore",
        workload_plan: Optional[SignedDagRunPlan | Mapping[str, Any]] = None,
        allocation_id: str = "",
        attempt_id: str = "",
    ) -> SofascoreProxyLease:
        if (
            isinstance(max_bytes, bool)
            or not isinstance(max_bytes, int)
            or max_bytes <= 0
            or isinstance(ttl_seconds, bool)
            or not isinstance(ttl_seconds, int)
            or ttl_seconds <= 0
        ):
            raise ValueError("max_bytes and ttl_seconds must be positive integers")
        source = str(source).strip().lower()
        if source not in ("sofascore", "sofascore_canary"):
            raise ValueError(
                "SofaScore lease source must be sofascore or sofascore_canary"
            )
        expected_dag_id = (
            "dag_ingest_sofascore"
            if source == "sofascore"
            else "dag_canary_sofascore_proxy"
        )
        if str(dag_id).strip() != expected_dag_id:
            raise ValueError(
                f"SofaScore source={source} lease requires dag_id={expected_dag_id}"
            )
        plan: Optional[SignedDagRunPlan] = None
        allocation: Optional[WorkloadAllocation] = None
        base_run_id = ""
        workload_phase = ""
        if source == "sofascore":
            if isinstance(workload_plan, SignedDagRunPlan):
                workload_plan.verify(self._control_token)
                plan = workload_plan
            elif isinstance(workload_plan, Mapping):
                plan = SignedDagRunPlan.from_dict(
                    workload_plan,
                    control_token=self._control_token,
                )
            else:
                raise ValueError(
                    "production SofaScore lease requires a signed workload plan"
                )
            if plan.dag_id != str(dag_id).strip() or plan.run_id != str(run_id).strip():
                raise ValueError("signed workload plan DAG/run mismatch")
            base_run_id, workload_phase = _phase_run_id(plan.run_id)
            try:
                allocation = next(
                    item
                    for item in plan.allocations
                    if item.allocation_id == str(allocation_id).strip()
                )
            except StopIteration as exc:
                raise ValueError(
                    "allocation_id is absent from the signed workload plan"
                ) from exc
            if (
                str(task_id).strip() != allocation.task_id
                or max_bytes != allocation.budget_bytes
                or not str(attempt_id).strip()
            ):
                raise ValueError(
                    "lease task/budget/attempt do not match signed allocation"
                )
        elif workload_plan is not None or allocation_id or attempt_id:
            raise ValueError("canary leases cannot carry production allocations")
        context = {
            "dag_id": str(dag_id).strip(),
            "run_id": str(run_id).strip(),
            "task_id": str(task_id).strip(),
            "canonical_url": str(canonical_url).strip(),
        }
        if not all(context.values()):
            raise ValueError("dag_id, run_id, task_id and canonical_url are required")
        request: dict[str, Any] = {
            **context,
            "source": source,
            "scope": allocation.scope if allocation is not None else str(scope),
            "capture_scope": str(scope) if allocation is not None else "",
            "entity": str(entity),
            "map_index": int(map_index),
            "try_number": int(try_number),
            "max_bytes": max_bytes,
            "ttl_seconds": ttl_seconds,
        }
        if plan is not None and allocation is not None:
            request.update(
                {
                    "workload_plan": plan.to_dict(),
                    "allocation_id": allocation.allocation_id,
                    "allocation": allocation.to_dict(),
                    "attempt_id": str(attempt_id).strip(),
                }
            )
        body = self._request("POST", "/v1/leases", payload=request)
        try:
            lease = SofascoreProxyLease(
                lease_id=str(body["id"]),
                token=str(body["token"]),
                proxy_url=str(body["proxy_url"]),
                max_bytes=int(body["max_bytes"]),
                expires_at=float(body["expires_at"]),
                source=source,
                artifact_id=plan.artifact_id if plan is not None else "",
                plan_digest=str(body.get("plan_digest") or ""),
                allocation_id=str(body.get("allocation_id") or ""),
                allocation_task_id=allocation.task_id if allocation else "",
                allocation_scope=allocation.scope if allocation else "",
                allocation_class=allocation.workload_class if allocation else "",
                allocation_batch_index=allocation.batch_index if allocation else -1,
                allocation_units=allocation.units if allocation else (),
                allocation_budget_bytes=int(body.get("allocation_budget_bytes") or 0),
                base_run_id=base_run_id,
                workload_phase=workload_phase,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SofascoreLeaseProtocolError(
                "proxy lease API response schema mismatch"
            ) from exc
        proxy = urlsplit(lease.proxy_url)
        if (
            not lease.lease_id
            or not lease.token
            or proxy.scheme not in ("http", "https")
            or not proxy.hostname
            or proxy.username is not None
            or proxy.password is not None
            or lease.max_bytes <= 0
            or lease.max_bytes > max_bytes
            or (
                plan is not None
                and (
                    lease.plan_digest != plan.plan_digest
                    or lease.allocation_id != allocation.allocation_id
                    or lease.allocation_budget_bytes != allocation.budget_bytes
                )
            )
        ):
            raise SofascoreLeaseProtocolError(
                "proxy lease API returned an unusable lease"
            )
        return lease

    def stats(self, lease: SofascoreProxyLease) -> SofascoreLeaseStats:
        body = self._request(
            "GET",
            f"/v1/leases/{quote(lease.lease_id, safe='')}/stats",
            token=lease.token,
        )
        stats = SofascoreLeaseStats.from_mapping(body)
        self._validate_stats_provenance(lease, stats)
        return stats

    @staticmethod
    def _validate_stats_provenance(
        lease: SofascoreProxyLease,
        stats: SofascoreLeaseStats,
    ) -> None:
        if stats.lease_id != lease.lease_id or stats.source != lease.source:
            raise SofascoreLeaseProtocolError("proxy lease stats id mismatch")
        if lease.source == "sofascore" and (
            not lease.plan_digest
            or stats.budget_artifact_id != lease.artifact_id
            or stats.plan_digest != lease.plan_digest
            or stats.allocation_id != lease.allocation_id
            or stats.allocation_task_id != lease.allocation_task_id
            or stats.allocation_scope != lease.allocation_scope
            or stats.allocation_class != lease.allocation_class
            or stats.allocation_batch_index != lease.allocation_batch_index
            or stats.allocation_units != lease.allocation_units
            or stats.allocation_budget_bytes != lease.allocation_budget_bytes
            or stats.max_bytes > lease.allocation_budget_bytes
            or stats.base_run_id != lease.base_run_id
            or stats.workload_phase != lease.workload_phase
            or stats.phase_plan_digest != lease.plan_digest
        ):
            raise SofascoreLeaseProtocolError(
                "proxy lease stats allocation provenance mismatch"
            )

    def begin_endpoint(
        self,
        lease: SofascoreProxyLease,
        endpoint: str,
    ) -> str:
        name = str(endpoint).strip()
        if not name:
            raise ValueError("endpoint must not be empty")
        body = self._request(
            "POST",
            f"/v1/leases/{quote(lease.lease_id, safe='')}/endpoints",
            token=lease.token,
            payload={"endpoint": name},
        )
        request_id = str(body.get("request_id") or "").strip()
        if not request_id:
            raise SofascoreLeaseProtocolError(
                "proxy lease endpoint boundary has no request_id"
            )
        return request_id

    def finish_endpoint(
        self,
        lease: SofascoreProxyLease,
        request_id: str,
    ) -> SofascoreLeaseStats:
        boundary = str(request_id).strip()
        if not boundary:
            raise ValueError("request_id must not be empty")
        body = self._request(
            "DELETE",
            f"/v1/leases/{quote(lease.lease_id, safe='')}/endpoints/"
            f"{quote(boundary, safe='')}",
            token=lease.token,
        )
        stats = SofascoreLeaseStats.from_mapping(body)
        self._validate_stats_provenance(lease, stats)
        return stats

    def close(
        self,
        lease: SofascoreProxyLease,
        *,
        endpoint_request_provider_bytes: Optional[Mapping[str, list[int]]] = None,
        completed: bool = False,
        proxy_exit_hash: Optional[str] = None,
    ) -> SofascoreLeaseStats:
        payload: dict[str, Any] = {
            "endpoint_request_provider_bytes": dict(
                endpoint_request_provider_bytes or {}
            ),
            "completed": bool(completed),
        }
        if proxy_exit_hash is not None:
            payload["proxy_exit_hash"] = str(proxy_exit_hash)
        body = self._request(
            "DELETE",
            f"/v1/leases/{quote(lease.lease_id, safe='')}/close",
            token=lease.token,
            payload=payload,
        )
        stats = SofascoreLeaseStats.from_mapping(body)
        self._validate_stats_provenance(lease, stats)
        if (
            stats.lease_id != lease.lease_id
            or stats.source != lease.source
            or not stats.closed
            or stats.active_tunnels != 0
            or stats.reserved_bytes != 0
        ):
            raise SofascoreLeaseProtocolError("proxy lease did not close cleanly")
        return stats

    @staticmethod
    def authenticated_proxy_url(lease: SofascoreProxyLease) -> str:
        """Build the Basic-auth URL consumed by Camoufox/Playwright."""
        parsed = urlsplit(lease.proxy_url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise SofascoreLeaseProtocolError("invalid lease proxy URL")
        host = parsed.hostname
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        port = f":{parsed.port}" if parsed.port is not None else ""
        netloc = f"lease:{quote(lease.token, safe='')}@{host}{port}"
        return urlunsplit((parsed.scheme, netloc, parsed.path, "", ""))

    @staticmethod
    def playwright_proxy(lease: SofascoreProxyLease) -> dict[str, str]:
        """Return Camoufox/Playwright proxy fields without URL credentials."""
        parsed = urlsplit(lease.proxy_url)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise SofascoreLeaseProtocolError("invalid lease proxy URL")
        return {
            "server": lease.proxy_url,
            "username": "lease",
            "password": lease.token,
        }


# Conventional spelling for new callers; keep the name communicated to the
# capture integration agent as an alias.
SofaScoreLeaseClient = SofascoreLeaseClient


__all__ = [
    "SofaScoreLeaseClient",
    "SofascoreLeaseClient",
    "SofascoreLeaseError",
    "SofascoreLeaseProtocolError",
    "SofascoreLeaseRejected",
    "SofascoreLeaseStats",
    "SofascoreProxyLease",
    "redact_sensitive",
]
