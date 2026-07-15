"""Fail-closed client for the FBref paid-proxy byte meter.

The data plane never receives an upstream residential credential.  It gets a
short-lived proxy-filter lease token and the proxy-filter charges every byte
before forwarding it to or from the provider.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional
from urllib.parse import urlsplit


METER_ID = "proxy_filter_provider_path_v2"
FBREF_DAG_IDS = frozenset({"dag_ingest_fbref", "dag_backfill_fbref"})
DEFAULT_CONTROL_TIMEOUT_SECONDS = 5.0
DEFAULT_DRAIN_TIMEOUT_SECONDS = 10.0
DEFAULT_LEASE_TTL_SECONDS = 7200


class FBrefProxyLeaseError(RuntimeError):
    """A paid lease could not be created or accounted exactly."""


@dataclass(frozen=True)
class FBrefProxyLease:
    lease_id: str
    token: str
    proxy_url: str
    max_bytes: int
    expires_at: float


@dataclass(frozen=True)
class FBrefLeaseStats:
    lease_id: str
    source: str
    dag_id: str
    run_id: str
    up_bytes: int
    down_bytes: int
    active_tunnels: int
    reserved_bytes: int
    closed: bool
    budget_exceeded: bool
    close_complete: bool = False

    @property
    def total_bytes(self) -> int:
        return self.up_bytes + self.down_bytes


def _control_token_from_environment() -> str:
    for name in (
        "FBREF_PROXY_CONTROL_TOKEN",
        "PROXY_FILTER_CONTROL_TOKEN",
        "SOFASCORE_PROXY_CONTROL_TOKEN",
    ):
        value = str(os.environ.get(name, "")).strip()
        if value:
            return value
    return ""


def _safe_error(value: object) -> str:
    """Return a bounded control-plane error without reflecting credentials."""

    rendered = " ".join(str(value or "unknown error").split())[:240]
    return "".join(
        character
        if character.isalnum() or character in " ._:/;=()[]-"
        else "?"
        for character in rendered
    )


class FBrefProxyLeaseClient:
    """Small synchronous adapter for proxy-filter's authenticated lease API."""

    def __init__(
        self,
        control_base_url: str,
        *,
        control_token: Optional[str] = None,
        session: Optional[Any] = None,
        timeout_seconds: float = DEFAULT_CONTROL_TIMEOUT_SECONDS,
        drain_timeout_seconds: float = DEFAULT_DRAIN_TIMEOUT_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        base = str(control_base_url).rstrip("/")
        parsed = urlsplit(base)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "FBref proxy control URL must be credential-free absolute HTTP(S)"
            )
        token = str(
            control_token
            if control_token is not None
            else _control_token_from_environment()
        ).strip()
        if len(token) < 32:
            raise FBrefProxyLeaseError(
                "FBREF_PROXY_CONTROL_TOKEN must contain at least 32 characters"
            )
        timeout = float(timeout_seconds)
        drain_timeout = float(drain_timeout_seconds)
        if timeout <= 0 or drain_timeout <= 0:
            raise ValueError("FBref lease timeouts must be positive")
        self.control_base_url = base
        self._control_scheme = str(parsed.scheme).lower()
        self._control_hostname = str(parsed.hostname).lower()
        self._control_token = token
        self._session = session
        self.timeout_seconds = timeout
        self.drain_timeout_seconds = drain_timeout
        self._sleep = sleep
        self._monotonic = monotonic

    def _client(self):
        if self._session is None:
            import requests

            self._session = requests.Session()
            # The control token belongs only on the Docker-internal meter.
            # Never let HTTP(S)_PROXY route it through an ambient proxy.
            self._session.trust_env = False
        return self._session

    def _request(
        self,
        method: str,
        path: str,
        *,
        lease: Optional[FBrefProxyLease] = None,
        payload: Optional[Mapping[str, Any]] = None,
        accepted_statuses: frozenset[int] = frozenset(),
    ) -> tuple[int, Mapping[str, Any]]:
        headers = {"X-Proxy-Control-Token": self._control_token}
        if lease is not None:
            headers["Authorization"] = f"Bearer {lease.token}"
        try:
            response = self._client().request(
                method,
                f"{self.control_base_url}{path}",
                json=None if payload is None else dict(payload),
                headers=headers,
                timeout=self.timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001 - requests adapter boundary
            raise FBrefProxyLeaseError(
                f"FBref proxy meter request failed: {type(exc).__name__}"
            ) from exc
        status = int(getattr(response, "status_code", 0) or 0)
        try:
            body = response.json()
        except Exception as exc:  # noqa: BLE001 - untrusted control response
            raise FBrefProxyLeaseError(
                f"FBref proxy meter returned invalid JSON (HTTP {status})"
            ) from exc
        if not isinstance(body, Mapping):
            raise FBrefProxyLeaseError(
                f"FBref proxy meter returned a non-object (HTTP {status})"
            )
        if not (200 <= status < 300 or status in accepted_statuses):
            raise FBrefProxyLeaseError(
                "FBref proxy meter rejected "
                f"{method} {path} (HTTP {status}): "
                f"{_safe_error(body.get('error'))}"
            )
        return status, body

    def acquire(
        self,
        *,
        max_bytes: int,
        ttl_seconds: int,
        metadata: Mapping[str, Any],
    ) -> FBrefProxyLease:
        requested = int(max_bytes)
        ttl = int(ttl_seconds)
        required = ("dag_id", "run_id", "task_id", "canonical_url")
        if requested <= 0 or ttl <= 0:
            raise ValueError("FBref lease byte and TTL limits must be positive")
        if not all(str(metadata.get(name) or "").strip() for name in required):
            raise FBrefProxyLeaseError(
                "FBref paid lease requires dag_id, run_id, task_id and canonical_url"
            )
        dag_id = str(metadata["dag_id"]).strip()
        if dag_id not in FBREF_DAG_IDS or str(metadata.get("source")) != "fbref":
            raise FBrefProxyLeaseError("FBref paid lease has invalid source provenance")
        payload = dict(metadata)
        payload.update({"max_bytes": requested, "ttl_seconds": ttl})
        _, body = self._request("POST", "/v1/leases", payload=payload)
        try:
            lease = FBrefProxyLease(
                lease_id=str(body["id"]),
                token=str(body["token"]),
                proxy_url=str(body["proxy_url"]),
                max_bytes=int(body["max_bytes"]),
                expires_at=float(body["expires_at"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise FBrefProxyLeaseError(
                "FBref proxy meter lease schema mismatch"
            ) from exc
        proxy = urlsplit(lease.proxy_url)
        if (
            not lease.lease_id
            or not lease.token
            or proxy.scheme not in {"http", "https"}
            or not proxy.hostname
            or proxy.username is not None
            or proxy.password is not None
            or str(proxy.scheme).lower() != self._control_scheme
            or str(proxy.hostname).lower() != self._control_hostname
            or proxy.path not in {"", "/"}
            or proxy.query
            or proxy.fragment
            or lease.max_bytes <= 0
            or lease.max_bytes > requested
        ):
            raise FBrefProxyLeaseError("FBref proxy meter returned an unsafe lease")
        return lease

    @staticmethod
    def playwright_proxy(lease: FBrefProxyLease) -> dict[str, str]:
        return {
            "server": lease.proxy_url,
            "username": "lease",
            "password": lease.token,
        }

    def _stats_from_mapping(
        self,
        lease: FBrefProxyLease,
        value: Mapping[str, Any],
        *,
        expected: Mapping[str, Any],
    ) -> FBrefLeaseStats:
        try:
            up = int(value["up_bytes"])
            down = int(value["down_bytes"])
            active = int(value["active_tunnels"])
            reserved = int(value["reserved_bytes"])
            total = int(value["total_bytes"])
            stats = FBrefLeaseStats(
                lease_id=str(value["id"]),
                source=str(value["source"]),
                dag_id=str(value["dag_id"]),
                run_id=str(value["run_id"]),
                up_bytes=up,
                down_bytes=down,
                active_tunnels=active,
                reserved_bytes=reserved,
                closed=bool(value["closed"]),
                budget_exceeded=bool(value["budget_exceeded"]),
                close_complete=bool(value.get("close_complete", False)),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise FBrefProxyLeaseError(
                "FBref proxy meter stats schema mismatch"
            ) from exc
        if (
            min(up, down, active, reserved) < 0
            or total != stats.total_bytes
            or stats.total_bytes > lease.max_bytes
            or stats.lease_id != lease.lease_id
            or stats.source != "fbref"
            or stats.dag_id != str(expected.get("dag_id") or "")
            or stats.run_id != str(expected.get("run_id") or "")
            or str(value.get("meter") or "") != METER_ID
        ):
            raise FBrefProxyLeaseError(
                "FBref proxy meter stats failed provenance validation"
            )
        return stats

    def stats(
        self,
        lease: FBrefProxyLease,
        *,
        expected: Mapping[str, Any],
    ) -> FBrefLeaseStats:
        _, body = self._request(
            "GET", f"/v1/leases/{lease.lease_id}/stats", lease=lease
        )
        return self._stats_from_mapping(lease, body, expected=expected)

    def wait_drained(
        self,
        lease: FBrefProxyLease,
        *,
        expected: Mapping[str, Any],
    ) -> FBrefLeaseStats:
        deadline = self._monotonic() + self.drain_timeout_seconds
        while True:
            stats = self.stats(lease, expected=expected)
            if stats.active_tunnels == 0 and stats.reserved_bytes == 0:
                return stats
            if self._monotonic() >= deadline:
                raise FBrefProxyLeaseError(
                    "FBref paid proxy tunnels did not drain before accounting"
                )
            self._sleep(0.05)

    def close(
        self,
        lease: FBrefProxyLease,
        *,
        expected: Mapping[str, Any],
    ) -> FBrefLeaseStats:
        deadline = self._monotonic() + self.drain_timeout_seconds
        while True:
            status, body = self._request(
                "DELETE",
                f"/v1/leases/{lease.lease_id}/close",
                lease=lease,
                payload={"completed": True},
                accepted_statuses=frozenset({409}),
            )
            stats = self._stats_from_mapping(lease, body, expected=expected)
            if status != 409 and stats.close_complete:
                return stats
            if self._monotonic() >= deadline:
                raise FBrefProxyLeaseError(
                    "FBref paid lease close did not return final counters"
                )
            self._sleep(0.05)


__all__ = [
    "DEFAULT_LEASE_TTL_SECONDS",
    "FBREF_DAG_IDS",
    "FBrefLeaseStats",
    "FBrefProxyLease",
    "FBrefProxyLeaseClient",
    "FBrefProxyLeaseError",
    "METER_ID",
]
