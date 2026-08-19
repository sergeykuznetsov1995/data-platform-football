"""Fail-closed client for the FBref paid-proxy byte meter.

The data plane never receives an upstream residential credential.  It gets a
short-lived proxy-filter lease token and the proxy-filter charges every byte
before forwarding it to or from the provider.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)


METER_ID = "proxy_filter_provider_path_v2"
FBREF_DAG_IDS = frozenset(
    {
        "dag_ingest_fbref",
        "dag_bootstrap_fbref",
        "dag_backfill_fbref",
        "dag_accept_fbref_bronze",
    }
)
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
    task_id: str = ""
    canonical_url: str = ""
    scope: str = ""
    active_provider_readers: int = 0
    provider_reserved_bytes: int = 0
    pending_client_hellos: int = 0
    staged_client_bytes: int = 0
    expired: bool = False
    accounting_uncertain: bool = False
    close_complete: bool = False

    @property
    def total_bytes(self) -> int:
        return self.up_bytes + self.down_bytes


def _control_token_from_environment() -> str:
    # FBref has a distinct paid pool and control plane. Falling back to the
    # shared/SofaScore secret silently couples their lifecycles and can point
    # an acceptance run at the wrong meter.
    return str(os.environ.get("FBREF_PROXY_CONTROL_TOKEN", "")).strip()


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
        provenance_fields = (
            "source",
            "dag_id",
            "run_id",
            "task_id",
            "canonical_url",
            "scope",
        )
        if any(
            name not in value
            or not isinstance(value[name], str)
            or not value[name]
            for name in provenance_fields
        ):
            raise FBrefProxyLeaseError(
                "FBref proxy meter stats schema mismatch"
            )
        if any(
            name not in expected
            or not isinstance(expected[name], str)
            or not expected[name]
            for name in provenance_fields
        ):
            raise FBrefProxyLeaseError(
                "FBref proxy meter stats failed provenance validation"
            )
        boolean_fields = (
            "closed",
            "expired",
            "budget_exceeded",
            "accounting_uncertain",
        )
        if any(
            name not in value or not isinstance(value[name], bool)
            for name in boolean_fields
        ) or (
            "close_complete" in value
            and not isinstance(value["close_complete"], bool)
        ):
            raise FBrefProxyLeaseError(
                "FBref proxy meter stats schema mismatch"
            )
        try:
            integer_names = (
                "up_bytes",
                "down_bytes",
                "active_tunnels",
                "reserved_bytes",
                "active_provider_readers",
                "provider_reserved_bytes",
                "pending_client_hellos",
                "staged_client_bytes",
                "total_bytes",
            )
            if any(
                isinstance(value[name], bool)
                or not isinstance(value[name], int)
                for name in integer_names
            ):
                raise TypeError("meter counters must be integers")
            (
                up,
                down,
                active,
                reserved,
                readers,
                provider_reserved,
                pending_hellos,
                staged_client,
                total,
            ) = (value[name] for name in integer_names)
            stats = FBrefLeaseStats(
                lease_id=str(value["id"]),
                source=str(value["source"]),
                dag_id=str(value["dag_id"]),
                run_id=str(value["run_id"]),
                task_id=str(value["task_id"]),
                canonical_url=str(value["canonical_url"]),
                scope=str(value["scope"]),
                up_bytes=up,
                down_bytes=down,
                active_tunnels=active,
                reserved_bytes=reserved,
                closed=value["closed"],
                budget_exceeded=value["budget_exceeded"],
                active_provider_readers=readers,
                provider_reserved_bytes=provider_reserved,
                pending_client_hellos=pending_hellos,
                staged_client_bytes=staged_client,
                expired=value["expired"],
                accounting_uncertain=value["accounting_uncertain"],
                close_complete=value.get("close_complete", False),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise FBrefProxyLeaseError(
                "FBref proxy meter stats schema mismatch"
            ) from exc
        if (
            min(
                up,
                down,
                active,
                reserved,
                readers,
                provider_reserved,
                pending_hellos,
                staged_client,
            ) < 0
            or total != stats.total_bytes
            or stats.total_bytes > lease.max_bytes
            or (stats.close_complete and not stats.closed)
            or stats.lease_id != lease.lease_id
            or stats.source != "fbref"
            or any(value[name] != expected[name] for name in provenance_fields)
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

    def extend(
        self,
        lease: FBrefProxyLease,
        *,
        max_bytes: int,
        expected: Mapping[str, Any],
    ) -> FBrefProxyLease:
        """Raise one drained FBref lease cap and keep its identity immutable."""

        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int):
            raise ValueError("FBref lease extension max_bytes must be an integer")
        if max_bytes <= lease.max_bytes:
            raise ValueError("FBref lease extension must increase max_bytes")
        _, body = self._request(
            "POST",
            f"/v1/leases/{lease.lease_id}/extend",
            lease=lease,
            payload={"max_bytes": max_bytes},
        )
        try:
            returned_max = body["max_bytes"]
            returned_expiry = body["expires_at"]
        except (KeyError, TypeError, ValueError) as exc:
            raise FBrefProxyLeaseError(
                "FBref proxy meter extension schema mismatch"
            ) from exc
        stats = self._stats_from_mapping(lease, body, expected=expected)
        if (
            isinstance(returned_max, bool)
            or not isinstance(returned_max, int)
            or returned_max != max_bytes
            or isinstance(returned_expiry, bool)
            or not isinstance(returned_expiry, (int, float))
            or float(returned_expiry) != lease.expires_at
            or str(body.get("id") or "") != lease.lease_id
            or str(body.get("meter") or "") != METER_ID
            or str(body.get("source") or "") != "fbref"
            or str(body.get("dag_id") or "") != str(expected.get("dag_id") or "")
            or str(body.get("run_id") or "") != str(expected.get("run_id") or "")
            or stats.active_tunnels != 0
            or stats.reserved_bytes != 0
            or stats.active_provider_readers != 0
            or stats.provider_reserved_bytes != 0
            or stats.pending_client_hellos != 0
            or stats.staged_client_bytes != 0
            or stats.closed
            or stats.expired
            or stats.budget_exceeded
            or stats.accounting_uncertain
            or (
                "token" in body
                and str(body.get("token") or "") != lease.token
            )
            or (
                "proxy_url" in body
                and str(body.get("proxy_url") or "") != lease.proxy_url
            )
        ):
            raise FBrefProxyLeaseError(
                "FBref proxy meter extension failed provenance validation"
            )
        return FBrefProxyLease(
            lease_id=lease.lease_id,
            token=lease.token,
            proxy_url=lease.proxy_url,
            max_bytes=returned_max,
            expires_at=lease.expires_at,
        )

    def wait_drained(
        self,
        lease: FBrefProxyLease,
        *,
        expected: Mapping[str, Any],
    ) -> FBrefLeaseStats:
        deadline = self._monotonic() + self.drain_timeout_seconds
        while True:
            stats = self.stats(lease, expected=expected)
            if (
                stats.accounting_uncertain
                or stats.budget_exceeded
                or stats.expired
                or stats.closed
            ):
                raise FBrefProxyLeaseError(
                    "FBref paid proxy drain found terminal accounting state"
                )
            if (
                stats.active_tunnels == 0
                and stats.reserved_bytes == 0
                and stats.active_provider_readers == 0
                and stats.provider_reserved_bytes == 0
                and stats.pending_client_hellos == 0
                and stats.staged_client_bytes == 0
            ):
                return stats
            if self._monotonic() >= deadline:
                raise FBrefProxyLeaseError(
                    "FBref paid proxy tunnels did not drain before accounting"
                )
            self._sleep(0.05)

    @staticmethod
    def _idle_tuple(stats: FBrefLeaseStats) -> tuple[object, ...]:
        return (
            stats.lease_id,
            stats.source,
            stats.dag_id,
            stats.run_id,
            stats.task_id,
            stats.canonical_url,
            stats.scope,
            stats.up_bytes,
            stats.down_bytes,
            stats.active_tunnels,
            stats.reserved_bytes,
            stats.active_provider_readers,
            stats.provider_reserved_bytes,
            stats.pending_client_hellos,
            stats.staged_client_bytes,
            stats.closed,
            stats.expired,
            stats.budget_exceeded,
            stats.accounting_uncertain,
            stats.close_complete,
        )

    @staticmethod
    def _idle_mismatches(
        stats: FBrefLeaseStats, expected_tunnels: int
    ) -> list[str]:
        """Name every condition the idle proof rejects, with its reading."""

        if expected_tunnels == 0:
            observed = (
                ("active_tunnels", stats.active_tunnels),
                ("active_provider_readers", stats.active_provider_readers),
                ("reserved_bytes", stats.reserved_bytes),
                ("provider_reserved_bytes", stats.provider_reserved_bytes),
            )
            return [
                f"{name}={value}!=0" for name, value in observed if value != 0
            ]
        mismatches = []
        if stats.active_tunnels != 1:
            mismatches.append(f"active_tunnels={stats.active_tunnels}!=1")
        if stats.active_provider_readers != 1:
            mismatches.append(
                f"active_provider_readers={stats.active_provider_readers}!=1"
            )
        if stats.reserved_bytes != stats.provider_reserved_bytes:
            mismatches.append(
                f"reserved_bytes={stats.reserved_bytes}"
                f"!=provider_reserved_bytes={stats.provider_reserved_bytes}"
            )
        return mismatches

    def wait_idle(
        self,
        lease: FBrefProxyLease,
        *,
        expected: Mapping[str, Any],
        expected_tunnels: int = 1,
    ) -> FBrefLeaseStats:
        """Prove one stable exact checkpoint without closing its tunnel."""

        if expected_tunnels not in {0, 1}:
            raise ValueError("expected_tunnels must be zero or one")
        deadline = self._monotonic() + self.drain_timeout_seconds
        previous: Optional[FBrefLeaseStats] = None
        stable_samples = 0
        samples = 0
        last_up = -1
        last_down = -1
        while True:
            stats = self.stats(lease, expected=expected)
            samples += 1
            if stats.up_bytes < last_up or stats.down_bytes < last_down:
                raise FBrefProxyLeaseError(
                    "FBref paid proxy idle counter moved backwards"
                )
            last_up, last_down = stats.up_bytes, stats.down_bytes
            if (
                stats.budget_exceeded
                or stats.accounting_uncertain
                or stats.expired
                or stats.closed
                or stats.pending_client_hellos != 0
                or stats.staged_client_bytes != 0
            ):
                raise FBrefProxyLeaseError(
                    "FBref paid proxy idle proof found terminal or staged work"
                )
            mismatches = self._idle_mismatches(stats, expected_tunnels)
            impossible = bool(mismatches)
            # The proof has no settling window by construction: idle is the
            # exact negation of impossible, so the first non-ideal sample is
            # terminal and the stable-sample wait below only ever runs on
            # samples that were already idle.  Keeping one predicate keeps a
            # later condition from landing in only half of the pair, which
            # would strand the loop in the generic deadline error instead of
            # the named one.
            idle = not impossible
            if impossible:
                # The proof is terminal on the first bad sample, so the
                # message is the only forensic record of what disagreed.
                raise FBrefProxyLeaseError(
                    "FBref paid proxy idle proof found unexpected tunnel"
                    f" state; sample={samples},"
                    f" expected_tunnels={expected_tunnels},"
                    f" mismatch={'|'.join(mismatches)}"
                )
            if idle:
                if (
                    previous is not None
                    and self._idle_tuple(previous) == self._idle_tuple(stats)
                ):
                    stable_samples += 1
                else:
                    stable_samples = 1
                previous = stats
                if stable_samples >= 2:
                    return stats
            else:
                previous = None
                stable_samples = 0
            if self._monotonic() >= deadline:
                raise FBrefProxyLeaseError(
                    "FBref paid proxy did not reach a stable idle checkpoint"
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
            close_error = str(body.get("close_error") or "")
            if status == 409 and close_error.startswith(
                "provider byte accounting is uncertain"
            ):
                # One mid-response abort latches the lease uncertain forever:
                # the filter retains the unproven byte tail as durable escrow
                # and by design never reports close_complete for that lease.
                # Its counters are the final client-visible ledger, so waiting
                # out the deadline would fail the whole run without protecting
                # any budget (#1099, mirrors #1096 for Transfermarkt).
                logger.warning(
                    "FBref lease %s closed with uncertain provider "
                    "accounting; filter retains the unproven tail as durable "
                    "escrow",
                    lease.lease_id,
                )
                return stats
            if self._monotonic() >= deadline:
                raise FBrefProxyLeaseError(
                    "FBref paid lease close did not return final counters"
                )
            self._sleep(0.05)

    def close_strict(
        self,
        lease: FBrefProxyLease,
        *,
        expected: Mapping[str, Any],
    ) -> FBrefLeaseStats:
        """Close once and accept only complete, exact, zero-lifecycle proof."""

        _, body = self._request(
            "DELETE",
            f"/v1/leases/{lease.lease_id}/close",
            lease=lease,
            payload={"completed": True},
        )
        stats = self._stats_from_mapping(lease, body, expected=expected)
        if (
            not stats.closed
            or not stats.close_complete
            or stats.active_tunnels != 0
            or stats.active_provider_readers != 0
            or stats.reserved_bytes != 0
            or stats.provider_reserved_bytes != 0
            or stats.pending_client_hellos != 0
            or stats.staged_client_bytes != 0
            or stats.expired
            or stats.budget_exceeded
            or stats.accounting_uncertain
        ):
            raise FBrefProxyLeaseError(
                "FBref paid lease strict close returned incomplete counters"
            )
        return stats


__all__ = [
    "DEFAULT_LEASE_TTL_SECONDS",
    "FBREF_DAG_IDS",
    "FBrefLeaseStats",
    "FBrefProxyLease",
    "FBrefProxyLeaseClient",
    "FBrefProxyLeaseError",
    "METER_ID",
]
