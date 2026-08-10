"""Import-light shared limits for the FBref control and Airflow interfaces."""

from __future__ import annotations

import os
from typing import Mapping

MIB = 1024 * 1024
FBREF_PRODUCTION_SAFETY_REQUEST_LIMIT = 4096
FBREF_PRODUCTION_SAFETY_BYTE_LIMIT_MIB = 2048
# Compatibility aliases: persisted control schemas and older callers still use
# these names.  The values are emergency runaway circuits, not tariff budgets.
DEFAULT_REQUEST_LIMIT = FBREF_PRODUCTION_SAFETY_REQUEST_LIMIT
DEFAULT_BYTE_LIMIT = FBREF_PRODUCTION_SAFETY_BYTE_LIMIT_MIB * MIB
DEFAULT_SHARD_SIZE = 8
# One clearance solve may consume 4 MiB.  Camoufox can rotate across four
# exits, so the browser cap and the control reservation must scale together.
DEFAULT_BROWSER_BYTE_LIMIT_BYTES = 4 * MIB
DEFAULT_HTTP_BODY_LIMIT_BYTES = 2 * MIB
DEFAULT_HTTP_WIRE_OVERHEAD_RESERVATION_BYTES = MIB
# Compatibility name used by DAG parameters and older callers.  It now means
# one warm target reservation only; bootstrap traffic has its own reservation.
DEFAULT_REQUEST_RESERVATION_BYTES = (
    DEFAULT_HTTP_BODY_LIMIT_BYTES
    + DEFAULT_HTTP_WIRE_OVERHEAD_RESERVATION_BYTES
)
# Sports Reference publishes a hard FBref limit of ten page requests per
# minute. Keep a small scheduling margin instead of sitting exactly on the
# six-second boundary.
MIN_DOMAIN_INTERVAL_SECONDS = 6.0
DEFAULT_DOMAIN_INTERVAL_SECONDS = 6.1
# One Turnstile solve costs 19 requests (measured in production). The bootstrap
# reservation is what the browser is allowed to spend in total, so it also
# decides how many times a bad exit IP may be retried: a run that reserves one
# solve gets exactly one attempt, and a wave whose first proxy stalls fails.
DEFAULT_BROWSER_REQUESTS_PER_SOLVE = 20
# A clearance may be re-solved on a fresh proxy when an exit IP stalls; the
# transport bounds that at four attempts (MAX_PROXY_ROTATIONS + 1).
MAX_CLEARANCE_SOLVE_ATTEMPTS = 4
DEFAULT_BOOTSTRAP_REQUEST_RESERVATION = DEFAULT_BROWSER_REQUESTS_PER_SOLVE
# A production run may solve again on a fresh proxy.  Four attempts remain the
# deliberate transport bound even though the larger circuit leaves ample room.
INGEST_BOOTSTRAP_REQUEST_RESERVATION = (
    DEFAULT_BROWSER_REQUESTS_PER_SOLVE * MAX_CLEARANCE_SOLVE_ATTEMPTS
)


def bootstrap_reservation_for(request_limit: int) -> int:
    """How many requests the browser may spend on one clearance, for this run.

    Derived from the run's own budget so that every caller agrees — the fetch
    wave runs in a subprocess that rebuilds its settings from the command line,
    so a reservation computed only in the DAG never reached the browser at all.
    A run must be able to pay for its clearance twice over before it is allowed
    to retry a stalled exit IP; a small canary gets one solve, while the
    production safety profile gets four.
    """
    return (
        INGEST_BOOTSTRAP_REQUEST_RESERVATION
        if int(request_limit) >= INGEST_BOOTSTRAP_REQUEST_RESERVATION * 2
        else DEFAULT_BOOTSTRAP_REQUEST_RESERVATION
    )


def bootstrap_byte_reservation_for(request_limit: int) -> int:
    """Byte ceiling matching every clearance solve the request cap permits."""

    solve_attempts = max(
        1,
        bootstrap_reservation_for(request_limit)
        // DEFAULT_BROWSER_REQUESTS_PER_SOLVE,
    )
    return DEFAULT_BROWSER_BYTE_LIMIT_BYTES * solve_attempts


MAX_SHARD_SIZE = 25


def strict_binary_flag(
    name: str,
    *,
    environ: Mapping[str, str] | None = None,
    default: str = "0",
) -> bool:
    """Read one deployment switch without accepting ambiguous spellings."""

    values = os.environ if environ is None else environ
    raw = values.get(name, default)
    if raw not in {"0", "1"}:
        raise ValueError(f"{name} must be exactly 0 or 1")
    return raw == "1"


FBREF_PERSISTENT_HTTP_SESSION = strict_binary_flag(
    "FBREF_PERSISTENT_HTTP_SESSION"
)


__all__ = [
    "DEFAULT_BOOTSTRAP_REQUEST_RESERVATION",
    "DEFAULT_BROWSER_REQUESTS_PER_SOLVE",
    "INGEST_BOOTSTRAP_REQUEST_RESERVATION",
    "MAX_CLEARANCE_SOLVE_ATTEMPTS",
    "bootstrap_byte_reservation_for",
    "bootstrap_reservation_for",
    "DEFAULT_BROWSER_BYTE_LIMIT_BYTES",
    "DEFAULT_BYTE_LIMIT",
    "FBREF_PRODUCTION_SAFETY_BYTE_LIMIT_MIB",
    "FBREF_PRODUCTION_SAFETY_REQUEST_LIMIT",
    "FBREF_PERSISTENT_HTTP_SESSION",
    "DEFAULT_DOMAIN_INTERVAL_SECONDS",
    "DEFAULT_HTTP_BODY_LIMIT_BYTES",
    "DEFAULT_HTTP_WIRE_OVERHEAD_RESERVATION_BYTES",
    "DEFAULT_REQUEST_LIMIT",
    "DEFAULT_REQUEST_RESERVATION_BYTES",
    "DEFAULT_SHARD_SIZE",
    "MAX_SHARD_SIZE",
    "MIN_DOMAIN_INTERVAL_SECONDS",
    "MIB",
    "strict_binary_flag",
]
