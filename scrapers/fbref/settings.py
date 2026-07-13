"""Import-light shared limits for the FBref control and Airflow interfaces."""

MIB = 1024 * 1024
DEFAULT_REQUEST_LIMIT = 200
DEFAULT_BYTE_LIMIT = 100 * MIB
DEFAULT_SHARD_SIZE = 8
# One target can consume the complete 4 MiB browser clearance budget, the
# cumulative 2 MiB HTTP body cap, and a conservative 1 MiB HTTP wire envelope
# for request/response headers across both bounded HTTP attempts.
DEFAULT_BROWSER_BYTE_LIMIT_BYTES = 4 * MIB
DEFAULT_HTTP_BODY_LIMIT_BYTES = 2 * MIB
DEFAULT_HTTP_WIRE_OVERHEAD_RESERVATION_BYTES = MIB
DEFAULT_REQUEST_RESERVATION_BYTES = (
    DEFAULT_BROWSER_BYTE_LIMIT_BYTES
    + DEFAULT_HTTP_BODY_LIMIT_BYTES
    + DEFAULT_HTTP_WIRE_OVERHEAD_RESERVATION_BYTES
)
DEFAULT_DOMAIN_INTERVAL_SECONDS = 3.0
# One Turnstile solve costs 19 requests (measured in production). The bootstrap
# reservation is what the browser is allowed to spend in total, so it also
# decides how many times a bad exit IP may be retried: a run that reserves one
# solve gets exactly one attempt, and a wave whose first proxy stalls fails.
DEFAULT_BROWSER_REQUESTS_PER_SOLVE = 20
# A clearance may be re-solved on a fresh proxy when an exit IP stalls; the
# transport bounds that at four attempts (MAX_PROXY_ROTATIONS + 1).
MAX_CLEARANCE_SOLVE_ATTEMPTS = 4
DEFAULT_BOOTSTRAP_REQUEST_RESERVATION = DEFAULT_BROWSER_REQUESTS_PER_SOLVE
# A daily ingest run can afford to solve again on a fresh proxy; its 200-request
# budget covers four attempts, and the reservation is released on settlement, so
# a healthy bootstrap still only bills ~19.
INGEST_BOOTSTRAP_REQUEST_RESERVATION = (
    DEFAULT_BROWSER_REQUESTS_PER_SOLVE * MAX_CLEARANCE_SOLVE_ATTEMPTS
)


def bootstrap_reservation_for(request_limit: int) -> int:
    """How many requests the browser may spend on one clearance, for this run.

    Derived from the run's own budget so that every caller agrees — the fetch
    wave runs in a subprocess that rebuilds its settings from the command line,
    so a reservation computed only in the DAG never reached the browser at all.
    A run must be able to pay for its clearance twice over before it is allowed
    to retry a stalled exit IP; the 25-request backfill therefore gets exactly
    one solve, and the 200-request daily run gets four.
    """
    return (
        INGEST_BOOTSTRAP_REQUEST_RESERVATION
        if int(request_limit) >= INGEST_BOOTSTRAP_REQUEST_RESERVATION * 2
        else DEFAULT_BOOTSTRAP_REQUEST_RESERVATION
    )
MAX_SHARD_SIZE = 25


__all__ = [
    "DEFAULT_BOOTSTRAP_REQUEST_RESERVATION",
    "DEFAULT_BROWSER_REQUESTS_PER_SOLVE",
    "INGEST_BOOTSTRAP_REQUEST_RESERVATION",
    "MAX_CLEARANCE_SOLVE_ATTEMPTS",
    "bootstrap_reservation_for",
    "DEFAULT_BROWSER_BYTE_LIMIT_BYTES",
    "DEFAULT_BYTE_LIMIT",
    "DEFAULT_DOMAIN_INTERVAL_SECONDS",
    "DEFAULT_HTTP_BODY_LIMIT_BYTES",
    "DEFAULT_HTTP_WIRE_OVERHEAD_RESERVATION_BYTES",
    "DEFAULT_REQUEST_LIMIT",
    "DEFAULT_REQUEST_RESERVATION_BYTES",
    "DEFAULT_SHARD_SIZE",
    "MAX_SHARD_SIZE",
    "MIB",
]
