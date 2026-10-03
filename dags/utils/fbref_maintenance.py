"""FBref-only lock waiting around the shared, sealed maintenance implementation."""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

FBREF_JANITOR_LOCK_WAIT_SECONDS = 90 * 60
FBREF_JANITOR_LOCK_POLL_SECONDS = 30


def maintain_fbref_stages_with_lock_wait(*, mode: str | None = None) -> dict[str, Any]:
    """Wait for the publisher, then let the janitor acquire and fence its lock.

    The read-only probe avoids creating failed maintenance runs while ingest is
    active. It is not an ownership check: the janitor's atomic acquire remains
    authoritative, including when another publisher wins the race after a probe.
    """

    from scrapers.fbref.control import ControlStore, StateConflict
    from utils.maintenance_tasks import maintain_fbref_generic_stages

    control = ControlStore.from_env()
    deadline = time.monotonic() + FBREF_JANITOR_LOCK_WAIT_SECONDS
    while True:
        lock = control.get_publication_lock()
        if not lock or not lock["active"]:
            try:
                return maintain_fbref_generic_stages(mode=mode)
            except StateConflict as exc:
                # The sealed ControlStore has no dedicated busy exception.
                # Retry only its active foreign-owner conflict. Invalid/expired
                # generations and other integrity errors fail immediately.
                if str(exc) != "FBref publication is locked by another control run":
                    raise
        remaining = deadline - time.monotonic()
        if remaining > 0:
            logger.info(
                "Waiting for FBref publication lock: remaining=%.0fs", remaining
            )
            time.sleep(min(FBREF_JANITOR_LOCK_POLL_SECONDS, remaining))
            if time.monotonic() < deadline:
                continue
        raise TimeoutError(
            "Timed out waiting for FBref publication lock after "
            f"{FBREF_JANITOR_LOCK_WAIT_SECONDS}s"
        )
