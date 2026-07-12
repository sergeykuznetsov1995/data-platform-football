#!/usr/bin/env python3
"""Run a non-publishing WhoScored catalog discovery benchmark.

The benchmark deliberately uses the production raw store and transport state
machine, but replaces the Trino repository with an in-memory sink.  It is safe
to use for direct-only canaries: raw responses may be cached, while no Bronze
table or manifest is changed.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Any

from scrapers.whoscored.raw_store import WhoScoredRawStore
from scrapers.whoscored.service import WhoScoredIngestService
from scrapers.whoscored.transport import TransportContext, WhoScoredTransport


@dataclass
class CapturedCatalog:
    competitions: int = 0
    seasons: int = 0
    stages: int = 0
    eligible_scopes: int = 0
    active_scopes: int = 0
    quarantined: int = 0
    discovery_batch_id: str | None = None
    raw_inputs: int = 0


class DryRunRepository:
    """Repository seam that proves discovery reached its atomic commit point."""

    def __init__(self) -> None:
        self.captured = CapturedCatalog()

    @staticmethod
    def load_discovered_catalog() -> Any:
        raise LookupError("dry-run repository has no previous snapshot")

    def persist_discovered_catalog(
        self,
        catalog: Any,
        *,
        discovery_batch_id: str,
        raw_uri: str,
        payload_sha256: str,
        raw_inputs: list[dict[str, str]],
    ) -> None:
        if not raw_uri or len(payload_sha256) != 64:
            raise RuntimeError("catalog commit lacks raw provenance")
        rows = catalog.to_rows()
        self.captured = CapturedCatalog(
            competitions=len(rows["competitions"]),
            seasons=len(rows["seasons"]),
            stages=len(rows["stages"]),
            eligible_scopes=len(catalog.eligible_scopes()),
            active_scopes=len(catalog.active_scopes()),
            quarantined=len(catalog.quarantined),
            discovery_batch_id=discovery_batch_id,
            raw_inputs=len(raw_inputs),
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--full-history",
        action="store_true",
        help="discover every season instead of only each tournament's latest season",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    repository = DryRunRepository()
    raw_store = WhoScoredRawStore.from_env(optional=False)
    transport = WhoScoredTransport(
        flaresolverr_url=os.environ.get("FLARESOLVERR_URL", "http://flaresolverr:8191"),
        paid_proxy_url=None,
        proxy_control_url=None,
        context=TransportContext.from_env().request_context(
            scope="catalog", entity="discovery_canary"
        ),
    )
    started = time.monotonic()
    try:
        result = WhoScoredIngestService.discover_catalog(
            repository=repository,
            transport=transport,
            raw_store=raw_store,
            full_history=args.full_history,
        )
    finally:
        transport.close()
    report = {
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "attempted": result.attempted,
        "succeeded": result.succeeded,
        "errors": list(result.errors),
        "counts": dict(result.counts),
        "traffic": dict(result.traffic),
        "published": asdict(repository.captured),
        "full_history": bool(args.full_history),
        "paid_route_configured": False,
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, default=str))
    return 0 if not result.errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
