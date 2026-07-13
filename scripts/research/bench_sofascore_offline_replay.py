#!/usr/bin/env python3
"""Network-disabled fixed-cohort benchmark for the production capture engine.

This is not a scraper. It seeds exact fixture JSON into the canonical raw store,
then measures offline replay and terminal-manifest no-op for the same 25-match /
50-player cohort required by the provider canary. Results are benchmark-only
and can never authorize a paid proxy budget.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "dags"))
sys.path.insert(0, str(ROOT))

from pyarrow import fs  # noqa: E402

from scrapers.sofascore.capture_engine import (  # noqa: E402
    RetryPolicy,
    SofaScoreCaptureEngine,
)
from scrapers.sofascore.manifest import InMemoryManifestStore  # noqa: E402
from scrapers.sofascore.pipeline import (  # noqa: E402
    EVENT_PATHS,
    PLAYER_PATHS,
    build_event_spec,
    build_player_spec,
)
from scrapers.sofascore.raw_store import RawPayloadStore  # noqa: E402


FIXTURES = ROOT / "tests" / "fixtures"
SOURCE_TOURNAMENT_ID = 17
SOURCE_SEASON_ID = 76986


class _NoNetwork:
    def request(self, url, *, provider_budget):
        raise AssertionError(f"benchmark attempted network access: {url}")


class _SuccessSink:
    def write(self, key, datasets, raw):
        return None


class _Unlimited:
    def acquire(self):
        return True


def _event_payload(endpoint: str, target_id: str) -> bytes:
    suffix = "" if endpoint == "event" else f"_{endpoint}"
    path = FIXTURES / f"sofascore_event_14023925{suffix}.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if endpoint == "event":
        payload["event"]["id"] = int(target_id)
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _player_payload(endpoint: str, target_id: str) -> bytes:
    suffix = "" if endpoint == "player_profile" else "_season_statistics"
    path = FIXTURES / f"sofascore_player_11111{suffix}.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if endpoint == "player_profile":
        payload["player"]["id"] = int(target_id)
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _engine(raw_store, manifest_store):
    return SofaScoreCaptureEngine(
        raw_store=raw_store,
        manifest_store=manifest_store,
        transport=_NoNetwork(),
        run_id="offline-fixed-cohort",
        task_id="benchmark",
        sink=_SuccessSink(),
        rate_limiter=_Unlimited(),
        retry_policy=RetryPolicy(max_attempts=1),
        max_workers=4,
    )


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="sofascore-replay-") as directory:
        raw_store = RawPayloadStore(fs.LocalFileSystem(), f"{directory}/raw")
        manifest_store = InMemoryManifestStore()
        specs = []
        for match_id in range(20_000_001, 20_000_026):
            for endpoint in EVENT_PATHS:
                spec = build_event_spec(
                    source_tournament_id=SOURCE_TOURNAMENT_ID,
                    source_season_id=SOURCE_SEASON_ID,
                    target_id=match_id,
                    endpoint=endpoint,
                    freshness_key="benchmark-final",
                    paid_proxy=False,
                )
                raw_store.store_bytes(
                    spec.raw_target,
                    _event_payload(endpoint, str(match_id)),
                    request_url=spec.url,
                    http_status=200,
                    response_headers={"content-type": "application/json"},
                )
                specs.append(spec)
        for player_id in range(30_000_001, 30_000_051):
            for endpoint in PLAYER_PATHS:
                spec = build_player_spec(
                    source_tournament_id=SOURCE_TOURNAMENT_ID,
                    source_season_id=SOURCE_SEASON_ID,
                    target_id=player_id,
                    endpoint=endpoint,
                    freshness_key="benchmark-week",
                    paid_proxy=False,
                )
                raw_store.store_bytes(
                    spec.raw_target,
                    _player_payload(endpoint, str(player_id)),
                    request_url=spec.url,
                    http_status=200,
                    response_headers={"content-type": "application/json"},
                )
                specs.append(spec)

        replay = _engine(raw_store, manifest_store)
        replay.capture_many(specs, offline=True, force_replay=True)
        no_op = _engine(raw_store, manifest_store)
        no_op.capture_many(specs)
        print(
            json.dumps(
                {
                    "budget_eligible": False,
                    "cohort": "25_matches_50_players",
                    "offline_replay": replay.metrics.snapshot(),
                    "no_op": no_op.metrics.snapshot(),
                },
                sort_keys=True,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
