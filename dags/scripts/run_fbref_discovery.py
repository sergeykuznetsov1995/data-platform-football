#!/usr/bin/env python3
"""Run a deliberately bounded FBref discovery graph.

This runner is separate from ``run_fbref_scraper.py`` so its safe defaults can
never fall through to the legacy ``full`` scrape.  It discovers match targets
but never downloads match pages.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import logging
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional, Sequence

# Keep the manual CLI genuinely standalone outside the Airflow image, whose
# environment normally injects /opt/airflow into PYTHONPATH.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.fbref.discovery_service import (  # noqa: E402
    DiscoveryRunResult,
    FBrefDiscoveryService,
)
from scrapers.fbref.discovery_queue import (  # noqa: E402
    MAX_QUEUE_ATTEMPTS,
    MAX_QUEUE_COMPETITIONS_PER_RUN,
    MAX_QUEUE_NETWORK_PAGES,
    MAX_QUEUE_SEASONS_PER_COMPETITION,
    FBrefDiscoveryQueue,
)
from scrapers.fbref.raw_store import RawPageStore  # noqa: E402


logger = logging.getLogger(__name__)

MAX_BATCH_COMPETITIONS = MAX_QUEUE_COMPETITIONS_PER_RUN
MAX_BATCH_SEASONS_PER_COMPETITION = MAX_QUEUE_SEASONS_PER_COMPETITION
MAX_BATCH_NETWORK_PAGES = MAX_QUEUE_NETWORK_PAGES
MAX_BATCH_ATTEMPTS = MAX_QUEUE_ATTEMPTS


class LazyFBrefLoader:
    """Create the heavy Camoufox scraper only when raw storage misses."""

    def __init__(self, *, proxy_file: Optional[str], raw_store_uri: str) -> None:
        self.proxy_file = proxy_file
        self.raw_store_uri = raw_store_uri
        self.scraper = None

    def __call__(self, url: str, page_kind: str) -> Optional[str]:
        if self.scraper is None:
            from scrapers.fbref import FBrefScraper

            self.scraper = FBrefScraper(
                leagues=[],
                seasons=[],
                headless=True,
                use_xvfb=True,
                proxy_file=self.proxy_file,
                use_nodriver=True,
                raw_store_uri=self.raw_store_uri,
            )
        return self.scraper._fetch_page(
            url,
            use_cache=False,
            page_type=page_kind,
        )

    def close(self) -> None:
        if self.scraper is not None:
            self.scraper.close()

    def diagnostics(self) -> dict:
        if self.scraper is None:
            return {"transport_created": False, "proxy_requests": 0}
        stats = self.scraper.get_stats()
        return {
            "transport_created": True,
            "successes": stats.get("successes", 0),
            "failures": stats.get("failures", 0),
            "proxy_bytes": (
                int(stats.get("real_bytes_downloaded", 0) or 0)
                + int(stats.get("http_bytes_downloaded", 0) or 0)
            ),
            "proxy_requests": (
                int(stats.get("real_requests_count", 0) or 0)
                + int(stats.get("http_requests_count", 0) or 0)
            ),
        }


def _positive(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _bounded_positive(name: str, maximum: int):
    def parse(value: str) -> int:
        parsed = _positive(value)
        if parsed > maximum:
            raise argparse.ArgumentTypeError(
                f"{name} must be at most {maximum}"
            )
        return parsed

    return parse


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def _queue_process_lock(
    raw_store_uri: str,
    queue_id: str,
) -> Iterator[None]:
    """Prevent two local workers from running the same durable queue."""
    lock_root = Path(os.environ.get(
        "FBREF_DISCOVERY_LOCK_DIR",
        "/tmp/fbref-discovery-locks",
    ))
    lock_root.mkdir(parents=True, exist_ok=True)
    identity = (
        f"{str(raw_store_uri).rstrip('/')}\0{str(queue_id).strip()}"
    ).encode("utf-8")
    lock_path = lock_root / f"{hashlib.sha256(identity).hexdigest()}.lock"
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                f"Discovery queue {queue_id!r} is already running"
            ) from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()}\n")
        handle.flush()
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bounded raw-first FBref competition discovery"
    )
    parser.add_argument(
        "--raw-store-uri",
        default=os.environ.get("FBREF_RAW_STORE_URI"),
        help="Required durable raw-store URI (or FBREF_RAW_STORE_URI)",
    )
    parser.add_argument(
        "--output",
        default="/tmp/fbref_discovery.json",
        help="JSON result path",
    )
    parser.add_argument(
        "--proxy-file",
        default=None,
        help="Residential proxy file for a network run",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Read committed raw pages only; never construct a transport",
    )
    parser.add_argument("--verbose", action="store_true")

    commands = parser.add_subparsers(dest="command", required=True)
    index = commands.add_parser(
        "index",
        help="Fetch/parse only /en/comps/; never follow competition links",
    )
    index.add_argument(
        "--max-network-pages",
        type=_positive,
        default=1,
        help="Hard network miss budget (default: 1)",
    )

    discover = commands.add_parser(
        "discover",
        help="Follow explicitly selected competition ids to match targets",
    )
    discover.add_argument(
        "--competition-id",
        action="append",
        required=True,
        help="Source-native id from the index; repeat for multiple ids",
    )
    discover.add_argument(
        "--season-label",
        action="append",
        help="Exact source season label; otherwise use the newest rows",
    )
    discover.add_argument(
        "--max-competitions",
        type=_positive,
        default=1,
    )
    discover.add_argument(
        "--max-seasons-per-competition",
        type=_positive,
        default=1,
    )
    discover.add_argument(
        "--max-network-pages",
        type=_positive,
        default=4,
        help="Hard network miss budget (default: 4)",
    )

    batch = commands.add_parser(
        "discover-batch",
        help=(
            "Process one durable slice of every competition found in "
            "/en/comps/; never fetch match pages"
        ),
        description=(
            "Process one durable slice of every competition found in "
            "/en/comps/. Run repeatedly with the same queue id. Match pages "
            "are never fetched. Set FBREF_DISCOVERY_LOCK_DIR to shared "
            "storage when launchers run on different hosts."
        ),
    )
    batch.add_argument(
        "--all-discovered-competitions",
        action="store_true",
        required=True,
        help="Required explicit opt-in to seed the queue from /en/comps/",
    )
    batch.add_argument(
        "--queue-id",
        required=True,
        help="Stable name reused by later invocations of the same queue",
    )
    batch.add_argument(
        "--max-competitions",
        type=_bounded_positive(
            "max competitions",
            MAX_BATCH_COMPETITIONS,
        ),
        default=1,
        help=f"Items attempted in this run (default: 1, cap: {MAX_BATCH_COMPETITIONS})",
    )
    batch.add_argument(
        "--max-seasons-per-competition",
        type=_bounded_positive(
            "max seasons per competition",
            MAX_BATCH_SEASONS_PER_COMPETITION,
        ),
        default=1,
        help=(
            "Newest source seasons per competition "
            f"(default: 1, cap: {MAX_BATCH_SEASONS_PER_COMPETITION})"
        ),
    )
    batch.add_argument(
        "--max-attempts",
        type=_bounded_positive("max attempts", MAX_BATCH_ATTEMPTS),
        default=3,
        help=f"Attempts before an item is terminal (default: 3, cap: {MAX_BATCH_ATTEMPTS})",
    )
    batch.add_argument(
        "--max-network-pages",
        type=_bounded_positive(
            "max network pages",
            MAX_BATCH_NETWORK_PAGES,
        ),
        default=4,
        help=f"Hard network miss budget (default: 4, cap: {MAX_BATCH_NETWORK_PAGES})",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    if not args.raw_store_uri:
        parser.error("--raw-store-uri or FBREF_RAW_STORE_URI is required")

    transport = os.environ.get("FBREF_TRANSPORT", "camoufox").strip().lower()
    if not args.offline and transport != "camoufox":
        parser.error(
            "Network discovery requires FBREF_TRANSPORT=camoufox; "
            f"got {transport!r}"
        )
    os.environ.setdefault("FBREF_TRANSPORT", "camoufox")

    raw_store = RawPageStore.from_uri(args.raw_store_uri)
    loader = None if args.offline else LazyFBrefLoader(
        proxy_file=args.proxy_file,
        raw_store_uri=args.raw_store_uri,
    )
    service = FBrefDiscoveryService(
        raw_store,
        loader=loader,
        offline=args.offline,
        max_network_pages=args.max_network_pages,
    )

    queue_payload = None
    try:
        try:
            if args.command == "index":
                result = service.discover_index()
            elif args.command == "discover":
                result = service.discover_graph(
                    args.competition_id,
                    max_competitions=args.max_competitions,
                    max_seasons_per_competition=(
                        args.max_seasons_per_competition
                    ),
                    season_labels=args.season_label,
                )
            else:
                with _queue_process_lock(args.raw_store_uri, args.queue_id):
                    queue_run = FBrefDiscoveryQueue(raw_store, service).run(
                        args.queue_id,
                        max_competitions=args.max_competitions,
                        max_seasons_per_competition=(
                            args.max_seasons_per_competition
                        ),
                        max_attempts=args.max_attempts,
                    )
                result = queue_run.result
                queue_payload = queue_run.queue
        except Exception as exc:
            result = DiscoveryRunResult(
                mode=args.command,
                offline=args.offline,
                errors=[{
                    "target_id": "fbref:discovery-run",
                    "page_kind": "discovery",
                    "dataset": "discovery",
                    "reason": "run_failed",
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                }],
            )
        payload = result.to_dict()
        payload["limits"] = {
            "max_network_pages": args.max_network_pages,
            "max_competitions": getattr(args, "max_competitions", None),
            "max_seasons_per_competition": getattr(
                args, "max_seasons_per_competition", None
            ),
        }
        if hasattr(args, "max_attempts"):
            payload["limits"]["max_attempts"] = args.max_attempts
        if queue_payload is not None:
            payload["queue"] = queue_payload
        payload["transport"] = (
            loader.diagnostics()
            if loader is not None
            else {"transport_created": False, "proxy_requests": 0}
        )
    finally:
        if loader is not None:
            loader.close()

    output = Path(args.output)
    _write_json_atomic(output, payload)
    logger.info(
        "FBref discovery complete: competitions=%d seasons=%d matches=%d "
        "raw_hits=%d raw_writes=%d network_pages=%d errors=%d",
        len(payload["competitions"]),
        len(payload["seasons"]),
        len(payload["matches"]),
        payload["raw"]["hits"],
        payload["raw"]["writes"],
        payload["raw"]["network_pages"],
        len(payload["errors"]),
    )
    if queue_payload is not None:
        return 0 if queue_payload.get("status") in {
            "progress",
            "paused",
            "complete",
        } else 1
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
