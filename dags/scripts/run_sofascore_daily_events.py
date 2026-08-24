#!/usr/bin/env python3
"""Fetch the source daily event lists into ``bronze.sofascore_schedule``.

Refresh lane (#1218, lane F): the football events of yesterday and today
(UTC) are fetched through the metered discovery client, kept in the raw
store, reduced to schedule rows of ready campaign tournaments (minus the
configured leagues the daily ingest owns) and MERGEd into Bronze under the
writer lock.  The JSON report carries the row counters and the discovery
client's byte accounting; any failure exits 1.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "dags") not in sys.path:
    sys.path.insert(0, str(ROOT / "dags"))

from scrapers.sofascore.catalog import SofaScoreCatalog  # noqa: E402
from scrapers.sofascore.daily_events import (  # noqa: E402
    fetch_daily_events,
    schedule_rows_from_events,
)
from scrapers.sofascore.discovery import (  # noqa: E402
    DISCOVERY_LEASE_MAX_BYTES,
    DISCOVERY_LEASE_TTL_SECONDS,
    LeaseBrowserSofaScoreClient,
)
from scrapers.sofascore.raw_store import RawPayloadStore  # noqa: E402

DAG_ID = "dag_refresh_sofascore_all_mens"
TASK_ID = "fetch_daily_events"
DEFAULT_BUDGET_CAP_BYTES = 16 * 1024 * 1024


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode()
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _default_dates(today: Optional[date] = None) -> list[date]:
    """Yesterday and today in UTC: the lists overlap across time zones."""

    today = today or datetime.now(timezone.utc).date()
    return [today - timedelta(days=1), today]


def _configured_tournament_ids() -> frozenset[int]:
    """Source ids of the leagues ``dag_ingest_sofascore`` already covers."""

    return frozenset(SofaScoreCatalog.load().tournament_map(enabled_only=True).values())


def write_schedule_rows(rows: list[dict]) -> str:
    """MERGE the rows into ``bronze.sofascore_schedule`` under the writer lock."""

    import pandas as pd

    from scrapers.sofascore.scraper import SofaScoreScraper
    from scrapers.sofascore.writer_lock import bronze_writer_lock

    with SofaScoreScraper() as scraper, bronze_writer_lock():
        frame = scraper._add_metadata(pd.DataFrame(rows), "schedule")
        return scraper.save_to_iceberg(
            df=frame,
            table_name="sofascore_schedule",
            partition_cols=["league", "season"],
            natural_keys=["league", "season", "game_id"],
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--dates", nargs="+", type=date.fromisoformat,
        help="UTC days to fetch (default: yesterday and today).",
    )
    parser.add_argument(
        "--control-url", default=os.environ.get("SOFASCORE_PROXY_CONTROL_URL", "")
    )
    parser.add_argument(
        "--budget-cap-bytes", type=int,
        default=int(
            os.environ.get("SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES", "").strip()
            or DEFAULT_BUDGET_CAP_BYTES
        ),
    )
    parser.add_argument(
        "--per-lease-max-bytes", type=int, default=DISCOVERY_LEASE_MAX_BYTES
    )
    parser.add_argument(
        "--lease-ttl-seconds", type=int, default=DISCOVERY_LEASE_TTL_SECONDS
    )
    parser.add_argument("--max-attempts", type=int, default=12)
    parser.add_argument(
        "--raw-store-uri", help="Override SOFASCORE_RAW_STORE_URI."
    )
    parser.add_argument(
        "--dag-id", default=os.environ.get("AIRFLOW_CTX_DAG_ID") or DAG_ID
    )
    parser.add_argument(
        "--run-id", default=os.environ.get("AIRFLOW_CTX_DAG_RUN_ID") or "manual"
    )
    parser.add_argument(
        "--task-id", default=os.environ.get("AIRFLOW_CTX_TASK_ID") or TASK_ID
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    dates = list(args.dates) if args.dates else _default_dates()
    report: dict[str, Any] = {
        "status": "running",
        "dates": [day.isoformat() for day in dates],
        "errors": [],
    }
    client: Optional[LeaseBrowserSofaScoreClient] = None
    try:
        if not str(args.control_url).strip():
            raise ValueError("--control-url is required")
        snapshot = json.loads(Path(args.snapshot).read_text(encoding="utf-8"))
        raw_store = (
            RawPayloadStore.from_uri(args.raw_store_uri)
            if args.raw_store_uri
            else RawPayloadStore.from_env(optional=False)
        )
        exclude = _configured_tournament_ids()
        client = LeaseBrowserSofaScoreClient(
            control_url=str(args.control_url).strip(),
            budget_cap_bytes=args.budget_cap_bytes,
            per_lease_max_bytes=args.per_lease_max_bytes,
            lease_ttl_seconds=args.lease_ttl_seconds,
            max_attempts=args.max_attempts,
            dag_id=args.dag_id,
            run_id=args.run_id,
            task_id=args.task_id,
        )
        events = fetch_daily_events(client, dates, raw_store)
        rows, counters = schedule_rows_from_events(events, snapshot, exclude)
        report.update(counters)
        report["excluded_tournaments"] = len(exclude)
        report["rows_written"] = len(rows)
        if rows:
            report["table"] = write_schedule_rows(rows)
        report["status"] = "success"
        exit_code = 0
    except Exception as exc:
        report["status"] = "failed"
        report["errors"].append(f"{type(exc).__name__}: {exc}")
        exit_code = 1
    finally:
        if client is not None:
            # Close first: the bytes of the open lease are billed to
            # paid_proxy_bytes only when the lease closes.
            try:
                client.close()
            except Exception as exc:
                report["status"] = "failed"
                report["errors"].append(f"{type(exc).__name__}: {exc}")
                exit_code = 1
            report["discovery"] = dict(client.stats)
        _atomic_json(Path(args.output), report)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main", "write_schedule_rows"]
