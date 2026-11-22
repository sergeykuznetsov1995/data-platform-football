"""Read-only coverage of the exact ClubElo history queue (#2280).

The denominator is the number of slugs on the latest /Ranking page for the
manifest's maximum rating_date. Only latest manifest statuses for those slugs
contribute to coverage. There is no fixed denominator. Each slug belongs to
one status bucket, and only done and no_page count as covered. Nothing is written.
"""
from __future__ import annotations

import base64
import gzip
import re
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from scrapers.clubelo.parse import LayoutChanged, parse_ranking_slugs  # noqa: E402, F401

MANIFEST_TABLE = "iceberg.bronze.clubelo_history_manifest"
RAW_PAGE_TABLE = "iceberg.bronze.clubelo_raw_page"
KNOWN_STATUSES = frozenset({"done", "no_page", "failed"})
_SHA256_HEX = re.compile(r"[0-9a-f]{64}")
BUCKETS = ("done", "no_page", "failed", "pending", "unknown")
LATEST_MANIFEST_SQL = (
    "SELECT slug, status FROM (SELECT slug, status, row_number() OVER "
    "(PARTITION BY slug ORDER BY _ingested_at DESC, fetched_at DESC) AS rn "
    f"FROM {MANIFEST_TABLE}) WHERE rn = 1"
)


def analyze_coverage(
    anchor_date: date,
    queue_slugs: Iterable[str],
    manifest_latest: Iterable[Tuple[str, object]],
) -> dict:
    """Count each queue slug once. A missing row is pending, an unexpected status unknown."""
    queue = list(queue_slugs)
    if len(set(queue)) != len(queue):
        raise ValueError("duplicate slug in /Ranking queue")

    latest = dict(manifest_latest)
    counts = dict.fromkeys(BUCKETS, 0)
    for slug in queue:
        if slug not in latest:
            counts["pending"] += 1
        else:
            status = latest[slug]
            bucket = status if isinstance(status, str) and status in KNOWN_STATUSES else "unknown"
            counts[bucket] += 1

    if sum(counts.values()) != len(queue):
        raise ValueError("status counts do not sum to the queue size")
    return {
        "anchor_date": anchor_date,
        "queue": len(queue),
        **counts,
        "covered": counts["done"] + counts["no_page"],
    }


def _unavailable(reason: str) -> dict:
    """Return the read failure reason with zero counts in every bucket."""
    return {"status": "unavailable", "reason": reason, **dict.fromkeys(BUCKETS, 0)}


def read_report(trino, *, today: date) -> dict:
    """Resolve the dated queue before reading latest statuses for its coverage report."""
    try:
        anchor_rows = trino.execute_query(
            f"SELECT cast(max(rating_date) AS varchar) FROM {MANIFEST_TABLE}"
        )
        anchor = anchor_rows[0][0] if anchor_rows else None
        if anchor is None:
            return _unavailable("манифест истории пуст")
        anchor_date = anchor if isinstance(anchor, date) else date.fromisoformat(anchor)

        try:
            page_rows = trino.execute_query(
                f"SELECT to_base64(body), sha256 FROM {RAW_PAGE_TABLE} "
                f"WHERE page = '/Ranking' AND rating_date = DATE '{anchor_date.isoformat()}' "
                "ORDER BY fetched_at DESC LIMIT 1"
            )
            if not page_rows:
                return _unavailable("страница очереди /Ranking не найдена")
            body, sha256 = page_rows[0]
            # The digest is the queue identity the summary prints: anything but
            # a real sha256 hex fails closed instead of showing "sha x" (Astra r1).
            if not isinstance(sha256, str) or not _SHA256_HEX.fullmatch(sha256):
                raise ValueError("queue page digest")
            compressed = base64.b64decode(body, validate=True)
            page_date, slugs = parse_ranking_slugs(gzip.decompress(compressed).decode("utf-8"))
        except Exception as exc:
            return _unavailable(f"очередь не читается: {type(exc).__name__}")
        if page_date != anchor_date:
            return _unavailable("дата страницы очереди не совпадает с манифестом")

        statuses = trino.execute_query(LATEST_MANIFEST_SQL)
        if any(not isinstance(row, (tuple, list)) or len(row) != 2 for row in statuses):
            return _unavailable("не разобран ответ манифеста")
        return {
            "status": "ok",
            "queue_sha256": sha256,
            "queue_age_days": (today - anchor_date).days,
            **analyze_coverage(anchor_date, slugs, statuses),
        }
    except Exception:
        return _unavailable("ошибка чтения Bronze")


def format_line(report: dict) -> str:
    """Format the queue identity and its five distinct counts on one line."""
    if report["status"] != "ok":
        return f"история: не посчитана — {report['reason']}"
    counts = ", ".join(f"{bucket} {report[bucket]}" for bucket in BUCKETS)
    return (
        f"история: очередь {report['queue']} "
        f"(Ranking {report['anchor_date']:%d.%m}, sha {report['queue_sha256'][:8]}): {counts}"
    )


TRINO_CLI_TIMEOUT_S = 240  # per query, as the morning summary's Trino helpers


class CliTrino:
    """Read-only Trino through the platform ``trino-ro.sh`` wrapper (#2280).

    The morning summary's cron python has no lxml and never imports this
    module; it runs ``main()`` with the platform test venv python (the same
    interpreter ``trino-ro.sh`` uses) and reads the two printed lines back.
    Rows come back exactly as ``_trino_rows`` in the summary parses them:
    quoted CSV fields, one row per line, NULL as an empty field.
    """

    def __init__(self, trino_sh: str) -> None:
        self._trino_sh = trino_sh

    def execute_query(self, sql: str) -> List[Tuple[Optional[str], ...]]:
        result = subprocess.run(
            [self._trino_sh, sql], capture_output=True, text=True,
            timeout=TRINO_CLI_TIMEOUT_S,
        )
        if result.returncode != 0:
            raise RuntimeError("trino")
        rows: List[Tuple[Optional[str], ...]] = []
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            cells = [cell.strip('"') for cell in line.split('","')]
            rows.append(tuple(cell if cell != "" else None for cell in cells))
        return rows


def main(argv: Optional[List[str]] = None) -> int:
    """Watchdog CLI: print ``format_line``, then the JSON report on the next line."""
    import argparse
    import json

    parser = argparse.ArgumentParser(description="ClubElo history queue coverage (#2280)")
    parser.add_argument("--trino", default="/root/.claude/bin/trino-ro.sh",
                        help="read-only Trino CLI wrapper")
    parser.add_argument("--today", default=datetime.now(timezone.utc).date().isoformat(),
                        help="ISO date of the report day (UTC)")
    args = parser.parse_args(argv)
    try:
        today = date.fromisoformat(args.today)
    except ValueError:
        today = None
    report = (read_report(CliTrino(args.trino), today=today)
              if today is not None
              else _unavailable(f"некорректная дата --today: {args.today}"))
    print(format_line(report))
    print(json.dumps(report, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
