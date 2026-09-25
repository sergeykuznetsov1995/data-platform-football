"""ClubElo history from the club pages ``/{slug}`` (#1462).

One run:

1. ``/Ranking`` → raw row, then the queue = slugs linked from its country
   tables (M-05, ~498 clubs).
2. Pending = queue minus slugs whose LAST manifest row is ``done`` or
   ``no_page``; a killed or failed run is resumed by the next one.
3. Pending slugs go in batches of ``batch_size``. Per batch the write order is
   raw pages → vega points → match rows → manifest rows, so a slug is closed
   only after its data is committed (a crash re-fetches it next time; rows
   written before the crash stay without a ``done`` manifest row, so readers
   take points/matches fenced by a ``done`` manifest row of the same
   ``slug`` and ``_batch_id``).
4. Every page is parsed from the stored gzip bytes (raw-first, R-54) and fails
   closed: a page that does not parse is ``failed``, nothing parsed is written.

All four tables are append-only (no replace/merge/optimize) and carry
``model_version='web-2026'`` — the new site recomputed the history, its values
must never be glued to the ``api-legacy`` archive without that mark (R-17).

The result dict is the run's report; ``exit_code`` turns any incompleteness
(failed pages, redirects, a block, pending slugs left, an error) into a
non-zero exit so the Airflow task goes red (R-01).
"""

from __future__ import annotations

import gzip
import logging
import pathlib
import sys
import time
import uuid
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Set

import pyarrow as pa

from scrapers.clubelo.parse import LayoutChanged, page_h1, parse_club_page, parse_ranking_slugs
from scrapers.clubelo.transport import (
    ClubEloBlocked,
    ClubEloFetchError,
    ClubEloTransport,
    Page,
    get_source,
)

logger = logging.getLogger(__name__)

DATABASE = "bronze"
RAW_TABLE = "clubelo_raw_page"
POINT_TABLE = "clubelo_club_elo_point"
MATCH_TABLE = "clubelo_club_match"
MANIFEST_TABLE = "clubelo_history_manifest"
MODEL_VERSION = "web-2026"
SOURCE = "clubelo_html"
CLOSED_STATUSES = frozenset({"done", "no_page"})
DEFAULT_BATCH_SIZE = 200
# A club without a page answers exactly "302 → /" (fixture lsapi-2483). Any
# other redirect is unexpected: failed, never a closing no_page (Sol r1 #2).
NO_PAGE_LOCATIONS = frozenset({"/"})
# /Ranking links 498 club pages (24–25.09.2026). A truncated but well-formed
# page must not pass for a complete queue and close the history (Sol r1 #5).
MIN_QUEUE = 400
MAX_REPORTED_FAILURES = 50
# The daily run adds at most this many new club pages per rating date (#1463).
MAX_NEW_SLUGS = 10

_META = [
    ("model_version", pa.string()),
    ("_source", pa.string()),
    ("_ingested_at", pa.timestamp("us")),
    ("_batch_id", pa.string()),
]

SCHEMAS: Dict[str, pa.Schema] = {
    RAW_TABLE: pa.schema(
        [
            ("rating_date", pa.date32()),  # h1 date of the page itself
            ("page", pa.string()),
            ("http_status", pa.int32()),
            ("fetched_at", pa.timestamp("us")),
            ("sha256", pa.string()),  # of the decoded body
            ("wire_bytes", pa.int64()),
            ("identity_bytes", pa.int64()),
            ("content_encoding", pa.string()),
            ("gzip_by", pa.string()),  # 'wire' = bytes as sent, 'us' = we gzipped
            ("body", pa.binary()),  # gzip bytes
        ]
        + _META
    ),
    POINT_TABLE: pa.schema(
        [
            ("slug", pa.string()),
            ("point_seq", pa.int32()),
            ("point_date", pa.date32()),
            ("elo", pa.float64()),
            ("golo", pa.float64()),
            ("segment_id", pa.int32()),
            ("captured_rating_date", pa.date32()),
            ("fetched_at", pa.timestamp("us")),
        ]
        + _META
    ),
    MATCH_TABLE: pa.schema(
        [
            ("slug", pa.string()),
            ("captured_rating_date", pa.date32()),
            ("row_seq", pa.int32()),
            ("match_date", pa.date32()),
            ("venue", pa.string()),
            ("opp_slug", pa.string()),
            ("opp_tlc", pa.string()),
            ("opp_name", pa.string()),
            ("opp_country", pa.string()),
            ("opp_rank", pa.int32()),
            ("prior_delta", pa.float64()),
            ("prior_delta_sigma", pa.float64()),
            ("hfa", pa.float64()),
            ("elo_pct", pa.float64()),
            ("ft", pa.string()),
            ("et", pa.string()),
            ("pen", pa.string()),
            ("game_delta", pa.float64()),
            ("game_delta_sigma", pa.float64()),
            ("post_game_delta", pa.float64()),
            ("post_game_delta_sigma", pa.float64()),
            ("elo_change", pa.float64()),
            ("new_elo", pa.int32()),
            ("new_rank", pa.int32()),
            ("has_result", pa.bool_()),
            ("fetched_at", pa.timestamp("us")),
        ]
        + _META
    ),
    MANIFEST_TABLE: pa.schema(
        [
            ("slug", pa.string()),
            ("status", pa.string()),  # done | no_page | failed
            ("http_status", pa.int32()),
            ("error", pa.string()),
            ("points", pa.int32()),
            ("first_point", pa.date32()),
            ("last_point", pa.date32()),
            ("matches", pa.int32()),
            ("fetched_at", pa.timestamp("us")),
            ("captured_rating_date", pa.date32()),
            ("rating_date", pa.date32()),  # h1 date of the run's /Ranking
            ("club_name", pa.string()),
            ("elo", pa.int32()),  # header "Elo: … (Best: …, reached on …)"
            ("elo_best", pa.int32()),
            ("elo_best_reached_on", pa.date32()),
            ("golo", pa.float64()),
        ]
        + _META
    ),
}
WRITE_ORDER = (RAW_TABLE, POINT_TABLE, MATCH_TABLE, MANIFEST_TABLE)


class IcebergHistoryStore:
    """Append-only writer of the four history tables (arrow → PyIceberg)."""

    def __init__(self, writer=None, database: str = DATABASE) -> None:
        if writer is None:
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()
        self.writer = writer
        self.database = database

    def ensure_tables(self) -> None:
        for table in WRITE_ORDER:
            self.writer.create_table_if_not_exists(self.database, table, SCHEMAS[table])

    def known_slugs(self) -> Set[str]:
        """Every slug that has any manifest row (done, no_page or failed)."""

        frame = self.writer.read_table(self.database, MANIFEST_TABLE, columns=["slug"])
        return set(frame["slug"])

    def closed_slugs(self) -> Set[str]:
        frame = self.writer.read_table(
            self.database, MANIFEST_TABLE, columns=["slug", "status", "fetched_at", "_ingested_at"]
        )
        return closed_from_manifest(frame.to_dict("records"))

    def append(self, table: str, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        # Type the rows by the table schema first: a plain DataFrame turns a
        # column that is empty in this batch (dates of no_page/failed rows)
        # into float NaN, which Arrow cannot cast to date32 (Sol r1 #1).
        frame = pa.Table.from_pylist(rows, schema=SCHEMAS[table]).to_pandas(date_as_object=True)
        self.writer.write_dataframe(
            frame, self.database, table, mode="append", add_metadata=False, bulk_arrow=True
        )


def closed_from_manifest(rows: Iterable[Dict[str, Any]]) -> Set[str]:
    """Slugs whose latest manifest row is ``done`` or ``no_page``."""

    latest: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = (row["_ingested_at"], row["fetched_at"])
        seen = latest.get(row["slug"])
        if seen is None or key >= (seen["_ingested_at"], seen["fetched_at"]):
            latest[row["slug"]] = row
    return {slug for slug, row in latest.items() if row["status"] in CLOSED_STATUSES}


def notify(message: str) -> None:
    """Telegram through the platform helper (``dags/utils/alerts.py``)."""

    dags_dir = str(pathlib.Path(__file__).resolve().parents[2] / "dags")
    if dags_dir not in sys.path:
        sys.path.append(dags_dir)

    from utils.alerts import send_telegram_message

    send_telegram_message(message, level="error")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class _Run:
    def __init__(self, transport: ClubEloTransport, store, batch_id: str) -> None:
        self.transport = transport
        self.store = store
        self.meta = {"model_version": MODEL_VERSION, "_source": SOURCE, "_batch_id": batch_id}
        self.rating_date: Optional[date] = None
        self.result: Dict[str, Any] = {
            "rating_date": None,
            "batch_id": batch_id,
            "queue": 0,
            "pending_before": 0,
            "pending_after": 0,
            "batches": 0,
            "pages_ok": 0,
            "no_page": 0,
            "pages_failed": 0,
            "redirects": 0,
            "date_mismatch": 0,
            "points": 0,
            "matches": 0,
            "wire_bytes": 0,
            "requests": 0,
            "elapsed_s": 0.0,
            "blocked": None,
            "error": None,
            "failed_slugs": [],
        }
        self.buffers: Dict[str, List[Dict[str, Any]]] = {t: [] for t in WRITE_ORDER}

    def _row(self, **values) -> Dict[str, Any]:
        return {**values, **self.meta, "_ingested_at": _utcnow()}

    def _raw_row(self, page: Page) -> Dict[str, Any]:
        try:
            page_date: Optional[date] = page_h1(page.body.decode("utf-8", "replace"))[0]
        except LayoutChanged:
            page_date = None
        return self._row(
            rating_date=page_date,
            page=page.path,
            http_status=page.status,
            fetched_at=page.fetched_at,
            sha256=page.sha256,
            wire_bytes=page.wire_bytes,
            identity_bytes=len(page.body),
            content_encoding=page.content_encoding,
            gzip_by=page.gzip_by,
            body=page.body_gz,
        )

    def flush(self) -> None:
        for table in WRITE_ORDER:
            rows, self.buffers[table] = self.buffers[table], []
            self.store.append(table, rows)

    def _manifest(self, slug: str, status: str, **values) -> None:
        row = {
            "slug": slug,
            "status": status,
            "rating_date": self.rating_date,
            "fetched_at": values.pop("fetched_at", None) or _utcnow(),
            **values,
        }
        self.buffers[MANIFEST_TABLE].append(self._row(**row))

    def _fail(self, slug: str, error: str, **values) -> None:
        self.result["pages_failed"] += 1
        if len(self.result["failed_slugs"]) < MAX_REPORTED_FAILURES:
            self.result["failed_slugs"].append(f"{slug}: {error}")
        logger.error("ClubElo history %s failed: %s", slug, error)
        self._manifest(slug, "failed", error=error[:1000], **values)

    def ranking(self) -> List[str]:
        page = self.transport.get("/Ranking")
        if page.status != 200:
            raise ClubEloFetchError(f"/Ranking: HTTP {page.status}")
        raw = self._raw_row(page)
        self.store.append(RAW_TABLE, [raw])
        self.rating_date, slugs = parse_ranking_slugs(gzip.decompress(raw["body"]).decode("utf-8"))
        if len(slugs) < MIN_QUEUE:
            raise LayoutChanged(f"/Ranking links {len(slugs)} club pages, expected >= {MIN_QUEUE}")
        self.result["rating_date"] = self.rating_date.isoformat()
        return slugs

    def club(self, slug: str) -> None:
        try:
            page = self.transport.get("/" + slug)
        except ClubEloFetchError as exc:
            self._fail(slug, str(exc))
            return
        if 300 <= page.status < 400:
            self.result["redirects"] += 1
            if page.status != 302 or page.location not in NO_PAGE_LOCATIONS:
                self._fail(slug, f"unexpected redirect HTTP {page.status} to {page.location}",
                           http_status=page.status, fetched_at=page.fetched_at)
                return
            # A club without a page answers 302 → '/': no_page, never followed.
            self.result["no_page"] += 1
            self._manifest(
                slug,
                "no_page",
                http_status=page.status,
                fetched_at=page.fetched_at,
                error=f"redirect to {page.location}",
            )
            return
        if page.status != 200:
            self._fail(slug, f"HTTP {page.status}", http_status=page.status, fetched_at=page.fetched_at)
            return
        raw = self._raw_row(page)
        self.buffers[RAW_TABLE].append(raw)
        try:
            club = parse_club_page(gzip.decompress(raw["body"]).decode("utf-8"), slug)
        except Exception as exc:  # any parse crash fails this page, not the queue
            self._fail(
                slug,
                f"{type(exc).__name__}: {exc}",
                http_status=page.status,
                fetched_at=page.fetched_at,
                captured_rating_date=raw["rating_date"],
            )
            return
        common = {"slug": slug, "captured_rating_date": club.captured_rating_date,
                  "fetched_at": page.fetched_at}
        self.buffers[POINT_TABLE].extend(self._row(**common, **p) for p in club.points)
        self.buffers[MATCH_TABLE].extend(self._row(**common, **m) for m in club.matches)
        self._manifest(
            slug,
            "done",
            http_status=page.status,
            fetched_at=page.fetched_at,
            points=len(club.points),
            first_point=club.points[0]["point_date"],
            last_point=club.points[-1]["point_date"],
            matches=len(club.matches),
            captured_rating_date=club.captured_rating_date,
            club_name=club.club_name,
            **club.header,
        )
        self.result["pages_ok"] += 1
        self.result["points"] += len(club.points)
        self.result["matches"] += len(club.matches)
        if club.captured_rating_date != self.rating_date:
            # Pages are rebuilt one by one (M-09): count, do not fail.
            self.result["date_mismatch"] += 1


def run_history(
    transport: ClubEloTransport,
    store,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    source: str = "html",
    notifier: Callable[[str], None] = notify,
    clock: Callable[[], float] = time.monotonic,
    batch_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Collect the history of every pending club; see the module docstring."""

    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    get_source(source)
    started = clock()
    run = _Run(
        transport,
        store,
        batch_id or f"clubelo-history-{_utcnow():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}",
    )
    result = run.result
    try:
        store.ensure_tables()
        queue = run.ranking()
        closed = store.closed_slugs()
        pending = [slug for slug in queue if slug not in closed]
        result["queue"] = len(queue)
        result["pending_before"] = len(pending)
        logger.info(
            "ClubElo history: rating_date=%s queue=%d pending=%d",
            result["rating_date"], len(queue), len(pending),
        )
        for start in range(0, len(pending), batch_size):
            try:
                for slug in pending[start:start + batch_size]:
                    run.club(slug)
            finally:
                run.flush()
            result["batches"] += 1
            logger.info("ClubElo history batch %d committed: %s", result["batches"],
                        {k: result[k] for k in ("pages_ok", "no_page", "pages_failed")})
    except ClubEloBlocked as exc:
        result["blocked"] = str(exc)
        logger.error("ClubElo blocked us, run stopped: %s", exc)
        try:
            notifier(f"ClubElo история: сайт блокирует запросы, прогон остановлен. {exc}")
        except Exception as notify_exc:  # the alert must not hide the block
            logger.error("Telegram alert failed: %s", notify_exc)
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        logger.error("ClubElo history failed: %s", result["error"], exc_info=True)
    finally:
        result["pending_after"] = max(
            0, result["pending_before"] - result["pages_ok"] - result["no_page"]
        )
        result["wire_bytes"] = transport.wire_bytes
        result["requests"] = transport.requests
        result["elapsed_s"] = round(clock() - started, 1)
    return result


def collect_new_slugs(
    run: "_Run",
    linked_slugs: Iterable[str],
    *,
    limit: int = MAX_NEW_SLUGS,
) -> Dict[str, Any]:
    """Daily branch (#1463): history pages of clubs that newly got a link.

    New = slugs linked from today's /Ranking (already fetched and parsed by
    the caller, no second request) minus every slug of the manifest; the first
    ``limit`` go through ``_Run.club`` and are flushed like a history batch.
    Until the first full history run (#1465) the manifest is empty, so this
    is simply the first ``limit`` clubs of the queue — each is then closed and
    skipped by that full run.
    """

    known = run.store.known_slugs()
    new = [slug for slug in linked_slugs if slug not in known]
    picked = new[:limit]
    try:
        for slug in picked:
            run.club(slug)
    finally:
        run.flush()
    return {
        "history_new_candidates": len(new),
        "history_new_fetched": len(picked),
        "history_new_ok": run.result["pages_ok"],
        "history_new_no_page": run.result["no_page"],
        "history_new_failed": run.result["pages_failed"],
        "history_new_failed_slugs": run.result["failed_slugs"],
    }


def exit_code(result: Dict[str, Any]) -> int:
    """0 only for a complete run: nothing failed, redirected, blocked or left."""

    incomplete = (
        result.get("error")
        or result.get("blocked")
        or result.get("pages_failed", 0) > 0
        or result.get("redirects", 0) > 0
        or result.get("pending_after", 0) > 0
    )
    return 1 if incomplete else 0


def run_default(*, batch_size: int = DEFAULT_BATCH_SIZE) -> Dict[str, Any]:
    """Production wiring: a requests session and the Iceberg store."""

    import requests

    with requests.Session() as session:
        return run_history(ClubEloTransport(session), IcebergHistoryStore(), batch_size=batch_size)
