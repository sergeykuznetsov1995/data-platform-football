"""ClubElo daily snapshot from ``/Ranking`` + ``/Results`` (#1463).

One run, in this order:

1. ``/Ranking`` → raw row (``bronze.clubelo_raw_page``) first, then the parse
   and the fail-closed contract (``parse_ranking`` + ``check_ranking``, C1–C6).
2. ``/Results`` → raw row, parse. Its h1 date may be newer than the /Ranking
   h1 date (owner decision 25.09 on M-09: the site rebuilds /Ranking later,
   live 25.09 /Ranking 2026-09-22 vs /Results 2026-09-24) — both are written,
   each under its own date. Only an OLDER /Results is fetched again after
   ``results_retry_pause`` (10 min), up to ``results_retries`` (3) times;
   still older → nothing parsed is written.
3. Completeness guard of the snapshot (``check_completeness``): absolute
   (>= 95 % of 1741 clubs), against the previous rating date (>= 95 %) and,
   only when this rating date is already stored, ``min_replace_ratio`` 0.9
   (M-06). A new partition is therefore still guarded.
4. ``bronze.clubelo_rank_snapshot``: the ``rating_date`` partition is replaced
   in one Iceberg transaction — never APPEND (#314). The same rating date again
   (the site did not rebuild yet) is a plain replace, no alert.
5. ``bronze.clubelo_result``: MERGE by ``(match_date, home_key, away_key)``.
   The /Results window is ~3 days and partial, so a replace by match date
   would erase rows of an earlier fetch (R-34, R-58); a new fetch updates the
   score and ``is_final``.
6. On a NEW rating date only: up to 10 club pages whose slug is linked from
   today's /Ranking but absent from the history manifest go through the
   history run (``history.collect_new_slugs``). Its failure makes the run red
   but does not roll back the written snapshot.

Any failed contract check, guard, date mismatch, block or error before step 4
writes nothing parsed, alerts with the failed check and exits non-zero.
"""

from __future__ import annotations

import gzip
import logging
import time
import uuid
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Tuple

import pyarrow as pa

from scrapers.clubelo import history
from scrapers.clubelo.parse import (
    LayoutChanged,
    RankingPage,
    ResultsPage,
    check_ranking,
    parse_ranking,
    parse_results,
)
from scrapers.clubelo.transport import (
    ClubEloBlocked,
    ClubEloFetchError,
    ClubEloTransport,
    Page,
    get_source,
)

logger = logging.getLogger(__name__)

DATABASE = history.DATABASE
SNAPSHOT_TABLE = "clubelo_rank_snapshot"
RESULT_TABLE = "clubelo_result"
RESULT_KEYS = ("match_date", "home_key", "away_key")
EXPECTED_CLUBS = 1741  # eloData rows of /Ranking on 2026-09-22
MIN_COMPLETENESS = 0.95
MIN_REPLACE_RATIO = 0.9
RESULTS_RETRIES = 3
RESULTS_RETRY_PAUSE = 600.0

_META = [
    ("model_version", pa.string()),
    ("_source", pa.string()),
    ("_ingested_at", pa.timestamp("us")),
    ("_batch_id", pa.string()),
]

SNAPSHOT_SCHEMA = pa.schema(
    [
        ("rating_date", pa.date32()),  # h1 date of /Ranking; the partition
        ("page_created_at", pa.timestamp("us")),  # "Page created on …", naive UTC
        ("fetched_at", pa.timestamp("us")),
        ("club_key", pa.string()),  # slug as is (case kept) or "~CC:Name"
        ("slug", pa.string()),
        ("name", pa.string()),
        ("country", pa.string()),
        ("level", pa.int32()),
        ("level_group", pa.string()),
        ("level_section", pa.string()),
        ("rank", pa.int32()),
        ("elo", pa.int32()),
        ("elo_precise", pa.float64()),
        ("golo", pa.float64()),
        ("elo_delta_1d_raw", pa.string()),
        ("is_provisional", pa.bool_()),
    ]
    + _META
)

RESULT_SCHEMA = pa.schema(
    [
        ("match_date", pa.date32()),  # the partition
        ("home_key", pa.string()),
        ("away_key", pa.string()),
        ("row_seq", pa.int32()),
        ("home_slug", pa.string()),
        ("home_tlc", pa.string()),
        ("home_name", pa.string()),
        ("home_country", pa.string()),
        ("home_rank", pa.int32()),
        ("away_slug", pa.string()),
        ("away_tlc", pa.string()),
        ("away_name", pa.string()),
        ("away_country", pa.string()),
        ("away_rank", pa.int32()),
        ("prior_delta", pa.float64()),
        ("prior_delta_sigma", pa.float64()),
        ("prior_delta_raw", pa.string()),  # cell text: "-100.0 ±80" or "NEW"
        ("hfa", pa.float64()),
        ("elo_pct", pa.float64()),
        ("ft", pa.string()),
        ("et", pa.string()),
        ("pen", pa.string()),
        ("game_delta", pa.float64()),
        ("game_delta_sigma", pa.float64()),
        ("post_game_delta", pa.float64()),
        ("post_game_delta_sigma", pa.float64()),
        ("is_final", pa.bool_()),
        ("rating_date", pa.date32()),  # h1 date of the /Results page
        ("page_created_at", pa.timestamp("us")),
        ("fetched_at", pa.timestamp("us")),
    ]
    + _META
)


class GuardRefused(RuntimeError):
    """The snapshot is too small to replace a partition — nothing written."""


class DatesDiffer(RuntimeError):
    """/Results kept an h1 date older than /Ranking after every retry."""


class IcebergDailyStore:
    """Writer of the two daily tables (snapshot: replace; results: MERGE)."""

    def __init__(self, writer=None, database: str = DATABASE) -> None:
        if writer is None:
            from scrapers.base.iceberg_writer import IcebergWriter

            writer = IcebergWriter()
        self.writer = writer
        self.database = database

    def ensure_tables(self) -> None:
        self.writer.create_table_if_not_exists(
            self.database, SNAPSHOT_TABLE, SNAPSHOT_SCHEMA,
            partition_spec=[("rating_date", "identity")],
        )
        self.writer.create_table_if_not_exists(
            self.database, RESULT_TABLE, RESULT_SCHEMA,
            partition_spec=[("match_date", "identity")],
        )

    def snapshot_counts(self, rating_date: date) -> Tuple[Optional[int], Optional[int]]:
        """eloData rows ``(of this rating date, of the latest earlier date)``."""

        table = f"{self.writer.catalog}.{self.database}.{SNAPSHOT_TABLE}"
        rows = self.writer._get_trino_manager().execute_query(
            f"SELECT rating_date, count(*) FROM {table} "
            f"WHERE rating_date <= DATE '{rating_date.isoformat()}' AND NOT is_provisional "
            "GROUP BY rating_date ORDER BY rating_date DESC LIMIT 2"
        )
        same = previous = None
        for day, count in rows:
            if day == rating_date:
                same = int(count)
            elif previous is None:
                previous = int(count)
        return same, previous

    def replace_snapshot(self, rating_date: date, rows: List[Dict[str, Any]]) -> int:
        table = pa.Table.from_pylist(rows, schema=SNAPSHOT_SCHEMA)
        # The writer compares the partition column with a string value; the
        # ISO string is cast back to the table's date type when aligned.
        iso = rating_date.isoformat()
        index = table.schema.get_field_index("rating_date")
        table = table.set_column(index, "rating_date", pa.array([iso] * len(table), pa.string()))
        return self.writer.replace_identity_partition_arrow_batches(
            [table],
            database=self.database,
            table=SNAPSHOT_TABLE,
            partition_column="rating_date",
            partition_value=iso,
        )

    def merge_results(self, rows: List[Dict[str, Any]]) -> None:
        frame = pa.Table.from_pylist(rows, schema=RESULT_SCHEMA).to_pandas(
            date_as_object=True, integer_object_nulls=True
        )
        self.writer.write_dataframe(
            frame, self.database, RESULT_TABLE, mode="append", add_metadata=False,
            merge_keys=list(RESULT_KEYS),
        )


def check_completeness(
    elo_rows: int,
    same: Optional[int],
    previous: Optional[int],
    *,
    expected: int = EXPECTED_CLUBS,
) -> None:
    """Guard of the snapshot replace; raises ``GuardRefused``."""

    if elo_rows < MIN_COMPLETENESS * expected:
        raise GuardRefused(
            f"G1 {elo_rows} clubs < {MIN_COMPLETENESS:.0%} of {expected}"
        )
    if previous is not None and elo_rows < MIN_COMPLETENESS * previous:
        raise GuardRefused(
            f"G2 {elo_rows} clubs < {MIN_COMPLETENESS:.0%} of {previous} of the previous rating date"
        )
    if same is not None and elo_rows < MIN_REPLACE_RATIO * same:
        raise GuardRefused(
            f"G3 {elo_rows} clubs < {MIN_REPLACE_RATIO:.0%} of {same} already stored for this date"
        )


def _html(raw: Dict[str, Any]) -> str:
    return gzip.decompress(raw["body"]).decode("utf-8")


class _Daily:
    def __init__(self, transport: ClubEloTransport, store, history_store, batch_id: str,
                 result: Dict[str, Any]) -> None:
        self.transport = transport
        self.store = store
        self.history_store = history_store
        # One history run object: its raw-row builder (#1462) for both pages
        # and its club() for the new slugs.
        self.run = history._Run(transport, history_store, batch_id)
        self.meta = dict(self.run.meta)
        self.result = result

    def fetch(self, path: str) -> Tuple[Page, str]:
        page = self.transport.get(path)
        if page.status != 200:
            raise ClubEloFetchError(f"{path}: HTTP {page.status}")
        raw = self.run._raw_row(page)
        self.history_store.append(history.RAW_TABLE, [raw])  # raw-first
        return page, _html(raw)

    def ranking(self) -> Tuple[Page, RankingPage]:
        page, html = self.fetch("/Ranking")
        ranking = parse_ranking(html)
        check_ranking(ranking)
        return page, ranking

    def results(self, rating_date: date, retries: int, pause: float,
                sleep: Callable[[float], None]) -> Tuple[Page, ResultsPage]:
        for attempt in range(retries + 1):
            if attempt:
                sleep(pause)
            page, html = self.fetch("/Results")
            parsed = parse_results(html)
            self.result["results_attempts"] = attempt + 1
            if parsed.rating_date >= rating_date:
                return page, parsed
            logger.warning(
                "ClubElo /Results h1 date %s older than /Ranking %s (attempt %d of %d)",
                parsed.rating_date, rating_date, attempt + 1, retries + 1,
            )
        raise DatesDiffer(
            f"M-09 /Results h1 date {parsed.rating_date} older than /Ranking {rating_date} "
            f"after {retries + 1} attempts"
        )

    def row(self, **values) -> Dict[str, Any]:
        return {**values, **self.meta, "_ingested_at": history._utcnow()}


def run_daily(
    transport: ClubEloTransport,
    store,
    history_store,
    *,
    source: str = "html",
    notifier: Callable[[str], None] = history.notify,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    results_retries: int = RESULTS_RETRIES,
    results_retry_pause: float = RESULTS_RETRY_PAUSE,
    new_slugs_limit: int = history.MAX_NEW_SLUGS,
    batch_id: Optional[str] = None,
) -> Dict[str, Any]:
    """One daily snapshot; see the module docstring. Returns the run report."""

    get_source(source)
    started = clock()
    batch_id = batch_id or f"clubelo-daily-{history._utcnow():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    result: Dict[str, Any] = {
        "batch_id": batch_id,
        "rating_date": None,
        "page_created_at": None,
        "fetched_at": None,
        "rows": 0,  # eloData clubs written
        "provisional": 0,
        "snapshot_rows": 0,
        "levels_matched_pct": None,
        "elo_precise_matched": 0,
        "same_date": None,
        "results_rating_date": None,
        "results_attempts": 0,
        "results_rows": 0,
        "results_final": 0,
        "results_duplicates": 0,
        "history_new_candidates": 0,
        "history_new_fetched": 0,
        "history_new_ok": 0,
        "history_new_no_page": 0,
        "history_new_failed": 0,
        "history_new_failed_slugs": [],
        "wire_bytes": 0,
        "wire_bytes_daily": 0,
        "requests": 0,
        "elapsed_s": 0.0,
        "written": False,
        "check": None,
        "blocked": None,
        "error": None,
    }
    daily = _Daily(transport, store, history_store, batch_id, result)
    try:
        history_store.ensure_tables()
        store.ensure_tables()
        ranking_page, ranking = daily.ranking()
        result.update(
            rating_date=ranking.rating_date.isoformat(),
            page_created_at=ranking.page_created_at.isoformat(),
            fetched_at=ranking_page.fetched_at.isoformat(),
            levels_matched_pct=ranking.levels_matched_pct,
            elo_precise_matched=ranking.elo_precise_matched,
        )
        results_page, results = daily.results(
            ranking.rating_date, results_retries, results_retry_pause, sleep
        )
        result["results_rating_date"] = results.rating_date.isoformat()
        same, previous = store.snapshot_counts(ranking.rating_date)
        check_completeness(ranking.elo_rows, same, previous)
        result["same_date"] = same is not None

        common = {"rating_date": ranking.rating_date, "page_created_at": ranking.page_created_at,
                  "fetched_at": ranking_page.fetched_at}
        snapshot = [daily.row(**common, **row) for row in ranking.rows]
        result["snapshot_rows"] = store.replace_snapshot(ranking.rating_date, snapshot)
        result["rows"], result["provisional"] = ranking.elo_rows, ranking.provisional
        common = {"rating_date": results.rating_date, "page_created_at": results.page_created_at,
                  "fetched_at": results_page.fetched_at}
        store.merge_results([daily.row(**common, **row) for row in results.rows])
        result["written"] = True
        result.update(
            results_rows=len(results.rows),
            results_final=sum(row["is_final"] for row in results.rows),
            results_duplicates=results.duplicates,
        )
        logger.info("ClubElo daily written: %s", {k: result[k] for k in (
            "rating_date", "rows", "provisional", "same_date", "results_rows")})
        result["wire_bytes_daily"] = transport.wire_bytes

        if result["same_date"]:
            logger.info("ClubElo rating date %s unchanged: no new history slugs", result["rating_date"])
        else:
            daily.run.rating_date = ranking.rating_date
            result.update(history.collect_new_slugs(daily.run, ranking.linked_slugs,
                                                    limit=new_slugs_limit))
    except ClubEloBlocked as exc:
        result["blocked"] = str(exc)
        logger.error("ClubElo blocked us, daily run stopped: %s", exc)
        _alert(notifier, f"ClubElo дневной снимок: сайт блокирует запросы, прогон остановлен. {exc}")
    except (LayoutChanged, GuardRefused, DatesDiffer) as exc:
        result["check"] = str(exc)
        logger.error("ClubElo daily check failed, nothing written: %s", exc)
        _alert(notifier, f"ClubElo дневной снимок: проверка не пройдена, ничего не записано. {exc}")
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        logger.error("ClubElo daily failed: %s", result["error"], exc_info=True)
    finally:
        if not result["wire_bytes_daily"]:
            result["wire_bytes_daily"] = transport.wire_bytes
        result["wire_bytes"] = transport.wire_bytes
        result["requests"] = transport.requests
        result["elapsed_s"] = round(clock() - started, 1)
    return result


def _alert(notifier: Callable[[str], None], message: str) -> None:
    try:
        notifier(message)
    except Exception as exc:  # the alert must not hide the failure
        logger.error("Telegram alert failed: %s", exc)


def exit_code(result: Dict[str, Any]) -> int:
    """0 only when the snapshot and results are written and no new slug failed."""

    failed = (
        not result.get("written")
        or result.get("check")
        or result.get("error")
        or result.get("blocked")
        or result.get("history_new_failed", 0) > 0
    )
    return 1 if failed else 0


def run_default() -> Dict[str, Any]:
    """Production wiring: a requests session and the Iceberg stores."""

    import requests

    with requests.Session() as session:
        return run_daily(
            ClubEloTransport(session), IcebergDailyStore(), history.IcebergHistoryStore()
        )
