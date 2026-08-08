"""Zero-network replay benchmark for FBref Bronze persistence.

The benchmark deliberately consumes only captured match documents.  It does
not construct a fetcher or a raw-store fetch callback, so source/proxy traffic
is always zero.  Trino remains the persistence target under measurement.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import sys
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scrapers.base.sql_validator import validate_catalog_qualified_name  # noqa: E402
from scrapers.base.trino_manager import TrinoTableManager  # noqa: E402
from scrapers.fbref.bronze import (  # noqa: E402
    FBrefGenericBronzeWriter,
    PAGE_MANIFEST_TABLE,
    TABLE_CELLS_TABLE,
    TABLE_INVENTORY_TABLE,
)
from scrapers.fbref.page_document import parse_page_document  # noqa: E402
from scrapers.fbref.typed_bronze import (  # noqa: E402
    FBrefTypedBronzeWriter,
    MATCH_AVAILABILITY_TABLE,
    MATCH_DATASET_TABLES,
    TypedSourceContext,
    parse_match_html,
)


VOLATILE_COLUMNS = frozenset({"_ingested_at", "persisted_at"})
BENCHMARK_TABLES = (
    TABLE_CELLS_TABLE,
    TABLE_INVENTORY_TABLE,
    PAGE_MANIFEST_TABLE,
    *MATCH_DATASET_TABLES.values(),
    MATCH_AVAILABILITY_TABLE,
)
OFFLINE_CONTEXT = TypedSourceContext(
    source_competition_id="9",
    source_season_id="2025-2026",
    competition_name="Premier League",
    season_label="2025-2026",
)


@dataclass(frozen=True)
class BenchmarkConfig:
    html_dir: Path
    sequential_schema: str
    batch_schema: str | None
    iterations: int = 3
    min_speedup: float = 4.0
    max_seconds_per_match: float = 20.0


@dataclass(frozen=True)
class GateResult:
    speedup: float
    seconds_per_match: float
    equivalent: bool
    proxy_requests: int
    proxy_bytes: int
    passed: bool


@dataclass(frozen=True)
class StatementCounts:
    execute: int
    execute_committing: int


@dataclass(frozen=True)
class TableDigest:
    present: bool
    rows: int | None
    sha256: str | None


@dataclass(frozen=True)
class PersistenceRun:
    seconds: float
    statement_counts: StatementCounts
    table_digests: Mapping[str, TableDigest]


@dataclass(frozen=True)
class BenchmarkReport:
    matches: int
    iterations: int
    sequential: PersistenceRun
    batch: PersistenceRun | None
    equivalent: bool | None
    proxy_requests: int
    proxy_bytes: int
    gate: GateResult | None

    def to_dict(self) -> dict[str, Any]:
        """Return the stable JSON report shape without leaking SQL values."""

        return asdict(self)


class BatchPersistenceUnavailableError(RuntimeError):
    """Raised until the Task 2 writer batch APIs are available."""


@contextmanager
def _suppress_trino_sql_logs() -> Iterator[None]:
    """Keep Trino SQL and bound values out of benchmark output and reports."""

    logger = logging.getLogger("scrapers.base.trino_manager")
    previously_disabled = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = previously_disabled


class CountingTrinoTableManager(TrinoTableManager):
    """Trino manager that records statement totals, never statement text."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._execute_count = 0
        self._execute_committing_count = 0

    def _execute(
        self,
        sql: str,
        fetch: bool = False,
        params: tuple | None = None,
    ) -> Any:
        self._execute_count += 1
        return super()._execute(sql, fetch=fetch, params=params)

    def _execute_committing(self, sql: str) -> None:
        self._execute_committing_count += 1
        return super()._execute_committing(sql)

    def statement_counts(self) -> StatementCounts:
        return StatementCounts(
            execute=self._execute_count,
            execute_committing=self._execute_committing_count,
        )


@dataclass(frozen=True)
class _ReplayItem:
    match_id: str
    html: str
    page: Any
    typed_match: Any


def evaluate_gate(
    *,
    sequential_seconds: float,
    batch_seconds: float,
    matches: int,
    equivalent: bool,
    proxy_requests: int,
    proxy_bytes: int,
    min_speedup: float = 4.0,
    max_seconds_per_match: float = 20.0,
) -> GateResult:
    speedup = sequential_seconds / batch_seconds
    seconds_per_match = batch_seconds / matches
    return GateResult(
        speedup=speedup,
        seconds_per_match=seconds_per_match,
        equivalent=equivalent,
        proxy_requests=proxy_requests,
        proxy_bytes=proxy_bytes,
        passed=(
            equivalent
            and proxy_requests == 0
            and proxy_bytes == 0
            and speedup >= min_speedup
            and seconds_per_match <= max_seconds_per_match
        ),
    )


def _load_replay_items(html_dir: Path) -> tuple[_ReplayItem, ...]:
    paths = tuple(sorted(html_dir.glob("*.html.gz")))
    if not paths:
        raise ValueError(f"No *.html.gz files found in {html_dir}")

    items = []
    for path in paths:
        match_id = path.name.split(".", 1)[0]
        with gzip.open(path, "rt", encoding="utf-8") as fixture:
            html = fixture.read()
        items.append(
            _ReplayItem(
                match_id=match_id,
                html=html,
                page=parse_page_document(
                    html,
                    target_id=f"fbref:match:{match_id}",
                    page_kind="match",
                    source_ids={"match_id": match_id},
                ),
                typed_match=parse_match_html(
                    html,
                    match_id=match_id,
                    context=OFFLINE_CONTEXT,
                    require_player_contract=False,
                ),
            )
        )
    return tuple(items)


def _mean_iteration_seconds(*, elapsed_seconds: float, iterations: int) -> float:
    """Convert a multi-iteration wall clock into one comparable replay time."""

    return elapsed_seconds / iterations


def _run_sequential(
    manager: CountingTrinoTableManager,
    *,
    schema: str,
    items: Sequence[_ReplayItem],
    iterations: int,
) -> float:
    generic_writer = FBrefGenericBronzeWriter(manager, schema=schema)
    typed_writer = FBrefTypedBronzeWriter(manager, schema=schema)
    started = time.perf_counter()
    for iteration in range(iterations):
        for item in items:
            run_id = f"fbref-persistence-benchmark-{iteration}-{item.match_id}"
            generic_writer.persist_page(
                item.page,
                canonical_url=(
                    f"https://fbref.com/en/matches/{item.match_id}/"
                ),
                run_id=run_id,
                staging_identity=run_id,
            )
            typed_writer.persist_match(
                item.typed_match,
                match_id=item.match_id,
                context=OFFLINE_CONTEXT,
                run_id=run_id,
                target_identity=f"benchmark:match:{item.match_id}",
            )
    return _mean_iteration_seconds(
        elapsed_seconds=time.perf_counter() - started,
        iterations=iterations,
    )


def _batch_api_available() -> bool:
    return all(
        (
            hasattr(FBrefGenericBronzeWriter, "persist_pages"),
            hasattr(FBrefTypedBronzeWriter, "persist_matches"),
        )
    )


def _run_batch(
    manager: CountingTrinoTableManager,
    *,
    schema: str,
    items: Sequence[_ReplayItem],
    iterations: int,
) -> float:
    """Run the Task 2 batch APIs when they become available.

    Imports stay dynamic so this Task 1 benchmark remains importable before
    the batch item dataclasses exist.
    """

    if not _batch_api_available():
        raise BatchPersistenceUnavailableError(
            "batch persistence API is unavailable"
        )

    from scrapers.fbref.bronze import GenericPagePersistItem
    from scrapers.fbref.typed_bronze import TypedMatchPersistItem

    generic_writer = FBrefGenericBronzeWriter(manager, schema=schema)
    typed_writer = FBrefTypedBronzeWriter(manager, schema=schema)
    started = time.perf_counter()
    for iteration in range(iterations):
        generic_items = []
        typed_items = []
        for item in items:
            run_id = f"fbref-persistence-benchmark-{iteration}-{item.match_id}"
            generic_items.append(
                GenericPagePersistItem(
                    page=item.page,
                    canonical_url=(
                        f"https://fbref.com/en/matches/{item.match_id}/"
                    ),
                    run_id=run_id,
                    staging_identity=run_id,
                )
            )
            typed_items.append(
                TypedMatchPersistItem(
                    parsed=item.typed_match,
                    match_id=item.match_id,
                    context=OFFLINE_CONTEXT,
                    run_id=run_id,
                    target_identity=f"benchmark:match:{item.match_id}",
                )
            )
        generic_writer.persist_pages(generic_items)
        typed_writer.persist_matches(typed_items)
    return _mean_iteration_seconds(
        elapsed_seconds=time.perf_counter() - started,
        iterations=iterations,
    )


def _normalized_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return value


def _normalized_rows(
    columns: Iterable[str], rows: Iterable[Sequence[Any]]
) -> list[str]:
    kept_columns = [
        column
        for column in columns
        if column.casefold() not in VOLATILE_COLUMNS
    ]
    normalized = []
    for row in rows:
        record = {
            column: _normalized_value(value)
            for column, value in zip(kept_columns, row)
        }
        normalized.append(
            json.dumps(
                record,
                default=str,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
    return sorted(normalized)


def _table_digests(
    manager: CountingTrinoTableManager, schema: str
) -> dict[str, TableDigest]:
    digests: dict[str, TableDigest] = {}
    for table in BENCHMARK_TABLES:
        if not manager.table_exists(schema, table):
            digests[table] = TableDigest(
                present=False,
                rows=None,
                sha256=None,
            )
            continue
        columns = manager.get_table_columns(schema, table)
        kept_columns = [
            name
            for name in columns
            if name.casefold() not in VOLATILE_COLUMNS
        ]
        qualified = validate_catalog_qualified_name(
            manager.catalog, schema, table
        )
        selected = ", ".join(f'"{column}"' for column in kept_columns)
        rows = manager._execute(
            f"SELECT {selected} FROM {qualified}", fetch=True
        )
        normalized = _normalized_rows(kept_columns, rows or ())
        payload = "\n".join(normalized).encode("utf-8")
        digests[table] = TableDigest(
            present=True,
            rows=len(normalized),
            sha256=hashlib.sha256(payload).hexdigest(),
        )
    return digests


def _digests_equivalent(
    sequential: Mapping[str, TableDigest], batch: Mapping[str, TableDigest]
) -> bool:
    """Require every expected table to exist and agree in both schemas."""

    for table in BENCHMARK_TABLES:
        left = sequential.get(table)
        right = batch.get(table)
        if left is None or right is None:
            return False
        if not left.present or not right.present:
            return False
        if left != right:
            return False
    return True


def _validate_schema_isolation(config: BenchmarkConfig) -> None:
    if (
        config.batch_schema is not None
        and config.sequential_schema.casefold() == config.batch_schema.casefold()
    ):
        raise ValueError(
            "sequential and batch schemas must be case-insensitively distinct"
        )


def _preflight_schemas(config: BenchmarkConfig) -> None:
    """Reject replay schemas containing any table the benchmark may touch."""

    manager = CountingTrinoTableManager()
    schemas = (config.sequential_schema,)
    if config.batch_schema is not None:
        schemas += (config.batch_schema,)
    for schema in schemas:
        present = [
            table
            for table in BENCHMARK_TABLES
            if manager.table_exists(schema, table)
        ]
        if present:
            raise ValueError(
                f"benchmark schema {schema!r} must be empty; found "
                + ", ".join(present)
            )


def _run_persistence(
    *,
    schema: str,
    items: Sequence[_ReplayItem],
    iterations: int,
    runner: Any,
) -> PersistenceRun:
    manager = CountingTrinoTableManager()
    elapsed = runner(manager, schema=schema, items=items, iterations=iterations)
    statement_counts = manager.statement_counts()
    return PersistenceRun(
        seconds=elapsed,
        statement_counts=statement_counts,
        table_digests=_table_digests(manager, schema),
    )


def run_benchmark(config: BenchmarkConfig) -> BenchmarkReport:
    """Persist an offline fixture cohort in sequential and optional batch mode."""

    if config.iterations < 1:
        raise ValueError("iterations must be at least 1")
    _validate_schema_isolation(config)
    if config.batch_schema and not _batch_api_available():
        raise BatchPersistenceUnavailableError(
            "batch persistence API is unavailable"
        )

    with _suppress_trino_sql_logs():
        _preflight_schemas(config)
        items = _load_replay_items(config.html_dir)
        sequential = _run_persistence(
            schema=config.sequential_schema,
            items=items,
            iterations=config.iterations,
            runner=_run_sequential,
        )
        if config.batch_schema is None:
            return BenchmarkReport(
                matches=len(items),
                iterations=config.iterations,
                sequential=sequential,
                batch=None,
                equivalent=None,
                proxy_requests=0,
                proxy_bytes=0,
                gate=None,
            )

        batch = _run_persistence(
            schema=config.batch_schema,
            items=items,
            iterations=config.iterations,
            runner=_run_batch,
        )
        equivalent = _digests_equivalent(
            sequential.table_digests, batch.table_digests
        )
        gate = evaluate_gate(
            sequential_seconds=sequential.seconds,
            batch_seconds=batch.seconds,
            matches=len(items),
            equivalent=equivalent,
            proxy_requests=0,
            proxy_bytes=0,
            min_speedup=config.min_speedup,
            max_seconds_per_match=config.max_seconds_per_match,
        )
        return BenchmarkReport(
            matches=len(items),
            iterations=config.iterations,
            sequential=sequential,
            batch=batch,
            equivalent=equivalent,
            proxy_requests=0,
            proxy_bytes=0,
            gate=gate,
        )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--html-dir", type=Path, required=True)
    parser.add_argument("--sequential-schema", required=True)
    parser.add_argument("--batch-schema")
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--min-speedup", type=float, default=4.0)
    parser.add_argument("--max-seconds-per-match", type=float, default=20.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        report = run_benchmark(
            BenchmarkConfig(
                html_dir=args.html_dir,
                sequential_schema=args.sequential_schema,
                batch_schema=args.batch_schema,
                iterations=args.iterations,
                min_speedup=args.min_speedup,
                max_seconds_per_match=args.max_seconds_per_match,
            )
        )
    except BatchPersistenceUnavailableError as error:
        raise SystemExit(str(error)) from error

    rendered = json.dumps(report.to_dict(), indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if report.gate is None or report.gate.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
