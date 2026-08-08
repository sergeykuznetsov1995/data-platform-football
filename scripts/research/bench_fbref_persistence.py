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
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scrapers.base.sql_validator import (  # noqa: E402
    validate_catalog_qualified_name,
    validate_identifier,
)
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
    strict_acceptance: bool = False
    sequential_control_run_id: str | None = None
    batch_control_run_id: str | None = None
    sentinel_match_id: str | None = None


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
class TableDiff:
    sequential_minus_batch: int
    batch_minus_sequential: int


@dataclass(frozen=True)
class SnapshotDelta:
    before_snapshot_id: int | None
    after_snapshot_id: int | None
    changed: bool


@dataclass(frozen=True)
class ControlRunEvidence:
    run_id: str
    evidence_source: str
    proxy_requests: int
    proxy_bytes: int
    logical_refreshes: int
    dataset_manifests: int
    observation_sha256: str
    manifest_sha256: str
    latest_state_sha256: str
    valid: bool
    failures: tuple[str, ...]


@dataclass(frozen=True)
class AcceptanceBaseline:
    schema: str
    sentinel_match_id: str
    snapshots: Mapping[str, int | None]
    sentinels: Mapping[str, TableDigest]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "fbref-pipeline-acceptance-baseline-v1",
            **asdict(self),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "AcceptanceBaseline":
        if (
            value.get("schema_version")
            != "fbref-pipeline-acceptance-baseline-v1"
        ):
            raise ValueError("acceptance baseline schema version is invalid")
        snapshots = value.get("snapshots")
        sentinels = value.get("sentinels")
        if not isinstance(snapshots, Mapping) or not isinstance(
            sentinels, Mapping
        ):
            raise ValueError("acceptance baseline is incomplete")
        parsed_sentinels = {}
        for table, item in sentinels.items():
            if not isinstance(item, Mapping):
                raise ValueError("acceptance sentinel evidence is invalid")
            parsed_sentinels[str(table)] = TableDigest(
                present=bool(item.get("present")),
                rows=(None if item.get("rows") is None else int(item["rows"])),
                sha256=(
                    None
                    if item.get("sha256") is None
                    else str(item["sha256"])
                ),
            )
        return cls(
            schema=str(value.get("schema") or ""),
            sentinel_match_id=str(value.get("sentinel_match_id") or ""),
            snapshots={
                str(table): None if item is None else int(item)
                for table, item in snapshots.items()
            },
            sentinels=parsed_sentinels,
        )


@dataclass(frozen=True)
class PersistenceRun:
    seconds: float
    statement_counts: StatementCounts
    table_digests: Mapping[str, TableDigest]
    snapshots_before: Mapping[str, int | None] = field(default_factory=dict)
    snapshots_after: Mapping[str, int | None] = field(default_factory=dict)
    snapshot_deltas: Mapping[str, SnapshotDelta] = field(default_factory=dict)
    sentinel_before: Mapping[str, TableDigest] = field(default_factory=dict)
    sentinel_after: Mapping[str, TableDigest] = field(default_factory=dict)


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
    table_diffs: Mapping[str, TableDiff] | None = None
    control_sequential: ControlRunEvidence | None = None
    control_batch: ControlRunEvidence | None = None
    control_equivalent: bool | None = None
    sentinels_preserved: bool | None = None
    passed: bool | None = None

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


def _single_count(rows: Any, *, label: str) -> int:
    if not rows or len(rows) != 1 or len(rows[0]) != 1:
        raise RuntimeError(f"Unexpected Trino result for {label}")
    return int(rows[0][0] or 0)


def _comparison_columns(
    manager: CountingTrinoTableManager,
    sequential_schema: str,
    batch_schema: str,
    table: str,
) -> tuple[str, ...]:
    sequential = manager.get_table_columns(sequential_schema, table)
    batch = manager.get_table_columns(batch_schema, table)
    sequential_signature = _normalized_column_signature(sequential)
    batch_signature = _normalized_column_signature(batch)
    if sequential_signature != batch_signature:
        raise ValueError(
            f"column/type signature mismatch for benchmark table {table!r}"
        )
    columns = tuple(
        name
        for name in sequential
        if name.casefold() not in VOLATILE_COLUMNS
    )
    if not columns:
        raise ValueError(f"benchmark table {table!r} has no stable columns")
    for column in columns:
        validate_identifier(column, "column")
    return columns


def _except_all_count(
    manager: CountingTrinoTableManager,
    *,
    left_schema: str,
    right_schema: str,
    table: str,
    columns: Sequence[str],
) -> int:
    left = validate_catalog_qualified_name(
        manager.catalog, left_schema, table
    )
    right = validate_catalog_qualified_name(
        manager.catalog, right_schema, table
    )
    selected = ", ".join(f'"{column}"' for column in columns)
    rows = manager._execute(
        "SELECT count(*) FROM ("
        f"SELECT {selected} FROM {left} "
        "EXCEPT ALL "
        f"SELECT {selected} FROM {right}"
        ") AS directional_diff",
        fetch=True,
    )
    return _single_count(rows, label=f"{left_schema}-{right_schema} {table}")


def _bidirectional_table_diffs(
    manager: CountingTrinoTableManager,
    sequential_schema: str,
    batch_schema: str,
) -> dict[str, TableDiff]:
    """Compare every physical match table with EXCEPT ALL in both directions."""

    diffs = {}
    for table in BENCHMARK_TABLES:
        if not manager.table_exists(
            sequential_schema, table
        ) or not manager.table_exists(batch_schema, table):
            raise ValueError(f"missing required benchmark table {table!r}")
        columns = _comparison_columns(
            manager, sequential_schema, batch_schema, table
        )
        diffs[table] = TableDiff(
            sequential_minus_batch=_except_all_count(
                manager,
                left_schema=sequential_schema,
                right_schema=batch_schema,
                table=table,
                columns=columns,
            ),
            batch_minus_sequential=_except_all_count(
                manager,
                left_schema=batch_schema,
                right_schema=sequential_schema,
                table=table,
                columns=columns,
            ),
        )
    return diffs


def _snapshot_ids(
    manager: CountingTrinoTableManager, schema: str
) -> dict[str, int | None]:
    """Read the latest Iceberg snapshot ID for every benchmark table."""

    validate_identifier(manager.catalog, "catalog")
    validate_identifier(schema, "schema")
    snapshots: dict[str, int | None] = {}
    for table in BENCHMARK_TABLES:
        validate_identifier(table, "table")
        rows = manager._execute(
            "SELECT max(snapshot_id) FROM "
            f'{manager.catalog}.{schema}."{table}$snapshots"',
            fetch=True,
        )
        if not rows or len(rows) != 1 or len(rows[0]) != 1:
            raise RuntimeError(f"Unexpected Iceberg snapshot result for {table}")
        snapshots[table] = (
            None if rows[0][0] is None else int(rows[0][0])
        )
    return snapshots


def _snapshot_deltas(
    before: Mapping[str, int | None],
    after: Mapping[str, int | None],
) -> dict[str, SnapshotDelta]:
    return {
        table: SnapshotDelta(
            before_snapshot_id=before.get(table),
            after_snapshot_id=after.get(table),
            changed=before.get(table) != after.get(table),
        )
        for table in BENCHMARK_TABLES
    }


def _sentinel_column(table: str) -> str:
    return "target_id" if table in {
        TABLE_CELLS_TABLE,
        TABLE_INVENTORY_TABLE,
        PAGE_MANIFEST_TABLE,
    } else "match_id"


def _sentinel_value(table: str, match_id: str) -> str:
    if _sentinel_column(table) == "target_id":
        return f"fbref:match:{match_id}"
    return match_id


def _sentinel_digests(
    manager: CountingTrinoTableManager,
    schema: str,
    match_id: str,
) -> dict[str, TableDigest]:
    """Snapshot one pre-seeded match outside the replay cohort."""

    if not str(match_id).strip():
        raise ValueError("sentinel_match_id must not be blank")
    digests: dict[str, TableDigest] = {}
    for table in BENCHMARK_TABLES:
        if not manager.table_exists(schema, table):
            digests[table] = TableDigest(False, None, None)
            continue
        columns = manager.get_table_columns(schema, table)
        kept_columns = [
            name
            for name in columns
            if name.casefold() not in VOLATILE_COLUMNS
        ]
        predicate_column = _sentinel_column(table)
        if predicate_column.casefold() not in {
            name.casefold() for name in columns
        }:
            raise ValueError(
                f"benchmark table {table!r} lacks sentinel column "
                f"{predicate_column!r}"
            )
        qualified = validate_catalog_qualified_name(
            manager.catalog, schema, table
        )
        selected = ", ".join(f'"{column}"' for column in kept_columns)
        rows = manager._execute(
            f"SELECT {selected} FROM {qualified} "
            f"WHERE {predicate_column} = ?",
            fetch=True,
            params=(_sentinel_value(table, str(match_id).strip()),),
        )
        normalized = _normalized_rows(kept_columns, rows or ())
        digests[table] = TableDigest(
            present=True,
            rows=len(normalized),
            sha256=hashlib.sha256(
                "\n".join(normalized).encode("utf-8")
            ).hexdigest(),
        )
    return digests


def capture_acceptance_baseline(
    manager: CountingTrinoTableManager,
    *,
    schema: str,
    sentinel_match_id: str,
) -> AcceptanceBaseline:
    """Capture direct, read-only evidence before a real pipeline replay."""

    snapshots = _snapshot_ids(manager, schema)
    missing_snapshots = [
        table for table, snapshot_id in snapshots.items() if snapshot_id is None
    ]
    if missing_snapshots:
        raise ValueError(
            "acceptance snapshot is missing from: "
            + ", ".join(missing_snapshots)
        )
    sentinels = _sentinel_digests(manager, schema, sentinel_match_id)
    missing = [
        table
        for table, evidence in sentinels.items()
        if not evidence.present or int(evidence.rows or 0) <= 0
    ]
    if missing:
        raise ValueError("acceptance sentinel is missing from: " + ", ".join(missing))
    return AcceptanceBaseline(
        schema=schema,
        sentinel_match_id=sentinel_match_id,
        snapshots=snapshots,
        sentinels=sentinels,
    )


def _stable_sha256(value: Any) -> str:
    rendered = json.dumps(
        value,
        default=str,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def _cursor_mapping_rows(cursor: Any) -> list[dict[str, Any]]:
    rows = list(cursor.fetchall() or ())
    if not rows:
        return []
    if isinstance(rows[0], Mapping):
        return [dict(row) for row in rows]
    names = [str(item[0]) for item in cursor.description]
    return [dict(zip(names, row)) for row in rows]


def _direct_processing_evidence(
    control: Any, run_id: str
) -> Mapping[str, Any] | None:
    """Read this replay's own observations/manifests, not source-run aliases."""

    transaction = getattr(control, "_transaction", None)
    if not callable(transaction):
        return None
    with transaction() as cursor:
        cursor.execute(
            """
            WITH ranked AS (
                SELECT target.ordinal, target.target_id,
                       target.logical_refresh_id,
                       target.status AS target_status,
                       frontier.page_kind, frontier.source_ids,
                       frontier.state AS frontier_state,
                       frontier.last_content_hash,
                       observed.content_hash,
                       observed.status AS observation_status,
                       observed.generic_status, observed.typed_status,
                       observed.stateful_status, observed.validation_status,
                       row_number() OVER (
                           PARTITION BY target.logical_refresh_id
                           ORDER BY observed.completed_at DESC NULLS LAST,
                                    observed.updated_at DESC NULLS LAST,
                                    observed.parser_version DESC,
                                    observed.typed_parser_version DESC
                       ) AS observation_rank
                FROM fbref_control.run_target AS target
                JOIN fbref_control.page_frontier AS frontier
                  ON frontier.target_id = target.target_id
                LEFT JOIN fbref_control.observation_processing AS observed
                  ON observed.logical_refresh_id = target.logical_refresh_id
                WHERE target.run_id = %s::uuid
            )
            SELECT * FROM ranked
            WHERE observation_rank = 1
            ORDER BY ordinal
            """,
            (run_id,),
        )
        observations = _cursor_mapping_rows(cursor)
        cursor.execute(
            """
            WITH ranked AS (
                SELECT target.ordinal, target.target_id,
                       target.logical_refresh_id,
                       observed.content_hash, observed.parser_version,
                       observed.typed_parser_version,
                       row_number() OVER (
                           PARTITION BY target.logical_refresh_id
                           ORDER BY observed.completed_at DESC NULLS LAST,
                                    observed.updated_at DESC NULLS LAST,
                                    observed.parser_version DESC,
                                    observed.typed_parser_version DESC
                       ) AS observation_rank
                FROM fbref_control.run_target AS target
                JOIN fbref_control.observation_processing AS observed
                  ON observed.logical_refresh_id = target.logical_refresh_id
                WHERE target.run_id = %s::uuid
            )
            SELECT ranked.ordinal, ranked.target_id,
                   ranked.logical_refresh_id, manifest.dataset,
                   manifest.availability, manifest.parse_status,
                   manifest.persistence_status, manifest.validation_status,
                   manifest.row_count,
                   CASE
                       WHEN manifest.availability IN (
                           'empty', 'restricted', 'not_applicable'
                       )
                       THEN manifest.error_message
                   END AS empty_reason
            FROM ranked
            JOIN fbref_control.dataset_manifest AS manifest
              ON manifest.target_id = ranked.target_id
             AND manifest.content_hash = ranked.content_hash
             AND manifest.parser_version IN (
                 ranked.parser_version, ranked.typed_parser_version
             )
            WHERE ranked.observation_rank = 1
            ORDER BY ranked.ordinal, manifest.parser_version,
                     manifest.dataset
            """,
            (run_id,),
        )
        datasets = _cursor_mapping_rows(cursor)
    return {"targets": observations, "datasets": datasets}


def _control_run_evidence(control: Any, run_id: str) -> ControlRunEvidence:
    """Read acceptance state directly from PostgreSQL-backed ControlStore."""

    run = control.get_run(run_id)
    summary = control.get_run_summary(run_id)
    evidence = _direct_processing_evidence(control, run_id)
    evidence_source = "direct_processing_rows"
    if evidence is None:
        evidence = control.get_acceptance_run_evidence(run_id)
        evidence_source = "control_api_fallback"
    failures: list[str] = []
    if not isinstance(run, Mapping):
        raise ValueError("control run is missing")
    if not isinstance(summary, Mapping):
        raise ValueError("control run summary is missing")
    if not isinstance(evidence, Mapping):
        raise ValueError("direct control evidence is missing")

    metadata = run.get("metadata")
    metadata = metadata if isinstance(metadata, Mapping) else {}
    strict = metadata.get("bronze_acceptance_replay")
    strict = strict if isinstance(strict, Mapping) else {}
    if str(run.get("status") or "").casefold() != "succeeded":
        failures.append("run_not_succeeded")
    if str(run.get("run_type") or "").casefold() != "replay":
        failures.append("run_not_replay")
    for metric in (
        "request_limit",
        "byte_limit",
        "requests_used",
        "bytes_used",
    ):
        if int(run.get(metric) or 0) != 0:
            failures.append(f"{metric}_not_zero")
    if (
        str(strict.get("status") or "").casefold() != "passed"
        or str(strict.get("processing_control_run_id") or "") != str(run_id)
    ):
        failures.append("strict_acceptance_missing")
    for metric in (
        "requests_reserved",
        "bytes_reserved",
        "unprocessed_raw_count",
    ):
        if int(summary.get(metric) or 0) != 0:
            failures.append(f"{metric}_not_zero")
    traffic = summary.get("traffic_totals")
    if not isinstance(traffic, Mapping) or int(
        traffic.get("network_attempts") or 0
    ) != 0:
        failures.append("network_attempts_not_zero")

    targets = evidence.get("targets")
    datasets = evidence.get("datasets")
    if not isinstance(targets, Sequence) or isinstance(targets, (str, bytes)):
        raise ValueError("direct target evidence is missing")
    if not isinstance(datasets, Sequence) or isinstance(datasets, (str, bytes)):
        raise ValueError("direct dataset evidence is missing")
    refresh_ids = [str(item.get("logical_refresh_id") or "") for item in targets]
    if not targets:
        failures.append("target_evidence_empty")
    if any(not refresh_id for refresh_id in refresh_ids):
        failures.append("logical_refresh_id_missing")
    if len(set(refresh_ids)) != len(refresh_ids):
        failures.append("logical_refresh_id_duplicate")
    if not datasets:
        failures.append("dataset_evidence_empty")
    required_match_datasets = {
        f"typed:{dataset}" for dataset in MATCH_DATASET_TABLES
    }
    match_targets = {
        str(item.get("target_id") or "")
        for item in targets
        if str(item.get("page_kind") or "").casefold() == "match"
    }
    datasets_by_target = {
        target_id: {
            str(item.get("dataset") or "")
            for item in datasets
            if str(item.get("target_id") or "") == target_id
        }
        for target_id in match_targets
    }
    if any(
        not required_match_datasets.issubset(
            datasets_by_target.get(target_id, set())
        )
        for target_id in match_targets
    ):
        failures.append("match_dataset_manifest_set_incomplete")
    for item in targets:
        target_status = item.get("target_status", item.get("status"))
        if str(target_status or "").casefold() != "succeeded":
            failures.append("target_not_succeeded")
            break
    for item in targets:
        observation_status = item.get("observation_status")
        if observation_status is not None and (
            str(observation_status).casefold() != "succeeded"
            or str(item.get("generic_status") or "").casefold()
            != "succeeded"
            or str(item.get("typed_status") or "").casefold()
            not in {"succeeded", "skipped"}
            or str(item.get("stateful_status") or "").casefold()
            not in {"succeeded", "skipped"}
            or str(item.get("validation_status") or "").casefold()
            != "succeeded"
        ):
            failures.append("observation_not_succeeded")
            break
    for item in datasets:
        if (
            str(item.get("parse_status") or "").casefold() != "succeeded"
            or str(item.get("persistence_status") or "").casefold()
            not in {"succeeded", "skipped"}
            or str(item.get("validation_status") or "").casefold()
            not in {"succeeded", "skipped"}
        ):
            failures.append("dataset_manifest_not_succeeded")
            break
        if str(item.get("dataset") or "") not in required_match_datasets:
            continue
        availability = str(item.get("availability") or "").casefold()
        row_count = int(item.get("row_count") or 0)
        if availability not in {
            "available",
            "empty",
            "restricted",
            "not_applicable",
        }:
            failures.append("dataset_availability_unproved")
            break
        if availability == "available" and row_count <= 0:
            failures.append("available_dataset_has_no_rows")
            break
        if availability in {"empty", "restricted", "not_applicable"} and (
            row_count != 0
            or not str(item.get("empty_reason") or "").strip()
        ):
            failures.append("empty_dataset_has_no_reason")
            break

    observations = sorted(
        (
            {
                "target_id": item.get("target_id"),
                "status": item.get("status"),
                "target_status": item.get("target_status"),
                "page_kind": item.get("page_kind"),
                "source_ids": item.get("source_ids"),
                "content_hash": item.get("content_hash"),
                "observation_status": item.get("observation_status"),
                "generic_status": item.get("generic_status"),
                "typed_status": item.get("typed_status"),
                "stateful_status": item.get("stateful_status"),
                "validation_status": item.get("validation_status"),
            }
            for item in targets
        ),
        key=lambda item: str(item.get("target_id") or ""),
    )
    manifests = sorted(
        (
            {
                key: item.get(key)
                for key in (
                    "target_id",
                    "dataset",
                    "availability",
                    "parse_status",
                    "persistence_status",
                    "validation_status",
                    "row_count",
                )
            }
            for item in datasets
        ),
        key=lambda item: (
            str(item.get("target_id") or ""),
            str(item.get("dataset") or ""),
        ),
    )
    latest_state = sorted(
        (
            {
                "target_id": item.get("target_id"),
                "status": item.get("status"),
                "target_status": item.get("target_status"),
                "frontier_state": item.get("frontier_state"),
                "last_content_hash": item.get("last_content_hash"),
                "evidence_class": item.get("evidence_class"),
            }
            for item in targets
        ),
        key=lambda item: str(item.get("target_id") or ""),
    )
    return ControlRunEvidence(
        run_id=str(run_id),
        evidence_source=evidence_source,
        proxy_requests=int(run.get("requests_used") or 0),
        proxy_bytes=int(run.get("bytes_used") or 0),
        logical_refreshes=len(refresh_ids),
        dataset_manifests=len(datasets),
        observation_sha256=_stable_sha256(observations),
        manifest_sha256=_stable_sha256(manifests),
        latest_state_sha256=_stable_sha256(latest_state),
        valid=not failures,
        failures=tuple(failures),
    )


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


def _normalized_column_signature(
    columns: Mapping[str, str],
) -> tuple[tuple[str, str], ...]:
    return tuple(
        sorted(
            (
                str(name).casefold(),
                " ".join(str(column_type).casefold().split()),
            )
            for name, column_type in columns.items()
        )
    )


def _table_has_rows(
    manager: CountingTrinoTableManager, schema: str, table: str
) -> bool:
    qualified = validate_catalog_qualified_name(manager.catalog, schema, table)
    rows = manager._execute(f"SELECT 1 FROM {qualified} LIMIT 1", fetch=True)
    return bool(rows)


def _table_has_non_sentinel_rows(
    manager: CountingTrinoTableManager,
    schema: str,
    table: str,
    match_id: str,
) -> bool:
    qualified = validate_catalog_qualified_name(manager.catalog, schema, table)
    predicate_column = _sentinel_column(table)
    rows = manager._execute(
        f"SELECT 1 FROM {qualified} "
        f"WHERE {predicate_column} IS DISTINCT FROM ? LIMIT 1",
        fetch=True,
        params=(_sentinel_value(table, match_id),),
    )
    return bool(rows)


def _close_preflight_manager(manager: CountingTrinoTableManager) -> None:
    connection = getattr(manager, "_conn", None)
    if connection is None:
        return
    try:
        connection.close()
    except Exception:  # pragma: no cover - diagnostic cleanup only
        pass
    finally:
        manager._conn = None


def _preflight_schemas(config: BenchmarkConfig) -> None:
    """Require isolated schemas, optionally containing only sentinels."""

    manager = CountingTrinoTableManager()
    try:
        schemas = (config.sequential_schema,)
        if config.batch_schema is not None:
            schemas += (config.batch_schema,)
        signatures: dict[str, dict[str, tuple[tuple[str, str], ...]]] = {}
        for schema in schemas:
            missing = [
                table
                for table in BENCHMARK_TABLES
                if not manager.table_exists(schema, table)
            ]
            if missing:
                raise ValueError(
                    f"benchmark schema {schema!r} is missing required benchmark "
                    "tables: "
                    + ", ".join(missing)
                )
            table_signatures = {}
            for table in BENCHMARK_TABLES:
                table_signatures[table] = _normalized_column_signature(
                    manager.get_table_columns(schema, table)
                )
                if config.sentinel_match_id is None:
                    if _table_has_rows(manager, schema, table):
                        raise ValueError(
                            f"benchmark schema {schema!r} table {table!r} must "
                            "contain zero rows"
                        )
                elif _table_has_non_sentinel_rows(
                    manager,
                    schema,
                    table,
                    config.sentinel_match_id,
                ):
                    raise ValueError(
                        f"benchmark schema {schema!r} table {table!r} contains "
                        "rows outside the configured sentinel"
                    )
            signatures[schema] = table_signatures

            if config.sentinel_match_id is not None:
                sentinels = _sentinel_digests(
                    manager, schema, config.sentinel_match_id
                )
                missing_sentinels = [
                    table
                    for table, evidence in sentinels.items()
                    if not evidence.present or int(evidence.rows or 0) <= 0
                ]
                if missing_sentinels:
                    raise ValueError(
                        f"benchmark schema {schema!r} is missing sentinel rows: "
                        + ", ".join(missing_sentinels)
                    )

        if config.batch_schema is None:
            return
        for table in BENCHMARK_TABLES:
            if (
                signatures[config.sequential_schema][table]
                != signatures[config.batch_schema][table]
            ):
                raise ValueError(
                    "column/type signature mismatch for benchmark table "
                    f"{table!r} between {config.sequential_schema!r} and "
                    f"{config.batch_schema!r}"
                )
    finally:
        _close_preflight_manager(manager)


def _run_persistence(
    *,
    schema: str,
    items: Sequence[_ReplayItem],
    iterations: int,
    runner: Any,
    sentinel_match_id: str | None = None,
) -> PersistenceRun:
    manager = CountingTrinoTableManager()
    try:
        snapshots_before = _snapshot_ids(manager, schema)
        sentinel_before = (
            {}
            if sentinel_match_id is None
            else _sentinel_digests(manager, schema, sentinel_match_id)
        )
        counts_before = manager.statement_counts()
        elapsed = runner(
            manager, schema=schema, items=items, iterations=iterations
        )
        counts_after = manager.statement_counts()
        statement_counts = StatementCounts(
            execute=counts_after.execute - counts_before.execute,
            execute_committing=(
                counts_after.execute_committing
                - counts_before.execute_committing
            ),
        )
        snapshots_after = _snapshot_ids(manager, schema)
        sentinel_after = (
            {}
            if sentinel_match_id is None
            else _sentinel_digests(manager, schema, sentinel_match_id)
        )
        return PersistenceRun(
            seconds=elapsed,
            statement_counts=statement_counts,
            table_digests=_table_digests(manager, schema),
            snapshots_before=snapshots_before,
            snapshots_after=snapshots_after,
            snapshot_deltas=_snapshot_deltas(
                snapshots_before, snapshots_after
            ),
            sentinel_before=sentinel_before,
            sentinel_after=sentinel_after,
        )
    finally:
        _close_preflight_manager(manager)


def _control_evidence_equivalent(
    sequential: ControlRunEvidence, batch: ControlRunEvidence
) -> bool:
    return (
        sequential.valid
        and batch.valid
        and sequential.logical_refreshes == batch.logical_refreshes
        and sequential.dataset_manifests == batch.dataset_manifests
        and sequential.observation_sha256 == batch.observation_sha256
        and sequential.manifest_sha256 == batch.manifest_sha256
        and sequential.latest_state_sha256 == batch.latest_state_sha256
    )


def _sentinels_preserved(*runs: PersistenceRun) -> bool:
    if not runs:
        return False
    for run in runs:
        if set(run.sentinel_before) != set(BENCHMARK_TABLES):
            return False
        if set(run.sentinel_after) != set(BENCHMARK_TABLES):
            return False
        for table in BENCHMARK_TABLES:
            before = run.sentinel_before[table]
            after = run.sentinel_after[table]
            if not before.present or int(before.rows or 0) <= 0:
                return False
            if before != after:
                return False
    return True


def _strict_config_error(config: BenchmarkConfig) -> str | None:
    if not config.strict_acceptance:
        return None
    missing = []
    if not config.sequential_control_run_id:
        missing.append("sequential_control_run_id")
    if not config.batch_control_run_id:
        missing.append("batch_control_run_id")
    if not config.sentinel_match_id:
        missing.append("sentinel_match_id")
    if missing:
        return "strict acceptance requires " + ", ".join(missing)
    return None


def _validate_acceptance_baseline(
    baseline: AcceptanceBaseline,
    *,
    schema: str,
    sentinel_match_id: str,
) -> None:
    if baseline.schema.casefold() != schema.casefold():
        raise ValueError("acceptance baseline schema does not match")
    if baseline.sentinel_match_id != sentinel_match_id:
        raise ValueError("acceptance baseline sentinel does not match")
    if set(baseline.snapshots) != set(BENCHMARK_TABLES):
        raise ValueError("acceptance baseline snapshots are incomplete")
    if any(snapshot_id is None for snapshot_id in baseline.snapshots.values()):
        raise ValueError("acceptance baseline snapshot IDs are missing")
    if set(baseline.sentinels) != set(BENCHMARK_TABLES):
        raise ValueError("acceptance baseline sentinels are incomplete")


def evaluate_existing_pipeline_acceptance(
    *,
    manager: CountingTrinoTableManager,
    control: Any,
    sequential_schema: str,
    batch_schema: str,
    sequential_control_run_id: str,
    batch_control_run_id: str,
    sequential_seconds: float,
    batch_seconds: float,
    matches: int,
    sequential_statement_counts: StatementCounts,
    batch_statement_counts: StatementCounts,
    sequential_baseline: AcceptanceBaseline,
    batch_baseline: AcceptanceBaseline,
    sentinel_match_id: str,
    min_speedup: float = 4.0,
    max_seconds_per_match: float = 20.0,
) -> BenchmarkReport:
    """Verify two already-completed real pipeline replays without writes."""

    if matches < 1:
        raise ValueError("matches must be at least 1")
    if sequential_seconds <= 0 or batch_seconds <= 0:
        raise ValueError("pipeline elapsed seconds must be positive")
    if sequential_schema.casefold() == batch_schema.casefold():
        raise ValueError(
            "sequential and batch schemas must be case-insensitively distinct"
        )
    _validate_acceptance_baseline(
        sequential_baseline,
        schema=sequential_schema,
        sentinel_match_id=sentinel_match_id,
    )
    _validate_acceptance_baseline(
        batch_baseline,
        schema=batch_schema,
        sentinel_match_id=sentinel_match_id,
    )

    sequential_after = _snapshot_ids(manager, sequential_schema)
    batch_after = _snapshot_ids(manager, batch_schema)
    sequential_sentinel_after = _sentinel_digests(
        manager, sequential_schema, sentinel_match_id
    )
    batch_sentinel_after = _sentinel_digests(
        manager, batch_schema, sentinel_match_id
    )
    sequential = PersistenceRun(
        seconds=float(sequential_seconds),
        statement_counts=sequential_statement_counts,
        table_digests=_table_digests(manager, sequential_schema),
        snapshots_before=dict(sequential_baseline.snapshots),
        snapshots_after=sequential_after,
        snapshot_deltas=_snapshot_deltas(
            sequential_baseline.snapshots, sequential_after
        ),
        sentinel_before=dict(sequential_baseline.sentinels),
        sentinel_after=sequential_sentinel_after,
    )
    batch = PersistenceRun(
        seconds=float(batch_seconds),
        statement_counts=batch_statement_counts,
        table_digests=_table_digests(manager, batch_schema),
        snapshots_before=dict(batch_baseline.snapshots),
        snapshots_after=batch_after,
        snapshot_deltas=_snapshot_deltas(
            batch_baseline.snapshots, batch_after
        ),
        sentinel_before=dict(batch_baseline.sentinels),
        sentinel_after=batch_sentinel_after,
    )
    table_diffs = _bidirectional_table_diffs(
        manager, sequential_schema, batch_schema
    )
    exact_tables = all(
        diff.sequential_minus_batch == 0
        and diff.batch_minus_sequential == 0
        for diff in table_diffs.values()
    )
    equivalent = exact_tables and _digests_equivalent(
        sequential.table_digests, batch.table_digests
    )
    control_sequential = _control_run_evidence(
        control, sequential_control_run_id
    )
    control_batch = _control_run_evidence(control, batch_control_run_id)
    control_equivalent = _control_evidence_equivalent(
        control_sequential, control_batch
    )
    sentinels_preserved = _sentinels_preserved(sequential, batch)
    snapshots_complete = all(
        set(run.snapshots_before) == set(BENCHMARK_TABLES)
        and set(run.snapshots_after) == set(BENCHMARK_TABLES)
        and any(
            run.snapshots_before[table] != run.snapshots_after[table]
            for table in BENCHMARK_TABLES
        )
        for run in (sequential, batch)
    )
    proxy_requests = (
        control_sequential.proxy_requests + control_batch.proxy_requests
    )
    proxy_bytes = control_sequential.proxy_bytes + control_batch.proxy_bytes
    gate = evaluate_gate(
        sequential_seconds=sequential.seconds,
        batch_seconds=batch.seconds,
        matches=matches,
        equivalent=equivalent,
        proxy_requests=proxy_requests,
        proxy_bytes=proxy_bytes,
        min_speedup=min_speedup,
        max_seconds_per_match=max_seconds_per_match,
    )
    passed = bool(
        gate.passed
        and snapshots_complete
        and sentinels_preserved
        and control_equivalent
    )
    return BenchmarkReport(
        matches=matches,
        iterations=1,
        sequential=sequential,
        batch=batch,
        equivalent=equivalent,
        proxy_requests=proxy_requests,
        proxy_bytes=proxy_bytes,
        gate=gate,
        table_diffs=table_diffs,
        control_sequential=control_sequential,
        control_batch=control_batch,
        control_equivalent=control_equivalent,
        sentinels_preserved=sentinels_preserved,
        passed=passed,
    )


def run_benchmark(
    config: BenchmarkConfig, *, control: Any | None = None
) -> BenchmarkReport:
    """Persist an offline fixture cohort in sequential and optional batch mode."""

    if config.iterations < 1:
        raise ValueError("iterations must be at least 1")
    strict_error = _strict_config_error(config)
    if strict_error is not None:
        raise ValueError(strict_error)
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
            sentinel_match_id=config.sentinel_match_id,
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
                passed=None,
            )

        batch = _run_persistence(
            schema=config.batch_schema,
            items=items,
            iterations=config.iterations,
            runner=_run_batch,
            sentinel_match_id=config.sentinel_match_id,
        )
        comparison_manager = CountingTrinoTableManager()
        try:
            table_diffs = _bidirectional_table_diffs(
                comparison_manager,
                config.sequential_schema,
                config.batch_schema,
            )
        finally:
            _close_preflight_manager(comparison_manager)
        exact_table_equivalence = all(
            diff.sequential_minus_batch == 0
            and diff.batch_minus_sequential == 0
            for diff in table_diffs.values()
        )
        equivalent = exact_table_equivalence and _digests_equivalent(
            sequential.table_digests, batch.table_digests
        )

        control_sequential = None
        control_batch = None
        control_equivalent = None
        if config.sequential_control_run_id or config.batch_control_run_id:
            if not (
                config.sequential_control_run_id
                and config.batch_control_run_id
            ):
                raise ValueError(
                    "both sequential and batch control run IDs are required"
                )
            if control is None:
                from scrapers.fbref.control import ControlStore

                control = ControlStore.from_env()
            control_sequential = _control_run_evidence(
                control, config.sequential_control_run_id
            )
            control_batch = _control_run_evidence(
                control, config.batch_control_run_id
            )
            control_equivalent = _control_evidence_equivalent(
                control_sequential, control_batch
            )

        sentinels_preserved = (
            None
            if config.sentinel_match_id is None
            else _sentinels_preserved(sequential, batch)
        )
        proxy_requests = 0
        proxy_bytes = 0
        if control_sequential is not None and control_batch is not None:
            proxy_requests = (
                control_sequential.proxy_requests
                + control_batch.proxy_requests
            )
            proxy_bytes = (
                control_sequential.proxy_bytes + control_batch.proxy_bytes
            )
        gate = evaluate_gate(
            sequential_seconds=sequential.seconds,
            batch_seconds=batch.seconds,
            matches=len(items),
            equivalent=equivalent,
            proxy_requests=proxy_requests,
            proxy_bytes=proxy_bytes,
            min_speedup=config.min_speedup,
            max_seconds_per_match=config.max_seconds_per_match,
        )
        passed = gate.passed
        if config.strict_acceptance:
            snapshots_complete = all(
                set(run.snapshots_before) == set(BENCHMARK_TABLES)
                and set(run.snapshots_after) == set(BENCHMARK_TABLES)
                for run in (sequential, batch)
            )
            passed = bool(
                passed
                and snapshots_complete
                and sentinels_preserved
                and control_equivalent
            )
        return BenchmarkReport(
            matches=len(items),
            iterations=config.iterations,
            sequential=sequential,
            batch=batch,
            equivalent=equivalent,
            proxy_requests=proxy_requests,
            proxy_bytes=proxy_bytes,
            gate=gate,
            table_diffs=table_diffs,
            control_sequential=control_sequential,
            control_batch=control_batch,
            control_equivalent=control_equivalent,
            sentinels_preserved=sentinels_preserved,
            passed=passed,
        )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--capture-baseline", action="store_true")
    modes.add_argument("--compare-existing", action="store_true")
    parser.add_argument("--html-dir", type=Path)
    parser.add_argument("--sequential-schema", required=True)
    parser.add_argument("--batch-schema")
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--min-speedup", type=float, default=4.0)
    parser.add_argument("--max-seconds-per-match", type=float, default=20.0)
    parser.add_argument("--strict-acceptance", action="store_true")
    parser.add_argument("--sequential-control-run-id")
    parser.add_argument("--batch-control-run-id")
    parser.add_argument("--sentinel-match-id")
    parser.add_argument("--sequential-baseline", type=Path)
    parser.add_argument("--batch-baseline", type=Path)
    parser.add_argument("--sequential-seconds", type=float)
    parser.add_argument("--batch-seconds", type=float)
    parser.add_argument("--matches", type=int)
    parser.add_argument("--sequential-execute-statements", type=int, default=0)
    parser.add_argument("--sequential-committing-statements", type=int, default=0)
    parser.add_argument("--batch-execute-statements", type=int, default=0)
    parser.add_argument("--batch-committing-statements", type=int, default=0)
    return parser.parse_args(argv)


def _read_baseline(path: Path) -> AcceptanceBaseline:
    try:
        if path.stat().st_size > 1024 * 1024:
            raise ValueError("acceptance baseline is too large")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise ValueError("acceptance baseline is unreadable") from None
    if not isinstance(payload, Mapping):
        raise ValueError("acceptance baseline is invalid")
    return AcceptanceBaseline.from_mapping(payload)


def _require_args(args: argparse.Namespace, *names: str) -> None:
    missing = [name for name in names if getattr(args, name) is None]
    if missing:
        raise ValueError("missing required arguments: " + ", ".join(missing))


def _render_output(report: Any, output: Path | None) -> str:
    payload = report.to_dict() if hasattr(report, "to_dict") else report
    rendered = json.dumps(payload, indent=2, sort_keys=True)
    if output is not None:
        output.write_text(rendered + "\n", encoding="utf-8")
        output.chmod(0o600)
    return rendered


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        if args.capture_baseline:
            _require_args(args, "sentinel_match_id", "output")
            manager = CountingTrinoTableManager()
            try:
                with _suppress_trino_sql_logs():
                    baseline = capture_acceptance_baseline(
                        manager,
                        schema=args.sequential_schema,
                        sentinel_match_id=args.sentinel_match_id,
                    )
            finally:
                _close_preflight_manager(manager)
            print(_render_output(baseline, args.output))
            return 0

        if args.compare_existing:
            _require_args(
                args,
                "batch_schema",
                "sequential_control_run_id",
                "batch_control_run_id",
                "sentinel_match_id",
                "sequential_baseline",
                "batch_baseline",
                "sequential_seconds",
                "batch_seconds",
                "matches",
            )
            from scrapers.fbref.control import ControlStore

            manager = CountingTrinoTableManager()
            try:
                with _suppress_trino_sql_logs():
                    report = evaluate_existing_pipeline_acceptance(
                        manager=manager,
                        control=ControlStore.from_env(),
                        sequential_schema=args.sequential_schema,
                        batch_schema=args.batch_schema,
                        sequential_control_run_id=(
                            args.sequential_control_run_id
                        ),
                        batch_control_run_id=args.batch_control_run_id,
                        sequential_seconds=args.sequential_seconds,
                        batch_seconds=args.batch_seconds,
                        matches=args.matches,
                        sequential_statement_counts=StatementCounts(
                            args.sequential_execute_statements,
                            args.sequential_committing_statements,
                        ),
                        batch_statement_counts=StatementCounts(
                            args.batch_execute_statements,
                            args.batch_committing_statements,
                        ),
                        sequential_baseline=_read_baseline(
                            args.sequential_baseline
                        ),
                        batch_baseline=_read_baseline(args.batch_baseline),
                        sentinel_match_id=args.sentinel_match_id,
                        min_speedup=args.min_speedup,
                        max_seconds_per_match=args.max_seconds_per_match,
                    )
            finally:
                _close_preflight_manager(manager)
            print(_render_output(report, args.output))
            return 0 if report.passed else 1

        _require_args(args, "html_dir")
        report = run_benchmark(
            BenchmarkConfig(
                html_dir=args.html_dir,
                sequential_schema=args.sequential_schema,
                batch_schema=args.batch_schema,
                iterations=args.iterations,
                min_speedup=args.min_speedup,
                max_seconds_per_match=args.max_seconds_per_match,
                strict_acceptance=args.strict_acceptance,
                sequential_control_run_id=args.sequential_control_run_id,
                batch_control_run_id=args.batch_control_run_id,
                sentinel_match_id=args.sentinel_match_id,
            )
        )
    except (BatchPersistenceUnavailableError, ValueError) as error:
        raise SystemExit(str(error)) from error

    print(_render_output(report, args.output))
    return 0 if report.passed is not False else 1


if __name__ == "__main__":
    raise SystemExit(main())
