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
import math
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
GENERIC_BENCHMARK_TABLES = frozenset(
    {TABLE_CELLS_TABLE, TABLE_INVENTORY_TABLE, PAGE_MANIFEST_TABLE}
)
ACCEPTANCE_RUN_TOKEN = "__fbref_acceptance_run__"
OFFLINE_CONTEXT = TypedSourceContext(
    source_competition_id="9",
    source_season_id="2025-2026",
    competition_name="Premier League",
    season_label="2025-2026",
)


def _is_sha256(value: Any) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _pipeline_metrics_sha256(value: Mapping[str, Any]) -> str:
    core = {
        key: item for key, item in value.items() if key != "artifact_sha256"
    }
    rendered = json.dumps(core, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


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
class PipelineRunMetrics:
    """Run-bound timing and Trino statement evidence from the replay runner."""

    control_run_id: str
    schema: str
    mode: str
    elapsed_seconds: float
    match_count: int
    match_keys_sha256: str
    statement_counts: StatementCounts
    artifact_sha256: str
    schema_version: str = "fbref-pipeline-run-metrics-v1"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def anchored(
        cls,
        *,
        control_run_id: str,
        schema: str,
        mode: str,
        elapsed_seconds: float,
        match_count: int,
        match_keys_sha256: str,
        statement_counts: StatementCounts,
    ) -> "PipelineRunMetrics":
        core = {
            "schema_version": "fbref-pipeline-run-metrics-v1",
            "control_run_id": control_run_id,
            "schema": schema,
            "mode": mode,
            "elapsed_seconds": elapsed_seconds,
            "match_count": match_count,
            "match_keys_sha256": match_keys_sha256,
            "statement_counts": asdict(statement_counts),
        }
        return cls.from_mapping(
            {**core, "artifact_sha256": _pipeline_metrics_sha256(core)}
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "PipelineRunMetrics":
        required_fields = {
            "schema_version",
            "control_run_id",
            "schema",
            "mode",
            "elapsed_seconds",
            "match_count",
            "match_keys_sha256",
            "statement_counts",
            "artifact_sha256",
        }
        if set(value) != required_fields:
            raise ValueError("pipeline metrics fields are invalid")
        if value.get("schema_version") != "fbref-pipeline-run-metrics-v1":
            raise ValueError("pipeline metrics schema version is invalid")
        control_run_id = value.get("control_run_id")
        schema = value.get("schema")
        mode = value.get("mode")
        elapsed_seconds = value.get("elapsed_seconds")
        match_count = value.get("match_count")
        match_keys_sha256 = value.get("match_keys_sha256")
        artifact_sha256 = value.get("artifact_sha256")
        counts = value.get("statement_counts")
        if not isinstance(control_run_id, str) or not control_run_id.strip():
            raise ValueError("pipeline metrics control run ID is invalid")
        if not isinstance(schema, str) or not schema.strip():
            raise ValueError("pipeline metrics schema is invalid")
        if mode not in {"sequential", "batch"}:
            raise ValueError("pipeline metrics mode is invalid")
        if (
            isinstance(elapsed_seconds, bool)
            or not isinstance(elapsed_seconds, (int, float))
            or not math.isfinite(float(elapsed_seconds))
            or float(elapsed_seconds) <= 0
        ):
            raise ValueError("pipeline metrics elapsed seconds are invalid")
        if (
            isinstance(match_count, bool)
            or not isinstance(match_count, int)
            or match_count < 1
        ):
            raise ValueError("pipeline metrics match count is invalid")
        if not _is_sha256(match_keys_sha256):
            raise ValueError("pipeline metrics match digest is invalid")
        if not _is_sha256(artifact_sha256) or artifact_sha256 != (
            _pipeline_metrics_sha256(value)
        ):
            raise ValueError("pipeline metrics artifact digest is invalid")
        if not isinstance(counts, Mapping):
            raise ValueError("pipeline metrics statement counts are invalid")
        if set(counts) != {"execute", "execute_committing"}:
            raise ValueError("pipeline metrics statement counts are invalid")
        execute = counts.get("execute")
        committing = counts.get("execute_committing")
        if any(
            isinstance(item, bool) or not isinstance(item, int) or item < 0
            for item in (execute, committing)
        ) or int(execute) + int(committing) <= 0:
            raise ValueError("pipeline metrics statement counts are invalid")
        return cls(
            control_run_id=control_run_id,
            schema=schema,
            mode=mode,
            elapsed_seconds=float(elapsed_seconds),
            match_count=match_count,
            match_keys_sha256=match_keys_sha256,
            statement_counts=StatementCounts(
                execute=execute,
                execute_committing=committing,
            ),
            artifact_sha256=artifact_sha256,
        )


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
    source_run_id: str
    proxy_requests: int
    proxy_bytes: int
    logical_refreshes: int
    dataset_manifests: int
    match_targets: int
    match_keys_sha256: str
    pipeline_metrics: PipelineRunMetrics | None
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
            present = item.get("present")
            rows = item.get("rows")
            sha256 = item.get("sha256")
            if not isinstance(present, bool):
                raise ValueError("acceptance sentinel presence is invalid")
            if rows is not None and (
                isinstance(rows, bool) or not isinstance(rows, int) or rows < 0
            ):
                raise ValueError("acceptance sentinel row count is invalid")
            if sha256 is not None and (
                not isinstance(sha256, str)
                or len(sha256) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in sha256
                )
            ):
                raise ValueError("acceptance sentinel digest is invalid")
            if present and (rows is None or sha256 is None):
                raise ValueError("acceptance sentinel evidence is incomplete")
            parsed_sentinels[str(table)] = TableDigest(
                present=present,
                rows=rows,
                sha256=sha256,
            )
        parsed_snapshots = {}
        for table, snapshot_id in snapshots.items():
            if snapshot_id is not None and (
                isinstance(snapshot_id, bool)
                or not isinstance(snapshot_id, int)
                or snapshot_id < 0
            ):
                raise ValueError("acceptance snapshot ID is invalid")
            parsed_snapshots[str(table)] = snapshot_id
        return cls(
            schema=str(value.get("schema") or ""),
            sentinel_match_id=str(value.get("sentinel_match_id") or ""),
            snapshots=parsed_snapshots,
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
    columns: Iterable[str],
    rows: Iterable[Sequence[Any]],
    *,
    table: str | None = None,
    expected_run_id: str | None = None,
    sentinel_match_id: str | None = None,
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
        if expected_run_id is not None:
            if table is None or sentinel_match_id is None:
                raise ValueError("run-normalized digest requires a sentinel")
            names = {name.casefold(): name for name in record}
            sentinel_column = names[_sentinel_column(table)]
            lineage_column = names[_lineage_column(table)]
            if (
                record[sentinel_column]
                != _sentinel_value(table, sentinel_match_id)
                and record[lineage_column] == expected_run_id
            ):
                record[lineage_column] = ACCEPTANCE_RUN_TOKEN
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
    manager: CountingTrinoTableManager,
    schema: str,
    *,
    expected_run_id: str | None = None,
    sentinel_match_id: str | None = None,
) -> dict[str, TableDigest]:
    if (expected_run_id is None) != (sentinel_match_id is None):
        raise ValueError("run-normalized digest arguments are incomplete")
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
        normalized = _normalized_rows(
            kept_columns,
            rows or (),
            table=table,
            expected_run_id=expected_run_id,
            sentinel_match_id=sentinel_match_id,
        )
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


def _lineage_column(table: str) -> str:
    return "run_id" if table in GENERIC_BENCHMARK_TABLES else "_batch_id"


def _comparison_projection(
    columns: Sequence[str],
    *,
    table: str,
    expected_run_id: str | None,
    sentinel_match_id: str | None,
) -> tuple[str, tuple[Any, ...]]:
    if expected_run_id is None:
        if sentinel_match_id is not None:
            raise ValueError("run-aware comparison arguments are incomplete")
        return ", ".join(f'"{column}"' for column in columns), ()
    if not expected_run_id or sentinel_match_id is None:
        raise ValueError("run-aware comparison arguments are incomplete")
    names = {column.casefold(): column for column in columns}
    lineage_name = names.get(_lineage_column(table))
    sentinel_name = names.get(_sentinel_column(table))
    if lineage_name is None or sentinel_name is None:
        raise ValueError(f"benchmark table {table!r} has no lineage columns")
    selected = []
    params: list[Any] = []
    for column in columns:
        if column == lineage_name:
            selected.append(
                "CASE WHEN "
                f'"{sentinel_name}" IS DISTINCT FROM ? '
                f'AND "{lineage_name}" = ? '
                f"THEN '{ACCEPTANCE_RUN_TOKEN}' "
                f'ELSE "{lineage_name}" END AS "{lineage_name}"'
            )
            params.extend(
                (_sentinel_value(table, sentinel_match_id), expected_run_id)
            )
        else:
            selected.append(f'"{column}"')
    return ", ".join(selected), tuple(params)


def _validate_run_lineage(
    manager: CountingTrinoTableManager,
    *,
    schema: str,
    expected_run_id: str,
    sentinel_match_id: str,
) -> None:
    """Reject every non-sentinel row not owned by the expected replay."""

    if not expected_run_id or not sentinel_match_id:
        raise ValueError("expected run lineage and sentinel must be nonblank")
    for table in BENCHMARK_TABLES:
        if not manager.table_exists(schema, table):
            raise ValueError(f"missing required benchmark table {table!r}")
        columns = manager.get_table_columns(schema, table)
        names = {name.casefold(): name for name in columns}
        lineage_name = names.get(_lineage_column(table))
        sentinel_name = names.get(_sentinel_column(table))
        if lineage_name is None or sentinel_name is None:
            raise ValueError(f"benchmark table {table!r} has no lineage columns")
        qualified = validate_catalog_qualified_name(
            manager.catalog, schema, table
        )
        rows = manager._execute(
            f"SELECT count(*) FROM {qualified} "
            f'WHERE "{sentinel_name}" IS DISTINCT FROM ? '
            f'AND "{lineage_name}" IS DISTINCT FROM ?',
            fetch=True,
            params=(
                _sentinel_value(table, sentinel_match_id),
                expected_run_id,
            ),
        )
        unexpected = _single_count(
            rows, label=f"unexpected lineage {schema}.{table}"
        )
        if unexpected:
            raise ValueError(
                "unexpected run lineage in benchmark table "
                f"{schema}.{table}: {unexpected} rows"
            )


def _validate_trino_match_cohort(
    manager: CountingTrinoTableManager,
    *,
    schema: str,
    expected_run_id: str,
    sentinel_match_id: str,
    control_evidence: ControlRunEvidence,
) -> None:
    """Bind the physical page-manifest match set to direct control evidence."""

    qualified = validate_catalog_qualified_name(
        manager.catalog, schema, PAGE_MANIFEST_TABLE
    )
    rows = manager._execute(
        "SELECT regexp_extract(\"target_id\", '^fbref:match:(.+)$', 1) "
        f"AS acceptance_match_id FROM {qualified} "
        'WHERE "target_id" IS DISTINCT FROM ? '
        'AND "run_id" = ? '
        "AND regexp_like(\"target_id\", '^fbref:match:.+$') "
        "ORDER BY acceptance_match_id",
        fetch=True,
        params=(
            _sentinel_value(PAGE_MANIFEST_TABLE, sentinel_match_id),
            expected_run_id,
        ),
    )
    match_keys = []
    for row in rows or ():
        if not isinstance(row, Sequence) or len(row) != 1:
            raise RuntimeError("Unexpected Trino match-cohort result")
        match_id = str(row[0] or "")
        if not match_id:
            raise ValueError("Trino match cohort contains an invalid key")
        match_keys.append(match_id)
    if (
        len(match_keys) != control_evidence.match_targets
        or len(set(match_keys)) != len(match_keys)
        or _stable_sha256(sorted(match_keys))
        != control_evidence.match_keys_sha256
    ):
        raise ValueError(
            f"Trino match cohort for {schema!r} differs from direct control"
        )


def _except_all_count(
    manager: CountingTrinoTableManager,
    *,
    left_schema: str,
    right_schema: str,
    table: str,
    columns: Sequence[str],
    left_run_id: str | None = None,
    right_run_id: str | None = None,
    sentinel_match_id: str | None = None,
) -> int:
    left = validate_catalog_qualified_name(
        manager.catalog, left_schema, table
    )
    right = validate_catalog_qualified_name(
        manager.catalog, right_schema, table
    )
    left_selected, left_params = _comparison_projection(
        columns,
        table=table,
        expected_run_id=left_run_id,
        sentinel_match_id=sentinel_match_id,
    )
    right_selected, right_params = _comparison_projection(
        columns,
        table=table,
        expected_run_id=right_run_id,
        sentinel_match_id=sentinel_match_id,
    )
    sql = (
        "SELECT count(*) FROM ("
        f"SELECT {left_selected} FROM {left} "
        "EXCEPT ALL "
        f"SELECT {right_selected} FROM {right}"
        ") AS directional_diff"
    )
    params = left_params + right_params
    if params:
        rows = manager._execute(sql, fetch=True, params=params)
    else:
        rows = manager._execute(sql, fetch=True)
    return _single_count(rows, label=f"{left_schema}-{right_schema} {table}")


def _bidirectional_table_diffs(
    manager: CountingTrinoTableManager,
    sequential_schema: str,
    batch_schema: str,
    *,
    sequential_run_id: str | None = None,
    batch_run_id: str | None = None,
    sentinel_match_id: str | None = None,
) -> dict[str, TableDiff]:
    """Compare every physical match table with EXCEPT ALL in both directions."""

    supplied = (
        sequential_run_id is not None,
        batch_run_id is not None,
        sentinel_match_id is not None,
    )
    if any(supplied) and not all(supplied):
        raise ValueError("run-aware comparison arguments are incomplete")
    if all(supplied):
        _validate_run_lineage(
            manager,
            schema=sequential_schema,
            expected_run_id=str(sequential_run_id),
            sentinel_match_id=str(sentinel_match_id),
        )
        _validate_run_lineage(
            manager,
            schema=batch_schema,
            expected_run_id=str(batch_run_id),
            sentinel_match_id=str(sentinel_match_id),
        )
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
                left_run_id=sequential_run_id,
                right_run_id=batch_run_id,
                sentinel_match_id=sentinel_match_id,
            ),
            batch_minus_sequential=_except_all_count(
                manager,
                left_schema=batch_schema,
                right_schema=sequential_schema,
                table=table,
                columns=columns,
                left_run_id=batch_run_id,
                right_run_id=sequential_run_id,
                sentinel_match_id=sentinel_match_id,
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
    control: Any, evidence_run_id: str
) -> Mapping[str, Any] | None:
    """Read immutable source observations that seed an isolated replay."""

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
                       observed.content_hash, observed.parser_version,
                       observed.typed_parser_version,
                       observed.stateful_parser_version,
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
            (evidence_run_id,),
        )
        observations = _cursor_mapping_rows(cursor)
        cursor.execute(
            """
            WITH ranked AS (
                SELECT target.ordinal, target.target_id,
                       target.logical_refresh_id,
                       observed.content_hash, observed.parser_version,
                       observed.typed_parser_version,
                       observed.stateful_parser_version,
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
                   ranked.logical_refresh_id, ranked.content_hash,
                   manifest.parser_version, manifest.dataset,
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
                 ranked.parser_version, ranked.typed_parser_version,
                 ranked.stateful_parser_version
             )
            WHERE ranked.observation_rank = 1
            ORDER BY ranked.ordinal, manifest.parser_version,
                     manifest.dataset
            """,
            (evidence_run_id,),
        )
        datasets = _cursor_mapping_rows(cursor)
    return {"targets": observations, "datasets": datasets}


def _isolated_replay_control_evidence(
    control: Any,
    *,
    replay_run_id: str,
    source_run_id: str,
    mode: str,
    source_evidence: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Apply replay completion to disposable PostgreSQL control targets.

    The frozen source rows are inputs only.  Every target, observation and
    manifest mutation below is confined to ``pg_temp`` and disappears on
    commit.  Sequential and batch therefore exercise distinct transactional
    control paths without touching production fences.
    """

    normalized_mode = str(mode).strip().casefold()
    if normalized_mode not in {"sequential", "batch"}:
        raise ValueError("acceptance control replay mode is invalid")
    source_targets = source_evidence.get("targets")
    source_datasets = source_evidence.get("datasets")
    if not isinstance(source_targets, Sequence) or isinstance(
        source_targets, (str, bytes)
    ):
        raise ValueError("source control targets are missing")
    if not isinstance(source_datasets, Sequence) or isinstance(
        source_datasets, (str, bytes)
    ):
        raise ValueError("source control manifests are missing")

    test_transaction = getattr(
        control, "replay_control_transaction_evidence", None
    )
    if callable(test_transaction):
        evidence = test_transaction(
            replay_run_id=str(replay_run_id),
            source_run_id=str(source_run_id),
            mode=normalized_mode,
            targets=source_targets,
            datasets=source_datasets,
        )
        if not isinstance(evidence, Mapping):
            raise ValueError("isolated replay control evidence is invalid")
        return evidence

    transaction = getattr(control, "_transaction", None)
    if not callable(transaction):
        raise ValueError(
            "isolated replay control transaction is unavailable"
        )

    target_payload = []
    target_defaults: dict[str, dict[str, str]] = {}
    for position, item in enumerate(source_targets):
        if not isinstance(item, Mapping):
            raise ValueError("source control target is invalid")
        target_id = str(item.get("target_id") or "")
        content_hash = str(item.get("content_hash") or "")
        parser_version = str(item.get("parser_version") or "")
        typed_parser_version = str(
            item.get("typed_parser_version") or ""
        )
        stateful_parser_version = str(
            item.get("stateful_parser_version") or ""
        )
        target_defaults[target_id] = {
            "content_hash": content_hash,
            "parser_version": parser_version,
            "typed_parser_version": typed_parser_version,
            "stateful_parser_version": stateful_parser_version,
        }
        target_payload.append(
            {
                "ordinal": int(item.get("ordinal", position)),
                "target_id": target_id,
                "logical_refresh_id": str(
                    item.get("logical_refresh_id") or ""
                ),
                "page_kind": str(item.get("page_kind") or ""),
                "source_ids": (
                    dict(item.get("source_ids") or {})
                    if isinstance(item.get("source_ids"), Mapping)
                    else {}
                ),
                "content_hash": content_hash,
                "parser_version": parser_version,
                "typed_parser_version": typed_parser_version,
                "stateful_parser_version": stateful_parser_version,
                "typed_status": str(item.get("typed_status") or ""),
                "stateful_status": str(
                    item.get("stateful_status") or ""
                ),
                "evidence_class": (
                    None
                    if item.get("evidence_class") is None
                    else str(item.get("evidence_class"))
                ),
            }
        )
    dataset_payload = []
    for item in source_datasets:
        if not isinstance(item, Mapping):
            raise ValueError("source control manifest is invalid")
        target_id = str(item.get("target_id") or "")
        defaults = target_defaults.get(target_id, {})
        dataset = str(item.get("dataset") or "")
        parser_version = str(item.get("parser_version") or "")
        if not parser_version:
            parser_version = str(
                defaults.get(
                    (
                        "typed_parser_version"
                        if dataset.startswith("typed:")
                        else "stateful_parser_version"
                        if dataset.startswith("stateful:")
                        else "parser_version"
                    ),
                    "",
                )
            )
        dataset_payload.append(
            {
                "target_id": target_id,
                "content_hash": str(
                    item.get("content_hash")
                    or defaults.get("content_hash")
                    or ""
                ),
                "parser_version": parser_version,
                "dataset": dataset,
                "availability": str(item.get("availability") or ""),
                "parse_status": str(item.get("parse_status") or ""),
                "persistence_status": str(
                    item.get("persistence_status") or ""
                ),
                "validation_status": str(
                    item.get("validation_status") or ""
                ),
                "row_count": int(item.get("row_count") or 0),
                "empty_reason": (
                    None
                    if item.get("empty_reason") is None
                    else str(item.get("empty_reason"))
                ),
            }
        )

    target_json = json.dumps(
        target_payload, default=str, sort_keys=True, separators=(",", ":")
    )
    dataset_json = json.dumps(
        dataset_payload, default=str, sort_keys=True, separators=(",", ":")
    )
    with transaction() as cursor:
        cursor.execute(
            """
            CREATE TEMP TABLE fbref_acceptance_control_target (
                ordinal bigint NOT NULL,
                source_target_id text PRIMARY KEY,
                replay_target_id text UNIQUE NOT NULL,
                source_logical_refresh_id text NOT NULL,
                replay_logical_refresh_id text UNIQUE NOT NULL,
                page_kind text NOT NULL,
                source_ids jsonb NOT NULL,
                content_hash text NOT NULL,
                parser_version text NOT NULL,
                typed_parser_version text NOT NULL,
                stateful_parser_version text NOT NULL,
                desired_typed_status text NOT NULL,
                desired_stateful_status text NOT NULL,
                evidence_class text,
                target_status text NOT NULL DEFAULT 'pending',
                frontier_state text NOT NULL DEFAULT 'queued',
                last_content_hash text
            ) ON COMMIT DROP
            """
        )
        cursor.execute(
            """
            CREATE TEMP TABLE fbref_acceptance_control_observation (
                replay_logical_refresh_id text PRIMARY KEY,
                replay_target_id text UNIQUE NOT NULL,
                content_hash text NOT NULL,
                parser_version text NOT NULL,
                typed_parser_version text NOT NULL,
                stateful_parser_version text NOT NULL,
                status text NOT NULL DEFAULT 'processing',
                generic_status text NOT NULL DEFAULT 'pending',
                typed_status text NOT NULL DEFAULT 'pending',
                stateful_status text NOT NULL DEFAULT 'pending',
                validation_status text NOT NULL DEFAULT 'pending'
            ) ON COMMIT DROP
            """
        )
        cursor.execute(
            """
            CREATE TEMP TABLE fbref_acceptance_control_manifest_seed (
                source_target_id text NOT NULL,
                content_hash text NOT NULL,
                parser_version text NOT NULL,
                dataset text NOT NULL,
                availability text NOT NULL,
                parse_status text NOT NULL,
                persistence_status text NOT NULL,
                validation_status text NOT NULL,
                row_count bigint NOT NULL,
                empty_reason text,
                PRIMARY KEY (
                    source_target_id, content_hash, parser_version, dataset
                )
            ) ON COMMIT DROP
            """
        )
        cursor.execute(
            """
            CREATE TEMP TABLE fbref_acceptance_control_manifest (
                source_target_id text NOT NULL,
                replay_target_id text NOT NULL,
                content_hash text NOT NULL,
                parser_version text NOT NULL,
                dataset text NOT NULL,
                availability text NOT NULL,
                parse_status text NOT NULL,
                persistence_status text NOT NULL,
                validation_status text NOT NULL,
                row_count bigint NOT NULL,
                empty_reason text,
                PRIMARY KEY (
                    replay_target_id, content_hash, parser_version, dataset
                )
            ) ON COMMIT DROP
            """
        )
        cursor.execute(
            """
            INSERT INTO pg_temp.fbref_acceptance_control_target (
                ordinal, source_target_id, replay_target_id,
                source_logical_refresh_id, replay_logical_refresh_id,
                page_kind, source_ids, content_hash, parser_version,
                typed_parser_version, stateful_parser_version,
                desired_typed_status, desired_stateful_status,
                evidence_class
            )
            SELECT source.ordinal, source.target_id,
                   'fbref:acceptance-replay:' || %s || ':'
                       || source.target_id,
                   source.logical_refresh_id,
                   %s || ':' || source.logical_refresh_id,
                   source.page_kind, source.source_ids, source.content_hash,
                   source.parser_version, source.typed_parser_version,
                   source.stateful_parser_version, source.typed_status,
                   source.stateful_status, source.evidence_class
            FROM jsonb_to_recordset(%s::jsonb) AS source(
                ordinal bigint, target_id text, logical_refresh_id text,
                page_kind text, source_ids jsonb, content_hash text,
                parser_version text, typed_parser_version text,
                stateful_parser_version text, typed_status text,
                stateful_status text, evidence_class text
            )
            """,
            (str(replay_run_id), str(replay_run_id), target_json),
        )
        cursor.execute(
            """
            INSERT INTO pg_temp.fbref_acceptance_control_observation (
                replay_logical_refresh_id, replay_target_id, content_hash,
                parser_version, typed_parser_version,
                stateful_parser_version
            )
            SELECT replay_logical_refresh_id, replay_target_id, content_hash,
                   parser_version, typed_parser_version,
                   stateful_parser_version
            FROM pg_temp.fbref_acceptance_control_target
            """
        )
        cursor.execute(
            """
            INSERT INTO pg_temp.fbref_acceptance_control_manifest_seed (
                source_target_id, content_hash, parser_version, dataset,
                availability, parse_status, persistence_status,
                validation_status, row_count, empty_reason
            )
            SELECT source.target_id, source.content_hash,
                   source.parser_version, source.dataset,
                   source.availability, source.parse_status,
                   source.persistence_status, source.validation_status,
                   source.row_count, source.empty_reason
            FROM jsonb_to_recordset(%s::jsonb) AS source(
                target_id text, content_hash text, parser_version text,
                dataset text, availability text, parse_status text,
                persistence_status text, validation_status text,
                row_count bigint, empty_reason text
            )
            """,
            (dataset_json,),
        )

        target_ids = [item["target_id"] for item in target_payload]
        completion_target_ids = (
            target_ids if normalized_mode == "sequential" else [None]
        )
        for target_id in completion_target_ids:
            target_predicate = (
                " AND target.source_target_id = %s"
                if target_id is not None
                else ""
            )
            params = () if target_id is None else (target_id,)
            cursor.execute(
                """
                UPDATE pg_temp.fbref_acceptance_control_observation
                       AS observation
                SET status = 'succeeded', generic_status = 'succeeded',
                    typed_status = target.desired_typed_status,
                    stateful_status = target.desired_stateful_status,
                    validation_status = 'succeeded'
                FROM pg_temp.fbref_acceptance_control_target AS target
                WHERE observation.replay_target_id = target.replay_target_id
                """
                + target_predicate,
                params,
            )
            cursor.execute(
                """
                INSERT INTO pg_temp.fbref_acceptance_control_manifest (
                    source_target_id, replay_target_id, content_hash,
                    parser_version, dataset, availability, parse_status,
                    persistence_status, validation_status, row_count,
                    empty_reason
                )
                SELECT seed.source_target_id, target.replay_target_id,
                       seed.content_hash, seed.parser_version, seed.dataset,
                       seed.availability, seed.parse_status,
                       seed.persistence_status, seed.validation_status,
                       seed.row_count, seed.empty_reason
                FROM pg_temp.fbref_acceptance_control_manifest_seed AS seed
                JOIN pg_temp.fbref_acceptance_control_target AS target
                  ON target.source_target_id = seed.source_target_id
                WHERE true
                """
                + target_predicate,
                params,
            )
            cursor.execute(
                """
                UPDATE pg_temp.fbref_acceptance_control_target AS target
                SET target_status = 'succeeded', frontier_state = 'fetched',
                    last_content_hash = target.content_hash
                WHERE true
                """
                + target_predicate,
                params,
            )

        cursor.execute(
            """
            SELECT target.ordinal,
                   target.source_target_id AS target_id,
                   target.replay_target_id,
                   target.replay_logical_refresh_id AS logical_refresh_id,
                   target.target_status AS status, target.target_status,
                   target.page_kind, target.source_ids,
                   target.frontier_state, target.last_content_hash,
                   target.content_hash, observation.parser_version,
                   observation.typed_parser_version,
                   observation.stateful_parser_version,
                   observation.status AS observation_status,
                   observation.generic_status, observation.typed_status,
                   observation.stateful_status,
                   observation.validation_status, target.evidence_class
            FROM pg_temp.fbref_acceptance_control_target AS target
            JOIN pg_temp.fbref_acceptance_control_observation AS observation
              ON observation.replay_target_id = target.replay_target_id
            ORDER BY target.ordinal
            """
        )
        replay_targets = _cursor_mapping_rows(cursor)
        cursor.execute(
            """
            SELECT target.ordinal, manifest.source_target_id AS target_id,
                   manifest.replay_target_id, manifest.content_hash,
                   manifest.parser_version, manifest.dataset,
                   manifest.availability, manifest.parse_status,
                   manifest.persistence_status, manifest.validation_status,
                   manifest.row_count, manifest.empty_reason
            FROM pg_temp.fbref_acceptance_control_manifest AS manifest
            JOIN pg_temp.fbref_acceptance_control_target AS target
              ON target.replay_target_id = manifest.replay_target_id
            ORDER BY target.ordinal, manifest.parser_version,
                     manifest.dataset
            """
        )
        replay_datasets = _cursor_mapping_rows(cursor)
    return {"targets": replay_targets, "datasets": replay_datasets}


def _control_run_evidence(control: Any, run_id: str) -> ControlRunEvidence:
    """Exercise and read disposable replay control completion state."""

    run = control.get_run(run_id)
    summary = control.get_run_summary(run_id)
    failures: list[str] = []
    if not isinstance(run, Mapping):
        raise ValueError("control run is missing")
    if not isinstance(summary, Mapping):
        raise ValueError("control run summary is missing")

    metadata = run.get("metadata")
    metadata = metadata if isinstance(metadata, Mapping) else {}
    evidence_run_id = str(
        metadata.get("acceptance_replay_source_run_id") or run_id
    )
    source_evidence = _direct_processing_evidence(control, evidence_run_id)
    if source_evidence is None:
        source_evidence = control.get_acceptance_run_evidence(
            evidence_run_id
        )
    if not isinstance(source_evidence, Mapping):
        raise ValueError("source control evidence is missing")
    metrics_value = metadata.get("pipeline_run_metrics")
    replay_mode = str(
        metadata.get("acceptance_persistence_mode")
        or (
            metrics_value.get("mode")
            if isinstance(metrics_value, Mapping)
            else ""
        )
        or ""
    ).casefold()
    evidence = _isolated_replay_control_evidence(
        control,
        replay_run_id=str(run_id),
        source_run_id=evidence_run_id,
        mode=replay_mode,
        source_evidence=source_evidence,
    )
    evidence_source = "isolated_replay_control_transaction"
    strict = metadata.get("bronze_acceptance_replay")
    strict = strict if isinstance(strict, Mapping) else {}
    strict_gates = strict.get("strict_gates")
    strict_gates = strict_gates if isinstance(strict_gates, Mapping) else {}
    if str(run.get("status") or "").casefold() != "succeeded":
        failures.append("run_not_succeeded")
    if str(run.get("run_type") or "").casefold() != "replay":
        failures.append("run_not_replay")
    run_zero_metrics = (
        "request_limit",
        "byte_limit",
        "requests_used",
        "bytes_used",
    )
    for metric in run_zero_metrics:
        if (
            metric not in run
            or type(run.get(metric)) is not int
            or run.get(metric) != 0
        ):
            failures.append(f"{metric}_not_zero")
    if (
        strict.get("schema_version")
        != "fbref-bronze-acceptance-replay-v1"
        or str(strict.get("status") or "").casefold() != "passed"
        or str(strict.get("processing_control_run_id") or "") != str(run_id)
        or not isinstance(strict.get("strict_gates"), Mapping)
    ):
        failures.append("strict_acceptance_missing")
    strict_zero_metrics = (
        "network_attempts",
        "requests_used",
        "bytes_used",
        "request_limit",
        "byte_limit",
        "replay_candidates_remaining",
    )
    if (
        str(strict_gates.get("source_acceptance_status") or "").casefold()
        != "passed"
        or str(strict_gates.get("raw_audit_status") or "").casefold()
        != "passed"
        or strict_gates.get("raw_zero_delta_required") is not True
        or any(
            metric not in strict_gates
            or type(strict_gates.get(metric)) is not int
            or strict_gates.get(metric) != 0
            for metric in strict_zero_metrics
        )
        or type(strict_gates.get("raw_audited_attempt_count")) is not int
        or int(strict_gates.get("raw_audited_attempt_count") or 0) <= 0
        or not _is_sha256(strict_gates.get("raw_audit_artifact_sha256"))
        or not _is_sha256(
            strict_gates.get("pipeline_metrics_artifact_sha256")
        )
    ):
        failures.append("strict_acceptance_gates_invalid")
    summary_zero_metrics = (
        "requests_reserved",
        "bytes_reserved",
        "unprocessed_raw_count",
    )
    for metric in summary_zero_metrics:
        if (
            metric not in summary
            or type(summary.get(metric)) is not int
            or summary.get(metric) != 0
        ):
            failures.append(f"{metric}_not_zero")
    traffic = summary.get("traffic_totals")
    if (
        not isinstance(traffic, Mapping)
        or "network_attempts" not in traffic
        or type(traffic.get("network_attempts")) is not int
        or traffic.get("network_attempts") != 0
    ):
        failures.append("network_attempts_not_zero")

    targets = evidence.get("targets")
    datasets = evidence.get("datasets")
    if not isinstance(targets, Sequence) or isinstance(targets, (str, bytes)):
        raise ValueError("isolated target evidence is missing")
    if not isinstance(datasets, Sequence) or isinstance(datasets, (str, bytes)):
        raise ValueError("isolated dataset evidence is missing")
    refresh_ids = [str(item.get("logical_refresh_id") or "") for item in targets]
    match_keys = []
    for item in targets:
        if str(item.get("page_kind") or "").casefold() != "match":
            continue
        target_id = str(item.get("target_id") or "")
        target_match_id = (
            target_id.removeprefix("fbref:match:")
            if target_id.startswith("fbref:match:")
            else ""
        )
        source_ids = item.get("source_ids")
        source_match_id = (
            str(source_ids.get("match_id") or "")
            if isinstance(source_ids, Mapping)
            else ""
        )
        if not target_match_id or (
            source_match_id and source_match_id != target_match_id
        ):
            failures.append("match_target_key_invalid")
            continue
        match_keys.append(target_match_id)
    if not targets:
        failures.append("target_evidence_empty")
    if any(not refresh_id for refresh_id in refresh_ids):
        failures.append("logical_refresh_id_missing")
    if len(set(refresh_ids)) != len(refresh_ids):
        failures.append("logical_refresh_id_duplicate")
    expected_replay_target_prefix = (
        f"fbref:acceptance-replay:{run_id}:"
    )
    if any(
        str(item.get("replay_target_id") or "")
        != expected_replay_target_prefix
        + str(item.get("target_id") or "")
        or not str(item.get("logical_refresh_id") or "").startswith(
            f"{run_id}:"
        )
        for item in targets
    ):
        failures.append("replay_control_target_not_isolated")
    if not match_keys:
        failures.append("match_target_evidence_empty")
    if len(set(match_keys)) != len(match_keys):
        failures.append("match_target_key_duplicate")
    if not datasets:
        failures.append("dataset_evidence_empty")
    target_ids = {str(item.get("target_id") or "") for item in targets}
    manifest_target_ids = {
        str(item.get("target_id") or "") for item in datasets
    }
    if any(
        str(item.get("replay_target_id") or "")
        != expected_replay_target_prefix
        + str(item.get("target_id") or "")
        for item in datasets
    ):
        failures.append("replay_control_manifest_not_isolated")
    if target_ids - manifest_target_ids:
        failures.append("target_dataset_manifest_missing")
    required_match_datasets = {
        f"typed:{dataset}" for dataset in MATCH_DATASET_TABLES
    }
    match_target_ids = {
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
        for target_id in match_target_ids
    }
    if any(
        not required_match_datasets.issubset(
            datasets_by_target.get(target_id, set())
        )
        for target_id in match_target_ids
    ):
        failures.append("match_dataset_manifest_set_incomplete")
    for item in targets:
        target_status = item.get("target_status", item.get("status"))
        if str(target_status or "").casefold() != "succeeded":
            failures.append("target_not_succeeded")
            break
        if (
            str(item.get("frontier_state") or "").casefold() != "fetched"
            or not str(item.get("content_hash") or "")
            or str(item.get("last_content_hash") or "")
            != str(item.get("content_hash") or "")
        ):
            failures.append("latest_frontier_not_completed")
            break
    for item in targets:
        observation_status = item.get("observation_status")
        if observation_status is None:
            failures.append("observation_missing")
            break
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

    pipeline_metrics = None
    if not isinstance(metrics_value, Mapping):
        failures.append("pipeline_metrics_missing")
    else:
        try:
            pipeline_metrics = PipelineRunMetrics.from_mapping(metrics_value)
        except ValueError:
            failures.append("pipeline_metrics_invalid")
        else:
            expected_match_sha256 = _stable_sha256(sorted(match_keys))
            if (
                pipeline_metrics.control_run_id != str(run_id)
                or pipeline_metrics.mode != replay_mode
                or pipeline_metrics.match_count != len(match_keys)
                or pipeline_metrics.match_keys_sha256
                != expected_match_sha256
                or strict_gates.get("pipeline_metrics_artifact_sha256")
                != pipeline_metrics.artifact_sha256
            ):
                failures.append("pipeline_metrics_control_mismatch")

    observations = sorted(
        (
            {
                "target_id": item.get("target_id"),
                "status": item.get("status"),
                "target_status": item.get("target_status"),
                "page_kind": item.get("page_kind"),
                "source_ids": item.get("source_ids"),
                "content_hash": item.get("content_hash"),
                "parser_version": item.get("parser_version"),
                "typed_parser_version": item.get("typed_parser_version"),
                "stateful_parser_version": item.get(
                    "stateful_parser_version"
                ),
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
                    "content_hash",
                    "parser_version",
                    "dataset",
                    "availability",
                    "parse_status",
                    "persistence_status",
                    "validation_status",
                    "row_count",
                    "empty_reason",
                )
            }
            for item in datasets
        ),
        key=lambda item: (
            str(item.get("target_id") or ""),
            str(item.get("parser_version") or ""),
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
        source_run_id=evidence_run_id,
        proxy_requests=int(run.get("requests_used") or 0),
        proxy_bytes=int(run.get("bytes_used") or 0),
        logical_refreshes=len(refresh_ids),
        dataset_manifests=len(datasets),
        match_targets=len(match_keys),
        match_keys_sha256=_stable_sha256(sorted(match_keys)),
        pipeline_metrics=pipeline_metrics,
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
        and sequential.evidence_source
        == batch.evidence_source
        == "isolated_replay_control_transaction"
        and sequential.source_run_id == batch.source_run_id
        and sequential.logical_refreshes == batch.logical_refreshes
        and sequential.dataset_manifests == batch.dataset_manifests
        and sequential.match_targets == batch.match_targets
        and sequential.match_keys_sha256 == batch.match_keys_sha256
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


def _validated_pipeline_metrics(
    metrics: PipelineRunMetrics,
    *,
    expected_control_run_id: str,
    expected_schema: str,
    expected_mode: str,
    control_evidence: ControlRunEvidence,
) -> PipelineRunMetrics:
    parsed = PipelineRunMetrics.from_mapping(metrics.to_dict())
    if parsed.control_run_id != expected_control_run_id:
        raise ValueError("pipeline metrics control run ID does not match")
    if parsed.schema.casefold() != expected_schema.casefold():
        raise ValueError("pipeline metrics schema does not match")
    if parsed.mode != expected_mode:
        raise ValueError("pipeline metrics mode does not match")
    if (
        parsed.match_count != control_evidence.match_targets
        or parsed.match_keys_sha256 != control_evidence.match_keys_sha256
    ):
        raise ValueError("pipeline metrics match cohort does not match control")
    return parsed


def evaluate_existing_pipeline_acceptance(
    *,
    manager: CountingTrinoTableManager,
    control: Any,
    sequential_schema: str,
    batch_schema: str,
    sequential_control_run_id: str,
    batch_control_run_id: str,
    sequential_baseline: AcceptanceBaseline,
    batch_baseline: AcceptanceBaseline,
    sentinel_match_id: str,
    min_speedup: float = 4.0,
    max_seconds_per_match: float = 20.0,
) -> BenchmarkReport:
    """Verify two real replays with no durable acceptance-side writes."""

    if sequential_schema.casefold() == batch_schema.casefold():
        raise ValueError(
            "sequential and batch schemas must be case-insensitively distinct"
        )
    if sequential_control_run_id == batch_control_run_id:
        raise ValueError("sequential and batch control run IDs must be distinct")
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

    control_sequential = _control_run_evidence(
        control, sequential_control_run_id
    )
    control_batch = _control_run_evidence(control, batch_control_run_id)
    if (
        control_sequential.pipeline_metrics is None
        or control_batch.pipeline_metrics is None
    ):
        raise ValueError("control-anchored pipeline metrics are missing")
    sequential_metrics = _validated_pipeline_metrics(
        control_sequential.pipeline_metrics,
        expected_control_run_id=sequential_control_run_id,
        expected_schema=sequential_schema,
        expected_mode="sequential",
        control_evidence=control_sequential,
    )
    batch_metrics = _validated_pipeline_metrics(
        control_batch.pipeline_metrics,
        expected_control_run_id=batch_control_run_id,
        expected_schema=batch_schema,
        expected_mode="batch",
        control_evidence=control_batch,
    )
    if (
        control_sequential.match_targets != control_batch.match_targets
        or control_sequential.match_keys_sha256
        != control_batch.match_keys_sha256
    ):
        raise ValueError("sequential and batch control match cohorts differ")
    matches = control_sequential.match_targets
    _validate_trino_match_cohort(
        manager,
        schema=sequential_schema,
        expected_run_id=sequential_control_run_id,
        sentinel_match_id=sentinel_match_id,
        control_evidence=control_sequential,
    )
    _validate_trino_match_cohort(
        manager,
        schema=batch_schema,
        expected_run_id=batch_control_run_id,
        sentinel_match_id=sentinel_match_id,
        control_evidence=control_batch,
    )
    table_diffs = _bidirectional_table_diffs(
        manager,
        sequential_schema,
        batch_schema,
        sequential_run_id=sequential_control_run_id,
        batch_run_id=batch_control_run_id,
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
        seconds=sequential_metrics.elapsed_seconds,
        statement_counts=sequential_metrics.statement_counts,
        table_digests=_table_digests(
            manager,
            sequential_schema,
            expected_run_id=sequential_control_run_id,
            sentinel_match_id=sentinel_match_id,
        ),
        snapshots_before=dict(sequential_baseline.snapshots),
        snapshots_after=sequential_after,
        snapshot_deltas=_snapshot_deltas(
            sequential_baseline.snapshots, sequential_after
        ),
        sentinel_before=dict(sequential_baseline.sentinels),
        sentinel_after=sequential_sentinel_after,
    )
    batch = PersistenceRun(
        seconds=batch_metrics.elapsed_seconds,
        statement_counts=batch_metrics.statement_counts,
        table_digests=_table_digests(
            manager,
            batch_schema,
            expected_run_id=batch_control_run_id,
            sentinel_match_id=sentinel_match_id,
        ),
        snapshots_before=dict(batch_baseline.snapshots),
        snapshots_after=batch_after,
        snapshot_deltas=_snapshot_deltas(
            batch_baseline.snapshots, batch_after
        ),
        sentinel_before=dict(batch_baseline.sentinels),
        sentinel_after=batch_sentinel_after,
    )
    exact_tables = all(
        diff.sequential_minus_batch == 0
        and diff.batch_minus_sequential == 0
        for diff in table_diffs.values()
    )
    equivalent = exact_tables and _digests_equivalent(
        sequential.table_digests, batch.table_digests
    )
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
