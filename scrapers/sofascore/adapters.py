"""Production adapters for the SofaScore raw-first capture contracts."""

from __future__ import annotations

import threading
from collections import deque
from typing import Callable, Iterable, Mapping, Optional, Protocol, Sequence

import pandas as pd

from scrapers.base.sql_validator import validate_identifier
from scrapers.sofascore.capture_engine import (
    CaptureResult,
    HttpPayload,
    ProviderBudgetToken,
)
from scrapers.sofascore.manifest import (
    EndpointManifest,
    ManifestKey,
    ManifestStatus,
    ManifestStore,
)
from scripts.proxy_filter.budget import BudgetAccountingError


MANIFEST_KEY_COLUMNS = (
    "source_tournament_id",
    "source_season_id",
    "target_type",
    "target_id",
    "endpoint",
    "freshness_key",
)
MANIFEST_COLUMNS = MANIFEST_KEY_COLUMNS + (
    "status",
    "run_id",
    "task_id",
    "attempts",
    "row_count",
    "http_status",
    "raw_content_hash",
    "raw_blob_key",
    "request_url",
    "error_type",
    "error_message",
    "duration_ms",
    "provider_bytes",
    "fetched_at",
    "parsed_at",
    "updated_at",
    "manifest_version",
)


class TrinoManagerProtocol(Protocol):
    catalog: str

    def create_schema(self, schema: str) -> None: ...

    def _execute(
        self,
        sql: str,
        fetch: bool = False,
        params: Optional[tuple] = None,
    ): ...

    def insert_dataframe_atomic(
        self,
        schema: str,
        table: str,
        df: pd.DataFrame,
        *,
        merge_keys: Sequence[str],
    ) -> int: ...


def render_manifest_ddl(
    *,
    catalog: str = "iceberg",
    schema: str = "ops",
    table: str = "sofascore_capture_manifest",
) -> str:
    for value, name in ((catalog, "catalog"), (schema, "schema"), (table, "table")):
        validate_identifier(value, name)
    qualified = f"{catalog}.{schema}.{table}"
    return f"""
CREATE TABLE IF NOT EXISTS {qualified} (
    source_tournament_id varchar,
    source_season_id varchar,
    target_type varchar,
    target_id varchar,
    endpoint varchar,
    freshness_key varchar,
    status varchar,
    run_id varchar,
    task_id varchar,
    attempts integer,
    row_count bigint,
    http_status integer,
    raw_content_hash varchar,
    raw_blob_key varchar,
    request_url varchar,
    error_type varchar,
    error_message varchar,
    duration_ms bigint,
    provider_bytes bigint,
    fetched_at varchar,
    parsed_at varchar,
    updated_at varchar,
    manifest_version varchar
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['source_tournament_id', 'source_season_id']
)
""".strip()


def manifest_to_row(record: EndpointManifest) -> dict:
    row = {
        **record.key.__dict__,
        "status": record.status.value,
        "run_id": record.run_id,
        "task_id": record.task_id,
        "attempts": record.attempts,
        "row_count": record.row_count,
        "http_status": record.http_status,
        "raw_content_hash": record.raw_content_hash,
        "raw_blob_key": record.raw_blob_key,
        "request_url": record.request_url,
        "error_type": record.error_type,
        "error_message": record.error_message,
        "duration_ms": record.duration_ms,
        "provider_bytes": record.provider_bytes,
        "fetched_at": record.fetched_at,
        "parsed_at": record.parsed_at,
        "updated_at": record.updated_at,
        "manifest_version": record.manifest_version,
    }
    return {column: row[column] for column in MANIFEST_COLUMNS}


def manifest_from_row(row: Sequence[object] | Mapping[str, object]) -> EndpointManifest:
    if isinstance(row, Mapping):
        values = {column: row.get(column) for column in MANIFEST_COLUMNS}
    else:
        if len(row) != len(MANIFEST_COLUMNS):
            raise ValueError("Trino manifest row has unexpected column count")
        values = dict(zip(MANIFEST_COLUMNS, row))
    key = ManifestKey(**{name: values.pop(name) for name in MANIFEST_KEY_COLUMNS})
    values["key"] = key
    values["status"] = ManifestStatus(values["status"])
    return EndpointManifest(**values)


class TrinoManifestStore(ManifestStore):
    """Iceberg long-manifest store with natural-key incremental MERGE."""

    def __init__(
        self,
        manager: TrinoManagerProtocol,
        *,
        schema: str = "ops",
        table: str = "sofascore_capture_manifest",
        ensure_table: bool = True,
    ) -> None:
        validate_identifier(schema, "schema")
        validate_identifier(table, "table")
        validate_identifier(manager.catalog, "catalog")
        self.manager = manager
        self.catalog = manager.catalog
        self.schema = schema
        self.table = table
        self.qualified = f"{self.catalog}.{self.schema}.{self.table}"
        if ensure_table:
            self.ensure_table()

    def ensure_table(self) -> None:
        self.manager.create_schema(self.schema)
        self.manager._execute(
            render_manifest_ddl(
                catalog=self.catalog,
                schema=self.schema,
                table=self.table,
            )
        )

    @staticmethod
    def _select_columns() -> str:
        return ", ".join(f'"{column}"' for column in MANIFEST_COLUMNS)

    def get(self, key: ManifestKey) -> Optional[EndpointManifest]:
        where = " AND ".join(f'"{column}" = ?' for column in MANIFEST_KEY_COLUMNS)
        rows = self.manager._execute(
            f"SELECT {self._select_columns()} FROM {self.qualified} "
            f"WHERE {where} LIMIT 1",
            fetch=True,
            params=key.as_tuple(),
        )
        if not rows:
            return None
        return manifest_from_row(rows[0])

    def upsert(self, record: EndpointManifest) -> None:
        frame = pd.DataFrame([manifest_to_row(record)], columns=MANIFEST_COLUMNS)
        self.manager.insert_dataframe_atomic(
            self.schema,
            self.table,
            frame,
            merge_keys=MANIFEST_KEY_COLUMNS,
        )

    def list_for_run(self, run_id: str) -> list[EndpointManifest]:
        run_id = str(run_id).strip()
        if not run_id:
            raise ValueError("run_id must not be empty")
        rows = self.manager._execute(
            f"SELECT {self._select_columns()} FROM {self.qualified} "
            'WHERE "run_id" = ?',
            fetch=True,
            params=(run_id,),
        )
        return [manifest_from_row(row) for row in (rows or [])]


ExactFetcher = Callable[[str, Optional[ProviderBudgetToken]], HttpPayload]


class PooledCaptureTransport:
    """Adapter over one already-warmed browser/API session.

    ``fetch_exact`` must return the exact response body bytes and provider-path
    byte count. Calls are serialized by default because Playwright pages are not
    thread-safe; all endpoints still reuse the same warmed session.
    """

    def __init__(self, fetch_exact: ExactFetcher, *, serialize: bool = True) -> None:
        if not callable(fetch_exact):
            raise TypeError("fetch_exact must be callable")
        self.fetch_exact = fetch_exact
        self._lock = threading.RLock() if serialize else None

    def request(
        self,
        url: str,
        *,
        provider_budget: Optional[ProviderBudgetToken],
    ) -> HttpPayload:
        def fetch() -> HttpPayload:
            result = self.fetch_exact(url, provider_budget)
            if not isinstance(result, HttpPayload):
                raise TypeError("pooled fetcher must return HttpPayload with exact bytes")
            return result

        if self._lock is None:
            return fetch()
        with self._lock:
            return fetch()


class PrefetchedCaptureTransport:
    """Zero-network queue for direct/test payloads already held in memory.

    Paid prefetched responses must use ``engine.authorize_request`` followed by
    ``engine.ingest_prefetched``; accepting a newly-created token after traffic
    has already moved would defeat the hard budget, so this adapter rejects it.
    """

    def __init__(self, payloads: Mapping[str, HttpPayload | Sequence[HttpPayload]]) -> None:
        self._payloads: dict[str, deque[HttpPayload]] = {}
        for url, values in payloads.items():
            if isinstance(values, HttpPayload):
                queue = [values]
            else:
                queue = list(values)
            if not queue or any(not isinstance(item, HttpPayload) for item in queue):
                raise TypeError("prefetched payload queues must contain HttpPayload")
            self._payloads[str(url)] = deque(queue)
        self._lock = threading.Lock()
        self.calls = 0

    def request(
        self,
        url: str,
        *,
        provider_budget: Optional[ProviderBudgetToken],
    ) -> HttpPayload:
        if provider_budget is not None:
            raise BudgetAccountingError(
                "paid prefetched bytes require preauthorization + ingest_prefetched"
            )
        with self._lock:
            try:
                result = self._payloads[url].popleft()
            except (KeyError, IndexError) as exc:
                raise KeyError(f"no prefetched SofaScore response for {url}") from exc
            self.calls += 1
            return result


def raw_lineage_map(results: Iterable[CaptureResult]) -> dict[ManifestKey, dict]:
    """Return endpoint→raw lineage after capture, replay, or manifest cache hit."""
    lineage: dict[ManifestKey, dict] = {}
    for result in results:
        record = result.manifest
        if not record.raw_content_hash or not record.raw_blob_key:
            continue
        lineage[record.key] = {
            "raw_content_hash": record.raw_content_hash,
            "raw_blob_key": record.raw_blob_key,
            "request_url": record.request_url,
            "http_status": record.http_status,
            "fetched_at": record.fetched_at,
        }
    return lineage


LEGACY_CORE_ENDPOINTS = ("event", "lineups", "statistics", "shotmap")


def _legacy_state(record: EndpointManifest) -> str:
    if record.status == ManifestStatus.SUCCESS:
        return "success"
    if record.status in {
        ManifestStatus.LEGITIMATE_EMPTY,
        ManifestStatus.NOT_SUPPORTED,
    }:
        return "not_available"
    if record.status == ManifestStatus.SCHEMA_ERROR:
        return "schema_error"
    if record.http_status == 403:
        return "blocked"
    if record.http_status == 429:
        return "rate_limited"
    if record.http_status is not None and record.http_status >= 500:
        return "server_error"
    return "transport_error"


def project_legacy_match_status(
    records: Iterable[EndpointManifest],
    *,
    league: str,
    season: str,
    endpoints: Sequence[str] = LEGACY_CORE_ENDPOINTS,
) -> list[dict]:
    """Project the long manifest to ``bronze.sofascore_match_capture_status``.

    ``endpoints`` defaults to the original four-column contract; deployments
    that have migrated incidents pass ``(*LEGACY_CORE_ENDPOINTS, 'incidents')``.
    Missing/non-terminal endpoints keep ``capture_complete=false``.
    """
    endpoint_names = tuple(dict.fromkeys(str(name).strip() for name in endpoints))
    if not endpoint_names or any(not name for name in endpoint_names):
        raise ValueError("legacy endpoint list must be non-empty")
    latest: dict[tuple[str, str], EndpointManifest] = {}
    for record in records:
        if record.key.target_type != "event":
            continue
        group_key = (record.key.target_id, record.key.endpoint)
        previous = latest.get(group_key)
        if previous is None or record.updated_at > previous.updated_at:
            latest[group_key] = record
    match_ids = sorted({target_id for target_id, _ in latest})
    rows = []
    for match_id in match_ids:
        states = {}
        complete = True
        for endpoint in endpoint_names:
            record = latest.get((match_id, endpoint))
            state = _legacy_state(record) if record else "missing"
            states[f"{endpoint}_status"] = state
            complete = complete and state in {"success", "not_available"}
        rows.append(
            {
                "match_id": match_id,
                **states,
                "capture_complete": complete,
                "league": str(league),
                "season": str(season),
            }
        )
    return rows
