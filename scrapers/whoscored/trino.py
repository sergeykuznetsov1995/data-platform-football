"""Trino and Iceberg clients used by both WhoScored lanes."""

from __future__ import annotations

import os
import re
import tempfile
from functools import cached_property
from pathlib import Path

from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.base.trino_manager import (
    TrinoAmbiguousWriteError,
    TrinoError,
    TrinoTableManager,
    _is_definite_iceberg_commit_conflict,
)
from scrapers.whoscored.trino_budget import TrinoBudget, WRITE_TIMEOUT


def shared_budget():
    locks = Path(
        os.environ.get(
            "WHOSCORED_LOCK_DIR", Path(tempfile.gettempdir()) / "whoscored_commit_locks"
        )
    )
    return TrinoBudget(locks / "trino")


# These patterns cover the SQL emitted by the repository and IcebergWriter.
# Ignore literals and comments: a source string may itself contain SQL text.
_NOISE = re.compile(r"'(?:''|[^'])*'|--[^\n]*|/\*.*?\*/", re.S)
_IDENT = r'(?:"[^"]+"|[a-z_]\w*)'
_RELATION = rf"{_IDENT}(?:\s*\.\s*{_IDENT}){{0,2}}"
_WRITE = re.compile(
    rf"^\s*(?:insert\s+into|merge\s+into|delete\s+from|update|truncate\s+table|"
    rf"create\s+table(?:\s+if\s+not\s+exists)?|alter\s+table|drop\s+table(?:\s+if\s+exists)?)\s+({_RELATION})",
    re.I,
)
_READ = re.compile(rf"\b(?:from|join|using)\s+({_RELATION})", re.I)


def _relation(name, catalog):
    parts = [part.strip().strip('"').lower() for part in name.split(".")]
    if len(parts) == 1:
        return catalog.lower(), "bronze", parts[0]
    if len(parts) == 2:
        return catalog.lower(), parts[0], parts[1]
    return tuple(parts)


def _manifests(table):
    # Import lazily: the repository also constructs these clients.
    from scrapers.whoscored.repository import (
        CATALOG_MANIFEST_TABLE,
        MATCH_DATASET_TABLES,
        MATCH_MANIFEST_TABLE,
        PREVIEW_DATASET_TABLES,
        PREVIEW_MANIFEST_TABLE,
        PROFILE_MANIFEST_TABLE,
        SCOPE_DATASET_TABLES,
        SCOPE_MANIFEST_TABLE,
    )

    if table.endswith(("_manifest", "_journal")):
        return (table,)
    if table == "whoscored_player_roster":
        return (MATCH_MANIFEST_TABLE, SCOPE_MANIFEST_TABLE, PREVIEW_MANIFEST_TABLE)
    for prefix, manifest in (
        ("whoscored_match_ingest_latest", MATCH_MANIFEST_TABLE),
        ("whoscored_preview_ingest_latest", PREVIEW_MANIFEST_TABLE),
        ("whoscored_scope_ingest_latest", SCOPE_MANIFEST_TABLE),
        ("whoscored_catalog_latest", CATALOG_MANIFEST_TABLE),
    ):
        if table in (prefix, prefix + "_success"):
            return (manifest,)
    if table.endswith("_current"):
        physical = table.removesuffix("_current")
        if physical in SCOPE_DATASET_TABLES:
            return (SCOPE_MANIFEST_TABLE,)
        if physical in MATCH_DATASET_TABLES.values():
            return (MATCH_MANIFEST_TABLE,)
        if physical in PREVIEW_DATASET_TABLES.values():
            return (PREVIEW_MANIFEST_TABLE,)
        if physical in (
            "whoscored_competitions",
            "whoscored_seasons",
            "whoscored_stages",
        ):
            return (CATALOG_MANIFEST_TABLE,)
        if physical in (
            "whoscored_player_profile",
            "whoscored_player_stage_participations",
            "whoscored_player_characteristics",
        ):
            return (PROFILE_MANIFEST_TABLE,)
    return ()


def statement_keys(sql, catalog):
    statement = _NOISE.sub(" ", sql).strip()
    target = _WRITE.match(statement)
    keys = set()
    if target:
        # Trino creates an append snapshot even when CREATE TABLE has no rows.
        if re.match(r"^(alter|drop)\b", statement, re.I):
            return keys
        keys.add("commit:" + ".".join(_relation(target[1], catalog)))
    elif not re.match(r"^(?:select|with|table)\b", statement, re.I):
        return keys
    for match in _READ.finditer(statement):
        source_catalog, schema, table = _relation(match[1], catalog)
        if table == "whoscored_player_profile_current" and schema == "silver":
            schema = "bronze"
        for manifest in _manifests(table):
            keys.add(f"poll:{source_catalog}.{schema}.{manifest}")
    return keys


class _StatementNotSubmitted(TrinoError):
    """The connection or its SELECT 1 health check failed before the caller's SQL."""


def _write_definitely_rejected(error):
    return isinstance(
        error, _StatementNotSubmitted
    ) or _is_definite_iceberg_commit_conflict(error)


class WhoScoredTrinoManager(TrinoTableManager):
    def __init__(
        self,
        *,
        host=None,
        port=None,
        catalog="iceberg",
        session_properties=None,
        query_max_run_seconds=WRITE_TIMEOUT,
        budget=None,
    ):
        password = os.environ.get("TRINO_SRC_WHOSCORED_PASSWORD")
        if not password:
            raise ValueError("Missing Trino credential: TRINO_SRC_WHOSCORED_PASSWORD")
        super().__init__(
            host=host or os.environ.get("TRINO_HOST", "trino"),
            port=port or int(os.environ.get("TRINO_PORT", 8443)),
            user="src_whoscored",
            catalog=catalog,
        )
        # The shared manager initializes caches and opens connections lazily.
        self._password = password
        if (
            type(query_max_run_seconds) is not int
            or not 0 < query_max_run_seconds <= WRITE_TIMEOUT
        ):
            raise ValueError(
                f"query_max_run_seconds must be between 1 and {WRITE_TIMEOUT}"
            )
        self._session_properties = dict(session_properties or {})
        if "query_max_run_time" in self._session_properties:
            raise ValueError("Set query_max_run_seconds to bound the server run time")
        self._session_properties["query_max_run_time"] = f"{query_max_run_seconds}s"
        self.budget = shared_budget() if budget is None else budget

    @property
    def connection(self):
        try:
            return super().connection
        except TrinoError as exc:
            # The shared manager only opens the connection and executes SELECT 1 here.
            raise _StatementNotSubmitted(str(exc)) from exc

    def _create_connection(self):
        import trino

        source = {
            "dag_ingest_whoscored": "whoscored-current",
            "dag_backfill_whoscored": "whoscored-history",
        }.get(os.environ.get("AIRFLOW_CTX_DAG_ID"), "whoscored-service")
        return trino.dbapi.connect(
            source=source,
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            http_scheme="https",
            auth=trino.auth.BasicAuthentication(self.user, self._password),
            verify=False,  # same self-signed source endpoint as the shared client
            session_properties=self._session_properties,
            # Retrying an initial POST can publish a second, ambiguous write.
            max_attempts=1,
        )

    def _execute_committing(self, sql, *, replay_safe=False):
        return super()._execute_committing(sql, replay_safe=False)

    def _execute(self, sql, fetch=False, params=None, *, replay_safe=True):
        keys = statement_keys(sql, self.catalog)
        writes = _WRITE.match(_NOISE.sub(" ", sql)) is not None
        if not keys and not writes:
            return super()._execute(
                sql, fetch=fetch, params=params, replay_safe=replay_safe
            )

        # Run one driver attempt per reservation, including fetching all pages.
        # A disconnected read may be resubmitted after its next poll slot.
        # For writes, the caller must reconcile the physical batch first.
        for attempt in range(2):
            try:
                with self.budget.statement(keys, rejected=_write_definitely_rejected):
                    return super()._execute(
                        sql, fetch=fetch, params=params, replay_safe=False
                    )
            except TrinoAmbiguousWriteError as exc:
                if writes or not replay_safe:
                    raise
                if attempt:
                    raise TrinoError(
                        "Connection lost while retrying a WhoScored poll"
                    ) from exc


class WhoScoredWriter(IcebergWriter):
    @cached_property
    def budget(self):
        return shared_budget()

    def _get_trino_manager(self):
        if self._trino_manager is None:
            self._trino_manager = WhoScoredTrinoManager(
                host=self.trino_host,
                port=self.trino_port,
                catalog=self.catalog,
                budget=self.budget,
            )
        return self._trino_manager

    def _append_dataframe_pyiceberg(self, arrow_table, *, database, table):
        from pyiceberg.exceptions import CommitFailedException

        key = f"commit:{self.catalog}.{database}.{table}".lower()
        with self.budget.statement(
            [key], rejected=lambda exc: isinstance(exc, CommitFailedException)
        ):
            return super()._append_dataframe_pyiceberg(
                arrow_table, database=database, table=table
            )

    def replace_identity_partition_arrow_batches(
        self, arrow_tables, *, database, table, partition_column, partition_value
    ):
        key = f"commit:{self.catalog}.{database}.{table}".lower()
        with self.budget.statement([key]):
            return super().replace_identity_partition_arrow_batches(
                arrow_tables,
                database=database,
                table=table,
                partition_column=partition_column,
                partition_value=partition_value,
            )
