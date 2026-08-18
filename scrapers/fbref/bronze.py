"""Idempotent generic Bronze persistence for lossless FBref page documents."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, Mapping, Optional, Sequence

import pandas as pd

from scrapers.base.sql_validator import validate_catalog_qualified_name
from scrapers.base.trino_manager import TrinoTableManager
from scrapers.fbref.page_document import PageDocument


PAGE_MANIFEST_TABLE = "fbref_page_manifest"
TABLE_INVENTORY_TABLE = "fbref_table_inventory"
TABLE_CELLS_TABLE = "fbref_table_cells"

GENERIC_TABLE_SCHEMAS: Dict[str, Dict[str, str]] = {
    PAGE_MANIFEST_TABLE: {
        "target_id": "VARCHAR",
        "canonical_url": "VARCHAR",
        "page_kind": "VARCHAR",
        "content_hash": "VARCHAR",
        "parser_version": "VARCHAR",
        "parse_status": "VARCHAR",
        "persist_status": "VARCHAR",
        "validation_status": "VARCHAR",
        "table_count": "BIGINT",
        "cell_count": "BIGINT",
        "errors_json": "VARCHAR",
        "run_id": "VARCHAR",
        "persisted_at": "TIMESTAMP(6)",
    },
    TABLE_INVENTORY_TABLE: {
        "target_id": "VARCHAR",
        "page_kind": "VARCHAR",
        "content_hash": "VARCHAR",
        "parser_version": "VARCHAR",
        "table_instance_id": "VARCHAR",
        "source_table_id": "VARCHAR",
        "table_id": "VARCHAR",
        "source_location": "VARCHAR",
        "source_ordinal": "BIGINT",
        "availability": "VARCHAR",
        "schema_signature": "VARCHAR",
        "content_signature": "VARCHAR",
        "duplicate_of": "VARCHAR",
        "caption": "VARCHAR",
        "row_count": "BIGINT",
        "reason": "VARCHAR",
        "run_id": "VARCHAR",
        "persisted_at": "TIMESTAMP(6)",
    },
    TABLE_CELLS_TABLE: {
        "target_id": "VARCHAR",
        "page_kind": "VARCHAR",
        "content_hash": "VARCHAR",
        "parser_version": "VARCHAR",
        "table_instance_id": "VARCHAR",
        "table_id": "VARCHAR",
        "row_id": "VARCHAR",
        "source_row_index": "BIGINT",
        "cell_id": "VARCHAR",
        "cell_index": "BIGINT",
        "data_stat": "VARCHAR",
        "raw_header_path": "VARCHAR",
        "raw_value": "VARCHAR",
        "entity_ids": "VARCHAR",
        "run_id": "VARCHAR",
        "persisted_at": "TIMESTAMP(6)",
    },
}

GENERIC_TABLE_KEYS = {
    PAGE_MANIFEST_TABLE: ("target_id", "content_hash", "parser_version"),
    TABLE_INVENTORY_TABLE: ("table_instance_id", "parser_version"),
    TABLE_CELLS_TABLE: (
        "table_instance_id", "row_id", "cell_id", "parser_version"
    ),
}


class GenericPersistenceError(RuntimeError):
    """Generic Bronze was partial or could not be validated."""


class GenericBronzeBatchUnsupported(GenericPersistenceError):
    """The cohort must use the already-claimed sequential fallback."""


@dataclass(frozen=True)
class GenericPagePersistItem:
    """One already-parsed generic page ready for Bronze persistence."""

    page: PageDocument
    canonical_url: str
    run_id: str
    staging_identity: str


@dataclass(frozen=True)
class _ValidatedGenericPagePersistItem:
    item: GenericPagePersistItem
    cells: tuple[dict, ...]
    inventory: tuple[dict, ...]
    manifest: Mapping[str, object]


def _token(value: Optional[str]) -> str:
    """Return a deterministic, janitor-readable staging owner token.

    Production passes ``logical_refresh_id`` here.  Keeping the UUID in the
    table name makes a retained stage attributable to exactly one immutable
    control-plane observation and makes a retry reuse that stage.  Callers
    outside the production pipeline still get a stable, collision-resistant
    token without leaking arbitrary input into an SQL identifier.
    """

    identity = str(value or "").strip()
    try:
        return f"lr_{uuid.UUID(identity).hex}"
    except (AttributeError, TypeError, ValueError):
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]
        return f"id_{digest}"


def _batch_token(staging_identities: Iterable[str]) -> str:
    """Hash the complete sorted identity sequence without delimiter ambiguity."""

    encoded = json.dumps(
        sorted(str(identity) for identity in staging_identities),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"batch_{hashlib.sha256(encoded).hexdigest()[:16]}"


class FBrefGenericBronzeWriter:
    """Persist a page in commit-marker order using idempotent Iceberg MERGE."""

    def __init__(
        self,
        manager: Optional[TrinoTableManager] = None,
        *,
        schema: str = "bronze",
    ) -> None:
        self.manager = manager or TrinoTableManager()
        self.schema = schema
        self._tables_ready = False

    def ensure_tables(self) -> None:
        if self._tables_ready:
            return
        for table, columns in GENERIC_TABLE_SCHEMAS.items():
            self.manager.create_iceberg_table(
                self.schema,
                table,
                columns,
                partition_columns=["page_kind"] if "page_kind" in columns else None,
            )
        # Cache only a fully successful preflight.  A partial DDL failure must
        # retry every idempotent CREATE on the next page rather than turning a
        # broken schema into process-local success.
        self._tables_ready = True

    def _merge_dataframe(
        self,
        table: str,
        frame: pd.DataFrame,
        *,
        keys: Sequence[str],
        staging_token: str,
    ) -> int:
        if frame.empty:
            return 0
        stage = f"{table}__stg_{staging_token}"
        target_name = validate_catalog_qualified_name(
            self.manager.catalog, self.schema, table
        )
        stage_name = validate_catalog_qualified_name(
            self.manager.catalog, self.schema, stage
        )
        self.manager.drop_table(self.schema, stage, if_exists=True)
        self.manager._execute(
            f"CREATE TABLE {stage_name} AS SELECT * FROM {target_name} WHERE false"
        )
        try:
            inserted = self.manager.insert_dataframe(
                self.schema, stage, frame
            )
            staged = self.manager._execute(
                f"SELECT count(*) FROM {stage_name}", fetch=True
            )[0][0]
            if inserted != len(frame) or staged != len(frame):
                raise GenericPersistenceError(
                    f"Stage row count mismatch for {table}: "
                    f"inserted={inserted}, staged={staged}, expected={len(frame)}"
                )
        except Exception:
            self.manager.drop_table(self.schema, stage, if_exists=True)
            raise

        columns = list(frame.columns)
        on_clause = " AND ".join(
            f't."{key}" = s."{key}"' for key in keys
        )
        update_columns = [column for column in columns if column not in keys]
        update_clause = ", ".join(
            f'"{column}" = s."{column}"' for column in update_columns
        )
        names = ", ".join(f'"{column}"' for column in columns)
        values = ", ".join(f's."{column}"' for column in columns)
        merge_sql = (
            f"MERGE INTO {target_name} t USING {stage_name} s ON ({on_clause}) "
            + (
                f"WHEN MATCHED THEN UPDATE SET {update_clause} "
                if update_clause else ""
            )
            + f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})"
        )
        try:
            # A natural-key upsert converges on re-execution, so it belongs on
            # the committing path the typed writer already uses: an Iceberg
            # commit conflict here would otherwise drop a whole cohort back to
            # one write per page, which is the cost cohorting exists to avoid.
            self.manager._execute_committing(merge_sql)
        except Exception:
            # Retain a fully validated stage for deterministic recovery.
            raise
        self.manager.drop_table(self.schema, stage, if_exists=True)
        return len(frame)

    @staticmethod
    def _decorate(records: Iterable[dict], run_id: str, persisted_at) -> pd.DataFrame:
        materialized = []
        for record in records:
            materialized.append({
                **record,
                "run_id": run_id,
                "persisted_at": persisted_at,
            })
        return pd.DataFrame(materialized)

    def persist_page(
        self,
        page: PageDocument,
        *,
        canonical_url: str,
        run_id: str,
        staging_identity: Optional[str] = None,
    ) -> dict:
        """Write cells, table inventory, and the page commit marker last."""

        if page.errors:
            return self._persist_page_error_evidence(
                page,
                canonical_url=canonical_url,
                run_id=run_id,
                staging_identity=staging_identity,
            )
        return self.persist_pages(
            [
                GenericPagePersistItem(
                    page=page,
                    canonical_url=canonical_url,
                    run_id=run_id,
                    staging_identity=staging_identity or run_id,
                )
            ]
        )[0]

    def _persist_page_error_evidence(
        self,
        page: PageDocument,
        *,
        canonical_url: str,
        run_id: str,
        staging_identity: Optional[str],
    ) -> dict:
        """Preserve the legacy error manifest before raising to the caller."""

        self.ensure_tables()
        persisted_at = datetime.now(timezone.utc).replace(tzinfo=None)
        base_token = _token(staging_identity or run_id)
        cells = self._decorate(page.cell_records(), run_id, persisted_at)
        inventory = self._decorate(
            page.inventory_records(), run_id, persisted_at
        )

        counts = {
            "cells": self._merge_dataframe(
                TABLE_CELLS_TABLE,
                cells,
                keys=GENERIC_TABLE_KEYS[TABLE_CELLS_TABLE],
                staging_token=f"{base_token}_c",
            ),
            "tables": self._merge_dataframe(
                TABLE_INVENTORY_TABLE,
                inventory,
                keys=GENERIC_TABLE_KEYS[TABLE_INVENTORY_TABLE],
                staging_token=f"{base_token}_t",
            ),
        }
        parse_status = "error" if page.errors else "success"
        manifest = pd.DataFrame([{
            "target_id": page.target_id,
            "canonical_url": canonical_url,
            "page_kind": page.page_kind,
            "content_hash": page.content_hash,
            "parser_version": page.parser_version,
            "parse_status": parse_status,
            "persist_status": "success",
            "validation_status": "error" if page.errors else "success",
            "table_count": len(page.tables),
            "cell_count": len(cells),
            "errors_json": json.dumps(page.errors, ensure_ascii=False),
            "run_id": run_id,
            "persisted_at": persisted_at,
        }])
        counts["manifest"] = self._merge_dataframe(
            PAGE_MANIFEST_TABLE,
            manifest,
            keys=GENERIC_TABLE_KEYS[PAGE_MANIFEST_TABLE],
            staging_token=f"{base_token}_m",
        )
        if page.errors:
            raise GenericPersistenceError(
                f"Page {page.target_id} contained parser errors: {page.errors[:3]}"
            )
        return counts

    @staticmethod
    def _validate_page_batch(
        items: Sequence[GenericPagePersistItem],
    ) -> tuple[_ValidatedGenericPagePersistItem, ...]:
        error_targets = [
            item.page.target_id for item in items if item.page.errors
        ]
        if error_targets:
            raise GenericBronzeBatchUnsupported(
                "generic persistence batch contains parser errors for "
                f"{error_targets[:3]}"
            )

        validated: list[_ValidatedGenericPagePersistItem] = []
        for item in items:
            cells = tuple(item.page.cell_records())
            inventory = tuple(item.page.inventory_records())
            validated.append(
                _ValidatedGenericPagePersistItem(
                    item=item,
                    cells=cells,
                    inventory=inventory,
                    manifest={
                        "target_id": item.page.target_id,
                        "canonical_url": item.canonical_url,
                        "page_kind": item.page.page_kind,
                        "content_hash": item.page.content_hash,
                        "parser_version": item.page.parser_version,
                        "parse_status": "success",
                        "persist_status": "success",
                        "validation_status": "success",
                        "table_count": len(item.page.tables),
                        "cell_count": len(cells),
                        "errors_json": json.dumps(
                            item.page.errors, ensure_ascii=False
                        ),
                    },
                )
            )

        records_by_table = {
            TABLE_CELLS_TABLE: tuple(
                record for item in validated for record in item.cells
            ),
            TABLE_INVENTORY_TABLE: tuple(
                record for item in validated for record in item.inventory
            ),
            PAGE_MANIFEST_TABLE: tuple(item.manifest for item in validated),
        }
        for table, records in records_by_table.items():
            keys = GENERIC_TABLE_KEYS[table]
            seen: set[tuple[object, ...]] = set()
            for record in records:
                identity = tuple(record[key] for key in keys)
                if identity in seen:
                    raise GenericBronzeBatchUnsupported(
                        f"duplicate {table} merge key in generic batch: "
                        f"{identity!r}"
                    )
                seen.add(identity)
        return tuple(validated)

    def _persist_validated_page_batch(
        self, items: Sequence[_ValidatedGenericPagePersistItem]
    ) -> list[dict[str, int]]:
        persisted_at = datetime.now(timezone.utc).replace(tzinfo=None)
        base_token = (
            _token(items[0].item.staging_identity)
            if len(items) == 1
            else _batch_token(item.item.staging_identity for item in items)
        )

        cell_frames = [
            self._decorate(item.cells, item.item.run_id, persisted_at)
            for item in items
            if item.cells
        ]
        inventory_frames = [
            self._decorate(item.inventory, item.item.run_id, persisted_at)
            for item in items
            if item.inventory
        ]
        manifest_frames = [
            self._decorate(
                (dict(item.manifest),), item.item.run_id, persisted_at
            )
            for item in items
        ]
        cells = (
            pd.concat(cell_frames, ignore_index=True, sort=False)
            if cell_frames
            else pd.DataFrame()
        )
        inventory = (
            pd.concat(inventory_frames, ignore_index=True, sort=False)
            if inventory_frames
            else pd.DataFrame()
        )
        manifests = pd.concat(
            manifest_frames, ignore_index=True, sort=False
        )

        # Finish every local transformation before the first DDL. This keeps a
        # malformed late page/frame from leaving preflight or earlier-table
        # side effects behind.
        self.ensure_tables()

        # The manifest is the generic completion marker and therefore remains
        # the final merge even when either preceding frame is empty.
        self._merge_dataframe(
            TABLE_CELLS_TABLE,
            cells,
            keys=GENERIC_TABLE_KEYS[TABLE_CELLS_TABLE],
            staging_token=f"{base_token}_c",
        )
        self._merge_dataframe(
            TABLE_INVENTORY_TABLE,
            inventory,
            keys=GENERIC_TABLE_KEYS[TABLE_INVENTORY_TABLE],
            staging_token=f"{base_token}_t",
        )
        self._merge_dataframe(
            PAGE_MANIFEST_TABLE,
            manifests,
            keys=GENERIC_TABLE_KEYS[PAGE_MANIFEST_TABLE],
            staging_token=f"{base_token}_m",
        )
        return [
            {
                "cells": len(item.cells),
                "tables": len(item.inventory),
                "manifest": 1,
            }
            for item in items
        ]

    def persist_pages(
        self, items: Sequence[GenericPagePersistItem]
    ) -> list[dict[str, int]]:
        """Persist a validated page cohort with one merge per generic table."""

        materialized = tuple(items)
        if not materialized:
            return []
        validated = self._validate_page_batch(materialized)
        return self._persist_validated_page_batch(validated)


__all__ = [
    "FBrefGenericBronzeWriter",
    "GenericBronzeBatchUnsupported",
    "GenericPagePersistItem",
    "GENERIC_TABLE_KEYS",
    "GENERIC_TABLE_SCHEMAS",
    "GenericPersistenceError",
    "PAGE_MANIFEST_TABLE",
    "TABLE_CELLS_TABLE",
    "TABLE_INVENTORY_TABLE",
]
