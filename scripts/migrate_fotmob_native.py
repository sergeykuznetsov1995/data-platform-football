#!/usr/bin/env python3
"""Audit/quarantine legacy FotMob snapshot rows and create native views.

Default mode is read-only.  ``--apply --quarantine-and-delete`` first copies
confirmed season-replicated rows into source-specific quarantine tables,
validates the copy count, and only then deletes those exact identities from the
legacy table.  Canonical raw/native tables are never modified by cleanup.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Iterable, Sequence

from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.fotmob.repository import FotMobRepository


QUARANTINE_REASON = "seasonless_source_snapshot_replicated_as_historical"


@dataclass(frozen=True)
class QuarantineSpec:
    table: str
    identity_columns: tuple[str, ...]

    @property
    def quarantine_table(self) -> str:
        return f"{self.table}_legacy_quarantine"


QUARANTINE_SPECS = (
    QuarantineSpec("fotmob_team_profile", ("team_id",)),
    QuarantineSpec("fotmob_team_squad", ("team_id", "player_id")),
    QuarantineSpec("fotmob_player_details", ("player_id",)),
    QuarantineSpec(
        "fotmob_transfers",
        (
            "player_id",
            "transfer_date",
            "from_club_id",
            "to_club_id",
            "transfer_type_key",
        ),
    ),
)


def _quote(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _qualified(catalog: str, schema: str, table: str) -> str:
    return ".".join(_quote(value) for value in (catalog, schema, table))


def duplicate_rank_sql(
    spec: QuarantineSpec,
    *,
    catalog: str = "iceberg",
    schema: str = "bronze",
) -> str:
    """Count rows that repeat a seasonless source identity across seasons."""

    source = _qualified(catalog, schema, spec.table)
    partition = ", ".join(_quote(column) for column in spec.identity_columns)
    return f"""
        SELECT COUNT(*)
        FROM (
            SELECT ROW_NUMBER() OVER (
                       PARTITION BY {partition}
                       ORDER BY _ingested_at DESC, season DESC, _batch_id DESC
                   ) AS duplicate_rank
            FROM {source}
        ) ranked
        WHERE duplicate_rank > 1
    """


def _identity_join(
    left: str,
    right: str,
    columns: Iterable[str],
) -> str:
    return " AND ".join(
        f"{left}.{_quote(column)} IS NOT DISTINCT FROM "
        f"{right}.{_quote(column)}"
        for column in columns
    )


def apply_quarantine(
    trino,
    spec: QuarantineSpec,
    *,
    catalog: str = "iceberg",
    schema: str = "bronze",
) -> dict[str, int | str]:
    """Copy then delete exact duplicate identities, validating row counts."""

    source = _qualified(catalog, schema, spec.table)
    quarantine = _qualified(catalog, schema, spec.quarantine_table)
    columns = list(trino.get_table_columns(schema, spec.table))
    required = {*spec.identity_columns, "season", "_ingested_at", "_batch_id"}
    missing = sorted(required - set(columns))
    if missing:
        raise RuntimeError(f"{spec.table}: required columns missing: {missing}")
    selected = ", ".join(f"ranked.{_quote(column)}" for column in columns)
    insert_columns = ", ".join(_quote(column) for column in columns)
    partition = ", ".join(_quote(column) for column in spec.identity_columns)

    trino._execute(
        f"""
        CREATE TABLE IF NOT EXISTS {quarantine} AS
        SELECT source.*,
               CAST(NULL AS VARCHAR) AS _quarantine_reason,
               CAST(NULL AS TIMESTAMP(6)) AS _quarantined_at
        FROM {source} source
        WHERE FALSE
        """
    )
    trino._execute(
        f"""
        INSERT INTO {quarantine} ({insert_columns}, _quarantine_reason, _quarantined_at)
        SELECT {selected}, '{QUARANTINE_REASON}', CURRENT_TIMESTAMP
        FROM (
            SELECT source.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY {partition}
                       ORDER BY _ingested_at DESC, season DESC, _batch_id DESC
                   ) AS duplicate_rank
            FROM {source} source
        ) ranked
        WHERE ranked.duplicate_rank > 1
          AND NOT EXISTS (
              SELECT 1 FROM {quarantine} q
              WHERE {_identity_join('q', 'ranked', spec.identity_columns)}
                AND q.season IS NOT DISTINCT FROM ranked.season
                AND q._batch_id IS NOT DISTINCT FROM ranked._batch_id
                AND q._quarantine_reason = '{QUARANTINE_REASON}'
          )
        """
    )
    expected = int(trino.execute_query(duplicate_rank_sql(spec, catalog=catalog, schema=schema))[0][0])
    quarantined = int(
        trino.execute_query(
            f"SELECT COUNT(*) FROM {quarantine} "
            f"WHERE _quarantine_reason = '{QUARANTINE_REASON}'"
        )[0][0]
    )
    if quarantined < expected:
        raise RuntimeError(
            f"{spec.table}: quarantine validation failed: "
            f"quarantined={quarantined} expected_at_least={expected}"
        )
    trino._execute(
        f"""
        DELETE FROM {source} source
        WHERE EXISTS (
            SELECT 1 FROM {quarantine} q
            WHERE {_identity_join('q', 'source', spec.identity_columns)}
              AND q.season IS NOT DISTINCT FROM source.season
              AND q._batch_id IS NOT DISTINCT FROM source._batch_id
              AND q._quarantine_reason = '{QUARANTINE_REASON}'
        )
        """
    )
    remaining = int(
        trino.execute_query(duplicate_rank_sql(spec, catalog=catalog, schema=schema))[0][0]
    )
    if remaining:
        raise RuntimeError(
            f"{spec.table}: {remaining} replicated rows remain after quarantine"
        )
    return {
        "table": spec.table,
        "quarantined": quarantined,
        "remaining_duplicates": remaining,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Audit or quarantine season-replicated legacy FotMob snapshots"
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--quarantine-and-delete",
        action="store_true",
        help="Required second confirmation before legacy DELETE statements",
    )
    parser.add_argument("--catalog", default="iceberg")
    parser.add_argument("--schema", default="bronze")
    args = parser.parse_args(argv)
    if args.quarantine_and_delete and not args.apply:
        parser.error("--quarantine-and-delete requires --apply")

    writer = IcebergWriter(catalog=args.catalog)
    trino = writer._get_trino_manager()
    report: dict[str, object] = {
        "mode": "apply" if args.apply else "audit",
        "quarantine_and_delete": bool(args.quarantine_and_delete),
        "tables": [],
        "views": [],
    }
    for spec in QUARANTINE_SPECS:
        if not trino.table_exists(args.schema, spec.table):
            report["tables"].append({"table": spec.table, "status": "absent"})
            continue
        duplicates = int(
            trino.execute_query(
                duplicate_rank_sql(spec, catalog=args.catalog, schema=args.schema)
            )[0][0]
        )
        item: dict[str, object] = {
            "table": spec.table,
            "duplicate_snapshot_rows": duplicates,
            "status": "audit_only",
        }
        if args.apply and args.quarantine_and_delete and duplicates:
            item.update(
                apply_quarantine(
                    trino, spec, catalog=args.catalog, schema=args.schema
                )
            )
            item["status"] = "quarantined_and_deleted"
        report["tables"].append(item)

    if args.apply:
        repository = FotMobRepository(
            writer=writer, catalog=args.catalog, schema=args.schema
        )
        repository.ensure_schema()
        report["views"] = repository.ensure_current_views()
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
