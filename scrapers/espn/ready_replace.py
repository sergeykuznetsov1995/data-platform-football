"""Bounded replacement for explicitly opted-in grouped ESPN publishers."""

import json
import logging
import uuid
from collections import defaultdict

import numpy as np
import pandas as pd

from scrapers.base.sql_validator import validate_catalog_qualified_name, validate_identifier
from scrapers.base.trino_manager import SQL_BYTE_BUDGET, TrinoError

from .bronze_schema import BRONZE_DATABASE, EVENTS_TABLE, LINEAGE_FIELDS, LINEUP_TABLE, TABLES


_KEY_COLUMNS = ("competition_slug", "season_year", "event_id")
_LINEAGE_COLUMNS = tuple(field.name for field in LINEAGE_FIELDS)
logger = logging.getLogger(__name__)


def _merge_parts(manager, qualified, columns, types, frame, match_keys, *, stage=None):
    literal = manager._format_sql_value
    column_list = ", ".join(f'"{name}"' for name in columns)
    yield f"MERGE INTO {qualified} AS t USING (VALUES "

    seen = set()
    editions = {}
    for key in match_keys:
        if key in seen:
            continue
        first = not seen
        if not first:
            yield ", "
        fields = dict(zip(_KEY_COLUMNS, key, strict=True))
        values = []
        for name in columns:
            if name in fields:
                values.append(literal(fields[name], types[name]))
            elif first:
                values.append(f"CAST(NULL AS {types[name]})")
            else:
                values.append("NULL")
        yield f"(TRUE, {', '.join(values)})"
        seen.add(key)
        editions[key[:2]] = None

    if not seen:
        raise ValueError("grouped replacement requires selected matches")
    if stage is None:
        for row in frame.itertuples(index=False, name=None):
            yield ", "
            values = (literal(value, types[name]) for name, value in zip(columns, row))
            yield f"(FALSE, {', '.join(values)})"
    else:
        yield f" UNION ALL SELECT FALSE, {column_list} FROM {stage}"

    yield f') AS s("_delete_marker", {column_list}) ON s."_delete_marker" AND '
    yield " AND ".join(f't."{name}" = s."{name}"' for name in _KEY_COLUMNS)
    # Keep static pruning possible with the ESPN session's dynamic filtering off.
    yield " AND ("
    for index, (slug, year) in enumerate(editions):
        if index:
            yield " OR "
        yield (
            f'(t."competition_slug" = {literal(slug, types["competition_slug"])} AND '
            f't."season_year" = {literal(year, types["season_year"])})'
        )
    yield ') WHEN MATCHED THEN DELETE WHEN NOT MATCHED AND NOT s."_delete_marker" '
    source_columns = ", ".join(f's."{name}"' for name in columns)
    yield f"THEN INSERT ({column_list}) VALUES ({source_columns})"


def _bounded_sql(fragments):
    parts = []
    remaining = SQL_BYTE_BUDGET
    for part in fragments:
        if len(part) > remaining:
            return None
        size = len(part.encode("utf-8"))
        if size > remaining:
            return None
        remaining -= size
        parts.append(part)
    return "".join(parts)


def _scalar(value):
    # Match the frame formatter's missing-cell convention. Keep Python integers
    # as integers all the way through json.dumps, even beside nullable siblings.
    if isinstance(value, np.datetime64):
        return pd.Timestamp(value) if not np.isnat(value) else None
    if isinstance(value, np.generic):
        value = value.item()
    if value is pd.NA or value is pd.NaT or (isinstance(value, float) and np.isnan(value)):
        return None
    return value


def _stage_writes(manager, stage, columns, types, frame):
    fixed = (*_KEY_COLUMNS, *_LINEAGE_COLUMNS)
    children = columns[len(_KEY_COLUMNS):-len(_LINEAGE_COLUMNS)]
    groups = defaultdict(list)
    for row in frame.itertuples(index=False, name=None):
        key = tuple(_scalar(v) for v in row[:len(_KEY_COLUMNS)] + row[-len(_LINEAGE_COLUMNS):])
        child = tuple(_scalar(v) for v in row[len(_KEY_COLUMNS):-len(_LINEAGE_COLUMNS)])
        payload = json.dumps(child, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        groups[key].append(payload.replace("'", "''"))

    projection = ", ".join(f'"{name}"' for name in columns)
    fixed_names = ", ".join(f'"{name}"' for name in fixed)
    child_names = ", ".join(f'"{name}"' for name in children)
    child_types = ", ".join(types[name] for name in children)
    select = f"SELECT {projection} FROM (VALUES "
    suffix = (
        f') AS g({fixed_names}, "_payload") CROSS JOIN UNNEST('
        f'CAST(json_parse(g."_payload") AS ARRAY(ROW({child_types})))) AS c({child_names})'
    )
    prefix = f"CREATE TABLE {stage} AS {select}"
    remaining = SQL_BYTE_BUDGET - len((prefix + suffix).encode("utf-8"))
    values = []
    for key, rows in groups.items():
        fields = []
        for name, value in zip(fixed, key, strict=True):
            # Iceberg stores these Arrow timestamp[us] fields at microsecond precision.
            dtype = "TIMESTAMP(6)" if name in ("_ingested_at", "_source_fetched_at") else types[name]
            fields.append(f"CAST({manager._format_sql_value(value, dtype)} AS {dtype})")
        opening = f"({', '.join(fields)}, '["
        closing = "]')"
        overhead = len((opening + closing).encode("utf-8"))
        start = 0
        while start < len(rows):
            free = remaining - overhead - (2 if values else 0)
            end = start
            while end < len(rows):
                size = len(rows[end].encode("utf-8")) + int(end > start)
                if size > free:
                    break
                free -= size
                end += 1
            if end > start:
                values.append(opening + ",".join(rows[start:end]) + closing)
                remaining = free
            if end < len(rows):
                if not values:
                    raise ValueError(
                        f"{stage}: single child row exceeds {SQL_BYTE_BUDGET} SQL bytes "
                        f"for match {key[:len(_KEY_COLUMNS)]!r}"
                    )
                yield prefix + ", ".join(values) + suffix
                prefix = f"INSERT INTO {stage} {select}"
                remaining = SQL_BYTE_BUDGET - len((prefix + suffix).encode("utf-8"))
                values = []
            start = end
    if values:
        yield prefix + ", ".join(values) + suffix


def _replace_large(manager, table, qualified, types, frame, match_keys, stage_id):
    columns = TABLES[table].names
    if stage_id:
        validate_identifier(stage_id, "staging id")
    token = f"{stage_id}_" if stage_id else ""
    stage = validate_catalog_qualified_name(
        manager.catalog, BRONZE_DATABASE, f"{table}__stg_{token}{uuid.uuid4().hex}"
    )
    merge = _bounded_sql(
        _merge_parts(manager, qualified, columns, types, frame, match_keys, stage=stage)
    )
    if merge is None:
        raise ValueError(f"{table}: grouped marker MERGE exceeds {SQL_BYTE_BUDGET} SQL bytes")
    # Preflight the entire bounded group before the CTAS, including late wide rows.
    writes = list(_stage_writes(manager, stage, columns, types, frame))
    count = f"SELECT count(*) FROM {stage}"
    drop = f"DROP TABLE IF EXISTS {stage}"
    if any(len(sql.encode("utf-8")) > SQL_BYTE_BUDGET for sql in (*writes, count, drop)):
        raise ValueError(f"{table}: stage SQL exceeds {SQL_BYTE_BUDGET} bytes")

    def discard_stage():
        try:
            manager._execute_committing(drop, replay_safe=False)
        except Exception:
            logger.warning("Could not clean up ESPN stage %s", stage)

    try:
        # Gate catalog creation like the ordinary stage. Inserts into this
        # private table can proceed independently of other publishers.
        manager._execute_committing(writes[0], replay_safe=False)
        for insert in writes[1:]:
            manager._execute(insert, replay_safe=False)
    except Exception:
        logger.error("ESPN stage write failed for %s, retaining stage if created", stage)
        raise

    try:
        staged = manager._execute(count, fetch=True)[0][0]
        if staged != len(frame):
            raise TrinoError(f"{stage}: staged {staged}, expected {len(frame)} before replacement")
    except Exception:
        discard_stage()
        raise

    try:
        manager._execute_replacement(merge)
    except Exception:
        logger.error("ESPN target replacement failed, retaining stage %s", stage)
        raise
    discard_stage()
    return len(frame)


def replace_ready_frame(manager, table, frame, *, match_keys, delete_filter, staging_id):
    """Replace all selected matches, including those with no rows in this frame."""
    schema = TABLES[table]
    if list(frame.columns) != schema.names:
        raise ValueError(f"{table}: expected grouped frame in declared schema order")
    types = manager.arrow_schema_to_trino(schema)
    qualified = validate_catalog_qualified_name(manager.catalog, BRONZE_DATABASE, table)
    match_keys = tuple(match_keys)
    merge = _bounded_sql(_merge_parts(manager, qualified, schema.names, types, frame, match_keys))
    if merge is not None:
        manager._execute_replacement(merge)
        return len(frame)

    if table in (LINEUP_TABLE, EVENTS_TABLE):
        return _replace_large(manager, table, qualified, types, frame, match_keys, staging_id)

    return manager.insert_dataframe_atomic(
        BRONZE_DATABASE, table, frame,
        delete_filter=delete_filter,
        staging_id=staging_id,
        single_statement_replace=True,
        target_column_types=types,
    )
