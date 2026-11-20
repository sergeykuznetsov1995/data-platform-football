"""Tournament-season batch writer of the ESPN bronze tables (#1503).

Each ``competition_slug`` and ``season_year`` has its own logical batch and stamp.
``write_ready_batches`` shares four target commits across ready editions and
replaces exactly the matches of those editions. The children can publish in
parallel. The match row declares their states and is committed after all three
child tables succeed (#1504). If a child write fails, the old match row remains
available for the next wave. Inline and staged MERGEs atomically replace the
selected rows of each target table.
``first_published_at`` of a match the batch publishes first is the moment
just before the match commit, after the children (#1505).
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence
import uuid

import pandas as pd
import pyarrow as pa

from .bronze_rows import BatchStamp, MatchPayload, batch_rows
from .bronze_schema import (
    BRONZE_DATABASE,
    EVENTS_TABLE,
    LINEUP_TABLE,
    MATCH_TABLE,
    TABLES,
    TEAM_STATS_TABLE,
)

# Children first, the match row (the completion marker) last.
WRITE_ORDER = (LINEUP_TABLE, TEAM_STATS_TABLE, EVENTS_TABLE, MATCH_TABLE)
_PRIOR_TABLES = (MATCH_TABLE, LINEUP_TABLE, TEAM_STATS_TABLE)


class BronzeBatchError(RuntimeError):
    """The target did not receive exactly the rows of the batch."""


@dataclass(frozen=True, slots=True)
class TournamentBatch:
    competition_slug: str
    season_year: int
    matches: Sequence[MatchPayload]


@dataclass(frozen=True, slots=True)
class BatchReceipt:
    batch_id: str
    ingested_at: datetime
    rows_per_table: dict[str, int]


def _validate(batch: TournamentBatch) -> list[int]:
    if type(batch.season_year) is not int:
        raise TypeError("season_year must be int")
    event_ids: list[int] = []
    for payload in batch.matches:
        schedule = payload.schedule
        if (
            schedule.competition_slug != batch.competition_slug
            or schedule.source_season_year != batch.season_year
        ):
            raise ValueError(
                f"event {schedule.event_id} of {schedule.competition_slug}:"
                f"{schedule.source_season_year} in batch "
                f"{batch.competition_slug}:{batch.season_year}"
            )
        event_ids.append(schedule.event_id)
    if len(set(event_ids)) != len(event_ids):
        raise ValueError("a batch carries every match once")
    return event_ids


def delete_filter(
    competition_slug: str, season_year: int, event_ids: Sequence[int]
) -> str:
    slug = competition_slug.replace("'", "''")
    ids = ", ".join(str(int(event_id)) for event_id in sorted(event_ids))
    return (
        f"competition_slug = '{slug}' AND season_year = {int(season_year)} "
        f"AND event_id IN ({ids})"
    )


def _new_stamp() -> BatchStamp:
    return BatchStamp(
        batch_id=uuid.uuid4().hex,
        ingested_at=datetime.now(timezone.utc).replace(tzinfo=None),
    )


def _check_types(rows):
    for table, schema in TABLES.items():
        pa.Table.from_pylist(rows[table], schema=schema)


def preflight_tournament_batch(batch: TournamentBatch) -> None:
    """Validate a logical batch and its typed source rows without accessing Trino."""
    if _validate(batch):
        _check_types(batch_rows(batch.matches, stamp=_new_stamp()))


def _ready_scopes(batches):
    ready = {}
    seen = set()
    for batch in batches:
        event_ids = _validate(batch)
        key = (batch.competition_slug, batch.season_year)
        if key in seen:
            raise ValueError(f"duplicate edition {key}")
        seen.add(key)
        if event_ids:
            ready[key] = (batch, delete_filter(*key, event_ids))
    return ready


def _prior_by_edition(trino, scope, editions):
    prior = {key: {table: [] for table in _PRIOR_TABLES} for key in editions}
    for table in _PRIOR_TABLES:
        for row in stored_rows(trino, table, scope):
            prior[row["competition_slug"], row["season_year"]][table].append(row)
    return prior


def _publish_ready(ready, *, trino, child_trino_factory=None, replace_frame=None):
    """Publish validated scopes while the caller holds their edition locks."""
    if not ready:
        return {}
    clauses = [clause for _, clause in ready.values()]
    scope = clauses[0] if len(clauses) == 1 else " OR ".join(f"({clause})" for clause in clauses)
    # As in single-edition publication, stamp the input before the prior reads.
    stamps = {key: _new_stamp() for key in ready}
    prior = _prior_by_edition(trino, scope, ready)
    receipts = {}
    combined = {table: [] for table in WRITE_ORDER}
    first_published = []
    for key, (batch, _) in ready.items():
        stamp = stamps[key]
        rows = batch_rows(batch.matches, stamp=stamp)
        before = prior[key]
        carry_deep_rows(before, rows)
        published_ids = {row["event_id"] for row in before[MATCH_TABLE]
                         if row["first_published_at"] is not None}
        first_published.extend(
            row for row, payload in zip(rows[MATCH_TABLE], batch.matches)
            if payload.first_published_at is None
            and row["first_published_at"] == stamp.ingested_at
            and row["event_id"] not in published_ids
        )
        counts = {table: len(rows[table]) for table in WRITE_ORDER}
        receipts[key] = BatchReceipt(stamp.batch_id, stamp.ingested_at, counts)
        for table in WRITE_ORDER:
            combined[table].extend(rows[table])

    _check_types(combined)
    # Keep nullable integers at full precision for the grouped literal formatter.
    frame_dtype = object if replace_frame is not None else None
    frames = {
        table: pd.DataFrame(combined[table], columns=schema.names, dtype=frame_dtype)
        for table, schema in TABLES.items()
    }
    physical_id = next(iter(stamps.values())).batch_id if len(ready) == 1 else uuid.uuid4().hex
    staging_id = f"b{physical_id}"
    if replace_frame is not None:
        match_keys = tuple(
            (slug, year, payload.schedule.event_id)
            for (slug, year), (batch, _) in ready.items()
            for payload in batch.matches
        )

    def write(manager, table):
        frame = frames[table]
        if replace_frame is None:
            inserted = manager.insert_dataframe_atomic(
                BRONZE_DATABASE,
                table,
                frame,
                delete_filter=scope,
                staging_id=staging_id,
                single_statement_replace=True,
                target_column_types=manager.arrow_schema_to_trino(TABLES[table]),
            )
        else:
            inserted = replace_frame(
                manager, table, frame, match_keys=match_keys,
                delete_filter=scope, staging_id=staging_id,
            )
        if inserted != len(frame):
            raise BronzeBatchError(f"{table}: inserted {inserted}, expected {len(frame)}")

    children = WRITE_ORDER[:-1]
    if child_trino_factory is None:
        for table in children:
            write(trino, table)
    else:
        def write_child(table):
            with child_trino_factory() as manager:
                write(manager, table)

        with ThreadPoolExecutor(max_workers=len(children)) as pool:
            futures = [pool.submit(write_child, table) for table in children]
            errors = []
            for future in futures:
                try:
                    future.result()
                except Exception as exc:
                    errors.append(exc)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            first = errors[0]
            raise ExceptionGroup(f"{type(first).__name__}: {first}", errors)

    if first_published:
        published_at = datetime.now(timezone.utc).replace(tzinfo=None)
        for row in first_published:
            row["first_published_at"] = published_at
        frames[MATCH_TABLE] = pd.DataFrame(
            combined[MATCH_TABLE], columns=TABLES[MATCH_TABLE].names, dtype=frame_dtype
        )
    write(trino, MATCH_TABLE)
    return receipts


def _write_tournament_batch(batch: TournamentBatch, *, trino) -> BatchReceipt | None:
    """Publish one edition with its lock already held."""
    receipts = _publish_ready(_ready_scopes([batch]), trino=trino)
    return receipts.get((batch.competition_slug, batch.season_year))


def write_ready_batches(
    batches: Sequence[TournamentBatch], *, trino, state_dir=None, child_trino_factory=None,
    replace_frame=None,
) -> dict[tuple[str, int], BatchReceipt]:
    """Publish ready editions. Pass ready_replace.replace_ready_frame for inline MERGE."""
    from .deep_store import edition_lock

    ready = _ready_scopes(batches)
    lock_keys = sorted(ready)
    # Offline edition_lock uses the same process-wide lock for every edition.
    if state_dir is None:
        lock_keys = lock_keys[:1]
    with ExitStack() as locks:
        for slug, year in lock_keys:
            locks.enter_context(edition_lock(slug, year, state_dir))
        return _publish_ready(
            ready, trino=trino, child_trino_factory=child_trino_factory,
            replace_frame=replace_frame,
        )


def stored_rows(trino, table, scope):
    schema = TABLES[table]
    values = trino.execute_query(
        f"SELECT {', '.join(schema.names)} FROM iceberg.bronze.{table} WHERE {scope}"
    )
    return [dict(zip(schema.names, row)) for row in values]


def _carry_deep(trino, batch, rows):
    """Re-read under edition_lock: every Summary path retains deep children."""
    scope = delete_filter(batch.competition_slug, batch.season_year,
                          [payload.schedule.event_id for payload in batch.matches])
    key = (batch.competition_slug, batch.season_year)
    carry_deep_rows(_prior_by_edition(trino, scope, [key])[key], rows)


def carry_deep_rows(before, rows):
    """Apply the same retained deep cells to previews and actual writes."""
    stored = {row["event_id"]: row for row in before[MATCH_TABLE]}
    for row in rows[MATCH_TABLE]:
        old = stored.get(row["event_id"])
        if old is not None:
            row["deep_state"] = old["deep_state"]
            if old["first_published_at"] is not None:
                row["first_published_at"] = old["first_published_at"]
    for table in (LINEUP_TABLE, TEAM_STATS_TABLE):
        keys = ("event_id", "team_id", "athlete_id") if table == LINEUP_TABLE else ("event_id", "team_id")
        old = {tuple(row[key] for key in keys): row for row in before[table]
               if row["deep_stats_json"] is not None}
        for row in rows[table]:
            previous = old.pop(tuple(row[key] for key in keys), None)
            if previous is not None:
                row["deep_stats_json"] = previous["deep_stats_json"]
        # A poorer Summary cannot delete already captured deep statistics.
        rows[table].extend(old.values())


def write_tournament_batch(batch: TournamentBatch, *, trino, state_dir=None) -> BatchReceipt | None:
    from .deep_store import edition_lock

    event_ids = _validate(batch)
    if not event_ids:
        return None
    with edition_lock(batch.competition_slug, batch.season_year, state_dir):
        return _write_tournament_batch(batch, trino=trino)
