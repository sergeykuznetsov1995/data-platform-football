"""Tournament-season batch writer of the ESPN bronze tables (#1503).

One batch = one ``competition_slug``/``season_year`` = four Iceberg commits,
in the order lineup -> team_stats -> events -> match (R-43: batches per
tournament, not per scope phase).  The match row goes last: its states
(``lineup_state`` etc.) claim the children, so it is committed only once they
are — a batch cut after a child commit leaves the old match row and the next
wave writes the match again (#1504).  Each table is replaced for exactly the
matches of the batch: ``insert_dataframe_atomic`` with a ``delete_filter`` on
the partition and the batch event ids and ``single_statement_replace=True``
(one MERGE with tombstones, no committed empty window; the FBref typed bronze
pattern).  Repeating a batch replaces the rows of its matches, it never
appends a generation; other matches of the partition stay untouched.
``first_published_at`` of a match the batch publishes first is the moment
just before the match commit, after the children (#1505).
"""

from __future__ import annotations

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


def write_tournament_batch(batch: TournamentBatch, *, trino) -> BatchReceipt | None:
    """Replace the rows of the batch matches in all four tables.

    ``trino`` is a ``TrinoTableManager``.  An empty batch writes nothing and
    returns None.
    """
    event_ids = _validate(batch)
    if not event_ids:
        return None
    stamp = BatchStamp(
        batch_id=uuid.uuid4().hex,
        ingested_at=datetime.now(timezone.utc).replace(tzinfo=None),
    )
    rows = batch_rows(batch.matches, stamp=stamp)
    # Matches this batch publishes first (no carried value); their time is
    # taken just before the match commit below.
    first_published = [
        row
        for row, payload in zip(rows[MATCH_TABLE], batch.matches)
        if payload.first_published_at is None and row["first_published_at"] is not None
    ]
    scope = delete_filter(batch.competition_slug, batch.season_year, event_ids)
    # Types of every row are checked against the DDL before the first commit.
    frames = {}
    for table, schema in TABLES.items():
        pa.Table.from_pylist(rows[table], schema=schema)
        frames[table] = pd.DataFrame(rows[table], columns=schema.names)
    written: dict[str, int] = {}
    for table in WRITE_ORDER:
        schema = TABLES[table]
        frame = frames[table]
        if table == MATCH_TABLE and first_published:
            # #1505: first publication = the start of the match commit, after
            # the children (the match row is what makes the match published);
            # the meter is off by one MERGE at most, never by the whole batch.
            published_at = datetime.now(timezone.utc).replace(tzinfo=None)
            for row in first_published:
                row["first_published_at"] = published_at
            frame = pd.DataFrame(rows[table], columns=schema.names)
        inserted = trino.insert_dataframe_atomic(
            BRONZE_DATABASE,
            table,
            frame,
            delete_filter=scope,
            # A staging id must start with a letter; a uuid hex may not.
            staging_id=f"b{stamp.batch_id}",
            single_statement_replace=True,
            target_column_types=trino.arrow_schema_to_trino(schema),
        )
        if inserted != len(frame):
            raise BronzeBatchError(
                f"{table}: inserted {inserted}, expected {len(frame)}"
            )
        written[table] = inserted
    return BatchReceipt(stamp.batch_id, stamp.ingested_at, written)
