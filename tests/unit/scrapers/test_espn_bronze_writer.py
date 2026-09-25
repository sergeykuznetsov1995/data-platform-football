"""Tournament-season batch writer of the ESPN bronze tables (#1503).

Acceptance of #1503: two batches of one tournament leave no duplicates.
No Trino: an in-memory manager applies the replace semantics of
``insert_dataframe_atomic`` (delete the batch scope, insert the frame), and
the real ``TrinoTableManager`` is driven with a patched ``_execute``.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import replace
from datetime import datetime
import re
import uuid
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from scrapers.espn.bronze_rows import MatchPayload, RawRef
from scrapers.espn.bronze_schema import NATURAL_KEYS, TABLES
from scrapers.espn.bronze_writer import (
    TournamentBatch,
    delete_filter,
    write_tournament_batch,
)
from tests.unit.scrapers.test_espn_probes import _parse

_SCOPE = re.compile(
    r"^competition_slug = '(?P<slug>[^']*)' AND season_year = (?P<year>\d+) "
    r"AND event_id IN \((?P<ids>[\d, ]+)\)$"
)


class FakeTrino:
    """``insert_dataframe_atomic`` with delete_filter replace semantics."""

    def __init__(self) -> None:
        self.tables: dict[str, list[dict]] = {name: [] for name in TABLES}
        self.calls: list[dict] = []

    def arrow_schema_to_trino(self, schema):
        from scrapers.base.trino_manager import TrinoTableManager

        return TrinoTableManager.arrow_schema_to_trino(MagicMock(), schema)

    def insert_dataframe_atomic(self, schema, table, df, **kwargs):
        self.calls.append({"schema": schema, "table": table, "rows": len(df), **kwargs})
        scope = _SCOPE.match(kwargs["delete_filter"])
        assert scope, kwargs["delete_filter"]
        ids = {int(value) for value in scope["ids"].split(", ")}
        self.tables[table] = [
            row
            for row in self.tables[table]
            if not (
                row["competition_slug"] == scope["slug"]
                and row["season_year"] == int(scope["year"])
                and row["event_id"] in ids
            )
        ]
        self.tables[table].extend(df.to_dict("records"))
        return len(df)


def _match(event_id: int, sha: str, *, summary: bool = True) -> MatchPayload:
    event, parsed = _parse("summary_eng1_2020.json")
    raw = RawRef(f"s3://raw/{sha}", sha * 32, datetime(2026, 9, 25, 10))
    return MatchPayload(
        replace(event, event_id=event_id),
        replace(parsed, event_id=event_id) if summary else None,
        raw,
    )


def _batch(*matches: MatchPayload) -> TournamentBatch:
    return TournamentBatch("eng.1", 2020, matches)


@pytest.mark.unit
def test_two_batches_of_one_tournament_leave_no_duplicates() -> None:
    trino = FakeTrino()

    first = write_tournament_batch(
        _batch(_match(1, "aa"), _match(2, "aa"), _match(3, "aa")), trino=trino
    )
    second = write_tournament_batch(
        _batch(_match(2, "bb"), _match(3, "bb", summary=False), _match(4, "bb")),
        trino=trino,
    )

    for table, rows in trino.tables.items():
        keys = Counter(tuple(row[key] for key in NATURAL_KEYS[table]) for row in rows)
        assert keys and max(keys.values()) == 1, table
    matches = {row["event_id"]: row for row in trino.tables["espn_match"]}
    assert sorted(matches) == [1, 2, 3, 4]
    assert matches[1]["_batch_id"] == first.batch_id
    for event_id in (2, 3, 4):
        assert matches[event_id]["_batch_id"] == second.batch_id
        assert matches[event_id]["raw_sha256"] == "bb" * 32
    # Match 3 came back without its Summary: its children are replaced too.
    assert matches[3]["lineup_state"] == "pending"
    lineup = Counter(row["event_id"] for row in trino.tables["espn_match_lineup"])
    assert lineup == {1: 40, 2: 40, 4: 40}
    assert {
        row["_batch_id"]
        for row in trino.tables["espn_match_lineup"]
        if row["event_id"] == 2
    } == {second.batch_id}
    assert second.rows_per_table == {
        "espn_match": 3,
        "espn_match_lineup": 80,
        "espn_team_stats": 4,
        "espn_match_events": 2 * 98,
    }


@pytest.mark.unit
def test_repeating_a_batch_replaces_instead_of_appending() -> None:
    trino = FakeTrino()
    batch = _batch(_match(1, "aa"), _match(2, "aa"))

    write_tournament_batch(batch, trino=trino)
    write_tournament_batch(batch, trino=trino)

    assert {table: len(rows) for table, rows in trino.tables.items()} == {
        "espn_match": 2,
        "espn_match_lineup": 80,
        "espn_team_stats": 4,
        "espn_match_events": 196,
    }


@pytest.mark.unit
def test_four_commits_in_order_scoped_to_the_batch_events() -> None:
    trino = FakeTrino()

    receipt = write_tournament_batch(
        _batch(_match(7, "aa"), _match(5, "aa", summary=False)), trino=trino
    )

    assert [call["table"] for call in trino.calls] == [
        "espn_match",
        "espn_match_lineup",
        "espn_team_stats",
        "espn_match_events",
    ]
    for call in trino.calls:
        assert call["schema"] == "bronze"
        assert call["delete_filter"] == (
            "competition_slug = 'eng.1' AND season_year = 2020 AND event_id IN (5, 7)"
        )
        assert call["single_statement_replace"] is True
        assert call["staging_id"] == f"b{receipt.batch_id}"
        assert "merge_keys" not in call
    stamps = {
        (row["_batch_id"], row["_ingested_at"])
        for rows in trino.tables.values()
        for row in rows
    }
    assert stamps == {(receipt.batch_id, receipt.ingested_at)}


@pytest.mark.unit
def test_empty_batch_writes_nothing() -> None:
    trino = FakeTrino()

    assert write_tournament_batch(_batch(), trino=trino) is None
    assert trino.calls == []


@pytest.mark.unit
def test_foreign_or_repeated_match_is_refused_before_any_commit() -> None:
    trino = FakeTrino()
    other = _match(1, "aa")
    other = replace(other, schedule=replace(other.schedule, source_season_year=2021))

    with pytest.raises(ValueError, match="in batch eng.1:2020"):
        write_tournament_batch(_batch(_match(2, "aa"), other), trino=trino)
    with pytest.raises(ValueError, match="every match once"):
        write_tournament_batch(_batch(_match(2, "aa"), _match(2, "bb")), trino=trino)
    assert trino.calls == []


@pytest.mark.unit
def test_delete_filter_quotes_the_slug() -> None:
    assert delete_filter("x'y", 2020, [3, 1]) == (
        "competition_slug = 'x''y' AND season_year = 2020 AND event_id IN (1, 3)"
    )


@pytest.mark.unit
def test_real_manager_replaces_with_one_merge_per_table() -> None:
    """The mode reaches Trino as a single MERGE with tombstones, no bare DELETE."""
    with patch.dict("sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}):
        from scrapers.base.trino_manager import TrinoTableManager

        manager = TrinoTableManager()
    staged: dict[str, int] = {}

    def fill_stage(schema, stage, frame, batch_size, **kwargs):
        staged[stage] = len(frame)
        return len(frame)

    def execute(sql, fetch=False):
        if fetch:
            stage = re.search(r"FROM iceberg\.bronze\.(\S+)", sql).group(1)
            return [[staged[stage]]]
        return None

    with (
        patch.object(manager, "_execute", side_effect=execute) as executed,
        patch.object(manager, "_execute_committing", side_effect=execute) as committed,
        patch.object(manager, "insert_dataframe", side_effect=fill_stage),
        patch.object(manager, "drop_table"),
        patch("scrapers.espn.bronze_writer.uuid.uuid4", return_value=uuid.UUID(int=0)),
    ):
        # A batch id starting with a digit still yields a valid staging id.
        write_tournament_batch(
            _batch(_match(1, "aa"), _match(2, "aa", summary=False)), trino=manager
        )

    merges = [call.args[0] for call in committed.call_args_list]
    assert [
        re.search(r"MERGE INTO iceberg\.bronze\.(\w+) t", sql).group(1)
        for sql in merges
    ] == list(TABLES)
    assert all("WHEN MATCHED THEN DELETE" in sql for sql in merges)
    statements = [call.args[0] for call in executed.call_args_list]
    assert not [sql for sql in statements if sql.startswith("DELETE FROM")]
    tombstones = [sql for sql in statements if "'delete' FROM iceberg.bronze." in sql]
    assert len(tombstones) == 4
    assert all(
        sql.endswith(
            "WHERE competition_slug = 'eng.1' AND season_year = 2020 "
            "AND event_id IN (1, 2)"
        )
        for sql in tombstones
    )


@pytest.mark.unit
def test_rows_that_do_not_fit_the_ddl_stop_before_the_first_commit() -> None:
    trino = FakeTrino()
    bad = _match(1, "aa")
    bad = replace(bad, schedule=replace(bad.schedule, home_team_id="not-an-id"))

    with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError)):
        write_tournament_batch(_batch(bad), trino=trino)
    assert trino.calls == []
