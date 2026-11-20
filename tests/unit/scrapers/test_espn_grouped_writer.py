from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
from threading import Barrier
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pytest

from scrapers.espn import deep_store
from scrapers.espn.bronze_schema import LINEUP_TABLE, MATCH_TABLE, TEAM_STATS_TABLE
from scrapers.espn.bronze_writer import (
    WRITE_ORDER,
    TournamentBatch,
    delete_filter,
    preflight_tournament_batch,
    write_ready_batches,
    write_tournament_batch,
)
from tests.unit.scrapers.test_espn_bronze_writer import FakeTrino, _SCOPE, _match


pytestmark = pytest.mark.unit


def _clauses(scope):
    return scope[1:-1].split(") OR (") if scope.startswith("(") else [scope]


def _key(row):
    return row["competition_slug"], row["season_year"], row["event_id"]


class GroupTrino(FakeTrino):
    def __init__(self):
        super().__init__()
        self.reads = []
        self.committed = {}

    def execute_query(self, sql, params=None):
        select, scope = sql.split(" WHERE ")
        self.reads.append((select.split(".")[-1], scope))
        result = []
        for clause in _clauses(scope):
            result.extend(super().execute_query(f"{select} WHERE {clause}", params))
        return result

    def insert_dataframe_atomic(self, schema, table, df, **kwargs):
        keys = set()
        for clause in _clauses(kwargs["delete_filter"]):
            match = _SCOPE.fullmatch(clause)
            assert match, clause
            keys.update((match["slug"], int(match["year"]), int(value))
                        for value in match["ids"].split(", "))
        self.calls.append({"table": table, **kwargs})
        self.tables[table] = [row for row in self.tables[table] if _key(row) not in keys]
        self.tables[table].extend(df.to_dict("records"))
        self.committed[table] = datetime.now(timezone.utc).replace(tzinfo=None)
        return len(df)


def _edition(year, *matches):
    return TournamentBatch("eng.1", year, [
        replace(payload, schedule=replace(payload.schedule, source_season_year=year))
        for payload in matches
    ])


def test_grouped_replacement_preserves_colliding_matches_and_deep_children():
    trino = GroupTrino()
    old_times = {year: datetime(year, 5, 1) for year in (2020, 2021)}
    deep_states = {2020: "captured", 2021: "pending"}
    siblings = {("eng.1", 2020, 9), ("eng.1", 2021, 8)}
    for year, sibling in ((2020, 9), (2021, 8)):
        old = replace(_match(7, "aa"), first_published_at=old_times[year])
        write_tournament_batch(_edition(year, old, _match(sibling, "bb")), trino=trino)
        old_match = next(row for row in trino.tables[MATCH_TABLE] if _key(row) == ("eng.1", year, 7))
        old_match["deep_state"] = deep_states[year]
        for table in (LINEUP_TABLE, TEAM_STATS_TABLE):
            child = next(row for row in trino.tables[table] if _key(row) == ("eng.1", year, 7))
            child["deep_stats_json"] = f'{{"season":{year}}}'
    sibling_rows = {table: [row for row in rows if _key(row) in siblings]
                    for table, rows in trino.tables.items()}
    trino.calls.clear()
    trino.reads.clear()

    receipts = write_ready_batches([
        _edition(2020, _match(7, "ca"), _match(8, "ca")),
        _edition(2021, _match(7, "db", summary=False), _match(9, "db")),
        _edition(2022),
    ], trino=trino)

    clauses = [delete_filter("eng.1", 2020, [7, 8]), delete_filter("eng.1", 2021, [7, 9])]
    scope = " OR ".join(f"({clause})" for clause in clauses)
    assert trino.reads == [(table, scope) for table in (MATCH_TABLE, LINEUP_TABLE, TEAM_STATS_TABLE)]
    assert [call["table"] for call in trino.calls] == list(WRITE_ORDER)
    assert all(call["delete_filter"] == scope and call["single_statement_replace"]
               for call in trino.calls)
    assert len({call["staging_id"] for call in trino.calls}) == 1
    assert set(receipts) == {("eng.1", 2020), ("eng.1", 2021)}
    assert len({receipt.batch_id for receipt in receipts.values()}) == 2
    assert sibling_rows == {table: [row for row in rows if _key(row) in siblings]
                            for table, rows in trino.tables.items()}

    for year, new_id, sha in ((2020, 8, "ca"), (2021, 9, "db")):
        receipt = receipts["eng.1", year]
        for table, rows in trino.tables.items():
            edition = [row for row in rows if row["season_year"] == year and _key(row) not in siblings]
            assert receipt.rows_per_table[table] == len(edition)
            new = [row for row in edition if row["event_id"] == new_id]
            assert new and {row["_batch_id"] for row in new} == {receipt.batch_id}
            if table in (LINEUP_TABLE, TEAM_STATS_TABLE):
                carried = [row["deep_stats_json"] for row in edition
                           if row["event_id"] == 7 and pd.notna(row["deep_stats_json"])]
                assert carried == [f'{{"season":{year}}}']
            if table == MATCH_TABLE:
                matches = {row["event_id"]: row for row in edition}
                assert {row["raw_sha256"] for row in edition} == {sha * 32}
                assert matches[7]["deep_state"] == deep_states[year]
                assert matches[7]["first_published_at"] == old_times[year]
                first = matches[new_id]["first_published_at"]
                assert receipt.ingested_at <= first <= trino.committed[MATCH_TABLE]
                assert all(trino.committed[child] <= first for child in WRITE_ORDER[:-1])


def test_group_checks_every_edition_before_writing():
    trino = GroupTrino()
    batch = _edition(2020, _match(1, "aa"))
    other = _edition(2021, _match(2, "bb"))
    assert write_ready_batches([_edition(2019)], trino=trino) == {}
    for invalid in (batch, replace(other, matches=batch.matches),
                    replace(other, matches=other.matches * 2)):
        with pytest.raises(ValueError):
            write_ready_batches([batch, invalid], trino=trino)
    assert trino.reads == []
    payload = other.matches[0]
    bad = _edition(2021, replace(payload, schedule=replace(payload.schedule, home_team_id="bad")))
    with pytest.raises((pa.ArrowInvalid, TypeError)):
        preflight_tournament_batch(bad)
    with pytest.raises((pa.ArrowInvalid, TypeError)):
        write_ready_batches([batch, bad], trino=trino)
    assert trino.calls == []


def test_group_holds_edition_locks_in_sorted_order(monkeypatch, tmp_path):
    held = []
    keys = [("eng.1", 2020), ("eng.1", 2021)]

    @contextmanager
    def lock(slug, year, state_dir):
        assert state_dir == tmp_path
        held.append((slug, year))
        assert held == keys[:len(held)]
        try:
            yield
        finally:
            assert held.pop() == (slug, year)

    class LockedTrino(GroupTrino):
        def execute_query(self, *args, **kwargs):
            assert held == keys
            return super().execute_query(*args, **kwargs)

        def insert_dataframe_atomic(self, *args, **kwargs):
            assert held == keys
            return super().insert_dataframe_atomic(*args, **kwargs)

    monkeypatch.setattr(deep_store, "edition_lock", lock)
    trino = LockedTrino()
    write_ready_batches([_edition(year, _match(1, "aa")) for _, year in reversed(keys)],
                        trino=trino, state_dir=tmp_path)
    assert held == []


@pytest.mark.parametrize("fail", [False, True])
def test_parallel_children_finish_before_match_or_error(fail):
    trino = GroupTrino()
    write_tournament_batch(_edition(2020, _match(1, "aa")), trino=trino)
    previous_matches = list(trino.tables[MATCH_TABLE])
    trino.calls.clear()
    children = WRITE_ORDER[:-1]
    barrier = Barrier(3, timeout=10)
    managers = {}
    closed = set()
    error = RuntimeError("child commit outcome unknown")

    @contextmanager
    def factory():
        manager = MagicMock(wraps=trino)

        def insert(schema, table, df, **kwargs):
            managers[table] = manager
            barrier.wait()
            count = trino.insert_dataframe_atomic(schema, table, df, **kwargs)
            if fail and table == TEAM_STATS_TABLE:
                raise error
            return count

        manager.insert_dataframe_atomic.side_effect = insert
        try:
            yield manager
        finally:
            closed.add(manager)

    batches = [_edition(2021), _edition(2020, _match(1, "bb"))]
    if fail:
        with pytest.raises(RuntimeError) as raised:
            write_ready_batches(batches, trino=trino, child_trino_factory=factory)
        assert raised.value is error
        assert trino.tables[MATCH_TABLE] == previous_matches
    else:
        receipts = write_ready_batches(batches, trino=trino, child_trino_factory=factory)
        assert receipts["eng.1", 2020].rows_per_table == {
            table: len(rows) for table, rows in trino.tables.items()
        }
        assert trino.calls[-1]["table"] == MATCH_TABLE
    assert set(managers) == set(children)
    assert len(closed) == 3 and closed == set(managers.values())
    assert sorted(call["table"] for call in trino.calls) == sorted(children if fail else WRITE_ORDER)
    assert all(call["delete_filter"] == delete_filter("eng.1", 2020, [1]) for call in trino.calls)
