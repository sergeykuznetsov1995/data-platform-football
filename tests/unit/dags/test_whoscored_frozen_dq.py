"""Snapshot-pinned, set-based historical DQ contracts."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest
import sqlglot

from dags.scripts.whoscored_frozen_dq import (
    DQ_STAGE_ARROW_BATCH_ROWS,
    DQ_STAGE_CLEANUP_LIMIT,
    DQ_STAGE_QUALIFIED,
    FrozenDQError,
    _identity_sha256,
    _relation_sha256,
    _stage_arrow_batches,
    _stage_population_marker,
    cleanup_staged_frozen_populations,
    frozen_historical_integrity,
    stage_frozen_population,
    staged_scope_relation_sql,
)


def _relation(
    *, matches: int = 1, previews: int = 0, players: int = 0
) -> dict[str, object]:
    return {
        "population_sha256": "a" * 64,
        "plan_id": "b" * 64,
        "checkpoint_sha256": "c" * 64,
        "scope_relation_sha256": "d" * 64,
        "match_relation_sha256": "e" * 64,
        "player_relation_sha256": "f" * 64,
        "expected_scopes": 1,
        "expected_matches": matches,
        "expected_previews": previews,
        "expected_players": players,
        "expected_rows": 2 + matches + players,
        "table": DQ_STAGE_QUALIFIED,
        "snapshot_id": 123456,
    }


def _population() -> dict[str, object]:
    matches = [
        {
            "league": "WS-1",
            "season": "2026",
            "game_id": 42,
            "preview_required": True,
        },
        {
            "league": "WS-1",
            "season": "2026",
            "game_id": 43,
            "preview_required": False,
        },
    ]
    return {
        "population_sha256": "a" * 64,
        "plan_id": "b" * 64,
        "checkpoint": {"sha256": "c" * 64},
        "scope_stages": [
            {
                "scope": "WS-1=2026",
                "league": "WS-1",
                "season": "2026",
                "stage_ids": [10],
            }
        ],
        "matches": matches,
        "player_ids": [7, 8],
        "counts": {
            "scopes": 1,
            "matches": 2,
            "previews": 1,
            "players": 2,
        },
    }


class _Cursor:
    def __init__(
        self,
        relation: dict[str, object],
        *,
        semantic_failure: bool = False,
        null_duplicate_failure: bool = False,
        invalid_stage_hashes: int = 0,
        parity_values: list[int] | None = None,
        profile_owner_mismatches: list[int] | None = None,
    ) -> None:
        self.relation = relation
        self.query = ""
        self.queries: list[str] = []
        self.semantic_failure = semantic_failure
        self.null_duplicate_failure = null_duplicate_failure
        self.invalid_stage_hashes = invalid_stage_hashes
        self.parity_values = parity_values
        self.parity_calls = 0
        self.profile_owner_mismatches = profile_owner_mismatches
        self.profile_owner_calls = 0

    def execute(self, query: str) -> None:
        self.query = query
        self.queries.append(query)

    def fetchall(self):
        if "match_shards AS" in self.query:
            return [
                [
                    self.relation["expected_rows"],
                    1,
                    1,
                    self.relation["expected_scopes"],
                    self.relation["expected_matches"],
                    self.relation["expected_previews"],
                    self.relation["expected_players"],
                    0,
                    0,
                    self.invalid_stage_hashes,
                    0,
                    0,
                    0,
                    0,
                    self.relation["scope_relation_sha256"],
                    self.relation["match_relation_sha256"],
                    self.relation["player_relation_sha256"],
                ]
            ]
        if "whoscored_frozen_coverage" in self.query:
            row = [0] * 14
            row[0] = self.relation["expected_matches"]
            row[7] = self.relation["expected_previews"]
            return [row]
        if "whoscored_frozen_events" in self.query:
            if self.null_duplicate_failure:
                return [[0, 0, 0, 1, 1]]
            return [[0] * 5]
        if "whoscored_frozen_match_snapshots" in self.query:
            row = [0] * 3
            if self.semantic_failure:
                row[2] = 1
            return [row]
        if "whoscored_frozen_schedule_extras" in self.query:
            return [[0] * 2]
        if "whoscored_frozen_profile_owner_parity" in self.query:
            if self.profile_owner_mismatches is not None:
                value = self.profile_owner_mismatches[
                    self.profile_owner_calls
                ]
                self.profile_owner_calls += 1
                return [[value]]
            return [[0]]
        if "latest_success AS" in self.query:
            row = [0] * 12
            row[0] = self.relation["expected_players"]
            return [row]
        if "whoscored_frozen_owner_parity" in self.query:
            if self.parity_values is not None:
                offset = self.parity_calls * 4
                self.parity_calls += 1
                return [self.parity_values[offset : offset + 4]]
            return [[0, 0, 0, 0]]
        raise AssertionError(f"unexpected query: {self.query[:200]}")


def test_stage_arrow_relation_contains_atomic_marker_and_exact_hashes():
    import pyarrow as pa

    population = _population()
    marker = _stage_population_marker(population)
    table = pa.concat_tables(list(_stage_arrow_batches(population, marker)))

    assert table.num_rows == 6
    assert table.column_names[0] == "population_sha256"
    assert table["row_kind"].to_pylist() == [
        "marker",
        "scope",
        "match",
        "match",
        "player",
        "player",
    ]
    assert marker["expected_rows"] == 6
    assert marker["expected_scopes"] == 1
    assert marker["expected_matches"] == 2
    assert marker["expected_previews"] == 1
    assert marker["expected_players"] == 2
    scope_hashes = table["row_sha256"].to_pylist()[1:2]
    match_hashes = table["row_sha256"].to_pylist()[2:4]
    player_hashes = table["row_sha256"].to_pylist()[4:]
    assert marker["scope_relation_sha256"] == _relation_sha256(scope_hashes)
    assert marker["match_relation_sha256"] == _relation_sha256(match_hashes)
    assert marker["player_relation_sha256"] == _relation_sha256(player_hashes)


def test_identity_ordered_fingerprint_matches_sql_shards_and_order_matters():
    rows = []
    for game_id in range(1, 2_000):
        row_hash = _identity_sha256(
            ("match", "WS-1", "2026", game_id, "0")
        )
        rows.append((("WS-1", "2026", game_id), row_hash))
        if sum(value[:3] == row_hash[:3] for _key, value in rows) == 2:
            break
    collision = [
        item for item in rows if item[1][:3] == rows[-1][1][:3]
    ]
    assert len(collision) == 2

    # Equivalent to Trino's GROUP BY first-three-hex shard, ARRAY_AGG hash
    # ORDER BY identity, then ARRAY_AGG shard record ORDER BY shard_id.
    shards: dict[int, list[tuple[tuple[str, str, int], str]]] = {}
    for identity, row_hash in rows:
        shards.setdefault(int(row_hash[:3], 16), []).append((identity, row_hash))
    sql_root = hashlib.sha256()
    for shard_id in sorted(shards):
        values = sorted(shards[shard_id])
        shard_sha = hashlib.sha256(
            "".join(row_hash for _identity, row_hash in values).encode("ascii")
        ).hexdigest()
        sql_root.update(
            f"{shard_id}:{len(values)}:{shard_sha}\n".encode("ascii")
        )

    assert _relation_sha256([row_hash for _key, row_hash in rows]) == (
        sql_root.hexdigest()
    )
    reversed_collision = [row_hash for _key, row_hash in rows]
    left = rows.index(collision[0])
    right = rows.index(collision[1])
    reversed_collision[left], reversed_collision[right] = (
        reversed_collision[right],
        reversed_collision[left],
    )
    assert _relation_sha256(reversed_collision) != sql_root.hexdigest()


def test_stage_arrow_batches_bound_peak_relation_size():
    players = list(range(1, DQ_STAGE_ARROW_BATCH_ROWS * 2 + 2))
    population = {"scope_stages": [], "matches": [], "player_ids": players}
    marker = {
        "population_sha256": "a" * 64,
        "plan_id": "b" * 64,
        "checkpoint_sha256": "c" * 64,
        "scope_relation_sha256": hashlib.sha256(b"").hexdigest(),
        "match_relation_sha256": "d" * 64,
        "player_relation_sha256": "e" * 64,
        "expected_scopes": 0,
        "expected_matches": 0,
        "expected_previews": 0,
        "expected_players": len(players),
    }

    sizes = [
        table.num_rows for table in _stage_arrow_batches(population, marker)
    ]

    assert sizes == [1, DQ_STAGE_ARROW_BATCH_ROWS, DQ_STAGE_ARROW_BATCH_ROWS, 1]
    assert max(sizes) == DQ_STAGE_ARROW_BATCH_ROWS


def test_stage_marker_rejects_non_identity_ordered_keys():
    population = _population()
    population["matches"] = list(reversed(population["matches"]))
    with pytest.raises(FrozenDQError, match="identity-ordered"):
        _stage_population_marker(population)


def test_staged_scope_relation_preserves_a_scope_without_matches():
    population = _population()
    population["matches"] = []
    population["counts"]["matches"] = 0
    population["counts"]["previews"] = 0

    marker = _stage_population_marker(population)
    relation = {
        **marker,
        "table": DQ_STAGE_QUALIFIED,
        "snapshot_id": 77,
    }
    query = staged_scope_relation_sql(relation)

    assert marker["expected_scopes"] == 1
    assert marker["expected_matches"] == 0
    assert marker["expected_rows"] == 4
    assert "FOR VERSION AS OF 77" in query
    assert "row_kind='scope'" in query


def test_stage_population_atomically_replaces_only_a_missing_partition():
    calls: list[tuple[object, ...]] = []

    class Trino:
        def _execute(self, query):
            calls.append(("ddl", query))

        def get_table_columns(self, _schema, _table):
            from dags.scripts.whoscored_frozen_dq import _STAGE_COLUMNS

            return _STAGE_COLUMNS

        def execute_query(self, query):
            calls.append(("query", query))
            return [
                (0, 0, None, None, None, None, None, 0, 0, 0, 0)
            ]

    class Writer:
        def _get_trino_manager(self):
            return Trino()

        def require_exact_identity_partition(self, **_kwargs):
            return None

        def replace_identity_partition_arrow_batches(self, arrow_tables, **kwargs):
            calls.append(
                ("replace", sum(table.num_rows for table in arrow_tables), kwargs)
            )

        def current_snapshot_id(self, **_kwargs):
            return 987

    relation = stage_frozen_population(_population(), writer=Writer())

    replaces = [call for call in calls if call[0] == "replace"]
    assert len(replaces) == 1
    assert replaces[0][1] == 6
    assert replaces[0][2]["partition_value"] == "a" * 64
    assert relation["snapshot_id"] == 987


def test_stage_population_refuses_to_repair_a_conflicting_partition():
    class Trino:
        def _execute(self, _query):
            return None

        def get_table_columns(self, _schema, _table):
            from dags.scripts.whoscored_frozen_dq import _STAGE_COLUMNS

            return _STAGE_COLUMNS

        def execute_query(self, _query):
            return [
                (
                    6,
                    1,
                    "wrong",
                    "c" * 64,
                    "d" * 64,
                    "e" * 64,
                    "f" * 64,
                    1,
                    2,
                    1,
                    2,
                )
            ]

    writer = SimpleNamespace(
        _get_trino_manager=lambda: Trino(),
        require_exact_identity_partition=lambda **_kwargs: None,
    )
    with pytest.raises(FrozenDQError, match="conflicts"):
        stage_frozen_population(_population(), writer=writer)


def test_stage_population_rejects_an_unpartitioned_existing_table():
    class Trino:
        def _execute(self, _query):
            return None

        def get_table_columns(self, _schema, _table):
            from dags.scripts.whoscored_frozen_dq import _STAGE_COLUMNS

            return _STAGE_COLUMNS

    writer = SimpleNamespace(
        _get_trino_manager=lambda: Trino(),
        require_exact_identity_partition=lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("unpartitioned")
        ),
    )
    with pytest.raises(FrozenDQError, match="partition contract"):
        stage_frozen_population(_population(), writer=writer)


def test_stage_proof_fails_closed_on_row_hash_drift():
    relation = _relation()
    cursor = _Cursor(relation, invalid_stage_hashes=1)

    with pytest.raises(FrozenDQError, match="invalid_match_hashes"):
        frozen_historical_integrity(
            cursor,
            {"staged_relation": relation},
            parser_version="parser-v1",
            availability_version="availability-v1",
            match_dataset_tables={},
            preview_dataset_tables={},
        )


def test_semantic_failures_are_reported_only_for_staged_snapshot():
    relation = _relation()
    cursor = _Cursor(relation, semantic_failure=True)

    counters, parity = frozen_historical_integrity(
        cursor,
        {"staged_relation": relation},
        parser_version="parser-v1",
        availability_version="availability-v1",
        match_dataset_tables={"matches": "whoscored_matches"},
        preview_dataset_tables={},
    )

    assert counters["completed_matches"] == 1
    assert counters["inconsistent_match_dataset_states"] == 1
    assert parity["whoscored_matches"] == {
        "manifest": 0,
        "physical": 0,
        "current": 0,
        "owner_mismatches": 0,
    }
    assert all("FOR VERSION AS OF 123456" in query for query in cursor.queries)
    assert all("VALUES ('WS-1'" not in query for query in cursor.queries)


def test_dataset_parity_columns_preserve_table_order_and_counts():
    relation = _relation()
    cursor = _Cursor(relation, parity_values=[1, 2, 3, 0, 4, 5, 6, 0])

    _counters, parity = frozen_historical_integrity(
        cursor,
        {"staged_relation": relation},
        parser_version="parser-v1",
        availability_version="availability-v1",
        match_dataset_tables={
            "matches": "whoscored_matches",
            "events": "whoscored_events",
        },
        preview_dataset_tables={},
    )

    assert parity["whoscored_matches"] == {
        "manifest": 1,
        "physical": 2,
        "current": 3,
        "owner_mismatches": 0,
    }
    assert parity["whoscored_events"] == {
        "manifest": 4,
        "physical": 5,
        "current": 6,
        "owner_mismatches": 0,
    }


def test_match_and_preview_owner_compensation_cannot_false_green():
    relation = _relation(previews=1)
    cursor = _Cursor(
        relation,
        parity_values=[2, 2, 2, 1, 3, 3, 3, 1],
    )

    _counters, parity = frozen_historical_integrity(
        cursor,
        {"staged_relation": relation},
        parser_version="parser-v1",
        availability_version="availability-v1",
        match_dataset_tables={"events": "whoscored_events"},
        preview_dataset_tables={
            "preview_lineups": "whoscored_preview_lineups"
        },
    )

    assert parity["whoscored_events"] == {
        "manifest": 2,
        "physical": 2,
        "current": 2,
        "owner_mismatches": 1,
    }
    assert parity["whoscored_preview_lineups"] == {
        "manifest": 3,
        "physical": 3,
        "current": 3,
        "owner_mismatches": 1,
    }


def test_both_profile_owner_compensations_cannot_false_green():
    relation = _relation(players=2)
    cursor = _Cursor(relation, profile_owner_mismatches=[1, 2])

    _counters, parity = frozen_historical_integrity(
        cursor,
        {"staged_relation": relation},
        parser_version="parser-v1",
        availability_version="availability-v1",
        match_dataset_tables={},
        preview_dataset_tables={},
    )

    assert parity["whoscored_player_profile_versions"][
        "owner_mismatches"
    ] == 1
    assert parity["whoscored_player_stage_participations"][
        "owner_mismatches"
    ] == 2


def test_null_event_id_duplicates_are_counted_with_explicit_sentinels():
    relation = _relation()
    cursor = _Cursor(relation, null_duplicate_failure=True)

    counters, _parity = frozen_historical_integrity(
        cursor,
        {"staged_relation": relation},
        parser_version="parser-v1",
        availability_version="availability-v1",
        match_dataset_tables={},
        preview_dataset_tables={},
    )

    assert counters["duplicate_source_event_ids"] == 1
    assert counters["duplicate_team_event_ids"] == 1
    event_query = next(
        query for query in cursor.queries if "whoscored_frozen_events" in query
    )
    assert "COALESCE(CAST(e.source_event_id AS VARCHAR),'<null>')" in event_query
    assert "COALESCE(CAST(e.team_id AS VARCHAR),'<null>')" in event_query
    assert "COALESCE(CAST(e.team_event_id AS VARCHAR),'<null>')" in event_query


def test_scale_1_9m_matches_has_constant_eighteen_query_upper_bound():
    """Regression model: cardinality cannot restore per-1k round trips."""

    relation = _relation(matches=1_900_000, previews=950_000, players=3_000_000)
    cursor = _Cursor(relation)
    match_tables = {
        f"match_entity_{index}": f"whoscored_match_table_{index}"
        for index in range(7)
    }
    preview_tables = {
        f"preview_entity_{index}": f"whoscored_preview_table_{index}"
        for index in range(3)
    }

    counters, parity = frozen_historical_integrity(
        cursor,
        {"staged_relation": relation},
        parser_version="parser-v1",
        availability_version="availability-v1",
        match_dataset_tables=match_tables,
        preview_dataset_tables=preview_tables,
    )

    assert counters["completed_matches"] == 1_900_000
    assert counters["frozen_profile_players"] == 3_000_000
    assert set(parity) >= set(match_tables.values()) | set(preview_tables.values())
    assert len(cursor.queries) == 18
    assert max(map(len, cursor.queries)) < 40_000
    assert all("FOR VERSION AS OF 123456" in query for query in cursor.queries)
    assert sum("whoscored_events_current" in query for query in cursor.queries) == 1
    assert all(sqlglot.parse_one(query, read="trino") for query in cursor.queries)


def test_retention_cleanup_is_partition_bounded_and_keeps_current():
    selected = [f"{index:064x}" for index in range(DQ_STAGE_CLEANUP_LIMIT)]

    class Cursor:
        def __init__(self):
            self.queries: list[str] = []

        def execute(self, query):
            self.queries.append(query)

        def fetchall(self):
            return [(value,) for value in selected]

    cursor = Cursor()
    keep = "f" * 64
    cleaned = cleanup_staged_frozen_populations(
        cursor, keep_population_sha256=keep
    )

    assert cleaned == DQ_STAGE_CLEANUP_LIMIT
    assert len(cursor.queries) == 2
    assert f"LIMIT {DQ_STAGE_CLEANUP_LIMIT}" in cursor.queries[0]
    assert keep in cursor.queries[0]
    assert cursor.queries[1].startswith(f"DELETE FROM {DQ_STAGE_QUALIFIED}")
    assert keep not in cursor.queries[1]
    assert cursor.queries[1].count("',") == DQ_STAGE_CLEANUP_LIMIT - 1


def test_scheduled_retention_cleanup_does_not_require_a_current_population():
    class Cursor:
        def __init__(self):
            self.queries: list[str] = []

        def execute(self, query):
            self.queries.append(query)

        def fetchall(self):
            return []

    cursor = Cursor()

    assert cleanup_staged_frozen_populations(cursor) == 0
    assert len(cursor.queries) == 1
    assert "WHERE population_sha256<>" not in cursor.queries[0]
    assert f"LIMIT {DQ_STAGE_CLEANUP_LIMIT}" in cursor.queries[0]
