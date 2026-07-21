from datetime import datetime

import pandas as pd
import pytest

from scrapers.fotmob.repository import (
    FotMobRepository,
    ManifestStatus,
    MemoryFotMobRepository,
    TableRows,
    TargetCommit,
    deterministic_target_batch_id,
    normalize_rows,
)


class RecordingWriter:
    def __init__(self):
        self.calls = []

    def write_dataframe(self, df: pd.DataFrame, **kwargs):
        self.calls.append((df.copy(), dict(kwargs)))
        return f"iceberg.{kwargs['database']}.{kwargs['table']}"


class ViewTrino:
    def __init__(self, columns=None):
        self.sql = []
        self.columns = columns

    def table_exists(self, schema, table):
        return table == "fotmob_matches"

    def get_table_columns(self, schema, table):
        return self.columns or [
            "competition_id",
            "source_season_key",
            "match_id",
            "_target_batch_id",
            "_observed_at",
            "_ingested_at",
        ]

    def _execute(self, sql):
        self.sql.append(sql)


class ViewWriter(RecordingWriter):
    def __init__(self, columns=None):
        super().__init__()
        self.trino = ViewTrino(columns)

    def _get_trino_manager(self):
        return self.trino


class CatalogSnapshotTrino:
    def __init__(self):
        self.sql = []

    def table_exists(self, schema, table):
        return table == "fotmob_competitions"

    def execute_query(self, sql):
        self.sql.append(sql)
        return []


class CatalogSnapshotWriter(RecordingWriter):
    def __init__(self):
        super().__init__()
        self.trino = CatalogSnapshotTrino()

    def _get_trino_manager(self):
        return self.trino


def _commit(**overrides):
    values = {
        "run_id": "run-1",
        "target_type": "season",
        "target_key": "https://www.fotmob.com/api/data/leagues?id=289&season=2017%2F2019",
        "competition_id": "289",
        "source_season_key": "2017/2019",
        "status": ManifestStatus.SUCCESS,
        "content_hash": "a" * 64,
        "raw_uri": "file:///raw/a.json.gz",
        "fetched_at": datetime(2026, 7, 11, 8, 30),
    }
    values.update(overrides)
    return TargetCommit(**values)


def test_target_batch_id_is_replay_stable_and_parser_sensitive():
    first = deterministic_target_batch_id("target", "hash", "parser-v1")
    assert first == deterministic_target_batch_id("target", "hash", "parser-v1")
    assert first != deterministic_target_batch_id("target", "hash", "parser-v2")


def test_repository_writes_physical_rows_before_success_manifest():
    writer = RecordingWriter()
    repository = FotMobRepository(writer=writer)
    commit = _commit()

    paths = repository.commit(
        commit,
        [
            TableRows(
                table="fotmob_matches",
                entity_type="matches",
                partition_cols=("competition_id", "source_season_key"),
                rows=[
                    {
                        "competition_id": "289",
                        "source_season_key": "2017/2019",
                        "match_id": "1",
                        "source_json": {"status": {"finished": False}},
                    }
                ],
            )
        ],
    )

    assert paths == [
        "iceberg.bronze.fotmob_matches",
        "iceberg.bronze.fotmob_ingest_manifest",
    ]
    assert [call[1]["table"] for call in writer.calls] == [
        "fotmob_matches",
        "fotmob_ingest_manifest",
    ]
    physical = writer.calls[0][0].iloc[0]
    manifest = writer.calls[1][0].iloc[0]
    assert physical["source_season_key"] == "2017/2019"
    assert physical["source_json"] == '{"status":{"finished":false}}'
    assert physical["_target_batch_id"] == commit.batch_id
    assert manifest["status"] == "success"
    assert manifest["actual_counts_json"] == '{"matches":1}'


def test_non_success_manifest_cannot_publish_rows():
    repository = MemoryFotMobRepository()
    with pytest.raises(ValueError, match="cannot carry|failed targets"):
        repository.commit(
            _commit(status=ManifestStatus.SCHEMA_DRIFT),
            [TableRows("fotmob_matches", [{"match_id": "1"}], "matches")],
        )


def test_manifest_count_mismatch_fails_before_storage_write():
    writer = RecordingWriter()
    repository = FotMobRepository(writer=writer)
    with pytest.raises(ValueError, match="actual counts disagree"):
        repository.commit(
            _commit(actual_counts={"matches": 2}),
            [TableRows("fotmob_matches", [{"match_id": "1"}], "matches")],
        )
    assert writer.calls == []


def test_source_expected_count_mismatch_fails_before_storage_write():
    writer = RecordingWriter()
    repository = FotMobRepository(writer=writer)
    with pytest.raises(ValueError, match="source expected counts disagree"):
        repository.commit(
            _commit(expected_counts={"matches": 2}),
            [TableRows("fotmob_matches", [{"match_id": "1"}], "matches")],
        )
    assert writer.calls == []


def test_normalize_rows_serializes_mixed_nested_values_without_arrow_coercion():
    rows = normalize_rows(
        [
            {
                "id": 1,
                "details": {"height": 180},
                "items": [1, "2"],
            }
        ]
    )
    assert rows == [
        {
            "id": 1,
            "details": '{"height":180}',
            "items": '[1,"2"]',
        }
    ]


def test_exact_source_season_keys_do_not_collide():
    repository = MemoryFotMobRepository()
    repository.record(_commit(source_season_key="2017/2019", target_key="a"))
    repository.record(_commit(source_season_key="2017/2018", target_key="b"))
    assert {item.source_season_key for item in repository.commits} == {
        "2017/2019",
        "2017/2018",
    }


def test_current_view_exposes_only_manifest_commits_and_deduplicates_natural_key():
    writer = ViewWriter()
    repository = FotMobRepository(writer=writer)

    created = repository.ensure_current_views()

    assert created == ["iceberg.bronze.fotmob_matches_current"]
    sql = writer.trino.sql[0]
    assert "status IN ('success', 'not_modified')" in sql
    assert "PARTITION BY target_type, competition_id, source_season_key" in sql
    assert "target_rn = 1" in sql
    assert "c.batch_id = r._target_batch_id" in sql
    assert 'r."competition_id", r."source_season_key", r."match_id"' in sql
    assert "ROW_NUMBER()" in sql


def test_current_view_fails_closed_when_any_natural_key_column_is_missing():
    writer = ViewWriter(
        [
            "competition_id",
            "source_season_key",
            "_target_batch_id",
            "_observed_at",
            "_ingested_at",
        ]
    )

    with pytest.raises(ValueError, match="match_id"):
        FotMobRepository(writer=writer).ensure_current_views()


def test_current_view_fails_closed_without_manifest_batch_column():
    writer = ViewWriter(
        [
            "competition_id",
            "source_season_key",
            "match_id",
            "_observed_at",
            "_ingested_at",
        ]
    )

    with pytest.raises(ValueError, match="_target_batch_id"):
        FotMobRepository(writer=writer).ensure_current_views()


def test_memory_completion_markers_are_exact_signature_and_season_scoped():
    repository = MemoryFotMobRepository()
    repository.record(
        _commit(
            target_type="scope_completion",
            target_key="scope-a",
            entity_id="fmplan1-a",
            source_season_key="2017/2019",
        )
    )
    repository.record(
        _commit(
            target_type="scope_completion",
            target_key="scope-b",
            entity_id="fmplan1-b",
            source_season_key="2017/2018",
        )
    )
    repository.record(
        _commit(
            target_type="competition_completion",
            target_key="competition-a",
            entity_id="fmplan1-a",
            source_season_key=None,
        )
    )

    assert repository.completed_scope_keys("fmplan1-a") == {(289, "2017/2019")}
    assert repository.completed_scope_keys("fmplan1-b") == {(289, "2017/2018")}
    assert repository.completed_competition_ids("fmplan1-a") == {289}
    assert repository.scope_completion_times("fmplan1-a") == {
        (289, "2017/2019"): datetime(2026, 7, 11, 8, 30)
    }
    assert repository.competition_completion_times("fmplan1-a") == {
        289: datetime(2026, 7, 11, 8, 30)
    }


def test_memory_current_squad_ids_come_only_from_latest_team_batch():
    repository = MemoryFotMobRepository()
    old = _commit(
        target_type="team",
        target_key="team-old",
        entity_id="1",
        source_season_key=None,
        content_hash="1" * 64,
    )
    repository.commit(
        old,
        [
            TableRows(
                "fotmob_squad_snapshots",
                [
                    {"team_id": "1", "member_type": "player", "member_id": "10"},
                    {"team_id": "1", "member_type": "coach", "member_id": "11"},
                ],
                "squad_snapshots",
            )
        ],
    )
    newest = _commit(
        target_type="team",
        target_key="team-new",
        entity_id="1",
        source_season_key=None,
        content_hash="2" * 64,
    )
    repository.commit(
        newest,
        [
            TableRows(
                "fotmob_squad_snapshots",
                [{"team_id": "1", "member_type": "player", "member_id": "20"}],
                "squad_snapshots",
            )
        ],
    )

    assert repository.current_squad_player_ids(1) == {20}


def test_catalog_absence_logic_ignores_uncommitted_physical_snapshots():
    writer = CatalogSnapshotWriter()
    repository = FotMobRepository(writer=writer)

    assert repository.previous_catalog_snapshots() == []

    sql = writer.trino.sql[0]
    assert "target_type = 'all_leagues'" in sql
    assert "status IN ('success', 'not_modified')" in sql
    assert "m.batch_id = c._target_batch_id" in sql
    # Trino forbids table-qualified references to a JOIN..USING column: the
    # coalesced column exists only unqualified (COLUMN_NOT_FOUND otherwise).
    assert "c.discovery_run_id" not in sql
    assert "USING (discovery_run_id)" in sql


def test_batched_commits_write_one_iceberg_commit_per_table_not_per_target():
    # One commit per target left the manifest with a single-row data file per
    # target (production: 4.3k files -> 9.5 s per one-row insert). Buffering
    # collapses N targets into one commit per table.
    writer = RecordingWriter()
    repository = FotMobRepository(writer=writer, batch_size=3)

    for index in range(3):
        repository.commit(
            _commit(
                target_key=f"https://example/{index}",
                content_hash=str(index) * 64,
            ),
            [
                TableRows(
                    "fotmob_matches",
                    [
                        {
                            "competition_id": "289",
                            "source_season_key": "2017/2019",
                            "match_id": str(index),
                        }
                    ],
                    "matches",
                    ("competition_id", "source_season_key"),
                )
            ],
        )
        if index < 2:
            assert writer.calls == [], "buffered targets must not write early"

    assert [call[1]["table"] for call in writer.calls] == [
        "fotmob_matches",
        "fotmob_ingest_manifest",
    ]
    assert len(writer.calls[0][0]) == 3
    assert len(writer.calls[1][0]) == 3


def test_flush_writes_rows_before_manifest_so_a_crash_only_loses_visibility():
    writer = RecordingWriter()
    repository = FotMobRepository(writer=writer, batch_size=50)

    repository.commit(
        _commit(),
        [
            TableRows(
                "fotmob_matches",
                [{"competition_id": "289", "source_season_key": "2017/2019"}],
                "matches",
                ("competition_id", "source_season_key"),
            )
        ],
    )
    assert writer.calls == []

    paths = repository.flush()

    assert paths == [
        "iceberg.bronze.fotmob_matches",
        "iceberg.bronze.fotmob_ingest_manifest",
    ]
    assert [call[1]["table"] for call in writer.calls][-1] == "fotmob_ingest_manifest"
    assert repository.flush() == [], "an empty buffer must not commit"


def test_buffered_manifest_answers_this_runs_incremental_reads():
    # The planner reads back what the run just committed (batch reuse, entity
    # freshness). A buffered commit is invisible to Trino, so it must be
    # served from the buffer or the run re-fetches and re-writes the target.
    writer = RecordingWriter()
    repository = FotMobRepository(writer=writer, batch_size=50)
    commit = _commit(target_type="team", entity_id="8650")

    repository.commit(commit)

    assert repository.latest_success(commit.target_key)["batch_id"] == commit.batch_id
    assert repository.latest_entity_success("team", 8650)["batch_id"] == commit.batch_id
    assert repository.latest_entity_success("team", 999) is None


def test_buffered_squad_rows_still_feed_the_player_fan_out():
    writer = ViewWriter()
    repository = FotMobRepository(writer=writer, batch_size=50)

    repository.commit(
        _commit(target_type="team", entity_id="1"),
        [
            TableRows(
                "fotmob_squad_snapshots",
                [
                    {"team_id": "1", "member_type": "player", "member_id": "20"},
                    {"team_id": "1", "member_type": "coach", "member_id": "99"},
                    {"team_id": "2", "member_type": "player", "member_id": "31"},
                ],
                "squad_snapshots",
            )
        ],
    )

    assert repository.current_squad_player_ids(1) == {20}
    assert repository.current_squad_player_ids(2) == {31}


def test_record_stays_callable_when_the_commit_is_buffered():
    repository = FotMobRepository(writer=RecordingWriter(), batch_size=10)

    assert repository.record(_commit(status=ManifestStatus.NOT_AVAILABLE)) == ""


class PreloadTrino:
    """Counts manifest reads: the whole point of the index is that there are none."""

    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def table_exists(self, schema, table):
        return True

    def get_table_columns(self, schema, table):
        return []

    def execute_query(self, sql):
        self.queries.append(sql)
        return self.rows

    def _execute(self, sql):
        self.queries.append(sql)


class PreloadWriter(RecordingWriter):
    def __init__(self, rows):
        super().__init__()
        self.trino = PreloadTrino(rows)

    def _get_trino_manager(self):
        return self.trino


def _manifest_row(target_key, batch_id, target_type="match", entity_id=None, completed="2026-07-10 00:00:00"):
    return (
        target_key,
        batch_id,
        "c" * 64,
        "file:///raw/x.gz",
        "fotmob-native-v1",
        "success",
        completed,
        completed,
        "{}",
        "{}",
        target_type,
        entity_id,
    )


def test_preloaded_manifest_answers_every_target_read_without_a_query():
    # Incremental planning asked Trino once per target (678 of ~1900 queries
    # per 40 min in production). One preload replaces all of them.
    writer = PreloadWriter(
        [
            _manifest_row("https://example/match/1", "fm1-a"),
            _manifest_row("https://example/player/9", "fm1-b", "player", "9"),
        ]
    )
    repository = FotMobRepository(writer=writer)

    assert repository.preload_manifest_index() == 2
    queries_after_preload = len(writer.trino.queries)

    assert repository.latest_success("https://example/match/1")["batch_id"] == "fm1-a"
    assert repository.latest_entity_success("player", 9)["batch_id"] == "fm1-b"
    # A miss is authoritative: the index holds every committed target.
    assert repository.latest_success("https://example/match/404") is None
    assert repository.latest_entity_success("player", 404) is None
    assert len(writer.trino.queries) == queries_after_preload


def test_a_flushed_commit_stays_visible_to_later_reads():
    # The pending buffer is cleared on flush; without folding it into the index
    # a target this run just ingested would look absent and be refetched.
    writer = PreloadWriter([])
    repository = FotMobRepository(writer=writer, batch_size=50)
    repository.preload_manifest_index()
    commit = _commit(target_type="team", entity_id="8650")

    repository.commit(commit)
    repository.flush()

    assert repository.latest_success(commit.target_key)["batch_id"] == commit.batch_id
    assert repository.latest_entity_success("team", 8650)["batch_id"] == commit.batch_id


def test_entity_index_keeps_the_newest_target_key_of_one_entity():
    # A rotating Next.js build id gives one player several target keys.
    writer = PreloadWriter(
        [
            _manifest_row("https://example/build-old/p9", "fm1-old", "player", "9",
                          "2026-07-01 00:00:00"),
            _manifest_row("https://example/build-new/p9", "fm1-new", "player", "9",
                          "2026-07-09 00:00:00"),
        ]
    )
    repository = FotMobRepository(writer=writer)
    repository.preload_manifest_index()

    assert repository.latest_entity_success("player", "9")["batch_id"] == "fm1-new"


def test_field_inventory_rows_are_deduplicated_across_a_buffered_batch():
    # Inventory rows carry no target identity: every match of a season emits the
    # same (target_type, competition, season, path, disposition) rows. Writing
    # fifty targets' worth staged ~30k rows for ~600 distinct ones, and that one
    # table dominated the run's Trino statements.
    writer = RecordingWriter()
    repository = FotMobRepository(writer=writer, batch_size=3)

    for index in range(3):
        repository.commit(
            _commit(target_key=f"https://example/match/{index}", content_hash=str(index) * 64),
            [
                TableRows(
                    "fotmob_field_inventory",
                    [
                        {
                            "target_type": "match",
                            "competition_id": "47",
                            "source_season_key": "2025/2026",
                            "json_path": "content.stats",
                            "disposition": "typed",
                        },
                        {
                            "target_type": "match",
                            "competition_id": "47",
                            "source_season_key": "2025/2026",
                            "json_path": "content.lineup",
                            "disposition": "typed",
                        },
                    ],
                    "field_inventory",
                    ("target_type",),
                )
            ],
        )

    inventory = [call for call in writer.calls if call[1]["table"] == "fotmob_field_inventory"]
    assert len(inventory) == 1
    frame = inventory[0][0]
    assert len(frame) == 2, "three identical targets must not write six rows"
    assert set(frame["json_path"]) == {"content.stats", "content.lineup"}


def test_deduplication_never_collapses_rows_that_carry_identity():
    # Matches, players, teams… all carry their own id: two targets are two rows.
    writer = RecordingWriter()
    repository = FotMobRepository(writer=writer, batch_size=2)

    for index in range(2):
        repository.commit(
            _commit(target_key=f"https://example/m/{index}", content_hash=str(index) * 64),
            [
                TableRows(
                    "fotmob_matches",
                    [
                        {
                            "competition_id": "47",
                            "source_season_key": "2025/2026",
                            "match_id": str(index),
                        }
                    ],
                    "matches",
                    ("competition_id", "source_season_key"),
                )
            ],
        )

    matches = [call for call in writer.calls if call[1]["table"] == "fotmob_matches"]
    assert len(matches[0][0]) == 2


def _inventory_row(json_path="content.stats"):
    return {
        "target_type": "match",
        "competition_id": "47",
        "source_season_key": "2025/2026",
        "json_path": json_path,
        "disposition": "typed",
    }


def _inventory_commit(repository, index, rows):
    repository.commit(
        _commit(target_key=f"https://example/m/{index}", content_hash=str(index % 10) * 64),
        [TableRows("fotmob_field_inventory", rows, "field_inventory", ("target_type",))],
    )


def test_inventory_dedup_survives_flush():
    # Inventory rows carry no target identity, so a key seen once needs no
    # second row this run: matches of one season share almost every json_path,
    # and re-emitting them each flush wrote ~2.4M rows per iteration.
    writer = RecordingWriter()
    repository = FotMobRepository(writer=writer, batch_size=2)

    for index in range(4):  # two flushes of two targets, identical rows
        _inventory_commit(repository, index, [_inventory_row()])

    inventory = [call for call in writer.calls if call[1]["table"] == "fotmob_field_inventory"]
    assert len(inventory) == 1, "the second flush must not re-write a seen key"
    assert len(inventory[0][0]) == 1


def test_inventory_dedup_still_writes_new_keys_after_flush():
    writer = RecordingWriter()
    repository = FotMobRepository(writer=writer, batch_size=2)

    for index in range(2):
        _inventory_commit(repository, index, [_inventory_row()])
    for index in range(2, 4):
        _inventory_commit(
            repository, index, [_inventory_row(), _inventory_row("content.lineup")]
        )

    inventory = [call for call in writer.calls if call[1]["table"] == "fotmob_field_inventory"]
    assert [len(frame) for frame, _ in inventory] == [1, 1]
    assert set(inventory[1][0]["json_path"]) == {"content.lineup"}


class SeedTrino:
    def __init__(self, rows, fail=False):
        self.rows = rows
        self.fail = fail
        self.queries = []

    def table_exists(self, schema, table):
        return True

    def execute_query(self, sql):
        self.queries.append(sql)
        if self.fail:
            raise RuntimeError("trino down")
        return self.rows


class SeedWriter(RecordingWriter):
    def __init__(self, trino):
        super().__init__()
        self.trino = trino

    def _get_trino_manager(self):
        return self.trino


def test_inventory_preload_dedups_keys_already_written_by_earlier_runs():
    # Iterations resume mid-scope: paths a season's matches share were written
    # by earlier runs; one SELECT DISTINCT per scope replaces re-learning them.
    trino = SeedTrino([("content.stats", "typed")])
    writer = SeedWriter(trino)
    repository = FotMobRepository(writer=writer, batch_size=2)

    for index in range(2):
        _inventory_commit(
            repository, index, [_inventory_row(), _inventory_row("content.lineup")]
        )

    inventory = [call for call in writer.calls if call[1]["table"] == "fotmob_field_inventory"]
    assert len(inventory) == 1
    assert set(inventory[0][0]["json_path"]) == {"content.lineup"}
    assert len(trino.queries) == 1, "one seeding query per scope, not per row"
    assert "IN ('47', '47.0')" in trino.queries[0], "both VARCHAR spellings"


def test_inventory_preload_normalizes_float_string_spellings():
    # The table answers with whatever spelling it holds; an int 47 live row
    # must still collide with it after normalization.
    trino = SeedTrino([("content.stats", "typed")])
    writer = SeedWriter(trino)
    repository = FotMobRepository(writer=writer, batch_size=2)

    row = _inventory_row()
    row["competition_id"] = 47  # live rows carry ints
    _inventory_commit(repository, 0, [row])
    _inventory_commit(repository, 1, [])

    inventory = [call for call in writer.calls if call[1]["table"] == "fotmob_field_inventory"]
    assert inventory == [], "preloaded key must drop the live int-keyed row"


def test_inventory_preload_skips_player_scope():
    trino = SeedTrino([])
    writer = SeedWriter(trino)
    repository = FotMobRepository(writer=writer, batch_size=2)

    row = _inventory_row()
    row["target_type"] = "player"
    row["competition_id"] = None
    row["source_season_key"] = None
    _inventory_commit(repository, 0, [row])
    _inventory_commit(repository, 1, [])

    assert trino.queries == []
    inventory = [call for call in writer.calls if call[1]["table"] == "fotmob_field_inventory"]
    assert len(inventory) == 1


def test_inventory_preload_failure_degrades_to_run_local_dedup():
    trino = SeedTrino([], fail=True)
    writer = SeedWriter(trino)
    repository = FotMobRepository(writer=writer, batch_size=2)

    for index in range(4):
        _inventory_commit(repository, index, [_inventory_row()])

    assert len(trino.queries) == 1, "a failing scope query must not retry per row"
    inventory = [call for call in writer.calls if call[1]["table"] == "fotmob_field_inventory"]
    assert len(inventory) == 1, "run-local dedup still applies"
    assert len(inventory[0][0]) == 1


def test_failed_flush_retry_writes_inventory_rows_exactly_once():
    # A failed flush keeps both the buffer and the seen keys: the retry must
    # re-append the very same rows, not lose them to the dedup set.
    class FlakyWriter(RecordingWriter):
        def __init__(self):
            super().__init__()
            self.failures = 1

        def write_dataframe(self, df, **kwargs):
            if self.failures:
                self.failures -= 1
                raise RuntimeError("iceberg commit failed")
            return super().write_dataframe(df, **kwargs)

    writer = FlakyWriter()
    repository = FotMobRepository(writer=writer, batch_size=2)

    _inventory_commit(repository, 0, [_inventory_row()])
    with pytest.raises(RuntimeError):
        _inventory_commit(repository, 1, [_inventory_row("content.lineup")])
    repository.flush()

    inventory = [call for call in writer.calls if call[1]["table"] == "fotmob_field_inventory"]
    assert len(inventory) == 1
    assert set(inventory[0][0]["json_path"]) == {"content.stats", "content.lineup"}
