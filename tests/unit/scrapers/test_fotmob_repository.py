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
