from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from scrapers.fotmob.repository import (
    CURRENT_VIEW_SPECS,
    LEGACY_PARSER_VERSION,
    PARSER_VERSION,
    REPLACE_TARGET_MANIFEST_IDENTITIES,
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


def test_catalog_observation_identity_is_separate_from_content_identity():
    first = deterministic_target_batch_id(
        "catalog", "same-content", PARSER_VERSION, "run-1"
    )
    second = deterministic_target_batch_id(
        "catalog", "same-content", PARSER_VERSION, "run-2"
    )
    assert first != second


def test_native_parser_contract_is_v2_and_playoff_key_uses_match_ids():
    assert PARSER_VERSION == "fotmob-native-v2"
    assert "match_ids" in CURRENT_VIEW_SPECS["fotmob_playoff_brackets"][1]
    assert REPLACE_TARGET_MANIFEST_IDENTITIES["fotmob_leaderboards"] == (
        "target_type",
        "competition_id",
        "source_season_key",
        "entity_id",
    )


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
    assert "'not_available'" in sql
    assert f"parser_version = '{PARSER_VERSION}'" in sql
    assert f"parser_version = '{LEGACY_PARSER_VERSION}'" in sql
    assert "CASE WHEN parser_version =" in sql
    assert "CASE WHEN c.parser_version =" in sql
    assert "PARTITION BY target_type, competition_id, source_season_key" in sql
    assert "target_rn = 1" in sql
    assert "c.batch_id = r._target_batch_id" in sql
    assert 'r."competition_id", r."source_season_key", r."match_id"' in sql
    assert "ROW_NUMBER()" in sql


def test_current_view_rolls_from_last_good_v1_to_v2_replacement_or_tombstone():
    writer = ViewWriter()
    FotMobRepository(writer=writer).ensure_current_views()
    sql = " ".join(writer.trino.sql[0].split())

    assert (
        f"parser_version = '{PARSER_VERSION}' AND status IN ( "
        "'success', 'not_modified', 'not_available' )"
    ) in sql
    assert (
        f"parser_version = '{LEGACY_PARSER_VERSION}' "
        "AND status IN ('success', 'not_modified')"
    ) in sql
    assert (
        f"ORDER BY CASE WHEN parser_version = '{PARSER_VERSION}' "
        "THEN 1 ELSE 0 END DESC, completed_at DESC"
    ) in sql
    assert "WHERE target_rn = 1 AND status IN ('success', 'not_modified')" in sql


def test_entity_tombstone_supersedes_previous_success_for_skip_state():
    repository = MemoryFotMobRepository()
    success = _commit(target_type="match", target_key="match-1", entity_id="1")
    repository.record(success)
    repository.record(
        _commit(
            target_type="match",
            target_key="match-1",
            entity_id="1",
            status=ManifestStatus.NOT_AVAILABLE,
        )
    )

    assert repository.latest_success("match-1") is None
    assert repository.latest_entity_success("match", "1") is None


def test_memory_latest_success_can_be_scoped_to_current_writer_run():
    repository = MemoryFotMobRepository()
    prior = _commit(
        run_id="prior-generation",
        target_type="player",
        target_key="a" * 64,
        entity_id="10",
    )
    repository.record(prior)
    repository.record(
        _commit(
            run_id="current-generation",
            target_type="player",
            target_key="b" * 64,
            entity_id="10",
        )
    )

    assert (
        repository.latest_success(prior.target_key, run_id="prior-generation")["run_id"]
        == "prior-generation"
    )
    assert (
        repository.latest_success(prior.target_key, run_id="current-generation") is None
    )
    assert (
        repository.latest_entity_success("player", 10, run_id="current-generation")[
            "target_key"
        ]
        == "b" * 64
    )
    assert (
        repository.latest_entity_success("player", 10, run_id="missing-generation")
        is None
    )


def test_memory_raw_target_prefers_v2_and_tombstone_blocks_older_payload():
    repository = MemoryFotMobRepository()
    repository.record(
        _commit(
            run_id="legacy",
            target_type="player",
            target_key="a" * 64,
            entity_id="10",
            parser_version=LEGACY_PARSER_VERSION,
            completed_at=datetime(2026, 7, 20, 8, 30),
        )
    )
    repository.record(
        _commit(
            run_id="native",
            target_type="player",
            target_key="b" * 64,
            entity_id="10",
            parser_version=PARSER_VERSION,
            completed_at=datetime(2026, 7, 19, 8, 30),
        )
    )

    raw = repository.latest_entity_raw_target("player", 10)
    assert raw is not None
    assert raw["target_key"] == "b" * 64
    assert raw["parser_version"] == PARSER_VERSION

    repository.record(
        _commit(
            run_id="native-tombstone",
            target_type="player",
            target_key="c" * 64,
            entity_id="10",
            parser_version=PARSER_VERSION,
            status=ManifestStatus.NOT_AVAILABLE,
            completed_at=datetime(2026, 7, 21, 8, 30),
            raw_uri=None,
        )
    )

    assert repository.latest_entity_raw_target("player", 10) is None


def test_memory_raw_bearing_not_available_is_replayable():
    repository = MemoryFotMobRepository()
    repository.record(
        _commit(
            run_id="legacy-null-player",
            target_type="player",
            target_key="d" * 64,
            entity_id="10",
            parser_version=LEGACY_PARSER_VERSION,
            status=ManifestStatus.NOT_AVAILABLE,
        )
    )

    raw = repository.latest_entity_raw_target("player", 10)

    assert raw is not None
    assert raw["target_key"] == "d" * 64
    assert raw["status"] == ManifestStatus.NOT_AVAILABLE.value


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


def test_memory_completion_resume_is_scoped_to_exact_publication_run_id():
    repository = MemoryFotMobRepository()
    repository.record(
        _commit(
            run_id="prior-generation",
            target_type="scope_completion",
            target_key="scope-prior",
            entity_id="fmplan1-scope",
            source_season_key="2017/2019",
        )
    )
    repository.record(
        _commit(
            run_id="current-generation",
            target_type="scope_completion",
            target_key="scope-current",
            entity_id="fmplan1-scope",
            source_season_key="2017/2018",
        )
    )
    repository.record(
        _commit(
            run_id="prior-generation",
            target_type="competition_completion",
            target_key="transfer-prior",
            entity_id="fmplan1-transfer",
            source_season_key=None,
        )
    )
    repository.record(
        _commit(
            run_id="current-generation",
            target_type="competition_completion",
            target_key="transfer-current",
            entity_id="fmplan1-transfer",
            source_season_key=None,
            competition_id="47",
        )
    )

    assert repository.completed_scope_keys("fmplan1-scope") == {
        (289, "2017/2019"),
        (289, "2017/2018"),
    }
    assert repository.completed_scope_keys(
        "fmplan1-scope", run_id="current-generation"
    ) == {(289, "2017/2018")}
    assert repository.completed_competition_ids(
        "fmplan1-transfer", run_id="current-generation"
    ) == {47}
    assert (
        repository.completed_competition_ids(
            "fmplan1-transfer", run_id="missing-generation"
        )
        == set()
    )


def test_issue930_resume_does_not_count_any_of_158_prior_generation_scopes():
    scope_file = (
        Path(__file__).resolve().parents[3]
        / "configs"
        / "fotmob"
        / "issue-930-scopes.txt"
    )
    scopes = [
        (int(line.split("=", 1)[0]), line.split("=", 1)[1])
        for line in scope_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(scopes) == 158
    repository = MemoryFotMobRepository()
    for index, (competition_id, season) in enumerate(scopes):
        repository.record(
            _commit(
                run_id="prior-generation",
                target_type="scope_completion",
                target_key=f"{index + 1:064x}",
                competition_id=str(competition_id),
                source_season_key=season,
                entity_id="fmplan1-issue930",
            )
        )

    assert len(repository.completed_scope_keys("fmplan1-issue930")) == 158
    assert (
        repository.completed_scope_keys("fmplan1-issue930", run_id="current-generation")
        == set()
    )


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
    assert "attempts > 0" in sql
    assert "COALESCE(stale, FALSE) = FALSE" in sql
    assert "m.batch_id = c._target_batch_id" in sql
    # Trino forbids table-qualified references to a JOIN..USING column: the
    # coalesced column exists only unqualified (COLUMN_NOT_FOUND otherwise).
    assert "c.discovery_run_id" not in sql
    assert "USING (discovery_run_id)" in sql


def test_memory_catalog_history_ignores_offline_replay_observation():
    repository = MemoryFotMobRepository()
    repository.tables["fotmob_competitions"] = [
        {
            "competition_id": "47",
            "discovery_run_id": "live-run",
            "is_tombstoned": False,
        },
        {
            "competition_id": "99",
            "discovery_run_id": "live-run",
            "is_tombstoned": False,
        },
        {
            "competition_id": "47",
            "discovery_run_id": "replay-run",
            "is_tombstoned": False,
        },
    ]
    repository.commits.append(
        _commit(
            run_id="replay-run",
            target_type="all_leagues",
            target_key="catalog",
            attempts=0,
        )
    )

    assert repository.previous_catalog_snapshots() == [{47, 99}]


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


class ReconcileTrino:
    def __init__(self, writer):
        self.writer = writer
        self.queries = []

    def table_exists(self, schema, table):
        return bool(self.writer.rows.get(table))

    def execute_query(self, sql):
        self.queries.append(sql)
        marker = "FROM iceberg.bronze."
        table = sql.split(marker, 1)[1].split()[0]
        if "SELECT run_id, batch_id, target_key, content_hash" in sql:
            return [
                (
                    row.get("run_id"),
                    row.get("batch_id"),
                    row.get("target_key"),
                    row.get("content_hash"),
                    row.get("parser_version"),
                    row.get("status"),
                )
                for row in self.writer.rows.get(table, [])
            ]
        batch_column = "_target_batch_id" if "_target_batch_id" in sql else "batch_id"
        counts = {}
        for row in self.writer.rows.get(table, []):
            batch_id = str(row[batch_column])
            counts[batch_id] = counts.get(batch_id, 0) + 1
        return sorted(counts.items())


class ReconcileWriter(RecordingWriter):
    def __init__(self, fail_after_commit=None):
        super().__init__()
        self.rows = {}
        self.fail_after_commit = fail_after_commit
        self.trino = ReconcileTrino(self)

    def _get_trino_manager(self):
        return self.trino

    def write_dataframe(self, df, **kwargs):
        table = kwargs["table"]
        self.calls.append((df.copy(), dict(kwargs)))
        self.rows.setdefault(table, []).extend(df.to_dict("records"))
        if self.fail_after_commit == table:
            self.fail_after_commit = None
            raise RuntimeError("lost writer response after Iceberg commit")
        return f"iceberg.{kwargs['database']}.{table}"


def test_restart_reconciles_already_committed_target_batch_without_duplicate_rows():
    writer = ReconcileWriter(fail_after_commit="fotmob_matches")
    commit = _commit()
    dataset = TableRows(
        "fotmob_matches",
        [
            {
                "competition_id": "289",
                "source_season_key": "2017/2019",
                "match_id": "1",
            }
        ],
        "matches",
        ("competition_id", "source_season_key"),
    )
    first = FotMobRepository(writer=writer, batch_size=50)
    first.commit(commit, [dataset])
    with pytest.raises(RuntimeError, match="lost writer response"):
        first.flush()

    restarted = FotMobRepository(writer=writer, batch_size=50)
    restarted.commit(commit, [dataset])
    restarted.flush()

    assert len(writer.rows["fotmob_matches"]) == 1
    assert len(writer.rows["fotmob_ingest_manifest"]) == 1
    assert any("GROUP BY _target_batch_id" in query for query in writer.trino.queries)


def test_restart_fails_closed_on_partial_or_duplicate_target_batch_count():
    writer = ReconcileWriter()
    commit = _commit()
    writer.rows["fotmob_matches"] = [
        {"_target_batch_id": commit.batch_id},
        {"_target_batch_id": commit.batch_id},
    ]
    repository = FotMobRepository(writer=writer, batch_size=50)
    repository.commit(
        commit,
        [
            TableRows(
                "fotmob_matches",
                [
                    {
                        "competition_id": "289",
                        "source_season_key": "2017/2019",
                        "match_id": "1",
                    }
                ],
                "matches",
                ("competition_id", "source_season_key"),
            )
        ],
    )

    with pytest.raises(RuntimeError, match="expected either 0 or 1"):
        repository.flush()


def test_prior_failure_manifest_cannot_swallow_later_success_with_same_batch_id():
    writer = ReconcileWriter()
    failure = _commit(status=ManifestStatus.SCHEMA_DRIFT)
    success = _commit(status=ManifestStatus.SUCCESS)
    assert failure.batch_id == success.batch_id
    writer.rows["fotmob_ingest_manifest"] = [failure.manifest_row()]
    dataset = TableRows(
        "fotmob_matches",
        [
            {
                "competition_id": "289",
                "source_season_key": "2017/2019",
                "match_id": "1",
            }
        ],
        "matches",
        ("competition_id", "source_season_key"),
    )

    repository = FotMobRepository(writer=writer, batch_size=50)
    repository.commit(success, [dataset])
    repository.flush()

    assert [row["status"] for row in writer.rows["fotmob_ingest_manifest"]] == [
        "schema_drift",
        "success",
    ]
    assert len(writer.rows["fotmob_matches"]) == 1

    restarted = FotMobRepository(writer=writer, batch_size=50)
    restarted.commit(success, [dataset])
    restarted.flush()

    assert len(writer.rows["fotmob_ingest_manifest"]) == 2
    assert len(writer.rows["fotmob_matches"]) == 1


def test_restart_reconciles_semantically_identical_manifest_after_lost_response():
    writer = ReconcileWriter(fail_after_commit="fotmob_ingest_manifest")
    commit = _commit()
    first = FotMobRepository(writer=writer, batch_size=50)
    first.commit(commit)
    with pytest.raises(RuntimeError, match="lost writer response"):
        first.flush()

    restarted = FotMobRepository(writer=writer, batch_size=50)
    restarted.commit(commit)
    restarted.flush()

    assert len(writer.rows["fotmob_ingest_manifest"]) == 1


def test_later_run_appends_fresh_observation_for_unchanged_content():
    writer = ReconcileWriter()
    previous = _commit(
        run_id="daily-old",
        completed_at=datetime(2026, 7, 1, 14, 0),
    )
    current = _commit(
        run_id="daily-new",
        status=ManifestStatus.NOT_MODIFIED,
        fetched_at=datetime(2026, 7, 1, 13, 55),
        completed_at=datetime(2026, 7, 8, 14, 0),
    )
    assert previous.batch_id == current.batch_id
    writer.rows["fotmob_ingest_manifest"] = [previous.manifest_row()]

    repository = FotMobRepository(writer=writer, batch_size=50)
    repository.commit(current)
    repository.flush()

    rows = writer.rows["fotmob_ingest_manifest"]
    assert len(rows) == 2
    assert [row["run_id"] for row in rows] == ["daily-old", "daily-new"]
    assert rows[-1]["status"] == "not_modified"
    assert rows[-1]["completed_at"] == datetime(2026, 7, 8, 14, 0)


def test_restart_fails_closed_on_duplicate_exact_manifest_semantics():
    writer = ReconcileWriter()
    commit = _commit()
    row = commit.manifest_row()
    writer.rows["fotmob_ingest_manifest"] = [dict(row), dict(row)]
    repository = FotMobRepository(writer=writer, batch_size=50)
    repository.commit(commit)

    with pytest.raises(RuntimeError, match="has 2 stored rows; expected either 0 or 1"):
        repository.flush()


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


def _manifest_row(
    target_key,
    batch_id,
    target_type="match",
    entity_id=None,
    completed="2026-07-10 00:00:00",
    parser_version=PARSER_VERSION,
    status="success",
):
    return (
        target_key,
        batch_id,
        "c" * 64,
        "file:///raw/x.gz",
        parser_version,
        status,
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


def test_v1_manifest_rows_are_ineligible_for_v2_skip_state():
    writer = PreloadWriter(
        [
            _manifest_row(
                "https://example/match/1",
                "fm1-old",
                parser_version="fotmob-native-v1",
            )
        ]
    )
    repository = FotMobRepository(writer=writer)

    assert repository.preload_manifest_index() == 0
    assert repository.latest_success("https://example/match/1") is None
    assert f"parser_version = '{PARSER_VERSION}'" in writer.trino.queries[0]


def test_preload_keeps_v1_raw_target_as_offline_replay_fallback():
    target_key = "a" * 64
    writer = PreloadWriter(
        [
            _manifest_row(
                target_key,
                "fm1-old",
                target_type="player",
                entity_id="9",
                parser_version=LEGACY_PARSER_VERSION,
            )
        ]
    )
    repository = FotMobRepository(writer=writer)

    repository.preload_manifest_index()

    raw = repository.latest_entity_raw_target("player", 9)
    assert raw is not None
    assert raw["target_key"] == target_key
    assert raw["parser_version"] == LEGACY_PARSER_VERSION


def test_preload_keeps_v1_raw_bearing_not_available_for_offline_replay():
    target_key = "e" * 64
    writer = PreloadWriter(
        [
            _manifest_row(
                target_key,
                "fm1-null-player",
                target_type="player",
                entity_id="2090857",
                parser_version=LEGACY_PARSER_VERSION,
                status=ManifestStatus.NOT_AVAILABLE.value,
            )
        ]
    )
    repository = FotMobRepository(writer=writer)

    repository.preload_manifest_index()

    raw = repository.latest_entity_raw_target("player", 2090857)
    assert raw is not None
    assert raw["target_key"] == target_key
    assert raw["status"] == ManifestStatus.NOT_AVAILABLE.value


def test_preload_builds_exact_run_index_without_per_target_queries():
    target_key = "b" * 64
    writer = PreloadWriter([_manifest_row(target_key, "fm1-current", "player", "9")])
    repository = FotMobRepository(writer=writer)

    repository.preload_manifest_index(run_id="current-generation")
    queries_after_preload = len(writer.trino.queries)

    assert (
        repository.latest_success(target_key, run_id="current-generation")["batch_id"]
        == "fm1-current"
    )
    assert (
        repository.latest_entity_success("player", 9, run_id="current-generation")[
            "batch_id"
        ]
        == "fm1-current"
    )
    assert len(writer.trino.queries) == queries_after_preload
    assert "run_id = 'current-generation'" in writer.trino.queries[1]


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
            _manifest_row(
                "https://example/build-old/p9",
                "fm1-old",
                "player",
                "9",
                "2026-07-01 00:00:00",
            ),
            _manifest_row(
                "https://example/build-new/p9",
                "fm1-new",
                "player",
                "9",
                "2026-07-09 00:00:00",
            ),
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
            _commit(
                target_key=f"https://example/match/{index}",
                content_hash=str(index) * 64,
            ),
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

    inventory = [
        call for call in writer.calls if call[1]["table"] == "fotmob_field_inventory"
    ]
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
            _commit(
                target_key=f"https://example/m/{index}", content_hash=str(index) * 64
            ),
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
        _commit(
            target_key=f"https://example/m/{index}", content_hash=str(index % 10) * 64
        ),
        [
            TableRows(
                "fotmob_field_inventory", rows, "field_inventory", ("target_type",)
            )
        ],
    )


def test_inventory_dedup_survives_flush():
    # Inventory rows carry no target identity, so a key seen once needs no
    # second row this run: matches of one season share almost every json_path,
    # and re-emitting them each flush wrote ~2.4M rows per iteration.
    writer = RecordingWriter()
    repository = FotMobRepository(writer=writer, batch_size=2)

    for index in range(4):  # two flushes of two targets, identical rows
        _inventory_commit(repository, index, [_inventory_row()])

    inventory = [
        call for call in writer.calls if call[1]["table"] == "fotmob_field_inventory"
    ]
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

    inventory = [
        call for call in writer.calls if call[1]["table"] == "fotmob_field_inventory"
    ]
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
        if "SELECT run_id, batch_id, target_key, content_hash" in sql:
            return []
        if "COUNT(*)" in sql:
            return []
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

    inventory = [
        call for call in writer.calls if call[1]["table"] == "fotmob_field_inventory"
    ]
    assert len(inventory) == 1
    assert set(inventory[0][0]["json_path"]) == {"content.lineup"}
    preload_queries = [query for query in trino.queries if "SELECT DISTINCT" in query]
    assert len(preload_queries) == 1, "one seeding query per scope, not per row"
    assert "IN ('47', '47.0')" in preload_queries[0], "both VARCHAR spellings"
    assert "m.batch_id = i._target_batch_id" in preload_queries[0]
    assert f"m.parser_version = '{PARSER_VERSION}'" in preload_queries[0]


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

    inventory = [
        call for call in writer.calls if call[1]["table"] == "fotmob_field_inventory"
    ]
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

    assert not any("SELECT DISTINCT" in query for query in trino.queries)
    inventory = [
        call for call in writer.calls if call[1]["table"] == "fotmob_field_inventory"
    ]
    assert len(inventory) == 1


def test_inventory_preload_failure_fails_closed():
    trino = SeedTrino([], fail=True)
    writer = SeedWriter(trino)
    repository = FotMobRepository(writer=writer, batch_size=2)

    with pytest.raises(RuntimeError, match="trino down"):
        _inventory_commit(repository, 0, [_inventory_row()])

    assert len(trino.queries) == 1
    assert writer.calls == []


def test_inventory_preload_failure_does_not_leave_half_target_buffered():
    trino = SeedTrino([], fail=True)
    writer = SeedWriter(trino)
    repository = FotMobRepository(writer=writer, batch_size=2)
    commit = _commit()
    datasets = [
        TableRows(
            "fotmob_matches",
            [
                {
                    "competition_id": "47",
                    "source_season_key": "2025/2026",
                    "match_id": "1",
                }
            ],
            "matches",
            ("competition_id", "source_season_key"),
        ),
        TableRows(
            "fotmob_field_inventory",
            [_inventory_row()],
            "field_inventory",
            ("target_type",),
        ),
    ]

    with pytest.raises(RuntimeError, match="trino down"):
        repository.commit(commit, datasets)

    assert repository.flush() == []
    assert repository._pending == {}
    assert repository._pending_manifest == []

    trino.fail = False
    repository.commit(commit, datasets)
    repository.flush()

    written_tables = [call[1]["table"] for call in writer.calls]
    assert written_tables.count("fotmob_matches") == 1
    assert written_tables.count("fotmob_field_inventory") == 1
    assert written_tables.count("fotmob_ingest_manifest") == 1


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

    inventory = [
        call for call in writer.calls if call[1]["table"] == "fotmob_field_inventory"
    ]
    assert len(inventory) == 1
    assert set(inventory[0][0]["json_path"]) == {"content.stats", "content.lineup"}
