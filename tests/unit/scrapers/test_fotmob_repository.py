import json
import threading
import time
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from scrapers.fotmob.repository import (
    CURRENT_VIEW_SPECS,
    LEGACY_PARSER_VERSION,
    _COMMIT_CONFLICT_RETRIES,
    PARSER_VERSION,
    REPLACE_TARGET_MANIFEST_IDENTITIES,
    FotMobRepository,
    ManifestStatus,
    MemoryFotMobRepository,
    TABLE_PARTITIONS,
    TableRows,
    TargetCommit,
    deterministic_target_batch_id,
    normalize_rows,
)
from scrapers.fotmob.domain import (
    CompetitionScopeEvidence,
    ProbeStatus,
    ScopeDecision,
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


class MissingPlayerTrino:
    def __init__(self, *, tables=None, rows=None):
        self.tables = set(
            tables
            or {
                "fotmob_squad_snapshots_current",
                "fotmob_player_snapshots_current",
            }
        )
        self.rows = list(rows or [])
        self.sql = []

    def table_exists(self, schema, table):
        return table in self.tables

    def execute_query(self, sql):
        self.sql.append(sql)
        return self.rows


class MissingPlayerWriter(RecordingWriter):
    def __init__(self, **kwargs):
        super().__init__()
        self.trino = MissingPlayerTrino(**kwargs)

    def _get_trino_manager(self):
        return self.trino


class ScopeEvidenceTrino:
    def __init__(self, probe_attempt_count=0):
        self.sql = []
        self.probe_attempt_count = probe_attempt_count

    def table_exists(self, schema, table):
        return table == "fotmob_competition_scope_observations"

    def execute_query(self, sql):
        self.sql.append(sql)
        return [
            (
                "47",
                "Premier League",
                "Premier League",
                "male",
                "adult",
                "league",
                "success",
                "included",
                "structurally confirmed adult men's competition",
                "include_structural_male_adult",
                "fotmob-men-v1",
                "b" * 64,
                "c" * 64,
                "d" * 64,
                0,
                self.probe_attempt_count,
                None,
                datetime(2026, 8, 8, 10),
            )
        ]


class ScopeEvidenceWriter(RecordingWriter):
    def __init__(self, probe_attempt_count=0):
        super().__init__()
        self.trino = ScopeEvidenceTrino(probe_attempt_count)

    def _get_trino_manager(self):
        return self.trino


class EntityAttemptTrino:
    def __init__(self):
        self.sql = []

    def execute_query(self, sql):
        self.sql.append(sql)
        return [
            (
                "target-key",
                "batch-id",
                None,
                None,
                PARSER_VERSION,
                "retryable_failure",
                False,
                datetime(2026, 8, 8, 9),
                datetime(2026, 8, 8, 10),
                "{}",
                "{}",
            )
        ]


class EntityAttemptWriter(RecordingWriter):
    def __init__(self):
        super().__init__()
        self.trino = EntityAttemptTrino()

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


def test_memory_scope_attempts_are_durable_contract_bound_and_incremented():
    repository = MemoryFotMobRepository()
    attempted_at = datetime(2026, 8, 8, 10)
    first = repository.record_scope_attempt(
        run_id="run-1",
        competition_id=47,
        source_season_key="2025 Apertura",
        plan_signature="fmplan1-one",
        outcome="retryable",
        reason="HTTP 503",
        last_attempt_at=attempted_at,
        next_retry_at=attempted_at + timedelta(minutes=15),
        attempt_identities=("raw-attempt-1",),
    )
    second = repository.record_scope_attempt(
        run_id="run-2",
        competition_id=47,
        source_season_key="2025 Apertura",
        plan_signature="fmplan1-one",
        outcome="success",
        reason="scope completion committed",
        last_attempt_at=attempted_at + timedelta(minutes=20),
        attempt_identities=("raw-attempt-2",),
    )

    assert first.attempt_count == 1
    assert second.attempt_count == 2
    assert repository.scope_attempt_states("fmplan1-one") == {
        (47, "2025 Apertura"): second
    }
    assert repository.scope_attempt_states("fmplan1-other") == {}
    commits = [c for c in repository.commits if c.target_type == "scope_attempt"]
    assert [c.entity_id for c in commits] == ["fmplan1-one", "fmplan1-one"]
    assert commits[0].attempts == 0  # transport retry counters are not overloaded

    retried_same_run = repository.record_scope_attempt(
        run_id="run-2",
        competition_id=47,
        source_season_key="2025 Apertura",
        plan_signature="fmplan1-one",
        outcome="success",
        reason="Airflow retried the same generation",
        last_attempt_at=attempted_at + timedelta(minutes=21),
    )
    assert retried_same_run.attempt_count == 2


def test_scope_attempt_retry_due_survives_an_attempt_longer_than_its_backoff():
    """Скоуп, работавший дольше окна повтора, не приносит просроченный срок.

    Бэкофф отсчитывается от начала работы над скоупом, а попытка фиксируется в
    её конце: у скоупа, качавшего игроков часами, next_retry_at оказывался в
    прошлом, и приёмка каталога краснила ран целиком.
    """

    repository = MemoryFotMobRepository()
    started_at = datetime(2026, 8, 12, 20, 17)
    finished_at = datetime(2026, 8, 12, 23, 4)
    overdue = repository.record_scope_attempt(
        run_id="run-1",
        competition_id=489,
        source_season_key="2026/2027",
        plan_signature="fmplan1-one",
        outcome="retryable",
        reason="scope 489=2026/2027 incomplete",
        last_attempt_at=finished_at,
        next_retry_at=started_at + timedelta(hours=1),
        attempt_identities=("run-1:489=2026/2027",),
    )
    assert overdue.next_retry_at > overdue.last_attempt_at

    honest = repository.record_scope_attempt(
        run_id="run-2",
        competition_id=490,
        source_season_key="2026/2027",
        plan_signature="fmplan1-one",
        outcome="deferred",
        reason="run deadline deferred scope",
        last_attempt_at=finished_at,
        next_retry_at=finished_at + timedelta(minutes=15),
        attempt_identities=("run-2:490=2026/2027",),
    )
    assert honest.next_retry_at == finished_at + timedelta(minutes=15)


class ScopeAttemptTrino:
    """Trino, отдающий одну прошлую попытку и считающий обращения к журналу."""

    def __init__(self):
        self.sql = []

    def table_exists(self, _schema, _table):
        # Единый путь записи сверяет пачку с хранилищем перед записью и при
        # batch_size=1 тоже — заглушка обязана отвечать на тот же вопрос, что и
        # настоящий Trino.
        return False

    def execute_query(self, sql):
        self.sql.append(sql)
        return [
            (
                "47",
                "2025/2026",
                "fmplan1-journal",
                datetime(2026, 8, 8, 9),
                None,
                json.dumps(
                    {
                        "plan_signature": "fmplan1-journal",
                        "attempt_count": 1,
                        "last_attempt_at": "2026-08-08T09:00:00+00:00",
                        "next_retry_at": None,
                        "outcome": "retryable",
                        "reason": "HTTP 503",
                        "attempt_identities": ["run-0:47=2025/2026"],
                        "run_id": "run-0",
                    }
                ),
            )
        ]

    @property
    def attempt_queries(self):
        return [sql for sql in self.sql if "target_type = 'scope_attempt'" in sql]


class ScopeAttemptWriter(RecordingWriter):
    def __init__(self):
        super().__init__()
        self.trino = ScopeAttemptTrino()

    def _get_trino_manager(self):
        return self.trino


def test_scope_attempt_journal_is_read_once_per_run():
    """Карта попыток читается одним оконным запросом, а не на каждую попытку.

    Под контентной подписью запрос возвращал ноль строк и его цена была
    незаметна. Под стабильной он проходит по всей истории полосы, а
    record_scope_attempt зовёт его на каждый из ~460 скоупов — это тот самый
    узел, из-за которого ран упирается в Trino вместо источника.
    """

    writer = ScopeAttemptWriter()
    repository = FotMobRepository(writer=writer)

    before = repository.scope_attempt_states("fmplan1-journal")
    assert before[(47, "2025/2026")].attempt_count == 1

    for index in range(3):
        repository.record_scope_attempt(
            run_id="run-1",
            competition_id=100 + index,
            source_season_key="2025/2026",
            plan_signature="fmplan1-journal",
            outcome="success",
            reason="scope completion committed",
            last_attempt_at=datetime(2026, 8, 8, 10, index),
            attempt_identities=(f"run-1:{100 + index}=2025/2026",),
        )

    assert len(writer.trino.attempt_queries) == 1
    after = repository.scope_attempt_states("fmplan1-journal")
    assert sorted(identity[0] for identity in after) == [47, 100, 101, 102]

    # read-your-writes переживает flush(): pending-манифест очищается, и без
    # собственного среза счётчик попыток поехал бы назад.
    repository.flush()
    flushed = repository.scope_attempt_states("fmplan1-journal")
    assert sorted(identity[0] for identity in flushed) == [47, 100, 101, 102]
    assert len(writer.trino.attempt_queries) == 1


def test_source_gap_requires_two_distinct_successful_attempt_identities():
    repository = MemoryFotMobRepository()

    with pytest.raises(ValueError, match="two distinct"):
        repository.record_scope_attempt(
            run_id="run-1",
            competition_id=47,
            source_season_key="2025/2026",
            plan_signature="fmplan1-one",
            outcome="source_gap",
            reason="finished match payload absent",
            attempt_identities=("only-one",),
        )


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


def _scope_evidence(
    competition_id=47,
    *,
    observed_at=datetime(2026, 8, 8, 10),
    decision=ScopeDecision.INCLUDED,
):
    return CompetitionScopeEvidence(
        competition_id=competition_id,
        catalog_name="Premier League",
        profile_name="Premier League",
        source_gender="male",
        source_age_group="adult",
        source_type="league",
        probe_status=ProbeStatus.SUCCESS,
        decision=decision,
        reason="structurally confirmed adult men's competition",
        policy_rule="include_structural_male_adult",
        classifier_version="fotmob-men-v1",
        profile_target_key="b" * 64,
        profile_content_hash="c" * 64,
        catalog_fingerprint="d" * 64,
        authoritative_miss_count=0,
        next_probe_at=None,
        observed_at=observed_at,
    )


def test_scope_observation_table_and_current_view_are_profile_manifest_gated():
    assert TABLE_PARTITIONS["fotmob_competition_scope_observations"] == (
        "competition_id",
    )


def test_repository_creates_stable_scope_evidence_schema_before_first_probe():
    writer = ViewWriter()

    FotMobRepository(writer=writer).ensure_schema()

    assert len(writer.trino.sql) == 3
    evidence_sql = " ".join(writer.trino.sql[1].split())
    assert "fotmob_competition_scope_observations" in evidence_sql
    assert "authoritative_miss_count INTEGER" in evidence_sql
    assert "probe_attempt_count INTEGER" in evidence_sql
    assert "partitioning = ARRAY['competition_id']" in evidence_sql
    migration_sql = " ".join(writer.trino.sql[2].split())
    assert "ADD COLUMN IF NOT EXISTS probe_attempt_count INTEGER" in migration_sql
    assert CURRENT_VIEW_SPECS["fotmob_competition_scope_observations"] == (
        "competition_profile",
        ("competition_id",),
    )


def test_memory_latest_scope_evidence_ignores_uncommitted_rows_and_selects_latest():
    repository = MemoryFotMobRepository()
    older = _scope_evidence(observed_at=datetime(2026, 8, 7, 10))
    newer = _scope_evidence(
        observed_at=datetime(2026, 8, 8, 10),
        decision=ScopeDecision.EXCLUDED,
    )
    for run_id, evidence in (("old", older), ("new", newer)):
        commit = _commit(
            run_id=run_id,
            target_type="competition_profile",
            target_key=evidence.profile_target_key,
            competition_id=str(evidence.competition_id),
            content_hash=evidence.profile_content_hash,
            observation_id=run_id,
        )
        repository.commit(
            commit,
            [
                TableRows(
                    "fotmob_competition_scope_observations",
                    [{**asdict(evidence), "discovery_run_id": run_id}],
                    "competition_scope_observations",
                )
            ],
        )
    repository.tables["fotmob_competition_scope_observations"].append(
        {
            **asdict(
                _scope_evidence(
                    observed_at=datetime(2026, 8, 9, 10),
                    decision=ScopeDecision.REVIEW_REQUIRED,
                )
            ),
            "discovery_run_id": "crashed",
            "_target_batch_id": "never-committed",
        }
    )

    latest = repository.latest_scope_evidence([47, 999])

    assert latest == {47: newer}


def test_iceberg_latest_scope_evidence_joins_only_committed_profile_manifests():
    writer = ScopeEvidenceWriter()
    repository = FotMobRepository(writer=writer)

    latest = repository.latest_scope_evidence([47, 999])

    assert latest[47] == _scope_evidence()
    sql = " ".join(writer.trino.sql[0].split())
    assert "target_type = 'competition_profile'" in sql
    assert "status IN ('success', 'not_modified')" in sql
    assert "m.batch_id = e._target_batch_id" in sql
    assert "PARTITION BY e.competition_id" in sql


def test_latest_scope_evidence_preserves_legacy_null_probe_attempt_count():
    repository = FotMobRepository(writer=ScopeEvidenceWriter(None))

    latest = repository.latest_scope_evidence([47])

    assert latest[47].probe_attempt_count is None


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


def test_iceberg_latest_entity_attempt_includes_failure_statuses():
    writer = EntityAttemptWriter()
    repository = FotMobRepository(writer=writer)

    latest = repository.latest_entity_attempt("competition_seasons", 47)

    assert latest is not None
    assert latest["status"] == "retryable_failure"
    assert latest["completed_at"] == datetime(2026, 8, 8, 10)
    sql = " ".join(writer.trino.sql[0].split())
    assert "target_type = 'competition_seasons'" in sql
    assert "entity_id = '47'" in sql
    assert "retryable_failure" in sql
    assert "schema_drift" in sql


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


def test_missing_current_squad_player_ids_uses_bounded_current_view_anti_join():
    writer = MissingPlayerWriter(rows=[(21,), (40,)])
    repository = FotMobRepository(writer=writer)

    assert repository.missing_current_squad_player_ids(2) == [21, 40]

    assert len(writer.trino.sql) == 1
    sql = writer.trino.sql[0]
    assert "fotmob_squad_snapshots_current" in sql
    assert "fotmob_player_snapshots_current" in sql
    assert "LEFT JOIN" in sql
    assert "member_type = 'player'" in sql
    assert "TRY_CAST(s.member_id AS BIGINT) > 0" in sql
    assert "p.player_id IS NULL" in sql
    assert "ORDER BY player_id" in sql
    assert "LIMIT 2" in sql


def test_missing_current_squad_player_ids_requires_both_current_views():
    writer = MissingPlayerWriter(tables={"fotmob_squad_snapshots_current"})
    repository = FotMobRepository(writer=writer)

    with pytest.raises(RuntimeError, match="current view is unavailable"):
        repository.missing_current_squad_player_ids(10)
    assert writer.trino.sql == []


@pytest.mark.parametrize("limit", [0, -1])
def test_missing_current_squad_player_ids_rejects_non_positive_limit(limit):
    with pytest.raises(ValueError, match="limit must be positive"):
        MemoryFotMobRepository().missing_current_squad_player_ids(limit)


def test_memory_missing_current_squad_players_are_unique_sorted_and_limited():
    repository = MemoryFotMobRepository()
    repository.commit(
        _commit(
            target_type="team",
            target_key="team-old",
            entity_id="1",
            content_hash="1" * 64,
        ),
        [
            TableRows(
                "fotmob_squad_snapshots",
                [{"team_id": "1", "member_type": "player", "member_id": "10"}],
                "squad_snapshots",
            )
        ],
    )
    repository.commit(
        _commit(
            target_type="team",
            target_key="team-current",
            entity_id="1",
            content_hash="2" * 64,
        ),
        [
            TableRows(
                "fotmob_squad_snapshots",
                [
                    {"team_id": "1", "member_type": "player", "member_id": "21"},
                    {"team_id": "1", "member_type": "player", "member_id": "21"},
                    {"team_id": "1", "member_type": "coach", "member_id": "22"},
                    {"team_id": "1", "member_type": "player", "member_id": "bad"},
                    {"team_id": "1", "member_type": "player", "member_id": "-1"},
                ],
                "squad_snapshots",
            )
        ],
    )
    repository.commit(
        _commit(
            target_type="team",
            target_key="team-two",
            entity_id="2",
            content_hash="3" * 64,
        ),
        [
            TableRows(
                "fotmob_squad_snapshots",
                [
                    {"team_id": "2", "member_type": "player", "member_id": "40"},
                    {"team_id": "2", "member_type": "player", "member_id": "20"},
                ],
                "squad_snapshots",
            )
        ],
    )
    repository.commit(
        _commit(
            target_type="player",
            target_key="player-existing",
            entity_id="20",
            content_hash="4" * 64,
        ),
        [
            TableRows(
                "fotmob_player_snapshots",
                [{"player_id": "20"}],
                "player_snapshots",
            )
        ],
    )

    assert repository.missing_current_squad_player_ids(1) == [21]
    assert repository.missing_current_squad_player_ids(10) == [21, 40]


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

    def _execute(self, sql):
        # DDL of `ensure_schema`: recorded, never a source of rows.
        self.queries.append(sql)
        return None

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
    stale=False,
):
    return (
        target_key,
        batch_id,
        "c" * 64,
        "file:///raw/x.gz",
        parser_version,
        status,
        stale,
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


def test_manifest_reads_carry_the_stale_replay_flag():
    # Реплей сырья коммитится как success со свежим completed_at: без этого
    # признака порог свежести примет его за подтверждение источника и заморозит
    # цель на весь TTL.
    writer = PreloadWriter(
        [
            _manifest_row("https://example/match/1", "fm1-a", stale=True),
            _manifest_row(
                "https://example/player/9", "fm1-b", "player", "9", stale=True
            ),
        ]
    )
    repository = FotMobRepository(writer=writer)

    repository.preload_manifest_index()

    assert repository.latest_success("https://example/match/1")["stale"] is True
    assert repository.latest_entity_success("player", 9)["stale"] is True


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


class _ConflictWriter(ReconcileWriter):
    """Падает конфликтом коммита Iceberg — до записи или уже ПОСЛЕ неё.

    Второй режим моделирует настоящий staged-путь общего писателя: INSERT в
    цель коммитится и только потом удаляется временный стейдж, поэтому
    распознаваемый конфликт может прилететь, когда строки уже видны.
    """

    _CONFLICT = (
        "SQL execution failed: Failed to commit the transaction during write: "
        "conflicting files"
    )

    def __init__(self, *, table, when="before", times=1):
        super().__init__()
        self.table = table
        self.when = when
        self.times = times
        self.attempts = []

    def write_dataframe(self, df, **kwargs):
        table = kwargs["table"]
        self.attempts.append(table)
        if table == self.table and self.times:
            self.times -= 1
            if self.when == "after":
                # Коммит состоялся: строки уже видны, отказ пришёл на уборке.
                self.calls.append((df.copy(), dict(kwargs)))
                self.rows.setdefault(table, []).extend(df.to_dict("records"))
            raise RuntimeError(self._CONFLICT)
        return super().write_dataframe(df, **kwargs)


def _matches_dataset():
    return TableRows(
        "fotmob_matches",
        [{"competition_id": "289", "source_season_key": "2017/2019", "match_id": "1"}],
        "matches",
        ("competition_id", "source_season_key"),
    )


def test_flush_retries_commit_conflict_that_landed_nothing(monkeypatch):
    """Проигранная гонка снимка не должна стоить цели целиком (#1199).

    Общие bronze-таблицы FotMob пишет не только волна контура: замок B7 держит
    единственность СВОЕГО писателя, но чужой коммит в тот же снимок всё равно
    отправляет наш в ICEBERG_COMMIT_ERROR.
    """

    monkeypatch.setattr("scrapers.fotmob.repository.time.sleep", lambda _: None)
    writer = _ConflictWriter(table="fotmob_matches", when="before", times=2)
    repository = FotMobRepository(writer=writer, batch_size=50)

    repository.commit(_commit(), [_matches_dataset()])
    repository.flush()

    assert writer.attempts.count("fotmob_matches") == 3
    assert len(writer.rows["fotmob_matches"]) == 1
    assert len(writer.rows["fotmob_ingest_manifest"]) == 1


def test_flush_retry_does_not_duplicate_rows_a_conflict_reported_after_the_commit(
    monkeypatch,
):
    """Конфликт НЕ доказывает, что ничего не записалось.

    Общий писатель коммитит INSERT в цель и только потом удаляет временный
    стейдж, поэтому распознаваемый конфликт может прилететь уже после того, как
    строки стали видны. Ретрай вокруг одной физической записи задвоил бы цель;
    повтор всего flush() начинается со сверки по `_target_batch_id`, снимает
    подтверждённый пакет с буфера и дописывает только недостающее.
    """

    monkeypatch.setattr("scrapers.fotmob.repository.time.sleep", lambda _: None)
    writer = _ConflictWriter(table="fotmob_matches", when="after", times=1)
    repository = FotMobRepository(writer=writer, batch_size=50)

    repository.commit(_commit(), [_matches_dataset()])
    repository.flush()

    assert writer.attempts.count("fotmob_matches") == 1
    assert len(writer.rows["fotmob_matches"]) == 1
    assert len(writer.rows["fotmob_ingest_manifest"]) == 1
    assert any(
        "GROUP BY _target_batch_id" in query for query in writer.trino.queries
    )


def test_flush_retry_keeps_written_table_evidence_for_the_silver_gate(monkeypatch):
    """Список записанных таблиц обязан пережить ретрай.

    По нему даг строит `bronze_inputs_changed` и решает, пересобирать ли silver
    (`dag_ingest_fotmob.py:833-875`). Запись, чьи строки легли, но которая потом
    упала, своего пути не возвращает — значит подтвердить её может только сверка.
    Иначе повторённый flush даёт зелёный ран поверх устаревшей витрины.
    """

    monkeypatch.setattr("scrapers.fotmob.repository.time.sleep", lambda _: None)
    writer = _ConflictWriter(table="fotmob_matches", when="after", times=1)
    repository = FotMobRepository(writer=writer, batch_size=50)

    repository.commit(_commit(), [_matches_dataset()])
    paths = repository.flush()

    assert "iceberg.bronze.fotmob_matches" in paths
    assert "iceberg.bronze.fotmob_ingest_manifest" in paths
    assert len(paths) == len(set(paths))


def test_flush_retry_keeps_manifest_evidence_when_the_manifest_write_conflicts(
    monkeypatch,
):
    """То же самое для манифеста: он пишется последним и падает отдельно."""

    monkeypatch.setattr("scrapers.fotmob.repository.time.sleep", lambda _: None)
    writer = _ConflictWriter(table="fotmob_ingest_manifest", when="after", times=1)
    repository = FotMobRepository(writer=writer, batch_size=50)

    repository.commit(_commit(), [_matches_dataset()])
    paths = repository.flush()

    assert "iceberg.bronze.fotmob_matches" in paths
    assert "iceberg.bronze.fotmob_ingest_manifest" in paths
    assert len(writer.rows["fotmob_ingest_manifest"]) == 1
    assert len(writer.rows["fotmob_matches"]) == 1


def test_flush_without_any_conflict_reports_exactly_what_it_wrote():
    """Регрессия: обычный flush не должен приобрести лишних путей."""

    writer = ReconcileWriter()
    repository = FotMobRepository(writer=writer, batch_size=50)

    repository.commit(_commit(), [_matches_dataset()])
    paths = repository.flush()

    assert paths == [
        "iceberg.bronze.fotmob_matches",
        "iceberg.bronze.fotmob_ingest_manifest",
    ]


def test_flush_retry_ignores_every_failure_that_is_not_a_commit_conflict(monkeypatch):
    """Отказ, после которого строки могли остаться, повторять нельзя."""

    monkeypatch.setattr("scrapers.fotmob.repository.time.sleep", lambda _: None)
    writer = ReconcileWriter(fail_after_commit="fotmob_matches")
    repository = FotMobRepository(writer=writer, batch_size=50)

    repository.commit(_commit(), [_matches_dataset()])
    with pytest.raises(RuntimeError, match="lost writer response"):
        repository.flush()

    assert len(writer.rows["fotmob_matches"]) == 1
    assert "fotmob_ingest_manifest" not in writer.rows


def test_flush_conflict_that_never_clears_fails_the_target_on_a_bounded_schedule(
    monkeypatch,
):
    """Исчерпанный ретрай красит цель, а паузы — ровно те, что заявлены."""

    slept = []
    monkeypatch.setattr("scrapers.fotmob.repository.time.sleep", slept.append)
    monkeypatch.setattr("scrapers.fotmob.repository.random.uniform", lambda _a, _b: 0.0)
    writer = _ConflictWriter(table="fotmob_matches", when="before", times=99)
    repository = FotMobRepository(writer=writer, batch_size=50)

    repository.commit(_commit(), [_matches_dataset()])
    with pytest.raises(RuntimeError, match="conflicting files"):
        repository.flush()

    assert writer.attempts.count("fotmob_matches") == _COMMIT_CONFLICT_RETRIES
    assert slept == [0.5, 1.0, 2.0, 4.0]
    assert "fotmob_matches" not in writer.rows


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


def test_reparsed_seasons_collide_under_one_batch_and_fail_the_reconcile():
    # The #1234 incident in one test: 46 rows of the pre-#1230 parse are
    # already stored under the content-only batch id, the new parse of the very
    # same bytes yields 34, and the reconcile refuses to append into a batch
    # that holds a different row count -- killing the whole wave buffer.
    writer = ReconcileWriter()
    commit = _commit(target_type="competition_seasons")
    stored = [
        {"_target_batch_id": commit.batch_id, "source_season_key": f"s{index}"}
        for index in range(46)
    ]
    writer.rows["fotmob_competition_seasons"] = stored
    repository = FotMobRepository(writer=writer, batch_size=500)
    repository.commit(
        commit,
        [
            TableRows(
                "fotmob_competition_seasons",
                [
                    {"competition_id": "289", "source_season_key": f"s{index}"}
                    for index in range(34)
                ],
                "competition_seasons",
                ("competition_id",),
            )
        ],
    )

    with pytest.raises(
        RuntimeError, match="has 46 stored rows; expected either 0 or 34"
    ):
        repository.flush()


def test_seasons_parse_identity_gives_the_reparse_its_own_batch():
    # With the parse identity in the batch id the same bytes land in a fresh
    # batch: the 46 historical rows stay, the 34 new ones are appended.
    writer = ReconcileWriter()
    old = _commit(
        target_type="competition_seasons",
        observation_id="seasons-parser:fotmob-seasons-v1",
    )
    new = _commit(
        target_type="competition_seasons",
        observation_id="seasons-parser:fotmob-seasons-v2",
    )
    assert old.batch_id != new.batch_id
    writer.rows["fotmob_competition_seasons"] = [
        {"_target_batch_id": old.batch_id, "source_season_key": f"s{index}"}
        for index in range(46)
    ]
    repository = FotMobRepository(writer=writer, batch_size=500)
    repository.commit(
        new,
        [
            TableRows(
                "fotmob_competition_seasons",
                [
                    {"competition_id": "289", "source_season_key": f"s{index}"}
                    for index in range(34)
                ],
                "competition_seasons",
                ("competition_id",),
            )
        ],
    )
    repository.flush()

    counts = {}
    for row in writer.rows["fotmob_competition_seasons"]:
        counts[row["_target_batch_id"]] = counts.get(row["_target_batch_id"], 0) + 1
    assert counts == {old.batch_id: 46, new.batch_id: 34}


# --- В1 (#1242): замок писателя вокруг записи, а не вокруг рана -------------


class _CountingGuard:
    """Общий межпроцессный замок писателя, каким его видит репозиторий."""

    def __init__(self, *, delay_table=None, writer=None):
        self._lock = threading.Lock()
        self.entries = 0
        self.max_concurrent = 0
        self._live = 0
        self._live_lock = threading.Lock()
        self._delay_table = delay_table
        self._writer = writer

    def __call__(self):
        return self

    def __enter__(self):
        self._lock.acquire()
        with self._live_lock:
            self.entries += 1
            self._live += 1
            self.max_concurrent = max(self.max_concurrent, self._live)
        return True

    def __exit__(self, *exc_info):
        with self._live_lock:
            self._live -= 1
        self._lock.release()
        return False


class _SlowReconcileWriter(ReconcileWriter):
    """Писатель, чья запись первой таблицы длится заметное время.

    Без замка это окно и есть дефект: второй писатель успевает сверить пачку с
    хранилищем, пока первая запись ещё не видна.
    """

    def __init__(self, slow_table, delay=0.05):
        super().__init__()
        self.slow_table = slow_table
        self.delay = delay

    def write_dataframe(self, df, **kwargs):
        if kwargs["table"] == self.slow_table:
            time.sleep(self.delay)
        return super().write_dataframe(df, **kwargs)


def _match_dataset(match_id):
    return TableRows(
        "fotmob_matches",
        [
            {
                "competition_id": "289",
                "source_season_key": "2017/2019",
                "match_id": str(match_id),
            }
        ],
        "matches",
        ("competition_id", "source_season_key"),
    )


def _writer_pair(writer, guard):
    # 50 целей в пачке: обе стороны держат буфер, пока flush не позовут явно.
    wave = FotMobRepository(writer=writer, batch_size=50, write_guard=guard)
    campaign = FotMobRepository(writer=writer, batch_size=50, write_guard=guard)
    return wave, campaign


def _shared_and_unique_commits():
    """Одна общая цель (тот же batch_id у обоих) плюс по одной своей."""

    shared = _commit(target_key="shared-target", content_hash="c" * 64)
    wave_only = _commit(target_key="wave-target", content_hash="d" * 64)
    campaign_only = _commit(target_key="campaign-target", content_hash="e" * 64)
    return shared, wave_only, campaign_only


def _batch_counts(writer, table):
    column = "_target_batch_id" if table != "fotmob_ingest_manifest" else "batch_id"
    counts = {}
    for row in writer.rows.get(table, []):
        counts[row[column]] = counts.get(row[column], 0) + 1
    return counts


def test_two_writers_interleaved_keep_exactly_once_under_one_guard():
    writer = ReconcileWriter()
    guard = _CountingGuard()
    wave, campaign = _writer_pair(writer, guard)
    shared, wave_only, campaign_only = _shared_and_unique_commits()

    wave.commit(shared, [_match_dataset(1)])
    wave.commit(wave_only, [_match_dataset(2)])
    campaign.commit(shared, [_match_dataset(1)])
    campaign.commit(campaign_only, [_match_dataset(3)])

    campaign.flush()
    wave.flush()

    assert _batch_counts(writer, "fotmob_matches") == {
        shared.batch_id: 1,
        wave_only.batch_id: 1,
        campaign_only.batch_id: 1,
    }
    # Ничего не потеряно: три уникальные пачки по одной строке.
    assert len(writer.rows["fotmob_matches"]) == 3
    manifest = _batch_counts(writer, "fotmob_ingest_manifest")
    assert set(manifest) == {
        shared.batch_id,
        wave_only.batch_id,
        campaign_only.batch_id,
    }
    # Замок входился на каждую физическую запись, а не один раз на процесс.
    assert guard.entries >= 2
    assert guard.max_concurrent == 1


def test_two_writers_flushing_in_parallel_keep_exactly_once():
    writer = _SlowReconcileWriter("fotmob_matches")
    guard = _CountingGuard()
    wave, campaign = _writer_pair(writer, guard)
    shared, wave_only, campaign_only = _shared_and_unique_commits()

    wave.commit(shared, [_match_dataset(1)])
    wave.commit(wave_only, [_match_dataset(2)])
    campaign.commit(shared, [_match_dataset(1)])
    campaign.commit(campaign_only, [_match_dataset(3)])

    errors: list[BaseException] = []

    def flush(repository):
        try:
            repository.flush()
        except BaseException as exc:  # noqa: BLE001 - тест обязан увидеть отказ
            errors.append(exc)

    threads = [
        threading.Thread(target=flush, args=(wave,)),
        threading.Thread(target=flush, args=(campaign,)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert errors == []
    assert _batch_counts(writer, "fotmob_matches") == {
        shared.batch_id: 1,
        wave_only.batch_id: 1,
        campaign_only.batch_id: 1,
    }
    assert len(writer.rows["fotmob_matches"]) == 3
    assert guard.max_concurrent == 1


def test_parallel_flush_without_guard_breaks_exactly_once():
    """Контроль: тест выше ловит дефект, а не проходит сам собой."""

    writer = _SlowReconcileWriter("fotmob_matches")
    shared, wave_only, campaign_only = _shared_and_unique_commits()
    wave = FotMobRepository(writer=writer, batch_size=50)
    campaign = FotMobRepository(writer=writer, batch_size=50)

    wave.commit(shared, [_match_dataset(1)])
    wave.commit(wave_only, [_match_dataset(2)])
    campaign.commit(shared, [_match_dataset(1)])
    campaign.commit(campaign_only, [_match_dataset(3)])

    errors: list[BaseException] = []

    def flush(repository):
        try:
            repository.flush()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=flush, args=(wave,)),
        threading.Thread(target=flush, args=(campaign,)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    duplicated = _batch_counts(writer, "fotmob_matches").get(shared.batch_id, 0) > 1
    assert duplicated or errors, (
        "без общего замка одновременный flush обязан задвоить пачку или упасть"
    )


def test_write_guard_is_not_held_between_writes():
    writer = ReconcileWriter()
    guard = _CountingGuard()
    repository = FotMobRepository(writer=writer, batch_size=2, write_guard=guard)

    repository.ensure_schema()
    after_schema = guard.entries
    assert after_schema == 1

    repository.commit(_commit(target_key="one", content_hash="1" * 64))
    assert guard.entries == after_schema, "буферизованный commit не пишет и не ждёт"

    repository.commit(_commit(target_key="two", content_hash="2" * 64))
    repository.flush()
    assert guard.entries == after_schema + 1
    assert guard.max_concurrent == 1

    # Пустой буфер не берёт замок вовсе.
    assert repository.flush() == []
    assert guard.entries == after_schema + 1


def test_lost_write_guard_fails_every_later_write_immediately():
    class _Refusing:
        def __init__(self):
            self.attempts = 0

        def __call__(self):
            self.attempts += 1
            raise RuntimeError("WriterLockBusy: another FotMob writer holds the lock")

    guard = _Refusing()
    writer = ReconcileWriter()
    repository = FotMobRepository(writer=writer, batch_size=50, write_guard=guard)
    repository.commit(_commit(target_key="one", content_hash="1" * 64))
    repository.commit(_commit(target_key="two", content_hash="2" * 64))

    with pytest.raises(RuntimeError, match="WriterLockBusy"):
        repository.flush()
    assert guard.attempts == 1

    with pytest.raises(RuntimeError, match="WriterLockBusy"):
        repository.flush()
    with pytest.raises(RuntimeError, match="WriterLockBusy"):
        repository.ensure_current_views()
    # Повторного ожидания замка нет: отказ поднимается из памяти процесса.
    assert guard.attempts == 1
    assert writer.rows == {}


def test_unbuffered_writers_keep_exactly_once_under_one_guard():
    """batch_size=1 сверяет пачку с хранилищем так же, как буферизованный путь.

    Прямая запись обходила сверку `_reconcile_pending_table`, и два писателя под
    одним замком клали одну и ту же пачку дважды — каждый в свой заход.
    """

    writer = ReconcileWriter()
    guard = _CountingGuard()
    shared = _commit(target_key="shared-target", content_hash="c" * 64)
    wave = FotMobRepository(writer=writer, batch_size=1, write_guard=guard)
    campaign = FotMobRepository(writer=writer, batch_size=1, write_guard=guard)

    wave.commit(shared, [_match_dataset(1)])
    campaign.commit(shared, [_match_dataset(1)])

    assert _batch_counts(writer, "fotmob_matches") == {shared.batch_id: 1}
    assert len(writer.rows["fotmob_matches"]) == 1
    manifest = [
        row for row in writer.rows["fotmob_ingest_manifest"]
        if row["batch_id"] == shared.batch_id
    ]
    assert len(manifest) == 1
    assert manifest[0]["status"] == "success"
    assert manifest[0]["run_id"] == shared.run_id
    assert guard.max_concurrent == 1
