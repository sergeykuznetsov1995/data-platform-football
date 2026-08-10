"""Contract coverage for the offline FBref persistence benchmark."""

import json
import logging
import uuid

import pytest

import scripts.research.bench_fbref_persistence as benchmark
from scrapers.fbref.control.replay_effects import (
    build_replay_control_effects,
    make_replay_control_refresh_id,
    make_replay_control_target_id,
)
from scripts.research.bench_fbref_persistence import (
    BENCHMARK_TABLES,
    AcceptanceBaseline,
    BenchmarkConfig,
    ControlRunEvidence,
    PipelineRunMetrics,
    PersistenceRun,
    StatementCounts,
    TableDigest,
    _bidirectional_table_diffs,
    _control_evidence_equivalent,
    _control_run_evidence,
    capture_acceptance_baseline,
    _digests_equivalent,
    _snapshot_ids,
    _sentinel_digests,
    _mean_iteration_seconds,
    _table_digests,
    _validate_trino_match_cohort,
    evaluate_gate,
    evaluate_existing_pipeline_acceptance,
    run_benchmark,
)


RUN_ONE_ID = str(uuid.UUID(int=101))
RUN_TWO_ID = str(uuid.UUID(int=102))
SOURCE_RUN_ID = str(uuid.UUID(int=103))
OTHER_SOURCE_RUN_ID = str(uuid.UUID(int=104))
SOURCE_REFRESH_ID = str(uuid.UUID(int=105))


@pytest.mark.unit
def test_gate_fails_for_slow_or_non_equivalent_candidate():
    report = evaluate_gate(
        sequential_seconds=80.0,
        batch_seconds=30.0,
        matches=2,
        equivalent=False,
        proxy_requests=0,
        proxy_bytes=0,
    )
    assert report.speedup == pytest.approx(80.0 / 30.0)
    assert report.passed is False


@pytest.mark.unit
def test_gate_requires_zero_network_and_four_x_speedup():
    report = evaluate_gate(
        sequential_seconds=80.0,
        batch_seconds=16.0,
        matches=2,
        equivalent=True,
        proxy_requests=0,
        proxy_bytes=0,
    )
    assert report.seconds_per_match == pytest.approx(8.0)
    assert report.passed is True


@pytest.mark.unit
def test_iteration_timings_are_meaned_before_gate_evaluation():
    assert _mean_iteration_seconds(elapsed_seconds=30.0, iterations=3) == (
        pytest.approx(10.0)
    )


@pytest.mark.unit
def test_table_digests_mark_every_missing_expected_table_as_absent():
    class Manager:
        catalog = "iceberg"

        def table_exists(self, schema, table):
            return False

    digests = _table_digests(Manager(), "bench")

    assert set(digests) == set(BENCHMARK_TABLES)
    assert all(digest.present is False for digest in digests.values())
    assert all(digest.rows is None for digest in digests.values())
    assert all(digest.sha256 is None for digest in digests.values())
    assert _digests_equivalent(digests, digests) is False


@pytest.mark.unit
def test_missing_table_on_either_side_cannot_pass_equivalence_or_gate():
    complete = {
        table: TableDigest(present=True, rows=0, sha256="digest")
        for table in BENCHMARK_TABLES
    }
    missing = dict(complete)
    missing[BENCHMARK_TABLES[0]] = TableDigest(
        present=False, rows=None, sha256=None
    )

    equivalent = _digests_equivalent(complete, missing)
    gate = evaluate_gate(
        sequential_seconds=80.0,
        batch_seconds=16.0,
        matches=2,
        equivalent=equivalent,
        proxy_requests=0,
        proxy_bytes=0,
    )

    assert equivalent is False
    assert gate.passed is False


@pytest.mark.unit
def test_case_insensitive_duplicate_schemas_fail_before_benchmark_run(
    monkeypatch, tmp_path
):
    called = []
    monkeypatch.setattr(
        benchmark,
        "CountingTrinoTableManager",
        lambda: called.append("manager"),
    )
    monkeypatch.setattr(
        benchmark, "_load_replay_items", lambda _path: called.append("load")
    )

    with pytest.raises(ValueError, match="case-insensitively distinct"):
        run_benchmark(
            BenchmarkConfig(
                html_dir=tmp_path,
                sequential_schema="Replay",
                batch_schema="replay",
            )
        )

    assert called == []


class _ProvisionedManager:
    catalog = "iceberg"

    def __init__(self, *, missing=(), rows=None, signatures=None):
        self.missing = set(missing)
        self.rows = dict(rows or {})
        self.signatures = dict(signatures or {})

    def table_exists(self, schema, table):
        return (schema, table) not in self.missing

    def get_table_columns(self, schema, table):
        return self.signatures.get((schema, table), {"identity": "VARCHAR"})

    def _execute(self, sql, fetch=False):
        if "EXCEPT ALL" in sql:
            return [(0,)]
        table = next(table for table in BENCHMARK_TABLES if table in sql)
        schema = next(
            schema for schema in ("sequential", "batch") if f".{schema}." in sql
        )
        return [(1,)] if self.rows.get((schema, table), 0) else []


def _reject_before_replay(monkeypatch):
    called = []
    monkeypatch.setattr(
        benchmark, "_load_replay_items", lambda _path: called.append("load")
    )
    monkeypatch.setattr(
        benchmark, "_run_persistence", lambda **_kwargs: called.append("run")
    )
    return called


@pytest.mark.unit
def test_partial_provisioning_fails_before_loading_or_timing(
    monkeypatch, tmp_path
):
    manager = _ProvisionedManager(
        missing={("sequential", BENCHMARK_TABLES[0])}
    )
    monkeypatch.setattr(benchmark, "CountingTrinoTableManager", lambda: manager)
    called = _reject_before_replay(monkeypatch)

    with pytest.raises(ValueError, match="missing required benchmark tables"):
        run_benchmark(
            BenchmarkConfig(tmp_path, "sequential", batch_schema=None)
        )

    assert called == []


@pytest.mark.unit
def test_row_containing_schema_fails_before_loading_or_timing(
    monkeypatch, tmp_path
):
    manager = _ProvisionedManager(
        rows={("sequential", BENCHMARK_TABLES[0]): 1}
    )
    monkeypatch.setattr(benchmark, "CountingTrinoTableManager", lambda: manager)
    called = _reject_before_replay(monkeypatch)

    with pytest.raises(ValueError, match="must contain zero rows"):
        run_benchmark(
            BenchmarkConfig(tmp_path, "sequential", batch_schema=None)
        )

    assert called == []


@pytest.mark.unit
def test_candidate_signature_mismatch_fails_before_loading_or_timing(
    monkeypatch, tmp_path
):
    manager = _ProvisionedManager(
        signatures={("batch", BENCHMARK_TABLES[0]): {"identity": "BIGINT"}}
    )
    monkeypatch.setattr(benchmark, "CountingTrinoTableManager", lambda: manager)
    monkeypatch.setattr(benchmark, "_batch_api_available", lambda: True)
    called = _reject_before_replay(monkeypatch)

    with pytest.raises(ValueError, match="column/type signature mismatch"):
        run_benchmark(BenchmarkConfig(tmp_path, "sequential", "batch"))

    assert called == []


@pytest.mark.unit
def test_fully_provisioned_empty_pair_proceeds_and_can_turn_green(
    monkeypatch, tmp_path
):
    manager = _ProvisionedManager()
    monkeypatch.setattr(benchmark, "CountingTrinoTableManager", lambda: manager)
    monkeypatch.setattr(benchmark, "_batch_api_available", lambda: True)
    monkeypatch.setattr(
        benchmark, "_load_replay_items", lambda _path: (object(), object())
    )
    runs = iter(
        (
            PersistenceRun(
                seconds=80.0,
                statement_counts=StatementCounts(
                    execute=0, execute_committing=0
                ),
                table_digests={
                    table: TableDigest(present=True, rows=0, sha256="digest")
                    for table in BENCHMARK_TABLES
                },
            ),
            PersistenceRun(
                seconds=16.0,
                statement_counts=StatementCounts(
                    execute=0, execute_committing=0
                ),
                table_digests={
                    table: TableDigest(present=True, rows=0, sha256="digest")
                    for table in BENCHMARK_TABLES
                },
            ),
        )
    )
    monkeypatch.setattr(
        benchmark, "_run_persistence", lambda **_kwargs: next(runs)
    )

    report = run_benchmark(BenchmarkConfig(tmp_path, "sequential", "batch"))

    assert report.equivalent is True
    assert report.gate is not None
    assert report.gate.passed is True
    assert report.passed is True
    assert set(report.table_diffs or {}) == set(BENCHMARK_TABLES)


@pytest.mark.unit
def test_fully_provisioned_empty_sequential_schema_proceeds(
    monkeypatch, tmp_path
):
    manager = _ProvisionedManager()
    monkeypatch.setattr(benchmark, "CountingTrinoTableManager", lambda: manager)
    monkeypatch.setattr(benchmark, "_load_replay_items", lambda _path: (object(),))
    monkeypatch.setattr(
        benchmark, "_run_persistence", lambda **_kwargs: _empty_persistence_run()
    )

    report = run_benchmark(
        BenchmarkConfig(tmp_path, "sequential", batch_schema=None)
    )

    assert report.batch is None


@pytest.mark.unit
def test_preflight_closes_its_manager_connection(monkeypatch, tmp_path):
    class Connection:
        closed = False

        def close(self):
            self.closed = True

    manager = _ProvisionedManager()
    connection = Connection()
    manager._conn = connection
    monkeypatch.setattr(benchmark, "CountingTrinoTableManager", lambda: manager)
    monkeypatch.setattr(benchmark, "_load_replay_items", lambda _path: (object(),))
    monkeypatch.setattr(
        benchmark, "_run_persistence", lambda **_kwargs: _empty_persistence_run()
    )

    run_benchmark(BenchmarkConfig(tmp_path, "sequential", batch_schema=None))

    assert connection.closed is True
    assert manager._conn is None


def _empty_persistence_run() -> PersistenceRun:
    return PersistenceRun(
        seconds=1.0,
        statement_counts=StatementCounts(execute=0, execute_committing=0),
        table_digests={
            table: TableDigest(present=True, rows=0, sha256="digest")
            for table in BENCHMARK_TABLES
        },
    )


@pytest.mark.unit
def test_benchmark_suppresses_trino_sql_logs_and_restores_logger(
    monkeypatch, caplog, tmp_path
):
    logger = logging.getLogger("scrapers.base.trino_manager")
    caplog.set_level(logging.DEBUG, logger=logger.name)
    original_disabled = logger.disabled

    def emit_sql_logs(*_args, **_kwargs):
        logger.debug("Executing SQL: SELECT 'secret-normal'")
        logger.error("SQL (truncated): DELETE secret-normal")
        logger.info("Replaced rows matching 'secret-normal'")
        return _empty_persistence_run()

    def preflight_with_sql_logs(_config):
        logger.debug("Executing SQL: SELECT 'secret-preflight'")

    monkeypatch.setattr(
        benchmark, "_preflight_schemas", preflight_with_sql_logs
    )
    monkeypatch.setattr(benchmark, "_load_replay_items", lambda _path: (object(),))
    monkeypatch.setattr(benchmark, "_run_persistence", emit_sql_logs)

    report = run_benchmark(
        BenchmarkConfig(tmp_path, "sequential", batch_schema=None)
    )

    assert report.batch is None
    assert "secret-normal" not in caplog.text
    assert "secret-preflight" not in caplog.text
    assert logger.disabled is original_disabled
    logger.info("trino logging restored")
    assert "trino logging restored" in caplog.text


@pytest.mark.unit
def test_benchmark_restores_trino_logger_when_persistence_fails(
    monkeypatch, caplog, tmp_path
):
    logger = logging.getLogger("scrapers.base.trino_manager")
    caplog.set_level(logging.DEBUG, logger=logger.name)
    original_disabled = logger.disabled

    def fail_with_sql_logs(*_args, **_kwargs):
        logger.debug("Executing SQL: SELECT 'secret-failure'")
        logger.error("SQL (truncated): DELETE secret-failure")
        logger.info("Replaced rows matching 'secret-failure'")
        raise RuntimeError("persistence failed")

    monkeypatch.setattr(benchmark, "_preflight_schemas", lambda _config: None)
    monkeypatch.setattr(benchmark, "_load_replay_items", lambda _path: (object(),))
    monkeypatch.setattr(benchmark, "_run_persistence", fail_with_sql_logs)

    with pytest.raises(RuntimeError, match="persistence failed"):
        run_benchmark(
            BenchmarkConfig(tmp_path, "sequential", batch_schema=None)
        )

    assert "secret-failure" not in caplog.text
    assert logger.disabled is original_disabled
    logger.error("trino failure logging restored")
    assert "trino failure logging restored" in caplog.text


class _DifferentialManager:
    catalog = "iceberg"

    def __init__(self, *, nonzero_query=None, trino_match_ids=("m1",)):
        self.queries = []
        self.nonzero_query = nonzero_query
        self.trino_match_ids = tuple(trino_match_ids)

    def table_exists(self, _schema, _table):
        return True

    def get_table_columns(self, _schema, table):
        if table in BENCHMARK_TABLES[:3]:
            return {
                "target_id": "VARCHAR",
                "run_id": "VARCHAR",
                "persisted_at": "TIMESTAMP(6)",
            }
        return {
            "match_id": "VARCHAR",
            "_batch_id": "VARCHAR",
            "_ingested_at": "TIMESTAMP(6)",
        }

    def _execute(self, sql, fetch=False, params=None):
        self.queries.append((sql, params))
        if '$snapshots"' in sql:
            return [(101,)]
        if "AS acceptance_match_id" in sql:
            return [(match_id,) for match_id in self.trino_match_ids]
        if "WHERE target_id = ?" in sql or "WHERE match_id = ?" in sql:
            return [("sentinel", "batch")]
        if sql.startswith("SELECT ") and "SELECT count(*)" not in sql:
            table = next(table for table in BENCHMARK_TABLES if table in sql)
            run_id = RUN_ONE_ID if ".sequential." in sql else RUN_TWO_ID
            identity = (
                "fbref:match:m1"
                if table in BENCHMARK_TABLES[:3]
                else "m1"
            )
            return [(identity, run_id)]
        value = 1 if self.nonzero_query and self.nonzero_query in sql else 0
        return [(value,)]


class _SourceHeaderManager(_DifferentialManager):
    """Columns as FBref actually lands them in Bronze: spaces and a percent."""

    def __init__(self, *, extra_column=None):
        super().__init__()
        self.extra_column = extra_column

    def get_table_columns(self, _schema, _table):
        columns = {
            "match_id": "VARCHAR",
            "_batch_id": "VARCHAR",
            "shot stopping_sota": "VARCHAR",
            "shot stopping_save%": "VARCHAR",
            "_ingested_at": "TIMESTAMP(6)",
        }
        if self.extra_column:
            columns[self.extra_column] = "VARCHAR"
        return columns


@pytest.mark.unit
def test_comparison_accepts_source_header_columns_and_quotes_them():
    manager = _SourceHeaderManager()

    columns = benchmark._comparison_columns(
        manager, "sequential", "batch", "fbref_match_keeper_stats"
    )

    assert "shot stopping_sota" in columns
    assert "shot stopping_save%" in columns
    assert "_ingested_at" not in columns
    projection, params = benchmark._comparison_projection(
        columns,
        table="fbref_match_keeper_stats",
        expected_run_id=None,
        sentinel_match_id=None,
    )
    assert '"shot stopping_sota"' in projection
    assert '"shot stopping_save%"' in projection
    assert params == ()


@pytest.mark.unit
def test_comparison_rejects_a_column_name_that_breaks_out_of_its_quotes():
    manager = _SourceHeaderManager(extra_column='bad" OR 1=1 --')

    with pytest.raises(ValueError, match="unquotable column name"):
        benchmark._comparison_columns(
            manager, "sequential", "batch", "fbref_match_keeper_stats"
        )


@pytest.mark.unit
def test_all_12_tables_use_dynamic_columns_and_bidirectional_except_all():
    manager = _DifferentialManager()

    diffs = _bidirectional_table_diffs(manager, "sequential", "batch")

    assert len(BENCHMARK_TABLES) == 12
    assert set(diffs) == set(BENCHMARK_TABLES)
    queries = [sql for sql, _params in manager.queries]
    assert len(queries) == 24
    assert all("EXCEPT ALL" in sql for sql in queries)
    assert all(
        ('"target_id"' in sql and '"run_id"' in sql)
        or ('"match_id"' in sql and '"_batch_id"' in sql)
        for sql in queries
    )
    assert all('"_ingested_at"' not in sql for sql in queries)
    assert all('"persisted_at"' not in sql for sql in queries)
    assert all(diff.sequential_minus_batch == 0 for diff in diffs.values())
    assert all(diff.batch_minus_sequential == 0 for diff in diffs.values())


@pytest.mark.unit
def test_distinct_control_run_ids_are_verified_then_canonicalized_in_diffs():
    manager = _DifferentialManager()

    diffs = _bidirectional_table_diffs(
        manager,
        "sequential",
        "batch",
        sequential_run_id=RUN_ONE_ID,
        batch_run_id=RUN_TWO_ID,
        sentinel_match_id="outside-match",
    )

    assert set(diffs) == set(BENCHMARK_TABLES)
    lineage_queries = [
        (sql, params)
        for sql, params in manager.queries
        if "IS DISTINCT FROM ?" in sql and "EXCEPT ALL" not in sql
    ]
    diff_queries = [
        (sql, params)
        for sql, params in manager.queries
        if "EXCEPT ALL" in sql
    ]
    assert len(lineage_queries) == 24
    assert len(diff_queries) == 24
    assert all("CASE WHEN" in sql for sql, _params in diff_queries)
    assert all("__fbref_acceptance_run__" in sql for sql, _params in diff_queries)
    assert any(RUN_ONE_ID in params for _sql, params in diff_queries)
    assert any(RUN_TWO_ID in params for _sql, params in diff_queries)


@pytest.mark.unit
def test_unexpected_cohort_lineage_fails_closed_before_comparison():
    manager = _DifferentialManager(nonzero_query="IS DISTINCT FROM")

    with pytest.raises(ValueError, match="unexpected run lineage"):
        _bidirectional_table_diffs(
            manager,
            "sequential",
            "batch",
            sequential_run_id=RUN_ONE_ID,
            batch_run_id=RUN_TWO_ID,
            sentinel_match_id="outside-match",
        )


@pytest.mark.unit
def test_trino_page_manifest_must_equal_the_direct_control_match_cohort():
    evidence = _control_run_evidence(_DirectControl(), RUN_ONE_ID)
    manager = _DifferentialManager(trino_match_ids=())

    with pytest.raises(ValueError, match="Trino match cohort"):
        _validate_trino_match_cohort(
            manager,
            schema="sequential",
            expected_run_id=RUN_ONE_ID,
            sentinel_match_id="outside-match",
            control_evidence=evidence,
        )


@pytest.mark.unit
def test_one_directional_difference_cannot_be_hidden_by_digest_equality():
    manager = _DifferentialManager(nonzero_query="FROM iceberg.batch")

    diffs = _bidirectional_table_diffs(manager, "sequential", "batch")

    assert any(diff.batch_minus_sequential == 1 for diff in diffs.values())


@pytest.mark.unit
def test_snapshot_evidence_is_collected_for_every_table():
    manager = _DifferentialManager()

    snapshots = _snapshot_ids(manager, "batch")

    assert snapshots == {table: 101 for table in BENCHMARK_TABLES}
    assert len(manager.queries) == 12
    assert all('$snapshots"' in sql for sql, _params in manager.queries)


@pytest.mark.unit
def test_outside_cohort_sentinel_uses_target_id_for_generic_and_match_id_for_typed():
    manager = _DifferentialManager()

    sentinels = _sentinel_digests(manager, "batch", "outside-match")

    assert set(sentinels) == set(BENCHMARK_TABLES)
    assert all(item.present and item.rows == 1 for item in sentinels.values())
    generic_queries = manager.queries[:3]
    typed_queries = manager.queries[3:]
    assert all("WHERE target_id = ?" in sql for sql, _params in generic_queries)
    assert all(params == ("fbref:match:outside-match",) for _sql, params in generic_queries)
    assert all("WHERE match_id = ?" in sql for sql, _params in typed_queries)
    assert all(params == ("outside-match",) for _sql, params in typed_queries)


@pytest.mark.unit
def test_strict_acceptance_requires_control_ids_and_outside_sentinel(tmp_path):
    with pytest.raises(ValueError, match="strict acceptance requires"):
        run_benchmark(
            BenchmarkConfig(
                tmp_path,
                "sequential",
                "batch",
                strict_acceptance=True,
            )
        )


class _DirectControl:
    def __init__(self):
        self.calls = []

    @staticmethod
    def _effects(run_id, mode):
        replay_target_id = make_replay_control_target_id(
            run_id, "fbref:match:m1"
        )
        replay_refresh_id = make_replay_control_refresh_id(
            run_id, SOURCE_REFRESH_ID
        )
        targets = [
            {
                "ordinal": 0,
                "target_id": "fbref:match:m1",
                "replay_target_id": replay_target_id,
                "source_logical_refresh_id": SOURCE_REFRESH_ID,
                "logical_refresh_id": replay_refresh_id,
                "status": "succeeded",
                "target_status": "succeeded",
                "page_kind": "match",
                "source_ids": {"match_id": "m1"},
                "frontier_state": "fetched",
                "last_content_hash": "a" * 64,
                "content_hash": "a" * 64,
                "parser_version": "generic-v1",
                "typed_parser_version": "typed-v1",
                "stateful_parser_version": "stateful-v1",
                "observation_status": "succeeded",
                "generic_status": "succeeded",
                "typed_status": "succeeded",
                "stateful_status": "skipped",
                "validation_status": "succeeded",
                "evidence_class": "full_match",
                "latest_guarded": True,
            }
        ]
        datasets = [
            {
                "ordinal": 0,
                "target_id": "fbref:match:m1",
                "replay_target_id": replay_target_id,
                "content_hash": "a" * 64,
                "parser_version": "typed-v1",
                "dataset": f"typed:{dataset}",
                "availability": "available",
                "parse_status": "succeeded",
                "persistence_status": "succeeded",
                "validation_status": "succeeded",
                "row_count": 3,
                "empty_reason": None,
            }
            for dataset in (
                "shot_events",
                "match_events",
                "lineups",
                "match_team_stats",
                "match_managers",
                "match_officials",
                "match_keeper_stats",
                "match_player_stats",
            )
        ]
        return build_replay_control_effects(
            control_run_id=run_id,
            source_run_id=SOURCE_RUN_ID,
            mode=mode,
            targets=targets,
            datasets=datasets,
        )

    def get_run(self, run_id):
        self.calls.append(("run", run_id))
        mode = "batch" if run_id == RUN_TWO_ID else "sequential"
        schema = "batch" if mode == "batch" else "sequential"
        metrics = PipelineRunMetrics.anchored(
            control_run_id=run_id,
            schema=schema,
            mode=mode,
            elapsed_seconds=16.0 if mode == "batch" else 80.0,
            match_count=1,
            match_keys_sha256=benchmark._stable_sha256(["m1"]),
            statement_counts=(
                StatementCounts(20, 4)
                if mode == "batch"
                else StatementCounts(120, 24)
            ),
        )
        return {
            "run_id": run_id,
            "run_type": "replay",
            "status": "succeeded",
            "request_limit": 0,
            "byte_limit": 0,
            "requests_used": 0,
            "bytes_used": 0,
            "metadata": {
                "acceptance_replay_source_run_id": SOURCE_RUN_ID,
                "acceptance_persistence_mode": mode,
                "acceptance_replay_control_effects": self._effects(
                    run_id, mode
                ),
                "pipeline_run_metrics": metrics.to_dict(),
                "bronze_acceptance_replay": {
                    "schema_version": "fbref-bronze-acceptance-replay-v1",
                    "status": "passed",
                    "processing_control_run_id": run_id,
                    "strict_gates": {
                        "source_acceptance_status": "passed",
                        "network_attempts": 0,
                        "requests_used": 0,
                        "bytes_used": 0,
                        "request_limit": 0,
                        "byte_limit": 0,
                        "replay_candidates_remaining": 0,
                        "raw_audit_status": "passed",
                        "raw_zero_delta_required": True,
                        "raw_audited_attempt_count": 1,
                        "raw_audit_artifact_sha256": "a" * 64,
                        "pipeline_metrics_artifact_sha256": (
                            metrics.artifact_sha256
                        ),
                    },
                },
            },
        }

    def get_run_summary(self, run_id):
        self.calls.append(("summary", run_id))
        return {
            "requests_reserved": 0,
            "bytes_reserved": 0,
            "unprocessed_raw_count": 0,
            "traffic_totals": {"network_attempts": 0},
        }

    def get_replay_control_effects(self, run_id):
        self.calls.append(("effects", run_id))
        mode = "batch" if run_id == RUN_TWO_ID else "sequential"
        return self._effects(run_id, mode)

@pytest.mark.unit
def test_control_evidence_uses_anchored_actual_replay_effects():
    control = _DirectControl()

    evidence = _control_run_evidence(control, RUN_ONE_ID)

    assert isinstance(evidence, ControlRunEvidence)
    assert control.calls == [
        ("run", RUN_ONE_ID),
        ("summary", RUN_ONE_ID),
        ("effects", RUN_ONE_ID),
    ]
    assert evidence.evidence_source == (
        "anchored_actual_replay_control_effects"
    )
    assert evidence.valid is True
    assert evidence.logical_refreshes == 1
    assert evidence.dataset_manifests == 8
    assert evidence.match_targets == 1
    assert evidence.match_keys_sha256
    assert evidence.observation_sha256
    assert evidence.manifest_sha256
    assert evidence.latest_state_sha256


@pytest.mark.unit
def test_source_hashes_alone_cannot_claim_replay_control_equivalence():
    class SourceOnlyControl(_DirectControl):
        get_replay_control_effects = None

        def get_run(self, run_id):
            run = super().get_run(run_id)
            del run["metadata"]["acceptance_replay_control_effects"]
            return run

    with pytest.raises(
        ValueError, match="anchored replay control effects are missing"
    ):
        _control_run_evidence(SourceOnlyControl(), RUN_ONE_ID)


@pytest.mark.unit
def test_control_artifact_must_match_its_frozen_source_run():
    class DifferentSourceControl(_DirectControl):
        def get_run(self, run_id):
            run = super().get_run(run_id)
            run["metadata"]["acceptance_replay_source_run_id"] = (
                OTHER_SOURCE_RUN_ID
            )
            return run

    with pytest.raises(
        ValueError, match="control effects differ from run"
    ):
        _control_run_evidence(DifferentSourceControl(), RUN_ONE_ID)


@pytest.mark.unit
def test_control_evidence_rejects_an_unversioned_strict_marker():
    class Unversioned(_DirectControl):
        def get_run(self, run_id):
            run = super().get_run(run_id)
            del run["metadata"]["bronze_acceptance_replay"]["schema_version"]
            return run

    evidence = _control_run_evidence(Unversioned(), RUN_ONE_ID)

    assert evidence.valid is False
    assert "strict_acceptance_missing" in evidence.failures


@pytest.mark.unit
def test_control_evidence_rejects_an_incomplete_strict_gate():
    class Incomplete(_DirectControl):
        def get_run(self, run_id):
            run = super().get_run(run_id)
            strict = run["metadata"]["bronze_acceptance_replay"]
            del strict["strict_gates"]["network_attempts"]
            return run

    evidence = _control_run_evidence(Incomplete(), RUN_ONE_ID)

    assert evidence.valid is False
    assert "strict_acceptance_gates_invalid" in evidence.failures


@pytest.mark.unit
def test_metadata_fallback_reads_only_the_anchored_replay_artifact():
    class MetadataControl(_DirectControl):
        get_replay_control_effects = None

    control = MetadataControl()

    evidence = _control_run_evidence(control, RUN_ONE_ID)

    assert evidence.valid is True
    assert control.calls == [
        ("run", RUN_ONE_ID),
        ("summary", RUN_ONE_ID),
    ]


@pytest.mark.unit
@pytest.mark.parametrize(
    "defect",
    ("observation_completion", "dataset_manifest", "latest_guard"),
)
def test_control_equivalence_rejects_a_divergent_batch_artifact(defect):
    """A shared source snapshot must not hide a broken batch completion."""

    class DivergentBatchControl(_DirectControl):
        def get_replay_control_effects(self, run_id):
            value = json.loads(
                json.dumps(
                    super().get_replay_control_effects(run_id)
                )
            )
            if run_id != RUN_TWO_ID:
                return value
            if defect == "observation_completion":
                value["targets"][0]["observation_status"] = "processing"
            elif defect == "dataset_manifest":
                value["datasets"].pop()
            else:
                value["targets"][0]["latest_guarded"] = False
            return build_replay_control_effects(
                control_run_id=value["control_run_id"],
                source_run_id=value["source_run_id"],
                mode=value["mode"],
                targets=value["targets"],
                datasets=value["datasets"],
            )

    control = DivergentBatchControl()

    sequential = _control_run_evidence(control, RUN_ONE_ID)
    batch = _control_run_evidence(control, RUN_TWO_ID)

    assert batch.valid is False
    assert _control_evidence_equivalent(sequential, batch) is False


@pytest.mark.unit
def test_control_effects_require_exact_source_to_replay_refresh_mapping():
    value = _DirectControl._effects(RUN_ONE_ID, "sequential")
    targets = json.loads(json.dumps(value["targets"]))
    targets[0]["logical_refresh_id"] = str(uuid.uuid4())

    with pytest.raises(ValueError, match="logical refresh identity"):
        build_replay_control_effects(
            control_run_id=RUN_ONE_ID,
            source_run_id=SOURCE_RUN_ID,
            mode="sequential",
            targets=targets,
            datasets=value["datasets"],
        )


@pytest.mark.unit
def test_control_evidence_rejects_a_corrupted_artifact_digest():
    class Corrupted(_DirectControl):
        def get_replay_control_effects(self, run_id):
            value = super().get_replay_control_effects(run_id)
            return {**value, "artifact_sha256": "0" * 64}

    with pytest.raises(
        ValueError, match="anchored replay control effects are invalid"
    ):
        _control_run_evidence(Corrupted(), RUN_ONE_ID)


@pytest.mark.unit
def test_pipeline_metrics_reject_zero_statement_or_unbound_artifacts():
    with pytest.raises(ValueError, match="statement counts"):
        PipelineRunMetrics.anchored(
            control_run_id=RUN_ONE_ID,
            schema="sequential",
            mode="sequential",
            elapsed_seconds=80.0,
            match_count=1,
            match_keys_sha256="a" * 64,
            statement_counts=StatementCounts(0, 0),
        )


@pytest.mark.unit
def test_real_pipeline_evidence_has_no_trino_writes_and_one_strict_verdict():
    manager = _DifferentialManager()
    control = _DirectControl()
    sentinel = "outside-match"
    baseline_sentinels = _sentinel_digests(manager, "sequential", sentinel)
    sequential_baseline = AcceptanceBaseline(
        schema="sequential",
        sentinel_match_id=sentinel,
        snapshots={table: 100 for table in BENCHMARK_TABLES},
        sentinels=baseline_sentinels,
    )
    batch_baseline = AcceptanceBaseline(
        schema="batch",
        sentinel_match_id=sentinel,
        snapshots={table: 100 for table in BENCHMARK_TABLES},
        sentinels=baseline_sentinels,
    )
    manager.queries.clear()
    report = evaluate_existing_pipeline_acceptance(
        manager=manager,
        control=control,
        sequential_schema="sequential",
        batch_schema="batch",
        sequential_control_run_id=RUN_ONE_ID,
        batch_control_run_id=RUN_TWO_ID,
        sequential_baseline=sequential_baseline,
        batch_baseline=batch_baseline,
        sentinel_match_id=sentinel,
    )
    rendered = report.to_dict()

    assert rendered["passed"] is True
    assert rendered["matches"] == 1
    assert len(rendered["table_diffs"]) == 12
    assert rendered["sentinels_preserved"] is True
    assert rendered["control_equivalent"] is True
    assert rendered["sequential"]["snapshots_before"]
    assert rendered["sequential"]["snapshots_after"]
    assert all(
        delta["changed"]
        for delta in rendered["sequential"]["snapshot_deltas"].values()
    )
    assert rendered["sequential"]["statement_counts"] == {
        "execute": 120,
        "execute_committing": 24,
    }
    assert all(
        not sql.lstrip().upper().startswith(
            ("INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP")
        )
        for sql, _params in manager.queries
    )


@pytest.mark.unit
def test_real_pipeline_rejects_metrics_for_a_different_match_cohort():
    manager = _DifferentialManager()

    class WrongCohort(_DirectControl):
        def get_run(self, run_id):
            run = super().get_run(run_id)
            if run_id == RUN_ONE_ID:
                run["metadata"]["pipeline_run_metrics"] = (
                    PipelineRunMetrics.anchored(
                        control_run_id=RUN_ONE_ID,
                        schema="sequential",
                        mode="sequential",
                        elapsed_seconds=80.0,
                        match_count=2,
                        match_keys_sha256=benchmark._stable_sha256(
                            ["m1", "m2"]
                        ),
                        statement_counts=StatementCounts(120, 24),
                    ).to_dict()
                )
            return run

    control = WrongCohort()
    sentinel = "outside-match"
    sentinels = _sentinel_digests(manager, "sequential", sentinel)

    with pytest.raises(ValueError, match="match cohort"):
        evaluate_existing_pipeline_acceptance(
            manager=manager,
            control=control,
            sequential_schema="sequential",
            batch_schema="batch",
            sequential_control_run_id=RUN_ONE_ID,
            batch_control_run_id=RUN_TWO_ID,
            sequential_baseline=AcceptanceBaseline(
                schema="sequential",
                sentinel_match_id=sentinel,
                snapshots={table: 100 for table in BENCHMARK_TABLES},
                sentinels=sentinels,
            ),
            batch_baseline=AcceptanceBaseline(
                schema="batch",
                sentinel_match_id=sentinel,
                snapshots={table: 100 for table in BENCHMARK_TABLES},
                sentinels=sentinels,
            ),
            sentinel_match_id=sentinel,
        )


@pytest.mark.unit
def test_acceptance_baseline_captures_all_snapshots_and_sentinels():
    manager = _DifferentialManager()

    baseline = capture_acceptance_baseline(
        manager, schema="sequential", sentinel_match_id="outside-match"
    )

    assert baseline.schema == "sequential"
    assert set(baseline.snapshots) == set(BENCHMARK_TABLES)
    assert set(baseline.sentinels) == set(BENCHMARK_TABLES)
