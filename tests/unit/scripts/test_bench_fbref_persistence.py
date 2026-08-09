"""Contract coverage for the offline FBref persistence benchmark."""

import logging
from contextlib import contextmanager

import pytest

import scripts.research.bench_fbref_persistence as benchmark
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
    _isolated_replay_control_evidence,
    _snapshot_ids,
    _sentinel_digests,
    _mean_iteration_seconds,
    _table_digests,
    _validate_trino_match_cohort,
    evaluate_gate,
    evaluate_existing_pipeline_acceptance,
    run_benchmark,
)


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
            run_id = "run-one" if ".sequential." in sql else "run-two"
            identity = (
                "fbref:match:m1"
                if table in BENCHMARK_TABLES[:3]
                else "m1"
            )
            return [(identity, run_id)]
        value = 1 if self.nonzero_query and self.nonzero_query in sql else 0
        return [(value,)]


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
        sequential_run_id="run-one",
        batch_run_id="run-two",
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
    assert any("run-one" in params for _sql, params in diff_queries)
    assert any("run-two" in params for _sql, params in diff_queries)


@pytest.mark.unit
def test_unexpected_cohort_lineage_fails_closed_before_comparison():
    manager = _DifferentialManager(nonzero_query="IS DISTINCT FROM")

    with pytest.raises(ValueError, match="unexpected run lineage"):
        _bidirectional_table_diffs(
            manager,
            "sequential",
            "batch",
            sequential_run_id="run-one",
            batch_run_id="run-two",
            sentinel_match_id="outside-match",
        )


@pytest.mark.unit
def test_trino_page_manifest_must_equal_the_direct_control_match_cohort():
    evidence = _control_run_evidence(_DirectControl(), "run-one")
    manager = _DifferentialManager(trino_match_ids=())

    with pytest.raises(ValueError, match="Trino match cohort"):
        _validate_trino_match_cohort(
            manager,
            schema="sequential",
            expected_run_id="run-one",
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

    def get_run(self, run_id):
        self.calls.append(("run", run_id))
        mode = "batch" if run_id == "run-two" else "sequential"
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
                "acceptance_replay_source_run_id": "source-run",
                "acceptance_persistence_mode": mode,
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
                }
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

    def get_acceptance_run_evidence(self, run_id):
        self.calls.append(("evidence", run_id))
        return {
            "targets": [
                {
                    "target_id": "fbref:match:m1",
                    "logical_refresh_id": f"refresh-{run_id}",
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
                }
            ],
            "datasets": [
                {
                    "target_id": "fbref:match:m1",
                    "content_hash": "a" * 64,
                    "parser_version": "typed-v1",
                    "dataset": f"typed:{dataset}",
                    "availability": "available",
                    "parse_status": "succeeded",
                    "persistence_status": "succeeded",
                    "validation_status": "succeeded",
                    "row_count": 3,
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
            ],
        }

    def replay_control_transaction_evidence(
        self,
        *,
        replay_run_id,
        source_run_id,
        mode,
        targets,
        datasets,
    ):
        self.calls.append(
            ("control_replay", replay_run_id, source_run_id, mode)
        )
        replay_targets = [
            {
                **dict(item),
                "replay_target_id": (
                    f"fbref:acceptance-replay:{replay_run_id}:"
                    f"{item['target_id']}"
                ),
                "logical_refresh_id": (
                    f"{replay_run_id}:{item['logical_refresh_id']}"
                ),
            }
            for item in targets
        ]
        replay_datasets = [
            {
                **dict(item),
                "replay_target_id": (
                    f"fbref:acceptance-replay:{replay_run_id}:"
                    f"{item['target_id']}"
                ),
            }
            for item in datasets
        ]
        return {"targets": replay_targets, "datasets": replay_datasets}


@pytest.mark.unit
def test_control_evidence_uses_an_isolated_transaction_outcome():
    control = _DirectControl()

    evidence = _control_run_evidence(control, "run-one")

    assert isinstance(evidence, ControlRunEvidence)
    assert control.calls == [
        ("run", "run-one"),
        ("summary", "run-one"),
        ("evidence", "source-run"),
        ("control_replay", "run-one", "source-run", "sequential"),
    ]
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
        replay_control_transaction_evidence = None

    with pytest.raises(
        ValueError, match="isolated replay control transaction is unavailable"
    ):
        _control_run_evidence(SourceOnlyControl(), "run-one")


@pytest.mark.unit
def test_control_equivalence_requires_the_same_frozen_source_run():
    class DifferentSourceControl(_DirectControl):
        def get_run(self, run_id):
            run = super().get_run(run_id)
            if run_id == "run-two":
                run["metadata"]["acceptance_replay_source_run_id"] = (
                    "other-source-run"
                )
            return run

    control = DifferentSourceControl()
    sequential = _control_run_evidence(control, "run-one")
    batch = _control_run_evidence(control, "run-two")

    assert sequential.observation_sha256 == batch.observation_sha256
    assert _control_evidence_equivalent(sequential, batch) is False


@pytest.mark.unit
def test_control_evidence_rejects_an_unversioned_strict_marker():
    class Unversioned(_DirectControl):
        def get_run(self, run_id):
            run = super().get_run(run_id)
            del run["metadata"]["bronze_acceptance_replay"]["schema_version"]
            return run

    evidence = _control_run_evidence(Unversioned(), "run-one")

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

    evidence = _control_run_evidence(Incomplete(), "run-one")

    assert evidence.valid is False
    assert "strict_acceptance_gates_invalid" in evidence.failures


@pytest.mark.unit
def test_postgres_source_rows_seed_an_isolated_replay_control_transaction():
    base = _DirectControl()
    datasets = base.get_acceptance_run_evidence("run-direct")["datasets"]
    source_run_id = "source-run-direct"
    observations = [
        {
            "ordinal": 0,
            "target_id": "fbref:match:m1",
            "logical_refresh_id": "refresh-direct",
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
        }
    ]

    class Cursor:
        def __init__(self):
            self.queries = []
            self.rows = []

        def execute(self, sql, params):
            self.queries.append((sql, params))
            self.rows = (
                observations
                if "SELECT * FROM ranked" in sql
                else datasets
            )

        def fetchall(self):
            return self.rows

    class DirectControl(_DirectControl):
        def __init__(self):
            super().__init__()
            self.cursor = Cursor()

        @contextmanager
        def _transaction(self):
            yield self.cursor

        def get_acceptance_run_evidence(self, _run_id):
            raise AssertionError("source-run alias evidence must not be used")

        def get_run(self, run_id):
            run = super().get_run(run_id)
            run["metadata"]["acceptance_replay_source_run_id"] = (
                source_run_id
            )
            return run

    control = DirectControl()

    evidence = _control_run_evidence(control, "run-direct")

    assert evidence.valid is True
    assert evidence.evidence_source == "isolated_replay_control_transaction"
    assert (
        "control_replay",
        "run-direct",
        source_run_id,
        "sequential",
    ) in control.calls
    assert len(control.cursor.queries) == 2
    assert all(
        params == (source_run_id,) for _sql, params in control.cursor.queries
    )
    assert "fbref_control.observation_processing" in control.cursor.queries[0][0]
    assert "fbref_control.dataset_manifest" in control.cursor.queries[1][0]


@pytest.mark.unit
@pytest.mark.parametrize(
    "defect",
    ("observation_completion", "dataset_manifest", "latest_frontier"),
)
def test_control_equivalence_rejects_a_divergent_batch_transaction(defect):
    """A shared source snapshot must not hide a broken batch completion."""

    class DivergentBatchControl(_DirectControl):
        def get_run(self, run_id):
            run = super().get_run(run_id)
            run["metadata"]["acceptance_replay_source_run_id"] = "source-run"
            run["metadata"]["acceptance_persistence_mode"] = (
                "batch" if run_id == "run-two" else "sequential"
            )
            return run

        def replay_control_transaction_evidence(
            self,
            *,
            replay_run_id,
            source_run_id,
            mode,
            targets,
            datasets,
        ):
            del source_run_id
            replay_targets = [
                {
                    **dict(item),
                    "replay_target_id": (
                        f"fbref:acceptance-replay:{replay_run_id}:"
                        f"{item['target_id']}"
                    ),
                    "logical_refresh_id": f"{replay_run_id}:refresh-m1",
                    "target_status": "succeeded",
                    "frontier_state": "fetched",
                    "last_content_hash": "a" * 64,
                    "content_hash": "a" * 64,
                    "observation_status": "succeeded",
                    "generic_status": "succeeded",
                    "typed_status": "succeeded",
                    "stateful_status": "skipped",
                    "validation_status": "succeeded",
                }
                for item in targets
            ]
            replay_datasets = [
                {
                    **dict(item),
                    "replay_target_id": (
                        f"fbref:acceptance-replay:{replay_run_id}:"
                        f"{item['target_id']}"
                    ),
                }
                for item in datasets
            ]
            if mode == "batch" and defect == "observation_completion":
                replay_targets[0]["observation_status"] = "processing"
            elif mode == "batch" and defect == "dataset_manifest":
                replay_datasets.pop()
            elif mode == "batch" and defect == "latest_frontier":
                replay_targets[0]["last_content_hash"] = "b" * 64
            return {"targets": replay_targets, "datasets": replay_datasets}

    control = DivergentBatchControl()

    sequential = _control_run_evidence(control, "run-one")
    batch = _control_run_evidence(control, "run-two")

    assert _control_evidence_equivalent(sequential, batch) is False


@pytest.mark.unit
def test_isolated_control_sql_uses_distinct_paths_and_only_temp_tables():
    source = _DirectControl().get_acceptance_run_evidence("source-run")
    second_target = {
        **source["targets"][0],
        "ordinal": 1,
        "target_id": "fbref:match:m2",
        "logical_refresh_id": "refresh-source-run-m2",
        "source_ids": {"match_id": "m2"},
    }
    source["targets"][0]["ordinal"] = 0
    source["targets"].append(second_target)
    source["datasets"].extend(
        {
            **dict(item),
            "target_id": "fbref:match:m2",
        }
        for item in list(source["datasets"])
    )

    class Cursor:
        def __init__(self, replay_run_id):
            self.replay_run_id = replay_run_id
            self.executions = []
            self.rows = []

        def execute(self, sql, params=None):
            normalized = " ".join(sql.split())
            self.executions.append((normalized, params))
            if "observation.status AS observation_status" in normalized:
                self.rows = [
                    {
                        **dict(item),
                        "replay_target_id": (
                            "fbref:acceptance-replay:"
                            f"{self.replay_run_id}:{item['target_id']}"
                        ),
                        "logical_refresh_id": (
                            f"{self.replay_run_id}:"
                            f"{item['logical_refresh_id']}"
                        ),
                    }
                    for item in source["targets"]
                ]
            elif "manifest.dataset" in normalized:
                self.rows = [
                    {
                        **dict(item),
                        "replay_target_id": (
                            "fbref:acceptance-replay:"
                            f"{self.replay_run_id}:{item['target_id']}"
                        ),
                    }
                    for item in source["datasets"]
                ]
            else:
                self.rows = []

        def fetchall(self):
            return list(self.rows)

    class Control:
        def __init__(self, replay_run_id):
            self.cursor = Cursor(replay_run_id)

        @contextmanager
        def _transaction(self):
            yield self.cursor

    sequential_control = Control("sequential-run")
    batch_control = Control("batch-run")

    sequential = _isolated_replay_control_evidence(
        sequential_control,
        replay_run_id="sequential-run",
        source_run_id="source-run",
        mode="sequential",
        source_evidence=source,
    )
    batch = _isolated_replay_control_evidence(
        batch_control,
        replay_run_id="batch-run",
        source_run_id="source-run",
        mode="batch",
        source_evidence=source,
    )

    def normalized(evidence):
        return {
            "targets": [
                {
                    key: value
                    for key, value in item.items()
                    if key not in {"replay_target_id", "logical_refresh_id"}
                }
                for item in evidence["targets"]
            ],
            "datasets": [
                {
                    key: value
                    for key, value in item.items()
                    if key != "replay_target_id"
                }
                for item in evidence["datasets"]
            ],
        }

    assert normalized(sequential) == normalized(batch)
    sequential_sql = [sql for sql, _params in sequential_control.cursor.executions]
    batch_sql = [sql for sql, _params in batch_control.cursor.executions]
    assert sum(
        sql.startswith(
            "UPDATE pg_temp.fbref_acceptance_control_observation"
        )
        for sql in sequential_sql
    ) == 2
    assert sum(
        sql.startswith(
            "UPDATE pg_temp.fbref_acceptance_control_observation"
        )
        for sql in batch_sql
    ) == 1
    assert sum(
        sql.startswith(
            "INSERT INTO pg_temp.fbref_acceptance_control_manifest ("
        )
        for sql in sequential_sql
    ) == 2
    assert sum(
        sql.startswith(
            "INSERT INTO pg_temp.fbref_acceptance_control_manifest ("
        )
        for sql in batch_sql
    ) == 1
    assert sum(
        sql.startswith(
            "UPDATE pg_temp.fbref_acceptance_control_target AS target"
        )
        for sql in sequential_sql
    ) == 2
    assert sum(
        sql.startswith(
            "UPDATE pg_temp.fbref_acceptance_control_target AS target"
        )
        for sql in batch_sql
    ) == 1
    for sql in (*sequential_sql, *batch_sql):
        if sql.startswith(("INSERT", "UPDATE")):
            assert "pg_temp.fbref_acceptance_control_" in sql
        if sql.startswith("CREATE"):
            assert sql.startswith("CREATE TEMP TABLE")
        assert "fbref_control." not in sql


@pytest.mark.unit
def test_pipeline_metrics_reject_zero_statement_or_unbound_artifacts():
    with pytest.raises(ValueError, match="statement counts"):
        PipelineRunMetrics.anchored(
            control_run_id="run-one",
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
        sequential_control_run_id="run-one",
        batch_control_run_id="run-two",
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
            if run_id == "run-one":
                run["metadata"]["pipeline_run_metrics"] = (
                    PipelineRunMetrics.anchored(
                        control_run_id="run-one",
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
            sequential_control_run_id="run-one",
            batch_control_run_id="run-two",
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
