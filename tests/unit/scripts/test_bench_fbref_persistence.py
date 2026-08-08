"""Contract coverage for the offline FBref persistence benchmark."""

import logging

import pytest

import scripts.research.bench_fbref_persistence as benchmark
from scripts.research.bench_fbref_persistence import (
    BENCHMARK_TABLES,
    BenchmarkConfig,
    PersistenceRun,
    StatementCounts,
    TableDigest,
    _digests_equivalent,
    _mean_iteration_seconds,
    _table_digests,
    evaluate_gate,
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


@pytest.mark.unit
def test_dirty_sequential_schema_fails_before_loading_or_timing(
    monkeypatch, tmp_path
):
    class Manager:
        catalog = "iceberg"

        def table_exists(self, schema, table):
            return table == BENCHMARK_TABLES[0]

    called = []
    monkeypatch.setattr(benchmark, "CountingTrinoTableManager", Manager)
    monkeypatch.setattr(
        benchmark, "_load_replay_items", lambda _path: called.append("load")
    )
    monkeypatch.setattr(
        benchmark, "_run_persistence", lambda **_kwargs: called.append("run")
    )

    with pytest.raises(ValueError, match="must be empty"):
        run_benchmark(
            BenchmarkConfig(
                html_dir=tmp_path,
                sequential_schema="replay",
                batch_schema=None,
            )
        )

    assert called == []


@pytest.mark.unit
def test_dirty_batch_schema_fails_before_loading_or_timing(
    monkeypatch, tmp_path
):
    class Manager:
        catalog = "iceberg"

        def table_exists(self, schema, table):
            return schema == "batch" and table == BENCHMARK_TABLES[0]

    called = []
    monkeypatch.setattr(benchmark, "CountingTrinoTableManager", Manager)
    monkeypatch.setattr(benchmark, "_batch_api_available", lambda: True)
    monkeypatch.setattr(
        benchmark, "_load_replay_items", lambda _path: called.append("load")
    )
    monkeypatch.setattr(
        benchmark, "_run_persistence", lambda **_kwargs: called.append("run")
    )

    with pytest.raises(ValueError, match="batch.*must be empty"):
        run_benchmark(
            BenchmarkConfig(
                html_dir=tmp_path,
                sequential_schema="sequential",
                batch_schema="batch",
            )
        )

    assert called == []


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

    monkeypatch.setattr(benchmark, "_preflight_schemas", lambda _config: None)
    monkeypatch.setattr(benchmark, "_load_replay_items", lambda _path: (object(),))
    monkeypatch.setattr(benchmark, "_run_persistence", emit_sql_logs)

    report = run_benchmark(
        BenchmarkConfig(tmp_path, "sequential", batch_schema=None)
    )

    assert report.batch is None
    assert "secret-normal" not in caplog.text
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
