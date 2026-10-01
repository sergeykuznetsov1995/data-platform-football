"""Isolated benchmark uses recorded payloads and the real serial batch writer."""
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from scrapers.espn import urls
from scrapers.espn.bronze_schema import TABLES, PARTITION_SPEC
from scrapers.espn.bronze_writer import WRITE_ORDER
from scrapers.espn.pace_benchmark import IsolatedManager, cached_payloads, run_benchmark
from scrapers.espn.pace_store import PaceStore
from scrapers.espn.raw_store import EspnRawStore
from scrapers.espn.transport import canonicalize_target
from tests.unit.scrapers.test_espn_bronze_writer import FakeTrino
from tests.unit.scrapers.test_espn_probes import PROBES

pytestmark = pytest.mark.unit


class BenchTrino(FakeTrino):
    def __init__(self):
        super().__init__()
        self.sql = []
    def execute_query(self, sql):
        self.sql.append(sql)
        if sql.startswith('SELECT count'):
            return [[len(self.tables[sql.rsplit('.', 1)[1]])]]
        return []


def test_recorded_payloads_write_only_isolated_namespace_and_cleanup(tmp_path):
    raw = EspnRawStore.from_uri((tmp_path / 'raw').as_uri())
    request = urls.summary('eng.1', 422285)
    raw.store(canonicalize_target(request.url, request.params), request.endpoint,
              (PROBES / 'summary_eng1_2015_422285.json').read_bytes())
    payloads = cached_payloads(raw, [422285])
    assert payloads[0].schedule.source_season_year == 2015
    trino = BenchTrino()
    created = []
    writer = SimpleNamespace(create_table_if_not_exists=lambda *args, **kwargs: created.append((args, kwargs)))
    store = PaceStore(tmp_path / 'pace.sqlite3')
    schema = 'espn_pace_bench_' + 'a' * 32
    result = run_benchmark(trino, writer, store, payloads, schema=schema, repeats=2)
    assert result['isolated']
    assert len(trino.calls) == 8
    assert [call['table'] for call in trino.calls] == list(WRITE_ORDER) * 2
    assert all(call['schema'] == schema and call['single_statement_replace'] for call in trino.calls)
    assert all(args[0] == schema and kwargs['partition_spec'] == PARTITION_SPEC for args, kwargs in created)
    assert [args[2] for args, _ in created] == list(TABLES.values())
    assert all('iceberg.bronze.' not in sql for sql in trino.sql)
    assert trino.sql[-1] == f'DROP SCHEMA iceberg.{schema}'
    assert len(store.rows('write', 0, datetime.now(timezone.utc).timestamp())) == 2


def test_namespace_and_target_escape_are_rejected():
    trino = BenchTrino()
    for schema in ('bronze', 'espn_pace_bench_x; DROP SCHEMA bronze', 'ops'):
        with pytest.raises(ValueError):
            IsolatedManager(trino, schema)
    adapter = IsolatedManager(trino, 'espn_pace_bench_' + 'b' * 32)
    with pytest.raises(ValueError):
        adapter.insert_dataframe_atomic('ops', 'espn_match', None, single_statement_replace=True)
    with pytest.raises(ValueError):
        adapter.insert_dataframe_atomic('bronze', 'other', None, single_statement_replace=True)
    assert not trino.calls


def test_namespace_creation_failure_never_drops_existing_schema(tmp_path):
    trino = BenchTrino()
    def fail(sql):
        trino.sql.append(sql)
        raise RuntimeError('schema already exists')
    trino.execute_query = fail
    with pytest.raises(RuntimeError, match='already exists'):
        run_benchmark(trino, None, PaceStore(tmp_path / 'pace.sqlite3'), [object()])
    assert len(trino.sql) == 1
    assert trino.sql[0].startswith('CREATE SCHEMA')
