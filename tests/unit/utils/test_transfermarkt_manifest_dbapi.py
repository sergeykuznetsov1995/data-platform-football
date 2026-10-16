"""Real Trino list-row DBAPI over the existing offline physical SQL backend."""
import copy
import socket

import pytest
from trino.dbapi import Cursor

from scrapers.transfermarkt import superseded, write_intents, ops_traffic
from tests.unit.utils.test_transfermarkt_current_write import (
    MemoryCursor, _native_history_portion, scraper,
)
from dags.utils import transfermarkt_current_write as current


class TrinoRows(Cursor):
    """Replace SQL I/O only; fetchone/fetchall are the actual Trino methods."""
    def __init__(self, db):
        self.delegate = MemoryCursor(db)
        self._iterator = iter(())

    @property
    def description(self):
        return self.delegate.description

    def execute(self, sql, params=()):
        self.delegate.execute(sql, params)
        self._iterator = iter([list(row) for row in self.delegate.fetchall()])
        return self

    def close(self):
        self.delegate.close()


@pytest.fixture
def trino_scraper(scraper, monkeypatch):
    monkeypatch.setattr(ops_traffic, 'record_traffic_run', lambda *_args, **_kwargs: None)
    network_calls = []
    def reject_network(*args, **kwargs):
        network_calls.append(args)
        raise AssertionError('Manifest DBAPI regression must not access network')
    monkeypatch.setattr(socket, 'getaddrinfo', reject_network)
    monkeypatch.setattr(scraper.test_db, 'cursor', lambda: TrinoRows(scraper.test_db))
    yield scraper
    assert network_calls == []


@pytest.mark.parametrize('mode', ['native-only', 'dual'])
def test_original_manifest_archive_accepts_actual_trino_rows(trino_scraper, monkeypatch, tmp_path, mode):
    source, result = _native_history_portion(trino_scraper, monkeypatch, tmp_path, '1', 9000000, mode)
    manifest = result['native_write_manifest' if mode == 'native-only' else 'batch_manifest']
    row, = manifest['rows']
    archive = write_intents.completed_capture('stable-history-child', 'market_value_points', row['native_batch_id'])
    original = copy.deepcopy(archive)
    proof = superseded.verify_complete(trino_scraper, current.run.ENTITY_SPECS['market_value_history'],
                                       archive, players=['1'], live=False, allow_later_manifest=False)
    assert proof['archive'] == original
    assert write_intents.completed_capture('stable-history-child', 'market_value_points', row['native_batch_id']) == original
    assert sum(len(client.get_calls) for client in source.clients) == 1


@pytest.mark.parametrize('mode', ['native-only', 'dual'])
def test_later_manifest_archive_keeps_original_trino_capture(trino_scraper, monkeypatch, tmp_path, mode):
    _native_history_portion(trino_scraper, monkeypatch, tmp_path, '1', 9000000, mode)
    manifest_table = 'transfermarkt_native_write_manifest_v2' if mode == 'native-only' else 'transfermarkt_dual_write_manifest_v2'
    batch, = trino_scraper.test_db.sql.execute(f'SELECT native_batch_id FROM {manifest_table}').fetchone()
    first = write_intents.completed_capture('stable-history-child', 'market_value_points', batch)
    original = copy.deepcopy(first)
    _native_history_portion(trino_scraper, monkeypatch, tmp_path, '2', 10000000, mode)
    proof = superseded.verify_complete(trino_scraper, current.run.ENTITY_SPECS['market_value_history'], first, players=['1'])
    assert proof['archive'] == original
    assert write_intents.completed_capture('stable-history-child', 'market_value_points', batch) == original


@pytest.mark.parametrize('mode', ['native-only', 'dual'])
@pytest.mark.parametrize('damage', ['missing', 'duplicate', 'native_hash', 'native_rows', 'status'])
def test_real_manifest_mismatch_still_rejected(trino_scraper, monkeypatch, tmp_path, mode, damage):
    _native_history_portion(trino_scraper, monkeypatch, tmp_path, '1', 9000000, mode)
    table = 'transfermarkt_native_write_manifest_v2' if mode == 'native-only' else 'transfermarkt_dual_write_manifest_v2'
    batch, = trino_scraper.test_db.sql.execute(f'SELECT native_batch_id FROM {table}').fetchone()
    archive = write_intents.completed_capture('stable-history-child', 'market_value_points', batch)
    original = copy.deepcopy(archive)
    sql = {
        'missing': f'DELETE FROM {table}',
        'duplicate': f'INSERT INTO {table} SELECT * FROM {table}',
        'native_hash': f"UPDATE {table} SET native_hash='wrong-hash'",
        'native_rows': f'UPDATE {table} SET native_rows=native_rows+1',
        'status': f"UPDATE {table} SET status='parity_mismatch'",
    }[damage]
    trino_scraper.test_db.sql.execute(sql)
    with pytest.raises(RuntimeError, match='no genuine original manifest readback'):
        superseded.attest_career_manifest(trino_scraper, copy.deepcopy(archive['receipt']))
    with pytest.raises(RuntimeError, match='lacks its genuine successful'):
        superseded.verify_complete(trino_scraper, current.run.ENTITY_SPECS['market_value_history'],
                                   archive, players=['1'], live=False, allow_later_manifest=False)
    assert write_intents.completed_capture('stable-history-child', 'market_value_points', batch) == original


@pytest.mark.parametrize('mode', ['native-only', 'dual'])
@pytest.mark.parametrize('empty', [False, True])
def test_failed_attestation_recovers_original_trino_intent_without_http(
        trino_scraper, monkeypatch, tmp_path, mode, empty):
    from tests.unit.utils.test_transfermarkt_current_write import (
        test_history_publication_failure_after_http_is_platform_and_cannot_heal_probe,
    )
    # Reuse the full original-clock/checkpoint/raw/expense recovery regression,
    # changing only SQL's DBAPI row representation to the actual Trino driver.
    test_history_publication_failure_after_http_is_platform_and_cannot_heal_probe(
        trino_scraper, monkeypatch, tmp_path, 'attestation', mode, empty)
