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


def test_rows_match_nullable_boolean_frame_against_trino_bool():
    import pandas as pd
    frame = pd.DataFrame({'player_id': ['1', '2'], 'is_upcoming': [False, True]}).astype({'is_upcoming': 'boolean'})
    trino = pd.DataFrame([['1', False], ['2', True]], columns=frame.columns, dtype=object)
    assert current._rows(frame) == current._rows(trino)
    assert current._rows(frame) != current._rows(trino.assign(is_upcoming=[True, False]))


def _native_transfer_portion(scraper, monkeypatch, tmp_path, player, mode):
    import contextlib
    import json
    import scrapers.transfermarkt
    from tests.unit.utils.test_transfermarkt_current_write import http
    http(scraper, [{'transfers': [{'id': 'source-' + player, 'date': 'Sep 1, 2025', 'season': '25/26',
        'upcoming': False, 'from': {'clubName': 'Old', 'href': '/old/verein/10'},
        'to': {'clubName': 'Next', 'href': '/next/verein/20'}, 'fee': 'free transfer', 'marketValue': 'EUR1m'}]}])
    scraper._batch_id = 'transfer-capture-' + player
    with monkeypatch.context() as patch:
        patch.setattr(scrapers.transfermarkt, 'TransfermarktScraper', lambda **_: contextlib.nullcontext(scraper))
        patch.setattr(current.run, '_select_player_ids', lambda *_args, **_kwargs:
            ([player], 0, 0, [], {'roster_size': 1, 'selected': 1, 'pending': 0}))
        result = current.run._run_entity(current.run.ENTITY_SPECS['transfers'], ['GB1'], 2026,
            1, str(tmp_path / ('transfer-result-' + player + '.json')), refresh_mode='history',
            run_key='stable-transfer-child', write_mode=mode, expected_reader_revision=7)
    assert result == 0
    return json.loads((tmp_path / ('transfer-result-' + player + '.json')).read_text())


class TrinoBoolRows(TrinoRows):
    """Trino returns BOOLEAN as Python bool; the offline backend stores its text."""
    def execute(self, sql, params=()):
        super().execute(sql, params)
        names = [column[0] for column in self.delegate.description or ()]
        rows = [[{'True': True, 'False': False}.get(value, value) if name == 'is_upcoming' else value
                 for name, value in zip(names, row)] for row in self._iterator]
        self._iterator = iter(rows)
        return self


@pytest.mark.parametrize('mode', ['native-only', 'dual'])
def test_transfer_archive_accepts_trino_python_bool(trino_scraper, monkeypatch, tmp_path, mode):
    result = _native_transfer_portion(trino_scraper, monkeypatch, tmp_path, '1', mode)
    monkeypatch.setattr(trino_scraper.test_db, 'cursor', lambda: TrinoBoolRows(trino_scraper.test_db))
    assert result['career_capture_archive_status'] == 'complete'
    manifest = result['native_write_manifest' if mode == 'native-only' else 'batch_manifest']
    row, = manifest['rows']
    archive = write_intents.completed_capture('stable-transfer-child', 'transfer_events', row['native_batch_id'])
    proof = superseded.verify_complete(trino_scraper, current.run.ENTITY_SPECS['transfers'],
                                       archive, players=['1'], live=False, allow_later_manifest=False)
    assert proof['archive'] == archive
