"""Exercise production response decoding with a local transport and raw store."""
import json

import pytest

from scrapers.transfermarkt.client import TransfermarktHttpClient
from scrapers.transfermarkt.models import FetchStatus, SharedTrafficLedger
from scrapers.transfermarkt.orchestrator import CoreRun, CoreScopePlan
from scrapers.transfermarkt.raw_store import RawResponseStore
from tests.unit.scrapers.test_transfermarkt_native_writer import CoreDB, CoreWriter
from tests.unit.scrapers.test_transfermarkt_sections import REPRESENTATION_URL, _representation_capture
from tests.unit.scrapers.test_transfermarkt_traffic import _ClientFactory, _FakeResp

pytestmark = pytest.mark.unit


@pytest.mark.parametrize('oversized', [True, False], ids=['oversized', 'ordinary'])
def test_production_representation_fetch_preserves_sibling_receipts(oversized, tmp_path, monkeypatch):
    # Round 9 finding 9
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)
    payload = json.loads(_representation_capture().body)
    first, second = payload['data']
    first['attributes']['consultantAgencyId'] = 'agency-token'
    second['attributes']['consultantAgencyId'] = 56
    token = '9' * 4301 if oversized else '55'
    body = json.dumps(payload).replace('"agency-token"', token).encode()
    transport = _ClientFactory([_FakeResp(body)])
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    client = TransfermarktHttpClient(
        proxy='http://127.0.0.1:8080', client_factory=transport,
        traffic_ledger=SharedTrafficLedger(), raw_store=store, require_raw_store=True,
    )

    def fetch(url, **kwargs):
        result = client.fetch(url, **kwargs)
        assert result.status is FetchStatus.OK
        assert store.load_capture(result.raw_capture_id)[0] == body
        assert [row['attributes']['consultantAgencyId'] for row in result.value['data']] == [
            token if oversized else 55, 56]
        return result

    plan = CoreScopePlan(league='GB1', season='2627', scope_id='s', cycle_id='c',
                         representation_player_ids=('7', '9'))
    db = CoreDB()
    try:
        report = CoreRun(fetch, raw_loader=store.load_capture).run(
            plan, writer=CoreWriter(db), connection_factory=lambda: db)
    finally:
        client.close()
    assert [url for tls in transport.clients for url, _ in tls.get_calls] == [REPRESENTATION_URL]
    first, second = report.outcomes
    assert first['source_capture_id'] == second['source_capture_id']
    assert (second['status'], second['rows']) == ('observed_present', 1)
    if oversized:
        assert (first['status'], first['status_reason'], first['rows']) == (
            'error', 'representation:invalid_packet', 0)
    else:
        assert (first['status'], first['rows']) == ('observed_present', 1)
    receipt, = report.receipts
    assert receipt['verified'] is True
    written = db.sql.execute(
        'SELECT player_id, representative_id FROM transfermarkt_player_representation_observations '
        'ORDER BY player_id').fetchall()
    assert written == ([('9', '56')] if oversized else [('7', '55'), ('9', '56')])
    scopes = db.sql.execute('SELECT scope_values_json FROM transfermarkt_core_receipts_v1')
    assert sorted(dict(json.loads(scope))['player_id'] for scope, in scopes) == [
        player for player, _ in written]
