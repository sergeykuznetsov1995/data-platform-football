import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from scrapers.transfermarkt import history_portion as history
from scrapers.transfermarkt.models import ProxyLease, LeaseTrafficSnapshot, TrafficBudgetExceeded


@pytest.fixture
def portion(tmp_path, monkeypatch):
    monkeypatch.setenv('TM_HISTORY_DEADLINE_AT', '2026-10-10T18:45:00+00:00')
    monkeypatch.setenv('TM_HISTORY_PORTION_ID', 'portion-one')
    monkeypatch.setenv('TM_HISTORY_ATTEMPT_LEDGER', str(tmp_path / 'attempts.json'))
    monkeypatch.setenv('TM_HISTORY_LEASE_DIR', str(tmp_path / 'leases'))
    monkeypatch.setattr(history, 'remaining_seconds', lambda: 2700)
    return tmp_path


def test_paid_attempt_cap_survives_workers_retries_and_restart(portion):
    def admit(_):
        try:
            history.reserve_attempt()
            return True
        except TrafficBudgetExceeded:
            return False
    with ThreadPoolExecutor(max_workers=8) as workers:
        results = list(workers.map(admit, range(520)))
    assert sum(results) == 500
    assert json.loads((portion / 'attempts.json').read_text())['attempts'] == 500
    with pytest.raises(TrafficBudgetExceeded):
        history.reserve_attempt()


def test_hardkill_lease_waits_for_ttl_and_authoritative_close(portion, monkeypatch):
    lease = ProxyLease('lease-one', 'private-token', 'http://proxy.test', 1000, 2000)
    history.journal_lease(lease, 'scope-one')
    path = history._lease_journal('scope-one')
    assert path.stat().st_mode & 0o777 == 0o600
    provider = Mock()
    monkeypatch.setattr(history.time, 'time', lambda: 1999)
    with pytest.raises(history.HistoryContinuation):
        history.reconcile_leases(provider, 'scope-one')
    provider.close.assert_not_called()
    monkeypatch.setattr(history.time, 'time', lambda: 2001)
    provider.close.side_effect = RuntimeError('close is not complete')
    with pytest.raises(RuntimeError, match='not complete'):
        history.reconcile_leases(provider, 'scope-one')
    assert json.loads(path.read_text())['leases']['lease-one']['status'] == 'active'
    provider.close.side_effect = None
    provider.close.return_value = LeaseTrafficSnapshot(up_bytes=4, down_bytes=6)
    history.reconcile_leases(provider, 'scope-one')
    proof = json.loads(path.read_text())['leases']['lease-one']
    assert proof['status'] == 'closed'
    assert proof['traffic']['up_bytes'] + proof['traffic']['down_bytes'] == 10
    assert 'token' not in proof['lease']
    provider.close.reset_mock()
    history.reconcile_leases(provider, 'scope-one')
    provider.close.assert_not_called()


def test_history_drains_1201_exact_careers_without_global_debt(portion, monkeypatch):
    from dags.scripts import run_transfermarkt_scraper as runner
    from scrapers.transfermarkt.scraper import TransfermarktScraper
    roster = [str(value) for value in range(1201)]
    state = {}
    monkeypatch.setattr(runner, '_resolve_roster', lambda *a: roster)
    monkeypatch.setattr(runner, '_load_fetch_state', lambda *a, **k: dict(state))
    monkeypatch.setattr(runner, '_load_pending_checkpoint', lambda *a: ({}, None))
    monkeypatch.setattr(runner, '_load_data_derived_state', lambda *a: {})
    processed = []
    for index in range(4):
        monkeypatch.setenv('TM_HISTORY_PORTION_ID', f'portion-{index}')
        monkeypatch.setenv('TM_HISTORY_ATTEMPT_LEDGER', str(portion / f'{index}.json'))
        selected, _, _, _, coverage = runner._select_player_ids(
            Mock(), runner.ENTITY_SPECS['transfers'], 'GB1', 2020, 500, 0,
            'historical', 'original-cycle', False, legacy_materialization_required=False)
        if not selected:
            break
        scraper = object.__new__(TransfermarktScraper)
        scraper._career_windows = {}
        scraper._batch_id = 'original-batch'
        counter = {'used': 0}
        scraper._http_client = SimpleNamespace(get_traffic_stats=lambda: {
            'request_attempt_budget': 650, 'request_attempts': counter['used'],
            'decoded_response_body_bytes': 0})
        scraper._resolve_scope = lambda *a: {'compatibility_league': 'GB1', 'canonical_season': '2020', 'competition_id': 'GB1', 'edition_id': '2020', 'scope_id': 'GB1__2020'}
        scraper._mark_authoritative_empty = lambda *a: None
        def fetch(*a, **k):
            history.reserve_attempt()
            counter['used'] += 1
            return {'transfers': []}
        scraper._fetch_json = fetch
        scraper._read_player_endpoint_rows(league='GB1', season=2020,
            player_ids=selected, limit=500, window_offset=0, label='transfer_events',
            path_template='/player/{player_id}', parser=lambda *a: [], columns=['player_id'],
            entity_type='transfer_events', decoded_body_soft_stop_bytes=10000)
        result = {'roster_coverage': coverage}
        done = runner._apply_career_window(scraper, runner.ENTITY_SPECS['transfers'], selected, result)
        state.update({pid: {'status': 'authoritative_empty'} for pid in done})
        processed.extend(done)
        assert result['roster_coverage']['remaining_ids'] == [pid for pid in roster if pid not in state]
        assert counter['used'] <= 500
    assert processed == roster
    assert len(set(processed)) == 1201


def test_pool_sizes_share_stream_canon():
    from scrapers.transfermarkt.streams import TransfermarktStreams
    from scrapers.transfermarkt.airflow_pools import pool_sizes
    assert pool_sizes(TransfermarktStreams()) == {
        'transfermarkt_control': 1, 'transfermarkt_proxy': 1,
        'transfermarkt_backfill_control': 0, 'transfermarkt_backfill_proxy': 0}
    assert pool_sizes(TransfermarktStreams(history_streams=2))['transfermarkt_backfill_proxy'] == 2


def test_recovery_probe_spends_only_one_attempt_including_retries(portion, monkeypatch):
    monkeypatch.setenv('TM_HISTORY_RECOVERY_PROBE', 'true')
    history.reserve_attempt()
    with pytest.raises(TrafficBudgetExceeded):
        history.reserve_attempt()
    assert json.loads((portion / 'attempts.json').read_text())['attempts'] == 1
    monkeypatch.delenv('TM_HISTORY_RECOVERY_PROBE')
    with pytest.raises(ValueError, match='limit drift'):
        history.reserve_attempt()


def test_custom_history_attempt_limit_is_validated(portion, monkeypatch):
    monkeypatch.setenv('TM_HISTORY_PORTION_REQUEST_LIMIT', '0')
    with pytest.raises(ValueError, match='1..500'):
        history.reserve_attempt()
    monkeypatch.setenv('TM_HISTORY_PORTION_REQUEST_LIMIT', '501')
    with pytest.raises(ValueError, match='1..500'):
        history.reserve_attempt()


def test_delivery_window_admission_uses_absolute_deadline(monkeypatch):
    from dags.utils.transfermarkt_current_timetable import work_deadline, remaining_work_seconds
    started = datetime(2026, 10, 10, 0, 14, tzinfo=timezone.utc)
    assert (work_deadline(started) - started).total_seconds() == 2700
    forbidden = datetime(2026, 10, 10, 0, 15, tzinfo=timezone.utc)
    assert work_deadline(forbidden) == forbidden
    assert remaining_work_seconds(work_deadline(started), started.replace(hour=1)) == 0
