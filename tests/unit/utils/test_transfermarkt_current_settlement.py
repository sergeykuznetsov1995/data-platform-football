"""Regression: an interrupted lease close is not an eternal scope lock."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json

import pytest
from dags.utils.transfermarkt_current_settlement import CurrentSettlement, CurrentSettlementError
from scrapers.transfermarkt.client import ProxyFilterLeaseProvider, CurrentPortionDeadlineExceeded


@pytest.fixture
def evidence(tmp_path):
    cycle, child = 'old-parent', 'old-child'
    traffic = {'provider_up_bytes': 2, 'provider_down_bytes': 3, 'provider_metered_bytes': 5,
               'requests': 1, 'by_entity': {}}
    data = {'in_flight': cycle, 'grant_cycle_id': cycle, 'traffic_used': {'provider_metered_bytes': 5},
            'traffic_receipts': [{'cycle_id': cycle, 'child_cycle_id': child, 'traffic': deepcopy(traffic)}]}
    report = {'cycle_id': cycle, 'finished_at': '2026-11-05T03:00:00+00:00', 'scopes': [
        {'scope_id': 'scope', 'child_cycle_id': child, 'traffic': deepcopy(traffic),
         'error': 'unsettled lease: deadline'}]}
    path = tmp_path / ('portion-' + hashlib.sha256(cycle.encode()).hexdigest()[:24] + '.json')
    path.write_text(json.dumps(report))
    base = {'run_id': child, 'scope': 'scope', 'dag_id': 'dag_ingest_transfermarkt', 'lease_id': 'lease',
            'source': 'transfermarkt', 'event_version': 'paid-proxy-v2'}
    events = [{**base, 'event_id': '1', 'event_type': 'lease_created'},
              {**base, 'event_id': '2', 'event_type': 'bytes', 'direction': 'up', 'bytes': 2},
              {**base, 'event_id': '3', 'event_type': 'bytes', 'direction': 'down', 'bytes': 8},
              {**base, 'event_id': '4', 'event_type': 'lease_closed', 'total_bytes': 10}]
    return data, events, path


def recover(data, events, path):
    saved = []
    CurrentSettlement(path.parent, events=events).recover(
        data, scope_id='scope', directory=path.parent, now=datetime.now(timezone.utc),
        save=lambda: saved.append(deepcopy(data)))
    return saved


def test_closed_evidence_reconciles_tail_exactly_once(evidence):
    data, events, path = evidence
    assert len(recover(data, events, path)) == 1
    assert 'in_flight' not in data
    assert data['traffic_used']['provider_metered_bytes'] == 10
    receipt = data['traffic_receipts'][0]
    assert receipt['traffic']['provider_metered_bytes'] == 10
    assert receipt['traffic_before_reconciliation']['provider_metered_bytes'] == 5
    assert receipt['traffic']['by_entity']['lease_close']['provider_bytes'] == 5
    assert recover(data, events, path) == []
    assert data['traffic_used']['provider_metered_bytes'] == 10


@pytest.mark.parametrize('case', ['no_close','no_creation','drift','wrong_scope','wrong_child',
                                  'duplicate_close','bytes_after_close','missing_receipt','missing_report','open_new_lease'])
def test_unproven_evidence_never_clears_or_modifies_state(evidence, case):
    data, events, path = evidence
    if case == 'no_close': events.pop()
    if case == 'no_creation': events.pop(0)
    if case == 'drift': events[-1]['total_bytes'] = 11
    if case == 'wrong_scope': events[1]['scope'] = 'other'
    if case == 'wrong_child': events[1]['run_id'] = 'other'
    if case == 'duplicate_close': events.append({**events[-1], 'event_id': '5'})
    if case == 'bytes_after_close': events.append({**events[1], 'event_id': '5'})
    if case == 'missing_receipt': data['traffic_receipts'] = []
    if case == 'missing_report': path.unlink()
    if case == 'open_new_lease': events.append({**events[0], 'event_id': '5', 'lease_id': 'new'})
    original = deepcopy(data)
    with pytest.raises(CurrentSettlementError): recover(data, events, path)
    assert data == original


def test_archive_overlap_same_event_is_not_double_counted(evidence):
    data, events, path = evidence
    recover(data, events + deepcopy(events), path)
    assert data['traffic_used']['provider_metered_bytes'] == 10


def test_truncated_gateway_file_stays_blocked(evidence):
    data, events, path = evidence
    (path.parent/'paid_requests.jsonl').write_text('\n'.join(map(json.dumps,events))+'\n{"partial":')
    original = deepcopy(data)
    with pytest.raises(CurrentSettlementError):
        CurrentSettlement(path.parent).recover(data,scope_id='scope',directory=path.parent,
                                               now=datetime.now(timezone.utc),save=lambda:None)
    assert data == original


def test_settlement_has_own_reserve_after_paid_admission_deadline():
    provider = ProxyFilterLeaseProvider('http://gateway', control_token='x'*32,
                                       request_deadline_monotonic=100, settlement_deadline_monotonic=160,
                                       monotonic_fn=lambda:110)
    with pytest.raises(CurrentPortionDeadlineExceeded): provider._admission_timeout()
    assert provider._settlement_timeout() == 5


def test_reconciliation_timeout_never_changes_state(evidence):
    data, events, path = evidence
    original = deepcopy(data)
    with pytest.raises(CurrentSettlementError, match='deadline'):
        CurrentSettlement(path.parent,events=events).recover(
            data,scope_id='scope',directory=path.parent,now=datetime.now(timezone.utc),
            save=lambda:None,deadline=1,clock=lambda:2)
    assert data == original


from tests.unit.scripts.test_run_transfermarkt_current import offline


def test_runner_recovers_failed_close_before_next_paid_work(offline, monkeypatch):
    from dags.utils import transfermarkt_current_settlement as settlement
    factory = offline.factory
    def fail_close_factory(*args):
        scraper = factory(*args)
        close = scraper.close
        def failed_close():
            close()
            raise RuntimeError('injected settlement deadline')
        scraper.close = failed_close
        return scraper
    offline.factory = fail_close_factory
    report = offline.run('failed-close')
    assert any('unsettled lease' in entry.get('error','') for entry in report['scopes'])
    state = offline.state()
    sid, data = next(iter(state['scopes'].items()))
    assert data['in_flight'] == 'failed-close'
    # The real offline HTTP client has no paid provider; zero-byte final leases
    # model its gateway acknowledgement without issuing any real requests.
    events = []
    for n, entry in enumerate(report['scopes']):
        if 'traffic' not in entry:
            continue
        base={'run_id':entry['child_cycle_id'],'scope':sid,'dag_id':'dag_ingest_transfermarkt','lease_id':str(n),
              'source':'transfermarkt','event_version':'paid-proxy-v2'}
        events += [{**base,'event_id':str(n)+'a','event_type':'lease_created'},
                   {**base,'event_id':str(n)+'b','event_type':'lease_closed','total_bytes':0}]
    monkeypatch.setattr(settlement,'_events',lambda directory:iter(events))
    offline.factory = factory
    offline.clock.advance(4*3600)
    resumed = offline.run('after-close')
    assert not any('settlement' in entry.get('error','') for entry in resumed['scopes'])
    data=offline.state()['scopes'][sid]
    assert data['traffic_reconciliations'][0]['cycle_id']=='failed-close'
    assert 'in_flight' not in data


def test_durable_full_escrow_charge_recovers_without_claiming_exact_spend(evidence):
    data, events, path = evidence
    events[-1] = {**events[-1], 'event_type':'bytes', 'event_version':'paid-proxy-v2',
                  'source':'transfermarkt','direction':'down','host':'uncertain-read-ahead',
                  'bytes':65536,'lease_total_bytes':65546}
    recover(data,events,path)
    assert data['traffic_used']['provider_metered_bytes']==65546
    proof=data['traffic_receipts'][0]['settlement']
    assert proof['accounting_uncertain_lease_ids']==['lease']
    assert proof['counter_basis']=='conservatively_charged'


@pytest.mark.parametrize('case',['more_bytes','wrong_source','wrong_version','wrong_total','open_lease','wrong_direction'])
def test_conservative_terminal_must_be_final_and_exact(evidence,case):
    data,events,path=evidence
    events[-1]={**events[-1], 'event_type':'bytes', 'event_version':'paid-proxy-v2',
                'source':'transfermarkt','direction':'down','host':'uncertain-read-ahead',
                'bytes':65536,'lease_total_bytes':65546}
    if case=='more_bytes':events.append({**events[1],'event_id':'more'})
    if case=='wrong_source':events[-1]['source']='other'
    if case=='wrong_version':events[-1]['event_version']='unknown'
    if case=='wrong_total':events[-1]['lease_total_bytes']=10
    if case=='open_lease':events.append({**events[0],'event_id':'more','lease_id':'other'})
    if case=='wrong_direction':events[-1]['direction']='up'
    original=deepcopy(data)
    with pytest.raises(CurrentSettlementError):recover(data,events,path)
    assert data==original


@pytest.mark.parametrize('field,value', [('event_type', 'unknown_terminal_state'),
                                        ('source', 'unexpected'), ('event_version', 'paid-proxy-v999')])
def test_unknown_evidence_after_close_keeps_scope_blocked(evidence, field, value):
    data, events, path = evidence
    events.append({**events[-1], 'event_id': 'unknown', field: value})
    original = deepcopy(data)
    with pytest.raises(CurrentSettlementError, match='unsupported gateway event'):
        recover(data, events, path)
    assert data == original


def test_failed_save_replays_from_durable_state_without_double_charge(evidence):
    data, events, path = evidence
    durable = deepcopy(data)
    def failed_save():
        raise OSError('atomic write failed before replace')
    with pytest.raises(OSError):
        CurrentSettlement(path.parent, events=events).recover(
            data, scope_id='scope', directory=path.parent, now=datetime.now(timezone.utc), save=failed_save)
    assert durable['in_flight']
    recover(durable, events, path)
    assert durable['traffic_used']['provider_metered_bytes'] == 10
    assert recover(durable, events, path) == []
