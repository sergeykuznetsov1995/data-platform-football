from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

import pytest

from scripts.acceptance import AcceptanceError, Store


START = datetime(2026, 10, 1, tzinfo=timezone.utc)


def stamp(day=0):
    return (START + timedelta(days=day)).isoformat()


@pytest.fixture
def registry(tmp_path):
    proof = tmp_path / 'measured.json'
    proof.write_text('{"sample_count": 10, "successful": 10}')
    return Store(tmp_path / 'state', evidence_roots=[tmp_path]), str(proof)


def spec(key='fotmob-1288', required=3, kind='days'):
    return {'id': key, 'source': 'fotmob', 'issue': int(key.split('-')[-1]),
            'title': 'Контроль данных', 'issue_body_sha256': 'a' * 64,
            'definition_confirmed': True, 'started_at': stamp(),
            'deployed_revision': 'v1', 'criteria': [
                {'id': 'data', 'title': 'Данные в срок', 'kind': kind,
                 'required': required, 'min_samples': 1,
                 **({'period_seconds': 86400} if kind == 'days' else {})},
                {'id': 'cleanup', 'title': 'Очистка завершена', 'kind': 'check',
                 'required': 1, 'min_samples': 1}]}


def observation(proof, index=0, criterion='data', status='pass', series=1):
    return {'series': series, 'criterion': criterion, 'key': f'run-{index}',
            'ordinal': index, 'start': stamp(index), 'end': stamp(index + 1),
            'checked_at': stamp(index + 1), 'status': status, 'samples': 10,
            'reason': 'Проверено по измерению', 'evidence': [proof]}


def progress(store, key='fotmob-1288', now=stamp(3)):
    return next(x for x in store.snapshot(now=now)['acceptances'] if x['id'] == key)


def test_independent_tasks_handoff_and_every_criterion(registry):
    store, proof = registry
    store.register(spec(), now=stamp())
    store.register(spec('fotmob-1289', 7), now=stamp())
    for i in range(3):
        store.observe('fotmob-1288', observation(proof, i), now=stamp(3))
    store.handoff('fotmob', {'path': proof, 'checked_at': stamp(3),
                             'summary': 'Начата третья задача', 'expected_revision': 0}, now=stamp(3))
    record = progress(store)
    assert record['criteria'][0]['progress'] == 3
    assert record['status'] != 'ready'
    store.observe('fotmob-1288', observation(proof, 0, 'cleanup'), now=stamp(3))
    assert progress(store)['status'] == 'ready'
    assert progress(store, 'fotmob-1289')['status'] != 'ready'
    assert len(store.snapshot(now=stamp(3))['acceptances']) == 2


def test_unknown_can_recover_but_missing_day_is_not_skipped(registry):
    store, proof = registry
    store.register(spec(), now=stamp())
    for index in [0, 2]:
        store.observe('fotmob-1288', observation(proof, index), now=stamp(3))
    assert progress(store)['criteria'][0]['progress'] == 1
    missing = observation(proof, 1, status='unknown')
    missing['evidence'] = []
    store.observe('fotmob-1288', missing, now=stamp(3))
    assert progress(store)['criteria'][0]['progress'] == 1
    store.observe('fotmob-1288', observation(proof, 1), now=stamp(3))
    assert progress(store)['criteria'][0]['progress'] == 3
    assert progress(store, now=stamp(4))['criteria'][0]['progress'] == 0


def test_one_successful_day_does_not_satisfy_three_day_target(registry):
    store, proof = registry
    store.register(spec(required=3), now=stamp())
    for criterion in ['data', 'cleanup']:
        store.observe('fotmob-1288', observation(proof, criterion=criterion), now=stamp(1))
    assert progress(store, now=stamp(1))['status'] != 'ready'
    with pytest.raises(AcceptanceError, match='not all'):
        store.prepare_close('fotmob-1288', 'a' * 64, now=stamp(1))


def test_failed_day_cannot_be_silently_repainted(registry):
    store, proof = registry
    store.register(spec(), now=stamp())
    store.observe('fotmob-1288', observation(proof, status='fail'), now=stamp(1))
    with pytest.raises(AcceptanceError, match='failed'):
        store.observe('fotmob-1288', observation(proof), now=stamp(1))
    assert progress(store, now=stamp(1))['status'] == 'failed'


def test_affected_change_keeps_history_and_other_acceptances(registry):
    store, proof = registry
    for key in ['fotmob-1288', 'fotmob-1289']:
        store.register(spec(key), now=stamp())
        store.observe(key, observation(proof), now=stamp(1))
    store.change('fotmob-1288', {'expected_series': 1, 'impact': 'affected',
        'expected_revision': store.show('fotmob-1288')['revision'],
        'deployed_revision': 'v2', 'started_at': stamp(1),
        'reason': 'Изменён путь записи', 'evidence': [proof]}, now=stamp(1))
    assert progress(store, now=stamp(1))['series'] == 2
    assert progress(store, 'fotmob-1289', stamp(1))['criteria'][0]['progress'] == 1
    assert any(x['kind'] == 'observation' and x['series'] == 1
               for x in store.history('fotmob-1288'))
    with pytest.raises(AcceptanceError, match='series'):
        store.observe('fotmob-1288', observation(proof), now=stamp(2))


def test_unaffected_change_preserves_progress_unknown_blocks_completion(registry):
    store, proof = registry
    store.register(spec(required=1), now=stamp())
    for criterion in ['data', 'cleanup']:
        store.observe('fotmob-1288', observation(proof, criterion=criterion), now=stamp(1))
    decision = {'expected_series': 1, 'impact': 'unaffected',
                'expected_revision': store.show('fotmob-1288')['revision'],
                'deployed_revision': 'v2', 'reason': 'Проверены зависимости', 'evidence': [proof]}
    store.change('fotmob-1288', decision, now=stamp(1))
    assert progress(store, now=stamp(1))['status'] == 'ready'
    decision.update(impact='unknown', deployed_revision='v3',
                    expected_revision=store.show('fotmob-1288')['revision'])
    store.change('fotmob-1288', decision, now=stamp(1))
    assert progress(store, now=stamp(1))['status'] == 'on_hold'


@pytest.mark.parametrize('change', [
    {'samples': 0}, {'evidence': []}, {'end': stamp(2)},
    {'start': stamp(-1)}, {'checked_at': stamp(-1)}, {'ordinal': True},
])
def test_success_requires_complete_mature_interval_and_evidence(registry, change):
    store, proof = registry
    store.register(spec(), now=stamp())
    value = observation(proof)
    value.update(change)
    with pytest.raises(AcceptanceError):
        store.observe('fotmob-1288', value, now=stamp(1))


def test_record_idempotency_distinct_runs_and_immutable_evidence(registry):
    store, proof = registry
    store.register(spec(required=2, kind='runs'), now=stamp())
    value = observation(proof)
    store.observe('fotmob-1288', value, now=stamp(1))
    rev = store.snapshot(now=stamp(1))['revision']
    store.observe('fotmob-1288', value, now=stamp(1))
    assert store.snapshot(now=stamp(1))['revision'] == rev
    reused = observation(proof, 1)
    reused['key'] = value['key']
    with pytest.raises(AcceptanceError, match='key'):
        store.observe('fotmob-1288', reused, now=stamp(2))
    from pathlib import Path
    Path(proof).write_text('later content')
    event = [e for e in store.history('fotmob-1288') if e['kind'] == 'observation'][0]
    saved = Path(event['payload']['evidence'][0]['snapshot'])
    assert saved.read_text() == '{"sample_count": 10, "successful": 10}'


def test_definition_is_immutable_and_unconfirmed_never_ready(registry):
    store, _ = registry
    item = spec()
    item['definition_confirmed'] = False
    store.register(item, now=stamp())
    changed = copy.deepcopy(item)
    changed['criteria'][0]['required'] = 1
    with pytest.raises(AcceptanceError, match='definition'):
        store.register(changed, now=stamp())
    assert progress(store)['status'] == 'draft'


def test_close_requires_matching_issue_and_all_criteria(registry):
    store, proof = registry
    store.register(spec(required=1), now=stamp())
    for criterion in ['data', 'cleanup']:
        store.observe('fotmob-1288', observation(proof, criterion=criterion), now=stamp(1))
    ready = progress(store, now=stamp(1))
    with pytest.raises(AcceptanceError, match='definition'):
        store.prepare_close('fotmob-1288', 'b' * 64, now=stamp(1))
    plan = store.prepare_close('fotmob-1288', 'a' * 64, now=stamp(1))
    assert plan['issue'] == 1288
    assert plan['acceptance_revision'] == ready['revision']
    store.confirm_closed('fotmob-1288', plan['token'],
        {'issue': 1288, 'state': 'CLOSED', 'body_sha256': 'a' * 64,
         'report_url': 'https://github.com/sergeykuznetsov1995/data-platform-football/issues/1288#issuecomment-1'}, now=stamp(1))
    assert progress(store, now=stamp(1))['status'] == 'closed'


def test_explicit_definition_change_resets_progress_with_history(registry):
    store, proof = registry
    store.register(spec(), now=stamp())
    store.observe('fotmob-1288', observation(proof), now=stamp(1))
    new = spec()
    new.update(issue_body_sha256='b' * 64, started_at=stamp(1))
    store.revise('fotmob-1288', {'spec': new, 'expected_revision': store.show('fotmob-1288')['revision'],
        'reason': 'Новый утверждённый критерий', 'evidence': [proof]}, now=stamp(1))
    record = progress(store, now=stamp(1))
    assert record['series'] == 2 and record['criteria'][0]['progress'] == 0
    assert any(e['kind'] == 'observation' for e in store.history('fotmob-1288'))


def test_source_close_hold_survives_successful_results(registry):
    store, proof = registry
    definition = spec(required=1)
    definition['metadata'] = {'close_hold': True}
    store.register(definition, now=stamp())
    for criterion in ['data', 'cleanup']:
        store.observe('fotmob-1288', observation(proof, criterion=criterion), now=stamp(1))
    assert progress(store, now=stamp(1))['status'] == 'on_hold'
    with pytest.raises(AcceptanceError):
        store.prepare_close('fotmob-1288', 'a' * 64, now=stamp(1))


def test_pr_acceptance_does_not_close_related_issue(registry):
    store, proof = registry
    definition = spec(required=1)
    definition.update(id='fotmob-pr-1652', issue=None, reference_kind='pull', pull_request=1652,
        reference_url='https://github.com/sergeykuznetsov1995/data-platform-football/pull/1652',
        auto_close_issue=False)
    store.register(definition, now=stamp())
    for criterion in ['data', 'cleanup']:
        store.observe(definition['id'], observation(proof, criterion=criterion), now=stamp(1))
    plan = store.prepare_close(definition['id'], 'a' * 64, now=stamp(1))
    assert plan['auto_close_issue'] is False and plan['issue'] is None
    store.confirm_closed(definition['id'], plan['token'],
        {'state':'ACCEPTED', 'body_sha256':'a' * 64, 'evidence':[proof]}, now=stamp(1))
    assert progress(store, definition['id'], stamp(1))['status'] == 'closed'


def test_concurrent_writes_preserve_distinct_tasks(registry):
    store, proof = registry
    keys = [f'fotmob-{2000+i}' for i in range(8)]
    for key in keys:
        store.register(spec(key), now=stamp())
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(lambda key: store.observe(key, observation(proof), now=stamp(1)), keys))
    rows = store.snapshot(now=stamp(1))['acceptances']
    assert len(rows) == 8 and all(r['criteria'][0]['progress'] == 1 for r in rows)


def test_closed_progress_is_frozen_and_receipt_can_be_retried(registry):
    store, proof = registry
    store.register(spec(required=1), now=stamp())
    for criterion in ['data', 'cleanup']:
        store.observe('fotmob-1288', observation(proof, criterion=criterion), now=stamp(1))
    plan = store.prepare_close('fotmob-1288', 'a' * 64, now=stamp(1))
    receipt = {'issue': 1288, 'state': 'CLOSED', 'body_sha256': 'a' * 64,
               'report_url': plan['issue_url'] + '#issuecomment-123'}
    closed = store.confirm_closed('fotmob-1288', plan['token'], receipt, now=stamp(1))
    retry = store.confirm_closed('fotmob-1288', plan['token'], receipt, now=stamp(4))
    assert retry['revision'] == closed['revision']
    assert progress(store, now=stamp(4))['criteria'][0]['progress'] == 1
    assert all(d['status'] == 'pass' for d in progress(store, now=stamp(4))['days'])


def test_handoff_updates_next_step_with_same_document(registry):
    store, proof = registry
    value = {'path': proof, 'summary': 'Продолжается приёмка', 'checked_at': stamp(),
             'next_step': 'Проверить данные', 'expected_revision': 0}
    prior = store.handoff('fotmob', value, now=stamp())
    value['next_step'] = 'Проверить журнал'
    value['expected_revision'] = prior['revision']
    store.handoff('fotmob', value, now=stamp(1))
    assert store.snapshot(now=stamp(1))['handoffs']['fotmob']['next_step'] == 'Проверить журнал'


def test_stale_impact_cannot_release_newer_hold(registry):
    store, proof = registry
    record = store.register(spec(required=1), now=stamp())
    decision = {'expected_series': 1, 'expected_revision': record['revision'],
                'impact': 'unknown', 'deployed_revision': 'v3',
                'reason': 'Новое изменение', 'evidence': [proof]}
    store.change('fotmob-1288', decision, now=stamp(1))
    decision.update(impact='unaffected', deployed_revision='v2')
    with pytest.raises(AcceptanceError, match='stale impact'):
        store.change('fotmob-1288', decision, now=stamp(1))
    assert store.show('fotmob-1288')['deployed_revision'] == 'v3'
    assert progress(store, now=stamp(1))['status'] == 'on_hold'


def test_due_missing_interval_is_not_postponed_by_export(registry):
    store, proof = registry
    store.register(spec(), now=stamp())
    assert progress(store, now=stamp(3))['next_check_at'] == stamp(1)
    store.observe('fotmob-1288', observation(proof), now=stamp(3))
    assert progress(store, now=stamp(3))['next_check_at'] == stamp(2)
    for index in [1, 2]:
        store.observe('fotmob-1288', observation(proof, index), now=stamp(3))
    assert progress(store, now=stamp(3))['next_check_at'] == stamp(4)


def test_stale_handoff_same_fact_time_cannot_erase_newer_summary(registry):
    store, proof = registry
    value = {'path': proof, 'summary': 'Исходное состояние', 'checked_at': stamp(),
             'expected_revision': 0}
    initial = store.handoff('fotmob', value, now=stamp())
    value.update(summary='Новое состояние', expected_revision=initial['revision'])
    latest = store.handoff('fotmob', value, now=stamp(1))
    assert store.handoff('fotmob', value, now=stamp(1))['revision'] == latest['revision']
    value['summary'] = 'Запоздавшее старое состояние'
    with pytest.raises(AcceptanceError, match='stale handoff revision'):
        store.handoff('fotmob', value, now=stamp(2))
    assert store.snapshot(now=stamp(2))['handoffs']['fotmob']['summary'] == 'Новое состояние'
