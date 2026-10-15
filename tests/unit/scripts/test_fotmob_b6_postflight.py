"""Captured evidence qualification; no services or source requests."""
import copy
import json
from pathlib import Path

import pytest

from scripts import fotmob_b6_postflight as mod


@pytest.fixture
def inputs(tmp_path):
    fingerprints = {f"runtime/{n}.py": f"{n:064x}" for n in range(30)}
    protected = {"/protected/hold": {"exists": True, "sha256": "f" * 64}}
    manifest = {"target": mod.TARGET, "runtime_sha256": fingerprints,
                "scripts_sha256": {"fotmob-auto-deliver.sh": "a" * 64,
                                   "b6-fotmob-deliver.sh": "b" * 64}}
    baseline = {"checked_at": "2026-10-14T12:00:00+00:00", "protected": protected}
    reflog = tmp_path / 'reflog.txt'
    reflog.write_text(f"{mod.TARGET}|HEAD@{{2026-10-14T15:00:00+02:00}}|checkout: moving from ecb87ee3 to deploy/fotmob-b6-master\n")
    checkout = {"head": mod.TARGET, "actual_checkout_at": "2026-10-14T13:00:00Z",
                "checked_at": "2026-10-14T13:01:00Z", "evidence_path": str(reflog),
                "evidence_line": 1, "evidence_sha256": mod.sha(reflog.read_bytes())}
    probe = {"head": mod.TARGET, "clean": True, "runtime_sha256": fingerprints,
             "mounted_match": True, "scripts": manifest["scripts_sha256"], "protected": protected,
             "checked_at": "2026-10-14T13:10:01Z",
             "metadb": {"checked_at": "2026-10-14T13:10:00Z", "import_errors": 0,
                        "dags": [{"dag_id": d, "is_active": True,
                                  "last_parsed_time": "2026-10-14T13:02:00Z"} for d in sorted(mod.DAGS)]}}
    baseline['metadb'] = {'dags': [{'dag_id': d, 'is_paused': d not in {
        'dag_ingest_fotmob', 'dag_orchestrate_fotmob'}} for d in sorted(mod.DAGS)]}
    for dag in probe['metadb']['dags']:
        dag['is_paused'] = dag['dag_id'] not in {'dag_ingest_fotmob', 'dag_orchestrate_fotmob'}
    environment = {
        'containers': {name: {'id': letter * 64, 'started': '2026-10-14T10:00:00Z',
                              'image': 'sha256:' + letter * 64,
                              'mounts': [{'Type': 'bind', 'Source': '/captured/code',
                                          'Destination': '/opt/airflow/dags', 'RW': False}]}
                       for name, letter in [('fotmob-airflow-scheduler', 'a'), ('fotmob-airflow-metadb', 'b')]},
        'cron_sha256': 'c' * 64, 'ignore_sha256': 'd' * 64,
        '1185': 'Id=fotmob-1185-study.service\nActiveState=active\nId=fotmob-1185-pane-notify.service\nActiveState=active',
    }
    baseline.update(copy.deepcopy(environment))
    probe.update(copy.deepcopy(environment))
    report = json.loads((Path(__file__).parents[2] / 'fixtures/fotmob/b6_postflight/unavailable-report.json').read_text())
    run = {"run_id": 'fotmob_orchestrated__' + report['run_id'], "state": 'failed',
           "start_date": '2026-10-14T14:33:00Z', "end_date": '2026-10-14T14:34:00Z'}
    receipt = {"run": run, "checked_at": "2026-10-14T14:35:00Z", "report_line": 12,
               "log_sha256": "a" * 64,
               "log_path": f"/captured/logs/dag_id=dag_ingest_fotmob/run_id={run['run_id']}/task_id=scrape_fotmob_data/attempt=1.log"}
    receipt['report_sha256'] = 'c' * 64
    retained_line = tmp_path / 'report.logline'
    retained_line.write_text('[2026-10-14T14:33:04.000+0000] {subprocess.py:106} INFO - ' + json.dumps(report) + '\n')
    receipt['retained_report_line_path'] = str(retained_line)
    receipt['retained_report_line_sha256'] = mod.sha(retained_line.read_bytes())
    return copy.deepcopy((manifest, baseline, probe, checkout, report, receipt, [run]))


def report_result(data, *, sync_line=True):
    if sync_line:
        retained = Path(data[5]['retained_report_line_path'])
        retained.write_text('[2026-10-14T14:33:04.000+0000] {subprocess.py:106} INFO - ' + json.dumps(data[4]) + '\n')
        data[5]['retained_report_line_sha256'] = mod.sha(retained.read_bytes())
    return mod.qualify_report(data[4], data[5], data[6], mod.instant(data[3]['actual_checkout_at']), 'c' * 64)


def test_exact_target_and_offset_checkout_pass(inputs):
    result = mod.qualify_delivery(*inputs[:4])
    assert result['actual_checkout_at'] == '2026-10-14T13:00:00+00:00'
    assert result['runtime_fingerprints'] == 30


@pytest.mark.parametrize('mutation,match', [
    (lambda x: x[2].update(head='ecb87ee399fd6bdc8bdfe0344694b74a06ff0a39'), 'runtime head'),
    (lambda x: x[0].update(target='f' * 40), 'exact PR1723'),
    (lambda x: x[2].update(clean=False), 'clean'),
    (lambda x: x[0]['runtime_sha256'].pop('runtime/29.py'), '30 runtime'),
    (lambda x: x[2]['runtime_sha256'].update({'runtime/0.py': 'e' * 64}), 'fingerprints'),
    (lambda x: x[2].update(mounted_match=False), 'mounted'),
    (lambda x: x[2]['scripts'].update({'b6-fotmob-deliver.sh': 'e' * 64}), 'script'),
    (lambda x: x[2]['protected'].clear(), 'protected'),
    (lambda x: x[2]['metadb'].update(import_errors=1), 'import'),
    (lambda x: x[2]['metadb']['dags'].pop(), 'six DAG'),
    (lambda x: x[2]['metadb']['dags'][0].update(last_parsed_time='2026-10-14T13:00:00Z'), 'strictly'),
    (lambda x: x[3].update(actual_checkout_at='2026-10-14T13:05:00Z'), 'actual Git'),
    (lambda x: x[3].update(evidence_sha256='0' * 64), 'SHA256'),
    (lambda x: x[3].update(evidence_line=2), 'range'),
])
def test_delivery_refuses_bad_facts(inputs, mutation, match):
    # Break shared dictionary aliases so a tampered probe cannot rewrite its baseline.
    data = [copy.deepcopy(x) for x in inputs]
    mutation(data)
    with pytest.raises(ValueError, match=match):
        mod.qualify_delivery(*data[:4])


def test_reflog_message_must_be_checkout(inputs):
    p = Path(inputs[3]['evidence_path'])
    p.write_text(p.read_text().replace('checkout: moving from', 'reset: moving from'))
    inputs[3]['evidence_sha256'] = mod.sha(p.read_bytes())
    with pytest.raises(ValueError, match='checkout record'):
        mod.qualify_delivery(*inputs[:4])


def test_diagnostic_pass_preserves_failed_runner_and_six_obligations(inputs):
    result = report_result(inputs)
    assert result['status'] == 'PASS'
    assert result['runner']['complete'] is False
    assert result['runner']['errors'] == inputs[4]['errors']
    assert result['diagnostics']['calendar_unavailable_count'] == 6
    assert result['six_obligations']['denominator'] == sorted(mod.OBLIGATIONS)
    assert result['runtime_full_success_1699_1711'] is None
    assert result['source_recovery_proven'] is False


@pytest.mark.parametrize('mutation,match', [
    (lambda x: x[4].update(mode='transfers'), 'seasonal mode'),
    (lambda x: x[5]['run'].update(run_id='manual__test'), 'natural DagRun'),
    (lambda x: x[4].update(started_at='2026-10-14T12:00:00'), 'after actual'),
    (lambda x: x[5].update(report_sha256='d' * 64), 'SHA256'),
    (lambda x: x[5].pop('log_sha256'), 'log SHA256'),
    (lambda x: x[4]['selection'].update(calendar_unavailable_scopes=[]), 'diagnostic list'),
    (lambda x: x[4]['selection']['calendar_unavailable_scopes'].append('999=2026'), 'contradict'),
    (lambda x: x[4]['selection'].update(planned_scope_count=5), 'count'),
    (lambda x: x[4].update(complete=True), 'cannot be complete'),
    (lambda x: x[4]['operations'][0]['metadata'].update(obligation_scopes=5), 'obligation'),
    (lambda x: x[4]['selection']['scope_attempts'][0].update(next_retry_at='2026-10-13T00:00:00Z'), 'attempt operation|retry evidence'),
    (lambda x: x[4]['selection']['scope_attempts'][0].update(attempt_identities=['other']), 'attempt operation|identity'),
])
def test_report_refuses_inconsistent_evidence(inputs, mutation, match):
    mutation(inputs)
    with pytest.raises(ValueError, match=match):
        report_result(inputs)


@pytest.mark.parametrize('key,value', [('content_hash', ''), ('raw_uri', ''), ('target_key', 'd' * 64),
                                       ('url', 'https://www.fotmob.com/api/data/leagues?id=999&season=2025'),
                                       ('stale', True), ('http_status', 500)])
def test_provenance_required_for_each_attempt(inputs, key, value):
    bundle = next(o for o in inputs[4]['operations'] if o['entity'] == 'season_bundle')
    bundle['metadata']['calendar_unavailability'][key] = value
    with pytest.raises(ValueError, match='provenance'):
        report_result(inputs)


def test_earlier_unreported_run_cannot_be_skipped(inputs):
    inputs[6].append({'run_id': 'fotmob_orchestrated__other', 'start_date': '2026-10-14T14:00:00Z'})
    with pytest.raises(ValueError, match='first captured'):
        report_result(inputs)


def test_green_run_without_six_does_not_prove_recovery(inputs):
    report = inputs[4]
    report['status'] = 'success'
    report['complete'] = True
    report['errors'] = []
    report['operations'] = [o for o in report['operations'] if o['entity'] in {'season_work_plan', 'scope_attempts'}]
    report['operations'][1]['metadata']['outcomes'] = []
    report['operations'][0]['metadata']['cooling_source_gap_scopes'] = sorted(mod.OBLIGATIONS)
    report['selection'].update(planned_scopes=[], completed_scopes=[], calendar_unavailable_scopes=[],
                               planned_scope_count=0, scope_attempts=[])
    result = report_result(inputs)
    assert result['diagnostics']['calendar_unavailable_count'] == 0
    assert result['six_obligations']['unplanned'] == sorted(mod.OBLIGATIONS)
    assert result['runner']['obligation_scopes'] == 6
    assert result['source_recovery_proven'] is False


def test_cli_preserves_first_fail_and_reports_missing_full_report(inputs, tmp_path):
    paths = {}
    for key, payload in zip(('manifest', 'baseline-probe', 'probe', 'checkout-evidence', 'report', 'report-receipt', 'natural-runs'), inputs):
        path = tmp_path / (key + '.json')
        path.write_text(json.dumps(payload))
        paths[key] = path
    # A real runner exception report may omit selection; this is not a passing full report.
    paths['report'].write_text(json.dumps({'run_id': inputs[4]['run_id'], 'mode': 'refresh', 'status': 'failed', 'errors': ['identity mismatch']}))
    receipt = json.loads(paths['report-receipt'].read_text())
    receipt['report_sha256'] = mod.sha(paths['report'].read_bytes())
    paths['report-receipt'].write_text(json.dumps(receipt))
    output = tmp_path / 'first-fail.json'
    argv = [a for key, path in paths.items() for a in ('--' + key, str(path))] + ['--output', str(output)]
    assert mod.main(argv) == 1
    saved = output.read_bytes()
    assert json.loads(saved)['captured_runner']['errors'] == ['identity mismatch']
    assert mod.main(argv) == 2
    assert output.read_bytes() == saved


def test_retained_line_must_match_hash_and_report(inputs):
    line = Path(inputs[5]['retained_report_line_path'])
    line.write_text(line.read_text().replace('"complete": false', '"complete": true'))
    with pytest.raises(ValueError, match='retained report line SHA256'):
        report_result(inputs, sync_line=False)
    inputs[5]['retained_report_line_sha256'] = mod.sha(line.read_bytes())
    with pytest.raises(ValueError, match='log line report differs'):
        report_result(inputs, sync_line=False)


def test_retained_line_requires_airflow_prefix(inputs):
    line = Path(inputs[5]['retained_report_line_path'])
    line.write_text(json.dumps(inputs[4]) + '\n')
    inputs[5]['retained_report_line_sha256'] = mod.sha(line.read_bytes())
    with pytest.raises(ValueError, match='invalid retained Airflow'):
        report_result(inputs, sync_line=False)


def replace_contract(report, contract):
    """Keep dependent report facts consistent when exercising another catalog."""
    selection = report['selection']
    selection['catalog_contract'] = contract
    selection['scope_plan_signature'] = contract['plan_signature']
    selection['entities'] = contract['entities']
    for operation in report['operations']:
        if operation['entity'] == 'season_work_plan':
            operation['metadata']['scope_plan_signature'] = contract['plan_signature']
            operation['metadata']['obligation_scopes'] = contract['scope_count']
        elif operation['entity'] == 'scope_attempts':
            for attempt in operation['metadata']['outcomes']:
                attempt['plan_signature'] = contract['plan_signature']
    for attempt in selection['scope_attempts']:
        attempt['plan_signature'] = contract['plan_signature']


def test_six_obligations_remain_when_cooldown_or_unplanned(inputs):
    from scrapers.fotmob.catalog_contract import build_catalog_contract

    original = inputs[4]['selection']['catalog_contract']
    contract = build_catalog_contract(
        catalog_batch_id=original['catalog_batch_id'], catalog_content_hash=original['catalog_content_hash'],
        classifier_version=original['classifier_version'], parser_version=original['parser_version'],
        entities=original['entities'], entity_policy=original['entity_policy'],
        included_ids=original['included_ids'], scopes=[s for s in original['scopes'] if s != '300=2025'],
    ).as_dict()
    replace_contract(inputs[4], contract)
    with pytest.raises(ValueError, match='disappeared from denominator'):
        report_result(inputs)


def test_known_transfers_before_seasonal_report_are_not_eligible(inputs):
    inputs[6].append({'run_id': 'fotmob_orchestrated__transfer', 'start_date': '2026-10-14T14:00:00Z', 'mode': 'transfers'})
    assert report_result(inputs)['status'] == 'PASS'


@pytest.mark.parametrize('with_report', [False, True])
def test_cli_local_success(inputs, tmp_path, with_report):
    paths = {}
    for key, payload in zip(('manifest', 'baseline-probe', 'probe', 'checkout-evidence', 'report', 'report-receipt', 'natural-runs'), inputs):
        path = tmp_path / (key + '.json')
        path.write_text(json.dumps(payload))
        paths[key] = path
    receipt = json.loads(paths['report-receipt'].read_text())
    receipt['report_sha256'] = mod.sha(paths['report'].read_bytes())
    paths['report-receipt'].write_text(json.dumps(receipt))
    if not with_report:
        for key in ('report', 'report-receipt', 'natural-runs'):
            del paths[key]
    output = tmp_path / 'qualified.json'
    argv = [a for key, path in paths.items() for a in ('--' + key, str(path))] + ['--output', str(output)]
    assert mod.main(argv) == 0
    result = json.loads(output.read_text())
    assert result['delivery']['status'] == 'PASS'
    assert result['report']['status'] == ('PASS' if with_report else 'NOT_CHECKED')
    assert all(len(v['sha256']) == 64 for v in result['inputs'].values())


@pytest.mark.parametrize('mutation', [
    lambda c: c.update(scope_count=999),
    lambda c: c.update(scope_sha256='0' * 64),
    lambda c: c.update(included_ids=[1], included_count=1),
    lambda c: c.update(plan_signature='corrupt'),
    lambda c: c.pop('schema'),
])
def test_catalog_contract_corruption_is_rejected(inputs, mutation):
    mutation(inputs[4]['selection']['catalog_contract'])
    with pytest.raises(ValueError, match='schema|contract|scope|included'):
        report_result(inputs)


def test_scopes_only_contract_is_rejected(inputs):
    inputs[4]['selection']['catalog_contract'] = {'scopes': sorted(mod.OBLIGATIONS)}
    with pytest.raises(ValueError, match='schema'):
        report_result(inputs)


@pytest.mark.parametrize('field,value', [('scope_plan_signature', 'corrupt'), ('entities', ['season'])])
def test_catalog_contract_must_match_selection(inputs, field, value):
    inputs[4]['selection'][field] = value
    with pytest.raises(ValueError, match='contract.*selection|selection.*contract'):
        report_result(inputs)


@pytest.mark.parametrize('other_first', [False, True])
def test_first_dagrun_tie_is_ambiguous_in_both_orders(inputs, other_first):
    other = dict(inputs[6][0], run_id='fotmob_orchestrated__another_unknown')
    inputs[6].insert(0 if other_first else 1, other)
    with pytest.raises(ValueError, match='ambiguous first'):
        report_result(inputs)


def test_full_catalog_denominator_is_retained(inputs):
    from scrapers.fotmob.catalog_contract import build_catalog_contract

    original = inputs[4]['selection']['catalog_contract']
    contract = build_catalog_contract(
        catalog_batch_id=original['catalog_batch_id'],
        catalog_content_hash=original['catalog_content_hash'],
        classifier_version=original['classifier_version'],
        parser_version=original['parser_version'],
        entities=original['entities'], entity_policy=original['entity_policy'],
        included_ids=original['included_ids'] + [47],
        scopes=original['scopes'] + ['47=2025/2026'],
    ).as_dict()
    replace_contract(inputs[4], contract)
    result = report_result(inputs)
    assert result['runner']['obligation_scopes'] == 7
    assert result['runner']['catalog_contract'] == contract


@pytest.mark.parametrize('field,value', [
    ('id', 'e' * 64), ('started', '2026-10-14T11:00:00Z'), ('image', 'sha256:' + 'e' * 64),
    ('mounts', [{'Type': 'bind', 'Source': '/other', 'Destination': '/opt/airflow/dags', 'RW': True}]),
])
def test_preserved_container_fields_cannot_change(inputs, field, value):
    inputs[2]['containers']['fotmob-airflow-scheduler'][field] = value
    with pytest.raises(ValueError, match='preserved|containers'):
        mod.qualify_delivery(*inputs[:4])


@pytest.mark.parametrize('field,value', [('cron_sha256', 'e' * 64), ('ignore_sha256', 'e' * 64),
                                        ('1185', 'ActiveState=inactive')])
def test_preserved_host_fields_cannot_change(inputs, field, value):
    inputs[2][field] = value
    with pytest.raises(ValueError, match='preserved'):
        mod.qualify_delivery(*inputs[:4])


def test_preserved_dag_pause_cannot_change(inputs):
    inputs[2]['metadb']['dags'][0]['is_paused'] = not inputs[2]['metadb']['dags'][0]['is_paused']
    with pytest.raises(ValueError, match='pause'):
        mod.qualify_delivery(*inputs[:4])


@pytest.mark.parametrize('capture_index', [1, 2])
@pytest.mark.parametrize('field', ['containers', 'cron_sha256', 'ignore_sha256', '1185'])
def test_preserved_host_fields_must_be_present(inputs, capture_index, field):
    del inputs[capture_index][field]
    with pytest.raises(ValueError, match='preserved|containers'):
        mod.qualify_delivery(*inputs[:4])


@pytest.mark.parametrize('capture_index', [1, 2])
@pytest.mark.parametrize('field,value', [('containers', []), ('cron_sha256', False), ('ignore_sha256', ''), ('1185', {})])
def test_preserved_host_fields_must_be_valid(inputs, capture_index, field, value):
    inputs[capture_index][field] = value
    with pytest.raises(ValueError, match='preserved|containers'):
        mod.qualify_delivery(*inputs[:4])


@pytest.mark.parametrize('capture_index', [1, 2])
@pytest.mark.parametrize('field,value', [('id', ''), ('started', 'naive'), ('image', None), ('mounts', [])])
def test_preserved_container_fields_must_be_valid(inputs, capture_index, field, value):
    inputs[capture_index]['containers']['fotmob-airflow-scheduler'][field] = value
    with pytest.raises(ValueError):
        mod.qualify_delivery(*inputs[:4])


@pytest.mark.parametrize('capture_index', [1, 2])
@pytest.mark.parametrize('value', [None, 'false', 0])
def test_preserved_dag_pause_must_be_boolean(inputs, capture_index, value):
    inputs[capture_index]['metadb']['dags'][0]['is_paused'] = value
    with pytest.raises(ValueError, match='pause'):
        mod.qualify_delivery(*inputs[:4])


def test_natural_writer_and_automatic_markers_can_change(inputs):
    inputs[1]['writers'] = []
    inputs[2]['writers'] = ['natural-writer-pid']
    inputs[1]['markers'] = {'fotmob-b6-inflight': {'exists': True}}
    inputs[2]['markers'] = {'fotmob-b6-accepted': {'exists': True}}
    assert mod.qualify_delivery(*inputs[:4])['status'] == 'PASS'
