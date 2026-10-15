"""Offline only: no HTTP, SQL, daemon, scheduler or production fixture."""
import copy
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scrapers.transfermarkt.offline_acceptance import InvalidEvidence, digest, read_jsonl, validate
from scrapers.transfermarkt.offline_harness import fixture, local_exercises, write_fixture


@pytest.fixture(scope='module')
def complete():
    return fixture()


def row(events, kind, *, window=None):
    return next(r for r in events if r['kind'] == kind and (window is None or r['window'] == window))


def subset(manifest, events, roles):
    m = copy.deepcopy(manifest)
    m['windows'] = [w for w in m['windows'] if w['role'] in roles]
    e = [copy.deepcopy(r) for r in events if r['window'] in roles]
    for i, r in enumerate(e, 1):
        r['producer_sequence'] = i
    m['producer']['sequence_end'] = len(e)
    return m, e


def test_complete_synthetic_shape_never_becomes_runtime_qualification(complete):
    m, e = complete
    report = validate(m, e, expected_manifest_sha256=digest(m))
    assert report['artifact_status'] == 'VALID', report['issues']
    assert all(c['artifact_status'] == 'VALID' for c in report['criteria'].values())
    assert report['producer_status'] == 'PINNED_OFFLINE_ARTIFACT'
    assert report['runtime_status'] == 'UNKNOWN'
    assert report['actual_acceptance_start'] is None and report['runtime_eligible'] is False
    assert report['windows']['canary1']['throughput_ratio'] == 2
    assert report['windows']['history']['all_attempts'] == 7
    assert report['windows']['history']['lease_count'] == 3
    assert report['windows']['baseline']['workload_denominator'] == 20
    assert len(report['windows']['baseline']['unfinished_work']) == 10


@pytest.mark.parametrize('role', ['writer', 'history'])
def test_independent_writer_history_do_not_require_missing_canary(complete, role):
    m, e = subset(*complete, {role})
    report = validate(m, e, expected_manifest_sha256=digest(m))
    assert report['artifact_status'] == 'VALID', report['issues']
    assert report['criteria'][role]['artifact_status'] == 'VALID'
    assert report['criteria']['canary']['artifact_status'] == 'UNKNOWN'


def test_absent_external_pin_keeps_producer_unknown(complete):
    m, e = subset(*complete, {'writer'})
    report = validate(m, e)
    assert report['producer_status'] == 'UNKNOWN'
    assert any('No external pinned manifest' in b for b in report['blockers'])


def test_wrong_external_pin_rejects_self_rewritten_inventory(complete):
    m, e = subset(*complete, {'writer'})
    before = digest(m)
    m['producer']['id'] = 'replacement'
    assert 'external pinned manifest digest' in validate(m, e, expected_manifest_sha256=before)['issues'][0]


@pytest.mark.parametrize('change,expected', [
    ('coverage_gap', 'coverage gap'), ('coverage_counters', 'coverage counters'),
    ('missing_query', 'inventory differs'), ('missing_attempt', 'producer sequence'),
    ('timeout_exact2pct', 'strictly<2%'), ('memory_swap', 'memory limit/swap'),
    ('zero_rss', 'invalid number'), ('bad_clock', 'UTC'),
    ('unknown_outcome', 'unknown attempt'), ('missing_provider_bytes', 'missing/unknown'),
    ('bad_workload_digest', 'workload digest'), ('duplicate_event', 'producer sequence'),
    ('wrong_streams', '1vs2TOTAL'), ('short_window', 'full24h'),
    ('writer_lock', 'shared lock'), ('writer_duplicate', 'duplicate'),
    ('writer_clock', 'clock drift'), ('missing_writer_sample', 'writer/maintenance'),
    ('table_drift', 'interval table'), ('unclosed_lease', 'strictly<1%'),
    ('lease_grant_drift', 'grant identity'), ('portion_too_long', '45min'),
    ('task_outside', 'task outside'), ('batch_dq', 'remainder/DQ'),
    ('current_wait', 'current waiting'), ('transport_extra_probe', 'exactly one'),
    ('transport_platform', 'not transport'), ('transport_unlinked', 'exact attempt'),
    ('season_disappearance', 'not season transition'), ('unknown_summary_flag', 'missing/unknown'),
    ('compaction_bounds', 'bounds exceeded'), ('anchor_drift', 'anchors changed'),
])
def test_missing_unknown_incomplete_or_violated_evidence_fails_closed(complete, change, expected):
    m, e = copy.deepcopy(complete)
    if change == 'coverage_gap': row(e, 'coverage')['end'] = '2026-10-01T00:00:30+00:00'
    elif change == 'coverage_counters': row(e, 'coverage')['attempts'] += 1
    elif change == 'missing_query':
        q = row(e, 'query'); e.remove(q)
        for i, r in enumerate(e, 1): r['producer_sequence'] = i
        m['producer']['sequence_end'] = len(e)
    elif change == 'missing_attempt': e.remove(row(e, 'attempt'))
    elif change == 'timeout_exact2pct':
        # One timeout in50 attempts is exactly2%, including retries/errors.
        m, e = subset(m, e, {'baseline'})
        a = row(e, 'attempt'); a['outcome'] = 'timeout'
        # Remove paid completion for that work; preserve attempts denominator50.
        target = next(w for w in m['windows'] if w['role'] == 'baseline')
        paid = row(e, 'completion'); e.remove(paid); target['inventory']['completion'].remove(paid['id'])
        for i in range(40):
            new = dict(a, id=f'retry{i}', outcome='error')
            e.append(new); target['inventory']['attempt'].append(new['id'])
        sample = row(e, 'coverage'); sample['attempts'] = 50; sample['provider_bytes'] = 5000; sample['completions'] = 9
        for i, r in enumerate(e, 1): r['producer_sequence'] = i
        m['producer']['sequence_end'] = len(e)
    elif change == 'memory_swap': row(e, 'coverage')['swap_bytes'] = 1
    elif change == 'zero_rss': row(e, 'coverage')['rss_process_bytes'] = 0
    elif change == 'bad_clock': row(e, 'attempt')['at'] = '2026-10-01T00:00:01'
    elif change == 'unknown_outcome': row(e, 'attempt')['outcome'] = 'UNKNOWN'
    elif change == 'missing_provider_bytes': del row(e, 'attempt')['provider_bytes']
    elif change == 'bad_workload_digest': m['workload'][0]['actor'] = 'reassigned'
    elif change == 'duplicate_event': e.append(e[0])
    elif change == 'wrong_streams': m['windows'][1]['total_streams'] = 3
    elif change == 'short_window': m['windows'][0]['end'] = '2026-10-01T23:59:00+00:00'
    elif change == 'writer_lock': row(e, 'writer')['lock_file_id'] = '9:9'
    elif change == 'writer_duplicate': row(e, 'writer')['observed_keys'].append('p1')
    elif change == 'writer_clock': row(e, 'writer')['observed_clocks'] = {'p1': '2026-10-02T00:00:00+00:00'}
    elif change == 'missing_writer_sample': row(e, 'coverage', window='writer')['writer_observations'] = {}
    elif change == 'table_drift': next(iter(row(e, 'coverage', window='writer')['table_checks'].values()))['rows'] = 1
    elif change == 'unclosed_lease': row(e, 'lease')['closed_at'] = None
    elif change == 'lease_grant_drift': row(e, 'lease')['grant_id'] = 'new-grant'
    elif change == 'portion_too_long': row(e, 'portion')['end'] = '2026-10-05T00:45:01+00:00'
    elif change == 'task_outside': row(e, 'task')['end'] = '2026-10-05T00:17:00+00:00'
    elif change == 'batch_dq': row(e, 'batch')['missing'] = 1
    elif change == 'current_wait': row(e, 'current')['history_wait_seconds'] = 1
    elif change == 'transport_extra_probe': next(r for r in e if r.get('name') == 'transport')['payload']['recovery_attempts'].append('extra')
    elif change == 'transport_platform': next(r for r in e if r.get('name') == 'transport')['payload']['failures'][0]['classification'] = 'platform'
    elif change == 'transport_unlinked': next(r for r in e if r.get('name') == 'transport')['payload']['failures'][0]['attempt_id'] = 'unseen'
    elif change == 'season_disappearance': next(r for r in e if r.get('name') == 'season_close')['payload']['registry_after'] = [{'scope': 'x', 'competition': 'other', 'season': 2026}]
    elif change == 'unknown_summary_flag': row(e, 'attempt')['PASS'] = True
    elif change == 'compaction_bounds': next(r for r in e if r.get('name') == 'compaction')['payload']['bytes']['transfermarkt_transfers'] = 536870913
    elif change == 'anchor_drift': next(r for r in e if r.get('name') == 'compaction')['payload']['protected_after'] = ['new-anchor']
    if change == 'timeout_exact2pct': reconcile_events(m, e)
    report = validate(m, e)
    assert report['artifact_status'] == 'INVALID'
    assert any(expected in issue for issue in report['issues']), report['issues']
    assert report['actual_acceptance_start'] is None


def test_jsonl_roundtrip_duplicate_keys_and_nonfinite_rejected(tmp_path, complete):
    m, e = subset(*complete, {'history'})
    target = tmp_path / 'events.jsonl'
    write_fixture(target, m, e)
    assert read_jsonl(target) == (m, e)
    target.write_text('{"schema":1,"schema":1}\n')
    with pytest.raises(InvalidEvidence, match='duplicate JSON field'): read_jsonl(target)
    target.write_text('{"value":NaN}\n')
    with pytest.raises(InvalidEvidence, match='nonfinite'): read_jsonl(target)


def test_cli_validation_invalid_json_never_reports_success(tmp_path):
    target = tmp_path / 'bad.jsonl'; target.write_text('{broken\n')
    script = Path(__file__).resolve().parents[3] / 'dags/scripts/transfermarkt_offline_acceptance.py'
    result = subprocess.run([sys.executable, str(script), 'validate', str(target)], capture_output=True, text=True, timeout=20)
    assert result.returncode == 2
    assert json.loads(result.stdout)['artifact_status'] == 'INVALID'


def test_real_owned_process_lock_intent_replay_and_local_circuit(tmp_path):
    previous = os.environ.get('TM_WRITE_INTENT_DIR')
    evidence = local_exercises(tmp_path)
    assert evidence['writer_count'] == 2 and len(evidence['writer_receipts']) == 8
    assert {r['lock_file_id'] for r in evidence['writer_receipts']} == {evidence['lock_file_id']}
    assert evidence['maintenance']['lock_file_id'] == evidence['lock_file_id']
    assert evidence['critical_sections_nonoverlapping'] is True
    assert evidence['crash_exit_code'] == 17 and len(evidence['immutable_intent_sha256']) == 64
    assert evidence['source_http_attempts'] == 0 and evidence['runtime_status'] == 'UNKNOWN'
    assert not evidence['circuit_journal'].get('paused_until')
    assert os.environ.get('TM_WRITE_INTENT_DIR') == previous


def test_resource_exporter_reads_retained_proc_cgroup_and_all_attempts(complete):
    from scrapers.transfermarkt.offline_acceptance import resource_event
    m, e = complete
    result = resource_event(m, e, window_id='baseline', identity='s0', sequence=1,
                            start='2026-10-01T00:00:00+00:00', end='2026-10-01T00:01:00+00:00',
                            process_status='Name:\tretained\nVmRSS:\t2 kB\n',
                            cgroup_stats={'memory.current': 4096, 'memory.max': 8192, 'memory.swap.current': 0})
    assert result['rss_process_bytes'] == 2048 and result['cgroup_memory_bytes'] == 4096
    assert result['attempts'] == 10 and result['provider_bytes'] == 1000
    assert result['completions'] == 10 and result['trino_memory_bytes'] == 64


@pytest.mark.parametrize('proc,cgroup', [
    ('Name:\tunknown\n', {'memory.current': 4096, 'memory.max': 8192, 'memory.swap.current': 0}),
    ('VmRSS:\t0 kB\n', {'memory.current': 4096, 'memory.max': 8192, 'memory.swap.current': 0}),
    ('VmRSS:\t2 kB\n', {'memory.current': 4096, 'memory.max': 'max', 'memory.swap.current': 0}),
    ('VmRSS:\t2 kB\n', {'memory.current': 4096, 'memory.max': 8192}),
])
def test_resource_exporter_rejects_missing_unknown_rss_or_memory_limit(complete, proc, cgroup):
    from scrapers.transfermarkt.offline_acceptance import resource_event
    m, e = complete
    with pytest.raises(InvalidEvidence):
        resource_event(m, e, window_id='baseline', identity='s0', sequence=1,
                       start='2026-10-01T00:00:00+00:00', end='2026-10-01T00:01:00+00:00',
                       process_status=proc, cgroup_stats=cgroup)


def test_configurable_sampling_interval_is_not_owner_criterion(complete):
    m, e = subset(*complete, {'history'})
    m['sampling_seconds'] = 120
    assert validate(m, e)['criteria']['history']['artifact_status'] == 'VALID'


def test_partial_canary_does_not_fabricate_complete_comparison(complete):
    m, e = subset(*complete, {'canary1'})
    report = validate(m, e)
    assert report['artifact_status'] == 'VALID'
    assert report['criteria']['canary']['artifact_status'] == 'UNKNOWN'


def test_child_failure_reaps_only_owned_sibling():
    import time
    from scrapers.transfermarkt.offline_harness import _owned_processes, _wait_owned
    recorded = []
    with pytest.raises(RuntimeError, match='exited unexpectedly'):
        with _owned_processes() as children:
            for index in range(2):
                pid = os.fork()
                if pid == 0:
                    if index == 0:
                        os._exit(23)
                    time.sleep(30)
                    os._exit(0)
                children.append(pid)
                recorded.append(pid)
            _wait_owned(children[0])
    for pid in recorded:
        with pytest.raises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)


@pytest.mark.parametrize('attempt_count,expected', [(500, 'VALID'), (501, 'INVALID')])
def test_shared_attempt_boundary_counts_errors_and_retries(complete, attempt_count, expected):
    m, e = subset(*complete, {'history'})
    window = m['windows'][0]
    original = row(e, 'attempt')
    existing = sum(a['kind'] == 'attempt' and a['portion_id'] == 'p0' for a in e)
    for i in range(attempt_count - existing):
        new = dict(original, id=f'additional-error-{i}', outcome='error')
        e.append(new)
        window['inventory']['attempt'].append(new['id'])
    first = row(e, 'coverage')
    matches = [a for a in e if a['kind'] == 'attempt' and first['start'] <= a['at'] < first['end']]
    first['attempts'] = len(matches)
    first['provider_bytes'] = sum(a['provider_bytes'] for a in matches)
    for i, r in enumerate(e, 1): r['producer_sequence'] = i
    m['producer']['sequence_end'] = len(e)
    reconcile_events(m, e)
    result = validate(m, e)
    assert result['criteria']['history']['artifact_status'] == expected, result['issues']
    if expected == 'INVALID':
        assert any('1..500' in issue for issue in result['issues'])


def test_cli_demo_runs_without_pytest_import_paths(tmp_path):
    script = Path(__file__).resolve().parents[3] / 'dags/scripts/transfermarkt_offline_acceptance.py'
    output = tmp_path / 'standalone-demo'
    clean_env = dict(os.environ)
    clean_env.pop('PYTHONPATH', None)
    result = subprocess.run([sys.executable, str(script), 'demo', str(output)], env=clean_env,
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report['artifact_status'] == 'VALID' and report['runtime_status'] == 'UNKNOWN'
    assert (output / 'owned-local-exercises.json').exists()


@pytest.mark.parametrize('role,scenario', [('history', 'season_close'), ('writer', 'crash_replay')])
def test_missing_scenario_is_independent_unknown(complete, role, scenario):
    m, e = subset(*complete, {role})
    missing = next(r for r in e if r.get('name') == scenario)
    e.remove(missing)
    m['windows'][0]['inventory']['scenario'].remove(missing['id'])
    for i, r in enumerate(e, 1): r['producer_sequence'] = i
    m['producer']['sequence_end'] = len(e)
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['artifact_status'] == 'VALID', result['issues']
    assert result['criteria'][role]['artifact_status'] == 'VALID'
    assert result['criteria'][scenario]['artifact_status'] == 'UNKNOWN'
    assert result['windows'][role]['resource_peak_qualification'] == 'UNKNOWN'
    assert 'rss_process_sampled_peak_bytes' in result['windows'][role]


def peak_pair(window):
    record = {'window': window['id'], 'process_identity': 'boot-id:pid42:startticks100',
              'cgroup_identity': 'mount-id:device:inode', 'process_started_at': window['start'],
              'cgroup_peak_reset_at': window['start'], 'observed_at': window['end'],
              'proc_status': 'VmHWM:\t4 kB\nVmRSS:\t2 kB\n', 'cgroup_memory_peak_bytes': 8192}
    expected = {k: record[k] for k in ('process_identity', 'cgroup_identity', 'process_started_at', 'cgroup_peak_reset_at', 'observed_at')}
    expected['record_sha256'] = digest(record)
    return expected, record


def test_retained_whole_window_peaks_need_pinned_exact_identity_and_resets(complete):
    from scrapers.transfermarkt.offline_acceptance import retained_peaks
    m, e = subset(*complete, {'writer'})
    expected, record = peak_pair(m['windows'][0])
    assert retained_peaks(m['windows'][0], expected, record, manifest_pinned=False)['qualification'] == 'UNKNOWN'
    m['peak_manifest'] = {'writer': expected}
    result = validate(m, e, expected_manifest_sha256=digest(m), peak_records=[record])
    assert result['artifact_status'] == 'VALID', result['issues']
    peak = result['windows']['writer']['retained_peak']
    assert peak['qualification'] == 'VALID' and peak['process_rss_peak_bytes'] == 4096
    assert peak['cgroup_memory_peak_bytes'] == 8192
    assert result['runtime_status'] == 'UNKNOWN'


@pytest.mark.parametrize('field,value', [('process_started_at', None), ('cgroup_peak_reset_at', None),
                                        ('observed_at', '2026-10-04T23:59:00+00:00'), ('process_identity', 'another-process')])
def test_retained_peaks_unknown_without_whole_window_reset_or_owner(complete, field, value):
    from scrapers.transfermarkt.offline_acceptance import retained_peaks
    window = complete[0]['windows'][3]
    expected, record = peak_pair(window)
    record[field] = value
    expected['record_sha256'] = digest(record)
    assert retained_peaks(window, expected, record, manifest_pinned=True)['qualification'] == 'UNKNOWN'


@pytest.mark.parametrize('status,memory', [('VmHWM:\tunknown kB\n', 8192), ('VmHWM:\t4 kB\n', 'unknown')])
def test_retained_peaks_reject_unknown_measurement_types(complete, status, memory):
    from scrapers.transfermarkt.offline_acceptance import retained_peaks
    window = complete[0]['windows'][3]
    expected, record = peak_pair(window)
    record['proc_status'], record['cgroup_memory_peak_bytes'] = status, memory
    expected['record_sha256'] = digest(record)
    assert retained_peaks(window, expected, record, manifest_pinned=True)['qualification'] == 'INVALID'


def reconcile_events(manifest, events):
    """Keep negative probes' independently pinned inventories/counters complete."""
    for window in manifest['windows']:
        local = [r for r in events if r['window'] == window['id']]
        for kind in window['inventory']:
            window['inventory'][kind] = [r['id'] for r in local if r['kind'] == kind]
        if 'attempt_owners' in window:
            window['attempt_owners'] = {r['id']: {k: r[k] for k in ('work_id', 'actor', 'stream_id')}
                                        for r in local if r['kind'] == 'attempt'}
        for sample in [r for r in local if r['kind'] == 'coverage']:
            matched = [r for r in local if r['kind'] == 'attempt' and sample['start'] <= r['at'] < sample['end']]
            sample['attempts'] = len(matched)
            sample['provider_bytes'] = sum(r['provider_bytes'] for r in matched)
    for i, r in enumerate(events, 1): r['producer_sequence'] = i
    manifest['producer']['sequence_end'] = len(events)


@pytest.mark.parametrize('at', ['2026-10-05T00:00:10+00:00', '2026-10-05T00:15:04+00:00'])
def test_review_transport_rejects_all_actual_paused_io_and_extra_unhealed_probes(complete, at):
    m, e = subset(*complete, {'history'})
    recovery = next(a for a in e if a['id'] == 'recovery-paid')
    e.append(dict(recovery, id='extra-paid', at=at))
    scenario = next(a for a in e if a.get('name') == 'transport')
    scenario['payload']['recovery_raw_at'] = '2026-10-05T00:15:20+00:00'
    reconcile_events(m, e)
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['criteria']['transport']['artifact_status'] == 'INVALID', result


@pytest.mark.parametrize('mutation', ['clock_drift', 'unbound_row_count'])
def test_review_history_rejects_unbound_original_clocks_and_counts(complete, mutation):
    m, e = subset(*complete, {'history'})
    for batch in [r for r in e if r['kind'] == 'batch']:
        if mutation == 'clock_drift': batch['raw_times'] = ['2020-01-01T00:00:00+00:00']
        else: batch['expected_rows'] = batch['observed_rows'] = 999
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['criteria']['history']['artifact_status'] == 'INVALID', result


@pytest.mark.parametrize('measurement', ['process', 'cgroup'])
def test_review_retained_peaks_cannot_undercut_observed_point_samples(complete, measurement):
    m, e = subset(*complete, {'writer'})
    sample = row(e, 'coverage')
    sample['rss_process_bytes'] = 8192 if measurement == 'process' else 128
    sample['cgroup_memory_bytes'] = 16384
    sample['memory_limit_bytes'] = 32768
    expected, record = peak_pair(m['windows'][0])
    if measurement == 'process':
        record['cgroup_memory_peak_bytes'] = 32768
        expected['record_sha256'] = digest(record)
    m['peak_manifest'] = {'writer': expected}
    result = validate(m, e, expected_manifest_sha256=digest(m), peak_records=[record])
    assert result['windows']['writer']['resource_peak_qualification'] == 'INVALID', result


def test_review_transport_does_not_block_unrelated_stream(complete):
    m, e = subset(*complete, {'history'})
    recovery = next(a for a in e if a['id'] == 'recovery-paid')
    e.append(dict(recovery, id='unrelated-paid', stream_id='current-0', at='2026-10-05T00:00:10+00:00'))
    reconcile_events(m, e)
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['artifact_status'] == 'VALID', result['issues']
    assert result['criteria']['transport']['artifact_status'] == 'VALID'


def test_review_attempt_owner_cannot_escape_pinned_stream_or_actor(complete):
    m, e = subset(*complete, {'history'})
    next(a for a in e if a['id'] == 'recovery-paid')['stream_id'] = 'current-0'
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['artifact_status'] == 'INVALID'
    assert any('stream/actor identity' in issue for issue in result['issues'])


@pytest.mark.parametrize('unit_index', [0, 1])
def test_review_history_rejects_omitted_data_or_typed_empty_unit(complete, unit_index):
    m, e = subset(*complete, {'history'})
    row(e, 'batch')['capture_units'].pop(unit_index)
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['criteria']['history']['artifact_status'] == 'INVALID'
    assert any('physical unit membership' in issue for issue in result['issues'])


def test_review_history_rejects_rebound_physical_unit(complete):
    m, e = subset(*complete, {'history'})
    row(e, 'batch')['capture_units'][0]['physical_unit_id'] = 'another-snapshot:batch'
    assert validate(m, e, expected_manifest_sha256=digest(m))['criteria']['history']['artifact_status'] == 'INVALID'


def test_review_history_original_identity_missing_remains_unknown(complete):
    m, e = subset(*complete, {'history'})
    del m['batch_manifest']
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['criteria']['history']['artifact_status'] == 'UNKNOWN'
    assert result['runtime_status'] == 'UNKNOWN'


def test_review_history_same_retained_unit_cannot_have_two_original_raw_clocks(complete):
    m, e = subset(*complete, {'history'})
    unit = m['batch_manifest']['b1'][0]
    unit['fetched_at'] = '2025-01-01T00:00:00+00:00'
    batch = next(r for r in e if r['kind'] == 'batch' and r['id'] == 'b1')
    batch['capture_units'][0]['fetched_at'] = unit['fetched_at']
    batch['raw_times'][0] = unit['fetched_at']
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['criteria']['history']['artifact_status'] == 'INVALID'
    assert any('global capture/physical/raw identity' in issue for issue in result['issues'])


def test_review_history_accepts_exact_old_cache_units_reused_across_batches(complete):
    m, e = subset(*complete, {'history'})
    original = '2020-01-01T00:00:00+00:00'
    for work in m['workload']:
        if work['id'] in {'w0', 'w2'}: work['original_capture_at'] = original
    m['workload_sha256'] = digest(m['workload'])
    for units in m['batch_manifest'].values():
        for unit in units: unit['fetched_at'] = unit['captured_at'] = original
    for batch in [r for r in e if r['kind'] == 'batch']:
        for unit in batch['capture_units']: unit['fetched_at'] = unit['captured_at'] = original
        batch['raw_times'] = [original, original]
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['artifact_status'] == 'VALID', result['issues']
    assert result['criteria']['history']['artifact_status'] == 'VALID'


@pytest.mark.parametrize('owner', ['process_identity', 'cgroup_identity', 'missing'])
def test_review_retained_peaks_unlinked_to_coverage_owners_stay_unknown(complete, owner):
    m, e = subset(*complete, {'writer'})
    sample = row(e, 'coverage')
    if owner == 'missing':
        sample.pop('process_identity'); sample.pop('cgroup_identity')
    else: sample[owner] = 'unlinked-owner'
    expected, record = peak_pair(m['windows'][0])
    m['peak_manifest'] = {'writer': expected}
    result = validate(m, e, expected_manifest_sha256=digest(m), peak_records=[record])
    assert result['artifact_status'] == 'VALID', result['issues']
    assert result['windows']['writer']['resource_peak_qualification'] == 'UNKNOWN'


def test_review_history_all_typed_empty_batches_have_nonzero_unit_denominator(complete):
    m, e = subset(*complete, {'history'})
    for batch_id, units in m['batch_manifest'].items():
        m['batch_manifest'][batch_id] = [u for u in units if u['kind'] == 'empty']
    for batch in [r for r in e if r['kind'] == 'batch']:
        batch['capture_units'] = [u for u in batch['capture_units'] if u['kind'] == 'empty']
        batch['raw_times'] = [u['fetched_at'] for u in batch['capture_units']]
        batch['expected_rows'] = batch['observed_rows'] = 0
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['artifact_status'] == 'VALID', result['issues']
    assert result['criteria']['history']['artifact_status'] == 'VALID'


def dual_outputs(manifest, events, *, rows=2, kind='rows'):
    """One HTTP capture can produce several independently immutable outputs."""
    for batch in [r for r in events if r['kind'] == 'batch']:
        original = manifest['batch_manifest'][batch['id']][0]
        legacy = dict(original, table='transfermarkt_market_value_history',
                      physical_unit_id='legacy-snapshot12:legacy-original-batch-0',
                      native_batch_id='legacy-original-batch-0', rows=rows, kind=kind,
                      empty_type='market_value' if kind == 'empty' else None)
        manifest['batch_manifest'][batch['id']].append(copy.deepcopy(legacy))
        batch['capture_units'].append(copy.deepcopy(legacy))
        batch['raw_times'].append(legacy['fetched_at'])
        batch['expected_rows'] += rows
        batch['observed_rows'] += rows


def test_round2_shared_http_capture_allows_native_legacy_outputs_and_cache_reuse(complete):
    m, e = subset(*complete, {'history'})
    dual_outputs(m, e)
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['criteria']['history']['artifact_status'] == 'VALID', result['issues']
    assert result['artifact_status'] == 'VALID' and result['runtime_status'] == 'UNKNOWN'


def test_round2_transport_rejects_owner_drift_even_after_history_failure(complete):
    m, e = subset(*complete, {'history'})
    recovery = next(r for r in e if r['id'] == 'recovery-paid')
    extra = dict(recovery, id='paused-extra', at='2026-10-05T00:00:10+00:00')
    e.append(extra)
    reconcile_events(m, e)
    pin = digest(m)
    extra['stream_id'] = 'current-0'
    result = validate(m, e, expected_manifest_sha256=pin)
    assert result['artifact_status'] == 'INVALID'
    assert result['criteria']['history']['artifact_status'] == 'INVALID'
    assert result['criteria']['transport']['artifact_status'] in {'INVALID', 'UNKNOWN'}, result


@pytest.mark.parametrize('rows,kind', [(1, 'rows'), (0, 'empty')])
def test_round2_shared_capture_keeps_row_count_and_empty_type_output_specific(complete, rows, kind):
    m, e = subset(*complete, {'history'})
    dual_outputs(m, e, rows=rows, kind=kind)
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['artifact_status'] == 'VALID', result['issues']


@pytest.mark.parametrize('field,value', [('raw_id', 'another-original-raw'),
                                        ('raw_sha256', digest('changed-source-body')),
                                        ('fetched_at', '2025-01-01T00:00:00+00:00'),
                                        ('captured_at', '2026-10-01T00:00:01+00:00')])
def test_round2_shared_capture_rejects_source_identity_hash_or_clock_drift(complete, field, value):
    m, e = subset(*complete, {'history'})
    dual_outputs(m, e)
    for units in m['batch_manifest'].values(): units[-1][field] = value
    for batch in [r for r in e if r['kind'] == 'batch']:
        batch['capture_units'][-1][field] = value
        if field == 'fetched_at': batch['raw_times'][-1] = value
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['criteria']['history']['artifact_status'] == 'INVALID'


@pytest.mark.parametrize('field,value', [('table', 'transfermarkt_transfers'),
                                        ('native_batch_id', 'replacement-output-batch'), ('rows', 3), ('kind', 'empty')])
def test_round2_reused_physical_output_cannot_change_table_batch_rows_or_empty_kind(complete, field, value):
    m, e = subset(*complete, {'history'})
    dual_outputs(m, e)
    batch = next(r for r in e if r['kind'] == 'batch' and r['id'] == 'b1')
    expected = m['batch_manifest']['b1'][-1]
    observed = batch['capture_units'][-1]
    for unit in (expected, observed):
        unit[field] = value
        if field == 'kind': unit['rows'] = 0; unit['empty_type'] = 'market_value'
    delta = observed['rows'] - 2
    batch['expected_rows'] += delta; batch['observed_rows'] += delta
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['criteria']['history']['artifact_status'] == 'INVALID'
    assert any('global capture/physical/raw identity' in issue for issue in result['issues'])


@pytest.mark.parametrize('proof', ['missing_attempt', 'missing_owner'])
def test_round2_transport_rejects_its_missing_shared_attempt_proof(complete, proof):
    m, e = subset(*complete, {'history'})
    recovery = next(r for r in e if r['id'] == 'recovery-paid')
    extra = dict(recovery, id='paused-extra', at='2026-10-05T00:00:10+00:00')
    e.append(extra)
    reconcile_events(m, e)
    if proof == 'missing_attempt':
        e.remove(extra)
        for i, r in enumerate(e, 1): r['producer_sequence'] = i
        m['producer']['sequence_end'] = len(e)
    else: del m['windows'][0]['attempt_owners']['paused-extra']
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['criteria']['transport']['artifact_status'] == 'INVALID'


def test_round2_transport_remains_independent_of_unrelated_history_dq_and_missing_season(complete):
    m, e = subset(*complete, {'history'})
    e.remove(next(r for r in e if r.get('name') == 'season_close'))
    row(e, 'batch')['missing'] = 1
    reconcile_events(m, e)
    result = validate(m, e, expected_manifest_sha256=digest(m))
    assert result['criteria']['history']['artifact_status'] == 'INVALID'
    assert result['criteria']['season_close']['artifact_status'] == 'UNKNOWN'
    assert result['criteria']['transport']['artifact_status'] == 'VALID', result['issues']
