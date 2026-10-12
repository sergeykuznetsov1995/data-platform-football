"""Once/day audit durability, snapshot and semantic DQ regressions (#1395)."""
from datetime import datetime, timedelta, timezone
import json
from threading import Thread

import pytest

from utils import transfermarkt_daily_dq as daily
from utils import transfermarkt_bronze_dq as dq

NOW = datetime(2026, 10, 12, 22, tzinfo=timezone.utc)  # 13 October Moscow


class Cursor:
    def __init__(self, empty=0, lineage=0, unknown=0, placeholder=(0, 0),
                 first_seen_null=0, first_seen_mismatch=0, first_seen_column=True, registry=True):
        self.sql = []
        self.empty, self.lineage, self.unknown = empty, lineage, unknown
        self.placeholder = placeholder
        self.first_seen_null, self.first_seen_mismatch = first_seen_null, first_seen_mismatch
        self.first_seen_column, self.registry = first_seen_column, registry

    def execute(self, sql):
        self.sql.append(sql)
        if '$snapshots' in sql:
            self.rows = [(123,)]
        elif 'information_schema.tables' in sql:
            self.rows = [(int(self.registry),)]
        elif 'information_schema.columns' in sql:
            self.rows = [('first_seen_at',)] if self.first_seen_column else []
        elif 'b.first_seen_at IS NULL' in sql:
            self.rows = [(5, self.first_seen_null)]
        elif 'r.first_seen_at IS NULL' in sql:
            self.rows = [(self.first_seen_mismatch,)]
        elif 'AS total_rows' in sql:
            tables = {**dq.ENTITY_BRONZE_TABLES,
                      'competitions': dq.COMPETITIONS_BRONZE_TABLE,
                      'competition_editions': dq.EDITIONS_BRONZE_TABLE}
            entity = next(entity for entity, table in tables.items() if table in sql)
            self.rows = [(10, 2, 3, 5, self.lineage, self.unknown,
                          *([8, 7, 0] * len(daily.FIELDS[entity])))]
        elif 'market_value_placeholder_zero' in sql:
            self.rows = [self.placeholder]
        elif 'WITH routes' in sql:
            self.rows = [('pokal', 100, self.empty, 0), ('wettbewerb', 200, 0, 0)]
        else:
            self.rows = [(0,)]

    def fetchall(self):
        return self.rows


@pytest.fixture
def full_runner(monkeypatch):
    calls = []
    def run(cur, **kwargs):
        calls.append(kwargs)
        # Exercise SQL pinning for evidence the old runner otherwise reads live.
        cur.execute('SELECT COUNT(*) FROM ' + dq.SCOPE_MANIFEST_TABLE)
        return [dq.BronzeCheckResult('tm_career_debt', 'career_debt', 'WARNING', True,
                value={'career_fetches_pending': 17, 'null_evidence_manifests': 0,
                       'complete_manifests': 300})]
    monkeypatch.setattr(dq, 'run_bronze_dq', run)
    return calls


def run(cursor, root, now=NOW, run_id='first'):
    return daily.run_daily_bronze_dq(cursor, state_root=root, run_id=run_id,
                                    now=now, routes={'GB1': 'wettbewerb', 'FAC': 'pokal'})


def test_completed_once_per_moscow_day_preserves_pins_and_first_run(tmp_path, full_runner):
    cur = Cursor()
    first = run(cur, tmp_path)
    assert first['day_msk'] == '2026-10-13'
    assert first['completed'] and first['status'] == 'GREEN'
    assert first['input_snapshot_ids'] == {table: 123 for table in (*daily.AUDIT_TABLES, daily.FIRST_SEEN_REGISTRY)}
    assert first['implementation_at'] == NOW.isoformat()
    assert full_runner[0]['zone'] == 'full'
    assert first['metrics']['career_debt']['career_fetches_pending'] == 17
    assert first['metrics']['cohorts']['transfer_events']['legacy_unknown'] == 2
    assert first['metrics']['fields']['transfer_events']['fee_eur']['fill_ratio'] == 7 / 8
    query_count = len(cur.sql)
    second = run(cur, tmp_path, NOW + timedelta(minutes=45), 'portion-2')
    assert second['cached'] and second['run_id'] == 'first'
    assert len(cur.sql) == query_count
    assert len(full_runner) == 1
    third = run(cur, tmp_path, NOW + timedelta(days=1), 'next-day')
    assert third['day_msk'] == '2026-10-14' and not third['cached']
    assert third['implementation_at'] == first['implementation_at']
    assert len(full_runner) == 2


def test_red_completed_does_not_repeat_full_sweep(tmp_path, full_runner):
    report = run(Cursor(empty=6, lineage=1, placeholder=(1, 0)), tmp_path)
    assert report['completed'] and report['status'] == 'RED'
    assert 'tm_daily_pokal_authoritative_empty' in report['errors']
    assert 'tm_daily_placeholder_zero[market_value]' in report['errors']
    assert all(f'tm_daily_null_lineage[{entity}]' in report['errors'] for entity in daily.FIELDS)
    assert run(Cursor(), tmp_path)['status'] == 'RED'
    assert len(full_runner) == 1


def test_pokal_exactly_five_percent_is_not_red(tmp_path, full_runner):
    report = run(Cursor(empty=5), tmp_path)
    assert report['status'] == 'GREEN'
    check = next(c for c in report['checks'] if c['kind'] == 'daily_empty_route')
    assert check['passed']


def test_failed_queries_are_retryable_with_durable_attempt_history(tmp_path, full_runner, monkeypatch):
    good = dq.run_bronze_dq
    def broken(*args, **kwargs):
        raise RuntimeError('warehouse unavailable')
    monkeypatch.setattr(dq, 'run_bronze_dq', broken)
    failed = run(Cursor(), tmp_path)
    assert not failed['completed'] and failed['status'] == 'UNKNOWN'
    assert 'warehouse unavailable' in failed['execution_errors'][0]
    monkeypatch.setattr(dq, 'run_bronze_dq', good)
    recovered = run(Cursor(), tmp_path, run_id='retry')
    assert recovered['completed'] and not recovered['cached']
    assert recovered['implementation_at'] == failed['implementation_at']
    assert len(list((tmp_path / 'attempts').glob('*.json'))) == 2


def test_a_check_execution_error_does_not_suppress_retry(tmp_path, monkeypatch):
    def failed_check(*args, **kwargs):
        return [dq.BronzeCheckResult('broken', 'test', 'ERROR', False, error='timed out')]
    monkeypatch.setattr(dq, 'run_bronze_dq', failed_check)
    report = run(Cursor(), tmp_path)
    assert not report['completed']
    assert report['status'] == 'UNKNOWN'
    assert report['errors'] == []
    assert report['execution_errors'] == ['broken']
    assert not run(Cursor(), tmp_path)['cached']


def test_confirmed_violation_plus_query_error_is_red_incomplete(tmp_path, monkeypatch):
    monkeypatch.setattr(dq, 'run_bronze_dq', lambda *args, **kwargs: [
        dq.BronzeCheckResult('duplicates', 'duplicates', 'ERROR', False, value=2),
        dq.BronzeCheckResult('broken', 'test', 'ERROR', False, error='timed out')])
    report = run(Cursor(), tmp_path)
    assert report['status'] == 'RED' and not report['completed']
    assert report['errors'] == ['duplicates']
    assert report['execution_errors'] == ['broken']


def test_simultaneous_portions_only_run_one_full_audit(tmp_path, full_runner):
    reports, errors = [], []
    def invoke(index):
        try:
            reports.append(run(Cursor(), tmp_path, run_id=str(index)))
        except Exception as exc:
            errors.append(exc)
    threads = [Thread(target=invoke, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    assert not errors and len(reports) == 2
    assert len(full_runner) == 1
    assert sorted(report['cached'] for report in reports) == [False, True]


def test_unknown_clock_and_missing_routes_do_not_become_green(tmp_path, full_runner):
    report = run(Cursor(unknown=2), tmp_path)
    assert report['completed'] and report['status'] == 'UNKNOWN'
    cur = Cursor()
    original = cur.execute
    def execute(sql):
        original(sql)
        if 'WITH routes' in sql:
            cur.rows = []
    cur.execute = execute
    report = run(cur, tmp_path / 'other')
    assert report['status'] == 'UNKNOWN'
    assert report['metrics']['empty_by_route']['pokal']['authoritative_empty'] is None


def test_empty_snapshot_prevents_unpinned_green(tmp_path, full_runner):
    cur = Cursor()
    original = cur.execute
    def execute(sql):
        original(sql)
        if '$snapshots' in sql:
            cur.rows = []
    cur.execute = execute
    report = run(cur, tmp_path)
    assert not report['completed'] and report['status'] == 'UNKNOWN'
    assert not full_runner


def test_every_input_is_frozen_without_duplicate_time_travel(tmp_path, full_runner):
    cur = Cursor()
    run(cur, tmp_path)
    assert any(f'{dq.SCOPE_MANIFEST_TABLE} FOR VERSION AS OF 123' in sql for sql in cur.sql)
    assert all('committed_at <= from_iso8601_timestamp' in sql for sql in cur.sql if '$snapshots' in sql)
    frozen = daily._PinnedCursor(cur, {'iceberg.bronze.transfermarkt_transfer_events': 17})
    frozen.execute('SELECT * FROM iceberg.bronze.transfermarkt_transfer_events FOR VERSION AS OF 17')
    assert cur.sql[-1].count('FOR VERSION AS OF') == 1
    literal = 'iceberg.bronze.transfermarkt_transfer_events'
    frozen.execute(f"SELECT '{literal}' table_name FROM {literal}")
    assert cur.sql[-1].startswith(f"SELECT '{literal}' table_name")
    assert cur.sql[-1].count('FOR VERSION AS OF') == 1
    assert not any(sql.lstrip().upper().startswith(('CREATE', 'INSERT', 'MERGE', 'DELETE', 'ALTER')) for sql in cur.sql)


def test_fill_applicability_and_zero_semantics_bind_raw_evidence():
    contract = daily.build_field_metrics_sql('player_contract_observations', NOW)
    assert "applicability_status = 'ok'" in contract
    fees = daily.build_field_metrics_sql('transfer_events', NOW)
    assert 'fee_text IS NOT NULL' in fees
    assert 'fetched_at >=' in fees and '_ingested_at >=' in fees
    sql = daily.build_placeholder_zero_sql()
    assert "'free transfer'" not in sql
    assert 'fee_eur = 0' in sql and 'fee_text' in sql
    assert 'value_eur = 0' in sql and 'mv_raw' in sql
    assert 'IS NULL' not in sql  # A missing raw value is unknown, not proof of a placeholder.


def test_read_summary_reports_missing_and_stale_unknown(tmp_path, full_runner):
    missing = daily.read_daily_result(state_root=tmp_path, now=NOW)
    assert missing['stale'] and missing['status'] == 'UNKNOWN'
    report = run(Cursor(), tmp_path)
    assert 'карьерный долг 17' in daily.format_daily_dq_summary(report)
    assert 'GREEN' in daily.format_daily_dq_summary(report)
    next_day = daily.read_daily_result(state_root=tmp_path, now=NOW + timedelta(days=1))
    assert next_day['stale'] and next_day['status'] == 'UNKNOWN'
    report['completed'] = False
    assert 'UNKNOWN' in daily.format_daily_dq_summary(report)
    path = tmp_path / '2026-10-13.json'
    value = json.loads(path.read_text())
    value['version'] = 'older-contract'
    path.write_text(json.dumps(value))
    assert daily.read_daily_result(state_root=tmp_path, now=NOW)['stale']


def test_no_synthetic_cutoff_and_naive_clocks_rejected(tmp_path, full_runner):
    run(Cursor(), tmp_path)
    with pytest.raises(ValueError, match='actual deployment'):
        run(Cursor(), tmp_path, NOW - timedelta(days=1))
    with pytest.raises(ValueError, match='timezone'):
        run(Cursor(), tmp_path, datetime(2026, 10, 12))


def test_first_seen_null_after_cutoff_is_red_and_registry_mismatch_is_red(tmp_path, full_runner):
    report = run(Cursor(first_seen_null=1, first_seen_mismatch=2), tmp_path)
    assert report['status'] == 'RED' and report['completed']
    assert len(report['metrics']['first_seen']) == 4
    assert all(item['null_new_rows'] == 1 for item in report['metrics']['first_seen'].values())
    assert all(item['registry_mismatches'] == 2 for item in report['metrics']['first_seen'].values())
    assert any(check['kind'] == 'daily_first_seen_invariance' and not check['passed']
               for check in report['checks'])


def test_missing_first_seen_column_or_registry_is_unknown_not_query_failure(tmp_path, full_runner):
    missing_column = run(Cursor(first_seen_column=False), tmp_path)
    assert missing_column['completed'] and missing_column['status'] == 'UNKNOWN'
    assert all(item['null_new_rows'] is None for item in missing_column['metrics']['first_seen'].values())
    assert all(item['missing_column'] for item in missing_column['metrics']['first_seen'].values())
    missing_registry = run(Cursor(registry=False), tmp_path / 'no-registry')
    assert missing_registry['completed'] and missing_registry['status'] == 'UNKNOWN'
    assert daily.FIRST_SEEN_REGISTRY not in missing_registry['input_snapshot_ids']
    assert all(item['registry_mismatches'] is None for item in missing_registry['metrics']['first_seen'].values())
    assert all(item['null_new_rows'] == 0 for item in missing_registry['metrics']['first_seen'].values())


def test_first_seen_audit_reuses_canonical_keys_including_nullable_left_date(full_runner):
    cur = Cursor()
    pins = {daily.FIRST_SEEN_REGISTRY: 123}
    frozen = daily._PinnedCursor(cur, pins)
    daily.collect_first_seen_metrics(frozen, NOW, pins)
    stints = next(sql for sql in cur.sql if 'r.key_json' in sql and 'transfermarkt_coach_stints' in sql)
    assert "'$[3]'" in stints
    assert 'IS NOT DISTINCT FROM CAST(CAST(b.left_date AS date) AS varchar)' in stints
    assert f'{daily.FIRST_SEEN_REGISTRY} FOR VERSION AS OF 123' in stints


def test_summary_preserves_known_red_when_other_checks_are_incomplete():
    report = {'day_msk': '2026-10-13', 'status': 'RED', 'completed': False,
              'errors': ['NULL lineage'], 'execution_errors': ['another query timed out']}
    summary = daily.format_daily_dq_summary(report)
    assert 'RED (неполная проверка)' in summary
    assert 'ошибок 1' in summary
    assert 'UNKNOWN' not in summary
    assert 'UNKNOWN' in daily.format_daily_dq_summary({**report, 'stale': True})


def test_baseline_red_is_retained_when_later_daily_sql_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(dq, 'run_bronze_dq', lambda *a, **k: [
        dq.BronzeCheckResult('duplicates', 'duplicates', 'ERROR', False, value=2)])
    monkeypatch.setattr(daily, 'collect_daily_metrics', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('late SQL failure')))
    report = run(Cursor(), tmp_path)
    assert not report['completed'] and report['status'] == 'RED'
    assert report['errors'] == ['duplicates']
    assert 'late SQL failure' in report['execution_errors'][0]
    assert 'RED (неполная проверка)' in daily.format_daily_dq_summary(report)


def test_daily_red_is_retained_when_first_seen_query_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(daily, 'pin_audit_snapshots', lambda *args: {})
    monkeypatch.setattr(daily.dq, 'run_bronze_dq', lambda *args, **kwargs: [])
    red = daily.dq.BronzeCheckResult(name='NULL lineage', kind='daily_null_lineage',
                                    severity='ERROR', passed=False, value=1)
    monkeypatch.setattr(daily, 'collect_daily_metrics', lambda *args, **kwargs: ({'fields': {'proved': 1}}, [red]))
    def fail(*args, **kwargs):
        raise RuntimeError('first_seen readback unavailable')
    monkeypatch.setattr(daily, 'collect_first_seen_metrics', fail)
    report = run(Cursor(), tmp_path)
    assert not report['completed'] and report['status'] == 'RED'
    assert report['errors'] == ['NULL lineage']
    assert report['metrics']['fields'] == {'proved': 1}
    assert report['checks'][0]['value'] == 1
    assert 'first_seen readback unavailable' in report['execution_errors'][0]
    assert 'RED (неполная проверка)' in daily.format_daily_dq_summary(report)


@pytest.mark.parametrize('stage', ['fields', 'first_seen'])
def test_collector_retains_completed_red_before_its_next_query_fails(tmp_path, full_runner, stage):
    class BrokenCursor(Cursor):
        def execute(self, sql):
            if ((stage == 'fields' and 'AS total_rows' in sql and '.transfermarkt_competition_editions' in sql)
                    or (stage == 'first_seen' and 'r.first_seen_at IS NULL' in sql)):
                raise RuntimeError('next audit query failed')
            return super().execute(sql)
    report = run(BrokenCursor(lineage=int(stage == 'fields'), first_seen_null=int(stage == 'first_seen')), tmp_path)
    assert report['status'] == 'RED' and not report['completed']
    kind = 'daily_null_lineage' if stage == 'fields' else 'daily_first_seen_completeness'
    assert any(c['kind'] == kind and not c['passed'] for c in report['checks'])
    assert 'next audit query failed' in report['execution_errors'][0]
    persisted = json.loads((tmp_path / (report['day_msk'] + '.json')).read_text())
    assert persisted['status'] == 'RED' and persisted['errors'] == report['errors']
