from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dags.scripts.run_transfermarkt_referees import production_referee_scraper, run_referee_sweep
from scrapers.transfermarkt.referee_capture import (
    referee_directory_url,
    referee_profile_url,
    referee_scopes,
)
from scrapers.transfermarkt.referees import RefereeParseError
from tests.unit.scrapers.test_transfermarkt_referee_capture import (
    DIRECTORY, PROFILE, FakeClient, registry_row, scope,
)

pytestmark = pytest.mark.unit
NOW = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)


class RuntimeFactory:
    def __init__(self, root, scopes, *, broken=()):
        self.root = root
        self.scopes = {item['scope_id']: item for item in scopes}
        self.broken = broken
        self.calls = []
        self.writes = []

    def __call__(self, value):
        scope_value = self.scopes[value['scope_id']]
        responses = {}
        directory_html = '<html>broken</html>' if value['scope_id'] in self.broken else DIRECTORY
        responses[referee_directory_url(scope_value)] = directory_html
        for referee_id in ('101', '202'):
            responses[referee_profile_url(referee_id)] = PROFILE if referee_id == '101' else PROFILE.replace('Ada', 'Boris')
        client = FakeClient(
            self.root / value['scope_id'], responses, scope_id=value['scope_id'],
        )
        self.calls.append((value['scope_id'], client.calls))
        return SimpleNamespace(_http_client=client)

    def writer(self, runtime, bundle, preflight):
        assert bundle.scope_id in self.scopes
        proof = {
            'verified': True, 'receipt_readback': True,
            'row_count': len(bundle.profiles) + len(bundle.aggregates),
            'raw_capture_ids': list(bundle.raw_capture_ids),
        }
        self.writes.append((bundle.capture_kind, bundle.scope_id, proof))
        return proof


def run(factory, scopes, state_dir, *, mode='current', now=NOW, selected=(), max_scopes=100):
    rows = [registry_row(value) for value in scopes]
    return run_referee_sweep(
        rows, mode=mode, run_id='run-test', state_dir=state_dir,
        selected_scope_ids=selected, max_scopes=max_scopes,
        scraper_factory=factory, writer=factory.writer, now_fn=lambda: now,
    )


def test_current_full_sweep_writes_aggregates_and_profiles_with_receipts(tmp_path):
    values = [scope()]
    factory = RuntimeFactory(tmp_path, values)
    report = run(factory, values, tmp_path)
    assert report['status'] == 'complete'
    assert report['coverage_complete'] is True
    assert report['expected_scopes'] == 1
    assert report['fresh_aggregate_scopes'] == 1 and report['fresh_profile_scopes'] == 1
    assert report['unresolved_scopes'] == 0 and report['match_row_count'] == 0
    assert [kind for kind, _, _ in factory.writes] == ['aggregates', 'profiles']
    for _, _, proof in factory.writes:
        assert proof['verified'] and proof['receipt_readback']


def test_fresh_second_run_is_idempotent_noop(tmp_path):
    values = [scope()]
    factory = RuntimeFactory(tmp_path, values)
    first = run(factory, values, tmp_path)
    calls = len(factory.calls)
    writes = len(factory.writes)
    second = run(factory, values, tmp_path)
    assert first['status'] == 'complete' and second['status'] == 'no_op'
    assert len(factory.calls) == calls and len(factory.writes) == writes
    assert second['continuation_required'] is False


def test_weekly_and_monthly_clocks_are_independent(tmp_path):
    values = [scope()]
    factory = RuntimeFactory(tmp_path, values)
    run(factory, values, tmp_path)
    run(factory, values, tmp_path, now=NOW + timedelta(days=6))
    weekly = run(factory, values, tmp_path, now=NOW + timedelta(days=7))
    assert weekly['fresh_aggregate_scopes'] == 1
    assert weekly['fresh_profile_scopes'] == 1  # The existing monthly observation remains fresh.
    assert [kind for kind, _, _ in factory.writes].count('aggregates') == 2
    assert [kind for kind, _, _ in factory.writes].count('profiles') == 1
    monthly = run(factory, values, tmp_path, now=NOW + timedelta(days=31))
    assert [kind for kind, _, _ in factory.writes][-2:] == ['aggregates', 'profiles']
    assert monthly['fresh_profile_scopes'] == 1


def test_failed_directory_stays_pending_and_does_not_write_profile(tmp_path):
    values = [scope()]
    broken = RuntimeFactory(tmp_path, values, broken={values[0]['scope_id']})
    first = run(broken, values, tmp_path)
    assert first['status'] == 'error'
    assert first['unresolved_scopes'] == 1 and first['continuation_required'] is True
    assert first['coverage_complete'] is False and broken.writes == []

    fixed = RuntimeFactory(tmp_path, values)
    second = run(fixed, values, tmp_path)
    assert second['status'] == 'complete' and second['unresolved_scopes'] == 0
    assert [kind for kind, _, _ in fixed.writes] == ['aggregates', 'profiles']


def test_history_backfill_has_separate_coverage_and_no_profiles(tmp_path):
    values = [scope(current=True), scope(current=False, edition='2024')]
    factory = RuntimeFactory(tmp_path, values)
    report = run(factory, values, tmp_path / 'history', mode='history')
    assert report['mode'] == 'history'
    assert report['expected_scopes'] == 1 and report['completed_history_scopes'] == 1
    assert report['history_coverage'] == 1.0 and report['coverage_complete'] is True
    assert [kind for kind, _, _ in factory.writes] == ['aggregates']


def test_bounded_explicit_scope_does_not_claim_full_universe(tmp_path):
    values = [scope(), scope(competition='AB2')]
    selected = values[0]['scope_id']
    factory = RuntimeFactory(tmp_path, values)
    report = run(factory, values, tmp_path, selected=(selected,))
    assert report['explicit_selection'] is True
    assert report['expected_scopes'] == 1 and report['universe_scopes'] == 2
    assert report['coverage_complete'] is False


def test_scope_limit_preserves_continuation_then_completes(tmp_path):
    values = [scope(), scope(competition='AB2')]
    first_factory = RuntimeFactory(tmp_path, values)
    first = run(first_factory, values, tmp_path, max_scopes=1)
    assert first['status'] == 'continuation' and first['continuation_required'] is True
    assert first['fresh_aggregate_scopes'] == 1
    second_factory = RuntimeFactory(tmp_path, values)
    second = run(second_factory, values, tmp_path)
    assert second['status'] == 'complete' and second['fresh_aggregate_scopes'] == 2


def test_parser_failure_is_not_reclassified_as_success(tmp_path):
    values = [scope()]
    factory = RuntimeFactory(tmp_path, values)
    def reject(runtime, bundle, preflight):
        raise RefereeParseError('metric_value_invalid')

    factory.writer = reject
    report = run(factory, values, tmp_path)
    assert report['status'] == 'error' and report['unresolved_scopes'] == 1
    assert report['continuation_required'] is True


@pytest.mark.parametrize('historical', [False, True])
def test_production_factory_checks_policy_and_passes_exact_registry_record(
    monkeypatch, tmp_path, historical,
):
    value = referee_scopes([
        registry_row(scope(current=not historical, edition='2024' if historical else '2025')),
    ], historical=historical)[0]
    monkeypatch.delenv('TM_STANDING_POLICY_ENABLED', raising=False)
    monkeypatch.delenv('TM_PROXY_CONTROL_URL', raising=False)
    monkeypatch.delenv('TM_BACKFILL_PROXY_CONTROL_URL', raising=False)
    monkeypatch.delenv('TRANSFERMARKT_REQUIRE_RAW_STORE', raising=False)
    with pytest.raises(Exception, match='standing_policy_gate_required'):
        production_referee_scraper(value)

    policy_path = tmp_path / 'policy.json'
    policy = json.loads(Path('dags/configs/transfermarkt/standing_approval_policy.json').read_text())
    policy['allowed_write_tables'] = list(dict.fromkeys([
        *policy['allowed_write_tables'],
        'iceberg.bronze.transfermarkt_referee_profiles',
        'iceberg.bronze.transfermarkt_referee_season_aggregates',
        'iceberg.ops.transfermarkt_referee_receipts_v1',
    ]))
    policy['expires_at'] = '2999-01-01T00:00:00Z'
    policy_path.write_text(json.dumps(policy))
    monkeypatch.setenv('TM_STANDING_POLICY_ENABLED', 'true')
    monkeypatch.setenv('TM_PROXY_CONTROL_URL', 'http://offline.invalid')
    monkeypatch.setenv('TM_BACKFILL_PROXY_CONTROL_URL', 'http://history.offline.invalid')
    monkeypatch.setenv('TRANSFERMARKT_REQUIRE_RAW_STORE', 'true')
    monkeypatch.setenv('TM_STANDING_POLICY_PATH', str(policy_path))
    constructed = []

    def create(**kwargs):
        constructed.append(kwargs)
        return 'scraper'

    from scrapers.transfermarkt import scraper as tm
    monkeypatch.setattr(tm, 'TransfermarktScraper', create)
    assert production_referee_scraper(value) == 'scraper'
    assert constructed[0]['leagues'] == ['XY1']
    assert constructed[0]['seasons'] == [2024 if historical else 2025]
    assert constructed[0]['proxy_control_url'] == (
        'http://history.offline.invalid' if historical else 'http://offline.invalid'
    )
    assert constructed[0]['canonical_season'] == value['canonical_season']
    assert constructed[0]['competition_records'][0].competition_id == 'XY1'
    assert constructed[0]['lease_metadata']['scope'] == value['scope_id']


def test_history_referee_lease_uses_dedicated_control_token(monkeypatch):
    from scrapers.transfermarkt.client import ProxyFilterLeaseProvider

    monkeypatch.setenv('TM_DAG_ID', 'dag_backfill_transfermarkt_referees')
    monkeypatch.setenv('TM_PROXY_CONTROL_TOKEN', 'c' * 32)
    monkeypatch.setenv('TM_BACKFILL_PROXY_CONTROL_TOKEN', 'h' * 32)
    provider = ProxyFilterLeaseProvider('http://offline.invalid')
    assert provider._control_token == 'h' * 32
