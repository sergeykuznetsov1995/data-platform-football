"""Offline orchestration tests for the stage-B core run (#2404)."""
import gzip
import json
from pathlib import Path

import pytest

from scrapers.transfermarkt import orchestrator
from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
from scrapers.transfermarkt.orchestrator import (
    ClubTarget,
    CoreRun,
    CoreRunError,
    CoreScopePlan,
    coach_history_url,
    coach_profile_url,
    mv_history_url,
    squad_url,
    transfers_url,
)
from tests.unit.scrapers.test_transfermarkt_core import EXTRACTED_AT, UNIVERSE, _load
from tests.unit.scrapers.test_transfermarkt_native_writer import CoreDB, CoreWriter

pytestmark = pytest.mark.unit

FIXTURES = Path('tests/fixtures/transfermarkt/stage_a_rewrite')
YEAR = 2026


@pytest.fixture(autouse=True)
def isolated_paths(monkeypatch, tmp_path):
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)


def _plan(**overrides):
    values = dict(
        league='GB1', season='2627', scope_id='tm-scope-test', cycle_id='tm-cycle-test',
        edition_id='2026',
        clubs=(ClubTarget(club_id='11', club_slug='arsenal', club_name='Arsenal',
                          squad_url=squad_url('arsenal', '11', YEAR)),),
        market_value_player_ids=(), transfer_player_ids=(), coach_profile_ids=(),
    )
    values.update(overrides)
    return CoreScopePlan(**values)


class _ReplayFetch:
    """Serve the recorded sample through the client fetch contract."""

    def __init__(self, endpoints):
        self.records = json.loads((FIXTURES / 'manifest.json').read_text())
        self.endpoints = endpoints
        self.calls = []

    def _fixture(self, url):
        for item in self.records['fixtures']:
            if item['endpoint'] not in self.endpoints:
                continue
            record = json.loads((FIXTURES / item['capture_file']).read_text())
            if record['url'] == url:
                return record, FIXTURES / item['blob_file']
        return None

    def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
        self.calls.append((label, url))
        found = self._fixture(url)
        if found is None:
            return FetchOutcome(status=FetchStatus.RETRY_EXHAUSTED, error='fixture_missing')
        record, blob = found
        text = gzip.decompress(blob.read_bytes()).decode('utf-8')
        value = json.loads(text) if as_json else text
        return FetchOutcome(
            status=FetchStatus.OK, value=value,
            raw_capture_id=record['capture_id'], raw_body_hash=record['content_hash'],
            raw_fetched_at=record['fetched_at'], cache_hit=False,
            status_code=record['status_code'],
        )

    def raw_loader(self, capture_id):
        for item in self.records['fixtures']:
            if item['capture_id'] != capture_id:
                continue
            record = json.loads((FIXTURES / item['capture_file']).read_text())
            return gzip.decompress((FIXTURES / item['blob_file']).read_bytes()), _Record(record)
        raise KeyError(capture_id)


class _Record:
    def __init__(self, record):
        self.content_hash = record['content_hash']
        self.url = record['url']
        self.fetched_at = record['fetched_at']


def test_squad_scope_run_produces_outcomes_dq_and_one_bundle_per_capture():
    replay = _ReplayFetch({'squad'})
    capture = _load('squad')
    club_id = capture.url.split('/verein/')[1].split('/')[0]
    plan = _plan(clubs=(ClubTarget(club_id=club_id, club_slug='club',
                                   squad_url=capture.url),))
    run = CoreRun(replay.fetch, raw_loader=replay.raw_loader,
                  universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert [item['entity'] for item in report.outcomes] == [
        'squad_memberships', 'player_attribute_observations', 'player_contract_observations']
    assert all(item['status'] == 'observed_present' for item in report.outcomes)
    assert report.dq['passed'] is True
    assert len(report.receipts) == 1 and report.receipts[0]['verified'] is True
    for table in ('transfermarkt_squad_memberships',
                  'transfermarkt_player_attribute_observations',
                  'transfermarkt_player_contract_observations'):
        assert db.sql.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0] == report.outcomes[0]['rows']
    # Idempotent replay of the same unit returns the same verified receipt.
    again = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert again.receipts == report.receipts
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_squad_memberships').fetchone()[0] \
        == report.outcomes[0]['rows']


def test_career_pair_writes_one_bundle_per_capture():
    replay = _ReplayFetch({'market_value_points'})
    capture = _load('market_value_points')
    player_id = capture.url.rsplit('/', 1)[-1]
    plan = _plan(clubs=(), market_value_player_ids=(player_id,))
    run = CoreRun(replay.fetch, raw_loader=replay.raw_loader,
                  universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert report.outcomes[0]['entity'] == 'market_value_points'
    assert len(report.receipts) == 1
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_market_value_points').fetchone()[0] \
        == report.outcomes[0]['rows']


def test_unavailable_fetch_records_captureless_outcomes_without_bundles():
    def dead_fetch(url, **kwargs):
        return FetchOutcome(status=FetchStatus.RETRY_EXHAUSTED, error='waf challenge')

    plan = _plan()
    run = CoreRun(dead_fetch, universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    report = run.run(plan)
    assert [item['status'] for item in report.outcomes] == ['unavailable'] * 3
    assert all(item['source_capture_id'] is None for item in report.outcomes)
    assert report.receipts == () and report.receipt is None
    assert report.dq['passed'] is True  # structural DQ; coverage stays 0


def test_json_capture_without_raw_loader_fails_closed():
    replay = _ReplayFetch({'market_value_points'})
    capture = _load('market_value_points')
    player_id = capture.url.rsplit('/', 1)[-1]
    plan = _plan(market_value_player_ids=(player_id,))
    run = CoreRun(replay.fetch, universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    with pytest.raises(CoreRunError, match='raw body loader'):
        run.run(plan)


def test_planned_urls_cover_every_target_kind():
    plan = _plan(
        clubs=(ClubTarget('11', 'arsenal', squad_url=squad_url('arsenal', '11', YEAR),
                          coach_history_url=coach_history_url('arsenal', '11')),),
        market_value_player_ids=('110',),
        transfer_player_ids=('110',),
        coach_profile_ids=(('42', 'mikel-arteta'),),
    )
    assert orchestrator.planned_urls(plan) == (
        squad_url('arsenal', '11', YEAR),
        coach_history_url('arsenal', '11'),
        mv_history_url('110'),
        transfers_url('110'),
        coach_profile_url('mikel-arteta', '42'),
    )


def test_coach_history_writes_the_club_scoped_stint_bundle():
    replay = _ReplayFetch({'coach_history'})
    capture = _load('coach_history')
    club_id = capture.url.split('/verein/')[1].split('/')[0]
    plan = _plan(clubs=(ClubTarget(club_id=club_id, club_slug='club',
                                   coach_history_url=capture.url),))
    run = CoreRun(replay.fetch, raw_loader=replay.raw_loader,
                  universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert report.outcomes[0]['entity'] == 'coach_stints'
    assert report.dq['verified_write_coverage'] == 1.0
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_coach_stints').fetchone()[0] == \
        report.outcomes[0]['rows']


def test_dq_reports_verified_coverage_only_after_receipts():
    replay = _ReplayFetch({'squad'})
    capture = _load('squad')
    club_id = capture.url.split('/verein/')[1].split('/')[0]
    plan = _plan(clubs=(ClubTarget(club_id=club_id, club_slug='club', squad_url=capture.url),))
    run = CoreRun(replay.fetch, raw_loader=replay.raw_loader,
                  universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    unwritten = run.run(plan)
    assert unwritten.dq['receipt_diagnostics'] == ['receipt_absent']


def test_offline_cli_acceptance_over_the_recorded_sample(tmp_path, capsys):
    from dags.scripts import run_transfermarkt_core as cli

    report_path = tmp_path / 'report.json'
    exit_code = cli.main([
        'offline', '--fixtures-root', str(FIXTURES),
        '--league', 'GB1', '--season', '2627', '--report-out', str(report_path),
    ])
    assert exit_code == 0
    report = json.loads(report_path.read_text())
    statuses = {(item['entity'], item['status']) for item in report['outcomes']}
    assert ('squad_memberships', 'observed_present') in statuses
    assert ('squad_memberships', 'observed_empty') in statuses
    assert ('market_value_points', 'observed_present') in statuses
    assert ('transfer_events', 'observed_present') in statuses
    assert ('coach_stints', 'observed_present') in statuses  # full club history now
    assert report['dq']['passed'] is True and report['dq']['errors'] == []
    assert report['receipt'] is None and report['receipts'] == []
    # The sample mixes seasons: this run owns the three 2026 squads plus the
    # season-independent career and coach captures.
    assert report['fetched'] == 19 and report['cache_hits'] == 0

    other_season = tmp_path / 'report-2223.json'
    assert cli.main([
        'offline', '--fixtures-root', str(FIXTURES),
        '--league', 'GB1', '--season', '2223', '--report-out', str(other_season),
    ]) == 0
    second = json.loads(other_season.read_text())
    squad_rows = {item['rows'] for item in second['outcomes']
                  if item['entity'] == 'squad_memberships'}
    assert squad_rows == {55}  # the 2022 Japan squad alone


def test_offline_cli_fails_fast_on_missing_planned_fixture(tmp_path):
    from dags.scripts import run_transfermarkt_core as cli

    root = tmp_path / 'fixtures'
    root.mkdir()
    (root / 'manifest.json').write_text(json.dumps({'fixtures': []}))
    with pytest.raises(SystemExit) as caught:
        cli.main(['offline', '--fixtures-root', str(root), '--league', 'GB1', '--season', '2627'])
    assert caught.value.code == 2


def test_live_cli_refuses_without_explicit_transport_permission(capsys):
    from dags.scripts import run_transfermarkt_core as cli

    with pytest.raises(SystemExit) as caught:
        cli.main(['live', '--league', 'GB1', '--season', '2627',
                  '--scope-id', 's', '--cycle-id', 'c', '--club', '11:arsenal'])
    assert caught.value.code == 2
