"""Contract rendering and packet enrichment, entirely offline."""
from dataclasses import replace
from datetime import date, datetime, timezone
import hashlib
import json
from types import SimpleNamespace

import pytest

from dags.scripts import run_transfermarkt_current as runner
from dags.utils import transfermarkt_current_write as writer
from scrapers.transfermarkt import scraper as tm, tmapi
from scrapers.transfermarkt.current_capture import FullRosterSnapshot, CurrentCaptureError
from tests.unit.scrapers.test_transfermarkt_current_capture import snapshot, club
from tests.unit.scripts.test_run_transfermarkt_current import squad, packet
from tests.unit.scrapers.test_transfermarkt_native_scope_contract import _patch_one_squad


@pytest.mark.parametrize('header,cell,status', [
    ('Contract', 'Jun 30, 2028', 'ok'), ('Contract', '-', 'unknown'),
    ('Current club', 'Club 10', 'not_rendered'), ('Contract', 'bad date', 'parse_error'),
])
def test_contract_rendering_in_both_bronze_builders(monkeypatch, header, cell, status):
    html = squad('10').replace('<th>Contract</th>', f'<th>{header}</th>').replace('Jun 30, 2028', cell)
    rows = tm._parse_squad_page(html, '10')
    scraper = tm.TransfermarktScraper()
    _patch_one_squad(monkeypatch, scraper)
    monkeypatch.setattr(tm, '_parse_squad_page', lambda *args, **kwargs: rows)
    historical = scraper.read_squad_data('GB1', 2026)
    assert historical['contract_observations'].iloc[0]['applicability_status'] == status
    saved = snapshot({'10': club(rows=rows)}, ['10'])
    saved = replace(saved, changed_club_ids=('10',))
    current = writer._roster_frames(scraper, saved, scraper._resolve_scope('GB1', 2026), 'cycle')
    assert current['contract_observations'].iloc[0]['applicability_status'] == status
    assert current['contract_observations'].iloc[0]['parser_revision'].endswith('-contracts-v2')
    if status == 'ok':
        assert current['contract_observations'].iloc[0]['contract_until'] == date(2028, 6, 30)


def scope_with_packet(*, current=True, assigned='10', contract='2028-06-30', corrupt=False):
    body = packet(['110'])
    body['data'][0]['clubAssignments'][0]['clubId'] = assigned
    body['data'][0]['attributes']['contractUntil'] = contract
    body['data'][0]['attributes']['lastContractRenewal'] = {'year': 2026, 'month': 1, 'day': 1}
    body = json.dumps(body).encode()
    url = tmapi.players_url(['110'])
    at = datetime(2026, 10, 8, tzinfo=timezone.utc)
    raw = SimpleNamespace(url=url, content_hash=hashlib.sha256(body).hexdigest(), status_code=200,
                          fetched_at=at.isoformat(), capture_id='packet-raw')
    store = SimpleNamespace(load_capture=lambda key: (body + b' ' if corrupt else body, raw))
    obs = SimpleNamespace(entity='player', entity_id='110', raw_capture_id='packet-raw', source_url=url,
                          source_body_hash=raw.content_hash, raw_fetched_at=at)
    scope = runner._Scope.__new__(runner._Scope)
    scope.row = {'is_current': current}
    scope.scope = {'record': SimpleNamespace(team_type=SimpleNamespace(value='club'))}
    scope.scraper = tm.TransfermarktScraper()
    scope.scraper._http_client._raw_store = store
    scope.signals = {'player:110': SimpleNamespace(observation=obs)}
    return scope, store


@pytest.mark.parametrize('header,status', [('Contract', 'unknown'), ('Current club', 'not_rendered')])
def test_saved_packet_fills_date_with_lineage_and_json_replay(header, status):
    scope, store = scope_with_packet()
    rows = tm._parse_squad_page(squad('10').replace('<th>Contract</th>', f'<th>{header}</th>').replace('Jun 30, 2028', '-'), '10')
    saved = replace(snapshot({'10': club(rows=rows)}, ['10']), changed_club_ids=('10',))
    enriched = scope.supplement_contracts(saved)
    assert enriched.clubs['10'].as_dict() == saved.clubs['10'].as_dict()
    restored = FullRosterSnapshot.from_mapping(json.loads(json.dumps(enriched.as_dict(), default=str)))
    from scrapers.transfermarkt.current_capture import verify_contract_overrides
    verify_contract_overrides(restored, store)
    assert restored.rows[0]['contract_until'] == '2028-06-30'
    frames = writer._roster_frames(scope.scraper, replace(restored, changed_club_ids=('10',)),
                                   scope.scraper._resolve_scope('GB1', 2026), 'cycle')
    for key in ('attribute_observations', 'contract_observations'):
        row = frames[key].iloc[0]
        assert row['contract_until'] == date(2028, 6, 30)
        assert row['source_url'] == tmapi.players_url(['110'])
        assert row['source_body_hash'] == restored.contract_overrides['10:110']['source_body_hash']
        assert row['fetched_at'] == datetime(2026, 10, 8)
    assert frames['contract_observations'].iloc[0]['applicability_status'] == status
    assert frames['memberships'].iloc[0]['source_url'] == saved.clubs['10'].source_url
    assert frames['legacy_players'].iloc[0]['contract_until'] == date(2028, 6, 30)
    bad = replace(restored, contract_overrides={'10:110': {**restored.contract_overrides['10:110'], 'contract_until': '2030-01-01'}})
    with pytest.raises(CurrentCaptureError, match='membership/date'):
        verify_contract_overrides(bad, store)


@pytest.mark.parametrize('changes', [{'current': False}, {'assigned': '20'}, {'contract': None}])
def test_packet_does_not_fill_historical_foreign_or_null(changes):
    scope, _ = scope_with_packet(**changes)
    rows = tm._parse_squad_page(squad('10').replace('Jun 30, 2028', '-'), '10')
    saved = replace(snapshot({'10': club(rows=rows)}, ['10']), changed_club_ids=('10',))
    assert not scope.supplement_contracts(saved).contract_overrides


def test_corrupt_packet_rejected_before_enrichment():
    scope, _ = scope_with_packet(corrupt=True)
    with pytest.raises(runner.CurrentPortionError, match='raw proof'):
        scope.supplement_contracts(snapshot())


def test_known_conflicting_html_date_is_never_replaced():
    scope, _ = scope_with_packet()
    saved = replace(snapshot(), changed_club_ids=('10',))
    assert not scope.supplement_contracts(saved).contract_overrides


def test_old_snapshot_replays_without_new_semantics():
    html = squad('10')
    parsed = tm._parse_squad_page(html, '10')
    legacy = [{key: value for key, value in row.items() if key != '_contract_status'} for row in parsed]
    assert tm.squad_replay_matches(html, '10', legacy)
    assert not tm.squad_replay_matches(html.replace('Jun 30, 2028', 'Jun 30, 2029'), '10', legacy)
    assert tm.contract_parser_revision(legacy[0]) == tm.PARSER_REVISION
    assert tm.contract_status({'contract_until': None}) == 'ok'  # old receipt evidence


def test_current_cycle_supplements_missing_html_contract_without_extra_http(tmp_path, monkeypatch):
    from tests.unit.scripts import test_run_transfermarkt_current as fixtures
    offline = fixtures.Offline(tmp_path, monkeypatch)
    offline.run('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-apply')
    monkeypatch.setattr(fixtures, 'squad', lambda *args, **kwargs: squad(*args, **kwargs).replace(
        '<th>Contract</th>', '<th>Current club</th>').replace('Jun 30, 2028', '-'))
    offline.player_values['110'] = 2000000
    offline.squad_values['10'] = '€2.0m'
    offline.calls.clear()
    offline.clock.advance(21 * 3600)
    offline.run('contract-change')
    offline.run('contract-apply')
    result = offline.writes[-1]
    row = next(row for row in result.rows if row['player_id'] == '110')
    assert result.clubs['10'].rows[0]['contract_until'] is None
    assert str(row['contract_until']) == '2028-06-30'
    assert row['_contract_status'] == 'not_rendered'
    assert len([url for _, url in offline.calls if '/players?' in url]) == 1
    assert len([url for _, url in offline.calls if '/kader/' in url]) == 1
    offline.run('restart')
    assert not any('source_mismatch' in line for line in offline.sql)


def test_malformed_contract_row_is_parse_error():
    html = squad('10').replace('<td>Jun 30, 2028</td>', '')
    row = tm._parse_squad_page(html, '10')[0]
    assert row['contract_until'] is None
    assert row['_contract_status'] == 'parse_error'


def test_guard_refuses_losing_previously_enriched_date():
    from scrapers.transfermarkt.current_capture import assemble_full_roster
    scope, _ = scope_with_packet()
    rows = tm._parse_squad_page(squad('10').replace('Jun 30, 2028', '-'), '10')
    saved = replace(snapshot({'10': club(rows=rows)}, ['10']), changed_club_ids=('10',))
    enriched = scope.supplement_contracts(saved)
    with pytest.raises(CurrentCaptureError, match='lost contract_until'):
        assemble_full_roster(enriched, ['10'], {'10': club(rows=rows)}, ['10'])


def test_old_unfinished_capture_is_reparsed_before_new_write(tmp_path, monkeypatch):
    from tests.unit.scripts import test_run_transfermarkt_current as fixtures
    monkeypatch.setattr(fixtures, 'squad', lambda *args, **kwargs: squad(*args, **kwargs).replace('Jun 30, 2028', '-'))
    offline = fixtures.Offline(tmp_path, monkeypatch)
    offline.stop_after_club = '10'
    offline.run('partial')
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    resume = json.loads(data['cursor']['resume_json'])
    for row in resume['captured']['10']['rows']:
        row.pop('_contract_status')
    data['cursor']['resume_json'] = json.dumps(resume)
    offline.write_state(state)
    offline.stop_after_club = None
    offline.clock.advance(60)
    offline.run('continue')
    result = offline.writes[-1]
    assert result.clubs['10'].rows[0]['_contract_status'] == 'unknown'
    assert result.clubs['10'].rows[0]['contract_until'] is None


def test_current_cycle_rejects_two_known_conflicting_dates(tmp_path, monkeypatch):
    from tests.unit.scripts import test_run_transfermarkt_current as fixtures
    offline = fixtures.Offline(tmp_path, monkeypatch)
    offline.run('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-apply')
    initial_writes = len(offline.writes)
    monkeypatch.setattr(fixtures, 'squad', lambda *args, **kwargs: squad(*args, **kwargs).replace(
        'Jun 30, 2028', 'Jun 30, 2029'))
    offline.player_values['110'] = 2000000
    offline.squad_values['10'] = '€2.0m'
    offline.clock.advance(21 * 3600)
    offline.run('conflict')
    offline.run('conflict-apply')
    assert len(offline.writes) == initial_writes
    assert any('source_mismatch' in line for line in offline.sql)


@pytest.mark.parametrize('html_contract,api_contract', [
    ('bad date', '2028-06-30'), ('Jun 30, 2028', None),
])
def test_current_cycle_rejects_contract_null_mismatch(tmp_path, monkeypatch, html_contract, api_contract):
    from tests.unit.scripts import test_run_transfermarkt_current as fixtures
    offline = fixtures.Offline(tmp_path, monkeypatch)
    offline.squad_players['10'] = ['110', '111']
    offline.run('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-apply')
    initial_writes = len(offline.writes)

    # Keep another player's date so the club completeness guard can pass.
    monkeypatch.setattr(fixtures, 'squad', lambda *args, **kwargs: squad(*args, **kwargs).replace(
        'Jun 30, 2028', html_contract, 1))

    def changed_packet(*args, **kwargs):
        body = packet(*args, **kwargs)
        for row in body['data']:
            if row['id'] == '110':
                row['attributes']['contractUntil'] = api_contract
        return body

    monkeypatch.setattr(fixtures, 'packet', changed_packet)
    offline.player_values['110'] = 2000000
    offline.squad_values['10'] = '€2.0m'
    offline.clock.advance(21 * 3600)
    offline.run('contract-change')
    report = offline.run('contract-apply')
    assert len(offline.writes) == initial_writes
    state = next(iter(offline.state()['scopes'].values()))['signals']['player:110']
    assert state['status'] == 'failed' and 'source_mismatch' in state['result']
    assert state['applied_signature'] != state['observation']['signature']
    assert any('source_mismatch' in item.get('error', '') for item in report['scopes'])
