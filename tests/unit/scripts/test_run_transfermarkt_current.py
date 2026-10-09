"""Offline orchestration through the real TM client, raw store and parsers."""
from datetime import datetime, timedelta, timezone
from dataclasses import replace
import hashlib
import json
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from dags.scripts import run_transfermarkt_current as current
from scrapers.transfermarkt.client import TransfermarktHttpClient
from scrapers.transfermarkt.models import SharedTrafficLedger
from scrapers.transfermarkt.raw_store import RawResponseStore
from scrapers.transfermarkt.scraper import TransfermarktScraper
from tests.unit.dags.test_transfermarkt_scope_planner import _competition, _edition, _joined_row
from tests.unit.scrapers.test_transfermarkt_traffic import _FakeResp


pytestmark = pytest.mark.unit


def listing(clubs=('10', '20'), values=None):
    values = values or {}
    return ('<html><table class="items"><thead><tr><th>Club</th><th>Value</th></tr></thead><tbody>'
            + ''.join(f'<tr><td class="hauptlink"><a href="/club-{club}/startseite/verein/{club}">Club {club}</a></td>'
                      f'<td>{values.get(club, "€1.0m")}</td></tr>' for club in clubs)
            + '</tbody></table></html>')


def squad(club, players=None, value='€1.0m'):
    players = players or [str(int(club) + 100)]
    return ('<html><table class="items"><thead><tr><th>Player</th><th>Date of birth/age</th><th>Nat.</th>'
            '<th>Height</th><th>Foot</th><th>Contract</th><th>Value</th></tr></thead><tbody>'
            + ''.join(f'<tr><td class="hauptlink"><a href="/p-{player}/profil/spieler/{player}">P {player}</a></td>'
                      '<td>Jan 1, 2000 (26)</td><td><img title="France"></td><td>1,80 m</td><td>right</td>'
                      f'<td>Jun 30, 2028</td><td class="rechts hauptlink">{value}</td></tr>' for player in players)
            + '</tbody></table></html>')


def packet(ids, values=None):
    values = values or {}
    return {'success': True, 'data': [
        {'id': player, 'attributes': {'contractUntil': '2028-06-30',
                                    'lastContractRenewal': {'year': None, 'month': None, 'day': None}},
         'marketValueDetails': {'current': {'value': values.get(player, 1000000), 'currency': 'EUR', 'determined': None}},
         'clubAssignments': [{'clubId': str(int(player) - 100), 'type': 'current',
                              'shirtNumber': None, 'isCaptain': False}]}
        for player in ids]}


class Clock:
    def __init__(self):
        # RawResponseStore uses this same clock via Offline's monkeypatch.
        # Continuation tests must not enter the delivery window by wall time.
        self.wall = datetime(2026, 10, 8, 18, tzinfo=timezone.utc)
        self.elapsed = 0.0

    def advance(self, seconds):
        self.elapsed += seconds
        self.wall += timedelta(seconds=seconds)


class Offline:
    def __init__(self, tmp_path, monkeypatch):
        self.root = tmp_path
        self.clock = Clock()
        self.calls, self.sql, self.writes, self.careers, self.clients = [], [], [], [], []
        self.store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
        monkeypatch.setattr('scrapers.transfermarkt.raw_store.utc_now_iso', lambda: self.clock.wall.isoformat())
        self.club_ids = ('10', '20')
        self.values, self.player_values, self.squad_values = {}, {}, {}
        self.player_dates = {}
        self.fail_scope, self.fail_club = None, None
        self.stop_after_club = None
        self.partial_careers = False
        self.bad_proof = False
        self.squad_players = {}
        self.injury_html = '<html><table class="items"><tbody></tbody></table></html>'
        self.packet_missing = False
        self.stop_in_writer = False
        self.squad_padding = {}
        self.stop_after_first_packet = False
        self.records = {}
        self.professional_clubs = {}
        self.bronze_exists = False
        self.retired_national_players = set()
        self.assignment_override = {}
        self.mv_payloads = {}
        self.unpriced_players = set()
        from tests.unit.scrapers.test_transfermarkt_signal_qualification import qualified_fixture
        self.qualification = qualified_fixture(tmp_path)
        monkeypatch.delenv('TM_PROXY_CONTROL_URL', raising=False)
        monkeypatch.setenv('TRANSFERMARKT_REQUIRE_RAW_STORE', 'false')

    def proof(self):
        return {'verified': not self.bad_proof, 'bronze_manifest': 'bronze-' + str(len(self.writes) + len(self.careers)),
                'committed_at': self.clock.wall.isoformat()}

    def factory(self, target, row, cache, deadline, used):
        competition = self.records.get(target.competition_id, _competition(target.competition_id))
        scraper = TransfermarktScraper(leagues=[target.competition_id], seasons=[2026],
                                      canonical_season=row['canonical_season'], competition_records=(competition,),
                                      response_cache=cache, resume_squad_cache=True, cache_ttl_seconds=86400)
        outer = self

        class Transport:
            def get(self, url, **kwargs):
                outer.calls.append((target.scope_id, url))
                parsed = urlsplit(url)
                if '/ceapi/marketValueDevelopment/' in parsed.path:
                    value = outer.mv_payloads.get(parsed.path.rsplit('/', 1)[-1], {'list': []})
                elif '/ceapi/transferHistory/' in parsed.path:
                    value = {'transfers': []}
                elif parsed.path == '/players':
                    value = packet(parse_qs(parsed.query)['ids[]'], outer.player_values)
                    if outer.stop_after_first_packet:
                        outer.clock.advance(2330)
                        outer.stop_after_first_packet = False
                    for row in value['data']:
                        if row['id'] in outer.unpriced_players:
                            row['marketValueDetails'] = None
                        if row['marketValueDetails'] is not None:
                            row['marketValueDetails']['current']['determined'] = outer.player_dates.get(row['id'])
                        for club, players in outer.squad_players.items():
                            if row['id'] in players:
                                row['clubAssignments'][0]['clubId'] = club
                        if competition.team_type.value == 'national_team':
                            national = row['clubAssignments'][0]['clubId']
                            row['clubAssignments'][0]['clubId'] = outer.professional_clubs.get(row['id'], '281')
                            if row['id'] not in outer.retired_national_players:
                                row['clubAssignments'].append({'clubId': national, 'type': 'nationalTeam',
                                                              'shirtNumber': None, 'isCaptain': False})
                        if row['id'] in outer.assignment_override:
                            row['clubAssignments'] = outer.assignment_override[row['id']]
                    if outer.packet_missing:
                        value['data'] = value['data'][:-1]
                elif '/verletztespieler/' in parsed.path:
                    value = outer.injury_html
                elif '/kader/' in parsed.path:
                    club = parsed.path.split('/verein/')[1].split('/')[0]
                    if outer.fail_club == club:
                        raise RuntimeError('offline source interruption')
                    if club in getattr(outer, 'squad_retry_once', set()):
                        outer.squad_retry_once.remove(club)
                        return _FakeResp(b'offline temporary unavailable', status=503)
                    value = squad(club, players=outer.squad_players.get(club), value=outer.squad_values.get(club, '€1.0m'))
                    if outer.squad_padding.get(club):
                        value = value.replace('<html>', '<html><!--' + 'x' * outer.squad_padding[club] + '-->')
                    if outer.stop_after_club == club:
                        outer.clock.advance(2350)
                elif parsed.path.startswith('/competition/'):
                    value = {'success': True, 'data': {'competitionId': target.competition_id,
                        'seasonId': int(parse_qs(parsed.query)['season'][0]), 'clubIds': list(outer.club_ids)}}
                elif '/teilnehmer/' in parsed.path:
                    value = listing(outer.club_ids, outer.values).replace('<html>', '<html><link rel="alternate" '
                        f'href="https://www.transfermarkt.com/x/teilnehmer/wettbewerb/{target.competition_id}/saison_id/2026">')
                else:
                    if outer.fail_scope == target.competition_id:
                        value = '<html>invalid layout</html>'
                    else:
                        value = listing(outer.club_ids, outer.values)
                body = json.dumps(value).encode() if isinstance(value, dict) else value.encode()
                return _FakeResp(body, json_value=value if isinstance(value, dict) else None)

            def close(self):
                pass

        scraper._http_client = TransfermarktHttpClient(
            proxy='http://offline.invalid:8000', client_factory=lambda **kwargs: Transport(),
            cache=cache, resume_squad_cache=True, raw_store=self.store, require_raw_store=True,
            request_deadline_monotonic=deadline, monotonic_fn=lambda: self.clock.elapsed,
            time_fn=lambda: self.clock.wall.timestamp(), sleep_fn=lambda seconds: self.clock.advance(seconds),
            traffic_ledger=SharedTrafficLedger(), lease_metadata={'scope': target.scope_id},
        )

        class Cursor:
            description = []
            statement = ''
            def execute(self, statement, params=()):
                outer.sql.append(statement)
                self.statement = statement
            def fetchall(self):
                if outer.bronze_exists and self.statement.startswith('SELECT 1 FROM iceberg.bronze.transfermarkt_squad_memberships'):
                    return [(1,)]
                return []
            def close(self):
                pass

        class Connection:
            _request_timeout = 30
            def cursor(self):
                return Cursor()
            def close(self):
                pass

        scraper._bronze_connection = lambda: Connection()
        self.clients.append(scraper)
        return scraper

    def roster_writer(self, scraper, snapshot, preflight, cycle_id):
        self.writes.append(snapshot)
        if self.stop_in_writer:
            self.clock.advance(2350)
        return self.proof()

    def career_writer(self, scraper, endpoint, ids, scope, preflight, cycle_id, *, decoded_body_soft_stop_bytes):
        self.careers.append((endpoint, tuple(ids), decoded_body_soft_stop_bytes))
        chosen = ids[:1] if self.partial_careers else ids
        reader = scraper.read_market_value_points if endpoint == 'market_value_points' else scraper.read_transfer_events
        reader(scope['competition_id'], int(scope['edition_id']), player_ids=chosen,
               decoded_body_soft_stop_bytes=decoded_body_soft_stop_bytes)
        window = scraper.get_career_window(endpoint)
        return {**self.proof(), 'career_window': {'processed_player_ids': window['attempted_ids'],
                                                 'deferred_player_ids': ids[len(chosen):]}}

    def coach_writer(self, scraper, scope, preflight, cycle_id, *, clubs=None):
        return {**self.proof(), 'complete': True}

    def run(self, cycle, competitions=('GB1',), max_scopes=8, **kwargs):
        rows = [_joined_row(self.records.get(competition, _competition(competition)), _edition(competition, '2026')) for competition in competitions]
        denominator = [{'competition_id': competition, 'live': True, 'competition_class': 'core_club', 'tier': 1}
                       for competition in competitions]
        return current.run_current_portion(
            rows, {'paid_io_allowed': True, 'write_mode': 'dual', 'revision': 1}, cycle,
            denominator_rows=denominator, state_dir=self.root / 'state', qualification=self.qualification,
            max_seconds=2400, max_scopes=max_scopes, scraper_factory=self.factory,
            roster_writer=self.roster_writer, career_writer=self.career_writer, coach_writer=self.coach_writer,
            now_fn=lambda: self.clock.wall, monotonic_fn=lambda: self.clock.elapsed, **kwargs,
        )

    def state(self):
        index = json.loads((self.root / 'state' / 'cursor-v1.json').read_text())
        index['scopes'] = {scope_id: json.loads((self.root / 'state' / entry['scope_file']).read_text())
                           for scope_id, entry in index['scopes'].items()}
        return index

    def write_state(self, state):
        index = {**state, 'scopes': {}}
        for scope_id, data in state['scopes'].items():
            relative = current._CurrentStore.scope_path(scope_id)
            current._atomic(self.root / 'state' / relative, data)
            index['scopes'][scope_id] = {'cursor': data.get('cursor'), 'scope_file': relative,
                                        'denominator_hash': data.get('denominator_hash')}
        current._atomic(self.root / 'state' / 'cursor-v1.json', index)


@pytest.fixture
def offline(tmp_path, monkeypatch):
    return Offline(tmp_path, monkeypatch)


@pytest.mark.parametrize('hour,minute', [(0, 30), (2, 59)])
def test_quiet_window_returns_before_client_or_state(offline, hour, minute):
    offline.clock.wall = offline.clock.wall.replace(hour=hour, minute=minute)
    report = offline.run('quiet')
    assert report['status'] == 'delivery_window'
    assert report['portion_budget_seconds'] == 0
    assert offline.calls == [] and offline.clients == []
    assert not (offline.root / 'state').exists()


def test_qualification_refuses_before_client_or_paid_io(offline):
    from tests.unit.scrapers.test_transfermarkt_signal_qualification import qualified_fixture
    offline.qualification = qualified_fixture(offline.root, changes=False)
    with pytest.raises(current.CurrentPortionError, match='same-day'):
        offline.run('unqualified')
    assert offline.calls == [] and offline.clients == []


def test_corrupt_qualification_file_refuses_before_client(offline):
    offline.qualification['evidence_sha256'] = 'a' * 64
    with pytest.raises(current.CurrentPortionError, match='hash'):
        offline.run('unqualified')
    assert offline.clients == []


def test_cold_real_raw_parser_and_career_pipeline_records_complete_roster(offline):
    report = offline.run('cold')
    assert len(offline.writes) == 1
    snapshot = offline.writes[0]
    assert snapshot.expected_team_ids == ('10', '20')
    assert len(snapshot.rows) == 2
    assert snapshot.rows[0]['height_cm'] == 180
    for club in snapshot.clubs.values():
        body, receipt = offline.store.load_capture(club.raw_capture_id)
        assert hashlib.sha256(body).hexdigest() == club.source_body_hash
        assert receipt.scope_id == snapshot.scope_id
    assert {endpoint for endpoint, _, _ in offline.careers} == {'market_value_points', 'transfer_events'}
    data = next(iter(offline.state()['scopes'].values()))
    assert data['cursor']['cold_complete']
    assert data['cursor']['resume_json'] == '{}'
    assert len(data['traffic_receipts']) == 2
    assert data['traffic_used']['requests'] > 0
    assert data['grant_cycle_id'] == 'cold'
    assert report['full_scope_completed'] is False
    assert any('transfermarkt_current_signals_v1' in sql for sql in offline.sql)


def test_missing_writer_proof_does_not_acknowledge_or_save_snapshot(offline):
    offline.bad_proof = True
    report = offline.run('bad-proof')
    assert any(scope['status'] == 'failed' for scope in report['scopes'])
    data = next(iter(offline.state()['scopes'].values()))
    assert 'snapshot' not in data
    assert all(value['status'] != 'applied' for value in data['signals'].values()
               if value['observation']['entity'] != 'injury')
    assert len(json.loads(data['cursor']['resume_json'])['captured']) == 2


def test_unchanged_listing_no_second_squad_download_after_player_baseline(offline):
    offline.run('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-apply')
    offline.calls.clear()
    previous_careers = list(offline.careers)
    offline.clock.advance(21 * 3600)
    offline.run('unchanged')
    offline.run('unchanged-resume')
    assert not any('/kader/' in url for _, url in offline.calls)
    assert offline.careers == previous_careers
    assert any('/players?' in url for _, url in offline.calls)


def test_player_signal_change_refetches_only_affected_full_club_and_value_career(offline):
    offline.run('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-apply')
    before = next(iter(offline.state()['scopes'].values()))['snapshot']['clubs']['20']
    offline.calls.clear()
    offline.careers.clear()
    offline.player_values['110'] = 2000000
    offline.squad_values['10'] = '€2.0m'
    offline.clock.advance(21 * 3600)
    offline.run('mv-change')
    offline.run('mv-apply')
    fetched = [url.split('/verein/')[1].split('/')[0] for _, url in offline.calls if '/kader/' in url]
    assert fetched == ['10']
    assert [(endpoint, ids) for endpoint, ids, _ in offline.careers] == [('market_value_points', ('110',))]
    after = next(iter(offline.state()['scopes'].values()))['snapshot']['clubs']['20']
    assert after == before
    assert len(offline.writes[-1].rows) == 2
    assert offline.writes[-1].changed_club_ids == ('10',)


@pytest.mark.parametrize('cache_version', [3, 4])
def test_interrupted_full_roster_resumes_verified_paid_pages_under48h(offline, cache_version):
    offline.stop_after_club = '10'
    first = offline.run('first')
    assert any(item['status'] == 'failed' for item in first['scopes'])
    assert offline.writes == []
    data = next(iter(offline.state()['scopes'].values()))
    original = json.loads(data['cursor']['resume_json'])['captured']['10']
    if cache_version == 3:
        state = offline.state()
        data = next(iter(state['scopes'].values()))
        resume = json.loads(data['cursor']['resume_json'])
        resume.pop('capture_proofs', None)
        data['cursor']['resume_json'] = json.dumps(resume)
        checkpoint = data['response_cache'][original['source_url']]['outcome']
        checkpoint['version'] = 3
        checkpoint.pop('raw_attempt_envelope_ids')
        checkpoint.pop('raw_attempt_envelope_id')
        offline.write_state(state)
    offline.stop_after_club = None
    offline.calls.clear()
    offline.clock.advance(3600)
    offline.run('continue')
    assert len(offline.writes) == 1
    assert not any('/verein/10/' in url and '/kader/' in url for _, url in offline.calls)
    assert offline.writes[0].clubs['10'].raw_fetched_at.isoformat() == original['raw_fetched_at']
    assert offline.writes[0].clubs['10'].raw_capture_id == original['raw_capture_id']
    data = next(iter(offline.state()['scopes'].values()))
    assert any(receipt['cache_sources'] for receipt in data['traffic_receipts'])


def test_adaptive_career_exact_remainder_survives_next_portion(offline):
    offline.partial_careers = True
    offline.run('first')
    data = next(iter(offline.state()['scopes'].values()))
    pending = json.loads(data['cursor']['resume_json'])['careers']
    assert pending == {'market_value_points': ['120'], 'transfer_events': ['120']}
    assert not data['cursor']['cold_complete']
    offline.partial_careers = False
    offline.calls.clear()
    offline.run('second')
    assert all('110' not in url for _, url in offline.calls if '/ceapi/' in url)
    assert next(iter(offline.state()['scopes'].values()))['cursor']['cold_complete']


def test_scope_failure_keeps_other_scope_write_and_traffic(offline):
    offline.fail_scope = 'GB1'
    report = offline.run('mixed', competitions=('GB1', 'FR1'))
    assert any(item['status'] == 'failed' for item in report['scopes'])
    assert len(offline.writes) == 1 and offline.writes[0].competition_id == 'FR1'
    states = offline.state()['scopes']
    assert len(states) == 2
    assert all(data['traffic_receipts'] for data in states.values())


def test_unsettled_previous_traffic_blocks_only_that_scope(offline):
    offline.run('cold')
    state = offline.state()
    next(iter(state['scopes'].values()))['in_flight'] = 'killed-process'
    offline.write_state(state)
    offline.calls.clear()
    offline.clock.advance(21 * 3600)
    report = offline.run('new')
    assert offline.calls == []
    assert all(item['status'] == 'failed' for item in report['scopes'])
    assert next(iter(offline.state()['scopes'].values()))['in_flight'] == 'killed-process'


def test_night_admission_never_constructs_client(offline):
    offline.clock.wall = offline.clock.wall.replace(hour=1)
    report = offline.run('night')
    assert report['status'] == 'delivery_window'
    assert offline.clients == []


def test_common_deadline_exhausted_before_scope_no_paid_io(offline):
    report = offline.run('too-late', deadline_monotonic=offline.clock.elapsed + 50)
    assert report['scopes'] == []
    assert offline.calls == []


def test_scope_ledger_attempt_limit_survives_entity_budget_resets():
    ledger = current._CurrentLedger({'requests': current.SCOPE_REQUEST_LIMIT - 1})
    ledger.ensure_request_allowed()
    ledger.record_attempt(entity='players', decoded_bytes=1,
                          provider_up_bytes=0, provider_down_bytes=0, retry=False, duration_seconds=0)
    from scrapers.transfermarkt.models import TrafficBudgetExceeded
    with pytest.raises(TrafficBudgetExceeded, match='within portion'):
        ledger.ensure_request_allowed()


def test_weekly_rechecks_all_squads_without_repeating_careers(offline):
    offline.run('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-apply')
    offline.calls.clear()
    offline.careers.clear()
    offline.clock.advance(7 * 24 * 3600 + 1)
    offline.run('weekly')
    assert sum('/kader/' in url for _, url in offline.calls) == 2
    assert offline.careers == []


def test_weekly_new_player_is_queued_without_career_download_in_weekly_portion(offline):
    offline.run('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-apply')
    offline.careers.clear()
    offline.squad_players['10'] = ['110', '111']
    offline.clock.advance(7 * 24 * 3600 + 1)
    offline.run('weekly')
    assert offline.careers == []
    resume = json.loads(next(iter(offline.state()['scopes'].values()))['cursor']['resume_json'])
    assert resume['careers'] == {'market_value_points': ['111'], 'transfer_events': ['111']}
    offline.run('weekly-new-player-continue')
    assert [(endpoint, ids) for endpoint, ids, _ in offline.careers] == [
        ('market_value_points', ('111',)), ('transfer_events', ('111',))]


def test_changed_listing_invalidates_unfinished_club_page_only(offline):
    offline.stop_after_club = '10'
    offline.run('interrupted')
    offline.stop_after_club = None
    offline.calls.clear()
    offline.values['10'] = '€2.0m'
    offline.squad_values['10'] = '€2.0m'
    offline.clock.advance(21 * 3600)
    offline.run('changed')
    offline.run('changed-continue')
    assert sum('/kader/verein/10/' in url for _, url in offline.calls) == 1
    assert offline.writes[-1].clubs['10'].rows[0]['market_value_eur'] == 2000000


def test_stale48h_page_is_refetched_not_promoted(offline):
    offline.stop_after_club = '10'
    offline.run('interrupted')
    offline.stop_after_club = None
    offline.calls.clear()
    offline.clock.advance(49 * 3600)
    offline.run('expired')
    offline.run('expired-continue')
    assert sum('/kader/verein/10/' in url for _, url in offline.calls) == 1


def test_partial_tmapi_packet_does_not_claim_daily_player_check(offline):
    offline.run('cold')
    offline.clock.advance(21 * 3600)
    offline.packet_missing = True
    offline.run('partial')
    data = next(iter(offline.state()['scopes'].values()))
    assert data['cursor']['players_checked_at'] is None
    assert any('uncertain' in sql and 'players' in sql for sql in offline.sql)


def test_injury_removal_updates_previous_affected_club(offline):
    offline.run('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-apply')
    offline.injury_html = '<table class="items"><tbody><tr><td><a href="/p/profil/spieler/110">P</a>ACL</td></tr></tbody></table>'
    offline.clock.advance(21 * 3600)
    offline.run('injured')
    offline.run('injured-apply')
    offline.injury_html = '<table class="items"><tbody></tbody></table>'
    offline.calls.clear()
    offline.clock.advance(21 * 3600)
    offline.run('recovered')
    offline.run('recovered-apply')
    assert sum('/kader/verein/10/' in url for _, url in offline.calls) == 1
    assert not any('/kader/verein/20/' in url for _, url in offline.calls)


def test_write_time_consumes_whole_deadline_and_preserves_pending_careers(offline):
    offline.stop_in_writer = True
    report = offline.run('write-tail')
    assert any(item['status'] == 'failed' for item in report['scopes'])
    assert offline.careers == []
    data = next(iter(offline.state()['scopes'].values()))
    assert data['snapshot']
    assert json.loads(data['cursor']['resume_json'])['careers']


def test_cold_scope_order_starts_top_five_before_lower_league(offline):
    offline.run('ordered', competitions=('BRA4', 'FR1', 'GB1'), max_scopes=6)
    assert offline.writes[0].competition_id == 'GB1'
    assert offline.writes[1].competition_id == 'FR1'
    assert offline.writes[2].competition_id == 'BRA4'


def test_mutated_retained_row_is_refused_before_source_or_warm_writer(offline):
    offline.run('cold')
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    data['snapshot']['clubs']['20']['rows'][0]['market_value_eur'] = 99000000
    offline.write_state(state)
    offline.calls.clear()
    previous_writes = len(offline.writes)
    offline.clock.advance(21 * 3600)
    report = offline.run('corrupt')
    assert all(item['status'] == 'failed' for item in report['scopes'])
    assert offline.calls == [] and len(offline.writes) == previous_writes
    assert all('retained full roster' in item['error'] for item in report['scopes'])


def test_arbitrary_manifest_string_without_structured_receipt_is_refused(offline):
    offline.run('cold')
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    data['snapshot']['clubs']['20']['bronze_manifest'] = 'pretend-commit'
    offline.write_state(state)
    offline.calls.clear()
    offline.clock.advance(21 * 3600)
    report = offline.run('corrupt')
    assert all(item['status'] == 'failed' for item in report['scopes'])
    assert offline.calls == []


def test_new_portion_grant_keeps_old_cap_receipts_and_resumes_exact_work(offline):
    offline.partial_careers = True
    offline.run('first')
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    data['traffic_used'].update(requests=current.SCOPE_REQUEST_LIMIT, retries=current.SCOPE_RETRY_LIMIT,
                                provider_metered_bytes=current.SCOPE_HARD_PROVIDER_BYTE_CAP)
    offline.write_state(state)
    offline.partial_careers = False
    report = offline.run('second')
    after = next(iter(offline.state()['scopes'].values()))
    assert after['cursor']['cold_complete']
    assert after['closed_grants'][0]['cycle_id'] == 'first'
    assert after['closed_grants'][0]['traffic']['requests'] == current.SCOPE_REQUEST_LIMIT
    assert after['closed_grants'][0]['traffic']['provider_metered_bytes'] == current.SCOPE_HARD_PROVIDER_BYTE_CAP
    assert after['grant_cycle_id'] == 'second'
    assert all(item['status'] != 'failed' for item in report['scopes'])


def test_first_matching_player_signal_baseline_does_not_redownload_roster(offline):
    offline.run('cold')
    original = next(iter(offline.state()['scopes'].values()))['roster_proof']
    offline.calls.clear()
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-continue')
    assert not any('/kader/' in url for _, url in offline.calls)
    data = next(iter(offline.state()['scopes'].values()))
    players = [state for state in data['signals'].values() if state['observation']['entity'] == 'player']
    assert players and all(state['result'] == 'baseline_verified' for state in players)
    assert all(state['committed_at'] == original['committed_at'] for state in players)


def test_market_value_date_only_queues_career_without_full_squad_download(offline):
    offline.run('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-continue')
    offline.calls.clear()
    offline.careers.clear()
    offline.player_dates['110'] = '2026-10-09'
    offline.clock.advance(21 * 3600)
    offline.run('date-change')
    offline.run('date-change-continue')
    assert not any('/kader/' in url for _, url in offline.calls)
    assert [(endpoint, ids) for endpoint, ids, _ in offline.careers] == [('market_value_points', ('110',))]


def test_actual_decoded_cap_preserves_first_page_and_next_grant_finishes_full_roster(offline):
    offline.squad_padding = {'10': 8 * 1024 * 1024, '20': 8 * 1024 * 1024}
    first = offline.run('first')
    assert any(item['status'] == 'failed' for item in first['scopes'])
    assert offline.writes == []
    before = next(iter(offline.state()['scopes'].values()))
    assert before['traffic_used']['decoded_bytes'] > 16 * 1024 * 1024
    saved = json.loads(before['cursor']['resume_json'])['captured']['10']
    assert json.loads(before['cursor']['resume_json'])['required'] == ['10', '20']
    offline.calls.clear()
    offline.clock.advance(3600)
    second = offline.run('next-grant')
    assert all(item['status'] != 'failed' for item in second['scopes'])
    assert len(offline.writes) == 1
    assert not any('/kader/verein/10/' in url for _, url in offline.calls)
    assert offline.writes[-1].clubs['10'].raw_capture_id == saved['raw_capture_id']
    assert offline.writes[-1].clubs['10'].raw_fetched_at.isoformat() == saved['raw_fetched_at']
    after = next(iter(offline.state()['scopes'].values()))
    assert after['closed_grants'][0]['traffic']['decoded_bytes'] == before['traffic_used']['decoded_bytes']


def test_large_career_queue_keeps_exact_remainder_across_new_grants(offline):
    offline.run('cold')
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    ids = [str(value) for value in range(1000, 21000)]
    data['cursor']['resume_json'] = json.dumps({'careers': {'market_value_points': ids},
        'clubs': data['listing_clubs'], 'squad_year': 2026, 'required': []})
    data['traffic_used'].update(requests=current.SCOPE_REQUEST_LIMIT,
                                provider_metered_bytes=current.SCOPE_HARD_PROVIDER_BYTE_CAP)
    offline.write_state(state)
    offline.partial_careers = True
    offline.run('portion-2')
    after = next(iter(offline.state()['scopes'].values()))
    assert json.loads(after['cursor']['resume_json'])['careers']['market_value_points'] == ids[1:]
    assert after['closed_grants'][0]['traffic']['requests'] == current.SCOPE_REQUEST_LIMIT
    offline.run('portion-3')
    after = next(iter(offline.state()['scopes'].values()))
    assert json.loads(after['cursor']['resume_json'])['careers']['market_value_points'] == ids[2:]


def test_cli_expired_job_writes_reviewable_report_without_calling_collector(tmp_path, monkeypatch):
    job = tmp_path / 'job.json'
    output = tmp_path / 'result.json'
    job.write_text(json.dumps({'cycle_id': 'expired', 'deadline_at': (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()}))
    monkeypatch.setattr(current, 'run_current_portion', lambda *args, **kwargs: pytest.fail('paid collector called after deadline'))
    assert current.main(['--job', str(job), '--report', str(output)]) == 0
    assert json.loads(output.read_text())['status'] == 'deadline'


def test_player_packets_300_and_exact_partial_cursor_resume_in_same_signal_cycle(offline):
    offline.club_ids = ('10',)
    offline.squad_players['10'] = [str(value) for value in range(101, 402)]
    offline.partial_careers = True
    offline.run('cold')
    offline.calls.clear()
    offline.stop_after_first_packet = True
    offline.clock.advance(21 * 3600)
    offline.run('first-packet')
    data = next(iter(offline.state()['scopes'].values()))
    assert data['player_check_progress']['offset'] == 300
    first_urls = [url for _, url in offline.calls if '/players?' in url]
    assert [len(parse_qs(urlsplit(url).query)['ids[]']) for url in first_urls] == [300]
    offline.calls.clear()
    offline.run('second-packet')
    urls = [url for _, url in offline.calls if '/players?' in url]
    assert [len(parse_qs(urlsplit(url).query)['ids[]']) for url in urls] == [1]
    assert 'player_check_progress' not in next(iter(offline.state()['scopes'].values()))
    assert not any('/kader/' in url for _, url in offline.calls)


def test_global_player_packet_is_paid_once_and_secondary_scope_keeps_primary_proof(offline):
    offline.run('cold', competitions=('GB1', 'FR1'))
    offline.calls.clear()
    offline.clock.advance(21 * 3600)
    report = offline.run('signals', competitions=('GB1', 'FR1'))
    assert sum('/players?' in url for _, url in offline.calls) == 1
    data = offline.state()['scopes']
    gb = next(value for value in data.values() if value['snapshot']['competition_id'] == 'GB1')
    fr = next(value for value in data.values() if value['snapshot']['competition_id'] == 'FR1')
    assert fr['checks']['players']['checked_at'] == gb['checks']['players']['checked_at']
    sources = json.loads(fr['checks']['players']['result'])['primary_packet_scopes']
    assert set(sources.values()) == {gb['cursor']['scope_id']}
    assert fr['traffic_receipts'][-1]['cache_sources']
    assert set(report['daily_complete_scope_ids']) == {gb['cursor']['scope_id'], fr['cursor']['scope_id']}


def test_pointer_cache_real_http_replay_keeps_old_squad_lineage_without_body_duplication(offline, monkeypatch):
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'outer-context')
    offline.bad_proof = True
    offline.run('failed-write')
    assert __import__('os').environ['TM_CHILD_CYCLE_ID'] == 'outer-context'
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    cache = data['response_cache']
    assert cache and all('value' not in value['outcome'] for value in cache.values())
    captured = json.loads(data['cursor']['resume_json'])['captured']
    resume = json.loads(data['cursor']['resume_json'])
    resume['captured'] = {}
    data['cursor']['resume_json'] = json.dumps(resume)
    offline.write_state(state)
    offline.bad_proof = False
    offline.calls.clear()
    offline.clock.advance(3600)
    offline.run('raw-replay')
    assert not any('/kader/' in url for _, url in offline.calls)
    assert offline.writes[-1].clubs['10'].raw_capture_id == captured['10']['raw_capture_id']
    assert offline.writes[-1].clubs['10'].raw_fetched_at.isoformat() == captured['10']['raw_fetched_at']
    assert __import__('os').environ['TM_CHILD_CYCLE_ID'] == 'outer-context'


def test_storage_v1_migrates_to_small_index_and_independent_scope_journal(offline):
    offline.run('cold')
    legacy = offline.state()
    legacy.pop('storage_version')
    current._atomic(offline.root / 'state' / 'cursor-v1.json', legacy)
    offline.run('migrated')
    index = json.loads((offline.root / 'state' / 'cursor-v1.json').read_text())
    assert index['storage_version'] == 2
    assert all(set(entry) == {'cursor', 'scope_file', 'denominator_hash'} for entry in index['scopes'].values())
    assert all(data['snapshot'] for data in offline.state()['scopes'].values())


def test_existing_bronze_scope_without_qualified_seed_is_not_recrawled(offline):
    offline.bronze_exists = True
    report = offline.run('needs-baseline')
    assert all(item['status'] == 'baseline_required' for item in report['scopes'])
    assert offline.calls == [] and offline.careers == [] and offline.writes == []


def test_real_trino_poll_chain_rechecks_shared_deadline_each_request():
    import requests
    import trino
    clock = Clock()
    seen = []
    session = requests.Session()

    def request(method, url, **kwargs):
        seen.append((method, kwargs['timeout']))
        clock.advance(4)
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps({'id': 'q', 'infoUri': 'http://offline.invalid/info',
            'nextUri': 'http://offline.invalid/next', 'stats': {'state': 'RUNNING'}, 'data': []}).encode()
        return response

    session.request = request
    connection = trino.dbapi.connect(host='offline.invalid', user='unit', http_session=session, max_attempts=4)
    current._bound_connection(connection, 40, lambda: clock.elapsed)
    with pytest.raises(current.CurrentPortionError, match='Trino query poll'):
        connection.cursor().execute('SELECT 1').fetchall()
    assert seen == [('POST', 10), ('GET', 6), ('GET', 2)]
    assert connection.max_attempts == 1


def national_record():
    from scrapers.transfermarkt.registry import CompetitionType, TeamType
    base = _competition('FIWC')
    evidence = replace(base.evidence[0], competition_type=CompetitionType.NATIONAL_TEAM_TOURNAMENT,
                       team_type=TeamType.NATIONAL_TEAM)
    return replace(base, competition_type=CompetitionType.NATIONAL_TEAM_TOURNAMENT,
                   team_type=TeamType.NATIONAL_TEAM, evidence=(evidence,))


def test_national_unchanged_roster_uses_national_assignment_and_no_fake_league_injury(offline):
    offline.records['FIWC'] = national_record()
    offline.run('cold', competitions=('FIWC',))
    offline.calls.clear()
    offline.clock.advance(21 * 3600)
    offline.run('signals', competitions=('FIWC',))
    offline.run('resume', competitions=('FIWC',))
    assert not any('/kader/' in url or '/verletztespieler/' in url for _, url in offline.calls)
    data = next(iter(offline.state()['scopes'].values()))
    assert json.loads(data['checks']['injury']['result'])['applicability_status'] == 'not_applicable'
    assert all(state['result'] == 'baseline_verified' for state in data['signals'].values()
               if state['observation']['entity'] == 'player')


def test_professional_club_transfer_queues_global_career_without_national_departure(offline):
    offline.records['FIWC'] = national_record()
    offline.run('cold', competitions=('FIWC',))
    offline.clock.advance(21 * 3600)
    offline.run('baseline', competitions=('FIWC',))
    offline.run('baseline-continue', competitions=('FIWC',))
    offline.calls.clear()
    offline.careers.clear()
    offline.professional_clubs['110'] = '999'
    offline.clock.advance(21 * 3600)
    offline.run('transfer', competitions=('FIWC',))
    offline.run('transfer-continue', competitions=('FIWC',))
    assert not any('/kader/' in url for _, url in offline.calls)
    assert [(endpoint, ids) for endpoint, ids, _ in offline.careers] == [('transfer_events', ('110',))]


def test_current_edition_turnover_changes_denominator_hash_before_any_paid_io(offline):
    first = offline.run('edition-1', deadline_monotonic=offline.clock.elapsed + 50)
    rows = [_joined_row(_competition('GB1'), _edition('GB1', '2027'))]
    second = current.run_current_portion(rows, {'paid_io_allowed': True, 'write_mode': 'dual'}, 'edition-2',
        qualification=offline.qualification, state_dir=offline.root / 'other-state',
        denominator_rows=[{'competition_id': 'GB1', 'live': True, 'competition_class': 'core_club', 'tier': 1}],
        scraper_factory=offline.factory, roster_writer=offline.roster_writer,
        career_writer=offline.career_writer, coach_writer=offline.coach_writer,
        now_fn=lambda: offline.clock.wall, monotonic_fn=lambda: offline.clock.elapsed,
        deadline_monotonic=offline.clock.elapsed + 50)
    assert first['denominator_hash'] != second['denominator_hash']
    assert first['current_targets'][0]['edition_id'] == '2026'
    assert second['current_targets'][0]['edition_id'] == '2027'
    assert offline.calls == []


def test_retired_national_assignment_does_not_cancel_global_packet_or_drop_roster(offline):
    offline.records['FIWC'] = national_record()
    offline.retired_national_players.add('110')
    offline.run('cold', competitions=('FIWC',))
    offline.calls.clear()
    offline.clock.advance(21 * 3600)
    report = offline.run('retired', competitions=('FIWC',))
    offline.run('retired-continue', competitions=('FIWC',))
    assert report['daily_complete_scope_ids']
    assert not any('/kader/' in url for _, url in offline.calls)
    data = next(iter(offline.state()['scopes'].values()))
    result = json.loads(data['checks']['players']['result'])
    assert result['national_membership_validation']['110'] == 'not_proven_by_tmapi'
    assert data['snapshot']['clubs']['10']['rows'][0]['player_id'] == '110'


def test_assignment_start_change_selects_transfer_career_even_with_same_club_ids(offline):
    offline.run('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-continue')
    offline.careers.clear()
    offline.assignment_override['110'] = [{'clubId': '10', 'type': 'current', 'shirtNumber': None,
                                           'isCaptain': False, 'start': '2026-10-08'}]
    offline.clock.advance(21 * 3600)
    offline.run('loan-ended')
    offline.run('loan-ended-continue')
    assert [(endpoint, ids) for endpoint, ids, _ in offline.careers] == [('transfer_events', ('110',))]


def test_shirt_number_change_does_not_select_transfer_or_value_career(offline):
    offline.run('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-continue')
    offline.careers.clear()
    offline.assignment_override['110'] = [{'clubId': '10', 'type': 'current', 'shirtNumber': 99, 'isCaptain': False}]
    offline.clock.advance(21 * 3600)
    offline.run('shirt')
    offline.run('shirt-continue')
    assert offline.careers == []


def test_lagging_squad_cannot_ack_player_change_and_only_bad_club_is_refetched(offline):
    offline.run('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-continue')
    writes = len(offline.writes)
    offline.player_values['110'] = 2000000
    offline.clock.advance(21 * 3600)
    offline.run('api-ahead')
    report = offline.run('api-ahead-continue')
    assert len(offline.writes) == writes
    data = next(iter(offline.state()['scopes'].values()))
    state = data['signals']['player:110']
    assert state['status'] == 'failed' and 'source_mismatch' in state['result']
    assert state['applied_signature'] != state['observation']['signature']
    assert any('source_mismatch' in item.get('error', '') for item in report['scopes'])
    assert not any('/kader/verein/10/' in url for url in data['response_cache'])
    offline.squad_values['10'] = '€2.0m'
    offline.calls.clear()
    offline.run('html-current')
    offline.run('html-current-continue')
    assert sum('/kader/verein/10/' in url for _, url in offline.calls) == 1
    assert not any('/kader/verein/20/' in url for _, url in offline.calls)
    state = next(iter(offline.state()['scopes'].values()))['signals']['player:110']
    assert state['status'] == 'applied'


def test_warm_same_value_unknown_or_changed_native_date_queues_only_mv(offline):
    offline.run('cold')
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    data['warm_baseline'] = True
    data['career_value_dates'] = {'110': '2026-08-30', '120': None}
    offline.write_state(state)
    offline.player_dates['110'] = '2026-09-30'
    offline.calls.clear()
    offline.careers.clear()
    offline.clock.advance(21 * 3600)
    offline.run('warm-date')
    offline.run('warm-date-continue')
    assert not any('/kader/' in url for _, url in offline.calls)
    assert [(endpoint, ids) for endpoint, ids, _ in offline.careers] == [('market_value_points', ('110',))]


def test_stale_nonempty_value_graph_retains_exact_pending_id_until_current_date(offline):
    offline.run('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-continue')
    offline.player_dates['110'] = '2026-09-30'
    offline.mv_payloads['110'] = {'list': [{'datum_mw': 'Aug 30, 2026', 'y': 1000000, 'mw': '€1.0m'}]}
    offline.clock.advance(21 * 3600)
    offline.run('api-new-date')
    offline.run('stale-graph')
    data = next(iter(offline.state()['scopes'].values()))
    assert json.loads(data['cursor']['resume_json'])['careers']['market_value_points'] == ['110']
    assert data['signals']['player:110']['status'] == 'failed'
    assert data['career_proofs']['market_value_points']['collector_window']['processed_player_ids'] == []
    assert not any('/marketValueDevelopment/graph/110' in url for url in data['response_cache'])
    offline.mv_payloads['110'] = {'list': [{'datum_mw': 'Sep 30, 2026', 'y': 1000000, 'mw': '€1.0m'}]}
    offline.calls.clear()
    offline.run('fresh-graph')
    data = next(iter(offline.state()['scopes'].values()))
    assert data['signals']['player:110']['status'] == 'applied'
    assert data['cursor']['resume_json'] == '{}'
    assert sum('/marketValueDevelopment/graph/110' in url for _, url in offline.calls) == 1


def test_warm_unpriced_player_with_old_value_history_does_not_redownload_history(offline):
    offline.squad_values['10'] = '-'
    offline.unpriced_players.add('110')
    offline.run('cold')
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    data['warm_baseline'] = True
    data['career_value_dates'] = {'110': '2002-08-30', '120': None}
    offline.write_state(state)
    offline.calls.clear()
    offline.careers.clear()
    offline.clock.advance(21 * 3600)
    offline.run('warm-unpriced')
    offline.run('warm-unpriced-continue')
    assert offline.careers == []
    assert not any('/kader/' in url for _, url in offline.calls)


def test_initial_empty_injury_signal_has_explicit_raw_durable_ack(offline):
    offline.run('cold')
    data = next(iter(offline.state()['scopes'].values()))
    signal = data['signals']['injury:GB1']
    assert signal['status'] == 'applied' and signal['result'] == 'raw_durable'
    assert signal['bronze_manifest'].startswith('raw:')
    _, raw = offline.store.load_capture(signal['bronze_manifest'][4:])
    assert raw.endpoint == 'current_injury'


def test_warm_missing_membership_signature_is_baseline_not_new_change(offline):
    offline.run('cold')
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    data['signals'].pop('listing:GB1')
    offline.write_state(state)
    offline.calls.clear()
    offline.clock.advance(21 * 3600)
    offline.run('warm-listing')
    offline.run('warm-listing-continue')
    data = next(iter(offline.state()['scopes'].values()))
    assert data['signals']['listing:GB1']['result'] == 'baseline_verified'
    assert not any('/kader/' in url for _, url in offline.calls)
    assert data['cursor']['resume_json'] == '{}'


def test_participant_only_removal_updates_memberships_without_squad_or_career_refetch(offline):
    offline.club_ids = tuple(str(club) for club in range(10, 20))
    offline.run('cold')
    offline.clock.advance(21 * 3600)
    offline.run('baseline')
    offline.run('baseline-continue')
    offline.club_ids = offline.club_ids[:-1]
    offline.calls.clear()
    offline.careers.clear()
    offline.clock.advance(21 * 3600)
    offline.run('participants')
    offline.run('participants-continue')
    assert not any('/kader/' in url for _, url in offline.calls)
    assert offline.careers == []
    assert offline.writes[-1].expected_team_ids == tuple(str(club) for club in range(10, 19))
    assert offline.writes[-1].changed_club_ids == ()


def test_cli_absolute_alarm_interrupts_blocking_work_and_writes_report(tmp_path):
    import subprocess
    import sys
    import time
    job, report = tmp_path / 'job.json', tmp_path / 'report.json'
    job.write_text(json.dumps({'cycle_id': 'blocking', 'deadline_at': (datetime.now(timezone.utc) + timedelta(seconds=7)).isoformat(),
                              'registry_rows': [], 'preflight': {}}))
    program = '''from dags.scripts import run_transfermarkt_current as c
import sys,time
c.available_work_seconds=lambda *args: 2700
c.run_current_portion=lambda *args, **kwargs: time.sleep(10)
raise SystemExit(c.main(['--job',sys.argv[1],'--report',sys.argv[2]]))
'''
    started = time.monotonic()
    result = subprocess.run([sys.executable, '-c', program, str(job), str(report)], capture_output=True, timeout=9)
    assert result.returncode == 1 and time.monotonic() - started < 5
    assert 'absolute deadline' in json.loads(report.read_text())['fatal_error']


def test_pending_rows_are_bound_to_actual_raw_parse(offline):
    offline.stop_after_club = '10'
    offline.run('interrupted')
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    resume = json.loads(data['cursor']['resume_json'])
    resume['captured']['10']['rows'][0]['height_cm'] = 999
    data['cursor']['resume_json'] = json.dumps(resume)
    offline.write_state(state)
    offline.stop_after_club = None
    offline.calls.clear()
    report = offline.run('forged')
    assert offline.writes == []
    assert any('parsed rows differ' in item.get('error', '') for item in report['scopes'])
    assert not any('/kader/' in url for _, url in offline.calls)


@pytest.mark.parametrize('mutation', ['missing_envelopes', 'wrong_count', 'foreign_envelope'])
def test_pending_squad_validates_actual_attempt_chain(offline, mutation):
    offline.stop_after_club = '10'
    offline.run('interrupted')
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    resume = json.loads(data['cursor']['resume_json'])
    checkpoint = resume['capture_proofs']['10']['outcome']
    if mutation == 'missing_envelopes':
        checkpoint['raw_attempt_envelope_ids'] = []
        checkpoint['raw_attempt_envelope_id'] = None
    elif mutation == 'wrong_count':
        checkpoint['attempts'] += 1
    else:
        listing_checkpoint = next(value['outcome'] for url, value in data['response_cache'].items() if '/kader/' not in url)
        checkpoint['raw_attempt_envelope_ids'] = listing_checkpoint['raw_attempt_envelope_ids']
    data['cursor']['resume_json'] = json.dumps(resume)
    offline.write_state(state)
    offline.stop_after_club = None
    offline.calls.clear()
    report = offline.run('forged-chain')
    assert offline.writes == []
    assert any(item['status'] == 'failed' for item in report['scopes'])
    assert not any('/kader/' in url for _, url in offline.calls)


def test_departed_participant_is_pruned_from_unfinished_capture(offline):
    offline.stop_after_club = '10'
    offline.run('interrupted')
    offline.stop_after_club = None
    offline.club_ids = ('20',)
    state = offline.state()
    state['turn'] = 0  # exercise a fresh listing before unfinished roster work
    offline.write_state(state)
    offline.clock.advance(21 * 3600)
    offline.calls.clear()
    offline.run('participants-changed')
    offline.run('participants-continue')
    assert len(offline.writes) == 1
    assert offline.writes[0].expected_team_ids == ('20',)
    assert all('/verein/10/' not in url for _, url in offline.calls)


def test_verified_replacement_is_durable_before_ops_ack_failure(offline, monkeypatch):
    original = current._Scope.acknowledge
    failed = False
    def fail_once(scope, proof, **kwargs):
        nonlocal failed
        if scope.snapshot is not None and not failed:
            failed = True
            raise RuntimeError('lost ops ack')
        return original(scope, proof, **kwargs)
    monkeypatch.setattr(current._Scope, 'acknowledge', fail_once)
    offline.run('lost-ops-ack')
    data = next(iter(offline.state()['scopes'].values()))
    assert data['snapshot'] and data['roster_receipts']
    assert 'roster_write_intent' not in data
    assert any(value['status'] != 'applied' for value in data['signals'].values())
    offline.calls.clear()
    offline.run('ack-retry')
    assert len(offline.writes) == 1
    assert not any('/kader/' in url for _, url in offline.calls)
    data = next(iter(offline.state()['scopes'].values()))
    assert data['cursor']['cold_complete']


@pytest.mark.parametrize('recovery_hours', [0, 49])
def test_physical_replacement_lost_readback_recovers_original_write_intent(offline, monkeypatch, recovery_hours):
    import sqlite3
    from dags.utils import transfermarkt_current_write as write
    from tests.unit.utils.test_transfermarkt_current_write import MemoryDB, MemoryWriter
    db = MemoryDB()
    factory = offline.factory
    original_readback = write._read_back
    injected = False
    write_cycles = []

    def physical_factory(*args):
        scraper = factory(*args)
        scraper._bronze_connection = lambda: db
        scraper._iceberg_writer = MemoryWriter(db)
        scraper._batch_id = args[0].scope_id + '-physical-' + str(len(offline.clients))
        def count(database, table, deletion, key):
            try:
                return db.sql.execute(f'SELECT COUNT(DISTINCT {key}) FROM {table} WHERE {deletion}').fetchone()[0]
            except sqlite3.OperationalError as exc:
                if 'no such table' in str(exc):
                    return 0
                raise
        scraper._count_existing_partition = count
        return scraper

    def roster_writer(scraper, snapshot, preflight, cycle):
        write_cycles.append((cycle, scraper._batch_id))
        proof = write.write_current_roster(scraper, snapshot, preflight, cycle)
        # Keep the injected clock consistent with source observation/ack time.
        return {**proof, 'committed_at': offline.clock.wall.isoformat()}

    def readback(*args, **kwargs):
        nonlocal injected
        if args[3] == 'changed' and not injected:
            injected = True
            raise write.CurrentWriteError('transient readback failure')
        return original_readback(*args, **kwargs)

    # Signal/control SQL is orthogonal to the real physical Bronze writer;
    # its exact SQL contract is exercised by the other runner tests.
    monkeypatch.setattr(current, '_sql', lambda scraper, statement: [])
    monkeypatch.setattr(current, '_load_ops_signals', lambda *args: [])
    monkeypatch.setattr(current, '_existing_bronze_roster', lambda *args: False)
    monkeypatch.setattr(write.run, '_authorize_write_mode', lambda mode, revision: {'write_mode': mode, 'expected_revision': revision})
    monkeypatch.setattr(write, '_read_back', readback)
    offline.factory, offline.roster_writer = physical_factory, roster_writer
    offline.run('baseline', baseline_verifier=write.verify_current_roster_baseline)
    before = next(iter(offline.state()['scopes'].values()))
    assert before['snapshot']
    offline.values['10'], offline.squad_values['10'], offline.player_values['110'] = '€2.0m', '€2.0m', 2000000
    offline.clock.advance(21 * 3600)
    offline.run('changed', baseline_verifier=write.verify_current_roster_baseline)
    offline.run('changed', baseline_verifier=write.verify_current_roster_baseline)
    pending = next(iter(offline.state()['scopes'].values()))
    assert injected and pending['roster_write_intent']['write_cycle'] == 'changed'
    assert db.sql.execute("SELECT market_value_eur FROM transfermarkt_players WHERE current_club_id='10'").fetchone()[0] == '2000000'
    with pytest.raises(write.CurrentWriteError):
        write.verify_current_roster_baseline(offline.clients[-1], current.FullRosterSnapshot.from_mapping(before['snapshot']),
                                            before['roster_receipts'], {'write_mode': 'dual', 'revision': 1})
    offline.clock.advance(recovery_hours * 3600)
    offline.calls.clear()
    offline.run('recover', max_scopes=1, baseline_verifier=write.verify_current_roster_baseline)
    after = next(iter(offline.state()['scopes'].values()))
    assert 'roster_write_intent' not in after
    assert after['snapshot']['clubs']['10']['rows'][0]['market_value_eur'] == 2000000
    assert write_cycles[-1] == write_cycles[-2]  # original cycle and physical batch
    assert offline.calls == []
    assert after['recovery_signal_refresh_required']['required_checks'] == ['listing', 'players', 'injury']
    assert all(after['cursor'][name + '_checked_at'] is None for name in ('listing', 'players', 'injury'))
    assert after['snapshot']['clubs']['10']['raw_fetched_at'] == pending['roster_write_intent']['candidate']['clubs']['10']['raw_fetched_at']
    assert after['roster_proof']['reconciliation']['source_ages']['10']['source_age_seconds'] >= recovery_hours * 3600
    verified = write.verify_current_roster_baseline(offline.clients[-1], current.FullRosterSnapshot.from_mapping(after['snapshot']),
                                                   after['roster_receipts'], {'write_mode': 'dual', 'revision': 1})
    assert verified['verified']
    state = offline.state()
    state['turn'] = 0
    offline.write_state(state)
    offline.run('fresh-after-repair', baseline_verifier=write.verify_current_roster_baseline)
    after = next(iter(offline.state()['scopes'].values()))
    assert 'recovery_signal_refresh_required' not in after
    assert all(after['cursor'][name + '_checked_at'] is not None for name in ('listing', 'players', 'injury'))
    assert len(write_cycles) == 3  # baseline, failed replacement, identical repair only


def test_old_complete_write_intent_repairs_without_source_cache_or_paid_get(offline, monkeypatch):
    offline.bad_proof = True
    offline.run('lost-ack')
    offline.bad_proof = False
    offline.clock.advance(49 * 3600)
    offline.calls.clear()
    state = offline.state()
    state['turn'] = 1
    data = next(iter(state['scopes'].values()))
    data['response_cache'] = {}  # complete intent carries its own original v4 proof
    original_fetched = data['roster_write_intent']['candidate']['clubs']['10']['raw_fetched_at']
    offline.write_state(state)
    monkeypatch.setattr(TransfermarktHttpClient, '_load_cached_outcome',
                        lambda *args, **kwargs: pytest.fail('reconciliation is not source cache continuation'))
    report = offline.run('old-complete-repair', max_scopes=1)
    assert offline.calls == []
    data = next(iter(offline.state()['scopes'].values()))
    assert 'roster_write_intent' not in data and data['snapshot']
    assert data['snapshot']['clubs']['10']['raw_fetched_at'] == original_fetched
    assert data['recovery_signal_refresh_required']
    proof = report['scopes'][0]['roster_reconciliations'][0]
    assert proof['original_write_cycle'] == 'lost-ack'
    assert proof['source_ages']['10']['source_age_seconds'] >= 49 * 3600
    assert all(data['cursor'][name + '_checked_at'] is None for name in ('listing', 'players', 'injury'))
    assert json.loads(data['cursor']['resume_json'])['careers']


@pytest.mark.parametrize('mutation', ['rows', 'raw_hash', 'attempts', 'foreign_scope', 'intent_hash', 'preflight', 'partial'])
def test_old_complete_write_intent_rejects_forged_proof_before_paid_or_write(offline, mutation):
    offline.bad_proof = True
    offline.run('lost-ack')
    offline.bad_proof = False
    offline.clock.advance(49 * 3600)
    state = offline.state()
    state['turn'] = 1
    data = next(iter(state['scopes'].values()))
    intent = data['roster_write_intent']
    club = intent['candidate']['clubs']['10']
    if mutation == 'rows':
        club['rows'][0]['height_cm'] = 999
    elif mutation == 'raw_hash':
        club['source_body_hash'] = 'f' * 64
    elif mutation == 'attempts':
        other = intent['capture_proofs']['20']['outcome']
        outcome = intent['capture_proofs']['10']['outcome']
        outcome['raw_attempt_envelope_ids'] = other['raw_attempt_envelope_ids']
        outcome['raw_attempt_envelope_id'] = other['raw_attempt_envelope_id']
    elif mutation == 'foreign_scope':
        intent['candidate']['scope_id'] = 'foreign-scope'
    elif mutation == 'preflight':
        intent['write_preflight']['revision'] += 1
    elif mutation == 'partial':
        intent['changed_club_ids'] = ['10']
    intent['intent_hash'] = current.semantic_signature({key: value for key, value in intent.items() if key != 'intent_hash'})
    if mutation == 'intent_hash':
        intent['intent_hash'] = 'f' * 64
    offline.write_state(state)
    offline.calls.clear()
    writes = len(offline.writes)
    report = offline.run('forged-old-intent', max_scopes=1)
    assert report['scopes'][0]['status'] == 'failed'
    assert offline.calls == [] and len(offline.writes) == writes
    assert next(iter(offline.state()['scopes'].values()))['roster_write_intent']


def test_cold_write_intent_recovery_precedes_existing_bronze_seed_gate(offline):
    offline.bad_proof = True
    offline.run('cold-write-lost-ack')
    offline.bad_proof = False
    offline.bronze_exists = True
    offline.calls.clear()
    report = offline.run('cold-write-retry')
    data = next(iter(offline.state()['scopes'].values()))
    assert data['snapshot'] and 'roster_write_intent' not in data
    assert all(item['status'] != 'baseline_required' for item in report['scopes'])
    assert not any('/kader/' in url for _, url in offline.calls)


def test_recovered_write_does_not_ack_newer_observed_signature(offline):
    offline.bad_proof = True
    offline.run('failed-original')
    state = offline.state()
    state['turn'] = 1
    data = next(iter(state['scopes'].values()))
    previous = current.SignalState.from_mapping(data['signals']['club:10'])
    newer = current.mark_seen(previous, replace(previous.observation, signature='f' * 64,
                                               checked_at=offline.clock.wall + timedelta(seconds=1)))
    data['signals']['club:10'] = newer.as_dict()
    offline.write_state(state)
    offline.bad_proof = False
    offline.run('recover-original', max_scopes=1)
    data = next(iter(offline.state()['scopes'].values()))
    assert data['signals']['club:10']['observation']['signature'] == 'f' * 64
    assert data['signals']['club:10']['status'] != 'applied'
    assert json.loads(data['cursor']['resume_json'])['required'] == ['10']


def _legacy_retry_intent(offline, *, keep_captured=True):
    offline.clock.wall = offline.clock.wall.replace(hour=10, minute=0, second=0)
    from tests.unit.scrapers.test_transfermarkt_traffic import _manager
    factory = offline.factory
    def with_rotating_fake_proxies(*args):
        scraper = factory(*args)
        scraper._http_client._explicit_proxy = None
        scraper._http_client._proxy_manager = _manager()
        return scraper
    offline.factory = with_rotating_fake_proxies
    offline.squad_retry_once = {'10'}
    offline.stop_after_club = '10'
    offline.run('authentic-retry')
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    resume = json.loads(data['cursor']['resume_json'])
    saved = resume['captured']['10']
    checkpoint = data['response_cache'][saved['source_url']]['outcome']
    assert checkpoint['attempts'] == 2 and len(checkpoint['raw_attempt_envelope_ids']) == 2
    original_ids = list(checkpoint['raw_attempt_envelope_ids'])
    statuses = [offline.store.load_attempt_envelope(key).status_code for key in original_ids]
    assert statuses == [503, 200]
    checkpoint['version'] = 3
    checkpoint.pop('raw_attempt_envelope_ids')
    checkpoint.pop('raw_attempt_envelope_id')
    resume.pop('capture_proofs', None)
    if not keep_captured:
        resume['captured'] = {}
    data['cursor']['resume_json'] = json.dumps(resume)
    offline.write_state(state)
    offline.stop_after_club = None
    offline.clock.advance(3600)
    offline.calls.clear()
    offline.bad_proof = True
    offline.run('legacy-continue')
    assert not any('/kader/verein/10/' in url for _, url in offline.calls)
    state = offline.state()
    data = next(iter(state['scopes'].values()))
    checkpoint = data['roster_write_intent']['capture_proofs']['10']
    assert checkpoint['outcome']['version'] == 3 and checkpoint['outcome']['attempts'] == 2
    assert not {'raw_attempt_envelope_ids', 'raw_attempt_envelope_id'} & checkpoint['outcome'].keys()
    assert checkpoint['legacy_proof'] == {
        'kind': 'legacy_v3_success_response', 'original_attempt_count': 2,
        'evidenced_attempt_count': 1, 'prefix_unknown': True,
        'raw_attempt_envelope_ids': [original_ids[-1]]}
    assert any(row['cache_sources'] for row in data['traffic_receipts'])
    return state


@pytest.mark.parametrize('keep_captured', [True, False])
def test_authentic_legacy_retry_resume_and_complete_intent_reconcile_without_count_loss(offline, keep_captured):
    state = _legacy_retry_intent(offline, keep_captured=keep_captured)
    state['turn'] = 1
    offline.write_state(state)
    offline.bad_proof = False
    offline.clock.advance(49 * 3600)
    offline.calls.clear()
    offline.run('legacy-reconcile', max_scopes=1)
    data = next(iter(offline.state()['scopes'].values()))
    assert 'roster_write_intent' not in data and data['snapshot']
    assert offline.calls == []
    source = data['roster_proof']['reconciliation']['source_ages']['10']
    assert source['proof_kind'] == 'legacy_v3_success_response'
    assert source['original_attempt_count'] == 2 and source['evidenced_attempt_count'] == 1
    assert source['prefix_unknown'] is True
    assert source['source_age_seconds'] >= 49 * 3600
    assert data['recovery_signal_refresh_required']


@pytest.mark.parametrize('mutation', ['foreign_envelope', 'count', 'kind', 'missing', 'v4_count_mismatch'])
def test_legacy_retry_sidecar_and_modern_chain_corruption_refused(offline, mutation):
    state = _legacy_retry_intent(offline)
    state['turn'] = 1
    data = next(iter(state['scopes'].values()))
    intent = data['roster_write_intent']
    proof = intent['capture_proofs']['10']
    if mutation == 'foreign_envelope':
        proof['legacy_proof']['raw_attempt_envelope_ids'] = intent['capture_proofs']['20']['outcome']['raw_attempt_envelope_ids']
    elif mutation == 'count':
        proof['legacy_proof']['original_attempt_count'] = 1
    elif mutation == 'kind':
        proof['legacy_proof']['kind'] = 'complete_v4'
    elif mutation == 'missing':
        proof.pop('legacy_proof')
    else:
        proof['outcome']['version'] = 4
        proof['outcome']['raw_attempt_envelope_ids'] = proof['legacy_proof']['raw_attempt_envelope_ids']
        proof['outcome']['raw_attempt_envelope_id'] = proof['legacy_proof']['raw_attempt_envelope_ids'][-1]
    intent['intent_hash'] = current.semantic_signature({key: value for key, value in intent.items() if key != 'intent_hash'})
    offline.write_state(state)
    offline.bad_proof = False
    offline.clock.advance(49 * 3600)
    offline.calls.clear()
    writes = len(offline.writes)
    report = offline.run('legacy-forged', max_scopes=1)
    assert report['scopes'][0]['status'] == 'failed'
    assert offline.calls == [] and len(offline.writes) == writes
    assert next(iter(offline.state()['scopes'].values()))['roster_write_intent']
