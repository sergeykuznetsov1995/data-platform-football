"""Verified current squads can finish across bounded daily attempts."""
from datetime import date, datetime, timezone
import json
import time

import pytest

import scrapers.transfermarkt.scraper as tm
from scrapers.transfermarkt.client import TransfermarktHttpClient
from scrapers.transfermarkt.models import TrafficBudgetExceeded
from scrapers.transfermarkt.raw_store import RawResponseStore, RawStoreError
from tests.unit.scrapers.test_transfermarkt_traffic import (
    _ClientFactory, _FakeResp, _NoWaitLimiter, _manager,
)
from tests.unit.scrapers.test_transfermarkt_participants import BRC
from tests.unit.scrapers.test_run_transfermarkt_scraper import _import_runner


pytestmark = pytest.mark.unit
URL = 'https://www.transfermarkt.com/team/kader/verein/1/saison_id/2025/plus/1'
CONTEXT = {'scope': 'BRC:2025'}


def _client(store, cache, bodies, *, resume=True, clock=None):
    factory = _ClientFactory(bodies)
    client = TransfermarktHttpClient(
        proxy_manager=_manager(3), raw_store=store, require_raw_store=True,
        cache=cache, resume_squad_cache=resume, client_factory=factory,
        rate_limiter=_NoWaitLimiter(), sleep_fn=lambda _: None,
        time_fn=clock or time.time,
    )
    return client, factory


def _fetch(client, *, url=URL, label='squad', context=CONTEXT):
    return client.fetch(url, as_json=False, label=label, context=context,
                        cache_key=url, cache_ttl_seconds=86400)


def test_verified_squad_resume_keeps_original_lineage_and_separate_attempts(tmp_path, monkeypatch):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    cache = {}
    now = time.time()
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-A')
    first, _ = _client(store, cache, [_FakeResp(b'full squad')], clock=lambda: now)
    paid = _fetch(first)
    expires = cache[URL]['expires_at']
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-B')
    second, factory = _client(store, cache, [], clock=lambda: now + 25 * 3600)
    replay = _fetch(second)
    assert replay.cache_hit and replay.value == 'full squad'
    assert replay.raw_capture_id == paid.raw_capture_id
    assert replay.raw_fetched_at == paid.raw_fetched_at
    assert cache[URL]['expires_at'] == expires
    assert factory.calls == []
    assert second.get_raw_attempt_records() == ()
    sources = second.get_cache_source_records()
    assert len(sources) == 1 and sources[0]['cycle_id'] == 'child-A'
    assert sources[0]['envelope_id'] == paid.raw_attempt_envelope_id
    assert second.get_traffic_stats()['requests'] == 0
    assert second.get_traffic_stats()['decoded_response_body_bytes'] == 0


@pytest.mark.parametrize('reason', ['expired', 'boundary', 'future', 'scope', 'url', 'disabled', 'listing', 'missing_raw', 'no_child', 'no_scope'])
def test_squad_resume_rejects_ineligible_cache(tmp_path, monkeypatch, reason):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    cache = {}
    now = time.time()
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-A')
    first, _ = _client(store, cache, [_FakeResp(b'old')], clock=lambda: now)
    _fetch(first)
    # The immutable capture time must bound reuse even if cache expiry drifts.
    cache[URL]['expires_at'] = now + 10 * 86400
    if reason == 'url':
        cache[URL + '/wrong'] = cache.pop(URL)
    if reason == 'missing_raw':
        cache[URL]['outcome']['raw_capture_id'] = None
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-B')
    if reason == 'no_child':
        monkeypatch.delenv('TM_CHILD_CYCLE_ID')
    fetched_at = datetime.fromisoformat(cache.get(URL, cache.get(URL + '/wrong'))['outcome']['raw_fetched_at']).timestamp()
    target_time = fetched_at + 48 * 3600 if reason == 'boundary' else (
        now + 48 * 3600 + 1 if reason == 'expired' else (now - 60 if reason == 'future' else now + 3600)
    )
    second, factory = _client(store, cache, [_FakeResp(b'fresh')],
                              resume=reason != 'disabled', clock=lambda: target_time)
    result = _fetch(second, url=URL + '/wrong' if reason == 'url' else URL,
                    label='listing' if reason == 'listing' else 'squad',
                    context=({'scope': 'BRC:2024'} if reason == 'scope'
                             else ({} if reason == 'no_scope' else CONTEXT)))
    assert not result.cache_hit and result.value == 'fresh'
    assert len(factory.clients[0].get_calls) == 1
    assert second.get_cache_source_records() == ()


def test_squad_resume_fails_closed_on_corrupt_raw(tmp_path, monkeypatch):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    cache = {}
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-A')
    first, _ = _client(store, cache, [_FakeResp(b'old')])
    _fetch(first)
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-B')
    monkeypatch.setattr(store, 'load_capture', lambda _: (_ for _ in ()).throw(RawStoreError('corrupt')))
    second, factory = _client(store, cache, [])
    with pytest.raises(RawStoreError, match='corrupt'):
        _fetch(second)
    assert factory.calls == []


def test_current_resume_does_not_extend_default_same_cycle_cache_ttl(tmp_path, monkeypatch):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    cache = {}
    now = time.time()
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-A')
    monkeypatch.setenv('AIRFLOW_CTX_TRY_NUMBER', '1')
    first, _ = _client(store, cache, [_FakeResp(b'old')], clock=lambda: now)
    _fetch(first)
    assert cache[URL]['expires_at'] == now + 86400
    monkeypatch.setenv('AIRFLOW_CTX_TRY_NUMBER', '2')
    force, _ = _client(store, cache, [_FakeResp(b'fresh')], resume=False,
                       clock=lambda: now + 25 * 3600)
    result = _fetch(force)
    assert not result.cache_hit and result.value == 'fresh'
    assert force.get_traffic_stats()['requests'] == 1


def test_prior_squad_retry_chain_is_exported_as_cache_sources(tmp_path, monkeypatch):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    cache = {}
    now = time.time()
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-A')
    first, _ = _client(store, cache, [_FakeResp(b'blocked', status=405),
                                     _FakeResp(b'full squad')], clock=lambda: now)
    paid = _fetch(first)
    assert paid.attempts == 2
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-B')
    second, factory = _client(store, cache, [], clock=lambda: now + 25 * 3600)
    replay = _fetch(second)
    assert replay.cache_hit and replay.attempts == 0 and not factory.calls
    assert second.get_raw_attempt_records() == ()
    sources = second.get_cache_source_records()
    assert {s['envelope_id'] for s in sources} == set(paid.raw_attempt_envelope_ids)
    assert {s['status_code'] for s in sources} == {200, 405}
    assert {s['cycle_id'] for s in sources} == {'child-A'}
    assert second.get_traffic_stats()['requests'] == 0


@pytest.mark.parametrize(('entity', 'mode', 'dag', 'enabled'), [
    ('players', 'current', 'dag_ingest_transfermarkt', True),
    ('players', 'historical', 'dag_ingest_transfermarkt', False),
    ('players', 'force', 'dag_ingest_transfermarkt', False),
    ('players', 'current', 'dag_backfill_transfermarkt', False),
    ('players', 'current', 'manual', False),
])
def test_runner_gates_resume_and_never_writes_after_budget_failure(tmp_path, monkeypatch, entity, mode, dag, enabled):
    import scrapers.transfermarkt as package
    runner = _import_runner()
    cache_path = tmp_path / 'cache.json'
    monkeypatch.setenv('TM_DAG_ID', dag)
    monkeypatch.setenv('TM_RESPONSE_CACHE_PATH', str(cache_path))
    monkeypatch.setenv('TM_RESPONSE_CACHE_TTL_SECONDS', '86400')
    monkeypatch.delenv('TM_REQUIRE_METERED_PROXY', raising=False)
    monkeypatch.delenv('TRANSFERMARKT_REQUIRE_RAW_STORE', raising=False)
    scraper = tm.TransfermarktScraper()
    received = {}
    def construct(**kwargs):
        received.update(kwargs)
        kwargs['response_cache']['paid-page'] = {'saved': True}
        return scraper
    monkeypatch.setattr(package, 'TransfermarktScraper', construct)
    def fail(*a, **k):
        raise TrafficBudgetExceeded('decoded-body budget exhausted')
    monkeypatch.setattr(runner, '_read_frames', fail)
    writes = []
    monkeypatch.setattr(runner, '_save_frames', lambda *a, **k: writes.append(a))
    result_path = tmp_path / 'result.json'
    rc = runner._run_entity(runner.ENTITY_SPECS[entity], ['GB1'], 2025, None,
                            str(result_path), refresh_mode=mode, write_mode='dual')
    assert rc == 1 and writes == []
    assert received['resume_squad_cache'] is enabled
    assert json.loads(cache_path.read_text())['paid-page'] == {'saved': True}
    report = json.loads(result_path.read_text())
    assert report['native_write_complete'] is False
    assert report['cache_sources'] == []


def _squad(club_id):
    # One player appears at two clubs; both memberships must survive.
    pid = '1000' if int(club_id) < 3 else str(1000 + int(club_id))
    headers = ['Player', 'Date of birth/Age', 'Nat.', 'Height', 'Foot', 'Contract', 'Market value']
    cells = (
        '<td class="posrela"><table class="inline-table"><tr>'
        f'<td class="hauptlink"><a href="/player/profil/spieler/{pid}">Player</a></td>'
        '</tr><tr><td>Goalkeeper</td></tr></table></td>'
        '<td>Sep 15, 1995 (30)</td><td><img title="Spain"/></td>'
        '<td>1,86m</td><td>right</td><td>Jun 30, 2028</td>'
        '<td class="rechts hauptlink">€30.00m</td>'
    )
    return ('<table class="items"><thead><tr>' + ''.join(f'<th>{h}</th>' for h in headers)
            + '</tr></thead><tbody><tr>' + cells + '</tr></tbody></table>').encode()


def test_brc_2025_full_roster_finishes_after_exact_decoded_cap_failure(tmp_path, monkeypatch):
    """Real roster reader + raw store + transport; no source or Bronze I/O."""
    clubs = [{'club_id': str(i), 'club_slug': f'team-{i}', 'club_name': f'Team {i}'}
             for i in range(1, 127)]
    monkeypatch.setattr(tm, '_parse_participant_table', lambda _: clubs)
    monkeypatch.setattr(tm, '_participant_page_is_for', lambda *a: True)
    payload = json.dumps({'success': True, 'data': {'competitionId': 'BRC',
                          'seasonId': 2025, 'clubIds': [c['club_id'] for c in clubs]}},
                         separators=(',', ':')).encode()
    listing = b'<table class="items"><a href="/team/startseite/verein/1">Team</a></table>'
    def pad(body, n):
        assert len(body) <= n
        return body + b' ' * (n - len(body))
    base, remainder = divmod(16759513, 102)
    squad_sizes = [base + (i < remainder) for i in range(102)] + [base] * 24
    squad_responses = [_FakeResp(pad(_squad(c['club_id']), n)) for c, n in zip(clubs, squad_sizes)]
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    cache = {}
    now = time.time()
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-A')
    first, _ = _client(store, cache, [
        _FakeResp(pad(payload, 1139)), _FakeResp(b'x' * 86239, status=405),
        _FakeResp(pad(listing, 86240)), *squad_responses,
    ], clock=lambda: now)
    scraper = tm.TransfermarktScraper(response_cache=cache, cache_ttl_seconds=86400,
                                      resume_squad_cache=True, competition_records=[BRC])
    scraper._http_client = first
    with pytest.raises(TrafficBudgetExceeded, match='16933131/16777216'):
        scraper.read_squad_data('BRC', 2025)
    assert first.get_traffic_stats()['requests'] == 105
    assert len([u for u in cache if '/kader/' in u]) == 101
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-B')
    second, factory = _client(store, cache, [
        _FakeResp(payload), _FakeResp(listing), *squad_responses[101:],
    ], clock=lambda: now + 25 * 3600)
    scraper = tm.TransfermarktScraper(response_cache=cache, cache_ttl_seconds=86400,
                                      resume_squad_cache=True, competition_records=[BRC])
    scraper._http_client = second
    bundle = scraper.read_squad_data('BRC', 2025)
    for key in ('memberships', 'attribute_observations', 'contract_observations'):
        assert len(bundle[key]) == 126
    obs = bundle['attribute_observations']
    assert set(obs['height_cm']) == {186}
    assert set(obs['nationality']) == {'Spain'}
    assert set(obs['dob']) == {date(1995, 9, 15)}
    assert set(obs['contract_until']) == {date(2028, 6, 30)}
    assert set(obs['market_value_eur']) == {30000000}
    assert len(bundle['memberships'].query("player_id == '1000'")) == 2
    assert len(scraper.get_scope_capture()['observed_team_ids']) == 126
    assert second.get_traffic_stats()['cache_hits'] == 101
    assert second.get_traffic_stats()['requests'] == 27
    assert second.get_traffic_stats()['decoded_response_body_bytes'] < 16777216
    assert len(second.get_cache_source_records()) == 101
    # Replay provenance is the physical response time, never this daily run.
    source = store.load_capture(next(v['outcome']['raw_capture_id'] for u, v in cache.items()
                                    if '/verein/1/' in u))[1]
    for key, club_column in [('memberships', 'club_id'), ('attribute_observations', 'club_id'),
                             ('contract_observations', 'team_id')]:
        row = bundle[key].loc[bundle[key][club_column] == '1'].iloc[0]
        assert row['fetched_at'].replace(tzinfo=timezone.utc) == datetime.fromisoformat(source.fetched_at)
    assert sum(len(c.get_calls) for c in factory.clients) == 27
    # A fresh participant listing governs replay: departed clubs disappear,
    # new clubs are fetched, and no saved page can invent a membership.
    changed = clubs[1:] + [{'club_id': '127', 'club_slug': 'team-127', 'club_name': 'Team 127'}]
    monkeypatch.setattr(tm, '_parse_participant_table', lambda _: changed)
    new_payload = json.dumps({'success': True, 'data': {'competitionId': 'BRC',
                             'seasonId': 2025, 'clubIds': [c['club_id'] for c in changed]}}).encode()
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-C')
    third, _ = _client(store, cache, [_FakeResp(new_payload), _FakeResp(listing),
                                    _FakeResp(_squad('127'))], clock=lambda: now + 26 * 3600)
    scraper._http_client = third
    updated = scraper.read_squad_data('BRC', 2025)
    assert set(updated['memberships']['club_id']) == {c['club_id'] for c in changed}
    assert third.get_traffic_stats()['requests'] == 3
    assert third.get_traffic_stats()['cache_hits'] == 125
