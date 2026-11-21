from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pandas as pd
import pytest

from dags.scripts import run_transfermarkt_club_values as runner
from dags.scripts.run_transfermarkt_club_values import ClubValueRunError, club_values_url, run_club_values
from dags.utils.transfermarkt_club_values import club_values_enabled
from scrapers.transfermarkt.models import FetchStatus
from tests.unit.utils.test_transfermarkt_club_values import club_value_policy, physical, scraper  # noqa: F401,F811
from tests.unit.utils.test_transfermarkt_current_write import PREFLIGHT


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def clean_transfermarkt_environment(monkeypatch):
    for name in ('TM_REQUIRE_METERED_PROXY', 'TM_PROXY_CONTROL_URL', 'TM_DAG_ID', 'TM_WRITE_MODE', 'TM_READER_REVISION'):
        monkeypatch.delenv(name, raising=False)


FIXTURES = Path(__file__).resolve().parents[2] / 'fixtures' / 'transfermarkt' / 'club_values'
BASE = 'https://www.transfermarkt.com/v-league-1/marktwerteverein/wettbewerb/VIE1'
PAYLOAD = {
    'scope_id': 'tm-VIE1-2026', 'competition_id': 'VIE1', 'edition_id': '2026',
    'child_cycle_id': 'tm-child-test', 'resume_cycle_id': 'tm-parent-test',
    'registry_snapshot_id': 'tm-discovery-test',
    'source_url': 'https://www.transfermarkt.com/v-league-1/startseite/wettbewerb/VIE1/saison_id/2026',
}


def outcome(instance, html, url):
    raw = instance.test_store.store_attempt(
        url=url, body=html.encode(), status_code=200, headers={},
        fetched_at='2026-11-20T15:40:00+00:00', cycle_id='tm-child-test',
        scope_id='tm-VIE1-2026', endpoint='club_values', attempt=0,
    )
    return SimpleNamespace(
        status=FetchStatus.OK, value=html, raw_capture_id=raw.capture_id,
        raw_body_hash=raw.content_hash, raw_fetched_at=raw.fetched_at,
    )


def test_url_builder_preserves_registry_route(physical):  # noqa: F811
    assert club_values_url(PAYLOAD) == BASE
    assert club_values_url(PAYLOAD, '2010-11-01') == BASE + '/stichtag/2010-11-01'
    with pytest.raises(ClubValueRunError, match='cannot build'):
        club_values_url(dict(PAYLOAD, source_url='https://www.transfermarkt.com/club/marktwertverlauf/verein/10'))


def test_current_runner_writes_real_capture_with_receipts(physical, monkeypatch):  # noqa: F811
    html = (FIXTURES / 'vie1-current.html').read_text()
    monkeypatch.setattr(physical, '_fetch_endpoint_outcome', lambda url, as_json, label: outcome(physical, html, url))
    result = run_club_values(physical, PAYLOAD, PREFLIGHT, mode='current')
    assert result['verified'] and result['mode'] == 'current'
    assert result['club_count'] == 14
    assert result['point_count'] == 32
    assert len(result['receipts']) == 14
    assert result['reconciliation']['reconciliation_passed']
    assert not result['full_available_history']
    rows = pd.read_sql_query('SELECT * FROM transfermarkt_club_value_observations', physical.test_db.sql)
    assert len(rows) == 32


def test_history_runner_discovers_earliest_and_writes_full_history(physical, monkeypatch):  # noqa: F811
    html = (FIXTURES / 'vie1-2013-04-15.html').read_text()
    soup = BeautifulSoup(html, 'html.parser')
    for option in soup.select('select[name="stichtag"] option'):
        if option['value'] < '2013-04-15':
            option.decompose()
    html = str(soup)
    monkeypatch.setattr(physical, '_fetch_endpoint_outcome', lambda url, as_json, label: outcome(physical, html, url))
    result = run_club_values(physical, PAYLOAD, PREFLIGHT, mode='history')
    assert result['history_url'] == BASE + '/stichtag/2013-04-15'
    assert result['full_available_history']
    assert result['point_count'] == 3499
    assert len(result['receipts']) == 14
    rows = pd.read_sql_query('SELECT * FROM transfermarkt_club_value_observations', physical.test_db.sql)
    assert len(rows) == 3499


def test_history_runner_rejects_changed_earliest_universe(physical, monkeypatch):  # noqa: F811
    current_html = (FIXTURES / 'vie1-current.html').read_text()
    history_html = (FIXTURES / 'vie1-2013-04-15.html').read_text()
    extraction = runner._extract(physical, PAYLOAD, BASE + '/stichtag/2013-04-15', history_html, outcome(physical, history_html, BASE + '/stichtag/2013-04-15'))
    monkeypatch.setattr(
        physical, '_fetch_endpoint_outcome',
        lambda url, as_json, label: outcome(physical, current_html if url == BASE else history_html, url),
    )
    calls = []
    def changed_earliest(*args):
        calls.append(args)
        if len(calls) == 1:
            return extraction
        return replace(
            extraction, club_ids=extraction.club_ids[:-1],
            rows=tuple(row for row in extraction.rows if row['club_id'] != extraction.club_ids[-1]),
            full_available_history=True)
    monkeypatch.setattr(runner, '_extract', changed_earliest)
    with pytest.raises(ClubValueRunError, match='universes differ'):
        run_club_values(physical, PAYLOAD, PREFLIGHT, mode='history')
    assert physical.test_db.sql.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='transfermarkt_club_value_observations'"
    ).fetchone() == (0,)


def test_history_runner_rejects_non_earliest_capture(physical, monkeypatch):  # noqa: F811
    current_html = (FIXTURES / 'vie1-current.html').read_text()
    history_html = (FIXTURES / 'vie1-2013-04-15.html').read_text()
    monkeypatch.setattr(
        physical, '_fetch_endpoint_outcome',
        lambda url, as_json, label: outcome(physical, current_html if url == BASE else history_html, url),
    )
    with pytest.raises(Exception, match='stichtag_header_mismatch'):
        run_club_values(physical, PAYLOAD, PREFLIGHT, mode='history')
    assert physical.test_db.sql.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='transfermarkt_club_value_observations'"
    ).fetchone() == (0,)


def test_club_values_enabled_requires_both_grants():
    policy = SimpleNamespace(allowed_write_tables=[
        'iceberg.bronze.transfermarkt_club_value_observations',
        'iceberg.ops.transfermarkt_club_value_receipts_v1',
    ])
    assert club_values_enabled(policy)
    policy.allowed_write_tables.pop()
    assert not club_values_enabled(policy)


@pytest.fixture
def club_values_main_budget(monkeypatch, tmp_path):
    import json
    import os
    import sys
    from dags.scripts import run_transfermarkt_scraper as budget_runner

    limit = 8 * 1024 * 1024
    reserved = 2 * 1024 * 1024 - 17
    state = SimpleNamespace(
        history_enabled=True, reservations=[], settlements=[], factories=[], closed=[],
        budget_runner=budget_runner,
        reports=[], traffic={'provider_metered_bytes': 731, 'response_bytes': 9999},
        limit=limit, report=tmp_path / 'history-result.json',
        budget={
            'path': tmp_path / 'ledger.json', 'limit_bytes': limit,
            'reservation_id': 'club-values-reservation', 'reserved_bytes': reserved,
            'remaining_after_reservation_bytes': limit - reserved,
        },
    )
    monkeypatch.setenv('TM_SCOPE_PAYLOAD_JSON', json.dumps(PAYLOAD))
    monkeypatch.setenv('TM_PROVIDER_HARD_CAP_BYTES', str(limit))
    monkeypatch.setenv('TM_HISTORY_PORTION_ID', 'portion-test')
    monkeypatch.delenv(runner.PROVIDER_GRANT_ENV_VAR, raising=False)
    monkeypatch.delenv('TM_HISTORY_GRANT_JSON', raising=False)
    monkeypatch.setattr(sys, 'argv', [
        'run_transfermarkt_club_values.py', '--mode', 'history',
        '--report', str(state.report),
    ])
    monkeypatch.setattr(runner, 'history_portion_enabled', lambda: state.history_enabled)

    def prepare(run_key, limit_bytes, *, entity, reserve_bytes):
        state.reservations.append((run_key, limit_bytes, entity, reserve_bytes))
        return state.budget

    def settle(run_key, limit_bytes, entity, decoded_bytes, *, reservation_id=None):
        state.settlements.append((
            run_key, limit_bytes, entity, decoded_bytes, reservation_id,
        ))

    state.scraper = SimpleNamespace(close=lambda: state.closed.append(True))

    def factory(**kwargs):
        state.factories.append(kwargs)
        key = 'tm-child-test:portion-test' if state.history_enabled else 'tm-child-test'
        assert state.reservations == [(key, limit, 'club_values', 2 * 1024 * 1024)]
        assert os.environ[runner.PROVIDER_GRANT_ENV_VAR] == str(reserved)
        assert os.environ['TM_CYCLE_LEDGER_KEY'] == key
        if state.history_enabled:
            assert json.loads(os.environ['TM_HISTORY_GRANT_JSON']) == {
                'ledger_path': str(state.budget['path']),
                'reservation_id': state.budget['reservation_id'],
                'cycle_ledger_key': key, 'entity': 'club_values',
                'limit_bytes': limit, 'reserved_bytes': reserved,
            }
        return state.scraper

    def run(instance, payload, preflight, *, mode):
        assert instance is state.scraper
        assert payload['child_cycle_id'] == 'tm-child-test'
        assert mode == 'history'
        return {'status': 'ok'}

    monkeypatch.setattr(budget_runner, '_prepare_cycle_budget', prepare)
    monkeypatch.setattr(budget_runner, '_record_cycle_traffic', settle)
    monkeypatch.setattr(runner, 'TransfermarktScraper', factory)
    monkeypatch.setattr(budget_runner, '_normalise_traffic', lambda instance: state.traffic)
    monkeypatch.setattr(runner, '_preflight_from_env', lambda: {})
    monkeypatch.setattr(runner, 'run_club_values', run)
    monkeypatch.setattr(
        runner, '_atomic_report', lambda path, report: state.reports.append((path, report)),
    )
    return state


@pytest.mark.parametrize('history_enabled', [True, False])
def test_main_reserves_before_factory_and_settles_provider_bytes(
    club_values_main_budget, history_enabled,
):
    state = club_values_main_budget
    state.history_enabled = history_enabled
    assert runner.main() == 0
    key = 'tm-child-test:portion-test' if history_enabled else 'tm-child-test'
    assert state.settlements == [(
        key, state.limit, 'club_values', 731, state.budget['reservation_id'],
    )]
    assert len(state.factories) == 1
    assert state.closed == [True]
    assert state.reports == [(state.report, {'status': 'ok'})]


def test_main_settles_provider_bytes_on_run_failure(monkeypatch, club_values_main_budget):
    state = club_values_main_budget
    failure = ClubValueRunError('capture failed')

    def fail(*args, **kwargs):
        raise failure

    monkeypatch.setattr(runner, 'run_club_values', fail)
    with pytest.raises(ClubValueRunError) as caught:
        runner.main()
    assert caught.value is failure
    assert state.settlements == [(
        'tm-child-test:portion-test', state.limit, 'club_values', 731,
        state.budget['reservation_id'],
    )]
    assert state.closed == [True]
    assert state.reports == []


def test_main_does_not_construct_without_reservation(monkeypatch, club_values_main_budget):
    state = club_values_main_budget

    def fail(*args, **kwargs):
        raise ClubValueRunError('budget exhausted')

    monkeypatch.setattr(state.budget_runner, '_prepare_cycle_budget', fail)
    with pytest.raises(ClubValueRunError, match='budget exhausted'):
        runner.main()
    assert state.factories == []
    assert state.settlements == []
    assert state.closed == []


@pytest.mark.parametrize('traffic', [
    {'response_bytes': 9999},
    {'provider_metered_bytes': None, 'response_bytes': 9999},
])
@pytest.mark.parametrize('run_fails', [False, True])
def test_main_unknown_telemetry_retains_reservation(
    monkeypatch, club_values_main_budget, traffic, run_fails,
):
    state = club_values_main_budget
    state.traffic = traffic
    failure = ClubValueRunError('capture failed')
    if run_fails:
        def fail(*args, **kwargs):
            raise failure
        monkeypatch.setattr(runner, 'run_club_values', fail)
    with pytest.raises(ClubValueRunError) as caught:
        runner.main()
    if run_fails:
        assert caught.value is failure
        assert any('telemetry is unknown' in note for note in caught.value.__notes__)
    else:
        assert 'telemetry is unknown' in str(caught.value)
    assert state.reservations == [
        ('tm-child-test:portion-test', state.limit, 'club_values', 2 * 1024 * 1024),
    ]
    assert state.settlements == [(
        'tm-child-test:portion-test', state.limit, 'club_values:telemetry_unknown',
        state.budget['remaining_after_reservation_bytes'], None,
    )]
    assert state.closed == [True]


def test_main_constructor_failure_settles_unused_reservation(
    monkeypatch, club_values_main_budget,
):
    state = club_values_main_budget
    failure = ClubValueRunError('constructor failed')

    def fail(**kwargs):
        raise failure

    monkeypatch.setattr(runner, 'TransfermarktScraper', fail)
    with pytest.raises(ClubValueRunError) as caught:
        runner.main()
    assert caught.value is failure
    assert state.settlements == [(
        'tm-child-test:portion-test', state.limit, 'club_values', 0,
        state.budget['reservation_id'],
    )]
    assert state.closed == []
