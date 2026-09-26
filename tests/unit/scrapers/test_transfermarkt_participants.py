"""Participants of cups, continental and national-team competitions (#1392).

Fixtures are live answers captured by the 2026-09-23 review probe: tmapi and
the ``/teilnehmer/`` page agree on Copa do Brasil 2026 (126/126).
"""
from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

import scrapers.transfermarkt.scraper as tm
from scrapers.transfermarkt import tmapi
from scrapers.transfermarkt.registry import (
    CompetitionRecord,
    resolve_competition,
)
from scrapers.transfermarkt.scraper import TransfermarktScraper

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / 'fixtures' / 'transfermarkt' / 'participants'


def _json(name):
    return json.loads((FIXTURES / name).read_text())


def _teilnehmer():
    return (FIXTURES / 'brc_teilnehmer_2025.html').read_text()


def _empty_teilnehmer():
    """The real page's hreflang head with no participant table."""

    html = _teilnehmer()
    return html[: html.index('</head>')] + '</head><body></body></html>'


def _copa_do_brasil():
    """The bootstrap AFCN record (single-year, /pokalwettbewerb/) re-keyed
    to Copa do Brasil, a club domestic cup — evidence included."""

    text = json.dumps(resolve_competition('AFCN').as_dict())
    for old, new in (
        ('national_team_tournament', 'domestic_cup'),
        ('national_team', 'club'),
        ('afrika-cup', 'copa-do-brasil'),
        ('Africa Cup of Nations', 'Copa do Brasil'),
        ('AFCN', 'BRC'),
    ):
        text = text.replace(old, new)
    return CompetitionRecord.from_mapping(json.loads(text))


BRC = _copa_do_brasil()


# ---------------------------------------------------------------- tmapi ---

def test_competition_clubs_reads_the_copa_do_brasil_fixture():
    ids = tmapi.parse_competition_clubs(
        _json('brc_club_2025.json'), competition_id='BRC', saison_id=2025,
    )
    assert len(ids) == 126


def test_competition_clubs_empty_answer_is_an_empty_tuple():
    assert tmapi.parse_competition_clubs(
        _json('bra3_club_2026_empty.json'),
        competition_id='BRA3', saison_id=2026,
    ) == ()


@pytest.mark.parametrize(
    ('competition_id', 'saison_id'), [('CL', 2025), ('BRC', 2024)],
)
def test_competition_clubs_refuses_another_competition_or_season(
    competition_id, saison_id,
):
    with pytest.raises(tmapi.TmapiSchemaError):
        tmapi.parse_competition_clubs(
            _json('brc_club_2025.json'),
            competition_id=competition_id, saison_id=saison_id,
        )


def test_failed_or_unknown_tmapi_answer_is_not_proof():
    assert tmapi.competition_clubs(lambda _url: None, 'BRC', 2025) is None
    assert tmapi.competition_clubs(
        lambda _url: {'success': False}, 'BRC', 2025,
    ) is None


def test_regulation_current_edition_of_a_national_tournament_is_the_last_played():
    seasons = tmapi.parse_regulation(
        _json('afcn_regulation.json'), competition_id='AFCN',
    )
    assert seasons[0].saison_id == 2026 and not seasons[0].is_current
    assert tmapi.current_saison_id(seasons) == 2024


def test_regulation_of_a_calendar_cup_keys_the_printed_year_minus_one():
    seasons = tmapi.parse_regulation(
        _json('brc_regulation.json'), competition_id='BRC',
    )
    current = next(item for item in seasons if item.is_current)
    assert (current.saison_id, current.display) == (2025, '2026')


# ------------------------------------------------------------ teilnehmer ---

def test_teilnehmer_page_matches_tmapi_126_of_126():
    clubs = tm._parse_participant_table(_teilnehmer())
    ids = _json('brc_club_2025.json')['data']['clubIds']
    assert len(clubs) == 126
    assert {club['club_id'] for club in clubs} == set(ids)
    assert all(club['club_slug'] for club in clubs)


def test_empty_teilnehmer_proof_needs_a_self_identifying_page():
    assert tm._participant_page_is_empty(_empty_teilnehmer(), 'BRC', 2025)
    assert not tm._participant_page_is_empty(_empty_teilnehmer(), 'CL', 2025)
    # Another edition or another route of the same competition proves nothing.
    assert not tm._participant_page_is_empty(_empty_teilnehmer(), 'BRC', 2024)
    startseite = _empty_teilnehmer().replace(
        '/teilnehmer/pokalwettbewerb/BRC/saison_id/2025',
        '/startseite/pokalwettbewerb/BRC',
    )
    assert not tm._participant_page_is_empty(startseite, 'BRC', 2025)
    assert not tm._participant_page_is_empty(_teilnehmer(), 'BRC', 2025)
    assert not tm._participant_page_is_empty('<html></html>', 'BRC', 2025)


# -------------------------------------------------------- read_squad_data ---

def _scraper(monkeypatch, *, api, page, record=BRC, season='2026'):
    """A cup scope whose fetches answer from fixtures; squads are stubbed."""

    scraper = TransfermarktScraper(
        competition_records=[record], canonical_season=season,
    )
    calls = []

    def _fetch_json(url, label='json', context=None):
        calls.append((label, url))
        return api

    def _fetch_html(url, label='html', context=None):
        calls.append((label, url))
        return page if label == 'teilnehmer' else '<html/>'

    monkeypatch.setattr(scraper, '_fetch_json', _fetch_json)
    monkeypatch.setattr(scraper, '_fetch_html', _fetch_html)
    monkeypatch.setattr(
        tm, '_parse_squad_page',
        lambda _html, club_id: [{
            'player_id': f'9{club_id}', 'player_slug': 'p', 'name': 'P',
            'club_id': club_id, 'position': 'Forward', 'dob': None,
            'age': 25, 'height_cm': 180, 'foot': 'right',
            'nationality': 'Brazil', 'contract_until': None,
            'market_value_eur': None,
        }],
    )
    return scraper, calls


def test_cup_participants_come_from_tmapi_with_teilnehmer_slugs(monkeypatch):
    scraper, calls = _scraper(
        monkeypatch, api=_json('brc_club_2025.json'), page=_teilnehmer(),
    )

    bundle = scraper.read_squad_data('BRC', 2026)

    capture = scraper.get_scope_capture()
    assert capture['listing_status'] == 'ok'
    assert len(capture['expected_team_ids']) == 126
    assert capture['listing_source_url'] == tmapi.competition_clubs_url('BRC', 2025)
    # The registry edition "2026" (printed year) fetches saison_id 2025.
    api_url = next(url for label, url in calls if label == 'participants_api')
    assert parse_qs(urlsplit(api_url).query) == {'season': ['2025']}
    squads = [url for label, url in calls if label == 'squad']
    assert len(squads) == 126
    assert all(url.endswith('/saison_id/2025/plus/1') for url in squads)
    assert '/x/' not in ''.join(squads)
    assert bundle['memberships']['club_id'].nunique() == 126
    evidence = scraper.get_participant_evidence()
    assert evidence['tmapi_count'] == evidence['teilnehmer_count'] == 126
    assert evidence['tmapi_only'] == evidence['teilnehmer_only'] == []
    assert evidence['source'] == 'tmapi'


def test_empty_shell_with_tmapi_participants_is_never_empty(monkeypatch):
    scraper, _calls = _scraper(
        monkeypatch, api=_json('brc_club_2025.json'), page=_empty_teilnehmer(),
    )

    bundle = scraper.read_squad_data('BRC', 2026)

    capture = scraper.get_scope_capture()
    assert capture['listing_status'] == 'ok'
    assert len(capture['expected_team_ids']) == 126
    assert not bundle['memberships'].empty
    assert scraper.get_participant_evidence()['teilnehmer_only'] == []
    assert len(scraper.get_participant_evidence()['tmapi_only']) == 126


def test_empty_shell_without_tmapi_is_unknown_not_empty(monkeypatch):
    scraper, _calls = _scraper(monkeypatch, api=None, page=_empty_teilnehmer())

    bundle = scraper.read_squad_data('BRC', 2026)

    assert scraper.get_scope_capture()['listing_status'] == 'unknown'
    assert bundle['memberships'].attrs.get('fetch_status') != 'authoritative_empty'
    assert scraper.get_participant_evidence()['source'] == 'none'


def test_unproven_teilnehmer_and_empty_tmapi_is_unknown(monkeypatch):
    empty = {'success': True, 'data': {
        'competitionId': 'BRC', 'seasonId': 2025, 'clubIds': [],
    }}
    scraper, _calls = _scraper(monkeypatch, api=empty, page='<html></html>')

    scraper.read_squad_data('BRC', 2026)

    assert scraper.get_scope_capture()['listing_status'] == 'unknown'


def test_both_sources_empty_is_authoritative_empty(monkeypatch):
    empty = {'success': True, 'data': {
        'competitionId': 'BRC', 'seasonId': 2025, 'clubIds': [],
    }}
    scraper, calls = _scraper(monkeypatch, api=empty, page=_empty_teilnehmer())

    bundle = scraper.read_squad_data('BRC', 2026)

    capture = scraper.get_scope_capture()
    assert capture['listing_status'] == 'authoritative_empty'
    assert capture['listing_source_url'].startswith(tmapi.TMAPI_BASE)
    assert bundle['memberships'].attrs['fetch_status'] == 'authoritative_empty'
    evidence = scraper.get_participant_evidence()
    assert evidence['tmapi_count'] == evidence['teilnehmer_count'] == 0
    assert not [label for label, _url in calls if label == 'squad']


def test_tmapi_failure_falls_back_to_teilnehmer(monkeypatch):
    scraper, _calls = _scraper(monkeypatch, api=None, page=_teilnehmer())

    scraper.read_squad_data('BRC', 2026)

    capture = scraper.get_scope_capture()
    assert capture['listing_status'] == 'ok'
    assert len(capture['expected_team_ids']) == 126
    assert '/teilnehmer/pokalwettbewerb/BRC/saison_id/2025' in (
        capture['listing_source_url']
    )
    assert scraper.get_participant_evidence()['source'] == 'teilnehmer'


def test_club_missing_from_teilnehmer_keeps_its_id_with_placeholder_slug(
    monkeypatch,
):
    api = _json('brc_club_2025.json')
    api['data']['clubIds'] = api['data']['clubIds'] + ['999999']
    scraper, calls = _scraper(monkeypatch, api=api, page=_teilnehmer())

    scraper.read_squad_data('BRC', 2026)

    assert scraper.get_participant_evidence()['tmapi_only'] == ['999999']
    squads = [url for label, url in calls if label == 'squad']
    assert any('/x/kader/verein/999999/' in url for url in squads)


def test_national_team_is_a_club_id_and_flags_the_unverified_squad(monkeypatch):
    afcn = resolve_competition('AFCN')
    api = {'success': True, 'data': {
        'competitionId': 'AFCN', 'seasonId': 2024, 'clubIds': ['3575'],
    }}
    scraper, _calls = _scraper(
        monkeypatch, api=api, page=None, record=afcn, season='2025',
    )

    bundle = scraper.read_squad_data('AFCN', 2025)

    assert list(bundle['memberships']['club_id']) == ['3575']
    evidence = scraper.get_participant_evidence()
    assert evidence['national_squad'] == 'kader_by_saison_id_unverified'
    assert evidence['teilnehmer_count'] is None
    assert bundle['contract_observations'].attrs['fetch_status'] == 'not_applicable'


def test_teilnehmer_fetch_validator_rejects_a_page_that_proves_nothing():
    validate = TransfermarktScraper._endpoint_validator('teilnehmer', False)
    assert validate(_teilnehmer()) is None
    assert validate(_empty_teilnehmer()) is None
    assert validate('<html><body><p>consent</p></body></html>') is not None


def test_club_only_on_teilnehmer_is_recorded_by_id(monkeypatch):
    api = _json('brc_club_2025.json')
    dropped = api['data']['clubIds'][0]
    api['data']['clubIds'] = api['data']['clubIds'][1:]
    scraper, _calls = _scraper(monkeypatch, api=api, page=_teilnehmer())

    scraper.read_squad_data('BRC', 2026)

    evidence = scraper.get_participant_evidence()
    assert evidence['teilnehmer_only'] == [dropped]
    assert evidence['tmapi_only'] == []
