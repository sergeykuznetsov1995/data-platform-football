from urllib.parse import parse_qsl, urlsplit
from copy import deepcopy
import json
from pathlib import Path

import pytest

from scrapers.transfermarkt.tmapi import players_url, parse_player_signals, TmapiSchemaError


def test_full_packet_preserves_requested_identity():
    ids = [str(i) for i in range(1, 301)]
    url = urlsplit(players_url(ids))
    assert url.hostname == 'tmapi.transfermarkt.technology'
    assert url.path == '/players'
    assert parse_qsl(url.query) == [('ids[]', value) for value in ids]


@pytest.mark.parametrize('ids', [[], ['1', '1'], ['0'], ['-1'], ['１２'],
                               ['1&token=secret'], list(range(1, 302))])
def test_packet_rejects_invalid_identity(ids):
    with pytest.raises(ValueError, match='unique positive'):
        players_url(ids)


def actual_packet():
    return json.loads((Path(__file__).resolve().parents[2] / 'fixtures/transfermarkt/tmapi_signal_packet_real.json').read_text())


def test_measured_packet_keeps_unknown_value_and_all_club_assignments():
    packet = actual_packet()
    ids = [row['id'] for row in packet['data']]
    signals = parse_player_signals(packet, expected_ids=ids)
    assert set(signals) == set(ids)
    for row in packet['data']:
        signal = signals[row['id']]
        assert signal.contract_until == row['attributes']['contractUntil']
        assert set(signal.club_ids) == {
            assignment['clubId'] for assignment in row['clubAssignments']
            if assignment['type'] in {'current', 'additional'}
        }
        if not row.get('marketValueDetails'):
            assert signal.market_value_present is False
            assert signal.market_value_eur is None


def test_missing_packet_id_is_uncertain_not_unchanged():
    packet = actual_packet()
    ids = [row['id'] for row in packet['data']]
    packet['data'].pop()
    with pytest.raises(TmapiSchemaError, match='identity differs'):
        parse_player_signals(packet, expected_ids=ids)


@pytest.mark.parametrize('field', ['contractUntil', 'lastContractRenewal'])
def test_missing_contract_field_is_uncertain(field):
    packet = actual_packet()
    ids = [row['id'] for row in packet['data']]
    del packet['data'][0]['attributes'][field]
    with pytest.raises(TmapiSchemaError):
        parse_player_signals(packet, expected_ids=ids)


def test_renewal_and_same_amount_new_date_change_signature():
    packet = actual_packet()
    row = next(row for row in packet['data'] if row.get('marketValueDetails'))
    packet['data'] = [row]
    first = parse_player_signals(packet, expected_ids=[row['id']])[row['id']]
    renewed = deepcopy(packet)
    renewed['data'][0]['attributes']['lastContractRenewal'] = {'year': 2026, 'month': 10, 'day': 8}
    assert parse_player_signals(renewed, expected_ids=[row['id']])[row['id']].signature != first.signature
    redated = deepcopy(packet)
    redated['data'][0]['marketValueDetails']['current']['determined'] = '2026-10-08'
    assert parse_player_signals(redated, expected_ids=[row['id']])[row['id']].signature != first.signature
