"""Offline old physical captures must remain provable after parser upgrades."""
import copy
from datetime import date

import pytest

from dags.utils import transfermarkt_current_write as current
from scrapers.transfermarkt import scraper as tm, superseded, write_intents
from scrapers.transfermarkt.revisions import CAREER_NULLABLE_ADDITIONS
from tests.unit.utils.test_transfermarkt_current_write import (
    PREFLIGHT, SCOPE, http, scraper,
)

pytestmark = pytest.mark.unit


def _capture(scraper, monkeypatch, entity, revision, shape):
    """Write the literal pre-#1408 projection, including uncleaned HTML fees."""
    if entity == 'market_value_points':
        payload = {'last_change': '01/01/2025', 'list': [
            {'datum_mw': 'Jan 1, 2025', 'y': 0, 'mw': '-', 'verein': 'Old', 'age': '20',
             'wappen': 'https://tm.test/wappen/profil/10.png'},
            {'datum_mw': 'Jan 2, 2025', 'y': 1000000, 'mw': 'EUR1m', 'verein': 'Next', 'age': '20'},
        ]}
        old_rows = [
            {'player_id': '1', 'mv_date': date(2025, 1, 1),
             'value_eur': 0, 'club_name': 'Old', 'age': 20, 'mv_raw': '-'},
            {'player_id': '1', 'mv_date': date(2025, 1, 2),
             'value_eur': 1000000, 'club_name': 'Next', 'age': 20, 'mv_raw': 'EUR1m'},
        ]
        parser, columns = '_parse_mv_history', 'MARKET_VALUE_POINT_COLUMNS'
        spec = current.run.ENTITY_SPECS['market_value_history']
    else:
        fee = 'Loan fee:<br /><i class="normaler-text">€500k</i>'
        payload = {'transfers': [{'id': 'event-1', 'date': 'Sep 1, 2025', 'season': '25/26',
            'from': {'clubName': 'Old', 'href': '/old/verein/10', 'isSpecial': False,
                     'countryFlag': 'https://tm.test/flagge/verysmall/40.png'},
            'to': {'clubName': 'Next', 'href': '/next/verein/20', 'isSpecial': True},
            'fee': fee, 'marketValue': '€1m'}]}
        old_rows = [{'transfer_id': tm._stable_transfer_id(payload['transfers'][0], '1'),
            'player_id': '1', 'transfer_date': date(2025, 9, 1),
            'event_season': '2526', 'season': '25/26',
            'from_club_id': '10', 'from_club_name': 'Old',
            'to_club_id': '20', 'to_club_name': 'Next', 'fee_text': fee,
            'fee_eur': 500000, 'market_value_eur': 1000000, 'is_upcoming': False}]
        parser, columns = '_parse_transfers', 'TRANSFER_EVENT_COLUMNS'
        spec = current.run.ENTITY_SPECS['transfers']
    source = http(scraper, [payload])
    with monkeypatch.context() as patch:
        patch.setattr(tm, 'PARSER_REVISION', revision[0])
        patch.setattr(tm, 'SCHEMA_REVISION', revision[1])
        patch.setattr(tm, parser, lambda *_args: copy.deepcopy(old_rows))
        if shape == 'absent':
            patch.setattr(tm, columns, [col for col in getattr(tm, columns)
                                       if col not in CAREER_NULLABLE_ADDITIONS])
        result = current.fetch_current_career(scraper, entity, ['1'], SCOPE, PREFLIGHT,
            'pre-upgrade', decoded_body_soft_stop_bytes=100000)
    row, = result['manifests'][0]['rows']
    archive = write_intents.completed_capture(result['manifest_cycle_id'], entity, row['native_batch_id'])
    assert archive is not None
    return spec, archive, source


@pytest.mark.parametrize('entity', ['market_value_points', 'transfer_events'])
@pytest.mark.parametrize('shape', ['absent', 'null'])
@pytest.mark.parametrize('revision', [('registry-v1', '3'), ('', '3'), ('v2', '2'),
                                     ('registry-v1', ''), ('', '')])
def test_supersession_replays_previous_physical_revision(scraper, monkeypatch, entity, shape, revision):
    spec, archive, source = _capture(scraper, monkeypatch, entity, revision, shape)
    original = copy.deepcopy(archive)
    proof = superseded.verify_complete(scraper, spec, archive, players=['1'])
    retired = [(None, None, {'retired_player_ids': ['1'], 'superseding_players': {
        '1': {'raw': proof['raw']['1'], 'unit': proof['unit']}}})]
    cached, blocked = superseded.cached_supersession(scraper, spec, retired, ['1'], reconcile=True)
    assert not blocked and set(cached) == {'1'}
    assert archive == original
    assert sum(len(client.get_calls) for client in source.clients) == 1
    if entity == 'market_value_points':
        assert cached['1']['frame'].value_eur.tolist() == [0, 1000000]
    else:
        assert '<br' in cached['1']['frame'].fee_text.iloc[0]


@pytest.mark.parametrize('entity', ['market_value_points', 'transfer_events'])
@pytest.mark.parametrize('revision', [('registry-v99', '3'), ('registry-v1', '99'),
                                     ('registry-v2', '3'), ('registry-v1', '4'), ('', '4')])
def test_supersession_rejects_unknown_physical_revision(scraper, monkeypatch, entity, revision):
    spec, archive, _ = _capture(scraper, monkeypatch, entity, revision, 'null')
    # Environment overrides must not make an unknown revision "current".
    with monkeypatch.context() as patch:
        patch.setattr(tm, 'PARSER_REVISION', revision[0])
        patch.setattr(tm, 'SCHEMA_REVISION', revision[1])
        with pytest.raises(RuntimeError, match='supersession unsupported physical parser/schema revision'):
            superseded.verify_complete(scraper, spec, archive, players=['1'])


@pytest.mark.parametrize('entity', ['market_value_points', 'transfer_events'])
@pytest.mark.parametrize('damage', ['business', 'new_field', 'source_hash', 'missing_field',
                                   'null_revision', 'null_schema', 'mixed_revision'])
def test_old_raw_comparison_still_rejects_different_physical_fields(scraper, monkeypatch, entity, damage):
    _, archive, _ = _capture(scraper, monkeypatch, entity, ('registry-v1', '3'), 'null')
    frame = write_intents.unpack_frames(archive['frames'])[entity]
    if damage == 'business':
        frame.loc[0, 'value_eur' if entity == 'market_value_points' else 'fee_eur'] = 123
    elif damage == 'new_field':
        column = 'club_id' if entity == 'market_value_points' else 'from_country_id'
        frame[column] = frame[column].astype(object)
        frame.loc[0, column] = '999'
    elif damage == 'source_hash':
        frame.loc[0, 'source_body_hash'] = 'wrong'
    elif damage == 'missing_field':
        frame = frame.drop(columns=['mv_raw' if entity == 'market_value_points' else 'fee_text'])
    elif damage == 'null_revision':
        frame.loc[0, 'parser_revision'] = None
    elif damage == 'null_schema':
        frame.loc[0, 'schema_revision'] = None
    else:
        if len(frame) == 1:
            frame = frame.loc[[0, 0]].reset_index(drop=True)
        frame.loc[0, 'parser_revision'] = tm.PARSER_REVISION
        frame.loc[0, 'schema_revision'] = tm.SCHEMA_REVISION
    # Isolate raw comparison: normal verify_complete also checks physical hashes.
    with pytest.raises(RuntimeError, match='supersession .* (differ|revision)'):
        superseded._raw_player(scraper, archive['journal'], frame, '1', entity)


@pytest.mark.parametrize('entity', ['market_value_points', 'transfer_events'])
@pytest.mark.parametrize('shape', ['absent', 'null'])
def test_current_revision_cannot_use_previous_projection(scraper, monkeypatch, entity, shape):
    spec, archive, _ = _capture(scraper, monkeypatch, entity,
        (tm.PARSER_REVISION, tm.SCHEMA_REVISION), shape)
    with pytest.raises(RuntimeError, match='supersession full raw business fields differ'):
        superseded.verify_complete(scraper, spec, archive, players=['1'])
