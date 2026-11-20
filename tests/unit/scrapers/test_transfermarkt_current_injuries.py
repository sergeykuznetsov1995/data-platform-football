from __future__ import annotations

import json

import pytest

from scrapers.transfermarkt.current_injuries import (
    CURRENT_INJURY_COLUMNS,
    CURRENT_INJURY_OBSERVATION_KEY,
    CURRENT_INJURY_STATUSES,
    CurrentInjuryParseError,
    confirmed_empty_current_injuries,
    not_applicable_current_injuries,
    parse_current_injuries,
)

pytestmark = pytest.mark.unit


def row(player=110, club=10, injury='Cruciate ligament tear', since='Jan 1, 2026', expected='Dec 31, 2026'):
    return (
        '<tr class="odd">'
        f'<td><a href="/p-{player}/profil/spieler/{player}">Player {player}</a></td>'
        f'<td><a href="/c-{club}/startseite/verein/{club}">Club {club}</a></td>'
        f'<td>{injury}</td><td>{since}</td><td>{expected}</td></tr>'
    )


def page(*rows, player_header='Player', include_thead=True, include_tbody=True):
    head = ('<thead><tr>' + ''.join(f'<th>{header}</th>' for header in (
        player_header, 'Club', 'Injury', 'Since', 'Expected return',
    )) + '</tr></thead>') if include_thead else ''
    body = '<tbody>' + ''.join(rows) + '</tbody>' if include_tbody else ''.join(rows)
    return f'<html><table class="items">{head}{body}</table></html>'


def test_full_capture_preserves_all_normalized_fields():
    result = parse_current_injuries(page(row()), competition_id='GB1', edition_id='2026')
    assert result.status == 'observed_present'
    assert len(result.episodes) == 1
    episode = result.episodes[0]
    assert episode.as_dict() == {
        'competition_id': 'GB1', 'edition_id': '2026', 'player_id': '110',
        'club_id': '10', 'injury': 'Cruciate ligament tear',
        'since': '2026-01-01', 'expected_return': '2026-12-31',
    }
    assert tuple(episode.as_dict()) == CURRENT_INJURY_COLUMNS


def test_same_player_with_two_injuries_is_not_deduplicated():
    result = parse_current_injuries(
        page(row(), row(injury='Hamstring injury')), competition_id='GB1', edition_id=2026,
    )
    assert len(result.episodes) == 2
    assert {episode.injury for episode in result.episodes} == {
        'Cruciate ligament tear', 'Hamstring injury',
    }


def test_unknown_return_and_since_remain_null():
    episode = parse_current_injuries(
        page(row(since='-', expected='Return unknown')), competition_id='GB1', edition_id=2026,
    ).episodes[0]
    assert episode.since is None and episode.expected_return is None


def test_replay_is_deterministic_and_json_safe():
    first = parse_current_injuries(page(row()), competition_id='GB1', edition_id=2026)
    second = parse_current_injuries(page(row()), competition_id='GB1', edition_id=2026)
    assert first == second and first.canonical_json() == second.canonical_json()
    assert json.loads(first.canonical_json())['episodes'][0]['player_id'] == '110'


def test_identical_duplicate_is_reconciled_and_conflict_fails():
    result = parse_current_injuries(page(row(), row()), competition_id='GB1', edition_id=2026)
    assert len(result.episodes) == 1 and result.reconciled_duplicates == 1
    with pytest.raises(CurrentInjuryParseError, match='injury_conflict'):
        parse_current_injuries(
            page(row(), row(expected='Jan 1, 2027')), competition_id='GB1', edition_id=2026,
        )


@pytest.mark.parametrize('html,reason', [
    ('<html>blocked</html>', 'injury_table_missing'),
    (page(row()) + page(row()), 'injury_table_ambiguous'),
    (page(row(), include_thead=False), 'injury_table_structure_changed'),
    (page(row(), include_tbody=False), 'injury_table_structure_changed'),
    (page(row(), player_header='Athlete'), 'injury_headers_changed'),
    (page(row()).replace('<th>Expected return</th>', ''), 'injury_headers_changed'),
    (page(row()).replace('<th>Expected return</th>', '<th>Since</th>'), 'injury_headers_changed'),
    (page(), 'empty_layout_unproven'),
    ('<table class="items"><thead><tr><th>Player</th><th>Club</th><th>Injury</th>'
     '<th>Since</th><th>Expected return</th></tr></thead><tbody>'
     '<tr><td colspan="5">No entries</td></tr></tbody></table>', 'injury_row_shape_changed'),
])
def test_missing_changed_or_unproven_empty_layout_fails_closed(html, reason):
    with pytest.raises(CurrentInjuryParseError, match=reason):
        parse_current_injuries(html, competition_id='GB1', edition_id=2026)


def test_malformed_required_identity_or_reason_fails_whole_capture():
    broken = row().replace('/p-110/profil/spieler/110', '/broken')
    with pytest.raises(CurrentInjuryParseError, match='player_identity_invalid'):
        parse_current_injuries(page(row(), broken), competition_id='GB1', edition_id=2026)
    with pytest.raises(CurrentInjuryParseError, match='club_identity_invalid'):
        parse_current_injuries(
            page(row().replace('/c-10/startseite/verein/10', '/broken')),
            competition_id='GB1', edition_id=2026,
        )
    with pytest.raises(CurrentInjuryParseError, match='injury_reason_invalid'):
        parse_current_injuries(page(row(injury='-')), competition_id='GB1', edition_id=2026)


def test_invalid_dates_and_order_fail_closed():
    with pytest.raises(CurrentInjuryParseError, match='since_invalid'):
        parse_current_injuries(page(row(since='bad')), competition_id='GB1', edition_id=2026)
    with pytest.raises(CurrentInjuryParseError, match='expected_return_invalid'):
        parse_current_injuries(page(row(expected='2026-02-30')), competition_id='GB1', edition_id=2026)
    with pytest.raises(CurrentInjuryParseError, match='date_order_invalid'):
        parse_current_injuries(
            page(row(since='2026-02-01', expected='2026-01-01')),
            competition_id='GB1', edition_id=2026,
        )


def test_empty_and_not_applicable_require_explicit_typed_proofs():
    empty = confirmed_empty_current_injuries(
        competition_id='GB1', edition_id=2026,
        completeness_proof='capture:sha256:full-empty-layout-v1',
    )
    assert empty.status == 'observed_empty' and empty.episodes == ()
    assert empty.completeness_proof == 'capture:sha256:full-empty-layout-v1'
    national = not_applicable_current_injuries(
        competition_id='FIWC', edition_id=2026,
        applicability_proof='participant-proof:registry:FIWC:2026',
    )
    assert national.status == 'not_applicable' and national.episodes == ()
    with pytest.raises(CurrentInjuryParseError, match='completeness_proof_missing'):
        confirmed_empty_current_injuries(
            competition_id='GB1', edition_id=2026, completeness_proof='',
        )
    with pytest.raises(CurrentInjuryParseError, match='applicability_proof_missing'):
        not_applicable_current_injuries(
            competition_id='FIWC', edition_id=2026, applicability_proof='',
        )


def test_observation_key_is_separate_from_parser_columns():
    assert CURRENT_INJURY_OBSERVATION_KEY[-1] == 'observed_at'
    assert 'observed_at' not in CURRENT_INJURY_COLUMNS
    assert CURRENT_INJURY_STATUSES == {'observed_present', 'observed_empty', 'not_applicable'}
