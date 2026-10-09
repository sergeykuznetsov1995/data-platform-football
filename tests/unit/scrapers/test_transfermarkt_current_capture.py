from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from scrapers.transfermarkt import current_capture as current
from scrapers.transfermarkt.registry import deterministic_scope_id, SeasonFormat, CompetitionType
from scrapers.transfermarkt import tmapi
from scrapers.transfermarkt.season import season_to_saison_id
from scrapers.transfermarkt.scraper import _uses_participant_api, _competition_listing_url, _CLUB_SQUAD_PATH
from dags.utils import transfermarkt_scope_planner as scope_planner
from tests.unit.dags.test_transfermarkt_scope_planner import _competition, _edition, _joined_row


NOW = datetime(2026, 10, 8, tzinfo=timezone.utc)
SCOPE_ID = deterministic_scope_id('GB1', '2026')


def player(pid='1', club='10', **changes):
    return {
        'player_id': pid, 'player_slug': f'player-{pid}', 'name': f'Player {pid}',
        'club_id': club, 'market_value_eur': 1000000, 'position': 'Striker',
        'dob': '2000-01-01', 'age': 26, 'height_cm': 180, 'foot': 'right',
        'nationality': 'France', 'contract_until': '2027-06-30', **changes,
    }


def club(club_id='10', rows=None, *, at=NOW, committed=True, **changes):
    return {
        'rows': rows if rows is not None else [player(club=club_id)],
        'raw_capture_id': f'raw-{club_id}', 'raw_fetched_at': at.isoformat(),
        'source_url': f'https://www.transfermarkt.com/club-{club_id}/kader/verein/{club_id}/saison_id/2026/plus/1',
        'source_body_hash': 'a' * 64, 'bronze_manifest': 'manifest-1' if committed else None,
        'signature': 'b' * 64, **changes,
    }


def snapshot(clubs=None, expected=None):
    clubs = clubs if clubs is not None else {'10': club(), '20': club('20', [player('2', '20')])}
    return current.FullRosterSnapshot.from_mapping({
        'scope_id': SCOPE_ID, 'competition_id': 'GB1', 'edition_id': '2026', 'squad_saison_id': 2026,
        'expected_team_ids': expected if expected is not None else list(clubs),
        'clubs': clubs,
    })


def listing(value='€10.0m', decoration=''):
    return f'''<html><script>token outside table</script><table class="items">
      <thead><tr><th>Club</th><th>Squad</th><th>Total market value</th></tr></thead>
      <tbody><tr class="odd"><td class="hauptlink">
      <a href="/club-a/startseite/verein/10?sort=marketValue#tab">Club A</a>
      {decoration}</td><td> 25 </td><td>{value}</td></tr></tbody></table></html>'''


def test_listing_signatures_ignore_script_tokens_and_decorative_url_attributes():
    baseline = current.club_listing_signatures(listing())
    decorated = listing(decoration='<script>changed token</script><input type="hidden" value="csrf-new">')
    decorated = decorated.replace('?sort=marketValue#tab', '?utm_source=another#other').replace('class="odd"', 'class="even"')
    assert current.club_listing_signatures(decorated) == baseline
    assert baseline['10']['club_slug'] == 'club-a'
    assert baseline['10']['club_name'] == 'Club A'


def test_listing_signature_changes_when_semantic_club_row_changes():
    original = current.club_listing_signatures(listing())['10']['signature']
    assert current.club_listing_signatures(listing('€12.0m'))['10']['signature'] != original
    assert current.club_listing_signatures(listing().replace('25', '26'))['10']['signature'] != original


def test_listing_signature_is_independent_of_attribute_order_and_whitespace():
    html = listing().replace('>Club A<', '>  Club   A <').replace(' 25 ', '\n25\xa0')
    assert current.club_listing_signatures(html) == current.club_listing_signatures(listing())
    assert current.semantic_signature({'value': 1, 'name': 'A'}) == current.semantic_signature({'name': 'A', 'value': 1})


def test_listing_refuses_layout_loss_or_conflicting_duplicate_clubs():
    with pytest.raises(current.CurrentCaptureError, match='items table'):
        current.club_listing_signatures('<html>captcha</html>')
    duplicate = listing().replace('</tbody>', '<tr><td><a href="/club-a/startseite/verein/10">Club A</a></td><td>99</td></tr></tbody>')
    with pytest.raises(current.CurrentCaptureError, match='duplicate'):
        current.club_listing_signatures(duplicate)


def test_player_changes_new_player_both_endpoints_removed_player_transfer():
    changes = current.player_changes([player('1')], [player('2')])
    assert changes['1'].endpoints == {'transfer_events'}
    assert changes['1'].old_club_ids == ('10',)
    assert changes['1'].new_club_ids == ()
    assert changes['2'].endpoints == {'market_value_points', 'transfer_events'}
    assert changes['2'].old_club_ids == ()


@pytest.mark.parametrize('changes', [{'market_value_eur': 1200000}, {'market_value_date': '2026-10-08'}])
def test_player_value_and_value_date_select_only_market_value_endpoint(changes):
    change = current.player_changes([player()], [player(**changes)])['1']
    assert change.endpoints == {'market_value_points'}


def test_contract_and_bio_changes_do_not_select_career_endpoints():
    assert current.player_changes([player()], [player(contract_until='2028-06-30', height_cm=181)]) == {}


def test_player_transfer_keeps_old_and_new_context():
    change = current.player_changes([player(club='10')], [player(club='20')])['1']
    assert change.endpoints == {'transfer_events'}
    assert change.old_club_ids == ('10',) and change.new_club_ids == ('20',)


def test_multiple_memberships_are_not_collapsed_to_one_current_club():
    previous = [player(club='10'), player(club='20')]
    assert current.player_changes(previous, list(reversed(previous))) == {}
    change = current.player_changes(previous, [player(club='20')])['1']
    assert change.endpoints == {'transfer_events'}
    assert change.old_club_ids == ('10', '20') and change.new_club_ids == ('20',)


def test_selective_assembly_keeps_every_other_club_and_original_lineage():
    old = snapshot()
    changed = club(rows=[player(market_value_eur=1200000)], at=NOW + timedelta(hours=20), committed=False)
    assembled = current.assemble_full_roster(old, ('10', '20'), {'10': changed}, ('10',))
    assert assembled.expected_team_ids == ('10', '20')
    assert assembled.changed_club_ids == ('10',)
    assert assembled.clubs['20'] is old.clubs['20']
    assert assembled.clubs['20'].raw_fetched_at == NOW
    assert assembled.clubs['10'].raw_fetched_at == NOW + timedelta(hours=20)
    assert len(assembled.rows) == 2
    assert assembled.rows[1]['_source_fetched_at'] == NOW.isoformat()
    assert assembled.rows[0]['market_value_eur'] == 1200000
    assert assembled.rows[1]['height_cm'] == 180
    # Fresh captures cannot become retained committed data before Bronze ack.
    with pytest.raises(current.CurrentCaptureError, match='Bronze manifest'):
        current.FullRosterSnapshot.from_mapping(assembled.as_dict())


def test_unchanged_edition_reuses_roster_without_observing_it_again():
    old = snapshot()
    assembled = current.assemble_full_roster(old, old.expected_team_ids, {}, ())
    assert assembled.rows == old.rows
    assert assembled.changed_club_ids == ()


def test_roster_snapshot_cannot_be_mutated_through_caller_dicts():
    payload = club()
    snap = snapshot({'10': payload})
    payload['rows'][0]['name'] = 'Injected'
    assert snap.rows[0]['name'] == 'Player 1'
    with pytest.raises(TypeError):
        snap.clubs['10'].rows[0]['name'] = 'Injected'


@pytest.mark.parametrize('changes', [
    {'raw_capture_id': ''}, {'source_body_hash': 'bad'}, {'signature': 'bad'},
    {'raw_fetched_at': '2026-10-08T00:00:00'}, {'bronze_manifest': None},
    {'source_url': 'https://www.transfermarkt.com/a/kader/verein/99/saison_id/2026/plus/1'},
    {'source_url': 'https://www.transfermarkt.com/a/kader/verein/10/saison_id/2025/plus/1'},
    {'source_url': 'https://www.transfermarkt.com/a/kader/verein/10/saison_id/2026'},
])
def test_retained_roster_requires_source_and_committed_full_capture(changes):
    with pytest.raises(current.CurrentCaptureError):
        snapshot({'10': club(**changes)})


def test_missing_full_fields_and_nan_refuse_snapshot():
    row = player()
    del row['height_cm']
    with pytest.raises(current.CurrentCaptureError, match='full /plus/1'):
        snapshot({'10': club(rows=[row])})
    with pytest.raises(current.CurrentCaptureError, match='finite'):
        snapshot({'10': club(rows=[player(market_value_eur=float('nan'))])})


def test_allclubs_guard_rejects_missing_even_when_90_percent_clubs_completed():
    clubs = {str(i): club(str(i), [player(str(i), str(i))]) for i in range(10, 20)}
    with pytest.raises(current.CurrentCaptureError, match='every expected club'):
        snapshot({key: value for key, value in clubs.items() if key != '19'}, expected=list(clubs))
    old = snapshot(clubs)
    with pytest.raises(current.CurrentCaptureError, match='incomplete or pending'):
        current.assemble_full_roster(old, old.expected_team_ids, {'10': club()}, ('10', '19'))


def test_90_percent_rowcount_guard_stays_in_force():
    old = snapshot({'10': club(rows=[player(str(i)) for i in range(10)])})
    with pytest.raises(current.CurrentCaptureError, match='90%'):
        current.assemble_full_roster(old, ('10',), {'10': club(rows=[player(str(i)) for i in range(8)])}, ('10',))
    assembled = current.assemble_full_roster(old, ('10',), {'10': club(rows=[player(str(i)) for i in range(9)])}, ('10',))
    assert len(assembled.rows) == 9


def test_membership_changes_need_verified_listing_and_full_new_club():
    old = snapshot()
    additions = {'30': club('30', [player('3', '30')], committed=False)}
    with pytest.raises(current.CurrentCaptureError, match='verified listing'):
        current.assemble_full_roster(old, ('10', '20', '30'), additions, ('30',))
    assembled = current.assemble_full_roster(old, ('10', '20', '30'), additions, ('30',),
                                             participant_membership_verified=True)
    assert assembled.expected_team_ids == ('10', '20', '30')
    with pytest.raises(current.CurrentCaptureError, match='new participant'):
        current.assemble_full_roster(old, ('10', '20', '30'), {}, (), participant_membership_verified=True)


def test_participant_removal_requires_listing_proof_and_preserves_rowcount_guard():
    old = snapshot()
    with pytest.raises(current.CurrentCaptureError, match='verified listing'):
        current.assemble_full_roster(old, ('10',), {}, ())
    with pytest.raises(current.CurrentCaptureError, match='90%'):
        current.assemble_full_roster(old, ('10',), {}, (), participant_membership_verified=True)


def test_updated_roster_cannot_drop_previously_populated_bio_fields():
    old = snapshot()
    degraded = club(rows=[player(height_cm=None)], at=NOW + timedelta(hours=1), committed=False)
    with pytest.raises(current.CurrentCaptureError, match='lost height_cm'):
        current.assemble_full_roster(old, ('10', '20'), {'10': degraded}, ('10',))


def test_cold_assembly_needs_every_club_and_does_not_require_preexisting_bronze():
    assembled = current.assemble_full_roster(None, ('10',), {'10': club(committed=False)}, ('10',),
                                             scope_id=SCOPE_ID, competition_id='GB1', edition_id='2026', squad_saison_id=2026)
    assert len(assembled.rows) == 1
    with pytest.raises(current.CurrentCaptureError, match='new participant'):
        current.assemble_full_roster(None, ('10', '20'), {'10': club(committed=False)}, ('10',),
                                     scope_id=SCOPE_ID, competition_id='GB1', edition_id='2026', squad_saison_id=2026)


def test_multi_club_player_remains_two_memberships_in_full_snapshot():
    snap = snapshot({'10': club(), '20': club('20', [player('1', '20')])})
    assert [(row['club_id'], row['player_id']) for row in snap.rows] == [('10', '1'), ('20', '1')]
    assert current.player_changes(snap.rows, snap.rows) == {}


def test_foreign_scope_or_older_updated_capture_is_rejected():
    old = snapshot()
    with pytest.raises(current.CurrentCaptureError, match='another scope'):
        current.assemble_full_roster(old, ('10', '20'), {}, (), scope_id='FR1:2026')
    with pytest.raises(current.CurrentCaptureError, match='older'):
        current.assemble_full_roster(old, ('10', '20'), {'10': club(at=NOW - timedelta(hours=1))}, ('10',))


@pytest.mark.parametrize('competition_id,fmt,route,squad_year', [
    ('GB1', SeasonFormat.SPLIT_YEAR, 'wettbewerb', 2026),
    ('CDB', SeasonFormat.SINGLE_YEAR, 'pokalwettbewerb', 2025),
])
def test_snapshot_uses_real_registry_scope_and_explicit_resolved_squad_year(
    competition_id, fmt, route, squad_year,
):
    competition = _competition(competition_id, season_format=fmt)
    competition = replace(competition, source_url=f'https://www.transfermarkt.com/competition/startseite/{route}/{competition_id}')
    if route == 'pokalwettbewerb':
        competition = replace(competition, competition_type=CompetitionType.DOMESTIC_CUP,
                              evidence=tuple(replace(proof, competition_type=CompetitionType.DOMESTIC_CUP)
                                             for proof in competition.evidence))
    edition = _edition(competition_id, '2026', season_format=fmt)
    plan = scope_planner.plan_transfermarkt_scopes(
        {'scopes': [f'{competition_id}:2026']}, parent_cycle_id='current-roster-integration',
        competitions=[competition], editions=[edition], now=NOW,
    )
    payload, = plan.mapped_payloads
    actual_year = (season_to_saison_id(edition.canonical_season, competition.season_format)
                   if _uses_participant_api(competition) else int(edition.edition_id))
    assert actual_year == squad_year
    listing_url = _competition_listing_url(competition, edition.edition_id)
    assert f'/{route}/{competition_id}/plus/' in listing_url
    squad_url = 'https://www.transfermarkt.com' + _CLUB_SQUAD_PATH.format(
        club_slug='club-a', club_id='10', year=actual_year,
    )
    capture = club(source_url=squad_url)
    snap = current.FullRosterSnapshot.from_mapping({
        'scope_id': payload['scope_id'], 'competition_id': payload['competition_id'],
        'edition_id': payload['edition_id'], 'squad_saison_id': actual_year,
        'expected_team_ids': ['10'], 'clubs': {'10': capture},
    })
    assert snap.scope_id == deterministic_scope_id(competition_id, edition.edition_id)
    assert snap.rows[0]['_source_url'] == squad_url
    assert snap.as_dict()['squad_saison_id'] == actual_year
    if route == 'pokalwettbewerb':
        assert '/competition/CDB/club?season=2025' in tmapi.competition_clubs_url(competition_id, actual_year)
        with pytest.raises(current.CurrentCaptureError, match='another edition'):
            current.FullRosterSnapshot.from_mapping({**snap.as_dict(), 'squad_saison_id': 2026})


def test_snapshot_refuses_synthetic_scope_id_or_missing_explicit_source_year():
    value = snapshot().as_dict()
    with pytest.raises(current.CurrentCaptureError, match='registry competition/edition'):
        current.FullRosterSnapshot.from_mapping({**value, 'scope_id': 'GB1:2026'})
    with pytest.raises(current.CurrentCaptureError, match='explicit source year'):
        current.FullRosterSnapshot.from_mapping({**value, 'squad_saison_id': None})


def test_reused_snapshot_must_match_new_owner_registry_identity():
    old = snapshot()
    with pytest.raises(current.CurrentCaptureError, match='competition_id'):
        current.assemble_full_roster(old, ('10', '20'), {}, (), competition_id='CDB')
    with pytest.raises(current.CurrentCaptureError, match='squad_saison_id'):
        current.assemble_full_roster(old, ('10', '20'), {}, (), squad_saison_id=2025)


def empty_club(club_id='10', *, committed=True, at=NOW):
    capture = club(club_id, rows=[], committed=committed, at=at)
    return {**capture, 'applicability_status': 'authoritative_empty', 'authoritative_empty_proof': {
        'kind': 'typed_fetch_state', 'status': 'authoritative_empty',
        'raw_capture_id': capture['raw_capture_id'], 'source_body_hash': capture['source_body_hash'],
    }}


def test_authoritative_empty_club_requires_matching_typed_raw_proof():
    empty = current.ClubRosterSnapshot.from_mapping('10', empty_club())
    assert empty.rows == ()
    assert empty.applicability_status == 'authoritative_empty'
    assert empty.authoritative_empty_proof['raw_capture_id'] == empty.raw_capture_id
    assert current.ClubRosterSnapshot.from_mapping('10', empty.as_dict()) == empty
    for change in ({'authoritative_empty_proof': None}, {'applicability_status': 'unknown'},
                   {'applicability_status': 'retry_exhausted'}, {'applicability_status': 'ok'}):
        with pytest.raises(current.CurrentCaptureError):
            current.ClubRosterSnapshot.from_mapping('10', {**empty_club(), **change})


@pytest.mark.parametrize('field,value', [('raw_capture_id', 'foreign-raw'), ('source_body_hash', 'd' * 64),
                                       ('status', 'ok'), ('kind', 'negative_cache')])
def test_empty_proof_cannot_be_borrowed_from_another_result(field, value):
    payload = empty_club()
    payload['authoritative_empty_proof'] = {**payload['authoritative_empty_proof'], field: value}
    with pytest.raises(current.CurrentCaptureError, match='matching typed raw proof'):
        current.ClubRosterSnapshot.from_mapping('10', payload)


def test_authoritative_empty_cannot_contain_player_rows():
    with pytest.raises(current.CurrentCaptureError, match='no rows'):
        current.ClubRosterSnapshot.from_mapping('10', {**empty_club(), 'rows': [player()]})


def test_retained_verified_empty_club_keeps_applicability_and_lineage():
    old = snapshot({'10': empty_club(), '20': club('20', [player('2', '20')])})
    assembled = current.assemble_full_roster(old, old.expected_team_ids, {}, ())
    assert len(assembled.rows) == 1
    assert assembled.expected_team_ids == ('10', '20')
    assert assembled.clubs['10'].applicability_status == 'authoritative_empty'
    assert assembled.clubs['10'].raw_fetched_at == NOW
    assert assembled.clubs['10'].authoritative_empty_proof == old.clubs['10'].authoritative_empty_proof
    assert current.FullRosterSnapshot.from_mapping(assembled.as_dict()).as_dict() == assembled.as_dict()


def test_new_proven_empty_club_can_complete_cold_capture_without_emptying_existing_roster():
    fresh = current.assemble_full_roster(None, ('10',), {'10': empty_club(committed=False)}, ('10',),
                                        scope_id=SCOPE_ID, competition_id='GB1', edition_id='2026', squad_saison_id=2026)
    assert fresh.rows == []
    assert fresh.expected_team_ids == ('10',)
    with pytest.raises(current.CurrentCaptureError, match='90%'):
        current.assemble_full_roster(snapshot(), ('10', '20'),
                                     {'10': empty_club(committed=False, at=NOW + timedelta(hours=1))}, ('10',))


def test_empty_participant_list_cannot_be_inferred_by_roster_constructor():
    with pytest.raises(current.CurrentCaptureError, match='nonempty'):
        current.FullRosterSnapshot.from_mapping({
            'scope_id': SCOPE_ID, 'competition_id': 'GB1', 'edition_id': '2026', 'squad_saison_id': 2026,
            'expected_team_ids': [], 'clubs': {},
        })
